import copy
import hashlib
import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "mongo_parity.py"
SPEC = importlib.util.spec_from_file_location("mongo_parity_source_provenance", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mongo_parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mongo_parity)


def sha256(contents):
    return hashlib.sha256(contents).hexdigest()


class MongoSourceProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.fixture_root = Path(self.temporary_directory.name)
        self.source_root = self.fixture_root / "source"
        self.contract_base = self.fixture_root / "v1"
        self.runner_root = self.contract_base / "runner"
        self.source_root.mkdir()

        self.upstream = {
            "tests/contracts/exact.py": b"EXACT = 1\n",
            "tests/contracts/adapted.py": b'VALUE = "upstream"\n',
            "tests/contracts/README.md": b"Upstream notes.\n",
            "LICENSE.txt": b"Fixture license.\n",
        }
        for relative, contents in self.upstream.items():
            target = self.source_root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(contents)

        self.git("init", "-q")
        self.git("config", "user.name", "BriskDB Tests")
        self.git("config", "user.email", "briskdb-tests@example.invalid")
        self.git("add", ".")
        self.git("commit", "-q", "-m", "fixture")
        self.commit = self.git("rev-parse", "HEAD").strip()

        self.exact_runner = "compat/mongo/v1/runner/contracts/exact.py"
        self.adapted_runner = "compat/mongo/v1/runner/contracts/adapted.py"
        self.license_runner = "compat/mongo/v1/runner/UPSTREAM_LICENSE.txt"
        self.patch_runner = "compat/mongo/v1/runner/adaptation.patch"
        self.adapted_contents = b'VALUE = "adapted"\n'
        self.patch_contents = (
            "--- a/tests/contracts/adapted.py\n"
            "+++ b/compat/mongo/v1/runner/contracts/adapted.py\n"
            "@@ -1 +1 @@\n"
            '-VALUE = "upstream"\n'
            '+VALUE = "adapted"\n'
        ).encode("utf-8")

        self.write_runner(self.exact_runner, self.upstream["tests/contracts/exact.py"])
        self.write_runner(self.adapted_runner, self.adapted_contents)
        self.write_runner(self.license_runner, self.upstream["LICENSE.txt"])
        self.write_runner(self.patch_runner, self.patch_contents)

        self.provenance = {
            "source": {
                "commit": self.commit,
                "license": self.source_entry(
                    "LICENSE.txt",
                    runner_path=self.license_runner,
                    runner_contents=self.upstream["LICENSE.txt"],
                ),
            },
            "adaptation": {
                "files": [
                    self.source_entry(
                        "tests/contracts/exact.py",
                        runner_path=self.exact_runner,
                        runner_contents=self.upstream["tests/contracts/exact.py"],
                        status="exact",
                    ),
                    self.source_entry(
                        "tests/contracts/adapted.py",
                        runner_path=self.adapted_runner,
                        runner_contents=self.adapted_contents,
                        status="adapted",
                    ),
                ],
                "excluded_upstream_files": [
                    self.source_entry("tests/contracts/README.md")
                ],
                "patch": self.patch_runner,
                "patch_sha256": sha256(self.patch_contents),
            },
        }
        self.provenance_path = self.runner_root / "provenance.json"
        self.write_provenance()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def git(self, *arguments):
        return subprocess.check_output(
            ["git", "-C", str(self.source_root)] + list(arguments),
            text=True,
        )

    def blob_id(self, relative):
        return self.git("rev-parse", "{0}:{1}".format(self.commit, relative)).strip()

    def source_entry(
        self,
        relative,
        *,
        runner_path=None,
        runner_contents=None,
        status=None,
    ):
        entry = {
            "upstream_path": relative,
            "upstream_git_blob": self.blob_id(relative),
            "upstream_sha256": sha256(self.upstream[relative]),
        }
        if runner_path is not None:
            entry["runner_path"] = runner_path
            entry["runner_sha256"] = sha256(runner_contents)
        if status is not None:
            entry["status"] = status
        return entry

    def write_runner(self, relative, contents):
        target = self.contract_base.joinpath(*Path(relative).parts[3:])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)

    def write_provenance(self):
        self.provenance_path.parent.mkdir(parents=True, exist_ok=True)
        self.provenance_path.write_text(
            json.dumps(self.provenance, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def verify(self):
        mongo_parity._verify_source_provenance(
            self.source_root,
            self.commit,
            self.provenance_path,
            self.contract_base,
        )

    def test_verifies_blob_oids_license_and_replays_adaptation_patch(self):
        self.verify()

    def test_rejects_contract_blob_oid_drift_even_when_sha256_is_unchanged(self):
        adapted = self.provenance["adaptation"]["files"][1]
        adapted["upstream_git_blob"] = self.blob_id("tests/contracts/exact.py")
        self.write_provenance()

        with self.assertRaisesRegex(
            mongo_parity.ContractError,
            "Git blob mismatch for tests/contracts/adapted.py",
        ):
            self.verify()

    def test_rejects_license_blob_oid_drift(self):
        license_entry = self.provenance["source"]["license"]
        license_entry["upstream_git_blob"] = self.blob_id("tests/contracts/exact.py")
        self.write_provenance()

        with self.assertRaisesRegex(
            mongo_parity.ContractError, "Git blob mismatch for LICENSE.txt"
        ):
            self.verify()

    def test_rejects_a_license_copy_that_differs_from_the_pinned_blob(self):
        self.write_runner(self.license_runner, b"Different license.\n")

        with self.assertRaisesRegex(
            mongo_parity.ContractError, "license is not byte-for-byte upstream"
        ):
            self.verify()

    def test_rejects_patch_output_that_does_not_reconstruct_runner(self):
        bad_patch = self.patch_contents.replace(b'"adapted"', b'"different"')
        self.write_runner(self.patch_runner, bad_patch)
        self.provenance["adaptation"]["patch_sha256"] = sha256(bad_patch)
        self.write_provenance()

        with self.assertRaisesRegex(
            mongo_parity.ContractError, "adaptation patch does not reconstruct"
        ):
            self.verify()

    def test_rejects_patch_paths_outside_the_adapted_inventory(self):
        provenance = copy.deepcopy(self.provenance)
        provenance["adaptation"]["files"][1]["status"] = "exact"
        self.provenance = provenance
        self.write_provenance()

        with self.assertRaisesRegex(
            mongo_parity.ContractError, "adaptation patch path inventory mismatch"
        ):
            self.verify()


if __name__ == "__main__":
    unittest.main()
