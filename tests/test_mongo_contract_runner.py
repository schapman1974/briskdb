import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
COMPAT = ROOT / "compat" / "mongo" / "v1"
RUNNER = COMPAT / "runner"
PROVENANCE = RUNNER / "provenance.json"
TINYMONGO_ADAPTER = RUNNER / "adapters" / "tinymongo.py"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def imported_roots(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module.split(".", 1)[0])
    return imports


def cargo_manifest_declares_dependency(contents, dependency):
    dependency = dependency.lower()
    section = ""
    dependency_section = False

    for raw_line in contents.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line.strip("[]").strip().lower()
            components = section.split(".")
            dependency_section = any(
                component in {"dependencies", "dev-dependencies", "build-dependencies"}
                for component in components
            )
            if dependency_section and components[-1] == dependency:
                return True
            continue
        if not dependency_section:
            continue

        assignment = re.match(r"""^["']?([a-z0-9_-]+)["']?\s*=""", line.lower())
        if assignment and assignment.group(1) == dependency:
            return True
        if re.search(
            r"""\bpackage\s*=\s*["']{}["']""".format(re.escape(dependency)),
            line,
            flags=re.IGNORECASE,
        ):
            return True

    return False


def cargo_lock_contains_package(contents, dependency):
    return bool(
        re.search(
            r"""(?m)^name\s*=\s*["']{}["']\s*$""".format(re.escape(dependency)),
            contents,
            flags=re.IGNORECASE,
        )
    )


def pyproject_declares_dependency(contents, dependency):
    dependency = re.escape(dependency)
    requirement = re.compile(
        r"""["']{}(?=[<>=!~;\[\]\s"'])""".format(dependency),
        flags=re.IGNORECASE,
    )
    poetry_key = re.compile(
        r"""(?m)^\s*["']?{}["']?\s*=""".format(dependency),
        flags=re.IGNORECASE,
    )
    return bool(requirement.search(contents) or poetry_key.search(contents))


class MongoContractProvenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.provenance = json.loads(PROVENANCE.read_text(encoding="utf-8"))
        cls.corpus = json.loads((COMPAT / "corpus.json").read_text(encoding="utf-8"))

    def test_frozen_source_identity_and_local_hashes(self):
        source = self.provenance["source"]
        self.assertEqual(source["commit"], "53cbf44e98b8caa036163725d195fd29592e1cc0")
        self.assertEqual(
            source["contract_git_tree"],
            "6f671122ff5f76902cfe051e4558a2c57d161bc4",
        )
        self.assertEqual(
            source["runtime_git_tree"],
            "0c94915ea81697a5728f64dc04e3929bf1e036e8",
        )

        license_entry = source["license"]
        self.assertEqual(
            sha256(ROOT / license_entry["runner_path"]),
            license_entry["runner_sha256"],
        )
        self.assertEqual(
            license_entry["runner_sha256"], license_entry["upstream_sha256"]
        )

        patch = self.provenance["adaptation"]
        patch_path = ROOT / patch["patch"]
        self.assertEqual(sha256(patch_path), patch["patch_sha256"])
        patch_text = patch_path.read_text(encoding="utf-8")

        for entry in patch["files"]:
            runner_path = ROOT / entry["runner_path"]
            self.assertEqual(sha256(runner_path), entry["runner_sha256"])
            if entry["status"] == "exact":
                self.assertEqual(entry["runner_sha256"], entry["upstream_sha256"])
            else:
                self.assertEqual(entry["status"], "adapted")
                self.assertNotEqual(entry["runner_sha256"], entry["upstream_sha256"])
                self.assertIn("--- a/" + entry["upstream_path"], patch_text)
                self.assertIn("+++ b/" + entry["runner_path"], patch_text)

        for entry in self.provenance["runner_files"]:
            self.assertEqual(sha256(ROOT / entry["path"]), entry["sha256"])

    def test_every_owned_runner_file_is_listed(self):
        adaptation = self.provenance["adaptation"]
        listed = {entry["runner_path"] for entry in adaptation["files"]}
        listed.update(entry["path"] for entry in self.provenance["runner_files"])
        listed.add(adaptation["patch"])
        listed.add(self.provenance["source"]["license"]["runner_path"])

        actual = {
            path.relative_to(ROOT).as_posix()
            for path in RUNNER.rglob("*")
            if path.is_file()
            and path != PROVENANCE
            and "__pycache__" not in path.parts
            and path.suffix not in (".pyc", ".pyo")
        }
        self.assertEqual(actual, listed)

    def test_manifest_transitively_locks_the_provenance_ledger(self):
        manifest = json.loads((COMPAT / "manifest.json").read_text(encoding="utf-8"))
        relative = manifest["files"]["runner_provenance"]
        self.assertEqual(relative, "runner/provenance.json")
        self.assertEqual(
            sha256(COMPAT / relative), manifest["sha256"]["runner_provenance"]
        )

    def test_public_validator_rejects_runner_file_drift(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            copied_compat = Path(temporary_directory) / "v1"
            shutil.copytree(
                COMPAT,
                copied_compat,
                ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache", "*.pyc"),
            )
            requirements = copied_compat / "runner" / "requirements.txt"
            requirements.write_text(
                requirements.read_text(encoding="utf-8") + "# drift\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "mongo_parity.py"),
                    "validate",
                    "--manifest",
                    str(copied_compat / "manifest.json"),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("requirements.txt hash mismatch", completed.stderr)

    def test_provenance_accounts_for_every_frozen_contract_source(self):
        adaptation = self.provenance["adaptation"]
        recorded = {
            entry["upstream_path"]: entry["upstream_sha256"]
            for entry in adaptation["files"]
        }
        recorded.update(
            {
                entry["upstream_path"]: entry["upstream_sha256"]
                for entry in adaptation["excluded_upstream_files"]
            }
        )
        catalog = {
            entry["path"]: entry["sha256"] for entry in self.corpus["source_files"]
        }
        self.assertEqual(recorded, catalog)
        self.assertEqual(
            self.corpus["source_commit"], self.provenance["source"]["commit"]
        )
        self.assertEqual(
            self.corpus["source_git_tree"],
            self.provenance["source"]["contract_git_tree"],
        )

    def test_all_228_catalog_cases_resolve_to_vendored_test_functions(self):
        cases = self.corpus["cases"]
        self.assertEqual(len(cases), 228)
        self.assertTrue(all(case["apis"] == ["sync", "async"] for case in cases))

        definitions = {}
        for module_path in sorted((RUNNER / "contracts").glob("test_*.py")):
            tree = ast.parse(
                module_path.read_text(encoding="utf-8"), filename=str(module_path)
            )
            definitions[module_path.stem] = {
                node.name
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }

        self.assertEqual(len(definitions), 16)
        for case in cases:
            module_name, item_name = case["id"].split("::", 1)
            module_name = module_name.rsplit(".", 1)[-1]
            function_name = item_name.partition("[")[0]
            self.assertIn(module_name, definitions)
            self.assertIn(function_name, definitions[module_name])


class MongoContractImportBoundaryTests(unittest.TestCase):
    def test_only_the_tinymongo_adapter_imports_tinymongo(self):
        importers = []
        for path in sorted(RUNNER.rglob("*.py")):
            if "tinymongo" in imported_roots(path):
                importers.append(path)
        self.assertEqual(importers, [TINYMONGO_ADAPTER])

    def test_runtime_package_manifests_do_not_depend_on_tinymongo(self):
        cargo_manifests = ("Cargo.toml", "python/Cargo.toml")
        for relative in cargo_manifests:
            contents = (ROOT / relative).read_text(encoding="utf-8")
            self.assertFalse(
                cargo_manifest_declares_dependency(contents, "tinymongo"), relative
            )

        lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
        self.assertFalse(cargo_lock_contains_package(lock, "tinymongo"), "Cargo.lock")

        pyproject = (ROOT / "python/pyproject.toml").read_text(encoding="utf-8")
        self.assertFalse(
            pyproject_declares_dependency(pyproject, "tinymongo"),
            "python/pyproject.toml",
        )

    def test_dependency_checks_distinguish_features_from_packages(self):
        feature_only = """
[features]
# Strict offline migration from TinyMongo's SQLite formats.
tinymongo-import = ["documents", "sqlite-import"]
"""
        self.assertFalse(cargo_manifest_declares_dependency(feature_only, "tinymongo"))
        self.assertTrue(
            cargo_manifest_declares_dependency(
                '[dependencies]\ntinymongo = "1.3"', "tinymongo"
            )
        )
        self.assertTrue(
            cargo_manifest_declares_dependency(
                '[dependencies]\noracle = { package = "tinymongo", version = "1.3" }',
                "tinymongo",
            )
        )
        self.assertTrue(
            cargo_lock_contains_package(
                '[[package]]\nname = "tinymongo"\nversion = "1.3.0"', "tinymongo"
            )
        )
        self.assertTrue(
            pyproject_declares_dependency(
                '[project]\ndependencies = ["tinymongo>=1.3"]', "tinymongo"
            )
        )

    def test_pinned_runner_requirements_exclude_the_oracle_package(self):
        requirements = RUNNER / "requirements.txt"
        for number, raw_line in enumerate(
            requirements.read_text(encoding="utf-8").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            self.assertNotIn("tinymongo", line.lower(), "line {0}".format(number))

    def test_importing_adapter_registry_loads_no_target_dependencies(self):
        code = "\n".join(
            [
                "import sys",
                "sys.path.insert(0, {!r})".format(str(ROOT)),
                "import compat.mongo.v1.runner.adapters",
                "loaded = sorted(name for name in sys.modules "
                "if name == 'tinymongo' or name.startswith('tinymongo.') "
                "or name == 'pymongo' or name.startswith('pymongo.'))",
                "assert loaded == [], loaded",
            ]
        )
        subprocess.run([sys.executable, "-I", "-c", code], check=True)

    def test_briskdb_adapter_requires_an_explicit_candidate_uri(self):
        sys.path.insert(0, str(ROOT))
        try:
            from compat.mongo.v1.runner.adapters import TargetUnavailable, open_target

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch.dict(
                    os.environ,
                    {"BRISKDB_MONGO_PARITY_BRISKDB_URI": ""},
                ):
                    with self.assertRaisesRegex(
                        TargetUnavailable, "BRISKDB_MONGO_PARITY_BRISKDB_URI"
                    ):
                        with open_target("briskdb", "sync", temporary_directory):
                            self.fail("the unconfigured adapter unexpectedly yielded")
        finally:
            if sys.path[0] == str(ROOT):
                sys.path.pop(0)

    def test_briskdb_adapter_forwards_uri_through_pymongo_transport(self):
        sys.path.insert(0, str(ROOT))
        try:
            from compat.mongo.v1.runner.adapters import TargetHandles, load_adapter

            adapter = load_adapter("briskdb")
            captured = {}
            client = object()
            database = object()
            collection = object()

            @contextmanager
            def fake_transport(target_name, api, tmp_path, options, **kwargs):
                captured.update(
                    target_name=target_name,
                    api=api,
                    tmp_path=tmp_path,
                    options=options,
                    kwargs=kwargs,
                )
                yield TargetHandles(
                    name=target_name,
                    transport="pymongo",
                    api=api,
                    client=client,
                    database=database,
                    collection=collection,
                    unsupported_warning=UserWarning,
                )

            with tempfile.TemporaryDirectory() as temporary_directory:
                with mock.patch.object(
                    adapter, "open_pymongo_target", new=fake_transport
                ):
                    with adapter.open_target(
                        "async",
                        temporary_directory,
                        {"appname": "mongo-contract"},
                        backend="briskdb",
                        uri="mongodb://127.0.0.1:27018/?directConnection=true",
                        database_name="contract_database",
                        collection_name="contract_collection",
                        connect_timeout=2.5,
                    ) as handles:
                        self.assertEqual(handles.name, "briskdb")
                        self.assertEqual(handles.transport, "pymongo")
                        self.assertIs(handles.client, client)
                        self.assertIs(handles.database, database)
                        self.assertIs(handles.collection, collection)

                    with mock.patch.dict(
                        os.environ,
                        {
                            "BRISKDB_MONGO_PARITY_BRISKDB_URI": (
                                "mongodb://127.0.0.1:27019/?directConnection=true"
                            )
                        },
                    ):
                        with adapter.open_target(
                            "sync", temporary_directory
                        ) as handles:
                            self.assertEqual(handles.name, "briskdb")
                            self.assertEqual(handles.transport, "pymongo")

                    self.assertEqual(
                        captured["kwargs"]["uri"],
                        "mongodb://127.0.0.1:27019/?directConnection=true",
                    )

                    with adapter.open_target(
                        "async",
                        temporary_directory,
                        {"appname": "mongo-contract"},
                        backend="briskdb",
                        uri="mongodb://127.0.0.1:27018/?directConnection=true",
                        database_name="contract_database",
                        collection_name="contract_collection",
                        connect_timeout=2.5,
                    ):
                        pass

            self.assertEqual(captured["target_name"], "briskdb")
            self.assertEqual(captured["api"], "async")
            self.assertEqual(captured["tmp_path"], temporary_directory)
            self.assertEqual(captured["options"], {"appname": "mongo-contract"})
            self.assertEqual(captured["kwargs"]["backend"], "briskdb")
            self.assertEqual(
                captured["kwargs"]["uri"],
                "mongodb://127.0.0.1:27018/?directConnection=true",
            )
            self.assertEqual(captured["kwargs"]["database_name"], "contract_database")
            self.assertEqual(
                captured["kwargs"]["collection_name"], "contract_collection"
            )
            self.assertEqual(captured["kwargs"]["connect_timeout"], 2.5)
        finally:
            if sys.path[0] == str(ROOT):
                sys.path.pop(0)


if __name__ == "__main__":
    unittest.main()
