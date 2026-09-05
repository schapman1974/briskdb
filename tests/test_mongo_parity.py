import argparse
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from xml.etree import ElementTree


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "mongo_parity.py"
SPEC = importlib.util.spec_from_file_location("mongo_parity", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mongo_parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mongo_parity)

APIS = ("sync", "async")
BACKENDS = (
    "memory",
    "json",
    "sqlite",
    "sqlite-sharded",
    "duckdb",
    "parquet",
    "mongodb",
)


def add_testcase(
    suite,
    name,
    *,
    api="sync",
    backend="memory",
    contract_suite="core",
    properties=None,
    outcome=None,
    outcome_type=None,
    message=None,
):
    testcase = ElementTree.SubElement(
        suite,
        "testcase",
        classname="tests.contracts.test_contract",
        name=name,
    )
    testcase_properties = ElementTree.SubElement(testcase, "properties")
    values = [
        ("tinymongo.api", api),
        ("tinymongo.backend", backend),
        ("tinymongo.suite", contract_suite),
    ]
    values.extend(properties or [])
    for key, value in values:
        ElementTree.SubElement(testcase_properties, "property", name=key, value=value)
    if outcome is not None:
        attributes = {}
        if outcome_type is not None:
            attributes["type"] = outcome_type
        if message is not None:
            attributes["message"] = message
        ElementTree.SubElement(testcase, outcome, **attributes)
    return testcase


def write_junit(path, testcases):
    root = ElementTree.Element("testsuites")
    suite = ElementTree.SubElement(root, "testsuite", name="contract")
    testcases(suite)
    ElementTree.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def execution(target, outcome="passed", *, reason=None, observation=None):
    value = {
        "api": "sync",
        "backend": target.rsplit("-", 1)[-1],
        "case_id": "tests.contracts.test_contract::test_case",
        "outcome": outcome,
        "reason": reason,
        "suite": "core",
        "target": target,
    }
    if observation is not None:
        value["observation"] = observation
    return value


class JunitIngestTests(unittest.TestCase):
    def test_only_exact_tinymongo_properties_define_contract_dimensions(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "results.xml"

            def cases(suite):
                add_testcase(
                    suite,
                    "test_case[sync-memory]",
                    properties=[
                        ("impostor.api", "async"),
                        ("impostor.backend", "mongodb"),
                        ("impostor.suite", "other"),
                    ],
                )

            write_junit(junit, cases)
            result = mongo_parity.ingest_junit(junit, "tinymongo")

        self.assertEqual(len(result["executions"]), 1)
        self.assertEqual(result["executions"][0]["api"], "sync")
        self.assertEqual(result["executions"][0]["backend"], "memory")
        self.assertEqual(result["executions"][0]["suite"], "core")

    def test_suffix_lookalike_properties_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "results.xml"

            def cases(suite):
                testcase = add_testcase(suite, "test_case[sync-memory]")
                properties = next(
                    child for child in testcase if child.tag == "properties"
                )
                for property_element in properties:
                    property_element.set(
                        "name", "impostor." + property_element.get("name")
                    )

            write_junit(junit, cases)
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "tinymongo.api.*tinymongo.backend"
            ):
                mongo_parity.ingest_junit(junit, "tinymongo")

    def test_target_prefix_is_removed_without_consuming_equal_user_parameters(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "results.xml"

            def cases(suite):
                add_testcase(
                    suite,
                    "test_case[sync-sqlite-sharded-sync-sqlite-sharded]",
                    api="sync",
                    backend="sqlite-sharded",
                )

            write_junit(junit, cases)
            result = mongo_parity.ingest_junit(junit, "candidate")

        self.assertEqual(
            result["executions"][0]["case_id"],
            "tests.contracts.test_contract::test_case[sync-sqlite-sharded]",
        )

    def test_target_tokens_outside_the_parameter_prefix_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "results.xml"

            def cases(suite):
                add_testcase(suite, "test_case[variant-sync-memory]")

            write_junit(junit, cases)
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "does not begin with API/backend"
            ):
                mongo_parity.ingest_junit(junit, "tinymongo")

    def test_explicit_contract_id_supports_vendored_runner_modules(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "results.xml"

            def cases(suite):
                add_testcase(
                    suite,
                    "test_case[sync-memory]",
                    properties=[
                        (
                            "tinymongo.contract_id",
                            "tests.contracts.test_contract::test_case",
                        )
                    ],
                )

            write_junit(junit, cases)
            result = mongo_parity.ingest_junit(junit, "tinymongo")

        self.assertEqual(
            result["executions"][0]["case_id"],
            "tests.contracts.test_contract::test_case",
        )

    def test_reason_redaction_preserves_uris_and_produces_stable_fingerprint(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_directory = Path(temporary_directory)
            first = temporary_directory / "first.xml"
            second = temporary_directory / "second.xml"
            message_one = (
                "failed at /Users/alice/project/test.py:42; "
                "docs https://example.test/api/v1; "
                "database mongodb://localhost:27017/contracts"
            )
            message_two = (
                "failed at /home/bob/project/test.py:42; "
                "docs https://example.test/api/v1; "
                "database mongodb://localhost:27017/contracts"
            )

            def first_case(suite):
                add_testcase(
                    suite,
                    "test_case[sync-memory]",
                    outcome="failure",
                    outcome_type="AssertionError",
                    message=message_one,
                )

            def second_case(suite):
                add_testcase(
                    suite,
                    "test_case[sync-memory]",
                    outcome="failure",
                    outcome_type="AssertionError",
                    message=message_two,
                )

            write_junit(first, first_case)
            write_junit(second, second_case)
            observed_one = mongo_parity.ingest_junit(first, "candidate")["executions"][
                0
            ]
            observed_two = mongo_parity.ingest_junit(second, "candidate")["executions"][
                0
            ]

        self.assertIn("https://example.test/api/v1", observed_one["reason"])
        self.assertIn("mongodb://localhost:27017/contracts", observed_one["reason"])
        self.assertNotIn("/Users/alice", observed_one["reason"])
        self.assertEqual(observed_one["reason"], observed_two["reason"])
        self.assertEqual(
            observed_one["observation"]["fingerprint"],
            observed_two["observation"]["fingerprint"],
        )
        self.assertEqual(observed_one["observation"]["category"], "AssertionError")
        self.assertRegex(observed_one["observation"]["fingerprint"], r"^[0-9a-f]{64}$")


class SnapshotMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source_temporary_directory = tempfile.TemporaryDirectory()
        cls.source_root = Path(cls.source_temporary_directory.name)
        contracts = cls.source_root / "tests" / "contracts"
        contracts.mkdir(parents=True)
        (contracts / "__init__.py").write_text("", encoding="utf-8")
        (contracts / "test_contract.py").write_text(
            "# Frozen synthetic contract source.\n", encoding="utf-8"
        )
        subprocess.run(["git", "init", "-q"], cwd=cls.source_root, check=True)
        subprocess.run(
            ["git", "config", "user.email", "parity-tests@example.invalid"],
            cwd=cls.source_root,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Parity Tests"],
            cwd=cls.source_root,
            check=True,
        )
        subprocess.run(
            ["git", "add", "tests/contracts"], cwd=cls.source_root, check=True
        )
        subprocess.run(
            ["git", "commit", "-q", "-m", "synthetic contract"],
            cwd=cls.source_root,
            check=True,
        )
        cls.source_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=cls.source_root, text=True
        ).strip()

    @classmethod
    def tearDownClass(cls):
        cls.source_temporary_directory.cleanup()

    def write_matrix(self, path, *, omit=None, duplicate=None):
        def cases(suite):
            for case_number in range(228):
                for api in APIS:
                    for backend in BACKENDS:
                        cell = (case_number, api, backend)
                        if cell == omit:
                            continue
                        add_testcase(
                            suite,
                            "test_case_{0:03d}[{1}-{2}]".format(
                                case_number, api, backend
                            ),
                            api=api,
                            backend=backend,
                        )
                        if cell == duplicate:
                            add_testcase(
                                suite,
                                "test_case_{0:03d}[{1}-{2}]".format(
                                    case_number, api, backend
                                ),
                                api=api,
                                backend=backend,
                            )

        write_junit(path, cases)

    def test_complete_matrix_yields_228_cases_and_memory_reference(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "matrix.xml"
            self.write_matrix(junit)
            corpus, reference, digest = mongo_parity.snapshot_corpus(
                junit, self.source_root, self.source_commit
            )

        self.assertEqual(len(corpus["cases"]), 228)
        self.assertTrue(
            all(case["apis"] == ["sync", "async"] for case in corpus["cases"])
        )
        self.assertEqual(len(reference["executions"]), 456)
        self.assertEqual(
            {item["target"] for item in reference["executions"]},
            {"tinymongo-memory"},
        )
        self.assertRegex(digest, r"^[0-9a-f]{64}$")

    def test_snapshot_rejects_one_missing_matrix_cell(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "matrix.xml"
            self.write_matrix(junit, omit=(227, "async", "mongodb"))
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "complete.*matrix|matrix.*missing"
            ):
                mongo_parity.snapshot_corpus(
                    junit, self.source_root, self.source_commit
                )

    def test_snapshot_rejects_one_duplicate_matrix_cell(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "matrix.xml"
            self.write_matrix(junit, duplicate=(0, "sync", "memory"))
            with self.assertRaisesRegex(mongo_parity.ContractError, "duplicate"):
                mongo_parity.snapshot_corpus(
                    junit, self.source_root, self.source_commit
                )


class StrictDifferenceTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {
            "contract": "tinymongo-v1",
            "source": {"commit": "0" * 40},
        }
        self.corpus = {
            "contract": "tinymongo-v1",
            "cases": [
                {
                    "id": "tests.contracts.test_contract::test_case",
                    "apis": ["sync"],
                }
            ],
        }
        self.reference = {
            "schema_version": 1,
            "contract": "tinymongo-v1",
            "executions": [execution("tinymongo-memory")],
        }

    def difference(self):
        return {
            "api": "sync",
            "candidate_outcome": "failed",
            "candidate_fingerprint": "1" * 64,
            "case_id": "tests.contracts.test_contract::test_case",
            "issue": "https://github.com/schapman1974/briskdb/issues/161",
            "reason": "reviewed synthetic difference",
            "reference_outcome": "passed",
            "target": "briskdb-memory",
        }

    def test_allowlist_for_absent_target_is_stale_and_fails_report(self):
        difference = self.difference()
        report, _, failed = mongo_parity.compare_results(
            self.manifest,
            self.corpus,
            {"differences": [difference]},
            [self.reference],
            "tinymongo-memory",
            require_allowlist_targets=True,
        )

        self.assertTrue(failed)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["stale_intentional_differences"], [difference])

    def test_optional_allowlist_target_can_be_absent_from_reference_report(self):
        report, _, failed = mongo_parity.compare_results(
            self.manifest,
            self.corpus,
            {"differences": [self.difference()]},
            [self.reference],
            "tinymongo-memory",
        )

        self.assertFalse(failed)
        self.assertEqual(report["status"], "reference-only")
        self.assertEqual(report["stale_intentional_differences"], [])

    def test_allowlist_is_stale_when_target_no_longer_differs(self):
        difference = self.difference()
        candidate = {
            "schema_version": 1,
            "contract": "tinymongo-v1",
            "executions": [execution("briskdb-memory")],
        }
        report, _, failed = mongo_parity.compare_results(
            self.manifest,
            self.corpus,
            {"differences": [difference]},
            [self.reference, candidate],
            "tinymongo-memory",
        )

        self.assertTrue(failed)
        self.assertEqual(report["stale_intentional_differences"], [difference])

    def test_allowlist_rejects_a_different_failure_fingerprint(self):
        difference = self.difference()
        candidate = {
            "schema_version": 1,
            "contract": "tinymongo-v1",
            "executions": [
                execution(
                    "briskdb-memory",
                    "failed",
                    reason="a different failure",
                    observation={
                        "category": "AssertionError",
                        "fingerprint": "2" * 64,
                    },
                )
            ],
        }
        report, _, failed = mongo_parity.compare_results(
            self.manifest,
            self.corpus,
            {"differences": [difference]},
            [self.reference, candidate],
            "tinymongo-memory",
        )

        self.assertTrue(failed)
        self.assertFalse(report["targets"][0]["mismatches"][0]["allowed"])

    def test_allowlist_accepts_the_reviewed_failure_fingerprint(self):
        difference = self.difference()
        candidate = {
            "schema_version": 1,
            "contract": "tinymongo-v1",
            "executions": [
                execution(
                    "briskdb-memory",
                    "failed",
                    reason="the reviewed failure",
                    observation={
                        "category": "AssertionError",
                        "fingerprint": "1" * 64,
                    },
                )
            ],
        }
        report, _, failed = mongo_parity.compare_results(
            self.manifest,
            self.corpus,
            {"differences": [difference]},
            [self.reference, candidate],
            "tinymongo-memory",
        )

        self.assertFalse(failed)
        self.assertEqual(report["status"], "passed")
        self.assertTrue(report["targets"][0]["mismatches"][0]["allowed"])


class CommandValidationTests(unittest.TestCase):
    def test_ingest_validates_contract_before_reading_results(self):
        arguments = argparse.Namespace(manifest=Path("manifest.json"))
        with mock.patch.object(
            mongo_parity,
            "validate_contract",
            side_effect=mongo_parity.ContractError("invalid contract sentinel"),
        ) as validate:
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "invalid contract sentinel"
            ):
                mongo_parity._command_ingest(arguments)
        validate.assert_called_once_with(arguments.manifest)

    def test_report_validates_contract_before_reading_results(self):
        arguments = argparse.Namespace(manifest=Path("manifest.json"))
        with mock.patch.object(
            mongo_parity,
            "validate_contract",
            side_effect=mongo_parity.ContractError("invalid contract sentinel"),
        ) as validate:
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "invalid contract sentinel"
            ):
                mongo_parity._command_report(arguments)
        validate.assert_called_once_with(arguments.manifest)


if __name__ == "__main__":
    unittest.main()
