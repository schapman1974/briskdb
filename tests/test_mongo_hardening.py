import importlib.util
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "mongo_parity.py"
SPEC = importlib.util.spec_from_file_location("mongo_parity_hardening", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mongo_parity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mongo_parity)

CASE_ID = "tests.contracts.test_contract::test_case"


def corpus():
    return {
        "schema_version": 1,
        "contract": "tinymongo-v1",
        "cases": [
            {
                "apis": ["sync"],
                "id": CASE_ID,
                "requirements": {},
                "source": "tests/contracts/test_contract.py",
                "suite": "core",
            }
        ],
    }


def execution(
    *,
    target="candidate-memory",
    backend="memory",
    suite="core",
    outcome="passed",
    category="passed",
    reason=None,
):
    return {
        "api": "sync",
        "backend": backend,
        "case_id": CASE_ID,
        "observation": mongo_parity._observation(outcome, category, reason),
        "outcome": outcome,
        "reason": reason,
        "suite": suite,
        "target": target,
    }


def results(item):
    return {
        "schema_version": 1,
        "contract": "tinymongo-v1",
        "executions": [item],
    }


def write_junit(path, *, name="test_case[sync-memory]", extra_properties=()):
    root = ElementTree.Element("testsuites")
    suite = ElementTree.SubElement(root, "testsuite", name="contract")
    testcase = ElementTree.SubElement(
        suite,
        "testcase",
        classname="compat.mongo.v1.runner.contracts.test_contract",
        name=name,
    )
    properties = ElementTree.SubElement(testcase, "properties")
    values = [
        ("tinymongo.api", "sync"),
        ("tinymongo.backend", "memory"),
        ("tinymongo.suite", "core"),
    ]
    values.extend(extra_properties)
    for property_name, value in values:
        ElementTree.SubElement(properties, "property", name=property_name, value=value)
    ElementTree.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    return testcase


class JunitMetadataHardeningTests(unittest.TestCase):
    def test_identical_duplicate_required_property_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "duplicate.xml"
            write_junit(junit, extra_properties=(("tinymongo.api", "sync"),))
            with self.assertRaisesRegex(mongo_parity.ContractError, "lacks unique"):
                mongo_parity.ingest_junit(junit, "candidate")

    def test_identical_duplicate_explicit_contract_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "duplicate-id.xml"
            write_junit(
                junit,
                extra_properties=(
                    ("tinymongo.contract_id", CASE_ID),
                    ("tinymongo.contract_id", CASE_ID),
                ),
            )
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "unique tinymongo.contract_id"
            ):
                mongo_parity.ingest_junit(junit, "candidate")

    def test_explicit_contract_id_does_not_bypass_target_prefix_validation(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "wrong-prefix.xml"
            write_junit(
                junit,
                name="test_case[async-memory]",
                extra_properties=(("tinymongo.contract_id", CASE_ID),),
            )
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "does not begin with API/backend"
            ):
                mongo_parity.ingest_junit(junit, "candidate")

    def test_explicit_contract_id_requires_target_parameters_in_name(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            junit = Path(temporary_directory) / "missing-prefix.xml"
            write_junit(
                junit,
                name="test_case",
                extra_properties=(("tinymongo.contract_id", CASE_ID),),
            )
            with self.assertRaisesRegex(
                mongo_parity.ContractError, "lacks API/backend parameters"
            ):
                mongo_parity.ingest_junit(junit, "candidate")


class ResultInvariantTests(unittest.TestCase):
    def test_suite_must_match_the_corpus_case(self):
        with self.assertRaisesRegex(mongo_parity.ContractError, "suite.*corpus"):
            mongo_parity.validate_results(
                results(execution(suite="talkpython")), corpus()
            )

    def test_backend_must_match_the_target_suffix(self):
        with self.assertRaisesRegex(mongo_parity.ContractError, "backend.*target"):
            mongo_parity.validate_results(results(execution(backend="json")), corpus())

    def test_passed_result_requires_passed_category(self):
        with self.assertRaisesRegex(
            mongo_parity.ContractError, "requires category passed"
        ):
            mongo_parity.validate_results(
                results(execution(category="AssertionError")), corpus()
            )

    def test_passed_result_requires_null_reason(self):
        with self.assertRaisesRegex(mongo_parity.ContractError, "null reason"):
            mongo_parity.validate_results(results(execution(reason="")), corpus())


class FailureFingerprintTests(unittest.TestCase):
    def test_suffix_after_500_characters_changes_fingerprint(self):
        prefix = "x" * 500
        observed = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            for suffix in ("A", "B"):
                junit = Path(temporary_directory) / (suffix + ".xml")
                write_junit(junit)
                tree = ElementTree.parse(junit)
                testcase = next(tree.getroot().iter("testcase"))
                ElementTree.SubElement(
                    testcase,
                    "failure",
                    type="AssertionError",
                    message=prefix + suffix,
                )
                tree.write(junit, encoding="utf-8", xml_declaration=True)
                observed.append(
                    mongo_parity.ingest_junit(junit, "candidate")["executions"][0]
                )

        self.assertEqual(observed[0]["reason"], prefix + "A")
        self.assertEqual(observed[1]["reason"], prefix + "B")
        self.assertNotEqual(
            observed[0]["observation"]["fingerprint"],
            observed[1]["observation"]["fingerprint"],
        )

    def test_report_truncates_reason_without_truncating_fingerprint_input(self):
        prefix = "x" * 500
        reference_execution = execution(
            target="tinymongo-memory",
            outcome="failed",
            category="AssertionError",
            reason=prefix + "A",
        )
        candidate_execution = execution(
            outcome="failed",
            category="AssertionError",
            reason=prefix + "B",
        )
        report, _, failed = mongo_parity.compare_results(
            {
                "contract": "tinymongo-v1",
                "source": {"commit": "0" * 40},
            },
            corpus(),
            {"differences": []},
            [results(reference_execution), results(candidate_execution)],
            "tinymongo-memory",
        )

        self.assertTrue(failed)
        mismatch = next(
            target["mismatches"][0]
            for target in report["targets"]
            if target["target"] == "candidate-memory"
        )
        self.assertEqual(mismatch["expected_reason"], prefix)
        self.assertEqual(mismatch["observed_reason"], prefix)
        self.assertNotEqual(
            mismatch["expected_fingerprint"], mismatch["observed_fingerprint"]
        )


if __name__ == "__main__":
    unittest.main()
