"""Run the locked, unchanged Talk Python wire-target contracts against BriskDB.

Only TINYMONGO_MONGODB_URI selects the candidate. The original fixture provides
both stock-PyMongo APIs and its own async adapter; no test globals are replaced.
"""
import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import pymongo

LOCK = "53cbf44e98b8caa036163725d195fd29592e1cc0"
MODULE = "tests.contracts.test_talkpython_contract"
FIXTURE = "tests/contracts/test_talkpython_contract.py"
SOURCES = {
    FIXTURE: "56ea087b75a6797d8bcb9557a39b73a5622514e95917fa7151c795d3531ec99a",
    "tests/contracts/conftest.py": "002944603a433d408c1c2b9ce067a27195c6bf78753f27086a2a5b380d5b31d5",
    "tests/contracts/support.py": "822c4b9739243ab3885f3b4912deb9de8b9c22a1a2d11a9b9b725a9f57c39e5a",
    "tests/contracts/__init__.py": "ce7185ce04b04dbce35c37a155ff35b26bf878939a3c188b66c8286f06db1999",
    "pyproject.toml": "8336c1a47de2ebf2f066364a15a9d5557576c39468a896e8423bab3df33e4a2e",
}
VERSIONS = {"pymongo": "4.17.0", "pytest": "8.4.2", "tinymongo": "1.3.0"}
CASES = (
    "projection_none_absent_fields_and_integer_id",
    "find_one_sort_returns_the_newest_match",
    "single_and_multi_key_sort_break_ties",
    "cursor_to_list_accepts_both_application_spellings",
    "cursor_skip_limit_windows_and_past_end",
    "cursor_limit_zero_means_unlimited",
    "scalar_equality_matches_an_array_member",
    "dot_notation_matches_an_embedded_document",
    "not_regex_with_case_insensitive_options",
    "nin_excludes_values_and_includes_missing_fields",
    "null_negation_distinguishes_missing_and_non_null_fields",
    "nin_and_negated_regex_share_one_field_specification",
    "write_result_metadata_used_by_the_application",
    "inc_creates_a_missing_counter",
    "replace_one_preserves_id_and_replaces_the_full_document",
    "distinct_returns_scalar_field_values",
    "create_indexes_accepts_the_real_mixed_batch_and_enforces_unique",
    "object_id_and_datetime_round_trip_and_range_query",
    "generated_id_uses_the_standard_object_id_round_trip",
    "invalid_document_has_a_bson_compatible_error",
    "binary_round_trip_query_and_mongodb_sort_order",
    "generic_binary_equality_matches_native_bytes_and_query_operators",
    "binary_ids_use_bson_equality_without_losing_subtype",
    "boolean_and_numeric_ids_are_bson_distinct",
    "mixed_timezone_datetimes_sort_by_utc_instant",
    "insert_many_accepts_a_document_generator",
    "duplicate_errors_are_catchable_as_pymongo_errors",
)


def expected_cases():
    cases = {f"test_{case}[{api}-mongodb]": api for case in CASES for api in ("sync", "async")}
    for api in ("sync", "async"):
        for parameter in ("True-expected_ids0-1", "False-expected_ids1-2"):
            cases[f"test_insert_many_reports_compatible_partial_failures[{api}-mongodb-{parameter}]"] = api
    assert len(cases) == 58
    return cases


def validate_sources(root):
    for path, digest in SOURCES.items():
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == digest, f"locked source changed: {path}"
    # Do not allow an unverified parent pytest hook to alter collection/bodies.
    for path in ("conftest.py", "tests/conftest.py", "tests/__init__.py", "pytest.ini", "tox.ini"):
        assert not (root / path).exists(), f"unexpected pytest input: {path}"


def validate_report(root):
    assert root.tag == "testsuites"
    suites = root.findall("testsuite")
    assert len(root) == len(suites) == 1 and suites[0].get("name") == "pytest"
    suite = suites[0]
    assert int(suite.get("tests", "-1")) == 58
    for field in ("errors", "failures", "skipped"):
        assert int(suite.get(field, "-1")) == 0, f"non-passing {field}"
    expected = expected_cases()
    cases = suite.findall("testcase")
    assert Counter(case.get("name") for case in cases) == Counter(expected.keys()), "missing, duplicate or unexpected cases"
    executions = []
    for case in cases:
        name = case.get("name")
        assert case.get("classname") == MODULE
        assert all(child.tag in ("properties", "system-out", "system-err") for child in case), f"non-passing outcome: {name}"
        properties = case.findall("properties/property")
        assert len(properties) == 3
        metadata = {prop.get("name"): prop.get("value") for prop in properties}
        assert metadata == {
            "tinymongo.api": expected[name], "tinymongo.backend": "mongodb", "tinymongo.suite": "talkpython",
        }, f"wrong contract target: {name}"
        executions.append({"name": name, "api": expected[name], "outcome": "passed"})
    return executions


def pytest_environment(uri):
    environment = os.environ.copy()
    environment.update(TINYMONGO_MONGODB_URI=uri, TINYMONGO_REQUIRE_MONGODB="1",
                       PYTHONDONTWRITEBYTECODE="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
                       PYTHONOPTIMIZE="0")
    environment.pop("PYTEST_ADDOPTS", None)
    environment.pop("PYTEST_PLUGINS", None)
    return environment


def checkpoint(uri, phase, after=False):
    # Upstream fixture databases are deliberately dropped during teardown.
    # This independent sentinel proves this harness reopened the same root;
    # it is not a claim that those application fixtures retain their documents.
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=10000) as client:
        collection = client.briskdb_talkpython_checkpoint.sentinel
        document = {"_id": "restart", "value": [1, "same-root", {"binary": b"checkpoint"}]}
        if phase == "initial" and not after:
            assert collection.count_documents({}) == 0
            collection.insert_one(document)
            collection.create_index("value", name="sentinel_value")
        assert collection.find_one({"_id": "restart"}) == document
        assert collection.index_information()["sentinel_value"] == {"key": [("value", 1)]}


def run(uri, source_root, phase, report_path):
    if not __debug__:
        raise RuntimeError("optimized Python would disable acceptance assertions")
    assert phase in ("initial", "reopened")
    source_root = source_root.resolve()
    report_path = report_path.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"source_commit": LOCK, "target": "briskdb-real-pymongo", "fixture_profile": "mongodb",
              "phase": phase, "source_hashes": SOURCES, "executions": [], "outcome": "failed"}
    try:
        validate_sources(source_root)
        versions = {name: importlib.metadata.version(name) for name in VERSIONS}
        assert versions == VERSIONS, versions
        report["versions"] = versions
        checkpoint(uri, phase)
        # Ignore caller-injected pytest flags/plugins; the source fixture, case
        # inventory and report are the authority for this exact gate.
        environment = pytest_environment(uri)
        with tempfile.TemporaryDirectory(prefix="briskdb-talkpython-report-") as temporary:
            xml = Path(temporary) / "results.xml"
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-o", "addopts=", "-m", "mongodb", FIXTURE,
                 "-p", "no:cacheprovider", "--confcutdir=" + str(source_root), "--junitxml=" + str(xml), "-q"],
                cwd=source_root, env=environment, timeout=120, check=False,
            )
            # A fresh per-run XML path prevents a previous success hiding an
            # empty/failed run. Preserve even failing reports for diagnosis.
            if xml.exists():
                assert xml.stat().st_size <= 4 * 1024 * 1024
                report_path.with_suffix(".xml").write_bytes(xml.read_bytes())
            assert result.returncode == 0, f"Talk Python pytest exit {result.returncode}"
            report["executions"] = validate_report(ET.parse(xml).getroot())
        checkpoint(uri, phase, after=True)
        report["persisted_sentinel"] = "passed"
        report["outcome"] = "passed"
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Verified all 58 unchanged Talk Python wire contracts and restart sentinel: {phase}.")


if __name__ == "__main__":
    assert len(sys.argv) == 5, "URI SOURCE_ROOT PHASE REPORT_PATH"
    run(sys.argv[1], Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
