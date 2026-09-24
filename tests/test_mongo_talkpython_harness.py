"""Require all locked wire cases, not merely a green or nonempty pytest run."""
import tempfile
import os
import subprocess
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch

import mongo_talkpython_client as runner


def report():
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", name="pytest", tests="58", errors="0", failures="0", skipped="0")
    for name, api in runner.expected_cases().items():
        case = ET.SubElement(suite, "testcase", classname=runner.MODULE, name=name)
        properties = ET.SubElement(case, "properties")
        for key, value in (("api", api), ("backend", "mongodb"), ("suite", "talkpython")):
            ET.SubElement(properties, "property", name="tinymongo." + key, value=value)
    return root


class TalkPythonHarnessTests(unittest.TestCase):
    def test_environment_cannot_silently_select_collect_only_or_disable_assertions(self):
        with patch.dict(os.environ, {"PYTEST_ADDOPTS": "--collect-only", "PYTEST_PLUGINS": "unexpected", "PYTHONOPTIMIZE": "1"}):
            environment = runner.pytest_environment("mongodb://127.0.0.1:12345/")
        self.assertNotIn("PYTEST_ADDOPTS", environment)
        self.assertNotIn("PYTEST_PLUGINS", environment)
        self.assertEqual(environment["PYTHONOPTIMIZE"], "0")
        self.assertEqual(environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"], "1")
        self.assertEqual(environment["TINYMONGO_REQUIRE_MONGODB"], "1")
        result = subprocess.run(
            [sys.executable, "-O", "-c", "import mongo_talkpython_client as r; r.run(None, None, None, None)"],
            cwd=Path(runner.__file__).parent, capture_output=True, text=True, timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("optimized Python would disable acceptance assertions", result.stderr)

    def test_exact_sync_async_wire_inventory_is_required(self):
        executions = runner.validate_report(report())
        self.assertEqual(len(executions), 58)
        self.assertEqual(sum(case["api"] == "sync" for case in executions), 29)
        for mode in ("empty", "missing", "duplicate", "unexpected", "wrong_class", "wrong_backend", "wrong_api", "wrong_suite", "duplicate_property", "missing_property"):
            with self.subTest(mode=mode):
                root = report()
                suite = root[0]
                first = suite[0]
                if mode == "empty": suite.clear()
                elif mode == "missing": suite.remove(first)
                elif mode == "duplicate": suite[1].set("name", first.get("name"))
                elif mode == "unexpected": first.set("name", "another_test[sync-mongodb]")
                elif mode == "wrong_class": first.set("classname", "tests.other")
                elif mode == "wrong_backend": first[0][1].set("value", "sqlite")
                elif mode == "wrong_api": first[0][0].set("value", "async")
                elif mode == "wrong_suite": first[0][2].set("value", "core")
                elif mode == "duplicate_property": first[0][2].set("name", "tinymongo.api")
                elif mode == "missing_property": first[0].remove(first[0][0])
                with self.assertRaises(AssertionError): runner.validate_report(root)

    def test_failures_errors_skips_and_xfails_cannot_look_successful(self):
        for outcome in ("failure", "error", "skipped", "xfailed", "xpassed"):
            with self.subTest(outcome=outcome):
                root = report()
                ET.SubElement(root[0][0], outcome)
                with self.assertRaises(AssertionError): runner.validate_report(root)
        for field in ("errors", "failures", "skipped", "tests"):
            root = report()
            root[0].set(field, "1")
            with self.assertRaises(AssertionError): runner.validate_report(root)

    def test_source_mismatch_is_rejected_before_running_tests(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / runner.FIXTURE
            fixture.parent.mkdir(parents=True)
            fixture.write_text("raise RuntimeError('must not execute')\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "locked source changed"):
                runner.validate_sources(root)


if __name__ == "__main__":
    unittest.main()
