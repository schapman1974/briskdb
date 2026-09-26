"""Exact source accounting for the two additional original query suites."""

from collections import Counter
from copy import deepcopy
import json
import os
from pathlib import Path
import re
import subprocess
import unittest

from test_index_suite_inventory import ROOT, validate_inventory


INVENTORY = ROOT / "compat/mongo/query-suite-inventory.json"


class QuerySuiteInventoryTests(unittest.TestCase):
    def test_query_inventory_names_every_function_and_real_candidate_tests(self):
        validate_inventory(json.loads(INVENTORY.read_text()), expected_scope=(2, 38, 51))

    def test_query_inventory_rejects_omitted_duplicated_or_unexplained_source(self):
        original = json.loads(INVENTORY.read_text())
        for change in [
            lambda data: data["suites"][0]["coverage"][0]["source"].pop(),
            lambda data: data["suites"][0]["coverage"][0]["source"].append("test_nin_query_operator"),
            lambda data: data["suites"][0]["coverage"][0].update(note=""),
            lambda data: data["suites"][0]["coverage"][0].update(kind="assumed_pass"),
            lambda data: data["suites"][0]["coverage"][0].update(evidence=["python/tests/test_upstream_queries.py::ids"]),
            lambda data: data["suites"][0].update(reference_case_count=18),
        ]:
            data = deepcopy(original)
            change(data)
            with self.assertRaises(ValueError):
                validate_inventory(data, expected_scope=(2, 38, 51))

    @unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_SOURCE_ROOT")
                         and os.environ.get("BRISKDB_MONGO_ORACLE_PYTHON"),
                         "requires isolated locked source and reference interpreter")
    def test_query_source_hash_membership_and_reference_parameter_counts(self):
        data = json.loads(INVENTORY.read_text())
        source = Path(os.environ["BRISKDB_MONGO_ORACLE_SOURCE_ROOT"]).resolve()
        validate_inventory(data, source, expected_scope=(2, 38, 51))
        command = [os.environ["BRISKDB_MONGO_ORACLE_PYTHON"], "-m", "pytest",
                   "--collect-only", "-qq", "-o", "addopts="]
        command.extend(suite["path"] for suite in data["suites"])
        output = subprocess.run(command, cwd=source,
                                env=dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1"),
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(output.returncode, 0, output.stdout + output.stderr)
        counts = Counter()
        for line in output.stdout.splitlines():
            if not line.strip():
                continue
            match = re.fullmatch(r"(tests/test_\w+\.py): (\d+)", line)
            self.assertIsNotNone(match, output.stdout)
            self.assertNotIn(match[1], counts)
            counts[match[1]] = int(match[2])
        self.assertEqual(dict(counts), {suite["path"]: suite["reference_case_count"] for suite in data["suites"]})


if __name__ == "__main__":
    unittest.main()
