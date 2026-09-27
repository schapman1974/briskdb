"""Exact source accounting for original query and public-client suites."""

from collections import Counter
from copy import deepcopy
import ast
import hashlib
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
        validate_inventory(json.loads(INVENTORY.read_text()), expected_scope=(58, 883, 1504))

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
                validate_inventory(data, expected_scope=(58, 883, 1504))

    @unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_SOURCE_ROOT")
                         and os.environ.get("BRISKDB_MONGO_ORACLE_PYTHON"),
                         "requires isolated locked source and reference interpreter")
    def test_query_source_hash_membership_and_reference_parameter_counts(self):
        data = json.loads(INVENTORY.read_text())
        source = Path(os.environ["BRISKDB_MONGO_ORACLE_SOURCE_ROOT"]).resolve()
        validate_inventory(data, source, expected_scope=(58, 883, 1504))
        command = [os.environ["BRISKDB_MONGO_ORACLE_PYTHON"], "-m", "pytest",
                   "--collect-only", "-qq", "-o", "addopts="]
        command.extend(suite["path"] for suite in data["suites"])
        environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
        # A legacy module opens its MongoDB URI during collection. Inventory
        # must never contact or mutate an ambient user-configured database.
        environment.pop("TINYMONGO_MONGODB_URI", None)
        output = subprocess.run(command, cwd=source, env=environment,
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

    @unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_SOURCE_ROOT"),
                         "requires isolated locked source")
    def test_every_top_level_source_suite_has_an_inventory_or_locked_odm_harness(self):
        source = Path(os.environ["BRISKDB_MONGO_ORACLE_SOURCE_ROOT"]).resolve()
        query = json.loads(INVENTORY.read_text())
        indexes = json.loads((ROOT / "compat/mongo/index-suite-inventory.json").read_text())
        query_paths = {suite["path"] for suite in query["suites"]}
        index_paths = {suite["path"] for suite in indexes["suites"]}
        self.assertFalse(query_paths & index_paths)
        # Read the pinned ODM fixture ledger without importing its reference
        # dependencies into the candidate-wheel interpreter.
        module = ast.parse((ROOT / "tests/mongo_odm_client.py").read_text())
        assignments = {target.id: node.value for node in module.body if isinstance(node, ast.Assign)
                       for target in node.targets if isinstance(target, ast.Name)}
        self.assertEqual(ast.literal_eval(assignments["LOCK"]), query["source_commit"])
        fixtures = ast.literal_eval(assignments["FIXTURES"])
        self.assertEqual([fixture[0] for fixture in fixtures], ["beanie", "mongoengine"])
        odm_paths = set()
        for _name, relative, digest, symbol in fixtures:
            path = (source / relative).resolve()
            self.assertIn(source, path.parents)
            content = path.read_bytes()
            self.assertEqual(hashlib.sha256(content).hexdigest(), digest)
            functions = {node.name for node in ast.parse(content).body
                         if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                         and node.name.startswith("test_")}
            self.assertEqual(functions, {symbol})
            self.assertNotIn(relative, odm_paths)
            odm_paths.add(relative)
        self.assertFalse(odm_paths & (query_paths | index_paths))
        actual = {path.relative_to(source).as_posix() for path in (source / "tests").glob("test_*.py")}
        self.assertEqual(actual, query_paths | index_paths | odm_paths)


if __name__ == "__main__":
    unittest.main()
