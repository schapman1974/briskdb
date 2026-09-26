"""Check source coverage accounting separately from candidate execution results."""

import ast
from collections import Counter
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "compat" / "mongo" / "index-suite-inventory.json"
SOURCE_COMMIT = "53cbf44e98b8caa036163725d195fd29592e1cc0"
KINDS = {"public_scenarios", "native_equivalent", "implementation_specific", "contract_difference"}


def checked_path(root, relative):
    path = (root / relative).resolve()
    if root.resolve() not in path.parents or not path.is_file():
        raise ValueError(f"evidence must be a file beneath its root: {relative}")
    return path


def validate_inventory(data, source_root=None):
    if data["schema_version"] != 1 or data["source_commit"] != SOURCE_COMMIT:
        raise ValueError("unexpected source/schema version")
    source_paths = set()
    total_functions = total_cases = 0
    symbols = {}
    for suite in data["suites"]:
        if suite["path"] in source_paths:
            raise ValueError("duplicate source suite")
        source_paths.add(suite["path"])
        names = []
        for group in suite["coverage"]:
            if group["kind"] not in KINDS or not group["note"].strip():
                raise ValueError("every group needs an explicit classification and rationale")
            if not group["source"] or not group["evidence"]:
                raise ValueError("every source group needs concrete evidence")
            names.extend(group["source"])
            for selector in group["evidence"]:
                relative, symbol = selector.split("::")
                if relative not in symbols:
                    path = checked_path(ROOT, relative)
                    text = path.read_text()
                    if path.suffix == ".py":
                        symbols[relative] = {node.name for node in ast.walk(ast.parse(text))
                                             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                                             and node.name.startswith("test_")}
                    elif path.suffix == ".rs":
                        symbols[relative] = set(re.findall(
                            r"#\[(?:tokio::)?test[^\]]*\]\s*(?:#\[[^\]]*\]\s*)*"
                            r"(?:async\s+)?fn\s+(\w+)\s*\(", text))
                    else:
                        raise ValueError("evidence must name a Python or Rust test")
                if symbol not in symbols[relative]:
                    raise ValueError(f"missing candidate test: {selector}")
        if len(names) != len(set(names)) or len(names) != suite["function_count"]:
            raise ValueError("duplicate or missing source functions")
        if any(not name.startswith("test_") for name in names):
            raise ValueError("source entries must name tests")
        if source_root is not None:
            source = checked_path(source_root, suite["path"]).read_bytes()
            if hashlib.sha256(source).hexdigest() != suite["sha256"]:
                raise ValueError(f"source hash changed: {suite['path']}")
            actual = {node.name for node in ast.parse(source).body
                      if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
            if set(names) != actual:
                raise ValueError(f"unmapped or unexpected source test: {suite['path']}")
        total_functions += len(names)
        total_cases += suite["reference_case_count"]
    if len(source_paths) != 4 or (total_functions, total_cases) != (73, 217):
        raise ValueError("source inventory changed; review the complete mapped scope")


class IndexSuiteInventoryTests(unittest.TestCase):
    def test_inventory_names_existing_candidate_tests_and_every_source_function_once(self):
        validate_inventory(json.loads(INVENTORY.read_text()))

    def test_inventory_rejects_missing_duplicate_and_unexplained_evidence(self):
        data = json.loads(INVENTORY.read_text())
        for mutate in [
            lambda copy: copy["suites"][0]["coverage"][0]["source"].pop(),
            lambda copy: copy["suites"][0]["coverage"][0]["source"].append(
                copy["suites"][0]["coverage"][0]["source"][0]),
            lambda copy: copy["suites"][0]["coverage"][0].update(note=""),
            lambda copy: copy["suites"][0]["coverage"][0].update(evidence=[]),
            lambda copy: copy["suites"][0]["coverage"][0].update(kind="assumed_pass"),
            lambda copy: copy["suites"][0]["coverage"][0].update(
                evidence=["python/tests/test_index_models.py::nonexistent_test"]),
            lambda copy: copy["suites"][0]["coverage"][0].update(
                evidence=["src/storage/document/enabled/index_operations/tests.rs::snapshot"]),
        ]:
            changed = deepcopy(data)
            mutate(changed)
            with self.assertRaises(ValueError):
                validate_inventory(changed)

    @unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_SOURCE_ROOT")
                         and os.environ.get("BRISKDB_MONGO_ORACLE_PYTHON"),
                         "requires isolated source-locked oracle checkout and interpreter")
    def test_locked_source_inventory_and_collected_reference_case_counts(self):
        source_root = Path(os.environ["BRISKDB_MONGO_ORACLE_SOURCE_ROOT"]).resolve()
        data = json.loads(INVENTORY.read_text())
        validate_inventory(data, source_root)
        environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
        command = [os.environ["BRISKDB_MONGO_ORACLE_PYTHON"], "-m", "pytest",
                   "--collect-only", "-qq", "-o", "addopts="]
        command.extend(suite["path"] for suite in data["suites"])
        result = subprocess.run(command, cwd=source_root, env=environment,
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        collected = Counter()
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            match = re.fullmatch(r"(tests/test_\w+\.py): (\d+)", line)
            self.assertIsNotNone(match, result.stdout)
            self.assertNotIn(match[1], collected, "duplicate source collection report")
            collected[match[1]] = int(match[2])
        self.assertEqual(dict(collected), {suite["path"]: suite["reference_case_count"] for suite in data["suites"]})


if __name__ == "__main__":
    unittest.main()
