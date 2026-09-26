"""Explicit optional oracle gate: two isolated interpreters, no client substitution."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import warnings

from bson import decode_all
from pymongo.errors import OperationFailure

import briskdb


@unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_PYTHON"), "requires source-locked test-only oracle")
class IndexModelsReferenceTests(unittest.TestCase):
    def test_source_locked_public_index_models(self):
        oracle = Path(__file__).resolve().parents[2] / "tests" / "mongo_index_models_oracle.py"
        output = subprocess.run([os.environ["BRISKDB_MONGO_ORACLE_PYTHON"], str(oracle)],
                                capture_output=True, timeout=60)
        self.assertEqual(output.returncode, 0, output.stderr.decode())
        self.assertLess(len(output.stdout), 1024 * 1024)
        cases = decode_all(output.stdout)
        self.assertEqual(len(cases), 144)
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder, shards=2) as client:
                for number, case in enumerate(cases):
                    with self.subTest(number=number):
                        collection = client.app[f"case_{number}"]
                        collection.create_indexes([case["base"]])
                        with warnings.catch_warnings(record=True) as caught:
                            warnings.simplefilter("always")
                            try:
                                outcome = {"names": collection.create_indexes(case["models"])}
                            except OperationFailure as error:
                                outcome = {"unsupported": True} if error.code == 115 else {"error": error.code}
                        self.assertEqual(outcome, case["outcome"])
                        self.assertEqual(len(caught), case["warning_count"])
                        actual = collection.index_information()
                        for spec in actual.values():
                            spec["key"] = [list(pair) for pair in spec["key"]]
                        self.assertEqual(actual, case["indexes"])
                        # Stay under the declared namespace cap without changing
                        # any index expectation or source result.
                        collection.drop()


if __name__ == "__main__":
    unittest.main()
