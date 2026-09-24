"""Gate-safety checks that do not require the actual ODM dependencies."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import mongo_odm_client as runner


class OdmHarnessTests(unittest.TestCase):
    def test_source_mismatch_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / runner.FIXTURES[0][1]
            path.parent.mkdir()
            path.write_text("raise RuntimeError('must never execute')\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "fixture changed"):
                runner.load_fixture(root, runner.FIXTURES[0])

    def test_exact_successful_fixture_set_is_required_without_skips(self):
        successful = [{"name": name, "outcome": "passed"} for name in ("beanie", "mongoengine")]
        runner.validate_executions(successful)
        for entries in [[], successful[:1], successful[::-1], [successful[0]] * 2,
                        [successful[0], {"name": "mongoengine", "outcome": "skipped"}],
                        [successful[0], {"name": "mongoengine", "outcome": "failed"}]]:
            with self.assertRaises(AssertionError):
                runner.validate_executions(entries)

    def test_factories_change_only_connection_configuration(self):
        uri = "mongodb://127.0.0.1:12345/?directConnection=true"
        factories = runner.connection_factories(uri)
        with patch.object(runner.pymongo, "AsyncMongoClient") as asynchronous:
            result = factories["beanie"].AsyncMongoClient(tinymongo_folder="unused", backend="sqlite")
            self.assertIs(result, asynchronous.return_value)
            asynchronous.assert_called_once_with(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=10000)
        with patch.object(runner.pymongo, "MongoClient") as synchronous:
            result = factories["mongoengine"].MongoClient(tinymongo_folder="unused", host="localhost", uuidRepresentation="standard")
            self.assertIs(result, synchronous.return_value)
            synchronous.assert_called_once_with(host=uri, uuidRepresentation="standard", serverSelectionTimeoutMS=3000, socketTimeoutMS=10000)
        self.assertIs(factories["mongoengine"].generate_id, runner.tinymongo.generate_id)
        with self.assertRaises(AssertionError):
            factories["beanie"].AsyncMongoClient(tinymongo_folder="unused", backend="memory")


if __name__ == "__main__":
    unittest.main()
