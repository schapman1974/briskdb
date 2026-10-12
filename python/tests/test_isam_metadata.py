"""Hybrid metadata source-wheel coverage; published default wheels stay SQLite."""

import os
from pathlib import Path
import tempfile
import unittest

import briskdb


class IsamMetadataTests(unittest.TestCase):
    def native_config(self, **kwargs):
        try:
            return briskdb.Config(metadata_backend="isam", **kwargs)
        except briskdb.UnsupportedError:
            if os.environ.get("BRISKDB_TEST_REQUIRE_ISAM") == "1":
                raise
            self.skipTest("wheel was built without experimental-isam")

    def test_sqlite_stays_default_and_selection_is_immutable(self):
        config = briskdb.Config()
        self.assertEqual(config.metadata_backend, "sqlite")
        with self.assertRaises(AttributeError):
            config.metadata_backend = "isam"
        with self.assertRaises(briskdb.InvalidArgumentError):
            briskdb.Config(metadata_backend="unknown")

    def test_native_metadata_crud_transactions_and_reopen(self):
        config = self.native_config(shards=2)
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, config=config) as database:
                with database.session(routing_key="one") as session:
                    session.migrate("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
                with database.transaction(routing_key="one") as transaction:
                    transaction.execute("INSERT INTO notes VALUES (?, ?)", [1, "committed"])
                with self.assertRaisesRegex(RuntimeError, "rollback"):
                    with database.transaction(routing_key="one") as transaction:
                        transaction.execute("INSERT INTO notes VALUES (?, ?)", [2, "discarded"])
                        raise RuntimeError("rollback")
            self.assertTrue((Path(root) / "manifest.isam").is_file())
            self.assertFalse((Path(root) / "manifest.sqlite").exists())
            with briskdb.open(root, config=self.native_config()) as database:
                with database.session(routing_key="one") as session:
                    self.assertEqual(session.query("SELECT * FROM notes")["rows"], [(1, "committed")])
            with self.assertRaises(briskdb.FailedPreconditionError):
                briskdb.open(root, shards=2)
            self.assertFalse((Path(root) / "manifest.sqlite").exists())

    def test_document_selection_fails_before_root_creation(self):
        config = self.native_config(shards=2, documents=True)
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "not-created"
            with self.assertRaises(briskdb.UnsupportedError):
                briskdb.open(root, config=config)
            self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
