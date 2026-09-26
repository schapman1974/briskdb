"""Native SQLite storage contracts and explicit alternative-backend limits."""

from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import briskdb
from briskdb import mongo


class UpstreamStorageBackendTests(unittest.TestCase):
    def test_default_and_sqlite_aliases_use_native_collection_storage_and_reopen(self):
        for backend in [None, "sqlite", "sqlite-sharded"]:
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as root:
                options = {} if backend is None else {"backend": backend}
                with briskdb.MongoClient(root, shards=2, **options) as client:
                    client.app.users.insert_one({"_id": 1, "name": "Ada", "age": 36})
                    client.app.events.insert_one({"_id": "e1", "kind": "login"})
                    self.assertEqual(set(client.app.list_collection_names()), {"users", "events"})
                    self.assertEqual(client.app.users.count_documents({}), 1)
                    self.assertEqual(client.app.events.count_documents({}), 1)
                    for value, attribute in [(client, "_backend"), (client, "_storage"), (client.app, "_foldername")]:
                        with self.assertRaises(AttributeError):
                            getattr(value, attribute)
                self.assertTrue((Path(root) / "manifest.sqlite").is_file())
                self.assertFalse((Path(root) / "app.sqlite").exists())
                self.assertFalse((Path(root) / "app.json").exists())
                self.assertFalse((Path(root) / "_storage.json").exists())
                rows = 0
                for shard in range(2):
                    connection = sqlite3.connect(str(Path(root) / "shards" / f"{shard:04}.sqlite"))
                    try:
                        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                        self.assertIn("briskdb_documents_v1", tables)
                        self.assertFalse(tables & {"users", "events", "tinydb"})
                        rows += connection.execute("SELECT count(*) FROM briskdb_documents_v1").fetchone()[0]
                    finally:
                        connection.close()
                self.assertEqual(rows, 2)
                with briskdb.MongoClient(root) as reader:
                    self.assertEqual(reader.app.users.find_one({"_id": 1}), {"_id": 1, "name": "Ada", "age": 36})
                    self.assertEqual(reader.app.events.find_one({"_id": "e1"}), {"_id": "e1", "kind": "login"})

    def test_common_storage_queries_mutations_and_natural_counts_survive_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, backend="sqlite-sharded", shards=4) as client:
                users = client.app.users
                users.insert_many([
                    {"_id": 1, "name": "Ada", "age": 36, "active": True, "tags": ["math"]},
                    {"_id": 2, "name": "Grace", "age": 40, "active": False, "tags": ["code"]},
                    {"_id": 3, "name": "Katherine", "age": 34, "tags": ["math", "space"]},
                ])
                self.assertEqual([row["_id"] for row in users.find({"age": {"$gte": 36}}).sort("_id")], [1, 2])
                self.assertEqual(users.count_documents({"name": {"$in": ["Ada", "Grace"]}}), 2)
                self.assertEqual(users.count_documents({"active": {"$exists": False}}), 1)
                self.assertEqual(users.count_documents({"tags": {"$all": ["math", "space"]}}), 1)
                result = users.update_one({"name": "Ada"}, {"$inc": {"age": 1}})
                self.assertEqual((result.matched_count, result.modified_count), (1, 1))
                self.assertEqual(users.find_one({"_id": 1})["age"], 37)
                self.assertEqual(users.delete_many({"age": {"$lt": 37}}).deleted_count, 1)
                self.assertEqual(users.count_documents({}), 2)
                expected = list(users.find({}).sort("_id"))
            with briskdb.MongoClient(root) as reader:
                self.assertEqual(list(reader.app.users.find({}).sort("_id")), expected)

    def test_alternative_backends_reject_before_storage_acquisition(self):
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "not-created"
            for client_class in [briskdb.MongoClient, briskdb.AsyncMongoClient]:
                for backend in ["tinydb", "json", "memory", "parquet", "parquetv2", "duckdb", "postgres", "mysql"]:
                    with self.subTest(client=client_class.__name__, backend=backend):
                        with mock.patch.object(mongo, "acquire", side_effect=AssertionError("storage acquired")):
                            with self.assertRaisesRegex(ValueError, "SQLite"):
                                client_class(root, backend=backend)
                        self.assertFalse(root.exists())


if __name__ == "__main__":
    unittest.main()
