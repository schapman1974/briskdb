"""ODM discovery and remaining public index/cursor lifecycle edges."""

import tempfile
import unittest
import warnings

from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError, InvalidOperation, OperationFailure

import briskdb
from briskdb.mongo import IndexCompatibilityWarning


class UpstreamDiscoveryEdgeTests(unittest.TestCase):
    def test_sync_discovery_identifies_briskdb_filters_names_and_preserves_closed_state(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            database = client.app
            database.items.insert_one({"_id": 1})
            info = database.command({"buildInfo": 1})
            self.assertEqual(database.command("buildinfo"), info)
            self.assertEqual(client.server_info(), info)
            self.assertTrue(info["version"].endswith("-briskdb"))
            self.assertEqual(len(info["versionArray"]), 4)
            self.assertTrue(all(type(part) is int for part in info["versionArray"]))
            self.assertNotIn("tinymongo", info)
            self.assertEqual(info["ok"], 1.0)
            for command in ("ping", {"ping": 1}):
                self.assertEqual(database.command(command), {"ok": 1.0})
            self.assertEqual(database.list_collection_names(authorizedCollections=True, nameOnly=True), ["items"])
            self.assertEqual(database.list_collection_names(None), ["items"])
            self.assertEqual(database.list_collection_names(filter={"name": "items"}), ["items"])
            self.assertEqual(database.list_collection_names(filter={"name": "missing"}), [])
            with self.assertRaises(OperationFailure) as caught:
                database.command("serverStatus")
            self.assertEqual(caught.exception.code, 59)
            with self.assertRaises(OperationFailure) as caught:
                database.list_collection_names(unknown=True)
            self.assertEqual(caught.exception.code, 72)
            # Keep the pinned driver's malformed-input errors distinct from
            # TinyMongo's private database wrapper and detached-object hooks.
            operations = [
                (lambda: database.command("ping", session=object()), AttributeError),
                (lambda: database.command({}), StopIteration),
                (lambda: database.command([]), StopIteration),
                (lambda: database.command({1: 1}), InvalidDocument),
                (lambda: database.list_collection_names(None, None, None, None), TypeError),
                (lambda: database.list_collection_names(None, session=None), TypeError),
                (lambda: database.list_collection_names(session=object()), AttributeError),
            ]
            for operation, error in operations:
                with self.subTest(error=error), self.assertRaises(error):
                    operation()
            self.assertEqual(database.items.find_one({}), {"_id": 1})
            client.close()
            with self.assertRaises(InvalidOperation):
                database.command("ping")

    def test_skipped_text_and_duplicate_field_index_drop_preserve_remaining_uniqueness(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                items.insert_one({"_id": 1, "email": "one"})
                with self.assertRaises(OperationFailure) as caught:
                    items.create_indexes([], object())
                self.assertEqual(caught.exception.code, 72)
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    self.assertEqual(items.create_indexes([{"key": {"body": "text"}, "name": "body_text"}]), ["body_text"])
                self.assertEqual(len(caught), 1)
                self.assertTrue(issubclass(caught[0].category, IndexCompatibilityWarning))
                self.assertIn("text indexing is skipped", str(caught[0].message))
                self.assertEqual(list(items.list_indexes()), [{"name": "_id_", "key": {"_id": 1}}])
                items.create_index("email", name="email_a")
                items.create_index("email", name="email_b", unique=True)
                items.drop_index("email_a")
                self.assertEqual(set(items.index_information()), {"_id_", "email_b"})
                with self.assertRaises(DuplicateKeyError):
                    items.insert_one({"_id": 2, "email": "one"})
                cursor = items.find({})
                clone = cursor.clone()
                self.assertEqual(clone.to_list(), [{"_id": 1, "email": "one"}])
                self.assertEqual(cursor.to_list(), [{"_id": 1, "email": "one"}])
                cursor.close()
                with self.assertRaises(StopIteration):
                    cursor.next()
                with self.assertRaises(AttributeError):
                    cursor.hasNext()
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(set(reopened.app.items.index_information()), {"_id_", "email_b"})
                self.assertEqual(reopened.app.items.count_documents({}), 1)
                with self.assertRaises(DuplicateKeyError):
                    reopened.app.items.insert_one({"_id": 2, "email": "one"})


class AsyncUpstreamDiscoveryEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_discovery_uses_real_metadata_and_closed_client_errors(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                database = client.app
                await database.items.insert_one({"_id": 1})
                info = await database.command({"buildInfo": 1})
                self.assertEqual(await database.command("buildinfo"), info)
                self.assertEqual(await client.server_info(), info)
                self.assertTrue(info["version"].endswith("-briskdb"))
                self.assertEqual(len(info["versionArray"]), 4)
                self.assertNotIn("tinymongo", info)
                self.assertEqual(await database.command("ping"), {"ok": 1.0})
                self.assertEqual(await database.list_collection_names(authorizedCollections=True, nameOnly=True), ["items"])
                self.assertEqual(await database.list_collection_names(filter={"name": "items"}), ["items"])
                with self.assertRaises(OperationFailure) as caught:
                    await database.command("serverStatus")
                self.assertEqual(caught.exception.code, 59)
                await client.close()
                with self.assertRaises(InvalidOperation):
                    await database.command("ping")


if __name__ == "__main__":
    unittest.main()
