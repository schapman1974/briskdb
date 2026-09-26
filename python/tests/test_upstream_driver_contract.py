"""Public scenarios from the locked PyMongo-shaped application source suites."""

import os
import inspect
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pymongo
from pymongo.errors import ConfigurationError, InvalidOperation, OperationFailure
from pymongo.synchronous.collection import Collection as DriverCollection
from pymongo.synchronous.cursor import Cursor
from pymongo.asynchronous.collection import AsyncCollection as AsyncDriverCollection
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.write_concern import WriteConcern

import briskdb


class UpstreamDriverContractTests(unittest.TestCase):
    def test_import_alias_exposes_sort_constants_and_public_crud(self):
        import briskdb as local_pymongo

        self.assertEqual((local_pymongo.ASCENDING, local_pymongo.DESCENDING), (1, -1))
        with tempfile.TemporaryDirectory() as root, local_pymongo.MongoClient(root, shards=2) as client:
            users = client.app.users
            users.create_index("email")
            result = users.insert_many([{"_id": 1, "email": "ada@example.com", "name": "Ada", "score": 7},
                                        {"_id": 2, "email": "grace@example.com", "name": "Grace", "score": 9}])
            self.assertEqual(result.inserted_ids, [1, 2])
            self.assertEqual(users.find_one({"email": "ada@example.com"})["name"], "Ada")
            self.assertEqual(users.count_documents({"score": {"$gte": 7}}), 2)
            self.assertEqual(users.update_one({"_id": 1}, {"$inc": {"score": 1}}).modified_count, 1)
            self.assertEqual([row["_id"] for row in users.find({"score": {"$gte": 8}}).sort("score", local_pymongo.DESCENDING)], [2, 1])
            self.assertEqual(users.delete_one({"_id": 1}).deleted_count, 1)
            self.assertEqual(users.count_documents({}), 1)

    def test_stock_pymongo_application_runs_inside_native_patch_scope(self):
        original = pymongo.MongoClient
        with tempfile.TemporaryDirectory() as root:
            with briskdb.patch(folder=root, shards=2):
                client = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=50,
                                              connect=False, tinymongo_folder="ignored-by-scope")
                self.assertTrue(client.server_info()["version"].endswith("-briskdb"))
                database = client["contract"]
                users = database["users"]
                users.create_index("email")
                users.insert_many([{"_id": 1, "email": "ada@example.com", "score": 7, "tags": ["math"]},
                                   {"_id": 2, "email": "grace@example.com", "score": 9, "tags": ["code"]}])
                users.update_one({"email": "ada@example.com"}, {"$inc": {"score": 2}})
                users.update_many({}, {"$addToSet": {"tags": "pioneer"}})
                self.assertEqual([row["email"] for row in users.find({"score": {"$gte": 9}}).sort("email", pymongo.ASCENDING)],
                                 ["ada@example.com", "grace@example.com"])
                self.assertEqual(users.count_documents({"tags": {"$all": ["pioneer"]}}), 2)
                self.assertEqual(users.delete_one({"_id": 1}).deleted_count, 1)
                self.assertIn("contract", client.list_database_names())
                self.assertIn("users", database.list_collection_names())
            self.assertIs(pymongo.MongoClient, original)
            with self.assertRaises(InvalidOperation):
                users.find_one({})
            with briskdb.MongoClient(root) as reader:
                self.assertEqual(reader.contract.users.find_one({})["_id"], 2)

    def test_local_folder_and_environment_configuration_do_not_use_tinydb_files(self):
        with tempfile.TemporaryDirectory() as parent:
            for name, options in [
                ("configured", {"host": "mongodb://localhost:27017", "connect": False}),
                ("plain", {}),
                ("network-list", {"host": ["localhost:27017"]}),
                ("network-ip", {"host": "127.0.0.1"}),
                ("network-ipv6", {"host": "[::1]:27017"}),
            ]:
                folder = Path(parent) / name
                kwargs = {"tinymongo_folder": folder, **options} if options else {"host": str(folder)}
                with briskdb.MongoClient(shards=2, **kwargs) as client:
                    self.assertEqual(client.briskdb_path, folder.resolve())
                    self.assertEqual(client.list_database_names(), [])
                    client.app.users.insert_one({"_id": name})
                    self.assertEqual(client.app.users.count_documents({}), 1)
                self.assertTrue((folder / "manifest.sqlite").exists())
                self.assertFalse((folder / "app.json").exists())
            env_folder = Path(parent) / "env-local"
            with mock.patch.dict(os.environ, {"BRISKDB_HOME": str(env_folder)}):
                with briskdb.MongoClient(host="localhost", port=27017, shards=2) as client:
                    self.assertEqual(client.briskdb_path, env_folder.resolve())
                    client.app.users.insert_one({"_id": "host-port"})
                    self.assertEqual(client.list_database_names(), ["app"])

    def test_noop_counts_and_single_multi_replacement_upserts(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            users = client.app.users
            self.assertEqual(users.delete_one({"_id": "missing"}).deleted_count, 0)
            users.insert_one({"_id": 1, "active": True})
            result = users.update_one({"_id": 1}, {"$set": {"active": True}})
            self.assertEqual((result.matched_count, result.modified_count), (1, 0))
            updated = users.update_one({"email": "ada@example.com"}, {"$set": {"active": True}}, upsert=True)
            self.assertEqual((updated.matched_count, updated.modified_count), (0, 0))
            self.assertIsNotNone(updated.upserted_id)
            self.assertIs(users.find_one({"email": "ada@example.com"})["active"], True)
            replaced = users.replace_one({"email": "grace@example.com"}, {"email": "grace@example.com", "active": True}, upsert=True)
            self.assertIsNotNone(replaced.upserted_id)
            many = client.app.many
            result = many.update_many({"profile.email": {"$eq": "ada@example.com"}, "ignored": {"$gt": 1}},
                                     {"$set": {"active": True}}, upsert=True)
            self.assertIsNotNone(result.upserted_id)
            self.assertEqual(many.count_documents({}), 1)
            self.assertEqual(many.find_one({})["profile"]["email"], "ada@example.com")
            self.assertNotIn("ignored", many.find_one({}))

    def test_backend_and_unsupported_calls_keep_native_boundaries(self):
        with tempfile.TemporaryDirectory() as parent:
            for backend in ["tinydb", "memory", "duckdb", "parquet"]:
                folder = Path(parent) / backend
                with self.assertRaises(ValueError):
                    briskdb.MongoClient(folder=folder, backend=backend)
                self.assertFalse(folder.exists())
            with briskdb.MongoClient(Path(parent) / "native", shards=2) as client:
                items = client.app.items
                for handle in [client, client.app, items]:
                    with self.assertRaises(OperationFailure):
                        handle.watch([])
                with self.assertRaises(OperationFailure):
                    client.app.command("serverStatus")
                with self.assertRaises(OperationFailure) as caught:
                    list(items.aggregate([{"$lookup": {}}]))
                self.assertEqual(caught.exception.code, 115)
                self.assertEqual(items.create_index([("email", -1)]), "email_-1")
                self.assertEqual(items.bulk_write([pymongo.InsertOne({"_id": 1})]).inserted_count, 1)
                with self.assertRaises(InvalidOperation):
                    items.bulk_write([])
                with self.assertRaises(OperationFailure):
                    items.with_options(write_concern=WriteConcern(w=2)).insert_one({"_id": 2})
                self.assertIsNone(items.find_one({"_id": 2}))
                self.assertFalse(callable(client.capabilities))
                self.assertFalse(callable(client.supports))

    def test_fake_session_options_reject_without_native_mutations(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            operations = [
                lambda: items.insert_one({"_id": 1}, session=object()),
                lambda: list(items.find({}, session=object())),
                lambda: items.find_one({}, session=object()),
                lambda: items.count_documents({}, session=object()),
                lambda: items.update_one({}, {"$set": {"x": 1}}, session=object()),
                lambda: items.update_many({}, {"$set": {"x": 1}}, session=object()),
                lambda: items.replace_one({}, {}, session=object()),
                lambda: items.find_one_and_update({}, {"$set": {"x": 1}}, session=object()),
                lambda: items.find_one_and_replace({}, {}, session=object()),
                lambda: items.delete_one({}, session=object()),
                lambda: items.delete_many({}, session=object()),
                lambda: items.drop(session=object()),
            ]
            for index, operation in enumerate(operations):
                # Ordinary driver write paths inspect transaction attributes;
                # count/drop and the guarded find paths reject with ValueError.
                error = ValueError if index in (1, 2, 3, 11) else AttributeError
                with self.subTest(operation=index), self.assertRaises(error):
                    operation()
                self.assertEqual(client.app.list_collection_names(), [])
            with self.assertRaises(TypeError):
                items.insert_many([], session=object())
            self.assertEqual(client.app.list_collection_names(), [])

    def test_invalid_find_sessions_never_reach_driver_cursor_construction(self):
        parameters = list(inspect.signature(Cursor).parameters.values())[1:]
        self.assertEqual(parameters[20].name, "session")
        positional = [parameter.default for parameter in parameters[:20]]
        positional[0] = {}
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            with mock.patch.object(DriverCollection, "find", side_effect=AssertionError("driver constructed cursor")):
                for invalid in [object(), {}, False, 0, "session"]:
                    with self.assertRaises(ValueError):
                        items.find({}, session=invalid)
                    with self.assertRaises(ValueError):
                        items.find(*positional, invalid)
                    with self.assertRaises(ValueError):
                        items.find_one({}, session=invalid)
                with self.assertRaises(TypeError):
                    items.find(*positional, None, session=None)
            self.assertEqual(list(items.find({}, session=None)), [])
            with client.start_session() as session:
                with items.find({}, session=session) as cursor:
                    self.assertIs(cursor.session, session)
                    with self.assertRaises(ConfigurationError):
                        list(cursor)
            self.assertEqual(client.app.list_collection_names(), [])


class AsyncFindSessionValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_async_find_sessions_never_construct_driver_cursors(self):
        parameters = list(inspect.signature(AsyncCursor).parameters.values())[1:]
        self.assertEqual(parameters[20].name, "session")
        positional = [parameter.default for parameter in parameters[:20]]
        positional[0] = {}
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                with mock.patch.object(AsyncDriverCollection, "find", side_effect=AssertionError("driver constructed cursor")):
                    for invalid in [object(), {}, False, 0, "session"]:
                        with self.assertRaises(ValueError):
                            items.find({}, session=invalid)
                        with self.assertRaises(ValueError):
                            items.find(*positional, invalid)
                        with self.assertRaises(ValueError):
                            await items.find_one({}, session=invalid)
                    with self.assertRaises(TypeError):
                        items.find(*positional, None, session=None)
                self.assertEqual(await items.find({}, session=None).to_list(), [])
                async with client.start_session() as session:
                    async with items.find({}, session=session) as cursor:
                        self.assertIs(cursor.session, session)
                        with self.assertRaises(ConfigurationError):
                            await cursor.to_list()
                self.assertEqual(await client.app.list_collection_names(), [])


if __name__ == "__main__":
    unittest.main()
