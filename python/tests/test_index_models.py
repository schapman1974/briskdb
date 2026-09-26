"""Public local-client index-model behavior, using the built wheel and real wire."""

import asyncio
import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import tempfile
import time
import unittest
from unittest import mock
import warnings

import pymongo
from bson import Int64
from pymongo.errors import DuplicateKeyError, InvalidOperation, OperationFailure

import briskdb
from briskdb.mongo import IndexCompatibilityWarning


class IndexModelsTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        environment = mock.patch.dict(os.environ, {"BRISKDB_HOME": folder.name})
        environment.start()
        self.addCleanup(environment.stop)

    def test_mixed_models_resolve_real_names_and_reopen(self):
        with tempfile.TemporaryDirectory() as folder:
            for reopen in (False, True):
                with briskdb.MongoClient(folder, shards=2) as client:
                    collection = client.app.items
                    if not reopen:
                        collection.insert_one({"_id": 1, "email": "one", "token": [1, 2], "active": True})
                        collection.create_index("token", name="existing")
                    plain = {"key": [("token", "hashed")], "name": "requested"}
                    original = deepcopy(plain)
                    models = [
                        plain,
                        pymongo.IndexModel("email", unique=True),
                        SimpleNamespace(document={"key": OrderedDict([("email", -1)]), "unique": True}),
                        {"key": {"body": "text"}},
                        {"key": {"_id": "hashed"}, "unique": False},
                    ]
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always")
                        self.assertEqual(collection.create_indexes(model for model in models),
                                         ["existing", "email_1", "email_1", "body_text", "_id_"])
                    self.assertEqual(len(caught), 4)
                    self.assertTrue(all(issubclass(item.category, IndexCompatibilityWarning) for item in caught))
                    self.assertIn("existing", str(caught[0].message))
                    self.assertIn("Reused", str(caught[0].message))
                    self.assertEqual(plain, original)
                    self.assertEqual(set(collection.index_information()), {"_id_", "existing", "email_1"})
                    self.assertEqual(collection.find_one({"token": 2})["email"], "one")
                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": 2, "email": "one"})
                    with self.assertRaises(OperationFailure) as error:
                        collection.create_indexes([{"key": {"token": 1}, "name": "another"}])
                    self.assertEqual(error.exception.code, 85)
                    if reopen:
                        collection.drop_index("existing")
                        self.assertNotIn("existing", collection.index_information())

    def test_earlier_batch_reuse_and_concurrent_whole_batches(self):
        with briskdb.patch(shards=2):
            with pymongo.MongoClient() as client:
                collection = client.app.items
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", IndexCompatibilityWarning)
                    self.assertEqual(collection.create_indexes([
                        {"key": {"value": "hashed"}, "name": "first"},
                        {"key": {"value": -1}, "name": "second"},
                    ]), ["first", "first"])
                    def create(number):
                        # Existing bounded schema admission reports Busy. Retry
                        # that explicit rejection, never arbitrary write errors.
                        deadline = time.monotonic() + 10
                        while True:
                            try:
                                return collection.create_indexes([
                                    {"key": {"other": -1}, "name": f"other_{number}"},
                                    {"key": {"other": "hashed"}, "name": f"tail_{number}"},
                                ])
                            except OperationFailure as error:
                                if error.code != 112 or time.monotonic() >= deadline:
                                    raise
                                time.sleep(0.01)
                    with ThreadPoolExecutor(max_workers=4) as pool:
                        names = list(pool.map(create, range(8)))
                self.assertTrue(all(value == names[0] for value in names))
                self.assertEqual(names[0][0], names[0][1])
                self.assertEqual(len(collection.index_information()), 3)

    def test_membership_unique_and_compound_identity_are_not_weakened(self):
        with briskdb.MongoClient(shards=2) as client:
            collection = client.app.items
            collection.create_indexes([
                {"key": {"value": 1}, "name": "ordinary"},
                {"key": {"value": 1}, "name": "unique", "unique": True},
                {"key": {"value": 1}, "name": "sparse", "sparse": True},
                {"key": {"value": 1}, "name": "partial", "partialFilterExpression": {"active": True}},
                {"key": {"value": 1, "tail": 1}, "name": "compound"},
            ])
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", IndexCompatibilityWarning)
                self.assertEqual(collection.create_indexes([
                    {"key": {"value": -1}, "unique": True},
                    {"key": {"value": -1}, "sparse": True},
                    {"key": {"value": -1}, "partialFilterExpression": {"active": True}},
                    {"key": {"value": -1, "tail": -1}},
                    {"key": {"value": -1}},
                ]), ["unique", "sparse", "partial", "compound", "ordinary"])
                self.assertEqual(collection.create_indexes([
                    {"key": {"tail": -1, "value": -1}, "name": "reversed"},
                    {"key": {"value": -1}, "name": "other_partial", "partialFilterExpression": {"active": False}},
                ]), ["reversed", "other_partial"])
            self.assertEqual(len(collection.index_information()), 8)

    def test_eager_invalid_inputs_have_no_namespace_or_partial_builds(self):
        with briskdb.MongoClient(shards=2) as client:
            collection = client.app.absent
            self.assertEqual(collection.create_indexes(iter([])), [])
            bad_models = [
                object(), {}, {"key": {"x": True}}, {"key": [("x", 1), ("x", -1)]},
                {"key": {"x": "hashed"}, "unique": True},
                {"key": {"x": "text"}, "unique": True},
                {"key": {"x": 1}, "unique": True, "expireAfterSeconds": 1},
                {"key": {"x": 1}, "partialFilterExpression": {"x": {"$bad": 1}}},
                {"key": {"x": 1}, "unexpected": True},
                {"key": {"_id": 1}, "unique": True},
                {"key": {"$bad": 1}}, {"key": {"x": 1}, "name": "x" * 257},
            ]
            for bad in bad_models:
                with self.subTest(bad=bad):
                    with self.assertRaises((TypeError, OperationFailure)):
                        collection.create_indexes([{"key": {"good": 1}}, bad])
                    self.assertNotIn("absent", client.app.list_collection_names())
            with self.assertRaises(OperationFailure):
                collection.create_indexes({"key": {"x": 1}} for _ in range(1001))
            with self.assertRaises(OperationFailure) as oversized:
                collection.create_indexes({"key": {"x": -1}, "name": "n" * 255} for _ in range(1000))
            self.assertEqual(oversized.exception.code, 10334)
            with self.assertRaises(OperationFailure):
                collection.create_indexes([], session=object())
            with self.assertRaises(TypeError):
                collection.create_indexes([], briskdbIndexModelCompatibility=False)
            self.assertNotIn("absent", client.app.list_collection_names())

    def test_extra_command_options_do_not_replace_the_command_verb(self):
        with briskdb.MongoClient(shards=2) as client:
            self.assertEqual(client.app.items.create_indexes(
                [{"key": {"value": 1}}], maxTimeMS=5000), ["value_1"])

    def test_empty_batches_do_not_reopen_closed_clients(self):
        with briskdb.patch(shards=2):
            client = pymongo.MongoClient()
            collection = client.app.items
            client.close()
            with self.assertRaises(InvalidOperation):
                collection.create_indexes([])
        async def run():
            async with briskdb.patch(shards=2):
                client = pymongo.AsyncMongoClient()
                collection = client.app.items
                await client.close()
                with self.assertRaises(InvalidOperation):
                    await collection.create_indexes([])
        asyncio.run(run())

    def test_metadata_protocol_and_factory_options_preserve_real_driver_types(self):
        class IndexSpec:
            def to_metadata(self):
                return {"v": 2, "key": [["value", 1]], "name": "spec", "unique": False,
                        "sparse": False, "partialFilterExpression": None}
        with briskdb.MongoClient("mongodb://ignored/app", shards=2, document_class=OrderedDict) as client:
            databases = [client.app, client["app"], client.get_database("app"),
                         client.get_default_database(), client.app.with_options()]
            for db in databases:
                self.assertIsInstance(db, pymongo.database.Database)
                collections = [db.items, db["items"], db.get_collection("items"), db.items.with_options(), db.items["child"]]
                for collection in collections:
                    self.assertIsInstance(collection, pymongo.collection.Collection)
                    self.assertIs(collection.codec_options.document_class, OrderedDict)
                    self.assertEqual(collection.create_indexes([IndexSpec()]), ["spec"])
            created = client.app.create_collection("explicit")
            self.assertEqual(created.create_indexes([IndexSpec()]), ["spec"])
            created.insert_one({"_id": Int64(3)})
            self.assertIsInstance(created.find_one({}), OrderedDict)
            # Creating a compatibility handle must not mutate stock driver APIs.
            with pymongo.MongoClient(connect=False) as ordinary:
                with self.assertRaises(TypeError):
                    ordinary.app.items.create_indexes(iter([]))

    def test_async_patch_models_factories_and_reopen(self):
        async def run(folder):
            for reopen in (False, True):
                async with briskdb.patch(folder=folder, shards=2):
                    async with pymongo.AsyncMongoClient("mongodb://ignored/app", document_class=OrderedDict) as client:
                        databases = [client.app, client["app"], client.get_database("app"),
                                     client.get_default_database(), client.app.with_options()]
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", IndexCompatibilityWarning)
                            for db in databases:
                                for collection in [db.items, db["items"], db.get_collection("items"), db.items.with_options(), db.items["child"]]:
                                    self.assertEqual(await collection.create_indexes(iter([
                                        {"key": {"value": 1}, "name": "original"},
                                        pymongo.IndexModel([("value", -1)]),
                                    ])), ["original", "original"])
                                    self.assertIs(collection.codec_options.document_class, OrderedDict)
                        if not reopen:
                            created = await client.app.create_collection("explicit")
                            self.assertEqual(await created.create_indexes([{"key": {"email": 1}, "unique": True}]), ["email_1"])
                            await created.insert_one({"_id": 1, "email": "one"})
                        self.assertEqual(await client.app.explicit.count_documents({}), 1)
                        with self.assertRaises(DuplicateKeyError):
                            await client.app.explicit.insert_one({"_id": 2, "email": "one"})
                        self.assertEqual(await client.app.absent.create_indexes([]), [])
        with tempfile.TemporaryDirectory() as folder:
            asyncio.run(run(Path(folder)))


if __name__ == "__main__":
    unittest.main()
