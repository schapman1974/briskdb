"""Public behaviors from TinyMongo's locked durable/advanced index suites.

Source: 53cbf44e98b8caa036163725d195fd29592e1cc0,
tests/test_durable_indexes.py::test_unique_insert_operations_follow_single_and_bulk_semantics
and the public collection assertions in tests/test_exact_id_semantics.py.
These execute the installed BriskDB wheel, not TinyMongo or a mocked collection.
Original file SHA-256 digests:
durable: 1764fc987b4c99034957ee60100cc4f6c54eb217725178528316f218e742550d
exact-ID: 492ea7368478cb0ecae4cc2ed0d6108e7a4cf4377bc95c4ed0149ab8973e58b8
Private Python matcher-helper assertions are covered by the separate shared
matcher oracles; this file exercises public operations over the real wire.
"""

import asyncio
from collections import UserDict
import tempfile
import unittest

import pymongo
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure

import briskdb


class UpstreamIndexTests(unittest.TestCase):
    def test_validation_bypass_never_bypasses_id_or_secondary_uniqueness(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder, shards=2) as client:
                users = client.app.users
                users.create_index("email", unique=True)
                users.insert_one({"_id": 1, "email": "ada@example.com"})
                for document in [
                    {"_id": 2, "email": "ada@example.com"},
                    {"_id": 1, "email": "replacement@example.com"},
                ]:
                    with self.assertRaises(DuplicateKeyError):
                        users.insert_one(document, bypass_document_validation=True)
                for documents in [
                    [{"_id": 4, "email": "grace@example.com"}, {"_id": 5, "email": "ada@example.com"}],
                    [{"_id": 6, "email": "hopper@example.com"}, {"_id": 7, "email": "hopper@example.com"}],
                ]:
                    with self.assertRaises(BulkWriteError) as error:
                        users.insert_many(documents, bypass_document_validation=True)
                    self.assertEqual(error.exception.details["nInserted"], 1)
                    self.assertEqual(error.exception.details["writeErrors"][0]["code"], 11000)
                expected = [
                    {"_id": 1, "email": "ada@example.com"},
                    {"_id": 4, "email": "grace@example.com"},
                    {"_id": 6, "email": "hopper@example.com"},
                ]
                self.assertEqual(list(users.find({})), expected)
            with briskdb.MongoClient(folder) as client:
                self.assertEqual(list(client.app.users.find({})), expected)

    def test_validation_bypass_preserves_update_upsert_and_return_image_rules(self):
        with briskdb.patch(shards=2):
            with pymongo.MongoClient() as client:
                users = client.app.users
                users.create_index("email", unique=True)
                users.insert_many([{"_id": 1, "email": "one"}, {"_id": 2, "email": "two"}])
                for method, update in [
                    (users.update_one, {"$set": {"email": "one"}}),
                    (users.update_many, {"$set": {"email": "one"}}),
                    (users.replace_one, {"email": "one"}),
                ]:
                    for upsert in (False, True):
                        with self.assertRaises(DuplicateKeyError):
                            method({"_id": 3 if upsert else 2}, update,
                                   upsert=upsert, bypass_document_validation=True)
                for method, update in [
                    (users.find_one_and_update, {"$set": {"email": "one"}}),
                    (users.find_one_and_replace, {"email": "one"}),
                ]:
                    with self.assertRaises(DuplicateKeyError):
                        method({"_id": 2}, update, bypassDocumentValidation=True)
                with self.assertRaises(OperationFailure) as changed_id:
                    users.update_one({"_id": 2}, {"$set": {"_id": 20}}, bypass_document_validation=True)
                self.assertEqual(changed_id.exception.code, 66)
                self.assertEqual(list(users.find({})), [{"_id": 1, "email": "one"}, {"_id": 2, "email": "two"}])
                self.assertEqual(users.find_one_and_update(
                    {"_id": 2}, {"$set": {"email": "changed"}},
                    return_document=pymongo.ReturnDocument.AFTER, bypassDocumentValidation=True),
                    {"_id": 2, "email": "changed"})
                result = users.update_one({"_id": 3}, {"$set": {"email": "three"}},
                                          upsert=True, bypass_document_validation=True)
                self.assertEqual(result.upserted_id, 3)

    def test_async_validation_bypass_keeps_constraints_and_successful_results(self):
        async def run():
            async with briskdb.patch(shards=2):
                async with pymongo.AsyncMongoClient() as client:
                    users = client.app.users
                    await users.create_index("email", unique=True)
                    await users.insert_one({"_id": 1, "email": "one"}, bypass_document_validation=True)
                    with self.assertRaises(DuplicateKeyError):
                        await users.insert_one({"_id": 2, "email": "one"}, bypass_document_validation=True)
                    with self.assertRaises(DuplicateKeyError):
                        await users.update_one({"_id": 2}, {"$set": {"email": "one"}},
                                               upsert=True, bypass_document_validation=True)
                    self.assertEqual(await users.find_one_and_replace(
                        {"_id": 1}, {"email": "changed"}, return_document=pymongo.ReturnDocument.AFTER,
                        bypassDocumentValidation=True), {"_id": 1, "email": "changed"})
        asyncio.run(run())


class UpstreamExactIdTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.db.items

    def test_embedded_document_equality_preserves_field_order(self):
        self.items.insert_one({"_id": "ordered", "value": {"first": 1, "second": 2}})
        self.assertIsNotNone(self.items.find_one({"value": {"first": 1.0, "second": 2.0}}))
        self.assertIsNone(self.items.find_one({"value": {"second": 2, "first": 1}}))

    def test_reordered_embedded_document_ids_remain_distinct(self):
        first, reordered = {"first": 1, "second": 2}, {"second": 2, "first": 1}
        result = self.items.insert_many([{"_id": first, "label": "first"},
                                         {"_id": reordered, "label": "reordered"}])
        self.assertEqual(result.inserted_ids, [first, reordered])
        self.assertEqual(self.items.find_one({"_id": first})["label"], "first")
        self.assertEqual(self.items.find_one({"_id": reordered})["label"], "reordered")

    def test_equivalent_embedded_document_ids_are_duplicates(self):
        with self.assertRaises(BulkWriteError) as error:
            self.items.insert_many([{"_id": {"number": 1}, "label": "integer"},
                                     {"_id": {"number": 1.0}, "label": "float"}])
        self.assertEqual(error.exception.details["nInserted"], 1)
        self.assertEqual(error.exception.details["writeErrors"][0]["code"], 11000)

    def test_exact_id_mutations_do_not_match_array_members(self):
        array_id = [1, 2]
        self.items.insert_many([{"_id": array_id, "label": "array"}, {"_id": 1, "label": "scalar"}])
        self.assertEqual(self.items.update_one({"_id": 1}, {"$set": {"updated": True}}).matched_count, 1)
        self.assertTrue(self.items.find_one({"_id": 1})["updated"])
        self.assertNotIn("updated", self.items.find_one({"_id": array_id}))
        self.assertEqual(self.items.replace_one({"_id": 1}, {"label": "replacement"}).matched_count, 1)
        self.assertEqual(self.items.find_one({"_id": 1})["label"], "replacement")
        self.assertEqual(self.items.find_one({"_id": array_id})["label"], "array")
        self.assertEqual(self.items.delete_one({"_id": 1}).deleted_count, 1)
        self.assertIsNone(self.items.find_one({"_id": 1}))
        self.assertIsNotNone(self.items.find_one({"_id": array_id}))

    def test_compound_and_logical_id_filters_keep_exact_identity(self):
        array_id = [1, 2]
        self.items.insert_many([{"_id": array_id, "score": 1}, {"_id": 1, "score": 0}])
        compound = {"_id": 1, "score": {"$gt": 0}}
        logical = {"$and": [{"_id": 1}, {"score": {"$gt": 0}}]}
        self.assertIsNone(self.items.find_one(compound))
        self.assertIsNone(self.items.find_one(logical))
        self.assertEqual(self.items.update_one(compound, {"$set": {"wrong": True}}).matched_count, 0)
        exact = {"$and": [{"_id": 1}, {"score": 0}]}
        self.assertEqual(self.items.update_one(exact, {"$set": {"updated": True}}).matched_count, 1)
        self.assertTrue(self.items.find_one({"_id": 1})["updated"])
        self.assertNotIn("updated", self.items.find_one({"_id": array_id}))
        self.assertEqual(self.items.delete_one({"_id": 1, "updated": True}).deleted_count, 1)
        self.assertIsNone(self.items.find_one({"_id": 1}))
        self.assertIsNotNone(self.items.find_one({"_id": array_id}))

    def test_mapping_subclasses_share_document_id_identity(self):
        identifier = {"z": 1, "a": 2}
        equivalent = UserDict([("z", 1.0), ("a", 2.0)])
        self.items.insert_one({"_id": identifier, "label": "original"})
        self.assertEqual(self.items.find_one({"_id": equivalent})["label"], "original")
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": equivalent, "label": "duplicate"})
        self.assertEqual(self.items.count_documents({}), 1)

    def test_exact_id_operator_queries_and_parser_write_paths(self):
        self.items.insert_many([{"_id": 1, "score": 1}, {"_id": 2, "score": 2}])
        self.assertEqual(self.items.find_one({"_id": {"$eq": 1}})["_id"], 1)
        self.assertEqual(self.items.find_one({"_id": {"$in": [2]}})["_id"], 2)
        self.assertEqual(self.items.update_one({"score": {"$gt": 1}}, {"$set": {"updated": True}}).matched_count, 1)
        self.assertTrue(self.items.find_one({"_id": 2})["updated"])
        self.assertEqual(self.items.replace_one({"score": {"$lt": 2}}, {"score": 10}).matched_count, 1)
        self.assertEqual(self.items.find_one({"_id": 1})["score"], 10)


if __name__ == "__main__":
    unittest.main()
