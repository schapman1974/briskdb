"""Public query/error/lifecycle edges without TinyMongo private helpers."""

import tempfile
import unittest

from pymongo import ReturnDocument
from pymongo.errors import InvalidOperation, OperationFailure, WriteError

import briskdb


class UpstreamCoverageEdgeTests(unittest.TestCase):
    def test_three_row_query_edges_return_exact_ids_and_counts(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            items.insert_many([
                {"_id": 1, "count": 1, "name": "alpha", "tags": ["a"], "meta": {"active": True}},
                {"_id": 2, "count": 5, "name": "beta", "tags": ["b"], "meta": {"active": False}},
                {"_id": 3, "count": 10, "name": "gamma", "tags": ["a", "c"]},
            ])
            for query, expected in [
                ({"count": {"$gt": 1, "$lt": 10}}, [2]),
                ({"count": {"$gte": 5, "$lte": 10}}, [2, 3]),
                ({"name": {"$ne": "alpha"}}, [2, 3]),
                ({"name": {"$regex": "^a"}}, [1]),
                ({"name": {"$not": {"$eq": "alpha"}}}, [2, 3]),
                ({"count": {"$not": {"$gt": 5}}}, [1, 2]),
                ({"tags": {"$in": ["c", "missing"]}}, [3]),
                ({"$and": [{"tags": {"$in": ["a"]}}, {"count": {"$lt": 5}}]}, [1]),
                ({"$or": [{"name": "alpha"}, {"name": "gamma"}]}, [1, 3]),
                ({"missing": {"$exists": False}}, [1, 2, 3]),
                ({"tags": [["a"]]}, []),
            ]:
                with self.subTest(query=query):
                    self.assertEqual([row["_id"] for row in items.find(query).sort("_id")], expected)
                    self.assertEqual(items.count_documents(query), len(expected))

    def test_invalid_queries_and_no_match_writes_do_not_create_collections(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            for query in ({"value": {"$exsits": True}}, {"tags": {"$unknown": ["a"]}}):
                # Real PyMongo evaluates find lazily; rejection occurs on use.
                cursor = items.find(query)
                with self.assertRaises(OperationFailure) as caught:
                    list(cursor)
                self.assertEqual(caught.exception.code, 115)
                self.assertEqual(client.app.list_collection_names(), [])
            self.assertEqual(items.count_documents({}), 0)
            self.assertEqual(items.update_one({"_id": "missing"}, {"$set": {"x": 1}}).matched_count, 0)
            self.assertEqual(items.update_many({"_id": "missing"}, {"$set": {"x": 1}}).matched_count, 0)
            self.assertEqual(items.replace_one({"_id": "missing"}, {"x": 1}).matched_count, 0)
            self.assertIsNone(items.find_one_and_update({"_id": "missing"}, {"$set": {"x": 1}}))
            self.assertIsNone(items.find_one_and_replace({"_id": "missing"}, {"x": 1}))
            self.assertEqual(client.app.list_collection_names(), [])

    def test_update_errors_preserve_rows_and_find_modify_returns_exact_images(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                row = {"_id": 1, "count": "one", "active": True}
                items.insert_one(dict(row))
                for method in (items.update_one, items.update_many):
                    with self.assertRaises(WriteError) as caught:
                        method({"_id": 1}, {"$inc": {"count": 1}})
                    self.assertEqual(caught.exception.code, 14)
                    self.assertEqual(items.find_one({}), row)
                for update in ({}, {"value": 3}):
                    with self.assertRaises(ValueError):
                        items.update_one({"_id": 1}, update)
                    self.assertEqual(items.find_one({}), row)
                with self.assertRaises(WriteError) as caught:
                    items.update_one({"_id": 1}, {"$set": "not-a-dict"})
                self.assertEqual(caught.exception.code, 9)
                with self.assertRaises(WriteError) as caught:
                    items.update_one({"_id": 1}, {"$set": {"_id": 2}})
                self.assertEqual(caught.exception.code, 66)
                self.assertEqual(items.find_one({}), row)
                items.insert_one({"_id": 2, "count": 5, "active": False})
                changed = items.update_many({}, {"$set": {"active": True}})
                self.assertEqual((changed.matched_count, changed.modified_count), (2, 1))
                same = items.replace_one({"_id": 1}, {"count": "one", "active": True})
                self.assertEqual((same.matched_count, same.modified_count), (1, 0))
                self.assertEqual(items.find_one_and_replace({"_id": 1}, {"value": 4}), row)
                self.assertEqual(items.find_one_and_replace({"_id": 1}, {"value": 5},
                    return_document=ReturnDocument.AFTER), {"_id": 1, "value": 5})
                self.assertIsNone(items.find_one_and_replace({"_id": 3}, {"value": 3}, upsert=True))
                self.assertEqual(items.find_one_and_update({"_id": 4}, {"$set": {"value": 4}},
                    upsert=True, return_document=ReturnDocument.AFTER), {"_id": 4, "value": 4})
                expected = list(items.find({}).sort("_id"))
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(list(reopened.app.items.find({}).sort("_id")), expected)

    def test_closed_metadata_and_listing_do_not_revive_client(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                database = client.app
                database.items.insert_one({"_id": "saved"})
            client.close()
            for operation in (client.server_info, client.list_database_names, client.list_databases,
                              database.list_collection_names, lambda: client.drop_database("app"),
                              lambda: database.items.find_one({})):
                with self.subTest(operation=operation), self.assertRaises(InvalidOperation):
                    operation()
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.find_one({}), {"_id": "saved"})


class AsyncUpstreamCoverageEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_closed_metadata_and_listing_do_not_revive_client(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                database = client.app
                await database.items.insert_one({"_id": "saved"})
            await client.close()
            for operation in (client.server_info, client.list_database_names, client.list_databases,
                              database.list_collection_names, lambda: client.drop_database("app"),
                              lambda: database.items.find_one({})):
                with self.subTest(operation=operation), self.assertRaises(InvalidOperation):
                    await operation()
            async with briskdb.AsyncMongoClient(root) as reopened:
                self.assertEqual(await reopened.app.items.find_one({}), {"_id": "saved"})


if __name__ == "__main__":
    unittest.main()
