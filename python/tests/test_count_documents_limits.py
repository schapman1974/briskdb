"""Scalar counts must not consume general aggregation input/work quotas."""

import tempfile
import unittest

import pymongo
from bson import Int64
from pymongo.errors import OperationFailure

import briskdb


class CountDocumentsLimitTests(unittest.TestCase):
    def test_native_count_diagnostics_and_filtered_index_candidates(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.open(folder, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection("count_limits", "diagnostics")
                    for i in range(20):
                        session.insert_one("count_limits", "diagnostics", {"_id": i, "n": i % 2})
                    def execute(query):
                        return session.aggregate("count_limits", "diagnostics", [
                            {"$match": query}, {"$group": {"_id": 1, "n": {"$sum": 1}}}],
                            plan_diagnostics=True, execution_stats=True)
                    result = execute({})
                    self.assertEqual(result["documents"], [{"_id": 1, "n": 20}])
                    self.assertEqual(result["plan"]["read_access"], {"kind": "scan", "reason": "count_rows"})
                    self.assertEqual(result["read_stats"]["storage_reads"], 4)
                    self.assertEqual(result["read_stats"]["documents_examined"], 0)
                    self.assertEqual(result["read_stats"]["source_matches"], 0)
                    session.create_built_index("count_limits", "diagnostics", {"n": 1})
                    filtered = execute({"n": 1})
                    self.assertEqual(filtered["documents"], [{"_id": 1, "n": 10}])
                    self.assertEqual(filtered["plan"]["read_access"]["kind"], "index_candidates")
                    self.assertEqual(filtered["read_stats"]["documents_examined"], 10)
                    self.assertEqual(filtered["read_stats"]["matcher_evaluations"], 10)
                    self.assertEqual(filtered["read_stats"]["source_matches"], 10)

    def test_many_small_documents_count_without_the_aggregation_row_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=4) as client:
                collection = client.count_limits.small
                for first in range(0, 75000, 1000):
                    collection.insert_many([
                        {"_id": i, "n": i, "email": f"user{i}@example.com"}
                        for i in range(first, first + 1000)
                    ])
                self.assertEqual(collection.count_documents({}), 75000)
                self.assertEqual(collection.count_documents({"n": {"$gte": 0}}), 75000)
                self.assertEqual(collection.count_documents({}, skip=70000), 5000)
                self.assertEqual(collection.count_documents({}, skip=70000, limit=3000), 3000)
                self.assertEqual(list(collection.aggregate([{"$count": "n"}])), [{"n": 75000}])
                self.assertEqual(collection.estimated_document_count(), 75000)
            with briskdb.MongoClient(folder=folder) as reopened:
                self.assertEqual(reopened.count_limits.small.count_documents({}), 75000)

    def test_nested_payloads_do_not_charge_scalar_count_work(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=4) as client:
                collection = client.count_limits.nested
                items = [{"sec": j, "text": "lorem ipsum dolor sit"} for j in range(1500)]
                # Keep each wire write within the existing decoded bulk budget.
                for i in range(600):
                    collection.insert_one({"_id": i, "n": i, "items": items})
                self.assertEqual(collection.count_documents({}), 600)
                self.assertEqual(collection.count_documents({"n": {"$lt": 300}}), 300)
                self.assertEqual(collection.count_documents({"n": {"$lt": 300}}, skip=10), 290)
                self.assertEqual(collection.count_documents({"n": {"$lt": 300}}, skip=10, limit=7), 7)
                self.assertEqual(list(collection.aggregate([{"$count": "n"}])), [{"n": 600}])
                # General transforms/grouping keep their existing work bound.
                with self.assertRaises(OperationFailure) as error:
                    list(collection.aggregate([
                        {"$set": {"copy": "$items"}},
                        {"$group": {"_id": 1, "n": {"$sum": 1}}},
                    ]))
                self.assertEqual(error.exception.code, 10334)
                self.assertEqual(collection.count_documents({}), 600)

    def test_scalar_shapes_preserve_empty_results_types_and_window_order(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                collection = client.count_limits.windows
                self.assertEqual(collection.count_documents({}), 0)
                self.assertEqual(list(collection.aggregate([{"$count": "n"}])), [])
                collection.insert_many([{"_id": i, "n": i % 2} for i in range(12)])
                for query, size in [({}, 12), ({"n": 1}, 6), ({"n": 3}, 0), ({"_id": 3}, 1)]:
                    for windows, expected in [([], size), ([{"$skip": 2}], max(size - 2, 0)),
                            ([{"$limit": 2}, {"$skip": 3}], 0),
                            ([{"$skip": 2}, {"$limit": 4}, {"$skip": 1}], max(min(size - 2, 4) - 1, 0))]:
                        for terminal in [{"$count": "n"}, {"$group": {"_id": 1, "n": {"$sum": Int64(1)}}}]:
                            with self.subTest(query=query, windows=windows, terminal=terminal):
                                rows = list(collection.aggregate([{"$match": query}, *windows, terminal]))
                                expected_rows = [] if expected == 0 else ([{"n": expected}] if "$count" in terminal else [{"_id": 1, "n": expected}])
                                self.assertEqual(rows, expected_rows)
                                if rows:
                                    self.assertIs(type(rows[0]["n"]), int)
                for stages in [[{"$limit": 0}, {"$count": "n"}],
                               [{"$match": {"_id": -1}}, {"$count": "n"}, {"$unsupported": 1}]]:
                    with self.assertRaises(OperationFailure):
                        list(collection.aggregate(stages))

    def test_zero_batch_count_cursor_and_drop_recreate_fence(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                db = client.count_limits
                db.pages.insert_many([{"_id": i} for i in range(12)])
                def begin():
                    result = db.command({"aggregate": "pages", "pipeline": [
                        {"$match": {}}, {"$group": {"_id": 1, "n": {"$sum": 1}}}],
                        "cursor": {"batchSize": 0}})["cursor"]
                    self.assertEqual(result["firstBatch"], [])
                    self.assertNotEqual(result["id"], 0)
                    return result["id"]
                cursor_id = begin()
                page = db.command({"getMore": cursor_id, "collection": "pages", "batchSize": 1})["cursor"]
                self.assertEqual(page["nextBatch"], [{"_id": 1, "n": 12}])
                self.assertEqual(page["id"], 0)
                cursor_id = begin()
                db.pages.drop()
                db.pages.insert_one({"_id": 100})
                with self.assertRaises(OperationFailure) as error:
                    db.command({"getMore": cursor_id, "collection": "pages"})
                self.assertEqual(error.exception.code, 43)
                self.assertEqual(db.pages.count_documents({}), 1)


class AsyncCountDocumentsLimitTests(unittest.IsolatedAsyncioTestCase):
    async def test_patched_async_driver_uses_the_shared_scalar_path(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.patch(folder, shards=2):
                async with pymongo.AsyncMongoClient() as client:
                    collection = client.count_limits.asynchronous
                    await collection.insert_many([{"_id": i, "n": i % 2} for i in range(30)])
                    self.assertEqual(await collection.count_documents({}), 30)
                    self.assertEqual(await collection.count_documents({"n": 1}, skip=2, limit=4), 4)
                    cursor = await collection.aggregate([{"$count": "n"}])
                    self.assertEqual(await cursor.to_list(), [{"n": 30}])


if __name__ == "__main__":
    unittest.main()
