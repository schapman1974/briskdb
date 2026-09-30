"""Real-driver regression coverage for retained sort positions (#563)."""

import tempfile
import unittest

from briskdb import mongo


class RetainedSortWindowTests(unittest.TestCase):
    def test_continuations_recheck_rows_without_retaining_stale_documents(self):
        with tempfile.TemporaryDirectory() as folder:
            with mongo.MongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                items.insert_many([{"_id": i, "rank": i, "visible": True, "body": "old"}
                                   for i in range(12)])
                cursor = items.find({"visible": True}).sort("rank").batch_size(2)
                self.assertEqual([next(cursor)["_id"], next(cursor)["_id"]], [0, 1])
                items.delete_one({"_id": 2})
                items.update_one({"_id": 3}, {"$set": {"visible": False}})
                items.update_one({"_id": 4}, {"$set": {"rank": -1}})
                items.update_one({"_id": 5}, {"$set": {"body": "new"}})
                rows = list(cursor)
                self.assertEqual([row["_id"] for row in rows], list(range(5, 12)))
                self.assertEqual(rows[0]["body"], "new")
                # This cursor retains positions, not a snapshot or a live view:
                # moved keys are omitted here; a new find sees the new order.
                self.assertEqual(items.find_one({"visible": True}, sort=[("rank", 1)])["_id"], 4)

    def test_byte_trimmed_sort_windows_preserve_global_order_and_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            with mongo.MongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                # Owned keys exceed the 16-MiB window budget. Projection makes
                # each returned page tiny; the key budget must still apply.
                for i in range(7):
                    items.insert_one({"_id": i, "rank": str(i) + "x" * (3 * 1024 * 1024)})
                for direction in (1, -1):
                    expected = list(range(7))[::direction]
                    rows = list(items.find({}, {"_id": 1}).sort("rank", direction).batch_size(2))
                    self.assertEqual([row["_id"] for row in rows], expected)
                    rows = list(items.find({}, {"_id": 1}).sort("rank", direction)
                                .skip(4).limit(2).batch_size(1))
                    self.assertEqual([row["_id"] for row in rows], expected[4:6])


class AsyncRetainedSortWindowTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_ties_compound_keys_projection_and_small_batches(self):
        with tempfile.TemporaryDirectory() as folder:
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as client:
                documents = [{"_id": i, "a": i % 7, "b": i % 3, "body": "x" * 1024}
                             for i in range(301)]
                await client.app.items.insert_many(documents)
                expected = sorted(documents, key=lambda row: (row["a"], -row["b"]))[17:267]
                for indexed in (False, True):
                    if indexed:
                        await client.app.items.create_index([("a", 1), ("b", -1)])
                    cursor = client.app.items.find({}, {"_id": 1}).sort([("a", 1), ("b", -1)])
                    rows = await cursor.skip(17).limit(250).batch_size(7).to_list(length=None)
                    self.assertEqual(rows, [{"_id": row["_id"]} for row in expected])


if __name__ == "__main__":
    unittest.main()
