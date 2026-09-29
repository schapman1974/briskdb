"""Real PyMongo regressions for requested versus internal batch sizes (#549)."""
import tempfile
import unittest

from bson import Int64
from pymongo.errors import OperationFailure

from briskdb import mongo


class CursorBatchCapTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = mongo.MongoClient(folder=root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items
        self.documents = [{"_id": i, "n": i} for i in range(1205)]
        self.items.insert_many(self.documents)

    def test_large_limits_and_batch_requests_preserve_complete_results(self):
        for limit in (1101, 1102, 1200, 1_000_000):
            for batch in (0, 1000, 1001, 1_000_000):
                with self.subTest(limit=limit, batch=batch):
                    rows = list(self.items.find().sort("_id").limit(limit).batch_size(batch))
                    self.assertEqual(rows, self.documents[:limit])

    def test_reported_300_document_feed_and_async_sized_getmore(self):
        expected = self.documents[:300]
        self.assertEqual(list(self.items.find({"n": {"$lt": 300}}).limit(1_000_000).sort("_id")), expected)

    def test_raw_commands_cap_each_page_and_keep_the_cursor(self):
        reply = self.client.app.command("find", "items", batchSize=Int64(2**63 - 1),
                                        sort={"_id": 1})
        cursor = reply["cursor"]
        self.assertEqual(cursor["firstBatch"], self.documents[:1000])
        self.assertNotEqual(cursor["id"], 0)
        reply = self.client.app.command("getMore", cursor["id"], collection="items",
                                        batchSize=Int64(2**63 - 1))
        self.assertEqual(reply["cursor"]["nextBatch"], self.documents[1000:])
        self.assertEqual(reply["cursor"]["id"], 0)

        # Byte limits may end a page before the document cap. A large request
        # must retain the rest of the cursor, not widen that wire budget.
        large = [{"_id": i, "payload": "x" * 60_000} for i in range(30)]
        self.client.app.large.insert_many(large)
        page = self.client.app.command("find", "large", batchSize=1_000_000,
                                       sort={"_id": 1})["cursor"]
        rows = page["firstBatch"]
        self.assertTrue(0 < len(rows) < len(large))
        while page["id"]:
            page = self.client.app.command("getMore", page["id"], collection="large",
                                           batchSize=1_000_000)["cursor"]
            rows.extend(page["nextBatch"])
        self.assertEqual(rows, large)

    def test_single_batch_and_empty_initial_batch_keep_their_meaning(self):
        reply = self.client.app.command("find", "items", batchSize=1_000_000,
                                        singleBatch=True, sort={"_id": 1})
        self.assertEqual(reply["cursor"]["firstBatch"], self.documents[:1000])
        self.assertEqual(reply["cursor"]["id"], 0)
        reply = self.client.app.command("find", "items", batchSize=0, sort={"_id": 1})
        cursor = reply["cursor"]
        self.assertEqual(cursor["firstBatch"], [])
        self.assertNotEqual(cursor["id"], 0)
        reply = self.client.app.command("getMore", cursor["id"], collection="items", batchSize=1001)
        self.assertEqual(reply["cursor"]["nextBatch"], self.documents[:1000])
        self.assertNotEqual(reply["cursor"]["id"], 0)
        self.client.app.command("killCursors", "items", cursors=[reply["cursor"]["id"]])

    def test_aggregation_and_metadata_accept_large_requests(self):
        self.assertEqual(list(self.items.aggregate([{"$sort": {"_id": 1}}], batchSize=1_000_000)),
                         self.documents)
        collections = list(self.client.app.list_collections(cursor={"batchSize": 1_000_000}))
        self.assertEqual([entry["name"] for entry in collections], ["items"])
        reply = self.client.app.command("listIndexes", "items", cursor={"batchSize": 1_000_000})
        self.assertEqual([entry["name"] for entry in reply["cursor"]["firstBatch"]], ["_id_"])

    def test_invalid_requests_are_not_silently_coerced(self):
        for size in (-1, 1.5, True, "1001"):
            with self.subTest(size=size), self.assertRaises(OperationFailure):
                self.client.app.command("find", "items", batchSize=size)
        reply = self.client.app.command("find", "items", batchSize=0)
        for size in (-1, 0, 1.5, True, "1001"):
            with self.subTest(size=size), self.assertRaises(OperationFailure):
                self.client.app.command("getMore", reply["cursor"]["id"], collection="items", batchSize=size)
        self.client.app.command("killCursors", "items", cursors=[reply["cursor"]["id"]])


class AsyncCursorBatchCapTests(unittest.IsolatedAsyncioTestCase):
    async def test_feed_and_multi_page_limits_with_real_async_driver(self):
        with tempfile.TemporaryDirectory() as folder:
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as client:
                documents = [{"_id": i} for i in range(1205)]
                await client.app.items.insert_many(documents)
                for limit in (1101, 1102, 1200, 1_000_000):
                    for batch in (0, 1001, 1_000_000):
                        with self.subTest(limit=limit, batch=batch):
                            cursor = client.app.items.find().sort("_id").limit(limit).batch_size(batch)
                            self.assertEqual(await cursor.to_list(length=None), documents[:limit])
                feed = client.app.items.find({"_id": {"$lt": 300}}).sort("_id").limit(1_000_000)
                self.assertEqual(await feed.to_list(length=None), documents[:300])
                aggregate = await client.app.items.aggregate([{"$sort": {"_id": 1}}], batchSize=1_000_000)
                self.assertEqual(await aggregate.to_list(length=None), documents)


if __name__ == "__main__":
    unittest.main()
