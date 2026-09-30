"""Real-driver correctness across selective and broad secondary-key frontiers."""

import re
import tempfile
import unittest

import briskdb


class SelectiveIndexReadTests(unittest.TestCase):
    def test_selective_membership_matches_scan_for_pages_counts_mutations_and_index_churn(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                scan, indexed = client.app.scan, client.app.indexed
                documents = [{"_id": i, "group": i % 64, "body": "x" * 4096} for i in range(160)]
                documents += [{"_id": 500, "group": [1, 2, 2]}, {"_id": 501, "group": None},
                              {"_id": 502}, {"_id": 503, "group": {"nested": 1}},
                              {"_id": 504, "group": True}, {"_id": 505, "group": "one"}]
                scan.insert_many(documents)
                indexed.insert_many(documents)
                indexed.create_index("group")
                queries = [
                    {"group": {"$in": [1, 2, 3, 2, 1.0]}},
                    {"group": {"$in": [None, 1]}},
                    {"group": {"$in": [True, 2]}},
                    {"group": {"$in": [900, 901, 902]}},
                    {"group": {"$in": [1, {"nested": 1}]}},
                    {"group": {"$in": [1, re.compile("^one")]}}]
                for query in queries:
                    expected = list(scan.find(query).batch_size(2))
                    self.assertEqual(list(indexed.find(query).batch_size(2)), expected)
                    self.assertEqual(indexed.count_documents(query), len(expected))
                    self.assertEqual(list(indexed.find(query).skip(1).limit(4).batch_size(2)), expected[1:5])
                query = queries[0]
                expected = list(scan.find(query))
                cursor = indexed.find(query).batch_size(2)
                prefix = [next(cursor), next(cursor)]
                indexed.drop_index("group_1")
                indexed.create_index("group", name="rebuilt_group")
                self.assertEqual(prefix + list(cursor), expected)
                for collection in (scan, indexed):
                    self.assertEqual(collection.update_many(query, {"$inc": {"visits": 1}}).modified_count, len(expected))
                    self.assertEqual(collection.find_one({"_id": 500})["visits"], 1)
                self.assertEqual(list(indexed.find({})), list(scan.find({})))
            with briskdb.MongoClient(folder=folder) as reopened:
                self.assertEqual(reopened.app.indexed.delete_many(query).deleted_count, len(expected))
                self.assertEqual(reopened.app.indexed.count_documents(query), 0)

    def test_selective_reads_survive_collection_growth_unrelated_data_and_reopen(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                people = client.app.people
                people.insert_one({"_id": 10000, "email": "target@example.com"})
                people.create_index("email")
                for end in (64, 256):
                    start = 0 if end == 64 else 64
                    people.insert_many([
                        {"_id": i, "email": f"person{i}@example.com"}
                        for i in range(start, end)
                    ])
                    client.app.unrelated.insert_many([
                        {"body": "x" * 16384, "email": "target@example.com"}
                        for _ in range(16)
                    ])
                    for _ in range(3):
                        self.assertEqual(people.find_one({"email": "target@example.com"}),
                                         people.find_one({"_id": 10000}))
                        self.assertIsNone(people.find_one({"email": "absent@example.com"}))
                    self.assertEqual(people.count_documents({"email": "target@example.com"}), 1)
                # Mutations use the same candidate helper and must preserve index maintenance.
                self.assertEqual(people.update_one({"email": "target@example.com"},
                    {"$set": {"email": "renamed@example.com"}}).modified_count, 1)
                self.assertIsNone(people.find_one({"email": "target@example.com"}))
            with briskdb.MongoClient(folder=folder) as reopened:
                self.assertEqual(reopened.app.people.find_one({"email": "renamed@example.com"})["_id"], 10000)
                self.assertEqual(reopened.app.people.delete_one({"email": "renamed@example.com"}).deleted_count, 1)
                self.assertIsNone(reopened.app.people.find_one({"email": "renamed@example.com"}))

    def test_natural_pages_multikey_and_fallback_match_scan_on_both_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                items.insert_many([
                    {"_id": i, "v": (["hit", "hit"] if i % 3 == 0 else
                                     {"nested": "hit"} if i % 3 == 1 else None)}
                    for i in range(240)
                ])
                queries = [{"v": "hit"}, {"v": "miss"}, {"v": None},
                           {"v": {"nested": "hit"}}, {"v": {"$in": ["hit", None]}}]
                expected = [list(items.find(query).batch_size(3)) for query in queries]
                items.create_index("v")
                for query, result in zip(queries, expected):
                    self.assertEqual(list(items.find(query).batch_size(3)), result)
                    self.assertEqual(items.count_documents(query), len(result))


class AsyncSelectiveIndexReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_membership_seeks_survive_unrelated_growth(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.AsyncMongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                await items.insert_many([{"_id": i, "group": i} for i in range(256)])
                await items.create_index("group")
                query = {"group": {"$in": [3, 127, 255]}}
                expected = [{"_id": i, "group": i} for i in (3, 127, 255)]
                for _ in range(2):
                    await client.app.unrelated.insert_many([{"body": "x" * 65536} for _ in range(16)])
                    self.assertEqual(await items.find(query).batch_size(1).to_list(None), expected)
                    self.assertEqual(await items.count_documents(query), 3)
                    self.assertEqual(await items.find({"group": {"$in": [900, 901, 902]}}).to_list(None), [])

    async def test_async_secondary_hit_and_miss(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.AsyncMongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                await items.insert_many([{"_id": i, "email": f"user{i}"} for i in range(128)])
                await items.create_index("email")
                for i in (0, 63, 127):
                    self.assertEqual(await items.find_one({"email": f"user{i}"}),
                                     {"_id": i, "email": f"user{i}"})
                self.assertIsNone(await items.find_one({"email": "missing"}))


if __name__ == "__main__":
    unittest.main()
