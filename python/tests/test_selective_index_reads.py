"""Real-driver correctness across selective and broad secondary-key frontiers."""

import tempfile
import unittest

import briskdb


class SelectiveIndexReadTests(unittest.TestCase):
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
