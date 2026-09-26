"""Public point/indexed reads with native, rather than Python SQL, work counts."""

import asyncio
import re
import tempfile
import unittest

from bson import ObjectId

import briskdb


class UpstreamPointReadTests(unittest.TestCase):
    def test_point_hits_misses_projection_and_native_work_remain_bounded(self):
        target = ObjectId("00000000000000000000000b")
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=4) as client:
                items = client.app.items
                records = [{"_id": n, "name": f"item-{n}", "body": "x" * 10000} for n in range(12)]
                items.insert_many(records + [{"_id": target, "body": "target"}])
                for _ in range(3):
                    self.assertEqual(items.find_one({"_id": 11}), records[11])
                    self.assertIsNone(items.find_one({"_id": 99}))
                    self.assertEqual(items.find_one({"_id": target}), {"_id": target, "body": "target"})
                self.assertEqual(items.find_one({"_id": {"$eq": 2}}, {"name": 1, "_id": 0}), {"name": "item-2"})
                self.assertEqual(items.count_documents({"_id": {"$eq": 2}}), 1)
                self.assertEqual(items.count_documents({"_id": {"$eq": 99}}), 0)
                self.assertEqual(list(items.find({"_id": 2}).skip(1).limit(1)), [])
            # The embedded API observes the same stored data and native engine.
            # Counters measure record reads, not SQL statements or page I/O.
            with briskdb.open(root, documents=True) as database:
                with database.session() as session:
                    for identifier, count in [(11, 1), (99, 0), (target, 1)]:
                        result = session.find("app", "items", {"_id": identifier}, execution_stats=True)
                        self.assertEqual(result["plan"]["kind"], "point")
                        self.assertEqual(len(result["plan"]["shards"]), 1)
                        stats = result["read_stats"]
                        self.assertEqual(stats["storage_reads"], 1)
                        self.assertEqual(stats["documents_examined"], count)
                        self.assertEqual(stats["source_matches"], count)
                        self.assertEqual(stats["matcher_evaluations"], 0)
                        self.assertEqual(stats["shards_read"], result["plan"]["shards"])

    def test_regex_logical_and_typed_id_queries_keep_exact_identities(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=4) as client:
            items = client.app.items
            items.insert_many([{"_id": "alpha", "value": 1}, {"_id": "beta", "value": 2}])
            self.assertEqual(items.find_one({"_id": re.compile("^a")}), {"_id": "alpha", "value": 1})
            query = {"$and": [{"_id": "beta"}]}
            self.assertEqual(list(items.find(query).limit(1)), [{"_id": "beta", "value": 2}])
            self.assertEqual(items.count_documents(query), 1)
            typed = client.app.typed
            typed.insert_many([{"_id": 1, "kind": "number"}, {"_id": "1", "kind": "string"},
                               {"_id": True, "kind": "boolean"}, {"_id": [1, 2], "kind": "array"}])
            for identifier, kind in [(1, "number"), (1.0, "number"), ("1", "string"), (True, "boolean"), ([1, 2], "array")]:
                self.assertEqual(typed.find_one({"_id": identifier})["kind"], kind)
                self.assertEqual(typed.find_one({"_id": {"$eq": identifier}})["kind"], kind)

    def test_indexed_scalar_array_union_bounds_and_native_candidate_work(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=4) as client:
                items = client.app.union
                items.insert_many([{"_id": "scalar", "value": "match"}, {"_id": "array", "value": ["other", "match"]},
                                   {"_id": "other", "value": "other"}, {"_id": "number", "value": 1},
                                   {"_id": "boolean", "value": True}, {"_id": "object-array", "value": [{"nested": 1}]}])
                items.create_index("value", name="value_lookup")
                for operand, expected in [("match", ["scalar", "array"]), (True, ["boolean"]), (1, ["number"])]:
                    for condition in [operand, {"$eq": operand}]:
                        self.assertEqual([row["_id"] for row in items.find({"value": condition})], expected)
                        self.assertEqual(items.count_documents({"value": condition}), len(expected))
                bounded = client.app.bounded
                bounded.insert_many([{"_id": n, "value": "match", "body": "x" * 10000} for n in range(20)]
                                    + [{"_id": 100, "value": ["match"], "body": "y" * 10000}])
                bounded.create_index("value")
                self.assertEqual([row["_id"] for row in bounded.find({"value": "match"}).limit(1)], [0])
                self.assertEqual([row["_id"] for row in bounded.find({"value": "match"}).skip(2).limit(1)], [2])
                rare = client.app.rare
                rare.insert_many([{"_id": n, "tags": ["match" if n == 197 else "other"]} for n in range(250)])
                rare.create_index("tags")
                self.assertEqual(list(rare.find({"tags": "match"})), [{"_id": 197, "tags": ["match"]}])
            with briskdb.open(root, documents=True) as database:
                with database.session() as session:
                    result = session.find("app", "rare", {"tags": "match"}, plan_diagnostics=True, execution_stats=True)
                    self.assertEqual(result["documents"], [{"_id": 197, "tags": ["match"]}])
                    access = result["plan"]["read_access"]
                    self.assertEqual((access["kind"], access["candidate_kind"], access["key_count"]),
                                     ("index_candidates", "equality", 1))
                    self.assertEqual(result["read_stats"]["documents_examined"], 1)
                    self.assertEqual(result["read_stats"]["source_matches"], 1)

    def test_peer_logical_drop_invalidates_retained_collection_and_index_state(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as first, briskdb.MongoClient(root) as second:
                retained = first.app.items
                retained.insert_one({"_id": 1, "value": "before"})
                retained.create_index("value", name="value_lookup")
                self.assertEqual(retained.find_one({"value": "before"})["_id"], 1)
                self.assertEqual(second.app.drop_collection("items"), {"ok": 1.0})
                self.assertIsNone(retained.find_one({"_id": 1}))
                self.assertEqual(list(retained.find({"value": "before"})), [])
                self.assertNotIn("items", first.app.list_collection_names())
                retained.insert_one({"_id": 2, "value": "after"})
                self.assertEqual(set(retained.index_information()), {"_id_"})
                self.assertEqual(retained.create_index("value", name="value_lookup"), "value_lookup")
                self.assertEqual(retained.find_one({"value": "after"}), {"_id": 2, "value": "after"})
            with briskdb.MongoClient(root) as reader:
                self.assertEqual(list(reader.app.items.find({})), [{"_id": 2, "value": "after"}])
                self.assertEqual(set(reader.app.items.index_information()), {"_id_", "value_lookup"})


class UpstreamAsyncPointReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_point_projection_count_and_bounds_after_sync_seed(self):
        with tempfile.TemporaryDirectory() as root:
            def seed():
                with briskdb.MongoClient(root, shards=4) as writer:
                    writer.app.items.insert_many([{"_id": 1, "name": "Ada", "body": "x" * 10000},
                                                  {"_id": 2, "name": "Grace", "body": "y" * 10000}])
            await asyncio.to_thread(seed)
            async with briskdb.AsyncMongoClient(root) as reader:
                items = reader.app.items
                self.assertEqual(await items.find_one({"_id": {"$eq": 2}}, {"name": 1, "_id": 0}), {"name": "Grace"})
                self.assertEqual(await items.count_documents({"_id": 99}), 0)
                self.assertEqual(await items.count_documents({"_id": {"$eq": 2}}), 1)
                self.assertEqual(await items.find({"_id": 2}).skip(1).limit(1).to_list(), [])
                self.assertIsNone(await items.find_one({"_id": 99}))


if __name__ == "__main__":
    unittest.main()
