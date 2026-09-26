"""Complex read results across native candidate selection and BSON limits."""

import tempfile
import unittest

from bson import Decimal128

import briskdb


BENCHMARK_QUERY = {"$and": [{"group": {"$in": ["g1", "g3", "g7"]}},
                            {"i": {"$gte": 40, "$lt": 360}}, {"i": {"$mod": [7, 0]}}]}


class UpstreamComplexReadTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)

    def ids(self, items, query):
        return [row["_id"] for row in items.find(query)]

    def test_combined_membership_range_mod_bounds_projection_and_count(self):
        items = self.client.app.items
        records = [{"_id": n, "group": f"g{n % 10}", "i": n, "label": f"item-{n}", "payload": "x" * 1000}
                   for n in range(400)]
        items.insert_many(records)
        expected = [n for n in range(400) if n % 10 in (1, 3, 7) and 40 <= n < 360 and n % 7 == 0]
        for state in ("unindexed", "indexed", "dropped"):
            with self.subTest(state=state):
                if state == "indexed":
                    items.create_index("group", name="group_lookup")
                    items.create_index("i", name="i_lookup")
                elif state == "dropped":
                    items.drop_index("group_lookup")
                    items.drop_index("i_lookup")
                self.assertEqual(self.ids(items, BENCHMARK_QUERY), expected)
                self.assertEqual([row["_id"] for row in items.find(BENCHMARK_QUERY).skip(2).limit(3)], expected[2:5])
                self.assertEqual(list(items.find(BENCHMARK_QUERY, {"label": 1, "_id": 0}).skip(1).limit(2)),
                                 [{"label": f"item-{n}"} for n in expected[1:3]])
                self.assertEqual(items.count_documents(BENCHMARK_QUERY), len(expected))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(self.ids(reader.app.items, BENCHMARK_QUERY), expected)

    def test_array_members_and_mixed_types_keep_matches_and_natural_bounds(self):
        arrays = self.client.app.arrays
        arrays.insert_many([{"_id": "scalar", "group": "g1", "i": 56},
                            {"_id": "array-group", "group": ["other", "g3"], "i": 63},
                            {"_id": "array-number", "group": "g7", "i": [-1, 70, 500]},
                            {"_id": "wrong-group", "group": ["g2", "g4"], "i": 77},
                            {"_id": "wrong-mod", "group": "g1", "i": [57, 58]}])
        arrays.create_index("group")
        arrays.create_index("i")
        self.assertEqual(self.ids(arrays, BENCHMARK_QUERY), ["scalar", "array-group", "array-number"])
        mixed = self.client.app.mixed
        values = [True, 1, "1", [False, True], [0, 1], ["0", "1"], False]
        mixed.insert_many([{"_id": n, "group": value, "i": 7} for n, value in enumerate(values)])
        mixed.create_index("group")
        self.assertEqual(self.ids(mixed, {"$and": [{"group": {"$in": [True, 1, "1"]}}, {"i": {"$mod": [7, 0]}}]}), list(range(6)))
        self.assertEqual(self.ids(mixed, {"group": {"$in": [True, True]}}), [0, 3])
        ordered = self.client.app.ordered
        ordered.insert_many([{"_id": "scalar-first", "group": "g1", "i": 7},
                             {"_id": "array-first", "group": ["g1"], "i": 14},
                             {"_id": "scalar-miss", "group": "g2", "i": 21},
                             {"_id": "array-second", "group": ["other", "g1"], "i": 28},
                             {"_id": "scalar-second", "group": "g1", "i": 35}])
        ordered.create_index("group")
        query = {"$and": [{"group": {"$in": ["g1"]}}, {"i": {"$mod": [7, 0]}}]}
        self.assertEqual([row["_id"] for row in ordered.find(query).skip(1).limit(2)], ["array-first", "array-second"])

    def test_embedded_object_and_decimal_residuals_remain_complete(self):
        objects = self.client.app.objects
        objects.insert_many([{"_id": "match", "group": {"name": "g1", "rank": 1}, "i": 14},
                             {"_id": "different-object", "group": {"name": "g1"}, "i": 14},
                             {"_id": "outside-range", "group": {"name": "g1", "rank": 1}, "i": -1}])
        objects.create_index("group")
        objects.create_index("i")
        query = {"$and": [{"group": {"$in": [{"name": "g1", "rank": 1}]}}, {"i": {"$gte": 0, "$lt": 100}}]}
        self.assertEqual(self.ids(objects, query), ["match"])
        decimals = self.client.app.decimals
        decimals.insert_many([{"_id": "decimal-match", "group": "g1", "i": Decimal128("56")},
                              {"_id": "decimal-mod-miss", "group": "g1", "i": Decimal128("57")},
                              {"_id": "ordinary-match", "group": "g3", "i": 63}])
        decimals.create_index("group")
        decimals.create_index("i")
        self.assertEqual(self.ids(decimals, BENCHMARK_QUERY), ["decimal-match", "ordinary-match"])

    def test_negative_safe_integer_and_int64_boundaries_remain_exact(self):
        items = self.client.app.numbers
        safe, minimum = 2 ** 53 - 1, -(2 ** 63)
        items.insert_many([{"_id": "below-safe", "group": "g1", "i": -safe - 1},
                           {"_id": "safe-match", "group": "g1", "i": -safe},
                           {"_id": "safe-neighbor", "group": "g1", "i": -safe + 1},
                           {"_id": "safe-array", "group": "g1", "i": [-safe - 1, -safe]},
                           {"_id": "int64-match", "group": "g1", "i": minimum},
                           {"_id": "int64-neighbor", "group": "g1", "i": minimum + 1},
                           {"_id": "wrong-group", "group": "g2", "i": -safe}])
        items.create_index("group")
        self.assertEqual(self.ids(items, {"$and": [{"group": {"$in": ["g1"]}},
                                                  {"i": {"$gte": -safe, "$lte": safe}},
                                                  {"i": {"$mod": [7, -3]}}]}), ["safe-match", "safe-array"])
        self.assertEqual(self.ids(items, {"$and": [{"group": {"$in": ["g1"]}},
                                                  {"i": {"$gte": minimum, "$lte": minimum + 1}},
                                                  {"i": {"$mod": [7, -1]}}]}), ["int64-match"])

    def test_or_negative_exists_and_large_membership_do_not_lose_candidates(self):
        items = self.client.app.branches
        items.insert_many([{"_id": 1, "group": "g1", "score": 1, "label": "inside"},
                           {"_id": 2, "group": "g1", "score": 2, "label": "inside"},
                           {"_id": 3, "group": "g2", "score": 3, "label": "outside-match"},
                           {"_id": 4, "group": "g2", "score": 4, "label": "outside-miss"}, {"_id": 5}])
        items.create_index("group")
        query = {"$or": [{"$and": [{"group": {"$in": ["g1"]}}, {"score": {"$ne": 2}}]},
                         {"label": {"$regex": "^outside-match$"}}]}
        self.assertEqual(self.ids(items, query), [1, 3])
        self.assertEqual(self.ids(items, {"group": {"$nin": ["g1"]}}), [3, 4, 5])
        self.assertEqual(self.ids(items, {"group": {"$exists": True}}), [1, 2, 3, 4])
        self.assertEqual(self.ids(items, {"group": {"$in": []}}), [])
        large = self.client.app.large
        large.insert_many([{"_id": "first", "group": 1, "i": 7}, {"_id": "second", "group": 899, "i": 14},
                           {"_id": "outside", "group": 900, "i": 21}])
        large.create_index("group")
        self.assertEqual(self.ids(large, {"$and": [{"group": {"$in": list(range(900))}}, {"i": {"$gte": 0}}]}), ["first", "second"])

    def test_unbounded_python_integers_reject_before_storage_or_query_fallback(self):
        items = self.client.app.items
        with self.assertRaises(OverflowError):
            items.insert_many([{"_id": "first", "i": 1}, {"_id": "huge", "i": 2 ** 100}])
        self.assertNotIn("items", self.client.app.list_collection_names())
        items.insert_one({"_id": "safe", "group": "g1", "i": 1})
        with self.assertRaises(OverflowError):
            list(items.find({"$and": [{"group": {"$in": ["g1"]}}, {"i": {"$gte": 2 ** 100}}]}))
        with self.assertRaises(TypeError):
            items.find("not-a-filter")
        self.assertEqual(list(items.find({})), [{"_id": "safe", "group": "g1", "i": 1}])


if __name__ == "__main__":
    unittest.main()
