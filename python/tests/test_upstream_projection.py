"""Owned public scenarios from locked projection/projection_memory/unset suites.

Private backend hooks/SQL traces and legacy cursor helpers are separately
accounted for, not presented as unchanged upstream candidate passes.
"""

from collections import OrderedDict
from copy import deepcopy
import gc
import tempfile
import tracemalloc
import unittest

from bson import Binary, Decimal128, Int64, ObjectId
from bson.errors import InvalidDocument
from pymongo import MongoClient as DriverClient
from pymongo.errors import OperationFailure

import briskdb


def fixture():
    return [
        {"_id": 1, "name": "Ada", "secret": "x", "score": 7,
         "profile": {"email": "ada@example.com", "age": 36},
         "items": [{"sku": "a", "qty": 1}, {"qty": 2}, {}, "scalar", None]},
        {"_id": 2, "name": "Grace", "score": 9, "profile": {"age": 40}, "items": []},
        {"_id": 3, "name": "Lin", "score": 8, "profile": "unknown"},
    ]


def unset_seed():
    return [{"_id": 1, "group": "many", "obsolete": "remove", "nested": {"obsolete": True, "keep": 1}},
            {"_id": 2, "group": "many", "obsolete": "remove", "nested": {"keep": 2}},
            {"_id": 3, "group": "many", "nested": {"keep": 3}}]


UNSET = {"$unset": {"obsolete": "", "nested.obsolete": "", "missing": "", "nested.missing": ""}}


def peak(operation):
    gc.collect()
    tracemalloc.start()
    try:
        result = operation()
        return result, tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


class UpstreamProjectionTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.open()

    def open(self):
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def seed(self):
        self.items.insert_many(fixture())

    def test_inclusion_sequences_and_id_rules(self):
        self.seed()
        for projection in ({"name": 1}, ["name"], ("name",), ["name", "name"], {"name"}):
            self.assertEqual(self.items.find_one({"_id": 1}, projection), {"_id": 1, "name": "Ada"})
        self.assertEqual(self.items.find_one({"_id": 1}, {"name": 2, "_id": 0}), {"name": "Ada"})
        self.assertEqual(self.items.find_one({"_id": 1}, {"_id": 1}), {"_id": 1})
        self.assertEqual(self.items.find_one({"_id": 1}, {"_id": 0}), {k: v for k, v in fixture()[0].items() if k != "_id"})
        for projection in ({}, [], ()):
            self.assertEqual(self.items.find_one(filter={"_id": 1}, projection=projection), fixture()[0])
            self.assertEqual(list(self.items.find(filter={"_id": 1}, projection=projection)), [fixture()[0]])

    def test_nested_inclusion_exclusion_and_scalar_array_branches(self):
        self.seed()
        self.assertEqual(self.items.find_one({"_id": 1}, {"profile.email": 1, "profile.age": 1, "_id": 0}), {"profile": fixture()[0]["profile"]})
        self.assertEqual(self.items.find_one({"_id": 1}, {"profile": {"email": 1}}), {"_id": 1, "profile": {"email": "ada@example.com"}})
        self.assertEqual(self.items.find_one({"_id": 2}, {"profile.email": 1}), {"_id": 2, "profile": {}})
        self.assertEqual(self.items.find_one({"_id": 3}, {"profile.email": 1}), {"_id": 3})
        self.assertEqual(self.items.find_one({"_id": 1}, {"items.sku": 1}), {"_id": 1, "items": [{"sku": "a"}, {}, {}]})
        self.assertEqual(self.items.find_one({"_id": 1}, {"items.sku": 0})["items"], [{"qty": 1}, {"qty": 2}, {}, "scalar", None])
        expected = deepcopy(fixture()[0]); expected.pop("secret"); expected["profile"].pop("age")
        self.assertEqual(self.items.find_one({"_id": 1}, {"secret": 0, "profile.age": 0, "_id": 1}), expected)
        self.items.insert_one({"_id": 4, "groups": [[], [{"name": "a", "other": 1}], "scalar"],
                               "scalar": "value", "parent": {"scalar": "value", "kept": {"name": "yes"}}})
        projection = {"groups.name": 1, "scalar.child": 1, "missing.child": 1,
                      "parent.scalar.child": 1, "parent.kept.name": 1}
        self.assertEqual(self.items.find_one({"_id": 4}, projection), {"_id": 4, "groups": [[], [{"name": "a"}]], "parent": {"kept": {"name": "yes"}}})
        self.assertEqual(self.items.find_one({"_id": 3}, {"missing": 0, "profile.email": 0}), fixture()[2])

    def test_nested_id_paths_and_missing_results(self):
        self.seed()
        self.assertEqual(self.items.find_one({"_id": 1}, {"_id.missing": 1}), {})
        self.assertEqual(self.items.find_one({"_id": 1}, {"_id.missing": 0}), fixture()[0])
        identifier = {"value": 1, "other": 2}
        self.items.insert_one({"_id": identifier, "name": "object"})
        self.assertEqual(self.items.find_one({"_id": identifier}, {"_id.value": 1}), {"_id": {"value": 1}})
        self.assertIsNone(self.items.find_one({"_id": "absent"}, {"name": 1}))

    def test_sort_projection_index_access_and_returned_values_are_independent(self):
        self.seed()
        expected = [{"_id": 2, "name": "Grace"}, {"_id": 3, "name": "Lin"}, {"_id": 1, "name": "Ada"}]
        cursor = self.items.find({}, {"name": 1}).sort("score", -1)
        self.assertEqual(next(cursor), expected[0])
        # Modern PyMongo indexing uses an unevaluated cursor, not TinyMongo's
        # mutable in-memory current-record/string-index interface.
        self.assertEqual(cursor.clone()[1], expected[1])
        cursor.close()
        self.assertEqual(list(self.items.find({}, {"name": 1}, sort=[("score", -1)])), expected)
        self.items.create_index("name")
        found = self.items.find_one({"name": "Ada"}, {"profile.email": 1})
        found["profile"]["email"] = "changed"
        self.assertEqual(self.items.find_one({"name": "Ada"})["profile"]["email"], "ada@example.com")
        self.assertEqual(list(self.items.find({"name": "Ada"}, {"name": 1})), [{"_id": 1, "name": "Ada"}])

    def test_projection_error_codes_and_unsupported_forms(self):
        self.seed()
        cases = [({"name": 1, "secret": 0}, 31254), ({"secret": 0, "name": 1}, 31253),
                 ({"profile": 1, "profile.email": 1}, 31249), ({"profile.email": 1, "profile": 1}, 31250),
                 ({"profile": 1, "profile.email": 0}, 31249), ({"profile.email": 0, "profile": 1}, 31250),
                 ({"_id": 1, "_id.value": 1}, 31249), ({"_id.value": 1, "_id": 1}, 31250),
                 ({"_id": 0, "_id.value": 1}, 31249),
                 ({"profile": {"email": 1}, "profile.email": 1}, 31250),
                 ({"profile.email": 1, "profile": {"email": 1}}, 31250)]
        cases.extend((p, 115) for p in [{"items.0.sku": 1}, {"items.$": 1}, {"name": {"$meta": "textScore"}},
                                      {"name": {}}, {"name": "$other"}, {"name": None}, {"name": [1]}])
        for collection in (self.items, self.client.app.absent):
            for projection, code in cases:
                with self.subTest(projection=projection, collection=collection.name):
                    with self.assertRaises(OperationFailure) as caught:
                        list(collection.find({}, projection))
                    self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.client.app.list_collection_names(), ["items"])

    def test_driver_projection_container_and_path_errors_remain_explicit(self):
        for projection in (42, "name", ["name", 1]):
            with self.assertRaises(TypeError):
                self.items.find({}, projection)
        for projection in ({1: 1}, {"bad\x00key": 1}):
            with self.assertRaises(InvalidDocument):
                list(self.items.find({}, projection))
        for projection in ({"": 1}, {"a..b": 1}):
            with self.assertRaises(OperationFailure):
                list(self.items.find({}, projection))

    def test_projection_matches_scan_index_sorted_and_reopened_paths(self):
        rows = [{"_id": 1, "score": 2, "profile": {"name": "Ada", "secret": "a"}, "tags": ["python", "db"]},
                {"_id": 2, "score": 3, "profile": {"name": "Grace", "secret": "b"}, "tags": ["compiler"]},
                {"_id": 3, "score": 1, "profile": {"name": "Lin", "secret": "c"}, "tags": ["python"]}]
        self.items.insert_many(rows)
        for stage in range(3):
            if stage == 1:
                self.items.create_index("tags")
            elif stage == 2:
                self.client.close(); self.open()
            self.assertEqual(list(self.items.find({"tags": "python"}, {"profile.name": 1, "_id": 0})),
                             [{"profile": {"name": "Ada"}}, {"profile": {"name": "Lin"}}])
            expected = [{"_id": i, "profile": {"name": rows[i - 1]["profile"]["name"]}} for i in [2, 1, 3]]
            self.assertEqual(list(self.items.find({}, {"profile.name": 1}).sort("score", -1)), expected)
            self.assertEqual(list(self.items.find({}, {"profile.name": 1}, sort=[("score", -1)])), expected)
            self.assertEqual(list(self.items.find({"profile.name": "Ada"}, {"profile.name": 1, "_id": 0})), [{"profile": {"name": "Ada"}}])
            self.assertEqual(list(self.items.find({"_id": {"$in": [1, 3]}}, {"profile.name": 1, "_id": 0}).skip(1).limit(1)), [{"profile": {"name": "Lin"}}])
            self.assertEqual([r["_id"] for r in self.items.find({"tags": "python"}).sort("_id", -1)], [3, 1])

    def test_bson_identifier_projection_preserves_types_and_array_routes(self):
        identifiers = [None, False, True, 1.25, "text-id", Int64(2**63 - 1), b"binary-id", {"nested": 1}, [1, "two"]]
        self.items.insert_many([{"_id": i, "payload": "large"} for i in identifiers])
        self.assertEqual(list(self.items.find({}, {"_id": 1})), [{"_id": i} for i in identifiers])
        self.assertEqual(self.items.find_one({"_id": [1, "two"]}, {"payload": 1, "_id": 0}), {"payload": "large"})
        self.assertEqual(list(self.items.find({"_id": ["missing"]}, {"payload": 1})), [])
        self.assertEqual(list(self.items.find({"_id": {"$in": [[1, "two"]]}}, {"payload": 1, "_id": 0})), [{"payload": "large"}])
        with self.assertRaises(OverflowError):
            self.items.insert_one({"_id": 2**80})
        self.assertEqual(self.items.count_documents({}), len(identifiers))

    def test_lazy_filter_and_projection_inputs_are_frozen_per_cursor(self):
        self.items.insert_many([{"_id": i, "rank": i, "nested": {"keep": i, "other": -i}} for i in range(12)])
        query = {"rank": {"$in": [0]}}
        projection = {"nested": {"keep": 1}, "_id": 0}
        cursor = self.items.find(query, projection).limit(1)
        query["rank"]["$in"][0] = 11
        projection["nested"].clear(); projection["nested"]["other"] = 1
        expected = [{"nested": {"keep": 0}}]
        self.assertEqual(list(cursor.clone()), expected)
        self.assertEqual(list(cursor), expected)
        self.assertEqual(list(cursor.rewind()), expected)

    def test_cursor_windows_clones_rewind_and_close_preserve_results(self):
        self.items.insert_many([{"_id": i, "rank": 10 - i} for i in range(6)])
        self.assertEqual([r["_id"] for r in self.items.find({}).limit(1).skip(2)], [2])
        self.assertEqual([r["_id"] for r in self.items.find({}).limit(1).limit(0)], list(range(6)))
        self.assertEqual([r["_id"] for r in self.items.find({}).skip(4)], [4, 5])
        self.assertEqual([r["_id"] for r in self.items.find({}).limit(1).sort("rank", 1)], [5])
        cursor = self.items.find({}, {"_id": 1}).skip(2).limit(1)
        self.assertEqual(list(cursor.clone()), [{"_id": 2}])
        self.assertEqual(list(cursor), [{"_id": 2}])
        self.assertEqual(list(cursor.rewind()), [{"_id": 2}])
        lazy = self.items.find({}); self.assertTrue(lazy.alive); lazy.close(); self.assertFalse(lazy.alive)
        empty = self.client.app.absent.find({}); self.assertTrue(empty.alive)
        self.assertEqual(list(empty), []); self.assertFalse(empty.alive)

    def test_single_result_count_and_filtered_windows_agree(self):
        self.items.insert_many([{"_id": i, "rank": i, "nested": {"selected": i == 3}, "values": list(range(i % 4)), "body": "x" * 10000} for i in range(12)])
        self.assertEqual(self.items.find_one({})["_id"], 0)
        self.assertEqual(self.items.find_one({}, {"_id": 1}), {"_id": 0})
        self.assertEqual(self.items.find_one({"_id": 11})["_id"], 11)
        self.assertEqual([r["_id"] for r in self.items.find({}, limit=1)], [0])
        self.assertEqual(self.items.find_one({"nested.selected": True})["_id"], 3)
        self.assertIsNone(self.items.find_one({"nested.selected": "missing"}))
        self.assertEqual([r["_id"] for r in self.items.find({"nested.selected": {"$exists": True}}).skip(2).limit(1)], [2])
        self.assertEqual(self.items.count_documents({}), 12)
        self.assertEqual(self.items.count_documents({"rank": {"$exists": True}}), 12)
        self.assertEqual(self.items.estimated_document_count(), 12)
        self.assertEqual(self.items.count_documents({"values": {"$size": 2}}), 3)

    def test_client_heap_projection_and_bounded_reads_avoid_full_materialization(self):
        self.items.insert_many([{"_id": i, "body": "x" * 100000} for i in range(64)])
        first, first_peak = peak(lambda: self.items.find_one({}))
        count, count_peak = peak(lambda: self.items.count_documents({}))
        projected, projected_peak = peak(lambda: list(self.items.find({}, {"_id": 1})))
        complete, complete_peak = peak(lambda: list(self.items.find({})))
        self.assertEqual(first["_id"], 0); self.assertEqual(count, 64)
        self.assertEqual(projected, [{"_id": i} for i in range(64)])
        self.assertEqual(len(complete), 64)
        # This checks only the Python client's heap, not native engine RSS.
        self.assertGreater(complete_peak - projected_peak, 4000000)
        for measured in (first_peak, count_peak, projected_peak):
            self.assertLess(measured * 5, complete_peak)

    def test_unset_top_level_nested_missing_and_noop_counts_persist(self):
        self.items.insert_many(unset_seed())
        one = self.items.update_one({"_id": 1}, UNSET)
        self.assertEqual((one.matched_count, one.modified_count), (1, 1))
        self.assertEqual(self.items.update_one({"_id": 1}, UNSET).modified_count, 0)
        many = self.items.update_many({"group": "many"}, UNSET)
        self.assertEqual((many.matched_count, many.modified_count), (3, 1))
        self.assertEqual(self.items.update_many({"group": "many"}, UNSET).modified_count, 0)
        expected = [{"_id": i, "group": "many", "nested": {"keep": i}} for i in (1, 2, 3)]
        self.assertEqual(list(self.items.find({}).sort("_id")), expected)
        self.client.close(); self.open()
        self.assertEqual(list(self.items.find({}).sort("_id")), expected)

    def test_plain_pymongo_cursors_keep_unmodified_driver_behavior(self):
        self.items.insert_many([{"_id": i, "rank": i} for i in (0, 11)])
        with DriverClient("mongodb://" + self.client._briskdb_store.listener.address) as driver:
            query = {"rank": 0}; cursor = driver.app.items.find(query); query["rank"] = 11
            self.assertEqual([r["_id"] for r in cursor], [11])
            self.assertEqual(driver.app.items.find_one({"_id": 0}, "rank"), {"_id": 0})

    def test_local_find_snapshots_keep_driver_validation_and_bson_forms(self):
        class CopyTracked(dict):
            copies = 0

            def __deepcopy__(self, memo):
                type(self).copies += 1
                return deepcopy(dict(self), memo)

        query = CopyTracked(_id=1)
        with self.assertRaises(TypeError):
            self.items.find(query, unexpected_keyword=True)
        self.assertEqual(CopyTracked.copies, 0)
        oid = ObjectId()
        values = {"oid": oid, "binary": Binary(b"abc", 128), "decimal": Decimal128("2.5")}
        self.items.insert_one({"_id": 1, **values})
        cursor = self.items.find(CopyTracked(values), OrderedDict([("oid", 1)]))
        self.assertEqual(list(cursor), [{"_id": 1, "oid": oid}])
        self.assertEqual(CopyTracked.copies, 1)


class AsyncUpstreamProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_filter_projection_snapshot_clone_and_driver_results(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                with self.assertRaises(TypeError):
                    items.find({}, projection="nested")
                await items.insert_many([{"_id": i, "nested": {"keep": i, "other": -i}} for i in (0, 11)])
                query = {"_id": {"$in": [0]}}; projection = {"nested": {"keep": 1}, "_id": 0}
                cursor = items.find(query, projection)
                query["_id"]["$in"][0] = 11
                projection["nested"].clear(); projection["nested"]["other"] = 1
                expected = [{"nested": {"keep": 0}}]
                self.assertEqual(await cursor.clone().to_list(None), expected)
                self.assertEqual(await cursor.to_list(None), expected)
                await cursor.rewind()
                self.assertEqual(await cursor.to_list(None), expected)

    async def test_async_projection_sorted_ids_and_unset_noops_survive_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                await items.insert_many(unset_seed())
                result = await items.update_many({"group": "many"}, UNSET)
                self.assertEqual((result.matched_count, result.modified_count), (3, 2))
                self.assertEqual((await items.update_many({}, UNSET)).modified_count, 0)
                self.assertEqual(await items.find({}, {"_id": 1}).sort("nested.keep", -1).to_list(None), [{"_id": 3}, {"_id": 2}, {"_id": 1}])
                self.assertEqual(await items.count_documents({}), 3)
                self.assertEqual(await items.find_one({}, {"_id": 1}), {"_id": 1})
                self.assertEqual(await items.find({}, {"_id": 1}).limit(1).to_list(None), [{"_id": 1}])
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                self.assertEqual(await client.app.items.find({}).sort("_id").to_list(None),
                                 [{"_id": i, "group": "many", "nested": {"keep": i}} for i in (1, 2, 3)])


if __name__ == "__main__":
    unittest.main()
