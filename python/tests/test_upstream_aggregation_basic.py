"""Owned public scenarios from the locked basic aggregation stage suite."""

import copy
from datetime import date
import tempfile
import unittest

from bson.errors import InvalidDocument
from pymongo.errors import OperationFailure

import briskdb


class UpstreamAggregationBasicTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def rows(self, pipeline):
        return list(self.items.aggregate(pipeline))

    def test_invalid_stage_arguments_reject_before_catalog_creation(self):
        for stage, code in [
            ({"$sort": None}, 15973), ({"$sort": []}, 15973), ({"$sort": {}}, 15976),
            ({"$sort": {"value": True}}, 15974), ({"$sort": {"value": 0}}, 15975),
            ({"$sort": {"value": 2}}, 15975), ({"$sort": {"": 1}}, 40352),
            ({"$sort": {"value..nested": 1}}, 15998), ({"$sort": {"value.": 1}}, 40353),
            ({"$sort": {"$value": 1}}, 16410),
            ({"$sort": {"field%d" % n: 1 for n in range(33)}}, 13103),
            ({"$skip": -1}, 5107200), ({"$skip": 1.5}, 5107200),
            ({"$skip": True}, 5107200), ({"$skip": "1"}, 5107200),
            ({"$limit": -1}, 5107201), ({"$limit": 0}, 15958),
            ({"$limit": 1.5}, 5107201), ({"$limit": True}, 5107201),
            ({"$limit": "1"}, 5107201), ({"$count": 1}, 40156),
            ({"$count": ""}, 40157), ({"$count": "$total"}, 40158),
            ({"$count": "nested.total"}, 40160), ({"$count": "bad\x00total"}, 40159),
            ({"$count": "_id"}, 15948),
        ]:
            with self.subTest(stage=stage), self.assertRaises(OperationFailure) as caught:
                self.rows([{"$match": {}}, stage])
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(self.client.app.list_collection_names(), [])
        for stage, error in [({"$sort": {1: 1}}, InvalidDocument),
                             ({"$skip": 2**63}, OverflowError),
                             ({"$limit": 2**63}, OverflowError)]:
            with self.subTest(stage=stage), self.assertRaises(error):
                self.rows([stage])
            self.assertEqual(self.client.app.list_collection_names(), [])

    def test_sort_meta_rejects_before_catalog_creation(self):
        with self.assertRaises(OperationFailure):
            self.rows([{"$sort": {"score": {"$meta": "textScore"}}}])
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_integral_numeric_arguments_preserve_pipeline(self):
        self.items.insert_many([{"_id": n} for n in range(1, 5)])
        pipeline = [{"$sort": {"_id": -1.0}}, {"$skip": 1.0}, {"$limit": 2.0}]
        original = copy.deepcopy(pipeline)
        self.assertEqual(self.rows(pipeline), [{"_id": 3}, {"_id": 2}])
        self.assertEqual(pipeline, original)

    def test_pagination_accepts_int64_maximum(self):
        self.items.insert_many([{"_id": 1}, {"_id": 2}])
        self.assertEqual(self.rows([{"$skip": 2**63 - 1}]), [])
        self.assertEqual(self.rows([{"$sort": {"_id": 1}}, {"$limit": 2**63 - 1}]),
                         [{"_id": 1}, {"_id": 2}])

    def test_count_is_empty_on_empty_input_and_feeds_projection(self):
        self.assertEqual(self.rows([{"$count": "total"}]), [])
        self.items.insert_many([{"_id": n, "keep": n != 2} for n in range(1, 4)])
        self.assertEqual(self.rows([{"$match": {"keep": True}}, {"$count": "total"},
                                    {"$project": {"_id": 0, "total": 1}}]), [{"total": 2}])

    def test_compound_sort_is_deterministic_and_results_are_isolated(self):
        documents = [{"_id": n, "team": team, "score": score, "nested": {"value": str(n)}}
                     for n, team, score in [(1, "b", 2), (2, "a", 1), (3, "a", 3), (4, "a", 3)]]
        self.items.insert_many(copy.deepcopy(documents))
        rows = self.rows([{"$sort": {"team": 1, "score": -1, "_id": 1}}])
        self.assertEqual([row["_id"] for row in rows], [3, 4, 2, 1])
        rows[0]["nested"]["value"] = "changed"
        self.assertEqual(list(self.items.find().sort("_id")), documents)

    def test_parallel_array_error_precedes_numeric_path_ambiguity(self):
        self.items.insert_one({"_id": 1, "first": [{"0": -5}, {"0": None}], "second": [2]})
        for sort, code in [({"first.0": 1}, 16746), ({"first.0": -1, "second": -1}, 2)]:
            with self.subTest(sort=sort), self.assertRaises(OperationFailure) as caught:
                self.rows([{"$sort": sort}])
            self.assertEqual(caught.exception.code, code)

    def test_skip_and_limit_apply_at_pipeline_positions(self):
        self.items.insert_many([{"_id": n, "value": value} for n, value in
                                [(1, 30), (2, 10), (3, 40), (4, 20)]])
        self.assertEqual(self.rows([{"$sort": {"value": 1}}, {"$skip": 1}, {"$limit": 2},
                                    {"$sort": {"value": -1}}]),
                         [{"_id": 1, "value": 30}, {"_id": 4, "value": 20}])

    def test_non_bson_sort_values_fail_encoding_not_private_python_warnings(self):
        for value in [date(2026, 1, 2), object()]:
            with self.assertRaises(InvalidDocument):
                self.items.insert_one({"_id": 1, "value": value})
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_missing_scalar_and_array_sort_paths_use_native_min_max(self):
        self.items.insert_many([{"_id": 1, "item": 1, "items": [1]},
                                {"_id": 2, "items": [{"score": 2}, {"score": 1}]},
                                {"_id": 3, "items": [{"score": 3}]}])
        for path in ["item.value", "items.9"]:
            self.assertEqual([r["_id"] for r in self.rows([{"$sort": {path: 1, "_id": 1}}])],
                             [1, 2, 3])
        for direction, expected in [(1, [1, 2, 3]), (-1, [3, 2, 1])]:
            self.assertEqual([r["_id"] for r in self.rows([{"$sort": {"items.score": direction}}])],
                             expected)

    def test_compound_sort_handles_large_shared_array(self):
        document = {"_id": 1, "values": [{"left": n, "right": 1000 - n} for n in range(1000)]}
        self.items.insert_one(document)
        self.assertEqual(self.rows([{"$sort": {"values.left": 1, "values.right": 1}}]), [document])


if __name__ == "__main__":
    unittest.main()
