"""Legacy query fixtures through modern PyMongo, without legacy cursor APIs."""

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import unittest

from bson.errors import InvalidDocument
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError

import briskdb


def legacy_rows():
    rows = []
    for number in range(100):
        row = {
            "_id": number, "count": number, "countStr": str(number),
            "countFloat": number + .1, "countBool": bool(number & 1),
            "countArray": list(range(number, number + 5)),
            "countDict": {"odd": bool(number & 1), "even": not (number & 1),
                          "three": not (number % 3), "five": not (number % 5)},
            "nestedArray": [[number + i] for i in range(5)],
            "dictArray": [{"number": number + i} for i in range(5)],
        }
        row["mixedDict"] = copy.deepcopy(row)
        rows.append(row)
    return rows


class UpstreamCoreClientTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_legacy_hundred_row_query_results_without_removed_cursor_count(self):
        self.items.insert_many(legacy_rows())
        for query, expected in [
            ({}, range(100)),
            ({"count": {"$gte": 50}}, range(50, 100)),
            ({"mixedDict.count": 0}, [0]),
            ({"mixedDict.count": {"$gte": 50}}, range(50, 100)),
            ({"mixedDict.countDict.even": True}, range(0, 100, 2)),
            ({"count": {"$gte": 50, "$lt": 51}}, [50]),
            ({"count": {"$gt": 50, "$lte": 51}}, [51]),
            ({"count": {"$ne": 50}}, [i for i in range(100) if i != 50]),
            ({"countStr": {"$regex": r"[5]{1,2}"}}, [5, *range(15, 50, 10), *range(50, 60), *range(65, 100, 10)]),
            ({"countStr": {"$regex": r"[^5][5]{1}"}}, [15, 25, 35, 45, 65, 75, 85, 95]),
            ({"count": {"$in": [22, 44, 66, 88]}}, [22, 44, 66, 88]),
            ({"countStr": {"$in": ["11", "33", "55", "77", "99"]}}, [11, 33, 55, 77, 99]),
            ({"countArray": {"$in": [22, 50]}}, [*range(18, 23), *range(46, 51)]),
            ({"$and": [{"count": {"$gt": 10}}, {"count": {"$lte": 50}}]}, range(11, 51)),
            ({"$or": [{"count": {"$lt": 10}}, {"count": {"$gte": 90}}]}, [*range(10), *range(90, 100)]),
        ]:
            with self.subTest(query=query):
                expected = list(expected)
                self.assertEqual([row["count"] for row in self.items.find(filter=query).sort("count")], expected)
                self.assertEqual(self.items.count_documents(query), len(expected))
        self.assertEqual(self.items.find_one(filter={"count": 3})["countStr"], "3")
        self.assertEqual([row["count"] for row in self.items.find().sort("count", -1).limit(2)], [99, 98])

    def test_negation_uses_current_conjunction_not_stale_legacy_expectation(self):
        self.items.insert_many(legacy_rows())
        # The old source expects 80 for this impossible conjunction; the locked
        # current TinyMongo engine and BriskDB both correctly return all 100.
        impossible = {"count": {"$not": {"$gte": 90, "$lt": 10}}}
        ordinary = {"count": {"$not": {"$gte": 10, "$lt": 90}}}
        self.assertEqual([row["count"] for row in self.items.find(impossible).sort("count")], list(range(100)))
        self.assertEqual([row["count"] for row in self.items.find(ordinary).sort("count")], [*range(10), *range(90, 100)])

    def test_crud_results_replacements_and_duplicate_batches_keep_exact_state(self):
        self.items.insert_many(legacy_rows())
        result = self.items.update_one({"count": 3}, {"$set": {"countStr": "three"}})
        self.assertEqual((result.matched_count, result.modified_count), (1, 1))
        self.assertTrue(result.raw_result["updatedExisting"])
        self.assertEqual(self.items.find_one({"count": 3})["countStr"], "three")
        self.assertEqual(self.items.delete_one({"count": 3}).deleted_count, 1)
        self.assertEqual(self.items.delete_many({"count": {"$gte": 50}}).deleted_count, 50)
        self.assertEqual(self.items.count_documents({}), 49)
        self.assertEqual(self.items.delete_many({}).deleted_count, 49)
        batch = [{"count": 1000 + i, "countStr": str(1000 + i)} for i in range(10)]
        self.assertEqual(self.items.insert_many(batch).inserted_ids, [row["_id"] for row in batch])
        self.assertEqual(self.items.count_documents({}), 10)
        self.assertEqual(self.items.delete_many({}).deleted_count, 10)
        inserted = {"name": "Ada", "score": 1}
        self.assertEqual(self.items.insert_one(inserted).inserted_id, inserted["_id"])
        updated = self.items.update_one({"score": {"$exists": True}}, {"$set": {"score": 2}})
        replaced = self.items.replace_one({"score": {"$exists": True}}, {"name": "Grace"})
        self.assertEqual((updated.matched_count, updated.modified_count, replaced.matched_count), (1, 1, 1))
        expected = {"_id": inserted["_id"], "name": "Grace"}
        self.assertEqual(self.items.find_one({}), expected)
        for ordered in (True, False):
            with self.assertRaises(BulkWriteError) as caught:
                self.items.insert_many([dict(expected)], ordered=ordered)
            details = caught.exception.details
            self.assertEqual(details["nInserted"], 0)
            self.assertEqual([(error["index"], error["code"]) for error in details["writeErrors"]], [(0, 11000)])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(list(reopened.app.items.find({})), [expected])

    def test_handle_selection_is_lazy_and_legacy_cursor_methods_are_not_installed(self):
        self.assertIsInstance(self.client["app"], Database)
        self.assertIsInstance(self.client.app["items"], Collection)
        self.assertEqual(self.client.app.list_collection_names(), [])
        self.items.insert_many([{"_id": 1}, {"_id": 2}])
        with briskdb.MongoClient(self.root.name) as peer:
            self.assertEqual(peer.app.list_collection_names(), ["items"])
        cursor = self.items.find({}).sort("_id")
        with self.assertRaises(AttributeError):
            cursor.count()
        with self.assertRaises(AttributeError):
            cursor.hasNext()
        self.assertEqual(next(cursor), {"_id": 1})
        self.assertEqual(cursor.next(), {"_id": 2})
        with self.assertRaises(StopIteration):
            cursor.next()
        with self.assertRaises(TypeError):
            self.items.count()
        with self.assertRaises(TypeError):
            self.client.app.collection_names()
        self.assertIsNone(self.items.drop())
        self.assertIsNone(self.items.drop())
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_direct_id_operator_routes_and_invalid_values_use_real_bson_driver(self):
        self.items.insert_many([{"_id": 1, "value": {"value": 1}}, {"_id": "id", "value": 1}])
        for query, expected in [({"_id": {"$eq": 1.0}}, 1), ({"_id": {"$in": [1]}}, 1),
                                ({"_id": re.compile("id")}, "id")]:
            with self.subTest(query=query):
                self.assertEqual(self.items.find_one(query)["_id"], expected)
        # Explicit $eq compares BSON regex identity; a bare regex matches text.
        self.assertIsNone(self.items.find_one({"_id": {"$eq": re.compile("id")}}))
        self.assertEqual(self.items.find_one({"value": 1})["_id"], "id")
        with self.assertRaises(InvalidDocument):
            self.items.insert_one({"value": object()})
        with self.assertRaises(TypeError):
            self.items.insert_many({"value": 1})
        with self.assertRaises(TypeError):
            self.items.insert_one([{"value": 1}])
        with self.assertRaises(TypeError):
            self.items.find({}).sort(["count"], 1)
        self.assertEqual(self.items.count_documents({}), 2)

    @unittest.skipUnless(os.environ.get("BRISKDB_MONGO_ORACLE_SOURCE_ROOT"), "requires locked source fixture")
    def test_locked_ninety_row_mixed_sort_matches_current_reference_order(self):
        path = Path(os.environ["BRISKDB_MONGO_ORACLE_SOURCE_ROOT"]) / "tests/dataset_sample/mixed_type.json"
        source = path.read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(), "ea1cfe399d7da3326c23eb636c7f098d8aab65034d0e5f3417e24dbe9d4265f9")
        rows = [dict(json.loads(line), _id=i + 1) for i, line in enumerate(source.splitlines())]
        self.assertEqual(len(rows), 90)
        # Captured from the locked current TinyMongo engine, including stable
        # source-order ties; no removed MongoDB Cursor.count API is required.
        expected = [17, 65, 32, 33, 47, 48, 63, 64, 68, 69, 78, 79, 23, 81, 42, 8, 21, 22, 70,
                    82, 84, 85, 86, 87, 16, 88, 83, 4, 41, 7, 44, 9, 10, 43, 45, 20, 90, 35, 38,
                    36, 37, 89, 19, 80, 31, 50, 74, 75, 76, 26, 27, 28, 73, 25, 34, 51, 5, 3, 30,
                    29, 49, 6, 2, 1, 59, 60, 58, 56, 57, 55, 54, 52, 53, 61, 62, 18, 67, 71, 11,
                    12, 13, 24, 72, 14, 15, 40, 46, 66, 39, 77]
        self.items.insert_many(rows)
        self.assertEqual([row["_id"] for row in self.items.find({}).sort([("item", 1), ("amount", -1)])], expected)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual([row["_id"] for row in reopened.app.items.find({}).sort([("item", 1), ("amount", -1)])], expected)


if __name__ == "__main__":
    unittest.main()
