"""Public bulk planning outcomes, without private Python planner counters."""

from datetime import datetime, timezone
import tempfile
import unittest

from bson import BSON, Decimal128
from bson.errors import InvalidDocument
from pymongo.errors import BulkWriteError, OperationFailure

import briskdb


class UpstreamBulkPlanningTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)

    def test_large_unique_batches_and_late_duplicate_keep_global_error_position(self):
        items = self.client.app.items
        records = [{"_id": n, "email": f"user-{n}@example.test", "group": "same"} for n in range(3100)]
        items.insert_many(records[:100])
        items.create_index("email", unique=True)
        items.create_index("group")
        self.assertEqual(items.insert_many(records[100:2100], ordered=False).inserted_ids, list(range(100, 2100)))
        incoming = records[2100:] + [{"_id": 0, "email": "otherwise-new@example.test"}]
        with self.assertRaises(BulkWriteError) as caught:
            items.insert_many(incoming, ordered=False)
        self.assertEqual(caught.exception.details["nInserted"], 1000)
        errors = caught.exception.details["writeErrors"]
        self.assertEqual([(error["index"], error["code"]) for error in errors], [(1000, 11000)])
        self.assertIs(errors[0]["op"], incoming[1000])
        self.assertEqual(list(items.find({}).sort("_id")), records)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(list(reader.app.items.find({}).sort("_id")), records)

    def test_document_id_and_unique_errors_preserve_input_order_and_original_ops(self):
        original = {"_id": {"number": 1}, "email": "taken@example.test"}
        incoming = [{"_id": {"number": 1.0}, "email": "different@example.test"},
                    {"_id": "first", "email": "first@example.test"},
                    {"_id": "unique-conflict", "email": "taken@example.test"},
                    {"_id": "batch-conflict", "email": "first@example.test"},
                    {"_id": "last", "email": "last@example.test"}]
        for ordered, inserted, positions in [(True, 0, [0]), (False, 2, [0, 2, 3])]:
            with self.subTest(ordered=ordered):
                items = self.client.app[f"errors_{ordered}"]
                items.insert_one(original)
                items.create_index("email", unique=True)
                with self.assertRaises(BulkWriteError) as caught:
                    items.insert_many(incoming, ordered=ordered)
                self.assertEqual(caught.exception.details["nInserted"], inserted)
                errors = caught.exception.details["writeErrors"]
                self.assertEqual([error["index"] for error in errors], positions)
                for error in errors:
                    self.assertEqual(error["code"], 11000)
                    self.assertIs(error["op"], incoming[error["index"]])
                    self.assertNotIn("keyPattern", error)
                    self.assertNotIn("keyValue", error)
                    self.assertNotIn("example.test", error["errmsg"])
                expected = [original] + ([] if ordered else [incoming[1], incoming[4]])
                self.assertEqual([BSON.encode(row) for row in items.find({})], [BSON.encode(row) for row in expected])

    def test_bulk_numeric_aliases_and_distinctions_match_bson_precision(self):
        duplicate_pairs = [(0, -0.0), (2 ** 60, float(2 ** 60)), (1, Decimal128("1.00")),
                           (Decimal128("1.00"), 1),
                           (datetime(2026, 8, 4, 12, 30), datetime(2026, 8, 4, 12, 30, tzinfo=timezone.utc)),
                           ({"first": 1, "second": 2}, {"first": 1, "second": 2})]
        for number, (existing, incoming) in enumerate(duplicate_pairs):
            with self.subTest(duplicate=number):
                items = self.client.app[f"alias_{number}"]
                original = {"_id": existing, "label": "original"}
                items.insert_one(original)
                with self.assertRaises(BulkWriteError) as caught:
                    items.insert_many([{"_id": incoming, "label": "duplicate"}])
                self.assertEqual(caught.exception.details["nInserted"], 0)
                self.assertEqual([(error["index"], error["code"]) for error in caught.exception.details["writeErrors"]], [(0, 11000)])
                self.assertEqual([BSON.encode(row) for row in items.find({})], [BSON.encode(original)])
        for number, (existing, incoming) in enumerate([(True, 1), (2 ** 53 + 1, float(2 ** 53 + 1))]):
            items = self.client.app[f"distinct_{number}"]
            records = [{"_id": existing}, {"_id": incoming}]
            items.insert_one(records[0])
            self.assertEqual(items.insert_many(records[1:]).inserted_ids, [incoming])
            self.assertEqual([BSON.encode(row) for row in items.find({})], [BSON.encode(row) for row in records])
        for number, outside_bson in enumerate([int(1e23), 10 ** 23]):
            items = self.client.app[f"outside_{number}"]
            with self.assertRaises(OverflowError):
                items.insert_many([{"_id": "must-not-commit"}, {"_id": outside_bson}], ordered=False)
            self.assertNotIn(items.name, self.client.app.list_collection_names())

    def test_nonunique_values_and_invalid_ids_do_not_weaken_batch_validation(self):
        items = self.client.app.items
        items.create_index("email")
        records = [{"_id": "seed", "email": "same@example.test"}, {"_id": "new", "email": "same@example.test"}]
        items.insert_one(records[0])
        self.assertEqual(items.insert_many(records[1:]).inserted_ids, ["new"])
        before = items.index_information()
        with self.assertRaises(OperationFailure) as caught:
            items.create_index("email", unique=True, name="unique_email")
        self.assertEqual(caught.exception.code, 11000)
        self.assertEqual(items.index_information(), before)
        with self.assertRaises(InvalidDocument):
            items.insert_many([{"_id": "must-not-commit"}, {"_id": object()}], ordered=False)
        self.assertEqual(list(items.find({})), records)


if __name__ == "__main__":
    unittest.main()
