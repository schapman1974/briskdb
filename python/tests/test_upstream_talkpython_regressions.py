"""Public native-wire scenarios from the locked Talk Python regression suite."""

from datetime import date, datetime, timedelta, timezone
import tempfile
import unittest
from uuid import uuid4

from bson import Binary, ObjectId
from bson.errors import InvalidDocument

import briskdb


class UpstreamTalkPythonRegressions(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_generated_objectids_cover_insert_and_every_upsert_path(self):
        single = self.items.insert_one({"kind": "single"})
        many = self.items.insert_many([{"kind": "many-a"}, {"kind": "many-b"}])
        one = self.items.update_one({"kind": "update-one"}, {"$set": {"created": True}}, upsert=True)
        multi = self.items.update_many({"kind": "update-many"}, {"$set": {"created": True}}, upsert=True)
        replacement = self.items.replace_one({"kind": "replacement"}, {"kind": "replacement", "created": True}, upsert=True)
        ids = [single.inserted_id, *many.inserted_ids, one.upserted_id, multi.upserted_id, replacement.upserted_id]
        self.assertEqual(len(set(ids)), 6)
        for value in ids:
            self.assertIs(type(value), ObjectId)
            self.assertEqual(self.items.find_one({"_id": ObjectId(str(value))})["_id"], value)
        self.assertEqual(self.items.count_documents({}), 6)

    def test_explicit_hex_string_ids_do_not_need_a_private_id_generator(self):
        value = uuid4().hex
        self.assertEqual(len(value), 32)
        self.assertIs(type(self.items.insert_one({"_id": value}).inserted_id), str)
        self.assertEqual(self.items.find_one({"_id": value}), {"_id": value})
        # The addon intentionally requires BSON/PyMongo; it has no TinyMongo
        # generate_id export or monkeypatchable no-BSON fallback allocator.
        self.assertFalse(hasattr(briskdb, "generate_id"))

    def test_invalid_no_match_updates_reject_before_collection_creation(self):
        for operation in ["update_one", "update_many", "replace_one"]:
            with self.subTest(operation=operation):
                payload = {"value": {1, 2}} if operation == "replace_one" else {"$unset": {"value": {1, 2}}}
                with self.assertRaises(InvalidDocument):
                    getattr(self.items, operation)({"_id": "missing"}, payload)
                self.assertEqual(self.client.app.list_collection_names(), [])
                self.assertEqual(self.items.count_documents({}), 0)

    def test_datetime_and_objectid_sort_in_both_directions(self):
        base = datetime(2026, 1, 1)
        self.items.insert_many([
            {"_id": ObjectId(f"{number:024x}"), "label": number, "published": base + timedelta(days=number)}
            for number in [3, 1, 5, 2, 4]
        ])
        for field in ["published", "_id"]:
            for direction in [1, -1]:
                with self.subTest(field=field, direction=direction):
                    expected = sorted(range(1, 6), reverse=direction < 0)
                    self.assertEqual([row["label"] for row in self.items.find({}).sort(field, direction)], expected)

    def test_binary_sort_orders_length_subtype_and_unsigned_bytes(self):
        values = {1: Binary(b"\xff", 128), 2: Binary(b"\xff\xff", 0),
                  3: Binary(b"\x00\x00", 128), 4: Binary(b"\xff\x00", 128),
                  5: Binary(b"\x00\x00\x00", 0)}
        self.items.insert_many([{"_id": label, "value": values[label]} for label in [4, 1, 5, 3, 2]])
        for direction in [1, -1]:
            self.assertEqual([row["_id"] for row in self.items.find({}).sort("value", direction)],
                             sorted(values, reverse=direction < 0))

    def test_mixed_timezone_and_compound_date_sorting(self):
        self.items.insert_many([
            {"_id": "late", "published": datetime(2026, 1, 1, 3, tzinfo=timezone.utc)},
            {"_id": "early", "published": datetime(2025, 12, 31, 21, tzinfo=timezone(timedelta(hours=-3)))},
            {"_id": "middle", "published": datetime(2026, 1, 1, 1)},
        ])
        for direction in [1, -1]:
            expected = ["early", "middle", "late"][::direction]
            self.assertEqual([row["_id"] for row in self.items.find({}).sort("published", direction)], expected)
        compound = self.client.app.compound
        compound.insert_many([
            {"_id": 1, "group": "a", "published": datetime(2026, 1, 2)},
            {"_id": 2, "group": "b", "published": datetime(2026, 1, 3)},
            {"_id": 3, "group": "a", "published": datetime(2026, 1, 3)},
            {"_id": 4, "group": "b", "published": datetime(2026, 1, 1)},
        ])
        self.assertEqual([row["_id"] for row in compound.find({}).sort([("group", 1), ("published", -1)])], [3, 1, 2, 4])

    def test_python_only_date_sort_inputs_reject_before_mutation(self):
        # Python date has no BSON type. Native cursors contain server BSON,
        # not arbitrary Python objects with warning-based fallback ordering.
        with self.assertRaises(InvalidDocument):
            self.items.insert_many([{"_id": 1, "published": datetime(2026, 1, 1)},
                                    {"_id": 2, "published": date(2026, 1, 2)}])
        self.assertEqual(self.client.app.list_collection_names(), [])
        self.assertEqual(self.items.count_documents({}), 0)


if __name__ == "__main__":
    unittest.main()
