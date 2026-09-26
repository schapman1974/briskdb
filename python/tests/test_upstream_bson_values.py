"""Owned BSON wire scenarios with explicit codec/clock/driver differences."""

from collections import OrderedDict
from datetime import datetime
import re
import tempfile
import unittest

from bson import BSON, Binary, Code, MaxKey, MinKey, ObjectId, Regex, Timestamp
from bson.errors import InvalidDocument
from pymongo.errors import BulkWriteError, OperationFailure, WriteError

import briskdb


class UpstreamBsonValueTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2, document_class=OrderedDict)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_zero_timestamp_stamping_preserves_id_nested_values_and_inputs(self):
        zero = Timestamp(0, 0)
        original = {"_id": zero, "stamp": zero, "near": Timestamp(0, 1), "nested": {"stamp": zero}}
        before = BSON.encode(original)
        self.items.insert_one(original)
        result = self.items.find_one({"_id": zero})
        self.assertEqual(BSON.encode(original), before)
        self.assertEqual((result["_id"], result["near"], result["nested"]["stamp"]), (zero, Timestamp(0, 1), zero))
        self.assertGreater(result["stamp"], zero)
        self.items.insert_one({"_id": "next", "stamp": zero})
        self.assertGreater(self.items.find_one({"_id": "next"})["stamp"], result["stamp"])

    def test_batch_timestamps_preserve_error_inputs_with_explicit_reservation_gaps(self):
        for ordered in [True, False]:
            collection = self.client.app["ordered" if ordered else "unordered"]
            collection.insert_one({"_id": "duplicate"})
            documents = [{"_id": value, "stamp": Timestamp(0, 0)}
                         for value in ["accepted", "duplicate", "continued"]]
            with self.assertRaises(BulkWriteError) as caught:
                collection.insert_many(documents, ordered=ordered)
            self.assertEqual(caught.exception.details["nInserted"], 1 if ordered else 2)
            self.assertIs(caught.exception.details["writeErrors"][0]["op"], documents[1])
            self.assertTrue(all(row["stamp"] == Timestamp(0, 0) for row in documents))
            collection.insert_one({"_id": "after", "stamp": Timestamp(0, 0)})
            first = collection.find_one({"_id": "accepted"})["stamp"]
            after = collection.find_one({"_id": "after"})["stamp"]
            self.assertGreater(after, first)
            # Native full-batch preflight reserves all three stamps. Unlike
            # TinyMongo's per-attempt clock, an unattempted ordered tail leaves
            # a gap. Never assert a fixed wall-clock second or gapless sequence.
            if after.time == first.time:
                self.assertEqual(after.inc - first.inc, 3)
            continued = collection.find_one({"_id": "continued"})
            if ordered:
                self.assertIsNone(continued)
            else:
                self.assertGreater(continued["stamp"], first)
                self.assertLess(continued["stamp"], after)

    def test_invalid_insert_never_reaches_timestamp_assignment_or_storage(self):
        self.items.insert_one({"_id": "before", "stamp": Timestamp(0, 0)})
        before = self.items.find_one({"_id": "before"})["stamp"]
        invalid = {"_id": "invalid", "stamp": Timestamp(0, 0), "bad": object()}
        with self.assertRaises(InvalidDocument):
            self.items.insert_one(invalid)
        self.items.insert_one({"_id": "after", "stamp": Timestamp(0, 0)})
        after = self.items.find_one({"_id": "after"})["stamp"]
        self.assertGreater(after, before)
        if after.time == before.time:
            self.assertEqual(after.inc - before.inc, 1)
        self.assertIsNone(self.items.find_one({"_id": "invalid"}))
        self.assertEqual(invalid["stamp"], Timestamp(0, 0))

    def test_native_bson_roundtrips_bounds_timestamps_and_nested_code(self):
        scope = OrderedDict(timestamp=Timestamp(1000, 3), bounds=[MinKey(), MaxKey()], nested={"code": Code("return inner;")})
        original = OrderedDict(_id=1, low=MinKey(), high=MaxKey(), stamp=Timestamp(1700000000, 17),
                               plain=Code("return value;"), scoped=Code("return nested;", scope))
        self.items.insert_one(original)
        result = self.items.find_one({"_id": 1})
        self.assertIs(type(result), OrderedDict)
        self.assertEqual(BSON.encode(result), BSON.encode(original))
        self.assertIs(type(result["plain"]), Code)
        self.assertIsNone(result["plain"].scope)
        self.assertIs(type(result["scoped"].scope["timestamp"]), Timestamp)
        self.assertIs(type(result["scoped"].scope["bounds"][0]), MinKey)
        self.assertIs(type(result["scoped"].scope["bounds"][1]), MaxKey)
        self.assertIs(type(result["scoped"].scope["nested"]["code"]), Code)

    def test_json_codec_tag_lookalikes_are_ordinary_native_bson_documents(self):
        cases = [("minkey", x) for x in [None, True, 0, 1.0]]
        cases += [("maxkey", x) for x in [None, True, 2]]
        cases += [("timestamp", x) for x in [None, {"time": 1}, {"time": 1, "inc": 2, "extra": 3},
                                             {"time": True, "inc": 0}, {"time": 0, "inc": False},
                                             {"time": -1, "inc": 0}, {"time": 2**32, "inc": 0},
                                             {"time": 0, "inc": 2**32}]]
        cases += [("code", x) for x in [None, {"code": "return 1;"},
                                        {"code": "return 1;", "scope": None, "extra": 1},
                                        {"code": 1, "scope": None}, {"code": "return 1;", "scope": []}]]
        documents = [{"_id": n, "value": {"__tinymongo_type_v1__": kind, "value": payload}}
                     for n, (kind, payload) in enumerate(cases)]
        self.items.insert_many(documents)
        self.assertEqual([BSON.encode(row) for row in self.items.find({}).sort("_id")],
                          [BSON.encode(row) for row in documents])

    def test_identity_distinguishes_timestamps_code_strings_scopes_and_scope_order(self):
        values = [Timestamp(1000, 2), Timestamp(1000, 3), MinKey(), MaxKey(), "same", Code("same"),
                  Code("same", {"answer": 42}), Code("same", {"a": 1, "b": 2}), Code("same", {"b": 2, "a": 1})]
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(values)])
        for n, value in enumerate(values):
            with self.subTest(index=n):
                self.assertEqual([row["_id"] for row in self.items.find({"value": {"$eq": value}})], [n])
        distinct = self.items.distinct("value")
        self.assertEqual({BSON.encode({"v": value}) for value in distinct},
                          {BSON.encode({"v": value}) for value in values})

    def test_whole_value_push_sort_uses_complete_bson_type_order(self):
        values = [MinKey(), None, 1, "text", {"value": 1}, [], Binary(b"x"),
                  ObjectId("000000000000000000000001"), False, datetime(2026, 8, 3, 12),
                  Timestamp(1000, 1), Regex("pattern"), Code("return 1;"),
                  Code("return scoped;", {"value": 1}), MaxKey()]
        self.items.insert_one({"_id": 1, "values": list(reversed(values))})
        self.items.update_one({"_id": 1}, {"$push": {"values": {"$each": [], "$sort": 1}}})
        result = self.items.find_one({"_id": 1})["values"]
        self.assertEqual(BSON.encode({"v": result}), BSON.encode({"v": values}))

    def test_native_regex_decodes_as_bson_regex_not_a_python_pattern(self):
        pattern = re.compile("native", re.IGNORECASE | re.MULTILINE)
        self.items.insert_one({"_id": 1, "nested": [pattern]})
        result = self.items.find_one({"_id": 1})["nested"][0]
        self.assertIs(type(result), Regex)
        self.assertEqual((result.pattern, result.flags), (pattern.pattern, pattern.flags))

    def test_code_is_not_a_string_for_commands_or_index_metadata(self):
        self.items.insert_one({"_id": 1, "source": 1, "value": 1})
        self.items.create_index("value", name="value_index")
        before = self.items.index_information()
        for operation, exception, code in [
            (lambda: self.items.update_one({"_id": 1}, {"$rename": {"source": Code("destination")}}), WriteError, 2),
            (lambda: list(self.items.aggregate([{"$count": Code("total")}])), OperationFailure, 40156),
            (lambda: self.items.distinct(Code("value")), OperationFailure, 14),
            (lambda: self.items.drop_index(Code("value_index")), OperationFailure, 14),
        ]:
            with self.assertRaises(exception) as caught:
                operation()
            self.assertEqual(caught.exception.code, code)
        for key in [Code("value"), [(Code("value"), 1)]]:
            with self.assertRaises(TypeError):
                self.items.create_index(key)
        with self.assertRaises(OperationFailure):
            self.items.create_index("source", name=Code("source_index"))
        self.assertEqual(self.items.index_information(), before)
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "source": 1, "value": 1})

    def test_code_cursor_sort_fields_fail_without_changing_records(self):
        self.items.insert_many([{"_id": "first", "value": 2}, {"_id": "second", "value": 1}])
        for operation in [lambda: self.items.find({}).sort(Code("value"), 1),
                          lambda: self.items.find({}).sort([(Code("value"), 1)]),
                          lambda: self.items.find({}, sort=[(Code("value"), 1)])]:
            with self.assertRaises(TypeError):
                operation()
        self.assertEqual({row["_id"] for row in self.items.find({})}, {"first", "second"})

    def test_invalid_scoped_code_is_rejected_before_any_write(self):
        document = {"_id": "broken", "outer": {"script": Code("return nested;", {"nested": [{"unsupported": {1, 2}}]})}}
        with self.assertRaises(InvalidDocument):
            self.items.insert_one(document)
        self.assertIsNone(self.items.find_one({"_id": "broken"}))
        self.assertEqual(document["outer"]["script"].scope["nested"][0]["unsupported"], {1, 2})


if __name__ == "__main__":
    unittest.main()
