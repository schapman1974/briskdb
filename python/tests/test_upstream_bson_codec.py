"""Native BSON scenarios; TinyMongo's private JSON format is not a wire codec."""

from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import tempfile
import unittest
from uuid import UUID

from bson import BSON, Binary, Code, Decimal128, ObjectId, Regex
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import InvalidDocument, InvalidStringData
from pymongo.errors import InvalidDocument as DriverInvalidDocument, OperationFailure, PyMongoError

import briskdb


class VerboseUnsupported:
    def __repr__(self):
        return "unsupported-" + "x" * 500


class UpstreamBsonCodecTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_nested_ids_dates_and_requested_timezone_survive_reopen(self):
        eastern = timezone(timedelta(hours=-4))
        created = datetime(2026, 7, 19, 9, 30, 45, 123456, tzinfo=eastern)
        document = {"_id": ObjectId(), "created": created,
                    "nested": {"owner_id": ObjectId(), "history": [{"at": created - timedelta(days=1)}, ObjectId()]}}
        expected = BSON(BSON.encode(document)).decode()
        self.items.insert_one(document)
        self.assertEqual(self.items.find_one({"created": created}), expected)
        self.assertEqual(expected["created"], datetime(2026, 7, 19, 13, 30, 45, 123000))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.find_one({"_id": document["_id"]}), expected)
            aware = reader.app.items.with_options(codec_options=CodecOptions(tz_aware=True, tzinfo=eastern))
            result = aware.find_one({"_id": document["_id"]})
            self.assertEqual(result["created"], created.replace(microsecond=123000))
            self.assertEqual(result["created"].utcoffset(), timedelta(hours=-4))
            self.assertEqual(result["nested"]["history"][0]["at"], (created - timedelta(days=1)).replace(microsecond=123000))
        self.assertTrue((Path(self.root.name) / "manifest.sqlite").is_file())
        self.assertFalse((Path(self.root.name) / "app.json").exists())

    def test_decimal_bid_binary_subtypes_and_uuid_options_preserve_wire_values(self):
        decimals = [Decimal128(value) for value in ["1234567890.00100", "-0", "NaN", "sNaN", "Infinity"]]
        binaries = [Binary(bytes(range(16)) if subtype in (3, 4) else b"\x01payload", subtype)
                    for subtype in [0, 3, 4, 128]]
        for number, value in enumerate(decimals + binaries):
            self.items.insert_one({"_id": number, "value": value})
            actual = self.items.find_one({"_id": number})["value"]
            self.assertEqual(BSON.encode({"value": actual}), BSON.encode({"value": value}))
            if isinstance(value, Decimal128):
                self.assertEqual(actual.bid, value.bid)
            elif value.subtype == 0:
                self.assertIs(type(actual), bytes)
            else:
                self.assertEqual(actual.subtype, value.subtype)
        value = UUID("00112233-4455-6677-8899-aabbccddeeff")
        standard = self.items.with_options(codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD))
        standard.insert_one({"_id": "uuid", "value": value})
        self.assertEqual(standard.find_one({"value": value})["value"], value)
        self.assertEqual(self.items.find_one({"_id": "uuid"})["value"], Binary(value.bytes, 4))

    def test_regex_representation_and_encoding_errors_follow_the_driver(self):
        values = [re.compile("Ab.c", re.I | re.M), re.compile(b"bytes", re.I),
                  Regex("Ab.c", "im"), Regex(b"bytes", "i")]
        for number, value in enumerate(values):
            self.items.insert_one({"_id": number, "value": value})
            actual = self.items.find_one({"_id": number})["value"]
            self.assertIs(type(actual), Regex)
            self.assertIs(type(actual.pattern), str)
            self.assertEqual(actual.pattern, value.pattern.decode() if isinstance(value.pattern, bytes) else value.pattern)
            self.assertEqual(actual.flags, value.flags)
        for value, error in [(Regex("nul\x00pattern"), InvalidDocument), (Regex(b"\xff"), InvalidStringData)]:
            with self.assertRaises(error):
                self.items.insert_one({"_id": "invalid", "value": value})
            self.assertEqual(self.items.count_documents({}), 4)

    def test_opaque_uuid_values_remain_queryable_and_unreadable_replies_are_bounded(self):
        for subtype in [3, 4]:
            for number, wrap in enumerate([lambda value: value, lambda value: [{"nested": value}],
                                           lambda value: Code("return value;", {"value": value})]):
                opaque = wrap(Binary(b"\x01payload", subtype))
                key = f"{subtype}-{number}"
                self.items.insert_one({"_id": key, "value": opaque})
                with self.assertRaises(OperationFailure) as caught:
                    self.items.find_one({"_id": key})
                self.assertEqual(caught.exception.code, 22)
                self.assertEqual(caught.exception.details["codeName"], "InvalidBSON")
                self.assertEqual(caught.exception.details["errmsg"],
                                 "reply contains a UUID binary value without a 16-byte payload")
                self.assertEqual(self.items.find_one({"value": opaque}, {"_id": 1}), {"_id": key})
                self.assertEqual(list(self.items.find({"_id": key}, {"_id": 1}).sort("value")), [{"_id": key}])
        with self.assertRaises(OperationFailure) as caught:
            self.items.distinct("value")
        self.assertEqual(caught.exception.code, 22)
        # A reply error is not a transaction rollback: find-and-modify may have
        # committed before its returned pre-image reaches the wire encoder.
        with self.assertRaises(OperationFailure) as caught:
            self.items.find_one_and_update({"_id": "3-0"}, {"$set": {"checked": True}})
        self.assertEqual(caught.exception.code, 22)
        self.assertEqual(self.items.find_one({"_id": "3-0"}, {"value": 0}), {"_id": "3-0", "checked": True})
        self.assertEqual(self.items.find_one_and_update(
            {"_id": "3-0"}, {"$set": {"checked": "projected"}}, projection={"value": 0}),
            {"_id": "3-0", "checked": True})
        self.assertEqual(self.items.find_one({"_id": "3-0"}, {"value": 0}), {"_id": "3-0", "checked": "projected"})
        self.assertEqual(self.items.update_many({}, {"$unset": {"value": ""}}).modified_count, 6)
        self.assertEqual(len(list(self.items.find({}))), 6)
        self.items.insert_one({"_id": "healthy", "value": Binary(bytes(range(16)), 3)})
        self.assertEqual(self.items.find_one({"_id": "healthy"})["value"], Binary(bytes(range(16)), 3))

    def test_json_tag_shapes_and_legacy_plain_json_remain_literal_bson(self):
        kinds = ["datetime", "objectid", "binary", "code", "decimal128", "maxkey", "minkey",
                 "timestamp", "uuid", "regex", "mapping", "future-type"]
        tags = [(kind, "2026-01-02T03:04:05") for kind in kinds]
        tags += [("datetime", "2026-07-19T09:30:45.123999-04:00"), ("future-type", {"nested": [1, 2]}),
                 ("float", "not-a-float")]
        tags += [("decimal128", value) for value in [None, 1, "short", "z" * 32, "00" * 17]]
        tags += [("uuid", value) for value in [None, 1, "00112233445566778899aabbccddeeff",
                                               "00112233-4455-6677-8899-aabbccddeezz", "{00112233-4455-6677-8899-aabbccddeeff}"]]
        tags += [("mapping", value) for value in ["not-an-item-list", [["missing-value"]]]]
        tags += [("binary", value) for value in ["not-a-mapping", {"base64": "AAE="},
                                                 {"base64": 123, "subtype": 0}, {"base64": "not base64!", "subtype": 0},
                                                 {"base64": "AAE=", "subtype": True}, {"base64": "AAE=", "subtype": 256}]]
        regex_payload = {"pattern": "x", "flags": 0, "representation": "python", "pattern_type": "string"}
        malformed_regex = [None, "not-a-mapping", {"pattern": "x", "flags": 0}]
        malformed_regex += [dict(regex_payload, **change) for change in [
            {"pattern": 1}, {"flags": True}, {"pattern": "x\x00y"}, {"representation": "future"},
            {"pattern_type": "future"}, {"flags": int(re.LOCALE)}]]
        tags += [("regex", value) for value in malformed_regex]
        documents = [{"_id": number, "value": {"__tinymongo_type_v1__": kind, "value": value}}
                     for number, (kind, value) in enumerate(tags)]
        self.items.insert_many(documents)
        self.assertEqual(list(self.items.find({}).sort("_id")), documents)
        with self.assertRaises(OverflowError):
            self.items.insert_one({"_id": "unencodable-tag", "value": {"__tinymongo_type_v1__": "regex",
                                  "value": dict(regex_payload, flags=1 << 100)}})
        self.assertEqual(self.items.count_documents({}), len(documents))
        legacy = json.loads('{"_id": "legacy", "nested": {"active": true}, "items": [null, "x"]}')
        self.items.insert_one(legacy)
        self.assertEqual(self.items.find_one({"_id": "legacy"}), legacy)

    def test_large_binary_document_reopens_and_bytearray_requires_conversion(self):
        binary_id = b"binary-id"
        uuid_binary = Binary(bytes(range(16)), 4)
        blob = b"\x89PNG\r\n\x1a\n" + b"x" * 100_000
        document = {"_id": binary_id, "blob": blob, "mutable": bytearray(b"abc"), "nested": {"tokens": [uuid_binary]}}
        with self.assertRaises(InvalidDocument):
            self.items.insert_one(document)
        self.assertEqual(self.client.app.list_collection_names(), [])
        document["mutable"] = bytes(document["mutable"])
        self.items.insert_one(document)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            actual = reader.app.items.find_one({"nested.tokens": uuid_binary})
            self.assertEqual(actual, document)
            self.assertIs(type(actual["_id"]), bytes)
            self.assertIs(type(actual["mutable"]), bytes)
            self.assertEqual(actual["nested"]["tokens"][0].subtype, 4)

    def test_invalid_document_keeps_the_pinned_driver_exception_contract(self):
        for value in [{1, 2}, VerboseUnsupported()]:
            document = {"_id": "broken", "outer": {"items": [{"valid": 1}, {"unsupported": value}]}}
            with self.assertRaises(InvalidDocument) as caught:
                self.items.insert_one(document)
            self.assertIsInstance(caught.exception, DriverInvalidDocument)
            self.assertNotIsInstance(caught.exception, PyMongoError)
            self.assertNotIsInstance(caught.exception, briskdb.BriskDBError)
            self.assertNotIsInstance(caught.exception, TypeError)
            if isinstance(value, VerboseUnsupported):
                self.assertIn("x" * 200, str(caught.exception))
            self.assertEqual(self.client.app.list_collection_names(), [])
        # TinyMongo's additional error inheritance, path/context metadata and
        # short offending-value repr are not provided by the pinned real driver.

    def test_nonfinite_doubles_and_legacy_binary_length_order_use_native_bson(self):
        values = [float("nan"), float("inf"), float("-inf")]
        self.items.insert_many([{"_id": number, "value": value} for number, value in enumerate(values)])
        for number, expected in enumerate(values):
            actual = self.items.find_one({"_id": number})["value"]
            self.assertTrue(math.isnan(actual) if math.isnan(expected) else actual == expected)
        binaries = [b"x", Binary(b"x", 128), b"xx", Binary(b"x", 2)]
        collection = self.client.app.binary_order
        collection.insert_many([{"_id": number, "value": value} for number, value in reversed(list(enumerate(binaries)))])
        self.assertEqual([row["_id"] for row in collection.find({}).sort("value")], [0, 1, 2, 3])
        self.assertEqual([row["_id"] for row in collection.find({}).sort("value", -1)], [3, 2, 1, 0])


if __name__ == "__main__":
    unittest.main()
