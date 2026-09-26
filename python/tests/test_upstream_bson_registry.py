"""Public BSON type-registry scenarios, not TinyMongo's private JSON codec."""

import asyncio
from datetime import datetime, timedelta, timezone
import re
import tempfile
import unittest
from uuid import UUID

from bson import BSON, Binary, Decimal128, ObjectId, Regex
from bson.errors import InvalidDocument
from pymongo.errors import BulkWriteError

import briskdb


class UpstreamBsonRegistryTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2, uuidRepresentation="standard")
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def ids(self, value):
        return sorted(row["_id"] for row in self.items.find({"value": {"$eq": value}}))

    def test_datetime_binary_sort_and_unordered_batch_use_real_bson(self):
        earlier, later = datetime(2026, 7, 29, 11, 30), datetime(2026, 7, 29, 12, 30)
        with self.assertRaises(InvalidDocument):
            self.items.insert_one({"_id": "mutable", "payload": bytearray(b"a")})
        self.assertEqual(self.client.app.list_collection_names(), [])
        result = self.items.insert_many([{"_id": "later", "created": later, "payload": b"b"},
                                         {"_id": "earlier", "created": earlier, "payload": b"a"}])
        self.assertEqual(result.inserted_ids, ["later", "earlier"])
        self.assertEqual(self.items.find_one({"payload": b"a"})["_id"], "earlier")
        self.assertEqual([row["_id"] for row in self.items.find({}).sort("created")], ["earlier", "later"])
        with self.assertRaises(BulkWriteError) as caught:
            self.items.insert_many([{"_id": "later"}, {"_id": "new"}], ordered=False)
        self.assertEqual(caught.exception.details["nInserted"], 1)
        self.assertIsNotNone(self.items.find_one({"_id": "new"}))

    def test_extended_types_roundtrip_as_native_bson_not_python_registry_tags(self):
        values = [datetime(2026, 7, 29, 12, 30), ObjectId("000000000000000000000001"),
                  Decimal128("19.950"), b"native", Binary(bytes(range(16)), 4),
                  UUID("00112233-4455-6677-8899-aabbccddeeff"), re.compile("native"), Regex("bson", "im")]
        for identifier, value in enumerate(values):
            document = {"_id": identifier, "value": value}
            self.items.insert_one(document)
            self.assertEqual(BSON.encode(self.items.find_one({"_id": identifier}), codec_options=self.items.codec_options),
                             BSON.encode(document, codec_options=self.items.codec_options))
        self.assertIsInstance(self.items.find_one({"_id": 6})["value"], Regex)

    def test_binary_and_numeric_equality_preserve_bson_subtypes(self):
        values = [b"same", Binary(b"same", 0), Binary(bytes(range(16)), 4), bytes(range(16)),
                  1, 1.0, Decimal128("1.00"), True, .1, Decimal128("0.1"), float("nan"), Decimal128("NaN")]
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(values)])
        for value, expected in [(b"same", [0, 1]), (Binary(bytes(range(16)), 4), [2]),
                                (bytes(range(16)), [3]), (1, [4, 5, 6]), (True, [7]),
                                (.1, [8]), (Decimal128("0.1"), [9]), (float("nan"), [10, 11])]:
            with self.subTest(value=value):
                self.assertEqual(self.ids(value), expected)

    def test_uuid_and_regex_identity_and_scalar_order_match_wire_values(self):
        value = UUID("00112233-4455-6677-8899-aabbccddeeff")
        self.items.insert_many([{"_id": 1, "value": value}, {"_id": 2, "value": Binary(value.bytes, 4)},
                                {"_id": 3, "value": re.compile("same", re.IGNORECASE)},
                                {"_id": 4, "value": Regex("same", "i")},
                                {"_id": 5, "value": Regex(b"same", "im")}])
        self.assertEqual(self.ids(value), [1, 2])
        self.assertEqual(self.ids(Regex("same", "iu")), [3])
        self.assertEqual(self.ids(Regex("same", "iz")), [4])
        self.assertEqual(self.ids(Regex("same", "im")), [5])
        ordered = [UUID("00112233-4455-6677-8899-aabbccddeefe"), value,
                   *[Regex("same", flags) for flags in ["", "i", "im", "iu", "u"]]]
        self.items.insert_one({"_id": 6, "values": list(reversed(ordered))})
        self.items.update_one({"_id": 6}, {"$push": {"values": {"$each": [], "$sort": 1}}})
        self.assertEqual(self.items.find_one({"_id": 6})["values"], ordered)
        self.items.insert_one({"_id": 7, "value": Regex("flags", "ilmsux")})
        self.assertEqual(self.items.find_one({"_id": 7})["value"].flags, Regex("flags", "ilmsux").flags)

    def test_nan_order_and_equal_sort_keys_preserve_explicit_input_order(self):
        values = [1.0, float("nan"), Decimal128("NaN"), -1.0, float("-inf"), 0.0, float("inf")]
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(values)])
        rows = list(self.items.aggregate([{"$sort": {"_id": 1}}, {"$sort": {"value": 1}}]))
        self.assertEqual([row["_id"] for row in rows], [1, 2, 4, 3, 5, 0, 6])
        for values in [[1, 1.0, Decimal128("1.00")],
                       [datetime(2026, 8, 2, 12, 0, 0, 123100), datetime(2026, 8, 2, 12, 0, 0, 123900)]]:
            self.items.delete_many({})
            self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(values)])
            for direction in [1, -1]:
                rows = list(self.items.aggregate([{"$sort": {"_id": 1}}, {"$sort": {"value": direction}}]))
                self.assertEqual([row["_id"] for row in rows], list(range(len(values))))

    def test_recursive_bson_equality_keeps_nested_bool_and_document_order(self):
        values = [{"items": [1, {"active": True}]}, {"items": [1.0, {"active": True}]},
                  {"items": [1.0, {"active": 1}]}, {"value": True}, {"value": 1}, 1,
                  {"first": 1, "second": 2}, {"second": 2, "first": 1}, ["value", 1]]
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(values)])
        self.assertEqual(self.ids(values[0]), [0, 1])
        for n in range(2, len(values)):
            # A scalar query also matches that scalar inside an array; whole
            # BSON identity is checked separately by the grouping below.
            self.assertEqual(self.ids(values[n]), [5, 8] if n == 5 else [n])
        groups = list(self.items.aggregate([{"$group": {"_id": "$value", "ids": {"$push": "$_id"}}}]))
        self.assertEqual(sorted(sorted(group["ids"]) for group in groups), [[0, 1], *[[n] for n in range(2, 9)]])
        for unsupported in [{1}, object()]:
            with self.assertRaises(InvalidDocument):
                self.ids(unsupported)

    def test_datetime_identity_uses_signed_utc_milliseconds(self):
        first = datetime(2026, 7, 29, 8, 30, 0, 123001, tzinfo=timezone(timedelta(hours=-4)))
        same = datetime(2026, 7, 29, 12, 30, 0, 123999, tzinfo=timezone.utc)
        before_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999)
        self.items.insert_many([{"_id": 1, "value": first}, {"_id": 2, "value": same},
                                {"_id": 3, "value": same.replace(microsecond=124000)},
                                {"_id": 4, "value": before_epoch}])
        self.assertEqual(self.ids(first), [1, 2])
        self.assertEqual(self.ids(same.replace(microsecond=124000)), [3])
        self.assertEqual(self.ids(datetime(1969, 12, 31, 23, 59, 59, 999000)), [4])
        self.assertEqual(self.items.find_one({"_id": 4})["value"], datetime(1969, 12, 31, 23, 59, 59, 999000))

    def test_json_tag_lookalikes_stay_documents_including_valid_tags(self):
        tags = [("datetime", "not-a-date"), ("datetime", 123), ("objectid", "too-short"),
                ("objectid", "z" * 24), ("objectid", "00 " * 8), ("objectid", 123),
                ("future-type", {"__tinymongo_type_v1__": "datetime", "value": "2026-07-29T12:30:00"}),
                ("datetime", "2026-07-29T08:30:00-04:00"), ("objectid", "000000000000000000000001")]
        for n, (kind, value) in enumerate(tags):
            document = {"_id": n, "value": {"__tinymongo_type_v1__": kind, "value": value}}
            self.items.insert_one(document)
            self.assertEqual(self.items.find_one({"_id": n}), document)


class AsyncUpstreamBsonRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_uuid_regex_survive_sync_and_async_reopen_with_required_driver(self):
        with tempfile.TemporaryDirectory() as root:
            value = UUID("00112233-4455-6677-8899-aabbccddeeff")
            expression = re.compile("native", re.IGNORECASE)
            def write_sync():
                with briskdb.MongoClient(root, shards=2, uuidRepresentation="standard") as client:
                    client.app.items.insert_many([{"_id": value, "value": expression}, {"_id": "text", "value": "NATIVE"}])
            await asyncio.to_thread(write_sync)
            async with briskdb.AsyncMongoClient(root, uuidRepresentation="standard") as client:
                restored = await client.app.items.find_one({"_id": value})
                self.assertIsInstance(restored["value"], Regex)
                self.assertEqual((restored["value"].pattern, restored["value"].flags), (expression.pattern, expression.flags))
                self.assertEqual({row["_id"] async for row in client.app.items.find({"value": expression})}, {value, "text"})
                await client.app.items.insert_one({"_id": "async", "value": "native"})
            def read_sync():
                with briskdb.MongoClient(root, uuidRepresentation="standard") as client:
                    return {row["_id"] for row in client.app.items.find({"value": expression})}
            self.assertEqual(await asyncio.to_thread(read_sync), {value, "text", "async"})


if __name__ == "__main__":
    unittest.main()
