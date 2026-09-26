"""Native BSON equivalents of locked decimal and codec fast-path scenarios."""

from copy import deepcopy
import math
import tempfile
import unittest

from bson import BSON, Binary, Code, Decimal128
from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError, OperationFailure, WriteError

import briskdb


class UpstreamDecimalCodecTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_mixed_integer_double_sum_is_native_and_exact_for_binary_fractions(self):
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate([7, .5, .25])])
        self.assertEqual(list(self.items.aggregate([{"$group": {"_id": None, "total": {"$sum": "$value"}}}])),
                         [{"_id": None, "total": 7.75}])

    def test_numeric_updates_reject_invalid_operands_and_preserve_decimal_infinity(self):
        self.items.insert_one({"_id": 1, "value": Decimal128("1")})
        for operand in [True, "not-a-number"]:
            with self.assertRaises(WriteError):
                self.items.update_one({"_id": 1}, {"$inc": {"value": operand}})
            self.assertEqual(self.items.find_one({"_id": 1})["value"], Decimal128("1"))
        with self.assertRaises(InvalidDocument):
            self.items.update_one({"_id": 1}, {"$inc": {"value": object()}})
        self.items.update_one({"_id": 1}, {"$inc": {"value": float("inf")}})
        self.assertEqual(self.items.find_one({"_id": 1})["value"].to_decimal(), Decimal128("Infinity").to_decimal())
        self.items.insert_one({"_id": 2, "value": "not-a-number"})
        with self.assertRaises(WriteError):
            self.items.update_one({"_id": 2}, {"$inc": {"value": 1}})
        self.assertEqual(self.items.find_one({"_id": 2})["value"], "not-a-number")

    def test_unique_numeric_index_uses_exact_double_decimal_identity(self):
        self.items.create_index("value", unique=True)
        for identifier, stored, aliases in [
            (1, Decimal128("0.00"), [0, 0.0]), (2, Decimal128("0.5"), [.5]),
            (3, 2**60, [float(2**60), Decimal128(str(2**60))]),
            (4, 1e23, [Decimal128(str(int(1e23)))]),
        ]:
            self.items.insert_one({"_id": identifier, "value": stored})
            for alias in aliases:
                self.assertEqual(self.items.find_one({"value": alias})["_id"], identifier)
                with self.assertRaises(DuplicateKeyError) as caught:
                    self.items.insert_one({"_id": "duplicate", "value": alias})
                self.assertEqual(caught.exception.code, 11000)
        self.items.insert_one({"_id": 5, "value": Decimal128("1E+23")})
        self.assertEqual(self.items.find_one({"value": Decimal128("1E+23")})["_id"], 5)
        self.assertEqual(self.items.find_one({"value": 1e23})["_id"], 4)
        with self.assertRaises(OverflowError):
            self.items.find_one({"value": 10**23})
        with self.assertRaises(OperationFailure) as caught:
            self.items.insert_one({"_id": "nonfinite", "value": Decimal128("Infinity")})
        self.assertEqual(caught.exception.code, 115)
        self.assertEqual(self.items.count_documents({}), 5)

    def test_extreme_decimal_ids_retain_exact_identity_after_reopen(self):
        extreme = Decimal128("1E+6144")
        alias = Decimal128.from_bid(extreme.bid)
        self.items.insert_one({"_id": extreme, "value": "extreme"})
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": alias})
        self.assertEqual(self.items.find_one({"_id": alias}), {"_id": extreme, "value": "extreme"})
        with self.assertRaises(OverflowError):
            self.items.insert_one({"_id": 10**600})
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(reopened.app.items.find_one({"_id": alias}), {"_id": extreme, "value": "extreme"})
            self.assertEqual(reopened.app.items.count_documents({}), 1)

    def test_native_comparison_rejects_non_bson_and_pull_handles_nan_array(self):
        self.items.insert_one({"_id": 1, "values": [[float("nan"), 0], [2], 3]})
        with self.assertRaises(InvalidDocument):
            self.items.find_one({"value": object()})
        self.items.update_one({"_id": 1}, {"$pull": {"values": {"$lt": 1}}})
        self.assertEqual(self.items.find_one({"_id": 1})["values"], [[2], 3])

    def test_native_string_range_queries_update_and_replace(self):
        self.items.insert_one({"_id": 1, "name": "Ada", "state": "ready", "score": "middle"})
        for query in [{"name": "Ada", "state": "ready"}, {"score": {"$gte": "first"}},
                      {"score": {"$lte": "zulu"}}, {"score": {"$lt": "zulu"}}]:
            self.assertEqual(self.items.find_one(query)["_id"], 1)
        self.assertEqual(self.items.update_one({"score": {"$gte": "first"}}, {"$set": {"seen": True}}).matched_count, 1)
        self.assertEqual(self.items.replace_one({"state": {"$lt": "zulu"}}, {"name": "Grace", "state": "ready"}).matched_count, 1)
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "name": "Grace", "state": "ready"})

    def test_builtin_and_subclass_values_use_bson_without_tagged_json(self):
        class Text(str):
            pass
        class Number(int):
            pass
        class Double(float):
            pass
        class Document(dict):
            pass
        class Array(list):
            pass

        builtin = {"_id": 1, "null": None, "boolean": True, "integer": 42, "double": 3.5,
                   "text": "ordinary", "array": [1, "two", False], "tuple": (3, None),
                   "nested": {"value": 4}, "nonfinite": float("inf")}
        subclasses = Document(_id=2, text=Text("subclass"), number=Number(7), double=Double(2.5),
                              array=Array([Text("nested")]))
        for original in [builtin, subclasses]:
            before = deepcopy(original)
            self.items.insert_one(original)
            self.assertEqual(original, before)
            result = self.items.find_one({"_id": original["_id"]})
            self.assertEqual(result, BSON.encode(original).decode())
            self.assertIs(type(result), dict)
            self.assertIs(type(result["text"]), str)
        self.assertEqual(self.items.find_one({"_id": 1})["tuple"], [3, None])

    def test_native_code_and_binary_subclasses_keep_type_and_payload(self):
        document = {"_id": 1, "script": Code("return answer;", {"answer": 42}),
                    "binary": Binary(bytes(range(16)), subtype=4)}
        self.items.insert_one(document)
        result = self.items.find_one({"_id": 1})
        self.assertEqual(result, document)
        self.assertIs(type(result["script"]), Code)
        self.assertIs(type(result["binary"]), Binary)
        self.assertEqual(result["binary"].subtype, 4)

    def test_encoding_failures_leave_no_catalog_and_keep_caller_inputs(self):
        for document in [{"_id": 1, "outer": [{"unsupported": {1, 2}}]}, {"_id": 1, "value": object()}]:
            identity = id(document["outer"]) if "outer" in document else id(document["value"])
            with self.assertRaises(InvalidDocument):
                self.items.insert_one(document)
            self.assertEqual(id(document["outer"]) if "outer" in document else id(document["value"]), identity)
            self.assertEqual(self.client.app.list_collection_names(), [])
        self.items.insert_one({"_id": 1, "value": 1})
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "value": 1})

    def test_nonfinite_floats_and_float_subclasses_roundtrip_natively(self):
        class Double(float):
            pass

        for identifier, value in enumerate([float("nan"), float("inf"), float("-inf"), Double("inf")]):
            self.items.insert_one({"_id": identifier, "value": value})
            result = self.items.find_one({"_id": identifier})["value"]
            self.assertIs(type(result), float)
            if math.isnan(value):
                self.assertTrue(math.isnan(result))
            else:
                self.assertEqual(result, value)


if __name__ == "__main__":
    unittest.main()
