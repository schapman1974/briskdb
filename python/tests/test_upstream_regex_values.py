"""Owned public scenarios from the locked UUID/regex source suite."""

import re
import tempfile
import unittest
from uuid import UUID

from bson import Binary, Regex
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import InvalidDocument, InvalidStringData
from pymongo.errors import DuplicateKeyError, OperationFailure

import briskdb


class UpstreamRegexValueTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def ids(self, value):
        return sorted(row["_id"] for row in self.items.find({"value": value}))

    def test_regex_predicates_and_explicit_equality_keep_distinct_meanings(self):
        stored = Regex("Ab.c", "i")
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(
            ["Abxc", ["no", "Abxc"], stored, Regex("Ab.c", "iu")])])
        self.assertEqual(self.ids({"$regex": "Ab.c", "$options": "i"}), [0, 1, 2])
        self.assertEqual(self.ids(stored), [0, 1, 2])
        self.assertEqual(self.ids({"$eq": stored}), [2])

    def test_regex_membership_all_not_and_empty_all(self):
        expression = Regex("^a", "i")
        self.items.insert_many([{"_id": n, "value": value} for n, value in enumerate(
            ["Alpha", "other", ["other", "Alpha"]])])
        for predicate, expected in [
            ({"$in": [expression]}, [0, 2]), ({"$nin": [expression]}, [1]),
            ({"$all": [expression]}, [0, 2]), ({"$all": []}, []),
            ({"$not": expression}, [1]),
        ]:
            with self.subTest(predicate=predicate):
                self.assertEqual(self.ids(predicate), expected)

    def test_regex_options_and_embedded_flag_conflicts_keep_error_codes(self):
        self.items.insert_one({"_id": 1, "value": "x"})
        self.assertEqual(self.ids({"$regex": "x", "$options": "imsxu"}), [1])
        self.assertEqual(self.ids({"$regex": Regex("x", 0), "$options": "i"}), [1])
        for options, code in [("l", 51108), ("z", 51108), (1, 2)]:
            with self.subTest(options=options), self.assertRaises(OperationFailure) as caught:
                self.ids({"$regex": "x", "$options": options})
            self.assertEqual(caught.exception.code, code)
        for expression in [Regex("x", "i"), re.compile("x", re.IGNORECASE)]:
            with self.assertRaises(OperationFailure) as caught:
                self.ids({"$regex": expression, "$options": "m"})
            self.assertEqual(caught.exception.code, 51075)

    def test_locale_flag_matches_stored_regex_identity_but_not_string_execution(self):
        expression = Regex("exact", "l")
        self.items.insert_many([{"_id": "regex", "value": expression}, {"_id": "string", "value": "exact"}])
        self.assertEqual(self.ids(expression), ["regex"])
        self.assertEqual(self.ids({"$regex": expression}), ["regex"])
        self.assertEqual(self.ids({"$eq": expression}), ["regex"])

    def test_malformed_regex_queries_reject_without_creating_a_collection(self):
        for query, code in [
            ({"$regex": 42}, 2), ({"$regex": b"bytes"}, 2), ({"$regex": "["}, 51091),
            ({"$regex": "nul\x00pattern"}, 2), (Regex("["), 51091),
            ({"$in": [Regex("[")]}, 51091), ({"$nin": [Regex("[")]}, 51091),
            ({"$all": [Regex("[")]}, 51091), ({"$not": Regex("[")}, 51091),
            ({"$not": {"$regex": None}}, 2),
            ({"$in": [{"$regex": "valid"}]}, 2), ({"$nin": [{"$regex": "valid"}]}, 2),
            ({"$all": [{"$regex": "valid"}]}, 2),
        ]:
            with self.subTest(query=query), self.assertRaises(OperationFailure) as caught:
                self.ids(query)
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(self.client.app.list_collection_names(), [])
        for expression, error in [(Regex("nul\x00pattern"), InvalidDocument),
                                   (Regex(b"\xff"), InvalidStringData)]:
            with self.assertRaises(error):
                self.ids(expression)
            self.assertEqual(self.client.app.list_collection_names(), [])

    def test_contextual_validation_distinguishes_literal_regex_from_predicates(self):
        for query, code in [
            (Regex("["), 51091), ({"$gt": Regex("[")}, 2), ({"$lte": re.compile("valid")}, 2),
            ({"$ne": Regex("[")}, 2), ({"$ne": {"nested": Regex("[")}}, None),
            ({"$eq": Regex("[")}, None), ({"$gte": {"nested": Regex("[")}}, None),
            ({"$in": [Regex("[")]}, 51091), ({"$nin": [{"nested": Regex("[")}]}, None),
            ({"$all": [Regex("[")]}, 51091), ({"$all": [[Regex("[")]]}, None),
            ({"$all": [{"$elemMatch": {"name": Regex("[")}}]}, 51091),
            ({"$not": Regex("[")}, 51091), ({"$elemMatch": {"name": Regex("[")}}, 51091),
            ({"$elemMatch": {"$or": [{"name": Regex("[")}] }}, 51091),
            ({"$elemMatch": {"$gt": {"nested": Regex("[")}}}, None),
            ({"nested": Regex("[")}, None),
        ]:
            with self.subTest(query=query):
                if code is None:
                    self.assertEqual(self.ids(query), [])
                else:
                    with self.assertRaises(OperationFailure) as caught:
                        self.ids(query)
                    self.assertEqual(caught.exception.code, code)
        with self.assertRaises(InvalidDocument):
            self.ids({"$gt": {"nested": Regex("nul\x00pattern")}})
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_full_wire_validation_does_not_expose_private_regex_only_shortcuts(self):
        self.assertEqual(list(self.items.find(None)), [])
        for predicate in [{"$in": Regex("[")}, {"$elemMatch": "not-a-document"}]:
            with self.assertRaises(OperationFailure) as caught:
                self.ids(predicate)
            self.assertEqual(caught.exception.code, 2)
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_native_unique_binary_keys_are_not_tinymongo_remote_sql_policy(self):
        items = self.items.with_options(codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD))
        value = UUID("00112233-4455-6677-8899-aabbccddeeff")
        items.create_index("value", unique=True)
        items.insert_one({"_id": 1, "value": value})
        with self.assertRaises(DuplicateKeyError) as caught:
            items.insert_one({"_id": 2, "value": Binary(value.bytes, 4)})
        self.assertEqual(caught.exception.code, 11000)
        self.assertEqual(items.find_one({"_id": 1})["value"], value)
        self.assertIsNone(items.find_one({"_id": 2}))

    def test_distinct_deduplicates_recursive_numeric_identity_but_rejects_non_bson_objects(self):
        class EqualValue:
            def __eq__(self, other):
                return True

        class OtherEqualValue:
            def __eq__(self, other):
                return True

        for value in [EqualValue(), EqualValue(), OtherEqualValue()]:
            with self.assertRaises(InvalidDocument):
                self.items.insert_one({"value": value})
        self.items.insert_many([{"_id": n, "value": {"number": value}}
                                for n, value in enumerate([1, 1.0, True])])
        values = self.items.distinct("value")
        self.assertEqual(len(values), 2)
        self.assertEqual(sum(type(row["number"]) is bool for row in values), 1)
        self.assertTrue(all(row["number"] == 1 for row in values))
        self.assertEqual(self.items.count_documents({}), 3)


if __name__ == "__main__":
    unittest.main()
