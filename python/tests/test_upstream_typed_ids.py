"""BSON-aware ID contracts, independent of TinyMongo's private physical keys."""

from datetime import datetime, timedelta, timezone
import tempfile
import unittest

from bson import BSON, Binary, Decimal128, ObjectId
from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError

import briskdb


class UpstreamTypedIdTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_distinct_bson_ids_coexist_and_stay_addressable_after_reopen(self):
        raw = bytes(range(16))
        object_id = ObjectId("000000000000000000000001")
        identifiers = [raw, Binary(raw, 4), 1, True, object_id, str(object_id),
                       float("inf"), float("-inf"), "1"]
        documents = [{"_id": value, "label": str(number)} for number, value in enumerate(identifiers)]
        self.items.insert_many(documents)
        self.assertEqual(self.items.count_documents({}), len(documents))
        for document in documents:
            self.assertEqual(BSON.encode(self.items.find_one({"_id": {"$eq": document["_id"]}})),
                             BSON.encode(document))
        self.assertEqual(self.items.find_one({"_id": Binary(raw, 0)})["label"], "0")
        replacement = {"_id": Binary(raw, 4), "label": "updated"}
        self.assertEqual(self.items.replace_one({"_id": Binary(raw, 4)}, replacement).modified_count, 1)
        self.assertEqual(self.items.delete_one({"_id": raw}).deleted_count, 1)
        self.assertEqual(self.items.replace_one({"_id": "missing"}, {"_id": "missing"}).matched_count, 0)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            collection = reader.app.items
            self.assertIsNone(collection.find_one({"_id": raw}))
            self.assertEqual(collection.find_one({"_id": Binary(raw, 4)}), replacement)
            self.assertEqual(collection.count_documents({}), len(documents) - 1)
            for document in documents[2:]:
                self.assertEqual(BSON.encode(collection.find_one({"_id": {"$eq": document["_id"]}})),
                                 BSON.encode(document))

    def test_bson_aliases_share_unique_identity_and_keep_original_representation(self):
        instant = datetime(2026, 7, 29, 12, 0, 0, 999)
        aware = instant.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-4)))
        aliases = [(b"same", Binary(b"same", 0)), (1, 1.0), (1, Decimal128("1.00")),
                   (instant, aware), (float("nan"), float("nan")),
                   (float("nan"), Decimal128("NaN")), ([1, True], (1, True))]
        for number, (first, alias) in enumerate(aliases):
            with self.subTest(number=number):
                collection = self.client.app[f"alias_{number}"]
                original = {"_id": first, "label": "original"}
                collection.insert_one(original)
                with self.assertRaises(DuplicateKeyError) as caught:
                    collection.insert_one({"_id": alias, "label": "duplicate"})
                self.assertEqual(caught.exception.code, 11000)
                self.assertEqual(collection.count_documents({}), 1)
                self.assertEqual(BSON.encode(collection.find_one({"_id": {"$eq": alias}})), BSON.encode(original))
                updated = {"_id": first, "label": "updated"}
                self.assertEqual(collection.replace_one({"_id": {"$eq": alias}}, updated).modified_count, 1)
                self.assertEqual(BSON.encode(collection.find_one({"_id": {"$eq": first}})), BSON.encode(updated))
                self.assertEqual(collection.delete_one({"_id": {"$eq": alias}}).deleted_count, 1)
                self.assertEqual(collection.count_documents({}), 0)
        # TinyMongo accepts bytearray in private keys; real BSON needs bytes.
        with self.assertRaises(InvalidDocument):
            self.items.insert_one({"_id": bytearray(b"same")})
        self.assertNotIn("items", self.client.app.list_collection_names())

    def test_string_id_escaping_survives_point_mutations_and_reopen(self):
        identifiers = ["", 'quote-"', "line\nbreak", "nul-\0", "snowman-\N{SNOWMAN}"]
        self.items.insert_many([{"_id": value, "position": number} for number, value in enumerate(identifiers)])
        for number, value in enumerate(identifiers):
            self.assertEqual(self.items.find_one({"_id": {"$eq": value}}), {"_id": value, "position": number})
            self.assertEqual(self.items.update_one({"_id": value}, {"$set": {"visited": True}}).modified_count, 1)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            for number, value in enumerate(identifiers):
                self.assertEqual(reader.app.items.find_one({"_id": value}),
                                 {"_id": value, "position": number, "visited": True})
                self.assertEqual(reader.app.items.delete_one({"_id": value}).deleted_count, 1)
            self.assertEqual(reader.app.items.count_documents({}), 0)

    def test_numeric_string_and_boolean_ids_do_not_alias_in_logical_filters(self):
        self.items.insert_many([{"_id": 1.0, "label": "number"}, {"_id": "1", "label": "string"},
                                {"_id": True, "label": "boolean"}, {"_id": 2, "label": "second"}])
        self.assertEqual(self.items.find_one({"_id": {"$eq": 1}})["label"], "number")
        self.assertEqual(self.items.find_one({"_id": True})["label"], "boolean")
        self.assertEqual(sorted(row["label"] for row in self.items.find({"$or": [{"_id": 1}, {"_id": 2}]})),
                         ["number", "second"])
        self.assertEqual(sorted(row["label"] for row in self.items.find({"$nor": [{"_id": 1}]})),
                         ["boolean", "second", "string"])
        self.assertEqual(self.items.replace_one({"_id": 1}, {"_id": 1.0, "label": "updated"}).modified_count, 1)
        self.assertIs(type(self.items.find_one({"_id": 1})["_id"]), float)
        self.assertEqual(self.items.delete_one({"_id": 1}).deleted_count, 1)
        self.assertIsNone(self.items.find_one({"_id": 1.0}))
        self.assertEqual(self.items.find_one({"_id": "1"})["label"], "string")
        self.assertEqual(self.items.find_one({"_id": True})["label"], "boolean")


if __name__ == "__main__":
    unittest.main()
