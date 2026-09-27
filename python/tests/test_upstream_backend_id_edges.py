"""Public BSON identities and error outcomes, without foreign backend hooks."""

from datetime import datetime, timezone
import tempfile
import unittest

from bson import BSON
from pymongo.errors import DuplicateKeyError

import briskdb


class UpstreamBackendIdEdgeTests(unittest.TestCase):
    def test_duplicate_errors_and_missing_replacements_preserve_stored_rows(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                original = {"_id": 1, "value": "private-original-value"}
                items.insert_one(original)
                with self.assertRaises(OverflowError):
                    items.insert_one({"_id": 10 ** 400})
                with self.assertRaises(OverflowError):
                    list(items.find({"_id": 10 ** 400}))
                with self.assertRaises(DuplicateKeyError) as caught:
                    items.insert_one({"_id": 1.0, "value": "private-incoming-value"})
                self.assertEqual(caught.exception.code, 11000)
                for secret in [root, "private-original-value", "private-incoming-value"]:
                    self.assertNotIn(secret, str(caught.exception))
                for collection in [items, client.app.absent]:
                    result = collection.replace_one({"_id": "missing"}, {"_id": "missing", "value": "new"})
                    self.assertEqual((result.matched_count, result.modified_count, result.upserted_id), (0, 0, None))
                self.assertNotIn("absent", client.app.list_collection_names())
                self.assertEqual(list(items.find({})), [original])
            with briskdb.MongoClient(root) as reader:
                self.assertEqual(list(reader.app.items.find({})), [original])
                self.assertNotIn("absent", reader.app.list_collection_names())

    def test_nested_container_datetime_and_nonfinite_ids_keep_canonical_identity(self):
        aliases = [([1, {"b": 2}], (1.0, {"b": 2.0})),
                   ([{"nested": {"number": 1}}], [{"nested": {"number": 1.0}}]),
                   ((1, "two"), [1.0, "two"]),
                   (datetime(2026, 7, 29, 12, 30), datetime(2026, 7, 29, 12, 30, tzinfo=timezone.utc))]
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=4) as client:
                for number, (original, alias) in enumerate(aliases):
                    collection = client.app[f"alias_{number}"]
                    document = {"_id": original, "value": "original"}
                    collection.insert_one(document)
                    self.assertEqual(BSON.encode(collection.find_one({"_id": {"$eq": alias}})), BSON.encode(document))
                    with self.assertRaises(DuplicateKeyError) as caught:
                        collection.insert_one({"_id": alias, "value": "duplicate"})
                    self.assertEqual(caught.exception.code, 11000)
                    self.assertEqual(collection.count_documents({}), 1)
                nonfinite = [{"_id": float("inf"), "label": "positive"},
                             {"_id": float("-inf"), "label": "negative"},
                             {"_id": float("nan"), "label": "nan"}]
                client.app.nonfinite.insert_many(nonfinite)
                for document in nonfinite:
                    self.assertEqual(BSON.encode(client.app.nonfinite.find_one({"_id": {"$eq": document["_id"]}})), BSON.encode(document))
            with briskdb.MongoClient(root) as reader:
                for number, (original, alias) in enumerate(aliases):
                    self.assertEqual(BSON.encode(reader.app[f"alias_{number}"].find_one({"_id": {"$eq": alias}})),
                                     BSON.encode({"_id": original, "value": "original"}))
                self.assertEqual(reader.app.nonfinite.count_documents({}), 3)
                for document in nonfinite:
                    self.assertEqual(BSON.encode(reader.app.nonfinite.find_one({"_id": {"$eq": document["_id"]}})), BSON.encode(document))


if __name__ == "__main__":
    unittest.main()
