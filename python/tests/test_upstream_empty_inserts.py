"""Observable empty-insert contracts, not TinyMongo's private planner traces."""

from collections import UserDict
import tempfile
import unittest

from bson import Timestamp
from pymongo.errors import BulkWriteError

import briskdb


class UpstreamEmptyInsertTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)

    def test_clean_empty_and_reused_collections_keep_rows_ids_and_caller_values(self):
        collection = self.client.app.create_collection("items")
        first = [{"_id": "one", "value": 1}, {"_id": "two", "value": 2}]
        self.assertEqual(collection.insert_many(first).inserted_ids, ["one", "two"])
        self.assertEqual(list(collection.find({}).sort("value")), first)
        self.assertEqual(collection.delete_many({}).deleted_count, 2)
        zero = Timestamp(0, 0)
        documents = [UserDict({"_id": "custom", "value": 1}),
                     {"_id": "stamp-one", "stamp": zero}, {"_id": "stamp-two", "stamp": zero}]
        self.assertEqual(collection.insert_many(documents).inserted_ids, ["custom", "stamp-one", "stamp-two"])
        self.assertEqual(documents[0], {"_id": "custom", "value": 1})
        self.assertEqual([row["stamp"] for row in documents[1:]], [zero, zero])
        first_stamp = collection.find_one({"_id": "stamp-one"})["stamp"]
        second_stamp = collection.find_one({"_id": "stamp-two"})["stamp"]
        self.assertGreater(first_stamp, zero)
        self.assertGreater(second_stamp, first_stamp)
        for key in ["later-one", "later-two"]:
            self.assertEqual(collection.insert_many([{"_id": key}]).inserted_ids, [key])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.count_documents({}), 5)
            self.assertEqual(reader.app.items.find_one({"_id": "custom"}), dict(documents[0]))
            self.assertEqual(reader.app.items.find_one({"_id": "stamp-one"})["stamp"], first_stamp)
            self.assertEqual(reader.app.items.find_one({"_id": "stamp-two"})["stamp"], second_stamp)

    def test_empty_duplicates_preserve_ordered_and_unordered_results_and_error_ops(self):
        for explicit in [False, True]:
            for ordered in [True, False]:
                name = f"duplicate_{explicit}_{ordered}"
                collection = self.client.app.create_collection(name) if explicit else self.client.app[name]
                documents = [{"_id": value} for value in ["first", "second", "first", "last"]]
                with self.assertRaises(BulkWriteError) as caught:
                    collection.insert_many(documents, ordered=ordered)
                details = caught.exception.details
                self.assertEqual(details["nInserted"], 2 if ordered else 3)
                self.assertEqual(details["writeConcernErrors"], [])
                self.assertEqual(len(details["writeErrors"]), 1)
                error = details["writeErrors"][0]
                self.assertEqual((error["index"], error["code"]), (2, 11000))
                self.assertIs(error["op"], documents[2])
                self.assertNotIn("keyValue", error)
                self.assertEqual({row["_id"] for row in collection.find({})},
                                 {"first", "second"} if ordered else {"first", "second", "last"})
                # A failed empty-batch attempt cannot leave hidden state which
                # prevents a subsequent healthy insert on the same collection.
                collection.insert_one({"_id": "healthy"})
                self.assertIsNotNone(collection.find_one({"_id": "healthy"}))

    def test_empty_unique_and_nonunique_indexes_enforce_their_actual_constraints(self):
        for unique in [True, False]:
            collection = self.client.app[f"indexed_{unique}"]
            collection.create_index("email", unique=unique)
            documents = [{"_id": "one", "email": "same@example.test"},
                         {"_id": "two", "email": "same@example.test"},
                         {"_id": "three", "email": "other@example.test"}]
            if unique:
                with self.assertRaises(BulkWriteError) as caught:
                    collection.insert_many(documents, ordered=False)
                details = caught.exception.details
                self.assertEqual(details["nInserted"], 2)
                self.assertEqual([(error["index"], error["code"]) for error in details["writeErrors"]], [(1, 11000)])
                self.assertIs(details["writeErrors"][0]["op"], documents[1])
                self.assertNotIn("same@example.test", details["writeErrors"][0]["errmsg"])
            else:
                self.assertEqual(collection.insert_many(documents).inserted_ids, ["one", "two", "three"])
            self.assertEqual(collection.count_documents({"email": "same@example.test"}), 1 if unique else 2)
            self.assertEqual(collection.count_documents({}), 2 if unique else 3)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.indexed_True.count_documents({}), 2)
            self.assertEqual(reader.app.indexed_False.count_documents({}), 3)


if __name__ == "__main__":
    unittest.main()
