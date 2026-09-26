"""Unique-index update outcomes without TinyMongo's Python decode-count hooks."""

import tempfile
import unittest

from pymongo.errors import DuplicateKeyError

import briskdb


class UpstreamUniqueUpdateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def counts(self, result, matched, modified):
        self.assertEqual((result.matched_count, result.modified_count), (matched, modified))

    def duplicate_leaves(self, collection, identifier, update):
        before = collection.find_one({"_id": identifier})
        with self.assertRaises(DuplicateKeyError) as caught:
            collection.update_one({"_id": identifier}, update)
        self.assertEqual(caught.exception.code, 11000)
        self.assertNotIn("keyValue", caught.exception.details)
        self.assertEqual(collection.find_one({"_id": identifier}), before)

    def verify_reopen(self):
        expected = {name: list(self.client.app[name].find({}).sort("_id"))
                    for name in self.client.app.list_collection_names()}
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(set(reader.app.list_collection_names()), set(expected))
            for name, documents in expected.items():
                self.assertEqual(list(reader.app[name].find({}).sort("_id")), documents)

    def test_point_updates_noops_misses_and_scalar_conflicts_preserve_other_rows(self):
        for count, unique in [(100, False), (10, True), (100, True), (1000, True)]:
            collection = self.client.app[f"point_{count}_{unique}"]
            documents = [{"_id": number, "email": f"user-{number}@example.test", "visits": 0}
                         for number in range(count)]
            collection.insert_many(documents)
            if unique:
                collection.create_index("email", unique=True)
            target = 73 if count == 100 else count - 1
            self.counts(collection.update_one({"_id": target}, {"$inc": {"visits": 1}}), 1, 1)
            self.counts(collection.update_one({"_id": target}, {"$set": {"email": documents[target]["email"]}}), 1, 0)
            self.counts(collection.update_one({"_id": "missing"}, {"$set": {"seen": True}}), 0, 0)
            documents[target]["visits"] = 1
            if unique:
                self.counts(collection.update_one({"_id": target}, {"$set": {"email": "available@example.test"}}), 1, 1)
                documents[target]["email"] = "available@example.test"
                self.duplicate_leaves(collection, target, {"$set": {"email": "user-0@example.test", "visits": 99}})
            self.assertEqual(list(collection.find({}).sort("_id")), documents)
        self.verify_reopen()

    def test_reordered_multikey_entries_preserve_uniqueness_and_array_order(self):
        self.items.insert_many([{"_id": number, "tags": [f"tag-{number}", f"shared-{number}"]} for number in range(100)])
        self.items.create_index("tags", unique=True)
        changed = ["shared-73", "tag-73"]
        self.counts(self.items.update_one({"_id": 73}, {"$set": {"tags": changed}}), 1, 1)
        self.counts(self.items.update_one({"_id": 73}, {"$set": {"tags": changed}}), 1, 0)
        self.assertEqual(self.items.find_one({"tags": "tag-73"}), {"_id": 73, "tags": changed})
        self.duplicate_leaves(self.items, 73, {"$set": {"tags": ["shared-0", "tag-73"]}})
        self.assertEqual(self.items.find_one({"_id": 0})["tags"], ["tag-0", "shared-0"])
        self.verify_reopen()

    def test_sparse_unique_membership_transitions_roll_back_conflicts(self):
        self.items.create_index("email", unique=True, sparse=True)
        self.items.insert_many([{"_id": 1, "name": "outside"}, {"_id": 2, "email": "taken@example.test"}, {"_id": 3}])
        self.counts(self.items.update_one({"_id": 1}, {"$set": {"name": "renamed"}}), 1, 1)
        self.counts(self.items.update_one({"_id": 1}, {"$set": {"email": "available@example.test"}}), 1, 1)
        self.counts(self.items.update_one({"_id": 1}, {"$unset": {"email": ""}}), 1, 1)
        self.duplicate_leaves(self.items, 1, {"$set": {"email": "taken@example.test", "name": "must-roll-back"}})
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "name": "renamed"})
        self.counts(self.items.update_one({"_id": 3}, {"$set": {"email": "available@example.test"}}), 1, 1)
        self.assertEqual(self.items.find_one({"email": "available@example.test"})["_id"], 3)
        self.verify_reopen()

    def test_partial_unique_membership_can_transfer_ownership_after_a_rejected_entry(self):
        self.items.create_index("email", name="active_email", unique=True, partialFilterExpression={"active": True})
        self.items.insert_many([{"_id": 1, "email": "same@example.test", "active": True},
                                {"_id": 2, "email": "same@example.test", "active": False},
                                {"_id": 3, "email": "other@example.test", "active": False}])
        self.counts(self.items.update_one({"_id": 2}, {"$set": {"note": "still outside"}}), 1, 1)
        self.duplicate_leaves(self.items, 2, {"$set": {"active": True, "note": "must-roll-back"}})
        self.counts(self.items.update_one({"_id": 1}, {"$set": {"active": False}}), 1, 1)
        self.counts(self.items.update_one({"_id": 2}, {"$set": {"active": True}}), 1, 1)
        self.assertIs(self.items.find_one({"_id": 1})["active"], False)
        self.assertEqual(self.items.find_one({"_id": 2}),
                         {"_id": 2, "email": "same@example.test", "active": True, "note": "still outside"})
        self.verify_reopen()

    def test_parent_path_update_detects_compound_multikey_overlap_atomically(self):
        self.items.create_index([("owner.id", 1), ("labels", 1)], name="owner_labels", unique=True)
        self.items.insert_many([
            {"_id": 1, "owner": {"id": "north", "name": "Ada"}, "labels": ["red", "blue"]},
            {"_id": 2, "owner": {"id": "south", "name": "Grace"}, "labels": ["blue", "green"]},
            {"_id": 3, "owner": {"id": "west", "name": "Lin"}, "labels": ["yellow"]}])
        self.counts(self.items.update_one({"_id": 2}, {"$set": {"note": "unrelated"}}), 1, 1)
        self.duplicate_leaves(self.items, 2, {"$set": {"owner": {"id": "north", "name": "Grace"}, "note": "must-roll-back"}})
        self.assertEqual(self.items.find_one({"_id": 2}),
                         {"_id": 2, "owner": {"id": "south", "name": "Grace"}, "labels": ["blue", "green"], "note": "unrelated"})
        self.verify_reopen()

    def test_filtered_multi_update_keeps_unselected_rows_and_unrelated_unique_values(self):
        documents = [{"_id": number, "email": f"user-{number}@example.test",
                      "group": "selected" if number % 20 == 0 else "other", "visits": 0} for number in range(200)]
        self.items.insert_many(documents)
        self.items.create_index("email", unique=True)
        self.items.create_index("group")
        self.counts(self.items.update_many({"group": "selected"}, {"$inc": {"visits": 1}}), 10, 10)
        for document in documents:
            if document["group"] == "selected":
                document["visits"] = 1
        self.assertEqual(list(self.items.find({}).sort("_id")), documents)
        self.assertEqual(self.items.count_documents({"visits": 1}), 10)
        self.assertEqual(self.items.count_documents({"visits": 0}), 190)
        self.verify_reopen()


if __name__ == "__main__":
    unittest.main()
