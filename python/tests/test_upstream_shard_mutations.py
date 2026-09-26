"""Real routed mutation outcomes and explicit per-document insert semantics."""

import tempfile
import unittest

from pymongo.errors import BulkWriteError, DuplicateKeyError

import briskdb


class UpstreamShardMutationTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        with briskdb.open(self.root.name, shards=2, documents=True) as database:
            with database.session() as session:
                session.create_collection("app", "items")
                identifiers = {}
                for identifier in range(128):
                    route = session.find("app", "items", {"_id": identifier})["plan"]["shards"][0]
                    identifiers.setdefault(route, identifier)
                    if len(identifiers) == 2:
                        break
                self.assertEqual(set(identifiers), {0, 1})
                # Natural first is deliberately on the physically higher shard.
                self.first, self.second = identifiers[1], identifiers[0]
        self.client = briskdb.MongoClient(self.root.name)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def counts(self, result, matched, modified):
        self.assertEqual((result.matched_count, result.modified_count), (matched, modified))

    def test_cross_shard_unique_updates_noops_and_replacement_preserve_owners(self):
        first = {"_id": self.first, "email": "first@example.test", "group": "selected", "state": "done"}
        second = {"_id": self.second, "email": "second@example.test", "group": "selected", "state": "pending"}
        self.items.create_index("email", unique=True)
        self.items.insert_many([first, second])
        self.counts(self.items.update_many({"group": "missing"}, {"$set": {"checked": True}}), 0, 0)
        self.counts(self.items.update_one({"_id": self.first}, {"$set": {"email": "first@example.test"}}), 1, 0)
        self.counts(self.items.update_one({"group": "selected"}, {"$set": {"state": "done"}}), 1, 0)
        self.assertEqual(self.items.find_one({"_id": self.second}), second)
        self.counts(self.items.update_one({"group": "selected"}, {"$set": {"checked": True}}), 1, 1)
        self.assertNotIn("checked", self.items.find_one({"_id": self.second}))
        replacement = {"_id": self.first, "email": "replacement@example.test", "kind": "replacement"}
        self.counts(self.items.replace_one({"_id": self.first}, replacement), 1, 1)
        with self.assertRaises(DuplicateKeyError) as caught:
            self.items.replace_one({"_id": self.first}, {"_id": self.first, "email": "second@example.test"})
        self.assertEqual(caught.exception.code, 11000)
        self.assertEqual(list(self.items.find({})), [replacement, second])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(list(reader.app.items.find({})), [replacement, second])

    def test_natural_first_targeted_updates_and_grouped_deletes_keep_exact_counts(self):
        self.items.insert_many([{"_id": self.first, "value": 0}, {"_id": self.second, "value": 0}])
        self.counts(self.items.update_one({}, {"$set": {"first_update": True}}), 1, 1)
        self.assertNotIn("first_update", self.items.find_one({"_id": self.second}))
        remaining = {"first_update": {"$exists": False}}
        self.counts(self.items.update_one(remaining, {"$set": {"routed_update": True}}), 1, 1)
        self.counts(self.items.update_one(remaining, {"$set": {"second_update": True}}), 1, 1)
        self.counts(self.items.update_many({"value": 0}, {"$set": {"all_updated": True}}), 2, 2)
        self.counts(self.items.update_many({"all_updated": True}, {"$inc": {"value": 1}}), 2, 2)
        for condition in [self.second, {"$eq": self.second}]:
            self.counts(self.items.update_one({"_id": condition}, {"$inc": {"value": 1}}), 1, 1)
        self.counts(self.items.replace_one({"_id": self.first}, {"_id": self.first, "value": 9}), 1, 1)
        self.counts(self.items.update_many({"_id": {"$in": []}}, {"$set": {"value": 99}}), 0, 0)
        self.assertEqual(self.items.delete_many({"_id": {"$in": []}}).deleted_count, 0)
        self.client.close()
        with briskdb.open(self.root.name, documents=True) as database:
            with database.session() as session:
                result = session.update_one("app", "items", {"_id": {"$eq": self.second}}, {"$inc": {"value": 1}})
                self.assertEqual((result["matched_count"], result["modified_count"]), (1, 1))
                self.assertEqual(result["plan"]["kind"], "point")
                self.assertEqual(result["plan"]["shards"], [0])
        with briskdb.MongoClient(self.root.name) as reader:
            items = reader.app.items
            self.assertEqual(list(items.find({})), [
                {"_id": self.first, "value": 9},
                {"_id": self.second, "value": 4, "routed_update": True, "second_update": True, "all_updated": True},
            ])
            self.assertEqual(items.delete_many({"_id": {"$in": [self.first, self.second]}}).deleted_count, 2)
            self.assertEqual(items.count_documents({}), 0)
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(list(reader.app.items.find({})), [])

    def test_public_empty_and_duplicate_batches_keep_per_document_commit_contract(self):
        with self.assertRaises(TypeError):
            self.items.insert_many([])
        self.assertEqual(self.items.count_documents({}), 0)
        for ordered, expected in [(True, [self.first]), (False, [self.first, self.second])]:
            incoming = [{"_id": self.first, "value": 1}, {"_id": self.first, "value": 2}, {"_id": self.second, "value": 3}]
            with self.assertRaises(BulkWriteError) as caught:
                self.items.insert_many(incoming, ordered=ordered, bypass_document_validation=True)
            self.assertEqual(caught.exception.details["nInserted"], len(expected))
            self.assertEqual([(error["index"], error["code"]) for error in caught.exception.details["writeErrors"]], [(1, 11000)])
            self.assertIs(caught.exception.details["writeErrors"][0]["op"], incoming[1])
            self.assertEqual([row["_id"] for row in self.items.find({})], expected)
            self.assertEqual(self.items.find_one({"_id": self.first}), incoming[0])
            self.assertEqual(self.items.delete_many({}).deleted_count, len(expected))


if __name__ == "__main__":
    unittest.main()
