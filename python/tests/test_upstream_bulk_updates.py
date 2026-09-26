"""Bulk-update counts, failure boundaries and shared-root client contention."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from bson import BSON, Decimal128
from pymongo.errors import DuplicateKeyError, OperationFailure, WriteError

import briskdb


class UpstreamBulkUpdateTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=4)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def counts(self, result, matched, modified):
        self.assertEqual((result.matched_count, result.modified_count), (matched, modified))

    def test_bulk_counts_natural_first_noop_and_upsert_keep_public_result_shapes(self):
        self.items.insert_many([{"_id": 1, "group": "selected", "count": 1},
                                {"_id": 2, "group": "selected", "count": 2},
                                {"_id": 3, "group": "other", "count": 3}])
        self.counts(self.items.update_many({"group": "selected"}, {"$inc": {"count": 10}}), 2, 2)
        self.assertEqual([row["count"] for row in self.items.find({}).sort("_id")], [11, 12, 3])
        for indexed in [False, True]:
            collection = self.client.app[f"first_{indexed}"]
            collection.insert_many([{"_id": "array-first", "group": ["selected"], "state": "done"},
                                     {"_id": "scalar-second", "group": "selected", "state": "pending"}])
            if indexed:
                collection.create_index("group")
            self.counts(collection.update_one({"group": "selected"}, {"$set": {"state": "done"}}), 1, 0)
            self.assertEqual(collection.find_one({"_id": "scalar-second"})["state"], "pending")
            self.counts(collection.update_many({"group": "selected"}, {"$set": {"state": "done"}}), 2, 1)
        result = self.items.update_one({"_id": "new", "group": "selected"}, {"$set": {"state": "created"}}, upsert=True)
        self.counts(result, 0, 0)
        self.assertEqual(result.upserted_id, "new")
        self.assertEqual(self.items.find_one({"_id": "new"}), {"_id": "new", "group": "selected", "state": "created"})

    def test_indexed_bulk_update_distinguishes_boolean_and_numeric_array_members(self):
        self.items.insert_many([{"_id": "array-bool", "flag": [True]}, {"_id": "scalar-bool", "flag": True},
                                {"_id": "scalar-number", "flag": 1}, {"_id": "array-number", "flag": [1]},
                                {"_id": "other", "flag": False}])
        self.items.create_index("flag")
        self.counts(self.items.update_many({"flag": True}, {"$set": {"matched": True}}), 2, 2)
        self.assertEqual({row["_id"] for row in self.items.find({"matched": True})}, {"array-bool", "scalar-bool"})
        self.counts(self.items.update_many({"flag": 1}, {"$set": {"numeric": True}}), 2, 2)
        self.assertEqual({row["_id"] for row in self.items.find({"numeric": True})}, {"array-number", "scalar-number"})
        self.assertEqual(self.items.find_one({"_id": "other"}), {"_id": "other", "flag": False})

    def test_unsupported_integer_filter_rejects_before_writes_and_safe_candidates_match(self):
        self.items.insert_many([{"_id": "number", "value": 1}, {"_id": "object", "value": {"nested": 1}}])
        self.items.create_index("value")
        for query in [{"_id": {"missing": True}}, {"value": float("nan")}]:
            self.counts(self.items.update_one(query, {"$set": {"seen": True}}), 0, 0)
        with self.assertRaises(OverflowError):
            self.items.update_one({"value": 10 ** 100}, {"$set": {"seen": True}})
        self.assertEqual(self.items.count_documents({"seen": {"$exists": True}}), 0)
        self.counts(self.items.update_many({"value": 1}, {"$set": {"seen": True}}), 1, 1)
        self.assertEqual(self.items.find_one({"_id": "object"}), {"_id": "object", "value": {"nested": 1}})

    def test_bulk_failure_rolls_back_one_shard_but_preserves_prior_shard_commits(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            client.app.routing.insert_many([{"_id": number} for number in range(64)])
            connection = sqlite3.connect(str(Path(root) / "shards" / "0000.sqlite"))
            try:
                same_shard = [BSON(row[0]).decode()["_id"] for row in connection.execute(
                    "SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")]
            finally:
                connection.close()
            self.assertGreaterEqual(len(same_shard), 2)
            first, second = same_shard[:2]
            collection = client.app.items
            originals = [{"_id": first, "count": 1}, {"_id": second, "count": "not-a-number"}]
            collection.insert_many(originals)
            with self.assertRaises(WriteError) as caught:
                collection.update_many({}, {"$inc": {"count": 1}})
            self.assertEqual(caught.exception.code, 14)
            self.assertEqual(list(collection.find({}).sort("_id")), originals)
            unique = client.app.unique
            originals = [{"_id": first, "email": "one@example.test"}, {"_id": second, "email": "two@example.test"}]
            unique.insert_many(originals)
            unique.create_index("email", unique=True)
            with self.assertRaises(DuplicateKeyError) as caught:
                unique.update_many({}, {"$set": {"email": "shared@example.test"}})
            self.assertEqual(caught.exception.code, 11000)
            self.assertEqual(list(unique.find({}).sort("_id")), originals)
        documents = [{"_id": number, "count": 0} for number in range(64)]
        self.items.insert_many(documents)
        owners = []
        for shard in range(4):
            connection = sqlite3.connect(str(Path(self.root.name) / "shards" / f"{shard:04}.sqlite"))
            try:
                owners.append([BSON(row[0]).decode()["_id"] for row in connection.execute(
                    "SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")])
            finally:
                connection.close()
        self.assertTrue(all(owners))
        bad = owners[3][-1]
        self.items.update_one({"_id": bad}, {"$set": {"count": "not-a-number"}})
        with self.assertRaises(OperationFailure) as caught:
            self.items.update_many({}, {"$inc": {"count": 1}})
        self.assertEqual(caught.exception.code, 14)
        self.assertNotIsInstance(caught.exception, WriteError)
        for document in documents:
            document["count"] = "not-a-number" if document["_id"] == bad else (0 if document["_id"] in owners[3] else 1)
        self.assertEqual(list(self.items.find({}).sort("_id")), documents)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(list(reader.app.items.find({}).sort("_id")), documents)

    def test_quiet_decimal_nan_increment_reports_execution_without_changing_bid(self):
        value = Decimal128("NaN")
        self.items.insert_one({"_id": "nan", "amount": value})
        self.counts(self.items.update_one({"_id": "nan"}, {"$inc": {"amount": Decimal128("0")}}), 1, 1)
        self.assertEqual(self.items.find_one({"_id": "nan"})["amount"].bid, value.bid)

    def test_six_shared_root_clients_keep_every_concurrent_increment(self):
        self.items.insert_one({"_id": "counter", "count": 0})
        self.client.close()
        for _ in range(3):
            clients = []
            try:
                # Retain every client until every future completes, so a fast
                # close cannot hide insufficient shared listener capacity.
                for _ in range(6):
                    clients.append(briskdb.MongoClient(self.root.name, socketTimeoutMS=5000))
                start = threading.Barrier(6)

                def increment(client):
                    start.wait(timeout=5)
                    result = client.app.items.update_one({"_id": "counter"}, {"$inc": {"count": 1}})
                    return result.matched_count, result.modified_count

                with ThreadPoolExecutor(max_workers=6) as pool:
                    futures = [pool.submit(increment, client) for client in clients]
                    self.assertEqual([future.result(timeout=10) for future in futures], [(1, 1)] * 6)
            finally:
                for client in clients:
                    client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.find_one({"_id": "counter"}), {"_id": "counter", "count": 18})


if __name__ == "__main__":
    unittest.main()
