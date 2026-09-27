"""Shared-client outcomes, with native SQLite rather than memory registries."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest

from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError

import briskdb


class UpstreamSharedStoreTests(unittest.TestCase):
    def test_retained_misses_indexes_and_preimages_observe_peer_changes(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as writer, briskdb.MongoClient(root) as reader:
                retained, unrelated = reader.app.items, reader.app.audit
                self.assertEqual(list(retained.find({"kind": "new"})), [])
                self.assertEqual(list(unrelated.find({})), [])
                source = {"_id": "later", "kind": "new", "name": "Ada", "count": 1,
                          "nested": {"count": 1}, "values": (1, 2)}
                writer.app.items.insert_one(source)
                source["nested"]["count"] = 99
                self.assertEqual(list(unrelated.find({})), [])
                found = retained.find_one({"kind": "new"})
                self.assertEqual(found["nested"], {"count": 1})
                self.assertEqual(found["values"], [1, 2])
                found["nested"]["count"] = 42
                retained.create_index("name")
                self.assertEqual(retained.find_one({"name": "Ada"})["nested"], {"count": 1})
                self.assertEqual(writer.app.items.update_one({"_id": "later"}, {"$set": {"name": "Grace", "count": 7}}).modified_count, 1)
                self.assertEqual(list(unrelated.find({})), [])
                self.assertIsNone(retained.find_one({"name": "Ada"}))
                self.assertEqual(retained.find_one({"name": "Grace"})["count"], 7)
                previous = retained.find_one_and_update({"_id": "later"}, {"$inc": {"count": 1}})
                self.assertEqual(previous["count"], 7)
                self.assertEqual(writer.app.items.find_one({"_id": "later"})["count"], 8)
                with self.assertRaises(InvalidDocument):
                    retained.insert_one({"_id": "unsupported", "value": {1, 2}})
                self.assertIsNone(retained.find_one({"_id": "unsupported"}))
                expected = list(retained.find({}))
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(list(reopened.app.items.find({})), expected)
                self.assertEqual(reopened.app.items.find_one({"name": "Grace"})["count"], 8)

    def test_concurrent_shared_clients_insert_all_rows_and_choose_one_duplicate_owner(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as reader:
                start = threading.Barrier(4, timeout=5)

                def insert_batch(worker):
                    with briskdb.MongoClient(root) as client:
                        start.wait()
                        records = [{"_id": f"{worker}-{number}", "worker": worker, "index": number} for number in range(20)]
                        return client.app.items.insert_many(records).inserted_ids

                with ThreadPoolExecutor(max_workers=4) as pool:
                    inserted = list(pool.map(insert_batch, range(4)))
                expected = {f"{worker}-{number}" for worker in range(4) for number in range(20)}
                self.assertEqual({identifier for batch in inserted for identifier in batch}, expected)
                self.assertEqual({row["_id"] for row in reader.app.items.find({})}, expected)
                race = threading.Barrier(4, timeout=5)

                def insert_duplicate(worker):
                    with briskdb.MongoClient(root) as client:
                        race.wait()
                        try:
                            client.app.items.insert_one({"_id": "same", "owner": worker})
                            return worker
                        except DuplicateKeyError as error:
                            self.assertEqual(error.code, 11000)
                            return None

                with ThreadPoolExecutor(max_workers=4) as pool:
                    outcomes = list(pool.map(insert_duplicate, range(4)))
                winners = [owner for owner in outcomes if owner is not None]
                self.assertEqual(len(winners), 1)
                self.assertEqual(reader.app.items.find_one({"_id": "same"}), {"_id": "same", "owner": winners[0]})
                self.assertEqual(reader.app.items.count_documents({}), 81)
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.count_documents({}), 81)
                self.assertEqual(reopened.app.items.find_one({"_id": "same"})["owner"], winners[0])

    def test_memory_selection_rejects_and_temporary_scopes_use_owned_sqlite(self):
        addresses = ["memory://named", "Memory://named", "memory://", "memory://nested/path",
                     "memory://name?option=1", "memory://name#x", "memory://two words",
                     "memroy://name", "mongodb://localhost"]
        with tempfile.TemporaryDirectory() as parent:
            absent = Path(parent) / "not-created"
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                for address in addresses:
                    with self.assertRaisesRegex(ValueError, "SQLite"):
                        client_class(address, folder=absent, backend="memory")
                    self.assertFalse(absent.exists())
            with self.assertRaisesRegex(ValueError, "SQLite"):
                briskdb.patch(folder=absent, backend="memory")
            self.assertFalse(absent.exists())
        with briskdb.patch(shards=2) as Outer:
            outer = Outer("memory://named", backend="memory")
            outer_root = outer.briskdb_path
            self.assertTrue((outer_root / "manifest.sqlite").is_file())
            outer.app.items.insert_one({"_id": "outer"})
            with briskdb.patch(shards=2) as Inner:
                inner = Inner("memory://named", backend="memory")
                inner_root = inner.briskdb_path
                self.assertNotEqual(inner_root, outer_root)
                self.assertTrue((inner_root / "manifest.sqlite").is_file())
                self.assertIsNone(inner.app.items.find_one({"_id": "outer"}))
            self.assertFalse(inner_root.exists())
            self.assertEqual(outer.app.items.find_one({"_id": "outer"}), {"_id": "outer"})
        self.assertFalse(outer_root.exists())


if __name__ == "__main__":
    unittest.main()
