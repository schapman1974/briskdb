"""Native point-read concurrency, ownership and WAL lifecycle outcomes."""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from pymongo.errors import InvalidOperation

import briskdb


CHECKPOINT = """
import json, pathlib, sqlite3, sys
root = pathlib.Path(sys.argv[1])
results = []
for relative in ['manifest.sqlite', 'shards/0000.sqlite', 'shards/0001.sqlite']:
    connection = sqlite3.connect(str(root / relative), timeout=3)
    try:
        results.append(connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
    finally:
        connection.close()
print(json.dumps(results))
"""


class UpstreamShardedPointLifecycleTests(unittest.TestCase):
    def test_four_concurrent_readers_keep_copies_isolated_and_observe_committed_updates(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2, maxPoolSize=4) as client:
            items = client.app.items
            items.insert_one({"_id": "target", "value": 0, "nested": {"items": [1, 2]}})
            for version in (0, 1):
                if version:
                    self.assertEqual(items.update_one({"_id": "target"}, {"$set": {"value": version}}).modified_count, 1)
                barrier = threading.Barrier(4, timeout=5)

                def read_repeatedly(_worker):
                    barrier.wait()
                    values = []
                    for _ in range(20):
                        row = items.find_one({"_id": "target"})
                        self.assertEqual(row["nested"]["items"], [1, 2])
                        row["nested"]["items"].append("client-only")
                        values.append(row["value"])
                    return values

                with ThreadPoolExecutor(max_workers=4) as pool:
                    self.assertEqual(list(pool.map(read_repeatedly, range(4))), [[version] * 20] * 4)
            self.assertEqual(items.find_one({"_id": "target"}),
                             {"_id": "target", "value": 1, "nested": {"items": [1, 2]}})

    def test_closed_client_cannot_revive_retained_collection_but_peer_and_reopen_work(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as first, briskdb.MongoClient(root) as peer:
                retained = first.app.items
                retained.insert_one({"_id": "target", "version": 0})
                self.assertEqual(retained.find_one({"_id": "target"})["version"], 0)
                first.close()
                with self.assertRaises(InvalidOperation):
                    retained.find_one({"_id": "target"})
                self.assertEqual(peer.app.items.update_one({"_id": "target"}, {"$inc": {"version": 1}}).modified_count, 1)
                self.assertEqual(peer.app.items.find_one({"_id": "target"}), {"_id": "target", "version": 1})
            with self.assertRaises(InvalidOperation):
                retained.find_one({"_id": "target"})
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.find_one({"_id": "target"}), {"_id": "target", "version": 1})

    def test_point_projection_typed_ids_fallback_and_nested_datetime_fidelity(self):
        stored = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone(timedelta(hours=-5)))
        expected = datetime(2026, 1, 2, 8, 4, 5, 123000, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(
                root, shards=2, document_class=OrderedDict, tz_aware=True) as client:
            items = client.app.items
            items.insert_many([{"_id": 1, "kind": "number", "secret": "hidden", "nested": {"when": stored, "items": [1, 2]}},
                               {"_id": True, "kind": "boolean", "nested": {"when": stored, "items": [3, 4]}},
                               {"_id": {"first": 1, "second": 2}, "kind": "container"}])
            self.assertEqual(items.find_one({"_id": 1.0}, {"kind": 1, "_id": 0}), {"kind": "number"})
            found = items.find_one({"_id": True}, {"secret": 0})
            self.assertIs(type(found), OrderedDict)
            self.assertIs(type(found["nested"]), OrderedDict)
            self.assertEqual(found["nested"]["when"], expected)
            found["nested"]["items"].append("client-only")
            self.assertEqual(items.find_one({"_id": True})["nested"]["items"], [3, 4])
            self.assertIsNone(items.find_one({"_id": "missing"}))
            self.assertIsNone(items.find_one({"_id": 1, "kind": "wrong"}))
            self.assertIsNone(items.find_one({"_id": {"$eq": {"other": 3}}}))
            self.assertEqual(items.find_one({"_id": {"$eq": {"first": 1.0, "second": 2.0}}})["kind"], "container")
            self.assertIs(items.find_one({"kind": "boolean"})["_id"], True)
            self.assertEqual(items.count_documents({"_id": "missing"}), 0)

    def test_warmed_readers_release_wal_snapshots_before_external_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                items.insert_one({"_id": "target", "value": 0})
                for version in (0, 1):
                    if version:
                        self.assertEqual(items.update_one({"_id": "target"}, {"$set": {"value": version}}).modified_count, 1)
                    for _ in range(25):
                        self.assertEqual(items.find_one({"_id": "target"})["value"], version)
                # Separate stdlib SQLite process: do not mix its POSIX lock
                # bookkeeping with the wheel's own SQLite library in-process.
                result = subprocess.run([sys.executable, "-c", CHECKPOINT, str(Path(root))],
                                        capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), [[0, 0, 0]] * 3)
                self.assertEqual(items.find_one({"_id": "target"}), {"_id": "target", "value": 1})
            with briskdb.MongoClient(root) as reader:
                self.assertEqual(reader.app.items.find_one({"_id": "target"}), {"_id": "target", "value": 1})


if __name__ == "__main__":
    unittest.main()
