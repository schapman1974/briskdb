"""Reopen audit optimization must preserve ordering and corruption rejection."""
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pymongo import IndexModel
from pymongo.errors import DuplicateKeyError

import briskdb


def records():
    return [{"_id": identifier, "email": f"{identifier}@example.test", "rank": n,
             "tags": [identifier, "shared"], "active": n % 2 == 0,
             **({"optional": n} if n % 2 == 0 else {}), "body": "x" * 60_000}
            for n, identifier in enumerate([90, 10, 80, 20, 70, 30])]


def indexes():
    return [IndexModel("email", unique=True), IndexModel("tags"),
            IndexModel("optional", sparse=True),
            IndexModel("rank", partialFilterExpression={"active": True})]


class StoreReopenAuditTests(unittest.TestCase):
    def test_reopen_preserves_natural_order_index_membership_and_unique_authority(self):
        expected = records()
        other = [{"_id": row["_id"], "other": True} for row in expected]
        with tempfile.TemporaryDirectory() as folder:
            for attempt in range(3):
                with briskdb.MongoClient(folder, shards=4) as client:
                    items = client.app.items
                    if attempt == 0:
                        items.insert_many(expected)
                        items.create_indexes(indexes())
                        client.other.items.insert_many(other)
                    self.assertEqual(list(items.find()), expected)
                    self.assertEqual(list(items.find({"tags": "shared"})), expected)
                    self.assertEqual(list(items.find({"active": True, "rank": {"$gte": 0}})),
                                     [row for row in expected if row["active"]])
                    self.assertEqual(list(items.find({"optional": {"$exists": True}})),
                                     [row for row in expected if "optional" in row])
                    self.assertEqual(list(client.other.items.find()), other)
                    with self.assertRaises(DuplicateKeyError):
                        items.insert_one({"_id": "duplicate", "email": expected[0]["email"]})
                    if attempt == 1:
                        # New writes retain the global natural-order high-water
                        # even though the startup audit traverses physical keys.
                        added = {"_id": 5, "email": "last@example.test", "rank": 6,
                                 "tags": ["shared"], "active": True}
                        items.insert_one(added)
                        expected.append(added)

    def test_reopen_still_rejects_record_and_index_corruption(self):
        faults = {
            "record checksum": "UPDATE briskdb_documents_v1 SET document_checksum=zeroblob(32)",
            "entry checksum": "UPDATE briskdb_document_index_entries_v1 SET entry_checksum=zeroblob(32)",
            "missing entries": "DELETE FROM briskdb_document_index_entries_v1",
            "orphan entries": "DELETE FROM briskdb_documents_v1",
        }
        for name, sql in faults.items():
            with self.subTest(fault=name), tempfile.TemporaryDirectory() as folder:
                with briskdb.MongoClient(folder, shards=2) as client:
                    client.app.items.insert_one({"_id": 1, "value": 7, "body": "x" * 100_000})
                    client.app.items.create_index("value")
                changed = 0
                for shard in range(2):
                    path = Path(folder) / "shards" / f"{shard:04}.sqlite"
                    # Corrupt only this test-owned, closed store; bypass FK
                    # cascading specifically to simulate an orphan on disk.
                    with sqlite3.connect(path) as connection:
                        connection.execute("PRAGMA foreign_keys=OFF")
                        changed += connection.execute(sql).rowcount
                self.assertGreater(changed, 0)
                with self.assertRaises(briskdb.DataCorruptionError):
                    with briskdb.open(folder, documents=True):
                        self.fail("damaged store became Ready")


class AsyncStoreReopenAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_reopen_keeps_natural_order_and_indexed_results(self):
        expected = records()
        with tempfile.TemporaryDirectory() as folder:
            for attempt in range(2):
                async with briskdb.AsyncMongoClient(folder, shards=4) as client:
                    if attempt == 0:
                        await client.app.items.insert_many(expected)
                        await client.app.items.create_indexes(indexes())
                    self.assertEqual(await client.app.items.find().to_list(length=None), expected)
                    self.assertEqual(await client.app.items.find({"tags": "shared"}).to_list(length=None), expected)


if __name__ == "__main__":
    unittest.main()
