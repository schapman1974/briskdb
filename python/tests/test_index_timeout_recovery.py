"""A real PyMongo timeout must not permanently fence the local client (#554)."""

from contextlib import contextmanager
import subprocess
import sys
import tempfile
import unittest

from pymongo import IndexModel
from pymongo.errors import DuplicateKeyError, ExecutionTimeout

from briskdb import mongo


@contextmanager
def block_first_shard_past_index_deadline(folder):
    """Hold only a test-owned SQLite writer lock, observing durable DDL intent.

    The command gets 3 seconds. Release 3.25 seconds *after* observing its
    committed journal, so cancellation necessarily happens with an admitted
    build. No performance threshold or large/slow data set is required.
    """
    # A separate process is essential: Python and the wheel use different
    # SQLite libraries, but POSIX advisory locks are owned by the process.
    script = r'''
from pathlib import Path
import sqlite3
import sys
import time
root = Path(sys.argv[1])
shard = sqlite3.connect(root / "shards/0000.sqlite", timeout=5)
manifest = sqlite3.connect(root / "manifest.sqlite", timeout=5)
try:
    shard.execute("BEGIN IMMEDIATE")
    print("locked", flush=True)
    deadline = time.monotonic() + 10
    while True:
        if manifest.execute("SELECT EXISTS(SELECT 1 FROM briskdb_document_index_operation)").fetchone()[0]:
            print("admitted", flush=True)
            time.sleep(3.25)
            break
        if time.monotonic() >= deadline:
            raise AssertionError("index build never committed its intent")
        time.sleep(0.005)
finally:
    shard.rollback()
    shard.close()
    manifest.close()
'''
    worker = subprocess.Popen([sys.executable, "-c", script, str(folder)],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        if worker.stdout.readline().strip() != "locked":
            raise AssertionError("test writer lock was not acquired")
        yield
    finally:
        try:
            output, errors = worker.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.communicate()
            raise AssertionError("test lock holder did not stop") from None
        if worker.returncode != 0 or output.strip() != "admitted":
            raise AssertionError(f"test did not observe durable index intent: {output} {errors}")


class IndexTimeoutRecoveryTests(unittest.TestCase):
    def test_same_client_recovers_after_admitted_build_times_out(self):
        with tempfile.TemporaryDirectory() as folder:
            with mongo.MongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                documents = [{"_id": i, "email": str(i), "n": i} for i in range(32)]
                items.insert_many(documents)
                items.create_indexes([IndexModel("email", unique=True)])
                client.other_db.items.insert_one({"_id": "other"})
                definitions = items.index_information()
                with block_first_shard_past_index_deadline(folder):
                    with self.assertRaises(ExecutionTimeout) as error:
                        items.create_indexes([IndexModel("n")], maxTimeMS=3000)
                    self.assertEqual(error.exception.code, 50)
                self.assertEqual(items.index_information(), definitions)
                self.assertEqual(list(items.find({}).sort("_id")), documents)
                self.assertEqual(client.other_db.items.find_one({})["_id"], "other")
                client.other_db.items.insert_one({"_id": "after"})
                items.insert_one({"_id": 32, "email": "new", "n": 32})
                with self.assertRaises(DuplicateKeyError):
                    items.insert_one({"_id": 33, "email": "new"})
                self.assertEqual(items.create_indexes([IndexModel("n")]), ["n_1"])
            with mongo.MongoClient(folder=folder, shards=2) as reopened:
                self.assertEqual(reopened.app.items.count_documents({}), 33)
                self.assertEqual(set(reopened.app.items.index_information()),
                                 {"_id_", "email_1", "n_1"})
                self.assertEqual(reopened.other_db.items.count_documents({}), 2)


class AsyncIndexTimeoutRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_async_client_recovers_after_admitted_build_times_out(self):
        with tempfile.TemporaryDirectory() as folder:
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as client:
                items = client.app.items
                documents = [{"_id": i, "n": i} for i in range(32)]
                await items.insert_many(documents)
                with block_first_shard_past_index_deadline(folder):
                    with self.assertRaises(ExecutionTimeout) as error:
                        await items.create_indexes([IndexModel("n")], maxTimeMS=3000)
                    self.assertEqual(error.exception.code, 50)
                self.assertEqual(set(await items.index_information()), {"_id_"})
                self.assertEqual(await items.find({}).sort("_id").to_list(), documents)
                await items.insert_one({"_id": 32, "n": 32})
                await client.other_db.items.insert_one({"_id": "other"})
                self.assertEqual(await items.create_indexes([IndexModel("n")]), ["n_1"])
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as reopened:
                self.assertEqual(await reopened.app.items.count_documents({}), 33)
                self.assertEqual(await reopened.other_db.items.find_one({}), {"_id": "other"})


if __name__ == "__main__":
    unittest.main()
