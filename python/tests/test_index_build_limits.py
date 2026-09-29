"""Index builds have their own bounded deadline, not an ordinary CRUD timeout."""

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pymongo
from pymongo import IndexModel
from pymongo.errors import ExecutionTimeout

import briskdb
from briskdb import _mongo_runtime as runtime, patching
from test_index_timeout_recovery import hold_first_shard_after_index_admission


def long_wait_policy():
    return briskdb.ContentionPolicy(initial_delay_ms=20, max_delay_ms=50,
        multiplier=2, jitter="none", max_retries=2000, max_elapsed_ms=60000)


class IndexBuildLimitTests(unittest.TestCase):
    def tearDown(self):
        self.assertEqual(runtime._stores, {})
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)

    def test_default_build_survives_old_wire_and_engine_deadlines(self):
        # A controlled lock, not data volume or CPU speed, takes the admitted
        # build beyond the previous 15s wire and 30s engine ceilings.
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2,
                                    contention_policy=long_wait_policy()) as client:
                client.app.items.insert_many([{"_id": i, "n": i} for i in range(16)])
                with hold_first_shard_after_index_admission(folder, seconds=31.25):
                    self.assertEqual(client.app.items.create_indexes(
                        [IndexModel("n")], maxTimeMS=120000), ["n_1"])
                self.assertEqual(client.app.items.find_one({"n": 15})["_id"], 15)
                self.assertEqual(set(client.app.items.index_information()), {"_id_", "n_1"})

    def test_default_explicit_bounds_and_engine_readback(self):
        for selected, expected in [(None, 300000), (1, 1), (600000, 600000),
                                   (86400000, 86400000)]:
            with self.subTest(selected=selected), tempfile.TemporaryDirectory() as folder:
                with briskdb.MongoClient(folder=folder, shards=2,
                                        index_build_timeout_ms=selected) as client:
                    self.assertEqual(client.briskdb_index_build_timeout_ms, expected)
                    self.assertEqual(client._briskdb_store.database.config.request_timeout_ms,
                                     max(30000, expected))
                    client.app.items.insert_one({"_id": 1})
                    self.assertEqual(client.app.items.count_documents({}), 1)
                # Runtime policy is not written into the persistent root.
                with briskdb.MongoClient(folder=folder) as reopened:
                    self.assertEqual(reopened.briskdb_index_build_timeout_ms, 300000)

    def test_shared_clients_and_patch_inherit_or_reject_conflicting_limits(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2, index_build_timeout_ms=60000) as owner:
                store = owner._briskdb_store
                for selected in (None, 60000):
                    with briskdb.MongoClient(folder=folder, index_build_timeout_ms=selected) as peer:
                        self.assertIs(peer._briskdb_store, store)
                        self.assertEqual(peer.briskdb_index_build_timeout_ms, 60000)
                for Client in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                    with self.assertRaisesRegex(ValueError, "index_build_timeout_ms does not match"):
                        Client(folder=folder, index_build_timeout_ms=300000)
                    self.assertEqual(store.references, 1)
                with briskdb.patch(folder) as Client:
                    self.assertIs(Client()._briskdb_store, store)
                    self.assertEqual(Client(index_build_timeout_ms=60000).briskdb_index_build_timeout_ms, 60000)
                    count = len(patching._entries[-1].clients)
                    with self.assertRaisesRegex(ValueError, "index_build_timeout_ms does not match"):
                        Client(index_build_timeout_ms=1000)
                    self.assertEqual(len(patching._entries[-1].clients), count)
                    with self.assertRaisesRegex(ValueError, "index_build_timeout_ms does not match"):
                        with briskdb.patch(folder, index_build_timeout_ms=1000):
                            self.fail("conflicting nested scope entered")
                    self.assertIs(pymongo.MongoClient, Client)
                self.assertEqual(store.references, 1)

    def test_invalid_limits_fail_before_storage_or_driver_mutation(self):
        with tempfile.TemporaryDirectory() as parent:
            absent = Path(parent) / "not-created"
            for selected in (True, False, 0, -1, 86400001, 1.5, "120000", {}, object()):
                for factory in (briskdb.MongoClient, briskdb.AsyncMongoClient,
                                briskdb.patch, briskdb.MongoPatch):
                    with self.subTest(selected=selected, factory=factory):
                        with mock.patch.object(runtime._briskdb, "open") as opened:
                            with mock.patch.object(runtime.tempfile, "mkdtemp") as made:
                                with self.assertRaisesRegex(ValueError, "index_build_timeout_ms"):
                                    factory(folder=absent, index_build_timeout_ms=selected)
                                opened.assert_not_called()
                                made.assert_not_called()
                        self.assertFalse(absent.exists())
            scope = briskdb.patch()
            scope.index_build_timeout_ms = 0
            with mock.patch.object(runtime.tempfile, "mkdtemp") as made:
                with self.assertRaisesRegex(ValueError, "index_build_timeout_ms"):
                    scope.__enter__()
                made.assert_not_called()
            for Client in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                with mock.patch.object(runtime._briskdb, "open") as opened:
                    with self.assertRaises(ValueError):
                        Client(folder=absent, index_build_timeout_ms=60000, maxPoolSize=-1)
                    opened.assert_not_called()

    def test_host_build_limit_still_bounds_a_larger_client_max_time(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder=folder, shards=2, index_build_timeout_ms=3000) as client:
                client.app.items.insert_one({"_id": 1, "n": 1})
                with hold_first_shard_after_index_admission(folder):
                    with self.assertRaises(ExecutionTimeout) as error:
                        client.app.items.create_indexes([IndexModel("n")], maxTimeMS=120000)
                    self.assertEqual(error.exception.code, 50)
                self.assertEqual(set(client.app.items.index_information()), {"_id_"})
                self.assertEqual(client.app.items.find_one({})["_id"], 1)


class AsyncIndexBuildLimitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        self.assertEqual(runtime._stores, {})
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)

    async def test_async_patch_shares_config_and_exceeds_old_wire_deadline(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.patch(folder, shards=2, index_build_timeout_ms=60000,
                                     contention_policy=long_wait_policy()):
                async with pymongo.AsyncMongoClient() as client:
                    self.assertEqual(client.briskdb_index_build_timeout_ms, 60000)
                    self.assertEqual(client._briskdb_store.database.config.request_timeout_ms, 60000)
                    await client.app.items.insert_one({"_id": 1, "n": 1})
                    with hold_first_shard_after_index_admission(folder, seconds=16.25):
                        self.assertEqual(await client.app.items.create_indexes(
                            [IndexModel("n")], maxTimeMS=45000), ["n_1"])
                    self.assertEqual(await client.app.items.count_documents({}), 1)
                    with self.assertRaisesRegex(ValueError, "index_build_timeout_ms does not match"):
                        pymongo.AsyncMongoClient(index_build_timeout_ms=300000)
                    async with pymongo.AsyncMongoClient(index_build_timeout_ms=60000) as peer:
                        self.assertIs(peer._briskdb_store, client._briskdb_store)


if __name__ == "__main__":
    unittest.main()
