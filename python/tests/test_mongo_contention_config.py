"""Managed Mongo owners share one native policy, never a frontend retry loop."""

from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pymongo
from pymongo.errors import OperationFailure

import briskdb
from briskdb import _mongo_runtime as runtime, patching


def policy(**changes):
    values = dict(initial_delay_ms=1, max_delay_ms=4, multiplier=2,
                  jitter="none", max_retries=3, max_elapsed_ms=20)
    values.update(changes)
    return briskdb.ContentionPolicy(**values)


@contextmanager
def hold_document_writer(database):
    # Use the same native SQLite library and engine, not a second Python
    # sqlite3 library whose POSIX descriptor closes could release shared locks.
    with database.session() as session:
        session.migrate("CREATE TABLE held_writer (id INTEGER PRIMARY KEY)")
        target = session.find("app", "items", {"_id": 2})["plan"]["shards"]
    for number in range(64):
        key = f"contention-{number}"
        with database.session(routing_key=key) as session:
            if session.query("SELECT 1")["shards"] == target:
                break
    else:
        raise AssertionError("could not find routing key for document shard")
    with database.transaction(routing_key=key) as transaction:
        transaction.execute("INSERT INTO held_writer VALUES (1)")
        yield


class MongoContentionConfigTests(unittest.TestCase):
    def setUp(self):
        self.original = pymongo.MongoClient
        self.original_async = pymongo.AsyncMongoClient

    def tearDown(self):
        self.assertIs(pymongo.MongoClient, self.original)
        self.assertIs(pymongo.AsyncMongoClient, self.original_async)
        self.assertEqual(runtime._stores, {})
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)

    def test_sync_defaults_native_readback_and_post_close_policy(self):
        for selected in (None, briskdb.ContentionPolicy.fail_fast(), policy()):
            with self.subTest(policy=repr(selected)), tempfile.TemporaryDirectory() as root:
                with briskdb.MongoClient(root, shards=2, contention_policy=selected) as client:
                    self.assertEqual(repr(client.briskdb_contention_policy), repr(selected))
                    self.assertEqual(repr(client._briskdb_store.database.config.contention_policy),
                                     repr(selected))
                    client.app.items.insert_one({"_id": 1})
                    self.assertEqual(client.app.items.find_one({}), {"_id": 1})
                self.assertEqual(repr(client.briskdb_contention_policy), repr(selected))
                # This is runtime configuration, not a persisted root setting.
                with briskdb.MongoClient(root) as reopened:
                    self.assertIsNone(reopened.briskdb_contention_policy)

    def test_shared_root_inherits_accepts_equal_values_and_rejects_every_conflict(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2, contention_policy=policy()) as owner:
                store = owner._briskdb_store
                for selected in (None, policy()):
                    with briskdb.MongoClient(root, contention_policy=selected) as peer:
                        self.assertIs(peer._briskdb_store, store)
                        self.assertEqual(repr(peer.briskdb_contention_policy), repr(policy()))
                        self.assertEqual(store.references, 2)
                for change in ({"initial_delay_ms": 2}, {"max_delay_ms": 5},
                               {"multiplier": 3}, {"jitter": "full"},
                               {"max_retries": 4}, {"max_elapsed_ms": 21}):
                    for Client in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                        with self.subTest(change=change, client=Client.__name__):
                            with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                                Client(root, contention_policy=policy(**change))
                            self.assertEqual(store.references, 1)
                with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                    briskdb.MongoClient(root, contention_policy=briskdb.ContentionPolicy.fail_fast())
                owner.app.items.insert_one({"_id": "still-open"})
                self.assertEqual(owner.app.items.count_documents({}), 1)

    def test_explicit_policy_cannot_reconfigure_a_legacy_engine(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as owner:
                with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                    briskdb.MongoClient(root, contention_policy=policy())
                self.assertIsNone(owner.briskdb_contention_policy)
                self.assertEqual(owner._briskdb_store.references, 1)

    def test_scope_owns_policy_rejects_conflicts_and_restores_nested_failure(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2, contention_policy=policy()) as owner:
                with briskdb.patch(root) as Client:
                    inherited = Client()
                    equal = Client(contention_policy=policy())
                    self.assertIs(inherited._briskdb_store, owner._briskdb_store)
                    self.assertEqual(repr(equal.briskdb_contention_policy), repr(policy()))
                    clients = len(patching._entries[-1].clients)
                    with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                        Client(contention_policy=briskdb.ContentionPolicy.fail_fast())
                    self.assertEqual(len(patching._entries[-1].clients), clients)
                    with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                        with briskdb.patch(root, contention_policy=briskdb.ContentionPolicy.fail_fast()):
                            self.fail("conflicting nested scope entered")
                    self.assertIs(pymongo.MongoClient, Client)
                    self.assertEqual(owner._briskdb_store.references, 2)
                    with briskdb.patch(root, contention_policy=policy()) as Nested:
                        self.assertIs(Nested()._briskdb_store, owner._briskdb_store)
                    equal.app.items.insert_one({"_id": 1})
                self.assertEqual(owner._briskdb_store.references, 1)
                self.assertEqual(owner.app.items.count_documents({}), 1)

    def test_invalid_types_fail_before_storage_or_scope_mutation(self):
        with tempfile.TemporaryDirectory() as parent:
            absent = Path(parent) / "must-not-exist"
            for selected in ({}, "fail-fast", 1, object()):
                for factory in (briskdb.MongoClient, briskdb.AsyncMongoClient,
                                briskdb.patch, briskdb.MongoPatch):
                    with self.subTest(factory=factory, policy=selected):
                        with mock.patch.object(runtime._briskdb, "open") as opened:
                            with mock.patch.object(runtime.tempfile, "mkdtemp") as made:
                                with self.assertRaisesRegex(TypeError, "contention_policy"):
                                    factory(folder=absent, contention_policy=selected)
                                opened.assert_not_called()
                                made.assert_not_called()
                        self.assertFalse(absent.exists())
            scope = briskdb.patch()
            scope.contention_policy = {}
            with mock.patch.object(runtime.tempfile, "mkdtemp") as made:
                with self.assertRaisesRegex(TypeError, "contention_policy"):
                    scope.__enter__()
                made.assert_not_called()

    def test_driver_validation_still_precedes_native_startup_with_policy(self):
        with tempfile.TemporaryDirectory() as parent:
            absent = Path(parent) / "must-not-exist"
            for Client in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                with mock.patch.object(runtime._briskdb, "open") as opened:
                    with self.assertRaises(ValueError):
                        Client(folder=absent, contention_policy=policy(), maxPoolSize=-1)
                    opened.assert_not_called()
                self.assertFalse(absent.exists())

    def test_sync_wire_contention_does_not_replay_failed_insert(self):
        for selected in (briskdb.ContentionPolicy.fail_fast(), policy()):
            with self.subTest(policy=repr(selected)):
                with briskdb.patch(shards=2, contention_policy=selected) as Client:
                    with Client() as client:
                        client.app.items.insert_one({"_id": 1})
                        with hold_document_writer(client._briskdb_store.database):
                            with self.assertRaises(OperationFailure) as failed:
                                client.app.items.insert_one({"_id": 2})
                            self.assertEqual(failed.exception.code, 112)
                        self.assertEqual(list(client.app.items.find({})), [{"_id": 1}])
                        client.app.items.insert_one({"_id": 2})
                        self.assertEqual(client.app.items.count_documents({}), 2)


class AsyncMongoContentionConfigTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        self.assertEqual(runtime._stores, {})
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)

    async def test_async_direct_and_patch_clients_share_the_native_policy(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2, contention_policy=policy()) as owner:
                self.assertEqual(repr(owner._briskdb_store.database.config.contention_policy), repr(policy()))
                async with briskdb.patch(root, contention_policy=policy()):
                    async with pymongo.AsyncMongoClient() as inherited:
                        self.assertIs(inherited._briskdb_store, owner._briskdb_store)
                        self.assertEqual(repr(inherited.briskdb_contention_policy), repr(policy()))
                        with self.assertRaisesRegex(ValueError, "contention_policy does not match"):
                            pymongo.AsyncMongoClient(contention_policy=briskdb.ContentionPolicy.fail_fast())
                        self.assertEqual(len(patching._entries[-1].async_clients), 1)
                        async with pymongo.AsyncMongoClient(contention_policy=policy()) as equal:
                            await equal.app.items.insert_one({"_id": 1})
                        self.assertEqual(await inherited.app.items.count_documents({}), 1)
                self.assertEqual(await owner.app.items.count_documents({}), 1)
                self.assertEqual(owner._briskdb_store.references, 1)
            self.assertEqual(repr(owner.briskdb_contention_policy), repr(policy()))

    async def test_async_wire_contention_does_not_replay_failed_insert(self):
        async with briskdb.patch(shards=2, contention_policy=briskdb.ContentionPolicy.fail_fast()):
            async with pymongo.AsyncMongoClient() as client:
                await client.app.items.insert_one({"_id": 1})
                with hold_document_writer(client._briskdb_store.database):
                    with self.assertRaises(OperationFailure) as failed:
                        await client.app.items.insert_one({"_id": 2})
                    self.assertEqual(failed.exception.code, 112)
                self.assertEqual(await client.app.items.find({}).to_list(), [{"_id": 1}])
                await client.app.items.insert_one({"_id": 2})
                self.assertEqual(await client.app.items.count_documents({}), 2)


if __name__ == "__main__":
    unittest.main()
