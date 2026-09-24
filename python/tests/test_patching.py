import asyncio
from collections import OrderedDict
import concurrent.futures
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

import pymongo
from bson import Decimal128, Int64
from pymongo.errors import DuplicateKeyError, InvalidOperation

import briskdb
from briskdb import _mongo_runtime, patching


class PatchingTests(unittest.TestCase):
    def setUp(self):
        self.original = pymongo.MongoClient
        self.original_async = pymongo.AsyncMongoClient

    def tearDown(self):
        self.assertIs(pymongo.MongoClient, self.original)
        self.assertIs(pymongo.AsyncMongoClient, self.original_async)
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)
        self.assertEqual(_mongo_runtime._stores, {})

    def test_shared_isolated_store_real_queries_results_and_cleanup(self):
        with briskdb.patch(shards=2) as Client:
            self.assertIs(pymongo.MongoClient, Client)
            writer = pymongo.MongoClient("mongodb://ignored.invalid")
            reader = pymongo.MongoClient()
            self.assertIsInstance(writer, self.original)
            self.assertIsInstance(reader, briskdb.MongoClient)
            root = writer.briskdb_path
            self.assertEqual(root, reader.briskdb_path)
            store = writer._briskdb_store
            address = store.listener.address
            collection = writer.app.users
            collection.create_index("email", unique=True)
            result = collection.insert_many([
                {"_id": 1, "email": "ada@example.com", "score": Int64(7), "tags": ["a", "b"]},
                {"_id": 2, "email": "grace@example.com", "score": Int64(9), "tags": ["b"]},
            ])
            self.assertEqual(result.inserted_ids, [1, 2])
            self.assertEqual(reader.app.users.count_documents({"tags": "b"}), 2)
            self.assertEqual(list(reader.app.users.find({"score": {"$gte": 7}}, {"_id": 1})
                                  .sort("score", -1).skip(1).limit(1)), [{"_id": 1}])
            self.assertEqual(reader.app.users.distinct("tags"), ["a", "b"])
            self.assertEqual(list(collection.aggregate([
                {"$group": {"_id": None, "total": {"$sum": "$score"}}}
            ])), [{"_id": None, "total": 16}])
            self.assertEqual(collection.update_one({"_id": 1}, {"$inc": {"score": 1}}).modified_count, 1)
            self.assertIsInstance(reader.app.users.find_one({"_id": 1})["score"], Int64)
            with self.assertRaises(DuplicateKeyError):
                collection.insert_one({"_id": 3, "email": "ada@example.com"})
            self.assertEqual(collection.delete_one({"_id": 2}).deleted_count, 1)
            writer.close()
            self.assertEqual(reader.app.users.count_documents({}), 1)
        self.assertFalse(root.exists())
        with self.assertRaises(InvalidOperation):
            reader.app.users.find_one({})
        with self.assertRaisesRegex(InvalidOperation, "scope is closed"):
            Client()
        host, port = address.rsplit(":", 1)
        with socket.socket() as probe:
            self.assertNotEqual(probe.connect_ex((host, int(port))), 0)

    def test_original_hosts_credentials_tls_srv_and_proxies_never_reach_driver(self):
        seen = []
        original_resolver = socket.getaddrinfo

        def resolver(host, *args, **kwargs):
            seen.append(host)
            self.assertEqual(host, "127.0.0.1")
            return original_resolver(host, *args, **kwargs)

        with mock.patch("socket.getaddrinfo", side_effect=resolver):
            with briskdb.patch(shards=2):
                with pymongo.MongoClient(
                    "mongodb+srv://private:secret@production.invalid/app?tls=true&authSource=admin"
                    "&replicaSet=production&uuidRepresentation=standard&retryWrites=true",
                    username="private", password="secret", tlsCAFile="/does/not/exist",
                    proxyHost="proxy.invalid", proxyPort=1080,
                    serverSelectionTimeoutMS=3000, document_class=OrderedDict,
                ) as client:
                    database = client.get_default_database()
                    self.assertEqual(database.name, "app")
                    database.items.insert_one({"_id": 1, "amount": Decimal128("1.25")})
                    row = database.items.find_one({"_id": 1})
                    self.assertIsInstance(row, OrderedDict)
                    self.assertEqual(row["amount"], Decimal128("1.25"))
                    self.assertEqual(client.address[0], "127.0.0.1")
                    self.assertNotIn("secret", repr(client))
        self.assertTrue(seen)

    def test_uri_repeated_tags_and_keyword_overrides_keep_driver_semantics(self):
        with briskdb.patch(shards=2):
            with pymongo.MongoClient(
                "mongodb://ignored/app?readPreference=secondaryPreferred"
                "&readPreferenceTags=dc:one&readPreferenceTags=dc:two&maxPoolSize=9",
                maxPoolSize=3, auto_encryption_opts=None,
            ) as client:
                self.assertEqual(client.read_preference.tag_sets, [{"dc": "one"}, {"dc": "two"}])
                self.assertEqual(client.options.pool_options.max_pool_size, 3)
                self.assertEqual(client.get_default_database().items.count_documents({}), 0)
            with self.assertRaisesRegex(ValueError, "automatic encryption"):
                pymongo.MongoClient(auto_encryption_opts=object())

    def test_nested_reused_scope_restores_lifo_and_preserves_outer(self):
        scope = briskdb.patch(shards=2)
        with scope as Outer:
            outer = pymongo.MongoClient()
            outer.app.items.insert_one({"_id": "outer"})
            with scope as Inner:
                inner = pymongo.MongoClient()
                self.assertIsNot(Outer, Inner)
                self.assertNotEqual(outer.briskdb_path, inner.briskdb_path)
                self.assertIsNone(inner.app.items.find_one({"_id": "outer"}))
            self.assertIs(pymongo.MongoClient, Outer)
            self.assertIsNotNone(outer.app.items.find_one({"_id": "outer"}))

    def test_explicit_folders_and_decorator_persist_and_reopen_layout(self):
        with tempfile.TemporaryDirectory() as parent:
            folder = Path(parent) / "persistent"

            @briskdb.patch(folder=folder, backend="sqlite", shards=3)
            def insert(identifier):
                client = pymongo.MongoClient(backend="memory", tinymongo_folder="ignored")
                client.app.items.insert_one({"_id": identifier})

            insert(1)
            insert(2)
            self.assertTrue((folder / "manifest.sqlite").exists())
            with briskdb.patch(folder=folder):
                client = pymongo.MongoClient()
                self.assertEqual(client._briskdb_store.database.shard_count, 3)
                self.assertEqual(client.app.items.count_documents({}), 2)

    def test_direct_clients_share_engine_until_last_close_and_match_native_data(self):
        with tempfile.TemporaryDirectory() as folder:
            first = briskdb.MongoClient(folder, shards=2)
            second = briskdb.MongoClient(tinymongo_folder=folder, backend="sqlite")
            try:
                self.assertIs(first._briskdb_store, second._briskdb_store)
                first.app.items.insert_one({"_id": 1, "value": "wire"})
                with first._briskdb_store.database.session() as session:
                    self.assertEqual(session.find("app", "items", {"_id": 1})["documents"],
                                     [{"_id": 1, "value": "wire"}])
                first.close()
                self.assertEqual(second.app.items.count_documents({}), 1)
                with briskdb.patch(folder=folder):
                    scoped = pymongo.MongoClient()
                    self.assertIs(scoped._briskdb_store, second._briskdb_store)
                    self.assertEqual(scoped.app.items.count_documents({}), 1)
                self.assertEqual(second.app.items.count_documents({}), 1)
            finally:
                first.close()
                second.close()
            with briskdb.MongoClient(folder=folder) as reopened:
                self.assertEqual(reopened.app.items.count_documents({}), 1)
            self.assertTrue(Path(folder).exists())

    def test_restore_after_exception_invalid_constructor_and_startup_failure(self):
        with self.assertRaisesRegex(RuntimeError, "application failed"):
            with briskdb.patch(shards=2):
                with self.assertRaises(ValueError):
                    pymongo.MongoClient(maxPoolSize=-1)
                pymongo.MongoClient().app.items.insert_one({"_id": 1})
                raise RuntimeError("application failed")
        with mock.patch.object(_mongo_runtime._briskdb, "open", side_effect=RuntimeError("startup failed")):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                with briskdb.patch(shards=2):
                    self.fail("unexpected entry")
        with briskdb.patch(shards=2):
            self.assertEqual(pymongo.MongoClient().app.items.count_documents({}), 0)

    def test_out_of_order_or_wrong_thread_exit_leaves_active_scope_unchanged(self):
        outer, inner = briskdb.patch(shards=2), briskdb.patch(shards=2)
        Outer = outer.__enter__()
        Inner = inner.__enter__()
        try:
            with self.assertRaisesRegex(RuntimeError, "nested order"):
                outer.__exit__(None, None, None)
            self.assertIs(pymongo.MongoClient, Inner)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(inner.__exit__, None, None, None)
                with self.assertRaisesRegex(RuntimeError, "owning thread/task"):
                    future.result(timeout=10)
            self.assertIs(pymongo.MongoClient, Inner)
        finally:
            inner.__exit__(None, None, None)
            self.assertIs(pymongo.MongoClient, Outer)
            outer.__exit__(None, None, None)
        with self.assertRaisesRegex(RuntimeError, "without being entered"):
            outer.__exit__(None, None, None)

    def test_overlapping_thread_scope_rejected(self):
        with briskdb.patch(shards=2):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                def overlap():
                    with briskdb.patch():
                        self.fail("unexpected overlapping scope")
                future = pool.submit(overlap)
                with self.assertRaisesRegex(RuntimeError, "cannot overlap"):
                    future.result(timeout=10)

    def test_async_requires_awaited_scope_and_coroutine_decorator_rejected(self):
        with briskdb.patch(shards=2):
            with self.assertRaisesRegex(RuntimeError, "async with"):
                pymongo.AsyncMongoClient()
        with self.assertRaisesRegex(TypeError, "does not decorate async functions"):
            @briskdb.patch()
            async def unsupported():
                pass
        with self.assertRaisesRegex(ValueError, "SQLite"):
            briskdb.patch(backend="memory")

    def test_import_and_scope_construction_do_not_require_pymongo(self):
        program = '''
import importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in ("pymongo", "bson"):
            raise ModuleNotFoundError("blocked optional dependency", name=fullname)
sys.meta_path.insert(0, Block())
import briskdb
from briskdb import *
scope = briskdb.patch()
assert "pymongo" not in sys.modules and "bson" not in sys.modules
try:
    with scope:
        raise AssertionError("entered without optional dependency")
except ImportError as error:
    assert "briskdb[pymongo]" in str(error)
else:
    raise AssertionError("missing dependency accepted")
from briskdb import patching, _mongo_runtime
assert not patching._entries and patching._owner is None
assert not _mongo_runtime._stores
'''
        result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_constructor_aliases_imported_before_scope_are_not_replaced(self):
        captured = self.original
        existing = captured("mongodb://untouched.invalid", connect=False)
        try:
            with briskdb.patch(shards=2):
                self.assertIs(captured, self.original)
                self.assertIsNot(captured, pymongo.MongoClient)
                self.assertIs(type(existing), self.original)
                self.assertEqual(pymongo.MongoClient().app.items.count_documents({}), 0)
        finally:
            existing.close()

    def test_one_close_failure_does_not_skip_other_clients_or_native_cleanup(self):
        scope = briskdb.patch(shards=2)
        scope.__enter__()
        first, second = pymongo.MongoClient(), pymongo.MongoClient()
        root = first.briskdb_path
        first.app.items.insert_one({"_id": 1})
        try:
            with mock.patch.object(first, "close", side_effect=RuntimeError("test close failure")):
                with self.assertRaisesRegex(RuntimeError, "test close failure"):
                    scope.__exit__(None, None, None)
            self.assertFalse(root.exists())
            with self.assertRaises(InvalidOperation):
                second.app.items.find_one({})
        finally:
            first.close()

    def test_direct_invalid_options_release_native_store_and_path_conflicts_fail(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):
                briskdb.MongoClient(folder=folder, maxPoolSize=-1)
            self.assertEqual(_mongo_runtime._stores, {})
            with self.assertRaisesRegex(TypeError, "folder only once"):
                briskdb.MongoClient(folder=folder, tinymongo_folder=folder)
            with self.assertRaisesRegex(ValueError, "folder must not be empty"):
                briskdb.MongoClient(folder="")
            with briskdb.MongoClient(folder=folder) as client:
                with self.assertRaisesRegex(ValueError, "does not match"):
                    briskdb.MongoClient(folder=folder, shards=3)
                self.assertEqual(client.app.items.count_documents({}), 0)

    def test_native_mongo_only_listeners_are_registered_for_database_close(self):
        with tempfile.TemporaryDirectory() as folder:
            database = briskdb.open(folder, shards=2, documents=True)
            first, second = database._serve_mongo(), database._serve_mongo()
            self.assertNotEqual(first.address, second.address)
            with self.original("mongodb://" + first.address, serverSelectionTimeoutMS=3000) as client:
                client.app.items.insert_one({"_id": 1})
            database.close()
            self.assertTrue(first.closed)
            self.assertTrue(second.closed)
            first.close()
            second.close()
            with self.assertRaises(briskdb.FailedPreconditionError):
                database._serve_mongo()

    def test_documented_example_runs_without_import_shadowing(self):
        example = Path(__file__).resolve().parents[1] / "examples" / "mongo" / "patch.py"
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable, str(example)], cwd=folder,
                                    capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("persistence passed", result.stdout)


class AsyncPatchingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original = pymongo.MongoClient
        self.original_async = pymongo.AsyncMongoClient

    async def asyncTearDown(self):
        self.assertIs(pymongo.MongoClient, self.original)
        self.assertIs(pymongo.AsyncMongoClient, self.original_async)
        self.assertEqual(patching._entries, [])
        self.assertIsNone(patching._owner)
        self.assertEqual(_mongo_runtime._stores, {})

    async def test_async_and_sync_share_scope_and_clients_are_closed(self):
        async with briskdb.patch(shards=2):
            client = pymongo.AsyncMongoClient("mongodb://ignored.invalid")
            sync = pymongo.MongoClient()
            root = client.briskdb_path
            await client.app.items.insert_many([{"_id": i, "group": i % 2} for i in range(12)])
            self.assertEqual(sync.app.items.count_documents({}), 12)
            rows = [row async for row in client.app.items.find({"group": 1}).sort("_id", -1).batch_size(2)]
            self.assertEqual([row["_id"] for row in rows], [11, 9, 7, 5, 3, 1])
            result = await client.app.items.update_one({"_id": 1}, {"$set": {"updated": True}})
            self.assertEqual(result.modified_count, 1)
            pipeline = await client.app.items.aggregate([{"$group": {"_id": "$group", "n": {"$sum": 1}}}])
            self.assertEqual(await pipeline.to_list(), [{"_id": 0, "n": 6}, {"_id": 1, "n": 6}])
        self.assertFalse(root.exists())
        with self.assertRaises(InvalidOperation):
            await client.app.items.find_one({})
        with self.assertRaises(InvalidOperation):
            sync.app.items.find_one({})

    async def test_direct_async_client_persists_and_reopens(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.AsyncMongoClient(folder=folder, shards=2) as client:
                await client.app.items.insert_one({"_id": 1})
            async with briskdb.AsyncMongoClient(folder=folder) as client:
                self.assertEqual(await client.app.items.count_documents({}), 1)

    async def test_other_tasks_cannot_overlap_even_on_same_thread(self):
        async with briskdb.patch(shards=2):
            async def overlap():
                async with briskdb.patch():
                    self.fail("overlapping scope entered")
            with self.assertRaisesRegex(RuntimeError, "cannot overlap"):
                await asyncio.create_task(overlap())

    async def test_cancelled_async_startup_drains_then_restores_scope(self):
        entered, release = threading.Event(), threading.Event()
        original = patching.acquire

        def delayed(*args):
            entered.set()
            if not release.wait(timeout=10):
                raise RuntimeError("test startup barrier timed out")
            return original(*args)

        async def scope():
            async with briskdb.patch(shards=2):
                self.fail("cancelled entry reached application")

        with mock.patch.object(patching, "acquire", side_effect=delayed):
            task = asyncio.create_task(scope())
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            task.cancel()
            # A second task must reject promptly while startup is off-thread.
            with self.assertRaisesRegex(RuntimeError, "cannot overlap"):
                async with briskdb.patch():
                    pass
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10)

    async def test_cancellation_during_exit_waits_for_native_release(self):
        entered, release = threading.Event(), threading.Event()
        original = _mongo_runtime._Store.release

        def delayed(store):
            entered.set()
            if not release.wait(timeout=10):
                raise RuntimeError("test cleanup barrier timed out")
            return original(store)

        async def scope():
            async with briskdb.patch(shards=2):
                client = pymongo.AsyncMongoClient()
                await client.app.items.insert_one({"_id": 1})

        with mock.patch.object(_mongo_runtime._Store, "release", delayed):
            task = asyncio.create_task(scope())
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            self.assertFalse(task.done())
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10)
