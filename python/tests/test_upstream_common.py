"""Owned common-client/concurrency scenarios from four locked source suites.

The inventory separately records modern PyMongo cursor behavior, shared-root
schema admission and private TinyMongo cache/lock hooks. This is not execution
of unchanged upstream source against BriskDB's storage.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import sys
import tempfile
import threading
import time
import unittest

from pymongo.errors import OperationFailure

import briskdb
from briskdb.mongo import IndexCompatibilityWarning


def _writer(root, worker, start, ready):
    # Spawn, never inherit a live engine or its background threads. Each child
    # owns its local listener; the schema is prepared before roots overlap.
    with briskdb.MongoClient(root, shards=2, maxPoolSize=1,
                            serverMonitoringMode="poll", socketTimeoutMS=10000) as client:
        ready.put(worker)
        if not start.wait(20):
            raise TimeoutError("writer start was not released")
        result = client.app.items.insert_many(
            [{"count": worker * 50 + i} for i in range(50)]
        )
        if len(result.inserted_ids) != 50:
            raise AssertionError("writer did not acknowledge its complete batch")


class UpstreamCommonApiTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2, socketTimeoutMS=10000)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_dotted_children_private_names_and_typo_errors_are_driver_compatible(self):
        database = self.client.app
        parent = database.users
        child = parent.archive
        self.assertIsNot(child, parent)
        self.assertEqual((child.name, child.full_name), ("users.archive", "app.users.archive"))
        self.assertEqual(parent["archive"], child)
        self.assertEqual(parent["_private"].name, "users._private")
        self.assertEqual(database["users._private"], parent["_private"])
        with self.assertRaises(AttributeError):
            _ = parent._private
        with self.assertRaises(AttributeError):
            _ = database._private
        parent.insert_one({"_id": "parent"})
        child.insert_one({"_id": "child"})
        self.assertEqual(parent.find_one({}), {"_id": "parent"})
        self.assertEqual(child.find_one({}), {"_id": "child"})
        with self.assertRaises(TypeError):
            parent.find_oni({})
        with self.assertRaises(TypeError):
            database.pingg()
        # Dotted child selection must retain local cursor input snapshots.
        query = {"_id": "child"}
        cursor = child.find(query)
        query["_id"] = "changed"
        self.assertEqual(list(cursor), [{"_id": "child"}])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(sorted(reopened.app.list_collection_names()), ["users", "users.archive"])
            self.assertEqual(reopened.app.users.archive.find_one({}), {"_id": "child"})

    def test_database_handles_logical_drop_and_explicit_statistics_gap(self):
        database = self.client.get_database("app")
        collection = database.get_collection("items")
        self.assertEqual(database.name, "app")
        self.assertIs(collection.database, database)
        self.assertEqual((collection.name, collection.full_name), ("items", "app.items"))
        collection.insert_one({"_id": 1})
        self.client.other.items.insert_one({"_id": "keep"})
        self.assertEqual(self.client.list_database_names(), ["app", "other"])
        self.assertEqual(self.client.list_databases(nameOnly=True).to_list(), [{"name": "app"}, {"name": "other"}])
        with self.assertRaises(OperationFailure) as caught:
            self.client.list_databases()
        self.assertEqual(caught.exception.code, 115)  # #166; never invent sizeOnDisk.
        self.assertIsNone(self.client.drop_database(database))
        self.assertIsNone(self.client.drop_database("missing"))
        self.assertEqual(self.client.list_database_names(), ["other"])
        self.assertEqual(self.client.other.items.find_one({}), {"_id": "keep"})
        with self.assertRaises(TypeError):
            self.client.drop_database(object())
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(reopened.list_database_names(), ["other"])
            self.assertIsNone(reopened.app.items.find_one({}))

    def test_find_one_and_delete_honors_sort_projection_and_missing_selection(self):
        self.items.insert_many([
            {"_id": 1, "status": "pending", "priority": 1, "secret": "a"},
            {"_id": 2, "status": "pending", "priority": 2, "secret": "b"},
        ])
        removed = self.items.find_one_and_delete({"status": "pending"},
                                                sort=[("priority", -1)], projection={"secret": 0})
        self.assertEqual(removed, {"_id": 2, "status": "pending", "priority": 2})
        self.assertIsNone(self.items.find_one({"_id": 2}))
        self.assertIsNone(self.items.find_one_and_delete({"status": "missing"}))
        self.assertEqual(self.items.count_documents({}), 1)

    def test_find_modify_before_after_upsert_sort_projection_and_validation(self):
        self.items.insert_many([
            {"_id": 1, "status": "pending", "rank": 1, "secret": "one"},
            {"_id": 2, "status": "pending", "rank": 2, "secret": "two"},
        ])
        after = self.items.find_one_and_update({"status": "pending"}, {"$set": {"status": "complete"}},
                                               sort=[("rank", -1)], projection={"status": 1}, return_document=True)
        self.assertEqual(after, {"_id": 2, "status": "complete"})
        self.assertEqual(self.items.find_one_and_replace({"_id": 1}, {"status": "replaced", "rank": 3})["status"], "pending")
        self.assertIsNone(self.items.find_one_and_update({"_id": 3}, {"$set": {"status": "inserted"}}, upsert=True))
        self.assertEqual(self.items.find_one({"_id": 3})["status"], "inserted")
        replacement = {"_id": 4, "status": "inserted replacement"}
        self.assertEqual(self.items.find_one_and_replace({"_id": 4}, replacement, upsert=True, return_document=True), replacement)
        for operation in (
            lambda: self.items.find_one_and_update({}, {"$set": {"seen": True}}, return_document=1),
            lambda: self.items.find_one_and_replace({}, {}, return_document="after"),
        ):
            with self.assertRaises(ValueError):
                operation()
        self.assertEqual(self.items.count_documents({}), 4)
        self.assertEqual(self.items.count_documents({"seen": {"$exists": True}}), 0)

    def test_distinct_index_metadata_and_modern_cursor_lifecycle(self):
        self.assertTrue(issubclass(IndexCompatibilityWarning, UserWarning))
        self.items.insert_many([
            {"_id": 1, "kind": "a", "profile": {"team": "one"}, "tags": ["x", "y"]},
            {"_id": 2, "kind": "b", "profile": {"team": "one"}, "tags": ["y"]},
            {"_id": 3, "kind": "a", "profile": {"team": "two"}},
        ])
        self.items.create_index("kind", name="kind_lookup")
        self.assertEqual(self.items.distinct("profile.team", {"kind": "a"}), ["one", "two"])
        self.assertEqual(self.items.distinct("tags"), ["x", "y"])
        self.assertEqual(self.items.index_information(), {"_id_": {"key": [("_id", 1)]}, "kind_lookup": {"key": [("kind", 1)]}})
        cursor = self.items.find({}).sort("_id")
        self.items.insert_one({"_id": 4, "kind": "c"})
        self.assertEqual([row["_id"] for row in cursor.clone().skip(1).limit(-1).to_list()], [2])
        self.assertEqual([row["_id"] for row in cursor.clone().to_list()], [1, 2, 3, 4])
        self.assertEqual(cursor.to_list(1)[0]["_id"], 1)
        self.assertEqual(cursor.rewind().to_list(1)[0]["_id"], 1)
        self.assertTrue(cursor.alive)
        cursor.close()
        # The pinned driver retains buffered documents after close and permits
        # rewind. Do not pretend TinyMongo's permanent-close behavior is shipped.
        self.assertEqual([row["_id"] for row in cursor.to_list()], [2, 3, 4])
        self.assertFalse(cursor.alive)
        self.assertEqual([row["_id"] for row in cursor.rewind().to_list()], [1, 2, 3, 4])
        with self.assertRaises(ValueError):
            self.items.find({}).skip(-1)
        with self.assertRaises(TypeError):
            self.items.find({}).limit(1.5)
        with self.assertRaises(ValueError):
            self.items.find({}).to_list(-1)
        unevaluated = self.items.find({})
        self.assertIs(unevaluated.skip(True), unevaluated)
        unevaluated.close()
        self.assertEqual(self.items.find({}).to_list(True)[0]["_id"], 1)

    def test_iterating_a_wire_cursor_preserves_consumed_position(self):
        self.items.insert_many([{"_id": i} for i in (1, 2, 3)])
        cursor = self.items.find({}).batch_size(1)
        self.assertEqual(next(cursor), {"_id": 1})
        self.assertEqual(list(cursor), [{"_id": 2}, {"_id": 3}])
        self.assertEqual(list(cursor), [])
        with self.assertRaises(StopIteration):
            cursor.next()

    def test_update_many_with_embedded_ids_persists(self):
        identity = {"tenant": 1, "item": 2}
        self.items.insert_one({"_id": identity, "value": 1})
        result = self.items.update_many({}, {"$set": {"value": 2}})
        self.assertEqual((result.matched_count, result.modified_count), (1, 1))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(reopened.app.items.find_one({"_id": identity})["value"], 2)

    def test_concern_documents_are_independent_snapshots(self):
        first = self.items.write_concern
        second = self.items.write_concern
        first.document["w"] = "majority"
        self.assertEqual(first.document, {})
        self.assertEqual(second.document, {})
        read = self.items.read_concern.document
        read["level"] = "majority"
        self.assertEqual(self.items.read_concern.document, {})

    def test_concurrent_database_selection_keeps_public_identity_not_a_private_cache(self):
        start = threading.Barrier(8)
        def select(_):
            start.wait(timeout=10)
            database = self.client.concurrent
            return database, database.get_collection("items")
        with ThreadPoolExecutor(max_workers=8) as pool:
            selected = list(pool.map(select, range(8)))
        for database, collection in selected:
            self.assertIs(database.client, self.client)
            self.assertIs(collection.database, database)
            self.assertEqual(database.name, "concurrent")
            self.assertEqual(collection.full_name, "concurrent.items")
            self.assertEqual(database, selected[0][0])
        self.assertEqual(self.client.list_database_names(), [])
        selected[0][1].insert_one({"_id": 1})
        for _, collection in selected:
            self.assertEqual(collection.find_one({}), {"_id": 1})

    def test_shared_collection_concurrent_writes_do_not_lose_updates(self):
        self.items.insert_one({"_id": 1, "count": 0})
        for operation in ("update_one", "update_many", "replace_one"):
            with self.subTest(operation=operation):
                self.items.update_one({"_id": 1}, {"$set": {"count": 0}})
                start = threading.Barrier(2)
                def write(worker):
                    start.wait(timeout=10)
                    for _ in range(12):
                        change = {"_id": 1, "count": worker} if operation == "replace_one" else {"$inc": {"count": 1}}
                        result = getattr(self.items, operation)({"_id": 1}, change)
                        if result.matched_count != 1:
                            raise AssertionError("concurrent write lost its selection")
                    return worker
                with ThreadPoolExecutor(max_workers=2) as pool:
                    self.assertEqual(list(pool.map(write, range(2))), [0, 1])
                count = self.items.find_one({"_id": 1})["count"]
                if operation == "replace_one":
                    self.assertIn(count, (0, 1))
                else:
                    self.assertEqual(count, 24)

    @unittest.skipUnless(sys.platform == "darwin" or sys.platform.startswith("linux"),
                         "native shared-root process locks require Linux or macOS")
    def test_six_spawned_writers_preserve_every_acknowledged_document(self):
        self.client.app.create_collection("items")
        self.client.close()
        context = multiprocessing.get_context("spawn")
        start = context.Event()
        ready = context.Queue()
        processes = []
        try:
            for worker in range(6):
                process = context.Process(target=_writer, args=(self.root.name, worker, start, ready))
                process.start()
                processes.append(process)
            self.assertEqual({ready.get(timeout=20) for _ in processes}, set(range(6)))
            start.set()
            deadline = time.monotonic() + 60
            for process in processes:
                process.join(max(0, deadline - time.monotonic()))
            self.assertTrue(all(not process.is_alive() for process in processes), "writer deadline exceeded")
            self.assertEqual([process.exitcode for process in processes], [0] * 6)
        finally:
            start.set()
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join(5)
                if process.is_alive():
                    process.kill()
                    process.join(5)
            ready.close()
            ready.join_thread()
        with briskdb.MongoClient(self.root.name) as reopened:
            records = list(reopened.app.items.find({}))
            self.assertEqual(len(records), 300)
            self.assertEqual(sorted(row["count"] for row in records), list(range(300)))
            self.assertEqual(len({row["_id"] for row in records}), 300)


class AsyncUpstreamCommonApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_dotted_children_keep_async_reads_names_and_input_snapshots(self):
        with tempfile.TemporaryDirectory() as root:
            client = briskdb.AsyncMongoClient(root, shards=2)
            try:
                database = client.app
                parent = database.users
                child = parent.archive
                self.assertIsNot(child, parent)
                self.assertEqual((child.name, child.full_name), ("users.archive", "app.users.archive"))
                self.assertEqual(parent["archive"], child)
                self.assertEqual(parent["_private"].name, "users._private")
                self.assertEqual(database["users._private"], parent["_private"])
                with self.assertRaises(AttributeError):
                    _ = parent._private
                with self.assertRaises(AttributeError):
                    _ = database._private
                await parent.insert_one({"_id": "parent"})
                await child.insert_one({"_id": "child"})
                self.assertEqual(await parent.find_one({}), {"_id": "parent"})
                self.assertEqual(await child.find_one({}), {"_id": "child"})
                query = {"_id": "child"}
                cursor = child.find(query)
                query["_id"] = "changed"
                self.assertEqual(await cursor.to_list(), [{"_id": "child"}])
                with self.assertRaises(TypeError):
                    parent.find_oni({})
                with self.assertRaises(TypeError):
                    database.pingg()
            finally:
                await client.close()

    async def test_shared_async_collection_serializes_competing_increments(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2, socketTimeoutMS=10000) as client:
                items = client.app.items
                await items.insert_one({"_id": 1, "count": 0})
                start = asyncio.Event()
                async def write():
                    await start.wait()
                    for _ in range(12):
                        result = await items.update_one({"_id": 1}, {"$inc": {"count": 1}})
                        self.assertEqual(result.matched_count, 1)
                tasks = [asyncio.create_task(write()) for _ in range(2)]
                start.set()
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=30)
                self.assertEqual((await items.find_one({"_id": 1}))["count"], 24)


if __name__ == "__main__":
    unittest.main()
