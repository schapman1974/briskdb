"""Real async PyMongo scenarios with explicit legacy TinyMongo differences."""

import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from pymongo import IndexModel, InsertOne
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.errors import InvalidOperation, OperationFailure
from pymongo.monitoring import CommandListener
from pymongo.write_concern import WriteConcern

import briskdb


class Activity(CommandListener):
    def __init__(self):
        self.names = []
        self.pending = 0

    def started(self, event):
        self.names.append(event.command_name)
        self.pending += 1

    def succeeded(self, event):
        self.pending -= 1

    def failed(self, event):
        self.pending -= 1


class UpstreamAsyncApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.activity = Activity()
        self.client = briskdb.AsyncMongoClient(self.root.name, shards=2, event_listeners=[self.activity])
        self.addAsyncCleanup(self.client.close)
        self.items = self.client.app.items

    async def test_async_crud_metadata_and_idempotent_close(self):
        result = await self.items.insert_many([{"_id": n, "name": name, "rank": rank}
                                               for n, name, rank in [(1, "one", 3), (2, "two", 1), (3, "three", 2)]])
        self.assertEqual(result.inserted_ids, [1, 2, 3])
        self.assertEqual(await self.items.count_documents({}), 3)
        cursor = self.items.find({}, {"name": 1, "_id": 0})
        self.assertIsInstance(cursor, AsyncCursor)
        self.assertEqual(await cursor.sort("rank").skip(1).limit(1).to_list(), [{"name": "three"}])
        self.assertEqual((await self.items.update_one({"_id": 2}, {"$set": {"rank": 4}})).matched_count, 1)
        self.assertEqual(await self.items.find_one({"_id": 2}, {"rank": 1}), {"_id": 2, "rank": 4})
        self.assertEqual((await self.items.delete_one({"_id": 1})).deleted_count, 1)
        self.assertEqual(await self.client.list_database_names(), ["app"])
        self.assertTrue((await self.client.server_info())["version"].endswith("-briskdb"))
        self.assertFalse(callable(self.client.supports))
        await self.client.close()
        await self.client.close()
        with self.assertRaises(InvalidOperation):
            await self.items.find_one({})

    async def test_database_listing_names_only_and_drop_keep_metadata_gap_explicit(self):
        await self.items.insert_one({"_id": 1})
        await self.client.zeta.events.insert_one({"_id": 2})
        metadata = await self.client.list_databases(nameOnly=True)
        self.assertIsInstance(metadata, AsyncCommandCursor)
        self.assertFalse(hasattr(metadata, "sort"))
        self.assertEqual(sorted(await metadata.to_list(), key=lambda row: row["name"]),
                         [{"name": "app"}, {"name": "zeta"}])
        with self.assertRaises(OperationFailure) as caught:
            await self.client.list_databases()
        self.assertEqual(caught.exception.code, 115)
        self.assertIsNone(await self.client.drop_database(self.client.app))
        self.assertEqual(await self.client.list_database_names(), ["zeta"])
        self.assertIsNone(await self.client.drop_database("zeta"))
        self.assertIsNone(await self.client.drop_database("missing"))
        with self.assertRaises(TypeError):
            await self.client.drop_database(object())

    async def test_find_is_lazy_and_clones_execute_independent_wire_queries(self):
        await self.items.insert_many([{"_id": 1, "n": 2}, {"_id": 2, "n": 1}])
        self.activity.names.clear()
        cursor = self.items.find({}).sort("n")
        clone = cursor.clone()
        self.assertEqual(self.activity.names, [])
        self.assertEqual(await cursor.next(), {"_id": 2, "n": 1})
        self.assertEqual(await cursor.to_list(), [{"_id": 1, "n": 2}])
        self.assertFalse(hasattr(cursor, "try_next"))
        with self.assertRaises(StopAsyncIteration):
            await cursor.next()
        self.assertEqual(self.activity.names.count("find"), 1)
        self.assertEqual([row async for row in clone], [{"_id": 2, "n": 1}, {"_id": 1, "n": 2}])
        self.assertEqual(self.activity.names.count("find"), 2)
        await clone.rewind()
        self.assertEqual(await clone.to_list(1), [{"_id": 2, "n": 1}])
        with self.assertRaises(ValueError):
            await clone.to_list(0)
        self.assertEqual(await clone.to_list(), [{"_id": 1, "n": 2}])

    async def test_event_loop_progresses_during_a_real_pending_insert(self):
        await self.client.admin.command("ping")
        ticks = 0
        finished = asyncio.Event()

        async def ticker():
            nonlocal ticks
            while not finished.is_set():
                if self.activity.pending:
                    ticks += 1
                await asyncio.sleep(0)

        task = asyncio.create_task(ticker())
        try:
            result = await asyncio.wait_for(self.items.insert_many(
                [{"_id": n, "payload": "x" * 1024} for n in range(256)]), timeout=15)
            self.assertEqual(result.inserted_ids, list(range(256)))
            self.assertGreater(ticks, 0)
            self.assertEqual(self.activity.pending, 0)
        finally:
            finished.set()
            await task

    async def test_cursor_validation_pagination_and_returned_value_isolation(self):
        await self.items.insert_many([{"_id": n, "profile": {"name": "Ada"}} for n in range(1, 5)])
        cursor = self.items.find({}, sort=[("_id", 1)], skip=1, limit=-1)
        self.assertEqual(await cursor.to_list(), [{"_id": 2, "profile": {"name": "Ada"}}])
        self.assertFalse(cursor.alive)
        self.assertFalse(hasattr(cursor, "count"))
        self.assertFalse(hasattr(cursor, "paginate"))
        self.assertFalse(hasattr(cursor, "has_next"))
        with self.assertRaises(InvalidOperation):
            cursor.sort("_id")
        with self.assertRaises(ValueError):
            self.items.find({}).skip(-1)
        with self.assertRaises(TypeError):
            self.items.find({}).limit(1.5)
        with self.assertRaises(ValueError):
            await self.items.find({}).to_list(-1)
        self.assertEqual(len(await self.items.find({}).limit(0).to_list()), 4)
        self.assertEqual(len(await self.items.find({}).to_list(True)), 1)
        # The driver accepts bool as int; the wire frontend rejects BSON bool.
        with self.assertRaises(OperationFailure) as caught:
            await self.items.find({}).skip(True).to_list()
        self.assertEqual(caught.exception.code, 2)
        cursor = self.items.find({}).sort("_id")
        (await cursor.next())["profile"]["name"] = "changed"
        await cursor.close()
        self.assertEqual([row["_id"] for row in await cursor.to_list()], [2, 3, 4])
        await cursor.rewind()
        self.assertEqual((await cursor.to_list())[0]["profile"], {"name": "Ada"})
        await cursor.rewind()
        listed = await cursor.to_list()
        listed[0]["profile"]["name"] = "changed again"
        await cursor.rewind()
        self.assertEqual((await cursor.next())["profile"], {"name": "Ada"})
        with self.assertRaises(OperationFailure):
            await self.items.find({"$unknown": 1}).to_list()
        self.assertEqual(await self.items.count_documents({}), 4)

    async def test_modern_async_collection_mutation_index_and_bulk_surface(self):
        self.assertEqual((await self.items.insert_one({"_id": 1, "kind": "a"})).inserted_id, 1)
        await self.items.insert_many([{"_id": 2, "kind": "a"}, {"_id": 3, "kind": "b"}])
        self.assertEqual((await self.items.update_many({}, {"$set": {"seen": True}})).matched_count, 3)
        self.assertEqual((await self.items.update_many({"kind": "a"}, {"$inc": {"score": 1}})).matched_count, 2)
        self.assertEqual((await self.items.replace_one({"_id": 3}, {"kind": "c", "score": 5})).matched_count, 1)
        self.assertEqual((await self.items.find_one_and_update({"_id": 1}, {"$set": {"kind": "changed"}}))["kind"], "a")
        self.assertEqual((await self.items.find_one_and_replace({"_id": 2}, {"kind": "replaced"}))["kind"], "a")
        self.assertEqual(await self.items.find_one_and_delete({"kind": {"$in": ["changed", "replaced"]}},
                         projection={"kind": 1, "_id": 0}, sort=[("kind", 1)]), {"kind": "changed"})
        self.assertEqual(await self.items.estimated_document_count(), 2)
        self.assertEqual(await self.items.create_index("kind"), "kind_1")
        indexes = await self.items.list_indexes()
        self.assertIsInstance(indexes, AsyncCommandCursor)
        self.assertEqual({row["name"] async for row in indexes}, {"_id_", "kind_1"})
        self.assertEqual((await self.items.index_information())["kind_1"]["key"], [("kind", 1)])
        self.assertEqual(set(await self.items.distinct("kind")), {"replaced", "c"})
        await self.items.drop_index("kind_1")
        self.assertEqual(await self.items.create_indexes([IndexModel("kind", name="batch_index")]), ["batch_index"])
        with self.assertRaises(TypeError):
            await self.items.create_indexes([object()])
        self.assertEqual((await self.items.delete_one({"_id": 1})).deleted_count, 0)
        self.assertEqual((await self.items.delete_many({"kind": "replaced"})).deleted_count, 1)
        self.assertEqual((await self.items.bulk_write([InsertOne({"_id": 4, "kind": "bulk"})])).inserted_count, 1)
        self.assertIsNone(await self.items.drop())
        self.assertEqual((await self.client.app.drop_collection("missing"))["code"], 26)

    async def test_database_options_private_names_and_unsupported_calls(self):
        database = self.client.get_database("app")
        items = database.get_collection("items")
        self.assertEqual((database.name, items.full_name), ("app", "app.items"))
        self.assertIsNot(items.with_options(), items)
        self.assertEqual(items.with_options().full_name, items.full_name)
        self.assertEqual((items.write_concern.document, items.read_concern.document), ({}, {}))
        for handle in [self.client, database]:
            with self.assertRaises(AttributeError):
                handle.__getattr__("_missing")
        self.assertFalse(callable(self.client.capabilities))
        # CPython 3.9 reports AttributeError for a missing __aenter__; newer
        # versions report TypeError. Match the interpreter's ordinary-object
        # rejection, not a version-specific exception chosen by BriskDB.
        try:
            async with object():
                self.fail("ordinary object unexpectedly became an async context manager")
        except (TypeError, AttributeError) as error:
            missing_context_error = type(error)
        with self.assertRaises(missing_context_error):
            async with self.client.context:
                self.fail("PyMongo database unexpectedly became an async context manager")
        self.assertEqual(await database.command("ping"), {"ok": 1.0})
        with self.assertRaises(OperationFailure):
            await database.command("serverStatus")
        self.assertEqual(await (await items.aggregate([])).to_list(), [])
        for handle in [items, database, self.client]:
            with self.assertRaises(OperationFailure):
                await handle.watch()
        class Concern:
            document = {"w": 2}
        with self.assertRaises(TypeError):
            items.with_options(write_concern=Concern())
        with self.assertRaises(OperationFailure):
            await items.with_options(write_concern=WriteConcern(w=2)).insert_one({"_id": 1})
        self.assertEqual(await items.count_documents({}), 0)
        folder = Path(self.root.name) / "invalid"
        with self.assertRaises(ValueError):
            briskdb.AsyncMongoClient(folder=folder, backend="invalid-pymongo")
        self.assertFalse(folder.exists())

    async def test_cancelled_direct_client_close_drains_native_release(self):
        await self.items.insert_one({"_id": 1})
        entered, release = threading.Event(), threading.Event()
        original = self.client._briskdb_release

        def delayed():
            entered.set()
            if not release.wait(timeout=10):
                raise RuntimeError("cleanup barrier timed out")
            return original()

        with mock.patch.object(self.client, "_briskdb_release", delayed):
            closing = asyncio.create_task(self.client.close())
            try:
                self.assertTrue(await asyncio.to_thread(entered.wait, 5))
                closing.cancel()
                await asyncio.sleep(0)
                self.assertFalse(closing.done())
            finally:
                release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(closing, 10)
        async with briskdb.AsyncMongoClient(self.root.name) as reopened:
            self.assertEqual(await reopened.app.items.find_one({"_id": 1}), {"_id": 1})


if __name__ == "__main__":
    unittest.main()
