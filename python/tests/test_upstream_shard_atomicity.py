"""Native wire contention scenarios, not TinyMongo private lock-hook tests."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from bson import BSON
from pymongo import ReturnDocument
from pymongo.monitoring import CommandListener

import briskdb


def hold_sqlite_write_lock(path, signal):
    """Use another process: the wheel and stdlib bundle different SQLite builds."""
    connection = sqlite3.connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        signal.send("locked")
        if not signal.poll(15) or signal.recv() != "release":
            raise RuntimeError("parent did not release the owned test lock")
    finally:
        connection.rollback()
        connection.close()
        signal.close()


class UpdateStarted(CommandListener):
    def __init__(self):
        self.target = None
        self.attempted = threading.Event()

    def started(self, event):
        if event.command_name == "update" and self.target is not None:
            if event.command["updates"][0]["q"].get("_id") == self.target:
                self.attempted.set()

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


class UpstreamShardAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.activity = UpdateStarted()
        self.client = briskdb.MongoClient(self.root.name, shards=2, event_listeners=[self.activity], socketTimeoutMS=5000)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def assert_reopen_preserves(self):
        expected = list(self.items.find({}).sort("_id"))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(list(reader.app.items.find({}).sort("_id")), expected)

    def seed_and_locate_shards(self):
        self.items.insert_many([{"_id": number, "value": 0} for number in range(64)])
        owners = []
        for shard in range(2):
            # Inspect only this test's owned SQLite root. No production private
            # Python routing/lock functions are exposed or monkeypatched.
            path = Path(self.root.name) / "shards" / f"{shard:04}.sqlite"
            with sqlite3.connect(str(path)) as connection:
                owners.append([BSON(row[0]).decode()["_id"] for row in connection.execute(
                    "SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")])
            connection.close()
        self.assertTrue(all(owners))
        self.assertEqual(sorted(owners[0] + owners[1]), list(range(64)))
        return owners

    def test_conditional_replace_and_version_update_have_exactly_one_winner(self):
        self.items.insert_many([{"_id": number, "version": 1, "phase": "original"} for number in range(24)])
        with ThreadPoolExecutor(max_workers=2) as pool:
            for number in range(24):
                barrier = threading.Barrier(2)

                def replace():
                    barrier.wait(timeout=3)
                    return self.items.replace_one({"_id": number, "version": 1},
                                                  {"version": 2, "phase": "replacement"})

                def update():
                    barrier.wait(timeout=3)
                    return self.items.update_one({"_id": number, "version": 1},
                                                 {"$set": {"version": 2, "phase": "competitor", "competitor": True}})

                replaced, updated = pool.submit(replace), pool.submit(update)
                replaced, updated = replaced.result(timeout=5), updated.result(timeout=5)
                self.assertEqual(replaced.matched_count + updated.matched_count, 1)
                self.assertEqual(replaced.modified_count + updated.modified_count, 1)
                expected = {"_id": number, "version": 2, "phase": "replacement"}
                if updated.matched_count:
                    expected.update(phase="competitor", competitor=True)
                self.assertEqual(self.items.find_one({"_id": number}), expected)
        self.assert_reopen_preserves()

    def test_find_update_returns_each_atomic_before_and_after_counter_image(self):
        for after in [False, True]:
            with self.subTest(after=after):
                self.items.insert_one({"_id": str(after), "counter": 0})
                barrier = threading.Barrier(4)

                def increment():
                    barrier.wait(timeout=3)
                    return [self.items.find_one_and_update(
                        {"_id": str(after)}, {"$inc": {"counter": 1}},
                        return_document=ReturnDocument.AFTER if after else ReturnDocument.BEFORE)["counter"]
                        for _ in range(24)]

                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(increment) for _ in range(4)]
                    observed = [value for future in futures for value in future.result(timeout=10)]
                self.assertEqual(sorted(observed), list(range(int(after), 96 + int(after))))
                self.assertEqual(self.items.find_one({"_id": str(after)})["counter"], 96)
        self.assert_reopen_preserves()

    def test_find_replace_and_delete_return_each_selected_preimage_once(self):
        for operation in ["replace", "delete"]:
            with self.subTest(operation=operation):
                self.items.delete_many({})
                self.items.insert_many([{"_id": number, "done": False} for number in range(48)])
                barrier = threading.Barrier(4)

                def consume():
                    barrier.wait(timeout=3)
                    seen = []
                    for _ in range(49):
                        if operation == "replace":
                            image = self.items.find_one_and_replace({"done": False}, {"done": True}, sort=[("_id", 1)])
                        else:
                            image = self.items.find_one_and_delete({"done": False}, sort=[("_id", 1)])
                        if image is None:
                            return seen
                        self.assertEqual(image, {"_id": image["_id"], "done": False})
                        seen.append(image["_id"])
                    self.fail("find-and-modify never exhausted its bounded input")

                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(consume) for _ in range(4)]
                    observed = [value for future in futures for value in future.result(timeout=10)]
                self.assertEqual(sorted(observed), list(range(48)))
                if operation == "replace":
                    self.assertEqual(list(self.items.find({}).sort("_id")),
                                     [{"_id": number, "done": True} for number in range(48)])
                else:
                    self.assertEqual(self.items.count_documents({}), 0)
        self.assert_reopen_preserves()

    def test_natural_and_sorted_find_modify_ignore_physical_shard_order(self):
        owners = self.seed_and_locate_shards()
        first, later = min(owners[1]), max(owners[0])
        self.assertLess(first, later)
        lower = next(value for value in owners[1] if value != first)
        higher = next(value for value in owners[0] if value != later)
        for number, group, rank in [(first, "natural", 1), (later, "natural", 2),
                                     (lower, "sorted", 1), (higher, "sorted", 10)]:
            self.items.update_one({"_id": number}, {"$set": {"group": group, "rank": rank}})
        before = self.items.find_one_and_replace({"group": "natural"}, {"group": "natural", "rank": 3})
        self.assertEqual(before["_id"], first)
        self.assertEqual(self.items.find_one({"_id": later})["rank"], 2)
        after = self.items.find_one_and_update({"group": "sorted"}, {"$set": {"selected": True}},
                                               sort=[("rank", -1)], return_document=ReturnDocument.AFTER)
        self.assertEqual(after["_id"], higher)
        self.assertIs(after["selected"], True)
        self.assertNotIn("selected", self.items.find_one({"_id": lower}))
        self.assert_reopen_preserves()

    def test_locked_shard_does_not_block_an_independent_exact_id_write(self):
        owners = self.seed_and_locate_shards()
        blocked_id, independent_id = owners[0][0], owners[1][0]
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        blocker = context.Process(target=hold_sqlite_write_lock,
                                  args=(str(Path(self.root.name) / "shards" / "0000.sqlite"), child))
        blocker.start()
        child.close()
        try:
            self.assertTrue(parent.poll(5), "external SQLite writer did not acquire its lock")
            self.assertEqual(parent.recv(), "locked")
            self.activity.target = blocked_id
            with ThreadPoolExecutor(max_workers=2) as pool:
                blocked = pool.submit(self.items.update_one, {"_id": blocked_id}, {"$inc": {"value": 1}})
                try:
                    self.assertTrue(self.activity.attempted.wait(3), "blocked wire update did not start")
                    with self.assertRaises(TimeoutError):
                        blocked.result(timeout=0.15)
                    independent = pool.submit(self.items.update_one, {"_id": independent_id}, {"$inc": {"value": 1}})
                    self.assertEqual(independent.result(timeout=3).modified_count, 1)
                    self.assertFalse(blocked.done())
                    self.assertEqual(self.items.find_one({"_id": independent_id})["value"], 1)
                finally:
                    parent.send("release")
                self.assertEqual(blocked.result(timeout=3).modified_count, 1)
        finally:
            parent.close()
            blocker.join(timeout=5)
            if blocker.is_alive():
                blocker.terminate()
                blocker.join(timeout=5)
        self.assertEqual(blocker.exitcode, 0)
        self.assertEqual(self.items.find_one({"_id": blocked_id})["value"], 1)
        self.assert_reopen_preserves()


if __name__ == "__main__":
    unittest.main()
