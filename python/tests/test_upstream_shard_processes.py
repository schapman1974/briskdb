"""Spawned native writers and WAL snapshots, not TinyMongo file-lock hooks."""

import multiprocessing
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from bson import BSON
from pymongo.monitoring import CommandListener

import briskdb


class InsertStarted(CommandListener):
    def __init__(self, signal, identifier):
        self.signal = signal
        self.identifier = identifier

    def started(self, event):
        if event.command_name == "insert":
            self.signal.send(("attempted", self.identifier))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def insert_in_process(root, identifier, signal):
    try:
        with briskdb.MongoClient(root, shards=2, event_listeners=[InsertStarted(signal, identifier)],
                                socketTimeoutMS=10000) as client:
            client.app.items.count_documents({})
            signal.send(("ready", identifier))
            if not signal.poll(15) or signal.recv() != "start":
                raise RuntimeError("parent did not start the owned writer")
            client.app.items.insert_one({"_id": identifier, "state": "inserted"})
            signal.send(("inserted", identifier))
    finally:
        signal.close()


def hold_transaction(path, dirty, signal):
    connection = sqlite3.connect(path, timeout=1, isolation_level=None)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        connection.execute("BEGIN IMMEDIATE")
        if dirty:
            # Deliberately invalid uncommitted bytes must be invisible to a
            # reader. Roll back, never commit, this owned test-only damage.
            changed = connection.execute(
                "UPDATE briskdb_documents_v1 SET document_bson=zeroblob(length(document_bson))")
            assert changed.rowcount > 0
        signal.send("locked")
        if not signal.poll(15) or signal.recv() != "release":
            raise RuntimeError("parent did not release the owned transaction")
    finally:
        connection.rollback()
        connection.close()
        signal.close()


def stop_owned_processes(processes):
    for process in processes:
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)


class UpstreamShardProcessTests(unittest.TestCase):
    def setUp(self):
        # Keep the objects alive: completed PyMongo monitors may disappear while
        # a new client starts, and CPython can immediately reuse their id().
        self.threads_before = set(threading.enumerate())
        self.children_before = {process.pid for process in multiprocessing.active_children()}
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2, socketTimeoutMS=5000)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items
        self.context = multiprocessing.get_context("spawn")

    def seed_shards(self):
        self.items.insert_many([{"_id": number, "state": "committed"} for number in range(64)])
        owners = []
        for shard in range(2):
            path = Path(self.root.name) / "shards" / f"{shard:04}.sqlite"
            connection = sqlite3.connect(str(path))
            try:
                owners.append([BSON(row[0]).decode()["_id"] for row in connection.execute(
                    "SELECT document_bson FROM briskdb_documents_v1 ORDER BY natural_order")])
            finally:
                connection.close()
        self.assertTrue(all(owners))
        self.assertEqual(sorted(owners[0] + owners[1]), list(range(64)))
        return owners

    def receive(self, signal, expected):
        self.assertTrue(signal.poll(5), f"owned process did not report {expected!r}")
        self.assertEqual(signal.recv(), expected)

    def test_spawned_writers_on_different_shards_progress_independently(self):
        owners = self.seed_shards()
        identifiers = [owners[0][0], owners[1][0]]
        self.assertEqual(self.items.delete_many({"_id": {"$in": identifiers}}).deleted_count, 2)
        processes, signals = [], []
        lock_signal = None
        try:
            for identifier in identifiers:
                parent, child = self.context.Pipe()
                process = self.context.Process(target=insert_in_process, args=(self.root.name, identifier, child))
                process.start()
                child.close()
                processes.append(process)
                signals.append(parent)
                self.receive(parent, ("ready", identifier))
            lock_signal, child = self.context.Pipe()
            holder = self.context.Process(target=hold_transaction,
                args=(str(Path(self.root.name) / "shards" / "0000.sqlite"), False, child))
            holder.start()
            child.close()
            processes.append(holder)
            self.receive(lock_signal, "locked")
            for signal in signals:
                signal.send("start")
            for signal, identifier in zip(signals, identifiers):
                self.receive(signal, ("attempted", identifier))
            self.receive(signals[1], ("inserted", identifiers[1]))
            self.assertFalse(signals[0].poll(0.15), "locked-shard writer completed before release")
            self.assertTrue(processes[0].is_alive())
            lock_signal.send("release")
            lock_signal.close()
            lock_signal = None
            self.receive(signals[0], ("inserted", identifiers[0]))
        finally:
            if lock_signal is not None:
                try:
                    lock_signal.send("release")
                except (BrokenPipeError, EOFError):
                    pass
                lock_signal.close()
            stop_owned_processes(processes)
            for signal in signals:
                signal.close()
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.count_documents({}), 64)
            for identifier in identifiers:
                self.assertEqual(reader.app.items.find_one({"_id": identifier}), {"_id": identifier, "state": "inserted"})

    def test_wal_readers_never_observe_uncommitted_raw_bson_damage(self):
        owners = self.seed_shards()
        expected = {"_id": owners[0][0], "state": "committed"}
        parent, child = self.context.Pipe()
        process = self.context.Process(target=hold_transaction,
            args=(str(Path(self.root.name) / "shards" / "0000.sqlite"), True, child))
        process.start()
        child.close()
        try:
            self.receive(parent, "locked")
            self.assertEqual(self.items.find_one({"_id": expected["_id"]}), expected)
            self.assertEqual(self.items.count_documents({}), 64)
        finally:
            try:
                parent.send("release")
            except (BrokenPipeError, EOFError):
                pass
            parent.close()
            stop_owned_processes([process])
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(self.items.find_one({"_id": expected["_id"]}), expected)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.find_one({"_id": expected["_id"]}), expected)
            self.assertEqual(reader.app.items.count_documents({}), 64)

    def test_real_driver_uses_background_threads_without_python_worker_processes(self):
        self.items.insert_one({"_id": "foreground"})
        self.assertEqual(self.items.find_one({"_id": "foreground"}), {"_id": "foreground"})
        self.assertTrue(any(thread not in self.threads_before and thread.is_alive() and thread.name.startswith("pymongo")
                            for thread in threading.enumerate()))
        self.assertEqual({process.pid for process in multiprocessing.active_children()}, self.children_before)
        self.client.close()
        self.assertEqual({process.pid for process in multiprocessing.active_children()}, self.children_before)
        # Native Tokio and driver pool/monitor threads are intentional. The
        # source's stronger no-background-threads contract does not apply.


if __name__ == "__main__":
    unittest.main()
