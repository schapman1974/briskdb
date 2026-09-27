"""Reject inherited local clients before the driver's network/topology work."""

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import warnings

import briskdb
from briskdb import mongo


def fork_probe():
    with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
        items = client.app.items
        items.insert_many([{"_id": number, "value": "parent"} for number in range(3)])
        assert items.find_one({"_id": 0}) == {"_id": 0, "value": "parent"}
        cursor = items.find({}).batch_size(1)
        assert next(cursor)["_id"] == 0
        read_fd, write_fd = os.pipe()
        with warnings.catch_warnings():
            # This deliberately probes the documented refusal of inherited
            # multithreaded handles; the child never owns native cleanup.
            warnings.simplefilter("ignore", DeprecationWarning)
            child = os.fork()
        if child == 0:
            os.close(read_fd)
            results = []
            try:
                for name, operation in [
                    ("find", lambda: items.find_one({"_id": 0})),
                    ("update", lambda: items.update_one({"_id": 0}, {"$set": {"value": "child"}})),
                    ("insert", lambda: items.insert_one({"_id": "child"})),
                    ("getMore", lambda: next(cursor)),
                    ("close", client.close),
                ]:
                    try:
                        operation()
                    except RuntimeError as error:
                        results.append((name, "cannot be inherited after fork" in str(error)))
                    except BaseException as error:
                        results.append((name, type(error).__name__))
                    else:
                        results.append((name, "unexpected success"))
                os.write(write_fd, json.dumps(results).encode())
            finally:
                os.close(write_fd)
                os._exit(0)
        os.close(write_fd)
        waited = False
        try:
            ready, _, _ = select.select([read_fd], [], [], 10)
            if not ready:
                raise AssertionError("inherited client operation did not reject promptly")
            results = json.loads(os.read(read_fd, 4096))
            _, status = os.waitpid(child, 0)
            waited = True
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
            assert results == [[name, True] for name in ["find", "update", "insert", "getMore", "close"]], results
            assert items.find_one({"_id": 0}) == {"_id": 0, "value": "parent"}
            assert items.count_documents({}) == 3
            assert [row["_id"] for row in cursor] == [1, 2]
        finally:
            os.close(read_fd)
            if not waited:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.waitpid(child, 0)
            cursor.close()


class MongoProcessOwnershipTests(unittest.TestCase):
    def test_sync_inherited_operations_reject_before_driver_topology(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            with briskdb.MongoClient(root, connect=False) as cold:
                items = client.app.items
                items.insert_one({"_id": 1})
                operations = [lambda: items.find_one({}), lambda: items.count_documents({}),
                              lambda: items.insert_one({"_id": 2}),
                              lambda: items.update_one({"_id": 1}, {"$set": {"value": 2}}),
                              lambda: items.delete_one({"_id": 1}), lambda: list(items.aggregate([])),
                              lambda: items.distinct("_id"), lambda: client.app.command("ping"),
                              client.app.list_collection_names, client.list_database_names, client.server_info,
                              lambda: cold.app.items.find_one({})]
                self.assertFalse(cold._opened)
                with mock.patch.object(client._briskdb_store, "pid", os.getpid() + 1):
                    with mock.patch.object(mongo._Client, "_get_topology", side_effect=AssertionError("driver topology entered")):
                        for operation in operations:
                            with self.assertRaisesRegex(RuntimeError, "cannot be inherited after fork"):
                                operation()
                self.assertFalse(cold._opened)
                self.assertEqual(list(items.find({})), [{"_id": 1}])

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_real_fork_rejects_warmed_reads_writes_and_getmore_and_preserves_parent(self):
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--fork-probe"],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class AsyncMongoProcessOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_inherited_operations_reject_before_driver_topology(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client, briskdb.AsyncMongoClient(root, connect=False) as cold:
                items = client.app.items
                await items.insert_one({"_id": 1})
                operations = [lambda: items.find_one({}), lambda: items.count_documents({}),
                              lambda: items.insert_one({"_id": 2}),
                              lambda: items.update_one({"_id": 1}, {"$set": {"value": 2}}),
                              lambda: items.delete_one({"_id": 1}), lambda: items.aggregate([]),
                              lambda: items.distinct("_id"), lambda: client.app.command("ping"),
                              client.app.list_collection_names, client.list_database_names, client.server_info,
                              lambda: cold.app.items.find_one({})]
                self.assertFalse(cold._opened)
                with mock.patch.object(client._briskdb_store, "pid", os.getpid() + 1):
                    with mock.patch.object(mongo._AsyncClient, "_get_topology", side_effect=AssertionError("driver topology entered")):
                        for operation in operations:
                            with self.assertRaisesRegex(RuntimeError, "cannot be inherited after fork"):
                                await operation()
                self.assertFalse(cold._opened)
                self.assertEqual(await items.find({}).to_list(), [{"_id": 1}])


if __name__ == "__main__":
    if sys.argv[1:] == ["--fork-probe"]:
        fork_probe()
    else:
        unittest.main()
