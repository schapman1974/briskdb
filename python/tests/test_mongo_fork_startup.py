"""Fork refusal must precede inherited locks, constructors and executors."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
import warnings

import pymongo

import briskdb
from briskdb import _mongo_runtime as runtime, mongo, patching


def locked_fork_probe(mode):
    with ExitStack() as stack:
        root = stack.enter_context(tempfile.TemporaryDirectory())
        client = stack.enter_context(briskdb.MongoClient(root, shards=2))
        client.app.items.insert_one({"_id": "parent"})
        absent = Path(root) / "child-must-not-create"
        scope = briskdb.patch(folder=root)
        if mode == "patch":
            configured = stack.enter_context(scope)
            lock = patching._lock
            operations = [
                ("patched-sync", configured),
                ("patched-async", pymongo.AsyncMongoClient),
                ("scope-enter", briskdb.patch(folder=absent).__enter__),
                ("scope-exit", lambda: scope.__exit__(None, None, None)),
            ]
        else:
            lock = runtime._lock
            if mode == "runtime-empty":
                client.close()
                assert not runtime._stores
            operations = [
                ("acquire", lambda: runtime.acquire(absent, 2)),
                ("sync", lambda: briskdb.MongoClient(absent, shards=2)),
                ("async", lambda: briskdb.AsyncMongoClient(absent, shards=2)),
            ]
        held, release = threading.Event(), threading.Event()

        def hold_lock():
            with lock:
                held.set()
                release.wait(20)

        holder = threading.Thread(target=hold_lock, daemon=True)
        holder.start()
        assert held.wait(5), "parent thread did not acquire the lock"
        read_fd, write_fd = os.pipe()
        child = None
        waited = False
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                child = os.fork()
            if child == 0:
                os.close(read_fd)
                results = []
                try:
                    for name, operation in operations:
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
                    os._exit(0)  # Never clean up a parent's native handles.
            os.close(write_fd)
            write_fd = None
            ready, _, _ = select.select([read_fd], [], [], 5)
            assert ready, f"{mode}: inherited lock blocked fork refusal"
            results = json.loads(os.read(read_fd, 4096))
            _, status = os.waitpid(child, 0)
            waited = True
            assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
            assert results == [[name, True] for name, _ in operations], results
        finally:
            os.close(read_fd)
            if write_fd is not None:
                os.close(write_fd)
            if child is not None and not waited:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.waitpid(child, 0)
            release.set()
            holder.join(5)
            assert not holder.is_alive(), "parent lock holder did not finish"
        assert not absent.exists()
        if mode == "patch":
            assert pymongo.MongoClient is configured
            with configured() as peer:
                assert peer.app.items.find_one({}) == {"_id": "parent"}
        with briskdb.MongoClient(root) as peer:
            assert list(peer.app.items.find({})) == [{"_id": "parent"}]


class MongoForkStartupTests(unittest.TestCase):
    def test_inherited_startup_and_scope_lifecycle_reject_before_shared_locks(self):
        with briskdb.patch(shards=2) as configured:
            scope = briskdb.patch()
            with mock.patch.object(runtime, "_pid", os.getpid() + 1):
                with mock.patch.object(runtime, "_lock") as store_lock, mock.patch.object(patching, "_lock") as patch_lock:
                    store_lock.__enter__.side_effect = AssertionError("store lock entered")
                    patch_lock.__enter__.side_effect = AssertionError("patch lock entered")
                    with mock.patch.object(mongo, "_configuration", side_effect=AssertionError("driver configuration entered")):
                        for operation in [lambda: runtime.acquire(None, 2), briskdb.MongoClient,
                                          briskdb.AsyncMongoClient, configured, pymongo.AsyncMongoClient,
                                          scope.__enter__, lambda: scope.__exit__(None, None, None)]:
                            with self.subTest(operation=operation):
                                with self.assertRaisesRegex(RuntimeError, "cannot be inherited after fork"):
                                    operation()
                    store_lock.__enter__.assert_not_called()
                    patch_lock.__enter__.assert_not_called()
            with configured() as parent:
                self.assertEqual(parent.app.command("ping"), {"ok": 1.0})

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_real_fork_rejects_live_empty_and_patch_startup_with_parent_locks_held(self):
        for mode in ("runtime-live", "runtime-empty", "patch"):
            with self.subTest(mode=mode):
                result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--locked-fork-probe", mode],
                                        capture_output=True, text=True, timeout=25)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class AsyncMongoForkStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_inherited_async_scope_rejects_before_executor_or_patch_lock(self):
        async with briskdb.patch(shards=2):
            scope = briskdb.patch()
            with mock.patch.object(runtime, "_pid", os.getpid() + 1):
                with mock.patch.object(patching.asyncio, "to_thread", side_effect=AssertionError("executor entered")) as executor:
                    with mock.patch.object(patching, "_lock") as patch_lock:
                        patch_lock.__enter__.side_effect = AssertionError("patch lock entered")
                        for operation in [scope.__aenter__, lambda: scope.__aexit__(None, None, None)]:
                            with self.assertRaisesRegex(RuntimeError, "cannot be inherited after fork"):
                                await operation()
                        executor.assert_not_called()
                        patch_lock.__enter__.assert_not_called()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--locked-fork-probe":
        locked_fork_probe(sys.argv[2])
    else:
        unittest.main()
