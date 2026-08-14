import subprocess
import sys
import tempfile
import threading
import time
import unittest

import briskdb


class EmbeddedBriskDbTests(unittest.TestCase):
    def test_validated_configuration_reaches_the_engine(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            config = briskdb.Config(
                shards=2,
                connections_per_shard=1,
                queue_capacity_per_shard=8,
                max_result_rows=500,
                max_result_bytes=1024 * 1024,
                request_timeout_ms=5000,
                shutdown_grace_ms=5000,
            )
            database = briskdb.open(data_dir, config=config)
            self.assertEqual(database.config.shards, 2)
            status = database.session().status()
            self.assertEqual(status["connections_per_shard"], 1)
            self.assertEqual(status["queue_capacity_per_shard"], 8)
            self.assertEqual(status["max_result_rows"], 500)
            self.assertEqual(status["request_timeout_ms"], 5000)
            database.close()

            with self.assertRaisesRegex(ValueError, "either shards or config"):
                briskdb.open(data_dir, shards=2, config=config)

            with self.assertRaisesRegex(RuntimeError, "maximum result rows"):
                briskdb.Config(max_result_rows=0)

    def test_basic_write_read_checkpoint_close_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            self.assertEqual(database.shard_count, 2)
            self.assertEqual(database.state, "running")

            session = database.session(routing_key="account-1")
            self.assertEqual(session.routing_key, "account-1")
            self.assertEqual(
                session.migrate(
                    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
                ),
                [0, 1],
            )
            write = session.execute(
                "INSERT INTO notes (id, body) VALUES (?1, ?2)", [1, "hello"]
            )
            self.assertEqual(write["rows_affected"], 1)
            self.assertIsNone(write["generated_key"])

            result = session.query(
                "SELECT id, body FROM notes WHERE id = ?1", [1]
            )
            self.assertEqual(
                result["columns"],
                [
                    {"name": "id", "type": "unknown"},
                    {"name": "body", "type": "unknown"},
                ],
            )
            self.assertEqual(result["rows"], [(1, "hello")])
            self.assertEqual(session.status()["shards"], 2)
            self.assertEqual(len(database.checkpoint()["shards"]), 2)

            session.close()
            self.assertTrue(session.closed)
            self.assertFalse(database.close()["forced"])
            self.assertTrue(database.closed)
            self.assertTrue(database.close()["already_closed"])

            reopened = briskdb.Database(data_dir, shards=2)
            reopened_session = reopened.session(routing_key="account-1")
            self.assertEqual(
                reopened_session.query(
                    "SELECT body FROM notes WHERE id = ?1", [1]
                )["rows"],
                [("hello",)],
            )
            reopened_session.close()
            reopened.close()

    def test_use_after_close_is_safe_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            session = database.session(routing_key="route")
            session.close()
            session.close()
            with self.assertRaisesRegex(RuntimeError, "session is closed"):
                session.query("SELECT 1")

            database.close()
            with self.assertRaisesRegex(RuntimeError, "database is closed"):
                database.session()

    def test_native_query_releases_the_gil(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            session = database.session(routing_key="route")
            stop = threading.Event()
            tick_lock = threading.Lock()
            ticks = 0

            def ticker() -> None:
                nonlocal ticks
                while not stop.is_set():
                    with tick_lock:
                        ticks += 1

            thread = threading.Thread(target=ticker)
            thread.start()
            time.sleep(0.01)
            with tick_lock:
                before = ticks
            result = session.query(
                "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL "
                "SELECT x + 1 FROM counter WHERE x < 1000000) "
                "SELECT sum(x) FROM counter"
            )
            with tick_lock:
                progressed = ticks - before
            stop.set()
            thread.join(timeout=5)

            self.assertEqual(result["rows"], [(500000500000,)])
            self.assertGreater(progressed, 100)
            session.close()
            database.close()

    def test_interpreter_shutdown_with_live_handles_does_not_abort(self) -> None:
        script = """
import briskdb
import tempfile

root = tempfile.mkdtemp()
database = briskdb.open(root, shards=2)
session = database.session(routing_key="shutdown")
session.query("SELECT 1")
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
