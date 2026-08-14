import datetime
import decimal
import math
import random
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

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

            with self.assertRaisesRegex(
                briskdb.InvalidArgumentError, "either shards or config"
            ):
                briskdb.open(data_dir, shards=2, config=config)

            with self.assertRaisesRegex(
                briskdb.InvalidArgumentError, "maximum result rows"
            ):
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
            with self.assertRaisesRegex(
                briskdb.FailedPreconditionError, "session is closed"
            ):
                session.query("SELECT 1")

            database.close()
            with self.assertRaisesRegex(
                briskdb.FailedPreconditionError, "database is closed"
            ):
                database.session()

    def test_sql_value_boundaries_and_binary_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            session = database.session(routing_key="values")

            exact_values = [
                None,
                -(2**63),
                2**63 - 1,
                -1.25,
                float("inf"),
                float("-inf"),
                "snowman ☃ and nul \x00",
                b"\x00\xffbinary",
            ]
            for value in exact_values:
                returned = session.query("SELECT ?1", [value])["rows"][0][0]
                self.assertEqual(returned, value)

            self.assertEqual(
                session.query("SELECT ?1", [bytearray(b"mutable")])["rows"],
                [(b"mutable",)],
            )
            self.assertEqual(
                session.query("SELECT ?1", [memoryview(b"view")])["rows"],
                [(b"view",)],
            )
            self.assertEqual(session.query("SELECT ?1", [True])["rows"], [(1,)])

            for value in [2**63, 2**64 - 1, 2**64, -(2**63) - 1]:
                with self.assertRaises(briskdb.NumericOutOfRangeError) as raised:
                    session.query("SELECT ?1", [value])
                self.assertEqual(raised.exception.code, "numeric_out_of_range")

            with self.assertRaises(briskdb.UnsupportedError):
                session.query("SELECT ?1", [float("nan")])
            with self.assertRaises(briskdb.UnsupportedError):
                session.query("SELECT ?1", [decimal.Decimal("12.3400")])
            with self.assertRaises(briskdb.UnsupportedError):
                session.query("SELECT ?1", [decimal.Decimal("NaN")])

            for value in [
                datetime.datetime.now(datetime.timezone.utc),
                uuid.UUID("00112233-4455-6677-8899-aabbccddeeff"),
            ]:
                with self.assertRaises(briskdb.TypeMismatchError):
                    session.query("SELECT ?1", [value])

            cyclic = []
            cyclic.append(cyclic)
            with self.assertRaises(briskdb.TypeMismatchError):
                session.query("SELECT ?1", [cyclic])

            session.close()
            database.close()

    def test_randomized_sql_values_round_trip_without_json(self) -> None:
        generator = random.Random(197)
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            session = database.session(routing_key="random-values")
            values = []
            for _ in range(100):
                values.extend(
                    [
                        generator.randint(-(2**63), 2**63 - 1),
                        generator.uniform(-1e100, 1e100),
                        "".join(
                            chr(generator.randint(0x20, 0x7E))
                            for _ in range(generator.randint(0, 40))
                        ),
                        generator.randbytes(generator.randint(0, 40)),
                    ]
                )

            for value in values:
                returned = session.query("SELECT ?1", [value])["rows"][0][0]
                if isinstance(value, float):
                    self.assertTrue(
                        math.isclose(returned, value, rel_tol=0, abs_tol=0)
                    )
                else:
                    self.assertEqual(returned, value)

            session.close()
            database.close()

    def test_engine_errors_have_stable_python_types_and_attributes(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            session = database.session(routing_key="errors")
            session.migrate("CREATE TABLE unique_values (id INTEGER PRIMARY KEY)")
            session.execute("INSERT INTO unique_values VALUES (?1)", [1])

            with self.assertRaises(briskdb.UniqueViolationError) as raised:
                session.execute("INSERT INTO unique_values VALUES (?1)", [1])
            error = raised.exception
            self.assertIsInstance(error, briskdb.ConstraintViolationError)
            self.assertIsInstance(error, briskdb.IntegrityError)
            self.assertIsInstance(error, briskdb.BriskDBError)
            self.assertEqual(error.code, "unique_violation")
            self.assertFalse(error.retryable)
            self.assertTrue(str(error))

            with self.assertRaises(briskdb.InvalidQueryError) as invalid:
                session.query("SELEKT private_literal")
            self.assertIsInstance(invalid.exception, briskdb.ProgrammingError)
            self.assertEqual(invalid.exception.code, "invalid_query")
            self.assertTrue(str(invalid.exception))
            self.assertTrue(briskdb.BusyError.retryable)

            session.close()
            database.close()

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
