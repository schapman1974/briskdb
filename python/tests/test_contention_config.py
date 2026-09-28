import asyncio
import os
from pathlib import Path
import tempfile
import time
import unittest

import briskdb


def policy(**changes):
    values = dict(initial_delay_ms=1, max_delay_ms=2, multiplier=2,
                  jitter="none", max_retries=2, max_elapsed_ms=20)
    values.update(changes)
    return briskdb.ContentionPolicy(**values)


class ContentionConfigTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "startup lock test uses Unix flock")
    def test_fail_fast_open_stops_before_initialization_and_can_be_retried(self):
        import fcntl

        config = briskdb.Config(shards=2, contention_policy=briskdb.ContentionPolicy.fail_fast())
        with tempfile.TemporaryDirectory() as root:
            with (Path(root) / ".briskdb-startup.lock").open("a+b") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                started = time.monotonic()
                with self.assertRaises(briskdb.BusyError):
                    briskdb.open(root, config=config)
                self.assertLess(time.monotonic() - started, 2)
                self.assertFalse((Path(root) / "manifest.sqlite").exists())
            with briskdb.open(root, config=config) as database:
                with database.session(routing_key="startup") as session:
                    self.assertEqual(session.query("SELECT 1")["rows"], [(1,)])

    def test_policy_is_opt_in_validated_immutable_and_preserved_by_config(self):
        self.assertIsNone(briskdb.Config().contention_policy)
        configured = policy(jitter="full")
        self.assertFalse(configured.is_fail_fast)
        for name, expected in dict(initial_delay_ms=1, max_delay_ms=2, multiplier=2,
                                   jitter="full", max_retries=2, max_elapsed_ms=20).items():
            self.assertEqual(getattr(configured, name), expected)
            with self.assertRaises(AttributeError):
                setattr(configured, name, expected)
        config = briskdb.Config(shards=2, contention_policy=configured)
        self.assertEqual(repr(config.contention_policy), repr(configured))
        self.assertIn("contention_policy=ContentionPolicy(", repr(config))
        with self.assertRaises(AttributeError):
            config.contention_policy = None
        fast = briskdb.ContentionPolicy.fail_fast()
        self.assertTrue(fast.is_fail_fast)
        self.assertEqual((fast.initial_delay_ms, fast.max_delay_ms, fast.max_retries,
                          fast.max_elapsed_ms, fast.jitter), (0, 0, 0, 0, "none"))
        self.assertEqual(repr(fast), "ContentionPolicy.fail_fast()")
        with self.assertRaises(TypeError):
            briskdb.Config(contention_policy={"max_retries": 2})

    def test_invalid_policy_values_fail_during_construction(self):
        for changes in [
            {"initial_delay_ms": 0}, {"initial_delay_ms": 3},
            {"max_delay_ms": 21}, {"max_elapsed_ms": 0},
            {"max_elapsed_ms": 86_400_001}, {"max_elapsed_ms": 2**64 - 1},
            {"multiplier": 0}, {"multiplier": 1025},
            {"max_retries": 0}, {"max_retries": 1_000_001}, {"jitter": "unknown"},
        ]:
            with self.subTest(changes=changes):
                with self.assertRaises(briskdb.InvalidArgumentError):
                    policy(**changes)
        for changes in [{"initial_delay_ms": -1}, {"max_elapsed_ms": 2**64},
                        {"max_retries": 2**32}, {"multiplier": -1}]:
            with self.subTest(changes=changes):
                with self.assertRaises(OverflowError):
                    policy(**changes)
        with self.assertRaises(TypeError):
            policy(initial_delay_ms=1.5)

    def test_sync_wait_policy_returns_busy_without_replaying_a_write(self):
        for selected in (briskdb.ContentionPolicy.fail_fast(), policy()):
            with self.subTest(policy=repr(selected)), tempfile.TemporaryDirectory() as root:
                config = briskdb.Config(shards=2, connections_per_shard=2,
                                        contention_policy=selected)
                with briskdb.open(root, config=config) as database:
                    self.assertEqual(repr(database.config.contention_policy), repr(selected))
                    with database.session(routing_key="same") as session:
                        self.assertEqual(session.status()["contention"], {
                            "retries_scheduled": 0, "wait_nanos": 0, "exhausted_budgets": 0})
                        session.migrate("CREATE TABLE contention_items (id INTEGER PRIMARY KEY)")
                    with database.transaction(routing_key="same") as owner:
                        owner.execute("INSERT INTO contention_items VALUES (1)")
                        with database.session(routing_key="same") as contender:
                            with self.assertRaises(briskdb.BusyError):
                                contender.execute("INSERT INTO contention_items VALUES (2)")
                    with database.session(routing_key="same") as session:
                        counters = session.status()["contention"]
                        self.assertEqual(counters["exhausted_budgets"], 1)
                        if selected.is_fail_fast:
                            self.assertEqual(counters["retries_scheduled"], 0)
                            self.assertEqual(counters["wait_nanos"], 0)
                        else:
                            self.assertGreater(counters["retries_scheduled"], 0)
                            self.assertLessEqual(counters["retries_scheduled"], selected.max_retries)
                            self.assertGreater(counters["wait_nanos"], 0)
                        self.assertEqual(session.query("SELECT id FROM contention_items")["rows"], [(1,)])
                        session.execute("INSERT INTO contention_items VALUES (2)")
                        self.assertEqual(session.query("SELECT id FROM contention_items ORDER BY id")["rows"], [(1,), (2,)])
                with briskdb.open(root, config=config) as reopened:
                    with reopened.session() as session:
                        self.assertEqual(session.status()["contention"], {
                            "retries_scheduled": 0, "wait_nanos": 0, "exhausted_budgets": 0})


class AsyncContentionConfigTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(os.name == "posix", "startup lock test uses Unix flock")
    async def test_fail_fast_async_open_shares_the_native_startup_policy(self):
        import fcntl

        config = briskdb.Config(shards=2, contention_policy=briskdb.ContentionPolicy.fail_fast())
        with tempfile.TemporaryDirectory() as root:
            with (Path(root) / ".briskdb-startup.lock").open("a+b") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(briskdb.BusyError):
                    await asyncio.wait_for(briskdb.open_async(root, config=config), 2)
                self.assertFalse((Path(root) / "manifest.sqlite").exists())
            async with await briskdb.open_async(root, config=config) as database:
                async with await database.session(routing_key="startup") as session:
                    self.assertEqual((await session.query("SELECT 1"))["rows"], [(1,)])

    async def test_async_config_uses_the_same_native_wait_policy(self):
        selected = briskdb.ContentionPolicy.fail_fast()
        with tempfile.TemporaryDirectory() as root:
            database = await briskdb.open_async(root, config=briskdb.Config(
                shards=2, connections_per_shard=2, contention_policy=selected))
            async with database:
                self.assertTrue(database.native.config.contention_policy.is_fail_fast)
                async with await database.session(routing_key="same") as session:
                    await session.migrate("CREATE TABLE contention_items (id INTEGER PRIMARY KEY)")
                async with await database.transaction(routing_key="same") as owner:
                    await owner.execute("INSERT INTO contention_items VALUES (1)")
                    async with await database.session(routing_key="same") as contender:
                        with self.assertRaises(briskdb.BusyError):
                            await contender.execute("INSERT INTO contention_items VALUES (2)")
                async with await database.session(routing_key="same") as session:
                    self.assertEqual((await session.status())["contention"], {
                        "retries_scheduled": 0, "wait_nanos": 0, "exhausted_budgets": 1})
                    self.assertEqual((await session.query("SELECT id FROM contention_items"))["rows"], [(1,)])
                    await session.execute("INSERT INTO contention_items VALUES (2)")


if __name__ == "__main__":
    unittest.main()
