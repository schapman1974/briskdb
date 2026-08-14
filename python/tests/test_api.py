import asyncio
import concurrent.futures
import tempfile
import time
import unittest

import briskdb


LONG_QUERY = (
    "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL "
    "SELECT x + 1 FROM counter WHERE x < 1000000000) "
    "SELECT sum(x) FROM counter"
)


class SyncApiTests(unittest.TestCase):
    def test_context_managers_and_bounded_cursor_batches(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.connect(data_dir, shards=2) as database:
                with database.session(routing_key="cursor") as session:
                    cursor = session.cursor(
                        "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3",
                        batch_size=2,
                    )
                    self.assertEqual(cursor.remaining, 3)
                    self.assertEqual(cursor.fetchmany(), [(1,), (2,)])
                    self.assertEqual(list(cursor), [(3,)])
                    self.assertEqual(cursor.fetchall(), [])
                    cursor.close()
                    cursor.close()
                    self.assertTrue(cursor.closed)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        cursor.fetchone()

                self.assertTrue(session.closed)
            self.assertTrue(database.closed)

    def test_deadline_and_explicit_cancellation_recover_the_session(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.connect(data_dir, shards=2) as database:
                with database.session(routing_key="cancel") as session:
                    with self.assertRaises(briskdb.DeadlineExceededError):
                        session.query(LONG_QUERY, timeout_ms=1)

                    token = briskdb.CancellationToken()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(
                            session.query,
                            LONG_QUERY,
                            None,
                            cancellation=token,
                        )
                        time.sleep(0.02)
                        self.assertTrue(token.cancel())
                        with self.assertRaises(briskdb.CancelledError):
                            future.result(timeout=5)

                    self.assertEqual(session.query("SELECT 1")["rows"], [(1,)])

    def test_shared_handles_are_safe_for_flask_style_worker_threads(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.connect(data_dir, shards=2) as database:

                def request_handler(value: int) -> int:
                    with database.session(routing_key=f"request-{value}") as session:
                        return session.query("SELECT ?1", [value])["rows"][0][0]

                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    self.assertEqual(
                        list(pool.map(request_handler, range(32))), list(range(32))
                    )


class AsyncApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_crud_cursor_and_context_management(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = await briskdb.open_async(data_dir, shards=2)
            async with database:
                session = await database.session(routing_key="async-crud")
                async with session:
                    self.assertEqual(
                        await session.migrate(
                            "CREATE TABLE async_notes "
                            "(id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
                        ),
                        [0, 1],
                    )
                    write = await session.execute(
                        "INSERT INTO async_notes VALUES (?1, ?2)", [1, "hello"]
                    )
                    self.assertEqual(write["rows_affected"], 1)
                    result = await session.query(
                        "SELECT body FROM async_notes WHERE id = ?1", [1]
                    )
                    self.assertEqual(result["rows"], [("hello",)])

                    cursor = await session.cursor(
                        "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3",
                        batch_size=2,
                    )
                    async with cursor:
                        self.assertEqual(await cursor.fetchmany(), [(1,), (2,)])
                        self.assertEqual(
                            [row async for row in cursor],
                            [(3,)],
                        )
                    self.assertTrue(cursor.closed)

                self.assertTrue(session.closed)
            self.assertTrue(database.closed)
            async with await briskdb.open_async(data_dir) as reopened:
                self.assertEqual(reopened.shard_count, 2)

    async def test_async_queries_do_not_block_the_event_loop(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            async with await briskdb.connect_async(data_dir, shards=2) as database:
                async with await database.session(routing_key="event-loop") as session:
                    ticks = 0
                    finished = asyncio.Event()

                    async def ticker() -> None:
                        nonlocal ticks
                        while not finished.is_set():
                            ticks += 1
                            await asyncio.sleep(0)

                    ticker_task = asyncio.create_task(ticker())
                    result = await session.query(
                        "WITH RECURSIVE counter(x) AS (VALUES(0) UNION ALL "
                        "SELECT x + 1 FROM counter WHERE x < 1000000) "
                        "SELECT sum(x) FROM counter"
                    )
                    finished.set()
                    await ticker_task
                    self.assertEqual(result["rows"], [(500000500000,)])
                    self.assertGreater(ticks, 100)

    async def test_python_task_cancellation_reaches_rust(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            async with await briskdb.open_async(data_dir, shards=2) as database:
                async with await database.session(routing_key="task-cancel") as session:
                    token = briskdb.CancellationToken()
                    query = asyncio.create_task(
                        session.query(LONG_QUERY, cancellation=token)
                    )
                    await asyncio.sleep(0.02)
                    query.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await query
                    self.assertTrue(token.cancelled)
                    recovered = await asyncio.wait_for(session.query("SELECT 1"), 5)
                    self.assertEqual(recovered["rows"], [(1,)])

    async def test_fastapi_and_warm_handler_patterns_share_one_database(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = await briskdb.open_async(data_dir, shards=2)

            async def fastapi_style_handler(value: int) -> int:
                async with await database.session(
                    routing_key=f"handler-{value}"
                ) as session:
                    result = await session.query("SELECT ?1", [value])
                    return result["rows"][0][0]

            results = await asyncio.gather(
                *(fastapi_style_handler(value) for value in range(32))
            )
            self.assertEqual(results, list(range(32)))
            await database.close()


if __name__ == "__main__":
    unittest.main()
