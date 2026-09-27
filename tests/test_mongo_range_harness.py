"""The range corpus keeps every case and bounded, fail-fast async phases."""

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import mongo_range_client as ranges


class RangeHarnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_phase_returns_results_and_propagates_assertion_failures(self):
        async def value():
            return 7

        async def mismatch():
            raise AssertionError("different rows")

        self.assertEqual(await ranges.bounded_phase("value", value()), 7)
        with self.assertRaisesRegex(AssertionError, "different rows"):
            await ranges.bounded_phase("mismatch", mismatch())

    async def test_stalled_phase_is_cancelled_drained_and_named(self):
        cleaned = asyncio.Event()

        async def stalled():
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()

        with patch.object(ranges, "ASYNC_PHASE_TIMEOUT", 0.01):
            with self.assertRaisesRegex(AssertionError, "phase timed out: query 3"):
                await ranges.bounded_phase("query 3", stalled())
        self.assertTrue(cleaned.is_set())

    async def test_initial_and_reopened_runs_keep_every_query_and_post_image_phase(self):
        database = SimpleNamespace(scan=object(), indexed=object())
        client = SimpleNamespace(wire_string_range_async=database)
        context = AsyncMock()
        context.__aenter__.return_value = client
        for reopened in (False, True):
            with self.subTest(reopened=reopened), \
                    patch.object(ranges.pymongo, "AsyncMongoClient", return_value=context), \
                    patch.object(ranges, "seed_async_ranges", new_callable=AsyncMock) as seed, \
                    patch.object(ranges, "compare_async_range", new_callable=AsyncMock) as compare, \
                    patch.object(ranges, "update_async_ranges", new_callable=AsyncMock) as update:
                await ranges.async_string_range_smoke("mongodb://unused", reopened)
                self.assertEqual(seed.await_count, 0 if reopened else 1)
                self.assertEqual(compare.await_count, len(ranges.QUERIES))
                self.assertEqual(
                    [call.args for call in compare.await_args_list],
                    [(database.scan, database.indexed, query) for query in ranges.QUERIES],
                )
                update.assert_awaited_once_with(database.scan, database.indexed)

    async def test_mismatch_stops_the_corpus_instead_of_skipping_to_next_query(self):
        context = AsyncMock()
        with patch.object(ranges.pymongo, "AsyncMongoClient", return_value=context), \
                patch.object(ranges, "compare_async_range", new_callable=AsyncMock,
                             side_effect=AssertionError("mismatch")) as compare, \
                patch.object(ranges, "update_async_ranges", new_callable=AsyncMock) as update:
            with self.assertRaisesRegex(AssertionError, "mismatch"):
                await ranges.async_string_range_smoke("mongodb://unused", True)
            self.assertEqual(compare.await_count, 1)
            update.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
