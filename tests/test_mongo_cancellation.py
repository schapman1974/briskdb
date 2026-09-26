"""Keep the cancellation fault injector bounded and in the explicit CI gate."""

import asyncio
from pathlib import Path
import struct
import unittest

from bson import BSON
import mongo_cancel_client as runner


class CancellationHarnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_packet_cap_is_checked_before_reading_the_body(self):
        for length in (-1, 0, 15, 1024 * 1024 + 1, 2**31 - 1):
            reader = asyncio.StreamReader()
            reader.feed_data(struct.pack("<iiii", length, 1, 2, 2013))
            # No EOF/body: waiting for an oversized body would time out.
            with self.assertRaises(AssertionError):
                await asyncio.wait_for(runner.packet(reader), timeout=1)

    async def test_original_packet_bytes_and_correlation_are_preserved(self):
        body = b"\0\0\0\0\0" + BSON.encode({"getMore": 7, "collection": "records"})
        raw = struct.pack("<iiii", len(body) + 16, 12, 34, 2013) + body
        reader = asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()
        actual, request, response, opcode, payload = await runner.packet(reader)
        self.assertEqual((actual, request, response, opcode), (raw, 12, 34, 2013))
        self.assertEqual(runner.message(payload), {"getMore": 7, "collection": "records"})

    async def test_truncated_headers_and_bodies_fail(self):
        for raw in (b"\0" * 15, struct.pack("<iiii", 20, 1, 2, 2013) + b"\0"):
            reader = asyncio.StreamReader()
            reader.feed_data(raw)
            reader.feed_eof()
            with self.assertRaises(asyncio.IncompleteReadError):
                await runner.packet(reader)

    def test_ci_explicitly_executes_the_ignored_real_driver_case(self):
        workflow = (Path(__file__).parents[1] / ".github/workflows/ci.yml").read_text()
        wire = workflow.split("  mongo-wire:", 1)[1].split("  mongo-odm:", 1)[0]
        self.assertIn("BRISKDB_MONGO_WIRE_PYTHON: python", wire)
        self.assertIn("--test mongo_wire \\\n          real_pymongo_async_cancellation -- --ignored --exact", wire)


if __name__ == "__main__":
    unittest.main()
