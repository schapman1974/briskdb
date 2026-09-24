"""Focused local count entrypoint; fixtures also run in the full real-wire gate."""

import asyncio
import sys

import mongo_wire_client as wire


if __name__ == "__main__":
    assert wire.pymongo.version == "4.17.0", "use the pinned real-driver version"
    assert len(sys.argv) == 3 and sys.argv[2] in ("initial", "reopened")
    uri, reopened = sys.argv[1], sys.argv[2] == "reopened"
    if not reopened:
        wire.count_smoke(uri)
    wire.count_checkpoint_smoke(uri)
    asyncio.run(asyncio.wait_for(wire.async_count_checkpoint_smoke(uri), timeout=20))
