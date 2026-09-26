"""Focused local entrypoint; every fixture remains in the full real-wire gate."""

import asyncio
import sys

import mongo_wire_client as wire


if __name__ == "__main__":
    assert wire.pymongo.version == "4.17.0", "use the pinned real-driver version"
    assert len(sys.argv) == 3 and sys.argv[2] in ("initial", "reopened")
    uri, reopened = sys.argv[1], sys.argv[2] == "reopened"
    for sync, asynchronous in [
        (wire.string_range_smoke, wire.async_string_range_smoke),
        (wire.membership_index_smoke, wire.async_membership_index_smoke),
        (wire.sparse_presence_smoke, wire.async_sparse_presence_smoke),
        (wire.absence_index_smoke, wire.async_absence_index_smoke),
        (wire.logical_index_smoke, wire.async_logical_index_smoke),
        (wire.partial_index_smoke, wire.async_partial_index_smoke),
    ]:
        sync(uri, reopened)
        asyncio.run(asyncio.wait_for(asynchronous(uri, reopened), timeout=20))
