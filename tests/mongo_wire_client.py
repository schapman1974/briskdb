"""Required real-wire discovery smoke, not a Mongo parity claim."""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

import pymongo
from pymongo.errors import OperationFailure


def check_hello(reply):
    assert reply["isWritablePrimary"] is True
    assert reply["maxWireVersion"] == 8
    assert reply["compression"] == []
    assert "logicalSessionTimeoutMinutes" not in reply
    assert "setName" not in reply


def sync_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        assert client.admin.command("ping")["ok"] == 1
        check_hello(client.admin.command("hello"))
        assert client.server_info()["version"].endswith("-briskdb")
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert all(pool.map(lambda _: client.admin.command("ping")["ok"] == 1, range(12)))
        try:
            client.example.items.find_one({"_id": "not-yet-supported"})
        except OperationFailure as error:
            assert error.code == 59
        else:
            raise AssertionError("unimplemented data commands must fail explicitly")
    # A new client proves reconnect after a pool is closed.
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.admin.command("ping")["ok"] == 1
    # Offering compression must not cause the driver to compress when the server
    # negotiates none. zlib is available without optional codec dependencies.
    with pymongo.MongoClient(uri, compressors="zlib", serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.admin.command("ping")["ok"] == 1


async def async_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        assert (await client.admin.command("ping"))["ok"] == 1
        check_hello(await client.admin.command("hello"))
        replies = await asyncio.gather(*(client.admin.command("ping") for _ in range(12)))
        assert all(reply["ok"] == 1 for reply in replies)
        try:
            await client.example.items.find_one({"_id": "not-yet-supported"})
        except OperationFailure as error:
            assert error.code == 59
        else:
            raise AssertionError("unimplemented async commands must fail explicitly")


if __name__ == "__main__":
    assert pymongo.version == "4.17.0", "use the pinned real-driver version"
    sync_smoke(sys.argv[1])
    asyncio.run(asyncio.wait_for(async_smoke(sys.argv[1]), timeout=20))
    print("PyMongo 4.17.0 sync/async discovery, pooling, reconnect, and rejection passed")
