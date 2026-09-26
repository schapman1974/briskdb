"""Real async driver cancellation against BriskDB through a reply-delay proxy.

The proxy forwards original packets unchanged. It withholds exactly one real
getMore reply, so cancellation cannot race with a fast local query completing.
No driver methods, command documents, results or frozen adapters are patched.
"""

import asyncio
from contextlib import suppress
import json
from pathlib import Path
import struct
import sys
from urllib.parse import urlsplit

from bson import BSON
import pymongo


async def packet(reader):
    header = await reader.readexactly(16)
    length, request, response, opcode = struct.unpack("<iiii", header)
    assert 16 <= length <= 1024 * 1024, "unexpected proxy packet size"
    body = await reader.readexactly(length - 16)
    return header + body, request, response, opcode, body


def message(body):
    assert len(body) >= 10 and body[4] == 0, "expected OP_MSG body section"
    size = struct.unpack_from("<i", body, 5)[0]
    assert 5 <= size <= len(body) - 5
    return BSON(body[5:5 + size]).decode()


class DelayProxy:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.armed = True
        self.held = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.tasks = set()
        self.cursor_id = None
        self.errors = []

    async def connection(self, reader, writer):
        owner = asyncio.current_task()
        self.tasks.add(owner)
        upstream = None
        children = []
        held_connection = False
        target_request = None
        try:
            upstream_reader, upstream = await asyncio.open_connection(self.host, self.port)

            async def requests():
                nonlocal target_request
                while True:
                    raw, request_id, _, opcode, body = await packet(reader)
                    if self.armed and opcode == 2013 and "getMore" in message(body):
                        self.armed = False
                        target_request = request_id
                    upstream.write(raw)
                    await upstream.drain()

            async def replies():
                nonlocal held_connection
                while True:
                    raw, _, response_id, opcode, body = await packet(upstream_reader)
                    if target_request is not None and response_id == target_request:
                        assert opcode == 2013
                        reply = message(body)
                        assert reply["ok"] == 1
                        assert reply["cursor"]["nextBatch"] == [{"_id": 1}]
                        self.cursor_id = reply["cursor"]["id"]
                        assert self.cursor_id > 0, "the real server must still own a cursor"
                        held_connection = True
                        self.held.set()
                        # Only downstream cancellation/EOF (requests task) ends
                        # this hold. The original reply is never rewritten.
                        await asyncio.Future()
                    writer.write(raw)
                    await writer.drain()

            children = [asyncio.create_task(requests()), asyncio.create_task(replies())]
            done, _ = await asyncio.wait(children, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as error:
            self.errors.append(repr(error))
            raise
        finally:
            for task in children:
                task.cancel()
            await asyncio.gather(*children, return_exceptions=True)
            if upstream is not None:
                upstream.close()
                with suppress(ConnectionError):
                    await upstream.wait_closed()
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            if held_connection:
                self.disconnected.set()
            self.tasks.discard(owner)

    async def close(self):
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def wait_signal(root, name):
    async def wait():
        while not (root / name).exists():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(wait(), timeout=10)


async def main(uri, root):
    assert pymongo.version == "4.17.0"
    target = urlsplit(uri)
    assert target.scheme == "mongodb" and target.hostname == "127.0.0.1"
    proxy = DelayProxy(target.hostname, target.port)
    server = await asyncio.start_server(proxy.connection, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = pymongo.AsyncMongoClient(
        f"mongodb://127.0.0.1:{port}/?directConnection=true",
        serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, maxPoolSize=2,
    )
    try:
        collection = client.cancel_acceptance.records
        await collection.drop()
        await collection.insert_many([{"_id": number} for number in range(6)])
        cursor = collection.find({}).sort("_id", 1).batch_size(1)
        assert await cursor.__anext__() == {"_id": 0}
        pending = asyncio.create_task(cursor.__anext__())
        await asyncio.wait_for(proxy.held.wait(), timeout=5)
        assert not pending.done(), "cancellation must target an in-flight operation"
        (root / "held").write_text(json.dumps({"cursor_id": proxy.cursor_id}))
        await wait_signal(root, "cancel")
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("the stock driver operation was not cancelled")
        await asyncio.wait_for(proxy.disconnected.wait(), timeout=5)
        (root / "cancelled").touch()
        # The Rust host proves old-cursor cleanup before cursor.close(), a new
        # query or client shutdown could obscure a cancellation leak.
        await wait_signal(root, "reuse")
        assert (await client.admin.command("ping"))["ok"] == 1
        assert await collection.find({}).sort("_id", 1).to_list() == [
            {"_id": number} for number in range(6)
        ]
        assert await collection.count_documents({}) == 6
        assert not proxy.errors, proxy.errors
        (root / "reused").touch()
        await wait_signal(root, "finish")
        await cursor.close()
        print("in-flight cancellation, socket replacement and data/cursor reuse passed", flush=True)
    finally:
        await client.close()
        server.close()
        await server.wait_closed()
        await proxy.close()


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(sys.argv[1], Path(sys.argv[2])), timeout=35))
