"""Independent PyMongo and raw TLS checks of the authenticated Mongo adapter."""

import asyncio
import base64
import hashlib
import hmac
import os
from pathlib import Path
import socket
import ssl
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from bson import BSON
import pymongo
from pymongo.errors import OperationFailure

PASSWORD = "private test password"
REPLACEMENT = "rotated test password"
PORT, CERTIFICATE, MARKERS = int(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
URI = f"mongodb://localhost:{PORT}/?directConnection=true"


def options(name="alice", password=PASSWORD):
    return dict(username=name, password=password, authSource="admin", tls=True,
                tlsCAFile=CERTIFICATE, serverSelectionTimeoutMS=3000,
                socketTimeoutMS=5000, maxPoolSize=4, compressors="zlib")


def denied(call, code=13):
    try:
        call()
    except OperationFailure as error:
        assert error.code == code, (error.code, str(error))
    else:
        raise AssertionError(f"operation was allowed, expected {code}")


class Raw:
    def __init__(self):
        context = ssl.create_default_context(cafile=CERTIFICATE)
        self.socket = context.wrap_socket(socket.create_connection(("localhost", PORT), 5), server_hostname="localhost")
        self.request = 0

    def close(self):
        self.socket.close()

    def exact(self, size):
        result = b""
        while len(result) < size:
            chunk = self.socket.recv(size - len(result))
            assert chunk, "connection closed unexpectedly"
            result += chunk
        return result

    def command(self, body, database="admin"):
        self.request += 1
        body = dict(body, **{"$db": database})
        payload = b"\0" * 5 + BSON.encode(body)
        self.socket.sendall(struct.pack("<iiii", 16 + len(payload), self.request, 0, 2013) + payload)
        size, _, response_to, opcode = struct.unpack("<iiii", self.exact(16))
        assert response_to == self.request and opcode == 2013
        return BSON(self.exact(size - 16)[5:]).decode()

    def start(self, name="alice", skip=True):
        escaped = name.replace("=", "=3D").replace(",", "=2C")
        first = f"n={escaped},r=".encode() + base64.b64encode(os.urandom(18))
        reply = self.command(dict(saslStart=1, mechanism="SCRAM-SHA-256", payload=b"n,," + first, options={"skipEmptyExchange": skip}))
        assert reply["ok"] == 1 and not reply["done"]
        return first, reply

    def proof(self, first, challenge, password=PASSWORD):
        fields = dict(field.split(b"=", 1) for field in challenge["payload"].split(b","))
        final = b"c=biws,r=" + fields[b"r"]
        transcript = first + b"," + challenge["payload"] + b"," + final
        salted = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(fields[b"s"]), int(fields[b"i"]))
        mac = lambda key, message: hmac.new(key, message, hashlib.sha256).digest()
        client_key = mac(salted, b"Client Key")
        signature = mac(hashlib.sha256(client_key).digest(), transcript)
        proof = bytes(a ^ b for a, b in zip(client_key, signature))
        expected = b"v=" + base64.b64encode(mac(mac(salted, b"Server Key"), transcript))
        return final + b",p=" + base64.b64encode(proof), expected

    def login(self, name="alice", password=PASSWORD, skip=True):
        first, challenge = self.start(name, skip)
        proof, expected = self.proof(first, challenge, password)
        response = self.command(dict(saslContinue=1, conversationId=challenge["conversationId"], payload=proof))
        assert response["ok"] == 1 and response["payload"] == expected
        assert response["done"] == skip
        if not skip:
            assert self.command(dict(find="items"), "app")["code"] == 13
            assert self.command(dict(saslContinue=1, conversationId=challenge["conversationId"], payload=b""))["done"]


def sync_checks():
    with pymongo.MongoClient(URI, **options()) as writer:
        writer.app.items.insert_many([{"_id": i, "value": i % 3} for i in range(16)])
        writer.app.items.create_index("value")
        assert len(list(writer.app.items.find({}).batch_size(2))) == 16
        assert writer.app.items.update_one({"_id": 1}, {"$inc": {"value": 1}}).modified_count == 1
        assert writer.app.items.delete_one({"_id": 15}).deleted_count == 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(lambda _: writer.app.items.count_documents({}), range(12))) == [15] * 12
        denied(lambda: writer.other.items.find_one())
        denied(lambda: writer.list_database_names())
        denied(lambda: writer.admin.command("createUser", "bypass", pwd="password", roles=[]), 59)
        # Metadata/creation grants cannot authorize empty data results or let a
        # rejected write implicitly create a collection before its real check.
        metadata = Raw()
        try:
            metadata.login("metadata")
            for body in [
                dict(find="items", batchSize=0, singleBatch=True),
                dict(find="missing", batchSize=0, singleBatch=True),
                dict(aggregate="missing", pipeline=[], cursor={}),
                dict(count="missing"),
                dict(distinct="missing", key="value"),
                dict(delete="missing", deletes=[dict(q={}, limit=1)]),
                dict(update="missing", updates=[dict(q={}, u={"$set": {"a": 1}})]),
                dict(update="missing", updates=[dict(q={}, u={"$set": {"a": 1}}, upsert=True)]),
                dict(findAndModify="missing", query={}, remove=True),
                dict(findAndModify="missing", query={}, update={"$set": {"a": 1}}),
                dict(insert="missing", documents=[{"_id": 1}]),
                dict(createIndexes="missing", indexes=[dict(key={"a": 1}, name="a_1")]),
                dict(listIndexes="missing"),
            ]:
                reply = metadata.command(body, "app")
                assert reply.get("code") == 13, (body, reply)
            assert set(writer.app.list_collection_names()) == {"items"}
        finally:
            metadata.close()
    for name in ["bob", "a,b=c"]:
        with pymongo.MongoClient(URI, **options(name)) as reader:
            assert len(list(reader.app.items.find({}).batch_size(2))) == 15
            assert "items" in reader.app.list_collection_names()
            denied(lambda: reader.app.items.insert_one({"_id": 99}))
            denied(lambda: reader.app.items.create_index("other"))
            denied(lambda: reader.app.drop_collection("items"))
    for name, password in [("alice", "wrong"), ("missing", PASSWORD)]:
        with pymongo.MongoClient(URI, **options(name, password)) as client:
            denied(lambda: client.app.items.find_one(), 18)
    # No authentication is allowed to reach only monitoring commands.
    anonymous = options()
    for key in ["username", "password", "authSource"]:
        del anonymous[key]
    with pymongo.MongoClient(URI, **anonymous) as client:
        assert client.admin.command("hello")["ok"] == 1
        denied(lambda: client.app.items.find_one())


async def async_checks():
    async with pymongo.AsyncMongoClient(URI, **options()) as client:
        assert len(await client.app.items.find({}).batch_size(2).to_list()) == 15
        assert await asyncio.gather(*[client.app.items.count_documents({}) for _ in range(8)]) == [15] * 8


def raw_and_rotation_checks():
    first, pooled, other, legacy = [Raw() for _ in range(4)]
    try:
        first.login()
        pooled.login()
        other.login("bob")
        legacy.login(skip=False)
        cursor = first.command(dict(find="items", batchSize=1), "app")["cursor"]["id"]
        assert cursor
        assert other.command(dict(getMore=cursor, collection="items", batchSize=1), "app")["code"] == 43
        assert other.command(dict(killCursors="items", cursors=[cursor]), "app")["cursorsNotFound"] == [cursor]
        assert pooled.command(dict(getMore=cursor, collection="items", batchSize=1), "app")["cursor"]["id"] == cursor
        first.close()  # Ownership must follow the pool handoff, not the old socket.
        assert pooled.command(dict(getMore=cursor, collection="items", batchSize=1), "app")["ok"] == 1
        bob_cursor = other.command(dict(find="items", batchSize=1), "app")["cursor"]["id"]
        MARKERS.joinpath("rotate.ready").touch()
        until = time.monotonic() + 30
        while not MARKERS.joinpath("rotate.done").exists():
            assert time.monotonic() < until, "host did not rotate credentials"
            time.sleep(0.01)
        assert pooled.command(dict(find="items"), "app")["code"] == 13
        assert pooled.command(dict(getMore=cursor, collection="items"), "app")["code"] == 13
        assert other.command(dict(getMore=bob_cursor, collection="items"), "app")["code"] == 13
        assert other.command(dict(find="items"), "app")["code"] == 13
        with pymongo.MongoClient(URI, **options(password=REPLACEMENT)) as updated:
            assert updated.app.items.count_documents({}) == 15
        with pymongo.MongoClient(URI, **options()) as stale:
            denied(lambda: stale.app.items.find_one(), 18)
        # A socket cannot reauthenticate in place, even with the new password.
        reply = pooled.command(dict(saslStart=1, mechanism="SCRAM-SHA-256", payload=b"n,,n=alice,r=new"))
        assert reply["code"] == 18
        assert pooled.socket.recv(1) == b""
    finally:
        for client in [first, pooled, other, legacy]:
            client.close()


if __name__ == "__main__":
    sync_checks()
    asyncio.run(async_checks())
    raw_and_rotation_checks()
    print("PyMongo sync/async SCRAM, verified TLS, roles, pooled cursors and live revocation passed")
