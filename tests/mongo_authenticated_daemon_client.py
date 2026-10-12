"""Verified TLS, independent sync/async identities and persistence in the real daemon."""
import asyncio
import sys
import os
import signal
import time
from pathlib import Path
import pymongo
from pymongo.errors import OperationFailure

port, certificate, iteration = sys.argv[1], sys.argv[2], int(sys.argv[3])
uri = f"mongodb://localhost:{port}/?directConnection=true"
tls = dict(tls=True, tlsCAFile=certificate, serverSelectionTimeoutMS=3000,
           socketTimeoutMS=5000)


def options(user):
    return dict(tls, username=user, password="test-only-password", authSource="admin")


def denied(call, code=13):
    try:
        call()
    except OperationFailure as error:
        assert error.code == code, error
    else:
        raise AssertionError("unauthorized operation succeeded")


with pymongo.MongoClient(uri, **options("writer")) as writer, \
     pymongo.MongoClient(uri, **options("reader")) as reader, \
     pymongo.MongoClient(uri, **tls) as anonymous:
    assert writer.app.items.count_documents({}) == iteration
    writer.app.items.insert_one({"_id": iteration})
    assert reader.app.items.count_documents({}) == iteration + 1
    denied(lambda: reader.app.items.insert_one({"_id": 999}))
    denied(lambda: reader.other.items.find_one())
    denied(lambda: anonymous.app.items.find_one())
    invalid = dict(options("reader"), password="incorrect")
    with pymongo.MongoClient(uri, **invalid) as bad:
        denied(lambda: bad.app.items.find_one(), 18)
    if len(sys.argv) > 4:
        # Each client also owns monitoring sockets. Release completed probes
        # before retaining sync + async sessions and opening a fresh client.
        writer.close()
        anonymous.close()
        pid, key, log = int(sys.argv[4]), Path(sys.argv[5]), Path(sys.argv[6])
        cert = Path(certificate)
        fixtures = Path(__file__).parent / "fixtures" / "postgres-tls"

        def reload_identity(expected, count):
            os.kill(pid, signal.SIGHUP)
            until = time.monotonic() + 15
            while log.read_text().count(expected) < count:
                assert time.monotonic() < until, log.read_text()
                time.sleep(0.01)

        async def retained_async():
            async with pymongo.AsyncMongoClient(uri, **options("reader")) as retained:
                assert await retained.app.items.count_documents({}) == iteration + 1
                cert.write_bytes((fixtures / "rotated.crt").read_bytes())
                key.write_bytes((fixtures / "rotated.key").read_bytes())
                reload_identity("listener security reloaded", 1)
                # Both pre-existing authenticated connections keep their rights.
                assert reader.app.items.count_documents({}) == iteration + 1
                assert await retained.app.items.count_documents({}) == iteration + 1
                denied(lambda: reader.app.items.insert_one({"_id": 997}))
                with pymongo.MongoClient(uri, **options("reader")) as fresh:
                    assert fresh.app.items.count_documents({}) == iteration + 1
                key.write_bytes((fixtures / "server.key").read_bytes())
                reload_identity("listener security reload rejected", 1)
                with pymongo.MongoClient(uri, **options("reader")) as fresh:
                    assert fresh.app.items.count_documents({}) == iteration + 1
                cert.write_bytes((fixtures / "server.crt").read_bytes())
                reload_identity("listener security reloaded", 2)
                assert await retained.app.items.count_documents({}) == iteration + 1

        asyncio.run(retained_async())


async def check_async():
    async with pymongo.AsyncMongoClient(uri, **options("reader")) as reader:
        assert await reader.app.items.count_documents({}) == iteration + 1
        try:
            await reader.app.items.insert_one({"_id": 998})
        except OperationFailure as error:
            assert error.code == 13
        else:
            raise AssertionError("async reader wrote data")


asyncio.run(check_async())
