"""Verified TLS, independent sync/async identities and persistence in the real daemon."""
import asyncio
import sys
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
