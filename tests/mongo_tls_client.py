"""Real PyMongo TLS validation and CRUD against an owned loopback test listener."""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


def options(certificate):
    return dict(tls=True, tlsCAFile=certificate, serverSelectionTimeoutMS=3000,
                socketTimeoutMS=5000, maxPoolSize=2, compressors="zlib")


def sync_checks(uri, certificate):
    with pymongo.MongoClient(uri, **options(certificate)) as client:
        assert client.admin.command("ping")["ok"] == 1
        collection = client.tls_checks.sync_items
        collection.insert_many([{"_id": i, "score": i % 3} for i in range(12)])
        collection.create_index("score")
        assert collection.count_documents({"score": 1}) == 4
        assert [row["_id"] for row in collection.find({"score": 1}).sort("_id").batch_size(2)] == [1, 4, 7, 10]
        assert collection.update_one({"_id": 1}, {"$inc": {"score": 10}}).modified_count == 1
        assert collection.find_one({"_id": 1})["score"] == 11
        assert collection.delete_one({"_id": 1}).deleted_count == 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert list(pool.map(lambda _: collection.count_documents({}), range(8))) == [11] * 8

    # No insecure options or custom verification callbacks are used. The
    # localhost URI is checked against the certificate SAN by stock PyMongo.
    with pymongo.MongoClient(uri, tls=True, serverSelectionTimeoutMS=600,
                             connectTimeoutMS=300, socketTimeoutMS=600) as untrusted:
        try:
            untrusted.admin.command("ping")
        except ServerSelectionTimeoutError as error:
            assert "certificate" in str(error).lower(), str(error)
        else:
            raise AssertionError("untrusted self-signed certificate was accepted")
    with pymongo.MongoClient(uri, tls=False, serverSelectionTimeoutMS=600,
                             connectTimeoutMS=300, socketTimeoutMS=600) as plaintext:
        try:
            plaintext.admin.command("ping")
        except ServerSelectionTimeoutError:
            pass
        else:
            raise AssertionError("plaintext client was accepted by the TLS listener")
    with pymongo.MongoClient(uri, **options(certificate)) as recovered:
        assert recovered.tls_checks.sync_items.count_documents({}) == 11


async def async_checks(uri, certificate):
    async with pymongo.AsyncMongoClient(uri, **options(certificate)) as client:
        assert (await client.admin.command("ping"))["ok"] == 1
        collection = client.tls_checks.async_items
        await collection.insert_many([{"_id": i, "value": "encrypted"} for i in range(6)])
        assert len(await collection.find({}).batch_size(2).to_list()) == 6
        assert (await collection.update_one({"_id": 2}, {"$set": {"value": "updated"}})).modified_count == 1
        assert (await collection.find_one({"_id": 2}))["value"] == "updated"
        assert (await collection.delete_one({"_id": 2})).deleted_count == 1
        assert await asyncio.gather(*[collection.count_documents({}) for _ in range(4)]) == [5] * 4


if __name__ == "__main__":
    port, certificate = sys.argv[1:]
    uri = f"mongodb://localhost:{int(port)}/?directConnection=true"
    sync_checks(uri, certificate)
    asyncio.run(async_checks(uri, certificate))
    print("PyMongo sync/async verified TLS, zlib, cursors, CRUD and rejection/recovery passed")
