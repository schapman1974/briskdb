"""Verified stock clients against the real daemon before and after restart."""

import asyncio
import sys

import pymongo

from mongo_tls_client import async_checks, options, sync_checks


async def reopened_async(uri, certificate):
    async with pymongo.AsyncMongoClient(uri, **options(certificate)) as client:
        collection = client.tls_checks.async_items
        assert await collection.count_documents({}) == 5
        assert await collection.find_one({"_id": 2}) is None
        assert len(await collection.find({}).batch_size(2).to_list()) == 5


if __name__ == "__main__":
    port, certificate, mode = sys.argv[1:]
    uri = f"mongodb://localhost:{int(port)}/?directConnection=true"
    if mode == "initial":
        sync_checks(uri, certificate)
        asyncio.run(async_checks(uri, certificate))
    elif mode == "reopened":
        with pymongo.MongoClient(uri, **options(certificate)) as client:
            collection = client.tls_checks.sync_items
            assert collection.count_documents({}) == 11
            assert collection.find_one({"_id": 1}) is None
            assert "score_1" in collection.index_information()
            assert len(list(collection.find({}).batch_size(2))) == 11
        asyncio.run(reopened_async(uri, certificate))
    else:
        raise AssertionError("unknown daemon test mode")
    print(f"Verified daemon TLS sync/async client checks passed: {mode}")
