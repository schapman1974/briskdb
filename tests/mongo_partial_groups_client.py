"""Byte-exact stock-driver partial-group versus ordered-stream/restart checks."""

from bson import BSON, Decimal128, Int64
import pymongo

VALUES = [None, Int64(1), 1.0, Decimal128("1.00"), -0.0, float("nan"), [1], "é\0x"]
RECORDS = [{"_id": i, "key": Int64(i % 7) if i % 2 else float(i % 7), "v": VALUES[i % 8]}
           for i in range(70)] + [{"_id": 70}]
GROUP = {"$group": {
    "_id": "$key", "n": {"$sum": 1}, "large": {"$sum": Int64(2**63 - 1)},
    "first": {"$first": "$v"}, "last": {"$last": "$v"},
    "min": {"$min": "$v"}, "max": {"$max": "$v"},
}}
PIPELINES = [[GROUP], [GROUP, {"$sort": {"n": -1}}, {"$skip": 1}, {"$limit": 4}]]


def partial_group_smoke(uri, reopened):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        # Reuse an existing fixture database: the complete driver gate is near
        # the production catalog's database-count ceiling. Collections stay
        # independent, and both fresh/reopen assertions remain unchanged.
        collection = client.wire_string_range_sync.partial_groups
        if not reopened:
            collection.insert_many(RECORDS)
        for pipeline in PIPELINES:
            expected = [BSON.encode(row) for row in collection.aggregate([{"$match": {}}] + pipeline)]
            for size in (0, 1, 100):
                actual = [BSON.encode(row) for row in collection.aggregate(pipeline, batchSize=size)]
                assert actual == expected, (pipeline, size)
        assert list(collection.aggregate([GROUP]))[0]["n"] == 10


async def async_partial_group_smoke(uri, reopened):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_string_range_async.partial_groups
        if not reopened:
            await collection.insert_many(RECORDS)
        for pipeline in PIPELINES:
            cursor = await collection.aggregate([{"$match": {}}] + pipeline)
            expected = [BSON.encode(row) async for row in cursor]
            for size in (0, 1, 100):
                cursor = await collection.aggregate(pipeline, batchSize=size)
                actual = [BSON.encode(row) async for row in cursor]
                assert actual == expected, (pipeline, size)
