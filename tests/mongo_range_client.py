"""Real sync/async driver string-range reads, writes and persisted index use."""

from bson import BSON, Decimal128
import pymongo


RECORDS = [{"_id": i, "a": f"k{i:03}", "rank": i} for i in range(30)] + [
    {"_id": 30, "a": ["a", "z", "z"], "rank": 30},
    {"_id": 31, "a": [["z"]], "rank": 31},
    {"_id": 32, "a": "k025\0", "rank": 32},
    {"_id": 33, "a": "é", "rank": 33},
    {"_id": 34, "rank": 34},
    {"_id": 35, "a": Decimal128("1"), "rank": 35},
    {"_id": 36, "a": [{"nested": "z"}], "rank": 36},
]
QUERIES = [{"a": {operator: bound}} for operator in ("$gt", "$gte", "$lt", "$lte")
           for bound in ("k025", "k025\0", "é", 1)] + [
    {"a": {"$gt": "x", "$lt": "b"}},
    {"$or": [{"a": {"$gte": "k025"}}, {"a": None}]},
    {"a.nested": {"$gte": "z"}},
]


def rows(cursor):
    return [BSON.encode(row) for row in cursor]


def string_range_smoke(uri, reopened):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        database = client.wire_string_range_sync
        scan, indexed = database.scan, database.indexed
        if not reopened:
            scan.insert_many(RECORDS)
            indexed.insert_many(RECORDS)
            indexed.create_index("a")
            indexed.create_index("a.nested")
        for query in QUERIES:
            assert rows(indexed.find(query, {"a": 1}).sort("rank", -1).skip(1).limit(7).batch_size(2)) == rows(scan.find(query, {"a": 1}).sort("rank", -1).skip(1).limit(7).batch_size(2))
            assert indexed.count_documents(query) == scan.count_documents(query)
            assert indexed.distinct("rank", query) == scan.distinct("rank", query)
        for collection in (scan, indexed):
            collection.update_many({"a": {"$gte": "k025"}}, {"$inc": {"visited": 1}})
        assert rows(indexed.find({})) == rows(scan.find({}))


async def async_string_range_smoke(uri, reopened):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        database = client.wire_string_range_async
        scan, indexed = database.scan, database.indexed
        if not reopened:
            await scan.insert_many(RECORDS)
            await indexed.insert_many(RECORDS)
            await indexed.create_index("a", sparse=True)
            await indexed.create_index("a.nested")
        for query in QUERIES:
            expected = [BSON.encode(row) async for row in scan.find(query).sort("rank", -1).batch_size(2)]
            actual = [BSON.encode(row) async for row in indexed.find(query).sort("rank", -1).batch_size(2)]
            assert actual == expected
            assert await indexed.count_documents(query) == await scan.count_documents(query)
            assert await indexed.distinct("rank", query) == await scan.distinct("rank", query)
        for collection in (scan, indexed):
            await collection.update_many({"a": {"$gte": "k025"}}, {"$inc": {"visited": 1}})
        assert [BSON.encode(row) async for row in indexed.find({})] == [BSON.encode(row) async for row in scan.find({})]
