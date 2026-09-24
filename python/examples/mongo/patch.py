"""Run with the testing wheel and its pymongo extra installed; no daemon needed."""

import asyncio
from pathlib import Path
import tempfile

import briskdb
import pymongo


def sync_application():
    # This function can live in an existing application that imports PyMongo.
    with pymongo.MongoClient("mongodb://ignored.example.com") as client:
        users = client.patch_demo.users
        users.create_index("email", unique=True)
        users.update_one(
            {"_id": 1},
            {"$set": {"email": "ada@example.com", "name": "Ada", "score": 9}},
            upsert=True,
        )
        assert users.find_one({"email": "ada@example.com"})["name"] == "Ada"
        return list(users.find({"score": {"$gte": 5}}, {"name": 1, "_id": 0}).sort("score", -1))


async def async_application(folder):
    async with briskdb.patch(folder=folder):
        async with pymongo.AsyncMongoClient() as client:
            # This is a new engine opening the same persisted BriskDB root.
            assert await client.patch_demo.users.count_documents({}) == 1
            rows = await client.patch_demo.users.find({}).to_list()
            assert rows[0]["name"] == "Ada"
            return rows


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="briskdb-patch-demo-") as parent:
        folder = Path(parent) / "data"
        original = pymongo.MongoClient
        with briskdb.patch(folder=folder, shards=2):
            print("sync:", sync_application())
        assert pymongo.MongoClient is original
        print("async reopened:", asyncio.run(async_application(folder)))
        with briskdb.MongoClient(folder=folder) as client:
            assert client.patch_demo.users.count_documents({}) == 1
        print("BriskDB wheel: patching, query/index/update, async and persistence passed.")
