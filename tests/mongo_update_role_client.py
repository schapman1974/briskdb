"""Real verified TLS/PyMongo role replacement, revocation and persistence."""
import asyncio
import sys

import pymongo
from mongo_auth_client import URI, denied, options


def privileges(*actions):
    return [{"resource": {"db": "app", "collection": "posts"}, "actions": list(actions)}]


def initial():
    with pymongo.MongoClient(URI, **options("alice")) as writer, \
         pymongo.MongoClient(URI, **options("update_operator")) as operator, \
         pymongo.MongoClient(URI, **options("dynamic_a")) as first, \
         pymongo.MongoClient(URI, **options("dynamic_b")) as second:
        writer.app.posts.insert_many([{"_id": i} for i in range(5)])
        assert first.app.posts.count_documents({}) == 5
        for role in ("custom", "missing-private-role"):
            denied(lambda role=role: first.app.command("updateRole", role, privileges=[]))
            denied(lambda role=role: operator.other.command("updateRole", role, privileges=[]))
        denied(lambda: operator.app.command("updateRole", "missing-private-role", privileges=[]), 31)
        for fields in (
            {"roles": ["read"]}, {"authenticationRestrictions": []},
            {"comment": "private-comment"}, {"writeConcern": {"w": 0}},
            {"writeConcern": {"w": "majority"}},
            {"privileges": [{"resource": {"db": "other", "collection": ""}, "actions": ["find"]}]},
        ):
            denied(lambda fields=fields: operator.app.command({"updateRole": "custom", "privileges": [], **fields}), 72)
        # Omitting privileges when clearing flat inheritance is not an empty policy.
        operator.app.command("updateRole", "custom", roles=[])
        assert first.app.posts.count_documents({}) == 5
        cursor = first.app.posts.find().batch_size(1)
        next(cursor)
        operator.app.command("updateRole", "custom", privileges=privileges("insert"), writeConcern={"w": 1})
        denied(lambda: next(cursor))
        cursor.close()
        denied(lambda: first.app.posts.find_one())
        assert second.app.posts.count_documents({}) == 5
        assert first.app.posts.insert_one({"_id": 10}).inserted_id == 10
        denied(lambda: first.app.posts.update_one({"_id": 0}, {"$set": {"n": 1}}))
        # Empty replacement removes all target grants, including admission;
        # another role's admission/read grants still work.
        operator.app.command("updateRole", "custom", privileges=[], roles=[])
        denied(lambda: first.app.posts.insert_one({"_id": 11}))
        assert second.app.posts.count_documents({}) == 6
        operator.app.command("updateRole", "custom", roles=[])
        denied(lambda: first.app.posts.find_one())
        for client, username, roles in (
            (first, "dynamic_a", ["custom"]),
            (second, "dynamic_b", ["backup", "custom"]),
        ):
            assert client.admin.command("usersInfo", username)["users"][0]["roles"] == [{"role": role, "db": "app"} for role in roles]
        denied(lambda: operator.app.posts.find_one())


async def finish_initial():
    async with pymongo.AsyncMongoClient(URI, **options("update_operator")) as operator, \
               pymongo.AsyncMongoClient(URI, **options("dynamic_a")) as first:
        assert (await operator.app.command("updateRole", "custom", privileges=privileges("insert")))["ok"] == 1
        assert (await first.app.posts.insert_one({"_id": 12})).inserted_id == 12


async def reopened():
    async with pymongo.AsyncMongoClient(URI, **options("dynamic_a")) as first, \
               pymongo.AsyncMongoClient(URI, **options("dynamic_b")) as second, \
               pymongo.AsyncMongoClient(URI, **options("update_operator")) as operator:
        assert await second.app.posts.count_documents({}) == 7
        assert (await first.app.posts.insert_one({"_id": 20})).inserted_id == 20
        for operation in (
            lambda: first.app.posts.find_one(),
            lambda: operator.app.command("updateRole", "custom", privileges=[]),
            lambda: operator.app.command("updateRole", "missing-private-role", roles=[]),
        ):
            try:
                await operation()
            except pymongo.errors.OperationFailure as error:
                assert error.code == 13, error
            else:
                raise AssertionError("removed authority returned after reopen")
        assert (await first.admin.command("usersInfo", "dynamic_a"))["users"][0]["roles"] == [{"role": "custom", "db": "app"}]


if __name__ == "__main__":
    if sys.argv[4] == "initial":
        initial()
        asyncio.run(finish_initial())
    else:
        assert sys.argv[4] == "reopened"
        asyncio.run(reopened())
    print("updateRole checks passed:", sys.argv[4])
