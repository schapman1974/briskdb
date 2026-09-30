"""Real TLS/PyMongo incremental role grants, current authority and persistence."""

import asyncio
import sys

import pymongo
from mongo_auth_client import URI, denied, options


def privileges(collection, *actions):
    return [{"resource": {"db": "app", "collection": collection}, "actions": list(actions)}]


def initial():
    with pymongo.MongoClient(URI, **options("alice")) as writer, \
         pymongo.MongoClient(URI, **options("privilege_operator")) as operator, \
         pymongo.MongoClient(URI, **options("dynamic_a")) as first, \
         pymongo.MongoClient(URI, **options("dynamic_b")) as second:
        writer.app.posts.insert_many([{"_id": i} for i in range(5)])
        writer.app.other.insert_one({"_id": 1})
        writer.app["system.js"].insert_one({"_id": 1})
        assert first.app.posts.count_documents({}) == 5
        denied(lambda: first.app.posts.insert_one({"_id": 10}))
        denied(lambda: second.app.other.find_one())
        cursor = first.app.posts.find().batch_size(1)
        assert next(cursor)["_id"] in range(5)
        for role in ("custom", "missing-private-role"):
            denied(lambda role=role: first.app.command("grantPrivilegesToRole", role, privileges=[]))
            denied(lambda role=role: operator.other.command("grantPrivilegesToRole", role, privileges=[]))
        denied(lambda: operator.app.command("createRole", "no-create-authority", privileges=[], roles=[]))
        try:
            operator.app.command("grantPrivilegesToRole", "missing-private-role", privileges=[])
        except pymongo.errors.OperationFailure as error:
            assert error.code == 31 and error.details["codeName"] == "RoleNotFound", error
            assert "missing-private-role" not in str(error)
        else:
            raise AssertionError("grant created a missing role")
        for fields in (
            {"privileges": [{"resource": {"db": "other", "collection": ""}, "actions": ["find"]}]},
            {"privileges": privileges("", "grantRole")},
            {"roles": []}, {"comment": "private-comment"},
            {"writeConcern": {"w": 0}}, {"writeConcern": {"w": "majority"}},
        ):
            denied(lambda fields=fields: operator.app.command({"grantPrivilegesToRole": "custom", "privileges": [], **fields}), 72)
        addition = privileges("posts", "insert")
        assert operator.app.command("grantPrivilegesToRole", "custom", privileges=addition, writeConcern={"w": 1})["ok"] == 1
        for client, username, key in ((first, "dynamic_a", 10), (second, "dynamic_b", 11)):
            assert client.app.posts.insert_one({"_id": key}).inserted_id == key
            assert client.admin.command("usersInfo", username)["users"][0]["roles"] == [{"role": "custom", "db": "app"}]
            denied(lambda: client.app.posts.update_one({"_id": key}, {"$set": {"n": 1}}))
            denied(lambda: client.app.other.find_one())
        assert next(cursor)["_id"] in range(12)
        cursor.close()
        for addition in (addition, []):
            assert operator.app.command("grantPrivilegesToRole", "custom", privileges=addition)["ok"] == 1
        assert first.app.posts.count_documents({}) == 7
        operator.app.command("grantPrivilegesToRole", "custom", privileges=privileges("", "find"))
        assert first.app.other.find_one()["_id"] == second.app.other.find_one()["_id"] == 1
        denied(lambda: first.app["system.js"].find_one())
        operator.app.command("grantPrivilegesToRole", "custom", privileges=privileges("scratch", "createCollection", "insert"))
        assert second.app.scratch.insert_one({"_id": 1}).inserted_id == 1
        assert first.app.scratch.find_one()["_id"] == 1
        # Changing somebody else's role does not assign it to the operator.
        denied(lambda: operator.app.posts.find_one())
        denied(lambda: first.app.list_collection_names())


async def reopened():
    async with pymongo.AsyncMongoClient(URI, **options("dynamic_a")) as reader, \
               pymongo.AsyncMongoClient(URI, **options("privilege_operator")) as operator:
        assert await reader.app.posts.count_documents({}) == 7
        assert (await reader.app.scratch.insert_one({"_id": 2})).inserted_id == 2
        assert (await reader.admin.command("usersInfo", "dynamic_a"))["users"][0]["roles"] == [{"role": "custom", "db": "app"}]
        for role in ("custom", "missing-private-role"):
            try:
                await operator.app.command("grantPrivilegesToRole", role, privileges=[])
            except pymongo.errors.OperationFailure as error:
                assert error.code == 13, error
            else:
                raise AssertionError("revoked operator retained grant authority")
        assert (await operator.admin.command("ping"))["ok"] == 1


if __name__ == "__main__":
    if sys.argv[4] == "initial":
        initial()
    else:
        assert sys.argv[4] == "reopened"
        asyncio.run(reopened())
    print("grantPrivilegesToRole checks passed:", sys.argv[4])
