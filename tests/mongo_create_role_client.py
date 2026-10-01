"""Custom database-local role creation with real sync/async verified TLS clients."""

import asyncio
import sys

import pymongo
from mongo_auth_client import URI, PASSWORD, denied, options


def privileges(collection, *actions):
    return [{"resource": {"db": "app", "collection": collection}, "actions": list(actions)}]


def app_options(username):
    return {**options(username), "authSource": "app"}


def initial():
    with pymongo.MongoClient(URI, **options("alice")) as writer, \
         pymongo.MongoClient(URI, **options("role_creator")) as operator:
        writer.app.posts.insert_many([{"_id": i} for i in range(5)])
        writer.app.other.insert_one({"_id": 1})
        writer.app["system.js"].insert_one({"_id": "reserved"})
        for username in ("bob", "create_only", "grant_only"):
            with pymongo.MongoClient(URI, **options(username)) as denied_client:
                denied(lambda: denied_client.app.command("createRole", "private-role", privileges=[], roles=[]))
        denied(lambda: operator.other.command("createRole", "private-role", privileges=[], roles=[]))
        denied(lambda: operator.admin.command("createRole", "private-role", privileges=[], roles=[]))
        invalid = [
            {"privileges": [{"resource": {"db": "other", "collection": ""}, "actions": ["find"]}]},
            {"privileges": [{"resource": {"db": "", "collection": ""}, "actions": ["find"]}]},
            {"privileges": [{"resource": {"cluster": True}, "actions": ["shutdown"]}]},
            {"privileges": privileges("", "grantRole")},
            {"roles": ["read"]},
            {"authenticationRestrictions": []},
            {"comment": "private-comment"},
            {"writeConcern": {"w": "majority"}},
            {"writeConcern": {"w": 0}},
        ]
        for fields in invalid:
            denied(lambda fields=fields: operator.app.command({
                "createRole": "invalid-role", "privileges": [], "roles": [], **fields,
            }), 72)
        assert operator.app.command("rolesInfo", "invalid-role")["roles"] == []
        assert operator.app.command("createRole", "post_reader", privileges=privileges("posts", "find", "listIndexes"), roles=[], writeConcern={"w": 1})["ok"] == 1
        exported = operator.app.command("rolesInfo", "post_reader", showPrivileges=True)["roles"][0]
        assert exported["privileges"] == exported["inheritedPrivileges"]
        assert {action for entry in exported["privileges"] for action in entry["actions"]} == {"find", "listIndexes"}
        assert all(entry["resource"] == {"db": "app", "collection": "posts"} for entry in exported["privileges"])
        operator.app.command("createRole", "roundtrip_reader", privileges=exported["privileges"], roles=[])
        assert operator.app.command("rolesInfo", "roundtrip_reader", showPrivileges=True)["roles"][0]["privileges"] == exported["privileges"]
        # Duplicate-name rejection must leave the original read-only policy intact.
        denied(lambda: operator.app.command("createRole", "post_reader", privileges=privileges("", "insert"), roles=[]), 11000)
        with pymongo.MongoClient(URI, **options("create_only")) as denied_client:
            for role in ("post_reader", "private-absent"):
                denied(lambda role=role: denied_client.app.command("createRole", role, privileges=[], roles=[]))
        operator.app.command("createUser", "custom_reader", pwd=PASSWORD, roles=["post_reader"])
        with pymongo.MongoClient(URI, **app_options("custom_reader")) as reader:
            assert reader.app.posts.count_documents({}) == 5
            assert list(reader.app.posts.list_indexes())
            denied(lambda: reader.app.list_collection_names())
            denied(lambda: reader.app.other.find_one())
            denied(lambda: reader.app.missing.find_one())
            denied(lambda: reader.app.missing.count_documents({}))
            denied(lambda: reader.app.posts.insert_one({"_id": 10}))
            denied(lambda: reader.app.posts.update_one({"_id": 0}, {"$set": {"changed": True}}))
            denied(lambda: reader.app.posts.delete_one({"_id": 0}))
            denied(lambda: reader.app.command("createRole", "escalated", privileges=[], roles=[]))
        assert operator.app.command("createRole", "non_system_reader", privileges=privileges("", "find"), roles=[])["ok"] == 1
        operator.app.command("createUser", "database_reader", pwd=PASSWORD, roles=["non_system_reader"])
        with pymongo.MongoClient(URI, **app_options("database_reader")) as reader:
            assert reader.app.other.find_one()["_id"] == 1
            denied(lambda: reader.app["system.js"].find_one())
        assert operator.app.command("createRole", "empty_role", privileges=[], roles=[])["ok"] == 1
        operator.app.command("createUser", "empty_user", pwd=PASSWORD, roles=["empty_role"])
        with pymongo.MongoClient(URI, **app_options("empty_user")) as reader:
            assert reader.admin.command("ping")["ok"] == 1
            denied(lambda: reader.app.posts.find_one())
        operator.app.command("createRole", "post_writer", privileges=privileges("posts", "find", "insert", "update", "remove"), roles=[])
        operator.app.command("createUser", "custom_writer", pwd=PASSWORD, roles=["post_writer"])
        with pymongo.MongoClient(URI, **app_options("custom_writer")) as writer:
            assert writer.app.posts.insert_one({"_id": 99}).inserted_id == 99
            assert writer.app.posts.update_one({"_id": 99}, {"$set": {"value": 1}}).modified_count == 1
            assert writer.app.posts.delete_one({"_id": 99}).deleted_count == 1
            denied(lambda: writer.app.other.insert_one({"_id": 99}))
        operator.app.command("createRole", "exact_system", privileges=privileges("system.js", "find"), roles=[])
        operator.app.command("createUser", "system_reader", pwd=PASSWORD, roles=["exact_system"])
        with pymongo.MongoClient(URI, **app_options("system_reader")) as reader:
            assert reader.app["system.js"].find_one()["_id"] == "reserved"
            denied(lambda: reader.app.posts.find_one())
        operator.app.command("createRole", "schema_role", privileges=(
            privileges("scratch", "createCollection", "createIndex", "dropIndex", "listIndexes", "dropCollection")
            + privileges("", "listCollections")
        ), roles=[])
        operator.app.command("createUser", "schema_user", pwd=PASSWORD, roles=["schema_role"])
        with pymongo.MongoClient(URI, **app_options("schema_user")) as schema:
            schema.app.create_collection("scratch")
            assert "scratch" in schema.app.list_collection_names()
            assert schema.app.scratch.create_index("n") == "n_1"
            assert "n_1" in [index["name"] for index in schema.app.scratch.list_indexes()]
            schema.app.scratch.drop_index("n_1")
            denied(lambda: schema.app.create_collection("forbidden_schema"))
            denied(lambda: schema.app.posts.find_one())
            denied(lambda: schema.app.scratch.insert_one({"_id": 1}))
            schema.app.drop_collection("scratch")
            assert "scratch" not in schema.app.list_collection_names()


async def reopened():
    async with pymongo.AsyncMongoClient(URI, **options("role_creator")) as operator, \
               pymongo.AsyncMongoClient(URI, **app_options("custom_reader")) as reader:
        assert len((await operator.app.command("rolesInfo", "post_reader"))["roles"]) == 1
        exported = (await reader.app.command("rolesInfo", "post_reader", showPrivileges=True))["roles"][0]
        assert exported["privileges"] == exported["inheritedPrivileges"]
        assert {action for entry in exported["privileges"] for action in entry["actions"]} == {"find", "listIndexes"}
        assert await reader.app.posts.count_documents({}) == 5
        cursor = reader.app.posts.find().batch_size(1)
        assert (await anext(cursor))["_id"] in range(5)
        assert (await operator.app.command("createRole", "async_empty", privileges=[], roles=[]))["ok"] == 1
        assert (await operator.app.command("dropRole", "post_reader"))["ok"] == 1
        try:
            await anext(cursor)
        except pymongo.errors.OperationFailure as error:
            assert error.code == 13, error
        else:
            raise AssertionError("dropped custom role retained cursor access")
        await cursor.close()
        assert (await reader.app.command("usersInfo", "custom_reader"))["users"][0]["roles"] == []
        assert (await operator.app.command("createRole", "post_reader", privileges=privileges("posts", "find"), roles=[]))["ok"] == 1
        try:
            await reader.app.posts.find_one()
        except pymongo.errors.OperationFailure as error:
            assert error.code == 13, error
        else:
            raise AssertionError("recreation restored membership")


if __name__ == "__main__":
    if sys.argv[4] == "initial":
        initial()
    else:
        assert sys.argv[4] == "reopened"
        asyncio.run(reopened())
    print("createRole checks passed:", sys.argv[4])
