"""Real TLS/PyMongo role deletion: least privilege, live revocation and reopen."""

import asyncio
import sys

import pymongo
from mongo_auth_client import URI, denied, options


def initial():
    with pymongo.MongoClient(URI, **options("alice")) as writer, \
         pymongo.MongoClient(URI, **options("bob")) as reader, \
         pymongo.MongoClient(URI, **options("a,b=c")) as second_reader, \
         pymongo.MongoClient(URI, **options("role_operator")) as operator:
        writer.app.items.insert_many([{"_id": i} for i in range(5)])
        cursor = reader.app.items.find().batch_size(1)
        assert next(cursor)["_id"] in range(5)
        assert second_reader.app.items.count_documents({}) == 5
        # Existence is not disclosed to a caller without DropRole on this realm.
        for role in ("reader", "missing-private-role"):
            denied(lambda: reader.admin.command("dropRole", role))
            denied(lambda: operator.other.command("dropRole", role))
        for fields in (dict(writeConcern={"w": 0}), dict(writeConcern={"w": "majority"}),
                       dict(writeConcern={"w": 1, "j": True}), dict(comment="private-comment")):
            denied(lambda fields=fields: operator.admin.command("dropRole", "reader", **fields), 72)
        assert reader.app.items.count_documents({}) == 5
        assert operator.admin.command("dropRole", "reader", writeConcern={"w": 1})["ok"] == 1
        assert operator.admin.command("rolesInfo", "reader")["roles"] == []
        denied(lambda: next(cursor))
        cursor.close()
        for client, username in ((reader, "bob"), (second_reader, "a,b=c")):
            denied(lambda: client.app.items.find_one())
            assert client.admin.command("usersInfo", username)["users"][0]["roles"] == []
            assert client.admin.command("ping")["ok"] == 1
        try:
            operator.admin.command("dropRole", "missing-private-role")
        except pymongo.errors.OperationFailure as error:
            assert error.code == 31, error
            assert error.details["codeName"] == "RoleNotFound"
            assert "missing-private-role" not in str(error)
        else:
            raise AssertionError("missing role unexpectedly deleted")
        assert writer.app.items.count_documents({}) == 5


async def reopened():
    async with pymongo.AsyncMongoClient(URI, **options("role_operator")) as operator, \
               pymongo.AsyncMongoClient(URI, **options("bob")) as reader:
        assert len((await operator.admin.command("rolesInfo", "reader"))["roles"]) == 1
        assert (await reader.admin.command("usersInfo", "bob"))["users"][0]["roles"] == []
        try:
            await reader.app.items.find_one()
        except pymongo.errors.OperationFailure as error:
            assert error.code == 13, error
        else:
            raise AssertionError("role recreation restored a removed membership")
        assert (await operator.admin.command("dropRole", "reader"))["ok"] == 1
        # Self-deletion is allowed, but removes the operator's own future grants.
        assert (await operator.admin.command("dropRole", "role_operator"))["ok"] == 1
        try:
            await operator.admin.command("dropRole", "writer")
        except pymongo.errors.OperationFailure as error:
            assert error.code == 13, error
        else:
            raise AssertionError("dropped operator retained administration")
        assert (await operator.admin.command("ping"))["ok"] == 1


if __name__ == "__main__":
    if sys.argv[4] == "initial":
        initial()
    else:
        assert sys.argv[4] == "reopened"
        asyncio.run(reopened())
    print("dropRole checks passed:", sys.argv[4])
