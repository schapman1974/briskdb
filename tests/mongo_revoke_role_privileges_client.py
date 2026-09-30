"""Real TLS/PyMongo exact privilege revocation and live/persistent authority."""

import asyncio
import sys

import pymongo
from mongo_auth_client import URI, denied, options


def privileges(collection, *actions):
    return [{"resource": {"db": "app", "collection": collection}, "actions": list(actions)}]


def initial():
    with pymongo.MongoClient(URI, **options("alice")) as writer, \
         pymongo.MongoClient(URI, **options("revoke_operator")) as operator, \
         pymongo.MongoClient(URI, **options("dynamic_a")) as first, \
         pymongo.MongoClient(URI, **options("dynamic_b")) as second:
        writer.app.posts.insert_many([{"_id": i} for i in range(5)])
        writer.app.other.insert_one({"_id": 1})
        assert first.app.posts.count_documents({}) == 5
        for role in ("custom", "missing-private-role"):
            denied(lambda role=role: first.app.command("revokePrivilegesFromRole", role, privileges=[]))
            denied(lambda role=role: operator.other.command("revokePrivilegesFromRole", role, privileges=[]))
        denied(lambda: operator.app.command("grantPrivilegesToRole", "custom", privileges=[]))
        try:
            operator.app.command("revokePrivilegesFromRole", "missing-private-role", privileges=[])
        except pymongo.errors.OperationFailure as error:
            assert error.code == 31 and error.details["codeName"] == "RoleNotFound", error
            assert "missing-private-role" not in str(error)
        else:
            raise AssertionError("revocation created a missing role")
        for fields in (
            {"privileges": [{"resource": {"db": "other", "collection": ""}, "actions": ["find"]}]},
            {"privileges": privileges("", "revokeRole")},
            {"roles": []}, {"comment": "private-comment"},
            {"writeConcern": {"w": 0}}, {"writeConcern": {"w": "majority"}},
        ):
            denied(lambda fields=fields: operator.app.command({"revokePrivilegesFromRole": "custom", "privileges": [], **fields}), 72)
        # Exact removal leaves wildcard authority in place, even when repeated.
        for _ in range(2):
            assert operator.app.command("revokePrivilegesFromRole", "custom", privileges=privileges("posts", "find"), writeConcern={"w": 1})["ok"] == 1
            assert first.app.posts.count_documents({}) == 5
        cursor = first.app.posts.find().batch_size(1)
        assert next(cursor)["_id"] in range(5)
        operator.app.command("revokePrivilegesFromRole", "custom", privileges=privileges("", "find"))
        denied(lambda: next(cursor))  # The next network batch observes revocation.
        cursor.close()
        denied(lambda: first.app.posts.find_one())
        denied(lambda: first.app.other.find_one())
        # Another assigned role keeps its independent read grant.
        assert second.app.posts.count_documents({}) == 5
        denied(lambda: second.app.other.find_one())
        for client, username, key, roles in (
            (first, "dynamic_a", 10, ["custom"]),
            (second, "dynamic_b", 11, ["backup", "custom"]),
        ):
            assert client.app.posts.insert_one({"_id": key}).inserted_id == key
            assert client.admin.command("usersInfo", username)["users"][0]["roles"] == [{"role": role, "db": "app"} for role in roles]
        operator.app.command("revokePrivilegesFromRole", "custom", privileges=privileges("posts", "insert"))
        denied(lambda: first.app.posts.insert_one({"_id": 12}))
        denied(lambda: second.app.posts.insert_one({"_id": 13}))
        assert first.app.posts.update_one({"_id": 0}, {"$set": {"n": 1}}).matched_count == 1
        # Revoking one CreateObject cannot remove the shared CreateDatabase grant.
        operator.app.command("revokePrivilegesFromRole", "custom", privileges=privileges("scratch", "createCollection"))
        denied(lambda: first.app.scratch.insert_one({"_id": 1}))
        assert second.app.other_new.insert_one({"_id": 1}).inserted_id == 1
        assert writer.app.other_new.find_one()["_id"] == 1
        assert operator.app.command("revokePrivilegesFromRole", "custom", privileges=[])["ok"] == 1
        denied(lambda: operator.app.posts.find_one())
        assert first.admin.command("ping")["ok"] == 1


async def reopened():
    async with pymongo.AsyncMongoClient(URI, **options("dynamic_a")) as first, \
               pymongo.AsyncMongoClient(URI, **options("dynamic_b")) as second, \
               pymongo.AsyncMongoClient(URI, **options("revoke_operator")) as operator:
        assert await second.app.posts.count_documents({}) == 7
        assert (await first.app.posts.update_one({"_id": 0}, {"$set": {"n": 2}})).matched_count == 1
        for operation in (lambda: first.app.posts.find_one(), lambda: first.app.posts.insert_one({"_id": 20})):
            try:
                await operation()
            except pymongo.errors.OperationFailure as error:
                assert error.code == 13, error
            else:
                raise AssertionError("removed permission returned after reopen")
        for role in ("custom", "missing-private-role"):
            try:
                await operator.app.command("revokePrivilegesFromRole", role, privileges=[])
            except pymongo.errors.OperationFailure as error:
                assert error.code == 13, error
            else:
                raise AssertionError("revoked administrator retained authority")
        assert (await first.admin.command("usersInfo", "dynamic_a"))["users"][0]["roles"] == [{"role": "custom", "db": "app"}]
        assert (await operator.admin.command("ping"))["ok"] == 1


if __name__ == "__main__":
    if sys.argv[4] == "initial":
        initial()
    else:
        assert sys.argv[4] == "reopened"
        asyncio.run(reopened())
    print("revokePrivilegesFromRole checks passed:", sys.argv[4])
