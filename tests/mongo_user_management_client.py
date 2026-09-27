"""Real driver coverage for explicitly authorized, revision-coupled user edits."""

import asyncio
import pymongo
from mongo_auth_client import PASSWORD, URI, Raw, denied, options


def account(password=PASSWORD):
    config = options("managed-user", password)
    config["authSource"] = "accounts"
    return pymongo.MongoClient(URI, **config)


def checks():
    with pymongo.MongoClient(URI, **options()) as writer:
        writer.app.items.insert_one({"_id": 1})
    with pymongo.MongoClient(URI, **options("operator")) as operator:
        assert operator.accounts.command("usersInfo", 1)["users"] == []
        denied(lambda: operator.admin.command("usersInfo", 1))
        denied(lambda: operator.admin.command("usersInfo", ["operator", {"user": "absent", "db": "other"}]))
        assert operator.accounts.command("usersInfo", "missing")["users"] == []
        # User administration does not itself authorize data or another realm.
        denied(lambda: operator.app.items.find_one())
        denied(lambda: operator.other.command("createUser", "no-access", pwd=PASSWORD, roles=[]))
        denied(lambda: operator.accounts.command("createUser", "no-access", pwd=PASSWORD,
                                                roles=[{"role": "unknown", "db": "other"}]))
        config = options("no-access")
        config["authSource"] = "accounts"
        with pymongo.MongoClient(URI, **config) as absent:
            denied(lambda: absent.admin.command("ping"), 18)

        operator.accounts.command("createUser", "managed-user", pwd=PASSWORD, roles=[],
                                  mechanisms=["SCRAM-SHA-256"], digestPassword=True,
                                  writeConcern={"w": 1})
        with account() as user:
            expected = {"_id": "accounts.managed-user", "user": "managed-user", "db": "accounts",
                        "roles": [], "mechanisms": ["SCRAM-SHA-256"]}
            assert user.accounts.command("usersInfo", "managed-user")["users"] == [expected]
            assert user.admin.command("usersInfo", {"user": "managed-user", "db": "accounts"})["users"] == [expected]
            assert user.accounts.command("usersInfo", [], showCredentials=False)["users"] == []
            denied(lambda: user.accounts.command("usersInfo", 1))
            denied(lambda: user.accounts.command("usersInfo", "missing"))
            denied(lambda: user.accounts.command("usersInfo", ["managed-user", {"user": "operator", "db": "admin"}]))
            assert operator.accounts.command("usersInfo", ["managed-user", "managed-user", "missing"])["users"] == [expected]
            for fields in [dict(showCredentials=True), dict(showPrivileges=True),
                           dict(showAuthenticationRestrictions=True), dict(filter={"user": "managed-user"}),
                           dict(comment="private-admin-comment")]:
                denied(lambda fields=fields: operator.accounts.command("usersInfo", 1, **fields), 72)
            denied(lambda: user.app.items.find_one())
            denied(lambda: user.accounts.command("createUser", "bypass", pwd=PASSWORD, roles=[]))
            denied(lambda: user.accounts.command("updateUser", "managed-user", pwd="changed-account-password"))
            operator.accounts.command("grantRolesToUser", "managed-user", roles=[{"role": "reader", "db": "admin"}])
            assert user.accounts.command("usersInfo", "managed-user")["users"][0]["roles"] == [{"role": "reader", "db": "admin"}]
            assert user.app.items.find_one() == {"_id": 1}
            # Mixed allowed/forbidden grants must not partially publish.
            denied(lambda: operator.accounts.command("grantRolesToUser", "managed-user", roles=[
                {"role": "writer", "db": "admin"}, {"role": "unknown", "db": "other"}]))
            denied(lambda: user.app.items.insert_one({"_id": 2}))
            operator.accounts.command("grantRolesToUser", "managed-user", roles=[{"role": "writer", "db": "admin"}])
            user.app.items.insert_one({"_id": 2})
            operator.accounts.command("revokeRolesFromUser", "managed-user", roles=[{"role": "writer", "db": "admin"}])
            denied(lambda: user.app.items.insert_one({"_id": 3}))
            assert user.app.items.count_documents({}) == 2
            for fields in [dict(mechanisms=["SCRAM-SHA-1"]), dict(digestPassword=False),
                           dict(writeConcern={"w": 0}), dict(roles=[]),
                           dict(customData={"secret": "private-admin-comment"}),
                           dict(authenticationRestrictions=[])]:
                denied(lambda fields=fields: operator.accounts.command("updateUser", "managed-user",
                                                                       pwd="changed-account-password", **fields), 72)
                assert user.app.items.count_documents({}) == 2
            operator.accounts.command("updateUser", "managed-user", pwd="changed-account-password")
            denied(lambda: user.app.items.find_one())  # Already-pooled credential generation revoked.
            denied(lambda: user.accounts.command("usersInfo", "managed-user"))
        with account() as stale:
            denied(lambda: stale.app.items.find_one(), 18)
        with account("changed-account-password") as user:
            assert user.app.items.count_documents({}) == 2
            operator.accounts.command("dropUser", "managed-user")
            denied(lambda: user.app.items.find_one())
            denied(lambda: user.accounts.command("usersInfo", "managed-user"))
        with account("changed-account-password") as dropped:
            denied(lambda: dropped.app.items.find_one(), 18)
        # Recreating the same name cannot revive the removed account's grants.
        operator.accounts.command("createUser", "managed-user", pwd=PASSWORD, roles=[])
        with account() as recreated:
            denied(lambda: recreated.app.items.find_one())
        operator.accounts.command("dropUser", "managed-user")
        assert operator.accounts.command("usersInfo", 1)["users"] == []
    anonymous = Raw()
    try:
        assert anonymous.command(dict(createUser="bypass", pwd=PASSWORD, roles=[]), "accounts")["code"] == 13
    finally:
        anonymous.close()


async def async_checks():
    async with pymongo.AsyncMongoClient(URI, **options("operator")) as operator:
        assert (await operator.accounts.command("usersInfo", 1))["users"] == []
        own = (await operator.admin.command("usersInfo", "operator", showCredentials=False,
                                            showCustomData=False, filter={}))["users"]
        assert own[0]["user"] == "operator" and "credentials" not in own[0]


if __name__ == "__main__":
    checks()
    asyncio.run(async_checks())
    print("PyMongo user creation, scoped role edits, rotation, deletion and redaction checks passed")
