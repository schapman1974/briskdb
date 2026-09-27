"""Real-driver least-privilege checks for explicitly provisioned Mongo profiles."""

import asyncio
import pymongo
from mongo_auth_client import PASSWORD, URI, denied, options


def managed():
    config = options("profile_managed")
    config["authSource"] = "accounts"
    return pymongo.MongoClient(URI, **config)


def checks():
    with pymongo.MongoClient(URI, **options("profile_writer")) as writer, \
         pymongo.MongoClient(URI, **options("profile_reader")) as reader:
        for database in ["app", "local", "config"]:
            db = writer[database]
            db.items.insert_many([{"_id": value, "n": value} for value in range(5)])
            db["system.js"].insert_one({"_id": "script", "value": "not executed"})
            db.create_collection("explicit")
            db.explicit.drop()
            assert db.items.update_one({"_id": 0}, {"$set": {"n": 10}}).modified_count == 1
            assert db.items.find_one_and_update({"_id": 0}, {"$inc": {"n": 1}})["n"] == 10
            assert db.items.replace_one({"_id": 1}, {"_id": 1, "n": 11}).modified_count == 1
            assert db.items.update_one({"_id": 7}, {"$set": {"n": 7}}, upsert=True).upserted_id == 7
            index = db.items.create_index("n")
            assert index in reader[database].items.index_information()
            db.items.drop_index(index)
            assert db.items.delete_one({"_id": 7}).deleted_count == 1
            assert reader[database].items.count_documents({}) == 5
            assert len(list(reader[database].items.find().batch_size(2))) == 5
            assert len(list(reader[database].items.aggregate([{"$match": {"n": {"$gte": 0}}}]))) == 5
            assert set(reader[database].items.distinct("n")) == {2, 3, 4, 11}
            assert reader[database]["system.js"].find_one()["_id"] == "script"
            assert "items" in reader[database].list_collection_names()
            for operation in [
                lambda: reader[database].items.insert_one({"_id": 99}),
                lambda: reader[database].items.update_one({}, {"$set": {"n": 99}}),
                lambda: reader[database].items.delete_one({}),
                lambda: reader[database].items.find_one_and_delete({}),
                lambda: reader[database].items.create_index("n"),
                lambda: reader[database].items.drop_index("absent"),
                lambda: reader[database].items.drop(),
                lambda: reader[database].create_collection("forbidden"),
                lambda: writer.drop_database(database),
                lambda: reader.drop_database(database),
            ]:
                denied(operation)
            for collection in ["system.users", "system.roles", "system.profile", "system.views", "system.js.child"]:
                denied(lambda: reader[database][collection].find_one())
                denied(lambda: writer[database][collection].insert_one({"_id": 1}))
                denied(lambda: writer[database][collection].drop())
            assert "forbidden" not in db.list_collection_names()
        denied(lambda: writer.local["replset.config"].insert_one({"_id": 1}))
        denied(lambda: reader.local["replset.config"].find_one())
        # Only local reserves replset.*; similarly named ordinary collections work.
        writer.app["replset.config"].insert_one({"_id": 1})
        writer.app["systemx.users"].insert_one({"_id": 1})
        assert reader.app["replset.config"].find_one() == {"_id": 1}
        for client in [reader, writer]:
            denied(lambda: client.other.items.find_one())
            denied(lambda: client.other.items.insert_one({"_id": 1}))
            denied(lambda: client.list_database_names())
            denied(lambda: client.app.command("createUser", "bypass", pwd=PASSWORD, roles=[]))
            denied(lambda: client.app.command("usersInfo", 1))

    with pymongo.MongoClient(URI, **options("profile_operator")) as operator:
        operator.accounts.command("createUser", "profile_managed", pwd=PASSWORD,
                                  roles=[{"role": "read", "db": "app"}])
        with managed() as user:
            assert user.app.items.count_documents({}) == 5
            denied(lambda: user.app.items.insert_one({"_id": 8}))
            operator.accounts.command("grantRolesToUser", "profile_managed", roles=[{"role": "readWrite", "db": "app"}])
            user.app.items.insert_one({"_id": 8})
            operator.accounts.command("revokeRolesFromUser", "profile_managed", roles=[{"role": "readWrite", "db": "app"}])
            denied(lambda: user.app.items.insert_one({"_id": 9}))
            cursor = user.app.items.find().batch_size(1)
            next(cursor)
            operator.accounts.command("revokeRolesFromUser", "profile_managed", roles=[{"role": "read", "db": "app"}])
            denied(lambda: next(cursor))
            denied(lambda: user.app.items.find_one())
            assert user.accounts.command("usersInfo", "profile_managed")["users"][0]["roles"] == []
            cursor.close()


async def async_checks():
    async with pymongo.AsyncMongoClient(URI, **options("profile_reader")) as reader:
        assert (await reader.app.items.find_one({"_id": 0}))["n"] == 11
        try:
            await reader.app.items.insert_one({"_id": 100})
        except pymongo.errors.OperationFailure as error:
            assert error.code == 13, error
        else:
            raise AssertionError("async reader wrote data")


if __name__ == "__main__":
    checks()
    asyncio.run(async_checks())
    print("Provisioned read/readWrite profiles passed CRUD, schema, reserved namespace and revocation checks")
