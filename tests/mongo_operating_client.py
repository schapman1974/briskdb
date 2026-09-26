"""Test-only stopped TinyMongo import and real PyMongo restore/rollback drill."""

from datetime import datetime
from hashlib import sha256
from pathlib import Path
import sqlite3
import sys

from bson import BSON, Binary, Decimal128, ObjectId, Regex, Timestamp


def documents():
    return [
        {
            "_id": "doc-%02d" % number,
            "email": "person-%02d@example.test" % number,
            "score": number,
            "nested": {"large": 2**40, "missing_is_not_null": None},
            "array": [number, "value", False],
            "object": ObjectId("0123456789abcdef01234567"),
            "decimal": Decimal128("12.50"),
            "binary": Binary(b"\x00\xffsaved", 128),
            "date": datetime(2024, 2, 3, 4, 5, 6, 789000),
            "regex": Regex("^saved", "i"),
            "timestamp": Timestamp(1234, 7),
        }
        for number in range(24)
    ]


def assert_documents(collection, changed=False):
    expected = documents()
    if changed:
        expected[0]["score"] = 999
        expected.append({"_id": "restored-only", "email": "restored@example.test", "score": 24})
    actual = list(collection.find({}).sort("_id", 1))
    assert [BSON.encode(row) for row in actual] == [BSON.encode(row) for row in expected]


def source_client(root, backend):
    # Verify the unmodified source modules used by this format-specific drill.
    import importlib

    for name, digest in {
        "tinymongo": "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0",
        "table_backends": "b16dbc8c435a639d85c29d857f8487b2c88d2eef10969a9e412d8afce02898a1",
        "sharded_sqlite": "c89aeeecb69ee2116c50d144f3023d5929e8b5778b1f54c2b9fe2cc3605e4445",
        "indexes": "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6",
        "bson_codec": "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617",
    }.items():
        module = importlib.import_module("tinymongo." + name)
        assert sha256(Path(module.__file__).read_bytes()).hexdigest() == digest, name
    from tinymongo import TinyMongoClient

    options = {"sqlite_shards": 2} if backend == "sqlite-sharded" else {}
    return TinyMongoClient(str(root), backend=backend, **options)


def source(mode, root, backend):
    assert backend in ("sqlite", "sqlite-sharded")
    if mode == "seed":
        assert not root.exists(), "source fixture requires a new test-owned directory"
    client = source_client(root, backend)
    try:
        if mode == "seed":
            # TinyMongo has no Database.create_collection method. Its public
            # insert/delete path leaves a real, durable empty collection.
            client.app.empty.insert_one({"_id": "remove-before-import"})
            assert client.app.empty.delete_one({"_id": "remove-before-import"}).deleted_count == 1
            client.app.items.insert_many(documents())
            client.app.items.create_index("email", name="email_unique", unique=True)
            client.app.items.create_index("score", name="score_index")
        assert_documents(client.app.items)
        assert set(client.app.list_collection_names()) == {"empty", "items"}
        assert client.app.empty.count_documents({}) == 0
    finally:
        client.close()
    if mode == "seed":
        # Checkpoint only these stopped, test-owned source files. Never delete
        # sidecars to make preflight pass or checkpoint a live application store.
        for path in root.rglob("*.sqlite"):
            connection = sqlite3.connect(str(path))
            try:
                assert connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0
            finally:
                connection.close()


def wire(mode, uri):
    import pymongo
    from pymongo.errors import DuplicateKeyError

    assert not any(name == "tinymongo" or name.startswith("tinymongo.") for name in sys.modules)

    with pymongo.MongoClient(
        uri, maxPoolSize=1, serverMonitoringMode="poll",
        serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=5000,
    ) as client:
        database = client.app
        assert set(database.list_collection_names()) == {"empty", "items"}
        assert database.empty.count_documents({}) == 0
        assert_documents(database.items, changed=mode == "after-write")
        indexes = {item["name"]: item for item in database.items.list_indexes()}
        assert set(indexes) == ({"_id_"} if mode == "pending" else {"_id_", "email_unique", "score_index"})
        if mode != "pending":
            assert indexes["email_unique"]["unique"] is True
            assert dict(indexes["email_unique"]["key"]) == {"email": 1}
            assert dict(indexes["score_index"]["key"]) == {"score": 1}
            assert database.items.find_one({"email": "person-01@example.test"}) == documents()[1]
            try:
                database.items.insert_one({"_id": "duplicate", "email": "person-01@example.test"})
            except DuplicateKeyError as error:
                assert error.code == 11000
            else:
                raise AssertionError("restored unique index must still enforce uniqueness")
        if mode == "mutate":
            assert database.items.update_one({"_id": "doc-00"}, {"$set": {"score": 999}}).modified_count == 1
            database.items.insert_one({"_id": "restored-only", "email": "restored@example.test", "score": 24})
            assert_documents(database.items, changed=True)
        assert database.items.find_one({"_id": "duplicate"}) is None


def main():
    mode, address, *extra = sys.argv[1:]
    if mode in ("seed", "rollback"):
        assert len(extra) == 1
        source(mode, Path(address), extra[0])
    else:
        assert mode in ("pending", "ready", "mutate", "after-write") and not extra
        wire(mode, address)
    print("Operating drill phase passed:", mode)


if __name__ == "__main__":
    main()
