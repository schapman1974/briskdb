"""Genuine locked SQLiteStorage blob migration, without mutating its source."""

from datetime import datetime
from hashlib import sha256
from pathlib import Path
import shutil
import sys
import tempfile

from bson import Binary, ObjectId


def documents():
    return {
        "users": {"1": {"_id": 1, "name": "Ada", "age": 36},
                  "2": {"_id": 2, "name": "Grace", "age": 40}},
        "events": {"1": {"_id": ObjectId("000000000000000000000001"),
                          "created": datetime(2026, 7, 29, 12, 30),
                          "binary": Binary(bytes(range(16)), subtype=4)}},
        "empty": {},
    }


def before(database):
    expected = documents()
    assert set(database.list_collection_names()) == set(expected)
    assert list(database.users.find({}).sort("_id")) == list(expected["users"].values())
    assert list(database.events.find({})) == list(expected["events"].values())
    assert database.empty.count_documents({}) == 0


def source(mode, root):
    import tinymongo.storage_backends as storage
    from mongo_operating_client import source_client

    assert sha256(Path(storage.__file__).read_bytes()).hexdigest() == "c9d96bb6ee8f96496e894b3b985bc05ce2d5098099f300edad5f307108d73bae"
    path = root / "app.sqlite"
    if mode == "seed":
        assert not root.exists(), "requires a new test-owned source"
        storage.SQLiteStorage(str(path)).write(documents())
    digest = sha256(path.read_bytes()).digest()
    assert storage.SQLiteStorage(str(path)).read() == documents()
    # The original public client migrates old blobs in place. Run it only on
    # an owned disposable copy; the actual migration source stays unchanged.
    with tempfile.TemporaryDirectory() as folder:
        copy_root = Path(folder)
        shutil.copy2(path, copy_root / "app.sqlite")
        client = source_client(copy_root, "sqlite")
        try:
            before(client.app)
        finally:
            client.close()
    assert sha256(path.read_bytes()).digest() == digest


def wire(mode, uri):
    import pymongo
    from pymongo.errors import DuplicateKeyError

    assert not any(name == "tinymongo" or name.startswith("tinymongo.") for name in sys.modules)
    with pymongo.MongoClient(uri, maxPoolSize=1, serverMonitoringMode="poll",
                            serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=5000) as client:
        database = client.app
        event = documents()["events"]["1"]
        if mode == "mutate":
            before(database)
            try:
                database.events.insert_one(event.copy())
            except DuplicateKeyError as error:
                assert error.code == 11000
            else:
                raise AssertionError("import must retain ObjectId uniqueness")
            assert database.users.update_one({"_id": 1}, {"$inc": {"age": 1}}).modified_count == 1
            event["stage"] = "migrated"
            assert database.events.replace_one({"_id": event["_id"]}, event).modified_count == 1
            database.empty.insert_one({"_id": "temporary"})
            assert database.empty.delete_one({"_id": "temporary"}).deleted_count == 1
        event["stage"] = "migrated"
        assert database.events.find_one({"_id": event["_id"]}) == event
        assert database.events.count_documents({}) == 1
        assert database.users.find_one({"_id": 1}) == {"_id": 1, "name": "Ada", "age": 37}
        assert database.users.find_one({"_id": 2}) == {"_id": 2, "name": "Grace", "age": 40}
        assert set(database.list_collection_names()) == {"users", "events", "empty"}
        assert database.empty.count_documents({}) == 0


if __name__ == "__main__":
    mode, address = sys.argv[1:]
    if mode in ("seed", "source-check"):
        source(mode, Path(address))
    else:
        assert mode in ("mutate", "reopen")
        wire(mode, address)
    print("Legacy-blob import phase passed:", mode)
