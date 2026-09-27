"""Native routed sharded storage, not TinyMongo's ATTACH pool/file format."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import tempfile
import unittest
from urllib.parse import quote

from pymongo.errors import BulkWriteError, DuplicateKeyError

import briskdb


def routed_ids(root, shards, names, per_shard=1):
    result = {}
    with briskdb.open(root, shards=shards, documents=True) as database:
        with database.session() as session:
            for name in names:
                session.create_collection("app", name)
                found = {shard: [] for shard in range(shards)}
                for identifier in range(10_000):
                    route = session.find("app", name, {"_id": identifier})["plan"]["shards"][0]
                    if len(found[route]) < per_shard:
                        found[route].append(identifier)
                    if all(len(values) == per_shard for values in found.values()):
                        break
                assert all(len(values) == per_shard for values in found.values()), found
                result[name] = found
    return result


class UpstreamShardedBackendTests(unittest.TestCase):
    def test_native_layout_wal_and_fixed_root_shards_survive_logical_drop(self):
        with tempfile.TemporaryDirectory(prefix="briskdb ? # café ") as root:
            path = Path(root)
            with briskdb.MongoClient(root) as client:
                retained = client.app
                retained.items.insert_one({"_id": "before"})
                client.other.items.insert_one({"_id": "keep"})
                client.drop_database("app")
                with self.assertRaisesRegex(ValueError, "does not match"):
                    briskdb.MongoClient(root, sqlite_shards=2)
                retained.items.insert_one({"_id": "after"})
                self.assertEqual(list(retained.items.find({})), [{"_id": "after"}])
                self.assertEqual(client.other.items.find_one({}), {"_id": "keep"})
            files = [path / "manifest.sqlite", *[path / "shards" / f"{shard:04}.sqlite" for shard in range(4)]]
            self.assertEqual(sorted(p.name for p in (path / "shards").glob("*.sqlite")),
                             [f"{shard:04}.sqlite" for shard in range(4)])
            for file in files:
                self.assertTrue(file.is_file())
                connection = sqlite3.connect("file:" + quote(str(file)) + "?mode=ro", uri=True)
                try:
                    self.assertEqual(connection.execute("PRAGMA journal_mode").fetchone()[0], "wal")
                finally:
                    connection.close()
            self.assertFalse((path / "app.sqlite-sharded").exists())
            with self.assertRaises(briskdb.FailedPreconditionError):
                briskdb.MongoClient(root, shards=2)
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.find_one({}), {"_id": "after"})
                self.assertEqual(reopened.other.items.find_one({}), {"_id": "keep"})
                # Logical drop cannot remove shared native shard files.
                reopened.drop_database("app")
                reopened.drop_database("other")
                self.assertEqual(reopened.list_database_names(), [])
                self.assertTrue(all(file.is_file() for file in files))

    def test_shard_configuration_aliases_reject_invalid_counts_before_storage(self):
        invalid = [True, False, 2.0, "2", [], {}, -1, 0, 1, 65, 100]
        with tempfile.TemporaryDirectory() as parent:
            root = Path(parent) / "not-created"
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                for value in invalid:
                    with self.assertRaisesRegex(ValueError, "shards must be an integer between 2 and 64"):
                        client_class(folder=root, backend="sqlite-sharded", sqlite_shards=value)
                    self.assertFalse(root.exists())
            for backend in ("sqlite", "sqlite-sharded"):
                folder = Path(parent) / backend
                with briskdb.MongoClient("mongodb://unused.invalid", tinymongo_folder=folder,
                                        backend=backend, sqlite_shards=2) as client:
                    client.app.items.insert_one({"_id": backend})
                self.assertEqual(len(list((folder / "shards").glob("*.sqlite"))), 2)
                with briskdb.MongoClient(folder) as reopened:
                    self.assertEqual(reopened.app.items.find_one({}), {"_id": backend})

    def test_eleven_shard_scans_global_windows_and_concurrent_readers_reopen(self):
        with tempfile.TemporaryDirectory(prefix="briskdb eleven ? # ") as root:
            identifiers = routed_ids(root, 11, ["items"], per_shard=3)["items"]
            rows = [{"_id": identifier, "group": "keep" if ordinal != 1 else "drop", "score": shard * 10 + ordinal}
                    for shard, values in identifiers.items() for ordinal, identifier in enumerate(values)]
            expected = sorted(rows, key=lambda row: row["_id"])
            with briskdb.MongoClient(root) as client:
                items = client.app.items
                items.insert_many(rows)
                self.assertEqual(items.count_documents({}), 33)
                self.assertEqual(items.count_documents({"group": "keep"}), 22)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    copies = list(pool.map(lambda _: list(items.find({}).sort("_id")), range(4)))
                self.assertEqual(copies, [expected] * 4)
                copies[0][0]["score"] = -100
                self.assertEqual(list(items.find({}).sort("_id")), expected)
                query = {"group": "keep", "score": {"$gte": 10}}
                window = sorted((row for row in rows if row["group"] == "keep" and row["score"] >= 10),
                                key=lambda row: row["score"], reverse=True)[1:4]
                self.assertEqual(list(items.find(query, sort=[("score", -1)], skip=1, limit=3)), window)
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(list(reopened.app.items.find({}).sort("_id")), expected)
                retained = reopened.app.items
                retained.drop()
                reopened.app.items.insert_one({"_id": "replacement"})
                self.assertEqual(list(retained.find({})), [{"_id": "replacement"}])

    def test_compound_sparse_and_partial_uniqueness_is_proven_cross_shard(self):
        with tempfile.TemporaryDirectory() as root:
            identifiers = routed_ids(root, 2, ["compound", "sparse", "partial"])
            with briskdb.MongoClient(root) as client:
                for name in identifiers:
                    with self.subTest(name=name):
                        first, second = identifiers[name][0][0], identifiers[name][1][0]
                        items = client.app[name]
                        if name == "compound":
                            items.create_index([("tenant", 1), ("username", 1)], unique=True, name="constraint")
                            owner = {"_id": first, "tenant": "north", "username": "ada"}
                            duplicate = {"_id": second, "tenant": "north", "username": "ada"}
                        elif name == "sparse":
                            items.create_index("email", unique=True, sparse=True, name="constraint")
                            items.insert_many([{"_id": first}, {"_id": second}])
                            items.delete_many({})
                            owner = {"_id": first, "email": "same"}
                            duplicate = {"_id": second, "email": "same"}
                        else:
                            items.create_index("handle", unique=True, partialFilterExpression={"active": True}, name="constraint")
                            items.insert_many([{"_id": first, "handle": "same", "active": False},
                                               {"_id": second, "handle": "same", "active": False}])
                            items.delete_many({})
                            owner = {"_id": first, "handle": "same", "active": True}
                            duplicate = {"_id": second, "handle": "same", "active": True}
                        items.insert_one(owner)
                        with self.assertRaises(DuplicateKeyError) as caught:
                            items.insert_one(duplicate)
                        self.assertEqual(caught.exception.code, 11000)
                        self.assertEqual(list(items.find({})), [owner])
            with briskdb.MongoClient(root) as reopened:
                for name in identifiers:
                    self.assertTrue(reopened.app[name].index_information()["constraint"]["unique"])
                    self.assertEqual(reopened.app[name].count_documents({}), 1)

    def test_ordered_and_unordered_cross_shard_duplicates_keep_input_error_indices(self):
        for ordered, inserted, errors in ((True, 2, [2]), (False, 3, [2, 4])):
            with self.subTest(ordered=ordered), tempfile.TemporaryDirectory() as root:
                identifiers = routed_ids(root, 2, ["items"], per_shard=2)["items"]
                seed, first = identifiers[0]
                second, tail = identifiers[1]
                with briskdb.MongoClient(root) as client:
                    items = client.app.items
                    items.insert_one({"_id": seed})
                    incoming = [{"_id": value} for value in (first, second, seed, tail, second)]
                    with self.assertRaises(BulkWriteError) as caught:
                        items.insert_many(incoming, ordered=ordered)
                    details = caught.exception.details
                    self.assertEqual(details["nInserted"], inserted)
                    self.assertEqual([error["index"] for error in details["writeErrors"]], errors)
                    for error in details["writeErrors"]:
                        self.assertEqual(error["code"], 11000)
                        self.assertIs(error["op"], incoming[error["index"]])
                    expected = {seed, first, second} | ({tail} if not ordered else set())
                    self.assertEqual({row["_id"] for row in items.find({})}, expected)
                with briskdb.MongoClient(root) as reopened:
                    self.assertEqual({row["_id"] for row in reopened.app.items.find({})}, expected)


if __name__ == "__main__":
    unittest.main()
