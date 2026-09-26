"""Frozen TinyMongo backends do not share a whole-post-image unique policy."""

import hashlib
from pathlib import Path
import tempfile
import unittest

import tinymongo
import tinymongo.sharded_sqlite as sharded_sqlite
import tinymongo.table_backends as table_backends
import tinymongo.tinymongo as sync_tinymongo
from pymongo.errors import DuplicateKeyError


class WriteBoundaryReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Exact runtime files from the existing 53cbf44e source lock.
        for module, digest in [
            (sync_tinymongo, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
            (table_backends, "b16dbc8c435a639d85c29d857f8487b2c88d2eef10969a9e412d8afce02898a1"),
            (sharded_sqlite, "c89aeeecb69ee2116c50d144f3023d5929e8b5778b1f54c2b9fe2cc3605e4445"),
        ]:
            if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() != digest:
                raise AssertionError("write-boundary oracle is not the frozen TinyMongo runtime")

    def test_transient_unique_collision_is_backend_dependent(self):
        for backend in ("memory", "sqlite", "sqlite-sharded"):
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as root:
                kwargs = {"backend": backend}
                if backend == "sqlite-sharded":
                    kwargs["sqlite_shards"] = 2
                with tinymongo.TinyMongoClient(root, **kwargs) as client:
                    collection = client.app.items
                    ids = [1, 2]
                    if backend == "sqlite-sharded":
                        # Both writes must hit one physical shard, with a transient
                        # collision before the second record vacates its old key.
                        ids = [value for value in range(100)
                               if client.app.engine._shard_index(value) == 0][:2]
                        self.assertEqual(len(ids), 2)
                    before = [{"_id": identity, "v": value}
                              for identity, value in zip(ids, (1, 2))]
                    collection.insert_many(before)
                    collection.create_index("v", unique=True)
                    if backend == "memory":
                        result = collection.update_many({}, {"$inc": {"v": 1}})
                        self.assertEqual((result.matched_count, result.modified_count), (2, 2))
                        self.assertEqual(list(collection.find({})),
                                         [{"_id": ids[0], "v": 2}, {"_id": ids[1], "v": 3}])
                    else:
                        with self.assertRaises(DuplicateKeyError):
                            collection.update_many({}, {"$inc": {"v": 1}})
                        self.assertEqual(list(collection.find({})), before)
                        # The rejected operation does not leave the backend unusable.
                        result = collection.update_many({}, {"$inc": {"v": 10}})
                        self.assertEqual((result.matched_count, result.modified_count), (2, 2))
                        self.assertEqual(list(collection.find({})),
                                         [{"_id": ids[0], "v": 11}, {"_id": ids[1], "v": 12}])


if __name__ == "__main__":
    unittest.main()
