"""A missing authoritative manifest must never become an empty replacement."""

from pathlib import Path
import tempfile
import unittest

import briskdb


class MongoMissingManifestTests(unittest.TestCase):
    def test_missing_manifest_rejects_default_and_explicit_startup_without_recreation(self):
        for shards in (2, 4):
            with self.subTest(shards=shards), tempfile.TemporaryDirectory() as root:
                with briskdb.MongoClient(root, shards=shards) as client:
                    client.app.items.insert_one({"_id": "keep", "value": "owned fixture"})
                path = Path(root)
                manifest = path / "manifest.sqlite"
                before = manifest.read_bytes()
                shard_files = sorted((path / "shards").glob("*.sqlite"))
                snapshots = {file: file.read_bytes() for file in shard_files}
                manifest.unlink()
                for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                    for options in ({}, {"shards": shards}, {"shards": 3}):
                        with self.subTest(client_class=client_class, options=options):
                            with self.assertRaises(briskdb.DataCorruptionError):
                                client_class(root, **options)
                            self.assertFalse(manifest.exists())
                            self.assertEqual({file: file.read_bytes() for file in shard_files}, snapshots)
                # Exact stopped fixture restoration is safe; automatic metadata
                # reconstruction from surviving files is not supported.
                manifest.write_bytes(before)
                with briskdb.MongoClient(root) as reopened:
                    self.assertEqual(reopened.app.items.find_one({}), {"_id": "keep", "value": "owned fixture"})


if __name__ == "__main__":
    unittest.main()
