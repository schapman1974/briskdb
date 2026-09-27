"""Native directory policy and durable dotted indexes at startup boundaries."""

from pathlib import Path
import tempfile
import unittest

from pymongo.errors import DuplicateKeyError

import briskdb


class UpstreamStartupTests(unittest.TestCase):
    def test_directory_policy_preserves_bystanders_and_rejects_unclaimed_shards(self):
        with tempfile.TemporaryDirectory() as parent:
            base = Path(parent)
            file_root = base / "not-a-directory"
            file_root.write_bytes(b"unrelated file")
            with self.assertRaises(briskdb.FailedPreconditionError):
                briskdb.MongoClient(file_root)
            self.assertEqual(file_root.read_bytes(), b"unrelated file")

            # Unlike TinyMongo's per-database directory, a native shared root
            # may coexist with unrelated root-level files without adopting them.
            root = base / "native"
            root.mkdir()
            note = root / "operator-note.txt"
            note.write_bytes(b"keep this exact note")
            with briskdb.MongoClient(root, shards=2) as client:
                client.app.items.insert_one({"_id": "saved"})
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.find_one({}), {"_id": "saved"})
            self.assertEqual(note.read_bytes(), b"keep this exact note")

            # Anything already occupying the shards directory denies fresh
            # initialization; no empty authoritative manifest may appear.
            unexplained = base / "unexplained"
            shards = unexplained / "shards"
            shards.mkdir(parents=True)
            marker = shards / "operator-note"
            marker.write_bytes(b"do not adopt")
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                with self.assertRaises(briskdb.DataCorruptionError):
                    client_class(unexplained, shards=2)
                self.assertFalse((unexplained / "manifest.sqlite").exists())
                self.assertEqual(list(shards.iterdir()), [marker])
                self.assertEqual(marker.read_bytes(), b"do not adopt")

    def test_dotted_unique_index_reopens_and_drop_remains_durable(self):
        with tempfile.TemporaryDirectory() as root:
            row = {"_id": "owner", "profile": {"email": "ada@example.test"}}
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                items.insert_one(row)
                for _ in range(2):
                    self.assertEqual(items.create_index("profile.email", unique=True, name="email"), "email")
            with briskdb.MongoClient(root) as reopened:
                items = reopened.app.items
                self.assertEqual(items.index_information()["email"], {"key": [("profile.email", 1)], "unique": True})
                self.assertEqual(items.find_one({"profile.email": "ada@example.test"}), row)
                with self.assertRaises(DuplicateKeyError) as caught:
                    items.insert_one({"_id": "duplicate", "profile": row["profile"]})
                self.assertEqual(caught.exception.code, 11000)
                self.assertEqual(list(items.find({})), [row])
                self.assertIsNone(items.drop_index("email"))
            with briskdb.MongoClient(root) as after_drop:
                self.assertEqual(after_drop.app.items.index_information(), {"_id_": {"key": [("_id", 1)]}})
                after_drop.app.items.insert_one({"_id": "duplicate", "profile": row["profile"]})
                self.assertEqual(after_drop.app.items.count_documents({"profile.email": "ada@example.test"}), 2)
            with briskdb.MongoClient(root) as final:
                self.assertEqual({item["_id"] for item in final.app.items.find({})}, {"owner", "duplicate"})
                self.assertNotIn("email", final.app.items.index_information())


if __name__ == "__main__":
    unittest.main()
