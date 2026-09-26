"""Reference-only evidence for TinyMongo's accepted-no-effect read options."""

import asyncio
import hashlib
from pathlib import Path
import unittest

import tinymongo
import tinymongo.asyncio as async_tinymongo
import tinymongo.tinymongo as sync_tinymongo

from mongo_read_options_client import DOCUMENTS, COMMENT


class ReadOptionReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Runtime files from the existing frozen 53cbf44e contract, not a moving checkout.
        for module, digest in [
            (sync_tinymongo, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
            (async_tinymongo, "c9db35f293bcb64ed89a3f4ed075a398b1b89d30a1b78110f3ffb6605d89c5b2"),
        ]:
            if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() != digest:
                raise AssertionError("read-option oracle is not the frozen TinyMongo runtime")

    def test_find_options_do_not_force_an_index_or_change_results(self):
        with tinymongo.TinyMongoClient(backend="memory") as client:
            collection = client.options.items
            collection.insert_many(DOCUMENTS)
            for hint in ("missing-private-index", {"v": -1}):
                rows = list(collection.find({}, hint=hint, comment=COMMENT,
                                            allow_disk_use=False, no_cursor_timeout=False,
                                            collation={"locale": "simple"}, let={})
                            .sort("_id", -1).skip(1).limit(3))
                self.assertEqual([row["_id"] for row in rows], [4, 3, 2])
            self.assertEqual(collection.find_one({"_id": 2}, hint="missing", comment=COMMENT), DOCUMENTS[2])
            self.assertEqual(list(collection.find({})), DOCUMENTS)

    def test_count_and_distinct_accept_advisory_hints_and_comments(self):
        with tinymongo.TinyMongoClient(backend="memory") as client:
            collection = client.options.items
            collection.insert_many(DOCUMENTS)
            self.assertEqual(collection.count_documents({"v": {"$gte": 1}}, hint="missing", comment=COMMENT), 4)
            self.assertEqual(collection.distinct("v", hint={"v": -1}, comment=COMMENT), [0, 1, 2])

    def test_async_options_preserve_the_same_noop_contract(self):
        async def run():
            async with tinymongo.AsyncTinyMongoClient(backend="memory") as client:
                collection = client.options.items
                await collection.insert_many(DOCUMENTS)
                rows = await collection.find({}, hint="missing", comment=COMMENT).sort("_id", -1).skip(1).limit(3).to_list()
                self.assertEqual([row["_id"] for row in rows], [4, 3, 2])
                self.assertEqual(await collection.count_documents({"v": {"$gte": 1}}, hint="missing", comment=COMMENT), 4)
                self.assertEqual(await collection.distinct("v", hint="missing", comment=COMMENT), [0, 1, 2])
        asyncio.run(run())

    def test_full_driver_gate_runs_focused_options_in_both_restart_phases(self):
        root = Path(__file__).parents[1]
        source = (root / "tests/mongo_wire_client.py").read_text()
        self.assertIn('sync_read_options(sys.argv[1], sys.argv[2] == "reopened")', source)
        self.assertIn('async_read_options(sys.argv[1])', source)
        self.assertIn('real_pymongo_sync_async_discovery -- --ignored --exact', (root / ".github/workflows/ci.yml").read_text())


if __name__ == "__main__":
    unittest.main()
