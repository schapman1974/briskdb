"""Live readers must not serve an unlinked/replaced physical shard inode."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from pymongo.errors import OperationFailure

import briskdb


PREPARE_REPLACEMENT = """
import json, pathlib, sqlite3, sys
from bson import BSON
root = pathlib.Path(sys.argv[1])
connections = [sqlite3.connect(str(root / 'shards' / f'{n:04}.sqlite')) for n in range(2)]
try:
    identifier = BSON(connections[0].execute(
        'SELECT document_bson FROM briskdb_documents_v1 LIMIT 1').fetchone()[0]).decode()['_id']
    for connection in connections:
        assert connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    backup = sqlite3.connect(str(root / 'replacement.sqlite'))
    try:
        connections[int(sys.argv[2])].backup(backup)
    finally:
        backup.close()
    print(json.dumps(identifier))
finally:
    for connection in connections:
        connection.close()
"""


class MongoStorageFileIdentityTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires replacement of an open POSIX file")
    def test_warmed_point_reads_reject_missing_or_replaced_shards_without_stale_results(self):
        for mutation in ("missing", "same-shard-copy", "other-shard"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as root:
                with briskdb.MongoClient(root, shards=2) as client:
                    client.app.items.insert_many([{"_id": n, "value": "private-owned-fixture"} for n in range(32)])
                    prepared = subprocess.run([sys.executable, "-c", PREPARE_REPLACEMENT, root,
                                               "1" if mutation == "other-shard" else "0"],
                                              capture_output=True, text=True, timeout=10)
                    self.assertEqual(prepared.returncode, 0, prepared.stderr)
                    identifier = json.loads(prepared.stdout)
                    expected = {"_id": identifier, "value": "private-owned-fixture"}
                    self.assertEqual(client.app.items.find_one({"_id": identifier}), expected)
                    target = Path(root) / "shards/0000.sqlite"
                    if mutation == "missing":
                        target.unlink()
                    else:
                        os.replace(Path(root) / "replacement.sqlite", target)
                    with self.assertRaises(OperationFailure) as caught:
                        client.app.items.find_one({"_id": identifier})
                    self.assertEqual(caught.exception.code, 1)
                    self.assertNotIn(root, str(caught.exception))
                    self.assertNotIn("private-owned-fixture", str(caught.exception))
                    for collection in (client.app.items, client.app.absent):
                        with self.assertRaises(OperationFailure) as degraded:
                            collection.find_one({})
                        self.assertEqual(degraded.exception.code, 1)
                    if mutation == "missing":
                        self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
