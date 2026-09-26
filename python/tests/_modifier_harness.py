"""Stored-record assertions shared by owned update modifier wire suites."""

from copy import deepcopy
import tempfile

from pymongo.errors import WriteError

import briskdb


class ModifierHarness:
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def apply(self, original, update):
        before = deepcopy(original)
        operation = deepcopy(update)
        self.items.replace_one({"_id": original["_id"]}, original, upsert=True)
        result = self.items.update_one({"_id": original["_id"]}, update)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(original, before)
        self.assertEqual(update, operation)
        return self.items.find_one({"_id": original["_id"]})

    def reject(self, original, update, code=None):
        before = deepcopy(original)
        operation = deepcopy(update)
        self.items.replace_one({"_id": original["_id"]}, original, upsert=True)
        with self.assertRaises(WriteError) as caught:
            self.items.update_one({"_id": original["_id"]}, update)
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.items.find_one({"_id": original["_id"]}), before)
        self.assertEqual(original, before)
        self.assertEqual(update, operation)
