"""Legacy examples and warning-only Python fallbacks through real PyMongo."""

from datetime import date, datetime
import inspect
from pathlib import Path
import tempfile
import unittest
import warnings

from bson.errors import InvalidDocument
from pymongo import DESCENDING, IndexModel
from pymongo.errors import DuplicateKeyError

import briskdb
from briskdb.mongo import IndexCompatibilityWarning


def assert_warning_origin(test, caught, line):
    test.assertEqual(len(caught), 1)
    test.assertIs(caught[0].category, IndexCompatibilityWarning)
    test.assertEqual(Path(caught[0].filename).resolve(), Path(__file__).resolve())
    test.assertEqual(caught[0].lineno, line)
    test.assertIn("descending", str(caught[0].message))


class UpstreamLegacyExampleTests(unittest.TestCase):
    def test_original_mongo_like_examples_keep_exact_public_results(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                items.insert_one({"_id": "a", "count": 1})
                self.assertIsNotNone(items.find_one({"_id": "a"}))
                result = items.update_one({"_id": "a"}, {"$set": {"count": 5}})
                self.assertEqual((result.matched_count, result.modified_count), (1, 1))
                self.assertEqual(items.find_one({"_id": "a"}), {"_id": "a", "count": 5})
                values = client.app.values
                values.insert_many([{"_id": i, "v": i} for i in range(5)])
                for i in range(5):
                    result = values.update_one({"_id": i}, {"$set": {"v": i + 10}})
                    self.assertEqual((result.matched_count, result.modified_count), (1, 1))
                self.assertEqual(list(values.find({}).sort("_id")), [{"_id": i, "v": i + 10} for i in range(5)])
                items.insert_one({"_id": "p", "a": 1, "b": 2, "c": 3})
                self.assertEqual(items.find_one({"_id": "p"}, {"a": 1, "_id": 0}), {"a": 1})
                items.insert_one({"_id": "fm", "counter": 0})
                items.update_one({"_id": "fm"}, {"$set": {"counter": 1}})
                self.assertEqual(items.find_one({"_id": "fm"}), {"_id": "fm", "counter": 1})
                items.insert_one({"_id": "u1", "v": 1})
                with self.assertRaises(DuplicateKeyError) as caught:
                    items.insert_one({"_id": "u1", "v": 2})
                self.assertEqual(caught.exception.code, 11000)
                self.assertEqual(items.find_one({"_id": "u1"}), {"_id": "u1", "v": 1})
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.items.count_documents({}), 4)
                self.assertEqual(reopened.app.items.find_one({"_id": "u1"}), {"_id": "u1", "v": 1})
                self.assertEqual(list(reopened.app.values.find({}).sort("_id")), [{"_id": i, "v": i + 10} for i in range(5)])

    def test_sync_warning_origin_and_non_bson_sort_boundary(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            records = [{"_id": 1, "published": datetime(2026, 1, 1)},
                       {"_id": 2, "published": datetime(2025, 1, 1)}]
            items.insert_many(records)
            before = items.index_information()
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                with self.assertRaises(InvalidDocument):
                    items.insert_one({"_id": "invalid", "published": date(2026, 1, 1)})
                self.assertEqual(items.index_information(), before)
                self.assertIsNone(items.find_one({"_id": "invalid"}))
                cursor = items.find({}).sort("published")
                clone = cursor.clone()
                self.assertEqual(list(cursor), records[::-1])
                self.assertEqual(list(clone), records[::-1])
                self.assertEqual(list(items.aggregate([{"$sort": {"published": 1}}])), records[::-1])
                self.assertEqual(list(items.find({}).sort("published", DESCENDING)), records)
            self.assertEqual(caught, [])
            for model, field, name in [
                ({"key": {"published": -1}}, "published", "published_-1"),
                (IndexModel([("alternate", DESCENDING)], name="alternate_desc"), "alternate", "alternate_desc"),
            ]:
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    line = inspect.currentframe().f_lineno + 1
                    names = items.create_indexes([model])
                assert_warning_origin(self, caught, line)
                self.assertEqual(names, [name])
                self.assertEqual(items.index_information()[name]["key"], [(field, 1)])
            self.assertEqual(list(items.find({}).sort("published", DESCENDING)), records)


class UpstreamAsyncWarningBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_warning_origin_and_non_bson_sort_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                records = [{"_id": 1, "published": datetime(2026, 1, 1)},
                           {"_id": 2, "published": datetime(2025, 1, 1)}]
                await items.insert_many(records)
                before = await items.index_information()
                with warnings.catch_warnings(record=True) as caught:
                    warnings.simplefilter("always")
                    with self.assertRaises(InvalidDocument):
                        await items.insert_one({"_id": "invalid", "published": date(2026, 1, 1)})
                    self.assertEqual(await items.index_information(), before)
                    self.assertIsNone(await items.find_one({"_id": "invalid"}))
                    cursor = items.find({}).sort("published")
                    clone = cursor.clone()
                    self.assertEqual(await cursor.to_list(), records[::-1])
                    self.assertEqual(await clone.to_list(), records[::-1])
                    self.assertEqual(await (await items.aggregate([{"$sort": {"published": 1}}])).to_list(), records[::-1])
                    self.assertEqual(await items.find({}).sort("published", DESCENDING).to_list(), records)
                self.assertEqual(caught, [])
                for model, field, name in [
                    ({"key": {"published": -1}}, "published", "published_-1"),
                    (IndexModel([("alternate", DESCENDING)], name="alternate_desc"), "alternate", "alternate_desc"),
                ]:
                    with warnings.catch_warnings(record=True) as caught:
                        warnings.simplefilter("always")
                        line = inspect.currentframe().f_lineno + 1
                        names = await items.create_indexes([model])
                    assert_warning_origin(self, caught, line)
                    self.assertEqual(names, [name])
                    self.assertEqual((await items.index_information())[name]["key"], [(field, 1)])
                self.assertEqual(await items.find({}).sort("published", DESCENDING).to_list(), records)


if __name__ == "__main__":
    unittest.main()
