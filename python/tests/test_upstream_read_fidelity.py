"""Recursive read/codec scenarios from locked TinyMongo client_read_fidelity.

Source SHA-256: 1ba5a7f2457b6f3b5e09871c5a185f71d16db89de01d0c8be8a7dabbb281271a
BriskDB uses its own SQLite backend, not the reference's five storage engines.
Full listDatabases statistics remain explicitly unsupported under #166.
"""

from collections import OrderedDict, UserDict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import tempfile
import unittest

from bson.son import SON
from pymongo.errors import OperationFailure

import briskdb


STORED = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone(timedelta(hours=-5)))
UTC_MILLIS = datetime(2026, 1, 2, 8, 4, 5, 123000)


def recursive(test, document, kind):
    for value in (document, document["inner"], document["items"][0], document["items"][0]["deep"]):
        test.assertIs(type(value), kind)


class UpstreamReadFidelityTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)

    def open(self, **options):
        client = briskdb.MongoClient(self.root.name, shards=2, **options)
        self.addCleanup(client.close)
        return client

    def test_sync_recursive_read_surfaces_preserve_document_class_and_dates(self):
        client = self.open(document_class=OrderedDict)
        items = client.app.items
        source = {"_id": "main", "when": STORED,
                  "before_epoch": datetime(1969, 12, 31, 23, 59, 59, 999999),
                  "inner": {"when": STORED}, "items": [{"deep": {"when": STORED}}]}
        original = deepcopy(source)
        items.insert_one(source)
        self.assertEqual(source, original)
        found = items.find_one({"_id": "main"})
        recursive(self, found, OrderedDict)
        self.assertEqual(found["when"], UTC_MILLIS)
        self.assertIsNone(found["when"].tzinfo)
        self.assertEqual(found["before_epoch"], datetime(1969, 12, 31, 23, 59, 59, 999000))
        self.assertEqual(found["inner"]["when"], UTC_MILLIS)
        self.assertEqual(found["items"][0]["deep"]["when"], UTC_MILLIS)
        projected = items.find_one({"_id": "main"}, {"inner": 1, "items": 1, "when": 1})
        recursive(self, projected, OrderedDict)
        cursor = items.find({"_id": "main"})
        recursive(self, cursor[0], OrderedDict)
        recursive(self, cursor.clone().to_list()[0], OrderedDict)
        aggregate = items.aggregate([{"$match": {"_id": "main"}}, {"$project": {"inner": 1, "items": 1, "when": 1}}])
        recursive(self, next(aggregate), OrderedDict)
        distinct = items.distinct("inner")
        self.assertEqual(len(distinct), 1)
        self.assertIs(type(distinct[0]), OrderedDict)
        self.assertEqual(distinct[0]["when"], UTC_MILLIS)
        items.insert_many([{"_id": name, "inner": {"value": 1}} for name in ("update", "replace", "delete")])
        updated = items.find_one_and_update({"_id": "update"}, {"$set": {"inner.value": 2}}, projection={"_id": 0, "inner": 1}, return_document=True)
        replaced = items.find_one_and_replace({"_id": "replace"}, {"inner": {"value": 2}}, projection={"_id": 0, "inner": 1}, return_document=True)
        deleted = items.find_one_and_delete({"_id": "delete"}, projection={"_id": 0, "inner": 1})
        for returned in (updated, replaced, deleted):
            self.assertIs(type(returned), OrderedDict)
            self.assertIs(type(returned["inner"]), OrderedDict)
        self.assertEqual((updated["inner"]["value"], replaced["inner"]["value"], deleted["inner"]["value"]), (2, 2, 1))

    def test_metadata_keeps_driver_shapes_and_full_database_statistics_remain_explicit(self):
        client = self.open(document_class=OrderedDict)
        client.app.items.insert_one({"_id": 1})
        with self.assertRaises(OperationFailure) as caught:
            client.list_databases().to_list()
        self.assertEqual(caught.exception.code, 115)
        rows = client.list_databases(nameOnly=True).to_list()
        self.assertEqual(rows, [{"name": "app"}])
        self.assertIs(type(rows[0]), dict)
        indexes = client.app.items.list_indexes().to_list()
        self.assertIs(type(indexes[0]), SON)
        self.assertIs(type(client.app.items.index_information()), dict)

    def test_timezone_options_survive_persistence_and_remain_client_local(self):
        writer = self.open()
        writer.app.items.insert_one({"_id": 1, "when": STORED})
        writer.close()
        aware = self.open(tz_aware="true")
        value = aware.app.items.find_one({"_id": 1})["when"]
        self.assertEqual(value, UTC_MILLIS.replace(tzinfo=timezone.utc))
        self.assertEqual(value.utcoffset(), timedelta(0))
        eastern = timezone(timedelta(hours=-4))
        converted = self.open(tz_aware=True, tzinfo=eastern)
        self.assertEqual(converted.app.items.find_one({"_id": 1})["when"], datetime(2026, 1, 2, 4, 4, 5, 123000, tzinfo=eastern))
        self.assertEqual(aware.app.items.find_one({"_id": 1})["when"].utcoffset(), timedelta(0))

    def test_same_millisecond_updates_and_replacements_are_noops(self):
        client = self.open()
        items = client.app.items
        first = datetime(2026, 1, 2, 3, 4, 5, 123001, tzinfo=timezone.utc)
        same = first.replace(microsecond=123999)
        items.insert_one({"_id": 1, "when": first})
        self.assertEqual(items.update_one({"_id": 1}, {"$set": {"when": same}}).modified_count, 0)
        self.assertEqual(items.replace_one({"_id": 1}, {"_id": 1, "when": same}).modified_count, 0)
        self.assertEqual(items.find_one({"_id": 1})["when"], datetime(2026, 1, 2, 3, 4, 5, 123000))

    def test_mutable_mapping_clients_are_recursive_and_do_not_share_returned_objects(self):
        writer = self.open()
        writer.app.items.insert_one({"_id": 1, "inner": {"value": 1}, "items": [{"deep": {"value": 2}}]})
        writer.close()
        user = self.open(document_class=UserDict)
        ordered = self.open(document_class=OrderedDict)
        first = user.app.items.find_one({"_id": 1})
        recursive(self, first, UserDict)
        recursive(self, ordered.app.items.find_one({"_id": 1}), OrderedDict)
        first["inner"]["value"] = 99
        self.assertEqual(ordered.app.items.find_one({"_id": 1})["inner"]["value"], 1)

    def test_parameterized_mapping_aliases_materialize_their_origin_type(self):
        for document_class in (dict[str, object], UserDict[str, object]):
            with self.subTest(document_class=document_class):
                client = self.open(document_class=document_class)
                items = client.app[document_class.__origin__.__name__]
                items.insert_one({"_id": 1, "inner": {"value": 1}})
                document = items.find_one({"_id": 1})
                self.assertIs(type(document), document_class.__origin__)
                self.assertIs(type(document["inner"]), document_class.__origin__)
                client.close()

    def test_bson_son_is_preserved_recursively(self):
        client = self.open(document_class=SON)
        client.app.items.insert_one({"_id": 1, "inner": {}, "items": [{"deep": {}}]})
        recursive(self, client.app.items.find_one({"_id": 1}), SON)


class AsyncUpstreamReadFidelityTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_recursive_codec_clone_aggregate_distinct_and_find_modify(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2, document_class=OrderedDict, tz_aware=True) as client:
                items = client.app.items
                await items.insert_one({"_id": "main", "when": STORED, "inner": {"when": STORED}, "items": [{"deep": {"when": STORED}}]})
                found = await items.find_one({"_id": "main"})
                recursive(self, found, OrderedDict)
                self.assertEqual(found["when"], UTC_MILLIS.replace(tzinfo=timezone.utc))
                cursor = items.find({"_id": "main"})
                clone = cursor.clone()
                recursive(self, (await cursor.to_list())[0], OrderedDict)
                recursive(self, (await clone.to_list())[0], OrderedDict)
                aggregate = await items.aggregate([{"$project": {"inner": 1, "items": 1, "when": 1}}])
                recursive(self, (await aggregate.to_list())[0], OrderedDict)
                self.assertIs(type((await items.distinct("inner"))[0]), OrderedDict)
                await items.insert_one({"_id": "update", "inner": {"value": 1}})
                returned = await items.find_one_and_update({"_id": "update"}, {"$set": {"inner.value": 2}}, projection={"_id": 0, "inner": 1}, return_document=True)
                self.assertIs(type(returned), OrderedDict)
                self.assertIs(type(returned["inner"]), OrderedDict)
                self.assertEqual(returned["inner"]["value"], 2)


if __name__ == "__main__":
    unittest.main()
