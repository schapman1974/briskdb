"""Public insert-many parity plus regression coverage beyond one wire batch.

Scenarios adapted from locked TinyMongo tests/test_insert_many_semantics.py.
Source SHA-256: a0160cc7ed515603ecd73307733e4c05e2fc02289b396d3ea7ff2890b2bc960d
Private backend retry/encoding hooks are not exercised by these wheel tests.
"""

from collections import UserDict
import tempfile
import unittest

from bson import BSON, ObjectId
from bson.codec_options import CodecOptions, TypeEncoder, TypeRegistry
from bson.errors import InvalidDocument
from bson.raw_bson import RawBSONDocument
from pymongo.errors import BulkWriteError, DocumentTooLarge, InvalidOperation
from pymongo.results import InsertManyResult
from pymongo.write_concern import WriteConcern

import briskdb
from briskdb import _mongo_bulk


def late_invalid():
    return [{"value": number} for number in range(1001)] + [{"value": {1, 2}}]


class UpstreamInsertManyTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)

    def test_late_serialization_error_precedes_all_wire_batches(self):
        for ordered in (True, False):
            for bypass in (True, False):
                with self.subTest(ordered=ordered, bypass=bypass):
                    items = self.client.app[f"bad_{ordered}_{bypass}"]
                    documents = late_invalid()
                    with self.assertRaises(InvalidDocument):
                        items.insert_many(iter(documents), ordered=ordered,
                                          bypass_document_validation=bypass)
                    self.assertEqual(items.count_documents({}), 0)
                    self.assertNotIn(items.name, self.client.app.list_collection_names())
                    self.assertTrue(all(isinstance(doc["_id"], ObjectId) for doc in documents))

    def test_late_oversized_document_is_rejected_before_any_insert(self):
        limit = self.client.admin.command("hello")["maxBsonObjectSize"]
        self.assertEqual(_mongo_bulk._MAX_DOCUMENT_BYTES, limit)
        documents = [{"_id": number} for number in range(1001)] + [{"value": "x" * limit}]
        with self.assertRaises(DocumentTooLarge):
            self.client.app.large.insert_many(documents)
        self.assertEqual(self.client.app.large.count_documents({}), 0)
        self.assertNotIn("large", self.client.app.list_collection_names())

    def test_ordered_and_unordered_duplicates_preserve_counts_indices_and_original_ops(self):
        for ordered, accepted, indices in ((True, {"seed", "first"}, [1]),
                                            (False, {"seed", "first", "last"}, [1, 2])):
            with self.subTest(ordered=ordered):
                items = self.client.app[f"duplicates_{ordered}"]
                items.insert_one({"_id": "seed"})
                documents = [{"_id": name} for name in ("first", "seed", "seed", "last")]
                with self.assertRaises(BulkWriteError) as caught:
                    items.insert_many(documents, ordered=ordered)
                details = caught.exception.details
                self.assertEqual(details["nInserted"], len(accepted) - 1)
                self.assertEqual(details["writeConcernErrors"], [])
                self.assertEqual([error["index"] for error in details["writeErrors"]], indices)
                for error in details["writeErrors"]:
                    self.assertEqual(error["code"], 11000)
                    self.assertIs(error["op"], documents[error["index"]])
                    # Wire diagnostics deliberately do not disclose BSON keys.
                    self.assertNotIn("keyValue", error)
                self.assertEqual({row["_id"] for row in items.find()}, accepted)

    def test_unordered_mixed_unique_rejections_keep_input_alignment(self):
        items = self.client.app.unique
        items.create_index("email", unique=True)
        items.insert_many([{"_id": "owner", "email": "taken"}, {"_id": "missing"}])
        documents = [{"_id": "first", "email": "batch"}, {"_id": "owner"},
                     {"_id": "second", "email": "taken"}, {"_id": "third", "email": "batch"},
                     {"_id": "fourth"}, {"_id": "last", "email": "free"}]
        with self.assertRaises(BulkWriteError) as caught:
            items.insert_many(documents, ordered=False)
        self.assertEqual(caught.exception.details["nInserted"], 2)
        errors = caught.exception.details["writeErrors"]
        self.assertEqual([error["index"] for error in errors], [1, 2, 3, 4])
        for error in errors:
            self.assertIs(error["op"], documents[error["index"]])
            self.assertEqual(error["code"], 11000)
        self.assertEqual({row["_id"] for row in items.find()}, {"owner", "missing", "first", "last"})

    def test_unique_duplicates_follow_ordering_and_keep_redacted_error_details(self):
        for ordered, count in ((True, 1), (False, 2)):
            items = self.client.app[f"unique_{ordered}"]
            items.create_index("email", unique=True)
            items.insert_one({"_id": "seed", "email": "private-address"})
            documents = [{"_id": "first", "email": "first"},
                         {"_id": "duplicate", "email": "private-address"},
                         {"_id": "last", "email": "last"}]
            with self.assertRaises(BulkWriteError) as caught:
                items.insert_many(documents, ordered=ordered)
            self.assertEqual(caught.exception.details["nInserted"], count)
            error = caught.exception.details["writeErrors"][0]
            self.assertEqual((error["index"], error["code"]), (1, 11000))
            self.assertNotIn("keyPattern", error)
            self.assertNotIn("keyValue", error)
            self.assertNotIn("private-address", error["errmsg"])

    def test_error_indices_remain_global_after_multiple_driver_batches(self):
        items = self.client.app.multibatch
        items.insert_one({"_id": "seed"})
        documents = [{"_id": number} for number in range(1001)]
        documents.extend([{"_id": "seed"}, {"_id": "last"}])
        with self.assertRaises(BulkWriteError) as caught:
            items.insert_many(documents, ordered=False)
        self.assertEqual(caught.exception.details["nInserted"], 1002)
        error = caught.exception.details["writeErrors"][0]
        self.assertEqual(error["index"], 1001)
        self.assertIs(error["op"], documents[1001])
        self.assertEqual(items.count_documents({}), 1003)

    def test_tuple_and_list_ids_share_the_same_bson_identity(self):
        items = self.client.app.arrays
        with self.assertRaises(BulkWriteError) as caught:
            items.insert_many([{"_id": (1, 2)}, {"_id": [1, 2]}])
        self.assertEqual(caught.exception.details["nInserted"], 1)
        self.assertEqual(caught.exception.details["writeErrors"][0]["index"], 1)
        self.assertEqual(list(items.find()), [{"_id": [1, 2]}])

    def test_raw_error_operations_and_unacknowledged_results_keep_driver_shapes(self):
        items = self.client.app.raw
        original = RawBSONDocument(BSON.encode({"_id": "raw"}))
        self.assertEqual(items.insert_many([original]).inserted_ids, [])
        with self.assertRaises(BulkWriteError) as caught:
            items.insert_many([original])
        self.assertIs(caught.exception.details["writeErrors"][0]["op"], original)
        document = {"_id": "unacknowledged"}
        result = items.with_options(write_concern=WriteConcern(w=0)).insert_many([document])
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.inserted_ids, ["unacknowledged"])

    def test_invalid_arguments_and_failing_iterables_do_not_create_collections(self):
        items = self.client.app.invalid
        for documents in ([], iter(()), {}, None, [UserDict({}), None]):
            with self.subTest(documents=documents):
                with self.assertRaises(TypeError):
                    items.insert_many(documents)
        for ordered in (1, 0, None, "true"):
            with self.assertRaises(TypeError):
                items.insert_many([{}], ordered=ordered)
        with self.assertRaises(TypeError):
            items.insert_many([{}], bypass_document_validation=1)

        def broken():
            for number in range(1001):
                yield {"_id": number}
            raise RuntimeError("input failed")

        with self.assertRaisesRegex(RuntimeError, "input failed"):
            items.insert_many(broken())
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_iterables_mapping_ids_raw_documents_and_results_survive_restart(self):
        items = self.client.app.items
        mapping = UserDict({"value": 1})
        raw = RawBSONDocument(BSON.encode({"_id": "raw", "value": 2}))
        documents = (mapping, {"_id": None}, {"_id": {"value": True}},
                     {"_id": {"value": 1}}, raw)
        result = items.insert_many(iter(documents))
        self.assertIsInstance(result, InsertManyResult)
        self.assertTrue(result.acknowledged)
        # Stock PyMongo intentionally omits caller-supplied RawBSONDocument IDs.
        self.assertEqual(result.inserted_ids, [mapping["_id"], None, {"value": True}, {"value": 1}])
        self.client.close()
        with briskdb.MongoClient(self.root.name, shards=2) as reopened:
            self.assertEqual(reopened.app.items.count_documents({}), 5)
            self.assertEqual(reopened.app.items.find_one({"_id": mapping["_id"]})["value"], 1)
            self.assertEqual(reopened.app.items.find_one({"_id": "raw"})["value"], 2)
            with self.assertRaises(BulkWriteError) as caught:
                reopened.app.items.insert_many([{"_id": None}])
            self.assertEqual(caught.exception.details["nInserted"], 0)

    def test_custom_encoder_is_called_once_per_value_before_any_write(self):
        class Value:
            def __init__(self, value):
                self.value = value

        calls = []

        class Encoder(TypeEncoder):
            python_type = Value

            def transform_python(self, value):
                calls.append(value.value)
                if len(calls) > 1002:
                    raise AssertionError("custom value encoded twice")
                return value.value

        codec = CodecOptions(type_registry=TypeRegistry([Encoder()]))
        items = self.client.app.codec.with_options(codec_options=codec)
        documents = [{"_id": number, "value": Value(number)} for number in range(1002)]
        self.assertEqual(items.insert_many(documents).inserted_ids, list(range(1002)))
        self.assertEqual(calls, list(range(1002)))
        self.assertEqual(items.count_documents({}), 1002)
        self.assertEqual(items.find_one({"_id": 1001})["value"], 1001)

    def test_closed_client_rejects_before_consuming_generator(self):
        items = self.client.app.items
        self.client.close()

        def forbidden():
            raise AssertionError("closed client consumed input")
            yield {}

        with self.assertRaises(InvalidOperation):
            items.insert_many(forbidden())


class AsyncUpstreamInsertManyTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_preflight_and_unordered_error_indices_match_sync(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                for ordered in (True, False):
                    items = client.app[f"invalid_{ordered}"]
                    with self.assertRaises(InvalidDocument):
                        await items.insert_many(late_invalid(), ordered=ordered)
                    self.assertEqual(await items.count_documents({}), 0)
                    self.assertNotIn(items.name, await client.app.list_collection_names())
                items = client.app.good
                documents = [{"_id": 1}, {"_id": 1}, {"_id": 2}]
                with self.assertRaises(BulkWriteError) as caught:
                    await items.insert_many(documents, ordered=False)
                self.assertEqual(caught.exception.details["nInserted"], 2)
                self.assertEqual(caught.exception.details["writeErrors"][0]["index"], 1)
                self.assertIs(caught.exception.details["writeErrors"][0]["op"], documents[1])
                self.assertEqual((await items.insert_many([{"_id": 3}])).inserted_ids, [3])
                with self.assertRaises(TypeError):
                    await items.insert_many([{}], ordered=1)
                limit = (await client.admin.command("hello"))["maxBsonObjectSize"]
                with self.assertRaises(DocumentTooLarge):
                    await items.insert_many([{"value": "x" * limit}])
                await client.close()

                def forbidden():
                    raise AssertionError("closed async client consumed input")
                    yield {}

                with self.assertRaises(InvalidOperation):
                    await items.insert_many(forbidden())


if __name__ == "__main__":
    unittest.main()
