import datetime
import decimal
import inspect
import math
import random
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid
from collections import OrderedDict

import briskdb
from bson import (
    BSON,
    Binary,
    Code,
    DBRef,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    SON,
    Timestamp,
)
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.datetime_ms import DatetimeMS


DATABASE = "app"
COLLECTION = "values"
UUID_VALUE = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")


def codec_options(representation: int = UuidRepresentation.STANDARD) -> CodecOptions:
    return CodecOptions(
        tz_aware=True,
        tzinfo=datetime.timezone.utc,
        uuid_representation=representation,
        datetime_conversion=DatetimeConversion.DATETIME_AUTO,
    )


def bson_bytes(
    document: dict, representation: int = UuidRepresentation.STANDARD
) -> bytes:
    return BSON.encode(document, codec_options=codec_options(representation))


class PythonDocumentApiTests(unittest.TestCase):
    def test_aggregate_transforms_preserve_bson_and_original_documents(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            documents = [{"_id": Int64(index), "source": Decimal128("1.50"), "items": [{"x": index}, None], "secret": "private"} for index in range(3)]
            pipeline = [
                {"$set": {"source": 0, "old": "$source", "items.tag": "$_id", "secret": "$$REMOVE"}},
                {"$project": {"_id": 0, "items": 1, "amount": "$old", "n": {"$size": "$items"},
                              "literal": {"$literal": Binary(b"\x00\xff", 128)}, "shell.gone": "$$REMOVE"}},
                {"$sort": {"amount": 1}},
            ]
            expected = [{"items": [{"x": index, "tag": Int64(index)}, {"tag": Int64(index)}],
                         "amount": Decimal128("1.50"), "n": 2, "literal": Binary(b"\x00\xff", 128), "shell": {}} for index in range(3)]
            before = bson_bytes({"documents": documents, "pipeline": pipeline})
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for document in documents:
                        session.insert_one(DATABASE, COLLECTION, document)
                    page = session.aggregate(DATABASE, COLLECTION, pipeline, batch_size=0)
                    rows = []
                    while page["cursor_id"] is not None:
                        page = session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                        rows.extend(page["documents"])
                    self.assertEqual(bson_bytes({"rows": rows}), bson_bytes({"rows": expected}))
                    self.assertEqual(bson_bytes({"rows": session.find(DATABASE, COLLECTION)["documents"]}), bson_bytes({"rows": documents}))
                    self.assertEqual(bson_bytes({"documents": documents, "pipeline": pipeline}), before)
                    with self.assertRaises(briskdb.InvalidQueryError):
                        session.aggregate(DATABASE, "absent", [{"$project": {"v": {"$ifNull": []}}}])
            with briskdb.open(root, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes({"rows": session.aggregate(DATABASE, COLLECTION, pipeline)["documents"]}), bson_bytes({"rows": expected}))

    def test_aggregate_streaming_and_sorted_cursors_keep_bson_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            documents = [{"_id": Int64(index), "group": index % 3, "value": Decimal128(str(index))} for index in range(32)]
            pipeline = [{"$sort": {"_id": -1}}, {"$match": {"group": 1}}, {"$skip": Decimal128("1.0")}, {"$limit": 6.0}]
            before = bson_bytes({"pipeline": pipeline})
            expected = [row for row in reversed(documents) if row["group"] == 1][1:7]
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for document in documents:
                        session.insert_one(DATABASE, COLLECTION, document)
                    identifier = uuid.uuid4()
                    page = session.aggregate(DATABASE, COLLECTION, pipeline, batch_size=0, request_id=identifier)
                    self.assertEqual(page["request_id"], identifier)
                    self.assertEqual(page["kind"], "cursor")
                    self.assertEqual(page["plan"]["kind"], "scatter")
                    self.assertEqual(page["documents"], [])
                    result = []
                    while page["cursor_id"] is not None:
                        page = session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=2)
                        result.extend(page["documents"])
                    self.assertEqual(bson_bytes({"rows": result}), bson_bytes({"rows": expected}))
                    self.assertEqual(bson_bytes({"pipeline": pipeline}), before)
                    self.assertEqual(session.aggregate(DATABASE, COLLECTION, [{"$count": "n"}])["documents"], [{"n": 32}])
                    self.assertEqual(session.aggregate(DATABASE, COLLECTION, [{"$skip": 7}, {"$limit": 10}, {"$match": {"group": 1}}])["documents"], [row for row in documents[7:17] if row["group"] == 1])
                    self.assertEqual(session.find(DATABASE, COLLECTION)["documents"], documents)
                    for invalid in [None, {}, (), False, [1], ["private"]]:
                        with self.assertRaises(briskdb.TypeMismatchError):
                            session.aggregate(DATABASE, COLLECTION, invalid)
                    for invalid in [[{}], [{"$limit": 0}], [{"$count": Code("private")}]]:
                        with self.assertRaises(briskdb.BriskDBError) as raised:
                            session.aggregate(DATABASE, COLLECTION, invalid)
                        self.assertNotIn("private", str(raised.exception))
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.aggregate(DATABASE, COLLECTION, [], batch_size=2, max_result_rows=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.aggregate(DATABASE, COLLECTION, [], cancellation=token)
                    page = session.aggregate(DATABASE, COLLECTION, [], batch_size=1)
                    self.assertTrue(session.kill_cursor(DATABASE, COLLECTION, page["cursor_id"])["killed"])
            with briskdb.open(root, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes({"rows": session.aggregate(DATABASE, COLLECTION, pipeline)["documents"]}), bson_bytes({"rows": expected}))

    def test_distinct_preserves_first_representation_paths_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    documents = [
                        {"_id": 0, "v": [Int64(1), True, None, [2, 3]], "nested": {"a": ["first", "second"]}},
                        {"_id": 1, "v": [1.0, False, [2.0, Int64(3)]], "nested": [{"a": "no-fanout"}]},
                        {"_id": 2, "v": Decimal128("1.00"), "": "empty-key", "payload": "x" * 100000},
                        {"_id": 3},
                    ]
                    for document in documents:
                        session.insert_one(DATABASE, COLLECTION, document)
                    result = session.distinct(DATABASE, COLLECTION, "v", max_result_rows=5, max_result_bytes=512)
                    expected = [Int64(1), True, None, [2, 3], False]
                    self.assertEqual(bson_bytes({"values": result["values"]}), bson_bytes({"values": expected}))
                    self.assertEqual(result["kind"], "distinct")
                    self.assertEqual(result["plan"]["kind"], "scatter")
                    self.assertEqual(session.distinct(DATABASE, COLLECTION, "nested.a")["values"], ["first", "second"])
                    self.assertEqual(session.distinct(DATABASE, COLLECTION, "")["values"], ["empty-key"])
                    self.assertEqual(session.distinct(DATABASE, COLLECTION, "v\x00")["values"], [])
                    point = session.distinct(DATABASE, COLLECTION, "v", {"_id": 2}, max_result_bytes=256)
                    self.assertEqual(point["plan"]["kind"], "point")
                    self.assertIsInstance(point["values"][0], Decimal128)
                    for field in [Code("v"), None, 1, [], False]:
                        with self.assertRaises(briskdb.TypeMismatchError):
                            session.distinct(DATABASE, COLLECTION, field)
                    for query in [[], (), 0, False, ""]:
                        with self.assertRaises(briskdb.TypeMismatchError):
                            session.distinct(DATABASE, COLLECTION, "v", query)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.distinct(DATABASE, COLLECTION, "v", max_result_rows=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.distinct(DATABASE, COLLECTION, "v", cancellation=token)
                    for document in documents:
                        found = session.find(DATABASE, COLLECTION, {"_id": document["_id"]})["documents"][0]
                        self.assertEqual(bson_bytes(found), bson_bytes(document))
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    values = session.distinct(DATABASE, COLLECTION, "v")["values"]
                    self.assertEqual(bson_bytes({"values": values}), bson_bytes({"values": expected}))

    def test_collection_existence_uses_scalar_result_and_request_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    missing = session.collection_exists(DATABASE, COLLECTION)
                    self.assertFalse(missing["exists"])
                    self.assertEqual(session.list_collections(DATABASE)["collections"], [])
                    session.create_collection(DATABASE, COLLECTION, options={"opaque": "x" * 10000})
                    identifier = uuid.uuid4()
                    exists = session.collection_exists(DATABASE, COLLECTION, request_id=identifier,
                                                       max_result_rows=1, max_result_bytes=34)
                    self.assertEqual(exists, {"kind": "collection_exists", "exists": True,
                                              "request_id": identifier, "plan": None})
                    self.assertFalse(session.collection_exists("other", COLLECTION)["exists"])
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.collection_exists(DATABASE, COLLECTION, cancellation=token)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.collection_exists(DATABASE, COLLECTION, max_result_bytes=33)
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertTrue(session.collection_exists(DATABASE, COLLECTION)["exists"])

    def test_sorted_cursors_use_original_fields_and_keep_stable_ties(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            for index in range(30):
                session.insert_one(DATABASE, COLLECTION, {"_id": index, "rank": Int64(index % 5), "hidden": True})
            expected = sorted(range(30), key=lambda index: -(index % 5))[3:22]
            batch = session.find(DATABASE, COLLECTION, {"hidden": True}, sort={"rank": -1},
                                 projection={"_id": 1}, skip=3, limit=19, batch_size=0)
            rows = []
            while not batch["exhausted"]:
                batch = session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=4)
                rows.extend(batch["documents"])
            self.assertEqual(rows, [{"_id": index} for index in expected])
            self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": 4}, sort={"rank": -1})["plan"]["kind"], "point")
            self.assertEqual(len(session.find(DATABASE, COLLECTION, sort={})["documents"]), 30)
            with self.assertRaises(briskdb.InvalidQueryError):
                session.find(DATABASE, COLLECTION, sort={"rank": 0})
            with self.assertRaises(briskdb.TypeMismatchError):
                session.find(DATABASE, COLLECTION, sort=[("rank", 1)])
            session.close()
            database.close()

    def test_projections_preserve_bson_arrays_order_and_cursor_state(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            documents = [SON([
                ("before", Int64(index)), ("_id", index),
                ("items", [{"name": "a", "value": 1}, {"value": 2}, None, [{"name": "b"}]]),
                ("secret", "filter-me"), ("large", "x" * 5000),
            ]) for index in range(12)]
            for document in documents:
                session.insert_one(DATABASE, COLLECTION, document)
            batch = session.find(DATABASE, COLLECTION, {"secret": "filter-me"},
                                 projection={"before": 1, "items.name": 1, "_id": 0},
                                 batch_size=3, max_result_bytes=600)
            found = batch["documents"]
            while not batch["exhausted"]:
                batch = session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=3, max_result_bytes=600)
                found.extend(batch["documents"])
            expected = [SON([("before", Int64(index)), ("items", [{"name": "a"}, {}, [{"name": "b"}]])]) for index in range(12)]
            self.assertEqual([bson_bytes(row) for row in found], [bson_bytes(row) for row in expected])
            for fields in [["before", "before"], ("before",), {"before"}, frozenset({"before"})]:
                self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": 7}, projection=fields)["documents"], [{"before": Int64(7), "_id": 7}])
            for fields in [None, {}, []]:
                actual = session.find(DATABASE, COLLECTION, {"_id": 7}, projection=fields)["documents"][0]
                self.assertEqual(bson_bytes(actual), bson_bytes(documents[7]))
            with self.assertRaises(briskdb.InvalidQueryError):
                session.find(DATABASE, COLLECTION, projection={"before": 1, "secret": 0})
            with self.assertRaises(briskdb.UnsupportedError):
                session.find(DATABASE, COLLECTION, projection={"items.0": 1})
            with self.assertRaises(briskdb.TypeMismatchError):
                session.find(DATABASE, COLLECTION, projection=[1])
            with self.assertRaises(briskdb.LimitExceededError):
                session.find(DATABASE, COLLECTION, projection=["before"] * 4097)
            session.close()
            database.close()

    def test_retained_cursor_batches_ownership_close_and_kill(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            for index in range(24):
                session.insert_one(DATABASE, COLLECTION, {"_id": index, "rank": index})
            batch = session.find(DATABASE, COLLECTION, {"rank": {"$gte": 5}}, skip=2, limit=13, batch_size=3)
            self.assertFalse(batch["exhausted"])
            identifier = batch["cursor_id"]
            foreign = database.session()
            with self.assertRaises(briskdb.FailedPreconditionError):
                foreign.get_more(DATABASE, COLLECTION, identifier)
            self.assertFalse(foreign.kill_cursor(DATABASE, COLLECTION, identifier)["killed"])
            found = batch["documents"]
            while not batch["exhausted"]:
                batch = session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=2)
                found.extend(batch["documents"])
            self.assertEqual([doc["_id"] for doc in found], list(range(7, 20)))
            with self.assertRaises(briskdb.FailedPreconditionError):
                session.get_more(DATABASE, COLLECTION, identifier)
            empty = session.find(DATABASE, COLLECTION, batch_size=0)
            self.assertEqual(empty["documents"], [])
            killed = session.kill_cursor(DATABASE, COLLECTION, empty["cursor_id"])
            self.assertEqual(killed["kind"], "cursor_killed")
            self.assertTrue(killed["killed"])
            self.assertFalse(session.kill_cursor(DATABASE, COLLECTION, empty["cursor_id"])["killed"])
            foreign.close()
            session.close()
            database.close()

    def test_general_matcher_filters_before_global_skip_limit_and_count(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            for index in range(24):
                session.insert_one(DATABASE, COLLECTION, {
                    "_id": index,
                    "items": [{"kind": "quiz", "score": index}],
                    "name": "Alpha" if index % 2 == 0 else "Beta",
                })
            query = {
                "items": {"$elemMatch": {"kind": "quiz", "score": {"$gte": 12}}},
                "name": {"$regex": "^alpha$", "$options": "i"},
            }
            found = session.find(DATABASE, COLLECTION, query, skip=1, limit=3, batch_size=3)
            self.assertEqual([doc["_id"] for doc in found["documents"]], [14, 16, 18])
            self.assertEqual(session.count_documents(DATABASE, COLLECTION, query)["count"], 6)
            self.assertEqual(session.count_documents(DATABASE, COLLECTION, query, skip=1, limit=3)["count"], 3)
            self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": {"$eq": 7.0}})["documents"][0]["_id"], 7)
            session.close()
            database.close()
            database = briskdb.open(root, documents=True)
            session = database.session()
            self.assertEqual(session.count_documents(DATABASE, COLLECTION, query)["count"], 6)
            session.close()
            database.close()

    def test_native_document_signatures_match_the_typed_api(self) -> None:
        for method_name in ("find", "aggregate", "get_more", "list_collections", "list_indexes"):
            with self.subTest(method=method_name):
                signature = inspect.signature(getattr(briskdb.Session, method_name))
                self.assertEqual(signature.parameters["batch_size"].default, 101)

    def open_session(
        self,
        root: str,
        *,
        uuid_representation: str = "standard",
        collection: str = COLLECTION,
    ):
        database = briskdb.open(
            root,
            shards=4,
            documents=True,
            uuid_representation=uuid_representation,
        )
        session = database.session()
        session.create_collection(DATABASE, collection)
        return database, session

    def test_document_commands_share_plans_identity_and_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database = briskdb.open(root, shards=4)
            disabled = database.session()
            with self.assertRaises(briskdb.FailedPreconditionError) as raised:
                disabled.create_collection(DATABASE, COLLECTION)
            self.assertEqual(raised.exception.code, "failed_precondition")
            disabled.close()
            database.close()

            database = briskdb.open(root, documents=True)
            session = database.session()
            self.assertEqual(session.list_collections(DATABASE)["collections"], [])

            request_id = uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
            created = session.create_collection(
                DATABASE,
                COLLECTION,
                options=OrderedDict([("first", 1), ("second", "two")]),
                request_id=request_id,
            )
            self.assertEqual(set(created), {"request_id", "plan", "kind", "collection"})
            self.assertEqual(created["request_id"], request_id)
            self.assertIsNone(created["plan"])
            self.assertEqual(created["kind"], "collection")
            collection = created["collection"]
            self.assertEqual(collection["namespace"], "app.values")
            self.assertEqual(list(collection["options"]), ["first", "second"])
            self.assertEqual(collection["placement"], {"code": 1, "version": 1})
            self.assertEqual(
                collection["indexes"],
                [
                    {
                        "name": "_id_",
                        "keys": {"_id": 1},
                        "unique": True,
                        "built_in": True,
                        "lifecycle": "ready",
                    }
                ],
            )

            generated_request_id = session.list_collections(DATABASE)["request_id"]
            self.assertIsInstance(generated_request_id, uuid.UUID)
            self.assertNotEqual(generated_request_id.int, 0)

            declared = session.create_index(
                DATABASE,
                COLLECTION,
                OrderedDict([("body", 1), ("rank", -1)]),
                name="body_rank",
                unique=True,
            )
            self.assertEqual(
                set(declared),
                {"request_id", "plan", "kind", "index_name", "lifecycle"},
            )
            self.assertEqual(declared["kind"], "index_name")
            self.assertEqual(declared["index_name"], "body_rank")
            self.assertEqual(declared["lifecycle"], "pending_build")
            self.assertIsNone(declared["plan"])
            indexes = session.list_indexes(DATABASE, COLLECTION)["indexes"]
            self.assertEqual(
                [index["name"] for index in indexes], ["_id_", "body_rank"]
            )
            self.assertEqual(list(indexes[1]["keys"]), ["body", "rank"])
            self.assertTrue(indexes[1]["unique"])
            self.assertFalse(indexes[1]["built_in"])
            self.assertEqual(indexes[1]["lifecycle"], "pending_build")

            first = OrderedDict([("_id", "first"), ("body", "one")])
            second = OrderedDict([("_id", "second"), ("body", "two")])
            inserted = session.insert_one(DATABASE, COLLECTION, first)
            session.insert_one(DATABASE, COLLECTION, second)
            self.assertEqual(inserted["kind"], "insert")
            self.assertTrue(inserted["acknowledged"])
            self.assertEqual(inserted["inserted_count"], 1)
            self.assertEqual(inserted["inserted_ids"], ["first"])
            self.assertEqual(inserted["plan"]["kind"], "point")
            self.assertEqual(len(inserted["plan"]["shards"]), 1)

            exact = session.find(DATABASE, COLLECTION, {"_id": "second"})
            self.assertEqual(exact["documents"], [second])
            self.assertEqual(exact["plan"]["kind"], "point")
            self.assertEqual(
                exact["namespace"], {"database": DATABASE, "collection": COLLECTION}
            )
            self.assertIsNone(exact["cursor_id"])
            self.assertTrue(exact["exhausted"])

            scatter = session.find(DATABASE, COLLECTION, limit=2, batch_size=2)
            self.assertEqual(scatter["documents"], [first, second])
            self.assertEqual(scatter["plan"]["kind"], "scatter")
            self.assertEqual(scatter["plan"]["shards"], [0, 1, 2, 3])
            count = session.count_documents(DATABASE, COLLECTION)
            self.assertEqual(count["count"], 2)
            self.assertEqual(count["plan"]["kind"], "scatter")
            exact_count = session.count_documents(
                DATABASE, COLLECTION, {"_id": "first"}
            )
            self.assertEqual(exact_count["count"], 1)
            self.assertEqual(exact_count["plan"]["kind"], "point")

            deleted = session.delete_one(DATABASE, COLLECTION, {"_id": "first"})
            self.assertTrue(deleted["acknowledged"])
            self.assertEqual(deleted["deleted_count"], 1)
            self.assertEqual(deleted["plan"]["kind"], "point")
            self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 1)

            source = {"body": "generated", "stamp": Timestamp(0, 0)}
            generated = session.insert_one(DATABASE, COLLECTION, source)
            generated_id = generated["inserted_ids"][0]
            self.assertIsInstance(generated_id, ObjectId)
            self.assertEqual(source, {"body": "generated", "stamp": Timestamp(0, 0)})
            actual = session.find(DATABASE, COLLECTION, {"_id": generated_id})["documents"][0]
            self.assertEqual(actual["body"], source["body"])
            self.assertGreater(actual["stamp"].time, 0)
            self.assertEqual(list(actual), ["_id", "body", "stamp"])

            session.close()
            database.close()

    def test_every_bson_family_round_trips_with_exact_wire_representation(self) -> None:
        nan = struct.unpack("<d", bytes.fromhex("010000000000f87f"))[0]
        document = OrderedDict(
            [
                ("_id", ObjectId("64b000000000000000000001")),
                ("null", None),
                ("boolean", True),
                ("int32_min", -(2**31)),
                ("int32_max", 2**31 - 1),
                ("plain_int64_low", -(2**31) - 1),
                ("plain_int64_high", 2**31),
                ("plain_int64_min", -(2**63)),
                ("plain_int64_max", 2**63 - 1),
                ("int64_min", Int64(-(2**63))),
                ("int64_max", Int64(2**63 - 1)),
                ("forced_int64", Int64(7)),
                ("negative_zero", -0.0),
                ("nan", nan),
                ("infinity", float("inf")),
                ("text", "snowman ☃ and nul \x00"),
                ("bytes", b"\x00\xffbytes"),
                ("binary_zero", Binary(b"zero", 0)),
                ("bytearray", bytearray(b"mutable")),
                ("memoryview", memoryview(b"view")),
                ("binary_old", Binary(b"old-binary", 2)),
                ("binary_user", Binary(b"user", 128)),
                ("uuid", UUID_VALUE),
                (
                    "binary_uuid",
                    Binary.from_uuid(UUID_VALUE, UuidRepresentation.STANDARD),
                ),
                (
                    "binary_legacy",
                    Binary.from_uuid(UUID_VALUE, UuidRepresentation.JAVA_LEGACY),
                ),
                ("binary_legacy_short", Binary(b"not-a-uuid", 3)),
                ("object_id", ObjectId("64b000000000000000000002")),
                ("date_naive", datetime.datetime(1969, 12, 31, 23, 59, 59, 999999)),
                (
                    "date_offset",
                    datetime.datetime(
                        2024,
                        1,
                        2,
                        3,
                        4,
                        5,
                        678999,
                        tzinfo=datetime.timezone(
                            datetime.timedelta(hours=5, minutes=30)
                        ),
                    ),
                ),
                ("date_out_of_range", DatetimeMS(2**62)),
                ("decimal", Decimal128("123456789.0012300")),
                (
                    "decimal_nan",
                    Decimal128.from_bid(bytes.fromhex("01" + "00" * 14 + "7c")),
                ),
                (
                    "decimal_noncanonical_zero",
                    Decimal128.from_bid(
                        bytes.fromhex("00000000648e8d37c087adbe09ed4130")
                    ),
                ),
                ("decimal_snan", Decimal128("sNaN")),
                ("decimal_infinity", Decimal128("-Infinity")),
                ("regex", Regex("^a.*z$", "mi")),
                ("timestamp", Timestamp(2**32 - 1, 2**32 - 1)),
                ("minimum", MinKey()),
                ("maximum", MaxKey()),
                ("code", Code("return 1;")),
                (
                    "scoped_code",
                    Code("return value;", SON([("value", Int64(2)), ("other", 1)])),
                ),
                ("array", [None, Int64(9), {"nested": Binary(b"x", 42)}]),
                ("tuple", (1, Int64(2))),
                ("ordered", SON([("z", 1), ("a", 2)])),
            ]
        )
        expected_document = OrderedDict(document)
        expected_document["bytearray"] = bytes(document["bytearray"])
        expected_document["memoryview"] = bytes(document["memoryview"])

        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            inserted = session.insert_one(DATABASE, COLLECTION, document)
            self.assertEqual(inserted["inserted_ids"], [document["_id"]])
            returned = session.find(DATABASE, COLLECTION, {"_id": document["_id"]})[
                "documents"
            ][0]

            self.assertEqual(bson_bytes(returned), bson_bytes(expected_document))
            self.assertEqual(list(returned), list(document))
            self.assertIs(type(returned["int32_min"]), int)
            self.assertIsInstance(returned["plain_int64_low"], Int64)
            self.assertIsInstance(returned["plain_int64_high"], Int64)
            self.assertIsInstance(returned["plain_int64_min"], Int64)
            self.assertIsInstance(returned["plain_int64_max"], Int64)
            self.assertIsInstance(returned["int64_min"], Int64)
            self.assertIsInstance(returned["forced_int64"], Int64)
            self.assertEqual(
                struct.pack("<d", returned["negative_zero"]),
                struct.pack("<d", -0.0),
            )
            self.assertTrue(math.isnan(returned["nan"]))
            self.assertEqual(struct.pack("<d", returned["nan"]), struct.pack("<d", nan))
            self.assertIsInstance(returned["bytes"], bytes)
            self.assertIs(type(returned["binary_zero"]), bytes)
            self.assertEqual(returned["binary_zero"], b"zero")
            self.assertIs(type(returned["bytearray"]), bytes)
            self.assertEqual(returned["bytearray"], b"mutable")
            self.assertIs(type(returned["memoryview"]), bytes)
            self.assertEqual(returned["memoryview"], b"view")
            self.assertIsInstance(returned["binary_old"], Binary)
            self.assertEqual(returned["binary_old"].subtype, 2)
            self.assertEqual(bytes(returned["binary_old"]), b"old-binary")
            self.assertIsInstance(returned["binary_user"], Binary)
            self.assertEqual(returned["binary_user"].subtype, 128)
            self.assertEqual(returned["uuid"], UUID_VALUE)
            self.assertEqual(returned["binary_uuid"], UUID_VALUE)
            self.assertIsInstance(returned["binary_legacy"], Binary)
            self.assertEqual(returned["binary_legacy"].subtype, 3)
            self.assertIsInstance(returned["binary_legacy_short"], Binary)
            self.assertEqual(returned["binary_legacy_short"].subtype, 3)
            self.assertEqual(returned["decimal"].bid, document["decimal"].bid)
            self.assertEqual(returned["decimal_nan"].bid, document["decimal_nan"].bid)
            self.assertEqual(
                returned["decimal_noncanonical_zero"].bid,
                document["decimal_noncanonical_zero"].bid,
            )
            self.assertEqual(returned["decimal_snan"].bid, document["decimal_snan"].bid)
            self.assertEqual(
                returned["decimal_infinity"].bid, document["decimal_infinity"].bid
            )
            self.assertEqual(returned["date_naive"].tzinfo, datetime.timezone.utc)
            self.assertEqual(
                returned["date_naive"],
                datetime.datetime(
                    1969, 12, 31, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc
                ),
            )
            self.assertEqual(returned["date_offset"].tzinfo, datetime.timezone.utc)
            self.assertIsInstance(returned["date_out_of_range"], DatetimeMS)
            self.assertEqual(int(returned["date_out_of_range"]), 2**62)
            self.assertEqual(list(returned["ordered"]), ["z", "a"])
            self.assertEqual(returned["tuple"], [1, Int64(2)])
            self.assertIsInstance(returned["regex"], Regex)
            self.assertIsInstance(returned["timestamp"], Timestamp)
            self.assertIsInstance(returned["minimum"], MinKey)
            self.assertIsInstance(returned["maximum"], MaxKey)
            self.assertIsInstance(returned["code"], Code)
            self.assertIsInstance(returned["scoped_code"], Code)
            self.assertEqual(list(returned["scoped_code"].scope), ["value", "other"])

            session.close()
            database.close()

    def test_semantic_id_aliases_route_without_losing_stored_representation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)

            stored_integer_id = Int64(1)
            session.insert_one(
                DATABASE,
                COLLECTION,
                {"_id": stored_integer_id, "kind": "int64"},
            )
            integer_match = session.find(DATABASE, COLLECTION, {"_id": 1})
            self.assertEqual(integer_match["plan"]["kind"], "point")
            self.assertIsInstance(integer_match["documents"][0]["_id"], Int64)
            self.assertEqual(integer_match["documents"][0]["_id"], 1)
            self.assertEqual(
                session.delete_one(DATABASE, COLLECTION, {"_id": 1})["deleted_count"],
                1,
            )

            matching_binary = Binary.from_uuid(UUID_VALUE, UuidRepresentation.STANDARD)
            session.insert_one(
                DATABASE,
                COLLECTION,
                {"_id": UUID_VALUE, "kind": "uuid"},
            )
            uuid_match = session.find(DATABASE, COLLECTION, {"_id": matching_binary})
            self.assertEqual(uuid_match["plan"]["kind"], "point")
            self.assertEqual(uuid_match["documents"][0]["_id"], UUID_VALUE)
            self.assertEqual(
                session.delete_one(DATABASE, COLLECTION, {"_id": matching_binary})[
                    "deleted_count"
                ],
                1,
            )
            session.close()
            database.close()

    def test_all_uuid_representations_match_pymongo_binary_rules(self) -> None:
        representations = {
            "standard": (
                UuidRepresentation.STANDARD,
                4,
                "00112233445566778899aabbccddeeff",
            ),
            "python_legacy": (
                UuidRepresentation.PYTHON_LEGACY,
                3,
                "00112233445566778899aabbccddeeff",
            ),
            "java_legacy": (
                UuidRepresentation.JAVA_LEGACY,
                3,
                "7766554433221100ffeeddccbbaa9988",
            ),
            "csharp_legacy": (
                UuidRepresentation.CSHARP_LEGACY,
                3,
                "33221100554477668899aabbccddeeff",
            ),
        }
        for name, (
            representation,
            expected_subtype,
            expected_hex,
        ) in representations.items():
            with (
                self.subTest(representation=name),
                tempfile.TemporaryDirectory() as root,
            ):
                database, session = self.open_session(root, uuid_representation=name)
                matching = Binary.from_uuid(UUID_VALUE, representation)
                self.assertEqual(matching.subtype, expected_subtype)
                self.assertEqual(bytes(matching).hex(), expected_hex)
                mismatch_representation = (
                    UuidRepresentation.PYTHON_LEGACY
                    if representation == UuidRepresentation.STANDARD
                    else UuidRepresentation.STANDARD
                )
                mismatching = Binary.from_uuid(UUID_VALUE, mismatch_representation)
                document = {
                    "_id": name,
                    "native": UUID_VALUE,
                    "matching_binary": matching,
                    "mismatching_binary": mismatching,
                }
                session.insert_one(DATABASE, COLLECTION, document)
                returned = session.find(DATABASE, COLLECTION, {"_id": name})[
                    "documents"
                ][0]
                self.assertEqual(returned["native"], UUID_VALUE)
                self.assertEqual(returned["matching_binary"], UUID_VALUE)
                self.assertIsInstance(returned["mismatching_binary"], Binary)
                self.assertEqual(
                    bytes(returned["mismatching_binary"]), bytes(mismatching)
                )
                self.assertEqual(
                    returned["mismatching_binary"].subtype, mismatching.subtype
                )
                self.assertEqual(
                    bson_bytes(returned, representation),
                    bson_bytes(document, representation),
                )
                session.close()
                database.close()

        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(
                root, uuid_representation="unspecified"
            )
            with self.assertRaises(briskdb.UnsupportedError) as raised:
                session.insert_one(
                    DATABASE, COLLECTION, {"_id": "native", "uuid": UUID_VALUE}
                )
            self.assertEqual(raised.exception.code, "unsupported")

            standard = Binary.from_uuid(UUID_VALUE, UuidRepresentation.STANDARD)
            legacy = Binary.from_uuid(UUID_VALUE, UuidRepresentation.JAVA_LEGACY)
            session.insert_one(
                DATABASE,
                COLLECTION,
                {"_id": "binary", "standard": standard, "legacy": legacy},
            )
            returned = session.find(DATABASE, COLLECTION, {"_id": "binary"})[
                "documents"
            ][0]
            self.assertIsInstance(returned["standard"], Binary)
            self.assertIsInstance(returned["legacy"], Binary)
            self.assertEqual(bytes(returned["standard"]), bytes(standard))
            self.assertEqual(bytes(returned["legacy"]), bytes(legacy))
            self.assertEqual(returned["standard"].subtype, 4)
            self.assertEqual(returned["legacy"].subtype, 3)
            session.close()
            database.close()

    def test_random_ordered_documents_round_trip_as_bson(self) -> None:
        generator = random.Random(198)
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)
            for index in range(40):
                values = [
                    None,
                    generator.choice([True, False]),
                    generator.randint(-(2**31), 2**31 - 1),
                    Int64(generator.randint(-(2**63), 2**63 - 1)),
                    struct.unpack("<d", generator.randbytes(8))[0],
                    "".join(
                        chr(generator.randint(0x20, 0x7E))
                        for _ in range(generator.randint(0, 32))
                    ),
                    generator.randbytes(generator.randint(0, 32)),
                    Binary(generator.randbytes(generator.randint(0, 16)), 128),
                    ObjectId(generator.randbytes(12)),
                    Decimal128(str(generator.randint(-(10**20), 10**20))),
                    Timestamp(generator.randrange(2**32), generator.randrange(2**32)),
                    Regex("item-{}".format(index), "im"),
                    [Int64(index), SON([("b", 2), ("a", 1)])],
                ]
                generator.shuffle(values)
                document = OrderedDict([("_id", "random-{}".format(index))])
                document.update(
                    ("value-{}".format(i), value) for i, value in enumerate(values)
                )
                session.insert_one(DATABASE, COLLECTION, document)
                returned = session.find(DATABASE, COLLECTION, {"_id": document["_id"]})[
                    "documents"
                ][0]
                self.assertEqual(bson_bytes(returned), bson_bytes(document))
                self.assertEqual(list(returned), list(document))
            session.close()
            database.close()

    def test_invalid_bson_values_fail_deterministically_and_recover(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            database, session = self.open_session(root)

            for value in [-(2**63) - 1, 2**63]:
                with (
                    self.subTest(integer=value),
                    self.assertRaises(briskdb.NumericOutOfRangeError) as raised,
                ):
                    session.insert_one(
                        DATABASE, COLLECTION, {"_id": "integer", "value": value}
                    )
                self.assertEqual(raised.exception.code, "numeric_out_of_range")

            cyclic = {"_id": "cyclic", "secret": "must-not-leak"}
            cyclic["cycle"] = cyclic
            with self.assertRaises(briskdb.InvalidArgumentError) as raised:
                session.insert_one(DATABASE, COLLECTION, cyclic)
            self.assertEqual(raised.exception.code, "invalid_argument")
            self.assertNotIn("must-not-leak", str(raised.exception))

            accepted_depth = {"_id": "depth-100"}
            cursor = accepted_depth
            for _ in range(99):
                child = {}
                cursor["nested"] = child
                cursor = child
            cursor["value"] = 1
            session.insert_one(DATABASE, COLLECTION, accepted_depth)

            deep = {"_id": "depth-101"}
            cursor = deep
            for _ in range(100):
                child = {}
                cursor["nested"] = child
                cursor = child
            cursor["value"] = 1
            with self.assertRaises(briskdb.LimitExceededError) as raised:
                session.insert_one(DATABASE, COLLECTION, deep)
            self.assertEqual(raised.exception.code, "limit_exceeded")

            code_scope = {}
            cyclic_code = Code("return self;", code_scope)
            code_scope["self"] = cyclic_code
            with self.assertRaises(briskdb.InvalidArgumentError):
                session.insert_one(
                    DATABASE,
                    COLLECTION,
                    {"_id": "cyclic-code", "value": cyclic_code},
                )

            invalid_documents = [
                ({"_id": "key", 1: "not a string"}, briskdb.TypeMismatchError),
                ({"_id": "nul", "bad\x00key": 1}, briskdb.InvalidArgumentError),
                (
                    {"_id": "surrogate-value", "value": "\ud800"},
                    briskdb.InvalidTextEncodingError,
                ),
                (
                    {"_id": "surrogate-key", "\ud800": 1},
                    briskdb.InvalidTextEncodingError,
                ),
                ({"_id": "set", "value": {1, 2}}, briskdb.TypeMismatchError),
                (
                    {"_id": "dbref", "value": DBRef("values", "private-id")},
                    briskdb.TypeMismatchError,
                ),
                (
                    {"_id": "decimal", "value": decimal.Decimal("1.25")},
                    briskdb.TypeMismatchError,
                ),
                (
                    {"_id": "regex-bytes", "value": Regex(b"bytes", 0)},
                    briskdb.TypeMismatchError,
                ),
                (
                    {"_id": "regex-flags", "value": Regex("flags", 256)},
                    briskdb.InvalidArgumentError,
                ),
            ]
            for document, error_type in invalid_documents:
                with (
                    self.subTest(document_id=document["_id"]),
                    self.assertRaises(error_type),
                ):
                    session.insert_one(DATABASE, COLLECTION, document)

            oversized = {"_id": "oversized", "value": "x" * (16 * 1024 * 1024)}
            with self.assertRaises(briskdb.LimitExceededError):
                session.insert_one(DATABASE, COLLECTION, oversized)
            del oversized

            class MisleadingAstralString(str):
                def isascii(self) -> bool:
                    return True

            oversized_astral = MisleadingAstralString("😀" * (4 * 1024 * 1024))
            with self.assertRaises(briskdb.LimitExceededError):
                session.insert_one(
                    DATABASE,
                    COLLECTION,
                    {"_id": "oversized-astral", "value": oversized_astral},
                )
            del oversized_astral

            shared = {"child": 1}
            valid = {"_id": "shared", "left": shared, "right": shared}
            session.insert_one(DATABASE, COLLECTION, valid)
            self.assertEqual(
                session.find(DATABASE, COLLECTION, {"_id": "shared"})["documents"],
                [{"_id": "shared", "left": {"child": 1}, "right": {"child": 1}}],
            )

            self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 2)
            session.close()
            database.close()

    def test_request_controls_errors_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            config = briskdb.Config(
                shards=4, documents=True, uuid_representation="standard"
            )
            database = briskdb.open(root, config=config)
            self.assertTrue(database.config.documents)
            self.assertEqual(database.config.uuid_representation, "standard")
            session = database.session()
            session.create_collection(DATABASE, COLLECTION)
            session.insert_one(DATABASE, COLLECTION, {"_id": 1})
            session.insert_one(DATABASE, COLLECTION, {"_id": 2})

            token = briskdb.CancellationToken()
            token.cancel()
            with self.assertRaises(briskdb.CancelledError) as cancelled:
                session.list_collections(DATABASE, cancellation=token)
            self.assertEqual(cancelled.exception.code, "cancelled")

            with self.assertRaises(briskdb.LimitExceededError):
                session.find(DATABASE, COLLECTION, max_result_rows=1)
            with self.assertRaises(briskdb.LimitExceededError):
                session.count_documents(DATABASE, COLLECTION, max_result_bytes=1)
            with self.assertRaises(briskdb.InvalidArgumentError):
                session.find(DATABASE, COLLECTION, limit=0)
            first = session.find(DATABASE, COLLECTION, batch_size=0)
            self.assertEqual(first["documents"], [])
            self.assertTrue(session.kill_cursor(DATABASE, COLLECTION, first["cursor_id"])["killed"])
            with self.assertRaises(briskdb.InvalidArgumentError):
                session.list_collections(DATABASE, timeout_ms=0)
            with self.assertRaises(briskdb.InvalidArgumentError):
                session.list_collections(DATABASE, request_id=uuid.UUID(int=0))

            self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 2)
            session.close()
            with self.assertRaises(briskdb.FailedPreconditionError):
                session.list_collections(DATABASE)
            database.close()

            reopened = briskdb.open(root, documents=True)
            reopened_session = reopened.session()
            self.assertEqual(
                reopened_session.count_documents(DATABASE, COLLECTION)["count"], 2
            )
            reopened_session.close()
            reopened.close()

        with tempfile.TemporaryDirectory() as parent:
            invalid_root = "{}/invalid".format(parent)
            with self.assertRaises(briskdb.InvalidArgumentError):
                briskdb.open(
                    invalid_root,
                    shards=2,
                    documents=True,
                    uuid_representation="not-a-representation",
                )
            with self.assertRaisesRegex(
                briskdb.InvalidArgumentError, "either direct open options or config"
            ):
                briskdb.open(
                    invalid_root,
                    config=briskdb.Config(shards=2, documents=True),
                    uuid_representation="standard",
                )

    def test_sql_only_use_does_not_import_or_require_bson(self) -> None:
        script = r"""
import importlib.abc
import pathlib
import sys
import tempfile

attempts = []

class BlockBson(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "bson" or fullname.startswith("bson.") or fullname == "pymongo" or fullname.startswith("pymongo."):
            attempts.append(fullname)
            raise ModuleNotFoundError("optional BSON dependency blocked for test")
        return None

blocker = BlockBson()
sys.meta_path.insert(0, blocker)
import briskdb

assert attempts == [], attempts
with tempfile.TemporaryDirectory() as parent:
    sql_root = pathlib.Path(parent) / "sql"
    with briskdb.open(sql_root, shards=2) as database:
        with database.session(routing_key="sql-only") as session:
            assert session.query("SELECT 1")["rows"] == [(1,)]
            try:
                session.create_collection("app", "disabled")
            except briskdb.FailedPreconditionError as error:
                assert error.code == "failed_precondition"
            else:
                raise AssertionError("disabled document support unexpectedly accepted work")
    assert attempts == [], attempts

    document_root = pathlib.Path(parent) / "documents"
    with briskdb.open(document_root, shards=2, documents=True) as database:
        with database.session(routing_key="sql-after-document-error") as session:
            try:
                session.create_collection("app", "must_not_commit")
            except briskdb.UnsupportedError as error:
                assert error.code == "unsupported"
                assert error.retryable is False
                assert str(error) == "Python document operations require the optional pymongo package"
            else:
                raise AssertionError("document command unexpectedly succeeded without bson")

            assert session.query("SELECT 2")["rows"] == [(2,)]

            sys.meta_path.remove(blocker)
            import bson
            assert session.list_collections("app")["collections"] == []
assert attempts and attempts[0] == "bson", attempts
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


class AsyncPythonDocumentApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_aggregate_transform_paging_and_missing_values(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=3, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(3):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(index), "items": [{"v": index}, None, {}]})
                    pipeline = [
                        {"$addFields": {"old": "$_id", "items.label": {"$ifNull": ["$missing", "ok"]}}},
                        {"$project": {"_id": 0, "copied": "$old", "values": "$items.v", "n": {"$size": "$items"}, "drop": {"$literal": 1}}},
                        {"$unset": "drop"},
                    ]
                    page = await session.aggregate(DATABASE, COLLECTION, pipeline, batch_size=1)
                    rows = page["documents"]
                    while page["cursor_id"] is not None:
                        page = await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                        rows.extend(page["documents"])
                    self.assertEqual(rows, [{"copied": Int64(index), "values": [index], "n": 3} for index in range(3)])
                    with self.assertRaises(briskdb.InvalidQueryError):
                        await session.aggregate(DATABASE, COLLECTION, [{"$set": {"bad": {"$size": "$_id"}}}])

    async def test_async_aggregate_paging_count_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=3, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(8):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(index)})
                    identifier = uuid.uuid4()
                    page = await session.aggregate(DATABASE, COLLECTION, [{"$sort": {"_id": -1}}], batch_size=2, request_id=identifier)
                    self.assertEqual(page["request_id"], identifier)
                    rows = page["documents"]
                    while page["cursor_id"] is not None:
                        page = await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=2)
                        rows.extend(page["documents"])
                    self.assertEqual(rows, [{"_id": Int64(index)} for index in reversed(range(8))])
                    count = await session.aggregate(DATABASE, COLLECTION, [{"$count": "n"}], max_result_rows=1, max_result_bytes=256)
                    self.assertEqual(count["documents"], [{"n": 8}])
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.aggregate(DATABASE, COLLECTION, [], batch_size=2, max_result_rows=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.aggregate(DATABASE, COLLECTION, [], cancellation=token)

    async def test_async_distinct_preserves_values_and_forwards_limits(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1, "v": [Int64(2), 2.0, None]})
                    identifier = uuid.uuid4()
                    result = await session.distinct(DATABASE, COLLECTION, "v", request_id=identifier)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["values"], [Int64(2), None])
                    self.assertIsInstance(result["values"][0], Int64)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.distinct(DATABASE, COLLECTION, "v", max_result_rows=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.distinct(DATABASE, COLLECTION, "v", cancellation=token)

    async def test_async_collection_existence_forwards_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    self.assertFalse((await session.collection_exists(DATABASE, COLLECTION))["exists"])
                    await session.create_collection(DATABASE, COLLECTION)
                    identifier = uuid.uuid4()
                    result = await session.collection_exists(DATABASE, COLLECTION, request_id=identifier,
                                                             max_result_rows=1, max_result_bytes=34)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertTrue(result["exists"])
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.collection_exists(DATABASE, COLLECTION, cancellation=token)

    async def test_async_sorting_precedes_projection_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(9):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": index, "rank": index})
                    batch = await session.find(DATABASE, COLLECTION, sort={"rank": -1}, projection={"_id": 1}, batch_size=2)
                    rows = batch["documents"]
                    while not batch["exhausted"]:
                        batch = await session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=3)
                        rows.extend(batch["documents"])
                    self.assertEqual(rows, [{"_id": index} for index in reversed(range(9))])

    async def test_async_projection_is_retained_across_pages(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(8):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": index, "value": Int64(index), "hidden": True})
                    batch = await session.find(DATABASE, COLLECTION, {"hidden": True}, projection={"value": 1, "_id": 0}, batch_size=0)
                    found = []
                    while not batch["exhausted"]:
                        batch = await session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=2)
                        found.extend(batch["documents"])
                    self.assertEqual(found, [{"value": Int64(index)} for index in range(8)])

    async def test_async_retained_cursors_forward_batches_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(12):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": index})
                    batch = await session.find(DATABASE, COLLECTION, batch_size=0)
                    found = []
                    while not batch["exhausted"]:
                        batch = await session.get_more(DATABASE, COLLECTION, batch["cursor_id"], batch_size=3)
                        found.extend(batch["documents"])
                    self.assertEqual([doc["_id"] for doc in found], list(range(12)))
                    batch = await session.find(DATABASE, COLLECTION, batch_size=1)
                    self.assertTrue((await session.kill_cursor(DATABASE, COLLECTION, batch["cursor_id"]))["killed"])

    async def test_async_document_methods_forward_values_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(
                root,
                shards=4,
                documents=True,
                uuid_representation="java_legacy",
            ) as database:
                async with await database.session() as session:
                    created = await session.create_collection(DATABASE, COLLECTION)
                    self.assertEqual(created["kind"], "collection")
                    inserted = await session.insert_one(
                        DATABASE,
                        COLLECTION,
                        {"_id": UUID_VALUE, "body": Decimal128("1.25")},
                    )
                    self.assertEqual(inserted["inserted_ids"], [UUID_VALUE])
                    found = await session.find(
                        DATABASE, COLLECTION, {"_id": UUID_VALUE}
                    )
                    self.assertEqual(
                        found["documents"][0]["body"].bid, Decimal128("1.25").bid
                    )
                    self.assertEqual(
                        (await session.count_documents(DATABASE, COLLECTION))["count"],
                        1,
                    )

                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.list_indexes(
                            DATABASE, COLLECTION, cancellation=token
                        )

                    deleted = await session.delete_one(
                        DATABASE, COLLECTION, {"_id": UUID_VALUE}
                    )
                    self.assertEqual(deleted["deleted_count"], 1)
                    self.assertEqual(
                        (await session.count_documents(DATABASE, COLLECTION))["count"],
                        0,
                    )


if __name__ == "__main__":
    unittest.main()
