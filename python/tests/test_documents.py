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
    def test_opt_in_execution_stats_measure_reads_and_index_work(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for identity in range(12):
                        session.insert_one(DATABASE, COLLECTION, {"_id": identity, "v": identity % 3})
                    self.assertNotIn("read_stats", session.find(DATABASE, COLLECTION))
                    result = session.find(DATABASE, COLLECTION, {"v": 1}, execution_stats=True)
                    self.assertEqual(result["read_stats"], {"storage_reads": 16, "documents_examined": 12, "matcher_evaluations": 12, "shards_read": [0, 1, 2, 3]})
                    self.assertNotIn("read_access", result["plan"])
                    session.create_built_index(DATABASE, COLLECTION, {"v": 1})
                    indexed = session.find(DATABASE, COLLECTION, {"v": 1}, execution_stats=True, plan_diagnostics=True)
                    self.assertEqual(indexed["read_stats"]["documents_examined"], 4)
                    self.assertEqual(indexed["documents"], result["documents"])
                    point = session.find(DATABASE, COLLECTION, {"_id": 1}, execution_stats=True)
                    self.assertEqual(point["read_stats"]["storage_reads"], 1)
                    self.assertEqual(point["read_stats"]["matcher_evaluations"], 0)
                    self.assertEqual(point["read_stats"]["shards_read"], point["plan"]["shards"])
                    first = session.find(DATABASE, COLLECTION, batch_size=0, execution_stats=True)
                    self.assertEqual(first["read_stats"]["storage_reads"], 0)
                    next_page = session.get_more(DATABASE, COLLECTION, first["cursor_id"], batch_size=1, execution_stats=True)
                    self.assertGreater(next_page["read_stats"]["storage_reads"], 0)
                    last = session.get_more(DATABASE, COLLECTION, next_page["cursor_id"])
                    self.assertNotIn("read_stats", last)
                    distinct = session.distinct(DATABASE, COLLECTION, "v", execution_stats=True)
                    self.assertGreaterEqual(distinct["read_stats"]["documents_examined"], 12)
                    aggregate = session.aggregate(DATABASE, COLLECTION, [{"$count": "n"}], execution_stats=True)
                    self.assertEqual(aggregate["documents"], [{"n": 12}])
                    self.assertGreaterEqual(aggregate["read_stats"]["documents_examined"], 12)
                    self.assertEqual(aggregate["read_stats"]["matcher_evaluations"], 0)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find(DATABASE, COLLECTION, execution_stats=True, max_result_bytes=1)

    def test_opt_in_plan_diagnostics_and_cursor_index_churn(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for identity in range(6):
                        session.insert_one(DATABASE, COLLECTION, {"_id": identity, "v": 1})
                    query = {"v": 1}
                    ordinary = session.find(DATABASE, COLLECTION, query)
                    self.assertNotIn("read_access", ordinary["plan"])
                    result = session.find(DATABASE, COLLECTION, query, plan_diagnostics=True)
                    self.assertEqual(result["documents"], ordinary["documents"])
                    self.assertEqual(result["plan"]["read_access"], {"kind": "scan", "reason": "no_ready_index"})
                    session.create_built_index(DATABASE, COLLECTION, {"v": 1})
                    first = session.find(DATABASE, COLLECTION, query, batch_size=1, plan_diagnostics=True)
                    original_access = first["plan"]["read_access"]
                    self.assertEqual(set(original_access), {"kind", "candidate_kind", "key_count", "index_id"})
                    self.assertEqual((original_access["kind"], original_access["candidate_kind"], original_access["key_count"]), ("index_candidates", "equality", 1))
                    session.drop_index(DATABASE, COLLECTION, "v_1")
                    second = session.get_more(DATABASE, COLLECTION, first["cursor_id"], batch_size=1, plan_diagnostics=True)
                    self.assertEqual(second["plan"]["read_access"], {"kind": "scan", "reason": "no_ready_index"})
                    session.create_built_index(DATABASE, COLLECTION, {"v": 1})
                    third = session.get_more(DATABASE, COLLECTION, second["cursor_id"], batch_size=1, plan_diagnostics=True)
                    self.assertNotEqual(third["plan"]["read_access"]["index_id"], original_access["index_id"])
                    last = session.get_more(DATABASE, COLLECTION, third["cursor_id"])
                    self.assertNotIn("read_access", last["plan"])
                    self.assertEqual(first["documents"] + second["documents"] + third["documents"] + last["documents"], ordinary["documents"])
                    for query, kind, count in [
                        ({"v": {"$in": [1, 2, 1.0]}}, "necessary_finite", 2),
                        ({"$or": [{"v": 1}, {"v": 2}]}, "logical_finite", 2),
                    ]:
                        access = session.find(DATABASE, COLLECTION, query, plan_diagnostics=True)["plan"]["read_access"]
                        self.assertEqual((access["candidate_kind"], access["key_count"]), (kind, count))
                    private = "never-return-query-values-in-plans"
                    result = session.find(DATABASE, COLLECTION, {"v": private}, plan_diagnostics=True)
                    self.assertNotIn(private, repr(result["plan"]))
                    distinct = session.distinct(DATABASE, COLLECTION, "v", {"v": 1}, plan_diagnostics=True)
                    self.assertEqual(distinct["values"], [1])
                    self.assertEqual(distinct["plan"]["read_access"]["candidate_kind"], "equality")
                    aggregate = session.aggregate(DATABASE, COLLECTION, [{"$match": {"v": 1}}], batch_size=1, plan_diagnostics=True)
                    self.assertEqual(aggregate["plan"]["read_access"], {"kind": "scan", "reason": "aggregation_input"})
                    continued = session.get_more(DATABASE, COLLECTION, aggregate["cursor_id"], plan_diagnostics=True)
                    self.assertEqual(continued["plan"]["read_access"], aggregate["plan"]["read_access"])
                    point = session.find(DATABASE, COLLECTION, {"_id": 1}, plan_diagnostics=True)
                    self.assertEqual(point["plan"]["kind"], "point")
                    self.assertNotIn("read_access", point["plan"])
                    token = briskdb.CancellationToken()
                    token.cancel()
                    for kwargs, error in [({"max_result_bytes": 1}, briskdb.LimitExceededError), ({"cancellation": token}, briskdb.CancelledError)]:
                        with self.assertRaises(error):
                            session.find(DATABASE, COLLECTION, {"v": 1}, plan_diagnostics=True, **kwargs)
                    self.assertEqual(len(session.find(DATABASE, COLLECTION, plan_diagnostics=True)["documents"]), 6)

    def test_nonunique_nested_candidates_follow_mutations_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    documents = [
                        {"_id": 1, "v": {"score": 2}},
                        {"_id": 2, "v": [{"score": 3}]},
                        {"_id": 3, "v": 1},
                        {"_id": 4, "v": [1, 2]},
                        {"_id": 5, "v": [[1, 2]]},
                        {"_id": 6, "v": ObjectId("64b000000000000000000006")},
                        {"_id": 7, "v": datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)},
                    ]
                    for collection in ("scan", "indexed"):
                        session.create_collection(DATABASE, collection)
                        for document in documents:
                            session.insert_one(DATABASE, collection, document)
                    session.create_built_index(DATABASE, "indexed", {"v": 1})
                    session.create_built_index(DATABASE, "indexed", {"v.score": 1})
                    session.create_built_index(DATABASE, "indexed", {"v": 1}, name="sparse", sparse=True)
                    session.create_built_index(DATABASE, "indexed", {"v": 1}, name="partial", partial_filter={"active": True})
                    operations = [
                        ("update_many", ({"v.score": 2}, {"$set": {"v": 1}})),
                        ("update_one", ({"v": 1}, {"$set": {"v": {"score": 2}, "active": True}})),
                        ("replace_one", ({"v.score": 3}, {"v": [1, {"score": 2}]})),
                        ("find_one_and_update", ({"v.score": 2}, {"$set": {"v": 2}})),
                        ("find_one_and_replace", ({"v": 2}, {"v": {"score": 4}})),
                        ("delete_many", ({"v.score": {"$exists": True}},)),
                    ]
                    for operation, args in operations:
                        method = getattr(session, operation)
                        expected = method(DATABASE, "scan", *args)
                        actual = method(DATABASE, "indexed", *args)
                        for key in ("matched_count", "modified_count", "deleted_count", "document"):
                            if key in expected:
                                self.assertEqual(actual[key], expected[key], (operation, key))
                        self.assertEqual(
                            bson_bytes({"rows": session.find(DATABASE, "indexed")["documents"]}),
                            bson_bytes({"rows": session.find(DATABASE, "scan")["documents"]}),
                            operation,
                        )
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(
                        bson_bytes({"rows": session.find(DATABASE, "indexed")["documents"]}),
                        bson_bytes({"rows": session.find(DATABASE, "scan")["documents"]}),
                    )

    def test_combined_index_creation_build_counts_options_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    session.insert_one(DATABASE, COLLECTION, {"_id": 1, "value": [1, 2], "active": True})
                    token = briskdb.CancellationToken()
                    token.cancel()
                    for control, error in [({"max_result_bytes": 1}, briskdb.LimitExceededError), ({"cancellation": token}, briskdb.CancelledError)]:
                        with self.assertRaises(error):
                            session.create_built_index(DATABASE, COLLECTION, {"value": 1}, **control)
                        self.assertEqual(len(session.list_indexes(DATABASE, COLLECTION)["indexes"]), 1)
                    identity = uuid.uuid4()
                    result = session.create_built_index(DATABASE, COLLECTION, {"value": 1}, sparse=True, request_id=identity, timeout_ms=5000)
                    self.assertEqual((result["request_id"], result["kind"], result["index_name"], result["lifecycle"], result["num_indexes_before"], result["num_indexes_after"]), (identity, "index_built", "value_1", "ready", 1, 2))
                    retry = session.create_built_index(DATABASE, COLLECTION, {"value": Int64(1)}, sparse=True)
                    self.assertEqual((retry["num_indexes_before"], retry["num_indexes_after"]), (2, 2))
                    session.create_built_index(DATABASE, COLLECTION, {"value": 1, "tail": -1}, name="partial", partial_filter={"active": True})
                    session.create_index(DATABASE, COLLECTION, {"other": 1})
                    result = session.create_built_index(DATABASE, COLLECTION, {"other": 1})
                    self.assertEqual((result["num_indexes_before"], result["num_indexes_after"]), (3, 4))
                    session.update_one(DATABASE, COLLECTION, {"_id": 1}, {"$set": {"value": [3, 4], "active": False}})
                    metadata = session.list_index_metadata(DATABASE, COLLECTION)["documents"]
                    self.assertEqual(metadata[-1], {"name": "value_1", "key": {"value": 1}, "sparse": True})
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_index_metadata(DATABASE, COLLECTION)["documents"], metadata)
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION, {"value": 4})["count"], 1)

    def test_paged_built_index_metadata_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_index_metadata(DATABASE, COLLECTION)["documents"], [])
                    session.create_collection(DATABASE, COLLECTION)
                    session.create_index(DATABASE, COLLECTION, {"value": 1}, name="pending", unique=True)
                    session.create_index(DATABASE, COLLECTION, {"value": 1, "tail": -1}, name="!ready", partial_filter={"active": True})
                    session.build_index(DATABASE, COLLECTION, "!ready")
                    expected = [{"name": "_id_", "key": {"_id": 1}}, {"name": "!ready", "key": {"value": 1, "tail": -1}, "partialFilterExpression": {"active": True}}]
                    identity = uuid.uuid4()
                    page = session.list_index_metadata(DATABASE, COLLECTION, batch_size=0, batch_byte_limit=1024, request_id=identity)
                    self.assertEqual(page["request_id"], identity)
                    self.assertEqual(page["documents"], [])
                    rows = []
                    while page["cursor_id"] is not None:
                        page = session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                        rows.extend(page["documents"])
                    self.assertEqual(rows, expected)
                    for control in [{"max_result_rows": 1}, {"max_result_bytes": 1}, {"batch_byte_limit": 1}]:
                        with self.assertRaises(briskdb.LimitExceededError):
                            session.list_index_metadata(DATABASE, COLLECTION, **control)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.list_index_metadata(DATABASE, COLLECTION, cancellation=token)
                    page = session.list_index_metadata(DATABASE, COLLECTION, batch_size=0)
                    self.assertTrue(session.kill_cursor(DATABASE, COLLECTION, page["cursor_id"])["killed"])
                    self.assertEqual(len(session.list_indexes(DATABASE, COLLECTION)["indexes"]), 3)
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_index_metadata(DATABASE, COLLECTION)["documents"], expected)

    def test_nonunique_index_build_maintenance_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    session.insert_one(DATABASE, COLLECTION, {"_id": 1, "value": [1, 2, 2]})
                    session.create_index(DATABASE, COLLECTION, {"value": 1})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.build_index(DATABASE, COLLECTION, "value_1", max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.build_index(DATABASE, COLLECTION, "value_1", cancellation=token)
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"][1]["lifecycle"], "pending_build")
                    identity = uuid.uuid4()
                    result = session.build_index(DATABASE, COLLECTION, "value_1", request_id=identity)
                    self.assertEqual((result["request_id"], result["index_name"], result["lifecycle"]), (identity, "value_1", "ready"))
                    self.assertEqual(session.build_index(DATABASE, COLLECTION, "value_1")["lifecycle"], "ready")
                    self.assertEqual(session.create_index(DATABASE, COLLECTION, {"value": 1})["lifecycle"], "ready")
                    session.insert_one(DATABASE, COLLECTION, {"_id": 2, "value": [1, 2]})
                    session.update_one(DATABASE, COLLECTION, {"_id": 1}, {"$set": {"value": [3, 4], "extra": True}})
                    session.delete_one(DATABASE, COLLECTION, {"_id": 2})
                    session.create_index(DATABASE, COLLECTION, {"value": 1}, name="unique", unique=True)
                    session.build_index(DATABASE, COLLECTION, "unique")
                    with self.assertRaises(briskdb.UniqueViolationError):
                        session.insert_one(DATABASE, COLLECTION, {"_id": 2, "value": 3.0})
                    session.create_index(DATABASE, COLLECTION, {"value": 1}, name="to_drop")
                    session.build_index(DATABASE, COLLECTION, "to_drop")
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.drop_index(DATABASE, COLLECTION, "to_drop", max_result_bytes=33)
                    self.assertTrue(session.drop_index(DATABASE, COLLECTION, "to_drop")["acknowledged"])
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    indexes = {item["name"]: item for item in session.list_indexes(DATABASE, COLLECTION)["indexes"]}
                    self.assertEqual(indexes["value_1"]["lifecycle"], "ready")
                    self.assertEqual(indexes["unique"]["lifecycle"], "ready")
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION, {"value": 4})["count"], 1)

    def test_sparse_partial_declarations_validate_preserve_options_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    keys = OrderedDict([("label", Int64(1)), ("rank", -1.0)])
                    partial = SON([("rank", {"$gte": Int64(2)}), ("active", True)])
                    before = bson_bytes(partial)
                    request_id = uuid.uuid4()
                    sparse_result = session.create_index(DATABASE, COLLECTION, keys, sparse=True, unique=True, request_id=request_id)
                    self.assertEqual(sparse_result["request_id"], request_id)
                    self.assertEqual((sparse_result["index_name"], sparse_result["lifecycle"]), ("label_1_rank_-1", "pending_build"))
                    session.create_index(DATABASE, COLLECTION, keys, name="partial", partial_filter=partial, unique=True)
                    for _ in range(2):
                        session.create_index(DATABASE, COLLECTION, keys, sparse=True, unique=True)
                        session.create_index(DATABASE, COLLECTION, keys, name="partial", partial_filter=partial, unique=True)
                    indexes = session.list_indexes(DATABASE, COLLECTION)["indexes"]
                    self.assertEqual([item["name"] for item in indexes], ["_id_", "label_1_rank_-1", "partial"])
                    self.assertEqual(bson_bytes(indexes[1]["keys"]), bson_bytes({"label": 1, "rank": -1}))
                    self.assertTrue(indexes[1]["sparse"])
                    self.assertNotIn("partial_filter", indexes[1])
                    self.assertNotIn("sparse", indexes[2])
                    self.assertEqual(bson_bytes(indexes[2]["partial_filter"]), before)
                    self.assertEqual(bson_bytes(partial), before)
                    self.assertEqual([item["lifecycle"] for item in indexes[1:]], ["pending_build", "pending_build"])
                    for options in [{"sparse": True, "partial_filter": {"active": True}}, {"partial_filter": {}}, {"partial_filter": {"rank": {"$ne": 1}}}, {"partial_filter": {"$or": [{"active": True}, {"rank": {"$ne": 1}}]}}]:
                        with self.assertRaises(briskdb.UnsupportedError):
                            session.create_index(DATABASE, COLLECTION, keys, name="invalid", **options)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        session.create_index(DATABASE, COLLECTION, keys, unique=True)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        session.create_index(DATABASE, COLLECTION, keys, name="partial", partial_filter={"active": False}, unique=True)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.create_index(DATABASE, COLLECTION, keys, name="bounded", sparse=True, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.create_index(DATABASE, COLLECTION, keys, name="cancelled", partial_filter=partial, cancellation=token)
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], indexes)
                    # Pending declarations do not enforce uniqueness or affect reads.
                    for identifier in (1, 2):
                        session.insert_one(DATABASE, COLLECTION, {"_id": identifier, "label": "same", "rank": 3, "active": True})
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 2)
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], indexes)
                    session.drop_index(DATABASE, COLLECTION, "partial")
                    self.assertEqual(len(session.list_indexes(DATABASE, COLLECTION)["indexes"]), 2)

    def test_pending_index_drop_protection_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    session.insert_one(DATABASE, COLLECTION, {"_id": 1, "label": "kept"})
                    for name in ["label_1", "keep", "*"]:
                        session.create_index(DATABASE, COLLECTION, {"label": 1}, name=name)
                    before = session.list_indexes(DATABASE, COLLECTION)["indexes"]
                    for name in ["_id", "_id_"]:
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.drop_index(DATABASE, COLLECTION, name)
                    for name in ["absent", "label", "LABEL_1"]:
                        with self.assertRaises(briskdb.FailedPreconditionError):
                            session.drop_index(DATABASE, COLLECTION, name)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.drop_index(DATABASE, COLLECTION, "label_1", max_result_bytes=33)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.drop_index(DATABASE, COLLECTION, "label_1", cancellation=token)
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], before)
                    request_id = uuid.uuid4()
                    result = session.drop_index(DATABASE, COLLECTION, "*", request_id=request_id, max_result_rows=1, max_result_bytes=34)
                    self.assertEqual(result, {"request_id": request_id, "plan": None, "kind": "acknowledged", "acknowledged": True})
                    remaining = session.list_indexes(DATABASE, COLLECTION)["indexes"]
                    self.assertEqual([item["name"] for item in remaining], ["_id_", "keep", "label_1"])
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 1)
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], remaining)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        session.drop_index(DATABASE, COLLECTION, "*")
                    session.create_index(DATABASE, COLLECTION, {"label": 1}, name="*")
                    session.drop_index(DATABASE, COLLECTION, "*")
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], remaining)

    def test_index_definitions_default_names_validation_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    keys = OrderedDict([("profile.name", Int64(1)), ("rank", -1.0)])
                    result = session.create_index(DATABASE, COLLECTION, keys, unique=True)
                    self.assertEqual((result["index_name"], result["lifecycle"]), ("profile.name_1_rank_-1", "pending_build"))
                    result = session.create_index(DATABASE, COLLECTION, {"profile.name": Decimal128("1.00"), "rank": -1}, name="profile.name_1_rank_-1", unique=True)
                    self.assertEqual(result["index_name"], "profile.name_1_rank_-1")
                    for invalid in [{}, {"bad..path": 1}, {"$private": 1}, {"valid": True}, {"valid": "hashed"}, {"valid": 0}, {"valid": float("nan")}]:
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.create_index(DATABASE, COLLECTION, invalid, name="must_not_exist")
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.create_index(DATABASE, COLLECTION, {"x" * 254: 1})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.create_index(DATABASE, COLLECTION, {"bounded": 1}, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.create_index(DATABASE, COLLECTION, {"cancelled": 1}, cancellation=token)
                    indexes = session.list_indexes(DATABASE, COLLECTION)["indexes"]
                    self.assertEqual(len(indexes), 2)
                    self.assertEqual(bson_bytes(indexes[1]["keys"]), bson_bytes({"profile.name": 1, "rank": -1}))
                    self.assertEqual(indexes[1]["lifecycle"], "pending_build")
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_indexes(DATABASE, COLLECTION)["indexes"], indexes)

    def test_find_upserts_images_metadata_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for replacement in (False, True):
                        method = session.find_one_and_replace if replacement else session.find_one_and_update
                        body = {"counter": 5, "stamp": Timestamp(0, 0), "hidden": True} if replacement else {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0), "hidden": True}}
                        for after in (False, True):
                            identifier = None if not replacement and not after else Int64(2 * replacement + after)
                            request_id = uuid.uuid4()
                            result = method(DATABASE, COLLECTION, {"_id": identifier, "counter": 3}, body, upsert=True, return_document=after, projection={"counter": 1, "_id": 0}, sort={"counter": -1}, request_id=request_id)
                            self.assertEqual((result["kind"], result["document"], result["did_upsert"], result["request_id"]), ("document", {"counter": 5} if after else None, True, request_id))
                            self.assertEqual(bson_bytes({"v": result["upserted_id"]}), bson_bytes({"v": identifier}))
                            stored = session.find(DATABASE, COLLECTION, {"_id": identifier})["documents"][0]
                            self.assertEqual(list(stored)[0], "_id")
                            self.assertTrue(stored["hidden"])
                            self.assertEqual(stored["stamp"] == Timestamp(0, 0), not replacement)
                            matched = method(DATABASE, COLLECTION, {"_id": identifier}, body, upsert=True, return_document=after, projection={"absent": 1, "_id": 0})
                            self.assertEqual((matched["document"], matched["did_upsert"], matched["upserted_id"]), ({}, False, None))
                        generated = method(DATABASE, COLLECTION, {"generated": replacement}, body, upsert=True)
                        self.assertIsInstance(generated["upserted_id"], ObjectId)
                        self.assertIsNone(generated["document"])
                    large_id = "x" * 1_100_000
                    result = session.find_one_and_update(DATABASE, COLLECTION, {"_id": large_id}, {"$set": {}}, upsert=True, max_result_bytes=16 * 1024 * 1024)
                    self.assertEqual(result["upserted_id"], large_id)
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]]
                    for replacement in (False, True):
                        method = session.find_one_and_replace if replacement else session.find_one_and_update
                        for after in (False, True):
                            body = {"_id": "z" * 4000} if replacement else {"$set": {"_id": "z" * 4000}}
                            with self.assertRaises(briskdb.LimitExceededError):
                                method(DATABASE, COLLECTION, {"absent": True}, body, upsert=True, return_document=after, projection={"absent": 1, "_id": 0}, max_result_bytes=1000)
                            body = {"value": 1} if replacement else {"$set": {"value": 1}}
                            token = briskdb.CancellationToken()
                            token.cancel()
                            with self.assertRaises(briskdb.CancelledError):
                                method(DATABASE, COLLECTION, {"_id": 999}, body, upsert=True, return_document=after, cancellation=token)
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {"_id": 999}, {"_id": 998} if replacement else {"$set": {"_id": 998}}, upsert=True)
                        with self.assertRaises(briskdb.UniqueViolationError):
                            method(DATABASE, COLLECTION, {"absent": True}, {"_id": None} if replacement else {"$set": {"_id": None}}, upsert=True)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]], before)

    def test_operator_upserts_seed_results_identity_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for method, identifier in [(session.update_one, None), (session.update_many, Int64(2))]:
                        request_id = uuid.uuid4()
                        result = method(DATABASE, COLLECTION, {"_id": identifier, "counter": 3, "seed_stamp": Timestamp(0, 0)}, {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0)}}, upsert=True, request_id=request_id)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"], result["request_id"]), (0, 0, True, request_id))
                        self.assertEqual(bson_bytes({"v": result["upserted_id"]}), bson_bytes({"v": identifier}))
                        self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION, {"_id": identifier})["documents"][0]), bson_bytes({"_id": identifier, "counter": 5, "seed_stamp": Timestamp(0, 0), "stamp": Timestamp(0, 0)}))
                        result = method(DATABASE, COLLECTION, {"_id": identifier}, {"$inc": {"counter": 2}}, upsert=True)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (1, 1, False))
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"tag": "chosen"}, {"$set": {"_id": "chosen"}}, upsert=True)["upserted_id"], "chosen")
                    generated = session.update_many(DATABASE, COLLECTION, {"tag": "generated"}, {"$inc": {"counter": 1}}, upsert=True)
                    self.assertIsInstance(generated["upserted_id"], ObjectId)
                    for method, identifier in [(session.update_one, Int64(1)), (session.update_many, Int64(2))]:
                        result = method(DATABASE, COLLECTION, {"_id.a": identifier, "_id.b": {"$eq": None}}, {"$inc": {"counter": 1}}, upsert=True)
                        self.assertEqual(bson_bytes({"v": result["upserted_id"]}), bson_bytes({"v": {"a": identifier, "b": None}}))
                        result = method(DATABASE, COLLECTION, {"_id.ignored": {"$gt": 1}, "tag": identifier}, {"$set": {}}, upsert=True)
                        self.assertIsInstance(result["upserted_id"], ObjectId)
                    large_id = "x" * 1_100_000
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"_id": large_id}, {"$inc": {"counter": 1}}, upsert=True, max_result_bytes=16 * 1024 * 1024)["upserted_id"], large_id)
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]]
                    for method in (session.update_one, session.update_many):
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {"_id": 99}, {"$set": {"_id": 98}}, upsert=True)
                        with self.assertRaises(briskdb.InvalidQueryError):
                            method(DATABASE, COLLECTION, {"a": 1, "a.b": 2}, {"$set": {}}, upsert=True)
                        with self.assertRaises(briskdb.UniqueViolationError):
                            method(DATABASE, COLLECTION, {"missing": True}, {"$set": {"_id": None}}, upsert=True)
                        with self.assertRaises(briskdb.LimitExceededError):
                            method(DATABASE, COLLECTION, {"_id": 99}, {"$set": {}}, upsert=True, max_result_bytes=1)
                        token = briskdb.CancellationToken(); token.cancel()
                        with self.assertRaises(briskdb.CancelledError):
                            method(DATABASE, COLLECTION, {"_id": 99}, {"$set": {}}, upsert=True, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION, max_result_bytes=16 * 1024 * 1024)["documents"]], before)

    def test_replacement_upsert_preserves_large_native_point_ids(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    identifier = "x" * 1_100_000
                    result = session.replace_one(DATABASE, COLLECTION, {"_id": identifier}, {"value": 7}, upsert=True, max_result_bytes=16 * 1024 * 1024)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (0, 0, True))
                    self.assertEqual(result["upserted_id"], identifier)
                    result = session.replace_one(DATABASE, COLLECTION, {"_id": {"$eq": identifier}}, {"value": 7}, upsert=True, max_result_bytes=16 * 1024 * 1024)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (1, 0, False))

    def test_replacement_upsert_ids_results_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for query, replacement, identifier in [
                        ({"_id": Int64(1)}, {"value": 7}, Int64(1)),
                        ({"_id": {"$eq": None}}, {"value": 7}, None),
                        ({"_id": 2, "missing": True}, {"value": 7, "_id": 2.0}, 2.0),
                    ]:
                        request_id = uuid.uuid4()
                        result = session.replace_one(DATABASE, COLLECTION, query, replacement, upsert=True, request_id=request_id)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"], result["request_id"]), (0, 0, True, request_id))
                        self.assertEqual(bson_bytes({"v": result["upserted_id"]}), bson_bytes({"v": identifier}))
                        row = session.find(DATABASE, COLLECTION, {"_id": identifier})["documents"][0]
                        self.assertEqual(bson_bytes(row), bson_bytes({"_id": identifier, "value": 7}))
                        result = session.replace_one(DATABASE, COLLECTION, {"_id": identifier}, {"value": 7}, upsert=True)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (1, 0, False))
                    result = session.replace_one(DATABASE, COLLECTION, {"missing": True}, {"generated": True}, upsert=True)
                    self.assertIsInstance(result["upserted_id"], ObjectId)
                    self.assertTrue(result["did_upsert"])
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]]
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        session.replace_one(DATABASE, COLLECTION, {"_id": 99}, {"_id": 98}, upsert=True)
                    with self.assertRaises(briskdb.UniqueViolationError):
                        session.replace_one(DATABASE, COLLECTION, {"missing": True}, {"_id": 1}, upsert=True)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.replace_one(DATABASE, COLLECTION, {"_id": 99}, {}, upsert=True, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.replace_one(DATABASE, COLLECTION, {"_id": 99}, {}, upsert=True, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)

    def test_increment_numeric_fidelity_counts_images_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "amount": Decimal128("2"), "overflow": Int64(2**63 - 1), "invalid": True})
                    identifier = uuid.uuid4()
                    result = session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": Int64(1), "amount": 0.1}}, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (4, 4, identifier))
                    self.assertEqual(session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": 0, "amount": Decimal128("0E-100")}})["modified_count"], 0)
                    image = session.find_one_and_update(DATABASE, COLLECTION, {}, {"$inc": {"counter": 1}}, sort={"_id": -1}, projection={"counter": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"counter": Int64(1)}))
                    image = session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$inc": {"counter": 1}}, return_document=True, projection={"counter": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"counter": Int64(3)}))
                    for row in session.find(DATABASE, COLLECTION)["documents"]:
                        self.assertEqual(row["amount"].bid, Decimal128("2.100000000000000").bid)
                    session.update_many(DATABASE, COLLECTION, {}, {"$set": {"nan": Decimal128("sNaN")}})
                    for _ in range(2):
                        self.assertEqual(session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"nan": 0}})["modified_count"], 4)
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]]
                    for method in (session.update_one, session.update_many):
                        for field in ("invalid", "overflow", "_id"):
                            with self.assertRaises(briskdb.InvalidArgumentError):
                                method(DATABASE, COLLECTION, {}, {"$set": {"marker": True}, "$inc": {field: 1}})
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {"_id": 99}, {"$inc": {"counter": True}})
                    for after in (False, True):
                        with self.assertRaises(briskdb.LimitExceededError):
                            session.find_one_and_update(DATABASE, COLLECTION, {}, {"$inc": {"counter": 1}}, return_document=after, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": 1}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)

    def test_pull_predicates_counts_images_errors_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    values = [Int64(1), 1.0, True, [1, 2], {"_id": [1, 2], "x": 1}, {"_id": 3, "x": 3}]
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "values": values, "keep": True})
                    identifier = uuid.uuid4()
                    result = session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"values": 1}}, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (4, 4, identifier))
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {}, {"$pull": {"absent": 1}})["modified_count"], 0)
                    image = session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pull": {"values": {"$eq": 1}}}, sort={"_id": -1}, projection={"values": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": values[2:]}))
                    image = session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$pull": {"values": {"_id": 2}}}, return_document=True, projection={"values": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": [True, {"_id": 3, "x": 3}]}))
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]]
                    for method in (session.update_one, session.update_many):
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {}, {"$set": {"atomic_marker": True}, "$pull": {"keep": 1}})
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {"_id": 99}, {"$pull": {"values": {"$expr": {"$eq": [1, 1]}}}})
                    for after in (False, True):
                        with self.assertRaises(briskdb.LimitExceededError):
                            session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pull": {"values": True}}, return_document=after, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"values": True}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)

    def test_push_modifiers_counts_images_fidelity_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "values": [Int64(3)], "keep": True})
                    identifier = uuid.uuid4()
                    expression = {"$push": {"values": {"$slice": 3, "$sort": 1, "$position": -1, "$each": [2, 1]}}}
                    result = session.update_many(DATABASE, COLLECTION, {}, expression, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (4, 4, identifier))
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {}, {"$push": {"values": {"$each": []}}})["modified_count"], 0)
                    image = session.find_one_and_update(DATABASE, COLLECTION, {}, {"$push": {"values": [4, 5]}}, sort={"_id": -1}, projection={"values": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": [1, 2, Int64(3)]}))
                    expression = {"$push": {"values": {"$each": [Timestamp(0, 0), Binary(b"value", 128)], "$slice": -3}}}
                    image = session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, expression, return_document=True, projection={"values": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": [[4, 5], Timestamp(0, 0), Binary(b"value", 128)]}))
                    before = [bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]]
                    for method in (session.update_one, session.update_many):
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            method(DATABASE, COLLECTION, {}, {"$set": {"atomic_marker": True}, "$push": {"keep": 1}})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        session.update_many(DATABASE, COLLECTION, {"_id": 99}, {"$push": {"values": {"$each": [], "$sort": {}}}})
                    for after in (False, True):
                        with self.assertRaises(briskdb.LimitExceededError):
                            session.find_one_and_update(DATABASE, COLLECTION, {}, {"$push": {"values": None}}, return_document=after, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$push": {"values": None}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], before)

    def test_array_membership_counts_fidelity_images_errors_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    original = [Int64(1), 1.0, True, {"a": 1, "b": 2}, {"b": 2, "a": 1}]
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "values": original, "keep": True})
                    identifier = uuid.uuid4()
                    expression = {"$addToSet": {"values": {"$each": [1, Binary(b"value", 128), Binary(b"value", 128)]}}}
                    result = session.update_many(DATABASE, COLLECTION, {}, expression, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (4, 4, identifier))
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {}, expression)["modified_count"], 0)
                    image = session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pullAll": {"values": [1, {"a": 1, "b": 2}]}}, sort={"_id": -1}, projection={"values": 1, "_id": 0})["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": original + [Binary(b"value", 128)]}))
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$addToSet": {"values": [1, 2]}}, return_document=True, projection={"values": 1, "_id": 0})["document"], {"values": [True, {"b": 2, "a": 1}, Binary(b"value", 128), [1, 2]]})
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"_id": 3}, {"$addToSet": {"empty": {"$each": []}}})["modified_count"], 1)
                    before = session.find(DATABASE, COLLECTION)["documents"]
                    for operator, operand in [("$addToSet", 1), ("$pullAll", [])]:
                        for method in (session.update_one, session.update_many):
                            with self.assertRaises(briskdb.InvalidArgumentError):
                                method(DATABASE, COLLECTION, {}, {"$set": {"atomic_marker": True}, operator: {"keep": operand}})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        session.update_many(DATABASE, COLLECTION, {"_id": 99}, {"$addToSet": {"values": {"$each": None}}})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {}, {"$addToSet": {"values": None}}, return_document=True, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$pullAll": {"values": [True]}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])

    def test_pop_rename_counts_images_errors_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "old": Binary(b"value", 128), "items": [Int64(1), Int64(2), Int64(3)], "keep": True})
                    identifier = uuid.uuid4()
                    expression = {"$rename": {"old": "new.value"}, "$pop": {"items": -1}}
                    result = session.update_many(DATABASE, COLLECTION, {}, expression, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (4, 4, identifier))
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pop": {"items": 1}}, sort={"_id": -1}, projection={"items": 1, "_id": 0})["document"], {"items": [Int64(2), Int64(3)]})
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$rename": {"new.value": "value"}}, return_document=True, projection={"value": 1, "_id": 0})["document"], {"value": Binary(b"value", 128)})
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {}, {"$rename": {"absent": "items.0"}})["modified_count"], 0)
                    before = session.find(DATABASE, COLLECTION)["documents"]
                    for operator, changes in [("$pop", {"keep": 1}), ("$pop", {"items": True}), ("$rename", {"new.value": "items.0"}), ("$rename", {"new": "_id"}), ("$rename", {"new": "new.child"})]:
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.update_one(DATABASE, COLLECTION, {"_id": 0}, {"$set": {"atomic_marker": True}, operator: changes})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pop": {"items": 1}}, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$pop": {"items": 1}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])

    def test_min_max_counts_images_bson_identity_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "low": 5.0, "high": 5.0, "keep": True, "a": [None]})
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"_id": 0}, {"$min": {"low": Decimal128("5")}, "$max": {"high": Int64(5)}})["modified_count"], 0)
                    expression = {"$min": {"low": 4, "a.3": 2}, "$max": {"high": 6}}
                    result = session.update_many(DATABASE, COLLECTION, {}, expression)
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual(session.update_many(DATABASE, COLLECTION, {}, expression)["modified_count"], 0)
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {}, {"$min": {"low": 3}}, sort={"_id": -1}, projection={"low": 1, "_id": 0})["document"], {"low": 4})
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$max": {"high": 7}}, return_document=True, projection={"high": 1, "_id": 0})["document"], {"high": 7})
                    before = session.find(DATABASE, COLLECTION)["documents"]
                    for expression in [{"$min": {"keep.x": 1}}, {"$max": {"_id": 99}}, {"$min": {"low": 1}, "$max": {"low": 2}}]:
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.update_one(DATABASE, COLLECTION, {"_id": 0}, expression)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {"_id": 0}, {"$max": {"large": "x" * 1000}}, return_document=True, max_result_bytes=128)
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])

    def test_find_one_and_update_images_projection_limits_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "rank": i, "keep": True})
                    identifier = uuid.uuid4()
                    before = session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {"rank": -1, "stamp": Timestamp(0, 0)}}, sort={"rank": -1}, projection={"rank": 1, "_id": 0}, request_id=identifier)
                    self.assertEqual((before["document"], before["request_id"]), ({"rank": 3}, identifier))
                    expected = {"_id": Int64(3), "rank": -1, "keep": True, "stamp": Timestamp(0, 0)}
                    for after in [False, True]:
                        image = session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3.0}, {"$set": {"rank": -1}}, return_document=after)["document"]
                        self.assertEqual(bson_bytes(image), bson_bytes(expected))
                    self.assertIsNone(session.find_one_and_update(DATABASE, COLLECTION, {"_id": 99}, {"$set": {}})["document"])
                    with self.assertRaises((TypeError, ValueError)):
                        session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {}}, return_document="after")
                    self.assertFalse(session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {}}, upsert=True)["did_upsert"])
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$set": {"large": "x" * 600000}}, return_document=True, max_result_bytes=128)
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION, {"_id": 3})["documents"][0]), bson_bytes(expected))
                    deep = {}
                    for _ in range(98):
                        deep = {"nested": deep}
                    # Insert at the native boundary: an update expression adds
                    # another container around this depth-100 stored document.
                    session.replace_one(DATABASE, COLLECTION, {"_id": 3}, {**expected, "deep": deep})
                    # Replacement stamps direct zero timestamps; restore the
                    # literal operator value before checking returned images.
                    session.update_one(DATABASE, COLLECTION, {"_id": 3}, {"$set": {"stamp": Timestamp(0, 0)}})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$set": {}}, return_document=True)
                    self.assertEqual(session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$set": {}}, return_document=True, projection=["_id"])["document"], {"_id": Int64(3)})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$unset": {"deep": 1}})
                    session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3}, {"$unset": {"deep": 1}}, projection=["_id"])
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {"changed": True}}, cancellation=token)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION, {"_id": 3})["documents"][0]), bson_bytes(expected))

    def test_update_many_counts_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(24):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "group": i % 2, "v": 1})
                    identifier = uuid.uuid4()
                    expression = {"$set": {"v": Int64(1), "stamp": Timestamp(0, 0)}}
                    result = session.update_many(DATABASE, COLLECTION, {"group": 0}, expression, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"], result["upserted_id"]), (12, 12, identifier, None))
                    self.assertEqual(session.update_many(DATABASE, COLLECTION, {"group": 0}, expression)["modified_count"], 0)
                    self.assertEqual(session.update_many(DATABASE, COLLECTION, {"_id": 1.0}, expression)["modified_count"], 1)
                    self.assertEqual(session.update_many(DATABASE, COLLECTION, {"_id": 99}, expression)["matched_count"], 0)
                    before = session.find(DATABASE, COLLECTION)["documents"]
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$unset": {"group": 1}}, max_result_bytes=1)
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$set": {"v.x": 2}})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_many(DATABASE, COLLECTION, {}, {"$unset": {"group": 1}}, cancellation=token)
                    self.assertFalse(session.update_many(DATABASE, COLLECTION, {}, {"$set": {}}, upsert=True)["did_upsert"])
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in before])
                    result = session.update_many(DATABASE, COLLECTION, {}, expression)
                    self.assertEqual((result["matched_count"], result["modified_count"]), (24, 11))
                    expected = session.find(DATABASE, COLLECTION)["documents"]
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual([bson_bytes(row) for row in session.find(DATABASE, COLLECTION)["documents"]], [bson_bytes(row) for row in expected])

    def test_update_one_preserves_fields_and_preflights_failures(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    original = {"_id": Int64(1), "v": 1, "keep": Binary(b"data", 128), "a": [1, {"x": 2}]}
                    session.insert_one(DATABASE, COLLECTION, original)
                    expression = {"$set": {"v": Int64(1), "nested.x": "$literal", "stamp": Timestamp(0, 0)}, "$unset": {"a.0": 1}}
                    identifier = uuid.uuid4()
                    result = session.update_one(DATABASE, COLLECTION, {"v": 1}, expression, request_id=identifier)
                    self.assertEqual((result["kind"], result["matched_count"], result["modified_count"], result["upserted_id"], result["request_id"]), ("update", 1, 1, None, identifier))
                    expected = {**original, "v": Int64(1), "a": [None, {"x": 2}], "nested": {"x": "$literal"}, "stamp": Timestamp(0, 0)}
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION)["documents"][0]), bson_bytes(expected))
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"_id": 1.0}, expression)["modified_count"], 0)
                    self.assertEqual(session.update_one(DATABASE, COLLECTION, {"_id": 99}, {"$set": {}})["matched_count"], 0)
                    for update in [{"$set": {"changed": 1, "v.x": 2}}, {"$set": {"_id": 2}}, {"$unset": {"_id": 1}}, {"$set": {"a": 1}, "$unset": {"a.x": 1}}, {"$set": {"a..b": 1}}, {"$set": 1}, {}]:
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.update_one(DATABASE, COLLECTION, {}, update)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.update_one(DATABASE, COLLECTION, {}, {"$set": {"a.9999999999999999999999999": 1}})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.update_one(DATABASE, COLLECTION, {}, {"$set": {"changed": 1}}, max_result_bytes=1)
                    with self.assertRaises(briskdb.UnsupportedError):
                        session.update_one(DATABASE, COLLECTION, {}, {"$mul": {"v": 1}})
                    self.assertFalse(session.update_one(DATABASE, COLLECTION, {}, {"$set": {}}, upsert=True)["did_upsert"])
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.update_one(DATABASE, COLLECTION, {}, {"$set": {"v": 2}}, cancellation=token)
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION)["documents"][0]), bson_bytes(expected))
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION)["documents"][0]), bson_bytes(expected))

    def test_find_one_and_replace_returns_both_images_and_preflights_output(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "rank": i})
                    identifier = uuid.uuid4()
                    result = session.find_one_and_replace(DATABASE, COLLECTION, {}, {"value": Int64(9)}, sort={"rank": -1}, projection={"rank": 1, "_id": 0}, request_id=identifier)
                    self.assertEqual((result["kind"], result["document"], result["request_id"]), ("document", {"rank": 3}, identifier))
                    after = session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3.0}, {"value": Int64(9)}, return_document=True)["document"]
                    self.assertEqual(bson_bytes(after), bson_bytes({"_id": Int64(3), "value": Int64(9)}))
                    self.assertIsNone(session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 99}, {})["document"])
                    with self.assertRaises((TypeError, ValueError)):
                        session.find_one_and_replace(DATABASE, COLLECTION, {}, {}, return_document="after")
                    large = {"payload": "x" * 600000}
                    for return_after, replacement in [(True, large), (False, {})]:
                        if not return_after:
                            session.replace_one(DATABASE, COLLECTION, {"_id": 3}, large)
                        before = session.find(DATABASE, COLLECTION, {"_id": 3})["documents"][0]
                        with self.assertRaises(briskdb.LimitExceededError):
                            session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, replacement, return_document=return_after, max_result_bytes=128)
                        self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": 3})["documents"][0], before)
                    deep = {}
                    for _ in range(99):
                        deep = {"nested": deep}
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, deep, return_document=True)
                    self.assertEqual(session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, deep, return_document=True, projection=["_id"])["document"], {"_id": Int64(3)})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, {}, return_document=False)
                    session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, {"value": "persisted"}, projection=["_id"])
                    self.assertFalse(session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3}, {"value": "persisted"}, upsert=True)["did_upsert"])
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION, {"_id": 3})["documents"][0]), bson_bytes({"_id": Int64(3), "value": "persisted"}))

    def test_replace_one_preserves_id_and_reports_representation_changes(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    session.insert_one(DATABASE, COLLECTION, {"_id": Int64(1), "v": 7, "obsolete": True})
                    replacement = {"v": Int64(7), "_id": 1.0}
                    identifier = uuid.uuid4()
                    result = session.replace_one(DATABASE, COLLECTION, {"v": 7}, replacement, request_id=identifier)
                    self.assertEqual((result["kind"], result["matched_count"], result["modified_count"], result["upserted_id"], result["request_id"]), ("update", 1, 1, None, identifier))
                    self.assertTrue(result["acknowledged"])
                    self.assertIs(type(replacement["_id"]), float)
                    self.assertEqual(session.replace_one(DATABASE, COLLECTION, {"_id": 1}, replacement)["modified_count"], 0)
                    self.assertEqual(session.replace_one(DATABASE, COLLECTION, {"_id": 2}, {})["matched_count"], 0)
                    for value in (7.0, Decimal128("7.00"), 7, Int64(7)):
                        self.assertEqual(session.replace_one(DATABASE, COLLECTION, {}, {"v": value})["modified_count"], 1)
                        self.assertEqual(session.replace_one(DATABASE, COLLECTION, {}, {"v": value})["modified_count"], 0)
                    for invalid in ({"_id": 9}, {"$set": {"v": 1}}):
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            session.replace_one(DATABASE, COLLECTION, {}, invalid)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.replace_one(DATABASE, COLLECTION, {}, {}, max_result_bytes=1)
                    self.assertFalse(session.replace_one(DATABASE, COLLECTION, {}, {"v": Int64(7)}, upsert=True)["did_upsert"])
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.replace_one(DATABASE, COLLECTION, {}, {}, cancellation=token)
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION)["documents"][0]), bson_bytes({"_id": Int64(1), "v": Int64(7)}))
                    session.insert_one(DATABASE, COLLECTION, {"v": 1, "_id": 2})
                    self.assertEqual(session.replace_one(DATABASE, COLLECTION, {"_id": 2}, {"v": 1})["modified_count"], 1)
                    self.assertEqual(session.replace_one(DATABASE, COLLECTION, {"_id": 2}, {"v": 1})["modified_count"], 0)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes(session.find(DATABASE, COLLECTION)["documents"][0]), bson_bytes({"_id": Int64(1), "v": Int64(7)}))

    def test_find_one_and_delete_projects_before_commit_and_preserves_preimage(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in reversed(range(12)):
                        session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "group": i % 2, "nested": {"v": i}})
                    identifier = uuid.uuid4()
                    result = session.find_one_and_delete(DATABASE, COLLECTION, {"group": 0}, sort={"_id": 1}, projection={"nested.v": 1, "_id": 0}, request_id=identifier)
                    self.assertEqual(result["kind"], "document")
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["document"], {"nested": {"v": 0}})
                    self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": 0})["documents"], [])
                    row = session.find_one_and_delete(DATABASE, COLLECTION, {"_id": 11.0})["document"]
                    self.assertIsInstance(row["_id"], Int64)
                    self.assertIsNone(session.find_one_and_delete(DATABASE, COLLECTION, {"_id": -1})["document"])
                    session.insert_one(DATABASE, COLLECTION, {"_id": "large", "value": "x" * 600000})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_delete(DATABASE, COLLECTION, {"_id": "large"}, max_result_bytes=128)
                    self.assertEqual(session.find_one_and_delete(DATABASE, COLLECTION, {"_id": "large"}, projection=["_id"], max_result_bytes=128)["document"], {"_id": "large"})
                    deep = {}
                    for _ in range(99):
                        deep = {"nested": deep}
                    deep["_id"] = "deep"
                    session.insert_one(DATABASE, COLLECTION, deep)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.find_one_and_delete(DATABASE, COLLECTION, {"_id": "deep"})
                    self.assertEqual(session.find_one_and_delete(DATABASE, COLLECTION, {"_id": "deep"}, projection=["_id"])["document"], {"_id": "deep"})
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.find_one_and_delete(DATABASE, COLLECTION, {}, cancellation=token)
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 10)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.find_one_and_delete(DATABASE, COLLECTION, {})["document"]["_id"], 10)

    def test_filtered_deletes_order_types_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    session.create_collection(DATABASE, COLLECTION)
                    for i in reversed(range(40)):
                        session.insert_one(DATABASE, COLLECTION, {"_id": i, "nested": {"v": i % 2}, "label": "remove"})
                    identifier = uuid.uuid4()
                    result = session.delete_one(DATABASE, COLLECTION, {"nested.v": 0}, request_id=identifier)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["deleted_count"], 1)
                    self.assertTrue(result["acknowledged"])
                    self.assertEqual(session.find(DATABASE, COLLECTION, {"_id": 38})["documents"], [])
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.delete_many(DATABASE, COLLECTION, {}, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.delete_many(DATABASE, COLLECTION, {}, cancellation=token)
                    with self.assertRaises(briskdb.UnsupportedError):
                        session.delete_many(DATABASE, COLLECTION, {"$where": "secret"})
                    result = session.delete_many(DATABASE, COLLECTION, {"nested.v": 0, "label": {"$regex": "^rem"}})
                    self.assertEqual(result["deleted_count"], 19)
                    self.assertEqual(session.delete_many(DATABASE, COLLECTION, {"_id": 39.0})["deleted_count"], 1)
                    self.assertEqual(session.delete_one(DATABASE, COLLECTION, {})["deleted_count"], 1)
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 18)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.delete_many(DATABASE, COLLECTION, {})["deleted_count"], 18)
                    self.assertEqual(session.delete_many(DATABASE, COLLECTION, {})["deleted_count"], 0)

    def test_database_names_filter_limits_controls_and_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    session.migrate("CREATE TABLE sql_only (id INTEGER PRIMARY KEY)")
                    self.assertEqual(session.list_database_names()["names"], [])
                    session.create_collection("one", "items")
                    session.create_collection("two", "items")
                    identifier = uuid.uuid4()
                    result = session.list_database_names({"name": {"$regex": "^o"}}, request_id=identifier, max_result_rows=1, max_result_bytes=64)
                    self.assertEqual(result, {"kind": "database_names", "names": ["one"], "request_id": identifier, "plan": None})
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.list_database_names(max_result_rows=1)
                    with self.assertRaises(briskdb.UnsupportedError):
                        session.list_database_names({"sizeOnDisk": 0})
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.list_database_names(cancellation=token)
                    session.drop_database("one")
            with briskdb.open(root, shards=2, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(session.list_database_names()["names"], ["two"])
                    session.drop_collection("two", "items")
                    self.assertEqual(session.list_database_names()["names"], [])

    def test_collection_metadata_paging_filters_limits_uuid_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=2, documents=True, uuid_representation="standard") as database:
                with database.session() as session:
                    session.migrate("CREATE TABLE sql_only (id INTEGER PRIMARY KEY)")
                    self.assertEqual(session.list_collection_metadata("absent")["documents"], [])
                    for name in ["a", "b", "c"]:
                        session.create_collection(DATABASE, name)
                    request_id = uuid.uuid4()
                    page = session.list_collection_metadata(DATABASE, batch_size=1, request_id=request_id)
                    self.assertEqual(page["request_id"], request_id)
                    self.assertIsNone(page["plan"])
                    first = page["documents"][0]
                    identity = first["info"]["uuid"]
                    self.assertIsInstance(identity, uuid.UUID)
                    rows = page["documents"][:]
                    while page["cursor_id"] is not None:
                        page = session.get_more(DATABASE, "$cmd.listCollections", page["cursor_id"], batch_size=1)
                        rows.extend(page["documents"])
                    self.assertEqual([row["name"] for row in rows], ["a", "b", "c"])
                    self.assertEqual(session.list_collection_metadata(DATABASE, {"name": "b"}, name_only=True)["documents"], [{"name": "b", "type": "collection"}])
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.list_collection_metadata(DATABASE, max_result_rows=1)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.list_collection_metadata(DATABASE, batch_byte_limit=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.list_collection_metadata(DATABASE, cancellation=token)
                    page = session.list_collection_metadata(DATABASE, batch_size=0)
                    self.assertTrue(session.kill_cursor(DATABASE, "$cmd.listCollections", page["cursor_id"])["killed"])
            with briskdb.open(root, shards=2, documents=True, uuid_representation="standard") as database:
                with database.session() as session:
                    self.assertEqual(session.list_collection_metadata(DATABASE, {"name": "a"})["documents"], [first])
                    session.drop_collection(DATABASE, "a")
                    session.create_collection(DATABASE, "a")
                    self.assertNotEqual(session.list_collection_metadata(DATABASE, {"name": "a"})["documents"][0]["info"]["uuid"], identity)

    def test_namespace_drop_scope_identity_cursor_controls_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertFalse(session.drop_database(DATABASE)["existed"])
                    first = session.create_collection(DATABASE, COLLECTION)["collection"]["id"]
                    session.create_collection(DATABASE, "keep")
                    for index in range(4):
                        session.insert_one(DATABASE, COLLECTION, {"_id": index})
                    session.insert_one(DATABASE, "keep", {"_id": 1})
                    page = session.aggregate(DATABASE, COLLECTION, [{"$sort": {"_id": -1}}], batch_size=1)
                    identifier = uuid.uuid4()
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        session.drop_database(DATABASE, cancellation=token)
                    with self.assertRaises(briskdb.LimitExceededError):
                        session.drop_collection(DATABASE, COLLECTION, max_result_bytes=1)
                    result = session.drop_collection(DATABASE, COLLECTION, request_id=identifier)
                    self.assertEqual(result, {"kind": "namespace_dropped", "existed": True, "request_id": identifier, "plan": None})
                    self.assertFalse(session.drop_collection(DATABASE, COLLECTION)["existed"])
                    second = session.create_collection(DATABASE, COLLECTION)["collection"]["id"]
                    self.assertGreater(second, first)
                    session.insert_one(DATABASE, COLLECTION, {"_id": 99})
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        session.get_more(DATABASE, COLLECTION, page["cursor_id"])
                    self.assertEqual(session.count_documents(DATABASE, "keep")["count"], 1)
                    self.assertTrue(session.drop_database(DATABASE)["existed"])
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertFalse(session.collection_exists(DATABASE, COLLECTION)["exists"])
                    self.assertGreater(session.create_collection(DATABASE, COLLECTION)["collection"]["id"], second)
                    self.assertEqual(session.count_documents(DATABASE, COLLECTION)["count"], 0)

    def test_computed_group_keys_and_literal_count_match_direct_count(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            documents = [{"_id": index, "k": Int64(1) if index == 0 else 1.0} for index in range(9)] + [{"_id": 9}]
            pipeline = [{"$group": {"_id": {"value": "$k", "absent": "$missing"}, "n": {"$sum": 1}}}]
            expected = [{"_id": {"value": Int64(1)}, "n": 9}, {"_id": {}, "n": 1}]
            for reopen in [False, True]:
                with briskdb.open(root, shards=4, documents=True) as database:
                    with database.session() as session:
                        if not reopen:
                            session.create_collection(DATABASE, COLLECTION)
                            for document in documents:
                                session.insert_one(DATABASE, COLLECTION, document)
                        page = session.aggregate(DATABASE, COLLECTION, pipeline, batch_size=1)
                        rows = page["documents"]
                        while page["cursor_id"] is not None:
                            page = session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                            rows.extend(page["documents"])
                        self.assertEqual(bson_bytes({"rows": rows}), bson_bytes({"rows": expected}))
                        query = {"k": 1}
                        direct = session.count_documents(DATABASE, COLLECTION, query, skip=2, limit=3)["count"]
                        stages = [{"$match": query}, {"$skip": 2}, {"$limit": 3}, {"$group": {"_id": 1, "n": {"$sum": 1}}}]
                        self.assertEqual(session.aggregate(DATABASE, COLLECTION, stages)["documents"], [{"_id": 1, "n": direct}])
                        self.assertEqual(bson_bytes({"rows": session.find(DATABASE, COLLECTION)["documents"]}), bson_bytes({"rows": documents}))

    def test_group_structured_keys_numeric_precision_and_cursor_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            documents = [{"_id": 1, "k": {"x": Int64(1)}, "v": Decimal128("1.00")},
                         {"_id": 2, "k": {"x": 1.0}, "v": 2.1}, {"_id": 3}, {"_id": 4, "k": None, "v": None}]
            pipeline = [{"$group": {"_id": "$k", "avg": {"$avg": "$v"}, "sum": {"$sum": "$v"},
                                    "first": {"$first": "$v"}, "last": {"$last": "$v"}, "push": {"$push": "$v"},
                                    "set": {"$addToSet": "$k"}, "min": {"$min": "$v"}, "max": {"$max": "$v"}}}]
            expected = [{"_id": {"x": Int64(1)}, "avg": Decimal128("1.550000000000000044408920985006262"),
                         "sum": Decimal128("3.100000000000000088817841970012523"), "first": Decimal128("1.00"), "last": 2.1,
                         "push": [Decimal128("1.00"), 2.1], "set": [{"x": Int64(1)}], "min": Decimal128("1.00"), "max": 2.1},
                        {"_id": None, "avg": None, "sum": 0, "first": None, "last": None, "push": [None], "set": [None], "min": None, "max": None}]
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
                    self.assertEqual(session.find(DATABASE, COLLECTION)["documents"], documents)
            self.assertEqual(bson_bytes({"documents": documents, "pipeline": pipeline}), before)
            with briskdb.open(root, shards=4, documents=True) as database:
                with database.session() as session:
                    self.assertEqual(bson_bytes({"rows": session.aggregate(DATABASE, COLLECTION, pipeline)["documents"]}), bson_bytes({"rows": expected}))

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
    async def test_async_opt_in_execution_stats(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for identity in range(3):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": identity, "v": 1})
                    first = await session.find(DATABASE, COLLECTION, batch_size=1, execution_stats=True)
                    self.assertIn(first["read_stats"]["documents_examined"], (2, 3))
                    second = await session.get_more(DATABASE, COLLECTION, first["cursor_id"], batch_size=1, execution_stats=True)
                    self.assertEqual(second["read_stats"]["documents_examined"], 2, "request-local, not cumulative")
                    last = await session.get_more(DATABASE, COLLECTION, second["cursor_id"])
                    self.assertNotIn("read_stats", last)
                    distinct = await session.distinct(DATABASE, COLLECTION, "v", execution_stats=True)
                    self.assertEqual(distinct["values"], [1])
                    self.assertGreaterEqual(distinct["read_stats"]["documents_examined"], 3)
                    aggregate = await session.aggregate(DATABASE, COLLECTION, [{"$count": "n"}], execution_stats=True)
                    self.assertEqual(aggregate["documents"], [{"n": 3}])
                    self.assertGreaterEqual(aggregate["read_stats"]["documents_examined"], 3)

    async def test_async_opt_in_plan_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for identity in range(3):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": identity, "v": 1})
                    await session.create_built_index(DATABASE, COLLECTION, {"v": 1})
                    first = await session.find(DATABASE, COLLECTION, {"v": 1}, batch_size=1, plan_diagnostics=True)
                    self.assertEqual(first["plan"]["read_access"]["candidate_kind"], "equality")
                    await session.drop_index(DATABASE, COLLECTION, "v_1")
                    next_page = await session.get_more(DATABASE, COLLECTION, first["cursor_id"], plan_diagnostics=True)
                    self.assertEqual(next_page["plan"]["read_access"], {"kind": "scan", "reason": "no_ready_index"})
                    self.assertEqual(len(first["documents"] + next_page["documents"]), 3)
                    distinct = await session.distinct(DATABASE, COLLECTION, "v", plan_diagnostics=True)
                    self.assertEqual(distinct["plan"]["read_access"], {"kind": "scan", "reason": "unfiltered"})
                    aggregate = await session.aggregate(DATABASE, COLLECTION, [{"$count": "n"}], plan_diagnostics=True)
                    self.assertEqual(aggregate["plan"]["read_access"], {"kind": "scan", "reason": "aggregation_input"})
                    self.assertEqual(aggregate["documents"], [{"n": 3}])
                    self.assertNotIn("read_access", (await session.find(DATABASE, COLLECTION))["plan"])

    async def test_async_nonunique_nested_candidates_upsert_and_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1, "v": [{"score": 2}]})
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 2, "v": 2})
                    await session.create_built_index(DATABASE, COLLECTION, {"v": 1})
                    await session.create_built_index(DATABASE, COLLECTION, {"v.score": 1})
                    self.assertEqual((await session.count_documents(DATABASE, COLLECTION, {"v.score": 2}))["count"], 1)
                    changed = await session.update_many(DATABASE, COLLECTION, {"v.score": 2}, {"$set": {"v": 2}})
                    self.assertEqual(changed["modified_count"], 1)
                    changed = await session.update_many(DATABASE, COLLECTION, {"v": 2}, {"$set": {"v": {"score": 3}}})
                    self.assertEqual(changed["modified_count"], 2)
                    upserted = await session.update_one(DATABASE, COLLECTION, {"_id": 3, "v.score": 7}, {"$set": {"tag": "new"}}, upsert=True)
                    self.assertEqual(upserted["upserted_id"], 3)
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    self.assertEqual((await session.count_documents(DATABASE, COLLECTION, {"v.score": 3}))["count"], 2)
                    self.assertEqual((await session.find(DATABASE, COLLECTION, {"v.score": 7}))["documents"][0]["tag"], "new")
                    removed = await session.delete_many(DATABASE, COLLECTION, {"v.score": 3})
                    self.assertEqual(removed["deleted_count"], 2)

    async def test_async_unique_build_mutations_reopen_and_drop(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1, "value": [1, 1.0]})
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 2, "value": 2})
                    result = await session.create_built_index(DATABASE, COLLECTION, {"value": 1}, unique=True)
                    self.assertEqual(result["lifecycle"], "ready")
                    for operation in [
                        lambda: session.insert_one(DATABASE, COLLECTION, {"_id": 3, "value": 1}),
                        lambda: session.update_one(DATABASE, COLLECTION, {"_id": 2}, {"$set": {"value": 1}}),
                        lambda: session.replace_one(DATABASE, COLLECTION, {"_id": 2}, {"value": 1}),
                    ]:
                        with self.assertRaises(briskdb.UniqueViolationError):
                            await operation()
                    self.assertEqual((await session.count_documents(DATABASE, COLLECTION, {"value": 2}))["count"], 1)
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    with self.assertRaises(briskdb.UniqueViolationError):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": 3, "value": 1})
                    await session.delete_one(DATABASE, COLLECTION, {"_id": 1})
                    await session.update_one(DATABASE, COLLECTION, {"_id": 2}, {"$set": {"value": 1}})
                    await session.drop_index(DATABASE, COLLECTION, "value_1")
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 3, "value": 1})
                    self.assertEqual((await session.count_documents(DATABASE, COLLECTION, {"value": 1}))["count"], 2)

    async def test_async_combined_index_creation_build_forwards_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.create_built_index(DATABASE, COLLECTION, {"value": 1}, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.create_built_index(DATABASE, COLLECTION, {"value": 1}, cancellation=token)
                    identity = uuid.uuid4()
                    result = await session.create_built_index(DATABASE, COLLECTION, {"value": 1}, name="partial", partial_filter={"active": True}, request_id=identity, timeout_ms=5000)
                    self.assertEqual((result["request_id"], result["lifecycle"], result["num_indexes_before"], result["num_indexes_after"]), (identity, "ready", 1, 2))
                    self.assertEqual((await session.list_index_metadata(DATABASE, COLLECTION))["documents"][1], {"name": "partial", "key": {"value": 1}, "partialFilterExpression": {"active": True}})
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1, "value": [1, 2], "active": True})
                    await session.drop_index(DATABASE, COLLECTION, "partial")
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    self.assertEqual(len((await session.list_index_metadata(DATABASE, COLLECTION))["documents"]), 1)

    async def test_async_paged_built_index_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.create_index(DATABASE, COLLECTION, {"value": 1}, sparse=True)
                    await session.build_index(DATABASE, COLLECTION, "value_1")
                    identity = uuid.uuid4()
                    page = await session.list_index_metadata(DATABASE, COLLECTION, batch_size=1, batch_byte_limit=1024, request_id=identity)
                    self.assertEqual(page["request_id"], identity)
                    self.assertEqual(page["documents"], [{"name": "_id_", "key": {"_id": 1}}])
                    page = await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                    self.assertEqual(page["documents"], [{"name": "value_1", "key": {"value": 1}, "sparse": True}])
                    self.assertIsNone(page["cursor_id"])
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.list_index_metadata(DATABASE, COLLECTION, max_result_bytes=1)

    async def test_async_nonunique_index_build_forwards_controls_and_ready(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.create_index(DATABASE, COLLECTION, {"value": 1})
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.build_index(DATABASE, COLLECTION, "value_1", max_result_bytes=1)
                    identity = uuid.uuid4()
                    result = await session.build_index(DATABASE, COLLECTION, "value_1", request_id=identity, timeout_ms=5000)
                    self.assertEqual((result["request_id"], result["lifecycle"]), (identity, "ready"))
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1, "value": "maintained"})
                    await session.create_index(DATABASE, COLLECTION, {"value": 1}, name="to_drop")
                    await session.build_index(DATABASE, COLLECTION, "to_drop")
                    self.assertTrue((await session.drop_index(DATABASE, COLLECTION, "to_drop"))["acknowledged"])
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    self.assertEqual((await session.list_indexes(DATABASE, COLLECTION))["indexes"][1]["lifecycle"], "ready")

    async def test_async_sparse_partial_declarations_forward_options_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    request_id = uuid.uuid4()
                    result = await session.create_index(DATABASE, COLLECTION, {"label": 1}, sparse=True, request_id=request_id, timeout_ms=5000)
                    self.assertEqual(result["request_id"], request_id)
                    self.assertEqual(result["lifecycle"], "pending_build")
                    partial = {"active": True}
                    await session.create_index(DATABASE, COLLECTION, {"label": 1}, name="partial", unique=True, partial_filter=partial)
                    indexes = (await session.list_indexes(DATABASE, COLLECTION))["indexes"]
                    self.assertTrue(indexes[1]["sparse"])
                    self.assertEqual(indexes[2]["partial_filter"], partial)
                    with self.assertRaises(briskdb.UnsupportedError):
                        await session.create_index(DATABASE, COLLECTION, {"label": 1}, name="invalid", sparse=True, partial_filter=partial)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.create_index(DATABASE, COLLECTION, {"label": 1}, name="bounded", partial_filter=partial, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.create_index(DATABASE, COLLECTION, {"label": 1}, name="cancelled", sparse=True, cancellation=token)
                    self.assertEqual((await session.list_indexes(DATABASE, COLLECTION))["indexes"], indexes)
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    self.assertEqual((await session.list_indexes(DATABASE, COLLECTION))["indexes"], indexes)

    async def test_async_pending_index_drop_forwards_controls_and_acknowledgement(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.create_index(DATABASE, COLLECTION, {"label": 1})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.drop_index(DATABASE, COLLECTION, "_id_")
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.drop_index(DATABASE, COLLECTION, "label_1", max_result_bytes=33)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.drop_index(DATABASE, COLLECTION, "label_1", cancellation=token)
                    self.assertEqual(len((await session.list_indexes(DATABASE, COLLECTION))["indexes"]), 2)
                    request_id = uuid.uuid4()
                    result = await session.drop_index(DATABASE, COLLECTION, "label_1", request_id=request_id, timeout_ms=5000, max_result_rows=1, max_result_bytes=34)
                    self.assertEqual(result, {"request_id": request_id, "plan": None, "kind": "acknowledged", "acknowledged": True})
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        await session.drop_index(DATABASE, COLLECTION, "label_1")
                    self.assertEqual([index["name"] for index in (await session.list_indexes(DATABASE, COLLECTION))["indexes"]], ["_id_"])

    async def test_async_index_definitions_generate_names_and_remain_pending(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    result = await session.create_index(DATABASE, COLLECTION, {"label": 1, "rank": -1})
                    self.assertEqual((result["index_name"], result["lifecycle"]), ("label_1_rank_-1", "pending_build"))
                    result = await session.create_index(DATABASE, COLLECTION, {"label": Int64(1), "rank": -1.0}, name=None)
                    self.assertEqual(result["index_name"], "label_1_rank_-1")
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.create_index(DATABASE, COLLECTION, {"bad.$private": 1})
                    self.assertEqual(len((await session.list_indexes(DATABASE, COLLECTION))["indexes"]), 2)

    async def test_async_find_upserts_images_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for replacement in (False, True):
                        method = session.find_one_and_replace if replacement else session.find_one_and_update
                        body = {"value": Int64(7)} if replacement else {"$set": {"value": Int64(7)}}
                        for after in (False, True):
                            identifier = None if not replacement and not after else Int64(2 * replacement + after)
                            result = await method(DATABASE, COLLECTION, {"_id": identifier}, body, upsert=True, return_document=after, projection={"value": 1, "_id": 0})
                            self.assertEqual((result["did_upsert"], result["upserted_id"], result["document"]), (True, identifier, {"value": Int64(7)} if after else None))
                            result = await method(DATABASE, COLLECTION, {"_id": identifier}, body, upsert=True, return_document=True, projection={"absent": 1, "_id": 0})
                            self.assertEqual((result["document"], result["did_upsert"], result["upserted_id"]), ({}, False, None))

    async def test_async_operator_upserts_preserve_null_results_and_literals(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for method, identifier in [(session.update_one, None), (session.update_many, Int64(2))]:
                        result = await method(DATABASE, COLLECTION, {"_id": identifier, "counter": 3}, {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0)}}, upsert=True)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (0, 0, True))
                        self.assertEqual(bson_bytes({"v": result["upserted_id"]}), bson_bytes({"v": identifier}))
                        self.assertEqual(bson_bytes((await session.find(DATABASE, COLLECTION, {"_id": identifier}))["documents"][0]), bson_bytes({"_id": identifier, "counter": 5, "stamp": Timestamp(0, 0)}))
                        result = await method(DATABASE, COLLECTION, {"_id": identifier}, {"$inc": {"counter": 0}}, upsert=True)
                        self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (1, 0, False))
                    result = await session.update_many(DATABASE, COLLECTION, {"tag": "generated"}, {"$set": {"stamp": Timestamp(0, 0)}}, upsert=True)
                    self.assertIsInstance(result["upserted_id"], ObjectId)

    async def test_async_replacement_upsert_distinguishes_null_identity(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    result = await session.replace_one(DATABASE, COLLECTION, {"_id": None}, {"value": Int64(1)}, upsert=True)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["upserted_id"], result["did_upsert"]), (0, 0, None, True))
                    result = await session.replace_one(DATABASE, COLLECTION, {"_id": None}, {"value": Int64(2)}, upsert=True)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["did_upsert"]), (1, 1, False))
                    result = await session.replace_one(DATABASE, COLLECTION, {"missing": True}, {}, upsert=True)
                    self.assertTrue(result["did_upsert"])
                    self.assertIsInstance(result["upserted_id"], ObjectId)
                    before = [bson_bytes(row) for row in (await session.find(DATABASE, COLLECTION))["documents"]]
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.replace_one(DATABASE, COLLECTION, {"_id": 99}, {"_id": 98}, upsert=True)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.replace_one(DATABASE, COLLECTION, {"_id": 99}, {}, upsert=True, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in (await session.find(DATABASE, COLLECTION))["documents"]], before)

    async def test_async_increment_counts_numeric_fidelity_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "amount": Decimal128("1.00")})
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": Int64(1), "amount": Decimal128("2.5")}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": 0, "amount": Decimal128("0E-100")}}))["modified_count"], 0)
                    image = (await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$inc": {"counter": 1}}, sort={"_id": -1}, projection={"counter": 1, "_id": 0}, return_document=True))["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"counter": Int64(2)}))
                    before = [bson_bytes(row) for row in (await session.find(DATABASE, COLLECTION))["documents"]]
                    for expression in ({"$inc": {"counter": True}}, {"$inc": {"_id": 1}}):
                        with self.assertRaises(briskdb.InvalidArgumentError):
                            await session.update_many(DATABASE, COLLECTION, {}, expression)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$inc": {"counter": 1}}, cancellation=token)
                    self.assertEqual([bson_bytes(row) for row in (await session.find(DATABASE, COLLECTION))["documents"]], before)

    async def test_async_find_one_and_replace_forwards_images_sort_projection_and_limits(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "rank": i})
                    identifier = uuid.uuid4()
                    result = await session.find_one_and_replace(DATABASE, COLLECTION, {}, {"value": Int64(9)}, sort={"rank": -1}, projection={"rank": 1, "_id": 0}, request_id=identifier)
                    self.assertEqual((result["document"], result["request_id"]), ({"rank": 3}, identifier))
                    result = await session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 3.0}, {"value": Int64(9)}, return_document=True)
                    self.assertEqual(bson_bytes(result["document"]), bson_bytes({"_id": Int64(3), "value": Int64(9)}))
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.find_one_and_replace(DATABASE, COLLECTION, {}, {}, max_result_bytes=1)
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.find_one_and_replace(DATABASE, COLLECTION, {}, {}, cancellation=token)
                    self.assertIsNone((await session.find_one_and_replace(DATABASE, COLLECTION, {"_id": 99}, {}))["document"])

    async def test_async_pull_counts_images_and_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "values": [Int64(1), [1, 2], True, {"x": 3}]})
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"values": {"$eq": 1}}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {}, {"$pull": {"missing": 1}}))["modified_count"], 0)
                    image = (await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pull": {"values": {"x": {"$gte": 3}}}}, sort={"_id": -1}, projection={"values": 1, "_id": 0}, return_document=True))["document"]
                    self.assertEqual(image, {"values": [True]})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"_id": 1}})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"values": {"$regex": "["}}})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$pull": {"values": True}}, cancellation=token)
                    self.assertEqual([row["values"] for row in (await session.find(DATABASE, COLLECTION))["documents"]], [[True, {"x": 3}]] * 3 + [[True]])

    async def test_async_push_counts_images_and_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "values": [Int64(3)]})
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$push": {"values": {"$each": [2, 1], "$sort": 1}}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {}, {"$push": {"values": {"$each": []}}}))["modified_count"], 0)
                    image = (await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$push": {"values": {"$each": [4], "$slice": -2}}}, sort={"_id": -1}, projection={"values": 1, "_id": 0}, return_document=True))["document"]
                    self.assertEqual(bson_bytes(image), bson_bytes({"values": [Int64(3), 4]}))
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$push": {"_id": 1}})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$push": {"values": None}}, cancellation=token)
                    self.assertEqual([row["values"] for row in (await session.find(DATABASE, COLLECTION))["documents"]], [[1, 2, Int64(3)]] * 3 + [[Int64(3), 4]])

    async def test_async_array_membership_counts_images_and_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "values": [Int64(1), True]})
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {"_id": 0}, {"$addToSet": {"values": 1.0}}))["modified_count"], 0)
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$addToSet": {"values": {"$each": [2, 2.0, [1, 2]]}}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pullAll": {"values": [1, [1, 2]]}}, sort={"_id": -1}, projection={"values": 1, "_id": 0}, return_document=True))["document"], {"values": [True, 2]})
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$addToSet": {"_id": 1}})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$pullAll": {"values": [True]}}, cancellation=token)
                    self.assertEqual([row["values"] for row in (await session.find(DATABASE, COLLECTION))["documents"]], [[Int64(1), True, 2, [1, 2]]] * 3 + [[True, 2]])

    async def test_async_pop_rename_counts_images_and_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "old": Int64(9), "items": [1, 2, 3]})
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$rename": {"old": "nested.value"}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {"_id": 0}, {"$pop": {"items": 1}}))["modified_count"], 1)
                    self.assertEqual((await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$pop": {"items": -1}}, sort={"_id": -1}, projection={"items": 1, "_id": 0}, return_document=True))["document"], {"items": [2, 3]})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$pop": {"items": 1}}, cancellation=token)
                    self.assertEqual([row["items"] for row in (await session.find(DATABASE, COLLECTION))["documents"]], [[1, 2], [1, 2, 3], [1, 2, 3], [2, 3]])

    async def test_async_min_max_counts_images_and_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "v": 5.0})
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {"_id": 0}, {"$min": {"v": Int64(5)}}))["modified_count"], 0)
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$max": {"v": 6}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (4, 4))
                    self.assertEqual((await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$min": {"v": 4}}, sort={"_id": -1}, projection={"v": 1, "_id": 0}, return_document=True))["document"], {"v": 4})
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$min": {"v": 0}}, cancellation=token)
                    self.assertEqual([row["v"] for row in (await session.find(DATABASE, COLLECTION))["documents"]], [6, 6, 6, 4])

    async def test_async_find_one_and_update_forwards_images_options_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(4):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "rank": i, "keep": True})
                    identifier = uuid.uuid4()
                    result = await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {"rank": -1}}, sort={"rank": -1}, projection={"rank": 1, "_id": 0}, request_id=identifier)
                    self.assertEqual((result["document"], result["request_id"]), ({"rank": 3}, identifier))
                    result = await session.find_one_and_update(DATABASE, COLLECTION, {"_id": 3.0}, {"$unset": {"rank": 1}}, return_document=True)
                    self.assertEqual(bson_bytes(result["document"]), bson_bytes({"_id": Int64(3), "keep": True}))
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {"changed": True}}, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.find_one_and_update(DATABASE, COLLECTION, {}, {"$set": {"changed": True}}, cancellation=token)
                    self.assertIsNone((await session.find_one_and_update(DATABASE, COLLECTION, {"_id": 99}, {"$set": {}}))["document"])

    async def test_async_update_many_forwards_values_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(6):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(i), "v": 1})
                    identifier = uuid.uuid4()
                    expression = {"$set": {"v": Int64(1)}}
                    result = await session.update_many(DATABASE, COLLECTION, {}, expression, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (6, 6, identifier))
                    self.assertEqual((await session.update_many(DATABASE, COLLECTION, {}, expression))["modified_count"], 0)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$unset": {"v": 1}}, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_many(DATABASE, COLLECTION, {}, {"$unset": {"v": 1}}, cancellation=token)
                    result = await session.update_many(DATABASE, COLLECTION, {}, {"$unset": {"v": 1}})
                    self.assertEqual((result["matched_count"], result["modified_count"]), (6, 6))

    async def test_async_update_one_forwards_values_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(1), "v": 1, "keep": True})
                    identifier = uuid.uuid4()
                    result = await session.update_one(DATABASE, COLLECTION, {"v": 1}, {"$set": {"v": Int64(1)}}, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (1, 1, identifier))
                    self.assertEqual((await session.update_one(DATABASE, COLLECTION, {"_id": 1}, {"$set": {"v": Int64(1)}}))["modified_count"], 0)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.update_one(DATABASE, COLLECTION, {}, {"$unset": {"keep": 1}}, max_result_bytes=1)
                    token = briskdb.CancellationToken(); token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        await session.update_one(DATABASE, COLLECTION, {}, {"$unset": {"keep": 1}}, cancellation=token)
                    self.assertEqual(bson_bytes((await session.find(DATABASE, COLLECTION))["documents"][0]), bson_bytes({"_id": Int64(1), "v": Int64(1), "keep": True}))

    async def test_async_replace_one_forwards_values_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(1), "v": 1})
                    identifier = uuid.uuid4()
                    result = await session.replace_one(DATABASE, COLLECTION, {"v": 1}, {"v": Int64(1)}, request_id=identifier)
                    self.assertEqual((result["matched_count"], result["modified_count"], result["request_id"]), (1, 1, identifier))
                    self.assertEqual((await session.replace_one(DATABASE, COLLECTION, {"_id": 1.0}, {"v": Int64(1)}))["modified_count"], 0)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.replace_one(DATABASE, COLLECTION, {}, {}, max_result_bytes=1)
                    self.assertFalse((await session.replace_one(DATABASE, COLLECTION, {}, {"v": Int64(1)}, upsert=True))["did_upsert"])
                    self.assertEqual(bson_bytes((await session.find(DATABASE, COLLECTION))["documents"][0]), bson_bytes({"_id": Int64(1), "v": Int64(1)}))

    async def test_async_find_one_and_delete_sort_projection_and_limits(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(5):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "nested": {"v": Int64(i)}})
                    identifier = uuid.uuid4()
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.find_one_and_delete(DATABASE, COLLECTION, {}, max_result_bytes=1)
                    result = await session.find_one_and_delete(DATABASE, COLLECTION, {}, projection={"nested": 1, "_id": 0}, sort={"_id": -1}, request_id=identifier)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["document"], {"nested": {"v": Int64(4)}})
                    self.assertIsInstance(result["document"]["nested"]["v"], Int64)
                    self.assertIsNone((await session.find_one_and_delete(DATABASE, COLLECTION, {"_id": 4}))["document"])
                    self.assertEqual((await session.count_documents(DATABASE, COLLECTION))["count"], 4)

    async def test_async_filtered_deletes_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for i in range(12):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": i, "v": [i % 2]})
                    self.assertEqual((await session.delete_one(DATABASE, COLLECTION, {"v": 0}))["deleted_count"], 1)
                    identifier = uuid.uuid4()
                    result = await session.delete_many(DATABASE, COLLECTION, {"v": 0}, request_id=identifier)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["deleted_count"], 5)
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.delete_many(DATABASE, COLLECTION, {}, max_result_bytes=1)
                    self.assertEqual((await session.delete_many(DATABASE, COLLECTION, {}))["deleted_count"], 6)

    async def test_async_database_names_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    self.assertEqual((await session.list_database_names())["names"], [])
                    await session.create_collection("async", "one")
                    identifier = uuid.uuid4()
                    result = await session.list_database_names({"name": "async"}, request_id=identifier, max_result_rows=1, max_result_bytes=64)
                    self.assertEqual(result["request_id"], identifier)
                    self.assertEqual(result["names"], ["async"])
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.list_database_names(max_result_bytes=1)
                    await session.drop_database("async")
                    self.assertEqual((await session.list_database_names())["names"], [])

    async def test_async_collection_metadata_forwards_cursor_and_controls(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=2, documents=True) as database:
                async with await database.session() as session:
                    for name in ["a", "b"]:
                        await session.create_collection(DATABASE, name)
                    identifier = uuid.uuid4()
                    page = await session.list_collection_metadata(DATABASE, {"type": "collection"}, name_only=True, batch_size=1, request_id=identifier, batch_byte_limit=1024)
                    self.assertEqual(page["request_id"], identifier)
                    self.assertEqual(page["documents"], [{"name": "a", "type": "collection"}])
                    page = await session.get_more(DATABASE, "$cmd.listCollections", page["cursor_id"], batch_size=1)
                    self.assertEqual(page["documents"], [{"name": "b", "type": "collection"}])
                    self.assertIsNone(page["cursor_id"])
                    with self.assertRaises(briskdb.LimitExceededError):
                        await session.list_collection_metadata(DATABASE, max_result_bytes=1)

    async def test_async_namespace_drop_and_recreate(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    first = (await session.create_collection(DATABASE, COLLECTION))["collection"]["id"]
                    await session.insert_one(DATABASE, COLLECTION, {"_id": 1})
                    identifier = uuid.uuid4()
                    result = await session.drop_collection(DATABASE, COLLECTION, request_id=identifier, timeout_ms=10000, max_result_rows=1, max_result_bytes=128)
                    self.assertTrue(result["existed"])
                    self.assertEqual(result["request_id"], identifier)
                    self.assertFalse((await session.drop_database(DATABASE))["existed"])
                    self.assertGreater((await session.create_collection(DATABASE, COLLECTION))["collection"]["id"], first)
                    self.assertTrue((await session.drop_database(DATABASE))["existed"])

    async def test_async_computed_group_key_error_and_literal_count(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for document in [{"_id": 1, "v": []}, {"_id": 2, "v": None}]:
                        await session.insert_one(DATABASE, COLLECTION, document)
                    rows = (await session.aggregate(DATABASE, COLLECTION, [{"$group": {"_id": 1, "n": {"$sum": 1}}}]))["documents"]
                    self.assertEqual(rows, [{"_id": 1, "n": (await session.count_documents(DATABASE, COLLECTION))["count"]}])
                    page = await session.aggregate(DATABASE, COLLECTION, [{"$group": {"_id": {"$size": "$v"}}}], batch_size=0)
                    self.assertEqual(page["documents"], [])
                    with self.assertRaises(briskdb.InvalidQueryError):
                        await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)

    async def test_async_group_order_and_error_cursor_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            async with await briskdb.open_async(root, shards=4, documents=True) as database:
                async with await database.session() as session:
                    await session.create_collection(DATABASE, COLLECTION)
                    for index in range(12):
                        await session.insert_one(DATABASE, COLLECTION, {"_id": Int64(index), "bucket": index % 3, "v": [] if index == 0 else None})
                    pipeline = [{"$sort": {"_id": -1}}, {"$group": {"_id": "$bucket", "n": {"$sum": 1}, "first": {"$first": "$_id"}, "last": {"$last": "$_id"}}}]
                    page = await session.aggregate(DATABASE, COLLECTION, pipeline, batch_size=1)
                    rows = page["documents"]
                    while page["cursor_id"] is not None:
                        page = await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                        rows.extend(page["documents"])
                    self.assertEqual(bson_bytes({"rows": rows}), bson_bytes({"rows": [{"_id": key, "n": 4, "first": Int64(9 + key), "last": Int64(key)} for key in (2, 1, 0)]}))
                    bad = [{"$group": {"_id": "$_id", "n": {"$first": {"$size": "$v"}}}}]
                    page = await session.aggregate(DATABASE, COLLECTION, bad, batch_size=0)
                    self.assertEqual(page["documents"], [])
                    with self.assertRaises(briskdb.InvalidQueryError):
                        await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        await session.get_more(DATABASE, COLLECTION, page["cursor_id"], batch_size=1)

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
