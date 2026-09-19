"""Required real-wire discovery and point operations, not a Mongo parity claim."""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pymongo
from bson import BSON, Binary, Code, Decimal128, Int64, ObjectId, Regex, Timestamp
from pymongo.errors import BulkWriteError, CollectionInvalid, DuplicateKeyError, OperationFailure


def array_membership_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_membership.items
        ordered, reversed_doc = {"a": 1, "b": 2}, {"b": 2, "a": 1}
        original = [Int64(1), 1.0, True, ordered, reversed_doc]
        collection.insert_many([{"_id": Int64(i), "values": original, "keep": True} for i in range(12)])
        assert collection.update_one({"_id": 0}, {"$addToSet": {"values": 1.0}}).modified_count == 0
        extra = [Timestamp(0, 0), Binary(b"value", 128), [1, 2]]
        expression = {"$addToSet": {"values": {"$each": extra + extra}}}
        result = collection.update_many({}, expression)
        assert (result.matched_count, result.modified_count) == (12, 12)
        assert collection.update_many({}, expression).modified_count == 0
        assert BSON.encode({"v": collection.find_one({"_id": 0})["values"]}) == BSON.encode({"v": original + extra})
        result = collection.update_many({}, {"$pullAll": {"values": [1, ordered]}})
        assert (result.matched_count, result.modified_count) == (12, 12)
        remaining = [True, reversed_doc] + extra
        assert collection.find_one_and_update({}, {"$pullAll": {"values": [Binary(b"value", 128)]}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}) == {"values": remaining}
        assert collection.find_one_and_update({"_id": 11}, {"$addToSet": {"nested.0.values": {"$each": []}}}, return_document=True, projection={"nested": 1, "_id": 0}) == {"nested": {"0": {"values": []}}}
        before = BSON.encode(collection.find_one({"_id": 0}))
        for expression, code in [
            ({"$addToSet": {"keep": 1}}, 2), ({"$pullAll": {"keep": []}}, 2),
            ({"$addToSet": {"values": {"$each": 1}}}, 2),
            ({"$addToSet": {"values": {"$each": [], "$slice": 1}}}, 2),
            ({"$pullAll": {"values": None}}, 2),
            ({"$addToSet": {"values.0.x": 1}}, 28),
            ({"$pullAll": {"values.01": []}}, 28),
        ]:
            try:
                collection.update_one({"_id": 0}, {"$set": {"atomic_marker": True}, **expression})
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expected array membership rejection")
            assert BSON.encode(collection.find_one({"_id": 0})) == before
        try:
            client.wire_membership.command("update", "items", ordered=False, updates=[
                {"q": {}, "u": {"$addToSet": {"keep": 1}}, "multi": True},
                {"q": {}, "u": {"$set": {"should_not_run": True}}},
            ])
        except OperationFailure as error:
            assert error.code == 2 and "writeErrors" not in error.details
        else:
            raise AssertionError("runtime multi-update failure must abort")
        assert collection.count_documents({"should_not_run": {"$exists": True}}) == 0
        queue = client.wire_membership.concurrent
        queue.insert_one({"_id": 1, "values": []})
        def add(worker):
            return sum(queue.update_one({}, {"$addToSet": {"values": {"$each": [Int64(worker), float(worker)]}}}).modified_count for _ in range(4))
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert sum(pool.map(add, range(4))) == 4
        assert sorted(queue.find_one()["values"]) == list(range(4))
        capped = client.wire_membership.capped
        capped.insert_one({"_id": 1, "values": ["x" * 280000]})
        try:
            capped.update_one({}, {"$addToSet": {"values": "y" * 280000}})
        except OperationFailure:
            pass
        else:
            raise AssertionError("oversized post-image must fail before commit")
        assert capped.find_one()["values"] == ["x" * 280000]


def pop_rename_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_pop_rename.items
        collection.insert_many([{"_id": Int64(i), "old": Binary(b"value", 128), "target": None, "items": [Int64(1), Int64(2), Int64(3)], "keep": True} for i in range(24)])
        expression = {"$pop": {"items": -1}, "$rename": {"old": "target"}}
        result = collection.update_one({"_id": 0}, expression)
        assert (result.matched_count, result.modified_count) == (1, 1)
        result = collection.update_many({"old": {"$exists": True}}, expression)
        assert (result.matched_count, result.modified_count) == (23, 23)
        assert collection.update_many({}, {"$rename": {"absent": "target"}, "$pop": {"missing.x": 1}}).modified_count == 0
        assert collection.find_one_and_update({}, {"$pop": {"items": 1}}, sort=[("_id", -1)], projection={"items": 1, "_id": 0}) == {"items": [Int64(2), Int64(3)]}
        assert collection.find_one_and_update({"_id": 23}, {"$rename": {"target": "nested.value"}}, return_document=True, projection={"nested": 1, "_id": 0}) == {"nested": {"value": Binary(b"value", 128)}}
        before = BSON.encode(collection.find_one({"_id": 0}))
        for operator, changes, code in [("$pop", {"keep": 1}, 14), ("$pop", {"items": 0}, 9), ("$rename", {"target": "items.0"}, 2), ("$rename", {"target": "keep.x"}, 28), ("$rename", {"target": "_id"}, 66), ("$rename", {"target": "target"}, 2), ("$rename", {"target": "bad\x00name"}, 2)]:
            try:
                collection.update_one({"_id": 0}, {"$set": {"atomic_marker": True}, operator: changes})
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expected atomic pop/rename rejection")
            assert BSON.encode(collection.find_one({"_id": 0})) == before
        try:
            client.wire_pop_rename.command("update", "items", ordered=False, updates=[
                {"q": {}, "u": {"$pop": {"keep": 1}}, "multi": True},
                {"q": {}, "u": {"$set": {"should_not_run": True}}},
            ])
        except OperationFailure as error:
            assert error.code == 14 and "writeErrors" not in error.details
        else:
            raise AssertionError("runtime multi-update failures must abort the batch")
        assert collection.count_documents({"should_not_run": {"$exists": True}}) == 0
        queue = client.wire_pop_rename.queue
        queue.insert_many([{"_id": i, "items": list(range(4)), "keep": True} for i in range(12)])
        def consume(_):
            seen = []
            while True:
                image = queue.find_one_and_update({"items.0": {"$exists": True}}, {"$pop": {"items": -1}})
                if image is None:
                    return seen
                seen.append((image["_id"], image["items"][0]))
        with ThreadPoolExecutor(max_workers=4) as pool:
            seen = list(pool.map(consume, range(4)))
        assert sorted(pair for group in seen for pair in group) == [(i, item) for i in range(12) for item in range(4)]
        assert queue.count_documents({"items": [], "keep": True}) == 12


def min_max_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_min_max.items
        collection.insert_many([{"_id": Int64(i), "low": 5.0, "high": 5.0, "keep": True, "a": [None]} for i in range(24)])
        equal = collection.update_one({"_id": 0}, {"$min": {"low": Int64(5)}, "$max": {"high": Decimal128("5")}})
        assert (equal.matched_count, equal.modified_count) == (1, 0)
        assert type(collection.find_one({"_id": 0})["low"]) is float
        result = collection.update_many({}, {"$min": {"low": 4, "a.3": 2}, "$max": {"high": 6}})
        assert (result.matched_count, result.modified_count) == (24, 24)
        assert collection.find_one_and_update({}, {"$min": {"low": 3}}, sort=[("_id", -1)], projection={"low": 1, "_id": 0}) == {"low": 4}
        assert collection.find_one_and_update({"_id": 23}, {"$max": {"high": 7}}, return_document=True, projection={"high": 1, "_id": 0}) == {"high": 7}
        before = BSON.encode(collection.find_one({"_id": 0}))
        for expression, code in [({"$min": {"keep.x": 1}}, 28), ({"$max": {"_id": 99}}, 66), ({"$min": {"low": 1}, "$max": {"low": 2}}, 40), ({"$min": []}, 9)]:
            try:
                collection.update_one({"_id": 0}, expression)
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expected min/max validation error")
            assert BSON.encode(collection.find_one({"_id": 0})) == before
        reply = client.wire_min_max.command("update", "items", ordered=False, updates=[
            {"q": {"_id": 0}, "u": {"$min": {"keep.x": 1}}},
            {"q": {"_id": 0}, "u": {"$max": {"high": 8}}},
        ])
        assert (reply["n"], reply["nModified"]) == (1, 1)
        assert [(error["index"], error["code"]) for error in reply["writeErrors"]] == [(0, 28)]
        # Concurrent extrema combine current stored values, never stale client copies.
        def write(worker):
            for step in range(1, 9):
                value = worker * 8 + step
                result = collection.update_many({}, {"$min": {"low": -value}, "$max": {"high": value}})
                assert result.matched_count == 24
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(write, range(4)))
        rows = list(collection.find({}))
        assert [row["_id"] for row in rows] == list(range(24))
        assert all(type(row["_id"]) is Int64 and row["low"] == -32 and row["high"] == 32 and row["keep"] and row["a"] == [None, None, None, 2] for row in rows)


def find_update_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_find_update.items
        collection.insert_many([{"_id": Int64(i), "group": i % 2, "rank": i, "done": False} for i in range(12)])
        before = collection.find_one_and_update({"group": 0}, {"$set": {"rank": -1, "done": True, "stamp": Timestamp(0, 0)}}, sort=[("rank", -1)], projection={"rank": 1, "_id": 0})
        assert before == {"rank": 10}
        expected = {"_id": Int64(10), "group": 0, "rank": -1, "done": True, "stamp": Timestamp(0, 0)}
        assert BSON.encode(collection.find_one({"_id": 10})) == BSON.encode(expected)
        for after in [False, True]:
            assert BSON.encode(collection.find_one_and_update({"_id": 10.0}, {"$set": {"done": True}}, return_document=after)) == BSON.encode(expected)
            assert collection.find_one_and_update({"_id": 99}, {"$set": {}}, return_document=after) is None
        assert collection.find_one_and_update({"_id": 10}, {"$unset": {"group": 1}}, return_document=True, projection={"group": 1, "_id": 0}) == {}
        expected.pop("group")
        assert BSON.encode(collection.find_one({"_id": 10})) == BSON.encode(expected)
        reply = client.absent_find_update.command("findAndModify", "items", update={"$set": {}}, new=True)
        assert reply["value"] is None and reply["lastErrorObject"] == {"n": 0, "updatedExisting": False}
        assert "absent_find_update" not in client.list_database_names()
        reply = client.wire_find_update.command("findAndModify", "items", query={"_id": 10}, update={"$set": {}}, new=True, fields={"missing": 1, "_id": 0})
        assert reply["value"] == {} and reply["lastErrorObject"] == {"n": 1, "updatedExisting": True}
        for expression, code in [({"$set": {"rank.x": 1}}, 28), ({"$unset": {"_id": 1}}, 66), ({"$set": {"a": 1}, "$unset": {"a.x": 1}}, 40)]:
            try:
                collection.find_one_and_update({"_id": 10}, expression)
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expected atomic update rejection")
            assert BSON.encode(collection.find_one({"_id": 10})) == BSON.encode(expected)
        limited = client.wire_find_update.limited
        limited.insert_one({"_id": 1, "small": True})
        try:
            limited.find_one_and_update({}, {"$set": {"large": "x" * 522000}}, return_document=True)
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("return envelope must be preflighted before update")
        assert limited.find_one({}) == {"_id": 1, "small": True}
        assert limited.find_one_and_update({}, {"$set": {"large": "x" * 522000}}, return_document=True, projection=["_id"]) == {"_id": 1}
        try:
            limited.find_one_and_update({}, {"$unset": {"large": 1}})
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("oversized before-image cannot commit")
        assert limited.count_documents({"large": {"$type": "string"}}) == 1
        concurrent = client.wire_find_update.concurrent
        concurrent.insert_many([{"_id": i, "done": False} for i in range(24)])
        def consume(after):
            ids = []
            while True:
                image = concurrent.find_one_and_update({"done": False}, {"$set": {"done": True}}, return_document=after)
                if image is None:
                    return ids
                assert image["done"] is after
                ids.append(image["_id"])
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(consume, [False, True, False, True]))
        assert sorted(i for ids in results for i in ids) == list(range(24))


def update_many_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_update_many.items
        collection.insert_many([{"_id": Int64(i), "group": i % 2, "done": False, "array": [1, 2]} for i in range(24)])
        expression = {"$set": {"done": True, "value": Int64(1), "stamp": Timestamp(0, 0)}, "$unset": {"array.0": 1}}
        result = collection.update_many({"group": 0}, expression)
        assert (result.matched_count, result.modified_count, result.upserted_id) == (12, 12, None)
        result = collection.update_many({"group": 0}, expression)
        assert (result.matched_count, result.modified_count) == (12, 0)
        assert collection.update_many({"_id": 1.0}, expression).modified_count == 1
        assert collection.update_many({"_id": 99}, expression).matched_count == 0
        result = collection.update_many({}, expression)
        assert (result.matched_count, result.modified_count) == (24, 11)
        rows = list(collection.find({}))
        assert [row["_id"] for row in rows] == list(range(24))
        assert all(type(row["_id"]) is Int64 and type(row["value"]) is Int64 and row["array"] == [None, 2] and row["stamp"] == Timestamp(0, 0) for row in rows)
        # Eager syntax errors are still safe indexed failures; unordered batches
        # may continue when the failed statement never reached execution.
        reply = client.wire_update_many.command("update", "items", ordered=False, updates=[
            {"q": {}, "u": {"$set": {"a": 1, "a.b": 2}}, "multi": True},
            {"q": {}, "u": {"$set": {"validated": True}}, "multi": True},
        ])
        assert (reply["n"], reply["nModified"]) == (24, 24)
        assert reply["writeErrors"][0]["code"] == 40
        # Runtime errors may follow committed shards. They must abort even an
        # unordered batch, never claim a zero-write indexed statement failure.
        try:
            client.wire_update_many.command("update", "items", ordered=False, updates=[
                {"q": {}, "u": {"$set": {"group.x": 1}}, "multi": True},
                {"q": {}, "u": {"$set": {"must_not_run": True}}, "multi": True},
            ])
        except OperationFailure as error:
            assert error.code == 28 and "writeErrors" not in error.details and "nModified" not in error.details
        else:
            raise AssertionError("multi runtime failure must abort the command")
        assert collection.count_documents({"must_not_run": {"$exists": True}}) == 0
        assert client.absent_update_many.items.update_many({}, {"$set": {"x": 1}}).matched_count == 0
        assert "absent_update_many" not in client.list_database_names()
        concurrent = client.wire_update_many.concurrent
        concurrent.insert_many([{"_id": i, "done": False} for i in range(24)])
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: concurrent.update_many({"done": False}, {"$set": {"done": True}}), range(4)))
        assert sum(result.matched_count for result in results) == sum(result.modified_count for result in results) == 24
        assert concurrent.count_documents({"done": True}) == 24
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda i: concurrent.update_many({}, {"$set": {f"worker{i}": i}}), range(4)))
        assert all(result.matched_count == result.modified_count == 24 for result in results)
        assert all(all(row[f"worker{i}"] == i for i in range(4)) for row in concurrent.find({}))


def field_update_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_field_update.items
        original = {"_id": Int64(1), "keep": Binary(b"data", 128), "v": 1, "array": [1, {"x": 2}]}
        collection.insert_one(original)
        expression = {"$set": {"v": Int64(1), "nested.0.x": "$literal", "stamp": Timestamp(0, 0)}, "$unset": {"array.0": "ignored"}}
        result = collection.update_one({"keep": Binary(b"data", 128)}, expression)
        assert (result.matched_count, result.modified_count, result.upserted_id) == (1, 1, None)
        expected = {**original, "v": Int64(1), "array": [None, {"x": 2}], "nested": {"0": {"x": "$literal"}}, "stamp": Timestamp(0, 0)}
        assert BSON.encode(collection.find_one({})) == BSON.encode(expected)
        assert collection.update_one({"_id": 1.0}, expression).modified_count == 0
        assert collection.update_one({}, {"$set": {"_id": 1.0}}).modified_count == 0
        assert collection.update_one({"_id": 99}, {"$set": {}}).matched_count == 0
        for expression, code in [
            ({"$set": {"changed": True, "v.x": 1}}, 28),
            ({"$set": {"_id": 2}}, 66), ({"$unset": {"_id": 1}}, 66),
            ({"$set": {"a": 1}, "$unset": {"a.x": 1}}, 40),
            ({"$set": {"a..b": 1}}, 56), ({"$set": {"array.$.x": 1}}, 115),
            ({"$inc": {"v": 1}}, 115), ({"$set": 1}, 9),
            ({"$set": {"array.9999999999999999999999999": 1}}, 10334),
        ]:
            try:
                collection.update_one({}, expression)
            except OperationFailure as error:
                assert error.code == code, error
            else:
                raise AssertionError(("expected update rejection", code))
            assert BSON.encode(collection.find_one({})) == BSON.encode(expected)
        capped = client.wire_field_update.capped
        capped.insert_one({"_id": 1, "old": "x" * 270000})
        try:
            capped.update_one({}, {"$set": {"new": "x" * 270000}})
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("combined post-image must respect the wire document cap")
        assert "new" not in capped.find_one({})
        for ordered in [True, False]:
            batch = client.wire_field_update[f"batch_{ordered}"]
            batch.insert_many([{"_id": i, "keep": True, "v": 0} for i in range(3)])
            reply = client.wire_field_update.command("update", batch.name, ordered=ordered, updates=[
                {"q": {"_id": 0}, "u": {"$set": {"v": 1}}},
                {"q": {"_id": 1}, "u": {"$set": {"keep.x": 1}}},
                {"q": {"_id": 2}, "u": {"v": 2}},
            ])
            assert (reply["n"], reply["nModified"]) == ((1, 1) if ordered else (2, 2)), reply
            assert [(e["index"], e["code"]) for e in reply["writeErrors"]] == [(1, 28)]
            assert batch.find_one({"_id": 1}) == {"_id": 1, "keep": True, "v": 0}
        reply = client.absent_field_update.command("update", "items", updates=[{"q": {}, "u": {"$set": {"a": 1}, "$unset": {"a.x": 1}}}])
        assert reply["writeErrors"][0]["code"] == 40
        assert client.absent_field_update.items.update_one({}, {"$set": {"x": 1}}).matched_count == 0
        assert "absent_field_update" not in client.list_database_names()
        for options in [{"multi": "yes"}, {"upsert": True}, {"arrayFilters": []}]:
            reply = client.wire_field_update.command("update", "items", updates=[{"q": {}, "u": {"$set": {"x": 1}}, **options}])
            assert reply["writeErrors"][0]["code"] == 72
        # Leave room for the driver's monitor sockets within the listener's
        # deliberate eight-connection admission limit.
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda i: collection.update_one({"_id": 1}, {"$set": {f"field{i}": i}}), range(24)))
        assert all(r.modified_count == 1 for r in results)
        row = collection.find_one({})
        assert all(row[f"field{i}"] == i for i in range(24))
        assert row["keep"] == original["keep"]


def find_replace_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_find_replace.items
        collection.insert_many([{"_id": Int64(i), "group": i % 2, "rank": i} for i in range(12)])
        before = collection.find_one_and_replace({"group": 0}, {"value": Int64(9)},
            sort=[("rank", -1)], projection={"rank": 1, "_id": 0})
        assert before == {"rank": 10}
        assert collection.find_one({"_id": 10}) == {"_id": 10, "value": 9}
        replacement = {"_id": 10.0, "value": Int64(9)}
        for after in (False, True):
            value = collection.find_one_and_replace({"_id": 10}, replacement, return_document=after)
            assert BSON.encode(value) == BSON.encode({"_id": Int64(10), "value": Int64(9)})
            assert collection.find_one_and_replace({"_id": -1}, {}, return_document=after) is None
        value = collection.find_one_and_replace({"_id": 10}, {"value": "persisted", "hidden": True},
            projection={"value": 1, "_id": 0}, return_document=True)
        assert value == {"value": "persisted"} and collection.find_one({"_id": 10})["hidden"] is True
        assert client.absent_find_replace.items.find_one_and_replace({}, {}) is None
        assert "absent_find_replace" not in client.list_database_names()
        reply = client.absent_find_replace.command("findAndModify", "items", update={}, new=True)
        assert reply == {"ok": 1, "lastErrorObject": {"n": 0, "updatedExisting": False}, "value": None}
        reply = client.wire_find_replace.command("findAndModify", "items", query={"_id": 0}, update={},
            new=True, fields={"_id": 0})
        assert reply == {"ok": 1, "lastErrorObject": {"n": 1, "updatedExisting": True}, "value": {}}
        for options, code in [
            ({"update": {"$inc": {"value": 2}}}, 115), ({"update": [{"$set": {"value": 2}}]}, 115),
            ({"update": {}, "remove": True}, 72), ({"remove": True, "new": True}, 72),
            ({"remove": False}, 72), ({"update": {}, "upsert": True}, 72),
            ({"update": {}, "hint": "_id_"}, 72), ({"update": {}, "let": {}}, 72),
            ({"update": {}, "writeConcern": {"w": 0}}, 72),
            ({"update": {}, "query": {"$where": "private"}}, 115),
            ({"update": {}, "fields": {"a": 1, "b": 0}}, 31254),
        ]:
            try:
                client.absent_find_replace.command("findAndModify", "items", **options)
            except OperationFailure as error:
                # Projection uses the existing shared error taxonomy.
                assert error.code == code, (options, error.code)
            else:
                raise AssertionError("invalid replacement command accepted")
        try:
            collection.find_one_and_replace({"_id": 1}, {"_id": 99})
        except OperationFailure as error:
            assert error.code == 66
        else:
            raise AssertionError("immutable ID changed")
        assert collection.find_one({"_id": 1})["rank"] == 1
        bounded = client.wire_find_replace.bounded
        bounded.insert_one({"_id": 1, "value": "keep"})
        large = {"payload": "x" * 522000}
        try:
            bounded.find_one_and_replace({}, large, return_document=True)
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("return-envelope budget must be checked before writing")
        assert bounded.find_one({}) == {"_id": 1, "value": "keep"}
        assert bounded.find_one_and_replace({}, large, return_document=True, projection=["_id"]) == {"_id": 1}
        # Large post-image remains stored despite the small returned projection.
        assert bounded.count_documents({"payload": {"$type": "string"}}) == 1
        parallel = client.wire_find_replace.parallel
        original = {"_id": 1, "a": [], "b": []}
        parallel.insert_one(original)
        for query in ({}, {"_id": 1}):
            for after in (False, True):
                try:
                    parallel.find_one_and_replace(query, {}, sort=[("a", 1), ("b", 1)], return_document=after)
                except OperationFailure as error:
                    assert error.code == 2
                else:
                    raise AssertionError("invalid runtime sort must not replace")
                assert parallel.find_one({}) == original
        concurrent = client.wire_find_replace.concurrent
        concurrent.insert_many([{"_id": i, "done": False} for i in range(24)])
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda after: concurrent.find_one_and_replace({"done": False}, {"done": True},
                sort=[("_id", 1)], return_document=after), [False, True] * 12))
        assert sorted(row["_id"] for row in values) == list(range(24))
        assert [row["done"] for row in values] == [False, True] * 12


def replacement_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_replacement.items
        collection.insert_many([{"_id": Int64(i), "group": i % 2, "obsolete": True} for i in range(12)])
        replacement = {"value": 7, "_id": 1.0}
        result = collection.replace_one({"group": 1}, replacement)
        assert (result.matched_count, result.modified_count, result.upserted_id) == (1, 1, None)
        assert replacement == {"value": 7, "_id": 1.0}
        row = collection.find_one({"_id": 1})
        assert row == {"_id": 1, "value": 7} and list(row) == ["_id", "value"]
        assert isinstance(row["_id"], Int64)
        assert [row["_id"] for row in collection.find()] == list(range(12))
        for value in (Int64(7), 7.0, Decimal128("7.00"), 7):
            assert collection.replace_one({"_id": 1}, {"value": value}).modified_count == 1
            assert collection.replace_one({"_id": 1}, {"value": value}).modified_count == 0
        assert collection.replace_one({"_id": -1}, {}).matched_count == 0
        assert client.absent_replacement.items.replace_one({}, {}).matched_count == 0
        assert "absent_replacement" not in client.list_database_names()
        for ordered in (True, False):
            batch = client.wire_replacement[f"batch_{ordered}"]
            batch.insert_many([{"_id": i, "v": 0} for i in range(3)])
            try:
                batch.bulk_write([pymongo.ReplaceOne({"_id": 0}, {"v": 1}),
                    pymongo.ReplaceOne({"_id": 1}, {"_id": 10, "v": 1}),
                    pymongo.ReplaceOne({"_id": 2}, {"v": 1})], ordered=ordered)
            except BulkWriteError as error:
                assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == [(1, 66)]
                assert error.details["nMatched"] == error.details["nModified"] == (1 if ordered else 2)
            else:
                raise AssertionError("conflicting replacement ID must fail")
            assert batch.find_one({"_id": 1}) == {"_id": 1, "v": 0}
            assert batch.find_one({"_id": 2})["v"] == (0 if ordered else 1)
        for statement, code in [
            ({"q": {}, "u": {"$inc": {"v": 1}}}, 115),
            ({"q": {}, "u": [{"$set": {"v": 1}}]}, 115),
            ({"q": {}, "u": {}, "multi": True}, 72),
            ({"q": {}, "u": {}, "upsert": True}, 72),
            ({"q": {}, "u": {}, "hint": "_id_"}, 72),
            ({"q": {}, "u": {}, "sort": {"_id": 1}}, 72),
            ({"q": {}, "u": {"value": 1, "$inc": {"n": 1}}}, 52),
            ({"q": {"$where": "private"}, "u": {}}, 115),
        ]:
            reply = client.absent_replacement.command("update", "items", updates=[statement])
            assert reply["ok"] == 1 and reply["n"] == reply["nModified"] == 0
            assert reply["writeErrors"][0]["code"] == code, reply
        assert "absent_replacement" not in client.list_database_names()
        zero = Timestamp(0, 0)
        collection.replace_one({"_id": 1}, {"a": zero, "b": zero, "nested": {"v": zero}})
        row = collection.find_one({"_id": 1})
        assert row["a"] != row["b"] and row["a"] != zero and row["nested"]["v"] == zero
        # Body-array update statements, distinct from PyMongo's OP_MSG sequence.
        reply = client.wire_replacement.command("update", "items", updates=[{"q": {"_id": 1}, "u": {"value": "persisted"}}])
        assert reply == {"ok": 1, "n": 1, "nModified": 1}
        # An old ID can make the normalized post-image too large despite a valid input.
        large = client.wire_replacement.large_id
        key = "k" * 270000
        large.insert_one({"_id": key, "v": "keep"})
        try:
            large.replace_one({}, {"payload": "x" * 270000})
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("post-image must obey advertised BSON limit")
        assert large.find_one({}) == {"_id": key, "v": "keep"}
        concurrent = client.wire_replacement.concurrent
        concurrent.insert_many([{"_id": i, "done": False} for i in range(24)])
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(lambda _: concurrent.replace_one({"done": False}, {"done": True}).modified_count, range(24)))
        assert outcomes == [1] * 24 and concurrent.count_documents({"done": True}) == 24
        from pymongo.write_concern import WriteConcern
        unack = collection.with_options(write_concern=WriteConcern(w=0))
        assert not unack.replace_one({"_id": 2}, {"unack": True}).acknowledged
        client.admin.command("ping")
        assert collection.find_one({"_id": 2})["unack"] is True


def find_delete_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_find_delete.items
        collection.insert_many([{"_id": Int64(i), "group": i % 2, "nested": {"v": i}} for i in reversed(range(12))])
        assert collection.find_one_and_delete({"group": 0}, sort=[("_id", 1)], projection={"nested.v": 1, "_id": 0}) == {"nested": {"v": 0}}
        assert collection.find_one({"_id": 0}) is None
        row = collection.find_one_and_delete({"_id": 11.0})
        assert isinstance(row["_id"], Int64)
        assert collection.find_one_and_delete({"_id": -1}) is None
        reply = client.wire_find_delete.command("findAndModify", "items", query={"_id": -1}, remove=True)
        assert reply == {"ok": 1, "lastErrorObject": {"n": 0}, "value": None}
        assert client.absent_find_delete.items.find_one_and_delete({}) is None
        assert "absent_find_delete" not in client.list_database_names()
        for options in ({"query": {"$where": "private"}}, {"fields": {"group": 1, "nested": 0}}, {"sort": {"_id": 0}}, {"upsert": True}, {"hint": "_id_"}, {"writeConcern": {"w": 0}}):
            try:
                client.absent_find_delete.command("findAndModify", "items", remove=True, **options)
            except OperationFailure:
                pass
            else:
                raise AssertionError("findAndModify must eagerly reject invalid/unsupported options")
        assert "absent_find_delete" not in client.list_database_names()
        assert collection.find_one_and_delete({}, sort=[("group", 1)])["_id"] == 10
        assert collection.count_documents({}) == 9
        concurrent = client.wire_find_delete.concurrent
        concurrent.insert_many([{"_id": i} for i in range(24)])
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _: concurrent.find_one_and_delete({}, sort=[("_id", 1)]), range(24)))
        assert sorted(row["_id"] for row in values) == list(range(24))
        assert concurrent.find_one_and_delete({}) is None
        empty_projection = client.wire_find_delete.empty_projection
        empty_projection.insert_one({"_id": 1})
        reply = client.wire_find_delete.command("findAndModify", "empty_projection", remove=True, fields={"_id": 0})
        assert reply == {"ok": 1, "lastErrorObject": {"n": 1}, "value": {}}
        assert empty_projection.find_one_and_delete({}) is None
        parallel = client.wire_find_delete.parallel
        parallel.insert_one({"_id": 1, "a": [], "b": []})
        for query in ({"_id": 1}, {}):
            try:
                parallel.find_one_and_delete(query, sort=[("a", 1), ("b", 1)])
            except OperationFailure as error:
                assert error.code == 2
            else:
                raise AssertionError("runtime sort validation must precede deletion, including point routes")
        assert parallel.find_one({"_id": 1}) is not None


def delete_smoke(uri):
    from pymongo import DeleteMany, DeleteOne
    from pymongo.write_concern import WriteConcern
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_deletes.items
        collection.insert_many([{"_id": i, "group": i % 2, "nested": {"label": "remove"}} for i in reversed(range(40))])
        assert collection.delete_one({"group": 0}).deleted_count == 1
        assert collection.find_one({"_id": 38}) is None
        assert collection.delete_many({"group": 0, "nested.label": {"$regex": "^rem"}}).deleted_count == 19
        assert collection.delete_many({"_id": 39.0}).deleted_count == 1
        assert collection.delete_one({}).deleted_count == 1
        assert collection.delete_many({"absent": {"$exists": True}}).deleted_count == 0
        assert client.absent_delete.items.delete_many({}).deleted_count == 0
        assert "absent_delete" not in client.list_database_names()
        for ordered in (True, False):
            batch = client.wire_deletes[f"ordered_{ordered}"]
            batch.insert_many([{"_id": i} for i in range(4)])
            try:
                batch.bulk_write([DeleteOne({"_id": 0}), DeleteMany({"$where": "private"}), DeleteMany({"_id": {"$gt": 1}})], ordered=ordered)
            except BulkWriteError as error:
                assert error.details["nRemoved"] == (1 if ordered else 3)
                assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == [(1, 115)]
                assert "private" not in error.details["writeErrors"][0]["errmsg"]
            else:
                raise AssertionError("invalid selector must be an indexed write error")
            assert batch.count_documents({}) == (3 if ordered else 1)
        for selector, code in [({"q": {}, "limit": 2}, 2), ({"q": {}, "limit": 0, "hint": "_id_"}, 72), ({"q": {"$where": "private"}, "limit": 0}, 115)]:
            reply = client.absent_delete.command("delete", "items", deletes=[selector])
            assert reply["n"] == 0 and reply["writeErrors"][0]["code"] == code
        assert collection.count_documents({}) == 18
        # Exercise OP_MSG moreToCome on the same pooled connection; ping is a
        # processing barrier, not a durability/replication claim.
        unack = collection.with_options(write_concern=WriteConcern(w=0))
        assert not unack.delete_many({"_id": {"$gt": 30}}).acknowledged
        client.admin.command("ping")
        assert collection.count_documents({}) == 15


def check_hello(reply):
    assert reply["isWritablePrimary"] is True
    assert reply["maxWireVersion"] == 8
    assert reply["compression"] == []
    assert "logicalSessionTimeoutMinutes" not in reply
    assert "setName" not in reply


def sync_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        assert client.admin.command("ping")["ok"] == 1
        check_hello(client.admin.command("hello"))
        assert client.server_info()["version"].endswith("-briskdb")
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert all(pool.map(lambda _: client.admin.command("ping")["ok"] == 1, range(12)))
        try:
            client.example.command("unsupportedCommand", "items")
        except OperationFailure as error:
            assert error.code == 59
        else:
            raise AssertionError("unimplemented data commands must fail explicitly")
    # A new client proves reconnect after a pool is closed.
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.admin.command("ping")["ok"] == 1
    # Offering compression must not cause the driver to compress when the server
    # negotiates none. zlib is available without optional codec dependencies.
    with pymongo.MongoClient(uri, compressors="zlib", serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.admin.command("ping")["ok"] == 1


def document_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        collection = client.wire_data.items
        assert client.never_written.items.find_one({"_id": "missing"}) is None
        document = {
            "_id": "typed", "integer": Int64(42), "decimal": Decimal128("1.250"),
            "binary": Binary(b"\x00\xff", 128), "when": datetime(2026, 1, 2, 3, 4, 5),
            "stamp": Timestamp(123, 7), "nested": {"values": [None, True, "hello"]},
        }
        assert collection.insert_one(document).inserted_id == "typed"
        actual = collection.find_one({"_id": "typed"})
        assert actual == document
        assert isinstance(actual["integer"], Int64)
        generated = {"value": "generated by PyMongo"}
        result = collection.insert_one(generated)
        assert isinstance(result.inserted_id, ObjectId)
        assert collection.find_one({"_id": result.inserted_id}) == generated
        for identifier in [None, Int64(42), Binary(b"id", 128), {"compound": "id"}]:
            collection.insert_one({"_id": identifier, "value": "typed id"})
            assert collection.find_one({"_id": identifier})["value"] == "typed id"
        try:
            collection.insert_one({"_id": 42.0, "secret": "must-not-leak"})
        except DuplicateKeyError as error:
            assert error.code == 11000
            assert "must-not-leak" not in str(error)
        else:
            raise AssertionError("BSON-equal IDs must raise DuplicateKeyError")
        # Existing engine routing remains authoritative across pooled sockets.
        def round_trip(number):
            item = {"_id": f"parallel-{number}", "number": number}
            collection.insert_one(item)
            return collection.find_one({"_id": item["_id"]}) == item
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert all(pool.map(round_trip, range(12)))
        boundary = {"_id": "near-limit", "value": "x" * (512 * 1024 - 100)}
        collection.insert_one(boundary)
        assert collection.find_one({"_id": "near-limit"}) == boundary
        assert client.other_database.items.find_one({"_id": "typed"}) is None
        for arguments, code in [
            ({"filter": {"$where": "unsupported"}}, 115),
            ({"filter": {"_id": "typed"}, "projection": {"integer": {"$slice": 1}}}, 115),
            ({"filter": {"_id": "typed"}, "lsid": {"id": Binary(b"0" * 16, 4)}}, 72),
        ]:
            try:
                client.wire_data.command("find", "items", **arguments)
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("unsupported options must fail explicitly")
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.wire_data.items.find_one({"_id": "typed"})["decimal"] == Decimal128("1.250")


def batch_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000, maxPoolSize=3) as client:
        for ordered in [True, False]:
            collection = client.wire_batches[f"ordered_{ordered}"]
            documents = [{"_id": identifier, "position": index} for index, identifier in enumerate([1, 1.0, 2, 2.0, 3])]
            try:
                collection.insert_many(documents, ordered=ordered)
            except BulkWriteError as error:
                assert error.details["nInserted"] == (1 if ordered else 3)
                assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == (
                    [(1, 11000)] if ordered else [(1, 11000), (3, 11000)]
                )
                assert error.details["writeConcernErrors"] == []
            else:
                raise AssertionError("duplicates must report batch indices and partial success")
            assert collection.find_one({"_id": 1}) == documents[0]
            assert (collection.find_one({"_id": 3}) is None) == ordered
        generated = [{"value": "generated"}, {"_id": None}, {"value": "generated again"}]
        result = client.wire_batches.generated.insert_many(generated)
        assert result.inserted_ids == [item["_id"] for item in generated]
        assert result.inserted_ids[1] is None
        assert all(isinstance(result.inserted_ids[index], ObjectId) for index in [0, 2])
        for item in generated:
            assert client.wire_batches.generated.find_one({"_id": item["_id"]}) == item
        # The driver's advertised maxWriteBatchSize split must preserve results.
        documents = [{"_id": number} for number in range(1001)]
        result = client.wire_batches.split.insert_many(documents)
        assert result.inserted_ids == list(range(1001))
        for number in [0, 999, 1000]:
            assert client.wire_batches.split.find_one({"_id": number}) == {"_id": number}
        # Contending connections must not admit BSON-equal duplicate IDs.
        collection = client.wire_batches.concurrent
        collection.insert_one({"_id": "seed"})
        def insert_contended(_):
            try:
                return len(collection.insert_many([{"_id": number} for number in range(20)], ordered=False).inserted_ids)
            except BulkWriteError as error:
                assert all(item["code"] == 11000 for item in error.details["writeErrors"])
                return error.details["nInserted"]
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert sum(pool.map(insert_contended, range(3))) == 20
        # Only direct zero timestamps are server-stamped, never nested values or IDs.
        zero = Timestamp(0, 0)
        documents = [
            {"_id": "first", "stamp": zero, "second": zero, "nested": {"stamp": zero}, "array": [zero]},
            {"_id": "second", "stamp": zero, "nonzero": Timestamp(0, 1)},
            {"_id": zero, "stamp": zero},
        ]
        client.wire_batches.timestamps.insert_many(documents)
        stamps = []
        for document in documents:
            actual = client.wire_batches.timestamps.find_one({"_id": document["_id"]})
            assert actual["stamp"].time > 0
            stamps.append(actual["stamp"])
            assert document["stamp"] == zero
        actual = client.wire_batches.timestamps.find_one({"_id": "first"})
        assert actual["nested"]["stamp"] == actual["array"][0] == zero
        assert actual["second"] != actual["stamp"]
        assert len(set(stamps)) == len(stamps)
        assert client.wire_batches.timestamps.find_one({"_id": "second"})["nonzero"] == Timestamp(0, 1)


def query_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        collection = client.wire_queries.items
        collection.insert_many([
            {"_id": "missing"},
            {"_id": "null", "v": None},
            {"_id": "array", "v": [2, 7, 12], "items": [{"score": 1}, {"score": 5}]},
            {"_id": "number", "v": 7.0, "items": [{"other": 1}, {"score": 2}]},
            {"_id": "string", "v": "Abxc"},
            {"_id": "nested", "v": [[1, 2]], "items": [[{"score": 1}]]},
        ])
        cases = [
            ({"v": None}, ["missing", "null"]),
            ({"v": {"$ne": None}}, ["array", "nested", "number", "string"]),
            ({"v": {"$elemMatch": {"$gte": 5, "$lt": 10}}}, ["array"]),
            ({"v": {"$in": [7]}}, ["array", "number"]),
            ({"v": {"$type": "array"}}, ["array", "nested"]),
            ({"v": {"$regex": "ab.c", "$options": "i"}}, ["string"]),
            ({"items.score": {"$gt": 1}}, ["array", "number"]),
            ({"v.0": [1, 2]}, ["nested"]),
            ({"$or": [{"v": 7}, {"v": "Abxc"}]}, ["array", "number", "string"]),
            ({"$nor": [{"v": {"$exists": True}}]}, ["missing"]),
            ({"_id": {"$eq": "number"}}, ["number"]),
        ]
        for query, expected in cases:
            assert sorted(item["_id"] for item in collection.find(query)) == expected, query
        assert len(list(collection.find())) == 6
        assert [item["_id"] for item in collection.find({"v": {"$ne": None}}).skip(1).limit(2)] == ["number", "string"]
        assert collection.find_one({"v": {"$type": "number"}})["_id"] == "array"
        result = client.wire_queries.command("find", "items", filter={}, batchSize=2, singleBatch=True)
        assert result["cursor"]["id"] == 0
        assert [item["_id"] for item in result["cursor"]["firstBatch"]] == ["missing", "null"]
        for database in [client.wire_queries, client.invalid_query_must_not_create]:
            for query, code in [
                ({"v": {"$not": {}}}, 2),
                ({"$or": [{}, {"v": {"$size": "bad"}}]}, 2),
                ({"v": {"$type": True}}, 14),
                ({"v": {"$type": []}}, 9),
                ({"v": {"$regex": "["}}, 51091),
                ({"v": {"$regex": "x", "$options": "q"}}, 51108),
                ({"v": {"$regex": Regex("x", "i"), "$options": "i"}}, 51075),
                ({"$where": "not executed"}, 115),
            ]:
                try:
                    database.command("find", "items", filter=query)
                except OperationFailure as error:
                    assert error.code == code, (query, error)
                else:
                    raise AssertionError("all branches must validate, even for an absent collection")
        assert client.admin.command("ping")["ok"] == 1


def cursor_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, maxPoolSize=3) as client:
        collection = client.wire_batches.split
        assert [row["_id"] for row in collection.find().batch_size(37)] == list(range(1001))
        assert [row["_id"] for row in collection.find({"_id": {"$gte": 17}}).skip(5).limit(123).batch_size(7)] == list(range(22, 145))
        first = client.wire_batches.command("find", "split", filter={}, batchSize=0)
        identifier = first["cursor"]["id"]
        assert identifier > 0 and first["cursor"]["firstBatch"] == []
        try:
            client.wire_batches.command("getMore", identifier, collection="wrong", batchSize=2)
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("foreign namespaces must not access a cursor")
        next_batch = client.wire_batches.command("getMore", identifier, collection="split", batchSize=3)
        assert next_batch["cursor"]["id"] == identifier
        assert [row["_id"] for row in next_batch["cursor"]["nextBatch"]] == [0, 1, 2]
        killed = client.wire_batches.command("killCursors", "split", cursors=[identifier])
        assert killed["cursorsKilled"] == [identifier]
        assert killed["cursorsAlive"] == killed["cursorsUnknown"] == []
        missing = client.wire_batches.command("killCursors", "split", cursors=[identifier])
        assert missing["cursorsNotFound"] == [identifier]
        cursor = collection.find(batch_size=2)
        assert next(cursor)["_id"] == 0
        identifier = cursor.cursor_id
        cursor.close()
        try:
            client.wire_batches.command("getMore", identifier, collection="split")
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("closing a PyMongo cursor must release its server cursor")
        def consume(start):
            return [row["_id"] for row in collection.find({"_id": {"$gte": start}}).limit(30).batch_size(3)]
        with ThreadPoolExecutor(max_workers=3) as pool:
            assert list(pool.map(consume, [0, 100, 200])) == [list(range(start, start + 30)) for start in [0, 100, 200]]
        large = client.wire_cursors.large
        large.insert_many([{"_id": index, "payload": "x" * 60_000} for index in range(30)])
        assert [row["_id"] for row in large.find(batch_size=1000)] == list(range(30))
        single = client.wire_cursors.command("find", "large", filter={}, batchSize=1000, singleBatch=True)
        assert single["cursor"]["id"] == 0
        assert 0 < len(single["cursor"]["firstBatch"]) < 30
        empty = client.wire_cursors.command("find", "large", batchSize=0, singleBatch=True)
        assert empty["cursor"]["id"] == 0 and empty["cursor"]["firstBatch"] == []


def projection_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        collection = client.wire_projection.items
        documents = [
            {"before": Int64(index), "_id": index, "secret": "filter-me", "profile": {"name": f"name-{index}", "age": 30},
             "items": [{"sku": "a", "qty": 1}, {"qty": 2}, {}, [None, {"sku": "b"}]], "payload": "x" * 5000}
            for index in range(24)
        ]
        collection.insert_many(documents)
        spec = {"profile.name": 1, "items.sku": 1, "before": 1, "_id": 0}
        rows = list(collection.find({"secret": "filter-me"}, spec).skip(2).limit(13).batch_size(3))
        assert rows == [
            {"before": Int64(index), "profile": {"name": f"name-{index}"}, "items": [{"sku": "a"}, {}, {}, [{"sku": "b"}]]}
            for index in range(2, 15)
        ]
        assert all(list(row) == ["before", "profile", "items"] and isinstance(row["before"], Int64) for row in rows)
        point = collection.find_one({"_id": 7}, {"items.qty": 0, "payload": 0, "secret": 0})
        assert point["items"] == [{"sku": "a"}, {}, {}, [None, {"sku": "b"}]]
        assert "payload" not in point and "secret" not in point
        assert collection.find_one({"_id": 7}) == documents[7]
        assert collection.find_one({"_id": 7}, ["before"]) == {"before": Int64(7), "_id": 7}
        assert collection.find_one({"_id": 7}, {}) == documents[7]
        assert collection.find_one({"_id": 7}, {"_id.missing": 1}) == {}
        for spec, code in [
            ({"before": 1, "secret": 0}, 31254),
            ({"secret": 0, "before": 1}, 31253),
            ({"profile": 1, "profile.name": 1}, 31249),
            ({"profile.name": 1, "profile": 1}, 31250),
            ({"items.0": 1}, 115),
            ({"items": {"$slice": 1}}, 115),
        ]:
            for target in [collection, client.unwritten_projection.items]:
                try:
                    list(target.find({}, spec))
                except OperationFailure as error:
                    assert error.code == code, (spec, error.code)
                else:
                    raise AssertionError("invalid projection must fail before storage admission")


def sorting_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=10000, maxPoolSize=3) as client:
        collection = client.wire_sorting.items
        documents = [{"_id": index, "rank": Int64(index % 5), "group": index % 2, "payload": "x" * 1000}
                     for index in range(36)]
        collection.insert_many(documents)
        expected = sorted((row for row in documents if row["group"] == 0), key=lambda row: -row["rank"])[2:15]
        rows = list(collection.find({"group": 0}, {"_id": 1}).sort("rank", -1).skip(2).limit(13).batch_size(3))
        assert rows == [{"_id": row["_id"]} for row in expected]
        assert collection.find_one({}, {"_id": 1}, sort=[("rank", -1), ("_id", -1)]) == {"_id": 34}
        assert collection.find_one({"_id": 7}, sort=[("rank", 1)]) == documents[7]
        assert [row["_id"] for row in collection.find().sort("_id", -1).limit(-4)] == [35, 34, 33, 32]
        initial = client.wire_sorting.command("find", "items", sort={"_id": -1}, batchSize=0)
        identifier = initial["cursor"]["id"]
        assert identifier and not initial["cursor"]["firstBatch"]
        reply = client.wire_sorting.command("getMore", identifier, collection="items", batchSize=2)
        assert [row["_id"] for row in reply["cursor"]["nextBatch"]] == [35, 34]
        client.wire_sorting.command("killCursors", "items", cursors=[identifier])
        assert [row["_id"] for row in client.wire_cursors.large.find().sort("_id", -1).batch_size(1000)] == list(reversed(range(30)))
        compound = client.wire_sorting.compound
        compound.insert_many([
            {"_id": 1, "items": [{"x": 1, "y": 9}, {"x": 2, "y": 8}]},
            {"_id": 2, "items": [{"x": 1, "y": 5}]},
        ])
        assert [row["_id"] for row in compound.find().sort([("items.x", 1), ("items.y", 1)]).batch_size(1)] == [2, 1]
        parallel = client.wire_sorting.parallel
        parallel.insert_one({"_id": 1, "a": [], "b": []})
        try:
            list(parallel.find({"_id": 1}).sort([("a", 1), ("b", 1)]).skip(1))
        except OperationFailure as error:
            assert error.code == 2
        else:
            raise AssertionError("point sort must validate parallel array keys before skip")
        for spec, code in [({"rank": 0}, 15975), ({"rank": True}, 15974), ({"": 1}, 40352),
                           ({"rank": {"$meta": "textScore"}}, 115)]:
            for database in [client.wire_sorting, client.unwritten_sorting]:
                try:
                    database.command("find", "items", sort=spec)
                except OperationFailure as error:
                    assert error.code == code, (spec, error.code)
                else:
                    raise AssertionError("invalid sort must fail before missing-collection handling")


def distinct_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_distinct.items
        assert collection.distinct("v") == []
        documents = [
            {"_id": 0, "v": [Int64(1), True, None, [2, 3]], "nested": {"a": ["first", "second"]}},
            {"_id": 1, "v": [1.0, False, [2.0, Int64(3)]], "nested": [{"a": "no-fanout"}]},
            {"_id": 2, "v": Decimal128("1.00"), "": "empty-key", "payload": "x" * 100000},
            {"_id": 3},
        ]
        collection.insert_many(documents)
        expected = [Int64(1), True, None, [2, 3], False]
        assert BSON.encode({"v": collection.distinct("v", maxTimeMS=10000)}) == BSON.encode({"v": expected})
        assert collection.distinct("nested.a") == ["first", "second"]
        assert collection.distinct("") == ["empty-key"]
        assert collection.distinct("v.0") == []
        assert collection.distinct("v\x00") == []
        assert isinstance(collection.distinct("v", {"_id": 2})[0], Decimal128)
        assert client.unwritten_distinct.items.distinct("v") == []
        for arguments, code in [
            ({"key": 7}, 14), ({"key": Code("v")}, 14), ({"key": "v", "query": []}, 14),
            ({"key": "v", "query": {"$where": "private-data"}}, 115),
            ({"key": "v", "hint": "_id_"}, 72), ({"key": "v", "collation": {"locale": "en"}}, 72),
            ({"key": "v", "skip": 1}, 72), ({"key": "v", "comment": "private-data"}, 72),
        ]:
            for database in [client.wire_distinct, client.unwritten_distinct]:
                try:
                    database.command("distinct", "items", **arguments)
                except OperationFailure as error:
                    assert error.code == code, (arguments, error.code)
                    assert "private-data" not in str(error)
                else:
                    raise AssertionError("distinct options must fail before missing-collection handling")
        # Global encounter order and first representation must survive internal
        # paging, not depend on the physical shard that happens to be read first.
        client.wire_distinct.ordered.insert_many([
            {"_id": index, "v": Int64(index) if index < 17 else float(index % 17)} for index in range(160)
        ])
        values = client.wire_distinct.ordered.distinct("v")
        assert values == list(range(17)) and all(isinstance(value, Int64) for value in values)
        # A result over the bootstrap byte budget fails wholly, then the socket
        # and session remain usable. No partial values array is returned.
        client.wire_distinct.too_large.insert_many([{"_id": i, "v": str(i) + "x" * 300000} for i in range(4)])
        try:
            client.wire_distinct.too_large.distinct("v")
        except OperationFailure as error:
            assert error.code == 10334
            assert "values" not in error.details
        else:
            raise AssertionError("oversized distinct must fail wholly")
        assert collection.distinct("nested.a") == ["first", "second"]
        assert list(collection.find().sort("_id", 1)) == documents


def count_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        collection = client.wire_count.items
        assert collection.estimated_document_count() == 0
        collection.insert_many([{"_id": index, "group": index % 3} for index in range(37)])
        assert collection.estimated_document_count(maxTimeMS=10000) == 37
        for arguments, expected in [
            ({}, 37), ({"limit": 0, "skip": 2}, 35),
            ({"query": {"group": 1}, "skip": 3, "limit": 5}, 5),
            ({"query": {"group": 1}, "skip": 3}, 9),
            ({"query": {"_id": {"$eq": Int64(7)}}}, 1),
            ({"query": {"_id": 7}, "skip": 1}, 0),
            ({"skip": Int64(2**63 - 1)}, 0),
        ]:
            assert client.wire_count.command("count", "items", **arguments)["n"] == expected
        assert client.unwritten_count.items.estimated_document_count() == 0
        for arguments, code in [
            ({"query": {"$where": "private-data"}}, 115),
            ({"query": []}, 72), ({"skip": -1}, 2), ({"limit": -1}, 2),
            ({"skip": True}, 2), ({"limit": 1.5}, 2), ({"maxTimeMS": -1}, 2),
            ({"hint": "_id_"}, 72), ({"readConcern": {"level": "majority"}}, 72),
            ({"collation": {"locale": "en"}}, 72), ({"comment": "private-data"}, 72),
            ({"sort": {"_id": 1}}, 72), ({"batchSize": 1}, 72),
        ]:
            for database in [client.wire_count, client.unwritten_count]:
                try:
                    database.command("count", "items", **arguments)
                except OperationFailure as error:
                    assert error.code == code, (arguments, error.code)
                    assert "private-data" not in str(error)
                else:
                    raise AssertionError("invalid count options must fail before existence handling")
        # Exercise the driver's actual match/skip/limit/literal-group pipeline.
        for query, options, expected in [
            ({}, {}, 37), ({"group": 1}, {}, 12),
            ({"group": 1}, {"skip": 3, "limit": 5}, 5),
            ({"group": 1}, {"skip": 3}, 9), ({}, {"skip": Int64(2**63 - 1)}, 0),
            ({"$and": [{"group": 1}, {"_id": {"$gt": 10}}]}, {}, 8),
            ({"_id": Int64(7)}, {}, 1), ({"_id": -1}, {}, 0),
        ]:
            assert collection.count_documents(query, maxTimeMS=10000, **options) == expected
            assert client.unwritten_count.items.count_documents(query, **options) == 0
        for options, code in [({"limit": 0}, 15958), ({"skip": -1}, 5107200),
                              ({"hint": "_id_"}, 72), ({"comment": "private-data"}, 72)]:
            for target in [collection, client.unwritten_count.items]:
                try:
                    target.count_documents({}, **options)
                except OperationFailure as error:
                    assert error.code == code, (options, error.code)
                    assert "private-data" not in str(error)
                else:
                    raise AssertionError("invalid count_documents options must fail eagerly")


def aggregation_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000, maxPoolSize=3) as client:
        collection = client.wire_aggregate.items
        documents = [{"_id": Int64(index), "group": index % 3, "value": Int64((179 - index) % 7)} for index in range(180)]
        collection.insert_many(documents)
        assert list(client.unwritten_aggregate.items.aggregate([])) == []
        assert list(client.unwritten_aggregate.items.aggregate([{"$count": "n"}])) == []
        assert list(collection.aggregate([], batchSize=7)) == documents
        selected = [row for row in reversed(documents) if row["group"] == 1][3:33]
        expected = sorted(selected, key=lambda row: row["value"])
        stages = [{"$sort": {"_id": -1}}, {"$match": {"group": 1}}, {"$skip": 3}, {"$limit": 30}, {"$sort": {"value": 1}}]
        actual = list(collection.aggregate(stages, batchSize=4, maxTimeMS=15000, allowDiskUse=False))
        assert BSON.encode({"rows": actual}) == BSON.encode({"rows": expected})
        assert list(collection.aggregate([{"$skip": 7}, {"$limit": 30}, {"$match": {"group": 1}}, {"$skip": 1}, {"$limit": 6}], batchSize=1)) == [row for row in documents[7:37] if row["group"] == 1][1:7]
        assert list(collection.aggregate([{"$match": {"group": 1}}, {"$skip": Decimal128("3.0")}, {"$limit": 7.0}, {"$count": "n"}])) == [{"n": 7}]
        assert list(collection.aggregate([{"$count": "n"}, {"$count": "again"}])) == [{"again": 1}]
        grouped = [{"$sort": {"_id": -1}}, {"$group": {
            "_id": "$group", "n": {"$sum": 1}, "sum": {"$sum": "$value"}, "avg": {"$avg": "$value"},
            "first": {"$first": "$_id"}, "last": {"$last": "$_id"}, "ids": {"$push": "$_id"},
            "values": {"$addToSet": "$value"}, "min": {"$min": "$value"}, "max": {"$max": "$value"},
        }}]
        grouped_expected = []
        for key in (2, 1, 0):
            source = [row for row in reversed(documents) if row["group"] == key]
            total = sum(row["value"] for row in source)
            grouped_expected.append({"_id": key, "n": 60, "sum": total, "avg": total / 60,
                                     "first": source[0]["_id"], "last": source[-1]["_id"],
                                     "ids": [row["_id"] for row in source], "values": list(dict.fromkeys(row["value"] for row in source)),
                                     "min": Int64(0), "max": Int64(6)})
        assert BSON.encode({"rows": list(collection.aggregate(grouped, batchSize=1))}) == BSON.encode({"rows": grouped_expected})
        for key, expected_key in [({"team": "$group", "missing": "$absent"}, lambda n: {"team": n}),
                                  (["$group", "$absent"], lambda n: [n, None])]:
            result = list(collection.aggregate([{"$group": {"_id": key, "n": {"$sum": 1}}}], batchSize=1))
            assert BSON.encode({"rows": result}) == BSON.encode({"rows": [{"_id": expected_key(n), "n": 60} for n in range(3)]})
        for key in [Int64(1), Code("$literal"), {"$private": [Binary(b"abc", 128), Decimal128("1.00")]}]:
            assert BSON.encode({"rows": list(collection.aggregate([{"$group": {"_id": {"$literal": key}, "n": {"$sum": 1}}}]))}) == BSON.encode({"rows": [{"_id": key, "n": 180}]})
        assert list(client.unwritten_aggregate.items.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}])) == []
        precision = client.wire_aggregate.precision
        precision.insert_many([{"_id": 1, "v": Decimal128("1.00")}, {"_id": 2, "v": 2.1}])
        numeric = [{"$group": {"_id": None, "sum": {"$sum": "$v"}, "avg": {"$avg": "$v"}}}]
        expected_numeric = [{"_id": None, "sum": Decimal128("3.100000000000000088817841970012523"),
                             "avg": Decimal128("1.550000000000000044408920985006262")}]
        assert BSON.encode({"rows": list(precision.aggregate(numeric))}) == BSON.encode({"rows": expected_numeric})
        first = client.wire_aggregate.command("aggregate", "items", pipeline=stages, cursor={"batchSize": 0})["cursor"]
        assert first["firstBatch"] == [] and first["id"] != 0
        identifier = first["id"]
        result = client.wire_aggregate.command("getMore", identifier, collection="items", batchSize=2)["cursor"]
        assert result["nextBatch"] == expected[:2]
        client.wire_aggregate.command("killCursors", "items", cursors=[identifier])
        try:
            client.wire_aggregate.command("getMore", identifier, collection="items")
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("aggregate kill must release retained results")
        for pipeline, options, code in [
            (None, {}, 14), ({}, {}, 14), ([1], {}, 2), ([{}], {}, 2),
            ([{"$skip": 0, "$limit": 1}], {}, 2), ([{"$skip": True}], {}, 5107200),
            ([{"$limit": 0}], {}, 15958), ([{"$count": Code("private")}], {}, 40156),
            ([{"$count": "private.field"}], {}, 40160), ([{"$sort": {}}], {}, 15976),
            ([{"$match": {"$where": "private"}}], {}, 115),
            ([{"$group": {"_id": {"private": "$$REMOVE"}}}], {}, 115),
            ([{"$group": {"_id": ["$$ROOT"]}}], {}, 115),
            ([{"$group": {"_id": {"$size": []}}}], {}, 16020),
            ([{"$group": {"_id": None, "private.path": {"$sum": 1}}}], {}, 40235),
            ([{"$group": {"_id": None, "private": {"$first": "$$REMOVE"}}}], {}, 115),
            ([{"$group": {"_id": None, "private": {"$avg": []}}}], {}, 40237),
            ([], {"allowDiskUse": True}, 72), ([], {"hint": "_id_"}, 72),
            ([], {"comment": "private"}, 72), ([], {"readConcern": {"level": "local"}}, 72),
            ([], {"cursor": []}, 14), ([], {"cursor": {"unknown": 1}}, 72),
            ([], {"cursor": {"batchSize": -1}}, 2),
        ]:
            for database in [client.wire_aggregate, client.unwritten_aggregate]:
                arguments = {"pipeline": pipeline, "cursor": {}}
                arguments.update(options)
                try:
                    database.command("aggregate", "items", **arguments)
                except OperationFailure as error:
                    assert error.code == code, (arguments, error.code)
                    assert "private" not in str(error)
                else:
                    raise AssertionError("invalid pipeline/options must fail before absent-collection handling")
        # Byte limits page valid output rather than eagerly building a reply
        # larger than the wire envelope. Sorting still uses a bounded core.
        large = client.wire_aggregate.large
        large.insert_many([{"_id": index, "payload": "x" * 400000} for index in range(5)])
        first = client.wire_aggregate.command("aggregate", "large", pipeline=[{"$sort": {"_id": -1}}], cursor={"batchSize": 1000})["cursor"]
        assert len(first["firstBatch"]) == 2
        rows = first["firstBatch"]
        identifier = first["id"]
        while identifier:
            page = client.wire_aggregate.command("getMore", identifier, collection="large", batchSize=1000)["cursor"]
            rows.extend(page["nextBatch"])
            identifier = page["id"]
        assert [row["_id"] for row in rows] == [4, 3, 2, 1, 0]
        first = client.wire_aggregate.command("aggregate", "large", pipeline=[{"$group": {"_id": "$_id", "payload": {"$first": "$payload"}}}], cursor={"batchSize": 1000})["cursor"]
        assert len(first["firstBatch"]) == 2 and first["id"]
        grouped_rows = first["firstBatch"]
        identifier = first["id"]
        while identifier:
            page = client.wire_aggregate.command("getMore", identifier, collection="large", batchSize=1000)["cursor"]
            grouped_rows.extend(page["nextBatch"])
            identifier = page["id"]
        assert [row["_id"] for row in grouped_rows] == list(range(5))
        # A whole group must fit BSON, and total retained group state must fit
        # its working quota even when a later limit asks for only one result.
        for copies in (10, 40):
            too_large = [{"$group": {"_id": None, "values": {"$push": {f"copy{i}": "$payload" for i in range(copies)}}}}, {"$limit": 1}]
            first = client.wire_aggregate.command("aggregate", "large", pipeline=too_large, cursor={"batchSize": 0})["cursor"]
            assert first["firstBatch"] == [] and first["id"]
            for code in (10334, 43):
                try:
                    client.wire_aggregate.command("getMore", first["id"], collection="large", batchSize=1)
                except OperationFailure as error:
                    assert error.code == code
                else:
                    raise AssertionError("oversized grouping must fail without partial replies and release the cursor")
        assert list(collection.find({})) == documents

        key_errors = client.wire_aggregate.key_errors
        key_errors.insert_many([{"_id": 1, "v": []}, {"_id": 2, "v": None}])
        bad_key = [{"$group": {"_id": {"$size": "$v"}, "n": {"$sum": 1}}}]
        first = client.wire_aggregate.command("aggregate", "key_errors", pipeline=bad_key, cursor={"batchSize": 0})["cursor"]
        assert first["firstBatch"] == [] and first["id"]
        for code in (17124, 43):
            try:
                client.wire_aggregate.command("getMore", first["id"], collection="key_errors", batchSize=1)
            except OperationFailure as error:
                assert error.code == code and "cursor" not in error.details
            else:
                raise AssertionError("key failure must return no partial groups and release its cursor")
        assert list(key_errors.aggregate([{"$limit": 1}] + bad_key)) == [{"_id": 0, "n": 1}]

        transformed = client.wire_aggregate.transforms
        original = {"_id": Int64(1), "source": Int64(9), "a": {"y": 2, "x": Int64(1), "old": 0},
                    "items": [{"name": "one", "secret": 1}, {}, 3], "secret": "private"}
        transformed.insert_one(original)
        pipeline = [
            {"$set": {"source": "new", "old": "$source", "items.label": "$source", "secret": "$$REMOVE"}},
            {"$project": {"_id": 0, "items.name": 1, "items.label": 1, "n": {"$size": "$items"},
                          "copied": "$old", "fallback": {"$ifNull": ["$absent", "$$REMOVE", None]},
                          "literal": {"$literal": "$source"}, "missing.shell": "$$REMOVE"}},
            {"$unset": "literal"},
        ]
        expected = [{"items": [{"name": "one", "label": Int64(9)}, {"label": Int64(9)}, {"label": Int64(9)}],
                     "n": 3, "copied": Int64(9), "fallback": None, "missing": {}}]
        assert BSON.encode({"rows": list(transformed.aggregate(pipeline, batchSize=1))}) == BSON.encode({"rows": expected})
        assert transformed.find_one({}) == original
        for stage in ("$set", "$addFields"):
            assert list(transformed.aggregate([{stage: {"source": 2, "old": "$source"}}]))[0]["old"] == Int64(9)
        for pipeline, code in [
            ([{"$project": {}}], 51272), ([{"$project": {"a": 0, "b": "$source"}}], 31310),
            ([{"$project": {"a": 0, "b": {"$literal": 1}}}], 31252),
            ([{"$set": {"a": 1, "a.b": 2}}], 40176), ([{"$unset": ["a", "a"]}], 31250),
            ([{"$set": {"private": {"$ifNull": []}}}], 1257300),
            ([{"$set": {"private": {"$size": []}}}], 16020),
            ([{"$project": {"private": "$$REMOVE.$bad"}}], 16410),
        ]:
            for target in (transformed, client.unwritten_aggregate.transforms):
                try:
                    list(target.aggregate(pipeline))
                except OperationFailure as error:
                    assert error.code == code, (pipeline, error.code)
                    assert "private" not in str(error)
                else:
                    raise AssertionError("transform validation must be eager")
        errors = client.wire_aggregate.transform_errors
        errors.insert_many([{"_id": 1, "v": []}, {"_id": 2, "v": None}])
        group_error = {"$group": {"_id": "$_id", "n": {"$first": {"$size": "$v"}}}}
        first = client.wire_aggregate.command("aggregate", "transform_errors", pipeline=[group_error], cursor={"batchSize": 0})["cursor"]
        assert first["firstBatch"] == [] and first["id"]
        for code in (17124, 43):
            try:
                client.wire_aggregate.command("getMore", first["id"], collection="transform_errors", batchSize=1)
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("group failure must return no partial rows and release its cursor")
        assert list(errors.aggregate([{"$limit": 1}, group_error])) == [{"_id": 1, "n": 0}]
        size = {"$set": {"n": {"$size": "$v"}}}
        first = client.wire_aggregate.command("aggregate", "transform_errors", pipeline=[size], cursor={"batchSize": 1})["cursor"]
        assert first["firstBatch"][0]["n"] == 0 and first["id"]
        for code in (17124, 43):
            try:
                client.wire_aggregate.command("getMore", first["id"], collection="transform_errors")
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expression failure must release its cursor")
        for prefix in ([], [{"$sort": {"_id": 1}}]):
            assert list(errors.aggregate(prefix + [size, {"$limit": 1}])) == [{"_id": 1, "v": [], "n": 0}]
        # Projection grows each row, but successful output still pages at the
        # same byte cap. A much larger broadcast fails before allocating it.
        expanded = client.wire_aggregate.expanded
        expanded.insert_many([{"_id": index, "payload": "x" * 200000, "items": [None] * 400} for index in range(5)])
        pipeline = [{"$project": {"_id": 1, "first": "$payload", "second": "$payload"}}, {"$sort": {"_id": -1}}]
        first = client.wire_aggregate.command("aggregate", "expanded", pipeline=pipeline, cursor={"batchSize": 1000})["cursor"]
        assert len(first["firstBatch"]) == 2
        rows = first["firstBatch"]
        identifier = first["id"]
        while identifier:
            page = client.wire_aggregate.command("getMore", identifier, collection="expanded", batchSize=1000)["cursor"]
            rows.extend(page["nextBatch"])
            identifier = page["id"]
        assert [row["_id"] for row in rows] == [4, 3, 2, 1, 0]
        try:
            list(expanded.aggregate([{"$set": {"items.copy": "$payload"}}]))
        except OperationFailure as error:
            assert error.code == 10334
        else:
            raise AssertionError("broadcast amplification must be bounded")


def lifecycle_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        client.drop_database("wire_lifecycle")
        database = client.wire_lifecycle
        database.one.drop()  # PyMongo suppresses a missing-namespace error.
        for name in ("one", "two"):
            database[name].insert_many([{"_id": index, "v": Int64(index)} for index in range(40)])
        client.wire_lifecycle_keep.one.insert_one({"_id": 1, "keep": True})
        for command, value, options, code in [
            ("drop", "one", {"comment": "private-data"}, 72),
            ("drop", "absent", {"writeConcern": {"w": 0}}, 72),
            ("dropDatabase", 1, {"writeConcern": {"w": "majority"}}, 72),
            ("dropDatabase", True, {}, 2), ("dropDatabase", 0, {}, 2),
            ("drop", Code("one"), {}, 2), ("drop", "one", {"maxTimeMS": -1}, 2),
        ]:
            try:
                database.command(command, value, **options)
            except OperationFailure as error:
                assert error.code == code, (command, options, error.code)
                assert "private-data" not in str(error)
            else:
                raise AssertionError("invalid lifecycle commands must fail before mutation")
        for aggregate in (False, True):
            if aggregate:
                cursor = database.one.aggregate([{"$sort": {"_id": -1}}], batchSize=1)
            else:
                cursor = database.one.find(batch_size=1)
            next(cursor)
            identifier = cursor.cursor_id
            assert identifier
            database.drop_collection("one")
            database.one.insert_many([{"_id": index, "new": True} for index in range(100, 140)])
            for _ in range(2):
                try:
                    database.command("getMore", identifier, collection="one", batchSize=1)
                except OperationFailure as error:
                    assert error.code == 43
                else:
                    raise AssertionError("an old wire cursor must not read a recreated collection")
            cursor.close()
        assert database.two.count_documents({}) == 40
        assert client.wire_lifecycle_keep.one.find_one({"_id": 1}) == {"_id": 1, "keep": True}
        client.drop_database("wire_lifecycle")
        assert database.one.count_documents({}) == database.two.count_documents({}) == 0
        client.drop_database("wire_lifecycle")
        try:
            database.command("drop", "absent")
        except OperationFailure as error:
            assert error.code == 26
        else:
            raise AssertionError("raw missing collection drop must return NamespaceNotFound")
        database.recreated.insert_one({"_id": 1, "after_drop": True})
        assert client.wire_lifecycle_keep.one.count_documents({}) == 1


def persisted_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        assert client.wire_pop_rename.queue.count_documents({"items": [], "keep": True}) == 12
        assert client.wire_pop_rename.items.find_one({"_id": 23})["nested"] == {"value": Binary(b"value", 128)}
        assert sorted(client.wire_membership.concurrent.find_one()["values"]) == list(range(4))
        assert client.wire_membership.items.find_one({"_id": 11})["nested"] == {"0": {"values": []}}
        assert client.wire_membership.capped.find_one()["values"] == ["x" * 280000]
        assert client.wire_min_max.items.count_documents({"low": -32, "high": 32, "keep": True}) == 24
        row = client.wire_find_update.items.find_one({"_id": 10})
        assert row["rank"] == -1 and row["done"] is True and "group" not in row and row["stamp"] == Timestamp(0, 0)
        assert client.wire_update_many.items.count_documents({"validated": True}) == 24
        assert client.wire_field_update.items.find_one({"_id": 1})["field23"] == 23
        assert BSON.encode(client.wire_find_replace.items.find_one({"_id": 10})) == BSON.encode({"_id": Int64(10), "value": "persisted", "hidden": True})
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        assert BSON.encode(client.wire_replacement.items.find_one({"_id": 1})) == BSON.encode({"_id": Int64(1), "value": "persisted"})
        assert client.wire_replacement.items.find_one({"_id": 2})["unack"] is True
        assert BSON.encode(client.async_replacement.items.find_one({})) == BSON.encode({"_id": Int64(1), "value": Int64(1)})
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        assert [row["_id"] for row in client.wire_find_delete.items.find({})] == list(reversed(range(1, 10)))
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        assert [row["_id"] for row in client.wire_deletes.items.find({})] == list(reversed(range(1, 30, 2)))
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        assert client.wire_data.items.find_one({"_id": "typed"})["decimal"] == Decimal128("1.250")
        assert client.wire_data.items.find_one({"_id": None})["value"] == "typed id"
        assert client.async_data.items.find_one({"_id": "async"})["value"] == "async write"
        assert client.wire_batches.ordered_True.find_one({"_id": 3}) is None
        assert client.wire_batches.ordered_False.find_one({"_id": 3})["position"] == 4
        assert client.wire_batches.split.find_one({"_id": 1000}) == {"_id": 1000}
        assert client.wire_batches.timestamps.find_one({"_id": "first"})["stamp"].time > 0
        assert client.async_data.batches.find_one({"_id": 2}) == {"_id": 2}
        assert sorted(item["_id"] for item in client.wire_queries.items.find({"v": {"$in": [7]}})) == ["array", "number"]
        assert [row["_id"] for row in client.wire_batches.split.find(batch_size=31).limit(150)] == list(range(150))
        row = client.wire_projection.items.find_one({"_id": 7}, {"profile.name": 1})
        assert row == {"_id": 7, "profile": {"name": "name-7"}}
        assert len(client.wire_projection.items.find_one({"_id": 7})["payload"]) == 5000
        assert [row["_id"] for row in client.wire_sorting.items.find({}, {"_id": 1}).sort("_id", -1).batch_size(4)] == list(reversed(range(36)))
        assert client.wire_count.items.estimated_document_count() == 37
        assert client.wire_count.command("count", "items", query={"group": 1}, skip=3)["n"] == 9
        assert client.wire_count.items.count_documents({"group": 1}, skip=3, limit=5) == 5
        values = client.wire_distinct.ordered.distinct("v")
        assert values == list(range(17)) and all(isinstance(value, Int64) for value in values)
        assert client.wire_distinct.items.distinct("nested.a") == ["first", "second"]
        assert list(client.wire_aggregate.items.aggregate([{"$count": "n"}])) == [{"n": 180}]
        assert [row["_id"] for row in client.wire_aggregate.items.aggregate([{"$sort": {"_id": -1}}, {"$limit": 7}], batchSize=2)] == list(reversed(range(173, 180)))
        assert list(client.wire_aggregate.transforms.aggregate([{"$project": {"_id": 0, "n": {"$size": "$items"}, "source": 1}}])) == [{"source": Int64(9), "n": 3}]
        assert list(client.wire_aggregate.items.aggregate([{"$group": {"_id": "$group", "n": {"$sum": 1}}}])) == [{"_id": key, "n": 60} for key in range(3)]
        assert client.wire_lifecycle.one.count_documents({}) == 0
        assert client.wire_lifecycle.two.count_documents({}) == 0
        assert client.wire_lifecycle.recreated.find_one({"_id": 1}) == {"_id": 1, "after_drop": True}
        assert client.wire_lifecycle_keep.one.find_one({"_id": 1}) == {"_id": 1, "keep": True}
        assert client.async_lifecycle.items.count_documents({}) == 0
        metadata = list(client.wire_metadata.list_collections(filter={"name": "alpha"}))
        assert len(metadata) == 1
        assert metadata[0]["info"]["uuid"] == client.wire_metadata.alpha.find_one({"_id": "metadata-uuid"})["value"]


def metadata_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000) as client:
        database = client.wire_metadata
        before = set(client.list_database_names())
        assert "wire_metadata" not in before
        assert client.absent_metadata.list_collection_names() == []
        assert list(client.absent_metadata.list_collections()) == []
        for name in ["alpha", "beta", "gamma"]:
            assert database.create_collection(name).name == name
        assert set(client.list_database_names()) == before | {"wire_metadata"}
        reply = client.admin.command("listDatabases", 1, nameOnly=True, filter={"name": "wire_metadata"}, authorizedDatabases=True)
        assert reply == {"databases": [{"name": "wire_metadata"}], "ok": 1.0}
        assert list(client.list_databases(nameOnly=True, filter={"$or": [{"name": "wire_metadata"}, {"name": "absent_metadata"}]})) == [{"name": "wire_metadata"}]
        for command in [
            {"listDatabases": 1}, {"listDatabases": 1, "nameOnly": False},
            {"listDatabases": 1, "nameOnly": True, "filter": {"sizeOnDisk": 0}},
            {"listDatabases": 1, "nameOnly": True, "filter": {"$or": [{"empty": False}]}},
            {"listDatabases": 1, "nameOnly": True, "filter": {"name": {"$unsupported": 1}}},
            {"listDatabases": 1, "nameOnly": 1},
            {"listDatabases": 1, "nameOnly": True, "cursor": {}},
        ]:
            try:
                client.admin.command(command)
            except OperationFailure:
                pass
            else:
                raise AssertionError(f"unsupported database listing accepted: {command}")
        try:
            database.command("listDatabases", 1, nameOnly=True)
        except OperationFailure as error:
            assert error.code == 13
        else:
            raise AssertionError("listDatabases must be an admin command")
        try:
            database.create_collection("alpha")
        except CollectionInvalid:
            pass
        else:
            raise AssertionError("PyMongo must detect an existing collection via discovery")
        assert database.create_collection("alpha", check_exists=False).name == "alpha"
        assert database.command("create", "alpha")["ok"] == 1
        assert sorted(database.list_collection_names()) == ["alpha", "beta", "gamma"]
        assert database.list_collection_names(filter={"name": "beta"}) == ["beta"]
        rows = list(database.list_collections(cursor={"batchSize": 1}, filter={"name": {"$regex": "a$"}}))
        assert {row["name"] for row in rows} == {"alpha", "beta", "gamma"}
        for row in rows:
            assert row["type"] == "collection" and row["options"] == {}
            assert row["info"]["readOnly"] is False
            assert isinstance(row["info"]["uuid"], Binary) and row["info"]["uuid"].subtype == 4
            assert row["idIndex"] == {"name": "_id_", "key": {"_id": 1}, "unique": True}
        identity = rows[0]["info"]["uuid"]
        database.alpha.insert_one({"_id": "metadata-uuid", "value": identity})
        first = database.command("listCollections", 1, nameOnly=True, cursor={"batchSize": 0})["cursor"]
        assert first["ns"] == "wire_metadata.$cmd.listCollections" and first["id"] and first["firstBatch"] == []
        database.create_collection("later")
        with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as peer:
            result = peer.wire_metadata.command("getMore", first["id"], collection="$cmd.listCollections", batchSize=2)["cursor"]
            names = [row["name"] for row in result["nextBatch"]]
            while result["id"]:
                result = peer.wire_metadata.command("getMore", result["id"], collection="$cmd.listCollections", batchSize=2)["cursor"]
                names.extend(row["name"] for row in result["nextBatch"])
        assert names == ["alpha", "beta", "gamma"]
        first = database.command("listCollections", 1, cursor={"batchSize": 0})["cursor"]
        killed = database.command("killCursors", "$cmd.listCollections", cursors=[first["id"]])
        assert killed["cursorsKilled"] == [first["id"]]
        for command in [
            {"create": "rejected", "capped": True},
            {"create": "rejected", "collation": {"locale": "en"}},
            {"create": "rejected", "writeConcern": {"w": 0}},
            {"create": "rejected", "writeConcern": {"w": "majority"}},
            {"listCollections": 1, "filter": {"$unsupported": 1}},
            {"listCollections": 1, "cursor": {"batchSize": -1}},
            {"listCollections": 1, "cursor": {"unknown": True}},
        ]:
            try:
                database.command(command)
            except OperationFailure:
                pass
            else:
                raise AssertionError(f"unsupported metadata command accepted: {command}")
        assert "rejected" not in database.list_collection_names()
        assert list(database.list_collections(nameOnly=True, authorizedCollections=True, filter={"info": {"$exists": True}})) == []


async def async_array_membership_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_membership.items
        await collection.insert_many([{"_id": i, "values": [Int64(1), True]} for i in range(4)])
        assert (await collection.update_one({"_id": 0}, {"$addToSet": {"values": 1.0}})).modified_count == 0
        result = await collection.update_many({}, {"$addToSet": {"values": {"$each": [2, 2.0, [1, 2]]}}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert await collection.find_one_and_update({}, {"$pullAll": {"values": [1, [1, 2]]}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}, return_document=True) == {"values": [True, 2]}
        try:
            await collection.update_one({}, {"$addToSet": {"values": {"$each": None}}})
        except OperationFailure as error:
            assert error.code == 2
        else:
            raise AssertionError("malformed each must be rejected")


async def async_pop_rename_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_pop_rename.items
        await collection.insert_many([{"_id": i, "old": Int64(9), "items": [1, 2, 3]} for i in range(4)])
        assert (await collection.update_one({"_id": 0}, {"$pop": {"items": 1}})).modified_count == 1
        result = await collection.update_many({}, {"$rename": {"old": "nested.value"}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert await collection.find_one_and_update({}, {"$pop": {"items": -1}}, sort=[("_id", -1)], projection={"items": 1, "_id": 0}, return_document=True) == {"items": [2, 3]}


async def async_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_min_max.items
        await collection.insert_many([{"_id": i, "v": 5.0} for i in range(4)])
        assert (await collection.update_one({"_id": 0}, {"$max": {"v": Int64(5)}})).modified_count == 0
        result = await collection.update_many({}, {"$min": {"v": 4}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert await collection.find_one_and_update({}, {"$max": {"v": 6}}, sort=[("_id", -1)], return_document=True, projection={"v": 1, "_id": 0}) == {"v": 6}
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_find_update.items
        await collection.insert_many([{"_id": Int64(i), "rank": i, "keep": True} for i in range(6)])
        before = await collection.find_one_and_update({}, {"$set": {"rank": -1}}, sort=[("rank", -1)], projection={"rank": 1, "_id": 0})
        assert before == {"rank": 5}
        after = await collection.find_one_and_update({"_id": 5.0}, {"$unset": {"rank": 1}}, return_document=True)
        assert BSON.encode(after) == BSON.encode({"_id": Int64(5), "keep": True})
        assert await collection.find_one_and_update({"_id": 99}, {"$set": {}}) is None
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_update_many.items
        await collection.insert_many([{"_id": Int64(i), "value": 1} for i in range(6)])
        result = await collection.update_many({}, {"$set": {"value": Int64(1)}})
        assert (result.matched_count, result.modified_count) == (6, 6)
        assert (await collection.update_many({}, {"$set": {"value": Int64(1)}})).modified_count == 0
        assert (await collection.update_many({"_id": 3.0}, {"$unset": {"value": 1}})).modified_count == 1
        assert await collection.find_one({"_id": 3}) == {"_id": Int64(3)}
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_field_update.items
        await collection.insert_one({"_id": Int64(1), "value": 1, "keep": True})
        result = await collection.update_one({"value": 1}, {"$set": {"value": Int64(1)}})
        assert (result.matched_count, result.modified_count) == (1, 1)
        assert (await collection.update_one({"_id": 1.0}, {"$set": {"value": Int64(1)}})).modified_count == 0
        assert (await collection.update_one({}, {"$unset": {"keep": 1}})).modified_count == 1
        assert BSON.encode(await collection.find_one({})) == BSON.encode({"_id": Int64(1), "value": Int64(1)})
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_find_replace.items
        await collection.insert_many([{"_id": Int64(i), "rank": i} for i in range(4)])
        assert await collection.find_one_and_replace({}, {"value": Int64(9)}, sort=[("rank", -1)], projection={"rank": 1, "_id": 0}) == {"rank": 3}
        value = await collection.find_one_and_replace({"_id": 3}, {"value": Int64(10)}, return_document=True)
        assert BSON.encode(value) == BSON.encode({"_id": Int64(3), "value": Int64(10)})
        assert await collection.find_one_and_replace({"_id": 99}, {}, return_document=True) is None
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_replacement.items
        await collection.insert_one({"_id": Int64(1), "value": 1})
        replacement = {"value": Int64(1)}
        result = await collection.replace_one({"value": 1}, replacement)
        assert (result.matched_count, result.modified_count) == (1, 1)
        assert (await collection.replace_one({"_id": 1.0}, replacement)).modified_count == 0
        assert (await collection.replace_one({"_id": 2}, {})).matched_count == 0
        assert BSON.encode(await collection.find_one({})) == BSON.encode({"_id": Int64(1), "value": Int64(1)})
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_find_delete.items
        await collection.insert_many([{"_id": i, "nested": {"v": Int64(i)}} for i in range(5)])
        assert await collection.find_one_and_delete({}, sort=[("_id", -1)], projection={"nested": 1, "_id": 0}) == {"nested": {"v": Int64(4)}}
        assert await collection.find_one_and_delete({"_id": 4}) is None
        assert await collection.count_documents({}) == 4
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_deletes.items
        await collection.insert_many([{"_id": i, "v": [i % 2]} for i in range(12)])
        assert (await collection.delete_one({"v": 0})).deleted_count == 1
        assert await collection.find_one({"_id": 0}) is None
        assert (await collection.delete_many({"v": 0})).deleted_count == 5
        assert (await collection.delete_many({})).deleted_count == 6
        assert (await collection.delete_many({})).deleted_count == 0
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000, maxPoolSize=3) as client:
        assert (await client.admin.command("ping"))["ok"] == 1
        check_hello(await client.admin.command("hello"))
        metadata = client.async_metadata
        await metadata.create_collection("one")
        await metadata.create_collection("two")
        assert "async_metadata" in await client.list_database_names()
        assert sorted(await metadata.list_collection_names()) == ["one", "two"]
        cursor = await metadata.list_collections(cursor={"batchSize": 1})
        assert [row["name"] async for row in cursor] == ["one", "two"]
        assert await metadata.list_collection_names(filter={"name": "two"}) == ["two"]
        await client.drop_database("async_metadata")
        assert "async_metadata" not in await client.list_database_names()
        assert await metadata.list_collection_names() == []
        await client.async_lifecycle.items.insert_one({"_id": 1})
        await client.async_lifecycle.items.drop()
        assert await client.async_lifecycle.items.count_documents({}) == 0
        await client.async_lifecycle.items.insert_one({"_id": 2})
        await client.drop_database("async_lifecycle")
        await client.drop_database("async_lifecycle")
        assert await client.async_lifecycle.items.count_documents({}) == 0
        assert await client.wire_count.items.estimated_document_count(maxTimeMS=10000) == 37
        assert (await client.wire_count.command("count", "items", query={"group": 1}, skip=3, limit=5))["n"] == 5
        assert await client.wire_count.items.count_documents({"group": 1}, skip=3, limit=5) == 5
        assert await client.unwritten_count.items.count_documents({}) == 0
        assert await client.unwritten_count.items.estimated_document_count() == 0
        values = await client.wire_distinct.items.distinct("v")
        assert isinstance(values[0], Int64) and len(values) == 5
        assert await client.wire_distinct.items.distinct("nested.a", maxTimeMS=10000) == ["first", "second"]
        assert await client.unwritten_distinct.items.distinct("v") == []
        aggregate = await client.wire_aggregate.items.aggregate([{"$match": {"group": 1}}, {"$sort": {"_id": -1}}, {"$skip": 2}, {"$limit": 7}], batchSize=2)
        assert [row["_id"] for row in await aggregate.to_list()] == [172, 169, 166, 163, 160, 157, 154]
        aggregate = await client.wire_aggregate.items.aggregate([{"$count": "n"}])
        assert await aggregate.to_list() == [{"n": 180}]
        aggregate = await client.wire_aggregate.items.aggregate([{"$sort": {"_id": -1}}, {"$group": {"_id": "$group", "n": {"$sum": 1}, "first": {"$first": "$_id"}, "last": {"$last": "$_id"}}}], batchSize=1)
        assert await aggregate.to_list() == [{"_id": key, "n": 60, "first": Int64(177 + key), "last": Int64(key)} for key in (2, 1, 0)]
        aggregate = await client.wire_aggregate.items.aggregate([{"$group": {"_id": {"team": "$group"}, "n": {"$sum": 1}}}], batchSize=1)
        assert await aggregate.to_list() == [{"_id": {"team": key}, "n": 60} for key in range(3)]
        aggregate = await client.wire_aggregate.transforms.aggregate([
            {"$addFields": {"items.tag": "$source", "secret": "$$REMOVE"}},
            {"$project": {"_id": 0, "n": {"$size": "$items"}, "copy": {"$ifNull": ["$absent", "$source"]}}},
        ], batchSize=1)
        assert await aggregate.to_list() == [{"n": 3, "copy": Int64(9)}]
        aggregate = await client.wire_aggregate.items.aggregate([], batchSize=1)
        assert (await aggregate.__anext__())["_id"] == 0
        identifier = aggregate.cursor_id
        await aggregate.close()
        try:
            await client.wire_aggregate.command("getMore", identifier, collection="items")
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("async aggregate close must release its cursor")
        replies = await asyncio.gather(*(client.admin.command("ping") for _ in range(12)))
        assert all(reply["ok"] == 1 for reply in replies)
        try:
            await client.example.command("unsupportedCommand", "items")
        except OperationFailure as error:
            assert error.code == 59
        else:
            raise AssertionError("unimplemented async commands must fail explicitly")
        collection = client.async_data.items
        assert (await collection.insert_one({"_id": "async", "value": "async write"})).acknowledged
        assert await collection.find_one({"_id": "async"}) == {"_id": "async", "value": "async write"}
        try:
            await collection.insert_one({"_id": "async"})
        except DuplicateKeyError as error:
            assert error.code == 11000
        else:
            raise AssertionError("async duplicate insert must fail")
        try:
            await client.async_data.batches.insert_many([{"_id": 1}, {"_id": 1.0}, {"_id": 2}], ordered=False)
        except BulkWriteError as error:
            assert error.details["nInserted"] == 2
            assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == [(1, 11000)]
        else:
            raise AssertionError("async unordered batch must report duplicate and continue")
        assert await client.async_data.batches.find_one({"_id": 2}) == {"_id": 2}
        rows = await client.wire_queries.items.find({"items.score": {"$gt": 1}}).to_list()
        assert sorted(item["_id"] for item in rows) == ["array", "number"]
        rows = await client.wire_batches.split.find(batch_size=17).skip(8).limit(121).to_list()
        assert [row["_id"] for row in rows] == list(range(8, 129))
        rows = await client.wire_projection.items.find({"secret": "filter-me"}, {"before": 1, "_id": 0}, batch_size=3).to_list()
        assert rows == [{"before": Int64(index)} for index in range(24)]
        rows = await client.wire_sorting.items.find({}, {"_id": 1}).sort("_id", -1).skip(3).limit(11).batch_size(2).to_list()
        assert rows == [{"_id": index} for index in range(32, 21, -1)]
        cursor = client.wire_batches.split.find(batch_size=2)
        assert (await cursor.__anext__())["_id"] == 0
        identifier = cursor.cursor_id
        await cursor.close()
        try:
            await client.wire_batches.command("getMore", identifier, collection="split")
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("async close must kill the cursor")


if __name__ == "__main__":
    assert pymongo.version == "4.17.0", "use the pinned real-driver version"
    if len(sys.argv) > 2 and sys.argv[2] == "reopened":
        persisted_smoke(sys.argv[1])
    else:
        sync_smoke(sys.argv[1])
        document_smoke(sys.argv[1])
        batch_smoke(sys.argv[1])
        query_smoke(sys.argv[1])
        cursor_smoke(sys.argv[1])
        projection_smoke(sys.argv[1])
        sorting_smoke(sys.argv[1])
        count_smoke(sys.argv[1])
        distinct_smoke(sys.argv[1])
        aggregation_smoke(sys.argv[1])
        lifecycle_smoke(sys.argv[1])
        metadata_smoke(sys.argv[1])
        delete_smoke(sys.argv[1])
        find_delete_smoke(sys.argv[1])
        replacement_smoke(sys.argv[1])
        field_update_smoke(sys.argv[1])
        update_many_smoke(sys.argv[1])
        find_update_smoke(sys.argv[1])
        min_max_smoke(sys.argv[1])
        pop_rename_smoke(sys.argv[1])
        array_membership_smoke(sys.argv[1])
        find_replace_smoke(sys.argv[1])
        # Give the added operator cases their own bounded phase; retain the
        # existing discovery/CRUD phase's deadline as the suite grows.
        asyncio.run(asyncio.wait_for(async_pop_rename_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_array_membership_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_smoke(sys.argv[1]), timeout=20))
    print("PyMongo 4.17.0 discovery, insert batches, filtered/cursor reads, BSON, and rejection passed")
