"""Required real-wire discovery and point operations, not a Mongo parity claim."""

import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pymongo
from bson import BSON, Binary, Code, Decimal128, Int64, ObjectId, Regex, Timestamp
from pymongo.errors import BulkWriteError, CollectionInvalid, DuplicateKeyError, OperationFailure, WriteError


def rolled_back_batch_smoke(database, collection, expression, code):
    before = [BSON.encode(row) for row in collection.find({})]
    try:
        collection.update_many({}, expression)
    except WriteError as error:
        assert error.code == code
    else:
        raise AssertionError("confirmed rollback must be a driver WriteError")
    for ordered in [True, False]:
        reply = database.command("update", collection.name, ordered=ordered, updates=[
            {"q": {}, "u": expression, "multi": True},
            {"q": {"_id": 0}, "u": {"$set": {"continued_after_rollback": True}}},
        ])
        changed = 0 if ordered else 1
        assert (reply["n"], reply["nModified"]) == (changed, changed)
        assert [(error["index"], error["code"]) for error in reply["writeErrors"]] == [(0, code)]
        assert collection.count_documents({"continued_after_rollback": True}) == changed
    collection.update_one({"_id": 0}, {"$unset": {"continued_after_rollback": 1}})
    assert [BSON.encode(row) for row in collection.find({})] == before


def find_upsert_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        database = client.wire_find_upsert
        for replacement in (False, True):
            collection = database[f"form_{replacement}"]
            body = {"counter": 5, "stamp": Timestamp(0, 0), "hidden": True} if replacement else {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0), "hidden": True}}
            for after, identifier in [(False, None), (True, Int64(1))]:
                reply = database.command("findAndModify", collection.name, query={"_id": identifier, "counter": 3}, update=body, upsert=True, new=after, fields={"counter": 1, "_id": 0}, sort={"counter": -1})
                assert reply["lastErrorObject"] == {"n": 1, "updatedExisting": False, "upserted": identifier}
                assert BSON.encode({"v": reply["lastErrorObject"]["upserted"]}) == BSON.encode({"v": identifier})
                assert reply["value"] == ({"counter": 5} if after else None)
                stored = collection.find_one({"_id": identifier})
                assert list(stored)[0] == "_id" and stored["hidden"]
                assert (stored["stamp"] == Timestamp(0, 0)) == (not replacement)
                reply = database.command("findAndModify", collection.name, query={"_id": identifier}, update=body, upsert=True, new=after, fields={"absent": 1, "_id": 0})
                assert reply["lastErrorObject"] == {"n": 1, "updatedExisting": True}
                assert reply["value"] == {}
            method = collection.find_one_and_replace if replacement else collection.find_one_and_update
            assert method({"generated": "before"}, body, upsert=True) is None
            after = method({"generated": "after"}, body, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
            assert isinstance(after["_id"], ObjectId) and after["hidden"]
            before = [BSON.encode(row) for row in collection.find({})]
            for query, bad, code in [
                ({"_id": 99}, {"_id": 98} if replacement else {"$set": {"_id": 98}}, 66),
                ({"absent": True}, {"_id": None} if replacement else {"$set": {"_id": None}}, 11000),
            ]:
                try:
                    method(query, bad, upsert=True)
                except OperationFailure as error:
                    assert error.code == code
                else:
                    raise AssertionError("find upsert must report the mutation failure")
            assert [BSON.encode(row) for row in collection.find({})] == before
        # The combined returned image plus inserted ID exceeds the reply budget
        # even though the input document and each component fit independently.
        limited = database.limited
        for replacement in (False, True):
            method = limited.find_one_and_replace if replacement else limited.find_one_and_update
            fields = {"_id": "x" * 270000, "value": 1}
            body = fields if replacement else {"$set": fields}
            try:
                method({"absent": True}, body, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
            except OperationFailure as error:
                assert error.code == 10334
            else:
                raise AssertionError("combined reply budget must fail before insert")
            assert limited.count_documents({}) == 0
            image = method({"absent": True}, body, upsert=True, return_document=pymongo.ReturnDocument.AFTER, projection={"value": 1, "_id": 0})
            assert image == {"value": 1}
            limited.delete_many({})
        # Find-and-modify nests metadata one level shallower than update batches.
        deep_id = 1
        for _ in range(98):
            deep_id = {"nested": deep_id}
        reply = database.command("findAndModify", "deep", query={"missing": True}, update={"_id": deep_id}, upsert=True, new=True, fields={"missing": 1, "_id": 0})
        assert reply["value"] == {} and reply["lastErrorObject"]["upserted"] == deep_id
        concurrent = database.concurrent
        def increment(_):
            return concurrent.find_one_and_update({"_id": Int64(999)}, {"$inc": {"counter": 1}}, upsert=True, return_document=pymongo.ReturnDocument.AFTER)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(increment, range(16)))
        assert sorted(row["counter"] for row in results) == list(range(1, 17))
        assert concurrent.count_documents({}) == 1
        assert concurrent.find_one({}) == {"_id": Int64(999), "counter": 16}


async def async_find_upsert_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        for replacement in (False, True):
            collection = client.async_find_upsert[f"form_{replacement}"]
            method = collection.find_one_and_replace if replacement else collection.find_one_and_update
            body = {"value": Int64(7)} if replacement else {"$set": {"value": Int64(7)}}
            assert await method({"_id": None}, body, upsert=True) is None
            assert await method({"_id": Int64(1)}, body, upsert=True, return_document=pymongo.ReturnDocument.AFTER) == {"_id": Int64(1), "value": Int64(7)}
            assert await method({"_id": None}, body, upsert=True, return_document=pymongo.ReturnDocument.AFTER, projection={"value": 1, "_id": 0}) == {"value": Int64(7)}


def operator_upsert_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_operator_upsert.items
        for method, identifier in [(collection.update_one, None), (collection.update_many, Int64(2))]:
            result = method({"_id": identifier, "counter": 3}, {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0)}}, upsert=True)
            assert result.did_upsert and result.modified_count == 0
            assert result.matched_count == (1 if identifier is None else 0)
            assert BSON.encode({"v": result.upserted_id}) == BSON.encode({"v": identifier})
            assert BSON.encode(collection.find_one({"_id": identifier})) == BSON.encode({"_id": identifier, "counter": 5, "stamp": Timestamp(0, 0)})
            result = method({"_id": identifier}, {"$inc": {"counter": 2}}, upsert=True)
            assert (result.matched_count, result.modified_count, result.did_upsert) == (1, 1, False)
        assert collection.update_one({"tag": "chosen"}, {"$set": {"_id": "chosen"}}, upsert=True).upserted_id == "chosen"
        assert isinstance(collection.update_many({"tag": "generated"}, {"$inc": {"counter": 1}}, upsert=True).upserted_id, ObjectId)
        dotted = client.wire_operator_upsert.dotted
        for method, identifier in [(dotted.update_one, Int64(1)), (dotted.update_many, Int64(2))]:
            result = method({"_id.a": identifier, "_id.b": {"$eq": None}}, {"$inc": {"counter": 1}}, upsert=True)
            assert BSON.encode({"v": result.upserted_id}) == BSON.encode({"v": {"a": identifier, "b": None}})
            assert dotted.find_one({"_id": result.upserted_id}) == {"_id": result.upserted_id, "counter": 1}
            result = method({"_id.ignored": {"$gt": 1}, "tag": identifier}, {"$set": {}}, upsert=True)
            assert isinstance(result.upserted_id, ObjectId)
        for multi in [False, True]:
            for ordered in [True, False]:
                batch = client.wire_operator_upsert[f"batch_{multi}_{ordered}"]
                reply = batch.database.command("update", batch.name, ordered=ordered, updates=[
                    {"q": {"_id": None}, "u": {"$inc": {"counter": 1}}, "multi": multi, "upsert": True},
                    {"q": {"a": 1, "a.b": 2}, "u": {"$set": {}}, "multi": multi, "upsert": True},
                    {"q": {"missing": True}, "u": {"$set": {"_id": None}}, "multi": multi, "upsert": True},
                    {"q": {"_id": 3}, "u": {"$set": {"counter": 3}}, "multi": multi, "upsert": True},
                ])
                assert (reply["n"], reply["nModified"]) == ((1, 0) if ordered else (2, 0)), reply
                assert [(error["index"], error["code"]) for error in reply["writeErrors"]] == ([(1, 54)] if ordered else [(1, 54), (2, 11000)])
                assert reply["upserted"] == ([{"index": 0, "_id": None}] if ordered else [{"index": 0, "_id": None}, {"index": 3, "_id": 3}])
                assert batch.count_documents({}) == (1 if ordered else 2)
        concurrent = client.wire_operator_upsert.concurrent
        def increment(index):
            method = concurrent.update_one if index % 2 == 0 else concurrent.update_many
            return method({"_id": Int64(999)}, {"$inc": {"counter": 1}}, upsert=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(increment, range(32)))
        assert sum(result.did_upsert for result in results) == 1
        assert sum(result.matched_count for result in results) == 31
        assert sum(result.modified_count for result in results) == 31
        assert concurrent.find_one() == {"_id": 999, "counter": 32}
        for multi, write_type in [(False, pymongo.UpdateOne), (True, pymongo.UpdateMany)]:
            bounded = client.wire_operator_upsert[f"bounded_{multi}"]
            writes = [write_type({"slot": i}, {"$set": {"_id": str(i) + "x" * 127000}}, upsert=True) for i in range(5)]
            writes.append(write_type({"slot": 9}, {"$set": {"_id": "small"}}, upsert=True))
            try:
                bounded.bulk_write(writes, ordered=False)
            except BulkWriteError as error:
                assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == [(4, 10334)], error.details
                assert error.details["nUpserted"] == 5
                assert [item["index"] for item in error.details["upserted"]] == [0, 1, 2, 3, 5]
            else:
                raise AssertionError("operator upsert aggregate reply must be preflighted")
            assert bounded.count_documents({}) == 5 and bounded.find_one({"slot": 4}) is None
            deep = client.wire_operator_upsert[f"deep_{multi}"]
            identifier = 1
            for _ in range(98):
                identifier = {"nested": identifier}
            method = deep.update_many if multi else deep.update_one
            try:
                method({"_id": identifier}, {"$set": {}}, upsert=True)
            except WriteError as error:
                assert error.code == 10334
            else:
                raise AssertionError("operator upsert reply depth must be preflighted")
            assert deep.count_documents({}) == 0
            assert method({"_id": 1}, {"$set": {}}, upsert=True).did_upsert
            one_way = client.wire_operator_upsert[f"one_way_{multi}"].with_options(write_concern=pymongo.write_concern.WriteConcern(w=0))
            method = one_way.update_many if multi else one_way.update_one
            assert not method({"_id": 1}, {"$inc": {"counter": 1}}, upsert=True).acknowledged
            assert one_way.find_one({"_id": 1}) == {"_id": 1, "counter": 1}


def replacement_upsert_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_replace_upsert.items
        for query, replacement, identifier in [
            ({"_id": Int64(100)}, {"value": 7}, Int64(100)),
            ({"_id": {"$eq": None}}, {"value": 7}, None),
            ({"_id": 101, "missing": True}, {"value": 7, "_id": 101.0}, 101.0),
        ]:
            original = BSON.encode(replacement)
            result = collection.replace_one(query, replacement, upsert=True)
            assert result.did_upsert and result.modified_count == 0
            assert result.matched_count == (1 if identifier is None else 0)
            assert BSON.encode({"v": result.upserted_id}) == BSON.encode({"v": identifier})
            assert BSON.encode(collection.find_one({"_id": identifier})) == BSON.encode({"_id": identifier, "value": 7})
            assert BSON.encode(replacement) == original
            result = collection.replace_one({"_id": identifier}, {"value": 7}, upsert=True)
            assert (result.matched_count, result.modified_count, result.did_upsert) == (1, 0, False)
        generated = collection.replace_one({"missing": True}, {"tag": "generated"}, upsert=True)
        assert isinstance(generated.upserted_id, ObjectId)
        for query, replacement, code in [({"_id": 999}, {"_id": 998}, 66), ({"missing": True}, {"_id": 100}, 11000)]:
            try:
                collection.replace_one(query, replacement, upsert=True)
            except WriteError as error:
                assert error.code == code
            else:
                raise AssertionError("upsert must preserve ID and duplicate errors")
        assert collection.count_documents({}) == 4
        concurrent = client.wire_replace_upsert.concurrent
        def upsert(_):
            return concurrent.replace_one({"_id": Int64(999)}, {"value": 1}, upsert=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(upsert, range(32)))
        assert sum(result.did_upsert for result in results) == 1
        assert sum(result.matched_count for result in results) == 31
        assert sum(result.modified_count for result in results) == 0
        assert concurrent.count_documents({}) == 1
        # OP_MSG sequence exceeds one BSON body but not the message limit. Four
        # large returned IDs fit; the fifth must fail before insertion, and an
        # unordered small final upsert must still fit and execute.
        bounded = client.wire_replace_upsert.bounded
        writes = [pymongo.ReplaceOne({"slot": i}, {"_id": str(i) + "x" * 127000, "slot": i}, upsert=True) for i in range(5)]
        writes.append(pymongo.ReplaceOne({"slot": 9}, {"_id": "small", "slot": 9}, upsert=True))
        try:
            bounded.bulk_write(writes, ordered=False)
        except BulkWriteError as error:
            assert [(item["index"], item["code"]) for item in error.details["writeErrors"]] == [(4, 10334)], error.details
            assert error.details["nUpserted"] == 5
            assert [item["index"] for item in error.details["upserted"]] == [0, 1, 2, 3, 5]
        else:
            raise AssertionError("aggregate upsert ID budget must be preflighted")
        assert bounded.count_documents({}) == 5 and bounded.find_one({"slot": 4}) is None
        for ordered in [True, False]:
            batch = client.wire_replace_upsert["ordered" if ordered else "unordered"]
            reply = batch.database.command("update", batch.name, ordered=ordered, updates=[
                {"q": {"_id": None}, "u": {"value": 1}, "upsert": True},
                {"q": {"_id": None}, "u": {"value": 2}, "upsert": True},
                {"q": {"missing": True}, "u": {"_id": None}, "upsert": True},
                {"q": {"_id": 4}, "u": {"value": 4}, "upsert": True},
            ])
            assert (reply["n"], reply["nModified"]) == ((2, 1) if ordered else (3, 1))
            assert reply["upserted"] == ([{"index": 0, "_id": None}] if ordered else [{"index": 0, "_id": None}, {"index": 3, "_id": 4}])
            assert [(error["index"], error["code"]) for error in reply["writeErrors"]] == [(2, 11000)]
            assert batch.find_one({"_id": None}) == {"_id": None, "value": 2}
            assert batch.count_documents({}) == (1 if ordered else 2)
        deep = client.wire_replace_upsert.deep
        identifier = 1
        for _ in range(98):
            identifier = {"nested": identifier}
        # An OP_MSG statement can carry this ID, but its upserted reply needs
        # one extra container. Reject before commit and leave the socket usable.
        try:
            deep.replace_one({"missing": True}, {"_id": identifier}, upsert=True)
        except WriteError as error:
            assert error.code == 10334
        else:
            raise AssertionError("upsert reply depth must be preflighted")
        assert deep.count_documents({}) == 0
        assert deep.replace_one({"_id": 1}, {}, upsert=True).did_upsert
        one_way = client.wire_replace_upsert.one_way
        result = one_way.with_options(write_concern=pymongo.write_concern.WriteConcern(w=0)).replace_one({"_id": "one-way"}, {"value": 7}, upsert=True)
        assert not result.acknowledged
        assert one_way.find_one({"_id": "one-way"}) == {"_id": "one-way", "value": 7}


def increment_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_increment.items
        collection.insert_many([{"_id": Int64(i), "counter": Int64(1), "amount": Decimal128("2"), "overflow": Int64(2**63 - 1), "invalid": True} for i in range(8)])
        result = collection.update_many({}, {"$inc": {"counter": 1, "amount": 0.1}})
        assert (result.matched_count, result.modified_count) == (8, 8)
        assert collection.update_many({}, {"$inc": {"counter": 0, "amount": Decimal128("0E-100")}}).modified_count == 0
        assert BSON.encode(collection.find_one_and_update({}, {"$inc": {"counter": 1}}, sort=[("_id", -1)], projection={"counter": 1, "_id": 0})) == BSON.encode({"counter": Int64(2)})
        assert BSON.encode(collection.find_one_and_update({"_id": 7}, {"$inc": {"counter": 1}}, return_document=True, projection={"counter": 1, "_id": 0})) == BSON.encode({"counter": Int64(4)})
        assert collection.find_one()["amount"].bid == Decimal128("2.100000000000000").bid
        for expression, code in [
            ({"$set": {"marker": True}, "$inc": {"invalid": 1}}, 14),
            ({"$set": {"marker": True}, "$inc": {"overflow": 1}}, 2),
            ({"$inc": {"counter": True}}, 14),
            ({"$inc": {"_id": 1}}, 66),
        ]:
            rolled_back_batch_smoke(client.wire_increment, collection, expression, code)
        for expression, code in [({"$inc": {"counter": True}}, 14), ({"$inc": {"counter.x": 1}, "$set": {"counter": 2}}, 40)]:
            try:
                collection.update_one({"_id": 99}, expression)
            except WriteError as error:
                assert error.code == code
            else:
                raise AssertionError("increment validation must precede matching")
        fidelity = client.wire_increment.fidelity
        fidelity.insert_one({"_id": 0, "value": Decimal128("sNaN")})
        for _ in range(2):
            assert fidelity.update_one({}, {"$inc": {"value": 0}}).modified_count == 1
            assert fidelity.find_one()["value"].bid == Decimal128("NaN").bid
        for value in [Int64(1), -0.0, Decimal128("sNaN")]:
            fidelity.update_one({}, {"$unset": {"missing": 1}})
            fidelity.update_one({}, {"$inc": {"missing": value}})
            assert BSON.encode({"v": fidelity.find_one()["missing"]}) == BSON.encode({"v": value})
        fidelity.update_one({}, {"$set": {"value": -0.0}})
        assert fidelity.update_one({}, {"$inc": {"value": 0}}).modified_count == 0
        assert BSON.encode({"v": fidelity.find_one()["value"]}) == BSON.encode({"v": -0.0})
        fidelity.update_one({}, {"$set": {"value": 2**31 - 1}})
        fidelity.update_one({}, {"$inc": {"value": 1}})
        assert BSON.encode({"v": fidelity.find_one()["value"]}) == BSON.encode({"v": Int64(2**31)})
        fidelity.update_one({}, {"$inc": {"value": -1}})
        assert BSON.encode({"v": fidelity.find_one()["value"]}) == BSON.encode({"v": Int64(2**31 - 1)})
        queue = client.wire_increment.concurrent
        queue.insert_one({"_id": 0, "counter": Int64(0)})
        def increment(_):
            return queue.find_one_and_update({}, {"$inc": {"counter": 1}})["counter"]
        with ThreadPoolExecutor(max_workers=4) as workers:
            observed = list(workers.map(increment, range(32)))
        assert sorted(observed) == list(range(32))
        assert BSON.encode({"v": queue.find_one()["counter"]}) == BSON.encode({"v": Int64(32)})


def pull_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_pull.items
        values = [Int64(1), 1.0, True, [1, 2], {"_id": [1, 2], "x": 1}, {"_id": 3, "x": 3}, "Alpha", "beta"]
        collection.insert_many([{"_id": Int64(i), "values": values, "keep": True} for i in range(12)])
        result = collection.update_many({}, {"$pull": {"values": 1}})
        assert (result.matched_count, result.modified_count) == (12, 12)
        assert collection.update_many({}, {"$pull": {"missing": 1}}).modified_count == 0
        assert collection.find_one()["values"] == values[2:]
        assert collection.find_one_and_update({}, {"$pull": {"values": {"$eq": 1}}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}) == {"values": values[2:]}
        assert collection.find_one_and_update({"_id": 11}, {"$pull": {"values": {"_id": 2}}}, return_document=True, projection={"values": 1, "_id": 0}) == {"values": [True, {"_id": 3, "x": 3}, "Alpha", "beta"]}
        result = collection.update_many({}, {"$pull": {"values": {"$regex": "^a", "$options": "i"}}})
        assert (result.matched_count, result.modified_count) == (12, 12)
        for expression, code in [
            ({"$pull": {"keep": 1}}, 2),
            ({"$pull": {"values": {"$expr": {"$eq": [1, 1]}}}}, 224),
            ({"$pull": {"values": {"$regex": "["}}}, 51091),
            ({"$pull": {"values": {"$regex": "a", "$options": "q"}}}, 51108),
        ]:
            rolled_back_batch_smoke(client.wire_pull, collection, expression, code)
        identity = client.wire_pull.identity
        identity.insert_one({"_id": {"values": [1, 2]}, "keep": True})
        try:
            identity.update_one({}, {"$pull": {"_id.values": {"$gte": 2}}})
        except WriteError as error:
            assert error.code == 66
        else:
            raise AssertionError("pull must preserve identity")
        assert identity.find_one()["_id"] == {"values": [1, 2]}
        queue = client.wire_pull.concurrent
        queue.insert_one({"_id": 1, "values": list(range(32))})
        def remove(worker):
            return sum(queue.update_one({}, {"$pull": {"values": {"$eq": worker * 8 + step}}}).modified_count for step in range(8))
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert sum(pool.map(remove, range(4))) == 32
        assert queue.find_one()["values"] == []


def push_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=20000) as client:
        collection = client.wire_push.items
        collection.insert_many([{"_id": Int64(i), "values": [Int64(3)], "keep": True} for i in range(12)])
        expression = {"$push": {"values": {"$slice": 3, "$sort": 1, "$position": -1, "$each": [2, 1]}}}
        result = collection.update_many({}, expression)
        assert (result.matched_count, result.modified_count) == (12, 12)
        assert collection.update_many({}, {"$push": {"values": {"$each": []}}}).modified_count == 0
        assert BSON.encode({"v": collection.find_one({"_id": 0})["values"]}) == BSON.encode({"v": [1, 2, Int64(3)]})
        assert collection.find_one_and_update({}, {"$push": {"values": [4, 5]}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}) == {"values": [1, 2, Int64(3)]}
        assert collection.find_one_and_update({"_id": 11}, {"$push": {"values": {"$each": [Timestamp(0, 0), Binary(b"value", 128)], "$slice": -3}}}, return_document=True, projection={"values": 1, "_id": 0}) == {"values": [[4, 5], Timestamp(0, 0), Binary(b"value", 128)]}
        before = BSON.encode(collection.find_one({"_id": 0}))
        for expression, code in [
            ({"$push": {"keep": 1}}, 2),
            ({"$push": {"values": {"$each": None}}}, 2),
            ({"$push": {"values": {"$each": [], "$slice": True}}}, 2),
            ({"$push": {"values": {"$each": [], "$position": 0.5}}}, 2),
            ({"$push": {"values": {"$each": [], "$sort": {}}}}, 2),
            ({"$push": {"values.0.x": 1}}, 28),
            ({"$push": {"values.01": 1}}, 28),
        ]:
            try:
                collection.update_one({"_id": 0}, {"$set": {"atomic_marker": True}, **expression})
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("expected push rejection")
            assert BSON.encode(collection.find_one({"_id": 0})) == before
        rolled_back_batch_smoke(client.wire_push, collection, {"$push": {"keep": 1}}, 2)
        identity = client.wire_push.identity
        identity.insert_one({"_id": {"values": [1]}, "keep": True})
        try:
            identity.update_one({}, {"$push": {"_id.values": 2}})
        except WriteError as error:
            assert error.code == 66
        else:
            raise AssertionError("push must preserve identity")
        assert identity.find_one()["_id"] == {"values": [1]}
        queue = client.wire_push.concurrent
        queue.insert_one({"_id": 1, "values": []})
        def append(worker):
            return sum(queue.update_one({}, {"$push": {"values": worker * 8 + step}}).modified_count for step in range(8))
        with ThreadPoolExecutor(max_workers=4) as pool:
            assert sum(pool.map(append, range(4))) == 32
        assert sorted(queue.find_one()["values"]) == list(range(32))
        capped = client.wire_push.capped
        capped.insert_one({"_id": 1, "values": ["x" * 280000]})
        try:
            capped.update_one({}, {"$push": {"values": "y" * 280000}})
        except OperationFailure:
            pass
        else:
            raise AssertionError("oversized push must fail before commit")
        assert capped.find_one()["values"] == ["x" * 280000]
        assert capped.update_one({}, {"$push": {"values": {"$each": ["y" * 280000], "$slice": -1}}}).modified_count == 1
        assert capped.find_one()["values"] == ["y" * 280000]


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
        rolled_back_batch_smoke(client.wire_membership, collection, {"$addToSet": {"keep": 1}}, 2)
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
        rolled_back_batch_smoke(client.wire_pop_rename, collection, {"$pop": {"keep": 1}}, 14)
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
        # This failure is in the first shard and explicitly rolls back. Raw-wire
        # tests separately force later-shard failures after persisted changes.
        rolled_back_batch_smoke(client.wire_update_many, collection, {"$set": {"group.x": 1}}, 28)
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
            ({"$mul": {"v": 1}}, 115), ({"$set": 1}, 9),
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
        for options in [{"multi": "yes"}, {"upsert": 1}, {"arrayFilters": []}]:
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
            ({"update": {"$mul": {"value": 2}}}, 115), ({"update": [{"$set": {"value": 2}}]}, 115),
            ({"update": {}, "remove": True}, 72), ({"remove": True, "new": True}, 72),
            ({"remove": False}, 72), ({"update": {}, "upsert": 1}, 72),
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
            ({"q": {}, "u": {"$mul": {"v": 1}}}, 115),
            ({"q": {}, "u": [{"$set": {"v": 1}}]}, 115),
            ({"q": {}, "u": {}, "multi": True}, 72),
            ({"q": {}, "u": {}, "upsert": 1}, 72),
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


def index_removal_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        database = client.wire_index_drop
        collection = database.items
        collection.insert_many([{"_id": n, "value": n, "tail": n} for n in range(8)])
        collection.create_index("value", name="value_lookup")
        collection.create_index("tail", name="tail_lookup", sparse=True)
        database.keep.create_index("value")
        for name in ("_id", "_id_"):
            try:
                collection.drop_index(name)
            except OperationFailure as error:
                assert error.code == 72
            else:
                raise AssertionError("built-in ID index was dropped")
        for selector, code in [("missing", 27), (1, 14), (["value_lookup"], 14), ({"value": 1}, 14)]:
            try:
                database.command("dropIndexes", "items", index=selector)
            except OperationFailure as error:
                assert error.code == code
            else:
                raise AssertionError("invalid index removal was accepted")
        result = database.command("dropIndexes", "items", index="value_lookup")
        assert result["nIndexesWas"] == 3
        # Source-compatible unambiguous legacy field alias.
        assert collection.drop_index("tail") is None
        assert set(collection.index_information()) == {"_id_"}
        assert set(database.keep.index_information()) == {"_id_", "value_1"}
        collection.create_indexes([pymongo.IndexModel("value"), pymongo.IndexModel("tail")])
        cursor = database.command("listIndexes", "items", cursor={"batchSize": 1})["cursor"]
        assert cursor["id"]
        result = database.command("dropIndexes", "items", index="*")
        assert result["nIndexesWas"] == 3
        more = database.command("getMore", cursor["id"], collection="items")["cursor"]
        assert more["id"] == 0 and more["nextBatch"] == []
        assert collection.drop_indexes() is None
        assert collection.count_documents({}) == 8
        collection.insert_one({"_id": 8, "value": 8, "tail": 8})
        assert set(collection.index_information()) == {"_id_"}
        try:
            database.command("dropIndexes", "absent", index="*")
        except OperationFailure as error:
            assert error.code == 26
        else:
            raise AssertionError("missing namespace removal succeeded")
        assert "absent" not in database.list_collection_names()


async def async_index_removal_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.wire_index_drop.async_items
        await collection.create_indexes([pymongo.IndexModel("value"), pymongo.IndexModel("tail")])
        await collection.insert_one({"_id": 1, "value": 2, "tail": 3})
        assert await collection.drop_index("value_1") is None
        assert await collection.drop_indexes() is None
        assert set(await collection.index_information()) == {"_id_"}
        assert await collection.count_documents({}) == 1


def index_creation_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        database = client.wire_index_create
        collection = database.items
        collection.insert_many([{"_id": n, "value": n, "tail": 12 - n, "active": n % 2 == 0} for n in range(12)])
        assert collection.create_index("value") == "value_1"
        models = [
            pymongo.IndexModel([("value", 1), ("tail", -1)], name="compound", sparse=True),
            pymongo.IndexModel("tail", name="partial", partialFilterExpression={"active": True}),
            pymongo.IndexModel("_id"),
        ]
        assert collection.create_indexes(models) == ["compound", "partial", "_id_1"]
        for definitions, before, after in [
            ([{"key": {"value": 1}, "name": "value_1"}], 4, 4),
            ([{"key": {"_id": 1}, "name": "ignored"}], 4, 4),
            ([{"key": {"active": 1}, "name": "active"}, {"key": {"active": 1}, "name": "active"}], 4, 5),
        ]:
            result = database.command("createIndexes", "items", indexes=definitions)
            assert (result["numIndexesBefore"], result["numIndexesAfter"]) == (before, after)
        for definitions, code in [
            ([{"key": {"value": 1}, "name": "value_1", "unique": True}], 86),
            ([{"key": {"tail": 1}, "name": "value_1"}], 86),
            ([{"key": {"value": 1}, "name": "another"}], 85),
            ([{"key": {"_id": 1}, "unique": False}], 197),
            ([{"key": {"_id": 1}, "unique": True}], 197),
            ([{"key": {"new": 1}, "unique": True}], 115),
        ]:
            try:
                database.command("createIndexes", "items", indexes=definitions)
            except OperationFailure as error:
                assert error.code == code, (error.code, code)
            else:
                raise AssertionError("index conflict/options must fail")
        # Invalid late shapes are rejected before an implicit collection exists.
        for bad in [
            {"key": {"bad": "hashed"}}, {"key": {"bad": True}},
            {"key": {"bad": 1}, "expireAfterSeconds": 60},
            {"key": {"bad": 1}, "unique": 1},
            {"key": {"bad": 1}, "partialFilterExpression": {"bad": {"$unknown": 1}}},
            {"key": {"bad": 1}, "sparse": True, "partialFilterExpression": {"bad": 1}},
        ]:
            try:
                database.command("createIndexes", "absent", indexes=[{"key": {"good": 1}}, bad])
            except OperationFailure:
                pass
            else:
                raise AssertionError("invalid index batch was accepted")
            assert "absent" not in database.list_collection_names()
        # Runtime failure retains the completed prefix and leaves the root usable.
        try:
            database.command("createIndexes", "prefix", indexes=[
                {"key": {"value": 1}, "name": "first"},
                {"key": {"second": 1}, "unique": True},
            ])
        except OperationFailure as error:
            assert error.code == 115
        else:
            raise AssertionError("unsupported unique build succeeded")
        assert set(database.prefix.index_information()) == {"_id_", "first"}
        collection.update_one({"_id": 1}, {"$set": {"active": True, "value": 100}})
        collection.delete_one({"_id": 2})
        collection.insert_one({"_id": 12, "value": 12, "tail": 0, "active": True})
        assert collection.count_documents({}) == 12


async def async_index_creation_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.wire_index_create.async_items
        assert await collection.create_index("value") == "value_1"
        assert await collection.create_indexes([pymongo.IndexModel("tail"), pymongo.IndexModel("_id")]) == ["tail_1", "_id_1"]
        assert set(await collection.index_information()) == {"_id_", "tail_1", "value_1"}
        await collection.insert_one({"_id": 1, "value": 2, "tail": 3})


def index_metadata_smoke(uri):
    expected = [
        {"name": "_id_", "key": {"_id": 1}},
        {"name": "!before_id", "key": {"value": 1, "tail": -1}, "sparse": True},
        {"name": "partial", "key": {"value": 1, "tail": -1}, "partialFilterExpression": {"active": True}},
        {"name": "z", "key": {"value": 1, "tail": -1}},
    ]
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        database = client.wire_indexes
        assert list(database.items.list_indexes()) == expected
        information = database.items.index_information()
        assert information == {row["name"]: {**{key: value for key, value in row.items() if key not in ("name", "key")}, "key": list(row["key"].items())} for row in expected}
        before = database.list_collection_names()
        assert list(database.missing.list_indexes()) == []
        assert database.missing.index_information() == {}
        assert database.list_collection_names() == before
        cursor = database.command("listIndexes", "items", cursor={"batchSize": 0}, maxTimeMS=10000)["cursor"]
        assert cursor["id"] and cursor["ns"] == "wire_indexes.items" and cursor["firstBatch"] == []
        rows = []
        with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as peer:
            while cursor["id"]:
                cursor = peer.wire_indexes.command("getMore", cursor["id"], collection="items", batchSize=1)["cursor"]
                rows.extend(cursor["nextBatch"])
        assert rows == expected
        cursor = database.command("listIndexes", "items", cursor={"batchSize": 1})["cursor"]
        assert cursor["firstBatch"] == expected[:1]
        assert database.command("killCursors", "items", cursors=[cursor["id"]])["cursorsKilled"] == [cursor["id"]]
        try:
            database.command("getMore", cursor["id"], collection="items")
        except OperationFailure as error:
            assert error.code == 43
        else:
            raise AssertionError("killed index metadata cursor remained usable")


async def async_index_metadata_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        rows = [row async for row in await client.wire_indexes.items.list_indexes()]
        assert [row["name"] for row in rows] == ["_id_", "!before_id", "partial", "z"]
        assert (await client.wire_indexes.items.index_information())["partial"] == {"key": [("value", 1), ("tail", -1)], "partialFilterExpression": {"active": True}}
        assert [row async for row in await client.wire_indexes.missing.list_indexes()] == []


def persisted_smoke(uri):
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        assert set(client.wire_index_drop.items.index_information()) == {"_id_"}
        assert client.wire_index_drop.items.count_documents({}) == 9
        assert set(client.wire_index_drop.keep.index_information()) == {"_id_", "value_1"}
        assert set(client.wire_index_drop.async_items.index_information()) == {"_id_"}
        assert client.wire_index_drop.async_items.count_documents({}) == 1
        assert set(client.wire_index_create.items.index_information()) == {"_id_", "value_1", "compound", "partial", "active"}
        assert client.wire_index_create.items.count_documents({}) == 12
        assert client.wire_index_create.items.find_one({"_id": 1})["value"] == 100
        assert set(client.wire_index_create.prefix.index_information()) == {"_id_", "first"}
        assert set(client.wire_index_create.async_items.index_information()) == {"_id_", "tail_1", "value_1"}
        assert client.wire_operator_upsert.items.count_documents({}) == 4
        assert client.wire_operator_upsert.items.find_one({"_id": None}) == {"_id": None, "counter": 7, "stamp": Timestamp(0, 0)}
        assert client.wire_operator_upsert.concurrent.find_one() == {"_id": 999, "counter": 32}
        assert client.wire_pop_rename.queue.count_documents({"items": [], "keep": True}) == 12
        assert client.wire_pop_rename.items.find_one({"_id": 23})["nested"] == {"value": Binary(b"value", 128)}
        assert sorted(client.wire_membership.concurrent.find_one()["values"]) == list(range(4))
        assert client.wire_membership.items.find_one({"_id": 11})["nested"] == {"0": {"values": []}}
        assert client.wire_membership.capped.find_one()["values"] == ["x" * 280000]
        assert sorted(client.wire_push.concurrent.find_one()["values"]) == list(range(32))
        assert client.wire_push.items.find_one({"_id": 11})["values"] == [[4, 5], Timestamp(0, 0), Binary(b"value", 128)]
        assert client.wire_push.capped.find_one()["values"] == ["y" * 280000]
        assert client.wire_pull.concurrent.find_one()["values"] == []
        assert isinstance(client.wire_increment.concurrent.find_one()["counter"], Int64)
        assert client.wire_increment.concurrent.find_one()["counter"] == 32
        assert client.wire_replace_upsert.items.count_documents({}) == 4
        assert client.wire_replace_upsert.items.find_one({"_id": None}) == {"_id": None, "value": 7}
        assert client.wire_replace_upsert.concurrent.count_documents({}) == 1
        assert client.wire_replace_upsert.bounded.count_documents({}) == 5
        assert client.wire_increment.items.find_one({"_id": 7})["counter"] == 4
        assert client.wire_increment.items.find_one()["amount"].bid == Decimal128("2.100000000000000").bid
        assert client.wire_pull.items.find_one({"_id": 11})["values"] == [True, {"_id": 3, "x": 3}, "beta"]
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


async def async_operator_upsert_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=15000) as client:
        collection = client.wire_async_operator_upsert.items
        for method, identifier in [(collection.update_one, None), (collection.update_many, Int64(2))]:
            result = await method({"_id": identifier, "counter": 3}, {"$inc": {"counter": 2}, "$set": {"stamp": Timestamp(0, 0)}}, upsert=True)
            assert result.did_upsert and result.modified_count == 0
            assert BSON.encode(await collection.find_one({"_id": identifier})) == BSON.encode({"_id": identifier, "counter": 5, "stamp": Timestamp(0, 0)})
            result = await method({"_id": identifier}, {"$inc": {"counter": 0}}, upsert=True)
            assert (result.matched_count, result.modified_count, result.did_upsert) == (1, 0, False)
        result = await collection.update_many({"tag": "generated"}, {"$set": {"stamp": Timestamp(0, 0)}}, upsert=True)
        assert isinstance(result.upserted_id, ObjectId)


async def async_replacement_upsert_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_replace_upsert.items
        result = await collection.replace_one({"_id": {"$eq": None}}, {"value": Int64(1)}, upsert=True)
        assert result.did_upsert and result.upserted_id is None
        assert (result.matched_count, result.modified_count) == (1, 0)
        result = await collection.replace_one({"_id": None}, {"value": Int64(2)}, upsert=True)
        assert (result.matched_count, result.modified_count, result.did_upsert) == (1, 1, False)
        assert BSON.encode(await collection.find_one()) == BSON.encode({"_id": None, "value": Int64(2)})
        generated = await collection.replace_one({"missing": True}, {}, upsert=True)
        assert isinstance(generated.upserted_id, ObjectId) and generated.did_upsert


async def async_increment_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_increment.items
        await collection.insert_many([{"_id": i, "amount": Decimal128("1.00")} for i in range(4)])
        result = await collection.update_many({}, {"$inc": {"counter": Int64(1), "amount": Decimal128("2.5")}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert (await collection.update_many({}, {"$inc": {"counter": 0, "amount": Decimal128("0E-100")}})).modified_count == 0
        image = await collection.find_one_and_update({}, {"$inc": {"counter": 1}}, sort=[("_id", -1)], projection={"counter": 1, "_id": 0}, return_document=True)
        assert BSON.encode(image) == BSON.encode({"counter": Int64(2)})
        assert (await collection.find_one())["amount"].bid == Decimal128("3.50").bid
        for expression, code in [({"$inc": {"counter": True}}, 14), ({"$inc": {"_id": 1}}, 66)]:
            try:
                await collection.update_many({}, expression)
            except WriteError as error:
                assert error.code == code
            else:
                raise AssertionError("async increment errors must be driver WriteErrors")


async def async_pull_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_pull.items
        await collection.insert_many([{"_id": i, "values": [Int64(1), [1, 2], True, {"x": 3}]} for i in range(4)])
        result = await collection.update_many({}, {"$pull": {"values": {"$eq": 1}}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert (await collection.update_one({}, {"$pull": {"missing": 1}})).modified_count == 0
        assert await collection.find_one_and_update({}, {"$pull": {"values": {"x": {"$gte": 3}}}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}, return_document=True) == {"values": [True]}
        for expression, code in [({"$pull": {"_id": 1}}, 2), ({"$pull": {"values": {"$expr": {"$eq": [1, 1]}}}}, 224)]:
            try:
                await collection.update_many({}, expression)
            except WriteError as error:
                assert error.code == code
            else:
                raise AssertionError("async pull error must be a WriteError")


async def async_push_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_push.items
        await collection.insert_many([{"_id": i, "values": [Int64(3)]} for i in range(4)])
        result = await collection.update_many({}, {"$push": {"values": {"$each": [2, 1], "$sort": 1}}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert (await collection.update_one({}, {"$push": {"values": {"$each": []}}})).modified_count == 0
        assert await collection.find_one_and_update({}, {"$push": {"values": {"$each": [4], "$slice": -2}}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}, return_document=True) == {"values": [Int64(3), 4]}
        try:
            await collection.update_many({}, {"$push": {"_id": 1}})
        except WriteError as error:
            assert error.code == 2
        else:
            raise AssertionError("async confirmed rollback must be a WriteError")


async def async_array_membership_smoke(uri):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        collection = client.async_membership.items
        await collection.insert_many([{"_id": i, "values": [Int64(1), True]} for i in range(4)])
        assert (await collection.update_one({"_id": 0}, {"$addToSet": {"values": 1.0}})).modified_count == 0
        result = await collection.update_many({}, {"$addToSet": {"values": {"$each": [2, 2.0, [1, 2]]}}})
        assert (result.matched_count, result.modified_count) == (4, 4)
        assert await collection.find_one_and_update({}, {"$pullAll": {"values": [1, [1, 2]]}}, sort=[("_id", -1)], projection={"values": 1, "_id": 0}, return_document=True) == {"values": [True, 2]}
        try:
            await collection.update_many({}, {"$addToSet": {"_id": 1}})
        except WriteError as error:
            assert error.code == 2
        else:
            raise AssertionError("async confirmed rollback must be a WriteError")
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
    index_metadata_smoke(sys.argv[1])
    asyncio.run(async_index_metadata_smoke(sys.argv[1]))
    if len(sys.argv) > 2 and sys.argv[2] == "reopened":
        persisted_smoke(sys.argv[1])
    else:
        index_creation_smoke(sys.argv[1])
        asyncio.run(async_index_creation_smoke(sys.argv[1]))
        index_removal_smoke(sys.argv[1])
        asyncio.run(async_index_removal_smoke(sys.argv[1]))
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
        push_smoke(sys.argv[1])
        pull_smoke(sys.argv[1])
        increment_smoke(sys.argv[1])
        replacement_upsert_smoke(sys.argv[1])
        operator_upsert_smoke(sys.argv[1])
        find_upsert_smoke(sys.argv[1])
        find_replace_smoke(sys.argv[1])
        # Give the added operator cases their own bounded phase; retain the
        # existing discovery/CRUD phase's deadline as the suite grows.
        asyncio.run(asyncio.wait_for(async_pop_rename_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_array_membership_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_push_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_pull_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_increment_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_replacement_upsert_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_operator_upsert_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_find_upsert_smoke(sys.argv[1]), timeout=20))
        asyncio.run(asyncio.wait_for(async_smoke(sys.argv[1]), timeout=20))
    print("PyMongo 4.17.0 discovery, insert batches, filtered/cursor reads, BSON, and rejection passed")
