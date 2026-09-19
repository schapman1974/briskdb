"""Source-locked field-update oracle with explicit per-operator boundaries.

TinyMongo's legacy set/unset helpers do not implement Mongo numeric array paths
and silently overwrite scalar set parents/restore changed IDs. Those unsafe or
different cases are deliberately outside this matrix, not rewritten or waived.
Rust unit, transaction, and wire tests check those set/unset boundaries
independently. Min/max, pop, and rename additionally cover whole BSON ordering,
numeric array paths, blocked parents, and immutable IDs using the unchanged
reference helpers. Add-to-set uses non-ID object paths only because its legacy
reference helper shares the old set-path behavior. Pull-all covers non-ID numeric
paths; both legacy membership helpers restore changed IDs silently. Strict
add-to-set paths and immutable IDs are tested independently, not waived.
"""
import hashlib
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.tinymongo as reference
import tinymongo.bson_codec as bson_codec
import tinymongo.bson_types as bson_types
from tinymongo.errors import OperationFailure


def emit(document, update):
    document = BSON(BSON.encode(document)).decode()
    update = BSON(BSON.encode(update)).decode()
    before = BSON.encode(document)
    case = {"document": document, "update": update}
    try:
        reference._validate_update_document(update)
        case["result"] = reference._apply_update_document(document, update)
    except OperationFailure as error:
        case["error"] = error.code
    assert BSON.encode(document) == before
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    for module, digest in [
        (reference, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (bson_codec, "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [None, False, True, 0, -1, Int64(1), Int64(2**63 - 1),
              1.0, -0.0, float("nan"), Decimal128("NaN"), Decimal128("0E-6000"),
              "$literal", "", [], [1, {"x": 2}], {"x": Int64(1)},
              Binary(b"data", 128), ObjectId("64b000000000000000000001"),
              Timestamp(0, 0), Regex("abc", "i"), Code("return x", {"x": 1})]
    randomizer = random.Random(171)
    for _ in range(1000):
        value, next_value = randomizer.choice(values), randomizer.choice(values)
        document = {"_id": Int64(7), "keep": value, "v": value,
                    "nested": {"leaf": value, "keep": next_value}}
        for update in [
            {"$set": {"v": next_value}},
            {"$set": {"nested.leaf": next_value, "missing.0.value": value}},
            {"$unset": {"nested.leaf": next_value, "absent.child": value}},
            {"$set": {"new": next_value}, "$unset": {"v": value}},
        ]:
            emit(document, update)
    for update in [{"$set": {}}, {"$unset": {}},
                   {"$set": {"a": 1, "a.b": 2}},
                   {"$set": {"a.b": 1}, "$unset": {"a": 1}},
                   {"$set": {"a..b": 1}}, {"$unset": {"a.": 1}},
                   {"$set": 1}, {"$unset": []}]:
        emit({"_id": 1, "v": 2}, update)

    comparison_values = values + [
        MinKey(), MaxKey(), float("-inf"), float("inf"),
        Decimal128("Infinity"), Decimal128("-Infinity"), Decimal128("-0"),
        Decimal128("1.000"), Int64(-(2**63)),
        datetime(2026, 1, 2, tzinfo=timezone.utc), Code("return x"), Binary(b"data", 0),
    ]
    assert len(comparison_values) == 34
    for operator in ["$min", "$max"]:
        for current in comparison_values:
            for candidate in comparison_values:
                emit({"_id": Int64(7), "keep": current, "v": current,
                      "nested": {"leaf": current}},
                     {operator: {"v": candidate, "nested.leaf": candidate}})
                emit({"_id": Int64(7), "a": [current]},
                     {operator: {"a.0": candidate, "a.3": candidate}})
            emit({"_id": 7}, {operator: {"missing.0.value": current}})
        for document, path, candidate in [
            ({"_id": 5}, "_id", 5.0),
            ({"_id": 5}, "_id", 4 if operator == "$min" else 6),
            ({"_id": 5, "a": 1}, "a.x", 1),
            ({"_id": 5, "a": None}, "a.x", 1),
            ({"_id": 5, "a": [1]}, "a.x", 1),
            ({"_id": 5, "a": [1]}, "a.01", 1),
            ({"_id": 5, "a": [1]}, "a.-1", 1),
            ({"_id": 5, "a": [1]}, "a.2.x", 1),
            ({"_id": 5, "a": [None]}, "a.0.x", 1),
        ]:
            emit(document, {operator: {path: candidate}})
        for update in [{operator: {}}, {operator: []}, {operator: {"a..b": 1}}]:
            emit({"_id": 7}, update)
    for update in [
        {"$min": {"a": 1}, "$max": {"a": 3}},
        {"$min": {"a": 1}, "$set": {"a.b": 3}},
        {"$max": {"a.b": 1}, "$unset": {"a": 1}},
    ]:
        emit({"_id": 7}, update)


    for _ in range(1000):
        items = [randomizer.choice(values) for _ in range(randomizer.randrange(6))]
        document = {"_id": 7, "front": items, "grid": [items], "keep": True}
        for direction in [-1, 1]:
            emit(document, {"$pop": {"front": direction, "grid.0": direction,
                                     "missing.child": direction, "grid.3": direction}})
        value, other = randomizer.choice(values), randomizer.choice(values)
        emit({"_id": 7, "source": value, "dest": other,
              "nested": {"old": other, "keep": value}, "tail": True},
             {"$rename": {"source": "dest", "nested.old": "new.value", "absent": "tail"}})
    for operand in [1, -1, Int64(1), -1.0, Decimal128("1.00"), Decimal128("-1"),
                    0, 2, 1.5, True, "1", None, [], {}, float("nan"), Decimal128("Infinity")]:
        emit({"_id": 7, "v": [1, 2, 3]}, {"$pop": {"v": operand}})
    for value in values:
        emit({"_id": 7, "v": value}, {"$pop": {"v": 1}})
    for document, path in [
        ({"_id": 7, "v": 1}, "v.x"),
        ({"_id": 7, "v": None}, "v.x"),
        ({"_id": 7, "v": [None]}, "v.0.x"),
        ({"_id": 7, "v": [[1, 2]]}, "v.0"),
        ({"_id": 7, "v": [[1, 2]]}, "v.3"),
        ({"_id": 7, "v": [[1, 2]]}, "v.99999999999999999999999"),
        ({"_id": 7, "v": [1]}, "v.01"),
        ({"_id": 7, "v": [1]}, "v.x"),
        ({"_id": 7, "v": [1]}, "v.-1"),
        ({"_id": 7}, "absent.x"),
        ({"_id": 7, "v": {}}, "v.missing"),
        ({"_id": {"list": [1, 2]}}, "_id.list"),
    ]:
        emit(document, {"$pop": {path: 1}})
    for document, source, target in [
        ({"_id": 7, "a": []}, "absent", "a.0"),
        ({"_id": 7, "a": 1}, "a.x", "new"),
        ({"_id": 7, "a": []}, "a.0", "new"),
        ({"_id": 7, "a": []}, "a.x", "new"),
        ({"_id": 7, "a": [], "v": 1}, "v", "a.0"),
        ({"_id": 7, "a": [], "v": 1}, "v", "a.x"),
        ({"_id": 7, "a": None, "v": 1}, "v", "a.x"),
        ({"_id": 7}, "absent", "_id"),
        ({"_id": 7, "v": 7}, "v", "_id"),
        ({"_id": 7}, "_id", "new"),
        ({"_id": {}}, "_id.missing", "new"),
        ({"_id": {"x": 1}}, "_id.x", "new"),
        ({"_id": 7}, "a", "a"),
        ({"_id": 7}, "a", "a.b"),
        ({"_id": 7}, "a.b", "a"),
        ({"_id": 7}, "a", 1),
        ({"_id": 7}, "a", ""),
        ({"_id": 7}, "a", "b..c"),
        ({"_id": 7}, "a", "b.$[].c"),
        ({"_id": 7}, "a.$[].b", "new"),
        ({"_id": 7}, "a..b", "new"),
        ({"_id": 7, "a": [1, 2]}, "a", "new"),
        ({"_id": 7, "a": None}, "a", "new.value"),
        ({"_id": 7, "a": {"x": 1}}, "a.x", "a.y"),
    ]:
        emit(document, {"$rename": {source: target}})
    for update in [
        {"$pop": {}, "$rename": {}},
        {"$rename": {"a": "b", "b": "a"}},
        {"$rename": {"a": "b"}, "$set": {"b": 1}},
        {"$pop": {"a": 1}, "$unset": {"a.0": 1}},
    ]:
        emit({"_id": 7}, update)

    for current in comparison_values:
        for candidate in comparison_values:
            document = {"_id": Int64(7), "v": [current, current],
                        "nested": {"v": [current]}, "keep": current}
            emit(document, {"$addToSet": {"v": candidate, "nested.v": candidate}})
            emit(document, {"$pullAll": {"v": [candidate], "nested.v": [candidate]}})
        emit({"_id": 7}, {"$addToSet": {"missing.0.v": current}})
        for operator, operand in [("$addToSet", 1), ("$pullAll", [1])]:
            emit({"_id": 7, "v": current}, {operator: {"v": operand}})
    for _ in range(1000):
        items = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(7))]
        candidates = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(7))]
        emit({"_id": 7, "v": items, "nested": {"v": items}},
             {"$addToSet": {"v": {"$each": candidates}, "nested.v": {"$each": candidates},
                            "new.v": {"$each": candidates}}})
        emit({"_id": 7, "v": items, "grid": [items]},
             {"$pullAll": {"v": candidates, "grid.0": candidates,
                           "grid.3": candidates, "absent.v": candidates}})
    for update in [
        {"$addToSet": {}}, {"$pullAll": {}},
        {"$addToSet": {"new": {"$each": []}}},
        {"$addToSet": {"v": {"$each": 1}}},
        {"$addToSet": {"v": {"$each": [], "$sort": 1}}},
        {"$addToSet": {"v": {"$each": [], "extra": 1}}},
        {"$addToSet": {"v": {"$unknown": 1}}},
        {"$pullAll": {"v": 1}}, {"$pullAll": {"v": None}},
        {"$pullAll": {"v": {"$each": []}}},
        {"$addToSet": {"v": 1}, "$pullAll": {"v": []}},
        {"$pullAll": {"v": []}, "$set": {"v.x": 1}},
        {"$addToSet": {"v..x": 1}}, {"$pullAll": {"v..x": []}},
        {"$addToSet": []}, {"$pullAll": []},
    ]:
        emit({"_id": 7, "v": [True, 1, 1.0]}, update)
    for document, path, candidates in [
        ({"_id": 7}, "missing.x", [None]),
        ({"_id": 7, "v": 1}, "v.x", []),
        ({"_id": 7, "v": None}, "v.x", []),
        ({"_id": 7, "v": [None]}, "v.0.x", []),
        ({"_id": 7, "v": [[1, 2]]}, "v.0", [1.0]),
        ({"_id": 7, "v": [[1, 2]]}, "v.3", [1]),
        ({"_id": 7, "v": [[1, 2]]}, "v.99999999999999999999999", [1]),
        ({"_id": 7, "v": [1]}, "v.01", []),
        ({"_id": 7, "v": [1]}, "v.x", []),
        ({"_id": 7, "v": [1]}, "v.-1", []),
    ]:
        emit(document, {"$pullAll": {path: candidates}})


if __name__ == "__main__":
    main()
