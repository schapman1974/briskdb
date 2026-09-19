"""Source-locked field-update oracle with explicit per-operator boundaries.

TinyMongo's legacy set/unset helpers do not implement Mongo numeric array paths
and silently overwrite scalar set parents/restore changed IDs. Those unsafe or
different cases are deliberately outside this matrix, not rewritten or waived.
Rust unit, transaction, and wire tests check those set/unset boundaries
independently. Min/max additionally cover whole BSON ordering, numeric array
paths, blocked parents, and immutable IDs using the unchanged reference helpers.
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


if __name__ == "__main__":
    main()
