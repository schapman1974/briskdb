"""Generate BSON sorting cases from the source-locked TinyMongo oracle."""

import hashlib
import itertools
import random
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.aggregation as aggregation
import tinymongo.bson_types as bson_types
import tinymongo.sorting as sorting
from tinymongo.errors import OperationFailure, TinyMongoNotSupportedError


def emit(documents, spec):
    envelope = BSON(BSON.encode({"documents": documents, "sort": spec})).decode()
    documents, spec = envelope["documents"], envelope["sort"]
    before = BSON.encode(envelope)
    case = dict(envelope)
    try:
        normalized = aggregation.AggregationEngine()._validate_sort(spec)
        result = sorting.sort_documents(documents, normalized)
        case["result"] = [row["_id"] for row in result]
        assert BSON.encode(envelope) == before
    except TinyMongoNotSupportedError:
        case["error"] = 115
    except OperationFailure as error:
        case["error"] = error.code
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    for module, digest in [
        (sorting, "76d7d9174d598c7319bcf001fcee0717544b26b132f1636a18dc1f06c8b4b9a6"),
        (aggregation, "3ad7c2bc6af69083559f163b0cc037977c7bd8aa64523b2c63c982cb58532cad"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [
        None, False, True, -10, 0, 1, Int64(1), Int64(2**63 - 1),
        1.0, 1.25, float("nan"), float("inf"), float("-inf"), -0.0,
        Decimal128("0E-6000"), Decimal128("1E-6000"), Decimal128("NaN"),
        Decimal128("-Infinity"), Decimal128("1.00000000000000000001"),
        "", "hello", {"a": 1, "b": 2}, {"b": Int64(2), "a": 1},
        [], [1, 2], [9, 1, 5], [[2, 1]], [[], [1]],
        [{"a": 1}, None, {"b": 2}, {}], [[{"a": 9}]],
        Binary(b"\x00\xff", 128), Binary(b"abc", 0), Binary(bytes(16), 4),
        ObjectId("64b000000000000000000001"), Timestamp(7, 2), datetime(2020, 1, 1),
        MinKey(), MaxKey(), Regex("Ab.c", "im"), Code("return 1"), Code("return x", {"x": 1}),
    ]
    docs = [{"_id": index, "v": value, "tie": index % 3} for index, value in enumerate(values)]
    docs.extend([{"_id": len(docs)}, {"_id": len(docs) + 1, "v": None}])
    for field in ["v", "v.a", "v.0", "v.1", "v.01", "v.-1", "v.9.a", "v.999999999999999999999999999999"]:
        for direction in (1, -1):
            for ordered in (docs, list(reversed(docs))):
                for spec in [{field: direction}, {field: direction, "tie": 1}, {field: direction, "_id": -1}]:
                    emit(ordered, spec)
    # Explicit provenance and error-precedence fixtures, including array paths
    # that branch only in distinct elements of the same parent array.
    members = [
        {}, {"x": 1, "y": 9}, {"x": 2, "y": 8}, {"x": 1}, {"y": 0},
        {"x": [2, 1], "y": 3}, {"x": 3, "y": [2, 1]},
        {"x": [2], "y": [1]}, {"0": 7, "x": 3},
        {"8": 1, "01": 2}, {"x": {"z": [3, 1]}, "y": 8},
        None, 1, [], [{"x": 3}],
    ]
    specs = [
        {"v.x": 1, "v.y": 1}, {"v.x": 1, "v.y": -1},
        {"v.x": -1, "v.y": 1}, {"v.x": -1, "v.y": -1},
        {"v.0": 1, "v.1": 1}, {"v.0.x": 1, "v.x": 1},
        {"v.8": 1, "v.x": -1}, {"v.x": 1, "v": 1},
        {"v.x.z": 1, "v.y": -1}, {"v.0": 1, "other": 1},
    ]
    for left, right in itertools.product(members, repeat=2):
        rows = [
            {"_id": 0, "v": [left, right], "other": [1]},
            {"_id": 1, "v": [right, left], "other": [2]},
            {"_id": 2, "v": [{"x": 1, "y": 5}]},
        ]
        for spec in specs:
            emit(rows, spec)
    randomizer = random.Random(172)
    for _ in range(250):
        rows = [{"_id": index, "v": [randomizer.choice(members) for _ in range(randomizer.randrange(5))]}
                for index in range(8)]
        for spec in specs[:9]:
            emit(rows, spec)
    # Eager specification validation applies even to empty input. Validation
    # here intentionally uses the shared ordinary numeric sort contract.
    invalid = [{}, {f"f{i}": 1 for i in range(33)}]
    for field in ["", ".", "a.", "a..b", "$a", "a.$v", "a.$id", "a.01", "a.²"]:
        invalid.append({field: 1})
    for direction in [None, "1", True, False, [], {}, {"$meta": "textScore"},
                      0, 2, -2, 1.5, float("nan"), float("inf"),
                      1.0, Int64(-1), Decimal128("1.000"), Decimal128("-1"), Decimal128("NaN")]:
        invalid.append({"v": direction})
    for spec in invalid:
        emit([], spec)
        emit(docs, spec)


if __name__ == "__main__":
    main()
