"""Compare whole basic pipelines with the frozen implementation, not stage replicas."""

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
import tinymongo.table_backends as backend
from tinymongo.errors import OperationFailure, TinyMongoNotSupportedError


def emit(documents, pipeline):
    envelope = BSON(BSON.encode({"documents": documents, "pipeline": pipeline})).decode()
    before = BSON.encode(envelope)
    case = dict(envelope)
    try:
        case["result"] = aggregation.AggregationEngine().run(envelope["documents"], envelope["pipeline"])
    except TinyMongoNotSupportedError:
        case["error"] = 115
    except OperationFailure as error:
        # The frozen generic stage-shape/$match errors have no numeric code.
        # The typed Rust core uses BadValue (2) for that otherwise code-less class.
        case["error"] = 2 if error.code is None else error.code
    assert BSON.encode(envelope) == before
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    for module, digest in [
        (aggregation, "3ad7c2bc6af69083559f163b0cc037977c7bd8aa64523b2c63c982cb58532cad"),
        (sorting, "76d7d9174d598c7319bcf001fcee0717544b26b132f1636a18dc1f06c8b4b9a6"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
        (backend, "b16dbc8c435a639d85c29d857f8487b2c88d2eef10969a9e412d8afce02898a1"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [
        None, False, True, -10, 0, 1, Int64(1), Int64(2**63 - 1),
        1.0, 1.25, float("nan"), float("inf"), float("-inf"), -0.0,
        Decimal128("1.00"), Decimal128("0E-6000"), Decimal128("1E-6000"), Decimal128("NaN"),
        Decimal128("-Infinity"), Decimal128("1.00000000000000000001"),
        "", "hello", {"a": 1, "b": 2}, {"b": Int64(2), "a": 1},
        [], [1, 2], [9, 1, 5], [[2, 1]], [[], [1]],
        [{"a": 1}, None, {"b": 2}, {}], [[{"a": 9}]],
        Binary(b"\x00\xff", 128), Binary(b"abc", 0), Binary(bytes(16), 4),
        ObjectId("64b000000000000000000001"), Timestamp(7, 2), datetime(2020, 1, 1),
        MinKey(), MaxKey(), Regex("Ab.c", "im"), Code("return 1"), Code("return x", {"x": 1}),
    ]
    documents = [{"_id": Int64(index), "v": value, "group": index % 3}
                 for index, value in enumerate(values)]
    documents.extend([{"_id": Int64(len(documents))}, {"_id": Int64(len(documents) + 1), "v": None}])
    for source in [[], documents, list(reversed(documents))]:
        emit(source, [])
        # All stage orderings, including count feeding match/sort/count again.
        for stages in itertools.permutations([
            {"$match": {"group": 1}}, {"$sort": {"v": -1}},
            {"$skip": 1}, {"$limit": 3}, {"$count": "n"}, {"$count": "total"},
        ]):
            emit(source, list(stages))
        for path in ["v", "v.a", "v.0", "v.01", "v.9.a", "v.999999999999999999999999999999"]:
            for direction in (1, -1):
                emit(source, [{"$sort": {"_id": -1}}, {"$sort": {path: direction}}, {"$skip": 1}, {"$limit": 20}])
                emit(source, [{"$match": {path: {"$exists": True}}}, {"$sort": {path: direction}}, {"$count": "n"}])
    numbers = [
        None, True, False, "1", Code("1"), [], {}, -1, 0, 1, 2, Int64(2**63 - 1),
        1.0, 1.5, -0.0, float("nan"), float("inf"), float("-inf"), float(2**63),
        Decimal128("1.000"), Decimal128("-0E-6000"), Decimal128("1E-6000"),
        Decimal128("1E+6000"), Decimal128("1.00000000000000000001"),
        Decimal128("9223372036854775807"), Decimal128("9223372036854775808"),
        Decimal128("NaN"), Decimal128("sNaN"), Decimal128("Infinity"),
    ]
    invalid = [{}, {"match": {}}, {"$skip": 0, "$limit": 1}, {"$unsupported-private": None}]
    for name in ("$skip", "$limit"):
        invalid.extend({name: value} for value in numbers)
    for field in [None, True, 1, {}, [], Code("n"), "", "$n.\0", "n.\0", "n.x", "_id", "n", "😀"]:
        invalid.append({"$count": field})
    for spec in [None, [], "v", {}, {"v": 0}, {"v": True}, {"v.": 1}, {"v": {"$meta": "textScore"}},
                 {f"f{i}": 1 for i in range(33)}, {"v": Decimal128("-1")}, {"v": Decimal128("1.00")}]:
        invalid.append({"$sort": spec})
    for spec in [None, [], "v", {"$where": "private"}, {"v": {"$unknown": 1}},
                 {"$or": [{}, {"v": {"$size": "invalid"}}]}, {"v": {"$regex": "["}}]:
        invalid.append({"$match": spec})
    for stage in invalid:
        for source in [[], documents]:
            emit(source, [stage])
            emit(source, [{"$skip": Int64(2**63 - 1)}, stage])
    # Parallel arrays and ambiguous numeric paths must retain the shared sort
    # errors; a preceding empty-producing stage must remove those input rows.
    for source in [
        [{"a": [1, 2], "b": [3, 4]}],
        [{"a": [{"0": 1}, 2]}],
        [{"a": [{"x": 1, "y": [2]}, {"x": [3], "y": 4}]}],
    ]:
        for spec in [{"a": 1, "b": 1}, {"a.0": 1}, {"a.x": 1, "a.y": -1}]:
            for prefix in [[], [{"$skip": 20}], [{"$match": {"missing": {"$exists": True}}}]]:
                emit(source, prefix + [{"$sort": spec}])
    randomizer = random.Random(179)
    choices = [
        {"$match": {}}, {"$match": {"group": 1}}, {"$match": {"v": {"$ne": None}}},
        {"$match": {"v.a": {"$in": [1, None]}}}, {"$match": {"v": {"$type": "number"}}},
        {"$sort": {"v": 1}}, {"$sort": {"v": -1, "_id": 1}}, {"$sort": {"v.a": 1}},
        {"$sort": {"group": 1}}, {"$sort": {"n": -1}},
        {"$skip": 0}, {"$skip": 1}, {"$skip": 20}, {"$limit": 1}, {"$limit": 5},
        {"$count": "n"}, {"$count": "again"}, {"$match": {"n": {"$gte": 2}}},
    ]
    for _ in range(2500):
        source = randomizer.sample(documents, randomizer.randrange(16))
        stages = [randomizer.choice(choices) for _ in range(randomizer.randrange(9))]
        emit(source, stages)


if __name__ == "__main__":
    main()
