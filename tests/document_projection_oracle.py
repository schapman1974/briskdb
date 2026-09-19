"""Generate BSON projection cases from the source-locked TinyMongo oracle."""

import hashlib
import random
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.bson_types as bson_types
import tinymongo.projection as projection
from tinymongo.errors import OperationFailure, TinyMongoNotSupportedError


def emit(document, spec):
    document = BSON(BSON.encode(document)).decode()
    spec = BSON(BSON.encode(spec)).decode()
    case = {"document": document, "projection": spec}
    before = BSON.encode(document)
    try:
        normalized = projection.normalize_projection(spec)
        case["result"] = projection.project_document(document, normalized)
        assert BSON.encode(document) == before
    except TinyMongoNotSupportedError:
        case["error"] = 115
    except OperationFailure as error:
        case["error"] = error.code
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    for module, digest in [
        (projection, "2027ad33c0df266d968cbad07b96c84f3e873560e241c1253ed69e87156e6035"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [
        None, False, True, -10, 0, 2, Int64(7), Int64(2**63 - 1),
        1.25, float("nan"), float("inf"), -0.0, Decimal128("0E-6000"),
        Decimal128("1E-6000"), Decimal128("NaN"), "", "hello",
        {"a": 1, "b": 2}, {"b": Int64(2), "a": 1}, [], [1, 2],
        [[{"a": 1}, 2], {"a": [1, 2]}], [{"a": 1}, None, {"b": 2}, {}],
        Binary(b"\x00\xff", 128), ObjectId("64b000000000000000000001"),
        Timestamp(7, 2), datetime(2020, 1, 1), MinKey(), MaxKey(),
        Regex("Ab.c", "im"), Code("return 1"), Code("return x", {"x": 1}),
    ]
    specs = [
        {}, {"v": 1}, {"v": 0}, {"v": 1, "_id": 0}, {"v": 0, "_id": 1},
        {"_id": 0}, {"_id": 1}, {"missing": 1}, {"missing": 0},
        {"v.a": 1}, {"v.a": 0}, {"v.a": 1, "v.b": 1},
        {"v": {"a": 1}}, {"v": {"a": 0, "b": False}},
        {"v.a.b": 1}, {"_id.a": 1}, {"_id.a": 0},
        {"v": 1, "secret": 0}, {"v": 0, "secret": 1},
        {"v": 1, "v.a": 1}, {"v.a": 1, "v": 1},
        {"v": {"a": 1}, "v.a": 1}, {"_id": 0, "_id.a": 0},
    ]
    for flag in [False, True, 0, Int64(0), 2, -2, 0.0, -0.0, 0.1,
                 Decimal128("-0"), Decimal128("1E-6000"), Decimal128("NaN"),
                 float("nan"), float("inf"), None, "1", [], {"$slice": 2}, {}]:
        specs.append({"v": flag})
    for field in ["", ".", "a..b", "a.", "$v", "a.$", "a.$[]", "a.0", "a.01",
                  "a.-1", "a.--1", "a.+1", "a.²", "a.١", "a.½", "a.Ⅻ", "a.1\n"]:
        specs.append({field: 1})
    # Shape normalization precedes mode/collision validation.
    specs.extend([
        {"v": 1, "secret": 0, "later": None},
        {"v": 1, "v.a": 1, "later": {}},
    ])
    documents = [{}]
    for value in values:
        documents.extend([
            {"before": Int64(1), "_id": 7, "v": value, "secret": "x", "after": 2},
            {"v": value, "_id": {"a": value, "other": "kept"}},
        ])
    for document in documents:
        for spec in specs:
            emit(document, spec)

    randomizer = random.Random(172)
    members = [None, 1, "s", {"a": 1, "b": 2}, {"a": None}, {},
               {"a": {"b": 7, "c": 8}}, [{"a": 9}, None], []]
    for _ in range(150):
        document = {"_id": 1, "v": [randomizer.choice(members) for _ in range(randomizer.randrange(6))]}
        for spec in [{"v.a": 1}, {"v.a": 0}, {"v.a.b": 1}, {"v.a.b": 0},
                     {"v": {"a": 1, "b": 1}}, {"v.a": 1, "_id": 0}]:
            emit(document, spec)


if __name__ == "__main__":
    main()
