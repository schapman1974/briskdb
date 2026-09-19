"""Equality partitions, membership and failures from unchanged index helpers.

Reference token strings are opaque: stable first-encounter IDs expose their exact
equality classes and order, without translating or reimplementing their format.
No expected token, source, corpus case or allowance is modified.
"""

import hashlib
import itertools
import random
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.bson_codec as codec
import tinymongo.bson_types as types
import tinymongo.indexes as reference
from tinymongo.errors import TinyMongoNotSupportedError


def emit(keys, documents, sparse=False, partial=None):
    envelope = BSON(BSON.encode({
        "keys": keys, "documents": documents, "sparse": sparse, "partial": partial,
    })).decode()
    before = BSON.encode(envelope)
    try:
        spec = reference.IndexSpec(keys=list(envelope["keys"].items()),
                                   sparse=sparse, partial_filter=envelope["partial"])
    except (TinyMongoNotSupportedError, ValueError, TypeError):
        result = {"compile_error": True}
    else:
        seen = {}
        expected = []
        for document in envelope["documents"]:
            try:
                tokens = reference.index_entry_tokens(document, spec)
            except TinyMongoNotSupportedError:
                expected.append(None)
            else:
                expected.append([seen.setdefault(token, len(seen)) for token in tokens])
        result = {"compile_error": False, "expected": expected}
    assert BSON.encode(envelope) == before
    envelope.update(result)
    sys.stdout.buffer.write(BSON.encode(envelope))


def main():
    for module, digest in [
        (reference, "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6"),
        (types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
        (codec, "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [
        None, False, True, -1, 0, 1, Int64(1), Int64(2**63 - 1), 1.0, 1.25, -0.0,
        0.1, Decimal128("0.1"), Decimal128("1.00"), Decimal128("0E-6000"),
        Decimal128("1E-6000"), Decimal128("1.00000000000000000001"),
        float("nan"), float("inf"), float("-inf"), Decimal128("NaN"), Decimal128("sNaN"),
        Decimal128("-Infinity"), "", "null:", "private", "é值", {"a": 1}, {},
        [], [1, Int64(1), 1.0, True, None], [[1]], [None], [0, -0.0, False],
        Binary(b"a", 0), Binary(b"a", 128), Binary(bytes(16), 4), Binary(bytes(16), 3),
        Regex("a.b", "im"), Regex("a.b", "mi"), Regex("a.b", "i"),
        MinKey(), MaxKey(), Timestamp(7, 2), Timestamp(2, 7),
        Code("return x"), "return x", Code("return x", {"x": 1}),
        Code("return x", {"x": 1.0}), Code("return x", {"x": Int64(1)}),
        Code("return x", {"x": 1, "a": [2]}), Code("return x", {"a": [2], "x": 1}),
        ObjectId("64b000000000000000000001"), datetime(2020, 1, 1),
    ]
    for left, right in itertools.product(values, repeat=2):
        emit({"v": 1}, [{}, {"v": left}, {"v": right}, {"v": left}])
        # Wrapping unsupported nested arrays/objects must fail, not flatten them.
        emit({"v": 1, "w": 1}, [{"v": [left, right, left], "w": "fixed"},
                                   {"v": right, "w": "fixed"},
                                   {"v": left, "w": "different"}])
    paths = ["v", "v.a", "v.a.b", "v.0", "v.01", "v.-1", "v.é"]
    for field in paths:
        for value in values:
            documents = [{}, {"v": value}, {"v": {"a": value, "0": value,
                         "01": value, "-1": value, "é": value}},
                         {"v": {"a": {"b": value}}}, {"v": [{"a": value}]}]
            emit({field: 1}, documents)
            emit({field: 1, "w": 1}, documents + [{"w": None}], sparse=True)
    filters = [
        {"enabled": True}, {"enabled": {"$exists": True}}, {"v": None},
        {"enabled": {"$eq": True}}, {"enabled": {"$gt": 0}}, {"enabled": {"$gte": 1}},
        {"enabled": {"$lt": 2}}, {"enabled": {"$lte": 1}},
        {"enabled": {"$in": [True, 1, None]}}, {"enabled": {"$type": "number"}},
        {"$and": [{"enabled": {"$gte": 0}}, {"enabled": {"$lt": 2}}]},
        {"$or": [{"enabled": True}, {"enabled": {"$type": "string"}}]},
        {"enabled": {"literal": 1}},
    ]
    randomizer = random.Random(174342)
    for _ in range(600):
        documents = [{"v": randomizer.choice(values), "w": randomizer.choice(values),
                      "enabled": randomizer.choice([None, False, True, 0, 1, "yes", {"literal": 1}])}
                     for _ in range(8)]
        emit({"v": 1, "w": 1}, documents, partial=randomizer.choice(filters))
    for partial in [{}, {"v": {"$exists": False}}, {"v": {"$exists": 1}},
                    {"v": {"$ne": 1}}, {"v": {"$in": 1}}, {"v": {"$regex": "x"}},
                    {"$nor": [{"v": 1}]}, {"$and": []}, {"$or": [1]},
                    {"v": {"$eq": 1, "literal": 2}}, {"v..a": 1},
                    {"$or": [{"v": 1}, {"v": {"$unsupported": True}}]}]:
        emit({"v": 1}, [], partial=partial)
    emit({"v": 1}, [], sparse=True, partial={"v": 1})


if __name__ == "__main__":
    main()
