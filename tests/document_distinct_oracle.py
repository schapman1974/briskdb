"""Generate distinct extraction/identity cases from the frozen source, not a reimplementation."""

import hashlib
import itertools
import random
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.bson_types as bson_types
import tinymongo.tinymongo as implementation


def emit(documents, field):
    envelope = BSON(BSON.encode({"documents": documents, "field": field})).decode()
    before = BSON.encode(envelope)

    class Input:
        def find(self, query):
            assert query == {}
            return envelope["documents"]

    values = implementation.TinyMongoCollection.distinct(Input(), field)
    assert BSON.encode(envelope) == before
    envelope["result"] = values
    sys.stdout.buffer.write(BSON.encode(envelope))


def main():
    for module, digest in [
        (implementation, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [
        None, False, True, -10, 0, 1, Int64(1), Int64(2**63 - 1),
        1.0, 1.25, float("nan"), float("inf"), float("-inf"), -0.0,
        Decimal128("0E-6000"), Decimal128("1E-6000"), Decimal128("NaN"),
        Decimal128("sNaN"), Decimal128("-Infinity"), Decimal128("1.00000000000000000001"),
        Decimal128("1.00"), "", "hello", {"a": 1, "b": 2}, {"b": Int64(2), "a": 1},
        {"a": Int64(1), "b": 2.0}, [], [1, 2], [1.0, Int64(2)], [9, 1, 5],
        [[2, 1]], [[], [1]], [{"a": 1}, None, {"b": 2}, {}], [[{"a": 9}]],
        Binary(b"\x00\xff", 128), Binary(b"abc", 0), Binary(bytes(16), 4), Binary(bytes(16), 3),
        ObjectId("64b000000000000000000001"), Timestamp(7, 2), datetime(2020, 1, 1),
        MinKey(), MaxKey(), Regex("Ab.c", "im"), Regex("Ab.c", "mi"), Regex("Ab.c", "i"),
        Code("return 1"), Code("return x", {"x": 1}), Code("return x", {"x": 1.0}),
    ]
    # Wrapping the candidates in an array keeps nested arrays as values, and
    # exact encoded output proves that the first representation wins.
    for left, right in itertools.product(values, repeat=2):
        emit([{"v": [left]}, {"v": [right]}, {"v": [left]}], "v")
    fields = ["v", "v.a", "v.a.b", "v.0", "v.01", "v.-1", "v.", "v..a", "", ".", "$v", "v.$ref", "v\x00"]
    for value in values:
        docs = [{}, {"v": value}, {"v": {"a": value, "0": value, "01": value,
                                               "": {"a": value}, "$ref": value}},
                {"v": [{"a": value}, {"a": value}]}, {"": value}, {"": {"": value}}, {"$v": value}]
        for field in fields:
            emit(docs, field)
            emit(list(reversed(docs)), field)
    randomizer = random.Random(172314)
    for _ in range(1200):
        docs = [{"v": randomizer.choice(values)} for _ in range(randomizer.randrange(20))]
        emit(docs, randomizer.choice(fields))
    for field in fields:
        emit([], field)


if __name__ == "__main__":
    main()
