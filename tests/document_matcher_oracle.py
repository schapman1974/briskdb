"""Generate BSON matcher cases from the locked test-only TinyMongo oracle."""

import hashlib
import random
import sys
from datetime import datetime
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.bson_types as bson_types
import tinymongo.table_backends as backend
from tinymongo.errors import OperationFailure, TinyMongoNotSupportedError


def verify_source():
    # Same commit as compat/mongo/v1/manifest.json, not the developer's checkout.
    for module, expected in [
        (backend, "b16dbc8c435a639d85c29d857f8487b2c88d2eef10969a9e412d8afce02898a1"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == expected


def emit(document, query):
    # Evaluate the exact values a BSON transport supplies, including regex flags.
    document = BSON(BSON.encode(document)).decode()
    query = BSON(BSON.encode(query)).decode()
    case = {"document": document, "query": query}
    try:
        backend.validate_filter_operators(query)
        case["matches"] = backend.matches_filter(document, query)
    except TinyMongoNotSupportedError:
        case["error"] = 115
    except OperationFailure as error:
        case["error"] = error.code
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    verify_source()
    values = [
        None, False, True, -10, 0, 1, 2, Int64(7), Int64(2**40),
        Int64(2**63 - 1), Int64(-(2**63)), 1.0, 10.9, -10.9,
        float("nan"), float("inf"), Decimal128("1.00"), Decimal128("1E-6000"),
        Decimal128("1E+6000"), Decimal128("NaN"), "", "abc", "Abxc",
        {"a": 1, "b": 2}, {"b": 2, "a": 1}, [], [1, 2], [[1, 2]],
        [{"a": 1}, {"a": 2}], Binary(b"abc", 2), Binary(b"abc", 128),
        ObjectId("64b000000000000000000001"), Timestamp(3, 2), datetime(2020, 1, 1),
        MinKey(), MaxKey(), Regex("Ab.c", "i"), Regex("Ab.c", "iu"),
        Regex("[", ""), Code("return 1"), Code("return x", {"x": 1}),
    ]
    documents = [{}] + [{"v": value} for value in values]
    conditions = []
    for value in values:
        conditions.extend([value, {"$eq": value}, {"$ne": value}, {"$in": [value]}, {"$nin": [value]}])
        conditions.extend({operator: value} for operator in ["$gt", "$gte", "$lt", "$lte"])
    conditions.extend([
        {"$exists": True}, {"$exists": False}, {"$size": 0}, {"$size": 2},
        {"$all": []}, {"$all": [None]}, {"$all": [1, 2]}, {"$all": [Regex("a", "i")]},
        {"$elemMatch": {}}, {"$elemMatch": {"$gt": 0, "$lt": 3}},
        {"$not": {"$eq": 1}}, {"$not": Regex("a", "i")},
        {"$type": "int"}, {"$type": "long"}, {"$type": "number"}, {"$type": ["string", "array"]},
        {"$mod": [4, 2]}, {"$mod": [4.9, 2.9]}, {"$mod": [4, -2]},
        {"$regex": "Ab.c", "$options": "i"}, {"$regex": r"^(abc)\1$"},
    ])
    for document in documents:
        for condition in conditions:
            emit(document, {"v": condition})

    # Exercise the regex dialect separately from BSON equality/type matching.
    for value in ["", "a", "a\n", "a\n\n", "a\rb", "a\nb", "A", "İ", "ı", "ſ", "K", "é", "²", "\u0301", "\x1c"]:
        for pattern in [r"^a$", r"a\Z", r"a.b", r"\w", r"\W", r"\d", r"\s", r"\b", r"[a-z]", r"(?i)i", r"[^i]", r"[^İ]", r"[^a-z]", r"[\w]", r"[\s]", r"\$", r"[$]", r"(?m:^a$)", r"(?i:a)(?-i:b)", r"(?P<word>a)(?P=word)", r"(?<=a)b", r"a # comment"]:
            for options in ["", "i", "m", "s", "x", "u"]:
                emit({"v": value}, {"v": {"$regex": pattern, "$options": options}})

    paths = ["items.a", "items.a.b", "items.0", "items.0.a", "items.1.b", "items.01"]
    randomizer = random.Random(167)
    members = [None, 1, [1, 2], [{"a": 2}], {"a": 1}, {"a": None}, {"a": {"b": 2}}, {"0": {"a": 2}}, {"b": 3}]
    for _ in range(100):
        document = {"items": [randomizer.choice(members) for _ in range(randomizer.randrange(5))]}
        for path in paths:
            for condition in [None, 1, 2, {"$ne": None}, {"$nin": [None]}, {"$exists": False}, {"$type": "int"}, {"$gt": 0, "$lt": 3}]:
                emit(document, {path: condition})
        emit(document, {"items": {"$elemMatch": {"a": None}}})
        emit(document, {"items": {"$all": [{"$elemMatch": {"a": {"$gt": 0}}}]}})
        emit(document, {"$and": [{"items.a": {"$ne": None}}, {"$nor": [{"items.a": 99}]}]})

    for query in [
        {"v": {"$type": []}}, {"v": {"$type": True}}, {"v": {"$type": "bad"}},
        {"v": {"$size": -1}}, {"v": {"$size": 1.2}}, {"v": {"$mod": [0, 1]}},
        {"v": {"$not": {}}}, {"v": {"$not": {"literal": 1}}},
        {"v": {"$in": [{"$regex": "a"}]}}, {"v": {"$options": "i"}},
        {"v": {"$regex": Regex("a", "i"), "$options": "i"}},
        {"v": {"$regex": "a", "$options": "q"}}, {"v": {"$regex": "["}},
        {"v": {"$elemMatch": {"$unknown": 1}}}, {"v": {"$comment": "test"}},
        {"$where": "not executed"}, {"$or": [{}, {"v": {"$size": "invalid"}}]},
    ]:
        emit({}, query)


if __name__ == "__main__":
    main()
