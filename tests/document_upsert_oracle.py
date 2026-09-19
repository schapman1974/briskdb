"""Exact common operator-upsert semantics from the unchanged frozen helper.

Only direct/sole-$eq, non-overlapping object-path equalities are compared. The
legacy helper ignores literal object/AND predicates, overwrites scalar parents,
and restores IDs silently; those differences are independently tested, not
rewritten or waived here. Explicit query IDs remove generated-ID nondeterminism.
Small integer increments avoid legacy numeric-width/overflow differences.
"""
import hashlib
import random
import sys
from pathlib import Path

from bson import BSON, Binary, Decimal128, Int64, ObjectId, Timestamp
import tinymongo.tinymongo as reference
import tinymongo.bson_codec as bson_codec
import tinymongo.bson_types as bson_types
from tinymongo.errors import OperationFailure


def main():
    for module, digest in [
        (reference, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (bson_codec, "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    rng = random.Random(340)
    values = [None, False, True, 0, -1, Int64(7), 1.25, -0.0, "value", "",
              [], [1, {"x": 2}], Binary(b"upsert", 128), Decimal128("1.00"),
              ObjectId("64b000000000000000000001"), Timestamp(0, 0)]
    count = 0
    for _ in range(40):
        value = rng.choice(values)
        seed = {"_id": Int64(7), "v": value, "nested.leaf": value,
                "items": [1, 2, 3], "counter": rng.randrange(-100, 100),
                "stamp": Timestamp(0, 0), "ignored": {"$gt": 99}}
        for update in [
            {"$set": {"new": value, "nested.leaf": rng.choice(values)}},
            {"$unset": {"v": 1, "nested.leaf": 1}},
            {"$inc": {"counter": rng.randrange(-10, 11)}},
            {"$min": {"v": rng.choice(values)}},
            {"$max": {"v": rng.choice(values)}},
            {"$pop": {"items": rng.choice([-1, 1])}},
            {"$rename": {"nested.leaf": "destination"}},
            {"$addToSet": {"items": {"$each": [2, 4]}}},
            {"$pullAll": {"items": [2, 3]}},
            {"$push": {"items": {"$each": [4, 5], "$position": 1, "$slice": -4}}},
            {"$pull": {"items": {"$gte": 2}}},
        ]:
            for explicit_eq in [False, True]:
                query = {key: {"$eq": value} if explicit_eq and key != "ignored" else value
                         for key, value in seed.items()}
                emit(query, update)
                count += 1
    for update in [
        {"$inc": {"v": 1}}, {"$pop": {"v": 1}},
        {"$push": {"v": 1}}, {"$pull": {"v": 1}},
        {"$pullAll": {"v": [1]}}, {"$addToSet": {"v": 1}},
    ]:
        emit({"_id": Int64(7), "v": "scalar"}, update)
        count += 1
    assert count == 886


def emit(query, update):
    query = BSON(BSON.encode(query)).decode()
    update = BSON(BSON.encode(update)).decode()
    before = BSON.encode(query), BSON.encode(update)
    case = {"query": query, "update": update}
    try:
        reference._validate_update_document(update)
        case["result"] = reference._document_for_upsert(query, update)
    except OperationFailure as error:
        case["error"] = error.code
    assert (BSON.encode(query), BSON.encode(update)) == before
    sys.stdout.buffer.write(BSON.encode(case))


if __name__ == "__main__":
    main()
