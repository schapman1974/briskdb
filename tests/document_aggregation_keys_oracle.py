"""Group-key extensions tested by composition of unchanged frozen operations.

The frozen implementation rejects literal/computed group keys. For these cases
only, evaluate the key with its supported $set expression evaluator, then group
by a collision-free temporary field. Rust executes the ORIGINAL pipeline. The
reference pipeline is retained in each case, so this is explicitly composition
coverage, not a claim that the frozen group grammar supports the extension.
"""

import itertools
import random
from datetime import datetime

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp

from document_aggregation_oracle import emit, locked_sources


def emit_keys(documents, pipeline, numeric_nan_fields=()):
    occupied = BSON.encode({"documents": documents, "pipeline": pipeline})
    reference = []
    changed = False
    for stage in pipeline:
        spec = stage.get("$group") if isinstance(stage, dict) and len(stage) == 1 else None
        if isinstance(spec, dict) and "_id" in spec:
            key = spec["_id"]
            # Invalid field/variable strings stay on the strict original path.
            field = isinstance(key, str) and not isinstance(key, Code) and key.startswith("$")
            if key is not None and not field:
                index = 0
                while (name := f"__briskdb_group_key_{index}").encode() in occupied:
                    index += 1
                occupied += name.encode()
                # The wrapper both normalizes missing to null and prevents
                # $set path flattening of object expressions (e.g. "0"/"a.b"
                # object member names). Accumulators still see original fields.
                reference.append({"$set": {name: {"$ifNull": [key, None]}}})
                spec = dict(spec)
                spec["_id"] = "$" + name
                stage = {"$group": spec}
                changed = True
        reference.append(stage)
    emit(documents, pipeline, numeric_nan_fields=numeric_nan_fields,
         reference_pipeline=reference if changed else None)


def main():
    locked_sources()
    values = [None, False, True, 0, 1, Int64(1), 1.0, Decimal128("1.00"), "one", "two",
              {}, {"x": 1}, {"x": Int64(1)}, {"a": 1, "b": 2}, {"b": 2, "a": 1},
              [], [1, 2], [Int64(1), 2.0], [[1]], [{"x": 2}],
              Binary(b"abc", 0), Binary(b"abc", 128), ObjectId("64b000000000000000000001"),
              Regex("ab", "im"), Code("$not_a_reference"), Code("return x", {"x": 1}),
              datetime(2020, 1, 1), Timestamp(7, 2), MinKey(), MaxKey(), float("nan"), -0.0,
              Decimal128("NaN"), Decimal128("-0E-6176")]
    source = [{"_id": Int64(index), "a": value, "b": index % 3, "items": [value, None],
               "payload": value, "__briskdb_group_key_0": "keep this field"} for index, value in enumerate(values)]
    source += [{"_id": Int64(len(source)), "items": []}, {"_id": Int64(len(source) + 1), "a": None, "items": [1]}]
    keys = values + [{"$literal": value} for value in values] + [
        "$a", "$missing", "$items.x", {"a": "$a", "b": "$b"}, ["$a", "$b", "$missing"],
        {"nested": {"value": "$a"}, "missing": "$missing"}, {"0": "$a", "a.b": "$b"},
        {"$ifNull": ["$a", "$b", 0]}, {"$size": "$items"},
        {"$literal": "$$REMOVE"}, {"$literal": {"$private": [1, 2]}},
        {"$literal": ["$a", "$$REMOVE"]}, {"$ifNull": ["$missing", {"constant": 1}]},
    ]

    def group(key):
        return {"$group": {"_id": key, "n": {"$sum": 1}, "first": {"$first": "$payload"},
                            "last": {"$last": "$payload"}, "ids": {"$push": "$_id"},
                            "original": {"$first": "$__briskdb_group_key_0"}}}

    for documents in [[], source, list(reversed(source))]:
        for key in keys:
            for stages in [[group(key)], [{"$sort": {"_id": -1}}, group(key)],
                           [group(key), {"$sort": {"_id": 1}}], [{"$limit": 3}, group(key)],
                           [group(key), {"$skip": 1}, {"$limit": 4}], [group(key), {"$count": "n"}]]:
                emit_keys(documents, stages)
    for key in [{"$ifNull": []}, {"$ifNull": None}, {"$size": []}, {"$size": [1, 2]},
                {"$private": 1}, {"$literal": 1, "extra": 2}, "$$ROOT", "$a.", "$a..b"]:
        for documents in [[], source]:
            emit_keys(documents, [group(key)])
            emit_keys(documents, [{"$skip": 1000}, group(key)])
    # Group keys have the same no-variables context as accumulator expressions;
    # context-dependent $$REMOVE failures are tested directly in Rust/clients,
    # not through $set, where that variable is deliberately allowed.
    for first, second in itertools.product(keys[-13:], repeat=2):
        emit_keys(source, [group(first), group(second)])
    randomizer = random.Random(176172)
    for _ in range(4000):
        key = randomizer.choice(keys)
        documents = randomizer.sample(source, randomizer.randrange(15))
        prefix = randomizer.choice([[], [{"$sort": {"_id": -1}}], [{"$skip": 1}], [{"$limit": 2}]])
        suffix = randomizer.choice([[], [{"$sort": {"_id": 1}}], [{"$limit": 1}], [{"$count": "n"}], [group({"prior": "$_id"})]])
        emit_keys(documents, prefix + [group(key)] + suffix)


if __name__ == "__main__":
    main()
