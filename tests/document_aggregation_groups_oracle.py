"""Whole-pipeline grouping against the unchanged source-locked implementation."""

import itertools
import math
import random
from datetime import datetime

from bson import Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp

from document_aggregation_oracle import emit as reference_emit, locked_sources


def emit(documents, pipeline, numeric_nan_fields=("avg", "sum")):
    # Only arithmetic fields disregard implementation-defined Double NaN bits.
    # Keys, sets, first/last/min/max and all other BSON compare byte-for-byte.
    reference_emit(documents, pipeline, numeric_nan_fields=numeric_nan_fields)


OPERATORS = ["$addToSet", "$avg", "$first", "$last", "$max", "$min", "$push", "$sum"]


def group(key="$bucket", operand="$v"):
    return {"$group": {"_id": key, **{operator[1:]: {operator: operand} for operator in OPERATORS}}}


def numeric(values):
    source = [{"_id": index, "v": value} for index, value in enumerate(values)]
    # Frozen Python's arbitrary-width integer totals are not BSON encodable.
    # Test that explicitly documented extension in Rust, not by changing the
    # reference or coercing its expected result here. Average is still compared.
    integers = [value for value in values if isinstance(value, int) and not isinstance(value, bool)]
    has_noninteger = any(isinstance(value, (float, Decimal128)) for value in values)
    overflow = not has_noninteger and not -(2**63) <= sum(integers) < 2**63
    spec = {"_id": None, "avg": {"$avg": "$v"}}
    if not overflow:
        spec["sum"] = {"$sum": "$v"}
    emit(source, [{"$group": spec}])


def main():
    locked_sources()
    numbers = [
        -10, 0, 1, Int64(1), 2**31 - 1, 2**31, -(2**31) - 1,
        Int64(2**63 - 1), Int64(-(2**63)), 2**60, -(2**60) + 2,
        0.0, -0.0, 1.0, 2.1, -2.1, 1e16, -1e16,
        5e-324, -5e-324, 1.7976931348623157e308, -1.7976931348623157e308,
        float("inf"), float("-inf"), float("nan"), -float("nan"),
        *[Decimal128(value) for value in [
            "0", "-0", "0.00", "-0.00", "1.00", "2.1", "-2.1",
            "1E34", "-1E34", "1E-6176", "-1E-6176", "0E-6176", "0E+6111",
            "1.000000000000000000000000000000001", "9.999999999999999999999999999999999E+6144",
            "-9.999999999999999999999999999999999E+6144", "1E-6143", "1E+6144",
            "NaN", "-NaN", "sNaN", "-sNaN", "Infinity", "-Infinity",
        ]],
    ]
    others = [None, False, True, "", "hello", {}, {"a": 1, "b": 2}, {"b": 2, "a": 1},
              [], [1, 2], [Int64(1), 2.0], [[1]], [{"a": 2}],
              Binary(b"abc", 0), Binary(b"abc", 2), Binary(bytes(16), 4),
              ObjectId("64b000000000000000000001"), Regex("ab", "im"), Code("return 1"),
              Code("return x", {"x": Int64(1)}), datetime(2020, 1, 1), Timestamp(7, 2), MinKey(), MaxKey()]
    for pair in itertools.product(numbers, repeat=2):
        numeric(pair)
    for values in [[], others, [1e16, 1.0, -1e16], [1e16, -1e16, 1.0],
                   [7900154101625246752, -1259608310039654329, -6593466016263951012],
                   [7900154101625246752, -1259608310039654329, float(-6593466016263951012)],
                   [Decimal128("1E34"), 1.0, Decimal128("-1E34")]]:
        numeric(values)
    values = numbers + others
    source = [{"_id": Int64(index), "v": value, "bucket": index % 3} for index, value in enumerate(values)]
    source += [{"_id": Int64(len(source)), "bucket": None}, {"_id": Int64(len(source) + 1)}]
    for documents in [[], source, list(reversed(source))]:
        for key in [None, "$bucket", "$v", "$missing", "$v.a", "$v.0"]:
            for operand in ["$v", "$missing", {"$literal": [1, 2]}, {"$ifNull": ["$v", -1]},
                            {"a": "$v", "missing": "$missing"}, {"$literal": {"$private": 1}}]:
                emit(documents, [group(key, operand)])
        for stages in itertools.permutations([
            {"$sort": {"_id": -1}}, group(), {"$limit": 3}, {"$skip": 1}, {"$count": "n"},
        ]):
            emit(documents, list(stages))
        for operator in OPERATORS:
            for operand in ["$v", "$missing", {"$size": "$v"}, {"$literal": None}]:
                emit(documents, [{"$group": {"_id": "$bucket", "value": {operator: operand}}}],
                     numeric_nan_fields=("value",) if operator in ("$sum", "$avg") else ())
    for value in values:
        stage = group("$k")
        if isinstance(value, int) and not -(2**63) <= 2 * value < 2**63:
            del stage["$group"]["sum"]  # Unencodable frozen total; see numeric().
        emit([{"k": value, "v": value}, {"k": value, "v": value}, {"k": None}, {}], [stage])
    # Equal identities keep first key/set representation; extrema ties keep last.
    for permutation in itertools.permutations([1, Int64(1), 1.0, Decimal128("1.00")]):
        emit([{"k": value, "v": value} for value in permutation], [group("$k")])
    invalid = [None, [], 1, {}, {"x": {"$sum": 1}}]
    invalid += [{"_id": key} for key in [True, 1, "literal", "$", "$$REMOVE", {}, [], Code("$v"), "$v.", "$v..x", "$v.$x", "$v.\0"]]
    for name in ["out", "", "$out", "out.x", "$out.x"]:
        for value in [None, 1, [], {}, {"not_operator": 1}, {"$sum": 1, "$avg": 1},
                      {"$sum": []}, {"$unknown": []}, {"$unknown": 1}, {"$first": "$$REMOVE"},
                      {"$sum": {"$size": []}}, {"$push": {"$ifNull": []}}]:
            invalid.append({"_id": None, name: value})
    for spec in invalid:
        for documents in [[], source]:
            emit(documents, [{"$group": spec}])
            emit(documents, [{"$skip": 1000}, {"$group": spec}])
    for operator in OPERATORS:
        documents = [{"v": [1]}, {"v": "bad"}]
        stage = {"$group": {"_id": None, "v": {operator: {"$size": "$v"}}}}
        for stages in [[stage], [{"$limit": 1}, stage], [stage, {"$limit": 1}],
                       [{"$sort": {"v": 1}}, {"$limit": 1}, stage]]:
            emit(documents, stages)
    randomizer = random.Random(176)
    for _ in range(3000):
        numeric(randomizer.choices(values, k=randomizer.randrange(20)))
    choices = [group(), group(None), group("$_id", "$sum"), {"$sort": {"_id": -1}},
               {"$skip": 1}, {"$limit": 4}, {"$count": "n"}, {"$match": {"v": {"$ne": None}}},
               {"$set": {"bucket": {"$ifNull": ["$bucket", "$v"]}}},
               {"$project": {"_id": 0, "v": 1, "bucket": 1}}, {"$unset": "bucket"},
               {"$group": {"_id": "$bucket", "first": {"$first": "$v"}, "set": {"$addToSet": "$v"}}}]
    for _ in range(3000):
        bounded = [row for row in source if row.get("v") not in (2**63 - 1, -(2**63))
                   and not (isinstance(row.get("v"), float) and not math.isfinite(row["v"]))]
        emit(randomizer.sample(bounded, randomizer.randrange(20)),
             randomizer.choices(choices, k=randomizer.randrange(1, 8)))


if __name__ == "__main__":
    main()
