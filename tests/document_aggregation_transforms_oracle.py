"""Whole projection/expression pipelines against the locked implementation."""

import itertools
import random
from datetime import datetime

from bson import Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
from document_aggregation_oracle import emit, locked_sources


def main():
    locked_sources()
    values = [
        None, False, True, 0, 1, -1, Int64(1), Int64(2**63 - 1), -0.0, 1.25,
        float("nan"), float("inf"), float("-inf"), Decimal128("-0E-6000"),
        Decimal128("1.000"), Decimal128("1E-6000"), Decimal128("NaN"),
        Decimal128("sNaN"), Decimal128("Infinity"), "", "hello", "$a", "$$REMOVE",
        [], [1, None], [[{"x": 7}]], {"b": 2, "a": Int64(1)}, {},
        Binary(b"ab", 0), Binary(b"ab", 128), ObjectId("64b000000000000000000001"),
        Regex("a.*", "im"), Code("$a"), Code("x", {"x": Int64(2)}),
        Timestamp(7, 2), datetime(2020, 1, 1), MinKey(), MaxKey(),
    ]
    sources = [
        {"_id": Int64(1), "a": {"y": 2, "x": 1, "old": 0}, "b": 2, "source": 9,
         "c": 3, "arr": [{"x": 1, "keep": "a"}, {"y": 2}, 3], "empty": [],
         "nested": [[{"x": 7}], 2], "scalar": 5, "$db": "db", "numeric": {"0": 7}},
        {"_id": Int64(2), "a": None, "source": None, "arr": [], "nested": [[], {}]},
        {"_id": Int64(3), "a": [None, {"x": [1, 2]}, {}, [{"x": 9}]], "arr": [None, {}]},
        {"_id": Int64(4)},
    ]
    expressions = values + [
        "$a.x", "$arr.x", "$nested.x", "$absent.x", "$numeric.0", "$a.$db", "$$REMOVE.0",
        ["$source", "$absent", "$$REMOVE", {"$literal": "$a"}],
        {"$ifNull": ["$source", "$absent", "fallback"]},
        {"$ifNull": ["$$REMOVE", "$source", None]},
        {"$ifNull": [1, {"$size": "$absent"}]},
        {"$ifNull": [None, {"$size": "$absent"}]},
        {"$size": "$arr"}, {"$size": ["$arr"]}, {"$size": [[]]},
        {"$size": {"$literal": [None, 2]}},
        [{"a": "$source", "gone": "$$REMOVE", "b": "$arr.x"}],
    ] + [{"$literal": value} for value in values]
    for expression in expressions:
        for stage in ("$project", "$set", "$addFields"):
            for source in ([], sources):
                emit(source, [{stage: {"v": expression}}])
                emit(source, [{stage: {"arr.v": expression, "scalar.v": expression,
                                       "missing.v": expression, "empty.v": expression}}])
    valid = [
        {"$project": {"name": 1}}, {"$project": {"_id": 0}}, {"$project": {"_id": 1}},
        {"$project": {"a.y": 0, "arr.x": 0}},
        {"$project": {"_id": 0, "arr.x": 1, "arr.z": "$source", "nested.z": "$source",
                      "scalar.z": "$source", "missing.z": "$absent", "empty.z": "$source"}},
        {"$project": {"a": {"x": 1, "copied": "$source"}, "_id": "$a.x"}},
        {"$project": {"arr.x": 1, "scalar.x": 1, "copy": "$source"}},
        {"$project": {"new_one": "$source", "c": 1, "new_two": "$source", "b": 1,
                      "a.old": "$source", "a.x": 1, "a.y": "$source", "_id": 0}},
        {"$project": {"_id.x": 1, "value": {"$literal": 3}}},
        {"$set": {}}, {"$addFields": {}},
        {"$set": {"source": "new", "old": "$source", "_id": 2, "old_id": "$_id"}},
        {"$set": {"a": {"x": 1}, "whole": {"$literal": {"x": 1}}, "empty": {}}},
        {"$set": {"a.y": "$$REMOVE", "arr.x": "$$REMOVE", "scalar.z": "$$REMOVE",
                  "missing.z": "$$REMOVE", "nested.z": "$$REMOVE"}},
        {"$unset": "source"}, {"$unset": ["_id", "a.y", "arr.x"]},
    ]
    for stage in valid:
        for source in ([], sources, list(reversed(sources))):
            emit(source, [stage])
            emit(source, [{"$sort": {"_id": -1}}, stage, {"$limit": 2}])
            emit(source, [stage, {"$sort": {"a": 1}}, {"$count": "n"}])
    for fields in itertools.permutations([("a.x", 1), ("a.y", "$source"), ("b", "$source"),
                                           ("c", 1), ("new", "$source")]):
        emit(sources, [{"$project": dict(fields)}])
    invalid = []
    for name in ("$project", "$set", "$addFields"):
        for value in (None, [], "v", Code("v"), False, 1):
            invalid.append({name: value})
        for spec in ({"": 1}, {"a.": 1}, {"a..b": 1}, {"$bad": 1}, {"a.$db": 1},
                     {"a.0": 1}, {"a.-1": 1}, {"a.²": 1}, {"a": 1, "a.b": 1},
                     {"a.b": 1, "a": 1}, {"a.b": 1, "a": {"b": 2}},
                     {"a": 0, "b": "$a"}, {"a": 0, "b": {"$unsupported": 1}},
                     {"b": "$a", "a": 0}, {"a": 0, "b": 1}, {"a": 1, "b": 0},
                     {"a": {}}, {"a": {"": 1}}, {"a.b": "$$REMOVE", "a": {}}):
            invalid.append({name: spec})
        for expression in ("$", "$$ROOT", "$$CURRENT", "$$REMOVE.", "$$REMOVE..x", "$$REMOVE.$x",
                           "$a.", "$a..b", "$a.$bad", "$a.\0", {"$ifNull": []},
                           {"$ifNull": [1]}, {"$ifNull": 1}, {"$size": []}, {"$size": [1, 2]},
                           {"$unknown": 1}, {"$size": "$a", "other": 1},
                           {"$literal": 1, "$size": 2}, [{"$unknown": 1}],
                           {"$ifNull": [1, {"$unknown": 2}]}):
            invalid.append({name: {"v": expression}})
    invalid.append({"$project": {}})
    for value in (None, 1, Code("a"), {}, [], ["a", 1], "", "a.", "a..b", "$a", "a.$b",
                  "a.0", "a.\0", ["a", "a"], ["a", "a.b"], ["a.b", "a"], ["a.", "a."]):
        invalid.append({"$unset": value})
    for stage in invalid:
        for source in ([], sources):
            emit(source, [stage])
            emit(source, [{"$skip": Int64(2**63 - 1)}, stage])
    # Runtime errors obey lazy consumption even after blocking stages.
    error_source = [{"_id": 1, "v": []}, {"_id": 2, "v": None}]
    for prefix in ([], [{"$sort": {"_id": 1}}], [{"$sort": {"_id": -1}}]):
        for stage in ("$project", "$set", "$addFields"):
            for suffix in ([], [{"$limit": 1}], [{"$skip": 2}], [{"$count": "n"}],
                           [{"$limit": 1}, {"$sort": {"n": 1}}]):
                emit(error_source, prefix + [{stage: {"n": {"$size": "$v"}}}] + suffix)
    rng = random.Random(177)
    choices = valid + [
        {"$set": {"size": {"$size": {"$ifNull": ["$arr", []]}}}},
        {"$project": {"_id": 0, "v": {"$ifNull": ["$source", "$a.x", 0]}}},
        {"$set": {"arr.label": "$a", "name": {"$literal": "$$REMOVE"}}},
        {"$set": {"arr": {"$literal": [1, {}, [2]]}}},
        {"$match": {"source": {"$ne": None}}}, {"$match": {"missing": 1}},
        {"$sort": {"a": 1}}, {"$sort": {"source": -1}},
        {"$skip": 1}, {"$skip": 5}, {"$limit": 1}, {"$limit": 3}, {"$count": "n"},
    ]
    for _ in range(5000):
        source = rng.sample(sources, rng.randrange(len(sources) + 1))
        pipeline = [rng.choice(choices) for _ in range(rng.randrange(1, 9))]
        emit(source, pipeline)


if __name__ == "__main__":
    main()
