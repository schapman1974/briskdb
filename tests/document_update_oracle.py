"""Source-locked field-update oracle with explicit per-operator boundaries.

TinyMongo's legacy set/unset helpers do not implement Mongo numeric array paths
and silently overwrite scalar set parents/restore changed IDs. Those unsafe or
different cases are deliberately outside this matrix, not rewritten or waived.
Rust unit, transaction, and wire tests check those set/unset boundaries
independently. Min/max, pop, and rename additionally cover whole BSON ordering,
numeric array paths, blocked parents, and immutable IDs using the unchanged
reference helpers. Add-to-set uses non-ID object paths only because its legacy
reference helper shares the old set-path behavior. Pull-all covers non-ID numeric
paths; both legacy membership helpers restore changed IDs silently. Strict
add-to-set paths and immutable IDs are tested independently, not waived.
Push covers non-ID object/array paths and blocked parents; its helper silently
restores changed IDs, so identity is tested independently. Push sorting uses
whole values or frozen object-only field
lookup, not ordinary query-sort array-element selection.
Pull covers non-ID paths and shared query predicates; embedded member _id fields
use ordinary field matching. Immutable collection IDs are tested independently.
Increment compares exact common numeric behavior on non-ID object paths; legacy
width, overflow, missing/signed-zero, path/ID and arithmetic-NaN differences are
explicitly bounded below and independently tested, never rewritten or waived.
"""
import hashlib
import math
import random
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path

from bson import BSON, Binary, Code, Decimal128, Int64, MaxKey, MinKey, ObjectId, Regex, Timestamp
import tinymongo.tinymongo as reference
import tinymongo.bson_codec as bson_codec
import tinymongo.bson_types as bson_types
from tinymongo.errors import OperationFailure


def emit(document, update):
    document = BSON(BSON.encode(document)).decode()
    update = BSON(BSON.encode(update)).decode()
    before = BSON.encode(document)
    case = {"document": document, "update": update}
    try:
        reference._validate_update_document(update)
        case["result"] = reference._apply_update_document(document, update)
    except OperationFailure as error:
        case["error"] = error.code
    assert BSON.encode(document) == before
    sys.stdout.buffer.write(BSON.encode(case))


def main():
    for module, digest in [
        (reference, "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0"),
        (bson_codec, "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617"),
        (bson_types, "a4b070ef1937b82f740ddb0c07279f4128d95ae3e75ba63ebec9e2068cdc9973"),
    ]:
        assert hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() == digest
    values = [None, False, True, 0, -1, Int64(1), Int64(2**63 - 1),
              1.0, -0.0, float("nan"), Decimal128("NaN"), Decimal128("0E-6000"),
              "$literal", "", [], [1, {"x": 2}], {"x": Int64(1)},
              Binary(b"data", 128), ObjectId("64b000000000000000000001"),
              Timestamp(0, 0), Regex("abc", "i"), Code("return x", {"x": 1})]
    randomizer = random.Random(171)
    for _ in range(1000):
        value, next_value = randomizer.choice(values), randomizer.choice(values)
        document = {"_id": Int64(7), "keep": value, "v": value,
                    "nested": {"leaf": value, "keep": next_value}}
        for update in [
            {"$set": {"v": next_value}},
            {"$set": {"nested.leaf": next_value, "missing.0.value": value}},
            {"$unset": {"nested.leaf": next_value, "absent.child": value}},
            {"$set": {"new": next_value}, "$unset": {"v": value}},
        ]:
            emit(document, update)
    for update in [{"$set": {}}, {"$unset": {}},
                   {"$set": {"a": 1, "a.b": 2}},
                   {"$set": {"a.b": 1}, "$unset": {"a": 1}},
                   {"$set": {"a..b": 1}}, {"$unset": {"a.": 1}},
                   {"$set": 1}, {"$unset": []}]:
        emit({"_id": 1, "v": 2}, update)

    comparison_values = values + [
        MinKey(), MaxKey(), float("-inf"), float("inf"),
        Decimal128("Infinity"), Decimal128("-Infinity"), Decimal128("-0"),
        Decimal128("1.000"), Int64(-(2**63)),
        datetime(2026, 1, 2, tzinfo=timezone.utc), Code("return x"), Binary(b"data", 0),
    ]
    assert len(comparison_values) == 34
    for operator in ["$min", "$max"]:
        for current in comparison_values:
            for candidate in comparison_values:
                emit({"_id": Int64(7), "keep": current, "v": current,
                      "nested": {"leaf": current}},
                     {operator: {"v": candidate, "nested.leaf": candidate}})
                emit({"_id": Int64(7), "a": [current]},
                     {operator: {"a.0": candidate, "a.3": candidate}})
            emit({"_id": 7}, {operator: {"missing.0.value": current}})
        for document, path, candidate in [
            ({"_id": 5}, "_id", 5.0),
            ({"_id": 5}, "_id", 4 if operator == "$min" else 6),
            ({"_id": 5, "a": 1}, "a.x", 1),
            ({"_id": 5, "a": None}, "a.x", 1),
            ({"_id": 5, "a": [1]}, "a.x", 1),
            ({"_id": 5, "a": [1]}, "a.01", 1),
            ({"_id": 5, "a": [1]}, "a.-1", 1),
            ({"_id": 5, "a": [1]}, "a.2.x", 1),
            ({"_id": 5, "a": [None]}, "a.0.x", 1),
        ]:
            emit(document, {operator: {path: candidate}})
        for update in [{operator: {}}, {operator: []}, {operator: {"a..b": 1}}]:
            emit({"_id": 7}, update)
    for update in [
        {"$min": {"a": 1}, "$max": {"a": 3}},
        {"$min": {"a": 1}, "$set": {"a.b": 3}},
        {"$max": {"a.b": 1}, "$unset": {"a": 1}},
    ]:
        emit({"_id": 7}, update)


    for _ in range(1000):
        items = [randomizer.choice(values) for _ in range(randomizer.randrange(6))]
        document = {"_id": 7, "front": items, "grid": [items], "keep": True}
        for direction in [-1, 1]:
            emit(document, {"$pop": {"front": direction, "grid.0": direction,
                                     "missing.child": direction, "grid.3": direction}})
        value, other = randomizer.choice(values), randomizer.choice(values)
        emit({"_id": 7, "source": value, "dest": other,
              "nested": {"old": other, "keep": value}, "tail": True},
             {"$rename": {"source": "dest", "nested.old": "new.value", "absent": "tail"}})
    for operand in [1, -1, Int64(1), -1.0, Decimal128("1.00"), Decimal128("-1"),
                    0, 2, 1.5, True, "1", None, [], {}, float("nan"), Decimal128("Infinity")]:
        emit({"_id": 7, "v": [1, 2, 3]}, {"$pop": {"v": operand}})
    for value in values:
        emit({"_id": 7, "v": value}, {"$pop": {"v": 1}})
    for document, path in [
        ({"_id": 7, "v": 1}, "v.x"),
        ({"_id": 7, "v": None}, "v.x"),
        ({"_id": 7, "v": [None]}, "v.0.x"),
        ({"_id": 7, "v": [[1, 2]]}, "v.0"),
        ({"_id": 7, "v": [[1, 2]]}, "v.3"),
        ({"_id": 7, "v": [[1, 2]]}, "v.99999999999999999999999"),
        ({"_id": 7, "v": [1]}, "v.01"),
        ({"_id": 7, "v": [1]}, "v.x"),
        ({"_id": 7, "v": [1]}, "v.-1"),
        ({"_id": 7}, "absent.x"),
        ({"_id": 7, "v": {}}, "v.missing"),
        ({"_id": {"list": [1, 2]}}, "_id.list"),
    ]:
        emit(document, {"$pop": {path: 1}})
    for document, source, target in [
        ({"_id": 7, "a": []}, "absent", "a.0"),
        ({"_id": 7, "a": 1}, "a.x", "new"),
        ({"_id": 7, "a": []}, "a.0", "new"),
        ({"_id": 7, "a": []}, "a.x", "new"),
        ({"_id": 7, "a": [], "v": 1}, "v", "a.0"),
        ({"_id": 7, "a": [], "v": 1}, "v", "a.x"),
        ({"_id": 7, "a": None, "v": 1}, "v", "a.x"),
        ({"_id": 7}, "absent", "_id"),
        ({"_id": 7, "v": 7}, "v", "_id"),
        ({"_id": 7}, "_id", "new"),
        ({"_id": {}}, "_id.missing", "new"),
        ({"_id": {"x": 1}}, "_id.x", "new"),
        ({"_id": 7}, "a", "a"),
        ({"_id": 7}, "a", "a.b"),
        ({"_id": 7}, "a.b", "a"),
        ({"_id": 7}, "a", 1),
        ({"_id": 7}, "a", ""),
        ({"_id": 7}, "a", "b..c"),
        ({"_id": 7}, "a", "b.$[].c"),
        ({"_id": 7}, "a.$[].b", "new"),
        ({"_id": 7}, "a..b", "new"),
        ({"_id": 7, "a": [1, 2]}, "a", "new"),
        ({"_id": 7, "a": None}, "a", "new.value"),
        ({"_id": 7, "a": {"x": 1}}, "a.x", "a.y"),
    ]:
        emit(document, {"$rename": {source: target}})
    for update in [
        {"$pop": {}, "$rename": {}},
        {"$rename": {"a": "b", "b": "a"}},
        {"$rename": {"a": "b"}, "$set": {"b": 1}},
        {"$pop": {"a": 1}, "$unset": {"a.0": 1}},
    ]:
        emit({"_id": 7}, update)

    for current in comparison_values:
        for candidate in comparison_values:
            document = {"_id": Int64(7), "v": [current, current],
                        "nested": {"v": [current]}, "keep": current}
            emit(document, {"$addToSet": {"v": candidate, "nested.v": candidate}})
            emit(document, {"$pullAll": {"v": [candidate], "nested.v": [candidate]}})
        emit({"_id": 7}, {"$addToSet": {"missing.0.v": current}})
        for operator, operand in [("$addToSet", 1), ("$pullAll", [1])]:
            emit({"_id": 7, "v": current}, {operator: {"v": operand}})
    for _ in range(1000):
        items = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(7))]
        candidates = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(7))]
        emit({"_id": 7, "v": items, "nested": {"v": items}},
             {"$addToSet": {"v": {"$each": candidates}, "nested.v": {"$each": candidates},
                            "new.v": {"$each": candidates}}})
        emit({"_id": 7, "v": items, "grid": [items]},
             {"$pullAll": {"v": candidates, "grid.0": candidates,
                           "grid.3": candidates, "absent.v": candidates}})
    for update in [
        {"$addToSet": {}}, {"$pullAll": {}},
        {"$addToSet": {"new": {"$each": []}}},
        {"$addToSet": {"v": {"$each": 1}}},
        {"$addToSet": {"v": {"$each": [], "$sort": 1}}},
        {"$addToSet": {"v": {"$each": [], "extra": 1}}},
        {"$addToSet": {"v": {"$unknown": 1}}},
        {"$pullAll": {"v": 1}}, {"$pullAll": {"v": None}},
        {"$pullAll": {"v": {"$each": []}}},
        {"$addToSet": {"v": 1}, "$pullAll": {"v": []}},
        {"$pullAll": {"v": []}, "$set": {"v.x": 1}},
        {"$addToSet": {"v..x": 1}}, {"$pullAll": {"v..x": []}},
        {"$addToSet": []}, {"$pullAll": []},
    ]:
        emit({"_id": 7, "v": [True, 1, 1.0]}, update)
    for document, path, candidates in [
        ({"_id": 7}, "missing.x", [None]),
        ({"_id": 7, "v": 1}, "v.x", []),
        ({"_id": 7, "v": None}, "v.x", []),
        ({"_id": 7, "v": [None]}, "v.0.x", []),
        ({"_id": 7, "v": [[1, 2]]}, "v.0", [1.0]),
        ({"_id": 7, "v": [[1, 2]]}, "v.3", [1]),
        ({"_id": 7, "v": [[1, 2]]}, "v.99999999999999999999999", [1]),
        ({"_id": 7, "v": [1]}, "v.01", []),
        ({"_id": 7, "v": [1]}, "v.x", []),
        ({"_id": 7, "v": [1]}, "v.-1", []),
    ]:
        emit(document, {"$pullAll": {path: candidates}})

    for current in comparison_values:
        for candidate in comparison_values:
            emit({"_id": 7, "v": [current], "nested": {"v": [current]}},
                 {"$push": {"v": candidate, "nested.v": candidate}})
        emit({"_id": 7}, {"$push": {"missing.0.v": current}})
        emit({"_id": 7, "v": current}, {"$push": {"v": "added"}})
    for _ in range(1000):
        items = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(7))]
        additions = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(5))]
        position = randomizer.choice([-99, -3, -1, 0, 1, 3, 99])
        slice_count = randomizer.choice([-99, -2, -1, 0, 1, 2, 99])
        direction = randomizer.choice([-1, 1])
        emit({"_id": 7, "v": items}, {"$push": {"v": {
            "$slice": slice_count, "$sort": direction, "$each": additions, "$position": position}}})
        documents = [{"group": randomizer.randrange(3), "score": value, "serial": index}
                     for index, value in enumerate(items)]
        added_documents = [{"group": randomizer.randrange(3), "score": value, "serial": index + 100}
                           for index, value in enumerate(additions)]
        emit({"_id": 7, "v": documents + [None, {}]}, {"$push": {"v": {
            "$each": added_documents, "$position": position,
            "$sort": {"group": 1, "score": -1}, "$slice": slice_count}}})
        emit({"_id": 7, "v": items}, {"$push": {"v": {"$each": [], "$sort": direction}}})
    for current in [[], [[], [1]], [None, []], 1, {"0": []}, {"x": {"y": []}}]:
        for path in ["v", "v.0", "v.1", "v.3", "v.01", "v.x", "v.0.x", "v.0.0", "v.x.y"]:
            for operand in [1, {"$each": []}, {"$each": [2, 1], "$sort": 1, "$slice": 1}]:
                emit({"_id": 7, "v": current}, {"$push": {path: operand}})
    modifier_values = [0, 1, -1, 99, -99, Int64(-(2**63)), Int64(2**63 - 1),
                       1.0, -1.0, 0.5, True, False, None, "1", float("nan"),
                       float("inf"), float("-inf"), Decimal128("NaN"), Decimal128("sNaN"),
                       Decimal128("0.5"), Decimal128("-0"), Decimal128("1E6144"),
                       Decimal128("-1E6144"), Decimal128("1E-6176")]
    assert len(modifier_values) == 24
    for value in modifier_values:
        for modifier in ["$position", "$slice"]:
            emit({"_id": 7, "v": [1, 2, 3]}, {"$push": {"v": {"$each": [9], modifier: value}}})
    sort_specs = [None, True, 0, 2, 1.5, [], {}, {"a": 0}, {"a": True},
                  {"a": 1, "b": -1}, {"": 1}, {"$x": -1}, {"a.0": 1}]
    assert len(sort_specs) == 13
    for sort_spec in sort_specs:
        emit({"_id": 7, "v": [{"a": [2], "": 2, "$x": 1}, {"a": [1], "": 1, "$x": 2}, None]},
             {"$push": {"v": {"$each": [], "$sort": sort_spec}}})
    for update in [
        {"$push": {}}, {"$push": []}, {"$push": {"v": {"$each": "bad"}}},
        {"$push": {"v": {"$slice": 1}}}, {"$push": {"v": {"$sort": 1}}},
        {"$push": {"v": {"$position": 1}}},
        {"$push": {"v": {"$each": [], "extra": 1}}},
        {"$push": {"v": {"$each": [], "$unknown": 1}}},
        {"$push": {"v..x": 1}}, {"$push": {"v": 1}, "$set": {"v.0": 1}},
        {"$push": {"v": {"$each": [], "$slice": 0}}},
        {"$push": {"missing": {"$each": []}}},
    ]:
        emit({"_id": 7, "v": [1, 2]}, update)

    # 1,156 literal cases distinguish whole-array equality from field predicates.
    for actual in comparison_values:
        for condition in comparison_values:
            emit({"_id": 7, "v": [actual, actual]}, {"$pull": {"v": condition}})
    conditions = [
        {"$eq": 1}, {"$ne": 1}, {"$gte": None}, {"$gt": [1]},
        {"$lt": MaxKey()}, {"$gt": MinKey()}, {"$in": [1, True, Regex("a", "i")]},
        {"$nin": [1, None]}, {"$all": [1, 2]}, {"$size": 2}, {"$type": "array"},
        {"$type": "number"}, {"$mod": [2, 0]}, {"$exists": False},
        {"$regex": "^a", "$options": "i"}, {"$elemMatch": {"$gt": 1}},
        {"$elemMatch": {"x": {"$gte": 1}}}, {}, {"x": 1}, {"x": None},
        {"x": {"$exists": False}}, {"x": {"$not": {"$gte": 1}}},
        {"a.x": {"$gt": 1}}, {"a.0": {"$eq": 1}}, {"_id": 2},
        {"_id": {"$gt": 1}}, {"$or": [{"x": 1}, {"_id": 2}]},
        {"$and": [{"x": {"$ne": 1}}, {"a": {"$exists": True}}]},
        {"$nor": [{"x": 1}, {"_id": 2}]},
        {"a": {"$elemMatch": {"x": 1, "y": {"$gt": 2}}}},
    ]
    assert len(conditions) == 30
    arrays = [comparison_values, comparison_values[::-1], [],
              [{"x": 1}, {"x": [1, 2]}, {"_id": [1, 2]}, {"_id": 2}, {}, None],
              [{"a": [{"x": 2}]}, {"a": [1, 2]}, {"a": {"0": 1}}, {"a": [{"x": 1, "y": 3}]}],
              [[1, 2], [1, 3], [], [[1]], {"x": 2}, "Alpha", "beta", Regex("a", "i")]]
    for condition in conditions:
        for items in arrays:
            emit({"_id": 7, "v": items}, {"$pull": {"v": condition}})
    for _ in range(1000):
        items = [randomizer.choice(comparison_values) for _ in range(randomizer.randrange(8))]
        operand = randomizer.choice(comparison_values)
        comparison = {randomizer.choice(["$gt", "$gte", "$lt", "$lte"]): operand}
        emit({"_id": 7, "v": items}, {"$pull": {"v": operand}})
        emit({"_id": 7, "v": items}, {"$pull": {"v": comparison}})
        documents = [{"x": value, "_id": [1, 2]} for value in items] + [{}, None]
        emit({"_id": 7, "v": documents}, {"$pull": {"v": {"x": comparison}}})
        emit({"_id": 7, "v": documents}, {"$pull": {"v": {"$or": [{"x": comparison}, {"_id": 2}]}}})
    for current in [[], [[], [1]], [None, []], 1, {"0": []}, {"x": {"y": [1]}}]:
        for path in ["v", "v.0", "v.1", "v.3", "v.01", "v.x", "v.0.x", "v.0.0", "v.x.y"]:
            for condition in [1, {}, {"$gte": 1}]:
                emit({"_id": 7, "v": current}, {"$pull": {path: condition}})
    invalid = [
        {"$expr": {"$eq": [1, 1]}}, {"$or": [{"$expr": {"$eq": [1, 1]}}]},
        {"x": {"$expr": {"$eq": [1, 1]}}}, {"$elemMatch": {"$expr": {"$eq": [1, 1]}}},
        {"$not": {"$eq": 1}}, {"$unknown": 1}, {"$comment": "no"}, {"$where": "no"},
        {"x": {"$not": {"$unknown": 1}}}, {"$and": []}, {"$or": [1]}, {"$nor": {}},
        {"$in": None}, {"$nin": 1}, {"$all": "no"}, {"$size": -1}, {"$type": "no"},
        {"$mod": [0, 1]}, {"$elemMatch": 1}, {"$regex": "["},
        {"$regex": "a", "$options": "q"}, {"$regex": Regex("a", "i"), "$options": "m"},
        {"$options": "i"}, {"$gt": Regex("a")}, {"x": 1, "$gt": 2},
    ]
    assert len(invalid) == 25
    for condition in invalid:
        for document in [{"_id": 7}, {"_id": 7, "v": []}, {"_id": 7, "v": [1, {"x": 2}]}]:
            emit(document, {"$set": {"atomic_marker": True}, "$pull": {"v": condition}})

    # Increment's legacy Python helper shrinks Int64, permits unencodable integer
    # overflow, adds zero to missing operands, and rewrites signed-zero no-ops.
    # Compare only the exact intersection, without coercing reference results.
    # Rust/storage/wire tests independently enforce Mongo width, overflow, missing
    # operand fidelity, signed-zero no-ops, strict paths and immutable IDs.
    numbers = [
        -10, 0, 1, Int64(1), 2**31 - 1, -(2**31), 2**31, -(2**31)-1,
        Int64(2**63 - 1), Int64(-(2**63)), 2**60,
        0.0, -0.0, 0.1, 1.5, 1e16, -1e16, 5e-324, -5e-324,
        1.7976931348623157e308, float("inf"), float("-inf"),
        float("nan"), -float("nan"),
        *[Decimal128(value) for value in [
            "0", "-0", "0.00", "-0.00", "1.00", "2.1", "-2.1",
            "1E34", "-1E34", "1E-6176", "-1E-6176", "0E-6176", "0E+6111",
            "1.000000000000000000000000000000001",
            "9.999999999999999999999999999999999E+6144",
            "-9.999999999999999999999999999999999E+6144", "1E-6143", "1E+6144",
            "NaN", "-NaN", "sNaN", "-sNaN", "Infinity", "-Infinity",
        ]],
    ]

    def compatible(left, right):
        result = bson_types.add_bson_numbers(left, right)
        if isinstance(result, int):
            if not -(2**63) <= result < 2**63:
                return False
            if any(isinstance(value, Int64) or (isinstance(value, int) and not -(2**31) <= value < 2**31)
                   for value in (left, right)) and -(2**31) <= result < 2**31:
                return False
        if isinstance(result, float):
            if math.isnan(result):  # Newly computed NaN bits are not portable.
                return False
            if isinstance(left, float) and result == left and BSON.encode({"v": result}) != BSON.encode({"v": left}):
                return False
        return True

    for left in numbers:
        for right in numbers:
            if compatible(left, right):
                emit({"_id": Int64(7), "v": left, "nested": {"v": left}},
                     {"$inc": {"v": right, "nested.v": right}})
    for _ in range(1000):
        # Diverse Double bit patterns exercise the 15-significant-digit update
        # promotion independently of aggregation's exact binary conversion.
        value = struct.unpack("<d", randomizer.getrandbits(64).to_bytes(8, "little"))[0]
        decimal = randomizer.choice([value for value in numbers if isinstance(value, Decimal128)])
        emit({"_id": 7, "v": decimal}, {"$inc": {"v": value}})
        emit({"_id": 7, "v": value}, {"$inc": {"v": decimal}})
    for value in [0, 1, -1, 2**31, 0.5, Decimal128("1.00"), Decimal128("1E-6176")]:
        emit({"_id": 7}, {"$inc": {"v": value, "missing.0.value": value}})
    for value in [None, True, False, "1", [], {}, Binary(b"x"), Timestamp(0, 0), Regex("x"), MinKey(), MaxKey()]:
        for document in [{"_id": 7}, {"_id": 7, "v": 1}]:
            emit(document, {"$inc": {"v": value}})
        emit({"_id": 7, "v": value}, {"$set": {"marker": True}, "$inc": {"v": 1}})
    for update in [{"$inc": {}}, {"$inc": []}, {"$inc": {"a..b": 1}},
                   {"$inc": {"a": 1, "a.b": 2}}, {"$set": {"a": 1}, "$inc": {"a.b": 2}}]:
        emit({"_id": 7}, update)


if __name__ == "__main__":
    main()
