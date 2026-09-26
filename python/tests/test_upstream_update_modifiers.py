"""Owned wire scenarios mapped to the locked update-operator-modifiers suite.

These exercise stored records through PyMongo, not TinyMongo's private Python
update/path helpers. Exact provenance and helper-only exclusions live in the
existing query suite inventory.
"""

import unittest

from pymongo.errors import WriteError

from _modifier_harness import ModifierHarness


class UpstreamUpdateModifierTests(ModifierHarness, unittest.TestCase):
    def test_min_max_bson_order_preserves_equal_numeric_representation(self):
        result = self.apply(
            {"_id": 1, "low": "text", "high": 1, "equal": 1.0, "values": [2]},
            {"$min": {"low": 5, "values": [1]}, "$max": {"high": {"v": 1}, "equal": 1}},
        )
        self.assertEqual(result, {"_id": 1, "low": 5, "high": {"v": 1}, "equal": 1.0, "values": [1]})
        self.assertIs(type(result["equal"]), float)

    def test_missing_nested_fields_and_empty_operands(self):
        result = self.apply(
            {"_id": 1, "profile": {"keep": True}},
            {"$min": {"profile.low": 2, "created.low": 3},
             "$max": {"profile.high": 8, "created.high": 9}},
        )
        self.assertEqual(result, {"_id": 1, "profile": {"keep": True, "low": 2, "high": 8},
                                  "created": {"low": 3, "high": 9}})
        changed = self.items.update_one({"_id": 1}, {"$min": {}, "$max": {}, "$rename": {}, "$pop": {}})
        self.assertEqual((changed.matched_count, changed.modified_count), (1, 0))
        self.assertEqual(self.items.find_one({"_id": 1}), result)

    def test_numeric_array_paths_and_sparse_growth(self):
        result = self.apply(
            {"_id": 1, "values": [5, 8], "nested": [[1, 2], [3]], "documents": [{"score": 9}]},
            {"$min": {"values.0": 3, "values.3": 7, "documents.0.score": 4, "documents.2.score": 4},
             "$max": {"values.1": 10}, "$pop": {"nested.0": 1}},
        )
        self.assertEqual(result, {"_id": 1, "values": [3, 10, None, 7], "nested": [[1], [3]],
                                  "documents": [{"score": 4}, None, {"score": 4}]})

    def test_existing_scalar_null_and_array_ancestors_reject_without_mutation(self):
        for value, update in [
            ("scalar", {"$min": {"path.value": 1}}),
            (None, {"$max": {"path.value": 1}}),
            ([None], {"$min": {"path.0.value": 1}}),
            ("scalar", {"$pop": {"path.values": 1}}),
        ]:
            with self.subTest(value=value, update=update):
                self.reject({"_id": 1, "path": value}, update, 28)

    def test_rename_moves_overwrites_and_ignores_missing_source(self):
        result = self.apply(
            {"_id": 1, "source": {"value": 1}, "destination": "old",
             "nested": {"old": [1, 2], "keep": True}},
            {"$rename": {"source": "destination", "nested.old": "created.deep.value", "missing": "untouched"}},
        )
        self.assertEqual(result, {"_id": 1, "destination": {"value": 1}, "nested": {"keep": True},
                                  "created": {"deep": {"value": [1, 2]}}})

    def test_rename_invalid_overlapping_and_positional_paths(self):
        for source, destination in [("field", 1), ("field", "field"), ("field", "field.child"),
                                    ("field.child", "field"), ("field.$", "other"),
                                    ("field", "other.$[item]")]:
            with self.subTest(source=source, destination=destination):
                self.reject({"_id": 1, "field": {"child": 1}}, {"$rename": {source: destination}}, 2)

    def test_update_path_conflicts_empty_paths_and_operand_types(self):
        for update, code in [
            ({"$min": {"other": 1}, "$max": {"other": 3}}, 40),
            ({"$set": {"field": {}}, "$pop": {"field.child": 1}}, 40),
            ({"$rename": {"other": "moved"}, "$set": {"moved": 3}}, 40),
            ({"$rename": {"": "moved"}}, 56),
            ({"$min": {"field.": 1}}, 56),
            ({"$pop": []}, 9),
        ]:
            with self.subTest(update=update):
                self.reject({"_id": 1, "field": {"child": 1}, "other": 2}, update, code)

    def test_pop_both_directions_missing_empty_and_integral_float_forms(self):
        result = self.apply(
            {"_id": 1, "front": [1, 2, 3], "back": [1, 2, 3], "empty": [], "nested": {"values": [4, 5]}},
            {"$pop": {"front": -1, "back": 1, "empty": 1, "missing": -1, "nested.values": -1}},
        )
        self.assertEqual(result, {"_id": 1, "front": [2, 3], "back": [1, 2], "empty": [], "nested": {"values": [5]}})
        for direction, expected in [(1.0, [1]), (-1.0, [2])]:
            with self.subTest(direction=direction):
                self.assertEqual(self.apply({"_id": 1, "values": [1, 2]}, {"$pop": {"values": direction}})["values"], expected)

    def test_pop_rejects_invalid_directions(self):
        for direction in [0, 2, -2, 1.5, True, "1", {}, []]:
            with self.subTest(direction=direction):
                self.reject({"_id": 1, "values": [1, 2]}, {"$pop": {"values": direction}}, 9)

    def test_pop_rejects_nonarray_targets(self):
        for value in [None, "scalar", 1, {}]:
            with self.subTest(value=value):
                self.reject({"_id": 1, "values": value}, {"$pop": {"values": 1}}, 14)

    def test_failed_modifier_cannot_commit_an_earlier_set(self):
        self.reject({"_id": 1, "status": "original", "values": "scalar"},
                    {"$set": {"status": "changed"}, "$pop": {"values": 1}}, 14)

    def test_no_match_still_validates_before_writing(self):
        with self.assertRaises(WriteError) as caught:
            self.items.update_one({"_id": "missing"}, {"$pop": {"values": 0}})
        self.assertEqual(caught.exception.code, 9)
        self.assertEqual(self.items.count_documents({}), 0)

    def test_upserts_apply_modifiers_to_equality_seed(self):
        cases = [
            ({"_id": "min", "score": 5}, {"$min": {"score": 3}}, {"_id": "min", "score": 3}),
            ({"_id": "max", "score": 5}, {"$max": {"score": 7}}, {"_id": "max", "score": 7}),
            ({"_id": "pop", "values": [1, 2]}, {"$pop": {"values": 1}}, {"_id": "pop", "values": [1]}),
            ({"_id": "rename", "source": "value"}, {"$rename": {"source": "destination"}},
             {"_id": "rename", "destination": "value"}),
        ]
        for query, update, expected in cases:
            with self.subTest(query=query):
                result = self.items.update_one(query, update, upsert=True)
                self.assertEqual(result.upserted_id, query["_id"])
                self.assertEqual(self.items.find_one({"_id": query["_id"]}), expected)

    def test_public_path_edges_corresponding_to_private_helper_cases(self):
        for original, update, code in [
            ({"_id": 1, "path": []}, {"$rename": {"path.value": "other"}}, 28),
            ({"_id": 1, "path": []}, {"$min": {"path.value": 1}}, 28),
            ({"_id": 1, "path": "scalar", "value": 1}, {"$rename": {"value": "path.value"}}, 28),
            ({"_id": 1, "path": [], "value": 1}, {"$rename": {"value": "path.value"}}, 28),
            ({"_id": 1, "path": []}, {"$set": {"path.value": 1}}, 28),
            ({"_id": 1, "path": [None]}, {"$set": {"path.0.value": 1}}, 28),
            ({"_id": 1, "path": [{"value": 1}]}, {"$rename": {"path.0.value": "other"}}, 2),
            ({"_id": 1, "path": []}, {"$rename": {"path.0": "other"}}, 2),
            ({"_id": 1, "path": []}, {"$rename": {"path.value.child": "other"}}, 28),
            ({"_id": 1, "path": "scalar"}, {"$rename": {"path.value.child": "other"}}, 28),
            ({"_id": 1, "path": "scalar"}, {"$rename": {"path.value": "other"}}, 28),
        ]:
            with self.subTest(original=original, update=update):
                self.reject(original, update, code)
        result = self.apply({"_id": 1, "path": [{}]}, {"$set": {"path.0.value": 1}, "$rename": {"missing.value": "other"}})
        self.assertEqual(result, {"_id": 1, "path": [{"value": 1}]})
        self.reject({"_id": 1}, {"$set": {"_id": 2}}, 66)


if __name__ == "__main__":
    unittest.main()
