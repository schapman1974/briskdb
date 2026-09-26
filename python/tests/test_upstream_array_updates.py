"""Owned stored-BSON counterparts of the locked array-update modifier suite."""

import unittest

from bson.errors import InvalidDocument
from pymongo.errors import WriteError

from _modifier_harness import ModifierHarness


class UpstreamArrayUpdateTests(ModifierHarness, unittest.TestCase):
    def test_push_rejects_malformed_modifiers_before_mutation(self):
        for operand in [
            {"$each": "value"}, {"$slice": 2}, {"$each": [], "$unknown": 1},
            {"$each": [], "$position": True}, {"$each": [], "$position": 1.5},
            {"$each": [], "$slice": False}, {"$each": [], "$slice": 1.5},
            {"$each": [], "$sort": 0}, {"$each": [], "$sort": True},
            {"$each": [], "$sort": {}}, {"$each": [], "$sort": {"score": 0}},
        ]:
            with self.subTest(operand=operand):
                self.reject({"_id": 1, "values": [1]}, {"$push": {"values": operand}}, 2)
        # Unlike a direct Python helper, BSON rejects non-string field keys
        # during driver encoding, before the native updater receives a request.
        with self.assertRaises(InvalidDocument):
            self.items.update_one({"_id": 1}, {"$push": {"values": {"$each": [], "$sort": {1: 1}}}})
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "values": [1]})

    def test_add_to_set_rejects_malformed_modifiers(self):
        for operand in [{"$each": "value"}, {"$unknown": []}, {"$each": [], "$slice": 1}]:
            with self.subTest(operand=operand):
                self.reject({"_id": 1, "values": []}, {"$addToSet": {"values": operand}})

    def test_push_sort_compares_whole_recursive_bson_and_document_fields(self):
        result = self.apply({"_id": 1, "values": [[3, 0], "text", [], None]},
                            {"$push": {"values": {"$each": [[1]], "$sort": 1.0}}})
        self.assertEqual(result["values"], [None, "text", [], [1], [3, 0]])
        result = self.apply({"_id": 1, "values": [{"score": 1}, {}, {"score": None}]},
                            {"$push": {"values": {"$each": [{"score": 2}], "$sort": {"score": -1.0}}}})
        self.assertEqual(result["values"][:2], [{"score": 2}, {"score": 1}])
        self.assertCountEqual(result["values"][2:], [{}, {"score": None}])

    def test_add_to_set_uses_bson_equality_and_initializes_an_empty_array(self):
        result = self.apply({"_id": 1, "values": [1, {"a": 1, "b": 2}, "duplicate", "duplicate"]},
                            {"$addToSet": {"values": {"$each": [1.0, True, {"b": 2, "a": 1}, True]}}})
        self.assertEqual(len(result["values"]), 6)
        self.assertIs(result["values"][-2], True)
        self.assertEqual(list(result["values"][-1]), ["b", "a"])
        self.assertEqual(result["values"].count("duplicate"), 2)
        self.assertEqual(self.apply({"_id": 2}, {"$addToSet": {"values": {"$each": []}}}), {"_id": 2, "values": []})

    def test_pull_exact_arrays_document_queries_logical_queries_and_type_brackets(self):
        source = {"_id": 1, "values": [[1, 2], [2, 1], {"kind": "keep", "score": 1},
                                        {"kind": "drop", "score": 8}, {"kind": "also-drop", "score": 9}, 1]}
        exact = self.apply(source, {"$pull": {"values": [1, 2]}})
        self.assertEqual(exact["values"], source["values"][1:])
        queried = self.apply(exact, {"$pull": {"values": {"$or": [{"kind": "drop"}, {"score": {"$gte": 9}}]}}})
        self.assertEqual(queried["values"], [[2, 1], {"kind": "keep", "score": 1}, 1])
        self.assertEqual(self.apply(queried, {"$pull": {"values": {}}})["values"], [[2, 1], 1])
        self.assertEqual(self.apply({"_id": 2, "values": [[1, 2], [2, 3], 1]},
                                    {"$pull": {"values": {"$eq": 1}}})["values"], [[2, 3]])
        typed = self.apply({"_id": 3, "values": [True, 1, 2, "3"]}, {"$pull": {"values": {"$gte": 1}}})
        self.assertEqual(typed["values"], [True, "3"])
        self.assertIs(typed["values"][0], True)
        values = [{"kind": "keep", "score": 9}, {"kind": "keep", "score": 1},
                  {"kind": "keep"}, {"kind": "other", "score": 9}]
        result = self.apply({"_id": 4, "values": values},
                            {"$pull": {"values": {"$and": [{"kind": "keep"}, {"score": {"$gte": 8}}]}}})
        self.assertEqual(result["values"], values[1:])
        values = [{"kind": "keep", "score": 1}, {"kind": "other", "score": 9}, {"kind": "other", "score": 1}]
        result = self.apply({"_id": 5, "values": values},
                            {"$pull": {"values": {"$nor": [{"kind": "keep"}, {"score": {"$gte": 8}}]}}})
        self.assertEqual(result["values"], values[:2])
        result = self.apply({"_id": 6, "values": [{"meta": {"score": 1}}, {"meta": {"score": 2}}]},
                            {"$pull": {"values": {"meta": {"score": 1}}}})
        self.assertEqual(result["values"], [{"meta": {"score": 2}}])

    def test_pull_membership_regex_and_elem_match(self):
        values = ["alpha", "ALPHA", "beta", "other", [1, 3], [1, 2]]
        for predicate, expected in [
            ({"$in": ["alpha", "beta"]}, ["ALPHA", "other", [1, 3], [1, 2]]),
            ({"$nin": ["alpha", "beta"]}, ["alpha", "beta"]),
            ({"$regex": "^alpha$", "$options": "i"}, ["beta", "other", [1, 3], [1, 2]]),
            ({"$elemMatch": {"$gt": 2}}, ["alpha", "ALPHA", "beta", "other", [1, 2]]),
        ]:
            with self.subTest(predicate=predicate):
                self.assertEqual(self.apply({"_id": 1, "values": values}, {"$pull": {"values": predicate}})["values"], expected)

    def test_pull_push_missing_numeric_sparse_paths_and_nonarray_errors(self):
        self.assertEqual(self.apply({"_id": 1, "nested": {}}, {"$pull": {"absent": "x", "nested.absent": "x"}}),
                          {"_id": 1, "nested": {}})
        self.assertEqual(self.apply({"_id": 2, "nested": [{"values": ["x", "y"]}]},
                                    {"$pull": {"nested.0.values": "x"}})["nested"], [{"values": ["y"]}])
        self.assertEqual(self.apply({"_id": 3, "nested": []}, {"$push": {"nested.2.values": "x"}})["nested"],
                          [None, None, {"values": ["x"]}])
        for operator in ["$pull", "$push"]:
            self.reject({"_id": 4, "values": "scalar"}, {operator: {"values": "x"}}, 2)
        self.reject({"_id": 5, "nested": None}, {"$pull": {"nested.values": "x"}}, 28)

    def test_pull_all_literal_bson_equality_and_operand_validation(self):
        result = self.apply({"_id": 1, "values": [True, 1, 1.0, [1, 2], [2, 1], {"a": 1, "b": 2}, {"b": 2, "a": 1}]},
                            {"$pullAll": {"values": [1.0, [1, 2], {"a": 1, "b": 2}]}})
        self.assertEqual(result["values"], [True, [2, 1], {"b": 2, "a": 1}])
        self.assertIs(result["values"][0], True)
        self.assertEqual(list(result["values"][-1]), ["b", "a"])
        self.assertEqual(self.apply({"_id": 2}, {"$pullAll": {"values": [1]}}), {"_id": 2})
        self.reject({"_id": 3, "values": "scalar"}, {"$pullAll": {"values": ["x"]}}, 2)
        for value in ["x", None, {"x": 1}]:
            self.reject({"_id": 4, "values": []}, {"$pullAll": {"values": value}}, 2)

    def test_pull_rejects_malformed_and_unsupported_predicates(self):
        for predicate in [
            {"$unknown": 1}, {"score": {"$unknown": 1}}, {"$or": {"score": 1}},
            {"$and": [1]}, {"$or": []}, {"$not": {"$unknown": 1}}, {"$all": 2},
            {"$not": {"$eq": 2}}, {"$options": "i"}, {"$regex": "["},
            {"$in": "value"}, {"$nin": "value"}, {"$elemMatch": "value"},
            {"$gte": 1, "$or": [{"score": 1}]}, {"$and": [{"$gte": 1}, {"$lte": 2}]},
            {"$or": [{"$eq": 1}, {"$eq": 2}]}, {"a": {"$or": [{"b": 1}, {"b": 2}]}},
            {"score": {"$gte": 1, "plain": 2}},
        ]:
            with self.subTest(predicate=predicate):
                self.reject({"_id": 1, "values": [1]}, {"$pull": {"values": predicate}})
        with self.assertRaises(InvalidDocument):
            self.items.update_one({"_id": 1}, {"$pull": {"values": {"$gte": {1}}}})
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "values": [1]})

    def test_pull_rejects_top_level_not_with_code_two(self):
        self.reject({"_id": 1, "values": [1, 2]}, {"$pull": {"values": {"$not": {"$eq": 2}}}}, 2)

    def test_preflight_and_numeric_failures_cannot_partially_write(self):
        original = {"_id": 1, "status": "original", "values": [1]}
        self.items.insert_one(original)
        with self.assertRaises(WriteError):
            self.items.update_many({}, {"$set": {"status": "changed"}, "$push": {"values": {"$each": [], "$unknown": 1}}})
        with self.assertRaises(WriteError):
            self.items.update_one({"_id": "missing"}, {"$unknown": {"value": 1}})
        for query, update in [({"_id": "missing"}, {"$inc": {"value": "one"}}),
                              ({"_id": 1}, {"$inc": {"status": 1}})]:
            with self.assertRaises(WriteError) as caught:
                self.items.update_one(query, update)
            self.assertEqual(caught.exception.code, 14)
        self.assertEqual(self.items.find_one({"_id": 1}), original)
        self.assertEqual(self.items.count_documents({}), 1)


if __name__ == "__main__":
    unittest.main()
