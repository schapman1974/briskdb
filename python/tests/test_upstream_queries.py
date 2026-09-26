"""Public query scenarios from the locked query_more/operator_coverage_edges suites.

Uses only the installed BriskDB wheel and stock PyMongo vocabulary. Legacy
TinyMongo cursor.count/negative indexing and private Python helper identities
are not claimed as unchanged client APIs; see the separate coverage inventory.
"""

import re
import tempfile
import unittest

from bson import Decimal128, Regex
from bson.errors import InvalidDocument
from pymongo.errors import OperationFailure, WriteError

import briskdb


class UpstreamQueryTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def ids(self, query):
        return {row["_id"] for row in self.items.find(query)}

    def rejects(self, query, code=2):
        with self.assertRaises(OperationFailure) as caught:
            list(self.items.find(query))
        self.assertEqual(caught.exception.code, code)

    def matches(self, document, query, expected):
        self.items.delete_many({})
        self.items.insert_one({"_id": 1, **document})
        self.assertEqual(self.ids(query), {1} if expected else set(), (document, query))

    def test_membership_logical_existence_and_filtered_counts(self):
        self.items.insert_many([
            {"_id": 1, "tag": "alpha", "tags": ["a", "b", "c"], "status": "draft", "score": 2, "meta": {"active": True}, "even": True},
            {"_id": 2, "tag": "beta", "tags": ["a", "c"], "status": "published", "score": 5, "even": False},
            {"_id": 3, "tag": "gamma", "tags": ["b", "a", "c"], "status": "archived", "score": 9, "even": True},
        ])
        self.assertEqual(self.ids({"tag": {"$nin": ["alpha", "beta"]}}), {3})
        self.assertEqual(self.ids({"tags": {"$all": ["a", "c"]}}), {1, 2, 3})
        self.assertEqual(self.ids({"$nor": [{"status": "draft"}, {"score": {"$gt": 8}}]}), {2})
        self.assertEqual(self.items.count_documents({"even": True}), 2)
        self.assertEqual(self.ids({"meta": {"$exists": True}}), {1})
        self.assertEqual(self.ids({"meta": {"$exists": False}}), {2, 3})

    def test_regex_filters_are_shared_by_single_multi_and_replacement_writes(self):
        self.items.insert_many([{"_id": 1, "name": "Alpha"}, {"_id": 2, "name": "amber"}, {"_id": 3, "name": "Beta"}])
        query = {"name": {"$regex": "^a", "$options": "i"}}
        self.assertEqual(self.items.update_one(query, {"$set": {"first": True}}).matched_count, 1)
        self.assertEqual(self.items.update_many(query, {"$set": {"matched": True}}).matched_count, 2)
        self.assertEqual(self.ids({"matched": True}), {1, 2})
        self.assertEqual(self.items.replace_one({"name": {"$regex": "^b", "$options": "i"}}, {"name": "Replaced"}).matched_count, 1)
        self.assertEqual(self.items.find_one({"_id": 3}), {"_id": 3, "name": "Replaced"})

    def test_replacement_and_find_modify_preserve_ids_and_return_old_image(self):
        self.items.insert_many([{"_id": "one", "count": 1, "tag": "keep"}, {"_id": "two", "count": 2, "tag": "keep"}])
        self.assertEqual(self.items.replace_one({"_id": "one"}, {"count": 42}).matched_count, 1)
        self.assertEqual(self.items.find_one({"_id": "one"}), {"_id": "one", "count": 42})
        self.assertEqual(self.items.find_one_and_update({"_id": "one"}, {"$set": {"count": 20}}), {"_id": "one", "count": 42})
        self.assertEqual(self.items.find_one({"_id": "one"})["count"], 20)
        self.assertEqual(self.items.find_one({"_id": "two"}), {"_id": "two", "count": 2, "tag": "keep"})

    def test_pagination_uses_modern_pymongo_not_legacy_cursor_helpers(self):
        self.items.insert_many([{"_id": number, "value": number} for number in range(20)])
        page = list(self.items.find(sort=[("value", 1)], skip=5, limit=5))
        self.assertEqual([row["value"] for row in page], [5, 6, 7, 8, 9])
        self.assertEqual(self.items.count_documents({}, skip=5, limit=5), 5)
        self.assertEqual(self.items.find(sort=[("value", 1)], skip=5, limit=5)[0]["value"], 5)

    def test_mixed_updates_create_nested_paths_and_apply_to_every_match(self):
        self.items.insert_one({"_id": 1, "count": 1, "tags": ["a"], "meta": {"old": True}})
        self.items.update_one({"_id": 1}, {"$inc": {"count": 2}, "$set": {"meta.active": True}, "$unset": {"meta.old": ""}, "$push": {"tags": "b"}})
        self.items.update_one({"_id": 1}, {"$addToSet": {"tags": "b"}})
        self.items.update_one({"_id": 1}, {"$pull": {"tags": "a"}})
        self.assertEqual(self.items.find_one({"_id": 1}), {"_id": 1, "count": 3, "tags": ["b"], "meta": {"active": True}})
        self.items.update_one({"_id": 1}, {"$set": {"profile.name": "Ada"}, "$inc": {"stats.views": 1}})
        self.assertEqual(self.items.find_one({"_id": 1})["profile"], {"name": "Ada"})
        self.assertEqual(self.items.find_one({"_id": 1})["stats"], {"views": 1})
        self.items.delete_many({})
        self.items.insert_many([{"_id": number, "group": "a" if number < 3 else "b", "count": number, "tags": []} for number in (1, 2, 3)])
        result = self.items.update_many({"group": "a"}, {"$inc": {"count": 10}, "$push": {"tags": "updated"}})
        self.assertEqual((result.matched_count, result.modified_count), (2, 2))
        self.assertEqual([self.items.find_one({"_id": number})["count"] for number in (1, 2, 3)], [11, 12, 3])
        self.assertEqual(self.ids({"tags": {"$all": ["updated"]}}), {1, 2})
        self.assertEqual(self.items.update_many({"group": "a"}, {"$set": {"active": True}}).modified_count, 2)
        self.assertEqual(self.ids({"active": True}), {1, 2})

    def test_invalid_updates_leave_records_unchanged(self):
        document = {"_id": 1, "tags": "not-a-list"}
        self.items.insert_one(document)
        with self.assertRaises(WriteError) as caught:
            self.items.update_one({"_id": 1}, {"$push": {"tags": "new"}})
        self.assertEqual(caught.exception.code, 2)
        with self.assertRaises(ValueError):
            self.items.update_many({}, {"replacement": True})
        self.assertEqual(self.items.find_one({"_id": 1}), document)

    def test_equality_index_results_follow_insert_update_delete_and_drop(self):
        self.items.insert_many([{"_id": 1, "email": "a@example.com", "tags": ["same", "same"]}, {"_id": 2, "email": "b@example.com", "tags": "same"}])
        self.assertEqual(self.ids({"tags": "same"}), {1, 2})
        for field in ("email", "secondary", "tags"):
            self.assertEqual(self.items.create_index(field), field + "_1")
        self.assertEqual(dict(self.items.index_information()["email_1"]["key"]), {"email": 1})
        self.assertEqual(len(list(self.items.find({"tags": "same"}))), 2)
        self.items.update_one({"_id": 2}, {"$set": {"email": "c@example.com"}})
        self.assertEqual(self.ids({"email": "c@example.com"}), {2})
        self.items.insert_one({"_id": 3, "email": "d@example.com"})
        self.assertEqual(self.ids({"email": "d@example.com"}), {3})
        self.items.delete_one({"_id": 3})
        self.assertEqual(self.ids({"email": "d@example.com"}), set())
        self.client.app.items.drop_index("secondary")
        self.items.drop_index("email")
        self.items.drop_index("tags")
        self.assertEqual(set(self.items.index_information()), {"_id_"})
        self.assertEqual(self.ids({"tags": "same"}), {1, 2})

    def test_size_mod_and_type_validation_keep_mongo_error_codes(self):
        for operand in (float("nan"), 1.5, -1, 2**31):
            self.rejects({"value": {"$size": operand}})
        self.rejects({"value": {"$mod": [0, 0]}})
        for operand, code in (([], 9), (42, 2), (True, 14)):
            self.rejects({"value": {"$type": operand}}, code)

    def test_malformed_queries_and_known_unsupported_operators_are_explicit(self):
        for query in ({"$and": []}, {"value": {"$eq": 1, "literal": 1}}, {"value": {"$in": 1}}, {"value": {"$options": "i"}}):
            self.rejects(query)
        for query in ({"$expr": {"$eq": ["$value", 1]}}, {"$jsonSchema": {"required": ["value"]}}, {"values": {"$elemMatch": {"$jsonSchema": {"required": ["score"]}}}}, {"values": {"$elemMatch": {"$bitsAllSet": 1}}}):
            self.rejects(query, 115)

    def test_mod_nonfinite_and_out_of_int64_bson_numbers_do_not_match(self):
        for value in (float("nan"), float("inf"), Decimal128("9223372036854775808"), Decimal128("-9223372036854775809")):
            self.matches({"value": value}, {"value": {"$mod": [2, 0]}}, False)
        self.matches({"value": 6}, {"value": {"$mod": [2, 0]}}, True)

    def test_unencodable_python_values_fail_at_the_driver_boundary(self):
        with self.assertRaises(InvalidDocument):
            list(self.items.find({1: "value"}))
        with self.assertRaises(InvalidDocument):
            self.items.insert_one({"_id": 1, "value": object()})
        for value in (2**63, -(2**63) - 1):
            with self.assertRaises(OverflowError):
                self.items.insert_one({"_id": 1, "value": value})
        self.assertEqual(self.items.count_documents({}), 0)

    def test_type_alias_duplicates_and_decimal_types_preserve_matches(self):
        self.items.insert_many([{"_id": 1, "value": "text"}, {"_id": 2, "value": 1}, {"_id": 3, "value": Decimal128("1.25")}])
        self.assertEqual(self.ids({"value": {"$type": ["string", 2, "string"]}}), {1})
        self.assertEqual(self.ids({"value": {"$type": 19}}), {3})
        self.assertEqual(self.ids({"value": {"$type": "number"}}), {2, 3})

    def test_empty_elem_match_requires_container_array_members(self):
        query = {"value": {"$elemMatch": {}}}
        for value, expected in (([{"kind": "document"}], True), ([["nested-array"]], True), ([1, "scalar", None], False), ("not-an-array", False)):
            self.matches({"value": value}, query, expected)

    def test_numeric_paths_preserve_indexed_endpoints_and_named_field_fanout(self):
        for document, query, expected in [
            ({"value": [{"3": "named"}]}, {"value.3": "named"}, True),
            ({"value": ["zero"]}, {"value.0": "zero"}, True),
            ({"value": list(range(13))}, {"value.12": 12}, True),
            ({"value": [{"0": "field"}, "index"]}, {"value.0": "field"}, True),
            ({"value": [{"0": "field"}, "index"]}, {"value.0": {"0": "field"}}, True),
            ({"value": [[1, 2]]}, {"value.0": 1}, False),
            ({"value": [[1, 2]]}, {"value.0": [1, 2]}, True),
            ({"value": [[1, 2], {"0": 1}]}, {"value.0": 1}, True),
            ({"value": [1]}, {"value.0.nested": None}, False),
            ({"value": ["zero", "one"]}, {"value.01": "one"}, False),
        ]:
            self.matches(document, query, expected)

    def test_missing_null_and_multiple_path_candidates_keep_operator_semantics(self):
        for document, query, expected in [
            ({"rows": [{"value": 1}, {"value": 2}]}, {"rows.value": 2}, True),
            ({"rows": [{"value": 1}, {"value": 2}]}, {"rows.value": {"$gt": 1}}, True),
            ({"rows": [{"value": 1}, {"value": 2}]}, {"rows.value": {"$gt": 3}}, False),
            ({"rows": [{"value": 1}, {"value": 2}]}, {"rows.value": {"$ne": 1}}, False),
            ({"rows": [{"name": "Ada"}, {"name": "Grace"}]}, {"rows.name": {"$regex": "^a", "$options": "i"}}, True),
            ({"value": [{"a": 1}, {"a": 2}]}, {"value": {"a": 2}}, True),
            ({"rows": [{}, {}]}, {"rows.value": {"$exists": False}}, True),
            ({"value": {}}, {"value.missing": None}, True),
            ({"value": {}}, {"value.missing": 0}, False),
            ({"value": 1}, {"value.missing": None}, True),
            ({"value": []}, {"value.missing": None}, False),
            ({"value": []}, {"value.missing": {"$ne": None}}, True),
            ({"value": []}, {"value.missing": {"$exists": False}}, True),
            ({"value": []}, {"value.missing": {"$exists": True}}, False),
        ]:
            self.matches(document, query, expected)

    def test_all_elem_match_and_nested_membership_operator_validation(self):
        query = {"value": {"$all": [{"$elemMatch": {"$gt": 1, "$lt": 3}}]}}
        self.matches({"value": [0, 2, 4]}, query, True)
        self.matches({"value": [0, 4]}, query, False)
        for operator in ("$all", "$in", "$nin"):
            self.rejects({"value": {operator: [{"$gt": 1}]}})
            self.rejects({"value": {operator: [{"$regex": "^value"}]}})

    def test_elem_match_accepts_logical_document_queries(self):
        query = {"value": {"$elemMatch": {"$or": [{"kind": "quiz"}, {"score": {"$gt": 8}}]}}}
        for value, expected in (([{"kind": "quiz", "score": 1}], True), ([{"kind": "exam", "score": 9}], True), ([{"kind": "exam", "score": 8}], False)):
            self.matches({"value": value}, query, expected)

    def test_regex_validation_traverses_legacy_logical_and_value_shapes(self):
        query = {"$and": [{"name": {"$regex": "^ada", "$options": "i"}}, {"alias": {"$not": {"$regex": "^admin"}}}],
                 "tags": {"$in": ["python", re.compile("^database")]}, "literal": re.compile("^value"),
                 "not_literal": {"$not": Regex("^hidden")}}
        self.matches({"name": "Ada", "alias": "user", "tags": ["database-tools"], "literal": "value", "not_literal": "shown"}, query, True)


if __name__ == "__main__":
    unittest.main()
