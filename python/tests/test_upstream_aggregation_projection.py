"""Owned public scenarios from the locked aggregation projection-stage suite."""

from copy import deepcopy
import tempfile
import unittest

from bson import Binary
from bson.errors import InvalidDocument
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.errors import OperationFailure

import briskdb


def fixture():
    return {"_id": 1, "name": "Ada", "secret": "hidden", "source": 5,
            "a": {"b": 2, "c": 3}, "arr": [{"x": 1, "keep": "a"}, {"y": 2}, 3],
            "scalar": 5, "empty": [], "nested": [[{"a": 1}], 2],
            "profile": {"email": "ada@example.com", "secret": "hidden"}}


class UpstreamAggregationProjectionTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.client = briskdb.MongoClient(root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def rows(self, pipeline):
        original = deepcopy(pipeline)
        result = list(self.items.aggregate(pipeline))
        self.assertEqual(pipeline, original)
        return result

    def seed(self):
        self.items.insert_one(fixture())

    def test_project_inclusion_exclusion_and_id_modes(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"name": 1}}]), [{"_id": 1, "name": "Ada"}])
        expected = fixture()
        del expected["secret"], expected["profile"]["secret"], expected["arr"][0]["x"]
        self.assertEqual(self.rows([{"$project": {"secret": 0, "profile.secret": 0,
                                                   "arr.x": 0, "_id": 1}}]), [expected])
        expected = fixture(); del expected["_id"]
        self.assertEqual(self.rows([{"$project": {"_id": 0}}]), [expected])
        del expected["a"]["b"]
        self.assertEqual(self.rows([{"$project": {"_id": 0, "a": {"b": 0}}}]), [expected])
        self.assertEqual(self.rows([{"$project": {"_id": False, "name": 2, "absent": 7}}]),
                         [{"name": "Ada"}])

    def test_project_computes_nested_renames_against_original_input(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": "$a.b", "a": {"b": 1, "copied": "$source"},
                                                   "new": {"label": "literal"}, "renamed": "$name",
                                                   "missing": "$missing"}}]),
                         [{"_id": 2, "a": {"b": 2, "copied": 5}, "new": {"label": "literal"},
                           "renamed": "Ada"}])

    def test_project_preserves_source_field_order_then_computed_order(self):
        self.items.insert_one({"_id": 1, "a": {"y": 2, "x": 1, "old": 0}, "b": 2, "source": 9, "c": 3})
        row = self.rows([{"$project": {"_id": 0, "new_one": "$source", "c": 1, "new_two": "$source",
                                       "b": 1, "a.old": "$source", "a.x": 1, "a.y": "$source"}}])[0]
        self.assertEqual(list(row), ["a", "b", "c", "new_one", "new_two"])
        self.assertEqual(list(row["a"]), ["x", "old", "y"])
        self.assertEqual(row, {"a": {"x": 1, "old": 9, "y": 9}, "b": 2, "c": 3,
                               "new_one": 9, "new_two": 9})

    def test_project_dotted_outputs_preserve_array_shape_and_missing_shells(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": 0, "arr.x": 1, "arr.z": "$source",
                                                   "scalar.z": "$source", "empty.z": "$source",
                                                   "nested.z": "$source", "missing.z": "$missing"}}]),
                         [{"arr": [{"x": 1, "z": 5}, {"z": 5}, {"z": 5}], "scalar": {"z": 5},
                           "empty": [], "nested": [[{"z": 5}], {"z": 5}], "missing": {}}])

    def test_project_include_only_branches_skip_scalar_array_members(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": 0, "arr.x": 1}}]), [{"arr": [{"x": 1}, {}]}])
        self.assertEqual(self.rows([{"$project": {"arr.x": 1, "scalar.x": 1, "copy": "$source"}}]),
                         [{"_id": 1, "arr": [{"x": 1}, {}], "copy": 5}])

    def test_literal_does_not_evaluate_operand(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": 0, "numeric": {"$literal": 2},
                         "boolean": {"$literal": False}, "path": {"$literal": "$source"},
                         "document": {"$literal": {"$size": "$arr"}},
                         "array": {"$literal": ("$source", {"$size": "$arr"})}, "projection_flag": 2}}]),
                         [{"numeric": 2, "boolean": False, "path": "$source", "document": {"$size": "$arr"},
                           "array": ["$source", {"$size": "$arr"}]}])

    def test_binary_pipeline_constants_keep_subtype_decoding(self):
        self.items.insert_one({"_id": 1})
        generic, custom = Binary(b"generic", 0), Binary(b"custom", 128)
        self.assertEqual(self.rows([{"$project": {"_id": 0, "direct": generic,
                         "literal": {"$literal": generic}, "custom": {"$literal": custom}}}]),
                         [{"direct": b"generic", "literal": b"generic", "custom": custom}])

    def test_remove_is_missing_except_in_arrays_or_literals(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": 0, "name": 1, "secret": "$$REMOVE",
                         "array": ["$$REMOVE"], "fallback": {"$ifNull": ["$$REMOVE", "fallback"]},
                         "literal": {"$literal": "$$REMOVE"}, "new.deep": "$$REMOVE", "arr.z": "$$REMOVE"}}]),
                         [{"name": "Ada", "array": [None], "fallback": "fallback", "literal": "$$REMOVE",
                           "new": {}, "arr": [{}, {}, {}]}])

    def test_remove_suffixes_resolve_to_missing(self):
        self.items.insert_one({"_id": 1, "gone": True, "nested": {"gone": True}})
        self.assertEqual(self.rows([{"$project": {"_id": 0, "gone": "$$REMOVE.foo",
                         "holder.deep": "$$REMOVE.foo.bar", "array": ["$$REMOVE.0"]}}]),
                         [{"holder": {}, "array": [None]}])
        self.assertEqual(self.rows([{"$set": {"gone": "$$REMOVE.foo", "nested.gone": "$$REMOVE.0"}}]),
                         [{"_id": 1, "nested": {}}])

    def test_field_references_do_not_recursively_cross_nested_arrays(self):
        self.seed()
        self.assertEqual(self.rows([{"$project": {"_id": 0, "value": "$nested.a"}}]), [{"value": []}])

    def test_set_aliases_use_original_input_and_allow_empty_noop(self):
        document = {"_id": 1, "name": "Ada", "gone": "remove me", "nested": {"value": 3}}
        self.items.insert_one(deepcopy(document))
        for stage in ["$set", "$addFields"]:
            with self.subTest(stage=stage):
                self.assertEqual(self.rows([{stage: {}}]), [document])
                self.assertEqual(self.rows([{stage: {"_id": 2, "name": "new", "previous": "$name",
                         "old_id": "$_id", "nested_copy": "$nested.value", "gone": "$missing"}}]),
                         [{"_id": 2, "name": "new", "nested": {"value": 3}, "previous": "Ada",
                           "old_id": 1, "nested_copy": 3}])
        self.assertEqual(self.items.find_one(), document)

    def test_set_dotted_paths_cross_objects_scalars_and_arrays(self):
        self.seed()
        expected = fixture()
        expected.update(a={"b": 2, "c": 3, "new": 5}, arr=[{"x": 1, "keep": "a", "z": 5},
                        {"y": 2, "z": 5}, {"z": 5}], scalar={"z": 5},
                        nested=[[{"a": 1, "z": 5}], {"z": 5}], missing={})
        self.assertEqual(self.rows([{"$set": {"a.new": "$source", "arr.z": "$source",
                         "scalar.z": "$source", "empty.z": "$source", "nested.z": "$source",
                         "missing.z": "$missing"}}]), [expected])

    def test_tuple_containers_are_encoded_as_arrays_without_input_mutation(self):
        document = {"_id": 1, "tuple_parent": ({"old": True}, 2), "array": [({"old": True}, 2)]}
        original = deepcopy(document)
        self.items.insert_one(document)
        self.assertEqual(self.rows([{"$set": {"tuple_parent.new": 1, "array.new": 1}}]),
                         [{"_id": 1, "tuple_parent": [{"old": True, "new": 1}, {"new": 1}],
                           "array": [[{"old": True, "new": 1}, {"new": 1}]]}])
        self.assertEqual(document, original)

    def test_nested_set_modifies_paths_while_literal_replaces_values(self):
        self.items.insert_one({"_id": 1, "a": {"b": 2, "c": 3}, "whole": {"old": True},
                               "empty": {"old": True}})
        self.assertEqual(self.rows([{"$set": {"a": {"b": 1}, "whole": {"$literal": {"b": 1}}, "empty": {}}}]),
                         [{"_id": 1, "a": {"b": 1, "c": 3}, "whole": {"b": 1}, "empty": {}}])

    def test_set_remove_deletes_leaves_and_preserves_containers(self):
        self.seed()
        expected = fixture()
        del expected["secret"], expected["a"]["b"], expected["arr"][0]["x"]
        expected.update(scalar={}, new={}); expected["arr"][2] = {}
        self.assertEqual(self.rows([{"$set": {"secret": "$$REMOVE", "a.b": "$$REMOVE",
                         "arr.x": "$$REMOVE", "scalar.z": "$$REMOVE", "new.deep": "$$REMOVE"}}]), [expected])

    def test_unset_accepts_string_list_and_tuple_with_exclusion_semantics(self):
        self.seed()
        expected = fixture(); del expected["secret"]
        self.assertEqual(self.rows([{"$unset": "secret"}]), [expected])
        del expected["_id"], expected["profile"]["secret"], expected["arr"][0]["x"]
        paths = ["_id", "secret", "profile.secret", "arr.x"]
        for argument in [paths, tuple(paths)]:
            self.assertEqual(self.rows([{"$unset": argument}]), [expected])

    def test_projection_stage_errors_keep_codes_before_catalog_creation(self):
        for stage, code in [
            ({"$project": {}}, 51272), ({"$project": []}, 15969), ({"$project": {"a": {}}}, 51270),
            ({"$project": {"a": {"": 1}}}, 40352),
            ({"$project": {"value": {"$ifNull": []}}}, 1257300),
            ({"$project": {"value": {"$size": []}}}, 16020),
            ({"$project": {"a": 0, "copy": "$source"}}, 31310),
            ({"$project": {"a": 0, "copy": {"$literal": 1}}}, 31252),
            ({"$project": {"a": 0, "copy": {"$ifNull": []}}}, 31252),
            ({"$project": {"copy": "$source", "a": 0}}, 31254),
            ({"$project": {"a": 0, "name": 1}}, 31253),
            ({"$project": {"a": 1, "a.b": 1}}, 31249), ({"$project": {"a.b": 1, "a": 1}}, 31250),
            ({"$set": {"a": 1, "a.b": 2}}, 40176), ({"$addFields": {"a.b": 2, "a": 1}}, 40176),
            ({"$set": []}, 40272), ({"$addFields": []}, 40272), ({"$set": {"": 1}}, 40352),
            ({"$project": {"a.": 1}}, 40353), ({"$set": {"a.": 1}}, 40353), ({"$unset": "a."}, 40353),
            ({"$project": {"$bad": 1}}, 16410), ({"$unset": []}, 31119), ({"$unset": ""}, 40352),
            ({"$unset": ["", ""]}, 40352), ({"$unset": ["a.", "a."]}, 40353),
            ({"$unset": 1}, 31002), ({"$unset": ["a", 1]}, 31120), ({"$unset": ["a", "a"]}, 31250),
            ({"$unset": ["a", "a.b"]}, 31249), ({"$unset": ["a.b", "a"]}, 31250),
        ]:
            with self.subTest(stage=stage), self.assertRaises(OperationFailure) as caught:
                self.rows([stage])
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(self.client.app.list_collection_names(), [])

    def test_unsupported_projection_features_fail_before_catalog_creation(self):
        for stage in [{"$project": {"value": {"$add": [1, 2]}}}, {"$set": {"arr.0": 1}},
                      {"$set": {"value": "$$ROOT"}}, {"$addFields": {"value": "$$CURRENT"}},
                      {"$unset": "arr.0"}]:
            with self.subTest(stage=stage), self.assertRaises(OperationFailure) as caught:
                self.rows([stage])
            self.assertEqual(caught.exception.code, 115)
            self.assertEqual(self.client.app.list_collection_names(), [])

    def test_remove_suffixes_and_earlier_expressions_validate_before_mode_conflicts(self):
        cases = [({"value": reference}, code) for reference, code in
                 [("$$REMOVE.", 40353), ("$$REMOVE.foo.", 40353), ("$$REMOVE..foo", 15998),
                  ("$$REMOVE.foo..bar", 15998), ("$$REMOVE.$foo", 16410)]]
        cases.extend([({"a.": 1, "excluded": 0}, 40353),
                      ({"value": {"$ifNull": []}, "excluded": 0}, 1257300)])
        for projection, code in cases:
            with self.subTest(projection=projection), self.assertRaises(OperationFailure) as caught:
                self.rows([{"$project": projection}])
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_non_string_output_fields_fail_driver_encoding(self):
        with self.assertRaises(InvalidDocument):
            self.rows([{"$project": {1: 1}}])
        self.assertEqual(self.client.app.list_collection_names(), [])


class AsyncUpstreamAggregationProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_projection_stages_use_real_command_cursor(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                await items.insert_many([{"_id": 1, "name": "Ada", "items": [1, 2], "obsolete": True},
                                         {"_id": 2, "name": "Grace", "obsolete": True}])
                cursor = await items.aggregate([
                    {"$sort": {"_id": 1}},
                    {"$set": {"count": {"$size": {"$ifNull": ["$items", []]}}, "copy": "$name"}},
                    {"$addFields": {"literal": {"$literal": "$name"}, "obsolete": "$$REMOVE"}},
                    {"$unset": "items"},
                    {"$project": {"_id": 0, "name": 1, "copy": 1, "count": 1, "literal": 1}},
                ])
                self.assertIsInstance(cursor, AsyncCommandCursor)
                self.assertEqual(await cursor.to_list(None),
                                 [{"name": "Ada", "copy": "Ada", "count": 2, "literal": "$name"},
                                  {"name": "Grace", "copy": "Grace", "count": 0, "literal": "$name"}])


if __name__ == "__main__":
    unittest.main()
