"""Owned public cases from the locked main aggregation source suite."""

from copy import deepcopy
import tempfile
import unittest

from bson.errors import InvalidDocument
from pymongo.command_cursor import CommandCursor
from pymongo.errors import ConfigurationError, OperationFailure

import briskdb


class UpstreamAggregationCoreTests(unittest.TestCase):
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

    def test_empty_pipeline_uses_real_command_cursor_and_isolates_returned_values(self):
        stored = {"_id": 1, "nested": {"value": 2}}
        self.items.insert_one(deepcopy(stored))
        pipeline = []
        with self.items.aggregate(pipeline) as cursor:
            self.assertIsInstance(cursor, CommandCursor)
            self.assertFalse(hasattr(cursor, "clone"))
            self.assertFalse(hasattr(cursor, "rewind"))
            next(cursor)["nested"]["value"] = 99
        self.assertFalse(cursor.alive)
        self.assertEqual(self.rows(pipeline), [stored])
        self.assertEqual(self.items.find_one(), stored)
        self.assertEqual(pipeline, [])

    def test_group_identity_distinguishes_bool_arrays_and_ordered_documents(self):
        keys = [1, 1.0, True, ["a", "b"], ["a", "b"], {"kind": "object"}, {"kind": "object"}]
        self.items.insert_many([{"_id": n, "key": key, "nested": {"amount": n + 1}}
                                for n, key in enumerate(keys, 1)])
        rows = self.rows([{"$group": {"_id": "$key", "total": {"$sum": "$nested.amount"}}}])
        self.assertEqual(len(rows), 4)
        self.assertEqual(sorted(row["total"] for row in rows), [4, 5, 11, 15])
        by_total = {row["total"]: row["_id"] for row in rows}
        self.assertIs(by_total[4], True)
        self.assertEqual(by_total[5], 1)
        self.assertNotIsInstance(by_total[5], bool)
        self.assertEqual(by_total[11], ["a", "b"])
        self.assertEqual(by_total[15], {"kind": "object"})
        self.assertEqual(self.rows([{"$group": {"_id": "$nested.amount.missing", "n": {"$sum": 1}}}]),
                         [{"_id": None, "n": 7}])
        self.items.delete_many({})
        self.items.insert_many([{"_id": 1, "key": {"a": 1, "b": 2}},
                                {"_id": 2, "key": {"b": 2, "a": 1}}])
        rows = self.rows([{"$group": {"_id": "$key", "n": {"$sum": 1}}}])
        self.assertEqual(len(rows), 2)
        self.assertEqual({tuple(row["_id"].items()) for row in rows},
                         {(("a", 1), ("b", 2)), (("b", 2), ("a", 1))})

    def test_dotted_array_references_skip_missing_members(self):
        self.items.insert_many([{"_id": n, "items": value} for n, value in enumerate(
            [[{"score": 1}, {}, {"score": 3}], [], [{}, {}]], 1)])
        self.assertEqual(self.rows([{"$sort": {"_id": 1}}, {"$project": {"_id": 0, "v": "$items.score"}}]),
                         [{"v": [1, 3]}, {"v": []}, {"v": []}])

    def test_non_bson_group_values_reject_in_driver_not_private_python_helpers(self):
        for value in [object(), {"value": object()}, [object()]]:
            with self.assertRaises(InvalidDocument):
                self.items.insert_one({"_id": 1, "key": value})
        for accumulator in ["$max", "$addToSet"]:
            with self.assertRaises(InvalidDocument):
                self.rows([{"$group": {"_id": None, "value": {accumulator: object()}}}])
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_pipeline_and_stage_shapes_reject_before_catalog_creation(self):
        for pipeline in [None, (), {}, iter(())]:
            with self.assertRaises(TypeError):
                self.items.aggregate(pipeline)
        # PyMongo inspects the last stage before sending the command.
        with self.assertRaises(TypeError):
            self.rows([None])
        for stage in [{}, {"$match": {}, "$group": {"_id": None}}, {"match": {}}]:
            with self.subTest(stage=stage), self.assertRaises(OperationFailure):
                self.rows([stage])
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_leading_and_post_group_match_preserve_stage_semantics(self):
        self.items.insert_many([{"_id": 1, "keep": True}, {"_id": 2, "keep": False}])
        self.assertEqual(self.rows([{"$match": {"keep": True}}]), [{"_id": 1, "keep": True}])
        rows = self.rows([{"$group": {"_id": "$keep", "n": {"$sum": 1}}}, {"$match": {"n": 1}}])
        self.assertEqual(sorted(rows, key=lambda row: row["_id"]),
                         [{"_id": False, "n": 1}, {"_id": True, "n": 1}])

    def test_project_evaluates_original_input_and_default_id_modes(self):
        stored = {"_id": 1, "name": "Ada", "count": 99, "zero": 0, "lectures": ["intro", "loops"],
                  "profile": {"email": "ada@example.com", "secret": "hidden"}}
        self.items.insert_one(deepcopy(stored))
        self.assertEqual(self.rows([{"$project": {"_id": 0, "name": 1, "profile.email": 1,
                         "alias": "$name", "count": {"$size": {"$ifNull": ["$lectures", []]}},
                         "old_count": "$count", "choice": {"$ifNull": ["$missing", "$zero", "fallback"]},
                         "omitted": "$missing"}}]),
                         [{"name": "Ada", "profile": {"email": "ada@example.com"}, "alias": "Ada",
                           "count": 2, "old_count": 99, "choice": 0}])
        self.assertEqual(self.rows([{"$project": {"name": 1, "missing": 1, "copy": "$name"}}]),
                         [{"_id": 1, "name": "Ada", "copy": "Ada"}])
        self.assertEqual(self.rows([{"$project": {"profile": {"email": 1}}}]),
                         [{"_id": 1, "profile": {"email": "ada@example.com"}}])
        self.assertEqual(self.rows([{"$project": {"_id": 0}}]),
                         [{key: value for key, value in stored.items() if key != "_id"}])
        self.assertEqual(self.items.find_one(), stored)

    def test_ifnull_preserves_false_zero_arrays_and_is_lazy(self):
        self.items.insert_one({"_id": 1, "zero": 0, "false": False, "empty": [],
                               "nullable": None, "values": [1, 2, 3]})
        self.assertEqual(self.rows([{"$project": {"_id": 0,
            "zero": {"$ifNull": ["$zero", 10]}, "false": {"$ifNull": ["$false", True]},
            "empty": {"$ifNull": ["$empty", [1]]}, "fallback": {"$ifNull": ["$missing", "$nullable", "last"]},
            "missing_fallback": {"$ifNull": ["$missing", "$also_missing"]},
            "lazy": {"$ifNull": ["$zero", {"$size": "$nullable"}]}, "size": {"$size": ["$values"]},
            "empty_size": {"$size": {"$ifNull": ["$missing", []]}},
            "literal_size": {"$size": [[1, 2]]}, "literal_empty_size": {"$size": [[]]}}}]),
            [{"zero": 0, "false": False, "empty": [], "fallback": "last", "lazy": 0,
              "size": 3, "empty_size": 0, "literal_size": 2, "literal_empty_size": 0}])

    def test_size_runtime_errors_keep_code(self):
        for document in [{"_id": 1}, *[{"_id": 1, "value": value} for value in
                                     [None, "three", {"nested": True}, 3]]]:
            self.items.replace_one({"_id": 1}, document, upsert=True)
            with self.assertRaises(OperationFailure) as caught:
                self.rows([{"$project": {"size": {"$size": "$value"}}}])
            self.assertEqual(caught.exception.code, 17124)

    def test_invalid_expressions_and_path_collisions_preflight(self):
        for stage in [
            {"$project": []}, {"$project": {}}, {"$project": {"value": {"$add": [1, 2]}}},
            {"$project": {"value": "$a..b"}}, {"$project": {"value": {"$size": {"$ifNull": ["$a.$bad", []]}}}},
            {"$project": {"value": {"$ifNull": "$missing"}}}, {"$project": {"value": {"$ifNull": ["$missing"]}}},
            {"$project": {"value": {"$size": []}}}, {"$project": {"value": {"$size": [1, 2]}}},
            {"$project": {"value": {"$size": "$items", "extra": 1}}},
            {"$project": {"parent": 1, "parent.child": "$value"}},
            {"$project": {"parent.child": "$value", "parent": 1}},
            {"$group": {"_id": None, "total": {"$sum": {"$add": [1, 2]}}}},
            {"$group": {"_id": "$a.", "total": {"$sum": 1}}},
            {"$group": {"_id": None, "total": {"$sum": "$a\x00b"}}},
        ]:
            with self.subTest(stage=stage), self.assertRaises(OperationFailure):
                self.rows([stage])
            self.assertEqual(self.client.app.list_collection_names(), [])

    def test_unsupported_stages_and_expressions_fail_for_empty_and_populated_input(self):
        stages = [{"$lookup": {}}, {"$group": {"_id": "$key", "value": {"$madeUp": "$value"}}},
                  {"$group": {"_id": "$$ROOT"}},
                  {"$group": {"_id": "$key", "top": {"$max": {"$add": [1, 2]}}}},
                  {"$group": {"_id": None, "top": {"$max": {"$add": [1, 2]}}}},
                  {"$group": {"_id": None, "total": {"$sum": "$$ROOT"}}}]
        for populated in [False, True]:
            if populated:
                self.items.insert_one({"_id": 1, "key": "one"})
            # The native engine supports constant grouping, beyond this TinyMongo source.
            self.assertEqual(self.rows([{"$group": {"_id": "literal"}}]),
                             [{"_id": "literal"}] if populated else [])
            for stage in stages:
                with self.subTest(stage=stage, populated=populated), self.assertRaises(OperationFailure) as caught:
                    self.rows([stage])
                self.assertEqual(caught.exception.code, 115)
            if not populated:
                self.assertEqual(self.client.app.list_collection_names(), [])

    def test_match_rejects_malformed_predicates_but_accepts_literals_and_not(self):
        queries = [{"$expr": {"$eq": ["$value", 1]}}, {"value": {"$madeUp": 1}}, {"$and": []},
                   {"$or": ["bad"]}, {"value": {"$options": "i"}}, {"value": {"$gt": 1, "literal": 2}},
                   {"value": {"$not": {"$madeUp": 1}}}, {"value": {"$in": 1}}, {"value": {"$nin": "one"}},
                   {"value": {"$all": 1}}, {"value": {"$not": {"$in": 1}}},
                   {"value": {"$not": {"$not": {"$madeUp": 1}}}},
                   {"value": {"$not": {"$gt": 1, "literal": 2}}}, {"value": {"$not": {"$options": "i"}}}]
        for query in queries:
            with self.subTest(query=query), self.assertRaises(OperationFailure):
                self.rows([{"$match": query}])
        with self.assertRaises(InvalidDocument):
            self.rows([{"$match": {1: "bad"}}])
        for query in [{"value": {"nested": 1}}, {"value": {"$not": {"$gt": 1}}}]:
            self.assertEqual(self.rows([{"$match": query}]), [])
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_group_argument_validation_preserves_error_precedence_and_codes(self):
        for group, code in [
            ({"_id": None, "value": []}, 40234), ({"_id": None, "value": {}}, 40234),
            ({"_id": None, "value": {"notAnAccumulator": 1}}, 40234),
            ({"_id": None, "bad.name": {"$sum": 1}}, 40235),
            ({"_id": None, "value": {"$sum": 1, "$max": 1}}, 40238),
            ({"_id": None, "$value": {"$sum": 1}}, 40236), ({"_id": None, "$value": []}, 40234),
            ({"_id": None, "bad.name": []}, 40234), ({"_id": None, "value": {"$sum": [1, 2]}}, 40237),
            ({"_id": None, "value": {"$madeUp": [1, 2]}}, 40237),
        ]:
            with self.subTest(group=group), self.assertRaises(OperationFailure) as caught:
                self.rows([{"$group": group}])
            self.assertEqual(caught.exception.code, code)
        for group in [{"_id": None, "value": {1: 1}}, {"_id": None, 1: {"$sum": 1}}]:
            with self.assertRaises(InvalidDocument):
                self.rows([{"$group": group}])
        for stage in [{"$match": []}, {"$group": []}, {"$group": {}}]:
            with self.assertRaises(OperationFailure):
                self.rows([stage])
        self.assertEqual(self.client.app.list_collection_names(), [])

    def test_sessions_and_spill_fail_explicitly_using_driver_options(self):
        self.items.insert_one({"_id": 1, "value": 2})
        with self.assertRaises(OperationFailure) as caught:
            list(self.items.aggregate([], allowDiskUse=True))
        self.assertEqual(caught.exception.code, 72)
        with self.client.start_session() as session:
            with self.assertRaises(ConfigurationError):
                list(self.items.aggregate([], session=session))
        for session in [object(), {}]:
            with self.assertRaises(ValueError):
                list(self.items.aggregate([], session))
        self.assertEqual(list(self.items.aggregate([], None)), [{"_id": 1, "value": 2}])
        # PyMongo's third positional parameter is let; TinyMongo rejected this call.
        self.assertEqual(list(self.items.aggregate([], None, None)), [{"_id": 1, "value": 2}])

    def test_tinymongo_capability_helpers_are_not_part_of_the_pymongo_client_api(self):
        for name in ["capabilities", "supports"]:
            database = getattr(self.client, name)
            self.assertEqual(database.name, name)
            self.assertFalse(callable(database))
        self.items.insert_one({"_id": 1})
        self.assertEqual(self.rows([{"$group": {"_id": None, "n": {"$sum": 1}}}]), [{"_id": None, "n": 1}])

    def test_expression_literals_are_copied_and_paths_validated(self):
        self.items.insert_one({"_id": 1, "reference": {"$id": 7}})
        literal = {"literal": [1, {"value": 2}]}
        pipeline = [{"$project": {"_id": 0, "copy": {"$literal": literal}, "missing": {"missing": "$missing"},
                     "tuple": (1, 2), "size": {"$size": ((1, 2),)}, "reference": "$reference.$id"}}]
        row = self.rows(pipeline)[0]
        self.assertEqual(row, {"copy": literal, "missing": {}, "tuple": [1, 2], "size": 2, "reference": 7})
        row["copy"]["literal"][1]["value"] = 3
        self.assertEqual(literal, {"literal": [1, {"value": 2}]})
        for expression in [{"$ifNull": ["$missing"]}, {"$add": [1, 2]}, "$", "$$ROOT"]:
            with self.assertRaises(OperationFailure):
                self.rows([{"$project": {"value": expression}}])
        self.assertEqual(self.rows([{"$project": {"_id": 0, "value": {"$literal": "field"}}}]), [{"value": "field"}])

    def test_group_is_repeatable_with_explicit_order_and_sum_ignores_non_numbers(self):
        self.items.insert_many([{"_id": 1, "group": "b"}, {"_id": 2, "group": "a"}, {"_id": 3, "group": "b"}])
        pipeline = [{"$sort": {"_id": 1}}, {"$group": {"_id": "$group", "count": {"$sum": 1}}}]
        for _ in range(2):
            self.assertEqual(self.rows(pipeline), [{"_id": "b", "count": 2}, {"_id": "a", "count": 1}])
        self.items.delete_many({})
        self.items.insert_many([{"_id": n, "value": v} for n, v in enumerate([True, "2", None, [2]])] + [{"_id": 4}])
        self.assertEqual(self.rows([{"$group": {"_id": None, "total": {"$sum": "$value"}}}]), [{"_id": None, "total": 0}])


class AsyncUpstreamAggregationCoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_command_cursor_iteration_close_and_context_manager(self):
        with tempfile.TemporaryDirectory() as root:
            async with briskdb.AsyncMongoClient(root, shards=2) as client:
                items = client.app.items
                await items.insert_many([{"_id": 1, "group": "a", "value": 2, "items": [1, 2]},
                                         {"_id": 2, "group": "a", "value": 3, "items": None}])
                pipeline = [{"$group": {"_id": "$group", "total": {"$sum": "$value"}}}]
                cursor = await items.aggregate(pipeline)
                self.assertTrue(cursor.alive)
                self.assertFalse(hasattr(cursor, "rewind"))
                self.assertEqual(await cursor.next(), {"_id": "a", "total": 5})
                await cursor.close()
                self.assertFalse(cursor.alive)
                self.assertEqual([row async for row in await items.aggregate(pipeline)], [{"_id": "a", "total": 5}])
                projected = await items.aggregate([{"$sort": {"_id": 1}}, {"$project": {"_id": 0, "group": 1,
                                "count": {"$size": {"$ifNull": ["$items", []]}}}}])
                self.assertEqual(await projected.to_list(None), [{"group": "a", "count": 2}, {"group": "a", "count": 0}])
                empty = await items.aggregate([{"$match": {"_id": "missing"}}])
                self.assertFalse(empty.alive)
                async with await items.aggregate([]) as managed:
                    self.assertEqual(len(await managed.to_list(None)), 2)
                self.assertFalse(managed.alive)
                with self.assertRaises(OperationFailure) as caught:
                    await items.aggregate([{"$lookup": {}}])
                self.assertEqual(caught.exception.code, 115)


if __name__ == "__main__":
    unittest.main()
