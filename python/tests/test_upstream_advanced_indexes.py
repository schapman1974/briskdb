"""Public advanced-index scenarios from the locked TinyMongo reference.

Source: 53cbf44e98b8caa036163725d195fd29592e1cc0/tests/test_advanced_indexes.py
SHA-256: 076602ea819f52c75ed74a6397660e703dc903eb64520ad51512e8f42e6ea5a9
The candidate is the installed BriskDB wheel's real PyMongo API. Backend-specific
SQLite JSON-expression introspection is not asserted against BriskDB's different
BSON/index-entry storage format; native storage/probe tests cover that boundary.
"""

import asyncio
from copy import deepcopy
import tempfile
import unittest

from pymongo.errors import DuplicateKeyError, OperationFailure

import briskdb


class UpstreamAdvancedIndexTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_advanced_index_metadata_round_trips_through_public_apis_and_restart(self):
        models = [
            {"key": [("tenant", 1), ("email", 1)], "name": "tenant_email", "unique": True},
            {"key": [("alias", 1)], "name": "alias_sparse", "sparse": True},
            {"key": [("sku", 1)], "name": "active_sku", "unique": True,
             "partialFilterExpression": {"active": True}},
        ]
        self.assertEqual(self.items.create_indexes(models), ["tenant_email", "alias_sparse", "active_sku"])
        expected = {"_id_": {"key": [("_id", 1)]},
                    **{model["name"]: {key: value for key, value in model.items() if key != "name"}
                       for model in models}}
        self.assertEqual(self.items.index_information(), expected)
        exposed = self.items.index_information()
        exposed["tenant_email"]["key"].append(("mutated", 1))
        exposed["active_sku"]["partialFilterExpression"]["active"] = False
        self.assertEqual(self.items.index_information(), expected)
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            items = reopened.app.items
            self.assertEqual(items.index_information(), expected)
            items.insert_one({"_id": 1, "tenant": "one", "email": "same", "sku": "sku", "active": True})
            for document in [
                {"_id": 2, "tenant": "one", "email": "same", "sku": "other", "active": False},
                {"_id": 3, "tenant": "two", "email": "other", "sku": "sku", "active": True},
            ]:
                with self.assertRaises(DuplicateKeyError):
                    items.insert_one(document)

    def test_unique_compound_index_enforces_insert_update_and_upsert_atomically(self):
        self.items.create_index([("tenant", 1), ("username", 1)], name="tenant_username", unique=True)
        rows = [{"_id": 1, "tenant": "north", "username": "ada"},
                {"_id": 2, "tenant": "south", "username": "ada"},
                {"_id": 3, "tenant": "north", "username": "grace"}]
        self.items.insert_many(rows)
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 4, "tenant": "north", "username": "ada"})
        with self.assertRaises(DuplicateKeyError):
            self.items.update_one({"_id": 3}, {"$set": {"username": "ada"}})
        with self.assertRaises(DuplicateKeyError):
            self.items.update_one({"_id": 5}, {"$set": {"tenant": "north", "username": "ada"}}, upsert=True)
        self.assertEqual(self.items.find_one({"_id": 3}), rows[2])
        self.assertIsNone(self.items.find_one({"_id": 5}))
        result = self.items.update_one({"_id": 6}, {"$set": {"tenant": "north", "username": "linus"}}, upsert=True)
        self.assertEqual(result.upserted_id, 6)
        self.assertEqual(self.items.find_one({"_id": 6})["username"], "linus")

    def test_compound_unique_keys_preserve_ordered_tuples_and_missing_values(self):
        self.items.create_index([("left", 1), ("right", 1)], unique=True)
        rows = [{"_id": 1, "left": "a", "right": "b"}, {"_id": 2, "left": "b", "right": "a"},
                {"_id": 3, "left": "a"}, {"_id": 4, "left": "b"}, {"_id": 5, "right": "a"}, {"_id": 6}]
        self.items.insert_many(rows)
        for duplicate in [{"_id": 7, "left": "a", "right": "b"}, {"_id": 8, "left": "a", "right": None},
                          {"_id": 9, "left": None, "right": "a"}, {"_id": 10, "left": None, "right": None}]:
            with self.assertRaises(DuplicateKeyError):
                self.items.insert_one(duplicate)
        self.assertEqual(self.items.count_documents({}), 6)

    def test_unique_compound_index_supports_one_flat_multikey_component(self):
        self.items.create_index([("owner.id", 1), ("labels", 1)], name="owner_labels", unique=True)
        self.items.insert_many([
            {"_id": 1, "owner": {"id": "north"}, "labels": ["red", "blue", "red"]},
            {"_id": 2, "owner": {"id": "south"}, "labels": ["blue"]},
            {"_id": 3, "owner": {"id": "north"}, "labels": ["green"]},
        ])
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 4, "owner": {"id": "north"}, "labels": ["yellow", "blue"]})
        self.assertIsNone(self.items.find_one({"_id": 4}))
        self.assertEqual(self.items.count_documents({}), 3)

    def test_compound_parallel_arrays_fail_clearly_and_atomically(self):
        self.items.create_index([("regions", 1), ("labels", 1)], name="region_labels", unique=True)
        with self.assertRaises(OperationFailure) as error:
            self.items.insert_one({"_id": 1, "regions": ["east", "west"], "labels": ["a", "b"]})
        self.assertEqual(error.exception.code, 115)
        self.assertIsNone(self.items.find_one({"_id": 1}))
        original = {"_id": 2, "regions": "east", "labels": "a"}
        self.items.insert_one(original)
        with self.assertRaises(OperationFailure) as error:
            self.items.update_one({"_id": 2}, {"$set": {"regions": ["east", "west"], "labels": ["a", "b"]}})
        self.assertEqual(error.exception.code, 115)
        self.assertEqual(self.items.find_one({"_id": 2}), original)

    def test_unique_sparse_index_skips_missing_but_indexes_explicit_null(self):
        self.items.create_index("email", name="email_sparse", unique=True, sparse=True)
        self.items.insert_many([{"_id": 1}, {"_id": 2}, {"_id": 3, "email": None}])
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 4, "email": None})
        with self.assertRaises(DuplicateKeyError):
            self.items.update_one({"_id": 1}, {"$set": {"email": None}})
        self.assertNotIn("email", self.items.find_one({"_id": 1}))
        self.items.update_one({"_id": 3}, {"$unset": {"email": ""}})
        self.items.insert_one({"_id": 4, "email": None})
        result = self.items.update_one({"_id": 5}, {"$set": {"other": True}}, upsert=True)
        self.assertEqual(result.upserted_id, 5)
        self.assertEqual(self.items.count_documents({"email": {"$exists": False}}), 4)

    def test_sparse_compound_membership_uses_any_present_field_and_complete_tuple(self):
        self.items.create_index([("left", 1), ("right", 1)], name="sparse_pair", sparse=True, unique=True)
        self.items.insert_many([{"_id": 1}, {"_id": 2}])
        self.items.insert_many([{"_id": 3, "left": "a"}, {"_id": 4, "right": "a"},
                                {"_id": 5, "left": "a", "right": "a"}, {"_id": 6, "left": None, "right": None}])
        for duplicate in [{"_id": 7, "left": "a", "right": None}, {"_id": 8, "left": None, "right": "a"},
                          {"_id": 9, "right": None}]:
            with self.assertRaises(DuplicateKeyError):
                self.items.insert_one(duplicate)
        self.assertEqual(self.items.count_documents({}), 6)

    def test_partial_unique_membership_transitions_on_update_and_upsert(self):
        self.items.create_index("email", name="active_email", unique=True, partialFilterExpression={"active": True})
        self.items.insert_many([{"_id": 1, "email": "same", "active": True},
                                {"_id": 2, "email": "same", "active": False}, {"_id": 3, "email": "same"}])
        with self.assertRaises(DuplicateKeyError):
            self.items.update_one({"_id": 2}, {"$set": {"active": True}})
        self.assertFalse(self.items.find_one({"_id": 2})["active"])
        self.items.update_one({"_id": 1}, {"$set": {"active": False}})
        self.items.update_one({"_id": 2}, {"$set": {"active": True}})
        self.assertTrue(self.items.find_one({"_id": 2})["active"])
        with self.assertRaises(DuplicateKeyError):
            self.items.update_one({"_id": 4}, {"$set": {"email": "same", "active": True}}, upsert=True)
        self.assertIsNone(self.items.find_one({"_id": 4}))
        result = self.items.update_one({"_id": 5}, {"$set": {"email": "same", "active": False}}, upsert=True)
        self.assertEqual(result.upserted_id, 5)

    def test_partial_filter_supports_boolean_range_in_type_and_logical_predicates(self):
        expression = {"$and": [{"enabled": True}, {"score": {"$gte": 10, "$lt": 20}},
                                {"tier": {"$in": ["pro", "team"]}}, {"code": {"$exists": True, "$type": "string"}}]}
        original = deepcopy(expression)
        self.items.create_index("slug", name="eligible_slug", unique=True, partialFilterExpression=expression)
        self.assertEqual(expression, original)
        self.items.insert_many([
            {"_id": 1, "slug": "same", "enabled": True, "score": 12, "tier": "pro", "code": "A"},
            {"_id": 2, "slug": "same", "enabled": True, "score": 9, "tier": "pro", "code": "B"},
            {"_id": 3, "slug": "same", "enabled": True, "score": 12, "tier": "free", "code": "C"},
            {"_id": 4, "slug": "same", "enabled": True, "score": 12, "tier": "team", "code": 4},
        ])
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 5, "slug": "same", "enabled": True, "score": 19, "tier": "team", "code": "E"})
        self.assertEqual(self.items.count_documents({}), 4)

    def test_conditional_unique_single_field_multikey_indexes(self):
        for name, options, member, nonmember in [
            ("sparse", {"sparse": True}, {"tags": ["red", "red", "blue"]}, {}),
            ("partial", {"partialFilterExpression": {"active": True}},
             {"tags": ["red", "red", "blue"], "active": True}, {"tags": ["blue"], "active": False}),
        ]:
            with self.subTest(name=name):
                items = self.client.app[name]
                items.create_index("tags", name=name, unique=True, **options)
                items.insert_one({"_id": 1, **member})
                items.insert_one({"_id": 2, **nonmember})
                overlap = {"_id": 3, "tags": ["green", "blue"]}
                disjoint = {"_id": 4, "tags": ["green", "yellow"]}
                if name == "partial":
                    overlap["active"] = disjoint["active"] = True
                with self.assertRaises(DuplicateKeyError):
                    items.insert_one(overlap)
                items.insert_one(disjoint)
                self.assertIsNone(items.find_one({"_id": 3}))
                self.assertEqual({row["_id"] for row in items.find({})}, {1, 2, 4})

    def test_invalid_partial_options_are_eager_and_leave_no_index_metadata(self):
        for options in [
            {"partialFilterExpression": []}, {"partialFilterExpression": {}},
            {"partialFilterExpression": {"active": {"$exists": False}}},
            {"partialFilterExpression": {"tier": {"$in": "pro"}}},
            {"partialFilterExpression": {"age": {"$gte": 18, "unit": "years"}}},
            {"partialFilterExpression": {"email": {"$ne": None}}},
            {"partialFilterExpression": {"$nor": [{"active": True}]}},
            {"sparse": True, "partialFilterExpression": {"active": True}},
            {"sparse": 1}, {"unique": 1}, {"collation": {"locale": "en"}},
        ]:
            with self.subTest(options=options):
                with self.assertRaises(OperationFailure) as error:
                    self.items.create_index("value", **options)
                self.assertIn(error.exception.code, (72, 115))
                self.assertEqual(list(self.items.list_indexes()), [])
                self.assertNotIn("items", self.client.app.list_collection_names())

    def test_duplicate_create_index_fields_are_not_silently_collapsed_by_driver(self):
        for keys in [[("tenant", 1), ("tenant", 1)], [("tenant", 1), ("tenant", -1)],
                     ["tenant", "tenant"], ["tenant", ("tenant", -1)],
                     (("tenant", 1), ("tenant", -1)), [["tenant", 1], ["tenant", -1]]]:
            with self.subTest(keys=keys):
                original = deepcopy(keys)
                with self.assertRaises(OperationFailure) as error:
                    self.items.create_index(keys)
                self.assertEqual(error.exception.code, 115)
                self.assertEqual(keys, original)
                self.assertNotIn("items", self.client.app.list_collection_names())

    def test_direct_create_index_preserves_driver_forms_options_and_directions(self):
        for number, keys in enumerate([
            [("tenant", -1), "email"], (("tenant", -1), ("email", 1)),
            {"tenant": -1, "email": 1}, {"tenant": -1, "email": 1}.items(),
        ]):
            with self.subTest(keys=keys):
                items = self.client.app[f"valid_{number}"]
                self.assertEqual(items.create_index(keys, name="lookup", unique=True,
                                                   background=True, maxTimeMS=5000), "lookup")
                self.assertEqual(items.index_information()["lookup"],
                                 {"key": [("tenant", -1), ("email", 1)], "unique": True})
                items.insert_one({"_id": 1, "tenant": "north", "email": "one"})
                with self.assertRaises(DuplicateKeyError):
                    items.insert_one({"_id": 2, "tenant": "north", "email": "one"})
        self.assertEqual(self.items.create_index("field"), "field_1")
        self.assertEqual(self.items.create_index(["first", "last"]), "first_1_last_1")

    def test_async_direct_index_validation_and_valid_options(self):
        async def run(folder):
            async with briskdb.AsyncMongoClient(folder, shards=2) as client:
                items = client.app.items
                for keys in [[("tenant", 1), ("tenant", -1)], ["tenant", "tenant"],
                             ["tenant", ("tenant", 1)]]:
                    with self.subTest(keys=keys):
                        original = deepcopy(keys)
                        with self.assertRaises(OperationFailure) as error:
                            await items.create_index(keys)
                        self.assertEqual(error.exception.code, 115)
                        self.assertEqual(keys, original)
                        self.assertNotIn("items", await client.app.list_collection_names())
                self.assertEqual(await items.create_index([("tenant", -1), "email"],
                                                          name="lookup", unique=True, maxTimeMS=5000), "lookup")
                self.assertEqual((await items.index_information())["lookup"],
                                 {"key": [("tenant", -1), ("email", 1)], "unique": True})
                await items.insert_one({"_id": 1, "tenant": "north", "email": "one"})
                with self.assertRaises(DuplicateKeyError):
                    await items.insert_one({"_id": 2, "tenant": "north", "email": "one"})
        with tempfile.TemporaryDirectory() as folder:
            asyncio.run(run(folder))


if __name__ == "__main__":
    unittest.main()
