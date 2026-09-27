"""Native table semantics, distinct from foreign storage backend internals."""

import tempfile
import unittest

from bson import Decimal128
from pymongo.errors import DuplicateKeyError, OperationFailure

import briskdb


class UpstreamTableBackendTests(unittest.TestCase):
    def test_null_negation_matrix_is_stable_before_index_and_after_reopen(self):
        examples = [
            ({}, (False, False, False, True, True)),
            ({"value": None}, (False, False, False, True, True)),
            ({"value": 0}, (True, True, False, False, False)),
            ({"value": 1}, (True, True, True, True, True)),
            ({"value": []}, (True, True, True, True, True)),
            ({"value": [None]}, (False, False, False, True, True)),
            ({"value": [0]}, (True, True, False, False, False)),
            ({"value": [None, 0]}, (False, False, False, False, False)),
        ]
        queries = [{"value": {"$ne": None}}, {"value": {"$nin": [None]}},
                   {"value": {"$nin": [None, 0]}}, {"value": {"$ne": 0}},
                   {"value": {"$nin": [0]}}]

        def check(items):
            for column, query in enumerate(queries):
                expected = [i for i, (_, matches) in enumerate(examples) if matches[column]]
                with self.subTest(query=query):
                    self.assertEqual([row["_id"] for row in items.find(query).sort("_id")], expected)
                    self.assertEqual(items.count_documents(query), len(expected))

        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                client.app.items.insert_many([dict(row, _id=i) for i, (row, _) in enumerate(examples)])
                check(client.app.items)
                client.app.items.create_index("value")
                check(client.app.items)
                client.app.nested.insert_one({"_id": "missing", "nested": {}})
                for query in ({"nested.value": {"$ne": None}}, {"nested.value": {"$nin": [None, "blocked"]}}):
                    self.assertEqual(list(client.app.nested.find(query)), [])
            with briskdb.MongoClient(root) as reopened:
                check(reopened.app.items)

    def test_array_equality_and_nor_results_keep_driver_error_boundaries(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            items = client.app.items
            row = {"_id": 1, "items": [1, 2], "nested": [["a", "b"], ["c"]], "name": "Ada", "value": 5}
            items.insert_one(row)
            for query in ({"items": [1, 2]}, {"items": 2}, {"nested": ["a", "b"]}):
                self.assertEqual(list(items.find(query)), [row])
            self.assertEqual(list(items.find({"items": [2, 1]})), [])
            self.assertEqual(list(items.find({"value": {"$gt": "not-a-number"}})), [])
            for query in ({"name": {"$regex": "["}}, {"name": {"$options": "i"}},
                          {"name": {"$unknown": "Ada"}}):
                with self.assertRaises(OperationFailure):
                    list(items.find(query))
                self.assertEqual(items.find_one({}), row)
            with self.assertRaises(TypeError):
                items.find("bad")
            client.app.status.insert_many([
                {"_id": 1, "status": "draft", "score": 2},
                {"_id": 2, "status": "published", "score": 5},
                {"_id": 3, "status": "archived", "score": 9},
            ])
            query = {"$nor": [{"status": "draft"}, {"score": {"$gt": 8}}]}
            self.assertEqual([row["_id"] for row in client.app.status.find(query)], [2])

    def test_scalar_array_union_and_broader_partial_queries_survive_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                people = client.app.people
                people.insert_many([
                    {"_id": 1, "email": "ada@example.com"},
                    {"_id": 2, "email": ["ada@example.com", "other@example.com"]},
                    {"_id": 3, "email": "grace@example.com"},
                ])
                people.create_index("email", name="email_lookup")
                items = client.app.items
                items.insert_many([
                    {"_id": 1, "category": "news", "active": True, "score": 2},
                    {"_id": 2, "category": "news", "active": False, "score": 4},
                ])
                items.create_index("category", name="active_category", partialFilterExpression={"active": True})
                with briskdb.MongoClient(root) as peer:
                    for active in (client, peer):
                        self.assertEqual([row["_id"] for row in active.app.people.find({"email": "ada@example.com"}).sort("_id")], [1, 2])
                        self.assertEqual([row["_id"] for row in active.app.items.find({"category": "news", "score": {"$mod": [2, 0]}}).sort("_id")], [1, 2])
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual([row["_id"] for row in reopened.app.people.find({"email": "ada@example.com"}).sort("_id")], [1, 2])
                self.assertEqual([row["_id"] for row in reopened.app.items.find({"category": "news", "score": {"$mod": [2, 0]}}).sort("_id")], [1, 2])

    def test_unique_numeric_identity_retains_decimal_precision_and_bson_bounds(self):
        with tempfile.TemporaryDirectory() as root:
            values = [1e23, Decimal128("1E+23"), True, 1, 0, 5e-324,
                      2**53 + 1, float(2**53 + 1), 2**60]
            with briskdb.MongoClient(root, shards=2) as client:
                items = client.app.items
                items.create_index("value", unique=True)
                items.insert_many([{"_id": i, "value": value} for i, value in enumerate(values)])
                for duplicate in (1.0, Decimal128("1.00"), -0.0, float(2**60)):
                    with self.assertRaises(DuplicateKeyError) as caught:
                        items.insert_one({"_id": "duplicate", "value": duplicate})
                    self.assertEqual(caught.exception.code, 11000)
                for too_large in (2**63, 2**63 + 1, 10**23):
                    with self.assertRaises(OverflowError):
                        items.insert_one({"_id": "unencodable", "value": too_large})
                self.assertEqual(items.count_documents({}), len(values))
            with briskdb.MongoClient(root) as reopened:
                for i, value in enumerate(values):
                    self.assertEqual(reopened.app.items.find_one({"value": value})["_id"], i)
                self.assertEqual(reopened.app.items.count_documents({}), len(values))


if __name__ == "__main__":
    unittest.main()
