"""Portable model outcomes from the locked TinyMongo index-model source suite.

Source: 53cbf44e98b8caa036163725d195fd29592e1cc0/tests/test_index_models.py
SHA-256: b1d69f29c5ac51a1f7fcd91aa39aaf67425f21bbf130c4091a0f40b15cb6b74a
The public create_indexes API is the observation boundary: BriskDB does not
expose TinyMongo's private IndexBatchPlan classes or object-identity semantics.
"""

from copy import deepcopy
from types import SimpleNamespace
import tempfile
import unittest
import warnings

from pymongo.errors import OperationFailure

import briskdb
from briskdb.mongo import IndexCompatibilityWarning


class UpstreamIndexModelTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_invalid_containers_noniterables_and_late_model_types_are_eager(self):
        for indexes in [None, [object()], [SimpleNamespace(document=[])],
                        [{"key": {"good": 1}}, object()]]:
            with self.subTest(indexes=indexes):
                with self.assertRaises(TypeError):
                    self.items.create_indexes(indexes)
                self.assertNotIn("items", self.client.app.list_collection_names())

    def test_all_source_invalid_key_and_option_models_reject_before_mutation(self):
        models = [
            {}, {"key": 42}, {"key": {}}, {"key": [("email",)]}, {"key": [(42, 1)]},
            {"key": {"email": True}}, {"key": {"email": "2dsphere"}},
            {"key": {"email": 1}, "partialFilterExpression": "active"},
            {"key": {"email": 1}, "sparse": True, "partialFilterExpression": {"active": True}},
            {"key": {"email": 1}, "collation": {"locale": "en"}},
            {"key": {"email": 1}, "unique": 1}, {"key": {"email": 1}, "sparse": 1},
            {"key": {"email": 1}, "background": 1},
            *[{"key": {"email": 1}, "expireAfterSeconds": ttl} for ttl in [True, "60", -1, float("inf")]],
            {"key": {"email": 1}, "name": ""}, {"key": {"email": 1}, "name": "_id_"},
            {"key": {"email": "hashed"}, "unique": True},
            {"key": {"email": "text"}, "unique": True},
            {"key": {"email": 1}, "unique": True, "expireAfterSeconds": 60},
        ]
        for model in models:
            with self.subTest(model=model):
                original = deepcopy(model)
                with self.assertRaises(OperationFailure):
                    self.items.create_indexes([{"key": {"good": 1}}, model])
                self.assertEqual(model, original)
                self.assertNotIn("items", self.client.app.list_collection_names())

    def test_duck_models_and_advanced_metadata_protocol_preserve_effective_definitions(self):
        class IndexSpec:
            def to_metadata(self):
                return {"v": 2, "name": "active_tenant_email", "key": [["tenant", 1], ["email", 1]],
                        "unique": True, "sparse": False, "partialFilterExpression": {"active": True}}

        duck = SimpleNamespace(document={"key": {"profile.email": 1}, "unique": True, "name": "login"})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertEqual(self.items.create_indexes([duck, IndexSpec()]), ["login", "active_tenant_email"])
        self.assertEqual(caught, [])
        self.assertEqual(self.items.index_information()["login"], {"key": [("profile.email", 1)], "unique": True})
        self.assertEqual(self.items.index_information()["active_tenant_email"],
                         {"key": [("tenant", 1), ("email", 1)], "unique": True,
                          "partialFilterExpression": {"active": True}})

    def test_false_performance_flags_and_sparse_membership_do_not_emit_warnings(self):
        for sparse in (False, True):
            with self.subTest(sparse=sparse), warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                items = self.client.app[f"sparse_{sparse}"]
                self.assertEqual(items.create_indexes([{"key": {"email": 1}, "sparse": sparse, "background": False}]),
                                 ["email_1"])
                self.assertEqual(caught, [])
                expected = {"key": [("email", 1)], **({"sparse": True} if sparse else {})}
                self.assertEqual(items.index_information()["email_1"], expected)

    def test_compound_generated_names_and_ordered_batch_warnings(self):
        models = [SimpleNamespace(document={"key": {"email": 1}, "unique": True}),
                  {"key": [("tenant", 1), ("created", -1)]}, {"key": {"token": "hashed"}}]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertEqual(self.items.create_indexes(model for model in models),
                             ["email_1", "tenant_1_created_-1", "token_hashed"])
        self.assertEqual(len(caught), 2)
        self.assertTrue(all(issubclass(item.category, IndexCompatibilityWarning) for item in caught))
        self.assertIn("tenant_1_created_-1", str(caught[0].message))
        self.assertIn("descending", str(caught[0].message))
        self.assertIn("token_hashed", str(caught[1].message))
        self.assertIn("hashed", str(caught[1].message))
        self.assertEqual(self.items.index_information()["tenant_1_created_-1"],
                         {"key": [("tenant", 1), ("created", 1)]})

    def test_combined_reductions_and_fractional_ttl_report_only_effective_metadata(self):
        model = {"name": "account_recent", "key": {"account_id": 1, "created": -1},
                 "sparse": True, "expireAfterSeconds": 0.5, "background": True}
        original = deepcopy(model)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.assertEqual(self.items.create_indexes([model]), ["account_recent"])
        self.assertEqual(model, original)
        self.assertEqual(len(caught), 1)
        self.assertTrue(issubclass(caught[0].category, IndexCompatibilityWarning))
        for feature in ["descending", "TTL expiration", "background"]:
            self.assertIn(feature, str(caught[0].message))
        self.assertEqual(self.items.index_information()["account_recent"],
                         {"key": [("account_id", 1), ("created", 1)], "sparse": True})


if __name__ == "__main__":
    unittest.main()
