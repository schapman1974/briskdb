"""Public durable-index scenarios from the locked TinyMongo reference.

Source: 53cbf44e98b8caa036163725d195fd29592e1cc0/tests/test_durable_indexes.py
SHA-256: 1764fc987b4c99034957ee60100cc4f6c54eb217725178528316f218e742550d
This exercises the installed wheel, not TinyMongo's five storage backends.
list_indexes is a real PyMongo cursor with BSON key documents, missing collections are not created by
reads, and drop_collection retains its driver result. Whole-update rollback is
asserted on one shard; cross-shard partial commits follow the #74/#183 contract.
Private TinyMongo v1 catalog injection/repair is not a BriskDB file format.
"""

from concurrent.futures import ThreadPoolExecutor
import tempfile
import threading
import unittest

from pymongo.errors import DuplicateKeyError, OperationFailure

import briskdb


class UpstreamDurableIndexTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.client = briskdb.MongoClient(self.root.name, shards=2)
        self.addCleanup(self.client.close)
        self.items = self.client.app.items

    def test_unique_index_and_enforcement_survive_client_restart(self):
        self.items.create_index("email", name="login_email", unique=True)
        self.items.insert_many([{"_id": 1, "email": "ada"}, {"_id": 2, "email": "grace"}])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            items = reopened.app.items
            self.assertEqual(items.index_information()["login_email"],
                             {"key": [("email", 1)], "unique": True})
            with self.assertRaises(DuplicateKeyError):
                items.insert_one({"_id": 3, "email": "ada"})
            with self.assertRaises(DuplicateKeyError):
                items.update_one({"_id": 2}, {"$set": {"email": "ada"}})
            self.assertEqual(items.find_one({"_id": 2})["email"], "grace")
            self.assertEqual(items.count_documents({}), 2)

    def test_index_metadata_is_collection_scoped_and_drop_persists(self):
        self.items.create_index("email", name="login_email", unique=True)
        self.client.app.audit.create_index("created_at", name="created_lookup")
        self.assertIsNone(self.items.drop_index("login_email"))
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            items, audit = reopened.app.items, reopened.app.audit
            self.assertEqual(list(items.list_indexes()), [{"name": "_id_", "key": {"_id": 1}}])
            self.assertEqual(audit.index_information()["created_lookup"], {"key": [("created_at", 1)]})
            items.insert_many([{"_id": 1, "email": "same"}, {"_id": 2, "email": "same"}])
            self.assertIsNone(audit.drop_index("created_at"))
            for name, code in [("created_at", 27), ("_id_", 72)]:
                with self.assertRaises(OperationFailure) as error:
                    audit.drop_index(name)
                self.assertEqual(error.exception.code, code)

    def test_catalog_identity_cannot_collide_across_collection_and_index_names(self):
        first, second = self.client.app["a:b"], self.client.app.a
        first.create_index("value", name="c", unique=True)
        second.create_index("value", name="b:c", unique=True)
        self.assertEqual(set(first.index_information()), {"_id_", "c"})
        self.assertEqual(set(second.index_information()), {"_id_", "b:c"})
        first.drop_index("c")
        self.assertIn("b:c", second.index_information())
        second.insert_one({"_id": 1, "value": "one"})
        with self.assertRaises(DuplicateKeyError):
            second.insert_one({"_id": 2, "value": "one"})

    def test_drop_collection_removes_its_durable_index_catalog(self):
        self.items.create_index("email", unique=True)
        self.items.insert_one({"_id": 1, "email": "same"})
        self.client.app.drop_collection("items")
        self.assertNotIn("items", self.client.app.list_collection_names())
        self.items.insert_many([{"_id": 2, "email": "same"}, {"_id": 3, "email": "same"}])
        self.client.close()
        with briskdb.MongoClient(self.root.name) as reopened:
            self.assertEqual(set(reopened.app.items.index_information()), {"_id_"})
            self.assertEqual(reopened.app.items.count_documents({}), 2)

    def test_unique_build_rejects_existing_duplicates_without_adding_metadata(self):
        self.items.insert_many([{"_id": 1, "email": "same"}, {"_id": 2, "email": "same"}])
        with self.assertRaises(DuplicateKeyError):
            self.items.create_index("email", name="login_email", unique=True)
        self.assertEqual(set(self.items.index_information()), {"_id_"})
        self.assertEqual(self.items.count_documents({}), 2)
        self.items.delete_one({"_id": 2})
        self.assertEqual(self.items.create_index("email", name="login_email", unique=True), "login_email")

    def test_unique_update_replace_and_upsert_fail_atomically_within_one_shard(self):
        with tempfile.TemporaryDirectory() as folder:
            # Use public native plan diagnostics only to choose two IDs on the
            # same physical shard. The operations under test are unchanged wire
            # calls; do not assume cross-shard transactions or change routing.
            with briskdb.open(folder, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection("app", "items")
                    def route(identifier):
                        return session.find("app", "items", {"_id": identifier},
                                            plan_diagnostics=True)["plan"]["shards"]
                    shard = route(1)
                    second = next(identifier for identifier in range(2, 64) if route(identifier) == shard)
            with briskdb.MongoClient(folder) as client:
                items = client.app.items
                items.create_index("email", unique=True)
                rows = [{"_id": 1, "email": "ada", "active": True},
                        {"_id": second, "email": "grace", "active": True}]
                items.insert_many(rows)
                operations = [
                    lambda: items.update_one({"_id": second}, {"$set": {"email": "ada"}}),
                    lambda: items.update_many({}, {"$set": {"email": "shared"}}),
                    lambda: items.replace_one({"_id": second}, {"email": "ada", "active": False}),
                    lambda: items.update_one({"_id": 100}, {"$set": {"email": "ada"}}, upsert=True),
                    lambda: items.replace_one({"_id": 101}, {"_id": 101, "email": "ada"}, upsert=True),
                ]
                for number, operation in enumerate(operations):
                    with self.subTest(operation=number):
                        with self.assertRaises(DuplicateKeyError):
                            operation()
                        self.assertEqual(list(items.find({}).sort("_id", 1)), rows)

    def test_unique_array_semantics_include_deduplication_overlap_and_empty_arrays(self):
        self.items.create_index("value", unique=True)
        self.items.insert_many([{"_id": 1, "value": ["alpha", "alpha", "beta"]},
                                {"_id": 2, "value": ["gamma"]}])
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 3, "value": ["beta", "delta"]})
        self.items.insert_one({"_id": 4, "value": []})
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 5, "value": []})
        self.assertEqual(self.items.count_documents({}), 3)

    def test_unique_null_and_missing_values_share_one_key(self):
        self.items.create_index("value", unique=True)
        self.items.insert_one({"_id": 1})
        for identifier, value in [(2, None), (3, [None])]:
            with self.assertRaises(DuplicateKeyError):
                self.items.insert_one({"_id": identifier, "value": value})
        self.assertEqual(self.items.count_documents({}), 1)

    def test_unique_scalar_types_keep_booleans_distinct_from_numbers(self):
        self.items.create_index("value", unique=True)
        self.items.insert_one({"_id": 1, "value": 1})
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 2, "value": 1.0})
        self.items.insert_one({"_id": 3, "value": True})
        self.assertEqual(self.items.count_documents({}), 2)

    def test_unique_index_rejects_unsupported_value_shapes(self):
        self.items.create_index("value", unique=True)
        for value in [{"nested": "object"}, [["nested"]]]:
            with self.subTest(value=value):
                with self.assertRaises(OperationFailure) as error:
                    self.items.insert_one({"_id": 1, "value": value})
                self.assertEqual(error.exception.code, 115)
                self.assertEqual(self.items.count_documents({}), 0)

    def test_nested_unique_field_is_enforced_without_array_traversal(self):
        self.items.create_index("profile.email", name="nested_email", unique=True)
        self.items.insert_one({"_id": 1, "profile": {"email": "ada"}})
        with self.assertRaises(DuplicateKeyError):
            self.items.insert_one({"_id": 2, "profile": {"email": "ada"}})
        with self.assertRaises(OperationFailure) as error:
            self.items.insert_one({"_id": 3, "profile": [{"email": "other"}]})
        self.assertEqual(error.exception.code, 115)
        self.assertEqual(self.items.find_one({"profile.email": "ada"})["_id"], 1)
        self.assertEqual(self.items.count_documents({}), 1)

    def test_index_creation_is_idempotent_and_rejects_conflicts(self):
        for _ in range(2):
            self.assertEqual(self.items.create_index("email", name="login_email", unique=True), "login_email")
        for field, name, unique, code in [("email", "other", True, 85),
                                           ("username", "login_email", True, 86),
                                           ("email", "login_email", False, 86)]:
            with self.subTest(field=field, name=name, unique=unique):
                with self.assertRaises(OperationFailure) as error:
                    self.items.create_index(field, name=name, unique=unique)
                self.assertEqual(error.exception.code, code)
        self.assertEqual(set(self.items.index_information()), {"_id_", "login_email"})

    def test_builtin_id_index_has_fixed_options_and_driver_return_names(self):
        # Direct create_index intentionally keeps PyMongo's requested return
        # name; create_indexes compatibility models resolve the actual _id_.
        self.assertEqual(self.items.create_index("_id"), "_id_1")
        self.assertEqual(self.items.create_index("_id", name="custom_id"), "custom_id")
        for unique in (True, False):
            with self.assertRaises(OperationFailure) as error:
                self.items.create_index("_id", unique=unique)
            self.assertEqual(error.exception.code, 197)
        self.assertEqual(self.items.create_indexes([{"key": {"_id": 1}}]), ["_id_"])
        self.assertEqual(list(self.items.list_indexes()), [{"name": "_id_", "key": {"_id": 1}}])

    def test_concurrent_unique_inserts_allow_exactly_one_writer(self):
        self.items.create_index("email", unique=True)
        self.client.close()
        start = threading.Barrier(4)

        def insert(worker):
            # Four clients, one monitor + one application socket each: stay
            # within the listener's explicit eight-connection admission cap.
            with briskdb.MongoClient(self.root.name, maxPoolSize=1, serverMonitoringMode="poll") as client:
                start.wait(timeout=10)
                try:
                    client.app.items.insert_one({"_id": worker, "email": "winner"})
                    return "inserted"
                except DuplicateKeyError:
                    return "duplicate"

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(insert, range(4)))
        self.assertEqual(results.count("inserted"), 1)
        self.assertEqual(results.count("duplicate"), 3)
        with briskdb.MongoClient(self.root.name) as reader:
            self.assertEqual(reader.app.items.count_documents({}), 1)


if __name__ == "__main__":
    unittest.main()
