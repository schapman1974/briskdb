"""Local PyMongo boundaries for metadata-only repeated index DDL (#547)."""
import tempfile
import unittest

from pymongo import IndexModel
from pymongo.errors import DuplicateKeyError, OperationFailure

from briskdb import mongo


class IndexDdlScopeTests(unittest.TestCase):
    def test_repeated_and_mixed_batches_preserve_definitions_and_unique_enforcement(self):
        with tempfile.TemporaryDirectory() as folder:
            for reopen in (False, True):
                with mongo.MongoClient(folder=folder, shards=2) as client:
                    collection = client.app.items
                    unique = IndexModel("email", unique=True)
                    sparse = IndexModel("optional", sparse=True)
                    partial = IndexModel("value", partialFilterExpression={"active": True})
                    models = [unique, sparse, partial]
                    if not reopen:
                        collection.insert_many([
                            {"_id": 1, "email": "one", "value": 10, "active": True},
                            {"_id": 2, "email": "two", "value": 20, "active": False},
                        ])
                        collection.create_indexes(models)
                    before = list(collection.find({}).sort("_id"))
                    definitions = collection.index_information()
                    self.assertEqual(collection.create_indexes(models),
                                     ["email_1", "optional_1", "value_1"])
                    self.assertEqual(collection.index_information(), definitions)
                    for conflict in [IndexModel("email"), IndexModel("optional"),
                                     IndexModel("value", partialFilterExpression={"active": False})]:
                        with self.assertRaises(OperationFailure) as error:
                            collection.create_indexes([conflict])
                        self.assertEqual(error.exception.code, 86)
                    self.assertEqual(collection.create_indexes([unique, IndexModel("new"), sparse]),
                                     ["email_1", "new_1", "optional_1"])
                    self.assertEqual(len(collection.index_information()), 5)
                    with self.assertRaises(DuplicateKeyError):
                        collection.insert_one({"_id": 3, "email": "one"})
                    self.assertEqual(list(collection.find({}).sort("_id")), before)


class AsyncIndexDdlScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_collection_in_another_database_and_repeated_batch(self):
        with tempfile.TemporaryDirectory() as folder:
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as client:
                big = client.other_db.big
                await big.create_indexes([IndexModel(f"k{j}") for j in range(4)])
                documents = [{"_id": i, **{f"k{j}": i for j in range(4)}, "body": "x" * 8192}
                             for i in range(64)]
                await big.insert_many(documents)
                empty = client.probe_db.empty
                models = [IndexModel(f"f{j}") for j in range(5)]
                expected = [f"f{j}_1" for j in range(5)]
                self.assertEqual(await empty.create_indexes(models), expected)
                definitions = await empty.index_information()
                for _ in range(3):
                    self.assertEqual(await empty.create_indexes(models), expected)
                    self.assertEqual(await empty.index_information(), definitions)
                self.assertEqual(await empty.count_documents({}), 0)
                self.assertEqual(await big.find({}).sort("_id").to_list(), documents)
            async with mongo.AsyncMongoClient(folder=folder, shards=2) as client:
                self.assertEqual(await client.probe_db.empty.create_indexes(models), expected)
                self.assertEqual(await client.other_db.big.find({}).sort("_id").to_list(), documents)


if __name__ == "__main__":
    unittest.main()
