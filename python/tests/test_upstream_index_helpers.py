"""Public equivalents of locked index helpers, with explicit driver boundaries."""

from copy import deepcopy
import json
import re
import tempfile
from types import SimpleNamespace
import unittest
from uuid import UUID

from bson import Binary, Regex
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from bson.errors import InvalidDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

import briskdb


class UpstreamIndexHelperTests(unittest.TestCase):
    def test_direct_key_forms_names_and_invalid_definitions_leave_no_namespace(self):
        with tempfile.TemporaryDirectory() as root, briskdb.MongoClient(root, shards=2) as client:
            for number, keys in enumerate(["profile.email", [("profile.email", 1)], [["profile.email", 1]]]):
                items = client.app[f"valid_{number}"]
                self.assertEqual(items.create_index(keys), "profile.email_1")
                self.assertEqual(items.index_information()["profile.email_1"], {"key": [("profile.email", 1)]})
            ordered = client.app.ordered
            self.assertEqual(ordered.create_index({"tenant": 1, "email": 1}, unique=True, sparse=True), "tenant_1_email_1")
            self.assertEqual(ordered.index_information()["tenant_1_email_1"],
                             {"key": [("tenant", 1), ("email", 1)], "unique": True, "sparse": True})
            unicode = client.app["café"]
            self.assertEqual(unicode.create_index("value", name="名前"), "名前")
            self.assertEqual(unicode.index_information()["名前"], {"key": [("value", 1)]})
            # The existing wire contract accepts TTL as explicitly reduced
            # behavior, unlike TinyMongo's private parse_index_spec helper.
            ttl = client.app.ttl
            self.assertEqual(ttl.create_index("email", expireAfterSeconds=10), "email_1")
            self.assertEqual(ttl.index_information()["email_1"], {"key": [("email", 1)]})
            reply = client.app.command("createIndexes", "ttl_wire", indexes=[
                {"key": {"email": 1}, "name": "email_1", "expireAfterSeconds": 10}])
            self.assertEqual(reply["briskdbIndexWarnings"][0]["reducedBehavior"], ["ttl: expiration is not performed"])
            reduced = client.app.reduced
            self.assertEqual(reduced.create_index([("email", "hashed")]), "email_hashed")
            self.assertEqual(reduced.index_information()["email_hashed"], {"key": [("email", 1)]})
            self.assertEqual(reduced.create_index([("body", "text")]), "body_text")
            self.assertNotIn("body_text", reduced.index_information())
            self.assertEqual(reduced.create_index([("rank", -1)], background=True), "rank_-1")
            self.assertEqual(reduced.index_information()["rank_-1"], {"key": [("rank", -1)]})
            invalid = client.app.invalid
            # Bare pairs are private TinyMongo helper inputs, not PyMongo's
            # public sequence-of-pairs form. Do not silently reinterpret them.
            for keys in [("email", 1), ["email", 1], 42, [(("not-a-string",), 1)]]:
                with self.assertRaises(TypeError):
                    invalid.create_index(keys)
            for keys in [[], [("email", 1, "extra")]]:
                with self.assertRaises((TypeError, ValueError)):
                    invalid.create_index(keys)
            for field in ["", ".email", "email.", "a..b", "$**", "a.$**"]:
                with self.assertRaises(OperationFailure):
                    invalid.create_index(field)
            with self.assertRaises(InvalidDocument):
                invalid.create_index("a\x00b")
            for options in [{"unique": 1}, {"name": ""}, {"name": "_id"}, {"name": "_id_"},
                            {"collation": {"locale": "en"}},
                            {"wildcardProjection": {"field": 1}}]:
                with self.assertRaises(OperationFailure):
                    invalid.create_index("email", **options)
            with self.assertRaises(OperationFailure) as caught:
                invalid.create_index("email", name=1)
            self.assertEqual(caught.exception.code, 72)
            for operator in ("$and", "$or"):
                for children in ([], "not-an-array"):
                    with self.assertRaises(OperationFailure):
                        invalid.create_index("email", partialFilterExpression={operator: children})
            self.assertNotIn("invalid", client.app.list_collection_names())
            self.assertEqual(list(invalid.list_indexes()), [])

    def test_unique_keys_keep_numeric_uuid_and_regex_bson_identity(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                numeric = client.app.numeric
                numeric.create_index("value", unique=True)
                for identifier, value in enumerate([False, 0, "0", "café"]):
                    numeric.insert_one({"_id": identifier, "value": value})
                for value in (0.0, -0.0):
                    with self.assertRaises(DuplicateKeyError) as caught:
                        numeric.insert_one({"_id": "duplicate", "value": value})
                    self.assertEqual(caught.exception.code, 11000)
                numeric.insert_one({"_id": "tuple", "value": ("tuple",)})
                with self.assertRaises(DuplicateKeyError):
                    numeric.insert_one({"_id": "duplicate", "value": ["tuple"]})
                for value in ({"nested": "object"}, [["nested"]], float("inf"), float("nan")):
                    with self.assertRaises(OperationFailure) as caught:
                        numeric.insert_one({"_id": "unsupported", "value": value})
                    self.assertEqual(caught.exception.code, 115)
                with self.assertRaises(InvalidDocument):
                    numeric.insert_one({"_id": "unsupported", "value": object()})
                regex = client.app.regex
                regex.create_index("value", unique=True)
                regex.insert_one({"_id": "python", "value": re.compile("same", re.IGNORECASE)})
                with self.assertRaises(DuplicateKeyError):
                    regex.insert_one({"_id": "duplicate", "value": Regex("same", "iu")})
                regex.insert_one({"_id": "bson", "value": Regex("same", "iz")})
                with self.assertRaises(DuplicateKeyError):
                    regex.insert_one({"_id": "duplicate", "value": Regex("same", "i")})
                value = UUID("00112233-4455-6677-8899-aabbccddeeff")
                uuids = client.app.uuids.with_options(codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD))
                uuids.create_index("value", unique=True)
                uuids.insert_one({"_id": "standard", "value": value})
                with self.assertRaises(DuplicateKeyError):
                    uuids.insert_one({"_id": "duplicate", "value": Binary(value.bytes, 4)})
                uuids.insert_one({"_id": "legacy", "value": Binary(value.bytes, 3)})
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.numeric.count_documents({}), 5)
                self.assertEqual(reopened.app.numeric.find_one({"_id": "tuple"})["value"], ["tuple"])
                self.assertEqual({row["_id"] for row in reopened.app.regex.find({})}, {"python", "bson"})
                self.assertEqual({row["_id"] for row in reopened.app.uuids.find({})}, {"standard", "legacy"})
                with self.assertRaises(DuplicateKeyError):
                    reopened.app.regex.insert_one({"_id": "later", "value": Regex("same", "iu")})

    def test_nested_absence_and_each_unique_constraint_are_enforced(self):
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                nested = client.app.nested
                nested.create_index("profile.email", unique=True)
                nested.insert_one({"_id": 1})
                for value in ({}, {"email": None}, "not-an-object"):
                    with self.assertRaises(DuplicateKeyError):
                        nested.insert_one({"_id": 2, "profile": value})
                with self.assertRaises(OperationFailure) as caught:
                    nested.insert_one({"_id": 2, "profile": [{"email": "other"}]})
                self.assertEqual(caught.exception.code, 115)
                users = client.app.users
                users.create_index("email", unique=True)
                users.create_index("username", unique=True)
                users.create_index("kind")
                rows = [{"_id": 1, "email": "one", "username": "first", "kind": "same"},
                        {"_id": 2, "email": "two", "username": "second", "kind": "same"}]
                users.insert_many(rows)
                for field, duplicate in (("email", "one"), ("username", "first")):
                    with self.assertRaises(DuplicateKeyError) as caught:
                        users.update_one({"_id": 2}, {"$set": {field: duplicate}})
                    self.assertEqual(caught.exception.code, 11000)
                    self.assertEqual(list(users.find({}).sort("_id")), rows)
                self.assertEqual(nested.count_documents({}), 1)
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(list(reopened.app.users.find({}).sort("_id")), rows)
                self.assertEqual(reopened.app.nested.count_documents({}), 1)

    def test_metadata_protocol_versions_and_nested_literal_partial_filter_survive_reopen(self):
        class Metadata:
            def __init__(self, value):
                self.value = value

            def to_metadata(self):
                return self.value

        legacy = {"v": 1, "name": "email_1", "key": [["email", 1]], "unique": False}
        current = {"v": 2, "name": "profile_login", "key": [["profile.email", 1]],
                   "unique": True, "sparse": False,
                   "partialFilterExpression": {"profile": {"status": "active"}}}
        with tempfile.TemporaryDirectory() as root:
            with briskdb.MongoClient(root, shards=2) as client:
                for number, metadata in enumerate((legacy, current, {**legacy, "v": 2})):
                    source = json.loads(json.dumps(metadata))
                    before = deepcopy(source)
                    items = client.app[f"version_{number}"]
                    self.assertEqual(items.create_indexes([Metadata(source)]), [metadata["name"]])
                    self.assertEqual(source, before)
                selected = client.app.version_1
                selected.insert_one({"_id": 1, "profile": {"status": "active"}})
                with self.assertRaises(DuplicateKeyError):
                    selected.insert_one({"_id": 2, "profile": {"status": "active"}})
                selected.insert_many([{"_id": 3}, {"_id": 4}])
                for metadata in (None, {}, {**legacy, "v": 99}, {**legacy, "v": True}, {**legacy, "extra": True}):
                    with self.assertRaises(TypeError):
                        client.app.invalid.create_indexes([Metadata(metadata)])
                for model in ("email", SimpleNamespace(document=[])):
                    with self.assertRaises(TypeError):
                        client.app.invalid.create_indexes([model])
                self.assertNotIn("invalid", client.app.list_collection_names())
                expected = selected.index_information()
            with briskdb.MongoClient(root) as reopened:
                self.assertEqual(reopened.app.version_0.index_information()["email_1"], {"key": [("email", 1)]})
                self.assertEqual(reopened.app.version_2.index_information()["email_1"], {"key": [("email", 1)]})
                self.assertEqual(reopened.app.version_1.index_information(), expected)
                self.assertEqual(reopened.app.version_1.count_documents({}), 3)


if __name__ == "__main__":
    unittest.main()
