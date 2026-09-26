"""Real-driver option validation must precede local storage acquisition."""

from datetime import timezone
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest import mock

import pymongo
from pymongo.errors import ConfigurationError, InvalidURI

import briskdb
from briskdb import _mongo_runtime, mongo


INVALID_OPTIONS = [
    ({"document_class": list}, TypeError),
    ({"tz_aware": 1}, TypeError),
    ({"tz_aware": "yes"}, ValueError),
    ({"tz_aware": "TRUE"}, ValueError),
    ({"tzinfo": "UTC"}, TypeError),
    ({"tzinfo": timezone.utc}, ValueError),
    ({"tinymongo_fodler": "misspelled"}, ConfigurationError),
    ({"maxPoolSize": -1}, ValueError),
    ({"minPoolSize": 3, "maxPoolSize": 2}, ValueError),
    ({"maxConnecting": 0}, ValueError),
    ({"connect": 1}, TypeError),
]


class UpstreamClientConfigurationTests(unittest.TestCase):
    def test_invalid_sync_and_async_options_do_not_create_storage(self):
        with tempfile.TemporaryDirectory() as parent:
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                for number, (options, error) in enumerate(INVALID_OPTIONS):
                    with self.subTest(client=client_class.__name__, options=options):
                        folder = Path(parent) / f"{client_class.__name__}_{number}"
                        with self.assertRaises(error):
                            client_class(folder=folder, shards=2, **options)
                        self.assertFalse(folder.exists(), "invalid options created a local database")
                        self.assertEqual(_mongo_runtime._stores, {})

    def test_invalid_uri_is_rejected_without_acquiring_an_engine(self):
        with tempfile.TemporaryDirectory() as parent:
            folder = Path(parent) / "absent"
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                with mock.patch.object(mongo, "acquire", side_effect=AssertionError("engine acquired")):
                    with self.assertRaises(InvalidURI):
                        client_class("mongodb://ignored.invalid/app/bad", folder=folder)
            self.assertFalse(folder.exists())

    def test_invalid_options_do_not_open_or_recover_existing_storage(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(folder, shards=2) as writer:
                writer.app.items.insert_one({"_id": 1})
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                with mock.patch.object(mongo, "acquire", side_effect=AssertionError("engine acquired")):
                    with self.assertRaises(TypeError):
                        client_class(folder=folder, document_class=list)
            with briskdb.MongoClient(folder) as reader:
                self.assertEqual(reader.app.items.find_one({}), {"_id": 1})

    def test_falsey_document_classes_and_timezone_forms_keep_driver_semantics(self):
        with tempfile.TemporaryDirectory() as folder:
            for document_class in (None, False, 0, "", [], {}):
                with briskdb.MongoClient(folder, shards=2, document_class=document_class) as client:
                    self.assertIs(client.codec_options.document_class, dict)
            for tz_aware in (None, False, True, "false", "true"):
                options = {"tz_aware": tz_aware}
                if tz_aware in (True, "true"):
                    options["tzInfo"] = timezone.utc
                with briskdb.MongoClient(folder, **options) as client:
                    self.assertEqual(client.codec_options.tz_aware, tz_aware in (True, "true"))

    def test_valid_options_bind_only_the_actual_local_listener(self):
        with tempfile.TemporaryDirectory() as folder:
            with briskdb.MongoClient(
                "mongodb+srv://private:secret@ignored.invalid/app?tls=true&maxPoolSize=9",
                folder=folder, shards=2, connect=False, maxPoolSize=1,
                appName="configuration-regression", retryWrites=False,
                serverSelectionTimeoutMS=50, connectTimeoutMS=50,
                event_listeners=[], server_selector=lambda servers: servers,
                server_api=None,
            ) as client:
                address = client._briskdb_store.listener.address
                host, port = address.rsplit(":", 1)
                endpoint = (host, int(port))
                self.assertEqual(client._seeds, {endpoint})
                self.assertEqual(client._resolve_srv_info["seeds"], {endpoint})
                self.assertEqual(client._topology_settings.seeds, {endpoint})
                self.assertIn(address, client._init_kwargs["host"])
                self.assertEqual(client._host, [client._init_kwargs["host"]])
                self.assertEqual(client.options.pool_options.max_pool_size, 1)
                self.assertEqual(client.options.pool_options.appname, "configuration-regression")
                self.assertFalse(client._opened)
                self.assertNotIn("secret", repr(client))
                client.get_default_database().items.insert_one({"_id": 1})
                self.assertEqual(client.address, endpoint)

    def test_default_connect_never_dials_the_placeholder_or_original_host(self):
        calls = []
        resolver = socket.getaddrinfo

        def resolve(host, port, *args, **kwargs):
            calls.append((host, port))
            self.assertEqual(host, "127.0.0.1")
            self.assertNotEqual(int(port), 1)
            return resolver(host, port, *args, **kwargs)

        with tempfile.TemporaryDirectory() as folder:
            with mock.patch("socket.getaddrinfo", side_effect=resolve):
                with briskdb.MongoClient("mongodb://ignored.invalid/app", folder=folder, shards=2) as client:
                    self.assertEqual(client.app.items.count_documents({}), 0)
                    actual_port = int(client._briskdb_store.listener.address.rsplit(":", 1)[1])
        self.assertTrue(calls)
        self.assertTrue(all(int(port) == actual_port for _, port in calls))

    def test_folder_alias_persists_but_duplicate_aliases_and_foreign_backends_are_explicit(self):
        with tempfile.TemporaryDirectory() as parent:
            folder = Path(parent) / "configured"
            with briskdb.MongoClient(tinymongo_folder=folder, shards=2) as client:
                client.app.items.insert_one({"_id": 1})
            self.assertTrue((folder / "manifest.sqlite").is_file())
            with briskdb.MongoClient(foldername=folder) as reader:
                self.assertEqual(reader.app.items.find_one({}), {"_id": 1})
            for client_class in (briskdb.MongoClient, briskdb.AsyncMongoClient):
                for other in (folder, Path(parent) / "different"):
                    with self.assertRaisesRegex(TypeError, "folder only once"):
                        client_class(folder=folder, tinymongo_folder=other)
                with self.assertRaisesRegex(ValueError, "SQLite"):
                    client_class(folder=folder, backend="duckdb")
                with self.assertRaises(ConfigurationError):
                    client_class(folder=folder, threads=2, duckdb_config={"memory_limit": "1GB"})

    def test_invalid_patch_client_does_not_release_scope_or_change_constructors(self):
        with briskdb.patch(shards=2) as Client:
            with self.assertRaises(TypeError):
                Client(document_class=list)
            self.assertIs(pymongo.MongoClient, Client)
            with Client() as client:
                client.app.items.insert_one({"_id": 1})
                self.assertEqual(client.app.items.count_documents({}), 1)
        self.assertEqual(_mongo_runtime._stores, {})

    def test_driver_setup_failure_after_binding_releases_owned_store(self):
        with tempfile.TemporaryDirectory() as folder:
            for client_class, driver_class in ((briskdb.MongoClient, mongo._Client),
                                               (briskdb.AsyncMongoClient, mongo._AsyncClient)):
                with mock.patch.object(driver_class, "_init_based_on_options",
                                       side_effect=RuntimeError("driver setup failed")):
                    with self.assertRaisesRegex(RuntimeError, "driver setup failed"):
                        client_class(folder=folder, shards=2)
                self.assertEqual(_mongo_runtime._stores, {})
            with briskdb.MongoClient(folder) as reader:
                self.assertEqual(reader.app.items.count_documents({}), 0)


class AsyncUpstreamClientConfigurationTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_valid_configuration_rebinds_and_shares_store(self):
        with tempfile.TemporaryDirectory() as folder:
            async with briskdb.AsyncMongoClient(
                "mongodb://ignored.invalid/app", folder=folder, shards=2,
                document_class=dict, tz_aware="true", tzInfo=timezone.utc, connect=False,
            ) as client:
                address = client._briskdb_store.listener.address
                host, port = address.rsplit(":", 1)
                self.assertEqual(client._seeds, {(host, int(port))})
                self.assertIn(address, client._init_kwargs["host"])
                await client.get_default_database().items.insert_one({"_id": 1})
                with briskdb.MongoClient(folder=folder) as peer:
                    self.assertIs(peer._briskdb_store, client._briskdb_store)
                    self.assertEqual(peer.app.items.find_one({}), {"_id": 1})
                self.assertEqual(await client.app.items.count_documents({}), 1)
        self.assertEqual(_mongo_runtime._stores, {})

    async def test_async_default_folder_alias_and_codec_option_forms(self):
        with tempfile.TemporaryDirectory() as parent:
            default = Path(parent) / "default"
            with mock.patch.dict(os.environ, {"BRISKDB_HOME": str(default)}):
                client = briskdb.AsyncMongoClient()
                await client.close()
            self.assertTrue((default / "manifest.sqlite").is_file())
            configured = Path(parent) / "configured"
            async with briskdb.AsyncMongoClient(tinymongo_folder=configured, shards=2) as client:
                await client.app.items.insert_one({"_id": 1})
            self.assertTrue((configured / "manifest.sqlite").is_file())
            for document_class in (None, False, 0, "", [], {}):
                async with briskdb.AsyncMongoClient(configured, document_class=document_class) as client:
                    self.assertIs(client.codec_options.document_class, dict)
            for tz_aware in (None, False, True, "false", "true"):
                options = {"tz_aware": tz_aware}
                if tz_aware in (True, "true"):
                    options["tzInfo"] = timezone.utc
                async with briskdb.AsyncMongoClient(configured, **options) as client:
                    self.assertEqual(client.codec_options.tz_aware, tz_aware in (True, "true"))
                    self.assertEqual(await client.app.items.count_documents({}), 1)


if __name__ == "__main__":
    unittest.main()
