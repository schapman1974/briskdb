from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import tempfile
import unittest
import urllib.error
import urllib.request

import briskdb
import psycopg
import pymongo


FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/postgres-tls"


class Identity:
    def __init__(self, directory: str) -> None:
        root = Path(directory)
        self.certificate = root / "server.crt"
        self.key = root / "server.key"
        self.password = root / "password"
        shutil.copyfile(FIXTURES / "server.crt", self.certificate)
        shutil.copyfile(FIXTURES / "server.key", self.key)
        self.password.write_text("python-mongo-tls-test-secret\n", encoding="utf-8")
        os.chmod(self.key, 0o600)
        os.chmod(self.password, 0o600)

    def mongo_args(self) -> dict[str, object]:
        return dict(mongo="127.0.0.1:0", mongo_tls_cert=self.certificate, mongo_tls_key=self.key)

    def client_args(self, address: str) -> dict[str, object]:
        return dict(host="mongodb://localhost:" + address.rsplit(":", 1)[1] + "/?directConnection=true",
                    tls=True, tlsCAFile=str(self.certificate), compressors="zlib",
                    serverSelectionTimeoutMS=3000, socketTimeoutMS=5000, maxPoolSize=2)


class MongoTlsServerTests(unittest.TestCase):
    def test_sync_tls_shares_documents_persists_and_coexists_with_postgres_scram(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection("tls_python", "items")
                    session.insert_one("tls_python", "items", {"_id": 1, "value": "native"})
                    with database.serve(**identity.mongo_args(), postgres="127.0.0.1:0",
                                        postgres_tls_cert=identity.certificate, postgres_tls_key=identity.key,
                                        postgres_password_file=identity.password) as server:
                        address = server.mongo_address or ""
                        with pymongo.MongoClient(**identity.client_args(address)) as client:
                            self.assertEqual(client.tls_python.items.find_one({"_id": 1})["value"], "native")
                            client.tls_python.items.insert_many([{"_id": i, "value": "wire"} for i in range(2, 7)])
                            client.tls_python.items.create_index("value")
                            self.assertEqual(len(list(client.tls_python.items.find({}).batch_size(2))), 6)
                            self.assertEqual(session.count_documents("tls_python", "items")["count"], 6)
                            with psycopg.connect(host="localhost", port=int((server.postgres_address or "").rsplit(":", 1)[1]),
                                                 dbname="default", user="briskdb", password="python-mongo-tls-test-secret",
                                                 sslmode="verify-full", sslrootcert=str(identity.certificate),
                                                 channel_binding="require", autocommit=True, connect_timeout=5) as sql:
                                self.assertEqual(sql.execute("SELECT 17").fetchone(), ("17",))
                        for options in [dict(tls=True), dict(tls=False)]:
                            with pymongo.MongoClient("mongodb://" + address, serverSelectionTimeoutMS=600,
                                                     connectTimeoutMS=300, socketTimeoutMS=600, **options) as rejected:
                                with self.assertRaises(pymongo.errors.ServerSelectionTimeoutError):
                                    rejected.admin.command("ping")
                    self.assertTrue(server.closed)
                with database.serve(**identity.mongo_args()) as restarted:
                    with pymongo.MongoClient(**identity.client_args(restarted.mongo_address or "")) as client:
                        self.assertEqual(client.tls_python.items.count_documents({}), 6)
            with briskdb.open(data, documents=True) as reopened:
                with reopened.serve(**identity.mongo_args()) as server:
                    with pymongo.MongoClient(**identity.client_args(server.mongo_address or "")) as client:
                        self.assertEqual(client.tls_python.items.count_documents({}), 6)

    def test_tls_coexists_with_authenticated_sqlite_remote(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            token = "python-mongo-tls-remote-test-token-0123456"
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.session(routing_key="tls-remote") as session:
                    session.migrate("CREATE TABLE users (id INTEGER PRIMARY KEY)")
                    session.execute("INSERT INTO users VALUES (7)")
                with database.serve(**identity.mongo_args(), admin=None, sqlite_remote_token=token,
                                    sqlite_remote_tables=["users"], sqlite_remote_routing_key="tls-remote") as server:
                    url = "http://" + server.http_address + "/sqlite/v1/catalog"
                    with self.assertRaises(urllib.error.HTTPError) as failure:
                        urllib.request.urlopen(url, timeout=5)
                    self.assertEqual(failure.exception.code, 401)
                    failure.exception.close()
                    request = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
                    with urllib.request.urlopen(request, timeout=5) as response:
                        self.assertEqual([table["name"] for table in json.load(response)["tables"]], ["users"])
                    with pymongo.MongoClient(**identity.client_args(server.mongo_address or "")) as client:
                        client.tls_python.items.insert_one({"_id": 1})
                        self.assertEqual(client.tls_python.items.count_documents({}), 1)

    def test_partial_disabled_remote_and_invalid_tls_inputs_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            with briskdb.open(data, shards=2, documents=True) as database:
                for options in [
                    dict(mongo="127.0.0.1:0", mongo_tls_cert=identity.certificate),
                    dict(mongo="127.0.0.1:0", mongo_tls_key=identity.key),
                    dict(mongo_tls_cert=identity.certificate, mongo_tls_key=identity.key),
                    dict(identity.mongo_args(), mongo="0.0.0.0:0"),
                    dict(identity.mongo_args(), mongo="[::]:0"),
                ]:
                    with self.subTest(options=options), self.assertRaises(briskdb.InvalidArgumentError):
                        database.serve(**options)
                for key in [Path(secrets) / "missing", identity.key]:
                    if key == identity.key:
                        shutil.copyfile(FIXTURES / "rotated.key", key)
                        os.chmod(key, 0o600)
                    reservation = socket.socket()
                    reservation.bind(("127.0.0.1", 0))
                    address = reservation.getsockname()
                    reservation.close()
                    with self.assertRaises(briskdb.OperationalError):
                        database.serve(http=f"127.0.0.1:{address[1]}", **dict(identity.mongo_args(), mongo_tls_key=key))
                    with socket.socket() as reusable:
                        reusable.bind(address)
                identity = Identity(secrets)
                with database.serve(**identity.mongo_args()) as recovered:
                    with pymongo.MongoClient(**identity.client_args(recovered.mongo_address or "")) as client:
                        self.assertEqual(client.admin.command("ping")["ok"], 1)
            with briskdb.open(data, documents=False) as disabled:
                with self.assertRaises(briskdb.OperationalError):
                    disabled.serve(**identity.mongo_args())

    def test_database_close_drains_tls_and_partial_handshake_sockets(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            database = briskdb.open(data, shards=2, documents=True)
            self.addCleanup(database.close)
            server = database.serve(**identity.mongo_args())
            port = int((server.mongo_address or "").rsplit(":", 1)[1])
            context = ssl.create_default_context(cafile=str(identity.certificate))
            with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
                with context.wrap_socket(raw, server_hostname="localhost") as established:
                    with socket.create_connection(("127.0.0.1", port), timeout=3) as pending:
                        pending.sendall(bytes([22, 3, 3]))
                        established.sendall(bytes([1, 2]))
                        database.close()
                        self.assertTrue(server.closed)
                        for peer in [established, pending]:
                            try:
                                self.assertEqual(peer.recv(1), b"")
                            except (ConnectionResetError, ssl.SSLEOFError):
                                # Cancelling an incomplete frame/handshake may
                                # reset TCP; a socket timeout must still fail.
                                pass
            self.assertTrue(server.close()["already_closed"])

    def test_sync_and_async_tls_options_are_optional_keyword_only(self) -> None:
        for cls in [briskdb.Database, briskdb.AsyncDatabase]:
            signature = inspect.signature(cls.serve)
            for name in ["mongo_tls_cert", "mongo_tls_key"]:
                self.assertIsNone(signature.parameters[name].default)
                self.assertEqual(signature.parameters[name].kind, inspect.Parameter.KEYWORD_ONLY)


class AsyncMongoTlsServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_verified_tls_shares_native_data_and_closes_with_database(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            database = await briskdb.open_async(data, shards=2, documents=True)
            try:
                for options in [dict(mongo="127.0.0.1:0", mongo_tls_cert=identity.certificate),
                                dict(mongo_tls_cert=identity.certificate, mongo_tls_key=identity.key)]:
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        await database.serve(**options)
                async with await database.session() as session:
                    await session.create_collection("async_tls", "items")
                    async with await database.serve(**identity.mongo_args()) as server:
                        async with pymongo.AsyncMongoClient(**identity.client_args(server.mongo_address or "")) as client:
                            await client.async_tls.items.insert_many([{"_id": i} for i in range(6)])
                            self.assertEqual(len(await client.async_tls.items.find({}).batch_size(2).to_list()), 6)
                            self.assertEqual((await session.count_documents("async_tls", "items"))["count"], 6)
                    self.assertTrue(server.closed)
                server = await database.serve(**identity.mongo_args())
                await database.close()
                self.assertTrue(server.closed)
                self.assertTrue((await server.close())["already_closed"])
            finally:
                await database.close()
