from __future__ import annotations

import asyncio
import inspect
import json
import os
from pathlib import Path
import shutil
import socket
import tempfile
import unittest
import urllib.error
import urllib.request

import briskdb
import psycopg
import pymongo


def split_address(address: str) -> tuple[str, int]:
    host, port = address.rsplit(":", 1)
    return host.strip("[]"), int(port)


def http_json(address: str, path: str, body: dict[str, object] | None = None) -> dict[str, object]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"http://{address}{path}",
        data=data,
        headers={"content-type": "application/json"} if data is not None else {},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


class AttachedServerTests(unittest.TestCase):
    def test_http_and_real_psycopg_round_trips_share_the_open_database(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            with database.serve(postgres="127.0.0.1:0") as server:
                self.assertNotEqual(split_address(server.http_address)[1], 0)
                self.assertEqual(server.data_address, server.http_address)
                self.assertIsNotNone(server.admin_address)
                self.assertNotEqual(server.admin_address, server.http_address)
                self.assertTrue(repr(server).startswith("Server(http_address="))
                self.assertIn("admin_address=", repr(server))
                self.assertIsNotNone(server.postgres_address)
                self.assertIsNone(server.mongo_address)
                health = http_json(server.admin_address or "", "/health")
                self.assertEqual(health["status"], "ok")
                self.assertEqual(health["shards"], 2)
                self.assertEqual(
                    health["global_indexes"],
                    {
                        "state": "healthy",
                        "total": 0,
                        "healthy": 0,
                        "degraded": 0,
                        "unavailable": 0,
                        "async_lag": 0,
                        "retained_outbox_events": 0,
                        "retained_outbox_bytes": 0,
                        "backpressured_outbox_shards": 0,
                    },
                )
                query = http_json(
                    server.http_address,
                    "/v1/query",
                    {"shard_key": "python-http", "sql": "SELECT 7 AS value"},
                )
                self.assertEqual(query["rows"], [[7]])
                with self.assertRaises(urllib.error.HTTPError) as missing_admin:
                    http_json(server.data_address, "/health")
                self.assertEqual(missing_admin.exception.code, 404)
                with self.assertRaises(urllib.error.HTTPError) as missing_data:
                    http_json(
                        server.admin_address or "",
                        "/v1/query",
                        {"shard_key": "python-http", "sql": "SELECT 9"},
                    )
                self.assertEqual(missing_data.exception.code, 404)

                host, port = split_address(server.postgres_address or "")
                with psycopg.connect(
                    host=host,
                    port=port,
                    dbname="default",
                    user="python_client",
                    autocommit=True,
                    cursor_factory=psycopg.ClientCursor,
                    connect_timeout=5,
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 42 AS answer")
                        self.assertEqual(cursor.fetchall(), [("42",)])

            self.assertTrue(server.closed)
            self.assertTrue(server.close()["already_closed"])
            with database.session(routing_key="after-server") as session:
                self.assertEqual(session.query("SELECT 1")["rows"], [(1,)])

            restarted = database.serve()
            self.assertEqual(
                http_json(restarted.admin_address or "", "/health")["status"], "ok"
            )
            database.close()
            self.assertTrue(restarted.closed)
            self.assertTrue(restarted.close()["already_closed"])

    def test_mongo_and_native_documents_share_data_and_survive_listener_and_database_restart(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.open(data_dir, shards=2, documents=True) as database:
                with database.session() as session:
                    session.create_collection("python_mongo", "items")
                    session.insert_one("python_mongo", "items", {"_id": 1, "value": "native"})
                    with database.serve(mongo="127.0.0.1:0", postgres="127.0.0.1:0") as server:
                        self.assertIn("mongo_address=", repr(server))
                        self.assertNotEqual(server.mongo_address, server.postgres_address)
                        self.assertEqual(http_json(server.admin_address or "", "/health")["status"], "ok")
                        with pymongo.MongoClient(
                            "mongodb://" + (server.mongo_address or ""),
                            compressors="zlib", serverSelectionTimeoutMS=3000,
                        ) as client:
                            collection = client.python_mongo.items
                            self.assertEqual(collection.find_one({"_id": 1}), {"_id": 1, "value": "native"})
                            collection.insert_one({"_id": 2, "value": "wire"})
                            self.assertEqual(session.find("python_mongo", "items", {"_id": 2})["documents"],
                                             [{"_id": 2, "value": "wire"}])
                            collection.create_index("value", unique=True)
                            self.assertEqual(collection.count_documents({"_id": {"$in": [1, 2]}}), 2)
                            with psycopg.connect(
                                host="127.0.0.1", port=split_address(server.postgres_address or "")[1],
                                dbname="default", user="python", autocommit=True,
                                cursor_factory=psycopg.ClientCursor, connect_timeout=5,
                            ) as connection:
                                self.assertEqual(connection.execute("SELECT 42").fetchall(), [("42",)])
                    self.assertTrue(server.closed)
                    self.assertEqual(session.count_documents("python_mongo", "items")["count"], 2)
                # A new listener attaches to the same still-running engine.
                with database.serve(admin=None, mongo="127.0.0.1:0") as restarted:
                    self.assertIsNone(restarted.admin_address)
                    with pymongo.MongoClient("mongodb://" + (restarted.mongo_address or ""),
                                             serverSelectionTimeoutMS=3000) as client:
                        self.assertEqual(client.python_mongo.items.update_one(
                            {"_id": 2}, {"$set": {"value": "persisted"}}).modified_count, 1)
            with briskdb.open(data_dir, documents=True) as database:
                with database.serve(mongo="127.0.0.1:0") as server:
                    with pymongo.MongoClient("mongodb://" + (server.mongo_address or ""),
                                             serverSelectionTimeoutMS=3000) as client:
                        self.assertEqual(client.python_mongo.items.find_one({"_id": 2}),
                                         {"_id": 2, "value": "persisted"})
                        self.assertTrue(client.python_mongo.items.index_information()["value_1"]["unique"])

    def test_mongo_preflight_and_bind_failures_preserve_database_and_release_sockets(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.open(data_dir, shards=2) as database:
                with self.assertRaisesRegex(briskdb.OperationalError, "enabled document support"):
                    database.serve(mongo="127.0.0.1:0")
                with database.serve() as server:
                    self.assertIsNone(server.mongo_address)
            with briskdb.open(data_dir, documents=True) as database:
                for address in ["0.0.0.0:0", "[::]:0", "192.0.2.1:27017"]:
                    with self.subTest(address=address), self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                        database.serve(mongo=address)
                with self.assertRaisesRegex(briskdb.InvalidArgumentError, "IP socket address"):
                    database.serve(mongo="localhost:27017")
                for listener in ["http", "admin", "postgres"]:
                    with self.subTest(listener=listener), self.assertRaisesRegex(briskdb.OperationalError, "distinct"):
                        database.serve(mongo="127.0.0.1:8765", **{listener: "127.0.0.1:8765"})
                with socket.socket() as occupied, socket.socket() as reservation:
                    occupied.bind(("127.0.0.1", 0))
                    occupied.listen()
                    reservation.bind(("127.0.0.1", 0))
                    http_port = reservation.getsockname()[1]
                    reservation.close()
                    with self.assertRaisesRegex(briskdb.OperationalError, "failed to bind Mongo"):
                        database.serve(http=f"127.0.0.1:{http_port}", admin=None,
                                       mongo=f"127.0.0.1:{occupied.getsockname()[1]}")
                    with socket.socket() as rebound:
                        rebound.bind(("127.0.0.1", http_port))
                with database.session(routing_key="after-mongo-failure") as session:
                    self.assertEqual(session.query("SELECT 1")["rows"], [(1,)])

    def test_database_close_drains_all_mongo_handles_and_partial_frames(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2, documents=True)
            try:
                servers = [database.serve(mongo="127.0.0.1:0") for _ in range(2)]
                with socket.create_connection(split_address(servers[0].mongo_address or ""), timeout=3) as partial:
                    partial.sendall(b"\x01\x02")
                    database.close()
                    try:
                        self.assertEqual(partial.recv(1), b"")
                    except ConnectionResetError:
                        pass
                for server in servers:
                    self.assertTrue(server.closed)
                    self.assertTrue(server.close()["already_closed"])
                    with self.assertRaises(OSError):
                        socket.create_connection(split_address(server.mongo_address or ""), timeout=1)
                with self.assertRaises(briskdb.FailedPreconditionError):
                    database.serve(mongo="127.0.0.1:0")
            finally:
                database.close()

    def test_mongo_coexists_with_authenticated_sqlite_remote_without_sharing_auth(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            with briskdb.open(data_dir, shards=2, documents=True) as database:
                with database.session(routing_key="remote-mongo") as session:
                    session.migrate("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                    session.execute("INSERT INTO users VALUES (1, 'native')")
                token = "test-only-remote-mongo-token-32-characters"
                with database.serve(
                    admin=None, mongo="127.0.0.1:0", sqlite_remote_token=token,
                    sqlite_remote_tables=["users"], sqlite_remote_routing_key="remote-mongo",
                ) as server:
                    catalog_url = "http://" + server.http_address + "/sqlite/v1/catalog"
                    with self.assertRaises(urllib.error.HTTPError) as unauthenticated:
                        urllib.request.urlopen(catalog_url, timeout=5)
                    self.assertEqual(unauthenticated.exception.code, 401)
                    unauthenticated.exception.close()
                    request = urllib.request.Request(catalog_url, headers={"Authorization": "Bearer " + token})
                    with urllib.request.urlopen(request, timeout=5) as response:
                        self.assertEqual([table["name"] for table in json.load(response)["tables"]], ["users"])
                    with pymongo.MongoClient("mongodb://" + (server.mongo_address or ""),
                                             serverSelectionTimeoutMS=3000) as client:
                        client.python_mongo.items.insert_one({"_id": 1, "separate": True})
                        self.assertEqual(client.python_mongo.items.count_documents({}), 1)

    def test_tls_scram_listener_works_with_real_psycopg(self) -> None:
        fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/postgres-tls"
        with tempfile.TemporaryDirectory() as data_dir, tempfile.TemporaryDirectory() as secrets:
            secrets_path = Path(secrets)
            certificate = secrets_path / "server.crt"
            private_key = secrets_path / "server.key"
            password_file = secrets_path / "postgres-password"
            shutil.copyfile(fixture / "server.crt", certificate)
            shutil.copyfile(fixture / "server.key", private_key)
            password_file.write_text("python-secret\n", encoding="utf-8")
            os.chmod(private_key, 0o600)
            os.chmod(password_file, 0o600)

            with briskdb.open(data_dir, shards=2, documents=True) as database:
                with database.serve(
                    postgres="127.0.0.1:0",
                    mongo="127.0.0.1:0",
                    postgres_tls_cert=certificate,
                    postgres_tls_key=private_key,
                    postgres_user="briskdb",
                    postgres_password_file=password_file,
                ) as server:
                    _, port = split_address(server.postgres_address or "")
                    with self.assertRaises(psycopg.OperationalError):
                        psycopg.connect(
                            host="localhost",
                            port=port,
                            dbname="default",
                            user="briskdb",
                            password="wrong",
                            sslmode="verify-full",
                            sslrootcert=str(certificate),
                            connect_timeout=5,
                        )
                    with psycopg.connect(
                        host="localhost",
                        port=port,
                        dbname="default",
                        user="briskdb",
                        password="python-secret",
                        sslmode="verify-full",
                        sslrootcert=str(certificate),
                        autocommit=True,
                        cursor_factory=psycopg.ClientCursor,
                        connect_timeout=5,
                    ) as connection:
                        with connection.cursor() as cursor:
                            cursor.execute("SELECT 84 AS answer")
                            self.assertEqual(cursor.fetchall(), [("84",)])
                    with pymongo.MongoClient("mongodb://" + (server.mongo_address or ""),
                                             serverSelectionTimeoutMS=3000) as client:
                        self.assertEqual(client.admin.command("ping")["ok"], 1)
                # PostgreSQL credentials must never permit public Mongo binding.
                with self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                    database.serve(
                        mongo="0.0.0.0:0", postgres="127.0.0.1:0",
                        postgres_tls_cert=certificate, postgres_tls_key=private_key,
                        postgres_password_file=password_file,
                    )

    def test_address_and_bind_failures_leave_the_database_usable(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                database.serve(http="0.0.0.0:0")
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                database.serve(admin="0.0.0.0:0")
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "distinct"):
                database.serve(http="127.0.0.1:8765", admin="127.0.0.1:8765")
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "IP socket address"):
                database.serve(http="localhost:0")
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "must be set together"):
                database.serve(postgres_tls_cert="missing.crt")

            reservation = socket.socket()
            reservation.bind(("127.0.0.1", 0))
            reservation.listen()
            address = f"127.0.0.1:{reservation.getsockname()[1]}"
            try:
                with self.assertRaisesRegex(briskdb.OperationalError, "failed to bind"):
                    database.serve(http=address)
            finally:
                reservation.close()

            with database.session(routing_key="after-bind-error") as session:
                self.assertEqual(session.query("SELECT 1")["rows"], [(1,)])

            data_only = database.serve(admin=None)
            self.assertIsNone(data_only.admin_address)
            self.assertEqual(
                http_json(data_only.data_address, "/v1")["api_version"], "1"
            )
            data_only.close()
            database.close()

    def test_listener_signature_and_address_properties_are_stable(self) -> None:
        signature = inspect.signature(briskdb.Database.serve)
        self.assertEqual(signature.parameters["http"].default, "127.0.0.1:0")
        self.assertEqual(signature.parameters["admin"].default, "127.0.0.1:0")
        self.assertIsNone(signature.parameters["postgres"].default)
        self.assertIsNone(signature.parameters["mongo"].default)
        self.assertEqual(signature.parameters["mongo"].kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertIsNone(inspect.signature(briskdb.AsyncDatabase.serve).parameters["mongo"].default)


class AsyncAttachedServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_mongo_shares_native_data_and_database_close_drains_listener(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = await briskdb.open_async(data_dir, shards=2, documents=True)
            try:
                async with await database.session() as session:
                    await session.create_collection("async_mongo", "items")
                    await session.insert_one("async_mongo", "items", {"_id": 1, "value": "native"})
                    async with await database.serve(mongo="127.0.0.1:0") as server:
                        self.assertEqual(server.mongo_address, server.native.mongo_address)
                        async with pymongo.AsyncMongoClient(
                            "mongodb://" + (server.mongo_address or ""),
                            compressors="zlib", serverSelectionTimeoutMS=3000,
                        ) as client:
                            self.assertEqual(await client.async_mongo.items.find_one({"_id": 1}),
                                             {"_id": 1, "value": "native"})
                            await client.async_mongo.items.insert_one({"_id": 2, "value": "wire"})
                            self.assertEqual((await session.find("async_mongo", "items", {"_id": 2}))["documents"],
                                             [{"_id": 2, "value": "wire"}])
                    self.assertTrue(server.closed)
                    self.assertEqual((await session.count_documents("async_mongo", "items"))["count"], 2)
                server = await database.serve(mongo="127.0.0.1:0", admin=None)
                await database.close()
                self.assertTrue(server.closed)
                self.assertTrue((await server.close())["already_closed"])
            finally:
                await database.close()

    async def test_async_context_manager_and_database_close_order(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = await briskdb.open_async(data_dir, shards=2)
            async with await database.serve() as server:
                self.assertEqual(server.data_address, server.http_address)
                self.assertIsNotNone(server.admin_address)
                self.assertIsNone(server.mongo_address)
                health = await asyncio.to_thread(
                    http_json, server.admin_address or "", "/health"
                )
                self.assertEqual(health["status"], "ok")
            self.assertTrue(server.closed)

            server = await database.serve()
            await database.close()
            self.assertTrue(server.closed)
            self.assertTrue((await server.close())["already_closed"])


if __name__ == "__main__":
    unittest.main()
