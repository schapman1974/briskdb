from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import threading
import unittest
from typing import Any

import briskdb
import psycopg
import pymongo

from test_mongo_tls_server import FIXTURES, Identity
from test_server_reload import Identity as PostgresIdentity


def replacement_identity(directory: str) -> Identity:
    identity = Identity(directory)
    shutil.copyfile(FIXTURES / "rotated.crt", identity.certificate)
    shutil.copyfile(FIXTURES / "rotated.key", identity.key)
    os.chmod(identity.key, 0o600)
    return identity


def reload_args(identity: Identity) -> dict[str, Any]:
    return dict(tls_cert=identity.certificate, tls_key=identity.key)


class MongoTlsReloadTests(unittest.TestCase):
    def test_sync_rotation_preserves_admitted_clients_and_isolates_postgres(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as old_dir, \
                tempfile.TemporaryDirectory() as new_dir, tempfile.TemporaryDirectory() as pg_dir:
            original, replacement = Identity(old_dir), replacement_identity(new_dir)
            postgres = PostgresIdentity(pg_dir)
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.serve(**original.mongo_args(), **postgres.serve_args()) as server:
                    address = server.mongo_address or ""
                    with pymongo.MongoClient(**original.client_args(address)) as established:
                        established.tls_reload.items.insert_one({"_id": 1, "value": "before"})
                        self.assertIsNone(server.reload_mongo_tls(**reload_args(replacement), timeout_ms=5000))
                        self.assertEqual(server.mongo_address, address)
                        self.assertEqual(established.tls_reload.items.find_one({"_id": 1})["value"], "before")
                        with pymongo.MongoClient(**dict(original.client_args(address),
                                                      serverSelectionTimeoutMS=600)) as rejected:
                            with self.assertRaises(pymongo.errors.ServerSelectionTimeoutError):
                                rejected.admin.command("ping")
                        with pymongo.MongoClient(**replacement.client_args(address)) as current:
                            current.tls_reload.items.update_one({"_id": 1}, {"$set": {"value": "after"}})
                            self.assertEqual(established.tls_reload.items.find_one({"_id": 1})["value"], "after")
                    with psycopg.connect(**postgres.connection_args(server.postgres_address or "")) as sql:
                        self.assertEqual(sql.execute("SELECT 43").fetchone(), ("43",))
                    # PostgreSQL's separate replacement must not replace Mongo's
                    # already-rotated certificate, even on one attached handle.
                    postgres.user = "rotated"
                    postgres.password = "changed-test-secret"
                    postgres.password_file.write_text(postgres.password + "\n", encoding="utf-8")
                    server.reload_postgres_security(**postgres.reload_args())
                    with psycopg.connect(**postgres.connection_args(server.postgres_address or "")) as sql:
                        self.assertEqual(sql.execute("SELECT 47").fetchone(), ("47",))
                    with self.assertRaises(briskdb.OperationalError):
                        server.reload_mongo_tls(tls_cert=replacement.certificate, tls_key=original.key)
                    with pymongo.MongoClient(**replacement.client_args(address)) as current:
                        self.assertEqual(current.tls_reload.items.count_documents({}), 1)
                    server.reload_mongo_tls(**reload_args(original))
                    with pymongo.MongoClient(**original.client_args(address)) as restored:
                        self.assertEqual(restored.tls_reload.items.find_one({"_id": 1})["value"], "after")

    def test_controls_plaintext_disabled_and_closed_handles(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as secrets:
            identity = Identity(secrets)
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.serve(**identity.mongo_args()) as server:
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        server.reload_mongo_tls(**reload_args(identity), cancellation=token)
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        server.reload_mongo_tls(**reload_args(identity), timeout_ms=0)
                    with pymongo.MongoClient(**identity.client_args(server.mongo_address or "")) as client:
                        self.assertEqual(client.admin.command("ping")["ok"], 1)
                with self.assertRaises(briskdb.FailedPreconditionError):
                    server.reload_mongo_tls(**reload_args(identity))
                for options in [{}, dict(mongo="127.0.0.1:0")]:
                    with database.serve(**options) as anonymous:
                        with self.assertRaisesRegex(briskdb.OperationalError, "already-encrypted"):
                            anonymous.reload_mongo_tls(**reload_args(identity))


class AsyncMongoTlsReloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_rotation_with_real_verified_pymongo(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as old_dir, \
                tempfile.TemporaryDirectory() as new_dir:
            original, replacement = Identity(old_dir), replacement_identity(new_dir)
            database = await briskdb.open_async(data, shards=2, documents=True)
            try:
                async with await database.serve(**original.mongo_args()) as server:
                    address = server.mongo_address or ""
                    async with pymongo.AsyncMongoClient(**original.client_args(address)) as established:
                        await established.tls_reload.items.insert_one({"_id": 1})
                        self.assertIsNone(await server.reload_mongo_tls(**reload_args(replacement), timeout_ms=5000))
                        self.assertEqual(await established.tls_reload.items.count_documents({}), 1)
                        async with pymongo.AsyncMongoClient(**replacement.client_args(address)) as current:
                            self.assertEqual(await current.tls_reload.items.count_documents({}), 1)
                    async with pymongo.AsyncMongoClient(**dict(original.client_args(address),
                                                             serverSelectionTimeoutMS=600)) as rejected:
                        with self.assertRaises(pymongo.errors.ServerSelectionTimeoutError):
                            await rejected.admin.command("ping")
            finally:
                await database.close()

    async def test_task_cancellation_signals_native_mongo_reload_token(self) -> None:
        started, finished, release = threading.Event(), threading.Event(), threading.Event()
        captured: list[Any] = []

        class PendingNative:
            def reload_mongo_tls(self, **kwargs: Any) -> None:
                token = kwargs["cancellation"]
                captured.append(token)
                started.set()
                try:
                    while not token.cancelled and not release.wait(0.001):
                        pass
                finally:
                    finished.set()

        server = briskdb.AsyncServer(PendingNative())  # type: ignore[arg-type]
        request = asyncio.create_task(server.reload_mongo_tls(tls_cert="certificate", tls_key="key"))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 5))
            request.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await request
            self.assertTrue(await asyncio.to_thread(finished.wait, 5))
            self.assertTrue(captured[0].cancelled)
        finally:
            release.set()
            await asyncio.to_thread(finished.wait, 5)
