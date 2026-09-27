from __future__ import annotations

import asyncio
import os
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from typing import Any

import briskdb
import psycopg
import pymongo


FIXTURES = Path(__file__).resolve().parents[2] / "tests/fixtures/postgres-tls"


class Identity:
    def __init__(self, directory: str, *, rotated: bool = False) -> None:
        root = Path(directory)
        stem = "rotated" if rotated else "server"
        self.certificate = root / "server.crt"
        self.key = root / "server.key"
        self.password_file = root / "password"
        self.user = "rotated" if rotated else "briskdb"
        self.password = "rotated-test-secret" if rotated else "original-test-secret"
        shutil.copyfile(FIXTURES / (stem + ".crt"), self.certificate)
        shutil.copyfile(FIXTURES / (stem + ".key"), self.key)
        self.password_file.write_text(self.password + "\n", encoding="utf-8")
        os.chmod(self.key, 0o600)
        os.chmod(self.password_file, 0o600)

    def reload_args(self) -> dict[str, Any]:
        return dict(tls_cert=self.certificate, tls_key=self.key, user=self.user,
                    password_file=self.password_file)

    def serve_args(self) -> dict[str, Any]:
        return dict(postgres="127.0.0.1:0", postgres_tls_cert=self.certificate,
                    postgres_tls_key=self.key, postgres_user=self.user,
                    postgres_password_file=self.password_file)

    def connection_args(self, address: str) -> dict[str, Any]:
        return dict(host="localhost", port=int(address.rsplit(":", 1)[1]),
                    dbname="default", user=self.user, password=self.password,
                    sslmode="verify-full", sslrootcert=str(self.certificate),
                    channel_binding="require", autocommit=True, connect_timeout=5)


class ServerReloadTests(unittest.TestCase):
    def test_sync_reload_rotates_real_tls_and_scram_plus_without_rebinding(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as old_dir, \
                tempfile.TemporaryDirectory() as new_dir:
            original, replacement = Identity(old_dir), Identity(new_dir, rotated=True)
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.serve(**original.serve_args(), mongo="127.0.0.1:0") as server:
                    address = server.postgres_address or ""
                    with psycopg.connect(**original.connection_args(address)) as established:
                        self.assertIsNone(server.reload_postgres_security(**replacement.reload_args()))
                        self.assertEqual(server.postgres_address, address)
                        self.assertEqual(established.execute("SELECT 17").fetchone(), ("17",))
                        with self.assertRaises(psycopg.OperationalError):
                            psycopg.connect(**original.connection_args(address))
                        old_password = replacement.connection_args(address)
                        old_password.update(user=original.user, password=original.password)
                        with self.assertRaises(psycopg.OperationalError):
                            psycopg.connect(**old_password)
                        with psycopg.connect(**replacement.connection_args(address)) as current:
                            self.assertEqual(current.execute("SELECT 19").fetchone(), ("19",))

                    invalid = replacement.reload_args()
                    invalid["tls_key"] = original.key
                    with self.assertRaises(briskdb.OperationalError) as failed:
                        server.reload_postgres_security(**invalid)
                    self.assertNotIn(replacement.password, str(failed.exception))
                    with psycopg.connect(**replacement.connection_args(address)) as current:
                        self.assertEqual(current.execute("SELECT 23").fetchone(), ("23",))
                    with pymongo.MongoClient("mongodb://" + (server.mongo_address or ""),
                                             serverSelectionTimeoutMS=3000) as mongo:
                        self.assertEqual(mongo.admin.command("ping")["ok"], 1)
                    server.reload_postgres_security(**original.reload_args())
                    with psycopg.connect(**original.connection_args(address)) as current:
                        self.assertEqual(current.execute("SELECT 29").fetchone(), ("29",))

    def test_pre_cancelled_zero_timeout_anonymous_and_closed_handles(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as old_dir, \
                tempfile.TemporaryDirectory() as new_dir:
            original, replacement = Identity(old_dir), Identity(new_dir, rotated=True)
            with briskdb.open(data, shards=2) as database:
                with database.serve(**original.serve_args()) as server:
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        server.reload_postgres_security(**replacement.reload_args(), cancellation=token)
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        server.reload_postgres_security(**replacement.reload_args(), timeout_ms=0)
                    with psycopg.connect(**original.connection_args(server.postgres_address or "")) as current:
                        self.assertEqual(current.execute("SELECT 31").fetchone(), ("31",))
                with self.assertRaises(briskdb.FailedPreconditionError):
                    server.reload_postgres_security(**original.reload_args())
                with database.serve(postgres="127.0.0.1:0") as anonymous:
                    with self.assertRaisesRegex(briskdb.OperationalError, "already-secure"):
                        anonymous.reload_postgres_security(**original.reload_args())


class AsyncServerReloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_reload_uses_real_certificate_and_channel_binding_validation(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as old_dir, \
                tempfile.TemporaryDirectory() as new_dir:
            original, replacement = Identity(old_dir), Identity(new_dir, rotated=True)
            database = await briskdb.open_async(data, shards=2)
            try:
                async with await database.serve(**original.serve_args()) as server:
                    address = server.postgres_address or ""
                    async with await psycopg.AsyncConnection.connect(**original.connection_args(address)) as old:
                        self.assertIsNone(await server.reload_postgres_security(**replacement.reload_args(), timeout_ms=5000))
                        self.assertEqual(await (await old.execute("SELECT 37")).fetchone(), ("37",))
                        async with await psycopg.AsyncConnection.connect(**replacement.connection_args(address)) as current:
                            self.assertEqual(await (await current.execute("SELECT 41")).fetchone(), ("41",))
                    with self.assertRaises(psycopg.OperationalError):
                        await psycopg.AsyncConnection.connect(**original.connection_args(address))
            finally:
                await database.close()

    async def test_async_task_cancellation_signals_the_native_request_token(self) -> None:
        # Deterministic wrapper test, separate from real-client rotation above
        # and native pending-publication/deadline tests.
        started, finished, release = threading.Event(), threading.Event(), threading.Event()
        captured: list[Any] = []

        class PendingNative:
            def reload_postgres_security(self, **kwargs: Any) -> None:
                token = kwargs["cancellation"]
                captured.append(token)
                started.set()
                try:
                    while not token.cancelled and not release.wait(0.001):
                        pass
                finally:
                    finished.set()

        server = briskdb.AsyncServer(PendingNative())  # type: ignore[arg-type]
        request = asyncio.create_task(server.reload_postgres_security(
            tls_cert="certificate", tls_key="key", user="briskdb", password_file="password"))
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


if __name__ == "__main__":
    unittest.main()
