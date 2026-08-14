from __future__ import annotations

import asyncio
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
                self.assertIsNotNone(server.postgres_address)
                self.assertEqual(
                    http_json(server.http_address, "/health"),
                    {"status": "ok", "shards": 2},
                )
                query = http_json(
                    server.http_address,
                    "/v1/query",
                    {"shard_key": "python-http", "sql": "SELECT 7 AS value"},
                )
                self.assertEqual(query["rows"], [[7]])

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
            self.assertEqual(http_json(restarted.http_address, "/health")["status"], "ok")
            database.close()
            self.assertTrue(restarted.closed)
            self.assertTrue(restarted.close()["already_closed"])

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

            with briskdb.open(data_dir, shards=2) as database:
                with database.serve(
                    postgres="127.0.0.1:0",
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

    def test_address_and_bind_failures_leave_the_database_usable(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = briskdb.open(data_dir, shards=2)
            with self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                database.serve(http="0.0.0.0:0")
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
            database.close()


class AsyncAttachedServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_context_manager_and_database_close_order(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            database = await briskdb.open_async(data_dir, shards=2)
            async with await database.serve() as server:
                health = await asyncio.to_thread(http_json, server.http_address, "/health")
                self.assertEqual(health["status"], "ok")
            self.assertTrue(server.closed)

            server = await database.serve()
            await database.close()
            self.assertTrue(server.closed)
            self.assertTrue((await server.close())["already_closed"])


if __name__ == "__main__":
    unittest.main()
