from __future__ import annotations

import asyncio
from contextlib import closing
import http.client
import inspect
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from typing import Any

import briskdb
import psycopg
import pymongo

from test_server import split_address
from test_server_reload import Identity


def tls_args(identity: Identity, plane: str = "http") -> dict[str, Any]:
    return {plane + "_tls_cert": identity.certificate, plane + "_tls_key": identity.key}


def client(address: str, identity: Identity | None) -> http.client.HTTPSConnection:
    context = ssl.create_default_context(cafile=str(identity.certificate) if identity else None)
    context.set_alpn_protocols(["http/1.1"])
    return http.client.HTTPSConnection("localhost", split_address(address)[1], context=context, timeout=3)


def request(connection: http.client.HTTPConnection, path: str, body: Any = None,
            token: str | None = None) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    connection.request("GET" if body is None else "POST", path,
                       None if body is None else json.dumps(body), headers)
    with connection.getresponse() as response:
        content = response.read(65536)
        assert len(content) < 65536
        payload = (json.loads(content) if "json" in response.getheader("Content-Type", "")
                   else content.decode("utf-8")) if content else None
        return response.status, payload


def fetch(address: str, identity: Identity, path: str, body: Any = None,
          token: str | None = None) -> tuple[int, Any]:
    with closing(client(address, identity)) as connection:
        return request(connection, path, body, token)


class HttpTlsServerTests(unittest.TestCase):
    def test_sync_planes_verify_identity_and_coexist_with_postgres_and_mongo(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a, \
                tempfile.TemporaryDirectory() as b:
            original, admin = Identity(a), Identity(b, rotated=True)
            with briskdb.open(data, shards=2, documents=True) as database:
                with database.serve(**tls_args(original), **tls_args(admin, "admin"),
                                    **original.serve_args(), mongo="127.0.0.1:0",
                                    mongo_tls_cert=original.certificate, mongo_tls_key=original.key) as server:
                    self.assertEqual(fetch(server.http_address, original, "/v1/query",
                                           {"shard_key": "https", "sql": "SELECT 7 AS value"})[1]["rows"], [[7]])
                    self.assertEqual(fetch(server.admin_address or "", admin, "/health")[1]["status"], "ok")
                    self.assertEqual(fetch(server.http_address, original, "/health")[0], 404)
                    self.assertEqual(fetch(server.admin_address or "", admin, "/v1")[0], 404)
                    for address, valid, wrong in [(server.http_address, original, admin),
                                                   (server.admin_address or "", admin, original)]:
                        for trust in (None, wrong):
                            with closing(client(address, trust)) as rejected, self.assertRaises(ssl.SSLCertVerificationError):
                                rejected.connect()
                        context = ssl.create_default_context(cafile=str(valid.certificate))
                        with socket.create_connection(split_address(address), timeout=3) as raw:
                            with self.assertRaises(ssl.SSLCertVerificationError):
                                context.wrap_socket(raw, server_hostname="wrong.invalid")
                        with closing(http.client.HTTPConnection(*split_address(address), timeout=3)) as plain:
                            with self.assertRaises((OSError, http.client.HTTPException)):
                                request(plain, "/v1")
                    with psycopg.connect(**original.connection_args(server.postgres_address or "")) as sql:
                        self.assertEqual(sql.execute("SELECT 19").fetchone(), ("19",))
                    with pymongo.MongoClient("mongodb://localhost:" + (server.mongo_address or "").rsplit(":", 1)[1],
                                             tls=True, tlsCAFile=str(original.certificate),
                                             serverSelectionTimeoutMS=3000) as mongo:
                        self.assertEqual(mongo.admin.command("ping")["ok"], 1)
                with database.session(routing_key="https-after-close") as session:
                    self.assertEqual(session.query("SELECT 23")["rows"], [(23,)])

    def test_sync_reload_keeps_existing_connections_and_other_plane_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a, \
                tempfile.TemporaryDirectory() as b:
            original, replacement = Identity(a), Identity(b, rotated=True)
            with briskdb.open(data, shards=2) as database:
                with database.serve(**tls_args(original), **tls_args(original, "admin")) as server:
                    addresses = (server.http_address, server.admin_address)
                    for method, address, path, other, other_path, other_identity in [
                        (server.reload_http_tls, server.http_address, "/v1", server.admin_address or "", "/health", original),
                        (server.reload_admin_tls, server.admin_address or "", "/health", server.http_address, "/v1", replacement),
                    ]:
                        with closing(client(address, original)) as established:
                            self.assertEqual(request(established, path)[0], 200)
                            self.assertIsNone(method(tls_cert=replacement.certificate, tls_key=replacement.key, timeout_ms=5000))
                            self.assertEqual(request(established, path)[0], 200)
                        with closing(client(address, original)) as rejected, self.assertRaises(ssl.SSLCertVerificationError):
                            rejected.connect()
                        self.assertEqual(fetch(address, replacement, path)[0], 200)
                        self.assertEqual(fetch(other, other_identity, other_path)[0], 200)
                        with self.assertRaises(briskdb.OperationalError):
                            method(tls_cert=original.certificate, tls_key=replacement.key)
                        self.assertEqual(fetch(address, replacement, path)[0], 200)
                    self.assertEqual((server.http_address, server.admin_address), addresses)

    def test_invalid_pairs_remote_addresses_keys_and_bind_failures_preserve_database(self) -> None:
        for cls in (briskdb.Database, briskdb.AsyncDatabase):
            signature = inspect.signature(cls.serve)
            for plane in ("http", "admin"):
                for suffix in ("cert", "key"):
                    self.assertIsNone(signature.parameters[plane + "_tls_" + suffix].default)
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a, \
                tempfile.TemporaryDirectory() as b:
            original, replacement = Identity(a), Identity(b, rotated=True)
            with briskdb.open(data, shards=2) as database:
                for plane in ("http", "admin"):
                    for suffix in ("cert", "key"):
                        with self.assertRaisesRegex(briskdb.InvalidArgumentError, "must be set together"):
                            database.serve(**{plane + "_tls_" + suffix: "missing"})
                    with self.assertRaisesRegex(briskdb.InvalidArgumentError, "loopback"):
                        database.serve(**{plane: "0.0.0.0:0", plane + "_tls_cert": "missing", plane + "_tls_key": "missing"})
                    with self.assertRaises(briskdb.OperationalError):
                        database.serve(**{plane + "_tls_cert": original.certificate, plane + "_tls_key": replacement.key})
                with self.assertRaisesRegex(briskdb.InvalidArgumentError, "enabled admin"):
                    database.serve(admin=None, **tls_args(original, "admin"))
                with socket.socket() as occupied, socket.socket() as reservation:
                    occupied.bind(("127.0.0.1", 0))
                    occupied.listen()
                    reservation.bind(("127.0.0.1", 0))
                    port = reservation.getsockname()[1]
                    reservation.close()
                    with self.assertRaises(briskdb.OperationalError):
                        database.serve(http=f"127.0.0.1:{port}", admin=f"127.0.0.1:{occupied.getsockname()[1]}",
                                       **tls_args(original), **tls_args(original, "admin"))
                    with socket.socket() as rebound:
                        rebound.bind(("127.0.0.1", port))
                with database.serve(**tls_args(original)) as server:
                    self.assertEqual(fetch(server.http_address, original, "/v1")[0], 200)
                    with closing(http.client.HTTPConnection(*split_address(server.admin_address or ""), timeout=3)) as plain:
                        self.assertEqual(request(plain, "/health")[0], 200)

    def test_reload_controls_and_database_close_drain_encrypted_and_partial_sockets(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a:
            identity = Identity(a)
            database = briskdb.open(data, config=briskdb.Config(shards=2, shutdown_grace_ms=100))
            try:
                server = database.serve(**tls_args(identity), **tls_args(identity, "admin"))
                for method in (server.reload_http_tls, server.reload_admin_tls):
                    token = briskdb.CancellationToken()
                    token.cancel()
                    with self.assertRaises(briskdb.CancelledError):
                        method(tls_cert=identity.certificate, tls_key=identity.key, cancellation=token)
                    with self.assertRaises(briskdb.InvalidArgumentError):
                        method(tls_cert=identity.certificate, tls_key=identity.key, timeout_ms=0)
                with database.serve(admin=None) as plain:
                    for method in (plain.reload_http_tls, plain.reload_admin_tls):
                        with self.assertRaisesRegex(briskdb.OperationalError, "already-encrypted"):
                            method(tls_cert=identity.certificate, tls_key=identity.key)
                with socket.create_connection(split_address(server.admin_address or ""), timeout=3) as pending, \
                        closing(client(server.http_address, identity)) as established:
                    pending.sendall(b"\x16\x03\x03")
                    established.connect()
                    established.sock.sendall(b"GET /v1 HTTP/1.1\r\nHost:")  # type: ignore[union-attr]
                    database.close()
                    self.assertTrue(server.closed)
                    try:
                        self.assertEqual(pending.recv(1), b"")
                    except (ConnectionResetError, ConnectionAbortedError):
                        pass
                    try:
                        self.assertEqual(established.sock.recv(1), b"")  # type: ignore[union-attr]
                    except (ConnectionResetError, ConnectionAbortedError, ssl.SSLEOFError):
                        pass
                for method in (server.reload_http_tls, server.reload_admin_tls):
                    with self.assertRaises(briskdb.FailedPreconditionError):
                        method(tls_cert=identity.certificate, tls_key=identity.key)
            finally:
                database.close()

    def test_https_sqlite_remote_keeps_bearer_checks_and_real_sqlite3_verification(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a:
            identity = Identity(a)
            token = "https-sqlite-remote-test-token-0123456789"
            with briskdb.open(data, shards=2) as database:
                with database.session(routing_key="https-remote") as session:
                    session.migrate("CREATE TABLE users (id INTEGER PRIMARY KEY)")
                    session.execute("INSERT INTO users VALUES (7)")
                with database.serve(**tls_args(identity), **tls_args(identity, "admin"),
                                    sqlite_remote_token=token, sqlite_remote_tables=["users"],
                                    sqlite_remote_routing_key="https-remote") as server:
                    self.assertEqual(fetch(server.http_address, identity, "/sqlite/v1/catalog")[0], 401)
                    self.assertEqual(fetch(server.http_address, identity, "/sqlite/v1/catalog", token=token)[0], 200)
                    self.assertEqual(fetch(server.admin_address or "", identity, "/sqlite/v1/catalog", token=token)[0], 404)
                    environment = dict(os.environ)
                    environment["SSL_CERT_FILE"] = str(identity.certificate)
                    program = """import briskdb, sqlite3, sys
connection = sqlite3.connect(':memory:')
with briskdb.attach_remote(connection, sys.argv[1], token=sys.argv[2]):
    assert connection.execute('SELECT * FROM remote.users').fetchall() == [(7,)]
connection.close()
print('verified HTTPS sqlite3 passed')
"""
                    result = subprocess.run([sys.executable, "-c", program,
                                             "https://localhost:" + server.http_address.rsplit(":", 1)[1], token],
                                            env=environment, text=True, capture_output=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn("verified HTTPS sqlite3 passed", result.stdout)


class AsyncHttpTlsServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_startup_and_independent_rotation_use_verified_https(self) -> None:
        with tempfile.TemporaryDirectory() as data, tempfile.TemporaryDirectory() as a, \
                tempfile.TemporaryDirectory() as b:
            original, replacement = Identity(a), Identity(b, rotated=True)
            async with await briskdb.open_async(data, shards=2) as database:
                async with await database.serve(**tls_args(original), **tls_args(original, "admin")) as server:
                    for method, address, path in [(server.reload_http_tls, server.http_address, "/v1"),
                                                  (server.reload_admin_tls, server.admin_address or "", "/health")]:
                        self.assertEqual((await asyncio.to_thread(fetch, address, original, path))[0], 200)
                        await method(tls_cert=replacement.certificate, tls_key=replacement.key, timeout_ms=5000)
                        self.assertEqual((await asyncio.to_thread(fetch, address, replacement, path))[0], 200)
                        with self.assertRaises(ssl.SSLCertVerificationError):
                            await asyncio.to_thread(fetch, address, original, path)
                        token = briskdb.CancellationToken()
                        token.cancel()
                        with self.assertRaises(briskdb.CancelledError):
                            await method(tls_cert=original.certificate, tls_key=original.key, cancellation=token)
                        self.assertEqual((await asyncio.to_thread(fetch, address, replacement, path))[0], 200)
                async with await database.session(routing_key="https-async") as session:
                    self.assertEqual((await session.query("SELECT 29"))["rows"], [(29,)])

    async def test_task_cancellation_signals_each_native_http_reload_token(self) -> None:
        for name in ("reload_http_tls", "reload_admin_tls"):
            started, finished, release = threading.Event(), threading.Event(), threading.Event()
            captured: list[Any] = []

            class PendingNative:
                def run(self, **kwargs: Any) -> None:
                    token = kwargs["cancellation"]
                    captured.append(token)
                    started.set()
                    try:
                        while not token.cancelled and not release.wait(0.001):
                            pass
                    finally:
                        finished.set()
                reload_http_tls = run
                reload_admin_tls = run

            server = briskdb.AsyncServer(PendingNative())  # type: ignore[arg-type]
            operation = asyncio.create_task(getattr(server, name)(tls_cert="certificate", tls_key="key"))
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 5))
                operation.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await operation
                self.assertTrue(await asyncio.to_thread(finished.wait, 5))
                self.assertTrue(captured[0].cancelled)
            finally:
                release.set()
                await asyncio.to_thread(finished.wait, 5)
