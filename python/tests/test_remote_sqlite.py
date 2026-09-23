from __future__ import annotations

import os
import http.client
from email.message import Message
import secrets
import sqlite3
import struct
import ssl
import threading
import tempfile
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import urllib.error
import urllib.request
from unittest import mock

import briskdb
from briskdb import remote


class RemoteSqliteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        available = (sqlite3.sqlite_version_info >= (3, 31, 0)
                     and hasattr(sqlite3.Connection, "enable_load_extension")
                     and hasattr(sqlite3.Connection, "load_extension"))
        if not available:
            message = "host SQLite lacks addon support; capability rejection is tested separately"
            if os.environ.get("BRISKDB_REQUIRE_REMOTE_SQLITE") == "1":
                raise AssertionError(message)
            raise unittest.SkipTest(message)

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = briskdb.open(self.directory.name, shards=2)
        self.addCleanup(self.database.close)
        self.session = self.database.session(routing_key="sqlite-remote")
        self.addCleanup(self.session.close)
        self.session.migrate("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)")
        self.session.migrate("CREATE TABLE private_notes (secret TEXT)")
        for row in [(1, "first", 1.25, b"\x00\xff"), (2, "second", -2.5, b""),
                    (3, "nul\x00unicode 🍋", None, None), (2**63 - 1, None, 0.0, b"z")]:
            self.session.execute("INSERT INTO users VALUES (?1, ?2, ?3, ?4)", row)
        self.token = secrets.token_urlsafe(32)
        self.server = self.database.serve(admin=None, sqlite_remote_token=self.token,
                                          sqlite_remote_tables=["users"],
                                          sqlite_remote_routing_key="sqlite-remote")
        self.addCleanup(self.server.close)
        self.url = "http://" + self.server.http_address
        self.connection = sqlite3.connect(":memory:")
        self.addCleanup(self.connection.close)

    def attach(self, **kwargs: object) -> briskdb.RemoteAttachment:
        return briskdb.attach_remote(self.connection, self.url, token=self.token, **kwargs)

    def test_real_sqlite_roundtrip_local_join_types_parameters_and_reads(self) -> None:
        with self.attach() as attached:
            self.assertIsInstance(self.connection, sqlite3.Connection)
            self.assertEqual(attached.tables, ("users",))
            self.assertEqual(attached.scope, "legacy-shard")
            self.assertEqual(self.connection.execute(
                "SELECT id, name, score, data FROM remote.users WHERE id=?", (1,)
            ).fetchall(), [(1, "first", 1.25, b"\x00\xff")])
            self.assertEqual(self.connection.execute(
                "SELECT name FROM users WHERE id=3").fetchone(), ("nul\x00unicode 🍋",))
            self.assertEqual(self.connection.execute(
                "SELECT id FROM users WHERE name IS NULL").fetchone(), (2**63 - 1,))
            self.assertEqual(self.connection.execute(
                "SELECT id FROM users WHERE id='2'").fetchone(), (2,))
            self.connection.execute("CREATE TABLE main.labels(id, label)")
            self.connection.execute("INSERT INTO main.labels VALUES (2, 'local')")
            self.connection.commit()
            self.assertEqual(self.connection.execute(
                "SELECT u.name, l.label FROM remote.users u JOIN main.labels l ON u.id=l.id"
            ).fetchall(), [("second", "local")])
            self.assertEqual(self.connection.execute(
                "SELECT count(*), sum(score) FROM users").fetchone(), (4, -1.25))
            self.assertEqual(self.connection.execute(
                "SELECT DISTINCT id FROM users ORDER BY id DESC LIMIT 2").fetchall(), [(2**63-1,), (3,)])
            self.assertEqual(self.connection.execute(
                "SELECT a.id FROM users a JOIN users b ON a.id=b.id ORDER BY a.id"
            ).fetchall(), [(1,), (2,), (3,), (2**63-1,)])
            self.session.execute("INSERT INTO users(id, name) VALUES (4, 'new')")
            self.assertEqual(self.connection.execute("SELECT name FROM users WHERE id=4").fetchone(), ("new",))
        self.assertTrue(attached.closed)
        attached.close()
        self.assertEqual([r[1] for r in self.connection.execute("PRAGMA database_list")], ["main"])

    def test_readonly_rowid_direct_only_and_extension_loading_disabled(self) -> None:
        self.attach()
        for sql in ["INSERT INTO users(id) VALUES (7)", "UPDATE users SET name='bad'", "DELETE FROM users"]:
            with self.subTest(sql=sql), self.assertRaises(sqlite3.OperationalError):
                self.connection.execute(sql)
            self.connection.rollback()
        with self.assertRaisesRegex(sqlite3.OperationalError, "rowid is unsupported"):
            self.connection.execute("SELECT rowid FROM users").fetchall()
        self.connection.execute("CREATE VIEW remote.forbidden AS SELECT * FROM users")
        with self.assertRaises(sqlite3.OperationalError):
            self.connection.execute("SELECT * FROM remote.forbidden").fetchall()
        with self.assertRaisesRegex(sqlite3.OperationalError, "not authorized"):
            self.connection.load_extension(briskdb._briskdb.__file__)
        schema_sql = self.connection.execute("SELECT sql FROM remote.sqlite_schema").fetchall()
        self.assertNotIn(self.token, repr(schema_sql))
        self.connection.execute("DROP TABLE remote.users")
        self.assertEqual(self.session.query("SELECT count(*) FROM users")["rows"], [(4,)])

    def test_authentication_and_dedicated_listener_do_not_expose_sql_or_admin(self) -> None:
        with self.assertRaisesRegex(sqlite3.OperationalError, "authentication"):
            briskdb.attach_remote(self.connection, self.url, token="x" * 32)
        for path in ["/v1/query", "/v1/execute", "/health", "/sqlite/v1/catalog"]:
            with self.subTest(path=path), self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(self.url + path, timeout=5)
            self.assertIn(error.exception.code, (401, 404))
            error.exception.close()
        client = remote._Client(self.url, self.token, 5)
        client.discover(None)
        self.assertEqual([table["name"] for table in client.tables], ["users"])
        with self.assertRaisesRegex(sqlite3.OperationalError, "not allowed"):
            client.request("/sqlite/v1/scan", {"instance": client.catalog["instance"],
                "generation": client.catalog["generation"], "table": "private_notes"})
        for path in ["/v1/query", "/v1/execute", "/health"]:
            with self.assertRaises(sqlite3.OperationalError):
                client.request(path, {"sql": "DROP TABLE users"})

    def test_schema_change_and_server_restart_fail_closed(self) -> None:
        attached = self.attach()
        self.session.migrate("ALTER TABLE users ADD COLUMN extra TEXT")
        with self.assertRaisesRegex(sqlite3.OperationalError, "schema/server changed"):
            self.connection.execute("SELECT * FROM users").fetchall()
        attached.close()
        self.attach()
        self.assertEqual(len(self.connection.execute("SELECT * FROM users").fetchone()), 5)
        address = self.server.http_address
        self.server.close()
        with self.database.serve(http=address, admin=None, sqlite_remote_token=self.token,
                                 sqlite_remote_tables=["users"], sqlite_remote_routing_key="sqlite-remote"):
            with self.assertRaisesRegex(sqlite3.OperationalError, "schema/server changed"):
                self.connection.execute("SELECT * FROM users").fetchall()

    def test_active_transaction_duplicate_schema_and_invalid_inputs_do_not_mutate(self) -> None:
        self.connection.execute("CREATE TABLE keep_me(x)")
        self.connection.execute("INSERT INTO keep_me VALUES (1)")
        with self.assertRaises(sqlite3.ProgrammingError):
            self.attach()
        self.assertTrue(self.connection.in_transaction)
        self.connection.rollback()
        self.attach()
        with self.assertRaisesRegex(sqlite3.OperationalError, "already attached"):
            self.attach()
        for url in ["http://example.com", "http://localhost", "https://user:pw@example.com",
                    "https://example.com/path", "https://example.com/?token=secret"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                briskdb.attach_remote(self.connection, url, token=self.token, schema="bad")
        self.assertEqual([r[1] for r in self.connection.execute("PRAGMA database_list")], ["main", "remote"])
        with self.assertRaises(sqlite3.OperationalError):
            self.attach(schema="bad", tables=["private_notes"])

    def test_two_connections_and_quoted_alias_cleanup(self) -> None:
        with self.attach(schema='quoted"alias', tables=["users"]):
            self.assertEqual(self.connection.execute('SELECT count(*) FROM "quoted""alias".users').fetchone(), (4,))
            other = sqlite3.connect(":memory:")
            try:
                with briskdb.attach_remote(other, self.url, token=self.token):
                    self.assertEqual(other.execute("SELECT count(*) FROM users").fetchone(), (4,))
            finally:
                other.close()

    def test_custom_row_factory_busy_detach_and_network_failure_recovery(self) -> None:
        self.connection.row_factory = lambda cursor, row: {column[0]: value for column, value in zip(cursor.description, row)}
        attached = self.attach()
        self.assertEqual(self.connection.execute("SELECT count(*) AS n FROM users").fetchone(), {"n": 4})
        cursor = self.connection.execute("SELECT * FROM users")
        cursor.fetchone()
        with self.assertRaises(sqlite3.OperationalError):
            attached.close()
        self.assertFalse(attached.closed)
        cursor.close()
        self.server.close()
        with self.assertRaisesRegex(sqlite3.OperationalError, "network request failed"):
            self.connection.execute("SELECT * FROM users").fetchall()
        attached.close()

    def test_legacy_scope_is_explicit_and_server_options_validate(self) -> None:
        with self.database.serve(admin=None, sqlite_remote_token=self.token,
                                 sqlite_remote_tables=["users"]) as server:
            with self.assertRaisesRegex(sqlite3.OperationalError, "logical placement"):
                briskdb.attach_remote(self.connection, "http://" + server.http_address, token=self.token)
        for options in [
            {"sqlite_remote_token": self.token},
            {"sqlite_remote_tables": ["users"]},
            {"sqlite_remote_routing_key": "secret"},
            {"sqlite_remote_token": "short", "sqlite_remote_tables": ["users"]},
            {"sqlite_remote_token": self.token, "sqlite_remote_tables": ["sqlite_schema"]},
        ]:
            with self.subTest(options=options), self.assertRaises(briskdb.InvalidArgumentError):
                self.database.serve(**options)

    def test_no_partial_results_even_for_limit_query(self) -> None:
        self.attach()
        self.session.execute("WITH RECURSIVE n(x) AS (VALUES(10) UNION ALL SELECT x+1 FROM n WHERE x<4110) INSERT INTO users(id) SELECT x FROM n")
        with self.assertRaisesRegex(sqlite3.OperationalError, "exceeded its limits"):
            self.connection.execute("SELECT * FROM users LIMIT 1").fetchall()
        self.session.execute("DELETE FROM users WHERE id>=10 AND id<5000")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM users").fetchone(), (4,))

    def test_native_frame_parser_rejects_malformed_and_truncated_frames(self) -> None:
        attached = self.attach()
        good = b"BRS1" + struct.pack("<II", 4, 1) + b"\0" * 4
        bad_frames = [b"", b"BRS1", good + b"x", good[:-1],
                      b"BRS1" + struct.pack("<II", 5, 1) + b"\0" * 5,
                      b"BRS1" + struct.pack("<II", 4, 4097),
                      good[:12] + b"\xff\0\0\0",
                      good[:12] + b"\x03" + struct.pack("<I", 0xffffffff),
                      good[:12] + b"\x01\x01"]
        for frame in bad_frames:
            with self.subTest(frame=frame):
                self.connection.create_function(attached._callback, 2, lambda i, m, frame=frame: frame)
                with self.assertRaises(sqlite3.OperationalError):
                    self.connection.execute("SELECT * FROM users").fetchall()
        self.connection.create_function(attached._callback, 2, lambda i, m: good)
        self.assertEqual(self.connection.execute("SELECT * FROM users").fetchall(), [(None,) * 4])

    def test_redirects_are_not_followed_and_proxy_environment_is_ignored(self) -> None:
        client = remote._Client(self.url, self.token, 5)
        self.assertIsNone(remote._NoRedirect().redirect_request(None, None, 302, "", None, "https://evil.invalid"))
        with mock.patch.dict("os.environ", {"http_proxy": "http://127.0.0.1:1", "https_proxy": "http://127.0.0.1:1", "no_proxy": ""}):
            remote._Client(self.url, self.token, 5).discover(None)
        with mock.patch.object(client.opener, "open", side_effect=urllib.error.URLError(self.token)):
            with self.assertRaises(sqlite3.OperationalError) as error:
                client.request("/sqlite/v1/catalog")
            self.assertNotIn(self.token, str(error.exception))

    def test_https_proxy_uses_certificate_verification_for_real_sqlite_queries(self) -> None:
        upstream = self.url
        forward = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        class Proxy(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.forward()

            def do_POST(self) -> None:
                self.forward()

            def forward(self) -> None:
                body = self.rfile.read(int(self.headers.get("Content-Length", "0"))) if self.command == "POST" else None
                request = urllib.request.Request(upstream + self.path, data=body, headers={
                    "Authorization": self.headers["Authorization"], "Content-Type": "application/json",
                })
                with forward.open(request, timeout=5) as response:
                    data = response.read()
                    self.send_response(response.status)
                    self.send_header("Content-Type", response.headers["Content-Type"])
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

            def log_message(self, *args: object) -> None:
                pass

        fixture = Path(__file__).resolve().parents[2] / "tests/fixtures/postgres-tls"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(fixture / "server.crt"), str(fixture / "server.key"))
        proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        proxy.socket = context.wrap_socket(proxy.socket, server_side=True)
        worker = threading.Thread(target=proxy.serve_forever, daemon=True)
        worker.start()
        try:
            url = "https://127.0.0.1:" + str(proxy.server_port)
            with self.assertRaisesRegex(sqlite3.OperationalError, "network request failed"):
                briskdb.attach_remote(self.connection, url, token=self.token)
            with mock.patch.dict("os.environ", {"SSL_CERT_FILE": str(fixture / "server.crt")}):
                with briskdb.attach_remote(self.connection, url, token=self.token):
                    self.assertEqual(self.connection.execute("SELECT count(*) FROM users").fetchone(), (4,))
        finally:
            proxy.shutdown()
            proxy.server_close()
            worker.join(timeout=5)


class RemoteSqliteCapabilityTests(unittest.TestCase):
    def test_malformed_http_errors_and_truncated_bodies_fail_closed(self) -> None:
        client = remote._Client("https://example.invalid", "x" * 32, 5)
        for error in (http.client.BadStatusLine(client.token),
                      http.client.IncompleteRead(client.token.encode(), 500)):
            with mock.patch.object(client.opener, "open", side_effect=error):
                with self.assertRaises(sqlite3.OperationalError) as observed:
                    client.request("/sqlite/v1/catalog")
                self.assertNotIn(client.token, str(observed.exception))
        for length in ("100", "invalid", "8388609"):
            headers = Message()
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = length
            response = mock.MagicMock()
            response.__enter__.return_value = response
            response.status = 200
            response.headers = headers
            response.read1.side_effect = [b"{}", b""]
            with mock.patch.object(client.opener, "open", return_value=response):
                with self.assertRaises(sqlite3.OperationalError):
                    client.request("/sqlite/v1/catalog")

    def test_missing_loader_is_rejected_before_network_or_connection_changes(self) -> None:
        class WithoutLoader(sqlite3.Connection):
            def __getattribute__(self, name: str) -> object:
                if name in ("enable_load_extension", "load_extension"):
                    raise AttributeError(name)
                return super().__getattribute__(name)

        for factory in (sqlite3.Connection, WithoutLoader):
            connection = sqlite3.connect(":memory:", factory=factory)
            try:
                if hasattr(connection, "enable_load_extension") and hasattr(connection, "load_extension"):
                    continue
                with mock.patch.object(remote._Client, "request", side_effect=AssertionError("network must not run")):
                    with self.assertRaisesRegex(sqlite3.NotSupportedError, "loadable extensions"):
                        briskdb.attach_remote(connection, "https://example.invalid", token="x" * 32)
                self.assertEqual(connection.execute("SELECT 1").fetchone(), (1,))
                self.assertEqual([row[1] for row in connection.execute("PRAGMA database_list")], ["main"])
            finally:
                connection.close()

    def test_old_host_sqlite_is_rejected_without_sending_credentials(self) -> None:
        connection = sqlite3.connect(":memory:")
        try:
            with mock.patch.object(sqlite3, "sqlite_version_info", (3, 30, 0)):
                with mock.patch.object(remote._Client, "request", side_effect=AssertionError("network must not run")):
                    with self.assertRaisesRegex(sqlite3.NotSupportedError, "3.31"):
                        briskdb.attach_remote(connection, "https://example.invalid", token="x" * 32)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
