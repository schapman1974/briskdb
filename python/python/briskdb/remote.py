"""Read-only remote BriskDB tables in Python's real sqlite3 connections."""

from __future__ import annotations

import ipaddress
import http.client
import json
import math
import secrets
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Literal, Optional, Sequence

from . import _briskdb

_MAX_BYTES = 8 * 1024 * 1024


def _quote(name: str) -> str:
    if not isinstance(name, str) or not name or "\0" in name or len(name.encode("utf-8")) > 255:
        raise ValueError("remote SQLite identifiers must contain 1..255 UTF-8 bytes and no NUL")
    return '"' + name.replace('"', '""') + '"'


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        # Never forward a bearer credential to a redirected destination.
        return None


class _Client:
    def __init__(self, url: str, token: str, timeout: float) -> None:
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
            raise ValueError("remote URL must be an HTTP(S) origin without credentials, path, query or fragment")
        if parsed.scheme == "http":
            try:
                local = ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError:
                local = False
            if not local:
                raise ValueError("remote connections require HTTPS; HTTP is allowed only for a literal loopback IP")
        if not isinstance(token, str) or not 32 <= len(token) <= 256 or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in token
        ):
            raise ValueError("remote token must contain 32..256 URL-safe ASCII characters")
        if not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ValueError("remote timeout must be greater than zero and at most 60 seconds")
        # Validate the port before touching SQLite or sending a request.
        _ = parsed.port
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        self.catalog: dict[str, Any] = {}
        self.tables: list[dict[str, Any]] = []

    def request(self, path: str, body: Optional[dict[str, Any]] = None) -> bytes:
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(self.url + path, data=data, headers={
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
            "Accept": "application/json" if body is None else "application/octet-stream",
        })
        deadline = time.monotonic() + self.timeout
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                expected = "application/json" if body is None else "application/octet-stream"
                if response.status != 200 or response.headers.get_content_type() != expected:
                    raise sqlite3.OperationalError("invalid BriskDB remote response type")
                if response.headers.get("Content-Encoding", "identity") != "identity":
                    raise sqlite3.OperationalError("compressed BriskDB remote responses are unsupported")
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isascii() or not length.isdecimal()
                                           or len(length) > 10 or int(length) > _MAX_BYTES):
                    raise sqlite3.OperationalError("invalid BriskDB remote response length")
                chunks = bytearray()
                while True:
                    if time.monotonic() >= deadline:
                        raise sqlite3.OperationalError("BriskDB remote request timed out")
                    block = response.read1(min(65536, _MAX_BYTES + 1 - len(chunks)))
                    chunks.extend(block)
                    if len(chunks) > _MAX_BYTES:
                        raise sqlite3.OperationalError("BriskDB remote response limit exceeded")
                    if not block:
                        if length is not None and len(chunks) != int(length):
                            raise sqlite3.OperationalError("truncated BriskDB remote response")
                        return bytes(chunks)
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            messages = {
                401: "BriskDB remote authentication failed",
                403: "BriskDB remote table is not allowed",
                409: "BriskDB remote schema/server changed; detach and attach again",
                412: "BriskDB remote requires registered logical placement or an explicit server-side legacy routing key",
                413: "BriskDB remote response limit exceeded",
                422: "BriskDB remote query failed or exceeded its limits",
                503: "BriskDB remote request capacity exceeded",
            }
            raise sqlite3.OperationalError(messages.get(code, "BriskDB remote request rejected")) from None
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            # URL, proxy configuration, server bodies and credentials are not
            # safe exception messages. Reads are never retried automatically.
            raise sqlite3.OperationalError("BriskDB remote network request failed") from None

    def discover(self, requested: Optional[Sequence[str]]) -> None:
        try:
            catalog = json.loads(self.request("/sqlite/v1/catalog"))
            if (not isinstance(catalog, dict) or catalog.get("version") != 1
                    or not isinstance(catalog.get("instance"), str)
                    or len(catalog["instance"]) != 64
                    or type(catalog.get("generation")) is not int
                    or not 0 <= catalog["generation"] < 2**64
                    or catalog.get("max_rows") != 4096 or catalog.get("max_bytes") != _MAX_BYTES
                    or catalog.get("scope") not in ("logical", "legacy-shard")
                    or not isinstance(catalog.get("tables"), list)
                    or not 1 <= len(catalog["tables"]) <= 256):
                raise ValueError("invalid remote catalog")
            seen: set[str] = set()
            for table in catalog["tables"]:
                _quote(table["name"])
                key = table["name"].lower()
                if key in seen or key.startswith("sqlite_") or key.startswith("briskdb"):
                    raise ValueError("invalid remote table")
                seen.add(key)
                columns = table["columns"]
                if not isinstance(columns, list) or not 1 <= len(columns) <= 256:
                    raise ValueError("invalid remote columns")
                names: set[str] = set()
                for column in columns:
                    _quote(column["name"])
                    if column["name"].lower() in names:
                        raise ValueError("duplicate remote column")
                    names.add(column["name"].lower())
                    declared = column["declared_type"]
                    if (not isinstance(declared, str) or len(declared.encode("utf-8")) > 255
                            or "\0" in declared):
                        raise ValueError("invalid remote declared type")
            tables = catalog["tables"]
            if requested is not None:
                if isinstance(requested, str) or not requested:
                    raise ValueError("tables must be a nonempty sequence of table names")
                wanted = set(requested)
                if len(wanted) != len(requested) or not wanted.issubset({t["name"] for t in tables}):
                    raise ValueError("requested tables must be distinct names exposed by the server")
                tables = [table for table in tables if table["name"] in wanted]
            self.catalog = catalog
            self.tables = tables
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            raise sqlite3.OperationalError("invalid BriskDB remote catalog or table selection") from None

    def callback(self, index: int, mode: int) -> Any:
        try:
            if type(index) is not int or not 0 <= index < len(self.tables):
                return b"Einvalid BriskDB remote table index"
            table = self.tables[index]
            if mode == 0:
                # Quoted type names retain SQLite affinity without letting a
                # remote schema inject constraints or executable SQL.
                columns = [
                    _quote(c["name"]) + (' "' + c["declared_type"].replace('"', '""') + '"'
                                          if c["declared_type"] else "")
                    for c in table["columns"]
                ]
                return "CREATE TABLE x(" + ",".join(columns) + ")"
            if mode != 1:
                return b"Einvalid BriskDB remote callback mode"
            return self.request("/sqlite/v1/scan", {
                "instance": self.catalog["instance"], "generation": self.catalog["generation"],
                "table": table["name"],
            })
        except sqlite3.OperationalError as error:
            return b"E" + str(error).encode("utf-8")[:240]
        except Exception:
            return b"EBriskDB remote callback failed"


class RemoteAttachment:
    """An attached read-only proxy schema; closing it never changes the server.

    No remote writes, shared transaction snapshots, hidden rowids or predicate
    pushdown are provided. Every scan is independently bounded and committed.
    """

    def __init__(self, connection: sqlite3.Connection, schema: str,
                 callback: str, tables: Sequence[str], scope: str) -> None:
        self._connection = connection
        self.schema = schema
        self.tables = tuple(tables)
        self.scope = scope
        self._callback = callback
        self.closed = False

    def close(self) -> None:
        if self.closed:
            return
        # A busy cursor/transaction makes DETACH fail without tearing down the
        # callback. The caller can finish its statements and retry close().
        self._connection.execute("DETACH DATABASE " + _quote(self.schema)).close()
        self._connection.create_function(self._callback, 2, None)
        self.closed = True

    def __enter__(self) -> RemoteAttachment:
        return self

    def __exit__(self, *exception: object) -> Literal[False]:
        self.close()
        return False


def attach_remote(connection: sqlite3.Connection, url: str, *, token: str,
                  schema: str = "remote", tables: Optional[Sequence[str]] = None,
                  timeout: float = 15.0) -> RemoteAttachment:
    """Expose authenticated BriskDB tables in an in-memory attached schema.

    Requires host SQLite >= 3.31 with extension loading enabled at build time.
    HTTPS uses platform certificate verification; plaintext is loopback only.
    Extension loading is disabled again immediately after loading the addon.
    Existing local transactions are never committed or rolled back implicitly.
    """
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("attach_remote requires a standard sqlite3.Connection")
    quoted = _quote(schema)
    if schema.lower() in ("main", "temp"):
        raise ValueError("remote schema must not be main or temp")
    if connection.in_transaction:
        raise sqlite3.ProgrammingError("finish the local transaction before attaching remote tables")
    if sqlite3.sqlite_version_info < (3, 31, 0):
        raise sqlite3.NotSupportedError("BriskDB remote requires host SQLite 3.31 or newer")
    if not hasattr(connection, "enable_load_extension") or not hasattr(connection, "load_extension"):
        raise sqlite3.NotSupportedError("this Python sqlite3 build does not support loadable extensions")
    cursor = connection.cursor()
    cursor.row_factory = None
    try:
        exists = cursor.execute(
            "SELECT 1 FROM pragma_database_list WHERE name = ? COLLATE NOCASE", (schema,)
        ).fetchone() is not None
    finally:
        cursor.close()
    if exists:
        raise sqlite3.OperationalError("requested remote schema is already attached")
    client = _Client(url, token, timeout)
    client.discover(tables)
    try:
        connection.enable_load_extension(True)
        connection.load_extension(_briskdb.__file__)
    finally:
        connection.enable_load_extension(False)
    callback = "__briskdb_remote_fetch_" + secrets.token_hex(16)
    connection.create_function(callback, 2, client.callback)
    attached = False
    try:
        connection.execute("ATTACH DATABASE ':memory:' AS " + quoted).close()
        attached = True
        for index, table in enumerate(client.tables):
            connection.execute(
                "CREATE VIRTUAL TABLE " + quoted + "." + _quote(table["name"])
                + " USING briskdb_remote(" + callback + "," + str(index) + ","
                + str(len(table["columns"])) + ")"
            ).close()
    except BaseException:
        if attached:
            connection.execute("DETACH DATABASE " + quoted).close()
        connection.create_function(callback, 2, None)
        raise
    return RemoteAttachment(connection, schema, callback, [t["name"] for t in client.tables], client.catalog["scope"])
