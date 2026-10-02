"""Opt-in ISAM catalog + immutable SQLite bases + S3/Parquet pending SQL writes.

Requires a native build with ``s3-overlay``. This separate mode
does not alter ordinary opens or existing databases. Every modifying statement
must target one table/key partition. No implicit retries of user SQL.
"""
from __future__ import annotations

import json
import math
import os
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Iterable, Mapping, Sequence


def _native():
    from . import _briskdb
    implementation = getattr(_briskdb, "S3OverlayDatabase", None)
    if implementation is None:
        raise _briskdb.UnsupportedError(
            "S3 overlay is opt-in: build BriskDB with --features s3-overlay"
        )
    return implementation


def _cell(value: Any):
    if value is None:
        return "Null"
    if isinstance(value, (bool, int)):
        if not -(2**63) <= value < 2**63:
            raise OverflowError("SQLite INTEGER requires a signed 64-bit value")
        return {"Integer": int(value)}
    if isinstance(value, float) and math.isfinite(value):
        return {"Real": value}
    if isinstance(value, str):
        return {"Text": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"Blob": list(bytes(value))}
    raise TypeError("overlay values must be None, int, finite float, str, or bytes")


def _value(cell):
    if cell == "Null":
        return None
    kind, value = next(iter(cell.items()))
    return bytes(value) if kind == "Blob" else value


def _params(values):
    return json.dumps([_cell(value) for value in values], allow_nan=False)


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: list[tuple[Any, ...]]


@dataclass(frozen=True)
class OpenOptions:
    """Explicit per-connection flags; normal BriskDB storage is unaffected."""
    parquet_pruning: bool = True
    read_only: bool = False

    def __post_init__(self):
        for name, value in asdict(self).items():
            if not isinstance(value, bool):
                raise TypeError(f"{name} must be a bool")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> OpenOptions:
        """Parse trusted deployment flags; invalid values never become False."""
        env = os.environ if environ is None else environ
        def flag(name, default):
            value = env.get(name)
            if value is None:
                return default
            if value not in ("true", "false"):
                raise ValueError(f"{name} must be 'true' or 'false'")
            return value == "true"
        return cls(parquet_pruning=flag("BRISKDB_OVERLAY_PARQUET_PRUNING", True),
                   read_only=flag("BRISKDB_OVERLAY_READ_ONLY", False))


def _options_json(options: OpenOptions | None) -> str:
    if options is None:
        options = OpenOptions()
    if not isinstance(options, OpenOptions):
        raise TypeError("options must be briskdb.s3_overlay.OpenOptions")
    return json.dumps(asdict(options))


class Database:
    """A synchronous explicit overlay connection; use a context manager to close.

    S3 credentials use the environment/instance role, never catalog fields.
    Catalog/schema are immutable after creation; no global transactions or DDL.
    """

    def __init__(self, root: str | os.PathLike[str], *, options: OpenOptions | None = None):
        options_json = _options_json(options)
        self._db = _native()(os.fspath(root), options_json)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Database:
        """Open an existing overlay explicitly selected by trusted env flags.

        Requires BRISKDB_STORAGE_MODE=s3-overlay and BRISKDB_OVERLAY_ROOT.
        The immutable catalog supplies bucket, region, prefix and schema.
        """
        env = os.environ if environ is None else environ
        if env.get("BRISKDB_STORAGE_MODE") != "s3-overlay":
            raise ValueError("set BRISKDB_STORAGE_MODE=s3-overlay to use this mode")
        root = env.get("BRISKDB_OVERLAY_ROOT")
        if not root:
            raise ValueError("BRISKDB_OVERLAY_ROOT is required")
        return cls(root, options=OpenOptions.from_env(env))

    @classmethod
    def create(cls, root: str | os.PathLike[str], *, bucket: str, region: str,
               prefix: str, tables: Sequence[Mapping[str, Any]],
               seed: Mapping[str, Iterable[Sequence[Any]]] | None = None,
               shards: int = 4, partitions: int = 64,
               compact_after_files: int = 32, max_pending_files: int = 64,
               write_retry_ms: int = 60_000,
               options: OpenOptions | None = None) -> Database:
        """Create a NEW root and namespace, optionally importing initial rows."""
        options_json = _options_json(options)
        if options is not None and options.read_only:
            raise ValueError("read-only overlay cannot create a database")
        config = dict(format=1, database_id=uuid.uuid4().hex, bucket=bucket,
                      region=region, prefix=prefix.rstrip("/"), shards=shards,
                      partitions=partitions, tables=list(tables),
                      compact_after_files=compact_after_files,
                      max_pending_files=max_pending_files, write_retry_ms=write_retry_ms)
        encoded = {name: [[_cell(v) for v in row] for row in rows]
                   for name, rows in (seed or {}).items()}
        result = cls.__new__(cls)
        result._db = _native().create(os.fspath(root), json.dumps(config), json.dumps(encoded), options_json)
        return result

    def query(self, sql: str, params: Sequence[Any] = ()) -> QueryResult:
        result = json.loads(self._db.query(sql, _params(params)))
        return QueryResult(tuple(result["columns"]),
                           [tuple(_value(v) for v in row) for row in result["rows"]])

    def execute(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any]:
        """Commit one modifying SQL statement; return affected_rows and commit_id."""
        return json.loads(self._db.execute(sql, _params(params)))

    def set_parquet_pruning(self, enabled: bool):
        """Enable/disable advisory ISAM primary-key file pruning (default on).

        This also controls whether new writes publish pruning metadata. Missing
        metadata always falls back to reading the file. SQLite reader only.
        """
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        self._db.set_parquet_pruning(enabled)

    def read_stats(self) -> dict[str, int]:
        """Last SQL scan's file reads/skips and transferred Parquet bytes.

        Internal publication/rebase checks and automatic compaction are excluded.
        """
        return json.loads(self._db.read_stats())

    def settings(self) -> dict[str, Any]:
        """Effective mode, persisted S3/schema config, and current open flags.

        Credentials are never part of this output.
        """
        return json.loads(self._db.settings())

    def query_partition_duckdb(self, table: str, routing_key: Any, sql: str,
                               params: Sequence[Any] = (), *, library: str,
                               sqlite_extension: str, threads: int = 2,
                               memory_mb: int = 256) -> QueryResult:
        """Experimental DuckDB 1.5.6 SELECT over ONE routed table partition.

        Requires duckdb-reader and trusted native library/extension
        paths. Other partitions are excluded; this is NOT an automatic full-DB
        query replacement. DuckDB scans SQLite directly, with BriskDB's verified
        Parquet changes combined in its SQL view. Never silently falls back.
        """
        method = getattr(self._db, "query_duckdb", None)
        if method is None:
            from . import _briskdb
            raise _briskdb.UnsupportedError("build with --features duckdb-reader")
        options = dict(library=os.fspath(library), sqlite_extension=os.fspath(sqlite_extension),
                       threads=threads, memory_mb=memory_mb)
        result = json.loads(method(table, json.dumps(_cell(routing_key)), sql,
                                   _params(params), json.dumps(options)))
        return QueryResult(tuple(result["columns"]),
                           [tuple(_value(v) for v in row) for row in result["rows"]])

    def compact(self, table: str | None = None, partition: int | None = None):
        """Safely merge pending files. Call from cron or a scheduled Lambda.

        With no arguments, visit all partitions. For short Lambda time budgets,
        pass one table and partition per scheduled invocation. Readers/writers
        can continue. Old files are retained, not deleted under active readers.
        """
        return json.loads(self._db.compact(table, partition))

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
