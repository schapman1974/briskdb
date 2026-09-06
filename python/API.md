# Python API

`briskdb` is a typed wrapper around the in-process Rust engine. Public
classes and functions are covered by the packaged `py.typed` marker and `.pyi`
files; the signatures below are the compact API map.

## Open and configure

- `open(path, *, shards=None, documents=False, uuid_representation=None, config=None) -> Database`
- `connect(...) -> Database` is the synchronous ergonomic alias.
- `await open_async(...) -> AsyncDatabase` and `connect_async(...)` open
  without blocking the event loop.
- `Config(...)` validates shard, pool, queue, result, prepared-object,
  deadline, and shutdown limits before opening storage.

`shards` is required to create storage and optional when reopening it. An
omitted count is read from the validated manifest; an explicit mismatch raises
`FailedPreconditionError`. `Database.shard_count` and `Database.config.shards`
always report the resolved count.

## Database and session

`Database` exposes `session()`, `transaction()`, `checkpoint()`, `serve()`, `close()`,
state/config properties, and a synchronous context manager. `Session` exposes
routing-key state, SQL and document commands, `status()`, `close()`, and a
context manager.

`Transaction` provides routed `execute()`/`query()`, explicit
`commit()`/`rollback()`, and commit-on-success/rollback-on-exception context
management. The `AsyncDatabase`, `AsyncSession`, `AsyncTransaction`, and
`AsyncCursor` facades provide the same lifecycle and SQL operations with
`await`/`async with`. Cancelling a task propagates a native
`CancellationToken` into the exact Rust request.

## Native document commands

The Python wheel contains BriskDB's document engine, but each database handle
must enable it explicitly:

- `open(path, *, documents=True, uuid_representation="standard", ...)`
- `Config(..., documents=True, uuid_representation="standard")`

The accepted UUID modes are `unspecified`, `standard`, `python_legacy`,
`java_legacy`, and `csharp_legacy`. Pass document settings either as direct
open options or through `config`; direct `documents=True` or an explicit UUID
mode cannot be combined with `config`. The document methods require the
optional `bson` package from PyMongo; SQL-only use has no PyMongo dependency.

`Session` exposes this current protocol-neutral engine slice:

- `create_collection(database, collection, *, options=None, ...)`
- `list_collections(database, *, skip=0, limit=None, batch_size=101, ...)`
- `create_index(database, collection, keys, *, name, unique=False, ...)`
- `list_indexes(database, collection, *, skip=0, limit=None, batch_size=101, ...)`
- `insert_one(database, collection, document, ...)`
- `find(database, collection, filter=None, *, skip=0, limit=None, batch_size=101, ...)`
- `count_documents(database, collection, filter=None, *, skip=0, limit=None, ...)`
- `delete_one(database, collection, filter, ...)`

Every method also accepts `request_id`, `timeout_ms`, `cancellation`,
`max_result_rows`, and `max_result_bytes`. A request ID is a nonzero
`uuid.UUID`; omitting it generates one. `AsyncSession` provides the same
methods as coroutines and propagates task or explicit-token cancellation to
the native request.

Each result is an insertion-ordered dictionary containing `request_id`,
`plan`, and `kind`, followed by the command payload. A point or scatter plan
has `kind`, `collection_id`, and ordered `shards`; catalog commands have a null
plan. Payload keys are:

| Result kind | Payload |
| --- | --- |
| `collection` | `collection` metadata |
| `collections` | ordered `collections` list |
| `index_name` | `index_name` and `lifecycle="pending_build"` |
| `indexes` | ordered `indexes` list |
| `insert` | `acknowledged`, `inserted_count`, `inserted_ids` |
| `cursor` | `namespace`, `cursor_id`, `exhausted`, `documents` |
| `count` | `count` |
| `delete` | `acknowledged`, `deleted_count` |

Collection metadata contains `id`, `database_id`, `database`, `name`,
`namespace`, exact BSON `options`, placement `code`/`version`, and index
metadata. Each index contains `name`, exact ordered `keys`, `unique`,
`built_in`, and `lifecycle`. Secondary index declarations currently remain
`pending_build`; the ready built-in `_id_` index is authoritative.

This API deliberately mirrors the document engine's implemented boundary:
one inserted document must carry an explicit `_id`; find/count accept an empty
filter or one literal `_id`; delete accepts one literal `_id`; and a find must
fit in one batch because cursor continuation has not landed. General matchers,
updates, replacements, projection, sorting, aggregation, bulk writes, and
retained cursor operations remain unsupported. There is no Python collection
object or MongoDB network listener in this slice.

## Attached listeners

- `db.serve(*, http="127.0.0.1:0", admin="127.0.0.1:0", postgres=None, postgres_tls_cert=None, postgres_tls_key=None, postgres_user="briskdb", postgres_password_file=None) -> Server`; pass `admin=None` to disable administration
- `await async_db.serve(...) -> AsyncServer`
- `Server.data_address` reports the data address and `Server.http_address`
  remains its compatibility alias. `.admin_address` and `.postgres_address`
  report optional bound addresses.
- `Server.close()` is idempotent; server context exit closes only listeners.
- Database close first drains every attached server, then stops the engine.

Data HTTP, administration HTTP, and unauthenticated PostgreSQL accept only
numeric loopback addresses. The data address serves `/v1` discovery, query, and
execute. The optional administration address serves `/health`, `/metrics`,
`/v1/health`, `/v1/admin/*`, and `/admin/*`; cross-plane paths return 404.
Certificate, key, and password-file arguments enable TLS/SCRAM PostgreSQL and
permit a non-loopback PostgreSQL address. This is single-identity
authentication, not roles or authorization. The PostgreSQL endpoint supports
BriskDB's documented bounded SQL subset. See the repository's
[HTTP listener contract](../docs/HTTP_LISTENERS.md).

## Results and errors

Queries return `shards`, `columns`, and tuple `rows`. Writes return `shard`,
`rows_affected`, and an optional `generated_key`. Cursors provide bounded
native streaming through `fetchone()`, `fetchmany()`, `fetchall()`, iteration,
and deterministic cancellation on close.

All native failures derive from `BriskDBError`; each has stable `code` and
`retryable` class attributes. See [value and error conversions](VALUE_CONVERSIONS.md)
and [sync/async lifecycle details](ASYNC_API.md).
