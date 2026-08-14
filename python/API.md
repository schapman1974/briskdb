# Python API

`briskdb` is a typed wrapper around the listener-free Rust engine. Public
classes and functions are covered by the packaged `py.typed` marker and `.pyi`
files; the signatures below are the compact API map.

## Open and configure

- `open(path, *, shards=None, config=None) -> Database`
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

`Database` exposes `session()`, `checkpoint()`, `close()`, state/config
properties, and a synchronous context manager. `Session` exposes routing-key
state, `migrate()`, `execute()`, `query()`, `query_logical()`, `cursor()`,
`logical_cursor()`, `status()`, `close()`, and a context manager.

The `AsyncDatabase`, `AsyncSession`, and `AsyncCursor` facades provide the same
lifecycle and SQL operations with `await`/`async with`. Cancelling a query task
propagates a native `CancellationToken` into the exact Rust request.

## Results and errors

Queries return `shards`, `columns`, and tuple `rows`. Writes return `shard`,
`rows_affected`, and an optional `generated_key`. Cursors provide
`fetchone()`, `fetchmany()`, `fetchall()`, iteration, and deterministic close.

All native failures derive from `BriskDBError`; each has stable `code` and
`retryable` class attributes. See [value and error conversions](VALUE_CONVERSIONS.md)
and [sync/async lifecycle details](ASYNC_API.md).

Transactions, retained SQLite streaming cursors, and document calls are not
claimed until their engine dependencies land.
