# Architecture

BriskDB is organized so network protocols can share one routing and execution
core. The module layout preserves the experimental HTTP contract and existing
Rust module paths while making future PostgreSQL and MySQL adapters explicit
peers.

```text
binary (main)
    |
    v
server ---------> protocol::http
    |                    |
    +--------+-----------+
             v
            core
           /    \
          v      v
      storage    sql
```

| Module | Responsibility | Must not own |
| --- | --- | --- |
| `core` | Protocol-neutral `Engine`, `Session`, statements, values, results, and errors; stable key routing; routed execute/query and schema broadcast | JSON/HTTP types, listeners, or Axum handlers |
| `storage` | Manifest and shard layout, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `sql` | SQLite statement execution and conversion between SQLite storage classes and BriskDB values | JSON, routing, filesystem layout, or protocol responses |
| `protocol::http` | HTTP request extraction plus JSON/BriskDB value and RFC 9457 problem-detail encoding | BLAKE3 routing, shard files, or rusqlite calls |
| `protocol::error` | Exhaustive HTTP, PostgreSQL, and MySQL mappings from stable engine error kinds | SQLite errors, routing decisions, or wire-protocol session state |
| `server` | Process configuration, database assembly, listener binding, and Axum lifecycle | SQL parsing or storage implementation details |

Implementation dependencies flow one way: adapters call the async `Engine` in
`core`; the engine coordinates routing, `storage`, and `sql`. An adapter supplies
protocol-neutral session routing context and an owned statement, then receives
the selected shard together with the operation result. It neither computes a
shard nor opens a SQLite connection. The only reverse-facing name is
`storage::Database`, a compatibility re-export of `core::Database`; the storage
implementation does not call core.

## Compatibility during the split

The module split deliberately preserved:

- the CLI flags, environment variables, defaults, and listener behavior;
- every HTTP route and request field, the health, execute, and broadcast response
  shapes, and current error statuses;
- BLAKE3 routing, shard filenames, manifest schema, WAL and synchronous modes;
- SQLite pass-through semantics and cell-level HTTP JSON encoding behavior; and
- the existing `briskdb::api::router` and `briskdb::storage::Database` Rust
  paths through compatibility re-exports.

The ordered-result follow-up deliberately changes the experimental `/v1/query`
response from name-keyed row objects to ordered column metadata and positional
row arrays. This is a pre-1.0 response-contract break needed to keep duplicate
and empty column names representable.

The structured-error follow-up likewise replaces the experimental blanket 500
response and `error`-member JSON body with kind-specific status codes and RFC
9457 problem details. Routes, request fields, routing, and persistence are
unchanged. Public Rust `Database` methods now return `EngineResult<T>` instead
of `anyhow::Result<T>`; this intentional pre-1.0 source migration gives callers
stable error identity while retaining automatic `?` conversion into `anyhow`.

Automated HTTP contract tests cover health, schema broadcast, routed writes,
routed reads, and structured problem-detail serialization through the shared
engine. Unit tests remain colocated with sessions, engine orchestration,
routing, storage, SQL conversion, CLI, and server assembly.

The module names are stable boundaries, not a claim that later roadmap work is
already complete. The async engine and initial session lifecycle are now in
place; connection pools, cancellation, and limits are separate issues. The
synchronous `Database` API remains available as a Rust compatibility surface
and is the implementation used behind the asynchronous boundary.

## Session and asynchronous engine boundary

`Session` is protocol-neutral mutable state owned by one frontend connection or
request. A new session is `Ready`; closing it is a terminal transition to
`Closed`, and engine operations reject a closed session with
`FailedPrecondition`. Ordinary statement failures do not close or poison a
session, so a frontend may correct a request and continue. Sessions are not
clonable. Frontends may issue concurrent calls against one borrowed session,
but the engine serializes them; the HTTP adapter instead creates an independent
session for every request.

The current routing context is an optional caller-supplied shard key. Routed
execute and query operations require that context, and the engine alone hashes
it and reports the selected shard in `Routed<T>`. `Statement` owns its SQL text
and typed parameters so an adapter can hand work across the asynchronous
boundary without borrowing protocol buffers. `EngineStatus` exposes the shard
count needed by health reporting without exposing the storage implementation.

HTTP is stateless at this stage: each execute or query request creates a fresh
session and initializes its routing context from the request's `shard_key`.
Consequently, session settings and transactions cannot span HTTP requests.
Schema broadcast and status calls also go through the shared engine, but do not
perform a routing decision in the adapter.

The local engine currently uses Tokio blocking workers around the existing
synchronous `Database` operations. Those workers are not BriskDB's planned
bounded execution pool, and each SQL operation still opens one or more shard
connections. Bounded per-shard pools and backpressure are issue #10;
cancellation, deadlines, limits, and graceful shutdown are issue #11. Real
`BEGIN`/`COMMIT`/`ROLLBACK`,
failed-transaction state, and single-shard pinning are deliberately deferred to
the PostgreSQL and MySQL transaction work in issues #34 and #47. `Ready` and
`Closed` therefore describe session lifecycle, not SQL transaction state.

Dropping an engine future does not cancel SQLite work in this phase. The
blocking worker retains the session lock until that work ends, so another call
cannot overlap it on the same session; an operation may still commit after its
frontend disconnects. Issue #11 owns interruption and cancellation semantics.

This boundary changes Rust orchestration only. It does not change command-line
or environment configuration, HTTP routes or JSON shapes, shard routing,
manifest schema, SQLite files, WAL or synchronous settings, or any stored data.

## Error boundary

The core exposes a stable `EngineErrorKind` without importing any protocol
response type. SQL and storage classify SQLite failures from primary and
extended result codes plus operation context; they never parse SQLite error
messages. Protocol-owned tables map each kind to an HTTP status and safe RFC
9457 problem, a PostgreSQL SQLSTATE, and a MySQL error number/SQLSTATE pair.
The PostgreSQL and MySQL entries are mapping contracts for future adapters, not
implemented listeners.

Client responses use fixed, safe text for the error kind. Diagnostic display
text and source chains stay available internally but are never serialized, so
SQLite messages, SQL text, and filesystem paths do not leak through an adapter.
Only `Busy` advertises that retrying may succeed; a 5xx status alone is not a
retry signal. The complete taxonomy and mapping table are in the
[error contract](ERRORS.md).

This boundary changes reporting, not persistence: the manifest schema, shard
files, stored values, routing, and configuration formats are unchanged.

## Typed result boundary

Core and SQL code do not use `serde_json::Value`. The protocol-neutral value
model distinguishes signed and unsigned 64-bit integers, binary floating point,
validated exact decimal text, valid UTF-8 text, text containing invalid UTF-8
bytes, and binary data. Decimal construction validates SQL-style decimal syntax
while preserving the caller's digits, scale, sign, and exponent text. This
prevents an adapter from silently narrowing an unsigned
integer, rounding a decimal, or replacing text bytes before it reaches the
storage boundary. `ResultSet` keeps an ordered `Vec<Column>` and each `Row`
keeps positional `Vec<Value>` data. SQLite cannot reliably provide one static
type for every dynamic result column, so column metadata begins as
`DataType::Unknown`; each value still reports its concrete type.

Conversions into SQLite are checked against its five storage classes. Unsigned
integers bind as `INTEGER` only when they fit in `i64`; larger unsigned values,
exact decimals, invalid UTF-8 text, and `NaN` are rejected rather than coerced
to another SQLite storage class. SQLite `TEXT` results preserve invalid bytes as
`Value::InvalidText` inside the core.

The experimental HTTP adapter is the only JSON conversion boundary.
`/v1/query` returns `shard`, an ordered `columns` array of `name` and
`data_type` metadata objects, and positional arrays in `rows`. Column and row
indices correspond exactly. Duplicate and empty names are valid, and metadata
is returned even when there are zero rows.

The adapter renders exact decimals as JSON strings, converts `InvalidText` to a
JSON string with invalid byte sequences replaced by U+FFFD, and maps non-finite
floats to JSON `null`; these losses are explicit adapter policy rather than
storage behavior. HTTP parameters that cannot bind to SQLite without loss fail
instead of being rounded or rewritten. The ordered response change affects only
HTTP query serialization: it does not change routing, configuration, the
manifest, shard files, or any other on-disk data.

The pre-1.0 Rust `Database::execute` and `Database::query` signatures now use
BriskDB `Value` and `ResultSet` directly instead of `serde_json::Value`. This is
an intentional source-level migration to establish the protocol-neutral core;
the legacy module paths remain available, but the old JSON-typed method
signatures do not.
