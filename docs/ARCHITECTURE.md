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
| `core` | Protocol-neutral `Value`, `DataType`, `Column`, `Row`, and `ResultSet`; stable key routing; routed execute/query and schema broadcast | JSON/HTTP types, listeners, or Axum handlers |
| `storage` | Manifest and shard layout, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `sql` | SQLite statement execution and conversion between SQLite storage classes and BriskDB values | JSON, routing, filesystem layout, or protocol responses |
| `protocol::http` | HTTP request extraction plus JSON/BriskDB value and response/error encoding | BLAKE3 routing, shard files, or rusqlite calls |
| `server` | Process configuration, database assembly, listener binding, and Axum lifecycle | SQL parsing or storage implementation details |

Implementation dependencies flow one way: adapters call `core`; `core`
coordinates `storage` and `sql`. The HTTP adapter receives the selected shard
together with the operation result, so it does not make a second routing
decision. The only reverse-facing name is `storage::Database`, a compatibility
re-export of `core::Database`; the storage implementation does not call core.

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

Automated HTTP contract tests cover health, schema broadcast, routed writes,
routed reads, and SQLite error serialization. Unit tests remain colocated with
routing, storage, SQL conversion, CLI, and server assembly.

The module names are stable boundaries, not a claim that later roadmap work is
already complete. The structured error taxonomy, session state, the async
`Engine` interface, connection pools, cancellation, and limits are separate
issues. Until those land, the core intentionally retains the current
synchronous execution interface.

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
