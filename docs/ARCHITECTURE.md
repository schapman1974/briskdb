# Architecture

BriskDB is organized so network protocols can share one routing and execution
core. The first module split preserves the experimental HTTP and Rust APIs
while making future PostgreSQL and MySQL adapters explicit peers.

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
| `core` | Stable key routing and protocol-neutral coordination of routed execute/query and schema broadcast | HTTP types, listeners, or Axum handlers |
| `storage` | Manifest and shard layout, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `sql` | SQLite statement execution and the current JSON/SQLite value conversion | Routing, filesystem layout, or protocol responses |
| `protocol::http` | HTTP request extraction and response/error encoding | BLAKE3 routing, shard files, or rusqlite calls |
| `server` | Process configuration, database assembly, listener binding, and Axum lifecycle | SQL parsing or storage implementation details |

Implementation dependencies flow one way: adapters call `core`; `core`
coordinates `storage` and `sql`. The HTTP adapter receives the selected shard
together with the operation result, so it does not make a second routing
decision. The only reverse-facing name is `storage::Database`, a compatibility
re-export of `core::Database`; the storage implementation does not call core.

## Compatibility during the split

This change deliberately preserves:

- the CLI flags, environment variables, defaults, and listener behavior;
- every HTTP route, request field, response shape, and current error status;
- BLAKE3 routing, shard filenames, manifest schema, WAL and synchronous modes;
- SQLite pass-through semantics and JSON value conversion; and
- the existing `briskdb::api::router` and `briskdb::storage::Database` Rust
  paths through compatibility re-exports.

Automated HTTP contract tests cover health, schema broadcast, routed writes,
routed reads, and SQLite error serialization. Unit tests remain colocated with
routing, storage, SQL conversion, CLI, and server assembly.

The module names are stable boundaries, not a claim that later roadmap work is
already complete. In particular, BriskDB-specific value/result types, the
structured error taxonomy, session state, the async `Engine` interface,
connection pools, cancellation, and limits are separate issues. Until those
land, the core intentionally retains the current synchronous SQLite and JSON
interfaces.
