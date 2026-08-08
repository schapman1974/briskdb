# BriskDB

[![CI](https://github.com/schapman1974/briskdb/actions/workflows/ci.yml/badge.svg)](https://github.com/schapman1974/briskdb/actions/workflows/ci.yml)

BriskDB is an experimental Rust server that spreads keyed workloads across
multiple SQLite databases. It takes the central sharding model proven in
TinyMongo and makes it available as a small network service.

See the [development roadmap](ROADMAP.md) for the protocol-neutral engine,
PostgreSQL and MySQL wire interfaces, durable shard catalog, scatter/gather
planner, APIs, and production-hardening milestones.
The [architecture map](docs/ARCHITECTURE.md) defines the crate's module
boundaries and dependency direction.

The [SQL compatibility contract](docs/SQL_COMPATIBILITY.md) distinguishes the
current SQLite pass-through API from planned PostgreSQL and MySQL compatibility.
The [error contract](docs/ERRORS.md) defines stable engine error kinds, safe
HTTP problem details, and the mappings reserved for future PostgreSQL and MySQL
adapters.
Contributions follow the repository's [test-first completion policy](CONTRIBUTING.md).
The [benchmark baseline](docs/BENCHMARKS.md) defines the reproducible storage
workloads used to measure the current prototype.
BriskDB is available under the [MIT License](LICENSE). The
[supported-platform policy](docs/SUPPORTED_PLATFORMS.md) defines the tested
operating-system, Rust, and filesystem boundaries.

BriskDB supports Rust 1.85 and newer stable releases. CI tests the declared
minimum supported Rust version (MSRV) and the latest stable toolchain.

## Current foundation

- Stable BLAKE3 routing from a caller-provided shard key
- A protocol-neutral async engine with per-request HTTP sessions
- Bounded, lazy per-shard SQLite connection pools with explicit backpressure
- Protocol-neutral typed values, ordered columns, positional rows, and results
- A fixed shard count recorded in `manifest.sqlite`
- One WAL-enabled SQLite database per shard
- Routed execute and query endpoints
- A broadcast endpoint for initializing schema on every shard
- Full SQLite synchronous durability and a five-second busy timeout
- Reproducible point-read, point-write, and four-shard write benchmarks

The on-disk layout is deliberately simple:

```text
briskdb-data/
├── manifest.sqlite
└── shards/
    ├── 0000.sqlite
    ├── 0001.sqlite
    ├── 0002.sqlite
    └── 0003.sqlite
```

## Run it

```bash
cargo run -- --data-dir ./briskdb-data --shards 4
```

The default listener is `127.0.0.1:7654`. Configuration can also be supplied
with `BRISKDB_LISTEN`, `BRISKDB_DATA_DIR`, and `BRISKDB_SHARDS`.

Rust embedders can customize pool sizing through the public `EngineOptions`
type. Existing engine constructors and server startup keep the defaults of four
active connections and 32 queued operations per shard. Server deployments can
override them with `--connections-per-shard` /
`BRISKDB_CONNECTIONS_PER_SHARD` and `--queue-capacity-per-shard` /
`BRISKDB_QUEUE_CAPACITY_PER_SHARD`.

Create a table on every shard:

```bash
curl -X POST http://127.0.0.1:7654/v1/admin/broadcast \
  -H 'content-type: application/json' \
  -d '{"sql":"CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL)"}'
```

Insert a keyed row:

```bash
curl -X POST http://127.0.0.1:7654/v1/execute \
  -H 'content-type: application/json' \
  -d '{
    "shard_key":"widget-1",
    "sql":"INSERT INTO widgets (id, name) VALUES (?1, ?2)",
    "params":["widget-1", "First widget"]
  }'
```

Read it from the same shard:

```bash
curl -X POST http://127.0.0.1:7654/v1/query \
  -H 'content-type: application/json' \
  -d '{
    "shard_key":"widget-1",
    "sql":"SELECT id, name FROM widgets WHERE id = ?1",
    "params":["widget-1"]
  }'
```

The response keeps column metadata and row values in matching index order. The
selected shard depends on the routing key; an example response is:

```json
{
  "shard": 0,
  "columns": [
    {"name": "id", "data_type": "unknown"},
    {"name": "name", "data_type": "unknown"}
  ],
  "rows": [["widget-1", "First widget"]]
}
```

## Deliberate boundaries

This is an initial scaffold, not a production database yet. The current API
accepts SQL and should only be exposed on a trusted network. The HTTP adapter
creates an ephemeral session for each data request, so session state and
transactions cannot span HTTP requests. Each shard has its own bounded pool, so
routed work queued for one shard does not consume another shard's capacity.
Pool admission happens before blocking SQLite work: once a shard's active slots
and queue are full, the engine returns retryable `Busy` (HTTP 503) instead of
growing work without bound. Connections are opened lazily and reused. Broadcast
is the deliberate cross-shard exception: it reserves one slot from every shard
before dispatch and can therefore occupy capacity in several pools at once.

`EngineOptions` permits 1–16 active connections and 1–1,024 queued operations
per shard, with at most 512 active connections across all shards.
SQLite statements that can leave connection-local state remain uncontracted
pass-through behavior. Such connections, and connections left in a transaction,
are retired rather than reused by another session. Clean read handles can be
shared for ordinary SQL, but a deny-only authorizer probe moves connection-local
SQL such as `PRAGMA data_version`, plus any cross-owner write, to a fresh
disposable handle before execution.
A handle that performed an ordinary write may return to the same session,
preserving SQLite write counters, but is replaced before a different session can
observe `last_insert_rowid()`, `changes()`, or `total_changes()`. Those functions
remain uncontracted across calls until sessions gain connection pinning.
Dropping queued work skips it before SQLite starts; dropping in-flight work does
not yet cancel it, and it may still commit.
Cancellation, deadlines, limits, and graceful shutdown remain roadmap work.
Broadcast changes and future scatter operations are not atomic across shard
files. The shard count is immutable after database creation, so resharding will
require an explicit migration workflow. Pooling does not change the manifest
schema, shard files, or stored-data format. Until graceful pool draining lands
in issue #11, dropping the final `Engine` may close idle SQLite handles
synchronously on the dropping thread; server operation dispatch itself remains
behind the bounded worker boundary.

Near-term work includes authentication, cancellation, schema migrations,
scatter/gather reads, observability, backup tooling, and failure-injection
tests for multi-shard operations.

## License

Copyright (c) 2026 Stephen Chapman. BriskDB is distributed under the
[MIT License](LICENSE).
