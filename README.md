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

## Deliberate boundaries

This is an initial scaffold, not a production database yet. The current API
accepts SQL and should only be exposed on a trusted network. Each request opens
a SQLite connection; pooling is the next performance step. Broadcast changes
and future scatter operations are not atomic across shard files. The shard
count is immutable after database creation, so resharding will require an
explicit migration workflow.

Near-term work includes authentication, connection pools, schema migrations,
scatter/gather reads, observability, backup tooling, and failure-injection
tests for multi-shard operations.

## License

Copyright (c) 2026 Stephen Chapman. BriskDB is distributed under the
[MIT License](LICENSE).
