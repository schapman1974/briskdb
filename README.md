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
The [request-control contract](docs/REQUEST_CONTROLS.md) defines cancellation,
deadlines, materialized-result budgets, and graceful shutdown.
The [manifest storage-format contract](docs/STORAGE_FORMAT.md) defines versioned
startup migrations, downgrade behavior, and recovery boundaries.
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
- Request cancellation and deadlines that interrupt SQLite and await cleanup
- Finite per-query row/logical-byte budgets with no partial results
- Explicit graceful drain, forced cancellation, and blocking handle cleanup
- Protocol-neutral typed values, ordered columns, positional rows, and results
- A transactionally versioned `manifest.sqlite` with a durable 4,096-bucket
  shard catalog
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

Queries default to 10,000 rows and 16 MiB of protocol-neutral logical result
data. Configure these with `--max-result-rows` / `BRISKDB_MAX_RESULT_ROWS` and
`--max-result-bytes` / `BRISKDB_MAX_RESULT_BYTES`. Requests default to a
30-second engine deadline; use `--request-timeout-ms` /
`BRISKDB_REQUEST_TIMEOUT_MS`, where zero disables that default. Graceful
shutdown allows 30 seconds before cancelling admitted work and is configured by
`--shutdown-grace-ms` / `BRISKDB_SHUTDOWN_GRACE_MS`. Ctrl-C and, on Unix,
SIGTERM stop new admissions, drain or cancel admitted SQLite work, close idle
handles, and then stop the process. Accepted HTTP connections are tracked;
connections that outlive the grace window are force-closed and joined before
the server returns.

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
Dropping queued work skips it before SQLite starts. Dropping in-flight work
interrupts its exact leased handle and retains lifecycle, worker, pool, and
session permits until SQLite cleanup finishes. Explicit cancellation behaves
the same way. A near-complete statement may still win the race and return
success; BriskDB never reports cancellation while a known running write might
still commit.
Queries have finite row and logical-byte budgets, account values before cloning
payloads, and return no partial result on `LimitExceeded`.
Broadcast changes and future scatter operations are not atomic across shard
files. The initial shard count is immutable, so resharding will require an
explicit migration workflow. Opening upgrades exact version-1 and version-2
manifests to version 3 through one transaction per numbered step. Version 3
persists hash, key-encoding, and bucket-algorithm versions, a generation-stamped
4,096-bucket map, and active physical-shard lifecycle records. Newer or malformed
manifests fail closed, and downgrade fences reject shipped version-1 and
version-2 readers. Runtime routing deliberately remains the existing BLAKE3
modulo calculation until the next roadmap item activates catalog lookup; the
generation-1 ranges are constructed so that activation can preserve every
existing placement, including non-power-of-two shard counts. This changes only
`manifest.sqlite`, not shard files, SQL behavior, or wire contracts. The
complete format and upgrade contract is in
[manifest storage format](docs/STORAGE_FORMAT.md).
Embedders should call
`Engine::shutdown`; merely dropping the final `Engine` is not the explicit
asynchronous cleanup contract.

Near-term work includes authentication, application-schema migrations,
scatter/gather reads, observability, backup tooling, and failure-injection
tests for multi-shard operations.

## License

Copyright (c) 2026 Stephen Chapman. BriskDB is distributed under the
[MIT License](LICENSE).
