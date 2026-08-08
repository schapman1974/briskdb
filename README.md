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
The [SQL parser decision record](docs/SQL_PARSER.md) defines the shared,
dialect-explicit syntax boundary and its resource and dependency limits.
The [common SQL subset contract](docs/SQL_SUBSET.md) defines the opt-in,
protocol-neutral structural validator and its exact accepted statement forms.
The [SQL parameter-normalization contract](docs/SQL_PARAMETERS.md) defines the
opt-in dialect-specific rewrite to canonical SQLite positional parameters and
its per-statement binding metadata.
The [shard-key inference contract](docs/SQL_SHARD_KEYS.md) defines the opt-in,
catalog-aware extraction of typed keys from predicates, bound parameters, and
multi-row inserts without routing or execution.
The [bound statement-planning contract](docs/SQL_PLANNING.md) defines the
synchronous engine API that turns one statement's actual bound values into
owned routes, validates inferred and explicit physical targets, rejects
unroutable sharded writes, and records a single-shard assignment where valid.
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

- Versioned BLAKE3-to-virtual-bucket routing from a caller-provided shard key
- A protocol-neutral async engine with per-request HTTP sessions
- Bounded, lazy per-shard SQLite connection pools with explicit backpressure
- Request cancellation and deadlines that interrupt SQLite and await cleanup
- Finite per-query row/logical-byte budgets with no partial results
- Explicit graceful drain, forced cancellation, and blocking handle cleanup
- Protocol-neutral typed values, ordered columns, positional rows, and results
- A bounded SQL AST parser plus recursive common-subset validator for explicit
  SQLite, PostgreSQL, and MySQL dialects, followed by opt-in source-preserving
  placeholder normalization, per-statement binding metadata, and catalog-aware
  typed shard-key inference plus synchronous bound-value-aware routing plans
  with single-shard write policy, all isolated from the current raw SQLite HTTP
  execution path
- A transactionally versioned `manifest.sqlite` with durable 4,096-bucket
  routing plus logical-database and table metadata
- Identity-bound, WAL-enabled SQLite shard files that are never silently
  recreated after initialization
- Routed execute and query endpoints
- A crash-resumable, journaled schema-migration endpoint for every shard
- A checksummed manifest, generation-bound shard-schema fingerprints, and
  fail-closed `Verifying`/`Ready`/`Migrating`/`Degraded` storage states
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

The retained migration journal identifies this batch by the BLAKE3 digest of
its exact UTF-8 bytes. A byte-identical retry is idempotent; even whitespace or
casing changes identify a new migration. Batches must contain 1 through 65,536
bytes and no NUL. The endpoint retains its experimental `broadcast` name and
returns `{"completed_shards":[...]}`, but it is the only client surface allowed
to change persistent application schema.

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
is now a journaled schema migration: it excludes new ordinary work, waits for
previously admitted operations to drain, and uses dedicated migration
connections rather than reserving every shard pool.

Rust callers may explicitly parse SQL and consume the result with
`validate_common_subset(ParsedSql)`, receiving an owned opaque `CommonSql` on
success. They may then opt into `normalize_placeholders(CommonSql)`, receiving
canonical SQLite `?N` text and per-statement occurrence-to-index metadata
without supplying parameter values. Callers can consume that result with
`translate_sql(NormalizedSql, SqlTranslationMode)`: explicit compatibility mode
maps the documented finite type and syntax set to separate canonical SQLite
SQL, while strict mode requires SQLite input and preserves the
placeholder-normalized SQLite text exactly. The translated result retains its
`NormalizedSql` for routing analysis. Given a complete bound-value slice and a
logical catalog database, callers may invoke
`infer_shard_keys(&Catalog, LogicalDatabaseId, &NormalizedSql, statement_index,
parameters)` to classify the statement as not applicable, not sharded,
unconstrained, contradictory, exact, or multiple and inspect any typed inferred
values. Given the same normalized statement and actual bound values, callers
may instead use synchronous
`Engine::plan_bound_statement(database, normalized, statement_index,
parameters, explicit_routing_key)` to retain the inference and produce one
owned canonical route per inferred value plus an independent explicit route.
That call compares finite inferred and explicit routes by physical shard,
rejects cross-shard or otherwise unroutable cataloged sharded DML, prevents
shard-key updates, and exposes the accepted `assigned_shard()`. The plan
records schema and routing provenance but does not classify complete request
behavior or execute anything. Translation and planning remain independent
opt-in branches over the same normalized request. The HTTP execute, query, and
migration endpoints invoke none of these opt-in layers and retain their
existing raw SQLite behavior. The exact translation matrix and strict-mode
boundary are in [`docs/SQL_TRANSLATION.md`](docs/SQL_TRANSLATION.md).

`EngineOptions` permits 1–16 active connections and 1–1,024 queued operations
per shard, with at most 512 active connections across all shards.
SQLite statements that can leave connection-local state remain uncontracted
pass-through behavior. Such connections, and connections left in a transaction,
are retired rather than reused by another session. Clean read handles can be
shared for ordinary SQL, but a deny-only authorizer probe moves connection-local
SQL such as `PRAGMA data_version`, plus any cross-owner write, to a fresh
disposable handle before execution.
BriskDB-owned shard metadata and storage-control PRAGMA mutations, including
`application_id`, `user_version`, `journal_mode`, `schema_version`, and
`writable_schema`, are denied through every client SQL surface rather than
allowed to invalidate the storage layout. Persistent DDL, including
`ALTER TABLE`, is denied through ordinary routed SQL and is allowed only inside
the journaled migration path, where BriskDB protects and revalidates its
reserved `briskdb` and `briskdb_*` namespaces.
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
Schema migrations preflight the complete batch on every shard before publishing
a journal. Each shard then commits the SQL batch and its next `user_version` in
one SQLite transaction, in ascending shard order. There is no cross-shard
transaction: interruption can retain a committed prefix, which a byte-identical
retry or the next startup validates and resumes. While a migration is running,
new ordinary operations and a second coordinator receive retryable `Busy`.
After durable partial progress, ordinary work receives non-retryable
`FailedPrecondition` until the same migration resumes. The exact SQL text is
retained permanently for recovery and idempotency, so migration batches must
not contain passwords, tokens, or other sensitive literals.

The initial shard count is immutable, so resharding will require an explicit
migration workflow. Opening upgrades exact version-1 through version-6
manifests to version 7 through ordered, resumable steps.
Version 3 introduced the versioned, generation-stamped 4,096-bucket routing
map. Version 4 adds schema generation 0 and immutable logical metadata with
default database ID 1 named `default`; identifier encoding version 1 accepts
only canonical lowercase ASCII names. Table rows can describe sharded, global,
or catalog placement and a sharded table's `Int64`, text, or binary key.

Version 5 binds the manifest and every shard to one random 16-byte layout ID.
Its `Creating`, `Adopting`, and `Ready` states make fresh provisioning and the
one-time adoption of exact legacy version-4 shard headers resumable without
claiming cross-file atomicity. A ready layout opens every shard read-write with
no-create and no-follow semantics and validates its physical ID, `BRSH`
application ID, schema-generation user version, metadata, and existing WAL
mode at startup and whenever another connection is opened. Missing, swapped,
foreign, non-WAL, stale-generation, and unexpected canonical shard files fail
closed, as do shards cloned into the wrong slot or from another layout. They are
not repaired or recreated. The layout ID is an accidental wrong-file guard, not
authentication or tamper protection.

Version 6 adds the retained schema-migration journal and permits committed
application-schema generations from 0 through 2,147,483,647. A fresh migration
advances exactly one generation. After manifest load or upgrade, startup resumes
any active schema migration before ordinary physical-layout reconciliation and
final strict shard validation, then returns an engine. Completed journal rows
remain as the exact generation history.

Version 7 adds a semantic BLAKE3 manifest root, generation-bound persistent
schema fingerprints, SQLite integrity checks, and explicit durable database
states. A v6 migration already in progress is completed under v6 rules before
the upgrade establishes its first trusted schema consensus. New migrations
preserve source and target fingerprints so restart recovery accepts only the
exact journal prefix. Detected corruption makes the shared schema gate fail
closed to new work with `DataCorruption`. When the trusted manifest is
writable, BriskDB makes a best-effort transition to terminal `Degraded` so a
restart also refuses service. Recovery requires stopping BriskDB and restoring
the complete manifest-and-shard set from one consistent known-good copy rather
than rewriting checksum values. These unkeyed
checksums are corruption detectors, not authentication, and they do not cover
application row values or provide a continuous whole-data scan.

The v3-to-v4 step deliberately leaves the table catalog empty: it neither
infers nor adopts tables already present in physical shard files. The v4-to-v5
upgrade retains every validated v4 catalog row, while shard adoption preserves
physical application tables and their data. The public catalog returned by
`Database::catalog()` and `Engine::catalog()` remains read-only and advisory.
Its database and table entries stay immutable, while its schema generation
advances after migration finalization; migrations neither infer nor mutate
`briskdb_tables`. Current query results, routing, SQLite value behavior, HTTP
shapes, and wire contracts are otherwise unchanged.
Startup loads routing and logical metadata in one validated shared snapshot;
runtime routing continues to hash the exact key bytes, derive a virtual bucket,
and read its persisted physical-shard assignment. The generation-1 ranges
preserve every earlier modulo placement, including non-power-of-two shard
counts. Newer or malformed manifests fail closed. The complete format and
upgrade contract is in [manifest storage format](docs/STORAGE_FORMAT.md).
Embedders should call
`Engine::shutdown`; merely dropping the final `Engine` is not the explicit
asynchronous cleanup contract.
Independent `Database` and `Engine` handles in one process that resolve to the
same canonical data directory share schema coordination. Separate BriskDB
server processes must not use the same data directory.

Near-term work includes authentication, richer migration administration and
status APIs in issue #53, scatter/gather reads, observability, and backup
tooling.

## License

Copyright (c) 2026 Stephen Chapman. BriskDB is distributed under the
[MIT License](LICENSE).
