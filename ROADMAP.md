# BriskDB roadmap

BriskDB is a sharded SQLite server written in Rust. Its core should be usable
through several connection interfaces without tying storage or query behavior
to any one protocol. The first three interfaces are:

1. PostgreSQL wire protocol
2. MySQL wire protocol
3. Versioned HTTP/JSON API

Additional protocols should be adapters over the same session and execution
engine, not separate database implementations. Any combination of listeners
may run concurrently; configuration enables listeners independently rather
than selecting one exclusive connection type.

This roadmap is ordered by dependency and risk rather than by calendar date.

## Product contract

### What compatibility means

PostgreSQL and MySQL support has three distinct layers:

- **Wire compatibility:** existing drivers can connect, authenticate, prepare
  statements, bind values, execute queries, receive typed rows, and manage a
  transaction.
- **SQL compatibility:** BriskDB accepts a documented common SQL subset and
  translates selected PostgreSQL/MySQL syntax to SQLite.
- **Behavioral compatibility:** metadata queries, error codes, types, and edge
  cases behave closely enough for named tools and ORMs.

The first public milestone promises wire compatibility and a documented SQL
subset. It must not claim to be a drop-in PostgreSQL or MySQL replacement.
Behavioral compatibility will be reported per client and ORM in a tested
compatibility matrix.

### Initial guarantees

- A transaction that touches one shard is atomic and durable according to the
  configured SQLite synchronous mode.
- Reads or writes with an exact shard key visit one shard.
- Multi-shard writes are rejected unless an operation explicitly opts into a
  later, documented coordination mode.
- Scatter reads merge committed results from several shards but do not provide
  a cross-file atomic snapshot in the first release.
- Schema changes are versioned, journaled, and applied to every shard. A
  partially completed migration is visible and resumable.
- The routing hash algorithm, key encoding, virtual-bucket count, and shard map
  are persisted and versioned in the manifest.

## Target architecture

```text
 PostgreSQL listener ─┐
 MySQL listener ──────┼─> protocol-neutral sessions and requests
 HTTP API ────────────┘                 │
                                       v
                         SQL parse / normalize / plan
                                       │
                            shard router / coordinator
                              │                  │
                       single-shard path    scatter path
                              │                  │
                              └──── execution engine ────┐
                                                        │
                         manifest pool     shard connection pools
                               │          │       │       │
                         manifest.sqlite  0000    0001    0002 ...
```

The core boundary should use protocol-neutral Rust types:

- `Session`: identity, logical database, transaction state, routing context,
  prepared statements, and cancellation handle.
- `Request`: query, prepare, bind/execute, begin, commit, rollback, and session
  setting operations.
- `Value` and `DataType`: one loss-aware type system for SQLite, PostgreSQL,
  MySQL, and JSON conversion.
- `ResultSet`: ordered column metadata plus rows; duplicate column names remain
  valid and are not collapsed into a JSON object. Production query paths expose
  a backpressured row stream rather than buffering unbounded results.
- `EngineError`: stable internal error kinds mapped to PostgreSQL SQLSTATE,
  MySQL error number/SQLSTATE, and HTTP status/problem details.
- `QueryPlan`: single shard, scatter read, schema broadcast, manifest operation,
  or rejected unsupported operation.

Protocol handlers may encode and decode messages, but must not open SQLite
connections or implement independent routing rules.

## Routing model

### Virtual buckets

Replace direct `hash(key) % shard_count` routing before the storage format is
declared stable:

```text
canonical key bytes -> versioned BLAKE3 hash -> virtual bucket -> physical shard
```

Start with 4,096 virtual buckets. The manifest stores each bucket's current
physical shard and map generation. This makes later rebalancing incremental
instead of remapping nearly every key whenever a shard is added.

### Declaring a shard key

Each sharded table has one cataloged shard-key expression, initially a single
`NOT NULL` column. The same logical schema exists on every physical shard.
Tables may later be declared `GLOBAL` for small replicated lookup data or
`CATALOG` for manifest-owned metadata.

The preferred routing order is:

1. Extract an equality value for the cataloged shard-key column from a parsed
   statement and its bound parameters.
2. Use an explicit transaction/session routing key when one has been set.
3. Scatter only if the statement is read-only and the planner supports it.
4. Reject an unroutable write before executing on any shard.

Early protocol testing may use explicit settings:

- PostgreSQL: `SET briskdb.shard_key = 'tenant-42'`
- MySQL: `SET @briskdb_shard_key = 'tenant-42'`
- HTTP: `shard_key` in the versioned request body

Explicit session state is useful for transactions, but pooled applications can
leak session settings. Automatic extraction from SQL and bound parameters is
therefore required before calling the driver interfaces production-ready.

### Cross-shard boundaries

- `BEGIN` pins a session to the first routed shard. A later statement targeting
  another shard fails with a stable cross-shard-transaction error.
- Unique constraints are shard-local unless they include the shard key. Global
  uniqueness requires a separately designed manifest-owned reservation index.
- Joins are initially supported only when every participating table is
  co-located on one shard. Distributed joins are a later planner feature.
- Globally unique identifiers should use application-assigned UUID/ULID-style
  values. A globally serialized integer sequence is optional future work.

## Milestones
### 0. Baseline and compatibility contract

Status: **complete**

- [x] Create the Rust service and Git repository.
- [x] Persist a fixed shard count in `manifest.sqlite`.
- [x] Create WAL-enabled shard files and stable keyed routing.
- [x] Provide experimental routed HTTP execute/query calls.
- [x] Add initial unit and end-to-end smoke tests.
- [x] Write `docs/SQL_COMPATIBILITY.md` with supported syntax and explicit
  SQLite/PostgreSQL/MySQL differences.
- [x] Add CI for formatting, Clippy, tests, and supported Rust versions.
- [x] Add a benchmark baseline for point reads, writes, and four-shard
  concurrent writes.
- [x] Choose and document the project license and supported-platform policy.

Exit criterion: the current prototype is reproducible in CI and its promises
and non-promises are written down.

### 1. Protocol-neutral core

Status: **complete**

- [x] Split the crate into `core`, `storage`, `sql`, `protocol/http`, and
  `server` modules without changing externally visible behavior.
- [x] Replace `serde_json::Value` in storage with BriskDB `Value`, `DataType`,
  `Column`, `Row`, and `ResultSet` types.
- [x] Preserve column order and duplicate column names.
- [x] Add a structured error taxonomy and mappings for HTTP, PostgreSQL, and
  MySQL.
- [x] Introduce a `Session` state machine and an async `Engine` interface used
  by every frontend.
- [x] Move blocking SQLite work behind a bounded worker/pool abstraction; add
  per-shard connection pools and backpressure.
- [x] Add cancellation, request deadlines, result row/byte limits, and graceful
  shutdown hooks to the core interface.

Exit criterion: the HTTP adapter contains no routing or SQLite logic and all
existing tests pass through the shared engine.

### 2. Durable shard catalog and routing

- [x] Version the manifest schema with transactional migrations.
- [x] Persist hash/key-encoding versions, 4,096 virtual buckets, physical shard
  records, map generation, and lifecycle state.
- [x] Replace modulo routing with virtual-bucket lookup and add golden routing
  vectors so upgrades cannot silently move keys.
- [x] Add logical databases and table metadata, including table placement and
  shard-key column/type.
- [x] Validate at startup that every shard is present, uses WAL, has the expected
  application ID/user version, and matches the cataloged schema generation.
- [ ] Implement a crash-resumable schema migration journal instead of the
  current best-effort broadcast endpoint.
- [ ] Add checksums/integrity checks and explicit states for degraded or
  partially migrated databases.

Exit criterion: restarts and upgrades cannot silently change routing, and an
interrupted schema migration can be diagnosed and resumed.

### 3. SQL frontend and query planner

- [ ] Parse SQL into an AST using a maintained parser after a focused parser
  spike; do not route by regular expression.
- [ ] Define the first common SQL subset: `CREATE TABLE`, indexes, `SELECT`,
  `INSERT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, and `ROLLBACK`.
- [ ] Normalize placeholders (`$1`, PostgreSQL/MySQL `?`) to SQLite parameters
  without interpolating values into SQL text.
- [ ] Infer shard keys from predicates and inserted values, including bound
  parameters and multi-row inserts.
- [ ] Plan prepared statements at bind/execute time, not parse time, because a
  routing key may be supplied as a bound parameter.
- [ ] Reject conflicting keys and unroutable writes before execution.
- [ ] Translate a deliberately small set of type names and syntax differences;
  preserve a strict mode that exposes SQLite SQL directly.
- [ ] Implement protocol-neutral prepare/bind/describe/execute lifecycle and a
  bounded per-session prepared-statement cache.
- [ ] Classify statements by read/write/schema/session behavior and block unsafe
  multi-statement combinations.

Exit criterion: the same typed request produces the same plan and result through
the engine regardless of its eventual wire protocol.

### 4. PostgreSQL wire-protocol frontend

- [ ] Run a separate configurable listener, initially
  `--postgres-listen 127.0.0.1:5433`; allow it to be disabled.
- [ ] Spike the current `pgwire` crate against the core interfaces before
  committing to it; `pgwire` 0.40.5 is the leading candidate as of this roadmap.
  Pin the selected pre-1.0 version and keep protocol code behind a BriskDB-owned
  adapter boundary.
- [ ] Support startup, database/user selection, parameter status, clean
  termination, and useful server-version identification.
- [ ] Support simple query flow and extended Parse/Bind/Describe/Execute/Sync,
  including named and unnamed statements/portals, Flush/Close, protocol error
  resynchronization, and portal suspension.
- [ ] Baseline protocol 3.0 and negotiate newer minor versions rather than
  assuming that wire version and server marketing version are identical.
- [ ] Map BriskDB types to PostgreSQL OIDs and support text format first, then
  the binary formats required by tested drivers.
- [ ] Implement `BEGIN`/`COMMIT`/`ROLLBACK`, failed-transaction state, and shard
  pinning. Report PostgreSQL's idle/in-transaction/failed (`I`/`T`/`E`) states
  even where SQLite's native behavior differs.
- [ ] Support cancellation requests and connection cleanup.
- [ ] Add TLS and SCRAM-SHA-256 before non-loopback use; never ship cleartext
  password authentication on an unencrypted listener.
- [ ] Add compatibility shims for `SELECT version()`, common `SHOW` commands,
  and only the catalog queries needed by explicitly tested clients.
- [ ] Test with `psql`, `tokio-postgres`, `psycopg`, and one migration/ORM tool.
- [ ] Stream rows with backpressure and wire PostgreSQL `CancelRequest` to an
  actual SQLite interrupt/cancellation path.

Deferred: `COPY`, replication, `LISTEN/NOTIFY`, large objects, PostgreSQL
extensions, and full `pg_catalog` emulation.

Exit criterion: the test matrix can connect, prepare/bind, perform routed CRUD
inside a single-shard transaction, handle errors, and reconnect cleanly.

### 5. MySQL wire-protocol frontend

- [ ] Run a separate configurable listener, initially
  `--mysql-listen 127.0.0.1:3307`; allow it to be disabled.
- [ ] Spike the current `mysql-mimic` and `opensrv-mysql` crates against the
  same core. `mysql-mimic` 0.9.0 has the broader recent session/prepared-query
  surface; `opensrv-mysql` is an established lower-level alternative. Select
  by conformance tests and isolate either behind a BriskDB-owned adapter.
- [ ] Implement handshake/capability negotiation, logical database selection,
  connection attributes, character set/collation, ping, quit, query, and
  connection-reset commands used by pools.
- [ ] Implement `COM_STMT_PREPARE`, parameter metadata,
  `COM_STMT_EXECUTE`, reset, and close.
- [ ] Map BriskDB types, nulls, status flags, affected rows, generated-key
  behavior, warnings, MySQL error numbers, and SQLSTATE values.
- [ ] Add selected MySQL syntax normalization: backtick identifiers, boolean
  conventions, `LIMIT offset,count`, and documented type aliases.
- [ ] Emulate the small session surface that real drivers issue automatically,
  including `SET NAMES`, selected `SHOW VARIABLES`, and selected `SELECT @@...`
  probes; test each shim rather than inventing a broad fake catalog.
- [ ] Implement transaction state and shard pinning identically to the
  PostgreSQL adapter.
- [ ] Add TLS and a modern authentication path before non-loopback use. Avoid
  compatibility choices that require sending reusable plaintext credentials.
- [ ] Test with the `mysql` CLI, Rust `mysql`, Python Connector/Python, and one
  migration/ORM tool.

Deferred: replication/binlog protocol, stored procedures, multiple result sets,
`LOAD DATA`, full `information_schema`, and broad MySQL dialect emulation.

Exit criterion: both wire frontends pass the same engine behavior suite, with
protocol-specific golden tests only for encoding and state-machine behavior.

### 6. HTTP data and administration APIs

- [ ] Replace the experimental endpoints with a versioned `/v1` contract built
  on the shared session/engine types.
- [ ] Return ordered columns and row arrays so duplicate names and binary values
  round-trip without loss; define JSON encodings for integers, decimals,
  timestamps, and blobs.
- [ ] Separate data-plane and admin-plane routers/listeners.
- [ ] Add endpoints for health, readiness, catalog inspection, migrations,
  shard state, query cancellation, backup, and maintenance.
- [ ] Add request IDs, idempotency keys for eligible writes, pagination/streaming,
  body/result limits, and stable problem-detail errors.
- [ ] Generate and test an OpenAPI document from the implementation.
- [ ] Require authentication and role checks; apply TLS or explicitly document
  trusted reverse-proxy termination.

Exit criterion: the HTTP API is safe to version independently while preserving
the same engine semantics as SQL clients.

### 7. Scatter/gather reads

- [ ] Fan out supported read-only plans with bounded concurrency and deadlines.
- [ ] Push filters, projections, and safe limits into each shard.
- [ ] Implement deterministic k-way merge for `ORDER BY`, followed by global
  `OFFSET`/`LIMIT`.
- [ ] Implement safe partial/final aggregation for `COUNT`, `SUM`, `MIN`, `MAX`,
  and `AVG`; reject unsupported aggregate/window semantics.
- [ ] Define duplicate handling and collation/null-order behavior across shards.
- [ ] Expose query plans and per-shard timing through `EXPLAIN BRISKDB` and
  observability data.
- [ ] Add concurrency, cancellation, slow-shard, and partial-failure tests.

Exit criterion: supported scatter queries match a single SQLite reference
database under differential/property tests.

### 8. Operations, security, and recovery

- [ ] User/role catalog, password hashing, credential rotation, and least-
  privilege authorization for data/schema/admin operations.
- [ ] TLS configuration and reload for every listener; safe non-loopback startup
  defaults.
- [ ] Structured logs, metrics, traces, slow-query log, pool saturation, shard
  skew, WAL size, migration state, and readiness reasons.
- [ ] Coordinated online backup using SQLite backup APIs plus a manifest-defined
  recovery point; document restore and regularly test it.
- [ ] WAL checkpoint policy, disk-full handling, corruption drills, and
  crash/failure-injection tests.
- [ ] Resource governance per user/session/query and defense against oversized
  packets, parameter counts, rows, and SQL text.
- [ ] Configuration file/env/CLI precedence, secret handling, and a config
  validation command.

Exit criterion: an operator can secure, observe, back up, restore, upgrade, and
diagnose BriskDB using tested procedures.

### 9. Online rebalance and production hardening

- [ ] Add physical shards and move selected virtual buckets with a durable state
  machine: copy, catch up, cut over map generation, verify, and retire source.
- [ ] Make clients detect/retry stale routing generations internally.
- [ ] Add a resumable offline reshard tool before attempting online movement.
- [ ] Decide whether cross-shard writes remain unsupported or warrant a durable
  transaction coordinator; do not imply atomicity without crash proofs.
- [ ] Long-running soak, concurrency, filesystem fault, upgrade/downgrade, and
  compatibility suites.
- [ ] Publish performance methodology and results against unsharded SQLite and
  a single-file BriskDB baseline.
- [ ] Stabilize on-disk format and compatibility policy for a `1.0` release.

Exit criterion: storage-format compatibility, operational recovery, and the
supported client matrix have release gates rather than best-effort claims.

## Immediate implementation sequence
The first buildable slices should be small and merge independently:

1. Add protocol-neutral `Value`, `Column`, `Row`, `ResultSet`, and `EngineError`.
2. Change the HTTP adapter to use those types and preserve duplicate columns.
3. Add a bounded per-shard SQLite pool and load/concurrency tests.
4. Define manifest schema v2 and golden routing vectors.
5. Add the virtual-bucket map while test data can still be discarded.
6. Add `Session`, transaction state, and one-shard pinning.
7. Add SQL parsing and plan classification without syntax translation.
8. Infer routing for one-table equality CRUD and reject unroutable writes.
9. Spike PostgreSQL simple-query connectivity end to end.
10. Add PostgreSQL extended query flow, then reuse the same conformance suite
    for the MySQL prepared-statement path.

## Test strategy

- Unit tests for canonical value conversion, routing, planning, error mapping,
  and every protocol state transition.
- Golden tests for hash/key encoding, wire packets, type mappings, SQL rewrite,
  and error responses.
- Differential tests that execute supported SQL against one reference SQLite
  database and BriskDB shards, then compare ordered typed results.
- Property tests for routing stability, key extraction, scatter merge, and
  arbitrary protocol fragmentation.
- Integration tests using real client libraries and CLI programs, not only
  handcrafted packets.
- Failure injection around process termination, partial schema migration,
  busy/locked shards, disk full, malformed packets, cancellation, and slow
  clients.
- Benchmarks report latency percentiles, throughput, connection count, shard
  skew, database/WAL size, synchronous mode, and hardware/filesystem details.

## Decisions to make before the first public release

- Exact supported PostgreSQL and MySQL client/driver versions.
- Whether SQLite SQL or the common translated subset is the default mode.
- Canonical decimal, timestamp/time-zone, unsigned integer, JSON, and blob
  semantics across all three interfaces.
- Authentication and authorization storage model.
- Default durability mode and checkpoint policy.
- Whether logical databases share one process/root or one BriskDB process owns
  exactly one logical database.
- Compatibility policy for manifest, shard files, routing hash, and wire
  behavior across upgrades.

## Protocol references and candidate libraries

- PostgreSQL frontend/backend protocol:
  <https://www.postgresql.org/docs/current/protocol.html>
- PostgreSQL message flow:
  <https://www.postgresql.org/docs/current/protocol-flow.html>
- MySQL client/server protocol:
  <https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html>
- PostgreSQL server library candidate: <https://crates.io/crates/pgwire>
- MySQL server library candidates: <https://crates.io/crates/mysql-mimic> and
  <https://crates.io/crates/opensrv-mysql>
- SQL parser candidate: <https://crates.io/crates/sqlparser>

Library choices remain spike outcomes, not permanent architecture. BriskDB must
own the adapter interfaces and conformance tests so a protocol crate can be
replaced without rewriting the engine.
