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

### Remote SQLite addon

Tracked in [#349](https://github.com/schapman1974/briskdb/issues/349).
The read-only preview ships an original native virtual-table module and remote
client in the Python wheel. Standard `sqlite3.Connection` queries allowlisted
BriskDB tables through a dedicated authenticated connector over the same Engine.
Registered tables use logical placement; legacy databases require an explicit
single-shard routing scope. HTTPS deployment currently uses a trusted reverse
proxy in front of the loopback-only dedicated listener.

The first checkpoint covers bounded full scans, schema/server fencing, typed
values, local joins and real wheel-loading tests. It deliberately does not claim
writable transactions, stable remote rowids, shared snapshots, paged cursors or
query pushdown. Next checkpoints add retained read sessions/cursors and safe
pushdown, then mutation/transaction mapping with rollback, disconnect and
unknown-commit-outcome tests. No write capability will be enabled solely because
SQLite exposes an `xUpdate` callback. The [Python API](python/API.md#remote-sqlite-addon)
states the current bounds and unsupported behavior.

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
- General multi-shard SQL writes are rejected unless an operation explicitly
  uses a documented operation-specific coordinator. Mongo/document batches have
  their own non-atomic per-input/per-shard commit contract, not a distributed transaction.
- Scatter reads merge committed results from several shards but do not provide
  a cross-file atomic snapshot in the first release.
- Schema changes are versioned, journaled, and applied to every shard. A
  partially completed migration is visible and resumable.
- The routing hash algorithm, key encoding, virtual-bucket count, and shard map
  are persisted and versioned in the manifest.

### Cross-shard transaction policy — alpha decision (#74)

Retain explicit rejection of general multi-file transactions. The current
protocol-neutral engine already pins SQL transactions to one shard and rejects
different-shard work before it mutates data. This policy applies to embedded
and wire callers alike; adapters must not bypass it or simulate rollback with
compensating writes.

Document batches are a separate, explicitly non-atomic command surface: inserts
commit per input, and update-many/delete-many commit per targeted shard. A failed
later input/shard can leave earlier commits intact. A successful global unique
check and its writer fence protect key ownership, not whole-batch atomicity;
transient collisions can reject an otherwise unique eventual image. Schema and
specialized global-index coordinators likewise do not authorize arbitrary
cross-shard transactions. See the [document commit boundaries](docs/DOCUMENT_ENGINE.md).

Cancellation, disconnect or process death after commit but before delivery can
leave an unknown outcome. Mongo logical sessions/retryable writes are not
advertised; document request IDs are correlation IDs, not idempotency receipts.
Reconcile by stable document identity before retrying uncertain work. Eligible
SQL receipt-backed writes retain only their existing, separately documented
idempotency contract. No cross-shard snapshot, generic exactly-once behavior,
new on-disk format, or new transaction capability is introduced by this decision.

A future general coordinator requires a separate design and explicit capability:
durable intent/decision records, fencing, bounded prepared resources, deterministic
recovery of every participant/decision boundary, unknown-outcome reconciliation,
retry deduplication, upgrade/backup rules and crash proofs. It is not a prerequisite
for the documented non-atomic Mongo command surface. #183's acceptance covers
that surface's failure, retry, concurrency and process-death recovery boundaries;
it does not implement or claim a general cross-shard coordinator.

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
- `QueryPlan`: single shard, scatter read, schema migration, manifest operation,
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
- Applications may use their own UUID/ULID-style values. Registered Sharded
  tables may instead opt into a versioned generated-integer policy; the exact
  single-row SQL and non-gapless allocation contract is documented in
  [`docs/GENERATED_KEYS.md`](docs/GENERATED_KEYS.md).
- The no-fork virtual-table facade remains experimental after its issue #131
  rollout review. Its frozen pass criteria, evidence, and hold decision are in
  [`docs/VTAB_ROLLOUT.md`](docs/VTAB_ROLLOUT.md).

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
- [x] Implement a crash-resumable schema migration journal instead of the
  previous unjournaled broadcast behavior.
- [x] Add checksums/integrity checks and explicit states for degraded or
  partially migrated databases.

Exit criterion: restarts and upgrades cannot silently change routing, and an
interrupted schema migration can be diagnosed and resumed.

### 3. SQL frontend and query planner

- [x] Parse SQL into an AST using a maintained parser after a focused parser
  spike; do not route by regular expression.
- [x] Define the first common SQL subset: `CREATE TABLE`, indexes, `SELECT`,
  `INSERT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, and `ROLLBACK`.
- [x] Normalize placeholders (`$1`, PostgreSQL/MySQL `?`) to SQLite parameters
  without interpolating values into SQL text.
- [x] Infer shard keys from predicates and inserted values, including bound
  parameters and multi-row inserts.
- [x] Plan prepared statements at bind/execute time, not parse time, because a
  routing key may be supplied as a bound parameter.
- [x] Reject conflicting keys and unroutable writes before execution.
- [x] Translate a deliberately small set of type names and syntax differences;
  preserve a strict mode that exposes SQLite SQL directly.
- [x] Implement protocol-neutral prepare/bind/describe/execute lifecycle and a
  bounded per-session prepared-statement cache.
- [x] Classify statements by read/write/schema/session behavior and block unsafe
  multi-statement combinations.
- [x] Recognize the documented SQLite `AUTOINCREMENT`, MySQL `AUTO_INCREMENT`,
  and PostgreSQL `BIGSERIAL`/identity declarations structurally and translate
  them to one canonical physical SQLite declaration.
- [x] Plan one omitted generated-key row through the shared engine, capture its
  ID in the committing operation, expose the protocol-neutral and HTTP result,
  and reject omitted-key multi-row inserts before mutation.
- [x] Add one crash-resumable DDL coordinator that durably binds the exact
  logical generated declaration to its canonical physical migration and
  catalog-provisioning identity and returns all three identities plus the
  published table ID through `Database::apply_generated_table_ddl`.

Exit criterion: the same typed request produces the same plan and result through
the engine regardless of its eventual wire protocol.

### 4. PostgreSQL wire-protocol frontend

- [x] Run a separate configurable listener, initially
  `--postgres-listen 127.0.0.1:5433`; allow it to be disabled.
- [x] Spike `pgwire` against the core interfaces and select exact version
  `0.36.3`, the newest release compatible with BriskDB's Rust 1.85 baseline.
  Enable only `server-api`, pin the pre-1.0 version, and keep its types behind
  the BriskDB-owned `protocol::postgres` adapter boundary.
- [x] Support protocol 3.0 startup, exact logical database/user selection,
  BriskDB-owned parameter status, clean termination and session cleanup, and
  useful `<package-version>-briskdb` server identification on loopback.
- [x] Support simple query flow and extended Parse/Bind/Describe/Execute/Sync,
  including named and unnamed statements/portals, Flush/Close, protocol error
  resynchronization, and portal suspension.
- [x] Negotiate explicitly supported newer protocol minor versions from the
  exact 3.0 baseline rather than conflating protocol and server versions.
- [x] Map BriskDB types to PostgreSQL OIDs and support text format first, then
  the binary formats required by tested drivers (issue #33, including the
  generated-key result contract).
- [x] Implement `BEGIN`/`COMMIT`/`ROLLBACK`, failed-transaction state, and shard
  pinning. Report PostgreSQL's idle/in-transaction/failed (`I`/`T`/`E`) states
  even where SQLite's native behavior differs.
- [x] Support `CancelRequest`/backend keys and wire cancellation to the core.
- [x] Add TLS and SCRAM-SHA-256 before non-loopback use; never ship cleartext
  password authentication on an unencrypted listener.
- [x] Add compatibility shims for `SELECT version()`, common `SHOW` commands,
  and only the catalog queries needed by explicitly tested clients.
- [x] Test with `psql`, `tokio-postgres`, `psycopg`, and SQLAlchemy ORM through
  a live, release-gating client matrix.
- [x] Stream rows with backpressure while preserving the PostgreSQL
  `CancelRequest` to SQLite interrupt path.

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
  behavior, warnings, MySQL error numbers, and SQLSTATE values (issue #44).
- [ ] Apply the implemented selected MySQL normalization for backtick
  identifiers, Boolean conventions, `LIMIT offset,count`, and documented type
  aliases to the MySQL wire request lifecycle.
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

- [x] Serve an early embedded, read-only data explorer at `/admin` with a
  temporary fixed login and bounded per-physical-shard row pages (issue #106),
  including exact large-integer display and ordered login responses (issue
  #110).
  This preview does not complete the versioned admin API, listener separation,
  authentication/role, pagination, or scatter/gather items below.
- [x] Replace the experimental endpoints with a versioned `/v1` contract built
  on the shared session/engine types, with discovery, strict envelopes, and
  fixed transport errors; see [the HTTP contract](docs/HTTP_API.md).
- [x] Preserve ordered columns and positional row arrays and add the explicitly
  selected `lossless-json-v1` encoding for integers, decimals, binary64 bits,
  blobs, and invalid SQLite text (issue #51). `legacy-json-v1` remains the
  default. Relational timestamps remain application-defined `Text` or `Int64`
  because the shared SQL value system has no timestamp type or storage policy;
  [#298](https://github.com/schapman1974/briskdb/issues/298) tracks that shared
  contract.
- [x] Separate data-plane and admin-plane routers/listeners, preserving
  `--listen` for data on `127.0.0.1:7654` and moving operator and browser routes
  to the optional loopback administration listener on `127.0.0.1:7655`
  (issue #52); see [the listener contract](docs/HTTP_LISTENERS.md).
- [x] Add endpoints for health, readiness, catalog inspection, migrations,
  shard state, query cancellation, backup, and maintenance.
- [x] Add request IDs, durable idempotency keys for eligible writes,
  request-local result limits, and bounded HTTP row streaming with stable
  problem-detail errors (issue #54). Retained pagination and global SQL ordering
  remain in Phase 7 issues #58 and #59.
- [x] Generate and test the deterministic code-first OpenAPI 3.1 document for
  the exact 17-path, 28-operation HTTP v1 machine surface, including explicit
  listener ownership and shipped Cargo/native/Debian artifact contracts; see
  [the OpenAPI contract](docs/OPENAPI.md) (issue #55).
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
- [x] [#74](https://github.com/schapman1974/briskdb/issues/74) — retain the existing
  single-shard explicit transaction boundary for the alpha. No general distributed
  transaction coordinator is enabled. Mongo's non-atomic fault/retry acceptance
  is recorded independently in #183; filesystem/power-loss soak remains separate.
- [ ] Long-running soak, concurrency, filesystem fault, upgrade/downgrade, and
  compatibility suites.
- [ ] Publish performance methodology and results against unsharded SQLite and
  a single-file BriskDB baseline.
- [ ] Stabilize on-disk format and compatibility policy for a `1.0` release.

Exit criterion: storage-format compatibility, operational recovery, and the
supported client matrix have release gates rather than best-effort claims.

## Prioritized implementation efforts

Each effort should remain a sequence of small, independently tested and merged
changes. Complete them in this order unless a correctness or release blocker
requires an earlier dependency:

1. [x] **Finish the SQLite facade and shard-safe generated IDs.** Keep ordinary
   SQLite WAL files, version the allocation policies, support native shard
   ranges and durable hi/lo leases, and retain the general virtual-table path
   behind its measured rollout gate.
2. [x] **Complete PostgreSQL support.** Finish the simple and extended query
   lifecycle, type mapping, transactions, cancellation, compatibility shims,
   and real-client conformance tests.
3. [x] **Implement global indexes, uniqueness, and value allocation.** Add the
   protocol-neutral authority and routing metadata needed for cross-shard
   constraints, collision-free allocation, and exact indexed shard targeting.
   Track the rollout in [#225](https://github.com/schapman1974/briskdb/issues/225):

   - [x] [#226](https://github.com/schapman1974/briskdb/issues/226) — baseline and regression suite
   - [x] [#227](https://github.com/schapman1974/briskdb/issues/227) — canonical key encoding
   - [x] [#228](https://github.com/schapman1974/briskdb/issues/228) — catalog lifecycle and compatibility fencing
   - [x] [#229](https://github.com/schapman1974/briskdb/issues/229) — storage-topology prototype
   - [x] [#230](https://github.com/schapman1974/briskdb/issues/230) — offline index construction
   - [x] [#231](https://github.com/schapman1974/briskdb/issues/231) — validation, repair, and rebuild
   - [x] [#232](https://github.com/schapman1974/briskdb/issues/232) — unique reservations and global allocation
   - [x] [#233](https://github.com/schapman1974/briskdb/issues/233) — authoritative write maintenance
   - [x] [#234](https://github.com/schapman1974/briskdb/issues/234) — exact indexed routing
   - [x] [#235](https://github.com/schapman1974/briskdb/issues/235) — candidate verification and repair
   - [x] [#236](https://github.com/schapman1974/briskdb/issues/236) — transactional shard outboxes
   - [x] [#237](https://github.com/schapman1974/briskdb/issues/237) — asynchronous indexing and watermarks
   - [x] [#238](https://github.com/schapman1974/briskdb/issues/238) — Bloom/min-max shard summaries
   - [x] [#239](https://github.com/schapman1974/briskdb/issues/239) — production/release gates
4. [x] **Finish the embedded Rust API.** Make sessions, transactions,
   cancellation, concurrency, document commands, and shutdown safe for a host
   process.
5. [x] **Finish the Python API and wheels.** Provide matching synchronous and
   asynchronous APIs, lossless values and exceptions, typing, documentation,
   and tested Linux/macOS ARM64/x86-64 wheels.
6. [ ] **Complete HTTP administration and the data browser.** Version the API,
   separate administration and data planes, add operational endpoints and
   pagination, and preserve protocol-neutral engine behavior.
7. [ ] **Complete cross-shard query execution.** Add bounded fan-out,
   pushdown, deterministic global ordering and pagination, supported
   aggregation, and explainable per-shard execution.
8. [ ] **Complete security, backup, observability, and production hardening.**
   Add identity and authorization, TLS, resource governance, metrics and
   tracing, online backup and restore, fault testing, and compatibility gates.
9. [ ] **Complete serverless support.** Define atomic snapshot storage,
   ephemeral-runtime lifecycle adapters, warm reuse, and fenced writer
   guarantees.
10. [ ] **Implement native MongoDB protocol compatibility with TinyMongo
    parity.** Build the document engine, BSON and wire layers, query and write
    semantics, indexes, aggregation, sharding behavior, and differential
    compatibility suites.

    - [x] [#161](https://github.com/schapman1974/briskdb/issues/161) — freeze
      the TinyMongo v1.3.0 contract, capability inventory, reference results,
      and strict differential report
    - [x] [#164](https://github.com/schapman1974/briskdb/issues/164) — canonical
      BSON model, codec, ordering, and versioned semantic keys; see
      [`docs/BSON.md`](docs/BSON.md)
    - [x] [#163](https://github.com/schapman1974/briskdb/issues/163) — versioned
      document catalog, sharded BSON storage, and atomic TinyMongo SQLite
      import; see [`docs/DOCUMENT_STORAGE.md`](docs/DOCUMENT_STORAGE.md)
    - [x] [#162](https://github.com/schapman1974/briskdb/issues/162) —
      protocol-neutral document engine with controlled point/scatter commands;
      see [`docs/DOCUMENT_ENGINE.md`](docs/DOCUMENT_ENGINE.md)
    - [x] [#191](https://github.com/schapman1974/briskdb/issues/191) — thin
      embedded Rust facade over protocol-neutral document commands
    - [x] [#198](https://github.com/schapman1974/briskdb/issues/198) — Python
      BSON conversions and document API
    - [x] [#200](https://github.com/schapman1974/briskdb/issues/200) — release
      wheels and Mongo smoke coverage
    - [ ] [#165](https://github.com/schapman1974/briskdb/issues/165) — Mongo
      listener and framing. In progress: opt-in `mongo` feature, bounded envelope
      and BSON body/document-sequence parsing, CRC-32C verification, and legacy
      handshake parsing. Host-owned `MongoServer::start` explicitly binds a
      loopback-only discovery listener; defaults still enable no Mongo socket.
      Bootstrap budgets are 1 MiB messages, 512 KiB BSON documents, 4 MiB decoded
      heap per document, eight connections, bounded sequences/batches, and an
      absolute 15-second frame-read deadline. Shutdown joins connection/parser
      work without closing the borrowed engine. Opt-in `--mongo-listen` /
      `BRISKDB_MONGO_LISTEN`, `server::run_with_mongo`, and
      `AttachedServer::start_with_mongo` now share the daemon/attached listener
      lifecycle, with passive all-listener binding, fail-closed startup,
      actual bound Mongo addresses, signal drain, and restart coverage. Existing
      config struct shapes and disabled defaults remain unchanged. Python's
      synchronous/async `db.serve(mongo=...)` now exposes the same listener and
      `mongo_address`, requires explicit document enablement, and shares native
      document storage and attached-server cleanup. It coexists with PostgreSQL
      TLS/SCRAM and SQLite remote without inheriting their authentication or
      relaxing Mongo's loopback boundary. Negotiated
      zlib now has bounded compressed/expanded envelopes, strict stream and
      original CRC validation, malformed/bomb/property tests, compressed replies
      with safe plain fallback, and real sync/async PyMongo restart coverage.
      Broader codec coverage and expanded resource/fuzz gates
      remain; this is not a completed Mongo compatibility milestone.
    - [x] [#170](https://github.com/schapman1974/briskdb/issues/170) and
      [#169](https://github.com/schapman1974/briskdb/issues/169) — standalone
      hello/isMaster, ping, buildInfo, correlated replies, and initial strict
      option validation implemented. No sessions, retryable writes, replication,
      or change streams are advertised; compression offers negotiate only zlib.
      Required CI smoke exercises
      real PyMongo 4.17.0 sync/async discovery, pooling, reconnect, and explicit
      unsupported-command errors (#184).
      Rust hosts can inspect bounded active-connection metadata through
      `MongoServer::client_metadata()`: only a closed driver-family enum and
      numeric version, never application/platform/OS/environment strings.
      First successful handshakes freeze the record; disconnect, abort and
      listener close remove it. Raw-socket, redaction, bounds/unwind and real
      sync/async driver observations cover this final discovery checkpoint.
    - [x] [#184](https://github.com/schapman1974/briskdb/issues/184) — the pinned
      stock PyMongo 4.17.0 sync/async matrix now includes deliberate in-flight
      async getMore cancellation, deterministic reply-delay fault injection,
      native cursor/socket cleanup before client close, and same-client pool
      replacement/reuse against fresh and reopened storage. No driver/response
      rewriting or cancelled-write outcome guarantee. The explicit local test is
      wired into the existing real-driver CI gate for final CI restoration.
    - [x] [#388](https://github.com/schapman1974/briskdb/issues/388) — Python
      testing wheels now provide TinyMongo-style `briskdb.patch()` and direct
      `MongoClient`/`AsyncMongoClient` imports backed by real pinned PyMongo and
      the shared Rust engine. Scopes own temporary SQLite or persistent roots,
      restore constructors in nested order, reject cross-task/thread overlap,
      and drain startup/cleanup on cancellation. Same-path managed clients share
      one engine. A private Mongo-only loopback listener opens no HTTP/admin
      ports; supplied remote hosts/credentials are never connection targets.
      Existing client aliases are outside the patch boundary. Async clients
      require async scopes. Optional dependency, typing, installed-wheel tests
      and README examples are part of this checkpoint, not new Mongo semantics
      or alternative TinyMongo backend support.
    - [ ] [#181](https://github.com/schapman1974/briskdb/issues/181) — actual
      Beanie 2.1.0 and MongoEngine 0.29.3 CRUD application bodies now run unchanged
      from the locked TinyMongo source against stock PyMongo 4.17.0 and a real
      four-shard listener. Only connection construction changes. Source hashes,
      exact executed-case checks, pinned dependencies, process deadlines and
      before/after-restart runs gate CI separately from the frozen command corpus.
      Persisted data and ODM-created index metadata are checked on reopen.
      The full unchanged Talk Python wire-target contract file now runs all
      58 sync/async cases through its original fixtures and adapters, changing
      only the connection URI. Exact identities/outcomes/target metadata and
      source hashes gate both fresh and reopened engines; an independent BSON/
      index sentinel verifies the same root because fixtures clean their own DBs.
      Separate JSON/JUnit reports reject omissions, skips and stale successes.
      These cover baseline ODM bodies and application-derived wire contracts,
      not broader ODM features, full app deployments or user-supplied large apps.
    - [ ] Initial wire data slice for [#173](https://github.com/schapman1974/briskdb/issues/173),
      [#172](https://github.com/schapman1974/briskdb/issues/172), and
      [#180](https://github.com/schapman1974/briskdb/issues/180): single-document
      insert and literal exact-ID find now use the same enabled document engine
      as embedded Rust/Python. First writes provision collections through the
      engine, duplicate IDs map to `DuplicateKeyError`, and read replies use
      engine-owned cursors. Tests cover BSON fidelity, numeric-equivalent IDs,
      cross-interface access, restart, host enablement, one-way writes, and
      response limits. Missing collections read empty without creating metadata.
      Additional update operators,
      and broader database options remain open.
    - [x] [#180](https://github.com/schapman1974/briskdb/issues/180) — bounded
      `_id: {$in: [...]}` literal lists now prune to distinct canonical-ID owners
      for find/getMore, legacy count, distinct and single/multi mutations. The
      existing matcher, natural-order merge, sorting, global pagination, cursor
      budgets and shard-local commit semantics remain authoritative. Pool-counter
      tests prove actual physical-shard access, numeric aliases/BSON IDs and
      restart behavior. Empty/oversized/regex lists and unproven filter shapes
      keep scans. Aggregation now also routes a safe leading exact-ID/list match,
      including PyMongo `count_documents()`, while retaining the complete pipeline
      and its input/work accounting. Matches after transforms/skip/limit do not
      establish routes. Compound/positive-AND exact-ID and list constraints now
      intersect owner sets; OR unions them only when every alternative is bounded,
      with at most 1024 canonical-ID visits across the filter. Full predicates
      still govern single-owner reads, mutations and upsert conflicts. Negations,
      dotted IDs, unbounded OR branches and empty owner intersections keep safe
      fallback behavior. Thirty-eight frozen BSON-ID vectors now pin v1 bytes,
      hash prefixes and initial ownership for every supported shard count.
      Typed reads/updates/deletes prove physical access on 3/8/64-shard roots;
      exact record/key/order/checksum/owner images survive a validated synthetic
      v17-schema upgrade and reopen. This closes the current v1 canonical-ID
      routing slice, not general candidate planning (#178), online resharding,
      or archived-release binary upgrade certification. No routing format or
      feature behavior changes in this test-only checkpoint.
    - [x] [#173](https://github.com/schapman1974/briskdb/issues/173) — ordered and
      unordered insert batches use canonical-ID routing and contiguous
      single-shard worker grouping. Missing IDs receive ObjectIds; explicit
      nulls are preserved. Duplicate failures retain input indices and partial
      success counts. Preflight validation and result budgets precede document
      writes, with no cross-shard atomicity promise. Tests cover BSON types,
      direct zero-timestamp normalization, raw server-generated IDs, restart,
      concurrent duplicates, driver batch splitting, and boundary rejection.
    - [ ] [#171](https://github.com/schapman1974/briskdb/issues/171) — shared
      `replace_one` now selects one match and atomically replaces its shard-local
      record while preserving natural order and the original `_id` representation.
      Exact stored BSON bytes determine matched/modified counts; top-level zero
      timestamps are stamped, nested values are preserved. Rust, native sync/async
      Python, and ordered/unordered wire replacement batches share this path.
      Result/post-image limits and immutable-ID checks precede commit. No global
      snapshot is claimed. Replacement upserts now infer direct/sole-equality IDs,
      preserve null and numeric representations, or generate ObjectIds. Same-ID
      races recheck under the target shard lock; result/post-image limits and
      returned-ID depth precede commit. Wire batches preflight aggregate reply
      budgets, preserve indexed upsert metadata/duplicate errors, and create
      missing namespaces; native calls require existing collections. Both expose
      unambiguous upsert presence, with native counts 0/0 on insertion.
      Mixed batches, deep/large IDs, one-way writes, concurrency, and restart
      are covered. Shared `update_one`/`update_many` support
      `$set`/`$unset`/`$min`/`$max`/`$pop`/`$rename`/`$addToSet`/`$pullAll`/`$push`/`$pull`/`$inc`
      across all three adapters, with object/array paths, eager conflict checks,
      immutable IDs, exact modified counts, bounded growth, and literal timestamps.
      Both scopes now support operator upserts: positive direct/`$eq`/`$and`
      equalities seed strict dotted object paths; overlapping equalities fail
      with code 54. Operators run before missing-ID generation, can provide an
      unbound ID, and retain literal zero timestamps. No advanced predicate
      simplification or global snapshot is promised; global uniqueness requires a
      Ready unique index. Result/depth limits,
      target-shard rechecks (all matches for many), and rollback-certified wire
      errors share the replacement path. 3,544 source-locked upsert executions
      cover common frozen behavior; independent tests cover strict inference,
      null/generated/large IDs, concurrent counters, batches and restart.
      30,489 source-locked update cases supplement transaction/wire tests: 4,008
      object-only set/unset, 4,719 min/max, 3,078 pop/rename, and 4,440 non-ID
      membership cases (object-only add-to-set paths), 4,459 push cases, and 5,573
      pull cases on non-ID object/array paths. Legacy push/pull/membership helpers
      restore changed IDs; add-to-set also overwrites scalar parents. Independent
      tests verify BriskDB's stricter IDs/paths without modifying frozen allowances.
      Add-to-set supports `$each`, retaining existing duplicates/types; pull-all
      removes all literal BSON-equal values. Equality work/growth are bounded,
      and concurrent membership updates avoid duplicate additions/lost values.
      Push supports literal values and `$each`/`$position`/`$sort`/`$slice` in fixed
      insertion-sort-slice order, stable whole-BSON/compound sorting, integral
      numeric boundary clamping, and bounded temporary growth/sort scratch/work.
      Compound sorting follows frozen document-only selectors, not query-sort
      array selection. Concurrent pushes preserve every value; final post-images,
      projected before/after replies, atomic failures, and restart are tested.
      Pull borrows the shared matcher for literal, field-predicate, and document
      conditions, with ordinary embedded-ID semantics and eager update-specific
      errors. Missing paths are no-ops; stable removal, comparison/regex work,
      path allocations, AST/program retention, and cancellation remain bounded.
      Increment completes the eleven planned operators: Int32 promotion, retained
      Int64 width, atomic overflow rejection, Double/Decimal promotion, exact
      missing operands, rounded no-ops, and executed NaN modification counts.
      Another 4,212 oracle cases cover common frozen numeric/object-path behavior;
      legacy Int64 shrinking, unencodable overflow, missing/signed-zero differences,
      and stricter paths/IDs have independent tests. No frozen allowances change.
      Concurrent counters, images, rollback, request controls and restart are tested.
      Min/max use whole BSON order, preserve equal stored types, distinguish
      missing array slots from null, and bound comparison work even on no-ops.
      Pop supports front/back removal and numeric paths; rename moves fields
      without array traversal. Missing pop targets/rename sources are no-ops; both rename paths
      participate in eager conflicts. Typed operand/path/ID failures and budgets
      precede SQL. Concurrent consumers return each array element once.
      Update-many streams one transaction per shard: a failing shard rolls back,
      earlier commits survive. Explicit rollback with zero earlier modifications
      now certifies validation failures for indexed wire write errors and
      unordered continuation. Partial/uncertain or operational failures still
      abort without fabricated counts. First-shard provisional writes, earlier
      no-op shards, partial commits, ordered/unordered behavior, restart,
      cancellation/task-abort, and continued-session tests cover
      this boundary. Find-and-modify upserts now share the same synthesis and
      recheck path, with explicit inserted IDs and optional before/after images.
      Ready secondary entries now follow the same record transaction, with
      post-image validation and cross-shard Ready-unique enforcement. #183 records
      the chosen shard-local bulk policy; broader corpus acceptance remains #186.
    - [ ] [#175](https://github.com/schapman1974/briskdb/issues/175) — filtered
      delete-one/many now use the shared matcher across Rust, native sync/async
      Python, and Mongo wire. Exact IDs stay single-shard; delete-one rechecks
      the earliest natural-order candidate under a write lock. Delete-many
      streams one shard-local transaction at a time; later failure/cancellation
      preserves earlier commits, with no global snapshot/atomicity claim.
      Wire batches preserve ordered/unordered selector errors and counts;
      missing collections return zero. Find-one-and-delete now returns a
      projected pre-image through native sync/async Python and wire
      findAndModify remove. Sort uses original fields and natural-order ties;
      the selected shard reselects under its write lock. Exact output budgets
      (including wire BSON bounds) are validated before mutation. Concurrent
      consumers, runtime sort failures, deadline rollback, and restart are
      covered. Find-one-and-replace now uses the same shared path with
      projected before/after images, sort, immutable-ID validation, and exact
      return/post-image preflight before commit. Find-one-and-update now shares
      this path for all eleven supported field/array operators, preserving untouched fields and supporting
      sorted before/after images across Rust, native Python, and wire clients.
      Concurrent consumers, no-op/no-match/projected-empty replies, depth/size
      rejection before writes, cancellation, and restart are tested. Both forms
      now support upserts with null before-images, projected after-images, exact
      inserted-ID metadata (including null), and combined ID/image preflight.
      Concurrent same-ID insertion/update images are atomic; native sync/async
      Python and wire replies preserve inserted-versus-matched distinctions.
      All eleven planned operators and secondary-index validation are implemented;
      indexed selection, transactional entry maintenance and Ready-unique conflicts
      share the same recheck path. Broader compatibility acceptance remains.
    - [x] [#174](https://github.com/schapman1974/briskdb/issues/174) — shared
      index-definition validation now checks ordered distinct paths and numeric
      ascending/descending directions on controlled workers before catalog writes.
      Missing names are generated deterministically; names/specifications/fields
      and path depth are bounded. Native sync/async Python accepts optional names.
      Equivalent declarations are idempotent without rewriting legacy numeric
      direction bytes. 64 source-locked valid ascending definitions verify exact
      names, key order, flags and pending metadata after restart. Independent tests
      cover descending/numeric aliases, controls and rejection atomicity. All
      native declarations start pending; combined create/build publishes Ready
      indexes, and Ready unique indexes enforce constraints across shards.
      The shared index-key generator now supplies bounded BSON-aware equality
      tuples, one-level multikey deduplication, missing/null and empty-array
      identities, and sparse/partial membership. 7,201 source-locked cases check
      equality partitions, ordering and errors across 29,370 document evaluations.
      Generated keys now round-trip through the versioned, bounded `BDIK` tuple
      codec before oracle comparisons. Fixed bytes, corrupt/future frames,
      arbitrary inputs, cancellation and resource limits have independent tests.
      Durable root-wide index IDs now survive reopening and committed namespace
      drops without reuse. The version-17 manifest migration preserves exact
      legacy/import metadata; checksummed coverage, allocator exhaustion and
      rollback are tested. IDs do not change Python/wire metadata shapes.
      Rust and native sync/async Python can now drop one pending declaration by
      exact name. Built-in protection, pre-commit controls/budgets, concurrent
      drops, crash recovery and ID non-reuse are covered. Built-index drops use
      the recoverable path below; wire selectors use the separate exclusive path below.
      Native Rust and sync/async Python declarations now accept sparse/partial
      options through the shared eager predicate validator. Exact membership
      metadata uses the existing import envelope, preserves IDs across restart,
      and has a combined size bound. Ordinary encodings are unchanged; unknown
      legacy envelopes remain opaque/readable. These declarations do not scan
      records or activate indexes. Document insert/replace/delete primitives now
      require active caller-owned transactions, including explicit per-input
      transactions for inserts and exact-ID deletes. Duplicate batch continuation
      requires successful rollback; controls, transaction failures and process
      crashes verify the committed-prefix boundary. No physical index is activated.
      Collection-wide preparation now compiles pending definitions and generates
      collection/index-scoped BDIK frames under shared work, memory and key-count
      bounds. Unknown definitions, late index failures and cancellation discard
      the whole result; input and catalog bytes remain unchanged. Reopen tests
      preserve IDs/options, and pending uniqueness still does not constrain writes.
      This is preflight only, not physical authority or a freshness guarantee.
      The version-18 storage upgrade now installs exact empty entry tables and
      by-record indexes under a downgrade fence and checksummed per-shard journal.
      Crash/reopen coverage preserves record bytes, declarations and permanent IDs;
      Ready roots reject missing/malformed schemas or unexpected entries. Namespace
      provisioning/deletion owns these tables; ordinary SQL cannot access them.
      This prepares physical storage without activating or populating an index.
      Version 19 adds explicit native Rust/sync/async Python non-unique builds:
      sole-process/exclusive schema admission, combined active-plus-candidate
      preparation bounds, sealed per-shard progress and atomic Ready publication.
      Every record write maintains entries in its own transaction; checksums bind
      exact records and startup verifies full coverage without silent repair.
      Process-exit tests cover build/abort commit boundaries. Interrupted builds
      reopen as pending with only their derived entries removed. Opaque/unique
      pending declarations stay non-enforcing. Ready equality reads are described
      below; global uniqueness and broader planner use remain open.
      Exact native `DropIndex`/sync/async `drop_index` now removes built indexes
      with version-19 Drop intent, sealed per-shard cleanup and restart completion.
      BSON, surviving Ready entries and permanent allocation history are preserved;
      the compiled/name cache is published under exclusive schema admission.
      Pending drops keep their concurrent metadata-only path. Commit-boundary
      crashes and cancellation after intent verify recovery without rollback claims.
      Ready-index metadata now pages through native Rust/sync/async Python and
      Mongo `listIndexes` / PyMongo `list_indexes` / `index_information`.
      Built-in-first/name ordering and exact sparse/partial options match the
      source-locked client; pending declarations remain visible only through the
      existing native catalog API. Cursors retain bounded identity/name state,
      exclude newly allocated IDs, invalidate on collection recreation and share
      byte/row/time/ownership limits. Reopen and real-driver tests cover both APIs.
      Combined native `CreateBuiltIndex` / sync/async `create_built_index` now
      preflights, declares and builds a non-unique index in one exclusive
      operation, with Ready counts for future wire responses. A new declaration
      is coupled to v19 cleanup intent until activation; interrupted creation
      removes that declaration on reopen, while preexisting Pending declarations
      survive aborted builds. Crash/recovery and admitted-cancellation tests
      preserve records, surviving entries and nonreused index identities.
      Native `CreateIndexes` now powers Mongo `createIndexes` and sync/async
      PyMongo `create_index` / `create_indexes`: bounded eager batch validation,
      sole-process admission retained across entries, actual Ready counts,
      built-in ID no-ops and source-compatible 85/86 name conflicts. Completed
      prefixes survive runtime failures; unfinished new entries are removed on
      reopen without changing earlier indexes or records. Real-driver restart,
      raw malformed-batch, source-locked metadata and crash-boundary tests cover
      this path. Whole-bulk unique post-image parity, broader planner acceleration and
      broader IndexModel warning/degradation compatibility remain open.
      Native `DropIndexes` now powers Mongo/PyMongo named, unambiguous legacy-field
      and wildcard removal under one sole-process/exclusive admission. It resolves
      selection/counts before mutation, protects the built-in ID index, removes
      both Ready entries and Pending definitions, and preserves BSON/allocator
      history. Completed prefixes survive interruption; only the admitted drop is
      finished on reopen, not later unstarted indexes. Raw-wire, sync/async driver,
      locked metadata/alias, cancellation and per-index crash tests cover this path.
      Name arrays/key-pattern selectors and ambiguous-alias parity remain open.
      Ready non-unique equality candidates now serve complete supported scalar
      tuples from direct/positive-conjunctive filters, with authoritative matching
      after bound SQLite probes. Natural paging, checksummed entry/record binding,
      write maintenance and index churn across cursor requests are covered.
      Unproven partial indexes, sparse all-null tuples and unsupported/incomplete shapes
      fall back to scans. Source-locked comparisons cover 201,349 matcher
      evaluations with no excluded true matches. #178's additional predicate
      proofs, diagnostics and benchmark acceptance are described below;
      broader index API compatibility remains #174.
      Necessary positive `$in` lists now derive bounded complete compound tuples:
      at most 128 scalar members per list, 128 distinct tuples and 1 MiB of encoded
      keys per probe. Existing equality probes keep priority. Bound SQL candidates
      include non-unique fallback records and deduplicate multikey matches before
      pagination; the full matcher still governs each read/mutation. Unproven partial,
      regex/unsupported, empty/oversized and possible sparse all-null probes retain
      scans. Native scan differentials, typed candidate proofs, physical selection,
      checksum, index churn and restart checks cover the extension.
      Necessary positive `$exists: true` clauses now select current non-partial
      sparse entries after finite key probes. Any indexed path can prove compound
      sparse membership; null/empty arrays and non-unique fallback entries stay
      included. Grouped natural-order candidates retain full-matcher checks,
      checksum validation and per-request authority. Logical-negation/alternative and
      unproven presence shapes keep scans; no format or aggregate-source change.
      Necessary `$exists: false` clauses now contribute ordinary null keys to
      complete finite probes, with explicit null removed by the full matcher.
      Existing compound/sparse/null safety and fallback markers remain in force.
      Singleton candidates preserve document-first streaming even with stale
      statistics and large null-key groups; no all-candidate sort is required.
      Bounded positive OR combinations now union per-path necessary witnesses
      only when every alternative is covered. Conjunctions never intersect
      array-valued matches. A 128-operand work cap, existing tuple/byte bounds,
      canonical deduplication and full matching retain conservative behavior;
      uncertain branches keep scans. Necessary finite probes remain preferred
      across indexes, followed by logical probes and sparse-presence scans.
      Partial Ready indexes now supply finite candidates when bounded positive
      AND/OR proofs establish every required scalar equality or explicit presence
      fact. Exact representation avoids unproven numeric-alias implications;
      ranges, type/list implications and negations still scan. Shared work limits,
      current Ready authority and full matching remain mandatory. Native read
      counters, scan differentials, churn/membership-changing mutations and
      sync/async PyMongo restart fixtures cover the private planner extension;
      frozen public equality inference and index formats remain unchanged.
      Necessary string ranges now filter single-component Ready entries using
      bound UTF-8 payload comparisons, never equality-frame byte order or SQL
      numeric coercion. One bound preserves independent multikey matches; fallback
      records and authoritative matching stay mandatory. Sparse and independently
      proven partial membership are supported. Numeric/compound/unproven ranges
      retain safe scans/other proven probes. SQL Unicode/multikey properties,
      cancellation, native read/write/restart/churn/corruption checks, real-driver
      scan differentials and same-root benchmarks validate this #178 slice.
      It avoids BSON decoding, not all index-entry traversal; no format change,
      ordered range seek, JSON shadow or Mongo explain claim is introduced.
      Native Rust and sync/async Python reads now offer opt-in, payload-free
      access-path diagnostics: candidate proof/index identity/key count or a
      conservative scan reason, freshly selected on each cursor page. Default
      responses and point plans are unchanged; sorted/aggregate page budgets
      include the metadata. Planned shard owners are not measured visits;
      MongoDB explain, physical counters and broader benchmarks remain open.
      Independent opt-in native execution statistics now measure record-read
      calls, examined BSON documents, source-matcher evaluations and actual
      read shards. Point/routed/scatter/sorted/distinct/aggregate reads share
      request-local counters; lookahead/rescans count again, buffered output
      can report zero source reads, and cursors retain no collector. Default
      execution/output is unchanged. These are not SQLite page/byte counters
      or MongoDB executionStats. #178's local candidate benchmarks and exact
      frozen contract pass are separate from #186's full inventory and #185's
      release-performance acceptance.
      Equality candidates now also narrow update/delete, replacement and
      find-and-modify selection, including sorted choices and upsert rechecks.
      Natural-order frontiers prevent duplicate processing when indexed keys
      change. Scan differentials, physical-selection proofs, exact record/entry
      rollback, cancellation/abort, sync/async PyMongo and restart checks retain
      the existing per-shard commit and recheck boundaries.
      Secondary-uniqueness groundwork now routes every record write through a
      root/collection/shard-bound transaction owning any cross-process writer
      fence until SQLite cleanup. Bounded retained stripes, cancellation,
      parent-task abort, process death and failed rollback are covered. Pending
      unique and Ready non-unique indexes do not request fences.
      Version 20 now fences older writers and enables unique builds/activation,
      cross-shard duplicate validation for all record mutations and upserts,
      and global ownership checks on reopen. Ordinary/compound/multikey/sparse/
      partial keys share the canonical generator. Build duplicates fail before
      intent; runtime duplicates return 11000. Disk-backed private scratch bounds
      build/startup key memory; writer stripes span record/entry commit or rollback.
      Removal releases enforcement through the existing recoverable lifecycle.
      Bulk writes still commit per input/shard and can reject transient collisions
      even when the eventual image is unique. Frozen TinyMongo memory and SQLite
      backends differ here; #74/#183 retain the explicit non-atomic contract.
      #183 adds 28 isolated process exits before/after every input/shard commit
      for insert/update-many/delete-many, with exact recovered records, Ready
      unique/multikey index queries, conditional/stable-ID replay, a second reopen,
      and resource reuse. Existing cancellation/rollback/partial-wire-error and
      independent-process unique-writer tests cover the other boundaries. This
      is not generic retryable writes, distributed transactions or a power-loss
      proof. Index selector compatibility remains #174; wider soak remains #185/#187.
      Non-unique physical indexes now accept nested/object/array and other valid
      BSON values via checksummed, record-bound fallback candidates. Candidate
      scans include these records before the complete matcher; unique indexes
      retain strict key semantics. Atomic maintenance, corruption checks, build/drop
      recovery and manifest-v21/digest-v13 downgrade fencing preserve restart safety.
      The pure key oracle remains unchanged. Mixed wire IndexModel batches accept
      non-unique hashed components as ascending equality keys, TTL without
      expiration, and synchronous background requests with explicit bounded
      `briskdbIndexWarnings` replies. Unique hashed/TTL and built-in ID degradation
      are rejected before mutation; effective metadata and retries survive reopen.
      Numeric direction metadata is preserved. Non-unique text declarations now
      skip the entire index with explicit `skipped: true` diagnostics; no phantom
      metadata or full-text querying is added. Unique text and malformed late
      models fail eagerly; existing same-name indexes keep their enforcement.
      All-text batches match frozen memory/JSON namespace behavior, with retries
      and reopen covered. The optional local PyMongo clients now accept bounded
      model iterables/mappings/duck models and resolve reduced equivalent names
      inside the exclusive server batch. Descending models normalize to equality
      keys; warnings identify actual reused names and skipped text models.
      Sync/async factories and patch scopes retain driver options; ordinary
      PyMongo behavior is unchanged. Ready-only reuse preserves ordered keys,
      unique/sparse/partial membership and durable identities. New wheel/reopen,
      concurrency-with-busy-retry, bounded-result and 144 source-locked public
      model outcomes cover this slice. The source-checked index inventory below
      completes #174's bounded acceptance; the broader corpus remains #186.
      Legacy private catalog repair is not claimed.
      The upstream durable-index audit additionally accepts the boolean
      `bypassDocumentValidation` write option as a no-op while collection
      validators remain unsupported. Insert/update/upsert/find-and-modify still
      enforce BSON, immutable IDs and all unique-index constraints; real
      sync/async wheel regressions retain bulk prefix counts and restart state.
      All seven public exact-ID source scenarios now also run against the
      installed wheel, including ordered document IDs, numeric aliases, array
      IDs, compound/logical selectors and mapping subclasses. Private Python
      helper internals remain covered by the shared matcher oracles.
      Public advanced-index wheel regressions now exercise compound tuples,
      conditional multikey uniqueness, sparse missing/null distinctions, partial
      membership transitions, rollback, invalid options and restart metadata.
      Direct local sync/async `create_index()` rejects duplicate fields before
      PyMongo can collapse them; valid direct directions/options remain unchanged.
      Durable-index wheel scenarios additionally cover namespace/catalog identity,
      drop/recreate and retry, typed/multikey uniqueness, four-client contention,
      and explicit same-shard rollback. Driver metadata/result shapes and the
      #74/#183 cross-shard boundary remain explicit compatibility differences.
      The source-checked inventory maps all 73 functions / 217 reference backend
      cases from the four index suites to executable public/native evidence or
      explicit implementation/contract rationales (59 public, 7 native equivalents,
      3 private exclusions, 4 contract differences). These counts are not unchanged
      candidate passes. Hashes, complete function membership, collected case counts
      and candidate test symbols are checked; the immutable v1 contract is untouched.
    - [x] [#182](https://github.com/schapman1974/briskdb/issues/182) — natural-order
      source pages now load their initial frontiers from at most eight target
      shards concurrently, sharing pool/worker admission and a checked aggregate
      frontier byte budget. Physical shard positions and the existing natural-order
      merge preserve global pagination across empty/uneven and owner-pruned shards.
      Errors, panics and cancellation stop admission and drain children before
      operation guards release; peer cancellation never poisons caller/listener
      tokens. Native/legacy counts now use the same bounded coordinator for
      per-shard scalar totals, retaining full matching and owner pruning before
      checked summation and global skip/limit. Sorted-window scans now share this
      coordinator and one global 1024-key/64-MiB heap, with bounded per-worker key
      derivation and globally ordered refetch. Tests cover bounded waves, blocked
      peers, shard errors, deadlines, caller abort, empty/uneven shards, pagination
      and arbitrary key arrival with memory trimming. Point reads remain direct;
      natural frontier refill stays sequential. No partial-page or new snapshot
      guarantee is introduced; broader fault-soak/release gates remain separate.
    - [ ] [#186](https://github.com/schapman1974/briskdb/issues/186) — beyond the
      frozen corpus and index inventory, nineteen query/public-client suites now
      have complete source accounting (251 functions/642 reference cases), backed
      by 175 installed-wheel scenario tests, including recursive document classes,
      async reads and timezone/millisecond fidelity. Legacy cursor/index
      representations, non-BSON Python values and private-helper exclusions
      are explicit;
      hash/membership/count/test-symbol gates reject unexplained omissions.
      These adapted scenarios are not the entire upstream corpus or unchanged
      source-body passes; larger inventories and fault/property tiers remain.
      Local sync/async `insert_many` now preflights the whole iterable before
      sending any insert, fixing partial writes on serialization errors beyond
      a 1,000-document wire batch. Encoded snapshots preserve one-pass custom
      codecs, input IDs and global duplicate indices; this is not a transaction
      or a change to the intentionally redacted duplicate-key diagnostics.
      Client options are now validated before opening local storage, preventing
      invalid sync/async configuration from creating files or recovering a root;
      endpoint binding precedes all driver topology/background work.
      Local find cursors now snapshot caller-owned filters/projections and reject
      bare-string projections, without changing ordinary PyMongo classes.
      Added projection/scan/index/restart and sync/async unset coverage includes
      client-heap bounded-read comparisons, not native RSS or private SQL-hook
      equivalence; legacy cursor helpers and backend-only hooks remain explicit.
      Common-client coverage includes dotted sync/async collections, logical
      lifecycle, concern isolation and concurrent shared-collection mutations.
      Six spawned writers retain all 300 acknowledged records after reopen on
      Linux/macOS; schema is prepared before overlapping roots. Private Python
      cache/lock counts, permanent-close cursor semantics and full database
      statistics remain explicit differences, not silently counted as parity.
      Update-modifier wire fixtures now cover min/max representation/order, sparse
      arrays, rename/pop validation, equality-seeded upserts, no-match preflight
      and single-record failure rollback; helper-only non-BSON inputs are explicit.
      Array-update cases add push ordering, BSON-sensitive addToSet/pullAll,
      logical/regex/elemMatch pull predicates, sparse paths and preflight failure
      preservation; driver encoding errors are distinct from private helper errors.
      BSON value-type coverage includes Code/string distinctions, scoped-code
      identity, whole-value ordering and isolated atomic-clock rollover/concurrency
      tests. Native preflight timestamp reservation gaps, bson.Regex decoding and
      driver versus TinyMongo diagnostic surfaces remain explicit differences.
      Regex fixtures distinguish predicates from literal BSON identity, validate
      nested contexts/flags, and prove malformed queries create no collection.
      Driver cstring errors, lazy evaluation and private backend/preflight hooks
      are classified explicitly; native UUID uniqueness is checked separately.
      Basic aggregation cases add exact validation codes, int64 pagination,
      stage ordering/count composition, array-sort error precedence and large
      shared-array results. Non-BSON inputs and private Python warning/call
      counts remain explicit differences, with separate native work bounds.
      Projection-stage fixtures cover BSON field order, recursive array transforms,
      literal/REMOVE semantics, set aliases, unset forms and async composition.
      Exact validation codes/precedence and no catalog creation are checked,
      with driver/private-helper differences still explicitly accounted for.
      Main aggregation coverage checks grouping identity, lazy expressions,
      validation, routed-source semantics and sync/async cursor cleanup.
      Native constant-key support, unsorted shard order, real command-cursor
      limitations and missing TinyMongo-only capability introspection are
      explicit differences, not a completed full-parity claim.
    - [ ] [#185](https://github.com/schapman1974/briskdb/issues/185) — operating
      drill imports real locked TinyMongo table-native/two-shard SQLite stores
      into four BriskDB shards, builds pending indexes explicitly, and checks
      BSON, empty collections, uniqueness and post-restore writes with stock
      PyMongo. Stopped whole-root copies preserve exact file membership/bytes;
      incomplete restores fail closed and the unchanged original source remains
      usable for pre-cutover rollback. This is not online backup, reverse data
      migration, cross-version/power-loss proof or complete release acceptance;
      a checked 12-workload benchmark now compares isolated release-wheel,
      locked TinyMongo sharded-SQLite and optional MongoDB reference clients.
      Raw timings, configuration/provenance and final-record hashes are retained;
      CI runs correctness smoke and controlled hosts can gate median regressions
      against a matching baseline. Cross-implementation timings are not equivalent
      transport/storage costs or a speedup claim. Normal wire finds now avoid a
      redundant collection-existence catalog command while retaining engine
      admission, manifest integrity verification and typed missing-namespace
      handling. Wider app/soak/security gates,
      representative sustained-load evidence and complete release acceptance
      remain open.
    - [ ] [#187](https://github.com/schapman1974/briskdb/issues/187) — Rust hosts
      can narrow listener-local connection caps (1–8) and command deadlines
      (positive, at most 15 seconds) with immutable `MongoResourceLimits`.
      The same policy narrows retained cursors to 1–32 per listener and 1–8 per
      connection. Registrations and handoffs enforce both quotas; rejected
      handoffs preserve the original owner, rejected registrations release
      native resources, and kills/disconnects/shutdown reclaim capacity.
      Complete-frame decoding/parser queuing and engine work share an absolute
      deadline; client maxTimeMS cannot relax it. Defaults and independent BSON,
      cursor/result/socket limits remain unchanged; authenticated per-user quotas
      await shared security. Hosts can inspect payload-free Mongo metrics: connection
      admission/lifecycle/failures, fixed command/error-code counters, one-way
      outcomes, write/response-limit errors and bounded latency histograms.
      Wire cursor metrics track registration, active/peak/closed entries, idle
      expiry and capacity rejection without double-counting batches or handoffs.
      Drop guards drain live gauges on completion/abort; counters saturate and
      snapshots remain available after close without retaining engine resources.
      Native concurrency/unwind and real socket fixtures cover outcomes, cap
      rejection, malformed frames, response limits and listener-local reset.
      Rust hosts can separately opt in to read-work metrics: record calls,
      examined documents, source matcher evaluations/acceptances, output items, access-plan
      counts and actual request/shard fanout, including zero-read buffered pages.
      Source acceptances include repeated/lookahead reads before pagination,
      projection and pipeline stages, not unique matched or returned documents.
      Fixed physical-ordinal counters expose request distribution and examined/
      matched row work, including repeated reads, not CPU or physical-I/O skew.
      Per-request native/Python shard summaries remain bounded to 64 ordinals,
      charge a conservative 2,048-byte diagnostic budget, and retain no labels.
      Off by default; enabled requests use bounded engine diagnostics
      without wire fields. Failed engine work, legacy count and mutations are
      excluded. Host-owned debug request spans/final events now correlate process
      session IDs, repeated wire IDs and connection-local sequences across async
      tasks/blocking workers. Fixed outcome/error classifications retain no BSON,
      namespace, comment, credential or diagnostic payload; guards release on
      completion/abort/unwind without installing a subscriber or export queue.
      Host readiness now reports listener/document/engine/schema admission states
      and explicit anonymous-loopback security through a non-owning live probe.
      Detected catalog/shard failures map to schema-degraded, without paths or
      hidden I/O; this is not deep integrity or authenticated network readiness.
      Bounded raw-wire stress reacquires all 32 cursor and eight connection slots
      after each handoff/disconnect/malformed/truncated-frame wave; the extended
      CI tier repeats 128 waves over four engine lifetimes and rejects stale
      cursors after reopen. Gauge/accounting checks are not allocator/RSS or
      long-duration/power-loss/disk-full soak certification.
      Per-shard timing/physical-I/O telemetry, exporters/engine-phase tracing, full
      governance and broader fault/soak acceptance remain open; no ceilings raised.
    - [ ] [#167](https://github.com/schapman1974/briskdb/issues/167) — shared Rust
      BSON matcher now powers embedded and wire find/count/distinct/aggregate/delete. Includes dotted
      paths, missing/null behavior, array/logical/comparison operators, type/mod,
      bounded regex evaluation, eager validation, and cancellation. Filtering
      precedes global pagination; exact `$eq` IDs retain point routing (#180).
      Required CI compares a generated BSON matrix with the locked oracle;
      real sync/async PyMongo and embedded Python tests cover queries/restart.
      Remaining work includes broader dialect/consumer conformance,
      remaining read helpers (#172), and write/index reuse.
    - [ ] [#168](https://github.com/schapman1974/briskdb/issues/168) — retained find
      cursors now support global filter/skip/limit paging, empty initial batches,
      byte-bounded pages, getMore, and killCursors. Native Rust/Python cursors are
      session-owned; wire cursors follow pooled sockets with disconnect cleanup.
      Count/retention quotas, idle expiry, cumulative wire time budgets,
      cancellation/error cleanup, and engine shutdown bound resources without
      retaining SQLite leases. Required tests exercise real sync/async drivers,
      restart, ownership, byte limits, and exhaustion. Aggregate cursors now
      share the same ownership/cleanup/quotas, retaining streaming stage counters
      or bounded blocking-stage results. Collection metadata now shares these
      limits with filtered, byte-bounded pages; Ready-index metadata shares the
      registry with bounded name/identity positions;
      no cross-shard or cross-batch snapshot is promised.
    - [x] [#172](https://github.com/schapman1974/briskdb/issues/172) — basic find
      projection now shares one Rust transform across embedded and wire APIs.
      Inclusion/exclusion, nested/dotted paths, array traversal, `_id` rules,
      field order, and BSON fidelity are retained across cursor batches. Filters
      use original values and byte budgets use projected output. Eager errors,
      cancellation, resource bounds, and cursor retention accounting are tested.
      Required CI compares 4,865 cases against the locked projection oracle;
      real sync/async drivers and Python APIs cover paging and unchanged storage.
      Shared BSON sort keys now cover stable semantic ties, dotted/numeric
      paths, correlated compound arrays, and parallel-array errors, with 4,654
      additional locked-oracle cases and bounded/cancellable work. Global sorted
      find now preserves natural-order ties, skip/limit, and projection across
      native/Python/wire batches using bounded top-key windows and retained
      last-key positions. Windows rescan until sorted indexes exist; key growth
      is reaccounted against cursor quotas. Wire `count` now shares engine
      filtering and global skip/limit, including PyMongo's sync/async
      `estimated_document_count()` and zero for absent collections. PyMongo's
      aggregation-based `count_documents()` now also works, including filtering,
      skip/limit, absent namespaces, async calls, and restart. Distinct shares one bounded BSON extractor across
      Rust, native sync/async Python, and wire clients. Global natural-order
      reads preserve the first exact representation and charge only unique
      outputs; missing/null/array/path semantics follow 4,888 additional locked
      oracle cases. It uses no retained cursor slot, rejects whole over-budget
      results, and remains a scan implementation without snapshot semantics.
      Wire reads now accept TinyMongo-style no-effect hints with fixed, redacted
      `briskdbReadWarnings` diagnostics, plus opaque comments, local read concern,
      simple binary collation and explicit no-op/default flags. Index selection
      remains automatic; hints do not force indexes or change natural order.
      Stock sync/async PyMongo checks cover chaining, getMore, absent namespaces,
      untouched catalogs/documents and reopen, with source-locked TinyMongo
      no-effect reference checks. Unknown options, stronger read guarantees,
      locale collations, live/tailable cursors, disk spill and sessions still
      fail explicitly rather than silently promising unsupported semantics.
    - [x] [#179](https://github.com/schapman1974/briskdb/issues/179) — shared basic
      aggregation core compiles `$match`, `$sort`, `$skip`, `$limit`, and `$count`
      eagerly and executes immutable BSON through shared stages.
      Matching and stable sorting reuse the authoritative implementations;
      exact numeric stage arguments, empty counts, stage ordering, cancellation,
      and resource limits are tested. Required CI compares 5,134 whole pipelines
      with the locked TinyMongo source in both materialized and incremental modes.
      Native Rust, sync/async Python, and real PyMongo `aggregate()` now share
      global source reads and retained, byte-bounded cursors. Simple prefixes
      stream; count retains a counter, and sort materializes bounded input.
      Resource/error/cancellation cleanup and cursor ownership are tested.
      Basic-suite projection consumers now use the shared transforms below.
      Grouping and PyMongo `count_documents()` are implemented below. Required CI
      now runs every frozen basic-aggregation case through the real four-shard
      BriskDB endpoint, in both sync/async modes, with no skips or corpus changes.
    - [x] [#177](https://github.com/schapman1974/briskdb/issues/177) — shared
      `$project`/`$set`/`$addFields`/`$unset` stages and `$ifNull`/`$literal`/`$size`
      expressions now run across Rust, native Python, and wire aggregation.
      Field references, missing/null, arrays, `$$REMOVE`, collision/error order,
      original-input assignments, and exact output field order follow 7,037
      additional locked-oracle pipelines. Nonblocking stages fuse around
      materialization boundaries so limits stop unused expression evaluation.
      Allocation amplification, depth/nodes/work, cancellation, cursor cleanup,
      sync/async clients, byte paging, and restart are covered. The frozen
      projection-to-group identity case now uses the shared group stage below.
      All frozen aggregation projection-stage and application aggregation cases
      now pass through the real four-shard endpoint in both API modes. Combined
      with the basic/group-accumulator suites, replacement, and three additional
      application write/identity contracts, nested unsetting, configured-client
      read fidelity, three cross-CRUD query-validation cases, and all nine
      non-upsert update-operator contracts and both upsert-operator contracts,
      the add-to-set non-array atomicity case,
      the complete array-update suite, and three pull/BSON-comparison cases,
      plus missing-counter, CRUD increment metadata, Decimal promotion, and
      Decimal representation/no-op contracts, plus replacement-upsert equality
      IDs, field order, numeric aliases, and ID conflicts, operator-upsert CRUD
      metadata and zero-timestamp write boundaries,
      plus three non-unique nested-value index contracts,
      and mixed IndexModel creation, required CI now checks all 456 frozen
      executions in a separate real-endpoint job and rejects missing, duplicated,
      substituted, skipped or failed cases. The broader TinyMongo inventory,
      application suites and operational acceptance criteria remain open.
    - [x] [#176](https://github.com/schapman1974/briskdb/issues/176) — shared
      `$group` supports literal/field/computed keys, recursive structured BSON identity, and
      all eight planned accumulators across Rust, sync/async native Python, and
      PyMongo. It retains bounded states in established global input order,
      including first/last and ordered Decimal128/mixed-numeric arithmetic.
      The 9,509 accumulator and 5,663 key pipelines pass in both execution modes;
      5,424 use explicit unchanged-reference expression/group composition for
      key forms outside the frozen group grammar. All others use identical pipelines;
      arithmetic double NaN bits are unspecified and normalized only in tagged
      numeric outputs. Separate tests cover unencodable-reference integer sums,
      memory/BSON limits, cancellation, byte paging, cleanup, and restart.
      All 30 frozen group-accumulator sync/async executions now pass against
      the real four-shard candidate, including empty-input setup via delete-many.
      Pipelines beginning with a group now use bounded shard-local states for
      integer-literal sums and first/last/min/max, merging exact totals and
      global input positions before the remaining stages. All states share the
      same memory/input/work quotas; failed children drain before cleanup.
      Dynamic/rounded sums, averages, push/set accumulators and preceding stages
      retain the ordered stream. No rounded shard totals, disk spill or snapshot.
      The complete frozen 456-execution command corpus is required independently;
      the broader source inventory and release acceptance remain separate issues.
    - [ ] [#166](https://github.com/schapman1974/briskdb/issues/166) — targeted
      collection existence checks now share the engine across Rust, embedded
      sync/async Python, and wire read/write namespace handling. Catalogs beyond
      101 collections and large unrelated metadata no longer break wire data
      commands. Missing probes create nothing. Durable collection/database drops
      now share that engine across all adapters, with checksummed shard-progress
      recovery and non-reused catalog identities protecting retained cursors.
      Format 16 upgrades preserve existing data; interrupted accepted drops roll
      forward on reopen. Explicit plain wire creation and retained collection
      metadata cursors now share the engine. Names/full metadata, filters, stable
      UUIDs, and pooled getMore work with real sync/async PyMongo. Names-only
      database discovery now shares exact-name filtering and request controls
      across Rust, native Python, and PyMongo. Full database statistics and
      advanced creation options remain open; shared physical files are not
      misreported as independent per-database disk usage.
11. [ ] **Implement online resharding and rebalance.** Add durable bucket
    movement, generation-aware retries, verification, and a supported offline
    reshard path before online movement.
12. [x] **Retain the single-shard transaction boundary (#74).** General distributed
    transactions remain unsupported for the alpha. Existing operation-specific
    coordinators and document batches keep only their documented guarantees;
    #183 separately records the Mongo crash/retry acceptance for that policy.
13. [ ] **Implement MySQL support.** Add the listener, wire lifecycle, prepared
    statements, type and error mapping, transactions, security, and real-client
    conformance after the higher-priority frontends are stable.

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
- Selected PostgreSQL server library: exact `pgwire` 0.36.3; see the
  [adapter decision record](docs/POSTGRES_ADAPTER.md)
- MySQL server library candidates: <https://crates.io/crates/mysql-mimic> and
  <https://crates.io/crates/opensrv-mysql>
- SQL parser candidate: <https://crates.io/crates/sqlparser>

Library choices remain spike outcomes, not permanent architecture. BriskDB must
own the adapter interfaces and conformance tests so a protocol crate can be
replaced without rewriting the engine.
