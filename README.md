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
The [experimental sharded virtual-table contract](docs/SHARDED_VIRTUAL_TABLE.md)
defines the no-fork coordinator, its opt-in autocommit write path, and the
features that deliberately remain on the established engine paths.
The [virtual-table rollout gate](docs/VTAB_ROLLOUT.md) freezes the correctness,
performance, resource, snapshot, and protocol criteria and records why the
facade remains experimental after issue #131.
The [generated-key contract](docs/GENERATED_KEYS.md) defines the exact accepted
SQLite, MySQL, and PostgreSQL declarations, omitted-key insert semantics,
protocol-neutral and HTTP results, and the two execution gates.

The [SQL compatibility contract](docs/SQL_COMPATIBILITY.md) distinguishes the
unregistered legacy SQLite pass-through from the authoritative-catalog HTTP
path, PostgreSQL startup support, and planned PostgreSQL/MySQL query
compatibility.
The [PostgreSQL listener contract](docs/POSTGRES_LISTENER.md) defines its
address configuration, startup/session behavior, parameter status, deferred
query boundary, and shared shutdown lifecycle.
The [PostgreSQL adapter decision record](docs/POSTGRES_ADAPTER.md) selects the
exact wire-library version and features, defines the BriskDB-owned connection
boundary, and records the work that remains for query execution.
The [SQL parser decision record](docs/SQL_PARSER.md) defines the shared,
dialect-explicit syntax boundary and its resource and dependency limits.
The [common SQL subset contract](docs/SQL_SUBSET.md) defines the opt-in,
protocol-neutral structural validator and its exact accepted statement forms.
The [statement-classification contract](docs/SQL_STATEMENT_CLASSIFICATION.md)
defines the shared read/write/schema/session taxonomy, conservative batch gate,
and behavior metadata consumed by planning and prepared execution.
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
The [prepared-statement contract](docs/SQL_PREPARED_STATEMENTS.md) defines the
protocol-neutral prepare/bind/describe/execute lifecycle, session-scoped
statement and portal caches, exact resource limits, metadata refresh, and
supported physical-target execution boundary shared by future adapters.
The [error contract](docs/ERRORS.md) defines stable engine error kinds, safe
HTTP problem details, the mapping consumed by PostgreSQL startup/query deferral
and the private adapter probe, and the mapping reserved for a future MySQL wire
adapter.
The [request-control contract](docs/REQUEST_CONTROLS.md) defines cancellation,
deadlines, materialized-result budgets, and graceful shutdown.
The [admin data-browser contract](docs/ADMIN_BROWSER.md) defines the embedded
read-only per-shard explorer, its temporary login, finite pages, and live-view
boundaries.
The [standard SQLite import contract](docs/SQLITE_IMPORT.md) defines the
offline, staged conversion of one ordinary SQLite database into an explicitly
cataloged BriskDB layout with exactly-once Sharded row placement.
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
- Catalog-aware logical reads: exact keys visit one owner, finite key sets visit
  their distinct owners, and unconstrained Sharded reads gather every shard
  with bounded concurrency and `UNION ALL` duplicate semantics
- Explicit graceful drain, forced cancellation, and blocking handle cleanup
- Independently configured HTTP and PostgreSQL TCP listeners; the loopback
  PostgreSQL endpoint supports protocol 3.0 startup, logical database/user
  selection, BriskDB parameter status, and clean connection termination through
  a BriskDB-owned `pgwire` 0.36.3 boundary
- An embedded read-only browser at `/admin` for inspecting user tables, showing
  exact logical-row totals, and reading bounded logical pages across each
  table's metadata-selected files without a shard selector; pages reuse each
  physical table's primary-key or rowid order instead of sorting every column
- Protocol-neutral typed values, ordered columns, positional rows, and results
- A bounded per-session prepared-statement and immutable bound-portal lifecycle
  with transient shard-0 metadata compilation, bind-time routing snapshots,
  retained logical behavior, fresh execute-time planning, and supported
  physical-target execution
- A bounded SQL AST parser plus recursive common-subset validator for explicit
  SQLite, PostgreSQL, and MySQL dialects, followed by opt-in source-preserving
  statement/batch classification, placeholder normalization, per-statement
  binding metadata, and catalog-aware typed shard-key inference plus
  synchronous bound-value-aware routing plans with single-shard write policy;
  a populated authoritative catalog composes that SQLite frontend and policy
  into the raw HTTP execute/query path, while an empty catalog alone retains
  the legacy pass-through behavior
- A transactionally versioned `manifest.sqlite` with durable 4,096-bucket
  routing, logical-database and authoritative table-placement metadata, and
  optional per-table durable `hilo_v1` ID block leasing
- A supported offline SQLite importer with complete per-table placement plans,
  one physical owner for every Sharded row, explicit-only Global replication,
  exact-value verification, and atomic no-replace publication
- Initialization-only registration of a complete, empty physical schema, with
  later schema migrations checked against its table and shard-key declarations
- An initialization-only `Database::apply_generated_table_ddl` bridge that
  durably binds one exact SQLite/PostgreSQL/MySQL generated-table declaration
  to canonical physical SQLite migration, `native_range_v1` provisioning, and
  stable receipt identities with automatic restart recovery
- Identity-bound, WAL-enabled SQLite shard files that are never silently
  recreated after initialization
- Routed writes and catalog-aware logical query execution, with a separately
  compiled and runtime-enabled virtual-table coordinator for explicit-key
  autocommit writes to registered Sharded tables and single-row omitted-key
  writes for active generated-ID policies; generated IDs are captured by the
  committing operation and returned through protocol-neutral and HTTP results
- A crash-resumable, journaled schema-migration endpoint for every shard
- A checksummed manifest, generation-bound shard-schema fingerprints, and
  fail-closed `Verifying`/`Ready`/`Migrating`/`Degraded` storage states
- Full SQLite synchronous durability and a five-second busy timeout
- Reproducible point-read, point-write, and four-shard write benchmarks

The on-disk layout is deliberately simple:

```text
briskdb-data/
├── briskdb-import-receipt.json  # present for imported layouts
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

The established physical-shard write path remains the default. To try the
stock-SQLite virtual-table coordinator for registered explicit-key writes or
single-row omitted-key writes on an active generated-ID table, enable both its
Cargo feature and runtime gate:

```bash
cargo run --features experimental-vtab -- \
  --experimental-vtab-writes \
  --data-dir ./briskdb-data \
  --shards 4

# The runtime gate is also available through the environment.
BRISKDB_EXPERIMENTAL_VTAB_WRITES=true \
  cargo run --features experimental-vtab -- --data-dir ./briskdb-data --shards 4
```

This opt-in changes only validated `Engine::execute` and
`Engine::execute_write` autocommit DML for a populated catalog, including the
`/v1/execute` adapter. `/v1/query` and the admin browser continue to use the
established metadata-driven scatter/gather readers. `BEGIN`, `COMMIT`,
`ROLLBACK`, and transactions spanning HTTP requests are not enabled by this
flag. Generated-key DDL and insert examples, the additive HTTP response field,
and deliberate protocol gaps are documented in
[the generated-key contract](docs/GENERATED_KEYS.md). The feature remains
explicitly opt-in under [the rollout decision](docs/VTAB_ROLLOUT.md).

To initialize a new data directory from an existing standard SQLite file, use
the separate offline importer. The destination must not already exist, and the
JSON plan must declare every source table as either Sharded with an authoritative
key or explicitly Global:

```bash
cargo run --bin briskdb-import -- \
  --source /path/to/source.db \
  --data-dir /path/to/new-briskdb-data \
  --shards 4 \
  --plan /path/to/import-plan.json
```

See the [import contract](docs/SQLITE_IMPORT.md) for schema support,
verification, cancellation, and publication behavior.

The HTTP listener defaults to `127.0.0.1:7654`. The separate PostgreSQL TCP
listener defaults to `127.0.0.1:5433`. Set either an explicit socket address or
the exact value `disabled` with `--postgres-listen`; the corresponding
environment variable is `BRISKDB_POSTGRES_LISTEN`. Command-line input takes
precedence over the environment, which takes precedence over the default. The
HTTP address, data directory, and shard count can also be supplied with
`BRISKDB_LISTEN`, `BRISKDB_DATA_DIR`, and `BRISKDB_SHARDS`.

An enabled PostgreSQL address must be IPv4 or IPv6 loopback in the current
phase. A non-loopback value is rejected before the engine opens or either
listener binds.

```bash
# Keep the existing HTTP service and do not bind the PostgreSQL port.
cargo run -- --postgres-listen disabled

# The environment has the same value grammar.
BRISKDB_POSTGRES_LISTEN=disabled cargo run
```

The PostgreSQL listener currently accepts only loopback addresses. It supports
protocol 3.0 startup and session selection, then reports fixed `0A000` errors
for SQL until issue #31 adds simple and extended query execution. It is not yet
a query-capable PostgreSQL interface. See the
[listener contract](docs/POSTGRES_LISTENER.md) and
[adapter decision record](docs/POSTGRES_ADAPTER.md).

The HTTP listener also serves the embedded data explorer at
<http://127.0.0.1:7654/admin>. Its temporary credentials are `admin` / `admin`.
The explorer is read-only: select a logical table, see its exact logical row
total, then move through live shard-major pages of at most 200 rows. Sharded
tables visit every owning file; Global tables visit canonical shard 0 once.
Browser sessions are held only in process memory, expire after eight hours,
and disappear on restart. The fixed credentials are a development convenience;
keep this experimental HTTP service on a trusted network. Existing `/health`
and `/v1/*` behavior is unchanged. See the
[admin browser contract](docs/ADMIN_BROWSER.md) for the route, table-filtering,
pagination, and session boundaries.

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
handles, and then stop the process. Core admission closes before both listener
sockets are dropped. Accepted HTTP and PostgreSQL connections are tracked;
connections that outlive the grace window are force-closed. HTTP task joins are
awaited; PostgreSQL task joins and retained-session closes get one additional
grace interval. If that second interval expires, remaining PostgreSQL session
closes are scheduled as best-effort runtime cleanup rather than delaying server
return. Every completed PostgreSQL startup owns one core session that is closed
on `Terminate`, EOF, or protocol failure; server shutdown applies the bounded
cleanup described above.

Each session defaults to at most 128 prepared statements, 128 bound portals,
and a 16 MiB ceiling for retained bound values/captured routing bytes and one
bind's conservative planning expansion. Configure
these with `--max-prepared-statements-per-session` /
`BRISKDB_MAX_PREPARED_STATEMENTS_PER_SESSION`,
`--max-portals-per-session` / `BRISKDB_MAX_PORTALS_PER_SESSION`, and
`--max-retained-bound-value-bytes` /
`BRISKDB_MAX_RETAINED_BOUND_VALUE_BYTES`. The hard caps are 1,024 statements,
1,024 portals, and 1 GiB. Full caches reject new entries without evicting open
handles. Before planner allocation, the captured route is charged once and
every normalized marker occurrence charges twice its logical accounted value
bytes against the same byte ceiling; repeated markers are charged per
occurrence even though the portal retains one bound value. This is an
accounting model, not a retained wire encoding.

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

Before loading rows, a Rust embedder may register the complete table layout once
with `Database::register_tables`. Every ordinary physical table must exist with
the same name on every shard, be empty, and be declared `Sharded` or `Global`;
a `Catalog` declaration must have no physical table or view. A sharded
declaration also names a visible, physically non-null column with compatible
`Int64`, text, or binary affinity. SQLite's non-null `INTEGER PRIMARY KEY`
rowid alias is accepted; nullable legacy primary-key forms are not. Text keys
use SQLite `BINARY` collation, and every primary/unique key on a sharded table
must include its shard key with `BINARY` collation. Application foreign keys
are accepted only for conservatively proven local placement; unsafe foreign
keys, triggers, and virtual tables are unsupported. An exact repeat is
idempotent, but a different or partial replacement is rejected. Registration
is an initialization boundary, not an online catalog-editing API; close and
reopen the registering handle after an ambiguous manifest-commit error.

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

Read it through the logical table:

```bash
curl -X POST http://127.0.0.1:7654/v1/query \
  -H 'content-type: application/json' \
  -d '{
    "sql":"SELECT id, name FROM widgets WHERE id = ?1",
    "params":["widget-1"]
  }'
```

The response keeps column metadata and row values in matching index order. The
inferred key selects its one metadata-owned shard, so this point query reports
one `shard`; a query that visits several reports a sorted `shards` array
instead. An example point response is:

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
By default, after a populated catalog validates `/v1/execute` as one routed
common-subset write, those ephemeral requests share a stateless physical-handle
ownership domain. This keeps the logical sessions separate while allowing clean
autocommit write handles to remain warm. When BriskDB is compiled with
`experimental-vtab` and `--experimental-vtab-writes` is set, an accepted
explicit-key write, or one single-row insert omitting an active declared
generated key, executes through one ephemeral writable coordinator and commits
its one physical child before HTTP success is returned. Explicit writes keep
their existing HTTP shape; a generated write adds `generated_key` and reports
the allocator-selected physical owner. The same generated result is available
through protocol-neutral Engine and prepared-portal execution. Each request is
still one autocommit statement; the option does not add session transaction
state, Global writes, or a virtual-table read path. Empty-catalog pass-through
SQL retains its existing behavior. Registration, startup, import, and migration
validation also reject `last_insert_rowid()`, `changes()`, and
`total_changes()` inside persistent table or index expressions, so a stored
`DEFAULT`, `CHECK`, generated expression, or index cannot bypass that boundary.
Pool admission happens before blocking SQLite work: once a shard's active slots
and queue are full, the engine returns retryable `Busy` (HTTP 503) instead of
growing work without bound. The native omitted-key path cannot know one target
before its bounded worker starts, so it uses a non-waiting reservation for one
round-robin candidate at a time and falls back around busy or exhausted owners;
it never queues while retaining that worker. Hi/lo reserves every possible
target capacity before consuming an ID. Connections are opened lazily and
reused. Broadcast is now a journaled schema migration: it excludes new ordinary
work, waits for previously admitted operations to drain, and uses dedicated
migration connections rather than reserving every shard pool.

Rust callers may explicitly parse SQL and consume the result with
`validate_common_subset(ParsedSql)`, receiving an owned opaque `CommonSql` on
success. Before consuming it, callers may borrow it with
`classify_statements(&CommonSql)`, receiving an ordered, source-redacted
read/write/schema/session classification. Empty input is `InvalidArgument`;
multi-statement input is accepted only when every statement is a read. They may
then opt into `normalize_placeholders(CommonSql)`, receiving
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
shard-key updates, applies the complete batch gate, and exposes both the
selected `behavior()` and accepted `assigned_shard()`. The synchronous plan API
records schema and routing provenance but does not execute anything. Direct
shard-key inference remains statement-local and does not grant batch permission.

Rust callers can instead create a `PrepareRequest` with an explicit logical
database, dialect, translation mode, and SQL string. `Engine::prepare_statement`
runs the complete frontend pipeline, requires exactly one top-level statement,
classifies it, transiently compiles metadata on shard 0, and caches only
BriskDB-owned SQL, behavior, and metadata in the session. Persistent schema SQL
is denied during that compile and publishes no handle. `bind_statement`
snapshots typed values and the session's current route into an immutable portal
after transiently validating a plan from those concrete values.
`describe_prepared` returns owned `Unknown` parameter/result types and refreshes
column metadata after a schema-generation change.
`execute_portal` always plans again from the retained values and route snapshot
under the current schema guard. Logical behavior, rather than SQLite result
columns, decides whether the target is a read, write, schema change, or session
control. It runs accepted sharded work on its assigned shard and safe
`NotApplicable`/`Global` reads on deterministic shard 0, returning routed rows
or an affected-row count. Schema/session execution and sharded reads requiring
scatter remain unsupported on that compatibility method. The corresponding
logical portal method uses the same fresh plan but gathers a supported
multi-owner or unconstrained Sharded read from the relevant physical files.
It concatenates shard results in physical-shard order, retaining duplicate
rows exactly as `UNION ALL`; a `Global` read still executes once on shard 0.
There is no implicit cache eviction, no retained plan, `rusqlite` statement, or
connection, and closing a statement closes all of its portals.

Translation and planning remain independently callable branches over the same
normalized request. With an empty table catalog, HTTP execute/query retains the
legacy caller-routed raw SQLite path. Once tables are registered, those same
endpoints require exactly one SQLite common-subset statement, normalize its
placeholders, apply strict SQLite translation, and infer its authoritative
placement. Writes still require one owner. Reads select their logical targets
from metadata: an exact key visits one owner, finite keys visit each distinct
owner, an unconstrained Sharded read visits every shard, and a `Global` or
table-free read visits shard 0 once. The migration endpoint keeps its separate
exact-text journal identity and parameterless batch contract. The exact
translation matrix and strict-mode boundary are in
[`docs/SQL_TRANSLATION.md`](docs/SQL_TRANSLATION.md), and the complete lifecycle
is in
[`docs/SQL_PREPARED_STATEMENTS.md`](docs/SQL_PREPARED_STATEMENTS.md).

`EngineOptions` permits 1–16 active connections and 1–1,024 queued operations
per shard, with at most 512 active connections across all shards.
SQLite statements that can leave connection-local state remain uncontracted
empty-catalog pass-through behavior; the populated-catalog common subset rejects
those forms. Such connections, and connections left in a transaction, are
retired rather than reused by another session. Clean read handles can be
shared for ordinary SQL, but a deny-only authorizer probe moves connection-local
SQL such as `PRAGMA data_version`, plus any cross-owner write, to a fresh
disposable handle before execution.
The read-only `table_list` metadata PRAGMA is an explicit tested exception: it
does not change connection state and remains reusable for admin discovery.
BriskDB-owned shard metadata and storage-control PRAGMA mutations, including
`application_id`, `user_version`, `journal_mode`, `schema_version`, and
`writable_schema`, are denied through every client SQL surface rather than
allowed to invalidate the storage layout. Persistent DDL, including
`ALTER TABLE`, is denied through ordinary routed SQL and is allowed only inside
the journaled migration path, where BriskDB protects and revalidates its
reserved `briskdb` and `briskdb_*` namespaces.
A handle that performed an ordinary write may return to the same session,
preserving SQLite write counters, but is replaced before a different observable
session can inspect `last_insert_rowid()`, `changes()`, or `total_changes()`.
Planner-validated populated-catalog HTTP writes are the narrow exception: they
may reuse a shared stateless write handle because that SQL boundary rejects
connection-local functions and session state before checkout, while catalog
validation rejects the same functions in stored schema expressions. Older
catalogs are rechecked at startup. A later ordinary session still replaces the
handle before inspection. Those functions remain uncontracted across calls
until sessions gain connection pinning.
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
not contain passwords, tokens, or other sensitive literals. Once the catalog is
registered, migration rejects row-moving DML, table drops, trigger creation,
unsafe or malformed foreign keys, virtual tables, and any change that breaks
declared placement, Text collation, or one-owner uniqueness.

The initial shard count is immutable, so resharding will require an explicit
migration workflow. Opening upgrades exact version-1 through version-9
manifests to version 10 through ordered, resumable steps.
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

Version 8 makes newly registered table placement authoritative and raises the
downgrade fence. Because version-7 table rows were only advisory, the v7-to-v8
upgrade clears them instead of silently treating them as proven declarations;
it preserves logical databases, routing, schema history, shard files, and rows.
`Database::register_tables` can then install one complete declaration set only
after its matching physical tables are empty. Once installed, migrations must
preserve that exact physical table set and every sharded key's required column
and affinity. Text shard keys use SQLite `BINARY` collation. Foreign keys are
accepted only when authoritative placement proves local enforcement (matching
co-sharded keys with the same generated-ID routing domain, Sharded-to-Global,
or Global-to-Global); unsafe relationships, triggers, and virtual tables remain
unsupported. Every
`PRIMARY KEY` or `UNIQUE` key on a sharded table must include its shard key with
`BINARY` collation so uniqueness has one physical owner. The public catalog
returned by `Database::catalog()` and `Engine::catalog()` remains read-only to
observers.

Version 9 adds one explicit generated-ID policy row per registered table and an
immutable allocation-owner slot for every physical shard. The v8-to-v9
migration preserves routing, placement, schema history, shard files, and rows;
it assigns existing tables policy `None`, seeds `owner_slot = physical_shard_id`,
raises the downgrade fence, and moves the semantic manifest checksum to version
2 so both new catalogs are covered. This version defines the persisted
`native_range_v1` ID format but does not activate allocation.

Version 10 adds generated-policy activation, active/retired allocation-owner
lifecycle, and a checksummed table-provisioning journal. The v9-to-v10 upgrade
preserves every policy but marks it inactive, retains each prior owner as the
active owner of its shard, raises the downgrade fence, and advances the
semantic manifest checksum to version 3. Every later owner successor for a
physical shard must have a greater slot than all of that shard's retired
owners, preserving SQLite's non-decreasing `AUTOINCREMENT` high-water mark.
Explicit activation requires the shard key to be exactly `INTEGER PRIMARY KEY
AUTOINCREMENT` on every shard.
Registration first commits the complete provisioning request, then installs
each owner's reserved `sqlite_sequence` floor while all tables are empty, and
publishes authority only after every floor is durable. Startup replays the
journal's exact durable prefix idempotently. Reopen and shard admission validate
the sequence range, owner-local rows, and high-water mark before use. Encoded
explicit IDs route through active or retired persisted owners; new explicit IDs
are rejected for retired owners, while marker-clear and negative legacy IDs
keep their original hash route. An internal coordinator seam can allocate and
capture one preflighted omitted-key row on an eligible active shard. The shared
AST and bound planner now authorize that seam for one omitted-key row, and
public Engine, prepared-portal, and HTTP execution expose the captured result
when both experimental coordinator gates are enabled. The exact contract is in
[`docs/GENERATED_KEYS.md`](docs/GENERATED_KEYS.md).

Version 11 adds optional `hilo_v1`, a manifest-leased global sequence for one
registered Sharded table. The generated column must be the table's exact
visible `Int64` shard key and physically `INTEGER PRIMARY KEY` without
`AUTOINCREMENT` on every shard. One immediate manifest transaction reserves a
fixed block of 4,096 sequences before any target-shard write lock; an in-memory
allocator then consumes that committed block and hash-routes each encoded ID
across the ordinary persisted bucket map. The value interval is
`0x2000_0000_0000_0001..=0x3fff_ffff_ffff_ffff`, disjoint from
`native_range_v1`. A monotonic fence and random 32-byte process incarnation
identify each committed lease without clocks or expiry. Committed ranges are
never reclaimed: restart, crash, rollback, cancellation, and constraint
failure may burn an ID or unused range tail, so gaps are expected and numeric
order is allocation order, not commit order. The allocator promises uniqueness
and non-reuse, not gapless IDs or global commit ordering. Explicit inserts may
still use negative and positive pre-marker legacy IDs, which hash-route normally;
the complete hi/lo namespace is allocator-owned and rejected when supplied by
the caller. The lower coordinator seam is also consumed by shared omitted-key
planning; protocol-neutral results and HTTP rendering are implemented.
PostgreSQL and MySQL wire mappings remain coordinated with issues #33 and #44
respectively.

Version 12 adds the retained generated-table DDL bridge used by
`Database::apply_generated_table_ddl`. One checksummed singleton stores the
exact source dialect and bytes, canonical physical SQLite SQL, fields that
reconstruct the derived `native_range_v1` declaration, independent
logical/physical/provisioning identities, the provisioning-time schema digest,
lifecycle, and final catalog table ID. The v11-to-v12 migration is
manifest-only: it creates the empty
bridge table, raises the downgrade fence, and advances the semantic manifest
checksum to version 5 without changing any shard schema or application row.
New bridge work atomically begins its canonical schema-migration row with the
logical request, resumes physical and provisioning prefixes after restart, and
retains the completed record for audit and exact retry even after a later
schema migration. See [the generated-key contract](docs/GENERATED_KEYS.md).

Registration marks schema admission `Pending` before its manifest commit. If
that commit reports an ambiguous cleanup or I/O failure, close the registering
handle and reopen the data root; a durably committed replacement cannot be
loaded while the stale pre-registration handle remains live. Do not retry a
new registration through that pending handle.

For a `Sharded` table, each logical row has exactly one owner selected from its
canonical shard-key bytes; storing the same ordinary row on several shards is
not sharding. `Global` is the explicit replicated placement, while `Catalog`
is manifest-only. Registration establishes the authority used by later import
and execution work; it does not repartition existing data.

Supported logical reads that are not pinned to one key now gather the relevant
physical files with at most eight shard tasks. Sharded rows remain stored once,
on the metadata-selected owner; the logical result concatenates per-shard rows
in physical-shard order with `UNION ALL` semantics, including duplicates.
Exact-key reads visit one owner, finite key sets visit only distinct owners, and
`Global` data is read once from canonical shard 0. One deadline, cancellation
signal, and combined result budget cover the complete operation; any shard
failure returns an error rather than a partial result. This baseline is
row-local: multi-shard `DISTINCT`, aggregate, grouping, ordering, pagination,
join, subquery, CTE, set-operation, and window semantics remain rejected until
their dedicated planner work lands. The admin browser uses a separate,
server-generated logical count and shard-major paging plan; it does not expose
arbitrary aggregate, ordering, or pagination SQL.
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

The `/admin` browser's fixed development login is separate from the planned
authentication and role model. Near-term work includes that model, richer
migration administration and status APIs in issue #53, richer global logical
query semantics, observability, and backup tooling.

## License

Copyright (c) 2026 Stephen Chapman. BriskDB is distributed under the
[MIT License](LICENSE).
