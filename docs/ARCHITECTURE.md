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
| `core` | Protocol-neutral `Engine`, `Session`, statements, values, results, errors, and read-only logical catalog; stable key routing; bounded per-shard admission and connection pools; routed execute/query and journaled schema migration | JSON/HTTP types, listeners, or Axum handlers |
| `storage` | Versioned routing/logical manifest, shard layout, migration journal and recovery, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `sql` | SQLite statement execution and conversion between SQLite storage classes and BriskDB values | JSON, routing, filesystem layout, or protocol responses |
| `protocol::http` | HTTP request extraction plus JSON/BriskDB value and RFC 9457 problem-detail encoding | BLAKE3 routing, shard files, or rusqlite calls |
| `protocol::error` | Exhaustive HTTP, PostgreSQL, and MySQL mappings from stable engine error kinds | SQLite errors, routing decisions, or wire-protocol session state |
| `server` | Process configuration, database assembly, listener binding, and tracked Axum HTTP/1 connection lifecycle | SQL parsing or storage implementation details |

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

Issue #17 intentionally changed the behavior behind the preserved
`/v1/admin/broadcast`, `Database::broadcast`, and `Engine::broadcast` shapes.
They now submit one journaled application-schema migration instead of an
untracked sequential batch. The HTTP success body remains
`{"completed_shards":[...]}`. A retained `Catalog` reference now observes a
durably published generation in place; consequently, its public
`schema_generation` accessor is no longer usable in a Rust `const` context.
That is an intentional pre-1.0 source-level change.

The module names are stable boundaries, not a claim that later roadmap work is
already complete. The async engine, session lifecycle, bounded per-shard pools,
request controls, and explicit shutdown lifecycle are now in place. The
synchronous `Database` API remains available as a Rust compatibility surface;
existing engine and server entry points retain their signatures and delegate to
the controlled defaults.

## Manifest storage boundary

The storage module owns an ordered manifest-format migration runner. It
identifies a current manifest with SQLite `application_id = 0x42524442` and uses
`user_version` as the single authoritative schema version. Version 2 replaced
the legacy key/value configuration with a strict singleton shard-count table.
Version 3 added the durable routing catalog: independently versioned hash, key
encoding, and bucket derivation; the initial map generation; exactly 4,096
virtual buckets; and contiguous, active physical-shard records. Version 4 adds
logical databases, table metadata, and an application-schema generation. Its
initial catalog has schema generation 0, identifier encoding version 1, and
default logical database ID 1 named `default`. Version-1 identifiers are 1 to
63 bytes of lowercase ASCII, begin with a letter or underscore, and exclude
the reserved `briskdb`, `briskdb_*`, and `sqlite_*` namespaces. Table metadata
records a stable positive ID, owning database, name, and one of sharded,
global, or catalog placement; only a sharded table also records one `Int64`,
text, or binary shard-key column.

Version 5 adds physical-layout identity and recovery state. The manifest's
`briskdb_shard_layout` singleton stores one random 16-byte layout ID, the
expected `BRSH` shard application ID, metadata encoding version 1, and state
code 1 (`Creating`), 2 (`Adopting`), or 3 (`Ready`). Every current shard has
the same layout ID in its exact BriskDB-owned metadata row, its cataloged
physical shard ID, `application_id = BRSH`, and `user_version` equal to the
cataloged application-schema generation. The layout ID catches
accidental copies, swaps, and cross-layout placement; it is not a secret,
checksum, or security boundary.

Version 6 adds the retained `briskdb_schema_migrations` journal and expands the
catalog generation from fixed zero to the range 0 through 2,147,483,647. Each
row records consecutive source and target generations, the shard count, an
ascending durable prefix, state `Applying` or `Complete`, the exact SQL text,
and its digest. Digest version 1 is the full BLAKE3 digest of the exact UTF-8
SQL bytes. Input is limited to 1 through 65,536 bytes with no NUL. Completed
history is contiguous and retained permanently; at most one active row may
target the generation immediately after the committed catalog. Exact SQL is
therefore operational metadata that may reveal sensitive literals.

Version 7 adds the `briskdb_integrity` singleton. It stores a canonical BLAKE3
semantic root over authoritative manifest values, a generation-bound BLAKE3
fingerprint for the committed application schema, an optional migration-target
fingerprint, and one of four durable states: `Verifying`, `Ready`, `Migrating`,
or `Degraded`. Manifest mutations reseal the semantic root inside their own
transaction. Startup establishes the first fingerprint or requires the existing
trusted value, and every later shard connection verifies it. A migration stores
source and target fingerprints before its first shard commit and verifies the
exact journal prefix during recovery. The full input encodings and state
invariants are frozen in the
[manifest storage format](STORAGE_FORMAT.md).

Each manifest version retains an intentionally incompatible
`briskdb_metadata` definition and row as a downgrade fence. The v3-to-v4
migration remains manifest-atomic. The v4-to-v5 step first validates the v4
source and commits state `Adopting`, fencing older binaries before any shard is
changed. Fresh initialization commits `Creating` only for an otherwise empty
layout. Cross-file work then proceeds one shard at a time and a final manifest
transaction moves to `Ready` only after strict revalidation of the complete
layout. A failure or panic leaves `Creating` or `Adopting` durable, so the next
open resumes instead of guessing whether a missing or partly stamped file is
safe.

The v5-to-v6 step is manifest-only: it preserves layout state, routing, logical
metadata, and data while rebuilding the schema-generation constraint, creating
an empty journal, and fencing v5 readers. The v6-to-v7 step is also
manifest-only and begins in `Verifying`; it cannot manufacture a historical
checksum that v6 never stored. There is no automatic downgrade; an older binary
requires a backup from before the newer format.

Startup first canonicalizes the data-directory path and joins the process-wide
root coordination keyed by that path. It acquires the shared schema gate before
loading the manifest, so independent `Storage`, `Database`, and `Engine` handles
in one process serialize startup and migration against ordinary admission and
share catalog-generation publication.

Each manifest connection enables and reads back SQLite cell-size checks and
requires a full manifest integrity check before parsing control-plane state.
Manifest loading then acquires `BEGIN IMMEDIATE` before making a format
migration or layout-state decision. Numbered manifest-only steps rewrite
schema/data, stamp and read back their target identity/version, validate the
destination, and commit in their own transaction. An `Applying` v6 migration is
finished under v6 rules before v7 establishes checksum authority. An active v7
migration is resumed only after every shard matches the preserved source or
target fingerprint for its exact journal-prefix position.

Layout reconciliation then acquires a new immediate manifest transaction,
re-reads and validates the layout state under that write lock, and holds the
lock through independently durable per-shard work and `Ready` publication. A
lagging opener re-reads `Ready` and strictly validates instead of provisioning
from a stale `Creating` observation. Only a locked, durable `Creating` state
permits missing canonical shard files to be created and WAL to be enabled. The
final strict shard opens and catalog reconciliation complete before the startup
guard publishes `Ready`; ordinary work is never served against a persisted
mixed-generation prefix. A first v7 open treats the consensus across all strict
generation-bound shard-schema fingerprints as its trust-on-first-upgrade
baseline. Later opens require that existing trusted fingerprint. A durable
`Degraded` state is terminal; recovery replaces the complete manifest and
shard set from one known-good consistent copy rather than rebaselining it.

`Adopting` recognizes only existing legacy shard files with exact zero
application-ID/user-version headers and an existing WAL mode. It writes current
identity metadata without changing application tables or rows. `Ready` and all
runtime connection opens use read-write, no-create, no-follow SQLite flags and
require the exact path, layout ID, shard ID, application ID, metadata encoding,
schema generation, and WAL mode. Missing, extra canonical, foreign, non-WAL,
and wrong-generation files fail closed, as do swapped files and files cloned
into a wrong slot or layout. Every shard open also enables cell-size checks,
checks the BriskDB metadata table with SQLite's table-scoped integrity check,
and verifies the persistent application-schema fingerprint. WAL and
shared-memory sidecars are transient and are not required layout members.

This is an internal storage-open concern, is unreachable from client SQL, and
is atomic only within `manifest.sqlite`. Validation returns routing and logical
metadata from the same locked transaction as one shared snapshot. The migration
coordinator publishes a newly committed generation into that snapshot only
after every shard verifies. `Database::catalog()` and `Engine::catalog()` expose
the logical portion as a read-only `Catalog` with lookup accessors.

The logical catalog remains advisory: there is no catalog mutation API,
planner integration, or schema enforcement yet. Fresh manifests and upgrades
originating before v4 contain no table rows; a v4-to-v5 upgrade retains every
validated v4 logical-catalog row. Schema migration does not inspect, infer, or
mutate `briskdb_tables`; instead, v7 independently requires the exact
`sqlite_schema` fingerprint to agree across shards without claiming the
advisory catalog describes it. Existing tables remain reachable through the
explicit-key execute/query surfaces. Core
routing still hashes the exact caller-provided key bytes, derives a versioned
virtual bucket, and reads the final physical shard from the snapshot without
querying SQLite. The generation-1 ranges reproduce prior modulo placement for
every supported initial shard count, including counts that do not divide 4,096.
Version-5 adoption adds only BriskDB identity metadata to legacy shards and
preserves their application schema and data. It changes no SQL planning, HTTP
shape, or routing result.

The storage-owned `briskdb_shard_metadata` table is inaccessible through client
SQL, and creation of new objects in the reserved `briskdb` or `briskdb_*`
namespaces is
denied by the SQLite authorizer. Client attempts to mutate `application_id`,
`user_version`, persistent `journal_mode`, `schema_version`, or
`writable_schema` are also denied. This prevents a pass-through statement from
invalidating the validated layout. Ordinary routed SQL also denies every
persistent DDL action. The journaled migration connection is the sole exception:
it allows main-schema DDL and DML, including `ALTER TABLE`, inside BriskDB's
transaction while denying transaction escape, attachments, temporary/virtual
objects, and reserved-state access. Because SQLite does not reveal an
`ALTER TABLE ... RENAME TO` destination to the authorizer, the coordinator also
compares the reserved schema before and after the batch. The exact format,
numeric codes, downgrade policy, recovery cases, and tests are documented in
[manifest storage format](STORAGE_FORMAT.md).
Integrity failure marks the canonical-root admission gate sticky `Degraded`;
ordinary operations, status calls, and migrations then fail with
`DataCorruption`, and a trusted manifest records that terminal state when
possible. BriskDB exposes no repair, rebaseline, or detailed integrity status
API here; richer migration administration and status surfaces remain issue
#53.

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
Schema-migration broadcast and status calls also go through the shared engine,
but do not perform a routing decision in the adapter.

### Bounded worker and connection-pool boundary

The local engine owns one independent pool per physical shard. `EngineOptions`
defaults each pool to four active connections and a queue of 32 admitted
operations. Connections are created lazily, up to the active limit, and are
reused after successful cleanup. Admission occurs on the asynchronous side
before work is handed to a Tokio blocking worker, so waiting for a shard slot
does not occupy an unbounded set of blocking threads.

Custom options allow 1–16 active connections and 1–1,024 queued operations per
shard. Construction also enforces an aggregate limit of 512
active connections (`shard_count * connections_per_shard`). Existing
constructors and server assembly use the defaults, so their APIs and behavior
remain compatible. Server configuration exposes
`--connections-per-shard` / `BRISKDB_CONNECTIONS_PER_SHARD` and
`--queue-capacity-per-shard` / `BRISKDB_QUEUE_CAPACITY_PER_SHARD`.
The same options boundary carries finite result rows/bytes, the optional
engine-wide request timeout, and the shutdown grace period. The binary exposes
them as `--max-result-rows`, `--max-result-bytes`,
`--request-timeout-ms`, and `--shutdown-grace-ms` with corresponding
`BRISKDB_*` environment variables.

When a shard has no active slot and its admission queue is full, a new operation
fails immediately with retryable `Busy`, which the HTTP adapter maps to 503.
Capacity for routed work belongs to its selected shard: saturation on shard A
neither consumes shard B's slots nor delays work already admitted there.

Schema migration uses a separate, shared admission gate. Transitioning from
`Ready` to `Migrating` immediately rejects new ordinary operations and a second
coordinator with retryable `Busy`, then asynchronously waits for already
admitted work to drain. The engine retires idle pooled handles and performs the
migration on fresh coordinator-owned connections; it no longer reserves one
slot in every shard pool. If a durable journal survives an error, panic,
cancellation, or dropped future, the gate becomes `Pending`. Ordinary work then
receives non-retryable `FailedPrecondition`, while a new migration call may
enter `Migrating` to resume the byte-identical SQL. Startup recovery completes
an active journal while holding the same in-process gate. Independent handles
for the same canonical root share the gate and live catalog publication.
Separate server processes for one data directory are unsupported; the gate is
not a distributed coordination mechanism.

After every-shard preflight succeeds, the coordinator records the exact SQL and
its BLAKE3 identity, then visits shards in ascending order. One shard's complete
batch and target `user_version` commit atomically, followed by a separate
manifest prefix update. There is no cross-shard transaction. Recovery accepts
the committed prefix plus the single possible shard commit whose acknowledgement
was interrupted, and never skips ahead. Finalization marks the retained row
complete and publishes the catalog generation only after every shard validates.
A byte-identical retry is idempotent; alternate whitespace or casing is a new
migration identity.

Pool checkout also establishes a connection-hygiene boundary. SQLite authorizer
events identify operations that can persist connection-local state, including
transaction and savepoint control, `PRAGMA`, `ATTACH`/`DETACH`, and temporary
objects. BriskDB-owned metadata access and storage-control PRAGMA mutations are
always denied. Other connection-local operations remain allowed under the
current one-call SQLite pass-through behavior, but that behavior is
uncontracted. Clean read handles may cross sessions for ordinary statements.
The pool retains the first session associated with each physical handle; an
ordinary foreign read does not relabel that history. Before a routed statement
uses such a foreign handle, the engine prepares it under a deny-only authorizer
probe. The first connection-local or write action is rejected before it can
run—even for PRAGMAs with prepare-time effects—and the real statement is then
executed once on a fresh handle. This also gives every cross-owner write clean
SQLite counter state.
The expected probe error is never exposed to the caller. Any other probe error
also fails closed to a fresh handle. Opening that replacement can surface its own
storage error; otherwise only the real execution determines the caller-visible
SQL result. The schema-migration path is outside this pool-owner probe. Before
publishing its journal, it executes the complete batch on every shard in a
rollback-only transaction, so later statements may depend on earlier schema
changes while a failure still leaves every shard unchanged.

A connection marked tainted by real execution is closed after the call instead
of returning to the pool. If a call leaves a transaction open, rollback is
attempted for cleanup and the connection is likewise retired; cleanup failures
also prevent reuse. Thus connection-local state and observer metadata such as
`PRAGMA data_version` cannot leak from one ephemeral HTTP `Session` to another.

Ordinary writes require a narrower rule because SQLite exposes per-connection
`last_insert_rowid()`, `changes()`, and `total_changes()` state. The pool records
the owning BriskDB session after any authorizer-observed insert, update, or
delete. That physical handle remains reusable by the same session, preserving
its SQLite semantics, but a checkout for any other session retires and replaces
it before SQL runs. Handles used only for reads remain reusable across sessions.
This ownership is a leakage-prevention rule, not connection pinning: a competing
session can replace an idle write-bearing handle, so write-counter functions
remain uncontracted across calls until transaction/session pinning is added.

Every operation acquires a lifecycle lease before its first await. Dropping a
queued future removes the operation before SQLite starts. Once work is running,
the future's drop guard interrupts the exact leased SQLite handle; the blocking
closure retains lifecycle, worker, pool, and session permits until rollback and
connection cleanup really finish. The lease-scoped progress callback and
interrupt handle are removed before check-in, and interrupted connections are
retired, preventing a late signal from affecting the next request.

`RequestContext` supplies a sticky cancellation token, an optional absolute
deadline, and optional narrower result limits. The engine default deadline and
result budget are owned by `EngineOptions`, so protocol adapters contain no
SQLite control policy. Queries account a stable logical result representation
while stepping SQLite and before cloning payloads. A row or byte overflow
returns no partial `ResultSet`; query statements must be read-only so early
termination cannot hide DML `RETURNING` effects. The exact accounting contract
is documented in [request controls](REQUEST_CONTROLS.md).

All engine clones share one mutex-protected lifecycle. The mutex makes the
`Running` admission check and active-operation increment atomic with
`begin_shutdown()` changing the state to `Draining`. New work then receives
`ShuttingDown`; admitted leases drain. After the grace period, shutdown cancels
the admitted set and still waits for blocking cleanup before closing idle
SQLite handles on a worker and marking `Stopped`. A timed-out cleanup remains
`Draining` and can be resumed. Ordinary clone destruction does not initiate
this explicit asynchronous path.

The server owns accepted HTTP/1 connections in a tracked task set. It stops
engine admission before dropping the listener, starts HTTP and core draining
together, and aborts then joins any connection that exceeds the HTTP grace
deadline. Signal receivers are installed before readiness is logged. Dropping
the server future aborts its tracked connection set and synchronously enters
`Draining`; a surviving embedder-owned `Engine` clone can resume asynchronous
cleanup with `shutdown()`.

Real multi-call `BEGIN`/`COMMIT`/`ROLLBACK`, failed-transaction state, and
single-shard pinning remain deferred to the PostgreSQL and MySQL transaction
work in issues #34 and #47. `Ready` and `Closed` therefore describe session
lifecycle, not SQL transaction state.

The pool/request-control boundary changed Rust orchestration and added opt-in
`EngineOptions` plus pool-sizing CLI/environment configuration. That earlier
change did not alter option defaults, HTTP routes or JSON shapes, shard routing,
storage formats, WAL or synchronous settings, or stored data.

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
SQLite messages, SQL text—including SQL retained in a migration journal—and
filesystem paths do not leak through an adapter.
Only `Busy` advertises that retrying may succeed; a 5xx status alone is not a
retry signal. The complete taxonomy and mapping table are in the
[error contract](ERRORS.md).

The error boundary changes reporting, not persistence; storage-format changes
are owned and documented separately by the manifest boundary above.

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
