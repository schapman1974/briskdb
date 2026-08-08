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
    |                    v
    +-----------------> core
    |                 /    \
    |                v      v
    |            storage    sql
    |
    +-- PostgreSQL TCP lifecycle
        (accept/close placeholder; no core request)
```

| Module | Responsibility | Must not own |
| --- | --- | --- |
| `core` | Protocol-neutral `Engine`, `Session`, statements, immutable bound portals, values, results, errors, read-only logical catalog, synchronous bound-value-aware plans, prepared lifecycle, and sharded routing policy; stable key routing; bounded per-session and per-shard admission; routed execution and journaled schema migration | JSON/HTTP types, listeners, or Axum handlers |
| `storage` | Versioned routing/logical manifest, shard layout, migration journal and recovery, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `sql` | Dialect-explicit SQL syntax parsing, recursive common-subset validation, protocol-neutral statement/batch classification, source-preserving placeholder normalization, explicit strict/compatibility translation, catalog-aware typed shard-key inference, and narrow crate-private DML-shape inspection behind BriskDB-owned boundaries; exact source retention; SQLite statement execution and conversion between SQLite storage classes and BriskDB values | JSON, key hashing or shard selection, mutable session state, physical write-routing policy, filesystem layout, protocol responses, protocol-buffer ownership, or protocol-specific support policy |
| `protocol::http` | HTTP request extraction plus JSON/BriskDB value and RFC 9457 problem-detail encoding | BLAKE3 routing, shard files, or rusqlite calls |
| `protocol::error` | Exhaustive HTTP, PostgreSQL, and MySQL mappings from stable engine error kinds | SQLite errors, routing decisions, or wire-protocol session state |
| `server` | Process configuration, database assembly, separate HTTP/PostgreSQL listener binding, tracked Axum HTTP/1 connection lifecycle, and the temporary PostgreSQL accept/close loop | SQL parsing, PostgreSQL wire messages, or storage implementation details |

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
already complete. The async engine, session and prepared-object lifecycle,
bounded per-session caches, bounded per-shard pools, request controls, and
explicit shutdown lifecycle are now in place. The synchronous `Database` API
remains available as a Rust compatibility surface; existing engine and server
function signatures remain in place and delegate to the controlled defaults.
Issue #28 adds `Config::postgres_listen: Option<SocketAddr>`, so pre-1.0 Rust
callers constructing `Config` with a struct literal must now choose an enabled
address or `None`.

## Listener boundary

The HTTP address remains `Config::listen`. The independent
`Config::postgres_listen` is either a numeric socket address or disabled with
`None`; the binary maps the exact `disabled` CLI/environment sentinel to that
option. The process default enables loopback `127.0.0.1:5433`. See the
[PostgreSQL listener contract](POSTGRES_LISTENER.md) for the full grammar and
startup order.

Until the subsequent wire-adapter work, the PostgreSQL listener is a server
lifecycle placeholder: it accepts and immediately drops each stream without
reading bytes, writing a PostgreSQL frame, opening a session, or calling the
engine. This keeps binding, concurrent acceptance, startup cleanup, and shared
shutdown testable without moving protocol behavior into `server`.

## SQL parser boundary

The SQL module owns a single parser entry point with explicit SQLite,
PostgreSQL, and MySQL dialect selection. It retains exact source text and wraps
the selected parser's ordered AST without exposing dependency-owned AST types
to protocols or the engine's public request model. There is no generic dialect,
autodetection, or fallback parse. This makes a frontend's dialect choice
deterministic and keeps the dependency replaceable.

Parsing is syntax recognition, not support validation or planning. The parser
has no session, parameter, catalog, storage, or routing access. The separate
common-subset validator, statement classifier, placeholder normalizer,
shard-key inference layer, and later planner layers consume structural syntax
through BriskDB-owned interfaces; shard-key inference does not inspect raw or
formatted SQL with regular expressions. Exact input remains authoritative
because AST formatting is lossy and is never executed.

Inputs are bounded to 65,536 UTF-8 bytes, 256 statements, and recursion depth
32. The dependency's recursive-protection feature remains enabled. Parse and
limit failures use protocol-neutral engine error kinds, whose diagnostics stay
internal. The full dependency, error, testing, and non-goal contract is in the
[SQL parser decision record](SQL_PARSER.md).

### Common-subset boundary

`validate_common_subset(ParsedSql)` consumes an opaque parsed batch and returns
an owned opaque `CommonSql` only when every statement recursively uses the
documented first subset. The marker retains the exact source, dialect, and
statement count without exposing the upstream AST. Parsed but unsupported
statement or expression forms return `Unsupported`; parse and parser-limit
failures retain their existing kinds.

Validation is structural and stateless. It does not normalize placeholders,
translate type names or syntax, inspect parameters or the catalog, infer a
shard, build a plan, classify statement behavior, authorize an endpoint, or
execute SQL. Empty and mixed batches can validate because the separate
classifier owns request-level batch policy. Recursive expression validation has
an independent depth limit of 128 so iteratively parsed flat operator chains remain bounded.
The normative accepted forms and exclusions are in the [common SQL subset
contract](SQL_SUBSET.md).

The current HTTP execute/query and migration paths deliberately remain raw
SQLite pass-through and call none of the parser, subset validator, statement
classifier, placeholder normalizer, translator, shard-key inference, bound
statement-planning, or prepared-lifecycle layers. Issues #19 through #27
therefore change no HTTP shape, SQL acceptance, execution routing, or storage
behavior. Rust callers can now invoke those layers together through the
separate prepared API.

### Statement-classification boundary

`classify_statements(&CommonSql)` borrows the validated opaque AST and returns
an owned ordered `StatementBatchClassification`. Each top-level statement is
classified as `Read`, a precise `Write` (`Insert`, `Update`, or `Delete`), a
precise `Schema` change (`CreateTable` or `CreateIndex`), or a precise `Session`
control (`Begin`, `Commit`, or `Rollback`). The result exposes ordered behavior,
count, indexed lookup, and whether the accepted batch is wholly read-only,
without exposing or rendering SQL or the dependency-owned AST.

Empty input is `InvalidArgument`. A singleton of any behavior is classified,
but a batch of two or more statements is accepted only when every member is a
read; the first non-read member makes the whole batch `Unsupported`. This is
logical request policy, not execution permission. The classifier has no
catalog, parameters, routing, session, storage, or SQLite access and opens no
connection.

The general planner applies this complete batch gate before planning a selected
member and retains that member's behavior. Direct shard-key inference remains
statement-local. Prepared statements keep their stricter exact-one rule, then
retain the singleton behavior for descriptions and execution target policy.
The normative taxonomy, matrix, errors, integration, and tests are in [SQL
statement and batch classification](SQL_STATEMENT_CLASSIFICATION.md).

### Placeholder-normalization boundary

`normalize_placeholders(CommonSql)` consumes the owned subset marker and
returns an owned `NormalizedSql`. It retains the exact source and produces a
separate `sqlite_parameter_sql()` representation in which only accepted
placeholder spans become canonical SQLite `?N` markers. PostgreSQL `$N`
retains `N`; MySQL `?` is numbered consecutively; SQLite `?` and `?NNN` use
SQLite's max-so-far rule. Numbering restarts per top-level statement and is
bounded by `MAX_SQL_PARAMETERS` (32,766).

Each statement, including one without placeholders, has an ordered
`StatementParameters` record with its largest assigned index, occurrence
count, and occurrence-to-index sequence. This metadata lets later bind-time
planning distinguish repeated parameters and gaps without owning protocol
buffers. Prepared bind uses that occurrence sequence for its conservative
planning-expansion preflight; normalization itself does not inspect the bound
values.

Normalization uses retained AST placeholder spans rather than regular
expressions or AST formatting. Every non-marker byte remains exact, including
comments, literals, whitespace, and UTF-8 text. SQLite named parameters are
deliberately unsupported. The layer has no session, catalog, storage, routing,
filesystem, or execution access and does not decide whether a statement batch
may execute; the classifier owns that policy. The normative API, dialect rules,
limits, errors, and tests are in the [SQL parameter-normalization
contract](SQL_PARAMETERS.md).

### SQL-translation boundary

`translate_sql(NormalizedSql, SqlTranslationMode)` consumes an owned normalized
request and returns an owned `TranslatedSql`. Strict mode accepts only SQLite
source and preserves `sqlite_parameter_sql()` byte-for-byte. Compatibility mode
clones the validated opaque AST, applies the finite dialect-specific type and
syntax matrix, resolves every retained placeholder to its existing `?N`
identity, and renders separate canonical SQLite SQL. Exact original source and
the complete normalized representation remain retained.

Compatibility integers canonicalize to `BIGINT` rather than exact SQLite
`INTEGER`, avoiding accidental `INTEGER PRIMARY KEY` rowid-alias behavior.
Boolean declarations/literals, variable text and binary declarations,
64-bit-real aliases, backtick identifiers, transaction aliases, and comma-form
limits have the only documented mappings. Other types and syntax remain
unsupported. Canonical compatibility rendering may normalize formatting and
comments; strict mode does not render the AST.

Translation is stateless and protocol-neutral. It accepts no catalog, values,
session, storage, or routing state and does not prepare or execute SQL. There is
no configured default mode yet. The full matrix, result, errors, non-goals, and
tests are normative in the [SQL translation contract](SQL_TRANSLATION.md).

### Shard-key inference boundary

`infer_shard_keys(&Catalog, LogicalDatabaseId, &NormalizedSql,
statement_index, parameters)` borrows an immutable catalog snapshot, one
normalized batch, and the selected statement's complete protocol-neutral
`Value` slice. It resolves the accepted base table and cataloged shard-key type,
then produces an owned `ShardKeyInference` classification and typed values.

For `SELECT`, `UPDATE`, and `DELETE`, the layer proves finite key sets only from
direct `Int64` or `Binary` shard-column equality and combines those sets through
Boolean `AND` and `OR`. Non-null `Text` equality remains unconstrained until the
catalog declares and physical schemas enforce comparison collation. For
`INSERT`, inference examines the explicit shard-key column in every `VALUES`
row and retains one value per row, including text values. The result
distinguishes statements that are not applicable, known non-sharded tables,
unconstrained predicates, contradictions, one exact key, and multiple keys.

This layer is catalog-aware and accepts bound values, but remains read-only and
statement-local. It does not own the catalog or caller values, encode or hash a
key, read the bucket map, choose a shard, construct an execution plan, apply
write policy, mutate session state, or execute SQL. The separate core planner
owns bind-time canonical encoding, physical routing, and rejection of
conflicting or unroutable sharded DML. The normative proof, type, result,
error, and testing rules are in the [shard-key inference
contract](SQL_SHARD_KEYS.md).

### Bound statement-planning boundary

Synchronous `Engine::plan_bound_statement` accepts a `NormalizedSql` batch, one
selected statement's complete bound `Value` slice, the selected logical
database, and an optional explicit routing byte sequence. It first applies the
complete statement/batch classifier, then invokes statement-local inference
only after the bound values exist, encodes every inferred value, and looks each
occurrence up through the validated routing catalog. The returned
`BoundStatementPlan` owns the selected `StatementBehavior`, inference, and one
`PlannedRoute` per inferred value in matching order, including duplicate
multi-row values and distinct keys that happen to select the same physical
shard.

Canonical version-1 inferred bytes are the shortest signed decimal ASCII form
for `Int64`, exact UTF-8 for `Text`, and exact bytes for `Binary`. Explicit
routing bytes are retained exactly. Inferred routes and the optional explicit
route remain separate in the result. Policy compares them by physical shard,
never by opaque bytes, and records `assigned_shard()` when one valid target
exists. Distinct logical keys co-located on one shard remain separate route
occurrences but form one physical assignment.

The public classifier supplies the precise selected read/write/schema/session
behavior. SQL also exposes a narrow crate-private DML shape, including whether
an `UPDATE` targets the cataloged shard-key column. Core uses those shapes to
reject unproven inserts, multi-shard writes, broad updates/deletes without
explicit fallback, and every shard-key assignment.

Planning holds the existing schema-operation guard while it reads logical and
routing state. The owned result records schema generation, map generation, and
the hash, key-encoding, and bucket-algorithm versions used for the lookup. That
provenance does not reserve future state. Prepared bind uses a transient plan
for validation; portal execution always creates a new plan from the portal's
owned bind snapshot under its current schema-operation guard.

The public planner remains stateless and its assignment alone is not execution
permission. It does not invoke translation, mutate a session, cache a prepared
statement, open a shard connection, scatter reads, or execute anything. It does
apply the shared batch gate so a mutating multi-statement request cannot reach
later planning through this boundary. The normative API, assignment matrix,
encoding, provenance, error, boundary, and testing rules are in the
[bound statement-planning and routing-policy
contract](SQL_PLANNING.md). Translation is the separate implemented issue #25
branch over the same `NormalizedSql`; the prepared lifecycle consumes the
classifier, translation, and planning layers.

### Prepared-statement and portal boundary

`Engine::prepare_statement` accepts an owned `PrepareRequest` with one logical
database, source dialect, explicit translation mode, and SQL string. It runs
parse, exact-one top-level validation, subset validation, statement
classification, placeholder normalization, and translation, then transiently
compiles metadata on shard 0. The session cache retains only BriskDB-owned
`TranslatedSql`, the precise singleton `StatementBehavior`, and owned
parameter/column metadata. No `rusqlite::Statement`, rows iterator, SQLite
connection, pool handle, or protocol buffer crosses the operation boundary.

Prepared-statement and portal IDs are opaque, monotonic, never reused, and
bound to their process-unique owning session. Binding validates concrete typed
values with a transient plan, snapshots the session routing bytes, and stores
only that snapshot and the values in an immutable logical portal. Later
session-route changes do not affect it. Describing after a schema-generation
change recompiles owned metadata on shard 0. Every execution plans again from
the portal snapshot under the current schema/routing guard before choosing the
supported physical target. Logical behavior, not SQLite result-column metadata,
distinguishes reads from writes, schema changes, and session control. Safe
`NotApplicable` and `Global` reads may use deterministic shard 0; a sharded
read that still needs scatter remains unsupported. Schema/session execution is
not implemented, and Catalog placement is never exposed as an application
target. `PreparedStatementDescription::behavior()` gives adapters the same
retained behavior.

Per-session limits independently bound statement count, portal count, and the
logical accounted value bytes plus routing bytes retained by all portals. Full
caches return `LimitExceeded`; there is no implicit eviction. Closing a
statement cascades to its portals, closing a portal releases its bytes, and
closing the session clears all prepared state. Same-session operations are
serialized by the existing session mutex, including SQLite metadata and
execution work.

The retained-value ceiling also preflights one bind's transient planning
expansion by charging its captured route once and each normalized marker
occurrence twice; repeated markers cannot cause unbounded inference/route
allocation before the check.

The prepared lifecycle integrates the classifier, translation, and planning
layers but deliberately does not execute a multi-statement batch, scatter
reads, execute schema/session statements, or implement transactions. Those
remain later planner/transaction milestones. The complete API, accounting,
execution, error, adapter, and persistence contract is in [prepared statements
and bound portals](SQL_PREPARED_STATEMENTS.md).

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
boundary without borrowing protocol buffers. Prepared statements and portals
are session-scoped logical objects; binding captures the routing context so a
later session change cannot retarget an existing portal. `EngineStatus` exposes
the shard count and prepared-state limits needed by health/configuration
reporting without exposing the storage implementation.

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
The same options boundary carries finite result rows/bytes, per-session
prepared-statement count, portal count and a retained-value/per-bind-planning
byte ceiling, the optional engine-wide request timeout, and the shutdown grace
period. Prepared defaults are 128 statements, 128 portals, and 16 MiB;
configurable hard caps are 1,024, 1,024, and 1 GiB. The binary exposes these as
`--max-result-rows`,
`--max-result-bytes`, `--max-prepared-statements-per-session`,
`--max-portals-per-session`, `--max-retained-bound-value-bytes`,
`--request-timeout-ms`, and `--shutdown-grace-ms`, with corresponding
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
SQLite control policy. Prepare, bind, describe, and portal execution expose the
same `*_with_context` boundary as raw operations. Queries and prepared row
results account a stable logical representation while stepping SQLite and
before cloning payloads. A row or byte overflow returns no partial
`ResultSet`; row-producing writes are rejected so early termination cannot hide
DML effects. The exact accounting contract is documented in
[request controls](REQUEST_CONTROLS.md).

All engine clones share one mutex-protected lifecycle. The mutex makes the
`Running` admission check and active-operation increment atomic with
`begin_shutdown()` changing the state to `Draining`. New work then receives
`ShuttingDown`; admitted leases drain. After the grace period, shutdown cancels
the admitted set and still waits for blocking cleanup before closing idle
SQLite handles on a worker and marking `Stopped`. A timed-out cleanup remains
`Draining` and can be resumed. Ordinary clone destruction does not initiate
this explicit asynchronous path. Prepared statement/portal close and terminal
session close remain available as in-memory cleanup while draining; no
prepared state is persisted for restart recovery.

The server owns accepted HTTP/1 connections in a tracked task set and manages a
separate optional PostgreSQL socket in the same accept lifecycle. It stops
engine admission before dropping both listeners, starts HTTP and core draining
together, and aborts then joins any HTTP connection that exceeds the HTTP grace
deadline. Placeholder PostgreSQL streams are already closed at accept time and
therefore own no drain task. Signal receivers are installed only after every
configured listener binds and before readiness is logged. Dropping the server
future closes both listeners, aborts its tracked HTTP connection set, and
synchronously enters `Draining`; a surviving embedder-owned `Engine` clone can
resume asynchronous cleanup with `shutdown()`.

Real multi-call `BEGIN`/`COMMIT`/`ROLLBACK`, failed-transaction state, and
single-shard pinning remain deferred to the PostgreSQL and MySQL transaction
work in issues #34 and #47. `Ready` and `Closed` therefore describe session
lifecycle, not SQL transaction state.

The pool/request-control and prepared-cache boundaries changed Rust
orchestration and added opt-in `EngineOptions` plus CLI/environment
configuration. They did not alter HTTP routes or JSON shapes, shard routing,
storage formats, WAL or synchronous settings, or stored schema. Prepared
execution can have the ordinary application-row effects of its one selected
SQLite statement.

## Error boundary

The core exposes a stable `EngineErrorKind` without importing any protocol
response type. SQL and storage classify SQLite failures from primary and
extended result codes plus operation context; they never parse SQLite error
messages. Protocol-owned tables map each kind to an HTTP status and safe RFC
9457 problem, a PostgreSQL SQLSTATE, and a MySQL error number/SQLSTATE pair.
The PostgreSQL and MySQL entries are mapping contracts for future wire
adapters. The PostgreSQL TCP placeholder emits no protocol frame and therefore
does not encode the PostgreSQL mapping yet; no MySQL listener exists yet.

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
