# Architecture

BriskDB is organized so network protocols can share one routing and execution
core. The module layout preserves the experimental HTTP contract and existing
Rust module paths while making the active PostgreSQL startup adapter and the
planned MySQL adapter explicit peers.

```text
binary (main)
    |
    v
server ---------> protocol::http
    |               |    |
    |               |    v
    |               +-> embedded admin browser
    |                    |
    |                    v
    +-----------------> core
    |                 /    \
    |                v      v
    |            storage    sql
    |
    +---------> protocol::postgres
                    |
                    v
                  core
        (startup/session active; SQL deferred)
```

| Module | Responsibility | Must not own |
| --- | --- | --- |
| `core` | Protocol-neutral `Engine`, `Session`, statements, immutable bound portals, values, results, errors, read-only catalog views and initialization declarations, generated-ID policy and codec types, synchronous bound-value-aware plans, prepared lifecycle, explicit-shard read-only inspection, logical Sharded read target selection and scatter/gather, and sharded routing policy; stable key routing; bounded per-session and per-shard admission; routed execution and journaled schema migration | JSON/HTTP types, listeners, or Axum handlers |
| `storage` | Versioned routing and authoritative logical manifest, persisted generated-ID policy/activation, stable active/retired allocation-owner slots, durable per-table hi/lo block leases, recoverable one-time table provisioning, shard layout, migration journals and recovery, SQLite connection opening, WAL/durability configuration | Network requests or response serialization |
| `import` | Offline source-schema preflight, explicit placement and generated-ID plan validation, exact-value row routing into private staging, independent verification, durable receipt creation, and atomic publication | Network handlers, live/incremental migration, generated-ID inference, implicit Global placement, or protocol-specific behavior |
| `sql` | Dialect-explicit SQL syntax parsing, recursive common-subset validation, protocol-neutral statement/batch classification, source-preserving placeholder normalization, explicit strict/compatibility translation, catalog-aware typed shard-key inference, and narrow crate-private DML-shape inspection behind BriskDB-owned boundaries; exact source retention; SQLite statement execution and conversion between SQLite storage classes and BriskDB values | JSON, key hashing or shard selection, mutable session state, physical write-routing policy, filesystem layout, protocol responses, protocol-buffer ownership, or protocol-specific support policy |
| `protocol::http` | Existing HTTP request extraction, shared JSON/BriskDB value and RFC 9457 problem-detail encoding, and the embedded admin shell/assets, temporary browser sessions, metadata-driven logical discovery, exact logical counts, and bounded shard-major page handlers | BLAKE3 routing, shard files, direct SQLite access, or rusqlite calls |
| `protocol::postgres` | BriskDB-owned bounded protocol-3.0 framing, finite parameter validation, selected identity/status, per-connection core-session ownership, query-deferral responses, and private compile/query-parser seam around the exactly pinned `pgwire` library | Listener binding, direct SQLite access, routing, unbounded authoritative prepared state, or public dependency-owned types |
| `protocol::error` | Exhaustive HTTP, PostgreSQL, and MySQL mappings from stable engine error kinds | SQLite errors, routing decisions, or wire-protocol session state |
| `server` | Process configuration, database assembly, loopback validation, separate HTTP/PostgreSQL listener binding, finite connection-task supervision, and shared graceful/forced draining | SQL parsing, PostgreSQL wire framing, or storage implementation details |

Implementation dependencies flow one way: adapters call the async `Engine` in
`core`; the engine coordinates routing, `storage`, and `sql`. An adapter supplies
protocol-neutral session context and an owned statement, then receives the
engine-selected physical target or target set together with the operation
result. It neither computes a shard nor opens a SQLite connection. The only
reverse-facing name is
`storage::Database`, a compatibility re-export of `core::Database`; the storage
implementation does not call core.

## Compatibility during the split

The module split deliberately preserved:

- the CLI flags, environment variables, defaults, and listener behavior;
- every HTTP route and request field, the health, execute, and broadcast response
  shapes, and current error statuses;
- BLAKE3 routing, shard filenames, manifest schema, WAL and synchronous modes;
- legacy SQLite pass-through semantics while the table catalog is empty,
  authoritative catalog gating after registration, and cell-level HTTP JSON
  encoding behavior; and
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
routed reads, structured problem-detail serialization, admin login/session
lifecycle, embedded assets, physical-shard discovery, and bounded row pages
through the shared engine. Unit tests remain colocated with sessions, engine
orchestration, routing, storage, SQL conversion, CLI, and server assembly.

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
Issue #28 added `Config::postgres_listen: Option<SocketAddr>`, so pre-1.0 Rust
callers constructing `Config` with a struct literal must now choose an enabled
address or `None`.

## Listener boundary

The HTTP address remains `Config::listen`. The independent
`Config::postgres_listen` is either a numeric socket address or disabled with
`None`; the binary maps the exact `disabled` CLI/environment sentinel to that
option. The process default disables PostgreSQL; callers may explicitly enable
loopback `127.0.0.1:5433`. See the
[PostgreSQL listener contract](POSTGRES_LISTENER.md) for the full grammar and
startup order.

Issue #29 selects exact `pgwire` 0.36.3 with only `server-api` and adds the
BriskDB-owned `protocol::postgres::{Adapter, Connection}` seam. Issue #30
connects the production loopback listener to that adapter for exact protocol
3.0 startup. Parameter validation, logical database/user selection, fixed
status/error frames, and terminal session cleanup stay in
`protocol::postgres`; socket binding, the 256-task cap, and task draining stay
in `server`. A successful startup creates one core `Session` only after
validation. `Terminate`, EOF, and protocol failure close it; shutdown hands it
to the bounded cleanup lifecycle described below. No dependency-owned type
crosses into core or the public server contract. SQL message execution remains
issue #31. See the
[adapter decision record](POSTGRES_ADAPTER.md).

## Admin browser boundary

Issue #106 adds an embedded application under `/admin` on the existing HTTP
listener. The public shell, stylesheet, and JavaScript are static bytes compiled
into the binary. Same-origin JSON handlers validate the temporary `admin` /
`admin` login, retain at most 128 opaque sessions in process memory for an
absolute eight hours, and enforce the cookie on session, discovery, logical
count, and row-page operations. Logout revokes a presented token when
possible and always clears
the cookie, so repeating it is harmless. These browser sessions are adapter
state, not core SQL sessions, and are neither written to storage nor recovered
after restart.

The adapter may select a validated physical shard only through the engine's
crate-private inspection boundary. `Engine::inspect_shard` retains ordinary
lifecycle admission, schema coordination, session serialization, per-shard pool
and worker bounds, request controls, read-only SQLite validation, and typed
`ResultSet` output. It does not require or synthesize a logical routing key.
This deliberately narrow path exists for administration; it is not a public
general-purpose direct-shard execution API.

Table discovery reads SQLite's physical `main` schema through that boundary,
returns only ordinary application tables, and excludes ASCII-case-insensitive
`sqlite_`, `briskdb`, and `briskdb_` names. The page handler accepts no SQL from
the browser. It validates the returned table identity, safely quotes that
identifier, binds finite limit/offset values, and exposes at most 200 logical
rows from metadata-selected files. Physical inspection derives a unique local
primary-key or rowid ordering and requires it to match across those files, so
wide tables do not require an every-column temporary sort. It never opens a
database file or imports `rusqlite` in the adapter. The specialized count
handler verifies the table on every relevant shard, runs bounded concurrent
read-only counts through the same engine boundary, and returns only a checked
logical-row sum. Separate pages and shard counts are live reads, not a
transaction or a cross-shard snapshot. The complete route and representation
contract is in the
[admin data browser](ADMIN_BROWSER.md).

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

HTTP execute/query has two explicit modes. An empty authoritative table catalog
retains the legacy caller-routed raw SQLite path. Once any table is registered,
the engine parses exactly one SQLite statement, validates the common subset,
classifies behavior, normalizes placeholders, applies strict SQLite
translation, infers catalog placement, and enforces a finite single-shard
target. The journaled migration endpoint remains a separate parameterless
exact-text SQLite batch whose SQL bytes define durable identity. Rust callers
can also invoke the SQL layers directly or through the prepared API.

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

For `SELECT`, `UPDATE`, and `DELETE`, the layer proves finite key sets from
direct `Int64`, `Text`, or `Binary` shard-column equality and combines those
sets through Boolean `AND` and `OR`. Authoritative registration requires a Text
shard-key column to retain SQLite `BINARY` collation, so exact UTF-8 equality is
safe to route without Unicode normalization or case folding. For `INSERT`,
inference examines the explicit shard-key column in every `VALUES` row and
retains one value per row, including text values. The result
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
supported physical target or target set. Logical behavior, not SQLite
result-column metadata,
distinguishes reads from writes, schema changes, and session control. Safe
`NotApplicable` and `Global` reads use deterministic shard 0. The compatibility
portal executor remains single-target; its logical counterpart gathers
supported finite multi-owner and unconstrained Sharded reads. Schema/session
execution is not implemented, and Catalog placement is never exposed as an
application target. `PreparedStatementDescription::behavior()` gives adapters
the same retained behavior.

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

The prepared lifecycle integrates the classifier, translation, planning, and
logical scatter layers but deliberately does not execute a multi-statement
batch, global non-row-local query semantics, schema/session statements, or
transactions. Those remain later planner/transaction milestones. The complete
API, accounting,
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

Version 8 makes newly registered `briskdb_tables` rows authoritative. Its new
downgrade fence prevents a v7 binary from treating those rows as advisory. The
v7-to-v8 transaction clears all legacy advisory table rows, reseals the
manifest, and preserves routing, logical databases, migration history, shard
schema, and application data.

Version 9 adds the authoritative `briskdb_generated_ids` catalog and stable
`briskdb_allocation_owners` map. The v8-to-v9 transaction assigns every existing
table explicit policy `None`, seeds each physical shard's same-numbered owner
slot, raises the downgrade fence, and changes the semantic manifest checksum to
version 2 so both new tables are covered. It preserves routing, placement,
schema history, shard files, and application rows. Version 9 freezes and
validates the `native_range_v1` ID encoding.

Version 10 separates a generated-ID policy from its activation state, gives
allocation owners explicit `Active` and `Retired` lifecycle, and adds the
transient `briskdb_table_provisioning` journal plus its complete declaration
rows. Exactly one active owner allocates on each physical shard; retired owners
remain mapped so historical native IDs continue to route, but cannot receive a
new insert. Manifest digest version 3 covers both lifecycle fields and both
provisioning tables. Native policy activation proves exact `INTEGER PRIMARY KEY
AUTOINCREMENT` storage and installs disjoint owner-local `sqlite_sequence`
floors on every shard before catalog authority is published. Omitted-key
dialect parsing and DDL rewriting are connected durably by version 12 below.

Version 11 adds the `briskdb_hilo_leases` allocation head and manifest digest
version 4. An active `hilo_v1` table has exactly one row with fixed block size
4,096, the first sequence not yet leased, a monotonic fence token, and the most
recent committed range plus its random 32-byte process-incarnation owner. One
`BEGIN IMMEDIATE` manifest transaction advances that row and reseals the
semantic root before an ID can reach a shard writer. The process-local cache
then consumes the committed block without another central write. It never
restores a range after exit or returns an issued ID after rollback,
cancellation, constraint failure, or an ambiguous commit. No timestamp, lease
expiry, or wall clock participates. The v10-to-v11 migration creates an empty
lease table, raises the downgrade fence, and changes no policy, shard, or
application row.

Version 12 adds the retained `briskdb_generated_table_ddl` singleton and
manifest digest version 5. `Database::apply_generated_table_ddl` parses exactly
one supported SQLite/PostgreSQL/MySQL declaration, emits canonical SQLite, and
derives one native-range Sharded declaration. The bridge stores the exact
source dialect/bytes and version-1 logical identity separately from the
canonical SQL and its ordinary physical migration identity. Lifecycle
`ApplyingPhysical` begins the bridge and physical migration in one manifest
transaction; `Provisioning` retains their completed schema digest and links it
to the table-provisioning identity; `Complete` atomically publishes the active
policy and stable table ID while clearing transient provisioning rows. The
retained provisioning-time digest keeps that identity auditable after later
schema migrations advance the current digest. Startup resumes either durable
prefix from the reconstructed declaration and never reparses journal text to
infer authority. The v11-to-v12 migration itself is manifest-only: it adds an
empty bridge table, raises the downgrade fence, advances the semantic root to
version 5, and changes no shard or application row.

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
checksum that v6 never stored. The v7-to-v8 step is likewise manifest-only and
clears advisory table rows before installing the authoritative-catalog fence.
The v8-to-v9 step is also manifest-only: it adds explicit generated-ID policies
and stable owner slots, installs checksum version 2, and changes no shard file.
The v9-to-v10 step remains manifest-only: it preserves every policy but marks it
inactive, marks existing owner rows active, creates empty provisioning tables,
raises the fence, and installs checksum version 3. A migrated native policy must
therefore be explicitly reprovisioned before it may generate a new key.
The v10-to-v11 step is likewise manifest-only: it creates the empty durable
hi/lo allocation table, raises the fence, and installs checksum version 4.
The v11-to-v12 step adds the empty durable generated-table DDL bridge, raises
the fence, and installs checksum version 5 without changing a shard.
There is no automatic downgrade; an older binary requires a backup from before
the newer format.

Startup first canonicalizes the data-directory path, takes the root startup
lock, and joins the in-memory coordination keyed by that path. Each process
holds a shared root lease for its handle lifetime. Initialization, format
upgrade, recovery, schema migration, catalog registration, and generated-table
DDL must upgrade that lease to sole-process ownership before mutation. Current
`Ready`/`Degraded` opens retain the shared lease, so independent processes may
serve steady-state work while in-process handles continue to share schema
admission and catalog-generation publication.

Each manifest connection enables and reads back SQLite cell-size checks and
requires a full manifest integrity check before parsing control-plane state.
Manifest loading then acquires `BEGIN IMMEDIATE` before making a format
migration or layout-state decision. Numbered manifest-only steps rewrite
schema/data, stamp and read back their target identity/version, validate the
destination, and commit in their own transaction. An `Applying` v6 migration is
finished under v6 rules before v7 establishes checksum authority. An active
checksum-authoritative migration is resumed only after every shard matches the
preserved source or target fingerprint for its exact journal-prefix position.

Layout reconciliation then acquires a new immediate manifest transaction,
re-reads and validates the layout state under that write lock, and holds the
lock through independently durable per-shard work and `Ready` publication. A
lagging opener re-reads `Ready` and strictly validates instead of provisioning
from a stale `Creating` observation. Only a locked, durable `Creating` state
permits missing canonical shard files to be created and WAL to be enabled. The
validated v12 manifest may also retain one generated-table DDL bridge and one
matching active table-provisioning record. Startup first resumes any
`Applying` physical migration under its ordinary exact-prefix rules. It then
keeps admission `Pending`, advances the bridge from `ApplyingPhysical` to
`Provisioning`, creates or validates provisioning from the bridge's trusted
derived declaration, and resumes the ascending `next_shard` prefix. A
shard-local sequence seed commits before its separate manifest acknowledgement;
if process loss lands between those boundaries, startup repeats that same seed
idempotently rather than skipping it. Only after all shards are durable does one
manifest transaction activate generated policy, clear the transient journal,
record the stable table ID, seal the bridge `Complete`, reseal digest version 5,
and publish the replacement catalog. A standalone table-provisioning journal
retains its existing recovery path. A conflict never causes BriskDB to infer a
new request from partial shard state.

The final strict shard opens and catalog reconciliation complete before the
startup guard publishes `Ready`; ordinary work is never served against a
persisted mixed-generation prefix. A first v7 open treats the consensus across
all strict generation-bound shard-schema fingerprints as its trust-on-first-upgrade
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

`Database::register_tables` is the sole logical-catalog mutation boundary. It
is an initialization-only operation over an empty catalog and requires one
exclusive live owner for the canonical root. The complete declaration set must
exactly match the empty application tables on every shard: each physical table
is `Sharded` or `Global`, each `Catalog` declaration remains manifest-only, and
every sharded key is a visible, physically non-null column with compatible
SQLite affinity. The non-null `INTEGER PRIMARY KEY` rowid alias is accepted,
but SQLite's nullable legacy primary-key forms are not. Text keys must use
SQLite `BINARY` collation. Foreign keys are accepted only when catalog
placement proves that the parent is present in the same physical file:
matching Sharded keys with the same generated-ID routing domain,
Sharded-to-Global, or Global-to-Global. Missing, Catalog, or cross-placement
relationships, triggers, and virtual tables are rejected.
Every primary or unique key on a sharded
table must contain the shard-key column with `BINARY` collation, which keeps
that constraint's complete equality domain on one owner. The coordinator
validates physical state and assigns deterministic table IDs. A declaration set
whose policies are all `None` is committed, resealed, revalidated, and published
in one manifest transaction. A declaration set containing `NativeRangeV1`
first commits a versioned provisioning identity, the complete declarations,
the committed schema fingerprint, and `next_shard = 0`. It then seeds each
shard's active-owner allocator floor and separately advances that durable
prefix. A final manifest transaction publishes the catalog, changes native
activation to `Active`, clears the provisioning rows, and reseals the semantic
root. No live snapshot can observe a native policy as active before every shard
is durable. An exact repeat is idempotent; any other replacement is rejected.
A v9 catalog migrated with an inactive native policy may submit its exact
declarations to this same journal while its physical tables remain empty; that
path activates the preserved policy without changing a catalog row or ID.

The registration guard changes admission to `Pending` before any manifest
commit that can leave durable registration or provisioning state. If SQLite
reports an ambiguous commit cleanup or I/O failure, the registering handle
deliberately keeps its old catalog and cannot serve ordinary work. The operator
must close that stale handle and reopen the canonical root so startup can
distinguish no request, an exact resumable shard prefix, and the complete new
catalog; a live stale handle prevents a conflicting durable catalog from being
published in-process. Startup never serves work while a provisioning journal
exists.

Once registered, table placement is authoritative. A `Sharded` row has exactly
one owner selected by its canonical key; only `Global` data is intentionally
replicated, and `Catalog` data is not an application-shard table. Registration
accepts only empty physical tables, so it cannot bless or repartition existing
duplicates. Every later schema-migration preflight must preserve the exact
registered table set on all shards and each sharded key's required column and
affinity and `BINARY` Text collation. It also preserves one-owner unique keys,
the conservative foreign-key co-location and SQLite-enforcement rules, and the
trigger and virtual-table restrictions before a journal can be published.

Core routing still hashes the exact key bytes, derives a versioned virtual
bucket, and reads the final physical shard from the snapshot without querying
SQLite. The generation-1 ranges reproduce prior modulo placement for every
supported initial shard count, including counts that do not divide 4,096.
Point reads and writes can therefore target one owner. The logical read path
uses this same metadata to select targets: exact inference visits one owner,
finite inference visits each distinct owner, unconstrained Sharded inference
visits every shard, and `Global` or table-free reads visit canonical shard 0
once. Supported multi-shard reads run with at most eight shard tasks and merge
in ascending physical-shard order as `UNION ALL`, without deduplicating rows.
Catalog registration establishes ownership; it never copies every Sharded row
into every file. The admin browser consumes the same placement metadata: it
targets every file for Sharded tables and canonical shard 0 once for Global
tables, then exposes one bounded shard-major logical page rather than a shard
selector.

The initial scatter executor accepts only a row-local single-table `SELECT`
whose translated SQL can run unchanged on every target. It rejects
multi-shard `DISTINCT`, aggregate or other function calls, grouping, ordering,
limit/offset, joins, subqueries, CTEs, set operations, and windows because
concatenating independently evaluated shard results would not implement their
global SQL semantics. A statement routed to one shard, including a `Global`
read on shard 0, is not subject to that multi-shard restriction.
Version-5 adoption preserves legacy application schema and data and does not
implicitly register it.

The storage-owned `briskdb_shard_metadata` table is inaccessible through client
SQL, and creation of new objects in the reserved `briskdb` or `briskdb_*`
namespaces is
denied by the SQLite authorizer. Client attempts to mutate `application_id`,
`user_version`, persistent `journal_mode`, `schema_version`, or
`writable_schema` are also denied. This prevents client SQL from
invalidating the validated layout. Ordinary routed SQL also denies every
persistent DDL action. The journaled migration connection is the sole exception:
it allows main-schema DDL, including `ALTER TABLE`, inside BriskDB's transaction
while denying transaction escape, attachments, temporary/virtual objects, and
reserved-state access. Before registration, its legacy batch may also contain
DML. With a populated authoritative catalog, an additional parser gate rejects
row-moving DML, `CREATE TABLE AS SELECT`, `DROP TABLE`, and `CREATE TRIGGER`;
postflight validation also rejects any resulting trigger, virtual table,
unsafe or malformed foreign key, invalid unique key, or placement/key change.
Because SQLite does not reveal an
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

## Generated-ID boundary

Generated IDs are authoritative catalog policy, never a conclusion drawn from
mutable shard DDL. `GeneratedIdPolicy::None` means BriskDB classifies every
stored signed integer as a legacy value and claims no generation authority for
it; that includes caller-supplied and previously imported SQLite-generated
values. `NativeRangeV1` is valid only when its named column is the same `Int64`
key that owns a `Sharded` table. Policy and activation are separate: an inactive
native policy retains classification and explicit-key routing but cannot
generate a key; activation additionally requires exact `INTEGER PRIMARY KEY
AUTOINCREMENT` storage on every physical shard. Import defaults every table to
`None` but may opt in through an explicit plan after proving that schema and the
legacy key domain. Every pre-v9 manifest upgrade selects `None`; v9-to-v10
preserves any native policy but marks it inactive. An old `AUTOINCREMENT` clause
or marker-looking imported value therefore cannot silently enable generation.
`HiloV1` has the same Sharded/visible-`Int64` catalog constraint but requires
exact `INTEGER PRIMARY KEY` storage without `AUTOINCREMENT`. Its policy also
remains inactive until empty-table provisioning has validated every shard and
atomically installed an initial manifest allocation head.

The version-1 native value is a positive signed 64-bit integer with bit 62 set,
an immutable 10-bit allocation-owner slot in bits 61 through 52, and a 52-bit
owner-local sequence in bits 51 through 0:

```text
0 | 1 | owner slot (10 bits) | local sequence (52 bits)
63  62       61..52                    51..0
```

Owner slots are stable identities rather than a live shard-count ordinal. The
manifest initially assigns each physical shard its same-numbered active slot.
Every physical shard has exactly one active owner allowed to allocate; a
retired owner retains its physical mapping for historical native reads and
existing-row mutation, while new inserts naming it fail closed. A future
shard-map change must retain every allocated slot rather than renumbering or
reusing IDs, and each shard's replacement owner must be strictly greater than
all owners previously retired on that shard. This monotonic succession matches
SQLite's `AUTOINCREMENT` rule: its committed high-water mark survives row
deletion and cannot safely move into a lower owner's range. Local sequence zero
is reserved; valid native sequences are
`1..=2^52-1`, slots are `0..=1023`, and the greatest encoding is `i64::MAX`.
Policy-aware classification treats values without the marker, including
negative imported values, as legacy. A marker with reserved local sequence zero
is corrupt. The strict native decoder instead rejects every non-native value,
and unsupported persisted encoding versions fail closed before decoding.

This design keeps SQLite upstream. In particular, a schema function cannot
replace the allocator for an exact `INTEGER PRIMARY KEY`: that declaration is a
rowid alias, and SQLite's special insert path chooses an omitted or NULL rowid
without evaluating a column `DEFAULT`. Moving allocation into a side-effecting
UDF would also put durable allocator state behind connection-local registration
and retry-sensitive schema evaluation, while a single shared allocator would
serialize the writers sharding is meant to separate. The native allocator
therefore reserves one disjoint range per owner and lets unmodified SQLite
advance its own shard-local `AUTOINCREMENT` state. Empty-table policy activation
first journals the exact declarations and trusted schema digest, then seeds one
shard and durably acknowledges its prefix at a time. Retries are idempotent and
never lower an existing same-owner high-water mark. A crash after a shard commit
but before acknowledgement repeats that shard; a crash at any other boundary
resumes the exact identity, never a guessed request. Only final manifest
publication activates policy and clears the journal. Every later shard
admission rejects missing, duplicate, malformed, out-of-owner, or row-lagging
allocator state. Issue #130's structural SQL frontend recognizes the finite
generated declarations, records `native_range_v1` intent beside canonical
physical SQLite DDL, and authorizes one omitted-key row only after this
catalog state is active.

The version-1 hi/lo value sets bit 61 and stores one global per-table sequence
in bits 60 through 0:

```text
0 | 0 | 1 | global table sequence (61 bits)
63  62  61               60..0
```

Sequence zero is reserved. Valid hi/lo IDs are therefore
`0x2000_0000_0000_0001..=0x3fff_ffff_ffff_ffff`, disjoint from the native
range beginning at bit 62. Each complete encoded `Int64` value is routed
through the frozen canonical hash and persisted virtual-bucket map; it does
not embed or pin a physical shard. Negative and positive pre-marker values
remain explicit legacy IDs with that same hash route. For a `hilo_v1` table,
every caller-supplied value at or above the hi/lo marker is reserved to
allocator namespaces and is rejected before mutation.

The manifest owns one durable global sequence head per active hi/lo table. A
lease transaction reserves up to 4,096 consecutive values, increments a
monotonic fence, records the random 32-byte incarnation of the requesting
process, reseals the semantic root, and commits before the allocator can expose
the first value. Independent BriskDB processes that reach this narrow allocator
path serialize only that refill transaction through SQLite and receive
non-overlapping ranges. The fence distinguishes successive durable reservations;
it is not an expiry time and does not revoke earlier IDs. There are no clocks,
heartbeats, or reclaim decisions. The same lifetime process lease permits
independently started same-host processes to serve steady-state work on a ready
local root. Processes after `exec` are supported; continuing to use an inherited
live handle or lease cache after `fork()` is not.

Committed leases are irrevocable. The process cache advances before the target
shard insert, never returns an ID after rollback, cancellation, constraint
failure, or an ignored insertion, and never reloads its unused tail after
restart. A manifest commit whose outcome cannot safely be acknowledged also
returns no lease, so a block that may have committed is burned. Gaps and
abandoned tails are expected. Numeric order reflects allocation order only;
transactions may commit in another order. `hilo_v1` promises uniqueness and
non-reuse, not a gapless sequence or global commit ordering. Allocation happens
before a target shard is selected or write-locked. Consequently, an explicit
transaction that has already pinned a physical child rejects later hi/lo
generation rather than trying to lease and move to another shard.

The `experimental-vtab` feature adds separate read-only and narrowly writable
SQLite coordinators that statically register `brisk_shard`. They prove a
no-fork logical table boundary while leaving the manifest and physical schemas
unchanged. The read-only coordinator opens validated physical children through
OS-level SQLite read-only handles, never attaches a shard, and never
dynamically loads an extension. Trusted metadata supplies each declared
schema. An exact, storage-class-compatible `Int64`, `Text`, or `Binary`
shard-key equality can open only its owner and bind the equality on that
physical child; `native_range_v1` IDs use the stable active/retired owner map
while `hilo_v1` and policy-accepted legacy integers use ordinary hash routing.
`NULL` is empty and a type
mismatch falls back to a full scan so SQLite can retain its comparison semantics.
Unconstrained scans visit shards in ascending order with `UNION ALL` duplicate
semantics. Remaining filters, aggregation, ordering, limits, and feature-local
joins execute in the stock SQLite coordinator without pushdown. That read
delegation remains internal: Engine logical queries, `/v1/query`, and the admin
browser continue to use the established metadata-driven scatter/gather
executor. Cursor ownership, schema admission, cancellation, bounded
materialization, static-loading policy, and rejected alternatives are specified
in the
[experimental sharded virtual-table facade](SHARDED_VIRTUAL_TABLE.md).

The writable coordinator accepts explicit-key INSERT and exactly routed
UPDATE/DELETE. Its first operation pins one `BEGIN IMMEDIATE` child shard;
reads used to locate UPDATE/DELETE rows reuse that child, and a second-shard
attempt aborts the whole transaction. A hidden versioned locator carries the
physical rowid or complete `WITHOUT ROWID` key without changing shard files.
The wrapper reconciles stock-SQLite transaction and savepoint callbacks, then
performs the fallible child commit before acknowledging success. Physical
constraints and conservatively admitted co-located foreign keys remain
authoritative. A narrow preflighted `native_range_v1` seam can arm one
single-row insert whose generated column is absent from the AST column list.
Engine leaves native owner selection to the writable registry. A per-table
atomic cursor rotates the first candidate, and the lower runner attempts one
active owner's bounded pool capacity at a time without waiting. It skips a busy
candidate; after pinning a candidate it checks range capacity under the child
lock, and an exhausted unmutated child is released before fallback. The chosen
child omits the physical ID column, captures `INSERT ... RETURNING id` on that
same handle, validates the owner and sequence, and publishes a protocol-neutral
`GeneratedKey` only after successful reconciliation. An explicit `NULL`,
omitted-key multi-row insert, and second generated callback remain rejected.
The same one-shot seam accepts `hilo_v1`: Engine first reserves every possible
target shard's capacity, then the coordinator leases and irrevocably consumes
an ID before any target-shard write lock, hashes the encoded value to its owner,
inserts it explicitly with `RETURNING`, and verifies the returned key. Because
that allocation may select any shard, a transaction that already pinned a child
rejects a later hi/lo insert. Engine, prepared-portal, and HTTP omitted-key
planning consume this seam. Global writes, generated multi-row inserts,
multi-shard transactions, caller-authored `RETURNING`, defaults, generated
columns, and triggers remain later work.

Engine integration has two independent opt-ins. The binary must be compiled
with `experimental-vtab`, and `EngineOptions::with_experimental_vtab_writes(true)`
must enable the runtime gate; the server exposes that option as
`--experimental-vtab-writes` and
`BRISKDB_EXPERIMENTAL_VTAB_WRITES=true`. The gate is false by default even in an
all-features build. When it is true, a populated-catalog Engine write is
dispatched through an ephemeral writable coordinator only after planning has
proven one explicit Sharded owner or one single-row generated allocator intent.
Prepared portal execution uses that same rule. The engine retains its
lifecycle, schema, session, capacity, worker, cancellation, deadline,
route-reporting, and error boundaries while the coordinator supplies the
physical affected-row count, optional generated key, and acknowledges success
only after its child commit. Empty-catalog SQL, Global and Catalog placement,
and all reads retain their established execution paths.

Physical table descriptors discovered from shard 0 are immutable for one schema
generation and cached per Engine. Cold discovery is serialized, cancellable,
and charged to shard 0's connection capacity; warm coordinators open no shard-0
handle. Explicit-key DML reserves only its already planned target. A native
generated write retains at most one non-waiting candidate reservation and may
replace it while falling back around busy or exhausted owners. Hi/lo reserves
capacity on every shard in stable order because it learns its hash-routed owner
only after consuming a durable lease. A published schema-generation mismatch
invalidates the cache and forces controlled rediscovery before another write.

This integration is intentionally autocommit-only. Each `Engine::execute`,
`Engine::execute_write`, or HTTP `/v1/execute` request owns one statement and
drops its coordinator after that statement reconciles. `Session` still has no
transaction state, HTTP still creates a fresh Session per request, and `BEGIN`,
`COMMIT`, `ROLLBACK`, read-your-writes, and transaction shard pinning are
deferred to the later session-transaction work.

## Session and asynchronous engine boundary

`Session` is protocol-neutral mutable state owned by one frontend connection or
request. A new session is `Ready`; closing it is a terminal transition to
`Closed`, and engine operations reject a closed session with
`FailedPrecondition`. Ordinary statement failures do not close or poison a
session, so a frontend may correct a request and continue. Sessions are not
clonable. Frontends may issue concurrent calls against one borrowed session,
but the engine serializes them; the HTTP adapter instead creates an independent
session for every request.

The current routing context is an optional caller-supplied shard key. Legacy
routed execute and query operations require that context, and the engine alone
hashes it and reports the selected shard in `Routed<T>`. Catalog-aware logical
reads instead derive their physical targets from the statement's inferred keys
and registered placement metadata and report those targets with the combined
result. `Statement` owns its SQL text and typed parameters so an adapter can
hand work across the asynchronous boundary without borrowing protocol buffers.
Prepared statements and portals
are session-scoped logical objects; binding captures the routing context so a
later session change cannot retarget an existing portal. `EngineStatus` exposes
the shard count and prepared-state limits needed by health/configuration
reporting without exposing the storage implementation.

HTTP SQL state remains request-local: each execute or query request creates a
fresh session. Execute and empty-catalog legacy query initialize routing from
the request's `shard_key`; a populated-catalog query selects logical targets
from catalog metadata and does not use a caller key to narrow its shard set.
Each admin inspection likewise creates a fresh core session but
selects a validated physical shard through the dedicated read-only boundary;
the count handler coordinates multiple such explicitly bounded inspections.
Consequently, session settings and transactions cannot span HTTP requests. The
eight-hour browser cookie authenticates the admin JSON calls; it does not retain
SQL session state. Schema-migration broadcast and status calls also go through
the shared engine, but do not perform a routing decision in the adapter.

### Bounded worker and connection-pool boundary

The local engine owns one independent pool per physical shard. `EngineOptions`
defaults each pool to four active connections and a queue of 32 admitted
operations. Connections are created lazily, up to the active limit, and are
reused after successful cleanup. Admission occurs on the asynchronous side
before work is handed to a Tokio blocking worker, so waiting for an exact shard
slot does not occupy an unbounded set of blocking threads. Target-unknown native
generation is the bounded exception: after taking one worker, it makes only
non-waiting pool attempts against one rotating candidate at a time and never
holds the worker while queued for capacity. Hi/lo reserves every possible
target capacity on the asynchronous side before its worker may consume an ID.

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
For native omitted-key generation, an immediately unavailable candidate is
skipped. If the scan completes without a selection, any observed busy candidate
yields `Busy`; `LimitExceeded` requires every active owner to be proven
exhausted.

A logical multi-shard read admits one outer operation and schedules no more
than eight shard tasks at once. Each child uses the independent pool for its
physical file, so SQLite writes to other shard files retain their own
connections and transaction locks while the read is gathering. Target order is
stable even though the child reads may complete in another order. The first
child failure cancels outstanding children, waits for their SQLite cleanup, and
returns only the error; BriskDB never exposes the successfully completed prefix
as a partial logical result.

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
An outer filesystem lease requires sole-process ownership before the migration
can inspect or publish durable state. A live peer therefore receives no schema
change; the coordinator returns retryable `Busy`. This is local-host advisory
coordination, not a distributed lock for network filesystems or object storage.

Every-shard preflight executes the complete batch inside a rollback-only
transaction. When the authoritative table catalog is populated, that tentative
schema must still contain exactly its declared physical tables, compatible
sharded keys, `BINARY` Text collation, one-owner unique constraints, and only
conservatively co-located, SQLite-enforceable foreign keys, with no application
trigger or virtual table. The catalog-aware migration gate also rejects
row-moving DML, table drops, and trigger creation before
preflight. Only after all preflights succeed does the coordinator record the
exact SQL and its BLAKE3 identity, then visit shards in ascending order. One
shard's complete batch and target `user_version` commit atomically, followed by a separate
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
empty-catalog one-call SQLite pass-through behavior, but that behavior is
uncontracted and is outside the populated-catalog common subset. Clean read
handles may cross sessions for ordinary statements.
The pool retains the first session associated with each physical handle; an
ordinary foreign read does not relabel that history. Before a routed statement
uses such a foreign handle, the engine prepares it under a deny-only authorizer
probe. The first connection-local or write action is rejected before it can
run—even for PRAGMAs with prepare-time effects—and the real statement is then
executed once on a fresh handle. This also gives every cross-owner write clean
SQLite counter state.
The read-only `table_list` metadata PRAGMA is the narrow exception to PRAGMA
tainting. It changes no connection-local state, remains reusable under an exact
pool regression test, and lets the admin explorer distinguish ordinary tables
from virtual-table shadow objects.
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
the owning BriskDB domain after any authorizer-observed insert, update, or
delete. Ordinary engine sessions have unique domains. A populated-catalog HTTP
write receives the shared stateless catalog-write domain only after planning
proves it is one routed common-subset write; that boundary rejects scalar
counter functions, session SQL, PRAGMAs, attachments, temporary objects, and
schema SQL. Catalog registration, import, post-migration validation, and
ordinary startup parse every persistent table and index definition and reject
those same functions inside `DEFAULT`, `CHECK`, generated-column, and index
expressions. Clean handles can therefore remain warm across those ephemeral
write requests without making their logical sessions shared. Empty-catalog HTTP
SQL retains its unique session owner. A later ordinary owner retires and
replaces a stateless write handle before SQL runs, and handles used only for
reads remain reusable across sessions. This ownership is a leakage-prevention
rule, not connection pinning: a competing ordinary session can replace an idle
write-bearing handle, so write-counter functions remain uncontracted across
calls until transaction/session pinning is added.

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
same `*_with_context` boundary as raw operations; explicit-shard inspection has
the same controlled form. Queries, inspection pages, and prepared row results
account a stable logical representation while stepping SQLite and before
cloning payloads. A logical scatter shares one budget, absolute deadline, and
cancellation source across all targets rather than resetting any of them per
shard. A row or byte overflow or any child failure returns no partial
`ResultSet`;
row-producing writes are rejected so early termination cannot hide DML effects.
The exact accounting contract is documented in [request
controls](REQUEST_CONTROLS.md).

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

The server owns accepted HTTP/1 and PostgreSQL connections in separate tracked
task sets under one accept lifecycle. It stops engine admission before dropping
both listeners, signals both task sets, and starts connection/core draining
together. Tasks that exceed the common grace deadline are aborted. HTTP task
joins are awaited, while the PostgreSQL supervisor gets one additional grace
interval to join aborted tasks and attempt to close each selected core session.
If that second interval expires, server return does not await the remaining
PostgreSQL session closes; they are scheduled on the runtime as best-effort
cleanup.
Partial startup sockets own a task but no session. Signal receivers are
installed only after every configured listener binds and before readiness is
logged. Dropping the server future closes both listeners, aborts both task
sets, synchronously enters `Draining`, and schedules best-effort terminal
PostgreSQL session cleanup; a surviving embedder-owned `Engine` clone can
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
Production PostgreSQL startup emits fixed fatal validation/protocol errors and
the pre-issue-31 query boundary emits the fixed `Unsupported` mapping. The
private adapter probe also consumes the engine mapping behind the same
dependency boundary. The MySQL entry remains a contract for its future adapter;
no MySQL listener exists yet.

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
`/v1/query` and the admin row-page response use the same ordered `columns` array
of `name` and `data_type` metadata objects plus positional arrays in `rows`.
Column and row indices correspond exactly. Duplicate and empty names are valid,
and metadata is returned even when there are zero rows. The admin response adds
physical-shard, table, and finite pagination metadata. It also tags signed or
unsigned integers outside JavaScript's exact range with their decimal text so
the browser cannot round them; the experimental `/v1/query` cell encoding is
unchanged.

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
