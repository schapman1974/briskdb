# SQL compatibility

BriskDB stores data in SQLite and is designed to expose the same database
engine through HTTP, the active PostgreSQL startup adapter, and planned SQL
flows for PostgreSQL, MySQL, and other future protocol adapters. A wire protocol
does not change SQLite into that protocol's namesake database. This document
defines the SQL and behavioral contract separately from connectivity.

## Status vocabulary

- **Implemented** means the behavior exists in the current source tree and is
  covered by the repository's tests.
- **Experimental** means the behavior exists, but its public contract may still
  change before the first stable release.
- **Planned** means the roadmap defines the behavior, but clients must not rely
  on it yet.
- **Unsupported** means BriskDB rejects the behavior or makes no compatibility
  promise for it.

The syntax parser, recursive common-subset validator, statement/batch
classifier, placeholder normalizer, finite SQL translator, catalog-aware
shard-key inference API, and synchronous engine bound-statement planner are now
available behind BriskDB-owned types.
Validation is explicit and returns `Unsupported` for a parsed form outside the
subset; parser acceptance alone is not product support. Translation,
classification, normalization, inference, bound planning, and routing policy
are also explicit APIs. HTTP execute/query retains raw SQLite pass-through only
while the authoritative table catalog is empty. Once any table is registered,
the same endpoints compose those SQLite-mode layers and enforce the committed
placement catalog. The empty-catalog behavior is a legacy compatibility
boundary, not a promise for registered databases.

## Compatibility layers

BriskDB tracks three independent compatibility layers:

1. **Wire compatibility** lets an existing driver connect, authenticate,
   prepare and bind statements, receive typed rows, and manage session state.
2. **SQL compatibility** parses a documented common subset and translates
   selected PostgreSQL or MySQL syntax into SQLite SQL.
3. **Behavioral compatibility** emulates the metadata, type, error, transaction,
   and session behavior needed by specifically tested clients and tools.

Completing only a PostgreSQL or MySQL handshake establishes startup
compatibility, not the full wire compatibility defined above. BriskDB will
publish wire and behavioral compatibility per tested driver or tool rather than
claiming to be a drop-in PostgreSQL or MySQL replacement.

## Current implementation

The initial alpha exposes experimental HTTP plus a disabled-by-default,
separately configured loopback PostgreSQL listener. PostgreSQL implements exact
protocol-3.0 startup, logical database/user selection, BriskDB parameter
status, tracked session termination, simple queries, and zero-parameter
text-format extended queries through a pinned `pgwire` 0.36.3 boundary. There
is no MySQL listener. The public Rust SQL facade
can parse an explicitly selected SQLite, PostgreSQL, or MySQL dialect, consume
that result with
`validate_common_subset(ParsedSql)`, borrow the result with
`classify_statements(&CommonSql)`, and then opt into
`normalize_placeholders(CommonSql)`. That step yields source-preserving SQLite
`?N` text and per-statement parameter metadata. A caller can consume the owned
normalized result with `translate_sql`, explicitly selecting either finite
compatibility translation or strict SQLite preservation; there is no default
mode. Translation returns separate SQLite SQL while retaining the exact source
and normalized bind metadata.

Callers may pass one statement's exact bound-value slice and selected logical
database from the retained normalized result to `infer_shard_keys`, which
returns a typed key classification. Synchronous
`Engine::plan_bound_statement` accepts the same concrete bind plus an optional
explicit routing byte sequence, retains the inference, and produces owned
physical routes with schema and routing provenance. It compares finite inferred
and explicit routes by physical shard, rejects unroutable cataloged sharded
DML, and records a valid single-shard assignment. The planner does not invoke
translation, authorize, or execute a statement; it does enforce the common
batch gate and exposes the selected statement behavior. The
protocol-neutral prepared lifecycle composes these SQL stages for exactly one
statement, exposes retained behavior in description metadata, binds typed
values into a session-scoped portal, refreshes stale description metadata,
plans freshly at every execution, and executes supported physical targets.
HTTP requests still send caller-provided SQLite SQL to the engine. With an empty
catalog it follows the legacy raw path. With a populated catalog, the engine
requires exactly one common-subset SQLite statement and runs normalization,
strict translation, classification, inference, and routing policy before
SQLite execution. Registered writes remain single-owner. Registered reads use
catalog metadata to visit an exact owner, the distinct owners of a finite key
set, every shard for an unconstrained Sharded read, or shard 0 once for a
`Global` or table-free read.

| Interface | Status | SQL accepted | Routing |
| --- | --- | --- | --- |
| HTTP `/v1/execute` | Experimental | Empty catalog: legacy SQLite statement. Populated catalog: exactly one SQLite common-subset write with normalized positional parameters and strict SQLite translation | Empty catalog requires caller `shard_key`; a populated catalog requires authoritative finite single-shard inference except for one active-policy omitted generated key, and rejects Global/Catalog writes |
| HTTP `/v1/query` | Experimental | Empty catalog: legacy raw SQLite query. Populated catalog: exactly one SQLite common-subset read; no session cache; multi-shard execution is limited to the row-local scatter-safe subset | Empty catalog requires caller `shard_key`; populated catalog derives targets from registered metadata, reads Global data once on shard 0, and denies Catalog/undeclared tables |
| HTTP `/v1/admin/broadcast` | Experimental | A journaled parameterless SQLite schema batch; populated catalogs reject row-moving DML, table drops, and trigger creation | Preflight on every shard, then ascending resumable apply |
| HTTP `/admin` browser | Experimental, read-only | No caller SQL; metadata-driven logical table discovery, specialized exact logical `COUNT(*)`, and bounded deterministic `SELECT *` page slices | Sharded tables visit all files; Global tables visit shard 0 once; no browser shard selector or arbitrary SQL |
| PostgreSQL wire protocol | Protocol-3.0 startup plus simple `Query` and zero-parameter text-format extended flow on loopback | Registered-table `SELECT`, `INSERT`, `UPDATE`, and `DELETE`; parameters, binary formats, DDL, and transactions remain unsupported | Startup selects an exact logical database; both flows use Engine prepare/bind/logical execution and fixed SQLSTATE mapping |
| MySQL wire protocol | Planned | Rust parsing, validation, classification, placeholder normalization, finite compatibility translation, and prepared lifecycle implemented; listener adoption planned | Core batch/write policy, bind validation, routing snapshots, current execute-time planning, and supported target execution implemented; wire mapping planned |

The parser, subset validator, statement classifier, placeholder normalizer, SQL
translator, shard-key inference function, engine planner, and prepared
lifecycle are implemented Rust APIs, not PostgreSQL or MySQL query interfaces.
PostgreSQL production startup connects only identity, catalog selection,
status, and session cleanup; the historical private issue-29 parser probe does
not make that prepared pipeline public wire behavior. Authoritative table
registration composes the SQLite frontend and planner into the existing HTTP
execute/query rows as described above; it adds no HTTP field or route.

The `experimental-vtab` Cargo feature also contains internal read-only and
writable `brisk_shard` coordinators for the no-fork virtual-table boundary.
They are not PostgreSQL or MySQL query interfaces. The module is statically
registered into stock SQLite and opens validated physical children through
OS-level SQLite handles; it does not use `ATTACH`, runtime extension loading, a
SQLite fork, or a storage-format change. The writable wrapper accepts
explicit-key Sharded INSERT and exactly routed UPDATE/DELETE, pins one physical
transaction, and rejects cross-shard or Global writes. Shared issue #130 AST
planning can also arm one generated callback for a single row whose declared
key is absent from the column list. `native_range_v1` chooses an eligible
owner from a per-table round-robin candidate list. Engine holds at most one
non-waiting candidate pool reservation, skips busy candidates, and releases an
exhausted unmutated candidate before fallback; the chosen child captures the ID
with `RETURNING` on the same handle. `hilo_v1` first reserves every possible
target's pool capacity, then durably leases before taking a child write lock,
consumes one global per-table ID, hash-routes it, inserts it explicitly, and
verifies the returned value. A
transaction that already pinned a shard rejects later hi/lo generation. Engine,
prepared-portal, and HTTP execution expose the protocol-neutral generated
result when both coordinator gates are enabled. Schema operations, attachments,
unsafe
PRAGMAs, extension loading, defaults, generated columns, triggers, and
caller-authored `RETURNING` remain rejected. This surface is internal and
cannot be reached by the query app or PostgreSQL/MySQL protocol adapters. See
[generated keys](GENERATED_KEYS.md) for the exact syntax and result contract.

The lower `hilo_v1` seam reserves fixed 4,096-value blocks in the manifest and
serves them from a fenced process-local cache. A monotonic fence and random
32-byte process incarnation identify the committed reservation; no clock or
expiry is involved. Committed ranges are never reclaimed. Restart, crash,
rollback, cancellation, an ignored insert, or a constraint failure can burn IDs
and leave gaps. The guarantee is uniqueness and non-reuse, not gaplessness or
global commit ordering; numeric order records allocation order only.

Within that feature gate only, a usable equality on the exact cataloged shard
key requests one virtual-table argument without `omit`. Exact SQLite `INTEGER`,
UTF-8 `TEXT`, and `BLOB` values can route matching `Int64`, `Text`, and `Binary`
keys to one child, where the value is bound against the indexed physical key.
A valid `native_range_v1` value uses its allocation owner. Valid `hilo_v1` IDs
and policy-accepted legacy integers use normal hash routing; caller-authored
hi/lo-namespace inserts are rejected. `NULL` is empty, while a non-null
type mismatch conservatively scans all placement targets. Unconstrained scans
visit shards in ascending order and preserve duplicates as `UNION ALL`.
SQLite applies remaining filters, aggregation, ordering, limits, and
feature-local joins above the facade without pushdown. That stock-SQLite
delegation is internal to the experimental coordinator; it does not expand the
Engine or protocol contracts documented elsewhere in this file, including
their rejection or delegation of unsupported advanced multi-shard SQL.

Every HTTP database operation now calls the same protocol-neutral async engine
used by PostgreSQL startup status and intended for issue-31 PostgreSQL SQL flow
and the future MySQL adapter. Execute and query requests create a fresh `Ready`
session and submit an owned statement. Execute and empty-catalog legacy query
put the request's `shard_key` in its routing context. Populated-catalog query
instead derives its target set from the statement and authoritative metadata;
a caller key does not narrow that logical set. The engine, rather than the HTTP
adapter, selects targets and acquires pooled SQLite connections. Admin
inspection requests also create fresh `Ready` sessions, but their generated
logical plan uses placement metadata to choose explicit read-only physical
inspections. The sessions are discarded when that HTTP request finishes, so a
transaction cannot span requests.

### Admin browser inspection

The `/admin` application is an early operational view rather than another SQL
compatibility mode. The browser never submits SQL. With a populated catalog,
its overview lists non-Catalog tables in the default logical database. An empty
catalog retains a shard-0 physical-discovery fallback through SQLite's typed
`table_list` metadata. ASCII-case-insensitive `sqlite_`, the exact name
`briskdb`, and `briskdb_` prefixes are excluded, as are non-table objects.

Table placement selects the physical targets: Sharded uses every file, Global
uses canonical shard 0 once, and the empty-catalog fallback uses every file.
The browser verifies table presence and identical column metadata, counts each
target, calculates only the shard-major slices needed for the requested offset,
and generates safely quoted, all-column-ordered `SELECT *` reads for those
slices. Duplicate rows are retained. Page limits are 1 through 200 and offsets
are 0 through 1,000,000; at most one extra logical row decides whether another
page is available. The user interface offers 25, 50, 100, and 200 and starts at
50. Table, limit, offset, target shape, and checked arithmetic are validated.
The combined page is checked against one configured row/logical-byte budget.

Table selection separately verifies the exact ordinary table on every shard
selected by placement and runs bounded read-only `COUNT(*)` inspections. It
returns their checked sum, so a Sharded row contributes from its sole owner and
a Global row contributes from shard 0 once. The sum remains a specialized admin
operation rather than the general logical query planner's aggregate contract.

Each offset page is a new set of committed reads, not a retained cross-file
snapshot. Concurrent changes may move or repeat rows between pages. The browser
does not accept arbitrary filters or SQL, and its dedicated count/paging plan
does not add general multi-shard aggregate, ordering, or pagination SQL.
The full route, login, and live-view contract is in the [admin data
browser](ADMIN_BROWSER.md).

The asynchronous boundary admits work to an independent bounded pool for each
shard before dispatching blocking SQLite work. The default pool permits four
active connections and 32 queued operations per shard. Public `EngineOptions`
accepts 1–16 active connections and 1–1,024 queued operations per shard,
provided the configured shard count and per-shard active limit do not exceed
512 total active connections. Connections are opened lazily and reused. A full
queue returns the retryable `Busy` error (HTTP 503), while single-shard routed
work waiting on one shard does not consume another shard's capacity. Schema
migration drains admitted ordinary work and uses fresh coordinator-owned
connections instead of reserving every shard pool.

Native omitted-key writes are the target-unknown exception: after acquiring one
bounded worker they use only non-waiting pool admission, retain at most one
per-table round-robin candidate, and fall back around `Busy` or exhausted
owners. Hi/lo instead reserves every possible target pool before its allocation
can choose a hash route. Neither path grows an unbounded worker or connection
queue.

SQLite forms that can leave connection-local state remain allowed by the
empty-catalog one-call pass-through, but they are uncontracted and are outside
the populated-catalog common subset. The pool observes
transaction and savepoint control, `PRAGMA`, `ATTACH`/`DETACH`, and temporary
objects and marks the connection tainted. A clean read handle can be reused by
another session for ordinary SQL without transferring the handle's recorded
first-owner history. Before connection-local routed SQL executes on such a
foreign handle, a deny-only authorizer probe identifies it without permitting
prepare-time effects. The probe also identifies writes, so a cross-owner write
starts with fresh SQLite counter state. The real statement then runs once on the
fresh handle. Any other probe error also fails closed to a fresh handle, after
which replacement acquisition can return a storage error; otherwise the normal
statement boundary produces the public SQL result or error. Broadcast batches
use dedicated handles and the migration-specific authorizer described below.
After an ordinary call, a tainted connection is closed instead of being reused.
A connection left in a transaction is also retired after rollback cleanup is
attempted. This preserves existing one-call SQLite behavior without allowing
connection-local state or observer metadata such as `PRAGMA data_version` to
leak into another ephemeral HTTP session; it does not add multi-request
transactions or promise compatibility for those statements.

The pass-through boundary excludes BriskDB-owned storage identity. Client SQL
may not read or mutate `briskdb_shard_metadata`, create objects in the reserved
`briskdb` or `briskdb_*` namespaces, or mutate `application_id`, `user_version`, persistent
`journal_mode`, `schema_version`, or `writable_schema`. The SQLite authorizer
denies those operations through every client execution path. Ordinary routed
SQL also denies persistent schema DDL. These controls are reserved for startup
validation and the journaled migration coordinator, so denial is stable
BriskDB behavior rather than connection-hygiene policy.

`ALTER TABLE` is denied through ordinary execute/query SQL but permitted inside
the journaled migration batch. That coordinator denies transaction/savepoint
escape, attachments, temporary and virtual objects, and storage-owned state.
SQLite reports the source table but not a `RENAME TO` destination, so BriskDB
also compares the reserved schema before and after the migration transaction.
When the authoritative catalog is populated, migration parsing additionally
rejects `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`,
`CREATE TABLE ... AS SELECT`, `DROP TABLE`, and `CREATE TRIGGER`. Final
rollback-only validation accepts only conservatively co-located foreign keys
and rejects unsafe foreign keys, triggers, virtual tables, invalid unique keys,
and any table-placement or shard-key change.

SQLite also retains `last_insert_rowid()`, `changes()`, and `total_changes()` on
the physical connection after ordinary writes. A write-bearing handle may be
reused by its owning BriskDB `Session`. Planner-validated populated-catalog HTTP
writes use a shared stateless ownership domain so separate ephemeral requests
can reuse clean autocommit handles; the admitted SQL cannot contain observer
functions or session-local forms. Persistent table and index definitions are
also checked during registration, import, migration, and startup; `DEFAULT`,
`CHECK`, generated-column, and index expressions cannot call
`last_insert_rowid()`, `changes()`, or `total_changes()`. Empty-catalog
pass-through writes keep unique session owners. Before an ordinary different
owner can inspect a write-bearing handle, the pool closes and replaces it.
Read-only handles can cross sessions. This preserves same-session write
metadata without exposing one HTTP request's connection-local counters to the
next request. It does not pin that handle, so these observer functions remain
uncontracted across calls in the current SQL surface.

Dropping or explicitly cancelling a request before its queued operation starts
skips SQLite entirely. After execution begins, BriskDB interrupts the exact
leased connection and does not return cancellation until blocking rollback and
connection cleanup finish. A successfully completed statement wins a very
close cancellation race. Request deadlines use the same cleanup path but retain
the distinct `DeadlineExceeded` error kind.

Queries are restricted to statements SQLite identifies as read-only and have
finite row and protocol-neutral logical-byte budgets. BriskDB returns no partial
result when a budget is exceeded. This deliberately rejects write-capable query
forms such as DML `RETURNING`; callers must use the execute surface, whose
result is rows affected rather than returned rows. Exact accounting and
configuration semantics are in [request controls](REQUEST_CONTROLS.md).

A supported logical multi-shard query schedules at most eight shard tasks. All
targets share one absolute request deadline, cancellation source, and combined
row/logical-byte budget. Rows are concatenated in ascending shard order and
duplicates are retained, matching `UNION ALL`. A failure on any target cancels
the remaining work, waits for cleanup, and returns no partial result. Because
each physical shard has its own SQLite file and connection pool, this read
coordination does not collapse the independent per-file write paths into one
database lock.

The `broadcast` surface now implements one application-schema migration. Before
it creates a journal, BriskDB runs the complete batch on every shard in an
immediate transaction and rolls each preflight transaction back. It then
records the exact SQL, applies shards in ascending order, and commits one
shard's complete batch together with its next `user_version` atomically. There
is no transaction across shard files. A failure or cancellation after journal
publication retains a valid committed prefix, and the byte-identical request or
the next startup resumes it before ordinary work may continue.

Migration identity is digest version 1: the full BLAKE3 digest of the exact
UTF-8 SQL bytes. SQL must contain 1 through 65,536 bytes and no NUL. Whitespace,
comments, and casing are identity-significant. Completed journal rows and their
exact SQL remain in the manifest, so callers must not include credentials or
other sensitive literals. A byte-identical retry of completed SQL is
idempotent and does not advance the schema generation again.
The migration does not infer or update `briskdb_tables` rows. When the
authoritative catalog is populated, however, every tentative shard schema must
still contain exactly the declared `Sharded` and `Global` tables, no physical
`Catalog` table, and each sharded key with its required column, affinity, and
`BINARY` Text collation. Every sharded primary/unique key must still include the
shard key with `BINARY` collation. Foreign keys must prove matching co-sharded
keys in the same generated-ID routing domain, Sharded-to-Global, or
Global-to-Global placement, and SQLite must accept the referenced parent key;
triggers and virtual tables remain prohibited. Row-moving DML, table drops, and
trigger creation are rejected before this rollback-only check. A violation fails
preflight before journal publication. Richer
migration/history APIs remain issue #53. Manifest v8 retains the v7
generation-bound persistent-schema fingerprint requirement in addition to this
catalog enforcement.

The schema gate admits no new routed work after migration begins and waits for
previously admitted operations to drain. During active preflight or apply,
ordinary requests and a second migration coordinator receive retryable `Busy`
(HTTP 503). If durable journal progress remains after failure or cancellation,
ordinary requests receive non-retryable `FailedPrecondition` (HTTP 409); the
same migration call may resume it. Cancellation before journal creation leaves
no schema change. Cancellation during one shard transaction rolls that
transaction back, although a commit that wins a close race remains durable and
is reconciled from the retained journal prefix.

### Current HTTP parameter and result conversion

Use SQLite positional placeholders such as `?1` and `?2`. Values are never
interpolated into SQL text. The HTTP adapter converts JSON parameters into
protocol-neutral BriskDB values; the SQL layer binds only those typed values.
An empty catalog binds the caller's SQLite markers on the legacy path. A
populated catalog runs `normalize_placeholders(CommonSql)` and executes its
strict translated `?N` SQL; named SQLite markers accepted by the legacy path
are therefore not part of the registered-catalog contract.

| JSON input | SQLite binding |
| --- | --- |
| `null` | `NULL` |
| `true` / `false` | `INTEGER` `1` / `0` |
| Signed integer representable as `i64` | `INTEGER` |
| Unsigned integer no greater than `i64::MAX` | `INTEGER` after a checked conversion |
| Unsigned integer greater than `i64::MAX` | Rejected; it is not rounded to `REAL` |
| Accepted fractional or exponent-form JSON number | `REAL` through `f64` |
| Number outside `serde_json`'s accepted range | Request decoding fails before SQLite binding |
| String | `TEXT` |
| Array or object | Compact JSON stored as `TEXT` |

SQLite results map to protocol-neutral BriskDB `Null`, `Int64`, `Float64`,
`Text`, `InvalidText`, and `Binary` values. Valid `TEXT` becomes `Text`; invalid
UTF-8 remains byte-for-byte intact in `InvalidText`. The wider value model also
has `UInt64` and a validated, exact string-backed `Decimal` variant for protocol
inputs. Decimal construction accepts SQL-style signed decimal and exponent
syntax and preserves its original digits and scale.
SQLite binding accepts `UInt64` only through a checked conversion to `i64` and
rejects larger values, `Decimal`, `InvalidText`, and `Float64(NaN)` instead of
rounding, rewriting, or allowing SQLite to turn `NaN` into `NULL`. Infinite
`Float64` values remain SQLite `REAL` values. Ordered column metadata and
positional rows are preserved inside `ResultSet`; SQLite result-column metadata
is marked `Unknown` because dynamic SQLite values do not guarantee one static
type.

The experimental `/v1/query` response exposes the ordered result directly. The
admin row-page endpoint reuses the same ordered columns and positional rows and
wraps them with physical-shard and pagination metadata. Its large-integer
display encoding differs as described below. For example, the existing query
shape is:

```json
{
  "shard": 0,
  "columns": [
    {"name": "value", "data_type": "unknown"},
    {"name": "value", "data_type": "unknown"}
  ],
  "rows": [[1, 2]]
}
```

A successful one-target query contains `"shard": N`. A logical query that
visits multiple targets replaces that member with `"shards": [N, ...]`; the
array is unique and sorted in physical-shard order, matching the order used to
concatenate its per-shard rows.

For every row, `rows[row_index][column_index]` is described by
`columns[column_index]`. Column names may be duplicated or empty and are never
used as JSON object keys. A query that produces no rows still returns all of its
ordered column metadata with `"rows": []`. The `data_type` label is one of
`unknown`, `null`, `boolean`, `int64`, `uint64`, `float64`, `decimal`, `text`,
or `binary`. SQLite result columns currently report `unknown` because SQLite
does not guarantee one static result type.

The `/v1/query` cell encoding retains the existing HTTP policy: nulls, booleans,
signed and unsigned integers, finite floats, and valid text use their direct
JSON forms; binary data is an array of byte-valued JSON integers; and exact
decimals are JSON strings. `InvalidText` is rendered lossily with invalid UTF-8
byte sequences replaced by U+FFFD. Because JSON has no non-finite number syntax,
infinite or `NaN` `Float64` values become `null`. Consumers that decode every
`/v1/query` JSON number through binary floating point must also account for
precision loss when reading large integer cells.

The admin row-page response keeps direct JSON integers within
`-9007199254740991..=9007199254740991`. It represents larger signed or unsigned
values as
`{"$briskdb_type":"int64","value":"exact decimal text"}` or the equivalent
`uint64` tag, and the embedded browser displays that text verbatim. This
admin-only tag avoids JavaScript rounding without changing `/v1/query`.

This ordered response intentionally replaces the earlier experimental
object-per-row shape, which collapsed duplicate names. Admin pages preserve the
same indexed relationship instead of converting rows into name-keyed objects.
The conversion changes only HTTP serialization; request fields, routing,
configuration, the manifest, shard files, and stored data are unchanged.

### Current error contract

The engine exposes stable protocol-neutral error kinds. The HTTP adapter maps
them to safe RFC 9457 problem details without serializing SQLite messages, SQL
text, filesystem paths, or internal source chains. SQL and storage classify
SQLite result codes and operation context; error-message text is never parsed
to choose an error kind.

The same kinds already have defined PostgreSQL SQLSTATE and MySQL error
number/SQLSTATE mappings. PostgreSQL startup emits a finite fixed fatal-error
table, and its current query boundary emits `Unsupported` / `0A000` without
query text; the private selected-adapter probe also consumes the engine mapping.
The MySQL mapping remains a contract for its future adapter, and no MySQL
listener is available. See the complete
[error taxonomy and mapping table](ERRORS.md) and the
[PostgreSQL listener lifecycle](POSTGRES_LISTENER.md).

## SQL surface

### Implemented syntax boundary

BriskDB uses an exact post-0.62 upstream `sqlparser` snapshot behind its own
dialect and parsed-batch types. The pinned snapshot contains corrected
`parse_interval` recursion accounting plus other reviewed upstream changes
after the `v0.62.0` tag. Callers select SQLite, PostgreSQL, or MySQL explicitly;
generic parsing, dialect autodetection, and fallback parsing are not available.
Exact SQL is retained because formatting an AST is not source preserving.
Compatibility translation renders a separate canonical SQLite representation;
the current network paths do not consume that representation.

Parsing establishes only that one dialect recognizes the syntax. It does not
make a statement part of BriskDB's supported common subset or establish that
its behavior matches SQLite. Inputs are bounded to 65,536 UTF-8 bytes, 256
statements, and recursion depth 32. The parser can represent an ordered batch,
but the current execution surfaces retain their existing endpoint-specific
single-statement and migration rules. The implemented classifier supplies the
shared common-SQL batch rule described below.

`validate_common_subset(ParsedSql)` is the separate support-validation step. It
consumes the opaque parsed result and returns an owned opaque `CommonSql` only
when every top-level statement and nested form is in the first subset. Empty
and mixed batches may validate because classification is a separate step.
`classify_statements(&CommonSql)` borrows that marker and returns ordered
behavior only when the complete batch passes policy.
`normalize_placeholders(CommonSql)` then returns an owned `NormalizedSql`
with canonical SQLite parameter text and one parameter record per statement.
`translate_sql` can consume it and return an owned `TranslatedSql` containing a
separate SQLite representation while retaining the complete normalized result.
`infer_shard_keys` can use that retained result with catalog context and a
complete bound-value slice for one statement. `Engine::plan_bound_statement`
consumes that same concrete bind when a caller needs physical routes and a
validated single-shard assignment. The SQL wrapper types retain exact source,
dialect, and statement count without exposing the upstream AST or rendering
SQL in `Debug` output.

The parser, validator, normalizer, and translator have no routing or storage
access. The implemented inference layer borrows only the read-only logical
catalog and protocol-neutral bound values; it consumes structural syntax,
never regular-expression matches over raw or formatted SQL. See the [SQL
parser decision record](SQL_PARSER.md) for the dependency and resource
contract, the [common SQL subset contract](SQL_SUBSET.md) for the normative
recursive whitelist, the [SQL parameter-normalization
contract](SQL_PARAMETERS.md) for numbering and source-preservation rules, and
the [SQL translation contract](SQL_TRANSLATION.md) for modes and the finite
type and syntax matrix. The [shard-key inference contract](SQL_SHARD_KEYS.md)
defines the supported proof grammar and typed result. The [bound
statement-planning and routing-policy contract](SQL_PLANNING.md) defines
canonical route bytes, occurrence ordering, complete batch admission,
physical-target comparison, write rejection, selected behavior, provenance,
and the non-executable planning boundary. The [statement and batch
classification contract](SQL_STATEMENT_CLASSIFICATION.md) defines the precise
taxonomy and request matrix.
The [prepared statements and bound portals
contract](SQL_PREPARED_STATEMENTS.md) defines the composed exact-one-statement
lifecycle, session limits, metadata, routing snapshots, current execution
planning, and supported physical targets.

### Implemented HTTP SQLite surface

The following operations work through the matching HTTP endpoint. “Legacy”
below means the authoritative table catalog is empty; registered databases use
the bounded catalog-aware path.

| Operation | Current contract | Important boundary |
| --- | --- | --- |
| Persistent DDL, including `CREATE`, `DROP`, and `ALTER` | Execute only through the migration/broadcast endpoint | Per-shard atomic and crash-resumable; with a populated catalog, row-moving DML, table drops, trigger creation, and any final catalog violation are rejected |
| `INSERT`, `UPDATE`, `DELETE` | Legacy: caller-selected shard. Registered: exactly one inferred owner, compatible with the caller route | Inserts prove every row key; shard-key updates and multi-owner writes are rejected |
| `SELECT` | Legacy: caller-selected shard. Registered: exact owner, distinct owners for finite keys, every owner for unconstrained Sharded reads, or shard 0 once for Global/table-free reads | Multi-shard execution accepts only unchanged row-local single-table reads and concatenates with `UNION ALL`; unsupported global semantics are rejected |
| SQLite expressions and functions | Legacy syntax may pass through; registered execution accepts only the common subset and strict SQLite translation | Executed semantics remain SQLite semantics |
| SQLite constraints | SQLite enforces each accepted constraint in one shard | Every sharded `PRIMARY KEY`/`UNIQUE` key includes the `BINARY` shard key, so all possible collisions have one owner; no independent global reservation service exists |

Hidden `rowid`, `_rowid_`, and `oid` values on Sharded tables are physical and
shard-local. They are not globally unique logical identities and must not be
used to infer ownership or cross-shard ordering. A visible `INTEGER PRIMARY
KEY` alias remains an ordinary declared key and follows the catalog's normal
locality rules.

Other SQLite syntax may happen to pass through only on the empty-catalog legacy
path and is not a stable BriskDB contract. In
particular, multi-request transactions, multi-shard writes, multiple statements
outside the migration endpoint, and attached-database operations are
uncontracted public API behavior today. Persistent DDL outside the migration
endpoint is explicitly denied. DML `RETURNING` is explicitly rejected from the
query surface, and the execute surface exposes only a rows-affected count,
never a returned rowset. Both empty- and populated-catalog HTTP modes use those
same engine boundaries.

The current `Session` `Ready`/`Closed` lifecycle is not a transaction state
machine. Real `BEGIN`/`COMMIT`/`ROLLBACK`, failed-transaction behavior, and
single-shard pinning remain planned for the PostgreSQL and MySQL transaction
work in issues #34 and #47.

The synchronous public Rust `Database` methods remain available for source
compatibility. Network frontends use `Engine`; retaining `Database` does not
authorize an adapter to bypass the shared asynchronous boundary.

### Implemented structural common subset

The opt-in validator recursively admits these statement families:

| Family | Structural contract |
| --- | --- |
| `CREATE TABLE` | One unqualified persistent table, optional `IF NOT EXISTS`, one or more columns with any explicit parsed type, and the supported literal defaults plus primary-key, unique, and check constraints |
| `CREATE INDEX` | A named optional-unique index on plain columns of one unqualified table; `IF NOT EXISTS` and `ASC`/`DESC` are accepted |
| `SELECT` | A nonempty projection with zero or one plain table; optional simple alias, `ALL`/`DISTINCT`, scalar filtering/grouping, `HAVING`, expression ordering, and standard or MySQL/SQLite comma-form numeric-or-placeholder limit/offset; PostgreSQL `LIMIT ALL` is parser-equivalent to no limit |
| `INSERT` | One named table, an explicit column list unique under conservative ASCII case folding, and one or more equal-width `VALUES` rows |
| `UPDATE` | One plain table, single-column assignments whose targets are unique under conservative ASCII case folding, and an optional predicate |
| `DELETE` | One plain `FROM` table and an optional predicate |
| Transactions | Plain `BEGIN`/`BEGIN TRANSACTION`/`BEGIN WORK`; the semantic commit AST including parser-accepted `TRANSACTION`/`WORK`/`TRAN` suffix aliases and explicit `AND NO CHAIN`; and the equivalent full-rollback AST including those forms and `ABORT`; no modes, `AND CHAIN`, blocks, or savepoints |

Scalar expressions include the documented literals and placeholders,
one-/two-part column references, arithmetic/comparison/Boolean operators,
null/Boolean predicates, nonempty `IN`, `BETWEEN`, `LIKE` without `ESCAPE`, and
`CASE`. Projection, `HAVING`, and ordering also admit unqualified, unquoted
`COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` with one expression argument, including
duplicate treatment; `COUNT(*)` is supported. Scalar functions, casts,
subqueries, joins, CTEs, set operations, windows, DML `RETURNING`, upserts,
foreign keys, generated columns, expression/partial indexes, and all other
forms are outside this first subset. The exact clause, expression, constraint,
name, and diagnostic rules are normative in
[the common SQL subset contract](SQL_SUBSET.md).

Structural acceptance is not permission to scatter a statement. When a
registered Sharded read selects more than one physical target, the issue-57
baseline requires one plain table and row-local projection/filter expressions
whose translated SQL can execute unchanged per shard. It rejects `DISTINCT`,
all functions and aggregates, grouping, ordering, limit/offset, joins,
subqueries, CTEs, set operations, and windows. A one-target read may still use
the broader implemented subset because SQLite evaluates it only once.

This implemented status means structural validation exists and is tested. The
subset is connected to prepared execution and populated-catalog HTTP
execute/query, but remains directly callable analysis and is not the
empty-catalog or migration contract. Column type names need only be explicit at
validation; the separate compatibility translator applies its
finite dialect-specific declaration whitelist. Insert/update duplicate checks
fold ASCII letter case regardless of quoting. Compatibility translation
canonicalizes accepted backtick-quoted identifiers, but does not define general
case folding, Unicode normalization, collation, or catalog equivalence.
Placeholder normalization is the separate implemented issue #21 layer described
below, the translator is the implemented issue #25 layer, and catalog-aware
typed key extraction is the implemented issue #22 layer. Synchronous bind-time
route construction is the implemented issue #23 layer, and issue #24 adds
physical-target comparison, assigned shards, and rejection of conflicting or
unroutable cataloged sharded writes.
Issue #26 adds the prepared/bind/describe/execute state described below. The
implemented classifier adds general behavior and batch policy; a prepared
handle retains its stricter exactly-one-statement cardinality rule.

The validator independently caps recursive expression AST depth at 128. This
also bounds flat operator chains that parse iteratively; exceeding the limit is
reported as `LimitExceeded`.

The driver-capable Rust execution path now consumes accepted sharded writes and
supported read targets and creates a fresh plan under every execution's current
schema guard. Persistent `CREATE TABLE` and `CREATE INDEX` must still use
journaled schema migrations, and real transactions still require
protocol-neutral transaction state. Cross-shard transactions remain later
request/session policy.

### Implemented statement and batch classification

The public Rust function `classify_statements(&CommonSql)` borrows the opaque
validated AST and returns an owned ordered `StatementBatchClassification`.
Behavior is derived structurally and is the same for SQLite, PostgreSQL, and
MySQL source:

| Statement | Behavior |
| --- | --- |
| `SELECT` | `Read` |
| `INSERT`, `UPDATE`, `DELETE` | `Write(Insert)`, `Write(Update)`, `Write(Delete)` |
| `CREATE TABLE`, `CREATE INDEX` | `Schema(CreateTable)`, `Schema(CreateIndex)` |
| `BEGIN`, `COMMIT`, `ROLLBACK` | `Session(Begin)`, `Session(Commit)`, `Session(Rollback)` |

Empty/comment-only input is `InvalidArgument`. Every singleton behavior can be
classified, but a batch of two or more is accepted only when all members are
`Read`; the first non-read member makes the whole batch `Unsupported`. This
classification is not by itself execution permission. Diagnostics and `Debug`
metadata never expose submitted SQL, literals, identifiers, or AST output.

`Engine::plan_bound_statement` applies that complete batch gate before it
selects a statement index and exposes the selected `behavior()` in its owned
plan. Direct shard-key inference remains statement-local. The prepared pipeline
uses the same classifier after its exact-one check and exposes behavior in
`PreparedStatementDescription`. The exact matrix, precedence, and boundary are
normative in [SQL statement and batch
classification](SQL_STATEMENT_CLASSIFICATION.md).

### Implemented placeholder normalization

The public Rust function `normalize_placeholders(CommonSql)` is opt-in and
accepts no bound values. Its `NormalizedSql::sqlite_parameter_sql()` output
retains every non-marker byte while representing each accepted marker as a
canonical SQLite `?N`. `statement_parameters()` reports each statement's
largest parameter index, occurrence count, and occurrence-to-index sequence.
Numbering restarts per statement; those records do not grant permission to
execute a batch.

PostgreSQL `$N` retains index `N`, including repeats, gaps, and out-of-order
occurrences. MySQL bare `?` markers receive consecutive indices from one.
SQLite positional `?` and `?NNN` follow SQLite's native max-so-far rule, while
SQLite named markers are deliberately unsupported. No assigned index may
exceed `MAX_SQL_PARAMETERS` (32,766). The complete API, examples, limits, and
error contract are in [SQL parameter normalization](SQL_PARAMETERS.md).

This implemented normalization does not bind or count supplied values, infer a
shard, translate other dialect syntax, create a prepared statement, classify
statement behavior, authorize a request, or execute SQL by itself. Translation
is the separate opt-in layer described next. The prepared lifecycle composes
both layers. Populated-catalog HTTP execute/query also runs normalization before
strict SQLite translation; empty-catalog HTTP and the migration identity path
retain their separate legacy/exact-text behavior.

### Implemented SQL translation

The public Rust function `translate_sql(NormalizedSql, SqlTranslationMode)` is
an opt-in, protocol-neutral step. There is deliberately no default mode.
`StrictSqlite` accepts only SQLite source and returns
`NormalizedSql::sqlite_parameter_sql()` byte for byte. `Compatibility` clones
the validated AST, applies the documented finite mappings, and renders a
separate canonical SQLite string. Both results retain the exact original source
and complete normalized placeholder metadata.

The compatibility type whitelist is keyed by source dialect. It maps selected
signed integer declarations to `BIGINT`, Boolean declarations to `BOOLEAN`,
selected 64-bit floating-point declarations to `REAL`, variable text to `TEXT`,
and variable binary to `BLOB`. It rejects declarations outside that matrix
instead of choosing an uncontracted representation. Strict SQLite translation
continues to preserve arbitrary validated SQLite declared type names.

The syntax mappings cover accepted backtick-quoted identifiers, Boolean
literals, transaction aliases, and MySQL/SQLite comma-form limits. Placeholder
indices retain their statement-local source identities even when comma-limit
operands are reordered. This layer does not implement server-specific value or
result metadata adapters, identifier case folding, collations, function shims,
upserts, wire session behavior, preparation, routing, or execution. The exact
matrix and error contract are in [SQL translation](SQL_TRANSLATION.md).

### Implemented shard-key inference

The public Rust function `infer_shard_keys` is an opt-in statement-local call
over a `Catalog`, selected `LogicalDatabaseId`, `NormalizedSql`, zero-based
statement index, and exact bound `Value` slice. It resolves known table
placement and a sharded table's key column/type. Its owned result distinguishes
not-applicable, non-sharded, unconstrained, contradictory, exact, and multiple
key outcomes and exposes typed integer, text, or binary values.

For `SELECT`, `UPDATE`, and `DELETE`, direct equality against an `Int64`, `Text`,
or `Binary` shard-key column produces a finite key set; Boolean `AND` intersects
and `OR` unions those proofs. Authoritative registration requires Text keys to
use SQLite `BINARY` collation, so inference compares their exact UTF-8 bytes
without case folding or Unicode normalization.
Other predicates do not establish a key. For `INSERT`, every `VALUES` row's
explicit shard-key cell must be a compatible direct literal or placeholder to
produce a complete result, and one value per row is retained, including text
values. The exact identifier, value, result, and error rules are in [shard-key
inference](SQL_SHARD_KEYS.md).

Inference by itself does not encode, hash, route, plan, authorize, enforce, or
execute. The implemented planner described below uses the result at
bind/execute time to construct routes, validate physical-target compatibility,
and reject unroutable sharded DML. Prepared execution and populated-catalog HTTP
execute/query invoke that policy; the empty-catalog legacy path does not.

### Implemented bound statement planning

The synchronous public Rust method `Engine::plan_bound_statement` accepts a
logical database, `NormalizedSql`, zero-based statement index, exact bound
`Value` slice, and optional explicit routing bytes. It applies the complete
batch gate before selecting the member, then runs inference only after those
concrete values exist and returns an owned `BoundStatementPlan`.

Each inferred `Int64` becomes shortest signed decimal ASCII, `Text` becomes
exact UTF-8, and `Binary` remains exact bytes. Explicit routing bytes also
remain exact. The plan retains one route per inferred occurrence in matching
order, including duplicate multi-row values and distinct keys that choose the
same shard. Its explicit route remains separate even when it agrees with an
inferred key or shard.

Finite inferred routes take precedence. Optional explicit context must select
the same physical shard as every inferred occurrence; matching raw bytes are
not required. Multiple logical keys are accepted for a write only when they
co-locate on one physical shard. `INSERT` must prove every row's key;
unconstrained or contradictory `UPDATE`/`DELETE` requires explicit fallback;
and every assignment to a cataloged shard-key column is rejected. A successful
plan exposes the selected `behavior()`; a successful single-shard plan also
exposes `assigned_shard()`.

Planning holds the schema-operation guard while consulting the catalog. The
result records schema generation, routing-map generation, and hash,
key-encoding, and bucket-algorithm versions. Those fields describe the snapshot
used to build the plan; they do not reserve that snapshot for future execution.
The method does not translate SQL, mutate a session, cache a prepared
statement, scatter reads, or execute SQLite. It does apply complete
statement/batch policy; direct `infer_shard_keys` remains statement-local. The
exact matrix and error precedence are in [bound statement planning and routing
policy](SQL_PLANNING.md).

### Implemented prepared-statement lifecycle

The public async Rust engine accepts a `PrepareRequest` with one logical
database, explicit source dialect, explicit translation mode, and SQL string.
Prepare runs parsing through classification and translation, requires exactly
one top-level statement, and transiently compiles parameter/column metadata on
shard 0. A session retains only BriskDB-owned translated SQL, precise logical
behavior, and owned metadata; no SQLite statement or connection is cached.

Binding validates a complete typed `Value` slice with a transient plan,
snapshots the session's current routing key, and returns an opaque
session-scoped `PortalId`. The portal retains the values and routing snapshot,
not a plan. Describe returns ordered columns and one `Unknown` type per
parameter/result column, exposes the classified behavior, and refreshes column
metadata after schema migration without changing that behavior. Every execution
creates a fresh plan under its current schema guard. It runs accepted sharded
work on its assigned shard and classified safe `NotApplicable`/`Global` reads
on deterministic shard 0 through the compatibility execution method. Logical
portal execution additionally visits the distinct metadata-selected targets of
a finite Sharded read or every shard for an unconstrained Sharded read. Catalog
access remains denied. Persistent schema prepare is denied before a handle is
published, and session behavior cannot execute through a portal. Results use
the same protocol-neutral values for SQLite, PostgreSQL, and MySQL source;
logical results also report the sorted, unique physical shards visited.

Per-session statement, portal, retained-bound-value, and per-bind planning
limits are finite and have no implicit eviction. Planning preflight charges the
captured route once and each marker occurrence twice. Explicit close releases
entries; closing a statement also invalidates all dependent portals. The API,
defaults, logical byte accounting, request controls, errors, and storage
boundary are normative in
[prepared statements and bound portals](SQL_PREPARED_STATEMENTS.md).

## PostgreSQL differences

The PostgreSQL TCP listener hosts the selected `pgwire` 0.36.3 boundary for
exact protocol-3.0 startup on loopback. It validates a finite parameter set,
selects one logical database and user label, advertises BriskDB-owned status,
and tracks the selected core session through termination. Simple and
zero-parameter extended SQL messages use the bounded Engine lifecycle;
extended errors discard messages until `Sync`. PostgreSQL-specific behavior is
not implemented unless listed as implemented in this document.
Configuration and lifecycle semantics are normative in the [PostgreSQL listener
contract](POSTGRES_LISTENER.md); dependency and adapter constraints are
normative in the [adapter decision record](POSTGRES_ADAPTER.md).

| Area | PostgreSQL | BriskDB contract |
| --- | --- | --- |
| Parameters | `$1`, `$2`, ... | Named/unnamed wire prepare/bind/describe/execute works for zero parameters; OID/value mapping remains issue #33 |
| Identifier quoting | Double quotes | Retained by opt-in compatibility translation; PostgreSQL case folding and catalog equivalence are not claimed |
| Type system | Static types identified by OIDs | Opt-in Rust translation maps a finite declaration set to `BIGINT`, `BOOLEAN`, `REAL`, `TEXT`, or `BLOB`; OID and value/result adaptation remain planned |
| Boolean | Dedicated `boolean` type | Opt-in translation maps the declaration to `BOOLEAN` and literals to `1`/`0`; Boolean wire/result metadata remains planned |
| `BIGSERIAL`, identity, sequences | Sequence-backed generation | Compatibility translation accepts exactly inline `BIGSERIAL PRIMARY KEY` or `BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY` as `native_range_v1` intent and canonical SQLite `AUTOINCREMENT`; sequence objects/options and PostgreSQL wire result behavior remain unsupported |
| `bytea` | Binary value | Opt-in declaration translation maps it to SQLite `BLOB`; binary value and wire adaptation remain planned |
| `json` / `jsonb` | Distinct PostgreSQL types | Planned JSON validation; no promise of PostgreSQL `jsonb` storage or operators |
| Arrays, ranges, enums, domains | Native PostgreSQL types | Unsupported initially |
| Schemas and `search_path` | Multiple schemas per database | Unsupported initially; compatibility shims may expose one logical schema |
| `RETURNING` | Common DML feature | Not in the initial common subset |
| `ON CONFLICT` | PostgreSQL upsert syntax | Outside the initial common subset and unsupported |
| Functions/operators | PostgreSQL catalog | SQLite functions/operators unless an explicit shim is documented |
| System catalogs | `pg_catalog`, `information_schema` | Only queries required by named, tested clients will be emulated |
| Error behavior | SQLSTATE and failed transaction state | Startup and execution errors are encoded; extended flow resynchronizes at `Sync`, while complete `I`/`T`/`E` transaction states remain planned |
| `COPY`, replication, `LISTEN/NOTIFY` | PostgreSQL subprotocols/features | Deferred and unsupported initially |

PostgreSQL's static result metadata does not always have an exact equivalent in
SQLite's dynamic type system. BriskDB will honor declared and bound types where
safe, infer expression types conservatively, and reject indeterminate binary
parameters instead of guessing.

## MySQL differences

The MySQL listener will target the client/server protocol and the same core SQL
engine used by PostgreSQL and HTTP.

| Area | MySQL | BriskDB contract |
| --- | --- | --- |
| Parameters | `?` in prepared statements | Rust normalization and session-scoped prepare/bind/describe/execute are implemented; MySQL command/type mapping remains planned |
| Identifier quoting | Backticks by default | Opt-in compatibility translation emits safely escaped double-quoted SQLite identifiers; case and collation equivalence are not claimed |
| Type system | Static signed/unsigned column types | Opt-in Rust translation maps a finite signed integer, Boolean, 64-bit float, text, and binary declaration set; unsigned declarations are rejected, and translation performs no value or result adaptation. Independently, BriskDB retains `UInt64` values without narrowing, while current SQLite binding rejects values above `i64::MAX` until a storage mapping exists |
| Boolean | Commonly `TINYINT(1)` | Opt-in translation maps exactly `TINYINT(1)` to `BOOLEAN` and Boolean literals to `1`/`0`; wire/result metadata remains planned |
| `AUTO_INCREMENT` | Table column attribute | Compatibility translation accepts inline `BIGINT` with `PRIMARY KEY AUTO_INCREMENT` or `AUTO_INCREMENT PRIMARY KEY` as `native_range_v1` intent and canonical SQLite `AUTOINCREMENT`; one omitted-key row can execute through the gated shared engine, while MySQL wire behavior remains issue #44 |
| `UNSIGNED`, display widths | MySQL column attributes | Rejected by compatibility translation, except that exactly `TINYINT(1)` is the documented Boolean declaration |
| `DATETIME`, `TIMESTAMP` | Distinct MySQL temporal behavior | No implicit compatibility; canonical timestamp encoding must be defined first |
| `JSON` | Native MySQL JSON type | Planned JSON validation stored using the canonical BriskDB representation |
| `LIMIT offset,count` | MySQL syntax | Opt-in compatibility translation emits `LIMIT count OFFSET offset` while retaining placeholder identities |
| `ON DUPLICATE KEY UPDATE` | MySQL upsert syntax | Outside the initial common subset and unsupported |
| Engines, character sets, collations | Per-table/column options | Engine clauses unsupported; charset/collation behavior must be explicitly mapped |
| Session probes | `SET NAMES`, `SHOW VARIABLES`, `SELECT @@...` | Only the subset required by named, tested clients will be emulated |
| Metadata | `information_schema` and MySQL metadata commands | Minimal tested compatibility only |
| Errors | MySQL error number plus SQLSTATE | Stable error-kind mapping defined; listener and wire encoding planned |
| Stored programs, binlog, `LOAD DATA` | MySQL-specific facilities | Deferred and unsupported initially |

## SQLite semantic differences

SQLite is the execution authority. Unless BriskDB documents a compatibility
translation, its behavior follows SQLite rather than PostgreSQL or MySQL.
Compatibility translation is a finite mapping, not a claim of full server
semantics. `StrictSqlite` preserves normalized SQLite source instead.

- SQLite values use the `NULL`, `INTEGER`, `REAL`, `TEXT`, and `BLOB` storage
  classes. Ordinary tables use type affinity rather than rigid column typing.
- SQLite has no separate Boolean storage class; false and true are represented
  by integer zero and one.
- SQLite has no dedicated date/time storage class. Applications can store time
  values as text, real, or integer values, so BriskDB must choose a canonical
  representation before promising cross-protocol temporal compatibility.
- Declared sizes such as `VARCHAR(255)` do not impose the same length behavior
  as PostgreSQL or MySQL.
- Numeric conversion, collation, null ordering, division, and comparison rules
  can differ from both server databases. Distributed merge operations must
  reproduce the chosen SQLite semantics explicitly.
- SQLite `STRICT` tables can provide stronger enforcement, but BriskDB does not
  enable them implicitly. That table option is unrelated to
  `SqlTranslationMode::StrictSqlite`.

See SQLite's official [SQL language reference](https://www.sqlite.org/lang.html)
and [datatype documentation](https://www.sqlite.org/datatype3.html) for the
underlying execution semantics.

## Sharding semantics

### Current

- HTTP execute continues to require an opaque caller `shard_key`. Empty-catalog
  legacy query also requires it. A populated-catalog logical query derives its
  targets from registered placement and SQL inference instead of using that
  caller context to narrow the shard set.
- Exact key bytes are hashed with version-1 BLAKE3; the little-endian 64-bit
  prefix selects one of 4,096 virtual buckets through the versioned
  compatibility algorithm.
- The final physical shard is read from the validated, generation-stamped
  bucket map retained in manifest version 9. Routing generation 1 preserves
  the earlier modulo placement for every supported shard count.
- Manifest version 9 retains the read-only catalog view introduced in v4, with
  a journaled schema generation from 0 through 2,147,483,647 and default
  database ID 1 named `default`. `Database::register_tables` may populate its
  table rows exactly once during initialization after proving that every shard
  has the same complete, empty physical table set. A sharded declaration names
  one visible, physically non-null `Int64`, text, or binary key column; the
  `INTEGER PRIMARY KEY` rowid alias qualifies, but nullable legacy primary-key
  forms do not. Text keys must use SQLite `BINARY` collation. Foreign keys must
  satisfy the conservative local-enforcement matrix; triggers and virtual
  tables remain unsupported, and
  every sharded primary/unique key must contain the shard key with `BINARY`
  collation.
- Registration changes schema admission to `Pending` before manifest commit.
  If commit reports an ambiguous cleanup or I/O error, close the stale
  registering handle and reopen the data root so startup can determine whether
  the old or complete new catalog committed. A live stale handle deliberately
  prevents publication of a conflicting durable catalog.
- The ready layout retained from v5 binds every shard to one random 16-byte
  layout ID and its
  physical shard ID. Each connection is opened without create or symlink
  following and must match the `BRSH` application ID, expected schema
  generation, exact metadata, and existing WAL mode.
- Present version-8 table metadata is authoritative. `Sharded` means each
  ordinary row has exactly one key-selected physical owner; `Global` is the
  explicit replicated placement; `Catalog` is manifest-only. The v7-to-v8
  upgrade clears legacy advisory rows rather than promoting them, and existing
  physical tables are never inferred or adopted. Registration requires empty
  tables and does not repartition existing data. The opt-in Rust APIs,
  prepared execution, and populated-catalog HTTP execute/query all consult the
  same catalog; only an empty catalog retains caller-key-only routing.
- Opt-in inference can extract typed cataloged keys from supported equality
  predicates and every row of a supported `INSERT`.
- Opt-in bound planning converts every inferred occurrence to an owned route,
  retains an independent explicit route, and records catalog/routing
  provenance. It compares finite routes by physical shard, rejects unroutable
  cataloged sharded writes, and records an accepted single-shard assignment.
- The Rust prepared lifecycle snapshots bound values and session routing in an
  immutable portal, transiently validates at bind, and plans on every
  execution. Its logical read method and populated-catalog HTTP query execute
  supported Sharded target sets without a second prepared cache; `Global` and
  table-free reads use shard 0 once.
- Point queries and writes visit only that key's owning shard.
- Finite multi-owner reads visit each distinct owner; unconstrained Sharded
  reads visit every shard. At most eight tasks run concurrently, and successful
  rows merge in ascending shard order with duplicates preserved as `UNION ALL`.
  Every target shares one request deadline, cancellation source, and result
  budget; any target failure yields no partial result.
- The issue-57 baseline does not implement global `DISTINCT`, aggregate,
  grouping, ordering, pagination, join, subquery, CTE, set-operation, or window
  semantics. Those forms are rejected when more than one shard is selected.
- SQLite transactions remain local to one shard. Registration accepts a
  sharded primary/unique key only when it contains the `BINARY` shard-key term,
  ensuring every possible collision is checked by one owning SQLite file.
- Schema migration preflights every shard, then commits an ascending prefix
  under a retained journal. Each shard is atomic; the shard set is not one
  transaction. The manifest preserves committed-source and target schema
  fingerprints, and startup resumes only an exact checksummed prefix before
  serving work. Once tables are registered, preflight also rejects a migration
  that changes the complete physical table set or a sharded key's required
  column, affinity, or Text collation; creates row-moving DML, drops a table, or
  creates a trigger; introduces an unsafe or malformed foreign key, trigger, or
  virtual table; or breaks one-owner unique-key locality.

### Planned stable contract

- Canonical key encoding, hash version, virtual bucket count, and bucket map are
  persisted in the manifest.
- A transaction is pinned to its first shard. Targeting another shard returns a
  stable cross-shard-transaction error.
- Later planner work may extend bounded logical reads with global filtering,
  ordering, pagination, aggregation, and other non-row-local semantics.
- Cross-shard writes remain unsupported unless a future coordinator can prove
  its crash semantics.
- A future reservation design is required for a unique key that intentionally
  omits the shard key. The current registration contract rejects that shape
  instead of exposing shard-local duplicates as logical uniqueness.

## Transactions and concurrency

SQLite provides atomic transactions within one database file. BriskDB will not
describe sequential commits to several shard files as atomic. The initial SQL
session contract will therefore pin explicit transactions to one shard and
reject cross-shard access.

Manifest-format migrations are separate from application SQL migrations. They
run internally during storage open, are transactional only within
`manifest.sqlite`, and cannot be requested through any protocol or SQL
statement. The atomic version-3-to-version-4 manifest upgrade added advisory
logical metadata and its downgrade fence. Version 8 clears any surviving v7
advisory table rows, installs a stronger downgrade fence, and allows a later
initialization-only registration to establish authoritative placement. Neither
upgrade infers, adopts, or repartitions physical rows. Version 5 added a resumable
physical-layout state machine. Its `Adopting` path accepts only exact legacy
zero-header WAL shards, preserves their tables and rows, and adds BriskDB
identity metadata. Version 6 adds the application-schema journal. A migration
batch and generation are atomic within each shard, and final journal state plus
catalog generation are atomic within the manifest, but no transaction spans
those files. Ascending-prefix validation and replay provide recovery rather
than cross-file atomicity. Version 7 adds the semantic manifest root, explicit
integrity states, and generation-bound shard-schema fingerprints; journal,
state, checksum, and catalog changes reseal atomically within the manifest.
Version 8 retains those integrity rules and includes authoritative table
registration in the same semantic root. Version 9 adds explicit generated-ID
policies and immutable allocation-owner slots, upgrades the manifest root to
checksum version 2, and preserves every existing table as policy `None`.
See the [manifest storage-format contract](STORAGE_FORMAT.md).

Scatter reads combine committed results from multiple SQLite files. They do not
claim an atomic cross-file snapshot; the files can observe different committed
instants while the bounded tasks run.

## Compatibility verification

A syntax or behavior moves from planned to implemented only with tests at the
right boundary:

- unit tests for parsing, normalization, translation, routing, type conversion,
  and errors;
- golden tests for wire messages, placeholders, type metadata, and error codes;
- differential tests against a single SQLite reference database for supported
  SQL and scatter/gather behavior;
- integration tests using named PostgreSQL, MySQL, and HTTP clients; and
- regression tests for every corrected compatibility bug.

The compatibility matrix will name the client and version tested. Compatibility
with an unlisted ORM, administration tool, extension, or server feature is not
implied.

## Change policy

Changes to SQL behavior, type mappings, sharding semantics, error mappings, or
the supported client matrix must update this document in the same pull request.
An implemented claim must not be merged without automated coverage. Removing a
supported behavior requires a migration or compatibility note before the first
stable release and follows the project's eventual versioning policy afterward.

## Protocol references

- [PostgreSQL SQL syntax](https://www.postgresql.org/docs/current/sql-syntax.html)
- [PostgreSQL frontend/backend protocol](https://www.postgresql.org/docs/current/protocol.html)
- [PostgreSQL error codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)
- [MySQL SQL statements](https://dev.mysql.com/doc/refman/en/sql-statements.html)
- [MySQL client/server protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html)
- [MySQL server error reference](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html)
