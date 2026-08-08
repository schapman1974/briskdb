# SQL compatibility

BriskDB stores data in SQLite and is designed to expose the same database
engine through HTTP, PostgreSQL, MySQL, and future protocol adapters. A wire
protocol does not change SQLite into that protocol's namesake database. This
document defines the SQL and behavioral contract separately from connectivity.

## Status vocabulary

- **Implemented** means the behavior exists in the current source tree and is
  covered by the repository's tests.
- **Experimental** means the behavior exists, but its public contract may still
  change before the first stable release.
- **Planned** means the roadmap defines the behavior, but clients must not rely
  on it yet.
- **Unsupported** means BriskDB rejects the behavior or makes no compatibility
  promise for it.

The syntax parser, recursive common-subset validator, and placeholder
normalizer are now available behind BriskDB-owned types. Validation is explicit
and returns `Unsupported` for a parsed form outside the subset; parser
acceptance alone is not product support. Normalization is also explicit and
rewrites only validated placeholder spans. The current experimental HTTP
interface is still a raw SQLite pass-through and can execute uncontracted
SQLite syntax because it calls none of these layers. That behavior is not a
compatibility promise.

## Compatibility layers

BriskDB tracks three independent compatibility layers:

1. **Wire compatibility** lets an existing driver connect, authenticate,
   prepare and bind statements, receive typed rows, and manage session state.
2. **SQL compatibility** parses a documented common subset and translates
   selected PostgreSQL or MySQL syntax into SQLite operations.
3. **Behavioral compatibility** emulates the metadata, type, error, transaction,
   and session behavior needed by specifically tested clients and tools.

Passing a PostgreSQL or MySQL handshake will establish only wire compatibility.
BriskDB will publish behavioral compatibility per tested driver or tool rather
than claiming to be a drop-in PostgreSQL or MySQL replacement.

## Current implementation

Only the experimental HTTP network interface is implemented today. There is no
PostgreSQL or MySQL listener or general dialect translation layer yet. The
public Rust SQL facade can parse an explicitly selected SQLite, PostgreSQL, or
MySQL dialect, consume that result with
`validate_common_subset(ParsedSql)`, and then opt into
`normalize_placeholders(CommonSql)`. The final step yields canonical SQLite
`?N` text and per-statement parameter metadata without accepting values. None
of these operations plans, routes, generally translates, authorizes, or
executes a statement. HTTP requests still send SQLite SQL directly to
`rusqlite`; they do not pass through these opt-in SQL layers.

| Interface | Status | SQL accepted | Routing |
| --- | --- | --- | --- |
| HTTP `/v1/execute` | Experimental | One SQLite statement with positional parameters | Required caller-provided `shard_key` |
| HTTP `/v1/query` | Experimental | One prepared SQLite statement executed through the row-returning path | Required caller-provided `shard_key` |
| HTTP `/v1/admin/broadcast` | Experimental | A journaled parameterless SQLite schema batch | Preflight on every shard, then ascending resumable apply |
| PostgreSQL wire protocol | Planned | Common subset plus documented PostgreSQL normalization | SQL/bound-parameter inference with explicit session fallback |
| MySQL wire protocol | Planned | Common subset plus documented MySQL normalization | SQL/bound-parameter inference with explicit session fallback |

The parser, subset validator, and placeholder normalizer are implemented Rust
APIs, not network interfaces. Each step is opt-in and does not change any row
in this table.

Every HTTP operation now calls the same protocol-neutral async engine intended
for future PostgreSQL and MySQL adapters. Execute and query requests create a
fresh `Ready` session, put the request's `shard_key` in its routing context, and
submit an owned statement. The engine, rather than the HTTP adapter, selects the
shard and acquires a pooled SQLite connection. The session is discarded when
that HTTP request finishes, so a transaction cannot span requests.

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

SQLite forms that can leave connection-local state remain allowed by the
current one-call pass-through, but they are uncontracted. The pool observes
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

SQLite also retains `last_insert_rowid()`, `changes()`, and `total_changes()` on
the physical connection after ordinary writes. A write-bearing handle may be
reused by its owning BriskDB `Session`, but the pool closes and replaces it
before checkout by a different session. Read-only handles can cross sessions.
This preserves same-session write metadata without exposing one HTTP request's
connection-local counters to the next request when the owned handle remains
available. It does not pin that handle, so these observer functions remain
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
The migration does not infer or update advisory `briskdb_tables` rows. Richer
migration/history APIs remain issue #53. Manifest v7 independently requires a
generation-bound persistent-schema fingerprint to agree across every shard; it
does not claim the advisory catalog describes that schema.

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
This existing execution-time binding is separate from the opt-in
`normalize_placeholders(CommonSql)` Rust API: the HTTP adapter neither invokes
that function nor changes the caller's SQL marker text.

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

The experimental `/v1/query` response exposes the ordered result directly. For
example:

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

For every row, `rows[row_index][column_index]` is described by
`columns[column_index]`. Column names may be duplicated or empty and are never
used as JSON object keys. A query that produces no rows still returns all of its
ordered column metadata with `"rows": []`. The `data_type` label is one of
`unknown`, `null`, `boolean`, `int64`, `uint64`, `float64`, `decimal`, `text`,
or `binary`. SQLite result columns currently report `unknown` because SQLite
does not guarantee one static result type.

Cell encoding retains the existing HTTP policy: nulls, booleans, signed and
unsigned integers, finite floats, and valid text use their direct JSON forms;
binary data is an array of byte-valued JSON integers; and exact decimals are
JSON strings. `InvalidText` is rendered lossily with invalid UTF-8 byte
sequences replaced by U+FFFD. Because JSON has no non-finite number syntax,
infinite or `NaN` `Float64` values become `null`. Consumers that decode every
JSON number through binary floating point must also account for precision loss
when reading large `uint64` cells.

This ordered response intentionally replaces the earlier experimental
object-per-row shape, which collapsed duplicate names. It changes only HTTP
query serialization; request fields, routing, configuration, the manifest,
shard files, and stored data are unchanged.

### Current error contract

The engine exposes stable protocol-neutral error kinds. The HTTP adapter maps
them to safe RFC 9457 problem details without serializing SQLite messages, SQL
text, filesystem paths, or internal source chains. SQL and storage classify
SQLite result codes and operation context; error-message text is never parsed
to choose an error kind.

The same kinds already have defined PostgreSQL SQLSTATE and MySQL error
number/SQLSTATE mappings, but those are contracts for the planned adapters.
They do not make either wire-protocol listener available. See the complete
[error taxonomy and mapping table](ERRORS.md).

## SQL surface

### Implemented syntax boundary

BriskDB uses an exact post-0.62 upstream `sqlparser` snapshot behind its own
dialect and parsed-batch types. The pinned snapshot contains corrected
`parse_interval` recursion accounting plus other reviewed upstream changes
after the `v0.62.0` tag. Callers select SQLite, PostgreSQL, or MySQL explicitly;
generic parsing, dialect autodetection, and fallback parsing are not available.
Exact SQL is retained because formatting an AST is not source preserving and
formatted AST text is never sent to SQLite.

Parsing establishes only that one dialect recognizes the syntax. It does not
make a statement part of BriskDB's supported common subset or establish that
its behavior matches SQLite. Inputs are bounded to 65,536 UTF-8 bytes, 256
statements, and recursion depth 32. The parser can represent an ordered batch,
but the current execution surfaces retain their existing endpoint-specific
single-statement and migration rules; later classification will decide which
multi-statement combinations are safe.

`validate_common_subset(ParsedSql)` is the separate support-validation step. It
consumes the opaque parsed result and returns an owned opaque `CommonSql` only
when every top-level statement and nested form is in the first subset. Empty
and mixed batches may validate because request-level batch policy remains issue
#27. `normalize_placeholders(CommonSql)` then returns an owned `NormalizedSql`
with canonical SQLite parameter text and one parameter record per statement.
All three types retain exact source, dialect, and statement count without
exposing the upstream AST or rendering SQL in `Debug` output.

The parser, validator, and normalizer have no routing or storage access. Future
shard inference and statement classification consume structural syntax, never
regular-expression matches over raw or formatted SQL. See the [SQL parser
decision record](SQL_PARSER.md) for the dependency and resource contract, the
[common SQL subset contract](SQL_SUBSET.md) for the normative recursive
whitelist, and the [SQL parameter-normalization
contract](SQL_PARAMETERS.md) for numbering and source-preservation rules.

### Implemented pass-through surface

The following operations work when expressed in syntax accepted by the bundled
SQLite library and used through the matching HTTP endpoint:

| Operation | Current contract | Important boundary |
| --- | --- | --- |
| Persistent DDL, including `CREATE`, `DROP`, and `ALTER` | Execute only through the migration/broadcast endpoint | Per-shard atomic and crash-resumable; not atomic across shards |
| `INSERT`, `UPDATE`, `DELETE` | Execute on the shard selected by `shard_key` | BriskDB does not yet prove that SQL values match the supplied key |
| `SELECT` | Query the shard selected by `shard_key` | No scatter/gather path |
| SQLite expressions and functions | Passed through without translation | Semantics are SQLite semantics |
| SQLite constraints | Enforced inside one shard | No global unique-constraint coordinator |

Other SQLite syntax may happen to pass through, but it is not a stable BriskDB
contract until it appears in this table and has conformance tests. In
particular, multi-request transactions, multi-shard writes, multiple statements
outside the migration endpoint, and attached-database operations are
uncontracted public API behavior today. Persistent DDL outside the migration
endpoint is explicitly denied. DML `RETURNING` is explicitly rejected from the
query surface, and the execute surface exposes only a rows-affected count,
never a returned rowset. The experimental raw HTTP path uses those same engine
boundaries.

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
| `SELECT` | A nonempty projection with zero or one plain table; optional simple alias, `ALL`/`DISTINCT`, scalar filtering/grouping, `HAVING`, expression ordering, and standard numeric-or-placeholder limit/offset; PostgreSQL `LIMIT ALL` is parser-equivalent to no limit |
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

This implemented status means structural validation exists and is tested. It
does not mean the subset is connected to execution. Column type names are only
required to be explicit; type compatibility and translation remain issue #25.
Insert/update duplicate checks fold ASCII letter case regardless of quoting but
do not define general identifier normalization, which also remains issue #25.
Placeholder normalization is the separate implemented issue #21 layer described
below. Catalog-aware key extraction, single-shard proof, bind-time planning,
and rejection of conflicting or unroutable writes remain issues #22 through
#24. Prepare/bind/describe/execute state remains issue #26, and empty or
multi-statement execution policy remains issue #27.

The validator independently caps recursive expression AST depth at 128. This
also bounds flat operator chains that parse iteratively; exceeding the limit is
reported as `LimitExceeded`.

The future driver-capable execution path will send `CREATE TABLE` and
`CREATE INDEX` through journaled schema migrations, route accepted CRUD only after its
single-shard or supported read plan is established, and implement real
transactions through protocol-neutral session state. Writes with no provable
shard, conflicting shard keys, unsafe statement combinations, and cross-shard
transactions will be rejected before changing data.

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
statement behavior, authorize a request, or execute SQL. The HTTP and engine
paths continue to bind their existing caller-supplied SQLite SQL directly.

## PostgreSQL differences

The PostgreSQL listener will target the frontend/backend wire protocol and a
deliberately small SQL compatibility surface. PostgreSQL-specific behavior is
not implemented unless listed as implemented in this document.

| Area | PostgreSQL | BriskDB contract |
| --- | --- | --- |
| Parameters | `$1`, `$2`, ... | Implemented opt-in Rust normalization to SQLite `?N`; value binding, wire support, and bind-time routing remain planned |
| Identifier quoting | Double quotes | Passed through where SQLite semantics agree |
| Type system | Static types identified by OIDs | Planned loss-aware mapping to BriskDB types and SQLite storage classes |
| Boolean | Dedicated `boolean` type | Stored as SQLite integer `0` or `1`; protocol adapter returns a Boolean value |
| `serial`, identity, sequences | Sequence-backed generation | Unsupported until explicit sequence/identity semantics are designed |
| `bytea` | Binary value | Planned mapping to SQLite `BLOB` |
| `json` / `jsonb` | Distinct PostgreSQL types | Planned JSON validation; no promise of PostgreSQL `jsonb` storage or operators |
| Arrays, ranges, enums, domains | Native PostgreSQL types | Unsupported initially |
| Schemas and `search_path` | Multiple schemas per database | Unsupported initially; compatibility shims may expose one logical schema |
| `RETURNING` | Common DML feature | Not in the initial common subset |
| `ON CONFLICT` | PostgreSQL upsert syntax | Supported only after a tested translation contract is defined |
| Functions/operators | PostgreSQL catalog | SQLite functions/operators unless an explicit shim is documented |
| System catalogs | `pg_catalog`, `information_schema` | Only queries required by named, tested clients will be emulated |
| Error behavior | SQLSTATE and failed transaction state | Stable error-kind-to-SQLSTATE mapping defined; wire encoding and `I`/`T`/`E` transaction states planned |
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
| Parameters | `?` in prepared statements | Implemented opt-in Rust normalization to consecutive SQLite `?N`; value binding and wire support remain planned |
| Identifier quoting | Backticks by default | Planned normalization; double-quoted strict SQL remains available |
| Type system | Static signed/unsigned column types | BriskDB retains `UInt64` without narrowing; current SQLite binding rejects values above `i64::MAX` until an explicit storage mapping exists |
| Boolean | Commonly `TINYINT(1)` | Stored as SQLite integer `0` or `1`; protocol adapter returns documented metadata |
| `AUTO_INCREMENT` | Table column attribute | Unsupported until generated-key semantics are designed |
| `UNSIGNED`, display widths | MySQL column attributes | No current SQLite equivalent; unsupported initially |
| `DATETIME`, `TIMESTAMP` | Distinct MySQL temporal behavior | No implicit compatibility; canonical timestamp encoding must be defined first |
| `JSON` | Native MySQL JSON type | Planned JSON validation stored using the canonical BriskDB representation |
| `LIMIT offset,count` | MySQL syntax | Planned normalization to canonical limit/offset form |
| `ON DUPLICATE KEY UPDATE` | MySQL upsert syntax | Unsupported until a tested translation contract is defined |
| Engines, character sets, collations | Per-table/column options | Engine clauses unsupported; charset/collation behavior must be explicitly mapped |
| Session probes | `SET NAMES`, `SHOW VARIABLES`, `SELECT @@...` | Only the subset required by named, tested clients will be emulated |
| Metadata | `information_schema` and MySQL metadata commands | Minimal tested compatibility only |
| Errors | MySQL error number plus SQLSTATE | Stable error-kind mapping defined; listener and wire encoding planned |
| Stored programs, binlog, `LOAD DATA` | MySQL-specific facilities | Deferred and unsupported initially |

## SQLite semantic differences

SQLite is the execution authority. Unless BriskDB documents a compatibility
translation, its behavior follows SQLite rather than PostgreSQL or MySQL.

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
- `STRICT` tables can provide stronger enforcement, but BriskDB does not enable
  strict mode implicitly.

See SQLite's official [SQL language reference](https://www.sqlite.org/lang.html)
and [datatype documentation](https://www.sqlite.org/datatype3.html) for the
underlying execution semantics.

## Sharding semantics

### Current

- The caller supplies an opaque `shard_key` independently from the SQL text.
- Exact key bytes are hashed with version-1 BLAKE3; the little-endian 64-bit
  prefix selects one of 4,096 virtual buckets through the versioned
  compatibility algorithm.
- The final physical shard is read from the validated, generation-stamped
  bucket map retained in manifest version 7. Routing generation 1 preserves
  the earlier modulo placement for every supported shard count.
- Manifest version 7 retains the read-only logical catalog introduced in v4,
  with a journaled schema generation from 0 through 2,147,483,647 and default
  database ID 1 named `default`. Its optional table rows can describe sharded,
  global, or catalog placement and a sharded table's `Int64`, text, or binary
  key column.
- The ready layout retained from v5 binds every shard to one random 16-byte
  layout ID and its
  physical shard ID. Each connection is opened without create or symlink
  following and must match the `BRSH` application ID, expected schema
  generation, exact metadata, and existing WAL mode.
- Logical metadata is currently read-only and advisory. Fresh manifests and
  upgrades originating before v4 contain no table rows; v4-to-v5 retains every
  validated v4 logical-catalog row. Existing physical tables are not inferred
  or adopted, and catalog contents do not alter SQL planning or execution.
- Point queries and writes visit only that shard.
- No scatter/gather query path exists.
- Unique constraints and transactions are local to one SQLite shard.
- Schema migration preflights every shard, then commits an ascending prefix
  under a retained journal. Each shard is atomic; the shard set is not one
  transaction. The manifest preserves committed-source and target schema
  fingerprints, and startup resumes only an exact checksummed prefix before
  serving work.

### Planned stable contract

- Every sharded table declares one non-null shard-key column in the catalog.
- Canonical key encoding, hash version, virtual bucket count, and bucket map are
  persisted in the manifest.
- The planner extracts equality keys from SQL and bound parameters. An explicit
  session routing key remains a controlled fallback.
- A transaction is pinned to its first shard. Targeting another shard returns a
  stable cross-shard-transaction error.
- Read-only plans may scatter with bounded concurrency and deterministic merge.
- Cross-shard writes remain unsupported unless a future coordinator can prove
  its crash semantics.
- Global uniqueness is unsupported unless backed by an explicit reservation
  design; shard-local uniqueness must include the shard key when applications
  require system-wide uniqueness by construction.

## Transactions and concurrency

SQLite provides atomic transactions within one database file. BriskDB will not
describe sequential commits to several shard files as atomic. The initial SQL
session contract will therefore pin explicit transactions to one shard and
reject cross-shard access.

Manifest-format migrations are separate from application SQL migrations. They
run internally during storage open, are transactional only within
`manifest.sqlite`, and cannot be requested through any protocol or SQL
statement. The atomic version-3-to-version-4 manifest upgrade adds only
read-only advisory logical metadata and its downgrade fence. It does not infer
or adopt existing physical tables and does not change shard schemas, supported
SQLite syntax, result conversion, or routing. Version 5 then adds a resumable
physical-layout state machine. Its `Adopting` path accepts only exact legacy
zero-header WAL shards, preserves their tables and rows, and adds BriskDB
identity metadata. Version 6 adds the application-schema journal. A migration
batch and generation are atomic within each shard, and final journal state plus
catalog generation are atomic within the manifest, but no transaction spans
those files. Ascending-prefix validation and replay provide recovery rather
than cross-file atomicity. Version 7 adds the semantic manifest root, explicit
integrity states, and generation-bound shard-schema fingerprints; journal,
state, checksum, and catalog changes reseal atomically within the manifest.
See the [manifest storage-format contract](STORAGE_FORMAT.md).

Scatter reads will combine committed results from multiple SQLite files. They
will not claim an atomic cross-file snapshot until BriskDB has an implementation
and failure tests that establish such a guarantee.

## Compatibility verification

A syntax or behavior moves from planned to implemented only with tests at the
right boundary:

- unit tests for parsing, normalization, routing, type conversion, and errors;
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
