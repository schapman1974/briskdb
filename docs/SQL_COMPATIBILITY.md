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

The syntax parser, recursive common-subset validator, statement/batch
classifier, placeholder normalizer, finite SQL translator, catalog-aware
shard-key inference API, and synchronous engine bound-statement planner are now
available behind BriskDB-owned types.
Validation is explicit and returns `Unsupported` for a parsed form outside the
subset; parser acceptance alone is not product support. Translation,
classification, normalization, inference, bound planning, and routing policy
are also explicit.
The current experimental HTTP interface is still a raw SQLite pass-through and
can execute uncontracted SQLite syntax because it calls none of these layers.
That behavior is not a compatibility promise.

## Compatibility layers

BriskDB tracks three independent compatibility layers:

1. **Wire compatibility** lets an existing driver connect, authenticate,
   prepare and bind statements, receive typed rows, and manage session state.
2. **SQL compatibility** parses a documented common subset and translates
   selected PostgreSQL or MySQL syntax into SQLite SQL.
3. **Behavioral compatibility** emulates the metadata, type, error, transaction,
   and session behavior needed by specifically tested clients and tools.

Passing a PostgreSQL or MySQL handshake will establish only wire compatibility.
BriskDB will publish behavioral compatibility per tested driver or tool rather
than claiming to be a drop-in PostgreSQL or MySQL replacement.

## Current implementation

Only the experimental HTTP network interface can execute network requests
today. `pgwire` 0.36.3 is selected and pinned behind a BriskDB-owned adapter
seam, while the separately configured PostgreSQL TCP listener still accepts
and immediately closes streams; it implements no PostgreSQL wire message.
There is no MySQL listener. The public Rust SQL facade
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
HTTP requests still send caller-provided SQLite SQL directly to the raw engine
path; they do not pass through these opt-in SQL layers.

| Interface | Status | SQL accepted | Routing |
| --- | --- | --- | --- |
| HTTP `/v1/execute` | Experimental | One SQLite statement with positional parameters | Required caller-provided `shard_key` |
| HTTP `/v1/query` | Experimental | One raw SQLite statement prepared transiently by the row-returning path; no session cache | Required caller-provided `shard_key` |
| HTTP `/v1/admin/broadcast` | Experimental | A journaled parameterless SQLite schema batch | Preflight on every shard, then ascending resumable apply |
| HTTP `/admin` browser | Experimental, read-only | No caller SQL; server-generated physical table discovery and bounded `SELECT *` pages | Required validated physical shard number; never a routed or scatter query |
| PostgreSQL wire protocol | `pgwire` 0.36.3 selected behind a BriskDB-owned core-session seam; production TCP listener remains an accept/close scaffold | Rust parsing, validation, classification, placeholder normalization, finite compatibility translation, and prepared lifecycle implemented; private parser compatibility probe only | Core batch/write policy, bind validation, routing snapshots, current execute-time planning, and supported target execution implemented; production wire mapping planned |
| MySQL wire protocol | Planned | Rust parsing, validation, classification, placeholder normalization, finite compatibility translation, and prepared lifecycle implemented; listener adoption planned | Core batch/write policy, bind validation, routing snapshots, current execute-time planning, and supported target execution implemented; wire mapping planned |

The parser, subset validator, statement classifier, placeholder normalizer, SQL
translator, shard-key inference function, engine planner, prepared lifecycle,
and PostgreSQL adapter seam are implemented Rust APIs, not PostgreSQL or MySQL
network interfaces. The private issue-29 probe composes the selected library
with those APIs, but the PostgreSQL socket scaffold does not connect them to the
network. They do not change any current HTTP row in this table.

Every HTTP database operation now calls the same protocol-neutral async engine
intended for future PostgreSQL and MySQL adapters. Execute and query requests
create a fresh `Ready` session, put the request's `shard_key` in its routing
context, and submit an owned statement. The engine, rather than the HTTP
adapter, selects the shard and acquires a pooled SQLite connection. Admin
inspection requests also create a fresh `Ready` session, but use a bounded
read-only engine operation with an already validated physical shard. The
session is discarded when that HTTP request finishes, so a transaction cannot
span requests.

### Admin browser inspection

The `/admin` application is an early operational view rather than another SQL
compatibility mode. The browser never submits SQL. Its overview selects one
physical shard from the configured range and discovers ordinary `main`-schema
tables through SQLite's typed `table_list` metadata. ASCII-case-insensitive `sqlite_`,
the exact name `briskdb`, and `briskdb_` prefixes are excluded, as are non-table
objects. This is a physical schema view, not a promise that advisory logical
catalog metadata describes the table or that another shard has an equal table.

Row browsing generates one safely quoted `SELECT *` for a table returned by
discovery. Page limits are 1 through 200 and offsets are 0 through 1,000,000;
the interface reads at most one extra row to decide whether another page is
available. The user interface offers 25, 50, 100, and 200 and starts at 50.
Shard, table, limit, offset, and checked arithmetic are validated before
execution. The engine still requires SQLite to classify the statement as
read-only and applies its schema gate, pool/worker admission, cancellation,
deadline, and effective result-byte and row budgets.

Each offset page is a new committed read. No order is promised for a general
SQLite table scan, and concurrent changes may move rows between pages. The
browser does not merge physical shards, preserve a multi-page snapshot, accept
arbitrary filters or SQL, or implement the planned scatter/gather query path.
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

The experimental `/v1/query` response exposes the ordered result directly. The
admin row-page endpoint reuses the same column and cell conversion and wraps it
with physical-shard and pagination metadata. For example, the existing query
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
number/SQLSTATE mappings. The private selected-adapter probe consumes the
PostgreSQL mapping; the MySQL mapping remains a contract for its future
adapter. The PostgreSQL TCP placeholder emits no error frame, and no MySQL
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

This implemented status means structural validation exists and is tested. It
does not mean the subset is connected to execution. Column type names need only
be explicit at validation; the separate compatibility translator applies its
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
both layers, while the raw HTTP paths continue to bind caller-supplied SQLite
SQL directly.

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

For `SELECT`, `UPDATE`, and `DELETE`, direct equality against an `Int64` or
`Binary` shard-key column produces a finite key set; Boolean `AND` intersects
and `OR` unions those proofs. Non-null `Text` equality remains unconstrained
because the current catalog does not declare or enforce comparison collation.
Other predicates do not establish a key. For `INSERT`, every `VALUES` row's
explicit shard-key cell must be a compatible direct literal or placeholder to
produce a complete result, and one value per row is retained, including text
values. The exact identifier, value, result, and error rules are in [shard-key
inference](SQL_SHARD_KEYS.md).

Inference does not encode, hash, route, plan, authorize, enforce, or execute.
The implemented planner described below uses the result at bind/execute time to
construct routes, validate physical-target compatibility, and reject
unroutable sharded DML. The raw HTTP execution paths do not invoke inference or
that policy.

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
on deterministic shard 0; catalog access and sharded reads requiring scatter
remain unavailable. Persistent schema prepare is denied before a handle is
published, and session behavior cannot execute through a portal. Results are
the same protocol-neutral routed rowset or affected-row count for SQLite,
PostgreSQL, and MySQL source.

Per-session statement, portal, retained-bound-value, and per-bind planning
limits are finite and have no implicit eviction. Planning preflight charges the
captured route once and each marker occurrence twice. Explicit close releases
entries; closing a statement also invalidates all dependent portals. The API,
defaults, logical byte accounting, request controls, errors, and storage
boundary are normative in
[prepared statements and bound portals](SQL_PREPARED_STATEMENTS.md).

## PostgreSQL differences

The bound PostgreSQL TCP scaffold will host the selected `pgwire` 0.36.3
adapter targeting the frontend/backend protocol and a deliberately small SQL
compatibility surface. The BriskDB-owned adapter/core-session boundary and a
private parser/socket fit probe exist, but the listener currently accepts and
closes without a handshake. PostgreSQL-specific behavior is not implemented
unless listed as implemented in this document. Configuration and lifecycle
semantics are normative in the [PostgreSQL listener
contract](POSTGRES_LISTENER.md); dependency and adapter constraints are
normative in the [adapter decision record](POSTGRES_ADAPTER.md).

| Area | PostgreSQL | BriskDB contract |
| --- | --- | --- |
| Parameters | `$1`, `$2`, ... | Rust normalization and session-scoped prepare/bind/describe/execute are implemented; PostgreSQL message/name/type mapping remains planned |
| Identifier quoting | Double quotes | Retained by opt-in compatibility translation; PostgreSQL case folding and catalog equivalence are not claimed |
| Type system | Static types identified by OIDs | Opt-in Rust translation maps a finite declaration set to `BIGINT`, `BOOLEAN`, `REAL`, `TEXT`, or `BLOB`; OID and value/result adaptation remain planned |
| Boolean | Dedicated `boolean` type | Opt-in translation maps the declaration to `BOOLEAN` and literals to `1`/`0`; Boolean wire/result metadata remains planned |
| `serial`, identity, sequences | Sequence-backed generation | Unsupported until explicit sequence/identity semantics are designed |
| `bytea` | Binary value | Opt-in declaration translation maps it to SQLite `BLOB`; binary value and wire adaptation remain planned |
| `json` / `jsonb` | Distinct PostgreSQL types | Planned JSON validation; no promise of PostgreSQL `jsonb` storage or operators |
| Arrays, ranges, enums, domains | Native PostgreSQL types | Unsupported initially |
| Schemas and `search_path` | Multiple schemas per database | Unsupported initially; compatibility shims may expose one logical schema |
| `RETURNING` | Common DML feature | Not in the initial common subset |
| `ON CONFLICT` | PostgreSQL upsert syntax | Outside the initial common subset and unsupported |
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
| Parameters | `?` in prepared statements | Rust normalization and session-scoped prepare/bind/describe/execute are implemented; MySQL command/type mapping remains planned |
| Identifier quoting | Backticks by default | Opt-in compatibility translation emits safely escaped double-quoted SQLite identifiers; case and collation equivalence are not claimed |
| Type system | Static signed/unsigned column types | Opt-in Rust translation maps a finite signed integer, Boolean, 64-bit float, text, and binary declaration set; unsigned declarations are rejected, and translation performs no value or result adaptation. Independently, BriskDB retains `UInt64` values without narrowing, while current SQLite binding rejects values above `i64::MAX` until a storage mapping exists |
| Boolean | Commonly `TINYINT(1)` | Opt-in translation maps exactly `TINYINT(1)` to `BOOLEAN` and Boolean literals to `1`/`0`; wire/result metadata remains planned |
| `AUTO_INCREMENT` | Table column attribute | Unsupported until generated-key semantics are designed |
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
  or adopted. The opt-in Rust inference and planning/policy APIs can consult
  catalog contents, but they do not alter current HTTP routing or execution.
- Opt-in inference can extract typed cataloged keys from supported equality
  predicates and every row of a supported `INSERT`.
- Opt-in bound planning converts every inferred occurrence to an owned route,
  retains an independent explicit route, and records catalog/routing
  provenance. It compares finite routes by physical shard, rejects unroutable
  cataloged sharded writes, and records an accepted single-shard assignment.
- The Rust prepared lifecycle snapshots bound values and session routing in an
  immutable portal, transiently validates at bind, plans on every execution,
  executes accepted sharded targets, and uses shard 0 for supported
  replicated-schema reads. It does not change current HTTP behavior or
  implement scatter reads.
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
