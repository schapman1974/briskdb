# SQL translation

Status: implemented for roadmap issue #25

BriskDB exposes an opt-in, protocol-neutral translation step after common SQL
validation and placeholder normalization:

```rust
translate_sql(
    normalized: NormalizedSql,
    mode: SqlTranslationMode,
) -> EngineResult<TranslatedSql>
```

The call consumes the owned `NormalizedSql` and returns an owned
`TranslatedSql`. The result retains the exact original source, source dialect,
statement parameter layouts, and complete normalized representation. It also
contains a separate `sqlite_sql()` string for a later SQLite prepare step.

Translation is pure SQL analysis. It accepts no catalog, session, routing key,
bound values, storage handle, or protocol object. It does not prepare,
authorize, classify a request batch, route, or execute a statement. The
separate [statement classifier](SQL_STATEMENT_CLASSIFICATION.md) borrows
`CommonSql` before normalization and owns logical behavior and batch policy.

## Explicit modes

BriskDB deliberately has no default translation mode. The roadmap leaves the
eventual server default undecided, so callers select a mode explicitly from
trusted connection or API context.

### `StrictSqlite`

Strict mode requires input parsed as `SqlDialect::Sqlite`. Input parsed as
PostgreSQL or MySQL returns `InvalidArgument` rather than silently changing its
dialect.

The output is byte-for-byte equal to
`NormalizedSql::sqlite_parameter_sql()`. Comments, whitespace, CRLF, keyword
case, quoted identifiers, literals, UTF-8, separators, and arbitrary explicit
SQLite declared type names remain unchanged. Only the earlier positional
placeholder normalization may differ from `source()`.

Strict translation does not bypass parsing, common-subset validation, or
placeholder normalization. It is also unrelated to SQLite's
`CREATE TABLE ... STRICT` option, which remains outside the structural common
subset. Populated-catalog HTTP execute/query uses this exact strict mode after
normalization; empty-catalog HTTP alone retains a separate raw SQLite
pass-through.

### `Compatibility`

Compatibility mode clones the already validated opaque AST, applies only the
finite mappings below, replaces placeholders with their existing SQLite `?N`
identities, and renders canonical SQLite SQL. `source()` remains the exact
caller input and is authoritative for source identity.

Canonical rendering may change comments, whitespace, keyword case, redundant
parentheses, identifier presentation, and statement separators. Ordered
statements are joined with `; `; an empty or comment-only input produces empty
SQLite SQL. Rendered compatibility SQL must never be used as application-schema
migration identity.

## Type mapping

Compatibility translation admits only the mappings in this section. The
whitelist is keyed by both source dialect and parsed type because the parser can
recognize spellings that the named server dialect does not promise.

| Logical family | SQLite source spellings | PostgreSQL source spellings | MySQL source spellings | Canonical declaration |
| --- | --- | --- | --- | --- |
| Signed integer | Bare `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `INTEGER`, `BIGINT` | Bare `INT2`, `SMALLINT`, `INT`, `INTEGER`, `INT4`, `BIGINT`, `INT8` | Bare `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `INTEGER`, `BIGINT` | `BIGINT` |
| Boolean | `BOOL`, `BOOLEAN` | `BOOL`, `BOOLEAN` | `BOOL`, `BOOLEAN`, exactly `TINYINT(1)` | `BOOLEAN` |
| 64-bit floating point | `REAL` | `FLOAT8`, `DOUBLE PRECISION` | Bare `DOUBLE`, `DOUBLE PRECISION` | `REAL` |
| Variable text | `TEXT`, `VARCHAR[(n)]`, `CHAR[ACTER] VARYING[(n)]` | Same variable-text family | Same variable-text family | `TEXT` |
| Variable binary | `BLOB` | `BYTEA` | `BLOB`, `VARBINARY[(n)]` | `BLOB` |

`n` is an unsigned integer with no length unit. Accepted varying text and
binary lengths are declaration metadata only: BriskDB removes them and SQLite
does not enforce those source-server length limits.

Signed integers canonicalize to `BIGINT`, not exact SQLite `INTEGER`. SQLite
gives `INTEGER PRIMARY KEY` special rowid-alias and generated-rowid behavior.
Compatibility mode does not invent that behavior for PostgreSQL or MySQL
integer aliases. Strict SQLite mode retains an explicitly requested
`INTEGER PRIMARY KEY` declaration exactly.

`TINYINT(1)` is accepted only as the documented MySQL Boolean convention. The
canonical `BOOLEAN` declaration and translated `0`/`1` literals do not add a
Boolean check constraint; ordinary SQLite affinity rules remain authoritative.

The initial compatibility set rejects, among other forms:

- signed integer display widths other than MySQL `TINYINT(1)` and every
  unsigned or zero-fill integer;
- `DECIMAL`, `NUMERIC`, PostgreSQL `REAL`/`FLOAT4`, MySQL `FLOAT`/`REAL`, and
  parameterized floating-point types;
- fixed `CHAR` and `BINARY`, whose padding semantics are not reproduced;
- temporal, interval, JSON/JSONB, UUID, bit-string, array, range, enum, domain,
  serial, identity, and custom types; and
- `VARCHAR(MAX)`, `VARBINARY(MAX)`, and explicit character-length units.

Those exclusions avoid choosing representations whose cross-protocol value or
comparison behavior has not been specified. Strict SQLite mode continues to
pass arbitrary validated SQLite declared type names through unchanged.

## Syntax mapping

Compatibility mode implements only these syntax differences:

| Accepted source form | Canonical SQLite form |
| --- | --- |
| MySQL or SQLite backtick-quoted identifier | Double-quoted SQLite identifier, with decoded embedded characters re-escaped |
| Boolean literal `TRUE` / `FALSE` | Integer literal `1` / `0` |
| `BEGIN`, `BEGIN TRANSACTION`, or `BEGIN WORK` | `BEGIN` |
| Accepted plain `COMMIT` aliases, including `WORK`, `TRAN`, and `AND NO CHAIN` | `COMMIT` |
| Accepted full rollback/`ABORT` aliases, including `AND NO CHAIN` | `ROLLBACK` |
| MySQL or SQLite `LIMIT offset, count` | `LIMIT count OFFSET offset` |
| `OFFSET n ROW` / `OFFSET n ROWS` when represented by the accepted AST | `OFFSET n` |

PostgreSQL `LIMIT ALL` is represented by the pinned parser as the same absent
limit as an omitted clause and therefore renders with no limit. Standard
`LIMIT count OFFSET offset` remains canonical.

MySQL comma-limit operands are reordered structurally, but placeholder identity
is not. For example:

```text
source:  SELECT `id` FROM `items` WHERE `tenant_id` = ? LIMIT ?, ?
indices:                                               1       2  3
SQLite:  SELECT "id" FROM "items" WHERE "tenant_id" = ?1 LIMIT ?3 OFFSET ?2
```

The translator looks up each placeholder by its retained statement-local source
span, so PostgreSQL repeats/gaps and reordered MySQL operands keep their
original bound-value identity. Numbering still restarts for each statement.

Unquoted identifier spelling is retained by the canonical AST. This layer does
not claim PostgreSQL/MySQL case folding, Unicode normalization, collation, or
catalog equivalence. It adds no casts, scalar-function shims, upsert,
`RETURNING`, generated columns, joins, engine clauses, character sets, or
session statements.

## Result contract

`TranslatedSql` exposes:

- `dialect()` and `mode()`;
- exact original `source()`;
- separate `sqlite_sql()`;
- `normalized_sql()` for the normalized AST and bind metadata still consumed
  by shard inference and `Engine::plan_bound_statement`;
- `statement_parameters()` in original source occurrence order; and
- `statement_count()` and `is_empty()`.

The type is owned, cloneable, `Send`, and `Sync`. Its `Debug` output contains
only dialect, mode, byte counts, and statement count. It never renders source,
translated SQL, identifiers, literals, or placeholder spelling.

## Errors and precedence

| Condition | `EngineErrorKind` |
| --- | --- |
| `StrictSqlite` with PostgreSQL or MySQL source | `InvalidArgument` |
| A `CREATE TABLE` column uses a type outside the dialect-specific compatibility whitelist | `Unsupported` |
| Canonical rendering contains a NUL decoded from source-dialect literal syntax | `InvalidQuery` |
| Retained AST, statement, parameter, or placeholder-span metadata is inconsistent | `Internal` |

Parsing, subset, and placeholder-normalization errors occur before this API and
retain their existing kinds. Translation checks the mode first, then statements
and `CREATE TABLE` columns in source order. The first unsupported type wins.

Diagnostics use fixed categories plus one-based statement and column ordinals
where useful. They contain no SQL, identifier or type spelling, comment,
literal, marker, parameter value, formatted AST, or source location. A failure
retains no caller buffer and changes no shared state; a later independent call
can succeed.

## Deliberate boundaries

Issue #25 added no CLI flag, environment variable, listener default, HTTP field,
wire message, catalog rule, session state, prepared-statement cache, routing
decision, manifest migration, shard-file change, or storage-format version. The
later authoritative-catalog integration composes strict translation into
populated-catalog HTTP execute/query. Empty-catalog HTTP and the migration
endpoint retain their legacy/exact-text engine paths.

The implemented protocol-neutral [prepared lifecycle](SQL_PREPARED_STATEMENTS.md)
owns prepare/bind/describe/execute state and adopts `sqlite_sql()` only after
requiring and classifying exactly one top-level statement. It transiently
compiles metadata, caches BriskDB-owned SQL and behavior rather than a SQLite
handle, and creates a fresh current plan from each portal's bind snapshot at
execution. The general planner applies the classifier's batch gate before
planning; translation remains an independently callable syntax branch. Schema
execution must still use the journaled migration path.

## Verification obligations

Tests cover every accepted type alias and excluded family; dialect-specific
whitelisting; exact strict-mode preservation; canonical identifier, Boolean,
transaction, and limit syntax; repeated, gapped, reordered, and per-statement
placeholder identities; empty and 256-statement batches; NUL and synthetic
metadata failures; diagnostic and `Debug` redaction; deterministic concurrent
translation; recovery after independent errors; equivalent SQLite,
PostgreSQL, and MySQL declarations, plans, and executed SQLite results; plus
empty-catalog legacy execution and populated-catalog strict-mode integration.
