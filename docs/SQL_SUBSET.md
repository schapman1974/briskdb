# Common SQL subset

Status: implemented for roadmap issue #20

BriskDB exposes a protocol-neutral structural validator for the first SQL
subset shared by SQLite, PostgreSQL, MySQL, and future frontends. Callers first
parse with an explicit [`SqlDialect`](../src/sql/parser.rs), then consume the
result with:

```rust
validate_common_subset(parsed: ParsedSql) -> EngineResult<CommonSql>
```

`CommonSql` is an owned, opaque marker that retains the selected dialect, the
byte-exact source, and the ordered statement count. It does not expose the
dependency-owned AST. Its `Debug` representation reports only the dialect,
source byte count, and statement count; it does not render SQL or AST contents.

Validation is deliberately opt-in. The current HTTP execute, query, and schema
migration paths remain raw SQLite pass-through surfaces and do not call either
the parser or this validator. Issue #20 therefore changes no HTTP request or
response shape, SQL accepted by HTTP, routing result, transaction behavior,
configuration, manifest, shard file, or stored data.

## Boundary

The validator answers one question: does every parsed statement use only the
structural forms in this document? It recursively checks nested clauses and
expressions rather than accepting a statement from its top-level keyword alone.
The complete batch succeeds as one validation operation or returns
`Unsupported` for the first rejected statement.

A successful `CommonSql` does not mean that the SQL:

- names existing databases, tables, columns, constraints, or types;
- has normalized or correctly numbered placeholders until the separate
  [`normalize_placeholders`](SQL_PARAMETERS.md) step succeeds;
- has an inferred key until the separate catalog-aware
  [`infer_shard_keys`](SQL_SHARD_KEYS.md) step succeeds;
- can be routed to one shard even after inference reports values;
- is safe to execute as an empty, single-, or multi-statement request;
- has a prepared-statement plan or protocol result description;
- has been translated to SQLite or preserves source-dialect semantics;
- is authorized for a particular endpoint or session; or
- has been executed.

Those responsibilities remain with the implemented issue #21 normalization and
issue #22 inference layers, issues #23 through #27, and the later wire
frontends. Validation never consults parameters, a session, the logical
catalog, storage, routing state, the filesystem, or SQLite. It never formats or
searches SQL text to make a structural decision.

Empty and comment-only parsed batches validate successfully. Every statement in
a mixed batch is checked independently and source order is retained, but that
is not permission to execute the batch. Issue #27 owns empty-request policy,
statement behavior classification, and safe statement combinations.

## Statement forms

| Family | Accepted structural form | Rejected forms include |
| --- | --- | --- |
| `CREATE TABLE` | One unqualified, persistent table; optional `IF NOT EXISTS`; one or more columns, each with an explicit parsed type; the column and table constraints described below | Qualified names, temporary or other table modifiers, table-as-query, `LIKE`/clone, storage/lifecycle/vendor options, partitioning/inheritance, SQLite `STRICT` or `WITHOUT ROWID` |
| `CREATE INDEX` | A named, unqualified index on one unqualified table; optional `UNIQUE` and `IF NOT EXISTS`; one or more plain columns with optional `ASC` or `DESC` | Unnamed, qualified, expression, or partial indexes; `USING`, operator classes, `INCLUDE`, null-order, concurrent/async, storage, or vendor options |
| `SELECT` | A nonempty projection with no `FROM` or one unqualified base table; optional simple table alias, `ALL`/`DISTINCT`, `WHERE`, `GROUP BY`, `HAVING`, expression `ORDER BY`, and standard `LIMIT`/`OFFSET` | CTEs, set operations, nested queries, multiple tables, joins, derived/table functions, `DISTINCT ON`, `SELECT INTO`, windows, locks, `FETCH`, `TOP`, query hints/settings/formats, MySQL comma-form limit |
| `INSERT` | `INSERT INTO` one unqualified named table with a nonempty explicit column list whose names are unique under ASCII case folding, plus one or more equal-width `VALUES` rows | Omitted, exact-duplicate, or ASCII-case-only duplicate columns; row-width mismatch; `DEFAULT VALUES`; `INSERT ... SELECT`; aliases; conflict/upsert/ignore/replace forms; partitioning; `RETURNING`; output; multi-table, format, or settings clauses |
| `UPDATE` | One unqualified base table with an optional simple alias, one or more single-column assignments whose targets are unique under ASCII case folding, and an optional `WHERE` | Joins, qualified or tuple assignment targets, exact-duplicate or ASCII-case-only duplicate targets, `FROM`, conflict modes, `RETURNING`, output, `ORDER BY`, or `LIMIT` |
| `DELETE` | `DELETE FROM` exactly one unqualified base table with an optional simple alias and optional `WHERE` | Missing `FROM`, multiple tables, joins, `USING`, `RETURNING`, output, `ORDER BY`, or `LIMIT` |
| `BEGIN` | `BEGIN`, `BEGIN TRANSACTION`, or `BEGIN WORK` | `START TRANSACTION`, `BEGIN TRAN`, transaction modes, SQLite deferred/immediate/exclusive modifiers, or procedural blocks |
| `COMMIT` | The plain commit AST produced from `COMMIT`, its parser-accepted `TRANSACTION`, `WORK`, or `TRAN` suffix aliases, and explicit `AND NO CHAIN` | `AND CHAIN`, procedural `END`, or other modifiers |
| `ROLLBACK` | The plain full-rollback AST produced from `ROLLBACK`, its parser-accepted `TRANSACTION`, `WORK`, or `TRAN` suffix aliases, `ABORT`, and explicit `AND NO CHAIN` | Rollback to a savepoint or `AND CHAIN` |
| Other statements | None | `ALTER`, `DROP`, views, savepoints, session statements, administrative statements, and every other top-level statement family |

The pinned parser collapses the listed plain commit and rollback aliases, plus
an explicit `AND NO CHAIN`, to the same semantic AST forms. The validator
deliberately accepts that AST family and does not search retained source text to
distinguish equivalent spellings; the explicit source dialect must still parse
a spelling first. `BEGIN`, `COMMIT`, and `ROLLBACK` remain syntax families only
at this boundary. Canonical syntax translation remains issue #25. Real
multi-call transaction state, failed-transaction behavior, connection and shard
pinning, and protocol status reporting remain issues #34 and #47.

An absent `WHERE` on `UPDATE` or `DELETE` is structurally valid. Likewise,
validation does not inspect whether an assignment changes a shard-key column.
The separate inference layer classifies the supported predicate proof; issue
#24 owns rejection of conflicting or unroutable writes and assignment policy.

### Names, aliases, and types

Table, index, insert-column, and update-target object names must contain exactly
one parsed name part. A scalar column reference may contain one part or exactly
two parts such as `alias.column`. A table alias is accepted when it has no
column-alias list or vendor-specific `AT` clause. A projection may use one
expression alias, `*`, or a one-part qualified wildcard such as `w.*`, without
wildcard modifiers.

Insert column lists and update assignment targets reject exact duplicates and
names that differ only by ASCII letter case, regardless of quoting. This is a
conservative common key for duplicate detection, not full PostgreSQL, MySQL,
SQLite, Unicode, quoted-identifier, or catalog normalization. General
identifier normalization and translation remain issue #25.

The validator requires every `CREATE TABLE` column to have an explicit parsed
type, but it deliberately does not restrict the type name. Acceptance therefore
does not promise a cross-protocol representation or SQLite translation for that
type. Canonical type aliases, dialect differences, and strict SQLite mode remain
issue #25.

### Table constraints and defaults

The following column options are accepted:

- unnamed `NULL` and `NOT NULL`;
- an unnamed literal `DEFAULT`;
- optionally named `PRIMARY KEY`, `UNIQUE`, and `CHECK` constraints.

Table-level `PRIMARY KEY`, `UNIQUE`, and `CHECK` constraints are accepted.
Primary-key and unique column lists must use plain columns with optional `ASC`
or `DESC`; index types, include columns, operator classes, null-distinctness,
constraint characteristics, and other options are rejected. Foreign keys,
generated or identity columns, collations, character sets, comments,
autoincrement forms, conflict clauses, and other column/table constraints are
outside the subset.

A default may be `NULL`, a Boolean, a numeric literal without an `L` suffix, a
single-quoted string, a parenthesized accepted literal, or unary `+`/`-` applied to a numeric
literal. Functions, identifiers, placeholders, and other default expressions
are rejected. A `CHECK` uses the scalar expression subset below but cannot use
placeholders or aggregate functions.

## Expression forms

The validator accepts these recursively composed expression forms where the
owning statement context permits them:

- one-part identifiers and two-part column references;
- `NULL`, Boolean, numeric literals without an `L` suffix, and single-quoted
  string literals;
- source-dialect placeholder nodes outside schema expressions;
- parentheses;
- unary `+`, `-`, and `NOT`;
- arithmetic `+`, `-`, `*`, `/`, and `%`;
- comparisons `=`, `<>`/`!=`, `<`, `<=`, `>`, and `>=`;
- Boolean `AND` and `OR`;
- `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS NOT TRUE`, `IS FALSE`, and
  `IS NOT FALSE`;
- `BETWEEN`/`NOT BETWEEN`;
- `IN`/`NOT IN` with a nonempty expression list;
- `LIKE`/`NOT LIKE` without `ANY` or an `ESCAPE` clause; and
- simple or searched `CASE` expressions.

The only accepted functions are the unqualified, unquoted aggregate names
`COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` in projection, `HAVING`, and `ORDER BY`
contexts. They take exactly one unnamed expression argument; duplicate treatment
such as `DISTINCT` is accepted. `COUNT(*)` is also accepted without duplicate
treatment. Aggregate filters, windows, null treatment, within-group clauses,
nested aggregates, qualified wildcards, named arguments, and every scalar or
unknown function are rejected.

`WHERE`, assignment, and `GROUP BY` expressions cannot contain aggregates.
Insert row expressions use the same scalar grammar but cannot reference a
column. A retained `LIMIT` or `OFFSET` value accepts only an unsigned digit-only
numeric literal or a placeholder. The pinned parser represents PostgreSQL
`LIMIT ALL` as the same absent-limit AST as omitting the clause, so structural
validation accepts that AST-equivalent spelling and does not inspect source text
to distinguish it. Placeholder spelling, numbering, count, and binding are not
validated or changed here. The separate [SQL parameter-normalization
contract](SQL_PARAMETERS.md) defines the implemented opt-in numbering step,
which consumes `CommonSql` without accepting bound values.
The [shard-key inference contract](SQL_SHARD_KEYS.md) then defines the exact
catalog-aware predicate and `INSERT` value forms that produce typed key values;
general expression support here is intentionally broader than that finite
proof grammar.

Common-subset expression validation has its own maximum recursive AST depth of
128. This catches long flat operator chains, which the parser can build
iteratively as deeply nested AST nodes without reaching its parser recursion
limit. Exceeding this validator limit returns `LimitExceeded` before deeper
validation; it is not classified as unsupported syntax.

Subqueries, casts, collations, empty `IN` lists, dialect-specific literal forms
and operators, JSON/array/row expressions, scalar functions, and all other AST
expression forms are outside the subset.

## Errors and diagnostics

Parsing happens before subset validation:

| Condition | `EngineErrorKind` |
| --- | --- |
| Tokenization or syntax failure | `InvalidQuery` |
| Parser byte, statement-count, or recursion limit, or common-subset expression depth limit | `LimitExceeded` |
| Parsed statement or nested form outside this contract | `Unsupported` |

An unsupported diagnostic identifies the one-based statement position and a
fixed feature category. It does not contain submitted SQL, literals, formatted
AST output, parser diagnostics, or source locations. Protocol adapters must use
the fixed public mapping for `Unsupported` rather than serializing the internal
diagnostic.

## Verification obligations

Tests cover a common form of every statement family in all three source
dialects, every recursive clause boundary, exact source ownership, redacted
debug and unsupported diagnostics, empty and mixed batches, dialect-native
placeholders, multi-row insert width, duplicate insert/update targets,
concurrent validation, ordinary nested expressions, and the exact independent
validator-depth boundary for flat operator chains. A raw HTTP regression must
also continue to execute SQLite syntax outside this subset, proving that this
opt-in marker has not become an implicit HTTP execution gate.
