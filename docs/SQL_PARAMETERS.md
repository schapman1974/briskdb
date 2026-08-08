# SQL parameter normalization

Status: implemented for roadmap issue #21

BriskDB exposes an opt-in, protocol-neutral placeholder normalizer for SQL that
has already passed the common-subset validator:

```rust
normalize_placeholders(common: CommonSql) -> EngineResult<NormalizedSql>
```

The normalizer consumes the owned [`CommonSql`](SQL_SUBSET.md) marker and
returns an owned `NormalizedSql`. Callers can inspect its source dialect,
byte-exact original source, rewritten SQLite parameter SQL, statement count,
empty-batch state, and one `StatementParameters` record per statement. The
public SQLite parameter-index ceiling is `MAX_SQL_PARAMETERS`, currently
32,766.

This layer changes placeholder tokens only. No parameter values enter the
normalizer, and values are never rendered or interpolated into SQL text. The
current HTTP execute, query, and migration endpoints and the asynchronous
engine do not invoke this opt-in layer; they retain their existing raw SQLite
behavior. Issue #27 owns whether an empty batch, one statement, or a particular
multi-statement combination may execute.

## Public result

`NormalizedSql` retains both representations:

- `source()` returns the caller's exact original SQL;
- `sqlite_parameter_sql()` returns the same text with accepted placeholder
  tokens replaced by canonical SQLite `?N` tokens;
- `dialect()`, `statement_count()`, and `is_empty()` retain the validated input
  metadata; and
- `statement_parameters()` returns `&[StatementParameters]` in statement order.

Each `StatementParameters` reports:

- `parameter_count()`: the largest parameter index assigned in that statement,
  or zero when the statement has no placeholders;
- `occurrence_count()`: the number of placeholder occurrences in that
  statement; and
- `parameter_indices()`: the assigned one-based index for each occurrence in
  source order.

`parameter_count()` is intentionally not always the same as
`occurrence_count()`. Repeated PostgreSQL parameters share one index, and an
explicit PostgreSQL or SQLite index can leave a gap. Numbering restarts for
each top-level statement. The per-statement records describe binding; they do
not authorize batch execution.

For example, PostgreSQL `SELECT $2, $1, $2` becomes
`SELECT ?2, ?1, ?2`. Its parameter count is 2, its occurrence count is 3, and
its parameter indices are `[2, 1, 2]`.

## Dialect rules

The source dialect selected during parsing determines the accepted marker
grammar. BriskDB does not autodetect or retry another dialect.

### PostgreSQL

PostgreSQL `$N` markers retain their positive numeric identity and become
SQLite `?N`. Repeated markers continue to refer to the same bound value;
out-of-order markers and gaps are retained. Decimal spelling is canonicalized,
so leading zeroes do not survive in the SQLite parameter SQL.

```text
source:  SELECT $02, $1, $02
SQLite:  SELECT ?2, ?1, ?2
indices: [2, 1, 2]
```

Index zero is invalid. An index above `MAX_SQL_PARAMETERS` exceeds the
normalizer limit.

### MySQL

Each MySQL prepared-statement `?` marker receives the next index from left to
right, starting at one for each statement.

```text
source:  INSERT INTO widgets (tenant_id, name) VALUES (?, ?)
SQLite:  INSERT INTO widgets (tenant_id, name) VALUES (?1, ?2)
indices: [1, 2]
```

MySQL source SQL does not acquire PostgreSQL-style numbered parameters at this
layer. The selected parser and subset boundary determine whether another token
is a placeholder, an identifier, or invalid syntax before normalization.

### SQLite

SQLite positional `?` and `?NNN` markers follow SQLite's native left-to-right
numbering. An explicit `?NNN` uses `NNN`. A bare `?` uses one more than the
largest index assigned earlier in the same statement. A later explicit index
may reuse or fall below that maximum; it does not renumber earlier occurrences.

```text
source:  SELECT ?2, ?, ?1, ?
SQLite:  SELECT ?2, ?3, ?1, ?4
indices: [2, 3, 1, 4]
```

SQLite named parameters such as `:name`, `@name`, and `$name` are deliberately
outside this contract. They are rejected as `Unsupported` rather than assigned
an order whose aliasing behavior could differ across frontends. Positional
index zero is invalid, and an assigned index above `MAX_SQL_PARAMETERS`
exceeds the normalizer limit.

## Source preservation

The normalizer uses placeholder nodes and source spans retained from the parsed
and validated AST. It does not find markers with regular expressions, render
the AST, or rewrite arbitrary token-shaped text. Every byte outside an accepted
placeholder span remains identical, including whitespace, comments, quoted
identifiers, string literals, semicolons, and UTF-8 text. Marker-like text in a
comment or literal is therefore unchanged.

The rewritten string is a separate representation. `source()` remains the
original input, so normalization cannot change migration identity or make a
lossy AST formatter authoritative.

## Limits and errors

Limits apply independently to each statement:

| Condition | `EngineErrorKind` |
| --- | --- |
| Positional marker index zero | `InvalidQuery` |
| Marker spelling incompatible with the selected PostgreSQL or MySQL dialect | `InvalidQuery` |
| Assigned index greater than `MAX_SQL_PARAMETERS` (32,766) | `LimitExceeded` |
| Named SQLite marker | `Unsupported` |
| Retained placeholder span does not match the retained source | `Internal` |

Diagnostics identify a fixed normalization category and statement or
occurrence position where useful. They do not contain submitted SQL, literals,
marker spelling, parameter values, formatted AST output, or source locations.
Protocol adapters must serialize the fixed public mapping for the error kind,
not the internal diagnostic.

Parser input, statement-count, and recursion limits still apply before this
layer, as does the common-subset validator's expression-depth limit.

## Boundaries after normalization

A successful `NormalizedSql` does not establish that:

- the caller supplied exactly `parameter_count()` values with compatible
  types;
- a bound value identifies one shard;
- catalog names and types exist or are compatible;
- non-placeholder PostgreSQL or MySQL syntax has been translated to SQLite;
- a statement or batch is permitted by an endpoint or session;
- a prepared statement has been described, cached, bound, or executed; or
- execution would preserve all source-dialect semantics.

Issues #22 through #27 own shard-key inference, bind-time planning, conflicting
or unroutable key rejection, syntax/type translation, prepared-statement state,
and request-level statement classification respectively.

## Verification obligations

Tests cover each dialect's numbering rules, repeats, gaps, mixed explicit and
anonymous SQLite markers, per-statement reset, empty and marker-free input,
exact preservation of comments/literals/whitespace/UTF-8, zero,
exact-maximum, and over-maximum index cases, named SQLite rejection, diagnostic
and `Debug` redaction, cloning, concurrent normalization, and recovery after an
independent error. A raw HTTP regression must continue to bind SQLite named
parameters without calling this layer, proving that issue #21 did not change
current execution behavior.
