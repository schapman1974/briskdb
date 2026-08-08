# Shard-key inference

Status: implemented for roadmap issue #22

BriskDB exposes opt-in, protocol-neutral shard-key inference for one statement
that has passed the common-subset validator and placeholder normalizer:

```rust
infer_shard_keys(
    catalog: &Catalog,
    database: LogicalDatabaseId,
    normalized: &NormalizedSql,
    statement_index: usize,
    parameters: &[Value],
) -> EngineResult<ShardKeyInference>
```

`statement_index` is zero-based. `parameters` must be the complete bound-value
slice for that statement and have exactly its normalized `parameter_count()`.
Numbered PostgreSQL and SQLite markers can leave unused positions, but those
positions still count toward the required slice length. Inference resolves
placeholder occurrences through the retained source spans and normalized
indices; values are never interpolated into SQL text.

This API performs catalog-aware analysis only. It does not encode or hash a
key, select a virtual bucket or physical shard, build an execution plan,
authorize a statement, enforce write policy, or execute SQL.

## Result contract

`ShardKeyInference` owns its result and reports `table_id()`, `key_type()`,
`kind()`, and `values()`. Its category has these meanings:

| Kind | Meaning |
| --- | --- |
| `NotApplicable` | The statement has no catalog table that needs key inference, such as DDL, transaction control, or `SELECT` without `FROM` |
| `NotSharded` | The selected catalog table has `Global` or `Catalog` placement |
| `Unconstrained` | The table is sharded, but the supported analysis cannot prove a finite key set |
| `Contradiction` | A predicate proves that no non-null shard key can match |
| `Exact` | The supported proof grammar establishes one distinct key under the declared key-type semantics |
| `Multiple` | Two or more distinct inferred keys are present |

`ShardKeyValue` is one of `Int64(i64)`, `Text(String)`, or `Binary(Vec<u8>)`
and exposes matching typed accessors. Predicate results retain unique values in
deterministic expression order. A complete multi-row `INSERT` retains one
value per row in row order, including duplicates; its `Exact` or `Multiple`
category still depends on the number of distinct values.

The `Debug` representations report metadata and counts without rendering key
contents. They do not render submitted SQL or the dependency-owned AST.

## Catalog and identifier resolution

Inference uses the requested `LogicalDatabaseId` and resolves one accepted base
table within that database. A missing logical database is `InvalidArgument`; a
table absent from the selected database is `InvalidQuery`.

Catalog identifiers use the manifest's canonical lowercase ASCII contract.
Unquoted SQL identifiers are ASCII-folded before catalog comparison; quoted
identifiers compare exactly. A qualified shard-key reference must use the
table alias when an alias is present, or the table name when there is no alias.
An unrelated qualifier does not constrain the shard key.

Known `Global` and `Catalog` tables return `NotSharded` with their table ID and
no key type or values. Statements that do not reference a table return
`NotApplicable` with no table ID, key type, or values.

## Predicate inference

For `SELECT`, `UPDATE`, and `DELETE`, inference examines only the `WHERE`
predicate. Projection, grouping, `HAVING`, ordering, limits, and `UPDATE`
assignments do not establish a routing key at this layer.

The finite proof grammar is deliberately small:

- direct equality between an `Int64` or `Binary` shard-key column and a
  compatible literal or placeholder, in either operand order, produces one
  value;
- transparent parentheses do not change that result;
- `AND` intersects the key sets proven by its operands;
- `OR` unions finite key sets, but any unconstrained branch makes the complete
  `OR` unconstrained; and
- all other accepted expressions contribute no finite proof.

Consequently, `tenant_id = 1 AND tenant_id = 2` is `Contradiction`, while
`tenant_id = 1 OR tenant_id = 2` is `Multiple`. A comparison to `NULL` also
produces `Contradiction`, because cataloged shard keys are non-null. Operators
such as inequalities, `IN`, `BETWEEN`, `LIKE`, `NOT`, arithmetic, and `CASE`
remain `Unconstrained` unless another `AND` branch independently proves a
finite set.

The current catalog declares UTF-8 `Text` key values but does not declare or
enforce their comparison collation. A non-null `Text` equality predicate is
therefore `Unconstrained`, even when its literal or bound value has the correct
type: case- or accent-folding equality could match a different key identity.
`Text = NULL` remains `Contradiction` because shard-key non-nullness is part of
the declaration. Text extraction from `INSERT` is unaffected because it
reports the value being supplied rather than proving comparison semantics.

## INSERT inference

An accepted `INSERT` has an explicit column list and one or more equal-width
`VALUES` rows. Inference finds the cataloged shard-key column and evaluates its
cell in every row.

- A compatible direct literal or placeholder contributes one row value.
- Repeated placeholders and repeated values retain one result entry per row.
- Omitting the shard-key column or using a non-atomic cell expression returns
  `Unconstrained` with no values.
- A literal or bound `NULL` returns `NotNullViolation`.
- A complete row set with one distinct value is `Exact`; two or more distinct
  values are `Multiple`.

This inference classification alone does not decide whether a multi-key insert
may run. The implemented [bound planning and routing policy](SQL_PLANNING.md)
accepts it only when every occurrence selects one physical shard.

## Value compatibility

Inference uses the shard-key type declared by the catalog:

| Catalog key type | Accepted SQL literal | Accepted bound `Value` |
| --- | --- | --- |
| `Int64` | An integral decimal magnitude with nested unary `+`/`-`, within the signed 64-bit range | `Int64`, or `UInt64` when it converts losslessly to `i64` |
| `Text` | A single-quoted string for `INSERT` extraction; predicate equality remains unconstrained without collation metadata | Valid UTF-8 `Text`, with the same predicate limitation |
| `Binary` | None in the first common literal subset | `Binary` |

Compatible literal and bound values become the same `ShardKeyValue` form.
Text is not Unicode-normalized. An incompatible type is `TypeMismatch`, a
signed-integer overflow is `NumericOutOfRange`, and an `InvalidText` value for
a text key is `InvalidTextEncoding`.

## Errors and diagnostics

| Condition | `EngineErrorKind` |
| --- | --- |
| Statement index outside the normalized batch | `InvalidArgument` |
| Bound-value count differs from the selected statement's normalized parameter count | `InvalidArgument` |
| Selected logical database does not exist | `InvalidArgument` |
| Selected table is absent from that logical database | `InvalidQuery` |
| Value is incompatible with the cataloged shard-key type | `TypeMismatch` |
| Integer is outside the signed 64-bit range | `NumericOutOfRange` |
| Bound text is not valid UTF-8 | `InvalidTextEncoding` |
| An `INSERT` supplies `NULL` for the non-null shard key | `NotNullViolation` |
| Retained validated/normalized metadata is internally inconsistent | `Internal` |

Diagnostics use fixed statement and inference categories where useful. They do
not contain submitted SQL, table or column spelling, literal or bound values,
formatted AST output, source locations, or catalog key contents. Protocol
adapters must serialize the fixed public mapping for the error kind rather than
the internal diagnostic.

## Execution and storage boundaries

Shard-key inference is a read-only call over immutable SQL metadata, a catalog
snapshot, and a borrowed parameter slice. It does not mutate the catalog,
manifest, shard files, configuration, session, or caller values. It introduces
no storage-format or network-contract change.

The current HTTP execute, query, and migration paths do not invoke parsing,
common-subset validation, normalization, or inference. They retain their
existing raw SQLite SQL and caller-provided `shard_key` behavior.

The implemented synchronous
[`Engine::plan_bound_statement`](SQL_PLANNING.md) API invokes inference at
bind/execute time and turns every inferred value into an owned route. It
retains optional explicit routing separately, compares finite physical targets,
rejects unroutable sharded DML, and records a valid single-shard assignment.
The plan by itself is not execution permission. The implemented issue #25
translator is a separate opt-in operation over the same normalized SQL; it does
not change an inference result. The implemented
[prepared lifecycle](SQL_PREPARED_STATEMENTS.md) validates a transient
bind-time plan, retains the typed values and routing snapshot, and repeats
planning under the current execution guard to select a supported target. Later
work owns authoritative statement classification, scatter/gather, and
wire-protocol integration.

## Verification obligations

Tests cover all result categories; literal and bound keys in SQLite,
PostgreSQL, and MySQL syntax; numbered gaps and repeated parameters; identifier
quoting, aliases, and qualifiers; Boolean intersection, union, and
contradiction; `SELECT`, `UPDATE`, and `DELETE`; multi-row inserts and retained
duplicates; all three catalog key types and the conservative text-collation
boundary; range, type, text, null, database,
table, statement-index, and arity errors; empty and multi-statement batches;
redacted diagnostics and `Debug`; concurrent deterministic inference; and
successful inference after an independent error. Raw HTTP regressions continue
to prove that this opt-in API does not change current execution behavior.
