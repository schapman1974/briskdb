# SQL statement and batch classification

Status: implemented for roadmap issue #27

BriskDB classifies the already validated common SQL AST into a small,
protocol-neutral behavior taxonomy. The classifier is the shared request gate
for future PostgreSQL, MySQL, and other adapters; a frontend must not infer
behavior from SQL text, a wire command, SQLite result columns, or whether
SQLite reports a statement as read-only.

```rust
classify_statements(
    common: &CommonSql,
) -> EngineResult<StatementBatchClassification>
```

The call borrows `CommonSql`. It neither consumes nor rewrites the caller's
source, so the same validated value can independently continue to placeholder
normalization. `StatementBatchClassification` is an owned, ordered result and
exposes:

- `behaviors()`, with one entry per top-level statement in source order;
- `behavior(index)`, using a zero-based index and returning `None` outside the
  batch;
- `statement_count()`; and
- `is_read_only()`, which is true only for an accepted nonempty batch whose
  entries are all `Read`.

The result does not expose the parser dependency's AST. Its `Debug`
representation contains behavior metadata only and never renders submitted SQL,
comments, identifiers, literals, placeholders, or source locations.

## Behavior taxonomy

`StatementBehavior` is protocol neutral and deliberately more precise than a
single read/write Boolean:

| Common-subset statement | `StatementBehavior` |
| --- | --- |
| `SELECT` | `Read` |
| `INSERT` | `Write(WriteBehavior::Insert)` |
| `UPDATE` | `Write(WriteBehavior::Update)` |
| `DELETE` | `Write(WriteBehavior::Delete)` |
| `CREATE TABLE` | `Schema(SchemaBehavior::CreateTable)` |
| `CREATE INDEX` | `Schema(SchemaBehavior::CreateIndex)` |
| `BEGIN` | `Session(SessionBehavior::Begin)` |
| `COMMIT` | `Session(SessionBehavior::Commit)` |
| `ROLLBACK` | `Session(SessionBehavior::Rollback)` |

The public enums are non-exhaustive so a future common-subset expansion can add
behavior without exposing dependency-owned syntax types. The pinned parser can
map several accepted spellings to the same semantic AST. For example,
`ROLLBACK`, accepted rollback suffix aliases, and PostgreSQL `ABORT` all classify
as `Session(Rollback)`. Classification follows that semantic AST rather than
searching retained source text.

This taxonomy describes logical behavior; it is not execution permission. In
particular, a successfully classified singleton schema statement is denied by
ordinary prepared-compile policy, a session statement cannot execute through a
portal, and a classified write must still satisfy catalog, routing, and target
policy.

## Batch policy

Classification is all-or-nothing. A caller receives the complete ordered
classification only when the batch passes this table:

| Top-level statements | Result |
| --- | --- |
| Zero, including whitespace or comments only | `InvalidArgument` |
| Exactly one `Read`, `Write`, `Schema`, or `Session` | Accepted |
| Two or more, all `Read` | Accepted in source order |
| Two or more containing any `Write`, `Schema`, or `Session` | `Unsupported` |

Equivalently, the complete pairwise rule is:

| First / second | `Read` | `Write` | `Schema` | `Session` |
| --- | --- | --- | --- | --- |
| `Read` | Accept | Reject | Reject | Reject |
| `Write` | Reject | Reject | Reject | Reject |
| `Schema` | Reject | Reject | Reject | Reject |
| `Session` | Reject | Reject | Reject | Reject |

The rule is conservative because BriskDB does not yet expose a general batch
result or atomic multi-statement data-execution contract. It prevents a
frontend from partially applying a mutating batch or hiding a session/schema
transition among otherwise read-only work. A future relaxation requires an
explicit result, transaction, routing, and adapter contract; it must not be
implemented independently in one wire frontend.

The first non-`Read` statement in a rejected multi-statement batch determines
the diagnostic. The one-based ordinal and coarse behavior name are trusted
diagnostic metadata; the nested subtype and caller SQL are omitted. No partial
classification is returned.

The parser's maximum of 256 statements remains a resource ceiling rather than
permission. A 256-statement all-read batch can classify successfully. A 257th
statement is rejected by parsing before classification.

## Pipeline and error precedence

The general SQL analysis pipeline is:

```text
explicit-dialect parse -> common-subset validation -> batch classification
                                               |-> placeholder normalization
                                               |-> later analysis branches
```

Classification borrows `CommonSql`; normalization continues to consume it.
Callers that need both must classify first and then pass the same validated
value to normalization. The ordering establishes these rules:

1. parser syntax and parser limits win before subset or batch policy;
2. common-subset validation checks every statement in order, so its first
   unsupported form or expression-depth error wins before batch policy;
3. empty and mutating multi-statement policy runs before placeholder
   normalization, SQL translation, planning, or execution; and
4. after an all-read or singleton batch passes, later layers retain their own
   documented errors.

The classifier itself returns:

| Condition | `EngineErrorKind` |
| --- | --- |
| Empty/comment-only validated batch | `InvalidArgument` |
| Multi-statement batch whose first non-read entry is at ordinal N | `Unsupported` |
| A validated AST family has no classifier mapping | `Internal` |

Diagnostics never contain submitted SQL, identifiers, comments, literal or
parameter values, formatted AST output, source locations, or parser messages.
Adapters map only the stable `EngineErrorKind` and fixed public text from
[the error contract](ERRORS.md).

A failed classification does not mutate `CommonSql`, allocate session state,
or poison a later call. Because the classifier borrows an immutable validated
AST and uses no shared mutable state, concurrent classifications of equal input
produce equal results or equal stable errors.

## Planning integration

`Engine::plan_bound_statement` is the generic policy-bearing planning boundary.
Before selecting its requested `statement_index`, it applies the complete batch
classifier to the retained `CommonSql` inside `NormalizedSql`. Consequently:

- an empty normalized batch is `InvalidArgument`;
- a multi-statement batch containing a write, schema change, or session control
  is `Unsupported`, even when the selected member alone could be routed;
- an all-read multi-statement batch may be planned one member at a time; and
- a successful `BoundStatementPlan` exposes the selected member through
  `behavior()`.

Shard-key inference remains deliberately statement-local. Direct
`infer_shard_keys` callers select one normalized statement and receive a key
classification without granting batch permission. Planning composes that
statement-local inference with the complete batch gate and retains the selected
logical behavior alongside routing provenance.

Routing policy retains its narrow crate-private DML shape, including the extra
detail needed to detect shard-key assignments, while the plan exposes the
matching public `WriteBehavior`. Downstream execution no longer infers that an
absent DML shape or SQLite result columns imply a read. This preserves the
existing single-shard write rules while making schema and session behavior
explicit.
`BoundStatementPlan::Debug` may report behavior metadata but continues to omit
SQL, bound values, and routing-key bytes.

## Prepared-statement integration

Prepared statements keep their stricter cardinality contract. Their frontend
pipeline is:

```text
parse -> exact-one check -> common-subset validation -> classification ->
placeholder normalization -> explicit translation -> transient SQLite compile
```

Empty input and any input with more than one parsed statement return
`InvalidArgument` immediately after parsing. Thus a two-`SELECT` batch can pass
the general classifier but cannot become one prepared handle. The exact-one
error also precedes a later member's subset, marker, or translation error.

The prepared cache retains the singleton `StatementBehavior` as owned template
metadata. `PreparedStatementDescription::behavior()` exposes it to adapters;
the value is unchanged by binding, schema-generation metadata refresh, or
portal execution. Bind and execute plans expose the same selected behavior.

Prepared target selection uses this logical classification rather than
`PreparedStatementDescription::returns_rows()`:

- an assigned cataloged `Read` or `Write` uses its planned shard;
- an unassigned `Read` with `NotApplicable` inference, such as `SELECT 1`, uses
  deterministic shard 0;
- a `Read` of a `Global` table uses deterministic shard 0;
- a write to `Global` placement, an unassigned sharded read, and other targets
  without one implemented physical execution path are `Unsupported`;
- `Catalog` placement remains `PermissionDenied`; and
- persistent `Schema` behavior is denied with `PermissionDenied` during
  transient preparation, so no handle is published, while `Session` behavior
  is `Unsupported` at portal target selection.

SQLite column metadata still describes protocol results, but it does not grant
read permission or decide logical behavior. Persistent schema changes continue
through the journaled migration API, and real transaction state and shard
pinning remain later transaction work.

## Adapter and raw-surface boundary

Future PostgreSQL and MySQL adapters must pass their explicitly selected
dialect through the shared parser, subset validator, and classifier. They may
map the nested behavior to protocol command tags or status handling, but must
not implement a second keyword classifier or loosen batch policy on their own.
The same typed common SQL produces the same classification regardless of its
wire protocol.

Issue #27 adds no PostgreSQL or MySQL listener and no new HTTP route, request
field, response body, or configuration option. The experimental HTTP execute,
query, and migration endpoints remain raw SQLite surfaces and do not invoke the
opt-in common frontend. In particular, the journaled migration endpoint keeps
its own bounded, parameterless SQLite batch contract and can accept a schema
batch that the general common-SQL classifier deliberately rejects.

## Storage-format and configuration boundary

Classification is process-memory analysis over an already owned AST. It does
not change manifest format version 7, manifest tables, logical catalog rows,
routing-key encoding, virtual buckets, shard identity, SQLite headers,
application-schema fingerprints, migration-journal records, WAL/synchronous
mode, or filenames. It opens no database connection and performs no recovery
work.

No CLI flag, environment variable, `EngineOptions` field, or cache limit is
added. Classification input remains bounded by the parser's SQL byte,
statement-count, and recursion limits and the common subset's expression-depth
limit. A classification result is ordinary owned memory and is not persisted
across process restart.

Canonical translated SQL is still not a migration identity. The migration
coordinator continues to retain and digest the caller's exact submitted SQL,
independently of common-SQL classification.

## Verification obligations

Tests cover every nested behavior in SQLite, PostgreSQL, and MySQL; accepted
transaction aliases; AST-only handling of behavior words and semicolons in
comments, strings, aliases, and quoted identifiers; empty input; the complete
pairwise batch matrix in both source orders; first/middle/last rejected
ordinals; all-read batches and the exact parser statement limit; error
precedence; ordered accessors; out-of-range lookup; read-only status; owned
public types; diagnostics and `Debug` redaction; deterministic concurrent calls;
and successful calls after independent errors.

Planner tests cover the complete batch gate, selected behavior, all-read member
planning, inference's separate statement-local boundary, and behavior retained
across equivalent dialect inputs. Prepared tests cover exact-one precedence,
description behavior, schema refresh, behavior-based physical target policy,
denied schema prepare and rejected session execution without side effects, and
later valid work in the same session. Raw HTTP and storage regressions prove
that classification does not alter the existing migration or pass-through
contracts.
