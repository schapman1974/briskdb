# Bound statement planning and routing policy

Status: implemented for roadmap issues #23, #24, and #27 integration

BriskDB exposes one synchronous, protocol-neutral engine call that plans a
normalized statement from the values actually bound for that execution:

```rust
Engine::plan_bound_statement(
    &self,
    database: LogicalDatabaseId,
    normalized: &NormalizedSql,
    statement_index: usize,
    parameters: &[Value],
    explicit_routing_key: Option<&[u8]>,
) -> EngineResult<BoundStatementPlan>
```

`statement_index` is zero-based. `parameters` must be the complete bound-value
slice for that statement, exactly as required by
[`infer_shard_keys`](SQL_SHARD_KEYS.md). A statement whose shard key is a
placeholder cannot be routed when SQL is parsed or prepared because its value
does not exist yet. A frontend may call this API directly for a concrete bind,
while the prepared lifecycle invokes the same admitted planner when validating
a new portal and again for every execution.

The call performs protocol-neutral analysis only. It infers typed shard-key
values, converts them to canonical routing bytes, looks up physical shards,
compares optional explicit routing context, applies the first single-shard DML
rules, and records an assigned shard when one is valid. Before selecting a
statement, it applies the shared complete-batch classifier. It does not prepare,
translate, authorize, or execute SQL.

## Result contract

`BoundStatementPlan` owns all of its results and reports:

- `database()`: the logical database used for catalog resolution;
- `statement_index()`: the selected statement in the normalized batch;
- `behavior()`: the selected statement's authoritative logical
  `StatementBehavior`;
- `inference()`: the complete owned `ShardKeyInference` result;
- `inferred_routes()`: one `PlannedRoute` for every entry returned by
  `inference().values()`, in the same order;
- `explicit_route()`: the independently retained caller-supplied routing key,
  when one was supplied;
- `assigned_shard()`: the physical shard selected by routing policy, or `None`
  when no single-shard assignment exists; and
- `schema_generation()`, `map_generation()`, `hash_version()`,
  `key_encoding_version()`, and `bucket_algorithm_version()`: the catalog and
  routing provenance observed while the plan was produced.

Each `PlannedRoute` owns its canonical `key_bytes()` and reports its current
physical `shard()`. Owning the bytes keeps the plan independent of frontend
parameter buffers after the call returns.

`assigned_shard()` is an assignment, not permission to execute. `None` is a
normal successful result for a statement that is not sharded and for a read
that still needs later scatter or empty-result planning. Every accepted write
to a cataloged sharded table has `Some(shard)`.

The [prepared execution lifecycle](SQL_PREPARED_STATEMENTS.md) consumes this
result only after values are bound. Bind validates then discards one plan;
execution creates another under its current schema guard. Execution uses an
assigned shard for cataloged sharded work and may select deterministic shard 0
for a classified safe `NotApplicable` or `Global` read. It still rejects
catalog placement and sharded reads that need scatter. That target-selection
integration does not change this planner's meaning of `assigned_shard()`.

`Debug` output reports identifiers, versions, shard IDs, and counts where
useful. It does not render SQL, AST contents, inferred key values, explicit key
bytes, or parameter values.

## Routing precedence

For a cataloged sharded table, planning applies this order:

1. Use finite routes inferred from the normalized AST and actual bound values.
2. If inference produced no route, use explicit routing context where the DML
   rules below permit a fallback.
3. Leave a read without one physical target unassigned for later scatter or
   empty-result planning.
4. Reject a write that cannot be assigned to exactly one physical shard.

An explicit route never narrows or overrides finite inference. When inferred
routes exist, an explicit route is compatible only if it selects the same
physical shard as every inferred occurrence. Compatibility compares physical
shards, not key bytes: distinct opaque keys that currently map to the same
shard are compatible. A mismatch is `InvalidArgument`.

The plan continues to retain both route sources after a successful comparison.
This preserves their provenance for later session and execution work.

## Read assignment

The implemented [statement classifier](SQL_STATEMENT_CLASSIFICATION.md)
publishes the precise read/write/schema/session behavior. Planning applies its
full batch rule before selecting an index and retains the selected behavior.
Only statements classified as `Read` follow these deferred-assignment rules:

| Inference result | No explicit route | Compatible explicit route |
| --- | --- | --- |
| `Exact`, or `Multiple` whose occurrences all select one shard | Assign that inferred shard | Assign that inferred shard |
| `Multiple` spanning physical shards | Leave unassigned for later scatter | Reject because one explicit route cannot agree with every inferred route |
| `Unconstrained` | Leave unassigned for later scatter | Assign the explicit shard |
| `Contradiction` | Leave unassigned for later empty-result/validation policy | Assign the explicit shard |
| `NotApplicable` or `NotSharded` | Leave unassigned | Leave unassigned; retain explicit context only as advisory metadata |

No scatter execution or no-row short circuit is implemented here. In
particular, a contradictory predicate is still allowed to reach later SQLite
prepare and validation work rather than having this advisory layer invent a
result.

## Sharded write policy

The first sharded DML contract is intentionally single-shard:

| DML shape | Accepted assignment |
| --- | --- |
| `INSERT` with a proven key for every row | Every inferred occurrence must select one physical shard; an explicit route, if present, must select that shard |
| `INSERT` with an omitted or non-atomic shard-key value | Rejected even when explicit context exists, because the row's actual placement was not proven |
| `UPDATE` or `DELETE` with finite inference | Every inferred route must select one physical shard; a compatible explicit route may also be retained |
| `UPDATE` or `DELETE` with `Unconstrained` or `Contradiction` inference | Requires an explicit fallback and assigns its physical shard |
| Any finite write spanning physical shards | Rejected before execution; explicit context cannot repair or narrow it |

Duplicate `INSERT` keys and distinct logical keys that collide on one physical
shard are valid single-shard writes. Their individual route occurrences remain
visible in the plan.

A cataloged shard-key column is immutable in this first contract. Every
sharded `UPDATE` assignment targeting it is rejected, including self-assignment,
assigning the same literal, assigning a placeholder, and a statement whose
predicate is contradictory. Unquoted identifiers use the inference layer's
ASCII case-folded catalog comparison; quoted identifiers compare exactly.
Row movement between shards is not implemented.

`NotApplicable` and `NotSharded` statements do not become successful sharded
writes. Global and Catalog placement, plus schema/session statements, retain
`assigned_shard() == None` at this planning layer. Downstream execution policy
decides whether a specific unassigned singleton can run; the prepared lifecycle
currently reads Global data on shard 0, rejects Catalog placement, and does not
execute schema/session behavior.

## Canonical routing-key bytes

Planner key encoding version 1 converts typed inferred values as follows:

| Inferred type | Canonical routing bytes |
| --- | --- |
| `Int64` | Shortest signed base-10 ASCII form, with `0` for zero, `-` only for negative values, and no leading zeroes |
| `Text` | Exact UTF-8 bytes, with no Unicode normalization or case conversion |
| `Binary` | Exact bytes, including empty values and embedded zero bytes |

An `Int64` value of `-42`, for example, becomes `b"-42"`; `i64::MIN` and
`i64::MAX` are supported without narrowing. The encoding adds no type,
database, table, or column prefix.

`explicit_routing_key` is already an untyped routing byte sequence. Its bytes
are retained exactly, including an empty sequence; they are not parsed,
normalized, or converted through the typed encoding table.

Both inferred and explicit bytes enter the same persisted, versioned routing
catalog: BLAKE3 hash version 1, bucket algorithm version 1, and the catalog's
current virtual-bucket map select the physical shard. The recorded versions
and map generation identify the routing rules used by the plan.

## Occurrence and ordering rules

`inferred_routes()` is aligned one-for-one with
`inference().values()`. Planning does not collapse entries merely because:

- two `INSERT` rows supply the same logical key;
- two distinct keys currently map to the same physical shard; or
- an inferred key and the explicit routing key select the same shard.

This preserves multi-row order and every inferred occurrence. The
implementation shares immutable owned byte storage for duplicate typed values,
but still exposes one `PlannedRoute` per occurrence. Physical shard
deduplication is used only to decide whether the statement has one
single-shard assignable target.

## Catalog coordination and provenance

The engine acquires the existing schema-operation guard before reading the
logical catalog and keeps it across inference, route construction, narrow DML
inspection, and policy validation. A schema migration cannot begin during the
call. If the gate is already migrating or degraded, planning returns the same
protocol-neutral gate error as other ordinary engine operations before
statement policy is evaluated. The guard is released when the synchronous
call returns.

The successful plan records the application-schema generation,
routing-map generation, hash version, key-encoding version, and
bucket-algorithm version used by the call. These fields are provenance, not a
lease on future state. Portal execution establishes that the physical schema
and routing snapshot remain authoritative by producing a new plan from the
portal's owned values and route snapshot under every execution's fresh
schema-operation guard. Bind uses its plan only for transient validation.

The logical table catalog remains read-only and advisory. Planning does not
assert that its table metadata describes an existing physical SQLite table or
column. Prepared setup transiently compiles translated SQL against shard 0, but
it does not reconcile advisory table rows with the physical schema or mutate
the catalog.

## Errors and recovery

The caller reaches this API only after parsing, subset validation, and
normalization succeed. Planning first classifies the complete retained batch,
then preserves shard-key inference errors for the selected member and can also
return:

| Condition | `EngineErrorKind` |
| --- | --- |
| Empty normalized batch | `InvalidArgument` |
| Multi-statement batch containing any non-read behavior | `Unsupported` |
| Selected statement index is outside an otherwise accepted batch | `InvalidArgument` |
| Explicit physical shard conflicts with any finite inferred route | `InvalidArgument` |
| `UPDATE` or `DELETE` has no finite route and no explicit fallback | `InvalidArgument` |
| A sharded `UPDATE` assigns the cataloged shard-key column | `InvalidQuery` |
| A sharded `INSERT` does not prove every row's key | `InvalidQuery` |
| A finite sharded write spans physical shards | `InvalidQuery` |
| Retained inference, catalog, AST, or route metadata is inconsistent | `Internal` |

Schema-gate errors retain their existing `Busy`, `FailedPrecondition`, and
`DataCorruption` classifications. No new error kind or protocol mapping is
introduced.

Error precedence is deterministic: public engine schema admission happens
first; complete batch policy is next; the selected index is checked next;
inference follows; an attempted shard-key update is rejected next; finite
explicit-route conflicts are checked next; remaining write routability is
checked last. Thus a blocked mutating batch wins over member-specific parameter
or route errors. Within an accepted singleton, a shard-key update wins over a
conflicting explicit key, while a multi-target write with an explicit route
that disagrees with part of its finite inference is `InvalidArgument`.

Policy diagnostics use fixed categories. They contain no submitted SQL,
identifier spelling, literal, parameter value, routing-key bytes, or formatted
AST. An error retains no caller buffer, changes no normalized SQL or session,
and poisons no engine state; a later independent call can succeed.

## Deliberate boundaries

The direct issues #23 and #24 planner API itself adds no configuration, network
message, HTTP shape, manifest migration, shard-file change, or SQLite execution
path. In particular:

- `Engine::plan_bound_statement` is synchronous and stateless; it neither
  accepts nor mutates a `Session`;
- `assigned_shard()` is not consumed by the current raw HTTP execute/query
  paths, but it is consumed by issue #26 portal execution;
- no read scatter, merge, contradiction short circuit, or write executes here;
- no transaction pinning or cross-call routing context is applied;
- this synchronous method creates no per-session or global cache; issue #26
  retains only typed values and a routing snapshot in each portal, not this
  plan;
- the prepared bind path applies its occurrence-based planning-expansion byte
  ceiling before calling this method; a direct stateless planner call has no
  session cache or `PreparedStatementLimits` to consult;
- planning does not invoke the separate PostgreSQL/MySQL-to-SQLite translation
  layer;
- the current HTTP execute, query, and migration paths do not invoke parsing,
  validation, classification, normalization, inference, or planning.

The implemented issue #25 translation API can independently consume the same
normalized statement, but it neither changes nor executes a bound plan. The
implemented issue #26 protocol-neutral prepare/bind/describe/execute lifecycle
integrates translation and planning, validates transiently at bind, and plans
again from the bounded session portal's snapshot at every execution. The
implemented classifier supplies authoritative statement behavior and the
empty/single/multi-statement gate. Direct inference remains statement-local;
later query-planner work owns scatter/gather execution.

## Verification obligations

Tests cover canonical bytes; every inference classification; inferred route
ordering and duplicate storage sharing; physical same-shard key collisions;
same-shard and conflicting explicit context including empty/binary bytes;
single- and multi-row `INSERT`; exact, multiple, unconstrained, and
contradictory `UPDATE`/`DELETE`; immutable shard-key assignments under all
source dialects and statement indexes; read deferral; error precedence and
redaction; policy-error retries; deterministic valid and rejected concurrent
calls; schema-gate precedence and recovery; owned
public results; selected behavior; empty, all-read, and rejected mutating batch
policy; provenance; and equivalent SQLite, PostgreSQL, and MySQL typed requests.
Raw HTTP regressions prove that opt-in planning still does not change existing
execution behavior.
