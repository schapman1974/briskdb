# Bound statement planning

Status: implemented for roadmap issue #23

BriskDB exposes a synchronous, protocol-neutral engine call that plans one
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
[`infer_shard_keys`](SQL_SHARD_KEYS.md). Requiring that slice is the important
timing boundary: a statement whose shard key is a placeholder cannot be routed
when SQL is parsed or prepared, because its value does not exist yet. A
frontend calls this API for a concrete bind/execute operation.

This is an advisory planning API. It infers typed shard-key values, converts
them to canonical routing bytes, and looks up their current physical shards.
It does not execute SQL or make a statement executable.

## Result contract

`BoundStatementPlan` owns all of its results and reports:

- `database()`: the logical database used for catalog resolution;
- `statement_index()`: the selected statement in the normalized batch;
- `inference()`: the complete owned `ShardKeyInference` result;
- `inferred_routes()`: one `PlannedRoute` for every entry returned by
  `inference().values()`, in the same order;
- `explicit_route()`: the independently planned caller-supplied routing key,
  when one was supplied; and
- `schema_generation()`, `map_generation()`, `hash_version()`,
  `key_encoding_version()`, and `bucket_algorithm_version()`: the catalog and
  routing provenance observed while the plan was produced.

Each `PlannedRoute` owns its canonical `key_bytes()` and reports the selected
physical `shard()`. Owning the bytes keeps the plan independent of frontend
parameter buffers after the call returns.

The plan retains the inference classification even when there is no inferred
route. `NotApplicable`, `NotSharded`, `Unconstrained`, and `Contradiction`
therefore remain successful advisory results with an empty
`inferred_routes()` slice. An explicit route can still be present in any of
those cases.

`Debug` output reports identifiers, versions, shard IDs, and counts where
useful. It does not render SQL, AST contents, inferred key values, explicit key
bytes, or parameter values.

## Canonical routing-key bytes

Planner key encoding version 1 converts typed inferred values as follows:

| Inferred type | Canonical routing bytes |
| --- | --- |
| `Int64` | Shortest signed base-10 ASCII form, with `0` for zero, `-` only for negative values, and no leading zeroes |
| `Text` | Exact UTF-8 bytes, with no Unicode normalization or case conversion |
| `Binary` | Exact bytes, including empty values and embedded zero bytes |

An `Int64` value of `-42`, for example, becomes the three ASCII bytes
`b"-42"`; `i64::MIN` and `i64::MAX` are supported without narrowing. The
encoding adds no type, database, table, or column prefix.

`explicit_routing_key` is already an untyped routing byte sequence. Its bytes
are retained exactly, including an empty sequence; they are not parsed,
normalized, or converted through the typed encoding table.

Both inferred and explicit bytes enter the same persisted, versioned routing
catalog: BLAKE3 hash version 1, bucket algorithm version 1, and the catalog's
current virtual-bucket map select the physical shard. The recorded versions
and map generation identify the routing rules used by this plan.

## Occurrence and ordering rules

`inferred_routes()` is aligned one-for-one with
`inference().values()`. Planning does not collapse entries merely because:

- two `INSERT` rows supply the same logical key;
- two distinct keys currently map to the same physical shard; or
- an inferred key and the explicit routing key select the same shard.

This preserves multi-row `INSERT` row order and every inferred occurrence for
the later policy layer. An implementation may share immutable owned storage
for duplicate canonical bytes, but the public route entry for each occurrence
remains present.

## Inferred and explicit routes remain separate

The explicit route is a candidate fallback, not an override selected by this
API. Planning deliberately does not:

- choose inferred routing over explicit routing or the reverse;
- declare matching key bytes or matching physical shards sufficient;
- reject different inferred and explicit keys;
- reject `Multiple`, `Unconstrained`, or `Contradiction` inference;
- decide whether a read may scatter; or
- decide whether a write may execute.

Roadmap issue #24 owns those assignment and write-policy decisions. Keeping
both inputs in the plan lets that layer compare logical keys and plan shape
without losing information during routing.

## Catalog coordination and provenance

The engine acquires the existing schema-operation guard before reading the
logical catalog and routing snapshot. A schema migration cannot begin while
the plan is being assembled. If the gate is already migrating or degraded,
planning returns the same protocol-neutral gate error as other ordinary engine
operations. The guard is released when the synchronous call returns.

The successful plan records the application-schema generation, routing-map
generation, hash version, key-encoding version, and bucket-algorithm version
used by the call. These fields are provenance, not a lease on future state.
This issue does not connect a plan to execution, so no SQLite statement runs
under that provenance yet. A future execution path must establish that the
physical schema and routing snapshot are still authoritative and reject or
replan stale provenance before using the planned shards.

The logical table catalog remains read-only and advisory. Planning does not
assert that its table metadata describes an existing physical SQLite table or
column. Physical catalog authority and execution validation remain future
integration work.

## Errors and recovery

Planning returns shard-key inference errors unchanged, including invalid
database, table, statement index, parameter arity, key type, integer range,
text encoding, and non-null cases. It can additionally return the existing
schema-gate errors while a migration or degraded state excludes ordinary work.
An internally inconsistent inference/result shape is `Internal`.

An error does not retain caller parameters, mutate the normalized SQL, poison
the engine, or populate session state. A later independent call may succeed.

## Deliberate boundaries

Issue #23 introduces no configuration, CLI option, network message, HTTP shape,
manifest migration, shard-file change, or new SQLite execution path. In
particular:

- `Engine::plan_bound_statement` is synchronous and stateless; it neither
  accepts nor mutates a `Session`;
- no per-session or global prepared-statement cache is created;
- no PostgreSQL or MySQL syntax or type is translated;
- no write, scatter, transaction, authorization, or batch policy is applied;
- no statement is prepared, described, executed, or returned to a client; and
- the current HTTP execute, query, and migration paths do not invoke parsing,
  common-subset validation, normalization, inference, or this planner.

Issue #24 owns conflicting and unroutable write policy. Issue #25 owns selected
syntax and type translation. Issue #26 owns the protocol-neutral
prepare/bind/describe/execute lifecycle, session integration, and bounded
per-session cache. Issue #27 owns statement behavior and empty, single-, and
multi-statement request policy.

## Verification obligations

Tests cover canonical bytes for signed integer boundaries, UTF-8 text, and
arbitrary binary values; explicit keys including empty and binary sequences;
all inference classifications; route ordering and retained duplicate
multi-row values; distinct logical keys that choose the same physical shard;
independent inferred and explicit routes; schema and routing provenance;
schema-gate exclusion and recovery; every supported source dialect; numbered
parameter gaps and rebinding the same normalized statement; propagated
inference errors and recovery; deterministic concurrent calls; owned results;
and redacted diagnostics and `Debug` output. Raw HTTP regressions continue to
prove that advisory planning does not change existing execution behavior.
