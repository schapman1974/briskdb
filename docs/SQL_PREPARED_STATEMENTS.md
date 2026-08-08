# Prepared statements and bound portals

Status: implemented for roadmap issue #26

BriskDB exposes one protocol-neutral lifecycle for SQL submitted by SQLite,
PostgreSQL, MySQL, and future adapters. A frontend owns a `Session`, explicitly
selects the logical database, source dialect, and translation mode, and maps its
wire concepts onto BriskDB's opaque prepared-statement and portal handles.

```rust
use briskdb::{
    core::{
        DescribeTarget, Engine, EngineResult, LogicalDatabaseId, PrepareRequest,
        PreparedExecution, Session, Value,
    },
    sql::{SqlDialect, SqlTranslationMode},
};

async fn example(
    engine: &Engine,
    session: &Session,
    database: LogicalDatabaseId,
) -> EngineResult<()> {
    let statement = engine
        .prepare_statement(
            session,
            PrepareRequest::new(
                database,
                SqlDialect::PostgreSql,
                SqlTranslationMode::Compatibility,
                "SELECT payload FROM events WHERE tenant_id = $1",
            ),
        )
        .await?;

    let _description = engine
        .describe_prepared(session, DescribeTarget::Statement(statement))
        .await?;
    let portal = engine
        .bind_statement(session, statement, vec![Value::from(7_i64)])
        .await?;
    let routed = engine.execute_portal(session, portal).await?;

    match routed.value {
        PreparedExecution::Rows(rows) => {
            // Encode `rows` for the calling protocol.
            let _ = rows;
        }
        PreparedExecution::AffectedRows(rows) => {
            // Encode the command count for the calling protocol.
            let _ = rows;
        }
        _ => {}
    }
    Ok(())
}
```

## Public lifecycle

The public engine methods are:

```text
prepare_statement(&Session, PrepareRequest)
    -> EngineResult<PreparedStatementId>
bind_statement(&Session, PreparedStatementId, Vec<Value>)
    -> EngineResult<PortalId>
describe_prepared(&Session, DescribeTarget)
    -> EngineResult<PreparedStatementDescription>
execute_portal(&Session, PortalId)
    -> EngineResult<Routed<PreparedExecution>>
close_prepared_statement(&Session, PreparedStatementId)
    -> EngineResult<bool>
close_portal(&Session, PortalId)
    -> EngineResult<bool>
```

Prepare, bind, describe, and execute also have `*_with_context` variants that
accept `RequestContext`. Close is in-memory session cleanup and has no request
context.

`PrepareRequest` owns four explicit inputs:

- a valid `LogicalDatabaseId` present in the current catalog;
- one `SqlDialect` (`Sqlite`, `PostgreSql`, or `MySql`);
- one `SqlTranslationMode`; and
- the original SQL string.

There is no dialect detection and no default translation mode. The request's
`Debug` representation reports only the database, dialect, translation mode,
and SQL byte count; it never renders SQL text.

Preparation runs the same ordered frontend pipeline for every protocol:

```text
parse -> exact-one top-level statement check -> common-subset validation ->
statement classification -> placeholder normalization -> explicit translation
-> transient SQLite compile
```

The request must contain exactly one top-level SQL statement. Empty,
comment-only, and multi-statement input returns `InvalidArgument`. This is a
prepared-handle cardinality rule, not the general request classifier. The
implemented [statement/batch classifier](SQL_STATEMENT_CLASSIFICATION.md)
accepts a general multi-statement request only when every member is a read and
supplies the singleton's precise read/write/schema/session behavior here.

## Logical cache and handle ownership

Every `Session` owns an independent bounded logical cache. A cached statement
contains BriskDB-owned `TranslatedSql` and owned description metadata. It never
retains a `rusqlite::Statement`, `rusqlite::Rows`, SQLite connection, pooled
handle, protocol buffer, or adapter object. Preparation transiently compiles
the translated SQLite SQL on physical shard 0, copies its metadata, drops the
SQLite statement, and only then publishes the logical cache entry.

`PreparedStatementId` and `PortalId` are opaque, session-scoped handles. Each
contains its owning process-unique session identity and a monotonically
increasing nonzero sequence. Sequences are never reused, including after a
close. A handle from another session or engine cannot resolve in the receiving
session.

The cache never evicts a client-visible handle implicitly. When a configured
count or byte limit is full, the new prepare or bind fails with
`LimitExceeded`; every existing handle remains valid. Explicit close releases
capacity:

- `close_portal` removes one portal and returns `true`; closing it again in the
  same session returns `false`;
- `close_prepared_statement` removes one statement and every portal bound from
  it, releases all of their retained-value bytes, and returns `true`; a second
  same-session close returns `false`; and
- `Session::close` is terminal and idempotent and clears routing context,
  prepared statements, portals, and retained-byte accounting.

Describing, binding, or executing an absent same-session handle returns
`FailedPrecondition`. Closing a statement invalidates all dependent portals.
An ordinary statement, bind, description, planning, or execution error does not
evict unrelated valid handles or poison the session.

## Description metadata

Preparation compiles canonical `TranslatedSql::sqlite_sql()` on deterministic
physical shard 0 under the normal schema-operation, pool, worker, cancellation,
deadline, connection-isolation, and SQLite cleanup boundaries. Shard 0 is the
metadata authority because every usable application schema is generation-bound
and verified to match across shards.

`PreparedStatementDescription` is owned, cloneable, `Send`, and `Sync`. It
reports:

- the authoritative parsed `StatementBehavior` retained at prepare time;
- one `DataType::Unknown` entry for every normalized one-based parameter index;
- ordered result `Column` values copied from SQLite, preserving duplicate and
  empty names;
- `DataType::Unknown` for every result column;
- the application-schema generation used for compilation; and
- `returns_rows()`, derived from whether SQLite reported result columns.

BriskDB deliberately does not infer PostgreSQL or MySQL wire types from SQLite
affinity in this milestone. An adapter decodes its parameter representation to
a concrete protocol-neutral `Value` before bind and maps `Unknown` metadata
according to its own documented wire rules.

`DescribeTarget::Statement` describes a statement;
`DescribeTarget::Portal` describes its underlying statement. A description at
the current schema generation is returned from owned memory. After a completed
schema migration changes the generation, describe recompiles the SQL on shard
0 and atomically replaces the cached column metadata while preserving the
classified behavior. A normalized-versus-SQLite
parameter-count mismatch is an `Internal` invariant failure and does not
publish mismatched metadata.

## Binding and portals

Binding happens only after concrete typed values exist. It validates the exact
parameter count and every value's lossless SQLite binding, then invokes the
same bound-statement planner documented in [SQL planning](SQL_PLANNING.md).
The planner receives statement index 0, the prepared statement's selected
logical database and retained normalized SQL, the complete value slice, and
the session's routing key at that instant.

A successful bind creates an immutable logical portal that owns:

- its parent `PreparedStatementId`;
- the complete protocol-neutral bound values;
- a byte-for-byte snapshot of the session routing key, when present.

The bind-time `BoundStatementPlan` is validation only and is discarded before
the portal is published. A portal never retains inferred route bytes, an
explicit planned route, schema/routing provenance, or another hidden planning
allocation. This keeps its retained-byte accounting equal to the documented
values-plus-route model.

Changing or clearing the session routing key after bind does not change an
existing portal. A later bind observes the later routing context. This lets an
adapter map PostgreSQL named and unnamed portals, MySQL statement executions,
or another protocol's equivalent operation onto one engine rule without
retaining frontend buffers. Portal `Debug` output reports only counts,
whether a route exists, and retained bytes; it does not render parameter values
or routing bytes.

Portals are reusable and are not consumed by execution. Multiple portals may
refer to one prepared statement, subject to the per-session limits. Portal row
suspension and protocol cursor semantics are not implemented; each execution
materializes one complete bounded result or returns an error.

## Cache limits and exact byte accounting

`PreparedStatementLimits` validates three independent finite per-session
limits:

| Limit | Default | Configurable range | CLI / environment |
| --- | ---: | ---: | --- |
| Prepared statements | 128 | 1–1,024 | `--max-prepared-statements-per-session` / `BRISKDB_MAX_PREPARED_STATEMENTS_PER_SESSION` |
| Bound portals | 128 | 1–1,024 | `--max-portals-per-session` / `BRISKDB_MAX_PORTALS_PER_SESSION` |
| Retained logical-accounted value/per-bind planning bytes | 16 MiB | 1 byte–1 GiB | `--max-retained-bound-value-bytes` / `BRISKDB_MAX_RETAINED_BOUND_VALUE_BYTES` |

Rust embedders configure these with
`EngineOptions::with_prepared_statement_limits`; `Engine::options()` and
`EngineStatus::prepared_statement_limits()` expose the effective values. A
server validates CLI/environment values before listener binding or data-file
creation.

The `max_retained_bound_value_bytes()` accessor and retained-bound-value
configuration name expose one ceiling used for two separate comparisons: the
session aggregate retained by open portals and one bind's transient planning
preflight. BriskDB does not add those two comparisons together.

The retained-byte limit covers values and captured routing bytes owned by all
open portals in one session. It is independent of result materialization and
does not count cached SQL; prepared SQL already has the parser's 65,536-byte
per-request ceiling and the separate prepared-statement count bound.

For every parameter, the stable protocol-neutral accounting model charges one
type-tag byte, an eight-byte length, and the payload:

| `Value` | Payload bytes |
| --- | ---: |
| `Null` | 0 |
| `Boolean`, `Int64`, `UInt64`, `Float64` | 8 |
| `Decimal` | Exact retained decimal-string byte length |
| `Text` | Exact UTF-8 byte length |
| `InvalidText`, `Binary` | Exact byte length |

The captured route adds exactly its byte length, with no route envelope, type,
or length charge. A missing route adds zero. Empty values and an empty captured
route remain valid and are accounted exactly. This is a logical accounting
model; BriskDB retains typed `Value` objects, not a serialized accounting or
wire encoding. All addition and integer conversion is checked. Equality at the
configured limit succeeds; exceeding it, or an accounting overflow, is
`LimitExceeded`. Failed binds retain no values and change no counters.

The same configured byte ceiling also bounds one bind's conservative transient
planning expansion before the planner can allocate inferred values or canonical
route bytes. The charge starts with one exact copy of the captured routing-key
bytes, or zero when no route exists. BriskDB then walks
`StatementParameters::parameter_indices()` in normalized occurrence order. For
each occurrence, including each appearance of a repeated marker, it charges
twice the referenced parameter's logical accounted value bytes: one type-tag
byte, an eight-byte length, and the payload listed above. The two charges cover
one possible typed-inference copy and one possible canonical-route copy.

This planning charge is a per-bind preflight, not retained cache state. It does
not include existing portals, is not added to the session's retained-byte
counter, and disappears when the check returns. Gapped parameters with no
marker occurrence add no planning charge, although their values are still part
of the exact retained portal input. Equality succeeds; overflow or a sum above
the ceiling is `LimitExceeded`. Consequently, a large captured route or
repeated markers can reject a bind even when its exact retained portal input
would otherwise fit the remaining session capacity. The planner is not called
and no portal is published on that failure.

## Execution and provenance

`execute_portal` never hashes a frontend buffer or selects a shard in an
adapter. It reloads both same-session handles and always creates a new
`BoundStatementPlan` from the portal's owned values and captured routing
snapshot under the current schema-operation guard. The new plan necessarily
contains the current schema generation, routing-map generation, hash version,
key-encoding version, and bucket-algorithm version. The engine keeps that same
guard through target selection and SQLite completion, then discards the plan.

Target selection is intentionally narrow and starts from the retained logical
behavior rather than SQLite result-column metadata:

- accepted cataloged sharded reads and writes use the current plan's one
  `assigned_shard()`;
- a classified `Read` with `NotApplicable` inference, such as `SELECT 1`, uses
  deterministic shard 0;
- a classified `Read` of a `Global` table uses deterministic shard 0;
- `Catalog` placement is `PermissionDenied`; and
- an unassigned sharded read, a `Global` write, or `Session` behavior is
  `Unsupported`.

Shard 0 is valid for the two replicated-schema read cases because every ready
shard has the same generation-bound application schema and `Global` declares
replicated lookup placement. This initial path reads one deterministic replica;
it does not compare replicas.

These paths remain outside the current lifecycle:

- scatter/gather or otherwise multi-shard reads;
- contradictory reads that might later become an empty result without SQLite;
- global writes, schema and session-statement execution paths;
- persistent DDL outside the journaled schema-migration API;
- explicit transaction state and shard pinning.

Cataloged sharded DML must already satisfy the single-shard policy in
[SQL planning](SQL_PLANNING.md). A cataloged read with finite inference, or one
accepted through the bind-time routing fallback, can execute when it has one
assigned shard. The shared classifier is authoritative; SQLite's ability to
compile a statement or report columns never grants execution permission.

Persistent `CREATE TABLE` and `CREATE INDEX` are classified as `Schema`, but
ordinary shard authorizer policy denies them during transient preparation with
`PermissionDenied`, so no prepared handle or description is published. A
session-control singleton can be prepared and described, but portal execution
returns `Unsupported` before SQLite is stepped and leaves the session usable.

SQLite transiently prepares and executes the retained canonical SQLite SQL on
the selected target. Classified reads must produce
`Routed<PreparedExecution::Rows(ResultSet)>`; commands produce
`Routed<PreparedExecution::AffectedRows(usize)>` only for classified writes. A
disagreement between classified behavior and SQLite execution metadata is an
`Internal` invariant failure. Row results preserve ordered
metadata, duplicate names, positional values, and the normal exact result row
and logical-byte limits. Row-producing writes are rejected rather than partly
materialized. The returned `Routed` value always names the physical shard that
performed the operation.

## Controls, concurrency, close, and shutdown

Prepare, bind, describe, and execute are ordinary engine operations. Their
`*_with_context` methods apply the caller's sticky cancellation token and
deadline; the engine default deadline is still an upper bound. Prepare and a
schema-refreshing describe apply those controls while waiting for shard 0 and
while SQLite compiles metadata. Execute also applies per-request result limits,
SQLite interruption, rollback, pool cleanup, and exact-handle retirement.

The engine serializes all concurrent operations borrowing one `Session`. The
session lock remains owned across pending admission and blocking SQLite work,
so two successful mutations of one logical cache have one deterministic order.
Different sessions have independent caches and can progress concurrently,
subject to the existing schema gate, per-shard pools, and worker limits.
Cancellation while waiting for the session publishes no statement or portal.
`Session::close` waits for an admitted same-session operation, then clears all
state; later prepared operations fail with `FailedPrecondition`.

After `begin_shutdown`, new prepare, bind, describe, and execute admissions
return `ShuttingDown`; previously admitted work drains under the ordinary
shutdown contract. `close_prepared_statement`, `close_portal`, and
`Session::close` are in-memory cleanup and remain available while the engine is
draining. Prepared state is not persisted and requires no restart recovery.

## Errors and recovery

The lifecycle preserves the existing stable `EngineErrorKind` taxonomy:

| Condition | Kind |
| --- | --- |
| Unknown logical database, empty/multi-statement prepare, wrong bind arity, or conflicting explicit/inferred route | `InvalidArgument` |
| SQL parse/SQLite compile failure or planner write-policy rejection where documented | `InvalidQuery` |
| Unsupported subset/translation form, session execution, unsupported physical target, or row-producing write | `Unsupported` or the narrower documented SQL-path kind |
| Persistent schema prepare, or execution with `Catalog` placement | `PermissionDenied` |
| Incompatible key/value type | `TypeMismatch` |
| Out-of-range unsigned integer or inferred integer | `NumericOutOfRange` |
| Invalid text value | `InvalidTextEncoding` |
| Full cache, full portal set, retained-value/planning limit, or sequence/accounting exhaustion | `LimitExceeded` |
| Closed session, foreign engine/session handle, absent statement/portal, or pending schema migration | `FailedPrecondition` |
| Pool/schema admission contention | `Busy` |
| Request cancellation or deadline | `Cancelled` / `DeadlineExceeded` |
| Degraded storage state | `DataCorruption` |
| Retained metadata/accounting mismatch, or classified behavior disagreeing with SQLite execution metadata | `Internal` |

Parsing, validation, classification, normalization, translation, inference,
planning, SQLite, request-control, and storage errors retain the precedence and
kind defined by their normative contracts. Cache-limit failures do not evict older entries.
Failed prepare does not publish an ID; failed bind does not publish a portal;
failed describe does not replace valid current metadata; and failed execution
retains the portal for inspection, retry where independently appropriate, or
explicit close. Corrected values require binding a new portal.

Adapters map only the stable kind and fixed public text according to
[the error contract](ERRORS.md); they never serialize an `EngineError` display,
source chain, or dependency-owned SQLite diagnostic. `PrepareRequest`, cached
template/state, portal, `BoundStatementPlan`, and
`PreparedStatementDescription` `Debug` output is deliberately redacted to
counts, modes, IDs, versions, and route presence. Internal trusted validation
diagnostics may include a rejected value where an existing value-conversion
contract says so, including out-of-range unsigned integers or decimals.
`PreparedExecution` `Debug` intentionally renders its user-visible affected-row
count or result values. Operators must therefore apply their normal result/log
handling to that type.

## Dialect and adapter compatibility

The lifecycle is implemented as a Rust engine API, not yet as a PostgreSQL or
MySQL listener. All three input paths share the same planning and result path:

| Input | Required mode and parameter form | Prepared execution result |
| --- | --- | --- |
| SQLite | `StrictSqlite` for exact normalized SQLite, or explicit finite `Compatibility`; `?` / `?N` | Protocol-neutral routed rows or affected-row count |
| PostgreSQL | `Compatibility`; `$N` identities, including repeats and gaps | Same protocol-neutral routed rows or affected-row count |
| MySQL | `Compatibility`; each `?` numbered left-to-right | Same protocol-neutral routed rows or affected-row count |

Future adapters own message framing, authentication, statement/portal naming,
wire parameter decoding, result type encoding, close acknowledgement, and
protocol resynchronization. They must call this lifecycle rather than retain a
SQLite handle, implement a second cache, interpolate values into SQL, or choose
a physical shard themselves.

## Storage-format boundary

Issue #26 adds only process-memory session state and execution orchestration.
It does not change manifest version 7, a manifest table, routing encoding,
shard identity, schema fingerprint, migration journal, SQLite header field,
WAL/synchronous mode, or filename. Prepared statements and portals disappear
when their owning process/session ends. Preparing or describing changes no
application data; executing a supported command has only its ordinary
single-shard SQLite row effects.

Canonical translated SQL is never a schema-migration identity. Persistent
application schema changes continue to require the journaled migration path,
which retains the caller's exact submitted SQL. No storage upgrade, downgrade,
or prepared-cache recovery step is required.

## Verification obligations

Tests cover the complete SQLite/PostgreSQL/MySQL typed lifecycle; exact
single-statement enforcement; unknown databases; transient shard-0 metadata;
classified behavior and parameter/column descriptions; schema-generation
behavior preservation; fresh planning
after schema/routing changes; routing snapshots; affected-row and row results;
result limits; session ownership; statement, portal, and retained-byte limits without
eviction; repeated-marker planning expansion before allocation; exact close and
cascading invalidation; invalid arity/value recovery; behavior-based unassigned
execution; schema-prepare denial and session-execution rejection without state
changes;
deterministic concurrent capacity races; cancellation while waiting for a
session; session close during admitted work; diagnostics and safe protocol
mapping; targeted request/cache/portal/plan/description `Debug` redaction;
result visibility; CLI/environment validation; public owned types; and raw
HTTP/storage-format regressions.
