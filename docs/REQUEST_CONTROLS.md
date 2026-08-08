# Request controls and shutdown

BriskDB applies cancellation, deadlines, result budgets, and shutdown at the
protocol-neutral `Engine` boundary. HTTP and future PostgreSQL/MySQL wire
adapters therefore share the same resource and cleanup semantics. The current
PostgreSQL TCP placeholder never creates an engine request.

## Per-request context

The existing `Engine::execute`, `query`, `broadcast`, and `status` methods, plus
`prepare_statement`, `bind_statement`, `describe_prepared`, and
`execute_portal`, use a default `RequestContext`. `broadcast` now means a
journaled application-schema migration. Frontends that have their own
cancellation or deadline source can call the corresponding `*_with_context`
method:

```rust
use std::time::Duration;
use briskdb::core::{CancellationToken, RequestContext, ResultLimits};

let cancellation = CancellationToken::new();
let context = RequestContext::new()
    .with_cancellation_token(cancellation.clone())
    .with_timeout(Duration::from_secs(2))?
    .with_result_limits(ResultLimits::new(500, 1024 * 1024)?);
# Ok::<(), briskdb::core::EngineError>(())
```

`CancellationToken` is cloneable, sticky, and idempotent. All existing and
future waiters observe cancellation after `cancel()` is called. Tokens belong
to request contexts, not sessions, so cancelling request A cannot accidentally
cancel a later request B on the same session.

An operation still waiting for its session, shard connection, or blocking
worker leaves the queue immediately when cancelled. It never starts SQL. Once
SQLite is running, BriskDB arms an interrupt handle and progress callback only
for the currently leased physical connection. Cancellation interrupts that
handle, waits for the blocking task to finish rollback and pool cleanup, and
only then returns `Cancelled`. The interrupted handle is retired before another
request can use it. Dropping the public operation future follows the same
interrupt path; lifecycle and pool permits remain held by the blocking closure
until cleanup really completes.

Lazy pooled-handle configuration and the authorizer probe used before a clean
handle crosses session owners run under the same controls. Cancellation can
therefore end SQLite lock waits or expensive preparation before the main SQL
call starts. Opening the database file itself is an operating-system call and
cannot be synchronously interrupted, but controls are checked before any
SQLite configuration or statement work proceeds.

Prepared operations use the same boundary. Prepare and a schema-refreshing
describe can wait for shard 0 and transient SQLite metadata compilation. Bind
can wait for the serialized session before it validates and snapshots values.
Portal execution can wait for its selected shard and runs with the same
exact-handle interruption and cleanup as raw execution. Cancellation before a
prepared object is published leaves the session cache unchanged; a cancelled
or failed execution retains the existing portal.

Completion wins a very close race with cancellation. A statement that is known
to have completed successfully returns success rather than a misleading
cancellation error. A single SQLite write statement interrupted before
completion retains SQLite's statement atomicity. Callers must still treat any
transport-level disconnect without a BriskDB response as an unknown outcome.

The engine default deadline is 30 seconds. `EngineOptions::with_request_timeout`
can change it or use `None` to disable only that default. An explicit absolute
deadline in `RequestContext` remains active, and the earlier of the engine and
request deadlines wins. Deadline failures use the distinct
`DeadlineExceeded` kind. The server flag `--request-timeout-ms 0` disables the
engine default.

## Materialized result budgets

Every query has a finite row and logical-byte budget. Defaults are 10,000 rows
and 16 MiB; the configurable hard caps are 1,000,000 rows and 1 GiB. A request
context may narrow but never widen its engine's configured budget. Equality at
the limit succeeds. Exceeding either limit returns `LimitExceeded` without a
partial `ResultSet`.

Logical bytes use a stable protocol-neutral model rather than JSON or future
wire-protocol encoding:

- 16 bytes for the result envelope;
- for each column, one type byte, an eight-byte length, and the UTF-8 column
  name bytes;
- eight bytes for each row; and
- for each value, one type byte, an eight-byte length, and its payload.

Null has a zero-byte payload. Integer and floating payloads use eight bytes.
Text and binary payloads use their exact byte length. All arithmetic is checked.
BriskDB accounts borrowed SQLite values before cloning any text or blob into the
result. Detecting a row overflow requires stepping one additional SQLite row,
and SQLite can still allocate its current row internally. The logical limit is
not an HTTP encoded-body limit; for example, the current JSON representation of
a blob expands bytes into JSON integers.

The raw query path accepts only SQLite statements reported as read-only. The
prepared executor likewise rejects row-producing writes. These rules prevent
an early result-budget failure from accompanying a partially consumed DML
`RETURNING` statement. Raw execute, prepared affected-row results, and schema
migration do not materialize a `ResultSet` and are unaffected by query result
budgets. A future scatter/gather implementation must apply one budget to the
combined result, not a fresh budget for every shard.

## Prepared-session limits

Prepared caches have a separate finite per-session budget: 128 statements, 128
portals, and a 16 MiB retained-value/per-bind-planning ceiling by default, with
hard caps of 1,024, 1,024, and 1 GiB. Full caches return `LimitExceeded` without
evicting open handles. `EngineOptions`, server CLI flags, and `BRISKDB_*`
environment variables can configure each value.

This retained-value budget is independent of materialized-result bytes. Its
logical accounting charges one type-tag byte and an eight-byte length for each
parameter, the documented value payload, and exactly the captured route's byte
length. It does not serialize or retain a wire encoding. Explicit close or
terminal session close releases the charge. The exact model and configuration
names are in
[prepared statements and bound portals](SQL_PREPARED_STATEMENTS.md).

Before planner allocation, one bind also compares a conservative transient sum
to the same byte ceiling. The sum starts with one exact copy of the captured
routing-key bytes. Each normalized marker occurrence then charges twice its
referenced logical accounted value bytes (one type-tag byte, an eight-byte
length, and its payload), once for a possible typed-inference copy and once for
a possible canonical-route copy. Repeated markers are charged again. The
transient sum is not retained or added to existing portal bytes. A failure
returns `LimitExceeded` before planning and publishes no portal.

## Schema-migration controls

A schema-migration request first acquires the exclusive schema gate and waits
for all previously admitted ordinary work to drain. New ordinary operations and
a second migration coordinator receive retryable `Busy` while that request is
preflighting or applying. The migration then uses fresh connections with the
same sticky cancellation token, deadline, cancellable busy handler, SQLite
progress callback, and exact-handle interrupt behavior as ordinary work.

Before the durable journal is created, cancellation or deadline expiration
rolls back the current preflight transaction, leaves every shard at the source
generation, and returns the gate to `Ready`. After journal creation, an
interrupted shard transaction rolls back but already committed shards remain as
an ascending prefix; the gate becomes `Pending` and rejects ordinary work with
non-retryable `FailedPrecondition`. Submitting byte-identical SQL or restarting
BriskDB validates and resumes that prefix. A shard commit can win a close
cancellation race; recovery recognizes the one committed-but-not-yet-recorded
prefix boundary and does not apply its SQL twice.

Request cancellation is checked before each manifest commit begins. Once
SQLite has attempted `COMMIT`, a cleanup or I/O error can make the outcome
ambiguous; BriskDB conservatively leaves the gate `Pending` so startup or an
exact retry can validate the durable journal before ordinary work resumes.

Dropping the migration future follows the same cleanup path. Its lifecycle and
worker lease remain live until SQLite cleanup finishes, and the gate publishes
`Ready` or restores `Pending` according to whether a durable journal exists.
The exact SQL is retained in that journal, so callers must not embed secrets or
other sensitive literals.

## Graceful shutdown

All `Engine` clones share a monotonic lifecycle:

```text
Running -> Draining -> Stopped
```

`begin_shutdown()` atomically stops new admissions. New operations return
`ShuttingDown`; operations admitted before the transition keep running.
`shutdown()` waits for those operations for the configured grace period. If the
period expires, it cancels admitted work and waits one additional grace period
for SQLite cleanup. A completed report records whether forced cancellation was
needed. Idle SQLite connections are closed on a bounded blocking worker before
the engine reaches `Stopped`.

If forced cleanup also exceeds its grace period, shutdown returns
`DeadlineExceeded` and leaves the engine safely in `Draining`. A later
`shutdown()` call resumes cleanup. Concurrent calls are serialized, completed
shutdown is idempotent, and dropping one shutdown waiter does not strand the
shared lifecycle. Dropping an ordinary `Engine` clone does not initiate
shutdown; embedders should call the explicit asynchronous hook.

Prepared statement/portal close and `Session::close` are in-memory cleanup and
remain available while the engine is draining. Terminal session close waits
for an admitted same-session operation, then clears every statement, portal,
captured route/value, and routing context. Nothing in the prepared cache is
persisted or recovered after process shutdown.

The server constructs its SIGINT/SIGTERM receivers after every configured
listener binds and before logging readiness on supported Unix hosts. It
transitions the engine to `Draining` before dropping both the HTTP listener and
the optional PostgreSQL listener and signaling each tracked HTTP/1 connection.
HTTP draining and core shutdown start together. A connection still active at
the HTTP grace deadline is aborted and joined before server shutdown returns;
accepted PostgreSQL placeholder streams were closed immediately and own no
task to drain. Core cleanup may continue through its separately documented
forced-cleanup grace. A forced cancellation cannot erase committed
schema-migration progress: the current shard transaction rolls back if still
running, the retained prefix remains resumable, and the next startup finishes
it before serving ordinary work.
