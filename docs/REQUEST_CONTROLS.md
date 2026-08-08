# Request controls and shutdown

BriskDB applies cancellation, deadlines, result budgets, and shutdown at the
protocol-neutral `Engine` boundary. HTTP and future PostgreSQL/MySQL adapters
therefore share the same resource and cleanup semantics.

## Per-request context

The existing `Engine::execute`, `query`, `broadcast`, and `status` methods keep
their signatures and use a default `RequestContext`. Frontends that have their
own cancellation or deadline source can call the corresponding
`*_with_context` method:

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

The query path accepts only SQLite statements reported as read-only. This
prevents an early result-budget failure from accompanying a partially consumed
DML `RETURNING` statement. Execute and broadcast do not materialize a
`ResultSet` and are unaffected by query result budgets. A future scatter/gather
implementation must apply one budget to the combined result, not a fresh budget
for every shard.

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

The server constructs its SIGINT/SIGTERM receivers before logging readiness on
supported Unix hosts. It transitions the engine to `Draining` before dropping
the listener and signaling each tracked HTTP/1 connection. HTTP draining and
core shutdown start together. A connection still active at the HTTP grace
deadline is aborted and joined before server shutdown returns; core cleanup may
continue through its separately documented forced-cleanup grace. Broadcast
remains sequential and non-atomic across shard files: cancellation preserves
the committed prefix and prevents later shards from starting.
