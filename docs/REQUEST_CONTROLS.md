# Request controls and shutdown

BriskDB applies cancellation, deadlines, result budgets, and shutdown at the
protocol-neutral `Engine` boundary. HTTP, PostgreSQL sessions, and a future
MySQL adapter therefore share the same resource and cleanup semantics. Current
PostgreSQL assigns every command a fresh request context. Its advertised backend
key lets a driver's separate `CancelRequest` connection cancel that command
through the same core boundary.

## Per-request context

The existing `Engine::execute`, `execute_write`, `query`, `broadcast`, and
`status` methods, the crate-private explicit-shard inspection operation, plus
`prepare_statement`, `bind_statement`, `describe_prepared`, `execute_portal`,
and their logical read counterparts use a default `RequestContext`. `broadcast`
now means a journaled application-schema migration. Frontends that have their
own cancellation or deadline source can call the corresponding
`*_with_context` method. `execute` and `execute_with_context` deliberately
project the complete `WriteResult` down to an affected-row count; callers that
need the generated-key result shape use `execute_write` or
`execute_write_with_context`:

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

The PostgreSQL adapter keeps an in-memory registry of random backend PID/secret
pairs. It installs the current request token only while a command is active,
requires both values to match, and silently ignores unknown, stale, or inactive
keys. Closing a PostgreSQL connection unregisters its key and cancels active
work. A cancelled command returns SQLSTATE `57014`; an explicit transaction
enters failed state until rollback.

The Engine also keeps a bounded registry for active HTTP queries. A query gets
an opaque 32-character lowercase hexadecimal handle before it enters engine
admission. Normal completion removes that handle when the handler finishes.
Dropping the handler unregisters it immediately while the Engine's independent
operation drop guard cancels SQLite and retains lifecycle and pool ownership
until cleanup actually finishes. `GET /v1/admin/queries` returns a bounded,
redaction-safe snapshot that never includes SQL, SQL digests, parameter values,
routing keys, sessions, or paths.
`POST /v1/admin/queries/{operation_id}/cancel` requests cancellation for that
exact live handle and returns HTTP 202. An unknown,
malformed, completed, or stale handle returns the fixed versioned 404 problem
and cannot affect another query. The response's `newly_requested` Boolean
distinguishes the call that changed the sticky token from a repeated request
while cleanup is still in progress.

Cancel takes its identity only from the path and requires a zero-byte body. A
nonempty body receives the fixed versioned HTTP 400 problem, or HTTP 413 when
it exceeds the 2 MiB limit, before the registry can be changed. An empty body
does not require a content type.

The registry is shared by every Engine clone, including the clones the server
passes to its separate data and administration routers. An embedder building
both planes must likewise clone one Engine into `data_router_with_engine` and
`admin_router_with_engine`; calling the two `Arc<Database>` compatibility
wrappers separately constructs independent Engines and registries. The registry
is process memory, has no manifest or restart state, and contains active
operations only. Listing and cancellation are inherently live observations: a
listed query can complete before the cancel request arrives. Completion known
to have succeeded still wins its close race with cancellation. Closing a data
connection or dropping its HTTP handler unregisters the HTTP handle, requests
exact-operation cancellation, and leaves the engine lease in place until the
leased SQLite work and pool cleanup have really stopped.

The registry holds at most 1,024 queries. When it is full, another HTTP query
returns the standard HTTP 422 `limit_exceeded` problem before Engine admission.
When a handler drops its tracking guard, removing the exact entry immediately
restores one registry slot even when the independently leased SQLite cleanup is
still finishing.

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
Portal execution can wait for its selected shard or logical target set and runs
with the same exact-handle interruption and cleanup as raw execution.
Cancellation before a prepared object is published leaves the session cache
unchanged; a cancelled or failed execution retains the existing portal.

A native omitted-key portal has no selected shard at bind time. After its
bounded worker starts, it never waits for candidate capacity: it immediately
tries one owner from the table's rotating candidate list, skips `Busy`, and
releases an exhausted unmutated candidate before fallback. Hi/lo takes the
opposite safe ordering and waits for all possible target capacities before its
worker consumes an irrevocable allocation. Both remain under the same deadline,
cancellation, and cleanup contract.

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

## HTTP request identity and idempotent writes

The HTTP adapter generates a fresh nonzero 128-bit request ID when one is
omitted, or validates and echoes one canonical value supplied by the caller. It
returns the value as 32 lowercase hexadecimal characters in
`BriskDB-Request-ID`. Generation normally uses operating-system randomness and
falls back to a process-local counter if that source fails, so the value is
correlation data rather than a global uniqueness or security guarantee.
Malformed or duplicate values fail before Engine admission and receive a new
server-generated ID on the error. Request IDs have no Engine or storage state:
reusing one does not deduplicate, cancel, authorize, or otherwise couple two
operations.

`BriskDB-Idempotency-Key` is a separate opt-in control for the narrow write
class the Engine can receipt atomically. `Engine::execute_idempotent_write`
accepts an `IdempotencyKey` only for one planner-proven exact-shard direct
autocommit DML statement over a registered table without generated-key or
global-index coordination. Unsupported shapes fail before mutation. The
ordinary execute APIs and an HTTP execute without the header retain their
existing at-least-once delivery semantics.

Key ownership covers the database root in the current unauthenticated,
service-wide namespace. It does not vary by listener, connection, request ID,
or source address. One of 256 fixed advisory lock stripes
serializes the key across processes while the Engine reserves ordinary shard
and worker admission and checks the receipt on every shard. A duplicate exact
semantic digest replays the retained `WriteResult`; a duplicate key with a
different digest returns `IdempotencyConflict`. A new operation executes the
DML and inserts its receipt in the same target-shard SQLite transaction. Crash
or disconnect before that commit retains neither; after commit it retains both,
so the next exact request can return the known result without rerunning SQL.
Cancellation observed only after that known commit cannot turn the success
into an unknown failure.

The semantic digest is a versioned core encoding of the API operation, exact
SQL bytes, typed parameter values and floating-point bits, explicit routing
input, default logical database, table, and resolved shard. It does not depend
on HTTP JSON whitespace/member order, the selected value representation,
request or query IDs, translated SQL, or current schema generation. Storage
keeps only the key and request digests plus the target, row count, format, and
retention timestamps; raw keys and request content are never persisted.

Each target shard retains at most 4,096 unexpired receipts for a fixed 24-hour
server-wall-clock window. No unexpired receipt is evicted. The target
transaction removes an expired same-key row plus at most 64 additional expired
rows before checking capacity; a full unexpired set rejects the write before
DML. Lock acquisition is nonblocking, so contention is the ordinary retryable
`Busy` result rather than an unbounded wait. The hidden table and lock-file
format are specified in [the storage contract](STORAGE_FORMAT.md).

## Query result budgets

Every query has a finite row and logical-byte budget. Defaults are 10,000 rows
and 16 MiB; the configurable hard caps are 1,000,000 rows and 1 GiB. A request
context may narrow but never widen its engine's configured budget. Equality at
the limit succeeds. Exceeding either limit returns `LimitExceeded`. Materialized
Engine APIs return no partial `ResultSet`; a streaming frontend may already
have delivered the bounded prefix that preceded a later limit failure.

HTTP query requests may supply a strict `result_limits` object containing one
or both positive integer members `max_rows` and `max_logical_bytes`. The
adapter creates the query's `RequestContext` from those values, and the Engine
uses the lower request or configured value independently for each dimension.
The object applies to both `/v1/query` and `/v1/query/stream`; execute and
administration envelopes reject it. The data-plane discovery document reports
the configured ceilings, not a per-request override.

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

The raw query and explicit-shard inspection paths accept only SQLite statements
reported as read-only. The prepared executor likewise rejects row-producing
writes. These rules prevent an early result-budget failure from accompanying a
partially consumed DML `RETURNING` statement. Raw execute, prepared affected-row
results, and schema migration do not materialize a `ResultSet` and are
unaffected by query result budgets. A logical scatter applies one budget to the
combined result, including one result envelope and one set of column metadata;
it does not grant every shard a fresh row or byte allowance.

PostgreSQL reads and HTTP `/v1/query/stream` use the protocol-neutral stream
with a 16-row handoff. SQLite stops stepping when that handoff is full and
resumes only as the client drains rows. Scatter streams visit physical shards
in ascending order to retain the same deterministic concatenation while
holding one shard connection at a time. Dropping or closing a stream cancels
its operation; request cancellation, deadline expiry, and shutdown interrupt
the currently leased SQLite handle and discard already-buffered rows.

The HTTP response owns both its `TrackedQuery` guard and the `RowStream` until
the final record or body drop. A successful NDJSON response ends only with a
completion record. Once HTTP 200 and metadata have been emitted, a deadline,
cancellation, storage, or result-limit failure appears as one redacted terminal
error record after the already delivered prefix. It is not a partial successful
query. EOF without completion is indeterminate. The adapter adds no retained
cursor, idle connection lease, SQL rewrite, or new cross-shard snapshot and
ordering promise.

Logical scatter/gather schedules at most eight shard tasks concurrently. All
children inherit the operation's one absolute deadline and sticky cancellation
source. Cancellation, deadline expiry, a result-budget overflow, inconsistent
column metadata, or any shard error cancels outstanding children and waits for
their cleanup. The caller receives only the error, never rows from the shards
that happened to finish first. Successful results are concatenated in ascending
physical-shard order and keep duplicate rows.

The `/admin` browser on the administration listener independently caps a
requested page at 200 returned rows
and validates offsets no greater than 1,000,000. It may inspect one additional
row to decide whether another page exists; that row is not serialized. The
physical inspections and their merged logical page use the engine's configured
row and logical-byte limits, so a lower limit or byte-heavy page can return
`LimitExceeded` with no partial page. Placement selects the targets: every file
for Sharded tables and shard 0 once for Global tables. At most eight inspections
run concurrently, and only the shard-major slices needed for the page are
materialized. Every offset page is a separate request, not a retained
multi-file snapshot. The continuation flag is false if the next calculated
offset would exceed the browser's 1,000,000 cap.

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

## Browser-session limits

Admin-browser authentication state is protocol-adapter memory, not an engine
`Session` or prepared cache. A successful temporary `admin` / `admin` login
creates one opaque cookie token with an absolute eight-hour lifetime. The HTTP
adapter retains at most 128 live browser sessions. Logout removes the presented
session, expiration makes it unusable, and process restart removes all of them.
A successful login at capacity evicts the earliest-expiring session, using its
token only as a deterministic tie-break; it never grows the store past 128. No
browser-session data is charged to a shard pool, written to the data directory,
or recovered at startup.

Each authenticated discovery or row-page call separately creates a core
`Session`. Discovery uses one engine operation. A row page uses one operation to
verify the exact visible table identity, followed by a separately bounded page
read that repeats the visibility predicate before returning data. The separate
all-shard count uses at most eight concurrent shard tasks. Each task owns a core
session, verifies the ordinary table, and runs one exact `COUNT(*)`; all tasks
share one request deadline and only a checked complete sum is returned. A
completed schema-generation change makes the request retryable instead of
mixing generations. Cancelling, failing, or completing any operation does not
extend the cookie lifetime or invalidate an otherwise valid browser session.
Conversely, logging out prevents later admin requests but does not retroactively
cancel an inspection already admitted to the engine.

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
transitions the engine to `Draining` before dropping the data HTTP listener,
the optional administration HTTP listener, and the optional PostgreSQL listener,
then signals every tracked HTTP/PostgreSQL connection. Connection draining and
core shutdown start together. A connection
still active at the grace deadline is aborted. HTTP task joins are awaited;
PostgreSQL task joins and retained-session closes get one additional grace
interval. If that second interval expires, server return does not await the
remaining PostgreSQL session closes and schedules them as best-effort runtime
cleanup. Completed PostgreSQL startups retain one core session; normal and
completed forced-task cleanup close it, while partial startups own no session.
Core cleanup may continue through its separately documented forced-cleanup
grace. A forced cancellation cannot erase committed
schema-migration progress: the current shard transaction rolls back if still
running, the retained prefix remains resumable, and the next startup finishes
it before serving ordinary work.

Data and administration HTTP connections use the same engine admission,
deadline, result, and drain controls. Listener separation changes which router
can admit a path; it does not add an independent pool, worker budget, deadline,
shutdown grace, or engine lifecycle. See
[HTTP data and administration listeners](HTTP_LISTENERS.md).
