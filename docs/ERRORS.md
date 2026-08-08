# Error contract

BriskDB classifies failures once, at the protocol-neutral engine boundary. An
`EngineErrorKind` is stable error identity; protocol adapters translate that
identity without inspecting an error message. The HTTP adapter uses the mapping
today. The PostgreSQL and MySQL columns below are contracts for their planned
adapters, not a claim that either wire-protocol listener is implemented.

## Taxonomy and protocol mappings

The stable code is suitable for programmatic comparisons. Clients must not use
the human-readable text to identify an error.

| Engine kind | Stable code | HTTP | HTTP title | Fixed public detail | PostgreSQL SQLSTATE | MySQL error | MySQL SQLSTATE | Retryable |
| --- | --- | ---: | --- | --- | --- | ---: | --- | --- |
| <a id="invalid-argument"></a>`InvalidArgument` | `invalid_argument` | 400 | Invalid argument | The request contains an invalid argument. | `22023` | 1210 | `HY000` | No |
| <a id="numeric-out-of-range"></a>`NumericOutOfRange` | `numeric_out_of_range` | 422 | Numeric value out of range | A numeric value is outside the supported range. | `22003` | 1690 | `22003` | No |
| <a id="invalid-text-encoding"></a>`InvalidTextEncoding` | `invalid_text_encoding` | 422 | Invalid text encoding | A text value has an unsupported encoding. | `22021` | 1366 | `HY000` | No |
| <a id="invalid-query"></a>`InvalidQuery` | `invalid_query` | 422 | Invalid query | The query could not be processed. | `42000` | 1105 | `HY000` | No |
| <a id="unsupported"></a>`Unsupported` | `unsupported` | 501 | Unsupported operation | The requested operation is not supported. | `0A000` | 1235 | `42000` | No |
| <a id="failed-precondition"></a>`FailedPrecondition` | `failed_precondition` | 409 | Failed precondition | The operation cannot run in the current state. | `55000` | 1105 | `HY000` | No |
| <a id="type-mismatch"></a>`TypeMismatch` | `type_mismatch` | 422 | Type mismatch | A value has an incompatible type. | `42804` | 1366 | `HY000` | No |
| <a id="constraint-violation"></a>`ConstraintViolation` | `constraint_violation` | 409 | Constraint violation | A database constraint was violated. | `23000` | 1105 | `HY000` | No |
| <a id="unique-violation"></a>`UniqueViolation` | `unique_violation` | 409 | Unique constraint violation | A unique constraint was violated. | `23505` | 1062 | `23000` | No |
| <a id="not-null-violation"></a>`NotNullViolation` | `not_null_violation` | 409 | Not-null constraint violation | A not-null constraint was violated. | `23502` | 1048 | `23000` | No |
| <a id="foreign-key-violation"></a>`ForeignKeyViolation` | `foreign_key_violation` | 409 | Foreign-key constraint violation | A foreign-key constraint was violated. | `23503` | 1105 | `HY000` | No |
| <a id="check-violation"></a>`CheckViolation` | `check_violation` | 409 | Check constraint violation | A check constraint was violated. | `23514` | 3819 | `HY000` | No |
| <a id="permission-denied"></a>`PermissionDenied` | `permission_denied` | 403 | Permission denied | The operation is not permitted. | `42501` | 1227 | `42000` | No |
| <a id="read-only"></a>`ReadOnly` | `read_only` | 403 | Read-only storage | The storage is read-only. | `25006` | 1290 | `HY000` | No |
| <a id="busy"></a>`Busy` | `busy` | 503 | Database busy | The database is busy; retry the operation later. | `55P03` | 1205 | `HY000` | Yes |
| <a id="cancelled"></a>`Cancelled` | `cancelled` | 500 | Request cancelled | The operation was cancelled. | `57014` | 1317 | `70100` | No |
| <a id="deadline-exceeded"></a>`DeadlineExceeded` | `deadline_exceeded` | 504 | Request deadline exceeded | The operation exceeded its request deadline. | `57014` | 3024 | `HY000` | No |
| <a id="limit-exceeded"></a>`LimitExceeded` | `limit_exceeded` | 422 | Limit exceeded | The request exceeds an engine limit. | `54000` | 1105 | `HY000` | No |
| <a id="shutting-down"></a>`ShuttingDown` | `shutting_down` | 503 | Server shutting down | The server is shutting down and cannot accept the operation. | `57P01` | 1053 | `08S01` | No |
| <a id="storage-full"></a>`StorageFull` | `storage_full` | 507 | Storage full | The storage has no available space. | `53100` | 1114 | `HY000` | No |
| <a id="out-of-memory"></a>`OutOfMemory` | `out_of_memory` | 503 | Out of memory | The engine does not have enough memory. | `53200` | 1037 | `HY001` | No |
| <a id="storage-unavailable"></a>`StorageUnavailable` | `storage_unavailable` | 503 | Storage unavailable | The storage is unavailable. | `58030` | 1105 | `HY000` | No |
| <a id="data-corruption"></a>`DataCorruption` | `data_corruption` | 500 | Data corruption | Stored data failed an integrity check. | `XX001` | 1105 | `HY000` | No |
| <a id="internal"></a>`Internal` | `internal` | 500 | Internal error | An internal engine error occurred. | `XX000` | 1105 | `HY000` | No |

Only `Busy` is retryable. A caller should retry it with bounded exponential
backoff and jitter. No other HTTP status, SQLSTATE, or MySQL error number
implies retryability in BriskDB; notably, `OutOfMemory` and
`StorageUnavailable` also use HTTP 503 but are not retryable under this
contract. Storage-open and I/O failures can be permanent, so BriskDB does not
guess that a later attempt will recover. `ShuttingDown` is also conservative:
retrying the same draining process cannot make progress, although a caller may
choose to reconnect to another server or wait for a replacement to start.

Bounded-pool admission uses the existing `Busy` kind; it does not add a separate
overload error. An operation receives `Busy` when its target shard has all
configured connection slots active and its per-shard admission queue is full.
The HTTP response is therefore the fixed 503 problem detail above. Admission
accounting is per shard, so one shard returning `Busy` does not by itself imply
that another shard is saturated. Routed single-shard requests consume only
their selected pool. Schema migration instead uses an in-process gate shared by
handles for the same canonical root, plus fresh coordinator-owned connections.
While that gate is `Migrating`, new
ordinary operations and another migration coordinator receive retryable
`Busy`. Clients that retry should use the same bounded exponential backoff and
jitter as for SQLite-originated `Busy` failures.

If a failed, cancelled, or dropped migration has already published a durable
journal row, the gate becomes `Pending`. Ordinary operations then receive
non-retryable `FailedPrecondition` (HTTP 409), because retrying arbitrary data
work cannot repair a mixed-generation prefix. A migration call may resume the
byte-identical SQL; a different migration is a failed precondition while the
active row remains. Startup automatically attempts the recorded SQL before it
returns an engine. `Cancelled` and `DeadlineExceeded` themselves remain
non-retryable classifications even though an operator may deliberately submit
the exact migration again.

If an integrity check fails, the gate for every in-process handle sharing that
canonical root becomes sticky `Degraded`. Ordinary execute/query work, status,
and schema migration then return non-retryable `DataCorruption` (HTTP 500); an
outstanding migration guard cannot restore admission when it drops. There is no
public repair or rebaseline operation, and startup never clears a persisted
`Degraded` marker from the same manifest. Service can return only after an
operator restores the complete manifest-and-shard set from one consistent,
known-good copy.

An explicit cancellation is `Cancelled`; expiration of an absolute request
deadline is `DeadlineExceeded`, even though PostgreSQL represents both with
SQLSTATE `57014`. MySQL distinguishes them with errors 1317 and 3024. If a
caller drops an operation future, there is no client left to receive either
error. Requests rejected after graceful shutdown begins use `ShuttingDown`.
Queue depth, pool internals, SQL text, deadline values, and connection-cleanup
details remain diagnostic data and must not be added to the fixed public
problem detail. This includes migration SQL retained in the manifest, which may
contain sensitive literals.

MySQL has separate foreign-key errors for the parent and child directions.
`ForeignKeyViolation` does not retain that direction, so BriskDB deliberately
uses the general 1105/`HY000` mapping instead of guessing a more specific MySQL
error number.

The MySQL table targets the MySQL 8.0 server-error catalog at version 8.0.16 or
newer; 8.0.16 is the first release in that line with error 3819 for enforced
check constraints. Exact supported client and server versions remain a wire
adapter decision.

## HTTP problem details

Engine failures returned by HTTP use
[RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html) and the
`application/problem+json` media type. The response has `type`, `title`,
`status`, `detail`, and the BriskDB extension member `code`. `type`, `title`,
`status`, `detail`, and `code` are selected from the adapter's fixed table for
the error kind. In particular, `detail` is safe, fixed text rather than the
underlying SQLite message, SQL text, filesystem path, or error source.

Each `type` is this document's URL followed by the stable code with underscores
changed to hyphens. For example, `invalid_argument` uses
`https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-argument`.
The anchors in the mapping table are therefore both identifiers and
human-readable documentation for the problem types.

`EngineError` display text and its source chain are diagnostic data for logs
and tests. An adapter must never serialize either one to a client. This keeps
the public response stable while preserving the original cause for operators.
Malformed JSON can still be rejected by the HTTP framework before a request
reaches the engine; that decoding rejection is outside this taxonomy.

## Classification boundary

The SQL and storage layers classify SQLite failures from primary and extended
[SQLite result codes](https://www.sqlite.org/rescode.html), plus the operation
context where SQLite uses one broad code. They never parse SQLite's
human-readable message to distinguish error kinds. Constraint subtypes use
extended result codes; an unrecognized constraint remains
`ConstraintViolation` rather than being guessed from text.

The SQL parser facade classifies tokenization and syntax failures as
`InvalidQuery`. Exceeding its configured SQL-input, statement-count, or
recursion limit is `LimitExceeded`. The upstream parser message, source
location, and submitted SQL remain trusted diagnostics only. Protocol adapters
serialize the fixed mapping for the BriskDB error kind and never expose those
diagnostics to a client.

The opt-in common-subset validator classifies a parsed top-level statement or
nested form outside its documented structural contract as `Unsupported`. Its
internal diagnostic contains the one-based statement position and a fixed
feature category, never the submitted SQL, a literal, formatted AST output, or
a parser diagnostic. Its independent recursive expression-depth limit is 128;
exceeding that limit is `LimitExceeded`. Empty and mixed batches can pass
structural validation; the separate request-level classifier decides whether
they pass batch policy. See the [common SQL subset
contract](SQL_SUBSET.md).

The statement/batch classifier reports an empty or comment-only validated
batch as `InvalidArgument`. A batch of two or more statements containing any
non-read behavior is `Unsupported`; its diagnostic identifies only the first
such one-based statement ordinal and a fixed coarse behavior category. A
validated AST family without a classifier mapping is an `Internal` invariant
failure. Submitted SQL, identifiers, literals, nested behavior details, and AST
output are never included. See the [statement and batch classification
contract](SQL_STATEMENT_CLASSIFICATION.md).

The opt-in placeholder normalizer classifies a zero positional index or marker
spelling incompatible with the selected PostgreSQL or MySQL dialect as
`InvalidQuery`. A named SQLite parameter is deliberately `Unsupported`.
Assigning an index greater than `MAX_SQL_PARAMETERS` (32,766) within one
statement is `LimitExceeded`; numbering restarts at each statement. A mismatch
between a retained placeholder span and the retained exact source violates an
internal SQL-layer invariant and is `Internal`. Normalization diagnostics use
fixed categories and positions and never contain SQL, literal or marker text,
bound values, formatted AST output, or source locations. See the [SQL
parameter-normalization contract](SQL_PARAMETERS.md).

The opt-in SQL translator reports `InvalidArgument` when strict SQLite mode is
requested for PostgreSQL or MySQL input. Compatibility mode reports
`Unsupported` for a parsed type outside its finite mapping and `InvalidQuery`
when canonical rendering would contain a NUL byte. A mismatch between retained
statement, column, or placeholder metadata is `Internal`. Translation
diagnostics identify only the trusted dialect and one-based statement or
column position where useful; they never contain SQL, identifier or type
spelling, literal text, parameters, or formatted AST output. See the [SQL
translation contract](SQL_TRANSLATION.md).

The opt-in shard-key inference layer reports an out-of-range statement index,
wrong bound-value count, or missing logical database as `InvalidArgument`; an
unknown table in the selected database as `InvalidQuery`; an incompatible key
value as `TypeMismatch`; a signed-integer overflow as `NumericOutOfRange`; an
invalid UTF-8 text key as `InvalidTextEncoding`; and a null inserted shard key
as `NotNullViolation`. Inference diagnostics may identify the one-based
statement position and a fixed category, but never contain SQL, identifier
spelling, literal or bound values, key contents, formatted AST output, or source
locations. See the [shard-key inference contract](SQL_SHARD_KEYS.md).

The synchronous bound statement planner preserves those inference error kinds
and diagnostics. Before inference it also acquires ordinary schema-operation
admission, so a migrating gate returns `Busy`, a pending migration returns
`FailedPrecondition`, and a degraded gate returns `DataCorruption`. It then
applies complete batch policy: empty is `InvalidArgument`, a mutating
multi-statement batch is `Unsupported`, and an out-of-range selected index in
an accepted batch is `InvalidArgument`. Routing policy reports an explicit physical-shard conflict, or a sharded
`UPDATE`/`DELETE` missing both finite inference and explicit fallback, as
`InvalidArgument`. A shard-key `UPDATE`, an `INSERT` without a proven key for
every row, or a finite write spanning physical shards is `InvalidQuery`.
Retained metadata inconsistency is `Internal`. Policy diagnostics contain no
SQL, identifier spelling, parameter value, or routing-key bytes. Planning adds
no new error kind, retains no failed parameters, and does not change protocol
mappings. See the [bound statement-planning and routing-policy
contract](SQL_PLANNING.md).

The prepared lifecycle preserves each earlier frontend/planner kind. Prepare
adds `InvalidArgument` for an unknown logical database and for anything other
than exactly one top-level statement, `LimitExceeded` for a full session
statement cache, `PermissionDenied` when ordinary shard policy denies a
persistent schema statement, and `InvalidQuery` when SQLite otherwise cannot
transiently compile the translated SQL on shard 0. Bind preserves parameter-count, value-conversion,
inference, and planning errors, and adds `LimitExceeded` for a full portal cache
or retained-value byte budget, or when the captured route and repeated
normalized occurrences exceed the conservative per-bind planning ceiling
before allocation. Describe preserves transient SQLite compilation errors. A
normalized-versus-SQLite parameter-count disagreement or retained accounting
inconsistency is `Internal`.

Prepared-statement and portal handles are session-scoped. A handle from another
session/engine, a closed session, or an absent statement/portal is
`FailedPrecondition`; closing an already absent same-session handle instead
returns `false`. Portal execution reports `PermissionDenied` for catalog
placement and `Unsupported` for a sharded read that still needs scatter or
another unimplemented target, including session behavior. Classified safe
`NotApplicable` and `Global` reads use deterministic shard 0; accepted sharded
work uses its current assigned shard. A disagreement between retained behavior
and SQLite execution metadata is `Internal`. Cancellation, deadlines, pool
admission, schema-gate state, SQLite execution, constraints, and result limits
retain their existing kinds.

A failed prepare or bind publishes no handle, full caches evict nothing, and an
execution failure retains its portal. Protocol adapters still serialize only
the fixed public mapping for the error kind. Redacted `Debug` applies to the
prepare request, cached state, portal, plan, and description; trusted internal
value-conversion diagnostics may contain the rejected value, and
`PreparedExecution` `Debug` intentionally contains user-visible results. See
the complete [prepared statements and bound portals
contract](SQL_PREPARED_STATEMENTS.md).

The core contains no HTTP, PostgreSQL, or MySQL response types. Conversely,
protocol adapters do not inspect SQLite errors. This lets every frontend share
one error identity while retaining its own response encoding.

Manifest compatibility uses the same protocol-neutral kinds. A foreign file,
a manifest newer than the running binary, or a requested shard-count mismatch
is `FailedPrecondition`; the file is not downgraded. A recognized BriskDB
manifest whose identity, schema, or invariant rows disagree is
`DataCorruption`.

Manifest v6 applies the same boundary to its retained migration journal. A
malformed digest, SQL limit violation, noncontiguous generation history,
inconsistent state/progress, wrong stored shard count, multiple or misplaced
active rows, or an active row that disagrees with the catalog is
`DataCorruption`. A recognized shard below the active source generation or in
an invalid source/target position is also corruption. A shard or manifest newer
than the coordinator can safely interpret is `FailedPrecondition`. Lock
contention during preflight, apply, progress, or startup recovery remains
retryable `Busy`.

Manifest v7 adds the same classification to integrity state. An unsupported
manifest-root or shard-schema digest encoding version is
`FailedPrecondition`; this binary will not reinterpret or rewrite it. A
recognized encoding with a semantic-root mismatch, malformed checksum blob,
invalid durable-state/checksum/journal combination, failed SQLite manifest or
shard-metadata integrity check, inconsistent shard-schema consensus, or source
or target fingerprint in the wrong migration-prefix position is
`DataCorruption`. BriskDB persists `Degraded` only when it can first validate
the existing manifest root, and that emergency write is best-effort because
lock, disk, or process failure can prevent it. The in-process gate remains
sticky even if persistence fails; a restart is never a supported repair. An
altered manifest payload is never blessed while handling its mismatch.

For submitted migration input, an empty batch, a batch over 65,536 UTF-8 bytes,
or a NUL byte is `InvalidArgument`. SQLite syntax or statement-shape failures
retain the normal SQL classification, and attempts to reach the reserved
BriskDB schema or storage controls are `PermissionDenied`. The public problem
detail never includes the retained SQL.

Shard-layout validation follows the same distinction. A foreign shard
application ID, unexpected canonical four-digit `.sqlite` shard file or
symbolic link, persistent journal mode other than WAL, or shard generation
newer than the catalog and outside an authorized active migration prefix is
`FailedPrecondition`; BriskDB neither claims, downgrades, nor repairs it. A
layout in `Adopting` or `Ready` with a missing shard is `DataCorruption`. In
`Ready`, missing identity metadata, a wrong layout or physical-shard ID, an
older mismatched generation, or otherwise invalid recognized BriskDB metadata
is also `DataCorruption`. SQLite lock contention while an opener waits for a
manifest or shard transaction remains retryable `Busy`; permission, read-only,
full, and I/O failures retain their precise storage kinds. Client access to
BriskDB-owned metadata or mutation of a storage-control PRAGMA is
`PermissionDenied`. These diagnostics originate in storage and are never
serialized directly by an adapter.

The earlier taxonomy change affected reporting only. Manifest v5 later added
identity metadata to shard files, manifest v6 added retained crash-resumable
application-schema history, and manifest v7 added semantic and schema
fingerprints plus explicit integrity state. Their format upgrades preserve
legacy application tables, rows, routing, SQL results, and wire configuration.

This is a pre-1.0 Rust API migration: public `Database` operations now return
`EngineResult<T>` instead of `anyhow::Result<T>`. The `?` operator still
converts `EngineError` into `anyhow::Error`, but callers with explicit result
types, direct returns, or function-pointer signatures must update them.

## Authoritative references

- [RFC 9457: Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc9457.html)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110.html#name-status-codes)
- [RFC 4918: HTTP 507 Insufficient Storage](https://www.rfc-editor.org/rfc/rfc4918.html#section-11.5)
- [PostgreSQL error codes](https://www.postgresql.org/docs/current/errcodes-appendix.html)
- [MySQL server error reference](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html)
- [MySQL error message elements](https://dev.mysql.com/doc/refman/8.4/en/error-message-elements.html)
- [SQLite result and extended result codes](https://www.sqlite.org/rescode.html)
