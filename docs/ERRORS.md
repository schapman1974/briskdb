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
| <a id="limit-exceeded"></a>`LimitExceeded` | `limit_exceeded` | 422 | Limit exceeded | The request exceeds an engine limit. | `54000` | 1105 | `HY000` | No |
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
guess that a later attempt will recover.

Bounded-pool admission uses the existing `Busy` kind; it does not add a separate
overload error. An operation receives `Busy` when its target shard has all
configured connection slots active and its per-shard admission queue is full.
The HTTP response is therefore the fixed 503 problem detail above. Admission
accounting is per shard, so one shard returning `Busy` does not by itself imply
that another shard is saturated. Routed single-shard requests consume only
their selected pool; broadcast is the intentional exception and reserves one
slot in every pool. Clients that retry should use the same bounded exponential
backoff and jitter as for SQLite-originated `Busy` failures.

If a caller drops a future while its operation is queued, the engine skips the
operation and there is no error response to serialize. Once blocking SQLite
execution has started, dropping the future does not cancel the operation; it may
still commit. Issue #11 defines future in-flight cancellation and deadline
behavior. Queue depth, pool internals, SQL text, and connection-cleanup details
remain diagnostic data and must not be added to the fixed public problem detail.

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

The core contains no HTTP, PostgreSQL, or MySQL response types. Conversely,
protocol adapters do not inspect SQLite errors. This lets every frontend share
one error identity while retaining its own response encoding.

Adding the taxonomy changes error reporting only. It does not change the
manifest schema, shard files, stored values, routing, configuration, or any
other on-disk format.

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
