# HTTP API version 1

Status: implemented for issue #50. BriskDB remains an alpha database; HTTP is
loopback-only and has no data/admin authorization. The `/admin` browser login
does not authenticate `/v1` requests.

## Version and compatibility

`GET /v1` (also `/v1/`) describes the transport contract:

```json
{
  "api_version": "1",
  "value_encoding": "legacy-json-v1",
  "session_scope": "request",
  "sql_dialect": "sqlite",
  "max_request_bytes": 2097152
}
```

Every response within `/v1`, including engine errors, decoding failures,
unknown endpoints, and unsupported methods, carries `BriskDB-API-Version: 1`.
The URL selects the API version; a client header does not select another
version. Unknown versions have no routes and return HTTP 404. Discovery is
static contract metadata, not a readiness check; use `/v1/health` for engine
health.

The HTTP API major version is independent of the package version and manifest
format. Within v1, existing request fields, successful response fields, known
error mappings, and the default value encoding keep their documented meaning.
Compatible additions may introduce optional request fields, new endpoints,
response fields, or new error codes. Clients must tolerate unknown response
fields and handle unknown error codes by status. A breaking transport change
requires a new API major version and a documented migration; it must not
silently reinterpret a v1 request. SQL support and storage compatibility still
follow their separately documented alpha contracts.

`legacy-json-v1` names the existing cell conversion, including its losses. It
does not claim lossless binary, decimal, or timestamp round trips. Issue #51
will define an explicitly selected lossless encoding or a new API major
version; it must preserve this default. A `value_encoding` request field is
not accepted yet.

## Routes

| Method and path | Success |
| --- | --- |
| `GET /v1`, `GET /v1/` | Version discovery above |
| `GET /v1/health` | Same engine and global-index health report as `/health` |
| `POST /v1/execute` | One routed write result |
| `POST /v1/query` | Ordered result columns and positional rows |
| `POST /v1/admin/broadcast` | Journaled application-schema migration result |
| `GET /v1/admin/global-indexes` | [Global-index operational report](GLOBAL_INDEX_RELEASE_GATE.md) |

GET routes also accept HEAD, returning headers without a body. Success is HTTP
200 with `application/json`. The unversioned `/health` and `/metrics` routes
remain operator conveniences. The browser's `/admin/api/*` contract is
separate and described in [ADMIN_BROWSER.md](ADMIN_BROWSER.md).

## SQL requests and session lifetime

Both SQL endpoints accept this exact envelope:

```json
{
  "sql": "SELECT id, name FROM widgets WHERE id = ?1",
  "params": ["widget-1"],
  "shard_key": "widget-1"
}
```

- `sql` is a required string. Use SQLite syntax and positional parameters;
  values are bound through the engine and never interpolated.
- `params` is an optional array, defaulting to `[]`; explicit `null` is invalid.
- `shard_key` is an optional string; omission or `null` means no explicit key.
- Unknown and duplicate envelope fields, missing required fields, and wrong
  field types are rejected before any engine operation.
- Supply `Content-Type: application/json`, optionally with a charset. JSON
  media types with a `+json` suffix are also accepted. The request body limit
  is 2,097,152 bytes, including whitespace. JSON syntax, numeric-range, and
  nesting validation occur before execution.

Each SQL or migration request creates a fresh core `Session` and invokes the
shared asynchronous `Engine` with typed `Statement`/`Value` inputs. Routing
state does not survive the response. There are no HTTP transaction handles,
prepared-statement handles, or selectable logical databases in this contract;
requests operate on the default logical database. Sending fields such as
`database`, `transaction`, or `dialect` fails instead of ignoring them.

The engine retains its existing catalog-dependent behavior:

- An empty catalog uses the legacy raw SQLite path and requires an explicit
  routing key. Execute still rejects schema and transaction-control bypasses.
- With registered tables, execute uses authoritative SQL/bound-value routing
  and checks explicit-key conflicts. Query selects targets from catalog
  metadata and SQL predicates; its legacy `shard_key` field is ignored.
- Registered reads can visit multiple shards only for the documented supported
  read shapes. Global tables are read once. Unroutable or cross-shard writes
  are rejected before mutation, apart from explicitly supported generated-key
  operations.

The adapter does not parse SQL, hash keys, open files, or implement transaction
or migration coordination. See [SQL compatibility](SQL_COMPATIBILITY.md),
[generated keys](GENERATED_KEYS.md), and [request controls](REQUEST_CONTROLS.md).
Engine deadlines, cancellation, admission, and result limits still apply.
This API buffers bounded results; it does not expose an HTTP row stream or a
multi-request cancellation endpoint.

## Successful responses

Execute returns the committing operation's shard and affected-row count:

```json
{"shard": 2, "rows_affected": 1}
```

When generated-key execution is enabled and the engine returns a key, it also
includes `generated_key`, for example
`{"column":"id","data_type":"int64","value":"9007199254740993"}`.
The value is exact decimal text. An absent generated key is omitted, not null.

Query returns one shard or the complete visited-shard array, never both:

```json
{
  "shard": 2,
  "columns": [{"name":"id","data_type":"text"}],
  "rows": [["widget-1"]]
}
```

For multiple shards, `"shards":[0,1]` replaces `shard`. Column order and
duplicate names are preserved; every row is positional. Zero-row results
retain their columns. Types use the names in
[the SQL value contract](SQL_COMPATIBILITY.md#current-http-parameter-and-result-conversion).
No partial rows are returned when execution or a combined result budget fails.
Scatter reads do not establish a cross-file atomic snapshot.

`legacy-json-v1` converts input arrays/objects to compact JSON **text**, not
binary values or document commands. Results encode blobs as byte arrays,
decimals as strings, integers as JSON numbers, invalid UTF-8 text lossily, and
non-finite floats as null. JavaScript can round integers outside its safe
range; returning such a number does not claim JavaScript-safe precision. The
browser's tagged large-integer representation is a different contract.

Broadcast accepts only `{"sql":"CREATE TABLE ..."}` and returns
`{"completed_shards":[0,1]}`. It is the existing journaled schema migration
operation, with preflight and resumable application across every shard. It
does not create catalog metadata implicitly or accept bound parameters.

## Errors

Errors within v1 have `Content-Type: application/problem+json`, the version
header, and exactly the currently defined fields `type`, `title`, `status`,
`detail`, and `code`. Text is fixed and redacted; request bodies, unknown field
names, query text, filesystem paths, and decoder diagnostics are not echoed.

| Failure | HTTP | Code |
| --- | ---: | --- |
| Malformed JSON or invalid request envelope | 400 | `invalid_argument` |
| Unknown v1 endpoint | 404 | `not_found` |
| Unsupported method on a known endpoint | 405 | `method_not_allowed` |
| Body exceeds 2 MiB | 413 | `request_too_large` |
| Missing or non-JSON content type | 415 | `unsupported_media_type` |
| Engine rejection | Per [error taxonomy](ERRORS.md) | Exact engine code |

Method errors retain the `Allow` header. The four transport-only codes use
`urn:briskdb:http:v1:` problem types with hyphenated code names; invalid
arguments use the same problem type as engine `InvalidArgument`. Only the
engine `busy` code advertises retryability. A write may commit before its
response reaches the client; v1 supplies no idempotency key and makes no
exactly-once delivery guarantee.

## Migration from the experimental endpoints

Existing route names, valid request bodies, successful result shapes, and cell
encodings continue to work. Unknown fields were previously ignored and now
return 400. Malformed-body responses previously used framework-specific text
and statuses; they now use the fixed problems above. Clients must remove
unused envelope fields and consume `code` instead of decoder text. No storage
format, Rust engine behavior, or startup configuration changes are involved.

`tests/http_v1.rs` exercises discovery, schema rejection before mutation, body
limits, routing errors, method headers, version isolation, and session isolation.
Existing HTTP tests cover catalog routing, generated keys, concurrent requests,
deadlines, result limits, and exact response shapes; embedded differential tests
verify shared-engine outcomes.
