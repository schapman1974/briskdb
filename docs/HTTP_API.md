# HTTP API version 1

Status: implemented for issues #50 through #54. BriskDB remains an alpha
database. Its data and administration HTTP listeners are separate,
loopback-only, and have no complete authorization boundary. The `/admin`
browser login authenticates only its browser endpoints.

## Version and compatibility

`GET /v1` (also `/v1/`) on the data listener describes the transport contract:

```json
{
  "api_version": "1",
  "value_encoding": "legacy-json-v1",
  "supported_value_encodings": ["legacy-json-v1", "lossless-json-v1"],
  "session_scope": "request",
  "sql_dialect": "sqlite",
  "max_request_bytes": 2097152,
  "max_result_rows": 10000,
  "max_result_logical_bytes": 16777216,
  "stream_buffer_rows": 16,
  "request_id_header": "BriskDB-Request-ID",
  "idempotency_key_header": "BriskDB-Idempotency-Key",
  "stream_media_type": "application/x-ndjson; charset=utf-8"
}
```

Every response within `/v1`, including engine errors, decoding failures,
unknown endpoints, and unsupported methods, carries `BriskDB-API-Version: 1`.
The URL selects the API version; a client header does not select another
version. Unknown versions have no routes and return HTTP 404. Discovery is
static contract metadata, not a readiness check; use `/v1/ready` on the
administration listener before sending traffic and `/v1/health` for the
broader engine and global-index health report.

The HTTP API major version is independent of the package version and manifest
format. Within v1, existing request fields, successful response fields, known
error mappings, and the default value encoding keep their documented meaning.
Compatible additions may introduce optional request fields, new endpoints,
response fields, or new error codes. Clients must tolerate unknown response
fields and handle unknown error codes by status. A breaking request or
representation change requires a new API major version and a documented
migration; it must not silently reinterpret a v1 request. The issue #52
listener split is a separately documented pre-1.0 deployment change: relative
v1 paths and their representations are preserved, while admin paths use a
different configured base address. SQL support and storage compatibility still
follow their separately documented alpha contracts.

`legacy-json-v1` remains the default and retains the original cell conversion,
including its losses. `lossless-json-v1` is an explicitly selected, tagged
encoding for the complete current protocol-neutral value model. Adding that
choice does not reinterpret an omitted v1 field or change the default response.

## Routes and listeners

The data listener defaults to `127.0.0.1:7654` through `--listen` and
`BRISKDB_LISTEN`:

| Data method and path | Success |
| --- | --- |
| `GET /v1`, `GET /v1/` | Version discovery above |
| `POST /v1/execute` | One routed write result |
| `POST /v1/query` | Ordered result columns and positional rows |
| `POST /v1/query/stream` | Bounded newline-delimited result stream |

The administration listener defaults to `127.0.0.1:7655` through
`--admin-listen` and `BRISKDB_ADMIN_LISTEN`; the exact value `disabled` omits
the entire plane:

| Administration method and path | Success |
| --- | --- |
| `GET /health` | Engine and aggregate global-index health report |
| `GET /metrics` | Prometheus text report |
| `GET /v1/health` | Versioned alias of `/health` |
| `GET /ready` | Unversioned readiness probe |
| `GET /v1/ready` | Versioned readiness probe |
| `POST /v1/admin/broadcast` | Journaled application-schema migration result |
| `GET /v1/admin/catalog` | Relational catalog metadata |
| `GET /v1/admin/migrations` | Current schema generation and migration summary |
| `GET /v1/admin/migrations/{target_generation}` | One exact migration generation |
| `GET /v1/admin/shards` | Validated physical-shard state |
| `GET /v1/admin/queries` | Bounded active-query report |
| `POST /v1/admin/queries/{operation_id}/cancel` | Cancel one exact active query |
| `GET /v1/admin/backup` | Supported stopped-server backup capability |
| `POST /v1/admin/maintenance/checkpoint` | Passive checkpoint of every database |
| `GET /v1/admin/global-indexes` | [Global-index operational report](GLOBAL_INDEX_RELEASE_GATE.md) |
| `/admin`, `/admin/`, assets, and `/admin/api/*` | [Admin data browser](ADMIN_BROWSER.md) |

GET routes also accept HEAD, returning headers without a body. Success is HTTP
200 with `application/json`, except for Prometheus text, browser assets, and a
successful cancellation request, which is HTTP 202. A readiness response uses
HTTP 503 while the engine cannot admit ordinary work but retains the readiness
JSON representation described below.
Each production router omits the other plane's handlers, so sending a route with
ordinary request controls to the wrong listener returns 404 without executing
it. Malformed request-control headers or an idempotency key on an unsupported
route can fail before route dispatch. The complete address, Rust/Python
configuration, startup, and drain contract is in
[HTTP_LISTENERS.md](HTTP_LISTENERS.md).

## Request identity

Every response from either production HTTP plane and from the combined Rust
router carries exactly one `BriskDB-Request-ID` header. This includes
unversioned health, readiness, metrics, browser, and missing-route responses as
well as every response below `/v1`.

A caller may supply the header once with exactly 32 lowercase hexadecimal
characters representing a nonzero 128-bit value. BriskDB echoes a valid value.
When it is absent, BriskDB generates a fresh nonzero server value. It normally
uses operating-system randomness and has a process-local counter fallback if
that source is unavailable; callers must not treat the value as a globally
unique identifier or security capability.
An empty, zero, uppercase, non-hexadecimal, comma-folded, duplicate, or parsed
value that still contains whitespace is rejected with the fixed HTTP 400
`invalid_argument` problem. Standard HTTP optional whitespace next to the
field delimiter is removed by the HTTP parser before this validation. A
rejection carries a newly generated request ID and never echoes the invalid
input.

The value is untrusted correlation data. Reusing it does not deduplicate work,
select an active query, identify a document operation, authenticate a caller,
or act as an idempotency key. Ordinary success bodies and the exact five-field
Problem Details representation do not repeat it.

## SQL requests and session lifetime

All three SQL endpoints accept this base envelope:

```json
{
  "sql": "SELECT id, name FROM widgets WHERE id = ?1",
  "params": ["widget-1"],
  "shard_key": "widget-1",
  "value_encoding": "lossless-json-v1"
}
```

- `sql` is a required string. Use SQLite syntax and positional parameters;
  values are bound through the engine and never interpolated.
- `params` is an optional array, defaulting to `[]`; explicit `null` is invalid.
- `shard_key` is an optional string; omission or `null` means no explicit key.
- `value_encoding` is optional. Omission and the exact string
  `legacy-json-v1` select the established conversion. The exact string
  `lossless-json-v1` selects the tagged conversion described below. Explicit
  `null`, any other type, and an unknown name are invalid.
- Unknown and duplicate envelope fields, missing required fields, and wrong
  field types are rejected before any engine operation.
- Supply `Content-Type: application/json`, optionally with a charset. JSON
  media types with a `+json` suffix are also accepted. The request body limit
  is 2,097,152 bytes, including whitespace. JSON syntax, numeric-range, and
  nesting validation occur before execution.

The two query endpoints also accept an optional strict `result_limits` object:

```json
{
  "sql": "SELECT id, name FROM widgets",
  "result_limits": {"max_rows": 100, "max_logical_bytes": 1048576}
}
```

The object must contain at least one of the positive integer members
`max_rows` and `max_logical_bytes`, and no other or duplicate member. It narrows
the Engine's configured protocol-neutral result budget for this request;
larger values cannot widen that budget. Equality at the effective boundary
succeeds. `/v1/execute` rejects `result_limits` instead of silently ignoring
it.

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

The adapter validates and converts the selected JSON value encoding, but does
not parse SQL, hash keys, open files, or implement transaction or migration
coordination. See [SQL compatibility](SQL_COMPATIBILITY.md),
[generated keys](GENERATED_KEYS.md), and [request controls](REQUEST_CONTROLS.md).
Engine deadlines, cancellation, admission, and result limits still apply.
`/v1/query` buffers a bounded result, while `/v1/query/stream` exposes the same
Engine row stream incrementally. The administration listener can enumerate and
cancel either kind of currently active query, as described below; the handle
does not retain an HTTP transaction or reusable result.

## Successful responses

Execute returns the committing operation's shard and affected-row count:

```json
{"shard": 2, "rows_affected": 1}
```

When generated-key execution is enabled and the engine returns a key, it also
includes `generated_key`, for example
`{"column":"id","data_type":"int64","value":"9007199254740993"}`.
The value is exact decimal text. An absent generated key is omitted, not null.
The execute response does not echo `value_encoding`; its generated-key shape is
already exact and remains unchanged for both encodings.

### Durable idempotent execute

An execute request may supply one `BriskDB-Idempotency-Key` header using the
same exact nonzero 32-lowercase-hex grammar as a request ID. It is a separate
value with different semantics. Malformed or duplicate keys fail with HTTP 400
before Engine admission. Supplying this header on any route other than exact
`POST /v1/execute` fails before that route can act.

The Engine accepts a key only after it proves that the request is one
catalog-routed, exact-target, direct autocommit DML statement. Generated-target
writes, tables with global-index definitions, experimental writable-vtable
coordination, schema or transaction statements, raw empty-catalog execution,
and every administration operation are ineligible and return the fixed
`unsupported` problem before mutation. Omitting the key retains the existing
at-least-once execute behavior.

The first committed keyed result has
`BriskDB-Idempotency-Status: created`. An exact retry within the promised
24-hour receipt window returns the same logical `shard`, `rows_affected`, and
optional generated-key result with
`BriskDB-Idempotency-Status: replayed`, without executing the DML again. Each
attempt still has its own request ID. Reusing a key for a different semantic
operation returns HTTP 409 `idempotency_conflict` without mutation.

Key ownership spans the complete database root and all of its current shards.
A key currently lives in one unauthenticated, service-wide namespace; it is not
scoped by request ID, connection, listener, or source address. Future identity
work must define authorization before lookup rather than silently changing this
v1 meaning.
A fixed cross-process lock stripe serializes one key while the Engine checks
every shard through normal admission, then commits the mutation and its hidden
receipt in the same target-shard SQLite transaction. The receipt binds the
exact SQL bytes, canonical typed parameters, explicit routing input, default
logical database, table, and resolved target. JSON whitespace and member order,
the request ID, schema generation, and representation-only value-encoding
choice are not mutation identity. Receipt storage keeps only digests and
bounded result metadata; it never retains the raw key, SQL, parameters,
routing value, or request ID.

Receipts are retained for 24 hours according to server wall time and are never
evicted while unexpired. A target shard admits at most 4,096 unexpired
receipts; capacity exhaustion rejects a new keyed write before DML, while
cleanup removes expired rows in bounded batches. The guarantee applies only
inside that retained window. Stopped-server backup preserves receipts with the
shard files.

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

### Streaming query response

`POST /v1/query/stream` returns
`Content-Type: application/x-ndjson; charset=utf-8`. Each record is one compact
JSON object followed by LF. The record order is:

1. exactly one metadata record with `"kind":"meta"`, the same `shard` or
   `shards`, the optional nondefault `value_encoding`, and ordered `columns`;
2. zero or more records with `"kind":"row"` and one positional `values`
   array; and
3. exactly one `{"kind":"complete","rows":N}` record on success.

For example:

```text
{"kind":"meta","shard":2,"columns":[{"name":"id","data_type":"text"}]}
{"kind":"row","values":["widget-1"]}
{"kind":"complete","rows":1}
```

The metadata must be read before interpreting rows. The selected legacy or
lossless cell codec is identical to the materialized endpoint. The stream uses
one 16-row Engine handoff, one absolute deadline, and one logical row/byte
budget across all visited shards. Scatter output remains shard-major in
ascending physical-shard order and does not establish a cross-file snapshot or
global SQL ordering.

A route, envelope, value, planning, or preparation failure before the metadata
record is an ordinary HTTP Problem response with its real status. After HTTP
200 and stream metadata are committed, a later Engine failure emits exactly
one flat terminal record with `"kind":"error"` plus the fixed `type`, `title`,
`status`, `detail`, and `code` values, and emits no completion record. This
in-band record is not itself an RFC 9457 HTTP response because the outer status
is already 200. Clients must observe `complete`; EOF without it is
indeterminate. Dropping the response cancels the Engine stream, interrupts its
current SQLite work, and unregisters its active-query handle.

This endpoint is the bounded streaming alternative for Phase 6. It creates no
retained cursor or continuation token, rewrites no SQL, and adds no
`ORDER BY`/`OFFSET`/`LIMIT` semantics. Deterministic global ordering and general
pagination remain in Phase 7 issues #58 and #59.

`legacy-json-v1` converts input arrays/objects to compact JSON **text**, not
binary values or document commands. Results encode blobs as byte arrays,
decimals as strings, integers as JSON numbers, invalid UTF-8 text lossily, and
non-finite floats as null. JavaScript can round integers outside its safe
range; returning such a number does not claim JavaScript-safe precision. The
browser's tagged large-integer representation is a different contract.

### Lossless value encoding

A request that selects `lossless-json-v1` receives the same ordered columns and
positional rows, with the selected encoding named in the response:

```json
{
  "shard": 2,
  "value_encoding": "lossless-json-v1",
  "columns": [
    {"name":"id","data_type":"int64"},
    {"name":"payload","data_type":"binary"}
  ],
  "rows": [[
    {"$briskdb_type":"int64","value":"9007199254740993"},
    {"$briskdb_type":"binary","value":"AP8="}
  ]]
}
```

The response omits `value_encoding` for the default legacy selection, including
when a request names `legacy-json-v1` explicitly. Column metadata describes the
result shape; every lossless cell remains self-describing even when SQLite marks
an expression's column type `unknown`.

The lossless encoding has one canonical JSON form for each current BriskDB
`Value` variant:

| BriskDB value | JSON form |
| --- | --- |
| `Null` | `null` |
| `Boolean` | `true` or `false` |
| `Text` | JSON string |
| `Int64` | `{"$briskdb_type":"int64","value":"-9223372036854775808"}` |
| `UInt64` | `{"$briskdb_type":"uint64","value":"18446744073709551615"}` |
| `Float64` | `{"$briskdb_type":"float64","value":"3ff8000000000000"}` |
| `Decimal` | `{"$briskdb_type":"decimal","value":"12.3400"}` |
| `Binary` | `{"$briskdb_type":"binary","value":"AP8="}` |
| `InvalidText` | `{"$briskdb_type":"invalid_text","value":"ZoA="}` |

Signed and unsigned integer tags are used at every magnitude, including values
inside JavaScript's exact range. Their `value` is the type's canonical base-10
text: no leading plus, no unnecessary leading zero, and no negative zero.
Decimal `value` text matches
`[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?` and deliberately
preserves the caller's valid sign, digits, scale, exponent case, and exponent
sign.

The `float64` `value` is exactly 16 lowercase hexadecimal digits containing the
IEEE-754 binary64 bits in most-significant-byte-first display order. This keeps
finite values, signed zero, infinities, and individual NaN representations
distinct without relying on JSON number behavior.

The `binary` and `invalid_text` values use canonical RFC 4648 standard-alphabet
base64, including `=` padding whenever the encoded length requires it and no
whitespace. `""` represents zero bytes. `InvalidText` is separate from `Binary`
because the core records that those bytes came from SQLite's `TEXT` storage
class.

For lossless parameters, JSON nulls, Booleans, and strings decode directly.
Numbers, arrays, and untagged objects are rejected because none is a canonical
lossless value. Every tag object contains exactly two fields,
`$briskdb_type` and string `value`; unknown tags, duplicate, missing, or extra
members, wrong member types, noncanonical integers, malformed float bits, and
malformed or noncanonical base64 fail as `invalid_argument` before the engine
can execute or mutate data. Object-member order and insignificant JSON
whitespace are not significant.

The tagged codec preserves the HTTP representation of a protocol-neutral value;
it does not expand SQLite's storage classes. SQL binding still rejects decimal
parameters, unsigned integers greater than `i64::MAX`, invalid-text parameters,
and `Float64` NaN rather than coercing them. Those engine failures retain their
existing problem codes.

Relational timestamps have no native BriskDB `Value`, `DataType`, SQL binding,
or HTTP tag. SQLite has no timestamp storage class, so applications currently
store their chosen representation as ordinary `Text` or `Int64` and own its
units, precision, time-zone, and normalization rules. In particular,
`{"$briskdb_type":"timestamp",...}` is invalid. The signed-microsecond
timestamp domain used by canonical global-index keys does not itself establish
a relational query-value or cross-protocol timestamp contract. Issue #298
tracks that shared temporal contract.

The 2 MiB request limit counts the encoded JSON body, including tag and base64
expansion. Engine result limits count protocol-neutral column, row, and value
bytes before JSON serialization. Base64 expands a binary payload and tag objects
add response bytes beyond that logical accounting. V1 has no separate
encoded-response-byte ceiling because a new default could reject materialized
responses that already succeed. A materialized result-budget failure returns
only the standard problem document, without partial columns, rows, or an
encoding echo. A stream can already have delivered its bounded prefix and then
emits the terminal error record described above.

Binary values can make a storage round trip without becoming JSON text. For
example, insert the canonical base64 tag with `lossless-json-v1`, query the BLOB
with the same selection, and reuse the returned tag as another parameter. The
second parameter decodes to `Value::Binary`, not to a JSON array or text value.

On the administration listener, broadcast accepts only
`{"sql":"CREATE TABLE ..."}` and returns
`{"completed_shards":[0,1]}`. It is the existing journaled schema migration
operation, with preflight and resumable application across every shard. It
does not create catalog metadata implicitly or accept bound parameters.

## Operational responses

### Readiness

`GET /ready` and `GET /v1/ready` return the same JSON. The versioned form adds
the v1 response header. A ready engine returns HTTP 200:

```json
{
  "status": "ready",
  "ready": true,
  "reasons": [],
  "engine_state": "running",
  "schema_state": "ready",
  "schema_generation": "7",
  "active_schema_operations": 2
}
```

Readiness means that the engine lifecycle is `running` and its schema gate is
`ready`, so a new ordinary operation can attempt admission. It does not reserve
a pool slot or promise that a later request cannot race with shutdown,
contention, or a schema transition. Global-index degradation remains visible in
`/health` and `/v1/admin/global-indexes`; it does not change this narrow
admission result.

A non-ready engine returns HTTP 503 with the same `application/json` shape,
`status:"not_ready"`, and `ready:false`. `engine_state` is `draining` or
`stopped`; `schema_state` is `migrating`, `pending`, or `degraded`. The ordered
`reasons` array uses the corresponding finite codes `engine_draining`,
`engine_stopped`, `schema_migrating`, `schema_recovery_pending`, and
`schema_degraded`. This probe response is not a Problem Details document.
`schema_generation` is exact decimal text. `active_schema_operations` is a
snapshot count and can change immediately.

### Relational catalog and physical shards

`GET /v1/admin/catalog` returns the immutable relational catalog view and its
currently published schema generation:

```json
{
  "identifier_encoding_version": 1,
  "schema_generation": "7",
  "default_database_id": "1",
  "databases": [{"id":"1","name":"default"}],
  "tables": [{
    "id": "9",
    "database_id": "1",
    "name": "events",
    "placement": {
      "kind": "sharded",
      "shard_key": {"column":"tenant_id","data_type":"text"}
    },
    "generated_id": {"policy":"none"}
  }],
  "global_indexes": [{
    "id": "3",
    "table_id": "9",
    "name": "events_email",
    "unique": false,
    "lifecycle": "ready",
    "schema_generation": "7",
    "key_encoding_version": 1
  }]
}
```

Database, table, and global-index IDs and schema generations use decimal
strings so JavaScript does not round a persisted `u64`. Arrays preserve the
catalog's stable order. A sharded placement includes one shard-key declaration
whose `data_type` is `int64`, `text`, or `binary`; `global` and `catalog`
placements contain only `kind`. `generated_id.policy` is `none`,
`native_range_v1`, or `hilo_v1`; enabled policies also include `column` and
`encoding_version`. Global-index lifecycle is one of `creating`, `ready`,
`invalid`, `rebuilding`, or `dropping`. This catalog response deliberately
omits migration SQL, index expressions and predicates, physical filenames, and
row contents. Runtime global-index health and recovery instructions remain in
the dedicated global-index endpoint.

`GET /v1/admin/shards` reopens and validates every configured physical shard
through the bounded engine path before returning any result:

```json
{
  "schema_generation": "7",
  "shards": [
    {"id":0,"state":"ready"},
    {"id":1,"state":"ready"}
  ]
}
```

The array is ordered by numeric shard ID. `ready` currently means that the
file's identity, layout, metadata, schema generation, and committed schema
digest all match the validated engine view. Any shard failure rejects the
whole request through the standard engine error mapping; the endpoint never
returns a partial healthy subset. It does not claim a heartbeat, replica state,
WAL-size measurement, load sample, or cross-file transaction snapshot.

### Migration inspection

`GET /v1/admin/migrations` returns a bounded summary rather than unbounded
history:

```json
{
  "schema_generation": "7",
  "active": null,
  "latest_complete": {
    "generation": "7",
    "source_generation": "6",
    "target_generation": "7",
    "state": "complete",
    "shard_count": 2,
    "next_shard": 2,
    "completed_shards": 2,
    "sql_bytes": 42
  }
}
```

`active` and `latest_complete` are always present and use JSON null when there
is no matching row. `generation` names the target generation and is repeated as
`target_generation` to make the source-to-target transition explicit.
`completed_shards` is a count and equals `next_shard`; it is separate from
broadcast's array of completed shard IDs. Neither the journal's exact SQL nor
its deterministic durable identity is returned. `sql_bytes` permits size
diagnosis without exposing its contents.

`GET /v1/admin/migrations/{target_generation}` returns the same migration
object for one canonical positive decimal generation. Leading zeroes, signs,
non-decimal text, zero, overflow, and a generation absent from retained history
all receive the fixed v1 404 problem. This exact lookup keeps the surface
bounded while general pagination remains later work.

Broadcast remains the only v1 migration mutation. Its SQL is capped at 65,536
UTF-8 bytes by the durable migration contract in addition to the HTTP body
limit. An exact retry resumes or recognizes the same journaled operation; a
different migration while one is active fails without replacing its durable
state. Migration inspection participates in engine cancellation, deadlines,
worker admission, and manifest validation, but may observe the active journal
without entering the ordinary schema gate that the migration excludes.

### Active query cancellation

`GET /v1/admin/queries` returns at most 1,024 currently registered HTTP query
operations, sorted by opaque operation ID:

```json
{
  "queries": [{
    "operation_id": "0123456789abcdef0123456789abcdef",
    "elapsed_ms": 17,
    "sql_bytes": 128,
    "cancellation_requested": false
  }]
}
```

The 32-character lowercase hexadecimal ID is random and exists only while that
query is active. `elapsed_ms` is the whole-millisecond age at snapshot time and
can increase between calls. The report does not contain SQL, a SQL digest,
parameters, routing keys, result data, sessions, or paths. Execute, migration,
checkpoint, browser, and PostgreSQL operations do not enter this HTTP-query
list. The list is live: an operation can complete after it is returned. When
all 1,024 registry entries are occupied, another HTTP query fails before Engine
admission with the standard HTTP 422 `limit_exceeded` problem. Removing a
tracking guard restores capacity.

`POST /v1/admin/queries/{operation_id}/cancel` selects the operation entirely
from the path and requires exactly zero request-body bytes; no content type is
required. A nonempty body within the 2 MiB limit receives the fixed HTTP 400
`invalid_argument` problem, while a larger body receives HTTP 413
`request_too_large`. Body rejection occurs before registry cancellation, so it
cannot cancel the named query. An exact active ID with an empty body receives
HTTP 202:

```json
{"operation_id":"0123456789abcdef0123456789abcdef","newly_requested":true}
```

`newly_requested` is false when cancellation was already sticky but cleanup
has not removed the operation yet. A malformed, all-zero, unknown, completed,
or stale ID receives the fixed 404 problem and cannot target another query.
The data query ultimately reports the existing `cancelled` engine problem,
currently HTTP 500. Completion known to have succeeded wins a close
cancellation race. Disconnecting the data client drops the HTTP handler and
removes its ID. The Engine's separate operation guard requests exact-handle
interruption and retains lifecycle and pool ownership until SQLite cleanup
finishes; registry disappearance does not claim that cleanup has already
finished. Handles are neither preallocated tickets, response headers, general
request IDs, durable records, nor idempotency keys.

### Backup capability and checkpoint maintenance

`GET /v1/admin/backup` reports the only supported alpha backup procedure:

```json
{
  "mode": "stopped_directory_copy",
  "online": false,
  "requires_all_processes_stopped": true,
  "checkpoint_endpoint": "/v1/admin/maintenance/checkpoint",
  "checkpoint_role": "preparation_only",
  "checkpoint_is_recovery_point": false
}
```

This response performs no copy and accepts no destination. Follow the complete
[stopped-server backup procedure](OFFLINE_BACKUP.md). Coordinated online backup
and a manifest-defined recovery point remain issue #67.

`POST /v1/admin/maintenance/checkpoint` accepts the exact JSON object `{}` and
passively checkpoints every shard, the manifest, and the global-index database
when present:

```json
{
  "operation": "passive_checkpoint",
  "busy": false,
  "complete": true,
  "recovery_point": false,
  "shards": [{
    "shard": 0,
    "busy": false,
    "counts_available": true,
    "wal_frames": 0,
    "checkpointed_frames": 0,
    "complete": true
  }],
  "databases": [{
    "database": "manifest",
    "busy": false,
    "counts_available": true,
    "wal_frames": 0,
    "checkpointed_frames": 0,
    "complete": true
  }]
}
```

Auxiliary `database` names are `manifest` and `global_index`. Every array is
deterministically ordered. HTTP 200 means the passive attempt ran; SQLite may
still report `busy:true` or `complete:false`, and unavailable counts are zero
with `counts_available:false`. Engine failures use Problem Details. Unknown or
duplicate request fields, a non-object, an empty body, or a missing JSON content
type fail before maintenance. A complete report can reduce retained WAL but
still has `recovery_point:false` and does not make a live directory copy safe.

## Errors

Errors within v1 have `Content-Type: application/problem+json`, the version and
request-ID headers, and exactly the currently defined fields `type`, `title`,
`status`, `detail`, and `code`. Text is fixed and redacted; request bodies,
unknown field names, query text, idempotency keys, filesystem paths, and decoder
diagnostics are not echoed.

| Failure | HTTP | Code |
| --- | ---: | --- |
| Malformed JSON, invalid request envelope/header, nonempty cancel body, or invalid value encoding/tag | 400 | `invalid_argument` |
| Unknown v1 endpoint | 404 | `not_found` |
| Unknown migration generation or malformed/stale query operation ID | 404 | `not_found` |
| Unsupported method on a known endpoint | 405 | `method_not_allowed` |
| Body exceeds 2 MiB | 413 | `request_too_large` |
| Missing or non-JSON content type | 415 | `unsupported_media_type` |
| Reused idempotency key with a different semantic request | 409 | `idempotency_conflict` |
| Engine rejection | Per [error taxonomy](ERRORS.md) | Exact engine code |

Method errors retain the `Allow` header. The four transport-only codes use
`urn:briskdb:http:v1:` problem types with hyphenated code names; invalid
arguments use the same problem type as engine `InvalidArgument`. Only the
engine `busy` code advertises retryability. An unkeyed write may commit before
its response reaches the client and remains at least once. An eligible keyed
write makes a known committed success replayable for the documented retention
window; failures before commit are not cached.

The readiness probe's HTTP 503 JSON is the documented exception to the
Problem Details shape: it is a successful observation that the engine cannot
currently admit ordinary work, not an `EngineError` serialization.

## Migration from the combined listener

Route names, valid request bodies, successful result shapes, and cell encodings
are unchanged. Data requests and discovery stay on `--listen`. Operator and
browser clients must use `--admin-listen` for `/health`, `/ready`, `/metrics`,
`/v1/health`, `/v1/ready`, `/v1/admin/*`, and `/admin/*`. The daemon default
serves those routes at `127.0.0.1:7655`; setting the admin listener to
`disabled` makes every one of them unavailable.

This authority change is intentional pre-1.0 listener configuration, not an
API-major representation change. It introduces no storage migration and does
not alter Rust engine behavior. The established Rust `router` and
`router_with_engine` helpers remain combined for host-owned integrations; the
daemon and attached server use the isolated plane routers with clones of one
Engine. A host constructing both planes must also pass clones of one Engine to
`data_router_with_engine` and `admin_router_with_engine` for shared lifecycle
and cancellation. Calling the two `Arc<Database>` split-router wrappers
separately creates independent Engines and operational registries.

The earlier experimental-to-v1 migration still applies: unknown envelope
fields and malformed bodies now return the fixed errors above, and clients may
adopt lossless cells one request at a time with
`"value_encoding":"lossless-json-v1"`.

`tests/http_v1.rs` exercises discovery, readiness and drain state, catalog and
operational response shapes, migration lookup redaction, checkpoint envelope
strictness, active-query cancellation and cleanup, schema rejection before
mutation, body limits, routing errors, method headers, version isolation, and
session isolation. Existing HTTP tests cover catalog routing, generated keys,
concurrent requests, deadlines, result limits, and exact response shapes.
Lossless-codec tests cover every tag, canonical validation, binary reuse, and
unchanged legacy responses; embedded differential tests verify shared-engine
outcomes. Listener tests prove the complete admin-only route matrix on separate
real sockets, selected-query cancellation from the admin plane, stale-handle
isolation, registry disappearance and later query usability after a client
disconnect, disabled administration, partial-bind cleanup, address reporting,
and common shutdown behavior.
