# HTTP API version 1

Status: implemented for issues #50, #51, and #52. BriskDB remains an alpha
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
  "max_request_bytes": 2097152
}
```

Every response within `/v1`, including engine errors, decoding failures,
unknown endpoints, and unsupported methods, carries `BriskDB-API-Version: 1`.
The URL selects the API version; a client header does not select another
version. Unknown versions have no routes and return HTTP 404. Discovery is
static contract metadata, not a readiness check; use `/v1/health` on the
administration listener for the current engine health report.

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

The administration listener defaults to `127.0.0.1:7655` through
`--admin-listen` and `BRISKDB_ADMIN_LISTEN`; the exact value `disabled` omits
the entire plane:

| Administration method and path | Success |
| --- | --- |
| `GET /health` | Engine and aggregate global-index health report |
| `GET /metrics` | Prometheus text report |
| `GET /v1/health` | Versioned alias of `/health` |
| `POST /v1/admin/broadcast` | Journaled application-schema migration result |
| `GET /v1/admin/global-indexes` | [Global-index operational report](GLOBAL_INDEX_RELEASE_GATE.md) |
| `/admin`, `/admin/`, assets, and `/admin/api/*` | [Admin data browser](ADMIN_BROWSER.md) |

GET routes also accept HEAD, returning headers without a body. Success is HTTP
200 with `application/json`, except for Prometheus text and browser assets.
Each production router omits the other plane's handlers, so sending a route to
the wrong listener returns 404 without executing it. The complete address,
Rust/Python configuration, startup, and drain contract is in
[HTTP_LISTENERS.md](HTTP_LISTENERS.md).

## SQL requests and session lifetime

Both SQL endpoints accept this exact envelope:

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
The execute response does not echo `value_encoding`; its generated-key shape is
already exact and remains unchanged for both encodings.

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
add response bytes beyond that logical accounting; v1 still buffers the bounded
result and has no separate encoded-response-byte or streaming contract. Those
transport controls remain later roadmap work. A result-budget failure returns
only the standard problem document, without partial columns, rows, or an
encoding echo.

Binary values can make a storage round trip without becoming JSON text. For
example, insert the canonical base64 tag with `lossless-json-v1`, query the BLOB
with the same selection, and reuse the returned tag as another parameter. The
second parameter decodes to `Value::Binary`, not to a JSON array or text value.

On the administration listener, broadcast accepts only
`{"sql":"CREATE TABLE ..."}` and returns
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
| Malformed JSON, invalid request envelope, or invalid value encoding/tag | 400 | `invalid_argument` |
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

## Migration from the combined listener

Route names, valid request bodies, successful result shapes, and cell encodings
are unchanged. Data requests and discovery stay on `--listen`. Operator and
browser clients must change their base address to `--admin-listen` for
`/health`, `/metrics`, `/v1/health`, `/v1/admin/*`, and `/admin/*`. The daemon
default moves those routes from `127.0.0.1:7654` to `127.0.0.1:7655`; setting
the admin listener to `disabled` makes every one of them unavailable.

This authority change is intentional pre-1.0 listener configuration, not an
API-major representation change. It introduces no storage migration and does
not alter Rust engine behavior. The established Rust `router` and
`router_with_engine` helpers remain combined for host-owned integrations; the
daemon and attached server use the isolated plane routers.

The earlier experimental-to-v1 migration still applies: unknown envelope
fields and malformed bodies now return the fixed errors above, and clients may
adopt lossless cells one request at a time with
`"value_encoding":"lossless-json-v1"`.

`tests/http_v1.rs` exercises discovery, schema rejection before mutation, body
limits, routing errors, method headers, version isolation, and session isolation.
Existing HTTP tests cover catalog routing, generated keys, concurrent requests,
deadlines, result limits, and exact response shapes. Lossless-codec tests cover
every tag, canonical validation, binary reuse, and unchanged legacy responses;
embedded differential tests verify shared-engine outcomes. Listener tests prove
the route matrix on separate real sockets, disabled administration, partial-bind
cleanup, address reporting, and common shutdown behavior.
