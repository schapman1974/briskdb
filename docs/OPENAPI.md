# OpenAPI version 1 artifact

BriskDB ships a deterministic OpenAPI 3.1 description of its versioned HTTP
machine API at [`docs/openapi-v1.json`](openapi-v1.json). The document is a
build artifact generated from the same annotated handler definitions that
register the `/v1` routes. Its schemas come from the request and response DTOs
used by those handlers, followed by explicit finalization for middleware and
wire behavior that a Rust derive cannot describe.

The artifact is useful for contract review, validation, and tool input. BriskDB
does not serve it from an HTTP endpoint and does not ship generated client
source. Applications that embed the HTTP adapter can obtain the same document
as a `serde_json::Value` from:

```rust
let document = briskdb::api::openapi_v1();
```

That function is available with the `http` Cargo feature. Its normal OpenAPI
generation dependencies are optional and selected only by `http`; the
independent document and schema validators are test-only dependencies.
SQL-only `embedded` builds do not include any of them on normal dependency
edges.

## Regeneration and byte identity

From the repository root, regenerate the checked artifact with:

```bash
cargo run --locked --no-default-features --features http --example generate-openapi-v1 > docs/openapi-v1.json
```

The example defines the checked serialization: pretty JSON with deterministic
object and array order and one trailing newline. To verify a checkout without
modifying it:

```bash
generated="$(mktemp)"
cargo run --locked --no-default-features --features http --example generate-openapi-v1 > "$generated"
cmp "$generated" docs/openapi-v1.json
rm "$generated"
```

Tests require the public function, the example output, and the checked file to
produce identical bytes. Every internal `$ref` must resolve within the same
document. The document reports `openapi: 3.1.0` and `info.version: "1"`;
the HTTP API version is independent of the Cargo package version and storage
format.

The checked file is included at these locations:

| Distribution | Artifact path |
| --- | --- |
| Source checkout and Cargo crate | `docs/openapi-v1.json` |
| Native release archive | `docs/openapi-v1.json` below the archive root |
| Debian package | `/usr/share/doc/briskdb/docs/openapi-v1.json` |

Cargo package, native archive, and Debian contract tests fail if the artifact
is absent. The OpenAPI integration tests also validate representative live
router discovery, query, Problem Details, and NDJSON records against the
generated component schemas before comparing generated and checked bytes.

## Exact scope and listener ownership

The document contains exactly 17 paths and 28 operations. GET endpoints have
their implicit HEAD operation written out because HEAD is part of the live Axum
contract. Each operation has one `servers` entry identifying the production
listener that owns it. Operation IDs are stable and unique across the document,
so a change is an explicit compatibility-artifact diff rather than generator
order noise.

| Listener | Path | Operations |
| --- | --- | --- |
| Data, `http://127.0.0.1:7654` | `/v1` | GET, HEAD |
| Data, `http://127.0.0.1:7654` | `/v1/` | GET, HEAD |
| Data, `http://127.0.0.1:7654` | `/v1/execute` | POST |
| Data, `http://127.0.0.1:7654` | `/v1/query` | POST |
| Data, `http://127.0.0.1:7654` | `/v1/query/stream` | POST |
| Administration, `http://127.0.0.1:7655` | `/v1/health` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/ready` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/broadcast` | POST |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/global-indexes` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/catalog` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/migrations` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/migrations/{target_generation}` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/shards` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/queries` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/queries/{operation_id}/cancel` | POST |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/backup` | GET, HEAD |
| Administration, `http://127.0.0.1:7655` | `/v1/admin/maintenance/checkpoint` | POST |

The server URLs identify the default loopback addresses and plane ownership;
they do not override configured addresses. The combined Rust router exposes
the same operations for host-owned integration, while production data and
administration routers reject cross-plane paths.

Unversioned `/health` and `/ready`, `/metrics`, every `/admin` browser route,
and versioned fallbacks are intentionally absent. The document does not add
authentication or authorization metadata ahead of the identity and role work
owned by issues #56 and #64, so it has no security scheme or operation security
requirement.

## What the schemas describe

The document describes the strict execute, query, result-limit, migration, and
checkpoint request objects; the discovery and successful data and operational
responses; the exact five-field Problem Details representation; every current
Engine error status and code mapping; readiness JSON at both HTTP 200 and 503;
and the request, API-version, idempotency, content-type, and method headers
applicable to each operation. Query response schemas preserve positional rows,
duplicate column names, the `shard` versus `shards` alternatives, legacy and
lossless values, catalog placement alternatives, and optional generated keys.
The root `x-briskdb-engine-errors` and `x-briskdb-transport-errors` extensions
inventory each current Engine and transport code with the exact HTTP status,
problem type, title, and redacted detail used by the adapter. Both catalogs are
generated from the mappings that build live error responses.

HEAD has the GET operation's status and headers but no response body on the
wire. The live router retains the GET representation's `Content-Length`, as
HTTP permits; clients must not try to decode a HEAD body.

The stream response uses `application/x-ndjson; charset=utf-8`. OpenAPI can
describe each metadata, row, completion, and terminal-error record, while its
`x-briskdb-ndjson` media-type extension records the framing and sequence rule:
one metadata record, zero or more rows, then exactly one completion or error
record, with every compact JSON record terminated by LF. The complete
behavioral contract, including an indeterminate EOF and errors after HTTP 200,
remains in
[HTTP API version 1](HTTP_API.md#streaming-query-response).

## Limits of the OpenAPI representation

The checked document is a faithful machine description of parsed requests and
responses, but these wire rules still require the live-router contract tests
and the prose HTTP contract. The root `x-briskdb-wire-contract` extension
records the raw request-byte ceiling, duplicate-member/header boundary, and
conditional idempotency-status behavior for tools that understand BriskDB's
extensions:

- OpenAPI and JSON Schema cannot detect duplicate object member names after a
  generic JSON parser has collapsed them. BriskDB's decoder rejects duplicates
  and unknown request members before engine execution.
- JSON Schema validates the numeric value after parsing and cannot retain the
  original JSON number token. The request schema accepts mathematical values
  whose conversion rounds to a finite binary64 value, while result cells stay
  within the finite values the serializer can emit. BriskDB additionally
  rejects bare integer tokens outside the `i64`/`u64` range. The
  `x-briskdb-number-token-limit` extension records this distinction.
- JSON Schema treats mathematically integral values such as `1.0` and `1e0`
  as integers. The live `result_limits.max_rows` and
  `result_limits.max_logical_bytes` decoders require unsigned-integer JSON
  token spelling, so the extension on `QueryResultLimits` records that extra
  lexical rule.
- An OpenAPI header schema cannot express repeated raw header lines or every
  HTTP field-combination rule. BriskDB rejects duplicate, comma-folded, empty,
  retained-whitespace, or otherwise noncanonical request IDs and idempotency
  keys according to [Request identity](HTTP_API.md#request-identity).
- A schema describes decoded values, not the 2,097,152-byte request ceiling,
  which includes JSON whitespace and is enforced on the raw body before
  deserialization. Logical result row and byte budgets are separate Engine
  limits and are not encoded-body size promises.
- Operations advertise `application/json` as the portable request media type.
  The live adapter also accepts valid `application/*+json` content types; that
  structured-suffix rule remains covered by its transport tests and prose
  contract.
- OpenAPI can list conditional headers but cannot require
  `BriskDB-Idempotency-Status` only after an eligible keyed execute or fully
  couple `created` and `replayed` to durable receipt state.
- JSON Schema describes the column and positional-row shapes independently; it
  cannot require every row width to equal the column count or bind later
  stream-row values to the encoding selected by the preceding metadata record.
  The live result-shape and streaming tests enforce both relationships.
- An NDJSON response is a byte sequence rather than one JSON instance. Record
  schemas alone cannot enforce LF framing, record order, terminal completion,
  or the meaning of connection loss; the BriskDB extension and live streaming
  tests carry that contract.

The artifact changes only the HTTP adapter and distribution contents. It does
not change routing, SQL behavior, Engine admission or cancellation, manifest
or shard formats, migrations, authentication, or listener exposure. The full
human-readable v1 contract remains [HTTP_API.md](HTTP_API.md), and listener
configuration and ownership remain [HTTP_LISTENERS.md](HTTP_LISTENERS.md).
