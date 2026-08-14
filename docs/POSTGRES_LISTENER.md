# PostgreSQL startup and listener contract

BriskDB serves a PostgreSQL protocol 3.0 baseline plus bounded simple and
parameterized extended queries on a disabled-by-default, separately configured
loopback TCP listener. A successful startup selects one logical database,
creates one protocol-neutral core session, publishes BriskDB-owned parameter
status, and keeps the socket alive until `Terminate`, EOF, server shutdown, or
a protocol failure.

## Process configuration

The binary exposes one value with one grammar:

| Source | Default | Enabled value | Disabled value |
| --- | --- | --- | --- |
| CLI | `--postgres-listen disabled` | `--postgres-listen 127.0.0.1:5433`, or another numeric loopback `SocketAddr` such as `[::1]:5433` | `--postgres-listen disabled` |
| Environment | `BRISKDB_POSTGRES_LISTEN=disabled` | `BRISKDB_POSTGRES_LISTEN=127.0.0.1:5433`, or the same numeric loopback grammar | `BRISKDB_POSTGRES_LISTEN=disabled` |

An explicit command-line value wins over the environment; the environment wins
over the default. `disabled` is the exact lowercase sentinel. Empty values,
hostnames, values without a port, and aliases such as `off` or `none` are
rejected during command-line parsing. Port zero retains the standard
`SocketAddr` meaning of asking the operating system to select a port.

Only IPv4 or IPv6 loopback addresses may activate the PostgreSQL wire endpoint
until the TLS and SCRAM work in issue #36. `server::Config` applies that rule
before opening the data directory or binding either listener. The existing
HTTP listener remains independently configured and always enabled.

`server::Config::postgres_listen` is `Option<SocketAddr>`:

- `Some(address)` validates, binds, and serves PostgreSQL startup;
- `None` skips its bind, accept branch, and shutdown work.

The default or explicit process-only `disabled` spelling is converted to `None`
before entering the server library. `server::run` and
`server::run_with_engine_options` retain their existing signatures.

## Startup and failure order

Startup has one deterministic order:

1. clap parses CLI/environment values and resource options are validated;
2. a configured PostgreSQL address is required to be loopback;
3. the engine opens the data directory and completes startup recovery;
4. the HTTP listener binds;
5. the PostgreSQL listener binds when configured;
6. process signal receivers are installed; and
7. readiness is logged and the shared accept loop starts.

The server never logs readiness after only one configured listener has bound.
An HTTP bind failure precedes a PostgreSQL bind attempt. If the PostgreSQL bind
or signal setup fails, the HTTP listener is released and engine shutdown is
awaited before the original error is returned. Disabled mode cannot cause a
PostgreSQL bind failure.

Engine open still precedes network binding, so a recorded storage recovery can
finish before a later bind failure. That completed recovery is durable; after
the address becomes available, reopening the same data directory is the
supported retry.

## Startup protocol

The implemented wire baseline is protocol 3.0. A startup request for any newer
3.x minor version receives `NegotiateProtocolVersion` with highest supported
minor zero, after which startup continues using 3.0. A different major version
is fatal. Version 3.1 is not treated as an implemented protocol; it is
downgraded just like 3.2 or a future minor. Unsupported `_pq_.` protocol options
are sorted, listed in the same negotiation message, ignored, and do not become
session metadata. Other unknown startup keys remain errors.

`SSLRequest` and `GSSENCRequest` must each be an exact eight-byte frame and
receive `N`, after which the client may send a plaintext startup packet on the
loopback socket. Malformed negotiation lengths cannot consume bytes from a
following startup packet. No TLS or GSS session is accepted in the current
phase. The listener waits at most 60 seconds for negotiation and a startup
packet; that timeout closes the socket without creating a core session.

For the startup packet and subsequent typed messages, a BriskDB-owned raw-frame
gate runs before general dependency decoding and releases exactly one complete
frame at a time. A startup packet's declared length may not exceed 10,000
bytes. After successful startup, `Query`, `Parse`, `Bind`, `Describe`,
`Execute`, `Close`, `Flush`, `Sync`, and `Terminate` frontend messages are
admitted, and their declared length may not exceed 65,541 bytes (the one-byte
message type is outside that declared length). Query, statement, and portal
strings must be valid UTF-8. Format codes, counts, lengths, target bytes, and
every message boundary are validated before dependency decoding. Other
frontend message types are rejected until their owning roadmap issues.
Malformed and oversized frames follow the fixed `08P01` protocol-failure path
when a response can be sent.

The accepted startup keys are finite:

| Key | Contract |
| --- | --- |
| `user` | Required session label: 1–63 bytes; first byte lowercase ASCII or `_`; remaining bytes lowercase ASCII, digits, or `_` |
| `database` | Optional exact logical-database name; omission selects the database whose name equals `user` |
| `client_encoding` | Optional `UTF8` or `UTF-8`; both become `UTF8` |
| `application_name` | Optional, at most 63 UTF-8 bytes, with no control or replacement character |
| `replication` | Optional literal `false`; other values are unsupported |

Every other key is rejected. The `user` value is a bounded connection label,
not a role lookup or credential check. Authentication and role catalogs are
separate later work; it is also unrelated to the HTTP browser's temporary
login. Database selection is an exact lookup through the protocol-neutral
catalog. Startup creates a core session only after every parameter and the
database selection have passed validation.

Successful startup emits these frames in order:

1. optional `NegotiateProtocolVersion(0, unsupported _pq_ options)`;
2. `AuthenticationOk`;
3. `ParameterStatus(server_version, <BriskDB package version>-briskdb)`;
4. `ParameterStatus(server_encoding, UTF8)`;
5. `ParameterStatus(client_encoding, UTF8)`;
6. `ParameterStatus(standard_conforming_strings, on)`;
7. `ParameterStatus(integer_datetimes, on)`;
8. optional validated `ParameterStatus(application_name, ...)`; and
9. `ReadyForQuery(I)`.

The values and ordering are BriskDB-owned rather than dependency defaults.
`BackendKeyData` is omitted until cancellation identifiers are implemented in
issue #35.

## Startup errors

Decoded startup validation/protocol rejections send one fixed `FATAL`
`ErrorResponse` and then close the socket; they do not append `ReadyForQuery`
or echo rejected values. A read timeout, server shutdown, or peer disconnect
can instead close without a response.

| Condition | SQLSTATE |
| --- | --- |
| Missing or invalid `user` | `28000` |
| Empty, invalid, or unknown logical database | `3D000` |
| Invalid client encoding or application name | `22023` |
| Unknown startup key or unsupported replication value | `0A000` |
| Unsupported protocol major version or malformed startup message | `08P01` |

A failed startup creates no retained core session, statement, portal, route, or
SQLite operation. A later connection can start normally.

## Simple-query boundary

The `Query` message executes exactly one non-empty PostgreSQL-dialect statement
through the protocol-neutral prepare, describe, bind, route, and logical
execute lifecycle. Registered-table `SELECT`, `INSERT`, `UPDATE`, and `DELETE`
are supported. Temporary statements and portals are closed after success or
failure. Empty input returns `EmptyQueryResponse`; a statement failure uses the
fixed Engine-to-SQLSTATE mapping, appends `ReadyForQuery(I)`, never includes
query text, and leaves the connection reusable.

Rows are bounded and materialized by the Engine, then returned in PostgreSQL
text format. Declared SQLite columns map `BOOL`/`BOOLEAN` to `bool`, names
containing `INT` to `int8`, `REAL`/`FLOA`/`DOUB` to `float8`,
`DECIMAL`/`NUMERIC` to `numeric`, `CHAR`/`CLOB`/`TEXT` to `text`, and `BLOB` to
`bytea`. Unknown declarations and expressions remain conservative `text`.
Invalid UTF-8 text is rejected instead of replaced. Write responses use ordinary
`INSERT 0 n`, `UPDATE n`, and `DELETE n` command tags. Sharded writes must infer
one exact owner from their literal shard-key values. Reads retain the Engine's
existing point/scatter limits and semantics.

## Extended-query boundary

The parameterized text/binary extended flow uses the same Engine lifecycle:

- `Parse` prepares exactly one statement. Named statements must be unique;
  replacing the unnamed statement closes it and every portal bound from it.
  Explicit OIDs are accepted for `bool`, `int2`/`int4`/`int8`, `oid`,
  `float4`/`float8`, `numeric`, `bytea`, PostgreSQL text families, and
  `json`/`jsonb`. OID zero, omitted OIDs, and trailing uninferred parameters
  conservatively become `text`; unsupported OIDs fail before a core prepared
  handle is retained.
- `Bind` requires the exact value count and accepts PostgreSQL's zero, unified,
  or per-parameter text/binary format list. Values are decoded into
  protocol-neutral `Value`s before Engine bind, planning, or routing. Named
  portals must be unique; replacing the unnamed portal closes its core handle.
- `Describe` returns the resolved parameter OIDs and text-format statement
  fields, or the bound portal's immutable requested result formats, without
  execution.
- `Execute` returns rows or a command tag. A positive row limit suspends and
  resumes the already materialized bounded response without rerunning the core
  portal; a completed portal cannot replay a write.
- `Close` releases a portal or cascades statement closure to its portals;
  `Flush` flushes pending output; `Sync` restores `ReadyForQuery(I)` after an
  error and causes skipped extended messages to be accepted again.

Result format lists may be empty, unified, or match the result-column count.
Binary results use PostgreSQL's native encodings for `bool`, `int8`, `float8`,
and `numeric`, raw bytes for `bytea`, and UTF-8 bytes for text. Text results use
canonical PostgreSQL spellings, including hex `bytea`. A dynamically stored
SQLite value that cannot satisfy its declared static type fails as `42804`
instead of emitting corrupt bytes. Decimal parameters decode correctly but the
current Engine still rejects them before mutation because SQLite has no
lossless decimal binding.

Statement and portal names are at most 63 UTF-8 bytes. Engine limits remain
authoritative for per-session prepared statements, portals, retained values,
rows, and bytes. DDL, transactions, and `COPY` are not supported. The offline
importer is the supported way to establish registered tables. See the
[copy/paste query quickstart](POSTGRES_QUICKSTART.md).

## Connection ownership and shutdown

Each accepted socket runs in a tracked task. At most 256 PostgreSQL socket
tasks are retained; a connection accepted while that limit is full is closed.
HTTP and PostgreSQL tasks share the process lifecycle but are tracked
separately. A PostgreSQL socket that completes startup owns exactly one core
session and tracks it for closure on every terminal path:

- `Terminate` closes promptly without waiting for client EOF;
- client EOF or a wire failure closes the session;
- ordinary server shutdown signals both HTTP and PostgreSQL tasks, then drains
  them concurrently with core shutdown;
- tasks that exceed the configured shutdown grace are aborted, then get one
  additional grace interval for their joins and retained-session closes;
- if that second interval expires, server return does not await the remaining
  PostgreSQL session closes and schedules them as best-effort runtime cleanup;
  and
- dropping the outer server future closes both listener sockets, aborts owned
  connection tasks, begins the resumable engine drain, and schedules
  best-effort terminal session cleanup.

Partial startup sockets are tracked and close during shutdown even though they
have not created a core session. An accept failure on either listener ends the
shared server and enters the same cleanup path.

## Compatibility and storage boundary

`pgwire` remains pinned exactly at 0.36.3 with default features disabled and
only `server-api` enabled. Production framing is contained in
`protocol::postgres`; `core`, `storage`, `sql`, `server`, and the public API do
not accept or return dependency types. The adapter uses BriskDB's catalog,
session lifecycle, fixed SQLSTATE table, and safe messages.

This work changes no HTTP route, JSON body, SQL subset, planner rule, manifest
table, shard header, migration journal, stored row, or storage-format version.
Declared SQLite type metadata is now retained in protocol-neutral statement
descriptions; PostgreSQL OIDs, formats, and raw parameter bytes remain confined
to the adapter and connection memory.

Library selection details and upgrade constraints are normative in the
[PostgreSQL adapter decision record](POSTGRES_ADAPTER.md).

## Verification contract

Automated coverage includes:

- disabled-by-default, explicit IPv4/IPv6, environment, CLI-over-environment, and
  malformed configuration values;
- loopback validation before database or listener creation;
- dual-listener binding, bind-failure cleanup, and clean retry;
- exact `SSLRequest`/`GSSENCRequest` refusal and boundary handling, startup frame
  order, selected identity, omitted-database behavior, and useful BriskDB server
  identification;
- exact 3.0 startup, deterministic downgrade of 3.1/3.2/future 3.x requests,
  sorted unsupported `_pq_.` option reporting, and continued 3.0 queries;
- the 60-second production startup timeout through an accelerated timer test;
- startup and typed-message size boundaries, exact frame isolation, UTF-8 and
  structural validation, duplicate startup keys, and unsupported message types;
- fixed failures for protocol, user, database, encoding, application-name,
  unknown-key, replication, and truncated-message cases before and after
  session creation, followed by successful recovery;
- end-to-end simple-query insert, select, update, delete, row/type encoding,
  and empty-query handling;
- named and unnamed extended write/read/describe/execute, suspension without
  replay, cascading close, flush, fixed-error `Sync` recovery, and raw frame
  validation;
- explicit and inferred parameter OIDs, mixed text/binary values, declared
  result OIDs, statement-versus-portal format descriptions, native binary
  rows, numeric base-10,000 framing, fixed decode errors, and recovery;
- immediate `Terminate`, client EOF, partial startup, normal shutdown, forced
  server-task cancellation, and core-session cleanup;
- the 256-task admission boundary, deterministic overflow close, and slot reuse
  after a tracked task completes;
- many concurrent PostgreSQL sessions while HTTP health remains live; and
- unchanged public HTTP behavior and unchanged storage version.
