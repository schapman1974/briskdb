# PostgreSQL adapter decision record

Status: accepted for roadmap issue #29; amended through issue #37 bounded
client compatibility probes

BriskDB needs a PostgreSQL frontend library that can own protocol framing and
message dispatch without becoming the database engine, routing policy,
prepared-object store, or public error taxonomy. This record selects that
library, fixes its dependency boundary, documents the compatibility probe, and
records the BriskDB-owned startup and security constraints applied to the
configured listener.

## Decision

BriskDB pins `pgwire` exactly at version `0.36.3`:

```toml
pgwire = { version = "=0.36.3", default-features = false, features = ["server-api"] }
```

`0.36.3` is the newest published `pgwire` release whose package metadata
declares Rust 1.85 support. The roadmap's original `0.40.5` candidate, the
current `0.40.6` release at the time of this spike, and every checked release
from `0.37.0` onward declare Rust 1.89. Raising BriskDB's compiler baseline for
one frontend dependency would contradict the existing Rust 1.85 support
contract, so those versions were not selected.

The exact requirement is intentional. `pgwire` is pre-1.0, and a normal caret
requirement would allow Cargo to select a later 0.36 patch without a deliberate
adapter review. A version change must be an explicit source, feature, API,
behavior, and MSRV decision followed by the complete verification matrix.

## Selected feature surface

Default features remain disabled. `server-api` supplies the Tokio socket entrypoint,
frontend/backend messages, handler traits, PostgreSQL type descriptors, and
row messages used by startup and the query adapter. BriskDB's `tls` feature
adds `server-api-ring` for rustls transport and SCRAM-SHA-256.

The dependency's defaults are disabled because they select AWS-LC and extended
chrono, decimal, and JSON adapters. BriskDB selects ring deliberately:

| `pgwire` feature area | Issue #29 decision | Owning follow-up |
| --- | --- | --- |
| Plain server API | Enabled | Foundation for the wire adapter |
| ring TLS provider | Enabled by BriskDB `tls`/`listeners` | TLS and SCRAM issue #36 |
| AWS-LC TLS provider | Not enabled | Avoid a second provider and dependency defaults |
| Chrono type adapter | Not enabled | No BriskDB date/time value type yet |
| Rust-decimal adapter | Not enabled | BriskDB-owned arbitrary decimal wire codec |
| Serde-JSON adapter | Not enabled | JSON parameters remain protocol-neutral text |
| Client API | Not enabled | Named external-client matrix issue #38 |

The resolved graph uses the repository's existing Tokio 1.53.1 rather than a
second Tokio version. It contains ring, rustls, and tokio-rustls, but no AWS-LC,
chrono, or rust-decimal adapter through `pgwire`.

## BriskDB-owned boundary

All production imports of the selected library are confined to
`protocol::postgres`. Neither `core`, `storage`, `sql`, `server`, nor the HTTP
adapter accepts or returns a `pgwire` type.

The public BriskDB seam consists of two owned types:

- `protocol::postgres::Adapter` holds a clone of the protocol-neutral `Engine`
  and the current default logical-database identifier. Constructing it has no
  network or session side effect.
- `protocol::postgres::Connection` owns exactly one non-cloneable core
  `Session`, exposes its `SessionId` plus selected user/database identity, can
  read controlled engine status, and can close that session idempotently.
  Separate connections never share session state.

The connection retains a private implementation of `pgwire`'s `QueryParser`
trait as the compile-time bridge. Its probe path prepares PostgreSQL-dialect
SQL through `Engine::prepare_statement` with compatibility translation and
describes the resulting BriskDB handle. It neither opens SQLite directly nor
computes a route. Production Parse validates raw OIDs before `pgwire` can
collapse both OID zero and unknown/custom OIDs to `None`. The private parser
therefore receives only validated types, treats `None` as inference-to-text,
and retains the resolved PostgreSQL types beside the protocol-neutral handle.

The private prepared wrapper contains a BriskDB `PreparedStatementId` and
`PreparedStatementDescription` plus connection-local PostgreSQL parameter
descriptors. It does not expose the dependency's statement, portal, message,
or type objects through a BriskDB signature. Production Parse/Bind name
management mirrors the wire store into bounded core handles. Statement closure
cascades to dependent portals, and unnamed replacement performs the same
cleanup before installing its successor.

Issue #37 adds a private, parser-bounded discovery shim at this same adapter
edge. It accepts exactly one statement, recognizes only the documented finite
client probes, and rewrites a match to a bounded SQLite `SELECT`. The rewritten
statement still enters the protocol-neutral Engine prepare/describe/bind and
execute lifecycle. Near-matches and all application SQL remain untouched; the
adapter neither reads SQLite directly nor implements a general system catalog.

## Compatibility probe

Automated tests exercise both sides of the boundary without changing the live
server:

1. one shared adapter creates independent connection contexts and unique core
   sessions;
2. concurrent contexts call `Engine::status` through their own sessions;
3. closing one context is terminal and idempotent while another remains usable;
4. the private `QueryParser` prepares and describes a PostgreSQL statement
   through the bounded core prepared lifecycle;
5. every `EngineErrorKind` becomes `pgwire::ErrorInfo` through BriskDB's fixed
   SQLSTATE and safe-message table, never through `EngineError::diagnostic`;
6. a test-only handler factory drives `pgwire::tokio::process_socket` over a
   loopback TCP connection, completes a startup/query probe, reaches core
   status, returns after client EOF, and explicitly closes its core session;
7. a rejected startup returns promptly, closes its core session, and leaves a
   later adapter connection usable; and
8. production server tests independently prove exact startup frames, concurrent
   HTTP/PostgreSQL service, tracked termination, and shutdown cleanup.

The issue-29 loopback command and its response tag remain only a private unit
probe. Production simple queries instead use the Engine's public
prepare/describe/bind/logical-execute lifecycle and have separate raw-wire
contract tests.

## Library fit and retained ownership

The selected version provides the protocol pieces required by the roadmap:

- a Tokio `process_socket` entrypoint with an optional TLS acceptor;
- startup, simple-query, extended-query, copy, error, and cancellation handler
  traits selected by a per-socket factory;
- Parse/Bind/Describe/Execute/Flush/Sync/Close message dispatch;
- text and binary field descriptors and data-row messages; and
- PostgreSQL connection and transaction-status tracking.

Those conveniences do not transfer product ownership to the library. In
particular:

- `pgwire`'s default startup handler advertises dependency-owned version and
  parameter values. Production startup therefore uses BriskDB-owned handlers,
  validates the protocol 3.0 baseline and a finite parameter set, and emits only
  the status documented in `POSTGRES_LISTENER.md`.
- Its default in-memory statement/portal store is unbounded, replacement does
  not return the old object for cleanup, and statement removal does not cascade
  into BriskDB handles. The adapter therefore mirrors every bound portal and
  makes the Engine's finite per-session prepared-statement, portal, and
  retained-value budgets authoritative.
- The default Bind path retains dependency-owned raw parameter bytes and does
  not call `Engine::bind_statement`. BriskDB's handler decodes supported
  text/binary values into BriskDB `Value`s first, binds one core portal, and
  snapshots the portal's result fields/formats before Execute.
- The selected socket loop does not treat `Terminate` as terminal and has no
  BriskDB session-close callback. Production uses a narrow BriskDB-owned loop
  around the library's public decoder/dispatcher and closes its `Connection`
  on `Terminate`, error, and EOF. Shutdown signals the loop; forced task cleanup
  hands retained connections to the server's bounded cleanup supervisor.
- The selected decoder assumes complete, correctly bounded frames. A
  BriskDB-owned raw-frame gate therefore runs before dependency decoding and
  releases exactly one validated frame at a time. BriskDB also owns plaintext
  negotiation: `SSLRequest` and `GSSENCRequest` must each be exactly eight bytes
  before the dependency identifies them, so a malformed negotiation cannot
  consume a following startup. Startup frames have a maximum declared length
  of 10,000 bytes. After startup, the simple and extended query lifecycle plus
  `Terminate` are admitted, with a maximum declared length of 65,541 bytes;
  strings, counts, lengths, formats, targets, and each frame boundary are
  validated before dependency decoding. Other frontend message types are
  rejected until their owning roadmap issues.
  Malformed or oversized input receives BriskDB's fixed `08P01` response when a
  response can be sent, the socket closes, and an already-selected core session
  is still closed. Raw-frame regression tests cover malformed input both before
  and after successful startup.
- The selected version understands protocol 3.0 and 3.2 and defaults its own
  client state to 3.2. BriskDB overrides that state with its exact 3.0 baseline.
  Any newer 3.x request receives a BriskDB-owned downgrade message naming minor
  zero; unsupported `_pq_.` options are sorted and reported in the same frame.
  Protocol 3.0 startup returns a random backend PID and secret used only for
  PostgreSQL `CancelRequest`; neither value is a process identifier or a core
  session identifier.
- The raw-frame caps are transport-retention boundaries, not budgets for SQL,
  names, raw parameters, prepared objects, or rows. Those values and connection
  tasks still require their BriskDB-owned finite limits.
- Portal suspension cannot cause a BriskDB portal to execute twice. The adapter
  retains and resumes the already produced bounded materialized response;
  issue #39 owns a separately reviewed streaming contract.

These are adapter requirements, not reasons to move protocol state into core.
The library remains replaceable as long as the BriskDB seam and conformance
tests remain authoritative.

## Current listener behavior

Issue #30 activates production protocol 3.0 startup. The listener validates a
finite startup parameter set, resolves an
exact logical database through the core catalog, creates one session only after
validation, emits BriskDB-owned status, and tracks that session through
termination and server shutdown. It does not use the dependency's default
startup values or protocol-version state. Issue #32 adds deterministic
newer-minor and protocol-option negotiation without expanding the implemented
3.0 message semantics.

Issue #157 adds the simple-query slice. Issue #31 adds bounded named/unnamed
Parse, Bind, statement/portal Describe, resumable Execute, Flush, Sync error
recovery, and cascading Close. Issue #33 adds declared result types, resolved
parameter OIDs, and basic text/binary value codecs while keeping PostgreSQL
types and raw bytes at this adapter edge. Both paths execute registered table
reads and writes through the Engine. Issue #34 adds protocol-neutral
single-shard transactions, retained connection ownership, failed state, and
exact `I`/`T`/`E` wire status. Issue #35 gives each connected backend a random
key and maps an exact-key `CancelRequest` to a fresh core cancellation token for
only the command currently running on that backend. Disconnect and shutdown
unregister the key and cancel active work. Issue #36 adds TLS plus
single-identity SCRAM-SHA-256, requires secure mode before non-loopback binding,
and delays database lookup/session creation until authentication succeeds.
Issue #37 adds exact version, identity, common setting, and psycopg absent-type
discovery responses. The advertised PostgreSQL 14 version is explicitly a
client-parser compatibility marker; the separate `briskdb_version` status
retains product identity, and no general PostgreSQL 14 compatibility is
claimed. Issue #38 adds the release-gating psql, tokio-postgres, psycopg, and
SQLAlchemy ORM matrix plus the exact bare `START TRANSACTION` and bind-only
`::VARCHAR` adapters those tested clients require.
The remaining roadmap issue retains row-streaming scope. The exact live
contract and user workflow are in the
[PostgreSQL listener document](POSTGRES_LISTENER.md) and
[query quickstart](POSTGRES_QUICKSTART.md).

## Errors, configuration, and storage

The compile bridge converts an `EngineError` only by calling
`protocol::error::postgres_error(error.kind())`. It creates severity `ERROR`,
the table's five-character SQLSTATE, and the fixed safe message. Trusted SQL,
SQLite text, paths, and source chains in `diagnostic()` are never serialized.
Startup applies fixed fatal severity and closes after its error; pre-query
errors use fixed ordinary severity. An error in an active transaction changes
the core session and PostgreSQL status to failed; later statements receive
`25P02` until rollback.

Issue #36 adds certificate, key, user, and password-file process settings while
leaving the listener disabled by default. It changes no HTTP route, JSON, SQL
support, routing, engine limit, manifest/shard schema, storage version,
migration journal, file header, checksum, or recovery behavior. Authentication
and connection state exist only in process memory and disappear on close or
restart.

## Upgrade and verification policy

A `pgwire` update must remain exact and must verify, at minimum:

- package MSRV and the complete resolved graph on Rust 1.85;
- enabled features and absence of unintended providers/type adapters;
- handler factory, startup, query, error, cancellation, portal, and socket-loop
  API changes;
- raw-frame gate/decoder boundaries, exact message limits, and connection-state
  behavior;
- independent per-connection BriskDB sessions and cleanup on every return path;
- fixed SQLSTATE/safe-message conversion for every engine error kind;
- the production listener's expected behavior for the roadmap issue being
  implemented; and
- formatting, Clippy, all targets, documentation tests, and the named stable
  and Rust 1.85 CI matrices with the locked dependency graph.

Primary upstream references:

- [`pgwire` 0.36.3 package metadata](https://crates.io/crates/pgwire/0.36.3)
- [`pgwire` server API documentation](https://docs.rs/pgwire/0.36.3/pgwire/)
- [PostgreSQL frontend/backend protocol](https://www.postgresql.org/docs/current/protocol.html)
