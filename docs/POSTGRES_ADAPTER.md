# PostgreSQL adapter decision record

Status: accepted for roadmap issue #29; amended by issue #30 production startup
activation

BriskDB needs a PostgreSQL frontend library that can own protocol framing and
message dispatch without becoming the database engine, routing policy,
prepared-object store, or public error taxonomy. This record selects that
library, fixes its dependency boundary, documents the compatibility probe, and
records the BriskDB-owned startup constraints applied when issue #30 activated
the configured loopback listener.

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

Only `server-api` is enabled. That supplies the Tokio socket entrypoint,
frontend/backend messages, handler traits, PostgreSQL type descriptors, and
row encoders used by startup and the planned query adapter.

The dependency's defaults are disabled because they additionally select an
AWS-LC TLS backend and extended chrono, decimal, and JSON adapters. BriskDB does
not need those dependencies for the issue-29 fit check:

| `pgwire` feature area | Issue #29 decision | Owning follow-up |
| --- | --- | --- |
| Plain server API | Enabled | Foundation for issues #30 and #31 |
| AWS-LC or ring TLS provider | Not enabled | TLS and SCRAM issue #36 |
| Chrono type adapter | Not enabled | Type mapping issue #33 |
| Rust-decimal adapter | Not enabled | Type mapping issue #33 |
| Serde-JSON adapter | Not enabled | Type mapping issue #33 |
| Client API | Not enabled | Named external-client matrix issue #38 |

The resolved graph uses the repository's existing Tokio 1.53.1 rather than a
second Tokio version. `cargo tree -e features -i pgwire` reports only the
`server-api` feature. The selected graph contains no `aws-lc`, `ring`,
`tokio-rustls`, `chrono`, or `rust_decimal` package through `pgwire`.

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
computes a route. PostgreSQL parameter OID lists are rejected as `Unsupported`
until issue #33 defines their exact mapping. Because `pgwire`
collapses both raw OID zero and unknown/custom OIDs to `None` before invoking
`QueryParser`, the probe rejects every nonempty parameter-type list rather than
guessing which raw value produced it.

The private prepared wrapper contains only a BriskDB
`PreparedStatementId` and `PreparedStatementDescription`. It does not expose
the dependency's statement, portal, message, or type objects through a BriskDB
signature. Production Parse/Bind name management and cleanup do not use this
probe yet; issue #31 owns that state machine.

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
probe. They are not supported SQL or public protocol behavior. Production
startup has separate raw-wire contract tests and deliberately returns `0A000`
for queries until issue #31.

## Library fit and retained ownership

The selected version provides the protocol pieces required by the roadmap:

- a Tokio `process_socket` entrypoint with an optional TLS acceptor;
- startup, simple-query, extended-query, copy, error, and cancellation handler
  traits selected by a per-socket factory;
- Parse/Bind/Describe/Execute/Flush/Sync/Close message dispatch;
- text and binary field descriptors and row encoders; and
- PostgreSQL connection and transaction-status tracking.

Those conveniences do not transfer product ownership to the library. In
particular:

- `pgwire`'s default startup handler advertises dependency-owned version and
  parameter values. Production startup therefore uses BriskDB-owned handlers,
  validates protocol 3.0 and a finite parameter set, and emits only the status
  documented in `POSTGRES_LISTENER.md`.
- Its default in-memory statement/portal store is unbounded, replacement does
  not return the old object for cleanup, and statement removal does not cascade
  into BriskDB handles. Issue #31 must keep the engine's existing finite
  per-session prepared-statement, portal, and retained-value budgets
  authoritative.
- The default Bind path retains dependency-owned raw parameter bytes and does
  not call `Engine::bind_statement`. The production adapter must decode into
  BriskDB `Value` objects and bind at Bind time so the route snapshot is
  immutable before Execute.
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
  of 10,000 bytes. After startup, only `Query`, `Parse`, `Sync`, and `Terminate`
  are admitted, with a maximum declared length of 65,541 bytes; strings must be
  valid UTF-8 and each frame must satisfy its exact structural boundary. Other
  frontend message types are rejected until their owning roadmap issues.
  Malformed or oversized input receives BriskDB's fixed `08P01` response when a
  response can be sent, the socket closes, and an already-selected core session
  is still closed. Raw-frame regression tests cover malformed input both before
  and after successful startup.
- The selected version understands protocol 3.0 and 3.2 and defaults its own
  client state to 3.2. BriskDB now overrides that state with an exact 3.0
  baseline; issue #32 owns explicit newer-minor negotiation.
- The raw-frame caps are transport-retention boundaries, not budgets for SQL,
  names, raw parameters, prepared objects, or rows. Those values and connection
  tasks still require their BriskDB-owned finite limits.
- Portal suspension in the dependency cannot cause a BriskDB portal to execute
  twice. The core currently returns a complete bounded materialized result;
  issue #31 must retain and resume the already produced response or provide a
  separately reviewed streaming contract.

These are adapter requirements, not reasons to move protocol state into core.
The library remains replaceable as long as the BriskDB seam and conformance
tests remain authoritative.

## Current listener behavior

Issue #30 activates production protocol 3.0 startup on configured loopback
addresses. The listener validates a finite startup parameter set, resolves an
exact logical database through the core catalog, creates one session only after
validation, emits BriskDB-owned status, and tracks that session through
termination and server shutdown. It does not use the dependency's default
startup values or protocol-version state.

Issue #31 owns simple and extended query flow. Until then, every simple query
and extended `Parse` receives a fixed `0A000` error with protocol recovery and
no retained statement. Later roadmap issues retain their existing
minor-version negotiation, type, transaction, cancellation, TLS/SCRAM,
client-matrix, and row-streaming scopes. The exact live contract is in the
[PostgreSQL listener document](POSTGRES_LISTENER.md).

## Errors, configuration, and storage

The compile bridge converts an `EngineError` only by calling
`protocol::error::postgres_error(error.kind())`. It creates severity `ERROR`,
the table's five-character SQLSTATE, and the fixed safe message. Trusted SQL,
SQLite text, paths, and source chains in `diagnostic()` are never serialized.
Startup applies fixed fatal severity and closes after its error; pre-query
errors use fixed ordinary severity. Failed-transaction state remains part of
the later transaction work.

This decision adds no CLI or environment setting and does not change listener
defaults, HTTP routes, JSON, SQL support, routing, engine limits, manifest or
shard schemas, storage versions, migration journals, file headers, checksums,
or recovery behavior. Adapter and connection state exist only in process
memory and disappear on close or restart.

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
