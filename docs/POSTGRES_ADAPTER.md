# PostgreSQL adapter decision record

Status: accepted for roadmap issue #29

BriskDB needs a PostgreSQL frontend library that can own protocol framing and
message dispatch without becoming the database engine, routing policy,
prepared-object store, or public error taxonomy. This record selects that
library, fixes its dependency boundary, and documents the compatibility probe.
It does not activate PostgreSQL wire behavior on the configured listener.

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
row encoders needed by the planned server adapter.

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
  `Session`, exposes its `SessionId`, can read controlled engine status, and can
  close that session idempotently. Separate connections never share session
  state.

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
8. the existing server tests continue to prove that the configured production
   PostgreSQL listener writes zero bytes and immediately closes each stream.

The loopback command and its response tag exist only inside the unit test. They
are not supported SQL, public protocol behavior, or a claim that a PostgreSQL
driver can use BriskDB today.

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
  parameter values. Issue #30 must replace it with BriskDB values before the
  production listener calls `process_socket`.
- Its default in-memory statement/portal store is unbounded, replacement does
  not return the old object for cleanup, and statement removal does not cascade
  into BriskDB handles. Issue #31 must keep the engine's existing finite
  per-session prepared-statement, portal, and retained-value budgets
  authoritative.
- The default Bind path retains dependency-owned raw parameter bytes and does
  not call `Engine::bind_statement`. The production adapter must decode into
  BriskDB `Value` objects and bind at Bind time so the route snapshot is
  immutable before Execute.
- The socket loop has no BriskDB session-close callback. The production wrapper
  must close its `Connection` on every ordinary, error, EOF, cancellation, and
  shutdown return path.
- The selected version understands protocol 3.0 and 3.2 and defaults its own
  client state to 3.2. Issue #32 must apply BriskDB's deliberate baseline and
  minor-version negotiation policy.
- Dependency message-size ceilings are not BriskDB retention limits. SQL,
  names, raw parameters, prepared objects, rows, and connection tasks still
  require BriskDB-owned finite limits.
- Portal suspension in the dependency cannot cause a BriskDB portal to execute
  twice. The core currently returns a complete bounded materialized result;
  issue #31 must retain and resume the already produced response or provide a
  separately reviewed streaming contract.

These are adapter requirements, not reasons to move protocol state into core.
The library remains replaceable as long as the BriskDB seam and conformance
tests remain authoritative.

## Current listener behavior

The configured PostgreSQL TCP listener still performs issue #28's exact
accept-and-close behavior. It does not construct an `Adapter` or `Connection`,
call `process_socket`, read a startup packet, emit a response, open a core
session, or run SQL. Selecting a library must not accidentally ship its default
startup behavior before BriskDB's production session policy is implemented.

Issue #30 owns production wiring plus startup, logical database/user selection,
BriskDB parameter status, termination, and server-version identification.
Issue #31 owns simple and extended query flow. Later roadmap issues retain
their existing protocol-version, type, transaction, cancellation, TLS/SCRAM,
client-matrix, and row-streaming scopes.

## Errors, configuration, and storage

The compile bridge converts an `EngineError` only by calling
`protocol::error::postgres_error(error.kind())`. It creates severity `ERROR`,
the table's five-character SQLSTATE, and the fixed safe message. Trusted SQL,
SQLite text, paths, and source chains in `diagnostic()` are never serialized.
Production severity, failed-transaction state, and connection-fatal policy
remain part of the later wire/session work.

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
- message limits and connection-state behavior;
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
