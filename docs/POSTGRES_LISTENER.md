# PostgreSQL listener lifecycle

Issue #28 adds the process and TCP-lifecycle boundary for a future PostgreSQL
wire adapter. Issue #29 subsequently selects the adapter library without
activating it here. The listener deliberately does not implement a PostgreSQL
startup packet, authentication, query flow, error response, or any other wire
message.

## Process configuration

The binary exposes one value with one grammar:

| Source | Default | Enabled value | Disabled value |
| --- | --- | --- | --- |
| CLI | `--postgres-listen 127.0.0.1:5433` | numeric `SocketAddr`, such as `127.0.0.1:6543` or `[::1]:6543` | `--postgres-listen disabled` |
| Environment | `BRISKDB_POSTGRES_LISTEN=127.0.0.1:5433` | the same numeric `SocketAddr` grammar | `BRISKDB_POSTGRES_LISTEN=disabled` |

An explicit command-line value wins over the environment; the environment wins
over the default. `disabled` is the exact lowercase sentinel. Empty values,
hostnames, address strings without a port, and aliases such as `off` or `none`
are rejected during command-line parsing. Port zero retains the standard
`SocketAddr` meaning of asking the operating system to select a port.

The existing `--listen` / `BRISKDB_LISTEN` setting remains the always-enabled
HTTP address and retains its `127.0.0.1:7654` default. The two listeners are
configured independently; this issue does not rename the HTTP option or add an
HTTP-disabled mode.

## Rust server configuration

`server::Config::postgres_listen` is `Option<SocketAddr>`:

- `Some(address)` binds and serves the placeholder PostgreSQL listener;
- `None` skips its bind, accept branch, and shutdown work.

The process-only `disabled` spelling is converted to `None` before calling the
server library, so embedders never parse a command-line sentinel. Adding this
field to the public pre-1.0 `Config` struct is a source-shape change for callers
that construct it with a literal; those callers must choose `Some(address)` or
`None`. The signatures of `server::run` and
`server::run_with_engine_options` are unchanged.

## Startup and failure order

Startup has one deterministic order:

1. clap parses every CLI/environment value and `Args::into_server_parts`
   validates resource options;
2. the engine opens the configured data directory, including existing startup
   recovery and shard validation;
3. the HTTP listener binds;
4. the PostgreSQL listener binds when configured;
5. process signal receivers are installed; and
6. readiness is logged and the accept loop starts.

The server never logs readiness after only one of two configured listeners has
bound. Preserving HTTP-first binding means an HTTP bind conflict is reported
before a PostgreSQL bind is attempted. If the PostgreSQL bind fails, the
already-bound HTTP listener is dropped, the engine enters its normal shutdown
path, cleanup is awaited, and the original PostgreSQL bind error is returned.
The same cleanup applies if signal setup fails. A disabled PostgreSQL listener
cannot cause a bind failure, even if the default port is already in use.

Engine open precedes network binding to retain BriskDB's existing startup
ordering. Consequently, an open may finish an already-recorded storage recovery
before a later listener bind fails. That durable recovery is not rolled back;
after the address becomes available, starting the same data directory again is
the supported retry.

## Placeholder connection behavior

After issue #29, the listener still accepts each TCP connection and immediately
drops the stream:

- it reads no client bytes;
- it writes no server bytes or PostgreSQL error frame;
- the peer observes EOF/connection close;
- no `Engine` session, prepared statement, portal, route, or SQLite operation
  is created; and
- accepted placeholder connections are not retained as background tasks.

Continuously accepting and closing prevents the operating-system backlog from
filling while making the incomplete wire behavior deterministic. Emitting even
a nominal PostgreSQL frame is deferred to production activation of the
selected BriskDB-owned adapter in issue #30. This TCP scaffold must not be
described as a usable PostgreSQL interface.

HTTP acceptance and placeholder PostgreSQL acceptance share one server
lifecycle. Neither listener implements routing, parsing, planning, or SQLite
access; those remain behind the protocol-neutral `Engine` boundary.

## Shutdown and recovery

SIGINT and, on supported Unix hosts, SIGTERM first stop new engine admissions.
Both TCP listeners are then dropped. Existing HTTP connection tasks drain under
the configured grace period alongside core shutdown. PostgreSQL placeholder
streams need no drain because they are closed as soon as they are accepted.

An accept failure on either listener ends the shared server: both listeners
close, HTTP tasks drain, and the engine completes its normal shutdown. Dropping
the server future closes both listener sockets and enters the same resumable
`Draining` engine state already documented for HTTP-only operation.

## Compatibility and storage boundary

Issue #28 itself adds no wire dependency. Issue #29 pins `pgwire` 0.36.3 behind
`protocol::postgres`, with default features disabled and only `server-api`
enabled. That selection changes no HTTP route, JSON body, SQL subset, planner
rule, typed result, engine error mapping, manifest table, shard header,
migration journal, stored row, or storage-format version. Listener settings and
the inactive adapter seam are not persisted in `manifest.sqlite` or a shard.

The PostgreSQL SQLSTATE table is consumed by the adapter compatibility probe;
the placeholder emits none of those mappings. The next roadmap item owns
PostgreSQL startup/session messages and production socket integration. Library
selection details and constraints are normative in the
[PostgreSQL adapter decision record](POSTGRES_ADAPTER.md).

## Verification contract

Automated coverage includes:

- default, explicit IPv4/IPv6, environment, CLI-over-environment, disabled, and
  malformed configuration cases;
- both listeners serving concurrently while the HTTP health path remains live;
- immediate close for one and many concurrent placeholder clients;
- disabled mode skipping PostgreSQL binding;
- PostgreSQL bind-failure cleanup and a clean retry of the data directory;
- ordinary graceful shutdown and dropped-server-future recovery with both
  listeners configured; and
- unchanged public HTTP behavior and unchanged storage version.
