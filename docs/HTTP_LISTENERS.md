# HTTP data and administration listeners

BriskDB serves data and administration traffic on separate HTTP/1 listeners.
Both routers use the same protocol-neutral `Engine`; the split changes network
reachability and does not create a second database, routing path, or storage
authority.

## Process configuration

| Plane | CLI | Environment | Default | Disable |
| --- | --- | --- | --- | --- |
| Data | `--listen SOCKET_ADDR` | `BRISKDB_LISTEN` | `127.0.0.1:7654` | No |
| Administration | `--admin-listen SOCKET_ADDR\|disabled` | `BRISKDB_ADMIN_LISTEN` | `127.0.0.1:7655` | Exact value `disabled` |

Command-line input takes precedence over the environment, which takes
precedence over the default. Addresses must be numeric IPv4 or IPv6 socket
addresses. The ordinary SQL data and administration routers have no user
authentication or role-authorization boundary, so each configured address must
be loopback even with the attached Rust TLS option below. A non-loopback
data or administration address is rejected before the database opens or any
listener binds.

An independently enabled, loopback-only Mongo listener can join the same server
when built with the non-default `mongo` feature. `--mongo-listen` /
`BRISKDB_MONGO_LISTEN` defaults to `disabled`; see the
[Mongo startup and lifecycle contract](MONGO_PARITY.md#current-wire-checkpoint).
It shares database ownership and shutdown with HTTP/PostgreSQL, not their data
model or security settings. A Mongo bind failure releases all passive sockets
before the server begins accepting requests.
Unreleased builds with `mongo-tls` also accept paired `--mongo-tls-cert` and
`--mongo-tls-key` (or `BRISKDB_MONGO_TLS_CERT` / `BRISKDB_MONGO_TLS_KEY`). This
encrypts only Mongo and does not change either HTTP plane's loopback-only policy;
see [daemon Mongo TLS](MONGO_PARITY.md#encrypted-daemon-mongo-unreleased).

The data and administration addresses must be distinct when a nonzero port is
configured. The same loopback address with port zero is valid for both; each
bind asks the operating system for a separate available port, and the returned
addresses are required to differ.

`--listen` and `BRISKDB_LISTEN` keep their established spelling and now name
the data plane. The second default port is intentional: scripts that used
operator or browser paths at `127.0.0.1:7654` must use
`127.0.0.1:7655`. Relative paths and successful response bodies are unchanged.

## Route ownership

| Data listener | Administration listener |
| --- | --- |
| `GET /v1`, `GET /v1/` | `GET /health` |
| `POST /v1/query` | `GET /metrics` |
| `POST /v1/query/stream` | `GET /v1/health` |
| `POST /v1/execute` |  |
|  | `GET /ready`, `GET /v1/ready` |
|  | `POST /v1/admin/broadcast` |
|  | `GET /v1/admin/catalog` |
|  | `GET /v1/admin/migrations`, `GET /v1/admin/migrations/{target_generation}` |
|  | `GET /v1/admin/shards` |
|  | `GET /v1/admin/queries`, `POST /v1/admin/queries/{operation_id}/cancel` |
|  | `GET /v1/admin/backup` |
|  | `POST /v1/admin/maintenance/checkpoint` |
|  | `GET /v1/admin/global-indexes` |
|  | `/admin`, `/admin/`, assets, and `/admin/api/*` |

Each production router omits the other plane's handlers. A cross-plane request
with ordinary request controls therefore receives HTTP 404 and cannot execute
the hidden operation. Malformed request-control headers or an idempotency key on
an unsupported route can fail before dispatch. Responses under an omitted `/v1`
path retain the version-1 problem-detail and version header behavior;
unversioned missing paths use the ordinary HTTP 404 response.
Every response from both planes, including unversioned fallbacks, carries one
`BriskDB-Request-ID`; this correlation header does not change route ownership.
The embedded browser and its JSON endpoints stay together on the administration
listener, so its same-origin cookie and asset rules do not change. Its temporary
cookie does not authenticate the operator endpoints above.

The public Rust HTTP module exposes `data_router` and
`data_router_with_engine`, plus `admin_router` and
`admin_router_with_engine`. Its established `router` and `router_with_engine`
functions remain combined-router compatibility helpers for applications that
deliberately own their HTTP serving boundary. BriskDB's daemon and attached
server clone one Engine into the two Engine-based router constructors. A host
building both planes must do the same so lifecycle and active-query
cancellation are shared. Calling the two `Arc<Database>` compatibility wrappers
separately creates independent Engines and therefore independent operational
registries. A host that serves a router itself owns socket validation and
exposure; loopback enforcement belongs to `server`, not to an Axum `Router`
value.

## Rust and Python attached servers

`server::Config` keeps `listen` for the data plane and adds
`admin_listen: Option<SocketAddr>`. `server::ListenerConfig` likewise keeps
`http_listen` and adds `admin_listen`. `None` disables administration.
`ListenerAddresses::data()` returns the actual data address, while
`ListenerAddresses::http()` remains its compatibility alias. `admin()` returns
the optional actual administration address, including an operating-system-
selected port requested as zero.

Python keeps `http` and `Server.http_address` as data-plane compatibility
names and adds the explicit `Server.data_address` alias. Synchronous and
asynchronous `Database.serve()` add keyword-only
`admin="127.0.0.1:0"`; passing `admin=None` disables that listener.
`Server.admin_address` and `AsyncServer.admin_address` return the optional
bound address. The ephemeral Python default avoids a fixed-port collision while
keeping the browser and operator surface available to an attached server.

### Encrypt attached Rust HTTP planes (unreleased)

With `listeners`, Rust hosts can opt into independent server certificates for
the data and administration planes through the existing options builder:

```rust,ignore
use briskdb::server::{AttachedServer, AttachedServerOptions, HttpTlsConfig, ListenerConfig};

let mut server = AttachedServer::start_with_options(&database, ListenerConfig {
    http_listen: "127.0.0.1:7654".parse()?,
    admin_listen: Some("127.0.0.1:7655".parse()?),
    postgres_listen: None,
}, AttachedServerOptions::new()
    .with_http_tls(HttpTlsConfig::new("./data.crt", "./data.key"))
    .with_admin_tls(HttpTlsConfig::new("./admin.crt", "./admin.key"))
).await?;
// Connect using HTTPS, a trusted issuing CA, and a certificate-matching hostname.
server.close().await?; // Stops listeners; the borrowed database remains open.
```

Either plane may remain plaintext; TLS is never enabled implicitly. An admin
identity requires an enabled admin address. The same bounded, descriptor-validated
certificate/key loader used by Mongo/PostgreSQL runs off-runtime before binding.
Unix private keys must not be group-writable or accessible to others. Invalid
material or a failed bind leaves no newly bound sockets and does not stop the
borrowed database. Existing `ListenerConfig` literals and legacy constructors
remain compatible. TLS does not alter route ownership, cookies, query semantics,
or the **loopback-only** policy. Do not disable certificate/hostname verification.

HTTP/1.1 is the only advertised ALPN; this does not add HTTP/2. Each TLS handshake
has a 15-second default deadline, which `with_handshake_timeout(Duration)` may
only narrow to a positive value. It uses an ordinary connection slot and is
cancelled on close/drop, including incomplete handshakes. Each plane has its own
identity; PostgreSQL credentials do not authenticate HTTP callers.

`with_http_tls(...)` also encrypts a selected `with_sqlite_remote(...)` router.
Its bearer-token and table-allowlist requirements stay in force; the TLS
certificate alone does not authorize remote-table access. Administration remains
separate. Both addresses still require loopback. Python/daemon HTTP TLS options
are not exposed by this increment; existing SIGHUP reload currently covers only
configured PostgreSQL/Mongo identities.

### Reload attached Rust HTTP identities (unreleased)

An attached host can replace an already-encrypted plane independently, without
rebinding or changing routes:

```rust,ignore
server.reload_http_tls(HttpTlsConfig::new("./next-data.crt", "./next-data.key")).await?;
server.reload_admin_tls(HttpTlsConfig::new("./next-admin.crt", "./next-admin.key")).await?;
```

Each successful publication replaces one complete certificate/key/handshake-budget
generation. Newly admitted sockets use it; established connections and handshakes
already admitted before publication retain the original generation and deadline.
Reloading one plane does not modify the other plane or PostgreSQL/Mongo identities.
It cannot enable a disabled listener or upgrade a plaintext listener to TLS.

`reload_http_tls_with_context(config, RequestContext)` and
`reload_admin_tls_with_context(config, RequestContext)` support cancellation and
absolute deadlines before preparation, while waiting, and immediately before
publication. Query result limits do not apply. Invalid material, cancellation,
deadline expiry, or listener/engine shutdown before publication preserves the
active identity. File loading runs off-runtime and the worker cannot publish a
late result after the waiting future is dropped. The reload handle only weakly
observes the borrowed engine, so retaining a closed handle does not retain its
storage pools. Concurrent successful reloads publish in completion order; a later
cancellation cannot undo an identity already published. This is not client
authentication, session revocation, a file watcher, or cross-plane atomic rotation.

### Finite socket admission (unreleased)

The attached and daemon HTTP hosts now allow at most **256 active data sockets
and 256 active administration sockets**, independently, for both plaintext and
TLS. The slot covers handshake, incomplete headers/body, active requests and
keep-alive until the connection task exits. At capacity, newly accepted sockets
are closed before request dispatch; no successful HTTP response is promised.
Completion, transport failure, timeout, task cancellation and shutdown reclaim
slots. Data saturation does not consume administration's reserved capacity.
This is a host socket bound, not per-user governance or an idle-HTTP timeout.

## Startup and shutdown

Configuration and engine options are validated first. The daemon opens and
recovers the database, then binds the data listener, the administration listener
when enabled, and PostgreSQL when enabled. It installs process signal receivers
and logs readiness only after every configured socket has bound. Any bind or
accept failure releases all listener sockets and enters the existing cleanup
path; a partially started daemon never reports readiness.

After binding, service and packaging probes use `GET /v1/ready` on the
administration listener. HTTP 200 means the shared engine lifecycle is running
and its schema gate is ready at that snapshot. A non-ready engine returns HTTP
503 with the same JSON report and finite lifecycle/schema reasons. `/ready` is
the unversioned alias. Data discovery remains static metadata and is not a
readiness signal. Disabling the administration listener also removes both
readiness paths, so a host that chooses that configuration must inspect its
embedded Engine lifecycle directly.

Both HTTP planes share the existing finite engine admission, deadline, result,
pool, and shutdown controls. One shutdown signal stops admission, closes every
listener, signals all tracked HTTP and PostgreSQL connections, and drains them
against the configured grace period. An attached server performs the same
listener drain without beginning shutdown of its borrowed database.

Active HTTP query handles are likewise stored in the shared Engine rather than
one router. A data-plane `/v1/query` or `/v1/query/stream` can therefore be
listed and cancelled from the administration listener even though neither
plane forwards requests to the other. Closing a data socket drops its handler
and removes the query handle; the engine's operation guard retains its own
lifecycle and pool leases until SQLite interruption and cleanup finish.

## Compatibility and deferred work

This is a deliberate pre-1.0 deployment change for administration and operator
HTTP clients. `/health`, `/metrics`, `/v1/health`, `/v1/admin/*`, and `/admin/*`
retain their paths and representations but move from the data address to the
administration address. `/v1/query`, `/v1/execute`, and version discovery stay
on the established data address. The move changes no SQL semantics, JSON value
codec, error mapping, manifest or shard format, migration, or dependency.

Issue #53 added readiness, catalog, migration, shard, active-query
cancellation, backup-capability, and checkpoint-maintenance endpoints. Issue
#54 adds plane-wide request IDs, eligible-write idempotency, request-local query
limits, and bounded HTTP row streaming. It deliberately adds no retained
pagination cursor or global ordering; those semantics remain in Phase 7 issues
#58 and #59. The checked [OpenAPI v1 artifact](OPENAPI.md) identifies the owner
of every versioned operation without describing the excluded unversioned and
browser routes.
Issue #56 owns the HTTP authentication and role-check boundary, issue #64 owns
the durable user/role and credential model, and issue #65 owns listener TLS and
safe remote activation. Until those land, the separate loopback listeners are
an exposure boundary rather than a production security boundary.
