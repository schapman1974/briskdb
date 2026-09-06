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
addresses. The current HTTP planes have no complete identity, authorization,
or TLS boundary, so each configured address must be loopback. A non-loopback
data or administration address is rejected before the database opens or any
listener binds.

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
#58 and #59. Issue #55 owns OpenAPI.
Issue #56 owns the HTTP authentication and role-check boundary, issue #64 owns
the durable user/role and credential model, and issue #65 owns listener TLS and
safe remote activation. Until those land, the separate loopback listeners are
an exposure boundary rather than a production security boundary.
