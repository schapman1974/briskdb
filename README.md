# BriskDB

[![CI](https://github.com/schapman1974/briskdb/actions/workflows/ci.yml/badge.svg)](https://github.com/schapman1974/briskdb/actions/workflows/ci.yml)
[![Release](https://github.com/schapman1974/briskdb/actions/workflows/release.yml/badge.svg)](https://github.com/schapman1974/briskdb/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/briskdb?label=PyPI&color=56e0ac)](https://pypi.org/project/briskdb/)
[![License: MIT](https://img.shields.io/badge/license-MIT-56e0ac.svg)](LICENSE)

> **SQLite files. One sharded database.**

BriskDB turns ordinary SQLite files into one database with **parallel writes,
PostgreSQL compatibility, HTTP access, and embedded Rust/Python APIs**. It keeps
SQLite's proven storage engine and tooling; BriskDB adds the routing layer,
shard-safe IDs, cross-shard indexes, protocols, and operational guardrails.

<p align="center">
  <img src="docs/assets/briskdb-demo.gif" alt="BriskDB demo: four Python writer threads writing through one engine into four ordinary SQLite WAL shards, with HTTP and PostgreSQL listeners" width="900">
</p>

| The useful part | What it means |
| --- | --- |
| **Parallel SQLite writes** | Independent shard files have independent WAL writer locks. |
| **Use existing clients** | PostgreSQL and HTTP work today; MongoDB and MySQL are next. |
| **Embed or run a service** | The same Rust engine powers the binary, Python wheel, and Rust crate. |
| **Keep inspectable files** | Every data shard remains a normal SQLite database—no SQLite fork. |

[Try it without a compiler](#try-it-in-30-seconds) ·
[Download an alpha](https://github.com/schapman1974/briskdb/releases) ·
[Open the data browser](#browse-the-whole-logical-database) ·
[Follow MongoDB and MySQL](#follow-the-build)

> [!IMPORTANT]
> BriskDB is an alpha, not a production-ready database service. The
> [boundaries are explicit](#honest-alpha-boundaries), and measured results are
> published even when they are not flattering.

## Why developers might care

- **No SQLite fork.** Each shard is an ordinary SQLite WAL database that normal
  tools can inspect.
- **No central write lock.** Writes to different shards use different WALs and
  can progress in parallel.
- **No central ID write per row.** Native range and hi/lo allocation provide
  collision-free generated IDs across shards and processes.
- **Safe cross-shard pruning.** Global uniqueness is authoritative; asynchronous
  indexes use verification, watermarks, Bloom filters, and min/max summaries so
  an optimization cannot silently hide a row.
- **One engine everywhere.** PostgreSQL, HTTP, Rust, and Python share routing,
  limits, cancellation, errors, and storage behavior.
- **Operations are visible.** `/health`, `/metrics`, admin JSON, and Rust status
  reports expose lag, repairs, rebuilds, contention, and outbox pressure.

## One engine, many ways in

```mermaid
flowchart LR
    subgraph Clients
        WEB[Browser + HTTP]
        PG[PostgreSQL clients]
        MONGO[MongoDB clients · planned]
        MYSQL[MySQL clients · planned]
        RUST[Rust embedding]
        PY[Python embedding]
    end

    WEB --> ENGINE
    PG --> ENGINE
    MONGO -.-> ENGINE
    MYSQL -.-> ENGINE
    RUST --> ENGINE
    PY --> ENGINE

    ENGINE[Protocol-neutral Rust engine] --> ROUTER[4,096 virtual buckets]
    ROUTER --> S0[(SQLite WAL · shard 0)]
    ROUTER --> S1[(SQLite WAL · shard 1)]
    ROUTER --> S2[(SQLite WAL · shard 2)]
    ROUTER --> SN[(SQLite WAL · shard N)]
```

The protocol adapters do not own database semantics. Routing, limits,
cancellation, values, sessions, and execution live in the shared Rust engine,
leaving room for more protocols and storage adapters later.

## Browse the whole logical database

![BriskDB data browser showing one logical table across four SQLite shards](docs/assets/admin-browser.svg)

BriskDB serves a responsive, read-only data browser at `/admin`. It uses the
same bounded HTTP engine paths as other clients, combines sharded rows into one
logical view, reads global tables once, and preserves large integer values.

For the current local alpha:

```text
http://127.0.0.1:7654/admin
username: admin
password: admin
```

The temporary credentials are a development convenience—not a security
boundary—which is why the server currently refuses non-loopback HTTP addresses.

## The unusual part: shard-safe generated IDs

BriskDB has two generated-ID designs for sharded tables:

- **`native_range_v1`** gives every shard a non-overlapping positive 64-bit
  range. SQLite's own `INTEGER PRIMARY KEY AUTOINCREMENT` performs the actual
  allocation locally, with no central write for each inserted row.
- **`hilo_v1`** durably leases blocks of 4,096 IDs from the manifest, then
  allocates in memory and hash-routes each ID. Crashes may leave gaps, but an ID
  is never reused.

Both policies are versioned in the manifest. Generated-key execution is still
experimental and opt-in; the exact contract lives in
[Generated keys](docs/GENERATED_KEYS.md).

## What works now

| Capability | Alpha status |
| --- | --- |
| Durable virtual-bucket routing over independent SQLite WAL files | Working |
| Exact-key routing and bounded scatter/gather reads | Working |
| HTTP query/write API and admin data browser | Working, loopback-only |
| PostgreSQL wire protocol | TLS/SCRAM, backpressured row streaming, SQLite-interrupt cancellation, text/binary CRUD, real single-shard transactions, and a live psql/tokio-postgres/psycopg/SQLAlchemy matrix |
| Offline import from a standard SQLite database | Working |
| Native-range and hi/lo generated IDs | Experimental, opt-in |
| Cross-shard indexes and global value leases | Experimental/opt-in: correctness, recovery, and shard pruning pass; current latency/write overhead is documented in the release gate |
| Global-index health and Prometheus metrics | `/health`, `/v1/admin/global-indexes`, `/metrics`, plus Rust operational reports |
| Ubuntu/macOS x86-64 and ARM64 release artifacts | Published |
| Debian package and hardened systemd service | Published |
| Rust library entrypoint with optional attached listeners | Working |
| Same-host service and embedded processes sharing one ready root | Working on local filesystems |
| Native MongoDB wire protocol with TinyMongo parity | [Planned](https://github.com/schapman1974/briskdb/issues/160) |
| MySQL wire protocol | [Planned](https://github.com/schapman1974/briskdb/issues/40) |
| Native Python extension | Sync/async API working; tagged releases build audited macOS/Linux ARM/x86 wheels |
| Serverless lifecycle | [Planned](https://github.com/schapman1974/briskdb/issues/194) |

## Where BriskDB fits

These projects solve different problems. This table is a compass, not a
benchmark scoreboard.

| Project | Built for | Write model | Access | Storage shape |
| --- | --- | --- | --- | --- |
| **BriskDB** | Same-host sharding, service + embedding | Parallel across independent shard WALs | PostgreSQL, HTTP, Rust, Python | Manifest + ordinary SQLite shard files |
| [SQLite](https://sqlite.org/wal.html) | Small, embedded, single-file databases | One writer per WAL file | SQLite API and ecosystem | One ordinary SQLite file |
| [rqlite](https://rqlite.io/docs/features/) | Simple multi-node availability | Writes flow through a Raft log; optimized for HA, not write scaling | HTTP + client libraries | Replicated SQLite state across nodes |
| [Turso / libSQL](https://docs.turso.tech/sdk/introduction) | Cloud/edge access and local-first sync | Product-dependent primary or local push/pull model | SDKs + HTTP | Turso Database or legacy SQLite-compatible libSQL |
| [Citus](https://www.postgresql.org/about/news/citus-120-released-2687/) | Mature distributed PostgreSQL | Parallel across PostgreSQL worker shards | PostgreSQL | PostgreSQL coordinator + worker cluster |

Choose BriskDB when you want one local service or embedded engine to spread
write contention across inspectable SQLite files while speaking familiar
database protocols. Choose the others when a single SQLite file, replicated
high availability, managed edge sync, or a mature multi-node PostgreSQL cluster
is the real requirement.

## Try it in 30 seconds

Install the published native wheel—no clone and no Rust compiler:

```bash
python -m pip install --only-binary=:all: briskdb
curl -fsSLO https://raw.githubusercontent.com/schapman1974/briskdb/main/examples/launch_demo.py
python launch_demo.py
```

The demo makes 32 routed writes from four Python threads, proves that all four
ordinary SQLite shard files received rows, reads every row back, checks HTTP
health, and starts the PostgreSQL listener. It uses a temporary directory and
cleans up after itself. The GIF renderer executes this exact scenario, and CI
tests it against every published wheel target.

To run the standalone service, download the matching macOS/Linux ARM64 or
x86-64 archive from the [latest GitHub release](https://github.com/schapman1974/briskdb/releases),
then:

```bash
./briskdb --data-dir ./briskdb-data --shards 4
```

Open the [data browser](http://127.0.0.1:7654/admin) or inspect the service:

```bash
curl http://127.0.0.1:7654/health
curl http://127.0.0.1:7654/metrics
```

Enable the PostgreSQL listener explicitly. Simple and parameterized
text/binary prepared queries share the same bounded engine path:

```bash
./briskdb --data-dir ./briskdb-data --postgres-listen 127.0.0.1:5433
psql -h 127.0.0.1 -p 5433 -d default
```

That local development form is unauthenticated and therefore loopback-only.
The [PostgreSQL quickstart](docs/POSTGRES_QUICKSTART.md) shows the four settings
for TLS plus SCRAM-SHA-256; secure mode is required for any remote bind.

Registered tables can also be queried over HTTP:

```bash
curl -X POST http://127.0.0.1:7654/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT id, name FROM widgets WHERE id = ?1","params":["widget-1"]}'
```

Have an existing SQLite database? Use the offline
[SQLite importer](docs/SQLITE_IMPORT.md). Linux releases also include `.deb`
packages with a hardened systemd service.

Embedding in Rust starts with `BriskDb::open()` or the validated builder. The
[embedded Rust guide](docs/EMBEDDED_RUST.md) includes a complete listener-free
example. Choose a shard count when creating data; later opens detect it from
the manifest and reject explicit mismatches. Use `default-features = false` with the
`embedded` feature to leave the network and CLI stacks out; see the
[crate feature map](docs/CRATE_FEATURES.md).

Python runs the same engine directly in-process. It starts no listener by
default, but `Database.serve()` can attach HTTP/PostgreSQL listeners (remote
PostgreSQL requires its TLS/SCRAM arguments):

```python
with briskdb.open("./data", shards=4) as db:
    with db.serve(postgres="127.0.0.1:0") as server:
        print(server.http_address, server.postgres_address)
```

See the [Python quickstart](python/README.md) for sync and asyncio write/read
examples. Tagged releases publish compiler-free `cp39-abi3` wheels for the
[supported platform matrix](python/COMPATIBILITY.md); repository checkouts can
still be installed from source with Rust 1.85+. Independently spawned Python,
Rust, and server processes can share a ready local data directory; read the
[multi-process contract](docs/MULTIPROCESS.md) before deploying that pattern.

## Still just inspectable files

```text
briskdb-data/
├── .briskdb-process.lock
├── .briskdb-startup.lock
├── manifest.sqlite
├── global-indexes/
│   └── global.sqlite
└── shards/
    ├── 0000.sqlite
    ├── 0001.sqlite
    ├── 0002.sqlite
    └── 0003.sqlite
```

The manifest versions routing, catalogs, migrations, generated-ID ownership,
and integrity metadata. Application rows stay in ordinary SQLite files.

## Where this is going

- **MongoDB:** a native Rust Mongo listener with BSON, queries, updates,
  indexes, cursors, aggregation, and differential TinyMongo parity.
- **More wire protocols:** broader PostgreSQL client compatibility and a MySQL
  listener, all sharing the same engine behavior.
- **Serverless storage:** atomic snapshots, object-store adapters, and fenced
  single-writer operation beyond today's embedded warm-handler pattern.
- **Future storage adapters:** SQLite is the first backend, while the engine
  boundaries are being kept reusable for other durable backends.

Follow the [roadmap](ROADMAP.md) or browse the
[open issues](https://github.com/schapman1974/briskdb/issues).

## Follow the build

Star BriskDB if you want to follow any of these bets:

- a native MongoDB wire protocol with large-app TinyMongo parity;
- MySQL compatibility over the same protocol-neutral Rust engine;
- serverless snapshots and object-store-backed lifecycle;
- more storage backends without giving up the ordinary SQLite option; or
- honest benchmark and failure evidence as the alpha becomes a real release.

If you try it, an issue with your client, workload, or missing SQL shape is even
more valuable than a star. Start with the
[alpha releases](https://github.com/schapman1974/briskdb/releases), then tell us
[what broke or what surprised you](https://github.com/schapman1974/briskdb/issues/new/choose).

## Honest alpha boundaries

- PostgreSQL has TLS and single-identity SCRAM-SHA-256 authentication, but no
  roles or authorization yet. HTTP remains a loopback-only development surface.
- No general atomic transaction across multiple shard files.
- Global ordering/pagination and general aggregate pushdown are still limited.
- The supported backup today is a stopped-server copy of the complete data
  directory after every server and embedder exits. Passive checkpoints now
  report shards, manifest, and global-index storage, but are not an online
  snapshot; online/serverless snapshots are planned.
- Multi-process access is same-host/local-filesystem only. Schema, catalog,
  upgrade, and recovery work requires sole-process ownership.
- Pre-1.0 storage and public-library compatibility can change between releases.
- Ubuntu 24.04 x86-64 receives the full required Rust CI suite. Python wheels
  receive native build, audit, install, restart, corruption, and concurrency
  checks on Linux/macOS x86-64 and ARM64.
- Global-index operational metrics are available, but BriskDB still lacks the
  broader production suite for traces, slow-query logs, resource saturation,
  alert rules, and long-running capacity validation.

## Go deeper

- [Architecture](docs/ARCHITECTURE.md)
- [Global uniqueness and value authority](docs/GLOBAL_INDEX_AUTHORITY.md)
- [Global-index production gate](docs/GLOBAL_INDEX_RELEASE_GATE.md)
- [Embedded Rust](docs/EMBEDDED_RUST.md)
- [Embedded SQL](docs/EMBEDDED_SQL.md)
- [Crate features and support tiers](docs/CRATE_FEATURES.md)
- [PostgreSQL quickstart](docs/POSTGRES_QUICKSTART.md)
- [Tested PostgreSQL clients](docs/POSTGRES_CLIENTS.md)
- [SQL compatibility](docs/SQL_COMPATIBILITY.md)
- [Generated keys](docs/GENERATED_KEYS.md)
- [Storage format](docs/STORAGE_FORMAT.md)
- [Sharing one data directory between processes](docs/MULTIPROCESS.md)
- [Debian and systemd installation](docs/DEBIAN_INSTALL.md)
- [Pre-1.0 compatibility policy](docs/PRE_1_COMPATIBILITY.md)
- [Contributing](CONTRIBUTING.md)

BriskDB is available under the [MIT License](LICENSE).
