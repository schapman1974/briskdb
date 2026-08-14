# Changes after 0.1.0-alpha.4

- Independent server, Rust, and Python processes may concurrently open one
  ready data root on the same Linux or macOS host and local filesystem.
- Schema, catalog, initialization, upgrade, and recovery work requires
  sole-process ownership and returns retryable `Busy` while a peer is open.
- Each process must open its own handle. Inherited live handles after `fork()`,
  network filesystems, multi-host volumes, and online backup remain unsupported.
- Installed-wheel and Rust subprocess tests cover overlapping writes,
  checkpoints, generated IDs, abrupt exit, recovery, and service-plus-embedded
  operation. See `docs/MULTIPROCESS.md`.

# BriskDB 0.1.0-alpha.4

Alpha 4 makes BriskDB usable as an in-process Rust or Python database, while
keeping the standalone HTTP/PostgreSQL server and Debian service from alpha 3.
It is intended for local evaluation and development, not production deployment.

## Install from PyPI

Compiler-free `cp39-abi3` wheels support CPython 3.9 through 3.14 on Linux
x86-64/ARM64 (`manylinux_2_28`) and macOS Intel/Apple Silicon (macOS 11+):

```bash
python -m pip install briskdb==0.1.0a4
```

```python
import briskdb

with briskdb.connect("./data", shards=4) as db:
    with db.session(routing_key="account-1") as session:
        session.migrate(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
        )
        session.execute("INSERT INTO notes VALUES (?1, ?2)", [1, "hello"])
        print(session.query("SELECT body FROM notes WHERE id = ?1", [1]))
```

The typed package includes synchronous context managers and an asyncio facade.
It runs the native Rust engine in the Python process without a listener,
subprocess, signal handler, or global logger. Async task cancellation reaches
the engine's native cancellation token.

## Embedded Rust library

The root crate now separates its listener-free engine from optional HTTP,
PostgreSQL, importer, and CLI layers. Downstream Rust applications can select
only the embedded API:

```toml
[dependencies]
briskdb = { git = "https://github.com/schapman1974/briskdb", tag = "v0.1.0-alpha.4", default-features = false, features = ["embedded"] }
```

`BriskDb` and owned `BriskSession` handles expose initialization, migrations,
prepared statements, routed SQL execution, checkpoints, cancellation,
deadlines, bounded results, and graceful close without installing process-wide
runtime behavior.

## Standalone distributions

The release also provides `briskdb` and `briskdb-import` archives for Ubuntu
24.04 x86-64/ARM64 and macOS Intel/Apple Silicon. Linux assets include systemd
`.deb` packages for `amd64` and `arm64`. Each package installs an unprivileged
`briskdb` account, administrator configuration under `/etc/default/briskdb`,
persistent state under `/var/lib/briskdb`, and journald logging.

The disabled-by-default PostgreSQL listener supports one registered-table
simple-query `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement at a time.
Psycopg 3 clients must use `psycopg.ClientCursor`; the ordinary cursor uses the
unsupported extended-query protocol. See `docs/POSTGRES_QUICKSTART.md`.

Every native archive and wheel is built and smoke-tested on its matching native
GitHub runner. Wheels are installed and tested under CPython 3.9 and 3.14, and
native dependencies are audited. Verify downloads against `SHA256SUMS` and the
GitHub build-provenance attestation.

## Critical alpha boundaries

- There is no authentication, authorization, or TLS. HTTP and PostgreSQL are
  restricted to loopback. Do not expose either listener to a network.
- PostgreSQL extended-query protocol is unsupported. Parameters sent through
  Parse/Bind/Execute, server-side prepared statements, transactions, DDL,
  `COPY`, and binary results are unavailable. Psycopg must use
  `psycopg.ClientCursor`.
- PostgreSQL accepts exactly one simple-query statement per message and only
  operates on an offline imported/registered catalog. It does not provide an
  online `CREATE TABLE` workflow or full PostgreSQL compatibility.
- General cross-shard transactions are unsupported. Global ordering,
  pagination, and aggregation pushdown are incomplete, and BriskDB does not
  claim full SQL compatibility.
- Backups require a stopped server and a complete data-directory copy. Online
  backup/restore, resharding, and online rebalance are unsupported.
- There is no production metrics or observability suite.
- The Python package does not claim DB-API 2.0 compatibility, transaction
  methods, retained SQLite streaming cursors, or native document operations.

## Storage compatibility

There is no stable pre-1.0 on-disk compatibility promise. This release writes
manifest version 12 and accepts the exact documented legacy version-1 shape and
manifest versions 2 through 11 for automatic, ordered forward migration.
Unknown, malformed, partially migrated, or newer layouts fail closed.

Before opening existing data, stop every process and make a complete backup as
described in `docs/OFFLINE_BACKUP.md`. Startup may migrate the data.
In-place downgrade is unsupported; rollback requires restoring the complete
pre-upgrade backup. This release has no on-disk format change from alpha 1,
alpha 2, or alpha 3.
