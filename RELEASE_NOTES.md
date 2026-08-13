# BriskDB 0.1.0-alpha.3

This is the third BriskDB alpha release. It is intended for local evaluation
and development, not production deployment.

This update makes the disabled-by-default PostgreSQL listener query-capable for
the first time. After initializing a registered catalog with `briskdb-import`,
a standard PostgreSQL simple-query client can execute one `SELECT`, `INSERT`,
`UPDATE`, or `DELETE` statement at a time through BriskDB's shared catalog,
shard routing, limits, session lifecycle, and fixed-error boundary.

## PostgreSQL quickstart

The complete copy/paste workflow for creating a SQLite source, importing it,
starting BriskDB, and using `psql` is in
`docs/POSTGRES_QUICKSTART.md`. The listener remains disabled by default. Enable
it only on loopback:

```bash
briskdb \
  --data-dir /path/to/imported-briskdb-data \
  --shards 4 \
  --postgres-listen 127.0.0.1:5433
```

Psycopg 3 has been exercised against the standalone alpha.3 binaries. Use
`psycopg.ClientCursor`, which performs client-side parameter binding and sends
the supported simple-query protocol:

```python
import psycopg
from psycopg import ClientCursor

connection = psycopg.connect(
    "host=127.0.0.1 port=5433 user=briskdb "
    "dbname=default sslmode=disable",
    autocommit=True,
)

with ClientCursor(connection) as cursor:
    cursor.execute(
        "INSERT INTO records (tenant_id, payload) VALUES (%s, %s)",
        ("tenant-a", "hello"),
    )
    cursor.execute(
        "SELECT tenant_id, payload FROM records WHERE tenant_id = %s",
        ("tenant-a",),
    )
    print(cursor.fetchone())
```

The ordinary Psycopg cursor uses PostgreSQL's extended-query protocol and is
not compatible with this release.

## Included binaries

Each archive contains `briskdb`, `briskdb-import`, this file, `README.md`, the
MIT license, and the repository documentation. Native archives are provided
for:

- Ubuntu 24.04 x86-64;
- Ubuntu 24.04 ARM64;
- macOS Intel x86-64; and
- macOS Apple Silicon ARM64.

Ubuntu 24.04 x86-64 is the only full-suite CI-supported platform. The other
archives are preview builds compiled and startup-smoke-tested on native
GitHub-hosted runners. Verify downloads against `SHA256SUMS`.

Linux release assets also contain
`briskdb_0.1.0.alpha.3-1_amd64.deb` and
`briskdb_0.1.0.alpha.3-1_arm64.deb`. Each package installs an unprivileged
`briskdb` account, hardened systemd service, administrator configuration under
`/etc/default/briskdb`, persistent state under `/var/lib/briskdb`, and journald
logging. Native Ubuntu release runners install, start, query, reinstall, and
remove each package while verifying configuration and database retention.

## What is available

- A protocol-neutral asynchronous engine over multiple bundled SQLite files.
- Deterministic keyed routing, bounded scatter reads, connection pools,
  cancellation, deadlines, result budgets, and graceful shutdown.
- An HTTP SQL interface and embedded read-only admin browser on loopback.
- Versioned manifest migrations, generated-key policies, prepared statements,
  standard SQLite import, and a tested stopped-server backup/restore procedure.
- A disabled-by-default loopback PostgreSQL endpoint with protocol 3.0 startup,
  registered-table simple queries, text-format rows, DML command tags, fixed
  safe SQLSTATE errors, recovery, and prepared-object cleanup.
- Native archives for four OS/architecture combinations and native Debian
  packages for Ubuntu 24.04 `amd64` and `arm64`.

## Critical alpha boundaries

- There is no authentication, authorization, or TLS. HTTP and PostgreSQL are
  restricted to loopback. Do not expose either listener to a network.
- PostgreSQL extended-query protocol is unsupported. Parameters sent through
  Parse/Bind/Execute, server-side prepared statements, transactions, DDL,
  `COPY`, and binary results are unavailable. Psycopg must use
  `psycopg.ClientCursor`; an ordinary cursor's unsupported pipelined extended
  sequence closes that connection.
- PostgreSQL accepts exactly one simple-query statement per message and only
  operates on an offline imported/registered catalog. It does not provide an
  online `CREATE TABLE` workflow or full PostgreSQL compatibility.
- General cross-shard transactions are unsupported. Global ordering,
  pagination, and aggregation pushdown are incomplete, and BriskDB does not
  claim full SQL compatibility.
- Backups require a stopped server and a complete data-directory copy. Online
  backup/restore, resharding, and online rebalance are unsupported.
- There is no production metrics or observability suite.

## Storage compatibility

There is no stable pre-1.0 on-disk compatibility promise. This release writes
manifest version 12 and accepts the exact documented legacy version-1 shape and
manifest versions 2 through 11 for automatic, ordered forward migration.
Unknown, malformed, partially migrated, or newer layouts fail closed.

Before opening existing data, stop the old process and make a complete backup
as described in `docs/OFFLINE_BACKUP.md`. Startup may migrate the data.
In-place downgrade is unsupported; rollback requires restoring the complete
pre-upgrade backup. This release has no on-disk format change from
`0.1.0-alpha.1` or `0.1.0-alpha.2`.
