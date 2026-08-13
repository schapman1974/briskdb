# BriskDB 0.1.0-alpha.2

This is the second BriskDB alpha release. It is intended for local evaluation
and development, not production deployment.

This update adds native `amd64` and `arm64` Debian packages. Installing a
package creates an unprivileged `briskdb` service account, installs a hardened
`briskdb.service`, keeps administrator configuration in
`/etc/default/briskdb`, stores database state in `/var/lib/briskdb`, and sends
stdout/stderr logs to the systemd journal.

## Included binaries

Each archive contains `briskdb`, `briskdb-import`, this file, `README.md`, the
MIT license, and the repository documentation. Native archives are provided
for:

- Ubuntu 24.04 x86-64;
- Ubuntu 24.04 ARM64;
- macOS Intel x86-64; and
- macOS Apple Silicon ARM64.

Ubuntu 24.04 x86-64 is the only full-suite CI-supported platform. The other
archives are preview builds that are compiled and startup-smoke-tested on native
GitHub-hosted runners. Verify downloads against `SHA256SUMS`.

The Linux release assets also contain `briskdb_0.1.0.alpha.2-1_amd64.deb` and
`briskdb_0.1.0.alpha.2-1_arm64.deb`. Each package is installed, started through
systemd, queried over loopback HTTP, checked through journald, reinstalled with
a locally modified conffile, and removed while retaining configuration and
database state on its native Ubuntu 24.04 release runner.

## What is available

- A protocol-neutral asynchronous engine over multiple bundled SQLite files.
- Deterministic keyed routing, bounded scatter reads, connection pools,
  cancellation, deadlines, result budgets, and graceful shutdown.
- An HTTP SQL interface and embedded read-only admin browser on loopback.
- Versioned manifest migrations, generated-key policies, prepared statements,
  standard SQLite import, and a tested stopped-server backup/restore procedure.
- A disabled-by-default PostgreSQL endpoint for protocol startup and session
  handling only.

## Critical alpha boundaries

- HTTP has no authentication, authorization, or TLS and is restricted to
  loopback. Do not expose it to a network.
- PostgreSQL cannot execute SQL yet. It is disabled by default and, when
  explicitly enabled on loopback, supports only startup/session handling.
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
`0.1.0-alpha.1`.
