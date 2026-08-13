# BriskDB 0.1.0-alpha.1

This is the first BriskDB alpha release. It is intended for local evaluation
and development, not production deployment.

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
pre-upgrade backup. This first published release has no on-disk format change
relative to the source revision from which it was cut.
