# Supported platforms

BriskDB is an experimental database server. “Supported” currently means that a
target is continuously built and tested and that a regression blocks a pull
request. It does not mean that BriskDB is production-ready or covered by a
service-level agreement.

## CI-supported target

The current support contract is intentionally narrow:

- `x86_64-unknown-linux-gnu` on Ubuntu 24.04;
- Rust 1.85, the declared minimum supported Rust version (MSRV); and
- the latest stable Rust release.

Every pull request must pass formatting, Clippy, unit and integration tests,
documentation tests, and benchmark correctness smoke tests on the pinned
Ubuntu 24.04 GitHub-hosted runner. BriskDB uses rusqlite's bundled SQLite build,
so the host's system SQLite version is not part of this contract.

The MSRV may only be raised deliberately, with documentation and CI changed in
the same pull request. A supported operating-system runner may be replaced
before its upstream end of life, also through a tested pull request and policy
update.

The server installs SIGINT and SIGTERM streams before reporting readiness on
Unix. A Windows build installs its Ctrl-C stream at the same point, although
Windows is not currently in the supported tier. Signal handling enters the same
tested `Engine` drain/cancel/cleanup lifecycle; platform-specific
service-manager integration beyond those signals is not yet part of the support
contract.

## Development-tested targets

Maintainers also develop and run the full suite on `aarch64-apple-darwin`.
macOS is useful for development and benchmark comparisons, but it is
best-effort until it has a required CI job. Linux ARM64, Intel macOS, Windows,
the musl target, 32-bit targets, big-endian targets, mobile platforms, and
WebAssembly are not currently tested or supported.

Reports from unlisted targets are welcome. A target moves into the supported
tier only after repeatable CI coverage exists for its compile, test, and
storage behavior.

## Filesystem and deployment boundary

All BriskDB data files must reside on storage local to the server host with
working file locks and durable synchronization semantics. Fresh shard
provisioning in `Creating` enables SQLite WAL mode; every `Adopting`, `Ready`,
and runtime shard open requires the persisted mode to already be WAL and does
not silently repair another mode. Every connection uses `synchronous=FULL`.
Network filesystems such as NFS or SMB, cloud-synchronized folders, userspace
filesystems with unverified locking, and sharing one data directory between
hosts are unsupported.

WAL, shared-memory, and rollback-journal sidecars may be absent and are not
layout members by themselves. The directory must nevertheless permit SQLite to
create and recover its sidecars. Do not infer damage from a missing sidecar;
BriskDB validates the database's persistent journal mode instead.

Only a single BriskDB server process per data directory is currently tested.
Do not copy, move, edit, or separately open the manifest, shard, WAL, or shared
memory files while the server is running. Backup, restore, multi-process
access, filesystem-fault behavior, and crash-recovery guarantees will become
supported only when their roadmap issues add the corresponding automated
tests.

Opening a data directory can transactionally upgrade `manifest.sqlite` and can
resume a manifest-recorded cross-file shard provisioning or adoption step; see
the [manifest storage-format contract](STORAGE_FORMAT.md). Outside the explicit
`Creating` state, every shard is opened read-write with SQLite create and
symbolic-link following disabled. A missing, extra canonical, swapped, foreign,
non-WAL, or wrong-generation shard file is rejected, as is a shard cloned into
another slot or layout. It is not recreated, reassigned, or silently
reconfigured. Recovery requires restoring the correct complete layout.

Manifest and shard application IDs plus the random 16-byte layout ID guard
against accidental wrong-file placement. They are not authentication, checksums,
or protection from a process that can write the data directory. Process-kill,
power-loss, and filesystem-fault certification remains later hardening work.

When reporting a platform problem, include the BriskDB revision, `rustc -Vv`,
operating-system and architecture details, filesystem type, mount options, and
whether the data directory is local or remote.
