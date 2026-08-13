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

The supported runner tests separate Tokio TCP listeners on numeric IPv4
loopback addresses, concurrent HTTP and PostgreSQL startup sessions, disabled
PostgreSQL binding, bind-failure cleanup, and shutdown of both listeners.
Numeric IPv6 `SocketAddr` parsing is part of the CLI contract; an individual
host must still provide the requested address family. The PostgreSQL endpoint
currently supports startup and session lifecycle but not SQL execution. Its
exact process behavior is documented in
[PostgreSQL listener lifecycle](POSTGRES_LISTENER.md).

The selected PostgreSQL library is pinned to `pgwire` 0.36.3 with only its
`server-api` feature. It is the newest release declaring compatibility with the
project's Rust 1.85 minimum; 0.37 and newer require Rust 1.89. CI compiles the
locked adapter/core compatibility probe on Rust 1.85 and stable. This library
selection is active only behind BriskDB-owned startup, status, error, and
session-lifecycle policy; see the [adapter decision record](POSTGRES_ADAPTER.md).

The same runner exercises the embedded `/admin` shell and assets, temporary
login/session lifecycle, physical-shard table discovery, and bounded row-page
JSON contract without contacting third-party asset hosts. The all-target test
suite also uses the pinned runner's Node.js executable to syntax-check both
embedded scripts and run the pure display/authentication-order logic tests.
Node.js is a development-test dependency, not a BriskDB runtime or frontend
build dependency. These are HTTP, content, and logic contract tests, not a named
desktop/mobile browser compatibility matrix, visual-regression suite, or
accessibility certification. The interface uses ordinary HTML, CSS, browser
JavaScript, and same-origin requests; an unlisted browser remains best-effort
until it has repeatable automated coverage. See the
[admin data-browser contract](ADMIN_BROWSER.md).

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

Only a single BriskDB server process per data directory is supported. The
process-wide registry makes independent `Database` and `Engine` handles that
resolve to the same canonical root share schema admission and catalog
publication. It does not coordinate separate server processes.
Admin browser sessions likewise belong to one process, but are not part of that
data-directory registry: their absolute eight-hour state is memory-only and is
discarded on restart.
Do not copy, move, edit, or separately open the manifest, shard, WAL, or shared
memory files while the server is running. The stopped-server,
complete-directory procedure in [offline backup](OFFLINE_BACKUP.md) is supported
and tested. Live or partial copies, coordinated online backup, multi-process
access, filesystem-fault behavior, and broader crash-recovery guarantees remain
unsupported until their roadmap issues add corresponding automated tests.

Opening a data directory can transactionally upgrade `manifest.sqlite` and can
resume a manifest-recorded cross-file shard provisioning, adoption, or
application-schema migration step; see the
[manifest storage-format contract](STORAGE_FORMAT.md). After manifest load or
upgrade, an active schema migration is resumed before ordinary layout
reconciliation and final strict shard validation. All startup work finishes
before either server listener accepts requests. Outside the explicit
`Creating` state, every shard is opened read-write with SQLite create and
symbolic-link following disabled. A missing, extra canonical, swapped,
foreign, non-WAL, or
wrong-generation shard file is rejected, as is a shard cloned into another slot
or layout. It is not recreated, reassigned, or silently reconfigured. Recovery
requires restoring the correct complete layout.

The manifest final path component is likewise required to be a regular file.
Startup and runtime migration opens disable symbolic-link following; a runtime
open will not create a missing manifest and rechecks the opened layout identity
before journal mutation.

Manifest v8 retains the v6 journal's exact migration SQL for idempotency and
startup recovery. Treat the data directory accordingly and do not include
credentials, tokens, or other sensitive literals in migration SQL.

Manifest and shard application IDs plus the random 16-byte layout ID guard
against accidental wrong-file placement. They are not authentication or
protection from a process that can write the data directory. The semantic and
schema checksums introduced in v7 and retained by v8 are likewise unkeyed
corruption detectors, not authentication, and do not checksum application row
values. Targeted
subprocess-abort tests cover schema-journal persistence boundaries, but
arbitrary process-kill timing, power-loss, and filesystem-fault certification
remain later hardening work.

When reporting a platform problem, include the BriskDB revision, `rustc -Vv`,
operating-system and architecture details, filesystem type, mount options, and
whether the data directory is local or remote.
