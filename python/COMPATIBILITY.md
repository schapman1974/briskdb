# Python compatibility

## Supported wheels

| Runtime | Platform tag | Architectures | Status |
|---|---|---|---|
| CPython 3.9–3.14 | `manylinux_2_28` (glibc) | x86-64, ARM64 | Built, audited, installed, and tested on native Linux runners |
| CPython 3.9–3.14 | macOS 11+ | Intel x86-64, Apple Silicon ARM64 | Built, dependency-inspected, installed, and tested on native macOS runners |
| CPython 3.9–3.14 | `musllinux`/Alpine | — | No wheel; source builds are untested and unsupported in this alpha |
| PyPy or free-threaded CPython | — | — | Unsupported |

One `cp39-abi3` wheel per platform supports the stated CPython range. A local
Rust compiler is not used when installing those wheels. The sdist is tested
separately and requires Rust 1.85 or newer.

## Version parity

The `briskdb-python` crate version must exactly equal the root `briskdb` Rust
crate version. Python metadata and `briskdb.__version__` use the equivalent PEP
440 spelling (`0.1.0-alpha.4` becomes `0.1.0a4`). Release automation rejects a
tag or artifact when those versions are not equivalent. A Python alpha package
supports only its exact bundled Rust engine; mixing an extension and core from
different releases is unsupported.

Before 1.0, Python APIs and type declarations may change between alpha/minor
releases. Breaking changes belong in `CHANGELOG.md` and the repository release
notes.

## Storage compatibility

The wheel uses the same files, manifest version, migrations, downgrade fence,
and recovery rules as the matching Rust release. There is no separate Python
storage format and no stable pre-1.0 compatibility promise.

Independently spawned interpreters may concurrently read and write one ready
root on the same supported host and local filesystem. Each interpreter must
open and close its own handle. Inherited post-`fork()` handles and network or
multi-host filesystems are unsupported. Schema/catalog/layout changes require
sole-process ownership and return retryable `BusyError` while a peer remains
open. The complete boundary and systemd account requirements are documented in
[sharing one data directory between processes](../docs/MULTIPROCESS.md).

Before upgrading a wheel that opens existing data, stop every user of the data
directory and retain a complete stopped-database backup. Startup can migrate
the directory. In-place downgrade is unsupported; rollback means restoring the
complete pre-upgrade backup. See the repository's
[pre-1.0 policy](../docs/PRE_1_COMPATIBILITY.md).
