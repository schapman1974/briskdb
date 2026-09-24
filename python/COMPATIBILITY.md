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

## Remote SQLite host requirements

`attach_remote` additionally requires the Python interpreter's own SQLite
library to be **3.31 or newer** and built with loadable-extension support.
Some system Python builds (notably macOS builds without extension loading)
cannot use this addon; use a compatible interpreter. Embedded BriskDB still
works independently of this host capability.

The native bridge dispatches exclusively through the host's extension API.
It never passes a host connection to BriskDB's bundled SQLite library. Multiple
connections to the same host library are supported; mixing different host
SQLite API tables in one process is rejected. The wheel is a Python addon,
not a standalone SQLite CLI extension. Wheel CI exercises actual stdlib
`sqlite3` loading and network reads on Linux Python 3.9 and 3.14, and on
extension-enabled Homebrew Python 3.14 for both macOS architectures. These jobs
require addon execution; they cannot pass by skipping it. The python.org macOS
interpreters still run the embedded suite and verify explicit addon capability
rejection when their SQLite loader is absent. The Linux source-distribution
tests also exercise the addon. A wheel tag alone does not guarantee that a
particular interpreter's host SQLite supports extension loading.

## Optional BSON dependency

SQL-only applications need no BSON package. The wheel does not declare a
mandatory runtime dependency on `bson` or PyMongo, and importing BriskDB, opening a
database, and executing SQL do not import either package. Calling a document
method without the optional package raises `UnsupportedError` with code
`unsupported` before the command can mutate storage; the same database remains
usable for SQL afterward.

Applications using `documents=True` should install PyMongo, which supplies the
supported `bson` package. The unrelated package named `bson` on PyPI is not a
supported substitute. The compatibility and release suites pin PyMongo 4.17.0
and test its BSON classes on every supported wheel target.

`briskdb[pymongo]` installs that exact optional driver for `patch()`, `MongoClient`
and `AsyncMongoClient`. Importing BriskDB (including SQL wildcard exports) and
constructing an unentered patch do not load PyMongo. Explicit Mongo client
imports and entering a patch do. Managed clients currently reject other PyMongo
versions rather than advertising an untested lifecycle compatibility range.

## Version parity

The `briskdb-python` crate version must exactly equal the root `briskdb` Rust
crate version. Python metadata and `briskdb.__version__` use the equivalent PEP
440 spelling (`0.1.0-alpha.6` becomes `0.1.0a6`). Release automation rejects a
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
