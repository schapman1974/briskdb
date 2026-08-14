# Crate features and support tiers

BriskDB is one crate with an embedded core and optional process/protocol
layers. Normal `cargo build` behavior is unchanged: the default features build
the `briskdb` server and `briskdb-import` binaries.

For an embedded application with no HTTP, PostgreSQL, command-line, or signal
handling dependencies:

```toml
[dependencies]
briskdb = { version = "0.1.0-alpha.5", default-features = false, features = ["embedded"] }
```

## Feature map

| Feature | Adds | Tier |
| --- | --- | --- |
| `embedded` | Listener-free `BriskDb` and `BriskSession` APIs | Alpha-supported |
| `http` | Axum HTTP API and admin browser | Alpha-supported |
| `postgres` | PostgreSQL wire adapter | Alpha-supported, bounded SQL subset |
| `server` | HTTP/PostgreSQL listener assembly and shutdown handling | Process integration |
| `server-cli` | `briskdb` binary, Clap, logging subscriber, multithread runtime | Process integration |
| `sqlite-import` | Offline SQLite import library | Alpha-supported |
| `sqlite-import-cli` | `briskdb-import` binary | Process integration |
| `experimental-vtab` | Sharded virtual-table prototype | Experimental |
| `documents` | Reserved Mongo/document boundary | Reserved; no API yet |
| `mysql` | Reserved MySQL boundary | Reserved; no listener yet |
| `tls` | Reserved transport-security boundary | Reserved; no TLS yet |

`default = ["server-cli", "sqlite-import-cli"]`. `server` selects
`embedded`, `http`, and `postgres`; applications using adapters directly may
select `http` or `postgres` without process assembly.

## Public API tiers

- The crate-root engine value types and the `embedded` facade are the intended
  downstream Rust API. They follow the documented pre-1.0 compatibility policy.
- `protocol::http`, `protocol::postgres`, `import`, and `server` are public for
  integration, but remain alpha surfaces that may evolve with their protocols.
- `experimental-vtab` has no compatibility promise.
- The public `core`, `sql`, and `storage` modules expose implementation-facing
  building blocks used by current adapters. Prefer crate-root and `embedded`
  APIs unless implementing a BriskDB adapter.
- Reserved features compile but intentionally expose no claimed implementation.

See [Pre-1.0 compatibility](PRE_1_COMPATIBILITY.md) for the versioning policy.

## Packaging baseline

Measured from the alpha.5 lockfile on macOS ARM64 with Cargo's normal-edge
dependency graph:

| Build | Unique packages |
| --- | ---: |
| `--no-default-features --features embedded` | 36 |
| Default server + importer | 176 |

The default graph currently has five version-skew families: `fallible-iterator`,
`getrandom`, `rand`, `rand_core`, and `syn`. The release binary-size and clean
compile-time measurements from the same host were:

| Measurement | Baseline |
| --- | ---: |
| Clean embedded `cargo check` | 12.59 seconds |
| Release `briskdb` binary | 16,611,376 bytes |
| Release `briskdb-import` binary | 10,940,848 bytes |

Reproduce the baseline with:

```bash
cargo tree --locked --duplicates --edges normal
cargo build --release --locked --bins
CARGO_TARGET_DIR=$(mktemp -d) cargo check --locked --no-default-features --features embedded --lib
```

These numbers are a regression baseline, not a size or build-time guarantee.
