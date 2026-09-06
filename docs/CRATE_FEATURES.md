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

For the same facade plus native BSON/document commands:

```toml
[dependencies]
briskdb = { version = "0.1.0-alpha.5", default-features = false, features = ["documents"] }
```

The `documents` feature selects `embedded`; each database handle must still be
opened with `DocumentSupport::Enabled` before its document facade accepts work.
Command, BSON, plan, and result types live under `briskdb::document`.

## Feature map

| Feature | Adds | Tier |
| --- | --- | --- |
| `embedded` | Listener-free `BriskDb` and `BriskSession` SQL APIs | Alpha-supported |
| `http` | Axum HTTP API and admin browser | Alpha-supported |
| `postgres` | PostgreSQL wire adapter, including TLS/SCRAM implementation | Alpha-supported, bounded SQL subset |
| `listeners` | Host-controlled HTTP/PostgreSQL listeners, selecting `tls`, without signals or engine ownership | Alpha-supported |
| `server` | Daemon listener assembly, signal handling, and engine ownership | Process integration |
| `server-cli` | `briskdb` binary, Clap, logging subscriber, multithread runtime | Process integration |
| `sqlite-import` | Offline SQLite import library | Alpha-supported |
| `sqlite-import-cli` | `briskdb-import` binary | Process integration |
| `tinymongo-import` | Strict TinyMongo v1.3 SQLite reader and atomic document import; selects `documents` and `sqlite-import` | Experimental migration API |
| `experimental-vtab` | Sharded virtual-table prototype | Experimental |
| `documents` | BSON values and codec, catalog/storage, TinyMongo-ready semantic keys, protocol-neutral document commands, and their `BriskDb`/`BriskSession` facade; also selects `embedded` | Experimental document engine; no MongoDB listener yet |
| `mysql` | Reserved MySQL boundary | Reserved; no listener yet |
| `tls` | Compatibility alias for the secure `postgres` surface | Alpha-supported; selected by `listeners` |

`default = ["server-cli", "sqlite-import-cli"]`. `listeners` selects
`embedded`, `http`, and `postgres`; `server` adds process signal handling.
Applications using adapters directly may select `http` or `postgres` without
listener assembly.

## Public API tiers

- The crate-root engine value types and the `embedded` facade are the intended
  downstream Rust API. They follow the documented pre-1.0 compatibility policy.
- `protocol::http`, `protocol::postgres`, `import`, and `server` are public for
  integration, but remain alpha surfaces that may evolve with their protocols.
- `experimental-vtab` has no compatibility promise.
- The public `core`, `sql`, and `storage` modules expose implementation-facing
  building blocks used by current adapters. Prefer crate-root and `embedded`
  APIs unless implementing a BriskDB adapter.
- The `documents` feature exposes the protocol-neutral BSON foundation,
  versioned persistence, first document-engine commands, and the thin embedded
  facade documented in the
  [BSON value and codec contract](BSON.md),
  [document storage contract](DOCUMENT_STORAGE.md), and
  [protocol-neutral document engine](DOCUMENT_ENGINE.md). Its public value,
  command, result, and metadata APIs remain experimental while Mongo semantics
  are completed. Callers must also open with `DocumentSupport::Enabled`; merely
  compiling the feature does not enable document commands on a handle. It does
  not expose a MongoDB listener or a collection-oriented convenience API.
- The `mysql` reserved feature compiles but intentionally exposes no claimed
  implementation.

See [Pre-1.0 compatibility](PRE_1_COMPATIBILITY.md) for the versioning policy.

## Packaging baseline

Measured from the alpha.5 lockfile on macOS ARM64 with Cargo's normal-edge
dependency graph:

| Build | Unique packages |
| --- | ---: |
| `--no-default-features --features embedded` | 36 |
| Default server + importer | 204 |

The default graph currently has six version-skew families: `const-oid`,
`fallible-iterator`, `getrandom`, `rand`, `rand_core`, and `syn`. The release binary-size and clean
compile-time measurements from the same host were:

| Measurement | Baseline |
| --- | ---: |
| Clean embedded `cargo check` | 12.59 seconds |
| Release `briskdb` binary | 19,011,504 bytes |
| Release `briskdb-import` binary | 11,094,608 bytes |

Reproduce the baseline with:

```bash
cargo tree --locked --duplicates --edges normal
cargo build --release --locked --bins
CARGO_TARGET_DIR=$(mktemp -d) cargo check --locked --no-default-features --features embedded --lib
```

These numbers are a regression baseline, not a size or build-time guarantee.
