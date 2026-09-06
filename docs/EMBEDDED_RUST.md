# Embedded Rust

Status: library entrypoint implemented by issue #189; native document-command
facade implemented by issue #191

`BriskDb` is the listener-free entrypoint for using the same protocol-neutral
engine inside a Rust application. Opening it does not bind sockets, install
signal handlers, configure tracing, or change process-global state.

See [Embedded SQL](EMBEDDED_SQL.md) for direct and prepared SQL APIs, value
guarantees, and the foreign-language runtime boundary. The opt-in native
document facade is covered below.

```rust
use briskdb::{BriskDb, Statement, Value};

# async fn run() -> briskdb::EngineResult<()> {
let db = BriskDb::builder("./data")
    .with_shard_count(4)
    .open()
    .await?;
let session = db.session();
session.set_routing_key("tenant-1").await?;

db.migrate(
    &session,
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
).await?;
db.execute_write(
    &session,
    Statement::new(
        "INSERT INTO notes (id, body) VALUES (?1, ?2)",
        vec![Value::from(1_i64), Value::from("hello")],
    ),
).await?;

let result = db.query(
    &session,
    Statement::new("SELECT body FROM notes WHERE id = ?1", vec![1_i64.into()]),
).await?;
assert_eq!(result.value.rows()[0].get(0), Some(&Value::from("hello")));

session.close().await?;
db.close().await?;
# Ok(())
# }
```

Run the complete example with:

```bash
cargo run --example embedded -- ./briskdb-embedded-example
```

Creating a database requires `.with_shard_count(...)`. After that,
`BriskDb::open(path)` detects the immutable count from the manifest. Supplying
a different explicit count fails with `FailedPrecondition` instead of opening
the data under the wrong layout.

## Defaults

| Setting | Default |
| --- | ---: |
| Physical shards | Detected when opening; explicit when creating |
| Active SQLite connections per shard | 4 |
| Queued operations per shard | 32 |
| Result rows | 10,000 |
| Result logical bytes | 16 MiB |
| Request timeout | 30 seconds |
| Shutdown grace | 30 seconds |
| Runtime | Caller-managed Tokio runtime |
| Native document support | Disabled; requires the `documents` feature and explicit per-handle enablement |

Use `EngineOptions`, `ResultLimits`, and `PreparedStatementLimits` to replace
resource defaults. Explicit shard counts are validated before storage is
created; count-dependent limits are validated after an existing manifest is
detected.

## Native document commands

Document embedding uses the same owned commands and results as the
protocol-neutral document engine. Select `documents` instead of `embedded`;
the feature includes the embedded facade and BSON dependencies:

```toml
[dependencies]
briskdb = { git = "https://github.com/schapman1974/briskdb", tag = "v0.1.0-alpha.6", default-features = false, features = ["documents"] }
```

The embedded document facade must also be enabled on each database handle.
This example uses the owned session facade; `BriskDb::execute_document`
accepts the same request together with a borrowed core `Session`.

```rust
use briskdb::{BriskDb, DocumentSupport, RequestContext};
use briskdb::document::{
    DocumentCollectionOptions, DocumentCommand, DocumentCreateCollectionRequest,
    DocumentNamespace, DocumentRequest, DocumentRequestId, DocumentResult,
    DocumentWriteOptions,
};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let db = BriskDb::builder("./data")
    .with_shard_count(4)
    .with_document_support(DocumentSupport::Enabled)
    .open()
    .await?;
let session = db.owned_session();
let namespace = DocumentNamespace::new("app", "notes")?;

// Use a fresh caller-generated value for each logical request.
let request_id = DocumentRequestId::new([1; 16])?;
let execution = session
    .execute_document(DocumentRequest::new(
        request_id,
        RequestContext::new(),
        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            namespace,
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        )),
    ))
    .await?;

assert_eq!(execution.request_id(), request_id);
assert!(matches!(execution.result(), DocumentResult::Collection(_)));

session.close().await?;
db.close().await?;
# Ok(())
# }
```

`BriskSession::execute_document` and `BriskDb::execute_document` are thin
facades: they pass `DocumentRequest` to the engine entrypoint defined for
adapters and return its `DocumentExecution` unchanged. The request owns its
nonzero identity and `RequestContext`, including cancellation, deadline, and
narrower result limits. Reusing a request identity helps correlate retries; it
does not make a write idempotent.

The current command slice is listed in the
[document engine contract](DOCUMENT_ENGINE.md#implemented-commands). It
preserves ordered BSON documents, exact BSON representations, typed document
results, and point/scatter routing plans. It is deliberately command-shaped;
there is no collection-oriented convenience API in this release.

The two enablement checks fail closed:

- Without the `documents` Cargo feature,
  the document facade methods are absent and
  `with_document_support(DocumentSupport::Enabled)` returns `Unsupported`
  during builder validation, before opening or creating storage.
- With the feature compiled but the handle left at its default
  `DocumentSupport::Disabled`, either facade method returns
  `FailedPrecondition` before submitting the request to the engine.

Enabling the facade does not expand the engine's supported Mongo semantics.
Requests for general matchers, updates, replacements, aggregation, distinct,
and retained cursors remain `Unsupported` as described by the engine contract.
There is no MongoDB listener in this release.

## Lifecycle and errors

`BriskDb` clones share one engine lifecycle and connection pools. Different
data directories can be opened independently in one process. Create a distinct
`Session` for each logical connection or request, close sessions when their
work ends, and explicitly await `BriskDb::close()` before the final handle is
dropped.

Independent processes may also open the same ready root on one local Linux or
macOS host. Every process must construct its own handle after it starts; using
an inherited handle after `fork()` is unsupported. Reads, autocommit writes,
generated IDs, and passive checkpoints may overlap. Schema/catalog/layout
changes require sole-process ownership and return retryable `Busy` while a peer
is open. See [sharing one data directory between processes](MULTIPROCESS.md).

`BriskDb::owned_session()` returns a cloneable `BriskSession` that retains its
owning database identity and exposes direct/prepared SQL methods without a
separate database argument. Clones share routing, prepared state, and terminal
close. Database shutdown is monotonic: a retained session can be closed after
shutdown, but it cannot submit work or resurrect the stopped engine. This is
the preferred handle for foreign-language wrappers.

Failures use `EngineError`. Match `EngineError::kind()` or the stable
machine-readable `EngineError::code()`; diagnostic text is intended for trusted
logs and is not a compatibility contract.

Schema changes use `BriskDb::migrate()` and the crash-resumable migration
journal. Ordinary DDL is deliberately rejected through the write method. Stop
other processes before migrating, then retry the exact migration if ownership
contention returned `Busy`.

The embedding host owns process cancellation. It can call `begin_close()` to
stop admission synchronously, `close_with_grace()` to select a finite drain
period, or await `close_when_cancelled()` with a host-owned `CancellationToken`.
BriskDB does not install signal handlers. `checkpoint()` performs a passive,
bounded WAL checkpoint on every shard and reports incomplete progress without
blocking active writers.

The initial library is async and requires a caller-managed Tokio runtime. The
typed dedicated-runtime mode remains reserved and fails with `Unsupported`
before storage is touched. The host may install any tracing subscriber it
wants; the library emits through the normal `tracing` facade and never
configures global logging itself.

`BriskDb::begin_transaction()` returns an owned `BriskTransaction` backed by a
private core session. Its first routed statement pins one physical shard;
cross-shard work fails the transaction, and committing a failed transaction
rolls it back exactly as the protocol adapters do. `commit()` and `rollback()`
consume the handle. Dropping an unfinished handle releases its private session
and pool hygiene rolls back a pinned SQLite transaction before reusing that
connection. `execute_routed_write()` and `query_routed()` retain the ordinary
explicitly routed SQL behavior used for tables created with `migrate()`. The
`execute_write()` and `query()` transaction methods use catalog-aware prepared
planning for registered logical tables.

Prepared reads can be opened with `stream_bound_logical()`. The returned
`BriskCursor` publishes ordered column metadata before the first row, buffers
at most the engine's finite row-stream capacity, and exposes `next_row()` for
asynchronous consumption. Dropping an unfinished cursor cancels all selected
shard work and interrupts its active SQLite operation. Prepared portals remain
session-owned and should be closed explicitly when the session will be reused.
For direct SQL, `BriskSession::stream()` streams one explicitly routed physical
owner and `stream_logical()` applies the same metadata-selected point/scatter
planning as `query_logical()`.

The public database, session, transaction, and cursor handles are `Send +
Sync`. Autocommit commands, transactions, prepared portals, cursors,
cancellation, bounded queueing, and result limits use the same engine semantics
as protocol adapters.
