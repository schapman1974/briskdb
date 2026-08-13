# Embedded Rust

Status: implemented by issue #189

`BriskDb` is the listener-free entrypoint for using the same protocol-neutral
engine inside a Rust application. Opening it does not bind sockets, install
signal handlers, configure tracing, or change process-global state.

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

## Defaults

| Setting | Default |
| --- | ---: |
| Physical shards | 4 |
| Active SQLite connections per shard | 4 |
| Queued operations per shard | 32 |
| Result rows | 10,000 |
| Result logical bytes | 16 MiB |
| Request timeout | 30 seconds |
| Shutdown grace | 30 seconds |
| Runtime | Caller-managed Tokio runtime |
| Native document support | Disabled |

Use `EngineOptions`, `ResultLimits`, and `PreparedStatementLimits` to replace
resource defaults. The builder validates the complete shard/runtime/document
configuration before opening or creating storage.

## Lifecycle and errors

`BriskDb` clones share one engine lifecycle and connection pools. Different
data directories can be opened independently in one process. Create a distinct
`Session` for each logical connection or request, close sessions when their
work ends, and explicitly await `BriskDb::close()` before the final handle is
dropped.

Failures use `EngineError`. Match `EngineError::kind()` or the stable
machine-readable `EngineError::code()`; diagnostic text is intended for trusted
logs and is not a compatibility contract.

Schema changes use `BriskDb::migrate()` and the crash-resumable migration
journal. Ordinary DDL is deliberately rejected through the write method.

The embedding host owns process cancellation. It can call `begin_close()` to
stop admission synchronously, `close_with_grace()` to select a finite drain
period, or await `close_when_cancelled()` with a host-owned `CancellationToken`.
BriskDB does not install signal handlers. `checkpoint()` performs a passive,
bounded WAL checkpoint on every shard and reports incomplete progress without
blocking active writers.

The initial library is async and requires a caller-managed Tokio runtime. The
typed dedicated-runtime and document-support modes are reserved and fail with
`unsupported` before storage is touched until their implementations land.
The host may install any tracing subscriber it wants; the library emits through
the normal `tracing` facade and never configures global logging itself.
