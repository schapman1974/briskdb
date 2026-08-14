# Embedded Rust

Status: implemented by issue #189

`BriskDb` is the listener-free entrypoint for using the same protocol-neutral
engine inside a Rust application. Opening it does not bind sockets, install
signal handlers, configure tracing, or change process-global state.

See [Embedded SQL](EMBEDDED_SQL.md) for direct and prepared command APIs,
value guarantees, and the foreign-language runtime boundary.

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
| Native document support | Disabled |

Use `EngineOptions`, `ResultLimits`, and `PreparedStatementLimits` to replace
resource defaults. Explicit shard counts are validated before storage is
created; count-dependent limits are validated after an existing manifest is
detected.

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
typed dedicated-runtime and document-support modes are reserved and fail with
`unsupported` before storage is touched until their implementations land.
The host may install any tracing subscriber it wants; the library emits through
the normal `tracing` facade and never configures global logging itself.

An engine `Session` accepts `BEGIN`, `COMMIT`, and `ROLLBACK`, retains and pins
the first exact one-shard connection, and exposes failed-transaction recovery.
Streaming cursors are not yet part of the embedded contract and remain
tracked by #39. Autocommit commands, prepared portals, cancellation, bounded
queueing, and materialized result limits use the same engine semantics as
protocol adapters.
