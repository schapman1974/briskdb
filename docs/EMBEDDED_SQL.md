# Embedded SQL

`BriskDb` exposes the same protocol-neutral SQL engine used by HTTP and
PostgreSQL. Callers pass typed `Statement`/`Value` inputs directly; no wire
message or server is involved.

## Direct commands

```rust
use briskdb::{BriskDb, Statement, Value};

# async fn run(db: &BriskDb) -> briskdb::EngineResult<()> {
let session = db.session();
session.set_routing_key("tenant-1").await?;

let write = db.execute_write(
    &session,
    Statement::new(
        "INSERT INTO notes (id, body) VALUES (?1, ?2)",
        vec![Value::from(1_i64), Value::from("hello")],
    ),
).await?;
assert_eq!(write.value.rows_affected, 1);

let rows = db.query(
    &session,
    Statement::new("SELECT body FROM notes WHERE id = ?1", vec![1_i64.into()]),
).await?;
assert_eq!(rows.value.rows()[0].get(0), Some(&Value::from("hello")));
# Ok(())
# }
```

Use `query_logical()` when catalog placement may require scatter/gather. Every
command also has a `_with_context` form for cancellation, deadlines, and
narrower result limits.

## Prepared commands

The embedded facade exposes the complete prepare → bind → describe → execute →
close lifecycle through `prepare`, `bind`, `describe`, `execute_bound`, and
`execute_bound_logical`. Bound portals are immutable and retain the route and
typed values captured at bind time.

Use `BriskDb::begin_transaction()` for an owned single-shard transaction. The
handle provides direct typed writes and queries as well as prepare, bind,
describe, and streaming operations. `commit()` and `rollback()` consume it;
dropping it unfinished rolls back its private session. Transaction statements
are compiled through the catalog-aware planner, so referenced tables must be
registered in the logical catalog.

For a prepared read, `stream_bound_logical()` returns `BriskCursor`. Column
metadata is ready immediately, `next_row()` advances asynchronously, and the
engine retains only a bounded number of decoded rows ahead of the caller.
Dropping the cursor cancels unfinished physical and scatter work. Close the
portal or its prepared statement explicitly before long-term session reuse.

`ResultSet` keeps columns and rows positional, including duplicate column
names. Nulls, blobs, invalid UTF-8 text, integers, and floating-point values are
not coerced by the facade. SQLite has no native decimal or timestamp storage
class: unsupported typed decimal bindings fail explicitly instead of becoming
lossy, while applications should use their chosen canonical text/integer
timestamp representation until a versioned logical type mapping is added.

Generated-key writes return `WriteResult::generated_key` from the same logical
operation that committed the row when the table's generated-ID policy is
enabled.

## Runtime boundary

The Rust API is asynchronous and never creates a runtime. Rust hosts await it
on their own Tokio runtime. Synchronous foreign-language bindings should own
one finite runtime outside the GIL/host lock, submit these async calls to it,
and map cancellation back through `RequestContext`; they must not create a new
runtime per query.

Native document commands remain unavailable until the BSON/document engine in
#162–#164 lands. Selecting `DocumentSupport::Enabled` fails before storage is
touched, so SQL-only embedding cannot accidentally claim Mongo compatibility.
