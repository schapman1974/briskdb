# Sync and asyncio API

BriskDB offers synchronous native handles and an asyncio facade over the same
listener-free engine. Both use the engine's existing queue limits, deadlines,
cancellation, and owned session lifecycle.

## Synchronous use

```python
import briskdb

with briskdb.connect("./data", shards=4) as db:
    with db.session(routing_key="account-1") as session:
        session.migrate("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
        session.execute("INSERT INTO notes VALUES (?1, ?2)", [1, "hello"])
        with session.cursor("SELECT id, body FROM notes", batch_size=100) as rows:
            for row in rows:
                print(row)
```

`Database`, `Session`, and `Cursor` may be shared across threads. Session state
changes and operations are serialized by the native engine; using one session
per request is usually clearer and allows independent routing state.

Every query is bounded by `Config.max_result_rows` and
`Config.max_result_bytes`. The current `Cursor` batches an already bounded
native result and releases unread rows on `close()`, context exit, or garbage
collection. A true SQLite-streaming cursor depends on the retained cursor work
in [#39](https://github.com/schapman1974/briskdb/issues/39).

## Asyncio use

```python
import briskdb

async def handle(account_id: str):
    async with await briskdb.open_async("./data", shards=4) as db:
        async with await db.session(routing_key=account_id) as session:
            result = await session.query(
                "SELECT body FROM notes WHERE id = ?1",
                [1],
                timeout_ms=2_000,
            )
            return result["rows"]
```

Async methods move native calls off the event-loop thread. Native engine work
already releases the GIL. Cancelling a Python task cancels the exact
`CancellationToken` passed to the Rust `RequestContext`, which interrupts
admitted SQLite work. A token can also be supplied explicitly:

```python
token = briskdb.CancellationToken()
task = asyncio.create_task(session.query(sql, cancellation=token))
task.cancel()                 # also calls token.cancel()
```

`AsyncDatabase` is safe to retain in a FastAPI-style application lifespan or
a warm function instance. Create an `AsyncSession` per request/task when their
routing keys differ. BriskDB does not install an event-loop policy, signal
handler, logger, listener, or framework dependency.

## Transaction and DB-API boundaries

This package does not claim Python DB-API 2.0 compliance yet. BriskDB currently
executes supported writes in autocommit mode, so it does not expose misleading
`commit()` or `rollback()` methods. Native transaction handles are tracked by
[#34](https://github.com/schapman1974/briskdb/issues/34); retained streaming
cursors are tracked by #39.

Context-manager exit deterministically closes the handle. It does not imply a
transaction commit or rollback. Mongo document CRUD/aggregation waits for the
native document engine in [#160](https://github.com/schapman1974/briskdb/issues/160),
and snapshot/fencing helpers for real serverless deployments remain in
[#194–#196](https://github.com/schapman1974/briskdb/issues/194).
