# Sync and asyncio API

BriskDB offers synchronous native handles and an asyncio facade over the same
in-process engine. Both use the engine's existing queue limits, deadlines,
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

`Database`, `Session`, `Transaction`, and `Cursor` may be shared across threads.
Session and transaction state changes are serialized by the native engine;
using one handle per request is usually clearer and allows independent routing
state.

`Cursor` pulls from the engine's bounded SQLite row stream. `fetchmany()` limits
the Python batch size while the native producer buffers only its fixed row
capacity. `close()`, context exit, or garbage collection cancels unread work
and interrupts the active SQLite operation. `fetchall()` intentionally
materializes every remaining row requested by the caller.

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
handler, logger, listener, or framework dependency when opening a database.

Listeners can be started explicitly without reopening storage:

```python
async with await database.serve(postgres="127.0.0.1:0") as server:
    print(server.http_address, server.postgres_address)
```

`AsyncServer.close()` drains its listeners but leaves the database running.
Closing the database closes every attached server first. Remote PostgreSQL
uses the same TLS certificate/key/user/password-file keyword arguments as the
synchronous `Database.serve()` method.

Native document methods have the same sync/async pairing. Enable the document
engine on open and install PyMongo for its `bson` value classes:

```python
from bson import ObjectId

async with await briskdb.open_async("./data", shards=4, documents=True) as db:
    async with await db.session() as session:
        await session.create_collection("app", "notes")
        note_id = ObjectId()
        await session.insert_one(
            "app", "notes", {"_id": note_id, "body": "hello"}
        )
        result = await session.find("app", "notes", {"_id": note_id})
```

`AsyncSession` includes create/list collection and index calls plus
`insert_one`, `find`, `count_documents`, and `delete_one`. They use the same
request IDs, deadlines, cancellation tokens, result limits, BSON conversion,
and point/scatter plans as their synchronous `Session` methods.

## Transactions and DB-API boundaries

`Database.transaction()` returns an owned, single-shard transaction. Its first
routed operation pins one shard; crossing to another shard fails the
transaction. Successful context exit commits, exceptional exit rolls back, and
explicit `commit()` or `rollback()` terminally closes the handle:

```python
with db.transaction(routing_key="account-1") as transaction:
    transaction.execute(
        "INSERT INTO notes (id, body) VALUES (?1, ?2)",
        [2, "committed on context exit"],
    )
```

The asyncio equivalent is `async with await db.transaction(...)`. BriskDB does
not claim Python DB-API 2.0 compliance; these are explicit native handles, not
implicit connection transactions. Document commands run on sessions rather
than SQL transactions; broader Mongo-compatible CRUD and aggregation remain
in [#160](https://github.com/schapman1974/briskdb/issues/160), and
snapshot/fencing helpers remain in
[#194–#196](https://github.com/schapman1974/briskdb/issues/194).
