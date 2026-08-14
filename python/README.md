# BriskDB for Python

This package runs BriskDB's sharded SQLite engine in the Python process. It
starts no listener, subprocess, signal handler, or global logger.

Build and install it from the repository with Python 3.9+ and Rust 1.85+:

```bash
python -m pip install ./python
```

```python
import briskdb

db = briskdb.open("./data", shards=4)
session = db.session(routing_key="account-1")
session.migrate("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
session.execute("INSERT INTO notes VALUES (?1, ?2)", [1, "hello"])
print(session.query("SELECT body FROM notes WHERE id = ?1", [1]))
session.close()
db.close()
```

Resource limits can be validated before the database opens:

```python
config = briskdb.Config(shards=4, max_result_rows=5_000)
db = briskdb.open("./data", config=config)
```

Database and session handles own their native resources, `close()` is
idempotent, and blocking engine work releases Python's GIL. Dropping live
handles during interpreter shutdown is also safe.

Synchronous handles support `with`; the asyncio facade keeps engine work off
the event loop and propagates task cancellation into Rust:

```python
async with await briskdb.open_async("./data", shards=4) as db:
    async with await db.session(routing_key="account-1") as session:
        rows = await session.query("SELECT body FROM notes WHERE id = ?1", [1])
```

See [sync and asyncio usage](ASYNC_API.md) for cursors, deadlines, cancellation,
thread/task safety, and the intentionally unclaimed DB-API transaction surface.

This is an alpha API. SQL supports `None`, `bool`, bounded integers, `float`,
`str`, bytes-like values, and exact `decimal.Decimal` conversion with explicit
errors when SQLite cannot store a value losslessly. See the executable
[value and exception contract](VALUE_CONVERSIONS.md) for boundaries and the
stable `BriskDBError` hierarchy.

Native Mongo/document commands are not claimed until BriskDB's document engine
lands. The extension uses only the listener-free `embedded` Rust feature.
