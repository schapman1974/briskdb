# BriskDB for Python

This package runs BriskDB's sharded SQLite engine in the Python process. It
starts no listener by default and never installs a signal handler or global
logger. A database can optionally expose its exact engine through
host-controlled HTTP and PostgreSQL listeners.

Tagged releases publish compiler-free wheels for CPython 3.9–3.14 on supported
macOS and Linux targets:

```bash
python -m pip install --only-binary=:all: briskdb
```

To build the current checkout from source, use Python 3.9+ and Rust 1.85+:

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

Pass `shards` when creating a data directory. Later calls may omit it and use
the count stored in the manifest. Passing the wrong count raises
`FailedPreconditionError`; omitting it for new/empty storage asks you to choose
one without creating files.

Resource limits can be validated before the database opens:

```python
config = briskdb.Config(shards=4, max_result_rows=5_000)
db = briskdb.open("./data", config=config)
```

To serve that same open database to browser/HTTP and PostgreSQL clients:

```python
with briskdb.open("./data") as db:
    with db.serve(postgres="127.0.0.1:0") as server:
        print(server.http_address)      # actual address; port 0 is resolved
        print(server.postgres_address)
```

HTTP and unauthenticated PostgreSQL are loopback-only. To expose PostgreSQL on
another address, pass `postgres_tls_cert`, `postgres_tls_key`, `postgres_user`,
and `postgres_password_file` to `serve()`; TLS plus SCRAM-SHA-256 are then
required for every database session. The password is read from the file, never
passed as a Python string.
Closing a server leaves the database usable; closing the database first closes
all of its attached servers. The asyncio API provides `await db.serve()` and
an `AsyncServer` context manager with the same lifecycle.

Database and session handles own their native resources, `close()` is
idempotent, and blocking engine work releases Python's GIL. Dropping live
handles during interpreter shutdown is also safe.

Multiple independently spawned Python processes may open the same ready data
directory on one local Linux or macOS host. Each process must create its own
handle; use `multiprocessing.get_context("spawn")`, not an inherited live
handle after `fork()`. Schema changes require every peer to close first and
otherwise return retryable `BusyError`. See the
[multi-process contract](../docs/MULTIPROCESS.md).

Synchronous handles support `with`; the asyncio facade keeps engine work off
the event loop and propagates task cancellation into Rust:

```python
async with await briskdb.open_async("./data") as db:
    async with await db.session(routing_key="account-1") as session:
        rows = await session.query("SELECT body FROM notes WHERE id = ?1", [1])
```

See [sync and asyncio usage](ASYNC_API.md) for cursors, deadlines, cancellation,
thread/task safety, and the intentionally unclaimed DB-API transaction surface.
The [API reference](API.md), [platform matrix](COMPATIBILITY.md), and
[serverless-shaped warm-handler example](SERVERLESS.md) define the supported
package surface and its current boundaries.

This is an alpha API. SQL supports `None`, `bool`, bounded integers, `float`,
`str`, bytes-like values, and exact `decimal.Decimal` conversion with explicit
errors when SQLite cannot store a value losslessly. See the executable
[value and exception contract](VALUE_CONVERSIONS.md) for boundaries and the
stable `BriskDBError` hierarchy.

Native Mongo/document commands are not claimed until BriskDB's document engine
lands. The extension uses the host-controlled `listeners` Rust feature and
does not include the daemon CLI, signal handler, or logging subscriber.
