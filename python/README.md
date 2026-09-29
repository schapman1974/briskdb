# BriskDB for Python

`0.1.0b1` is a **local/developer beta**, not full MongoDB compatibility or a
production-ready remote database service. The
[beta scope](https://github.com/schapman1974/briskdb/blob/main/docs/MONGO_PARITY.md#localdeveloper-beta-scope)
lists exclusions, including full database statistics, advanced collection
options, Mongo sessions/transactions, TTL expiration and full-text search.

This package runs BriskDB's sharded SQLite engine in the Python process. It
starts no listener by default and never installs a signal handler or global
logger. A database can optionally expose its exact engine through
host-controlled data HTTP, administration HTTP, and PostgreSQL listeners.

Tagged releases publish compiler-free wheels for CPython 3.9–3.14 on supported
macOS and Linux targets:

```bash
python -m pip install --only-binary=:all: 'briskdb==0.1.0b1'
```

Document commands use PyMongo's public BSON classes as an optional companion:

```bash
python -m pip install --only-binary=:all: 'briskdb[pymongo]==0.1.0b1'
```

The BriskDB wheel does not require or import PyMongo for SQL-only applications.

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

## Patch PyMongo for local testing

The beta wheel includes `briskdb.patch()`, a TinyMongo-style
context manager/decorator. Install its optional pinned driver companion:

```bash
python -m pip install --only-binary=:all: 'briskdb[pymongo]==0.1.0b1'
# Or build a repository checkout with Rust 1.85+:
python -m pip install './python[pymongo]'
# For a supplied test wheel (replacing any same-version older build):
python -m pip install --force-reinstall './briskdb-...whl[pymongo]'
```

```python
import briskdb
import pymongo

with briskdb.patch():  # isolated temporary SQLite files, not RAM-only
    writer = pymongo.MongoClient("mongodb://ignored.example.com")
    reader = pymongo.MongoClient()
    writer.app.users.insert_one({"_id": 1, "name": "Ada", "score": 9})
    assert reader.app.users.find_one({"_id": 1})["name"] == "Ada"
# Clients close, PyMongo is restored, and the temporary files are removed.
```

Keep data with `briskdb.patch(folder="./test-data", shards=4)`, also usable as
a synchronous test decorator. New roots default to four shards; reopening
detects the stored layout unless an explicit count is supplied. Persistent
folders are never deleted. Nested default scopes are isolated; managed clients
and scopes using one canonical persistent path share one process-local engine
until its last owner closes. Unrelated native `briskdb.open()` handles still
obey the ordinary schema/multi-process ownership restrictions.

```python
import asyncio
import briskdb
import pymongo

async def test_application():
    async with briskdb.patch(folder="./async-test-data"):
        async with pymongo.AsyncMongoClient() as client:
            await client.app.users.update_one(
                {"_id": 1}, {"$set": {"name": "Ada"}}, upsert=True
            )
            print(await client.app.users.find({}).sort("name", 1).to_list())

asyncio.run(test_application())
```

Async clients require `async with briskdb.patch()` for awaited cleanup.
Async scope startup/cleanup runs off the event loop and drains even on caller
cancellation. Async decorators and async clients in synchronous scopes are
rejected explicitly. Enter the patch before importing application/ODM modules
that capture client aliases, and retain it for the full application lifetime.
Only top-level `pymongo.MongoClient`/`AsyncMongoClient` are replaced. Existing
clients, earlier constructor aliases and explicit PyMongo submodule imports
are unchanged and may still contact their original server. Process-global
scopes cannot overlap across threads/tasks; nesting must unwind in order.

### Direct PyMongo-shaped clients

```python
from briskdb import MongoClient, DESCENDING

with MongoClient(folder="./app-data", shards=4) as client:
    print(list(client.app.users.find({"score": {"$gte": 5}}).sort("score", DESCENDING)))
```

`briskdb.AsyncMongoClient` provides `async with` and awaited operations/close.
Direct async-client construction opens storage synchronously; an async patch
scope offloads startup. A positional filesystem path, `briskdb_folder`, and
migration aliases `tinymongo_folder`, `tinymongo_path`, `foldername`, and
`sqlite_shards` are accepted. Direct clients default to `BRISKDB_HOME` or
`./briskdb-data`; use explicit close or context managers. `client.briskdb_path`
reports the root. `ASCENDING`, `DESCENDING`, `ReturnDocument`, `IndexModel`,
and `briskdb.errors` support common client-side call sites.

Both forms use real PyMongo 4.17.0 collections, cursors, results and exceptions
with one private loopback Mongo listener over the bundled Rust engine. No
HTTP/admin/SQL port or separate process starts. Supplied hosts/SRV destinations,
credentials, TLS, proxy and replica-set settings are ignored for this explicit
local replacement. URI database names and ordinary driver/codec settings are
retained and validated by PyMongo; automatic encryption is rejected, not bypassed.
There is no remote fallback. The unauthenticated loopback socket is **not a
security boundary against other local users/processes**.

Existing BriskDB Mongo query/resource limits still apply. Managed clients sharing
a root share a bounded 32-socket listener,
including PyMongo monitor/pool sockets (the default client pool size is two).
Standalone Mongo listeners still default to eight sockets. This does not add
transactions, change streams, TinyMongo-only metadata helpers or alternative
memory/JSON/DuckDB/remote backends. `backend="sqlite"`/`"sqlite-sharded"` are
accepted migration spellings; `backend="memory"` is rejected instead of calling
temporary files RAM. Use fresh BriskDB roots, not TinyMongo database files, and
spawned processes with newly constructed handles, never inherited clients after
`fork()`. PyMongo remains optional for SQL-only use and unentered patch objects.

After installing a test wheel, run `python python/examples/mongo/patch.py`
from the repository (or copy that script elsewhere). It exercises sync queries,
unique indexes, updates, async access and persisted reopen without a daemon.

## Query a remote BriskDB server through standard `sqlite3` (read-only preview)

The same wheel includes an original native SQLite virtual-table addon and its
remote client; this is not a VFS or a replacement for Python's `sqlite3` driver.
Against an explicitly enabled BriskDB remote listener:

```python
import os
import sqlite3
import briskdb

conn = sqlite3.connect(":memory:")
with briskdb.attach_remote(conn, "https://db.example.com", token=os.environ["BRISKDB_TOKEN"]):
    print(conn.execute("SELECT * FROM remote.users WHERE id = ?", (123,)).fetchall())
conn.close()
```

Start the dedicated, authenticated listener against an existing engine with
`db.serve(admin=None, sqlite_remote_token=token, sqlite_remote_tables=["users"])`.
It serves only allowlisted tables; the normal `/v1/query` and `/v1/execute`
routes are absent. Publish its loopback address through a trusted HTTPS reverse
proxy for network access. Do not publish a separate admin/ordinary HTTP listener.
Tokens should be generated with `secrets.token_urlsafe(32)` and kept out of source.

Registered logical tables use BriskDB's normal shard placement. Uncataloged
legacy databases require an explicit server-side `sqlite_remote_routing_key=`
and expose **only that routed shard**, reported as `attachment.scope ==
"legacy-shard"`; registered tables report `"logical"`. A routing key is not a
row-level authorization filter: every allowlisted table row on that shard is readable.

This preview supports bounded reads, parameters, aggregates, local SQL functions,
and joins with local tables. Each scan is limited to 4,096 rows, 1 MiB of engine
results and an 8 MiB encoded response (or stricter engine settings). No predicate,
projection, or LIMIT pushdown yet: even `LIMIT 1` must fit the full table scan.
Oversized results fail, never truncate. Reads across tables/shards are not a
transaction snapshot. Writes, hidden rowids, remote DDL and transaction mapping
are not implemented. See [the API contract](API.md#remote-sqlite-addon) and
[host SQLite requirements](COMPATIBILITY.md#remote-sqlite-host-requirements).

Enable native BSON commands per database handle. The default `standard` UUID
representation uses subtype 4; legacy and `unspecified` modes are documented
in the [value conversion contract](VALUE_CONVERSIONS.md).

```python
from bson import ObjectId
import briskdb

with briskdb.open("./data", shards=4, documents=True) as db:
    with db.session() as session:
        session.create_collection("app", "notes")
        note_id = ObjectId()
        session.insert_one(
            "app", "notes", {"_id": note_id, "body": "hello"}
        )
        result = session.find("app", "notes", {"_id": note_id})
        print(result["documents"])
```

The document API supports collection/index metadata, single-document insertion
with generated IDs, BSON find/count/distinct, filtered deletes, replacements,
`$set`/`$unset`/`$min`/`$max`/`$pop`/`$rename`/`$addToSet`/`$pullAll`/`$push`/`$pull`/`$inc` updates, and retained cursors through synchronous and asyncio
sessions. Find-one-and-delete returns a projected before-image;
find-one-and-replace/update support sorted projected before/after images.
Return size/depth checks precede mutation. Push supports `$each`, `$position`, `$sort`,
and `$slice` in fixed insertion-sort-slice order, with bounded growth and stable
whole-BSON/document-field sorting. Pull removes literal values or applies bounded
query predicates to array members without creating missing fields. Increment
preserves numeric width, rejects integer overflow, handles Decimal128 promotion
and rounded no-ops, and safely updates concurrent counters.
`replace_one(..., upsert=True)` inserts on no match, retaining an equality-bound
query ID or generating an ObjectId when the replacement omits one. Insertions
return zero matched/modified counts and `did_upsert=True`, including null IDs;
matches return `did_upsert=False`. Native upserts require an existing collection.
Reply and document bounds precede insertion; same-ID races update the winner.
`update_one` and `update_many` also accept `upsert=True`: positive direct/`$eq`
equalities (including `$and` clauses and dotted object paths) seed the new document,
then operators run. Conflicting equality paths fail before insertion; range,
regex, and alternative predicates do not supply values. Operator timestamps stay
literal. An unbound `_id` can be supplied by the update; otherwise one is generated
after applying operators. `find_one_and_replace` and `find_one_and_update` accept
`upsert=True` too. Their results retain `kind: "document"` and add `did_upsert`
and `upserted_id`; an inserted null ID is distinct from no insertion. The default
before-image is `None` on insertion, while `return_document=True` returns the
projected inserted document. Matched images, including `{}`, report
`did_upsert=False`. The ID metadata and image share the pre-commit result budget.
Continue a
non-null `result["cursor_id"]` with
`session.get_more(database, collection, cursor_id, batch_size=101)`; stop early
with `session.kill_cursor(database, collection, cursor_id)`. Cursors belong to
the creating session and are released on session close. `find(..., batch_size=0)`
opens an empty initial batch. Pages preserve global natural order, not a snapshot
under concurrent writes. Request result limits still fail the whole command if
exceeded. Pass `projection={"body": 1, "_id": 0}` or `projection=["body"]` to
select returned fields; nested/array paths and exclusion are supported without
mutating stored documents. The projection persists across cursor batches.
Pass `sort={"priority": -1, "_id": 1}` for global BSON sorting before skip/limit
and projection; the sort persists across cursor batches. Stable ties use natural
order. Sorted pages use bounded key windows and rescan until sorted indexes
exist; large skips may need repeated scans. `create_index(database, collection,
{"body": 1, "rank": -1})` now generates `body_1_rank_-1`; `name=` is optional in
both synchronous and asyncio APIs. Numeric direction aliases normalize to Int32,
while malformed/duplicate paths, invalid directions, reserved names and oversized
definitions fail before catalog changes. Explicit names remain supported.
Secondary declarations start `pending_build`. `session.build_index(database,
collection, name)` explicitly builds a declared index and marks it Ready after
every shard commits; later document writes maintain its entries transactionally.
It requires sole-process ownership; interrupted builds require reopening for
cleanup and retry. Non-unique indexes accept nested objects, arrays and other
valid BSON values, retaining conservative candidates when an equality key cannot
be generated. Unique indexes retain the stricter supported key subset.
Ready indexes provide conservative scalar-equality candidates;
unsupported shapes still scan. `unique=True` indexes validate existing records
before activation and enforce canonical keys across shards on every write,
raising `UniqueViolationError` on conflicts. Use `create_built_index(..., unique=True)`
to declare and build in one call. The built-in `_id_` remains
ready and enforced, but its redeclaration is not part of this checkpoint;
other update operators, additional aggregation expressions, and bulk-write
Python helpers remain future work. Multi-delete/update commit one shard at a time;
failure rolls back the current shard, not earlier commits. See the
[write boundaries](../docs/DOCUMENT_ENGINE.md#field-updates-and-single-record-write-boundaries).

`session.drop_collection(database, collection)` and
`session.drop_database(database)` return `{"kind": "namespace_dropped",
"existed": bool, "request_id": ..., "plan": None}`. Async sessions expose the
same methods and request controls. Missing targets return false. Drops preserve
unrelated namespaces and SQL tables; dropping the final collection removes its
empty logical database. Retained cursors cannot read a recreated collection.
Drops require sole-process ownership. If interrupted after durable intent, the
root remains fenced until reopen finishes deletion; cancellation does not promise
rollback. See [document storage](../docs/DOCUMENT_STORAGE.md#namespace-deletion-and-restart).

`session.aggregate(database, collection, pipeline, batch_size=101)` accepts a list
of `$match`, `$sort`, `$skip`, `$limit`, `$count`, `$project`, `$set`, `$addFields`,
`$unset`, and `$group` stages and returns the same
cursor shape as find, with `get_more`/`kill_cursor` continuation and cleanup.
The async session has the same method and request controls. Simple pipelines
stream across batches; count retains a counter and sort uses bounded working
memory; group retains bounded accumulator states. There is no disk spill or snapshot guarantee. A zero batch size defers
input reads. Pipeline conversion uses one 16-MiB BSON/64-MiB heap budget; execution
has 65,536 consumed-input and four-million-step bounds over the whole cursor.
Projection stages support field references, arrays, `$literal`, `$ifNull`, `$size`,
and `$$REMOVE`. Assignments read the original input; no pipeline stage changes
stored documents. A later limit stops unused expression evaluation, including
after a sort. Each transform specification has a 1-MiB/4,096-node/depth-100 bound;
per-row allocation-work and step limits reject computed-output amplification.
Group keys may be literal BSON, field references, or computed objects/arrays and
the existing `$literal`/`$ifNull`/`$size` expressions. Variables such as `$$REMOVE`
and `$$ROOT` are not supported in group expressions (use `$literal` to retain
such strings as data). Supported accumulators are `$addToSet`, `$avg`, `$first`,
`$last`, `$max`, `$min`, `$push`, and `$sum`. They share global input order, BSON
identity, Decimal128 arithmetic, memory limits and cursor cleanup with Rust and
PyMongo; see the [numeric and grouping contract](../docs/DOCUMENT_ENGINE.md#aggregation-groups-and-numeric-accumulators)
for result-type boundaries. Group results must each fit BSON before delivery.
Native `count_documents()` remains the separate direct engine count helper;
PyMongo's version now runs its actual constant-key aggregation pipeline, with
aggregation row/work limits and an error for explicit `limit=0`.

Pass `shards` when creating a data directory. Later calls may omit it and use
the count stored in the manifest. Passing the wrong count raises
`FailedPreconditionError`; omitting it for new/empty storage asks you to choose
one without creating files.

Resource limits can be validated before the database opens:

```python
config = briskdb.Config(shards=4, max_result_rows=5_000)
db = briskdb.open("./data", config=config)
```

Unreleased main can also configure lock/admission waiting (sync or async):

```python
policy = briskdb.ContentionPolicy(
    initial_delay_ms=2, max_delay_ms=50, multiplier=2,
    jitter="full", max_retries=8, max_elapsed_ms=250,
)
config = briskdb.Config(shards=4, contention_policy=policy)
with briskdb.open("./data", config=config) as db:
    pass
```

Use `ContentionPolicy.fail_fast()` for no retries, or omit the policy to keep
existing behavior. This never replays commands or uncertain writes. On
unreleased main, managed Mongo clients and patch scopes accept the same policy:

```python
with briskdb.MongoClient(folder="./mongo-data", contention_policy=policy) as client:
    print(client.briskdb_contention_policy)

with briskdb.patch(contention_policy=policy):
    pass  # newly constructed pymongo clients use this engine's policy
```

`AsyncMongoClient` and `async with briskdb.patch(...)` use the same keyword.
Owners sharing a folder share one policy: omitted/`None` inherits an open
engine's policy; explicit conflicting settings raise `ValueError`. A new engine
without a policy keeps legacy behavior. These settings are not saved in the
database. See the
[complete waiting contract](../docs/REQUEST_CONTROLS.md#opt-in-storage-contention-backoff).

### Index-build deadlines (unreleased main)

Managed `MongoClient`, `AsyncMongoClient`, and `patch()` give `createIndexes`
a separate five-minute host deadline by default. Ordinary Mongo commands retain
their 15-second bound. A positive command `maxTimeMS` narrows the build deadline:

```python
with briskdb.MongoClient(folder="./mongo-data", index_build_timeout_ms=600_000) as client:
    client.app.users.create_indexes(
        [briskdb.IndexModel("email")], maxTimeMS=120_000,
    )
    print(client.briskdb_index_build_timeout_ms)  # 600000
```

`index_build_timeout_ms` accepts integer milliseconds from 1 through 86,400,000
(24 hours). Omitted/`None` inherits an already-open managed engine's setting or
selects five minutes for a new engine. Explicit conflicts on a shared folder or
inside a patch scope fail before changing it. The setting is not persisted.
The managed engine's internal deadline accommodates this build budget; other
native engines and standalone Mongo listeners keep their existing defaults.
Client socket/overall timeouts and lock-contention policies can still end an
operation earlier. Builds remain offline under exclusive schema admission;
a longer deadline does not enable concurrent queries during index creation.

On unreleased main, `Config(storage_profile="local")` makes the default storage
contract explicit. The reserved `storage_profile="nfs"` currently raises
`UnsupportedError` during configuration, before opening or creating files.
It is not EFS support and never converts an existing database. See the
[storage-profile contract](../docs/STORAGE_FORMAT.md#explicit-profile-selection-and-reserved-nfs-format).

To serve that same open database to browser/HTTP and PostgreSQL clients:

```python
with briskdb.open("./data") as db:
    with db.serve(postgres="127.0.0.1:0") as server:
        print(server.data_address)      # data API; port 0 is resolved
        print(server.http_address)      # compatibility alias for data_address
        print(server.admin_address)     # health, metrics, and browser
        print(server.postgres_address)
```

The data and administration HTTP listeners are separate and loopback-only.
Administration defaults to another operating-system-selected loopback port;
pass `admin=None` to disable `/health`, `/metrics`, `/v1/admin/*`, and the
browser. Unauthenticated PostgreSQL is also loopback-only. To expose PostgreSQL
on another address, pass `postgres_tls_cert`, `postgres_tls_key`,
`postgres_user`, and `postgres_password_file` to `serve()`; TLS plus
SCRAM-SHA-256 are then required for every database session. The password is
read from the file, never passed as a Python string.
Closing a server leaves the database usable; closing the database first closes
all of its attached servers. The asyncio API provides `await db.serve()` and
an `AsyncServer` context manager with the same lifecycle.

<a id="encrypt-and-reload-httpadmin-unreleased"></a>

### Encrypt and reload HTTP/admin (beta.1)

Current main can encrypt either HTTP plane independently, including SQLite-remote:

```python
import http.client
import ssl

with briskdb.open("./data") as db:
    with db.serve(http_tls_cert="data.crt", http_tls_key="data.key",
                  admin_tls_cert="admin.crt", admin_tls_key="admin.key") as server:
        trust = ssl.create_default_context(cafile="data-ca.crt")
        client = http.client.HTTPSConnection(
            "localhost", int(server.http_address.rsplit(":", 1)[1]), context=trust)
        client.request("GET", "/v1")
        print(client.getresponse().read())
        client.close()
        server.reload_http_tls(tls_cert="next-data.crt", tls_key="next-data.key")
        server.reload_admin_tls(tls_cert="next-admin.crt", tls_key="next-admin.key")
```

Use a certificate-matching hostname and trusted CA; never disable verification.
Certificate/key arguments accept strings or `PathLike`. Each pair must be complete;
admin TLS requires an enabled admin address. Defaults stay plaintext, and both
addresses remain **loopback-only**: TLS authenticates the server, not HTTP callers.
It does not change routes, cookies, or SQLite-remote bearer/table checks. The
ordinary `sqlite3` addon continues to verify HTTPS using platform trust settings.

`AsyncDatabase.serve()` accepts the same keywords. Await
`AsyncServer.reload_http_tls(...)` and `reload_admin_tls(...)`; both sync/async
reload methods accept `timeout_ms=` and `cancellation=`. Controls apply while
queued and preparing, with a final check before publication. Failed/cancelled
preparation keeps the active identity; admitted handshakes/connections keep their
old identity after success. Async task cancellation signals the native token;
cancellation cannot undo a completed publication. Plaintext/disabled listeners
cannot be upgraded with reload. This is not session revocation or authentication.
HTTP/1.1, finite handshakes and per-plane socket bounds follow the
[shared HTTP listener contract](../docs/HTTP_LISTENERS.md).

### Reload PostgreSQL security

Beta.1 can reload an **already-secure** attached server without
changing its address. Pass all four fields; no plaintext password argument is used:

```python
server.reload_postgres_security(
    tls_cert="next/server.crt", tls_key="next/server.key",
    user="briskdb", password_file="next/password", timeout_ms=5_000,
)
# With AsyncServer: await server.reload_postgres_security(...)
```

New connections use the new certificate and credentials. Existing sessions and
in-progress handshakes retain their original identity; this is not immediate
revocation. Bad configuration, cancellation or an expired deadline observed
before publication leaves the active identity unchanged. Cancellation cannot
undo a completed publication; blocking file preparation may finish in the
background without publishing. `cancellation=briskdb.CancellationToken()` is also
supported, and cancelling the asyncio task signals its native request token.
Anonymous, closing and closed servers cannot be upgraded with this method.
Mongo stays loopback-only; this does not authenticate Mongo or expose HTTP.

### Mongo listener

For PyMongo clients, the same wheel includes an optional Mongo listener:

```python
from pymongo import MongoClient

with briskdb.open("./data", documents=True) as db:
    with db.serve(mongo="127.0.0.1:0") as server:
        with MongoClient(f"mongodb://{server.mongo_address}") as client:
            client.app.notes.update_one(
                {"_id": 123}, {"$set": {"body": "hello"}}, upsert=True
            )
            print(client.app.notes.find_one({"_id": 123}))
```

This uses the same BSON collections as native document sessions, not SQL tables.
Mongo is disabled by default (`mongo=None`), requires `documents=True`, and is
unauthenticated/loopback-only. Neither PostgreSQL credentials nor SQLite remote
tokens authenticate Mongo. Do not publicly proxy this port. `AsyncDatabase.serve`
accepts the same option and `AsyncServer.mongo_address` reports its bound address.
PyMongo is still optional for importing BriskDB and for SQL-only applications.
See the [Mongo compatibility contract](../docs/MONGO_PARITY.md) for the supported
subset and limits; this is not a full MongoDB server.

<a id="encrypt-the-mongo-listener-unreleased"></a>

### Encrypt the Mongo listener (beta.1)

Source builds support paired certificate/key paths in synchronous and async
`serve()`. For a server certificate valid for `localhost`:

```python
import briskdb
from pymongo import MongoClient

with briskdb.open("./data", shards=2, documents=True) as db:
    with db.serve(mongo="127.0.0.1:0", mongo_tls_cert="./server.crt",
                  mongo_tls_key="./server.key") as server:
        port = server.mongo_address.rsplit(":", 1)[1]
        with MongoClient(f"mongodb://localhost:{port}/?directConnection=true",
                         tls=True, tlsCAFile="./ca.crt") as client:
            print(client.admin.command("ping"))
```

Trust the issuing CA and keep hostname/certificate verification enabled. Unix
private keys must not be group-writable or accessible by others (`0600` works).
Both paths and an explicit Mongo listener are required; defaults remain plaintext.
Async hosts use the same keywords with `await db.serve(...)` and stock
`AsyncMongoClient` with the same TLS options. Encrypted Mongo can coexist with
PostgreSQL TLS/SCRAM or authenticated SQLite remote, without sharing credentials.
TLS authenticates the server only: Mongo remains anonymous and loopback-only,
and must not be publicly proxied. Managed `briskdb.patch()` clients are unchanged.

<a id="reload-mongo-tls-unreleased"></a>

### Reload Mongo TLS (beta.1)

An already-encrypted attached listener can replace its certificate/key without
rebinding. Within the `serve()` context above:

```python
server.reload_mongo_tls(tls_cert="./next.crt", tls_key="./next.key", timeout_ms=5_000)
# With AsyncServer: await server.reload_mongo_tls(...)
```

Fresh connections must trust the new certificate; admitted connections and
pending handshakes keep their original identity. Invalid replacements leave the
active identity unchanged. Optional `cancellation=` and `timeout_ms=` apply while
queued and preparing, with a final check before publication; asyncio task
cancellation signals the native token. They cannot undo a completed publication.
This does not upgrade a plaintext listener, authenticate users, revoke existing
sessions, change PostgreSQL's identity, or remove Mongo's loopback restriction.

### Handle and process ownership

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

See [sync and asyncio usage](ASYNC_API.md) for transactions, streaming cursors,
deadlines, cancellation, thread/task safety, and the intentionally unclaimed
DB-API compatibility surface.
The [API reference](API.md), [platform matrix](COMPATIBILITY.md), and
[serverless-shaped warm-handler example](SERVERLESS.md) define the supported
package surface and its current boundaries.

This is a pre-1.0 API. SQL supports `None`, `bool`, bounded integers, `float`,
`str`, bytes-like values, and exact `decimal.Decimal` conversion with explicit
errors when SQLite cannot store a value losslessly. See the executable
[value and exception contract](VALUE_CONVERSIONS.md) for boundaries and the
stable `BriskDBError` hierarchy.

The extension uses the host-controlled `listeners` and `documents` Rust
features and does not include the daemon CLI, signal handler, or logging
subscriber. Enabling documents starts no MongoDB listener.
