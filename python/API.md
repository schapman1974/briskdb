# Python API

`briskdb` is a typed wrapper around the in-process Rust engine. Public
classes and functions are covered by the packaged `py.typed` marker and `.pyi`
files; the signatures below are the compact API map.

## Open and configure

For networked use of Python's real `sqlite3`, see [remote SQLite](#remote-sqlite-addon).

- `open(path, *, shards=None, documents=False, uuid_representation=None, config=None) -> Database`
- `connect(...) -> Database` is the synchronous ergonomic alias.
- `await open_async(...) -> AsyncDatabase` and `connect_async(...)` open
  without blocking the event loop.
- `Config(...)` validates shard, pool, queue, result, prepared-object,
  deadline, and shutdown limits before opening storage.

`shards` is required to create storage and optional when reopening it. An
omitted count is read from the validated manifest; an explicit mismatch raises
`FailedPreconditionError`. `Database.shard_count` and `Database.config.shards`
always report the resolved count.

## Database and session

`Database` exposes `session()`, `transaction()`, `checkpoint()`, `serve()`, `close()`,
state/config properties, and a synchronous context manager. `Session` exposes
routing-key state, SQL and document commands, `status()`, `close()`, and a
context manager.

`Transaction` provides routed `execute()`/`query()`, explicit
`commit()`/`rollback()`, and commit-on-success/rollback-on-exception context
management. The `AsyncDatabase`, `AsyncSession`, `AsyncTransaction`, and
`AsyncCursor` facades provide the same lifecycle and SQL operations with
`await`/`async with`. Cancelling a task propagates a native
`CancellationToken` into the exact Rust request.

## Native document commands

The Python wheel contains BriskDB's document engine, but each database handle
must enable it explicitly:

- `open(path, *, documents=True, uuid_representation="standard", ...)`
- `Config(..., documents=True, uuid_representation="standard")`

The accepted UUID modes are `unspecified`, `standard`, `python_legacy`,
`java_legacy`, and `csharp_legacy`. Pass document settings either as direct
open options or through `config`; direct `documents=True` or an explicit UUID
mode cannot be combined with `config`. The document methods require the
optional `bson` package from PyMongo; SQL-only use has no PyMongo dependency.

`Session` exposes this current protocol-neutral engine slice:

- `create_collection(database, collection, *, options=None, ...)`
- `collection_exists(database, collection, ...)`
- `list_collections(database, *, skip=0, limit=None, batch_size=101, ...)`
- `list_collection_metadata(database, filter=None, *, name_only=False, batch_size=101, batch_byte_limit=None, ...)`
- `list_database_names(filter=None, ...)`
- `create_index(database, collection, keys, *, name=None, unique=False, sparse=False, partial_filter=None, ...)`
- `drop_index(database, collection, name, ...)`
- `list_indexes(database, collection, *, skip=0, limit=None, batch_size=101, ...)`
- `list_index_metadata(database, collection, *, batch_size=101, batch_byte_limit=None, ...)`
- `insert_one(database, collection, document, ...)`
- `find(database, collection, filter=None, *, projection=None, sort=None, skip=0, limit=None, batch_size=101, ...)`
- `get_more(database, collection, cursor_id, *, batch_size=101, ...)`
- `kill_cursor(database, collection, cursor_id, ...)`
- `count_documents(database, collection, filter=None, *, skip=0, limit=None, ...)`
- `distinct(database, collection, field, filter=None, ...)`
- `delete_one(database, collection, filter, ...)`
- `delete_many(database, collection, filter, ...)`
- `find_one_and_delete(database, collection, filter, *, projection=None, sort=None, ...)`
- `replace_one(database, collection, filter, replacement, *, upsert=False, ...)`
- `update_one(database, collection, filter, update, *, upsert=False, ...)`
- `update_many(database, collection, filter, update, *, upsert=False, ...)`
- `find_one_and_replace(database, collection, filter, replacement, *, projection=None, sort=None, return_document=False, upsert=False, ...)`
- `find_one_and_update(database, collection, filter, update, *, projection=None, sort=None, return_document=False, upsert=False, ...)`

Both delete methods accept the shared BSON filters and return an acknowledged
`deleted_count`. Exact `_id` filters route directly; other `delete_one` filters
remove the first natural-order match after a shard-local identity/predicate
recheck. `delete_many` commits one shard at a time. Later errors or cancellation
can leave earlier commits in place: this is not a cross-shard transaction or
snapshot. Request/result limits are checked before mutation; existing missing
collection preconditions are unchanged. See [commit boundaries](../docs/DOCUMENT_ENGINE.md#filtered-deletion-and-commit-boundaries).

`find_one_and_delete` returns `kind="document"` and `document` containing the
projected pre-delete value, or `None` for no match. Sort precedes projection;
durable natural order breaks ties. Selection is rechecked under the winning
shard's write lock, not a global cross-shard transaction/snapshot. Projection,
sort, and result-budget failures are checked before deletion. Both native API
styles forward the usual identity, deadline, cancellation, and result controls.

`replace_one` returns `kind="update"`, `acknowledged`, `matched_count`,
`modified_count`, and `upserted_id` (currently always `None`). It replaces the
first matching document, preserves the original `_id` representation and natural
order, and compares stored BSON bytes for modification counts. Equivalent
numeric ID aliases are accepted; conflicting IDs and update-operator documents
fail before writing. Top-level non-ID zero timestamps are stamped; nested values
are preserved. `upsert=True` is explicitly unsupported in this checkpoint.
Selection/replacement is atomic on the winning shard, not across shards. The
usual request/result controls and missing-collection precondition apply.

`update_one` returns the same `UpdateResult` as replacement and edits the first
matching document atomically on its shard. This checkpoint supports `$set`,
`$unset`, `$min`, `$max`, `$pop`, `$rename`, `$addToSet`, `$pullAll`, `$push`, `$pull`, and `$inc`, with dotted paths and numeric
indices in existing arrays except rename's object-only traversal.
Min/max compare whole BSON values, including null and arrays;
equal values preserve their stored types. Missing fields/array slots receive the
candidate, while blocked scalar paths fail. Comparison work is bounded even for
no-op updates.
Pop accepts numeric -1/1 for front/back removal, skips missing/empty arrays, and
rejects non-array targets. Rename moves present fields to string destinations,
overwriting existing values without array traversal. Missing sources are no-ops;
source/destination path conflicts and immutable IDs are checked before writing.
Add-to-set supports literal values or `$each`, retaining existing duplicates and
types. Pull-all removes every literal BSON-equal value, not query matches.
Numeric aliases compare equal, booleans remain distinct, and document field
order matters. Missing add-to-set targets become arrays (even with empty `$each`);
missing pull-all targets are no-ops. Non-array targets and malformed modifiers
fail atomically. Equality work and growth are bounded, including no-op updates.
Push appends a literal value or uses `$each` with `$position`, `$sort`, and `$slice`,
always inserting, stably sorting, then slicing. Integral numeric positions/slices
clamp at array boundaries. Scalar sort compares whole BSON values; compound sort
follows document-only selectors (missing/array traversal is null), not query-sort
array selection. Temporary growth and sort work are bounded even when trimmed;
the document-size cap applies after slicing. Missing push targets become arrays.
Pull removes literal-equal values or uses shared field/document query predicates,
including nested paths, regex, logical clauses, and ordinary embedded-ID matching.
Literal scalars do not implicitly match arrays containing them; query predicates
may. Missing targets stay absent. Invalid conditions are checked even without
matches; `$expr` and top-level `$not` are unsupported in pull conditions. Predicate
work, path allocations, regex programs, and comparisons share update budgets.
Increment requires numeric operands/targets (not booleans or null). It preserves
Int64 width, promotes overflowing Int32 sums, and atomically rejects Int64 overflow.
Double/Decimal promotion, 15-digit Double-to-Decimal conversion, and rounded
Decimal no-ops preserve documented numeric semantics. Missing paths copy the exact
operand. Equal Double results preserve signed zero; arithmetic on an existing
NaN counts as modified even with identical BSON. Concurrent counters re-read
under the write lock. Arithmetic workspace and cancellation remain bounded.
Untouched fields/types/order survive; operator-assigned zero timestamps stay
literal. Missing unset paths are no-ops; unsetting an array slot leaves null.
Invalid paths, conflicting prefixes, changed/removed IDs, or post-image/result
limits fail before writing. Positional paths, other operators,
and `upsert=True` remain unsupported. The normal controls and existing native
missing-collection precondition apply. See the shared
[field-update boundaries](../docs/DOCUMENT_ENGINE.md#field-updates-and-single-record-write-boundaries).

`update_many` uses the same operators and controls, returning aggregate matched
and modified counts on success. It commits one shard-local transaction at a time.
A failure rolls back the current shard, but earlier commits remain. Cancellation
and task abort have the same partial-commit boundary; callers receive an error,
not guessed partial counts. There is no cross-shard snapshot/transaction or exact
MongoDB per-document failure-atomicity claim. Result/plan delivery limits are
checked before the first write, and exact-ID filters remain point-routed.

`find_one_and_replace` returns `kind="document"` with the projected original
document by default, or the projected post-image with `return_document=True`.
No match returns `document=None`; no-op replacements still return an image.
Sort uses the original stored values. Projection changes only the returned
document, not the stored replacement. Both post-image storage limits and exact
returned size/depth budgets are checked before committing; failure leaves the
original record intact. It shares replacement identity rules and normal request
controls. `upsert=True` remains unsupported.

`find_one_and_update` has the same return shape, projection/sort and boolean
`return_document` options, request controls, and pre-commit output checks. It
applies the same eleven operators through the shared update engine, retaining untouched
fields and exact ID representation. No match returns `document=None`, and
no-op updates still return the selected image. Projection may produce `{}`
without losing the match. Other operators and upsert remain unsupported.

`list_collection_metadata` returns the usual `cursor` result, with a null plan.
Continue or kill it using collection name `$cmd.listCollections`. Full rows
contain name/type, persisted options, a stable UUID and read-only flag in `info`,
and the built-in `_id_` index definition. UUID conversion follows the configured
representation. `name_only=True` returns/filters only name/type. An absent
database returns exhausted/empty without creation; a zero-sized initial batch
on a present database defers scanning. Later creations are excluded and dropped
rows may disappear; drop/recreate invalidates an old database cursor. No snapshot
is promised. Soft byte limits carry across pages; hard result limits reject the
whole request. The existing materialized `list_collections` result is unchanged.

`list_index_metadata` returns a `cursor` with BSON metadata for built indexes:
`_id_` first, then Ready secondary indexes by name. Rows contain `name`, ordered
`key`, and applicable `sparse` / `partialFilterExpression` options, without an
invented Mongo index version. Pending declarations are excluded, even if marked
unique. The existing `list_indexes` method still returns all catalog declarations.
Use the original collection name with `get_more` / `kill_cursor`. Missing
collections return exhausted/empty without creation. Cursors retain only bounded
identity/name positions: later index creations or recreations are excluded,
drops may disappear, and dropping/recreating the collection invalidates the cursor.
There is no cross-page snapshot; an already-declared index built between pages
may appear if its name has not been passed. Zero-sized first batches and the same
byte, result, deadline, cancellation and ownership controls apply in sync/async.

Every method also accepts `request_id`, `timeout_ms`, `cancellation`,
`max_result_rows`, and `max_result_bytes`. A request ID is a nonzero
`uuid.UUID`; omitting it generates one. `AsyncSession` provides the same
methods as coroutines and propagates task or explicit-token cancellation to
the native request.

Each result is an insertion-ordered dictionary containing `request_id`,
`plan`, and `kind`, followed by the command payload. A point or scatter plan
has `kind`, `collection_id`, and ordered `shards`; catalog commands have a null
plan. Payload keys are:

| Result kind | Payload |
| --- | --- |
| `collection` | `collection` metadata |
| `collection_exists` | `exists` boolean |
| `collections` | ordered `collections` list |
| `database_names` | `names` list of exact logical document database names |
| `index_name` | `index_name` and `lifecycle="pending_build"` or `"ready"` |
| `index_built` | `index_name`, `lifecycle="ready"`, `num_indexes_before`, `num_indexes_after` |
| `acknowledged` | `acknowledged` boolean (pending or built index removal) |
| `indexes` | ordered `indexes` list |
| `insert` | `acknowledged`, `inserted_count`, `inserted_ids` |
| `cursor` | `namespace`, `cursor_id`, `exhausted`, `documents` |
| `cursor_killed` | `killed` boolean |
| `count` | `count` |
| `distinct` | ordered `values` list, preserving first BSON representations |
| `delete` | `acknowledged`, `deleted_count` |
| `document` | `document` pre-image or `None` |

`collection_exists` checks one exact namespace without enumerating the catalog,
creating missing metadata, or returning collection options/indexes. It has a
null plan and scalar result accounting, even with large catalogs.

Collection metadata contains `id`, `database_id`, `database`, `name`,
`namespace`, exact BSON `options`, placement `code`/`version`, and index
metadata. Each index contains `name`, exact ordered `keys`, `unique`,
`built_in`, and `lifecycle`. New secondary declarations start `pending_build`;
explicit non-unique builds become `ready`. The built-in `_id_` is always ready.

Both sync and async `create_index` accept `sparse=True` or a `partial_filter`
mapping, but not both. The shared index validator checks the supported predicate
subset eagerly, including branches that would otherwise short-circuit. Empty
filters and unsupported operators (such as `$ne`) raise `UnsupportedError`.
Keys, name and filter together have a 1 MiB stored-specification limit. This is a
declaration only: it does not scan existing records, accelerate queries, or enforce
uniqueness yet. Redeclaring an identical built index preserves `lifecycle="ready"`.

Sync and async `build_index(database, collection, name, ...)` build a declared
non-unique index and return its name with `lifecycle="ready"`. They accept the
usual request ID, timeout, cancellation and result limits. Builds require no
other process to hold the database open and exclusively pause schema admission.
All current records and the combined index-key budget are validated before
durable intent. Publication happens only after all shards commit. Interruption
after intent requires closing/reopening the root; recovery removes the unfinished
build's derived entries and leaves its declaration pending for an explicit retry.
Ready entries are maintained with inserts, replacements, updates and deletes.
Queries still scan; this does not accelerate them or enforce secondary uniqueness.
`unique=True` builds raise `UnsupportedError` until global uniqueness is implemented.

```python
session.create_index("app", "events", {"kind": 1}, name="by_kind")
session.build_index("app", "events", "by_kind")  # lifecycle: ready
```

To create and build in one operation, sync and async sessions also provide
`create_built_index(database, collection, keys, ...)`, with the same keys, name,
sparse/partial options and request controls as `create_index`. The collection
must already exist. It returns `kind="index_built"`, `lifecycle="ready"`, and
Ready-index counts before/after (including `_id_`, excluding Pending declarations)
observed under the same exclusive admission. A matching Ready retry leaves both
counts equal. Unique secondary builds remain unsupported.

```python
result = session.create_built_index("app", "events", {"kind": 1}, name="by_kind")
assert result["lifecycle"] == "ready"
```

Preflight failures leave no new declaration. Interruption after durable intent
requires closing/reopening the root; recovery removes a newly created unfinished
declaration and its derived entries. If the declaration existed before this
call, recovery preserves it as Pending, just like `build_index`. Committed index
identities are never reused. This native helper does not yet enable Mongo wire
`createIndexes`, query acceleration, or secondary uniqueness.

Recognized advanced index metadata reports normalized ordered `keys` plus optional
`sparse=True` or `partial_filter` fields; absence means no such membership option.
Filters preserve their BSON types and field order across restart. Same-name
advanced declarations require the same normalized envelope bytes, not merely
logically equivalent predicates. Recognized imported v2 metadata is presented the
same way; unknown legacy envelopes retain their previous opaque `keys` value and
are not silently interpreted. Ordinary and built-in result shapes are unchanged.

`drop_index` is available on both sync and async sessions. It removes one pending
declaration or built non-unique index by exact, case-sensitive name and returns `kind="acknowledged"`,
`acknowledged=True`, and a null plan. It never removes documents or the built-in
ID index. `_id`/`_id_` raise `InvalidArgumentError`; missing indexes raise
`FailedPreconditionError`, including a repeated drop. Field aliases, key-pattern
selectors and bulk removal are not supported: `*` is only an exact native name,
not a wildcard. For pending declarations, cancellation/deadline or insufficient
result limits before the metadata commit leave the declaration intact. Built-index
preflight failures before durable intent also leave it unchanged. Recreating a removed name gets a new durable
internal ID. Built indexes require sole-process ownership and exclusive schema
admission. Their entries are removed through the recovery journal without changing
records or other Ready indexes. Cancellation after durable intent leaves the root
fenced until closing/reopening finishes the admitted drop; it does not restore the
index. The same options work through asyncio. Mongo wire `dropIndexes`, wildcard
and field-alias removal remain unfinished.

This API deliberately mirrors the document engine's implemented boundary:
`insert_one` generates a missing ObjectId while preserving explicit null and
leaving the caller's document unchanged. Direct non-ID `Timestamp(0, 0)` fields
are server-stamped; nested timestamps and timestamp IDs are preserved.
Find/count/delete use the [shared BSON matcher](../docs/DOCUMENT_ENGINE.md), including
dotted paths, arrays, comparisons, logical operators, and bounded regexes.
Filtering precedes global skip/limit; exact `_id`/`$eq` routes to one shard.
Find returns a bounded page. When `cursor_id` is not `None`, use
`get_more(database, collection, cursor_id, *, batch_size=101, ...)` on the same
session; it returns the same `cursor` result shape. `batch_size=0` is accepted
only on initial find. Stop early with
`kill_cursor(database, collection, cursor_id, ...)`, which returns
`kind="cursor_killed"` and a `killed` boolean. Both methods accept the same request
identity, timeout, cancellation, and result-limit controls, including asyncio
wrappers. Session close also releases retained cursors. Failed executing
continuations discard the cursor; pre-admission argument errors do not advance it.
Cross-batch reads are not a snapshot under concurrent writes.
`projection` accepts a mapping or a sequence/set of field names. Repeated names
in the field-name shorthand are deduplicated; an empty mapping/list leaves the
document unchanged. Basic numeric/boolean inclusion and exclusion, nested
mappings, dotted paths, arrays, and `_id` rules use the shared Rust projector.
Stored documents and exact BSON representations remain unchanged. Filters use
original values; result byte limits apply to projected output. A cursor retains
its initial projection. Invalid mixed modes and conflicting paths fail eagerly;
expression, positional, and numeric-array-index projections are unsupported.

`sort` accepts an ordered mapping such as `{"priority": -1, "_id": 1}` with up
to 32 numeric `1`/`-1` directions. An empty mapping preserves natural order.
Sorting uses original values before skip/limit and projection; equal BSON keys
retain durable natural order. It persists across cursor batches. Bounded sorting
windows currently rescan matching documents; large skips may require several
scans and an internal window/memory boundary may return a short batch. Metadata
and expression sorts are unsupported. Python pair-list sort shorthand is not
part of the embedded API; ordinary PyMongo chaining works through the wire API.

`distinct` uses the shared BSON matcher and identity in global encounter order.
Missing values are omitted, null is retained, and only a final array is flattened
one level. Intermediate arrays are not traversed by dotted paths. Empty and
numeric components are literal mapping keys; BSON Code is not a string key.
There is no pagination option or retained cursor. Request budgets apply to unique
output values rather than unrelated input payloads; exceeding a bound fails the
whole command. See the [distinct contract and limits](../docs/DOCUMENT_ENGINE.md#distinct-values).

Operator updates/findAndModify, upserts, and native bulk-write helpers remain
unsupported. There is no Python collection
object or Python-hosted MongoDB network listener in this slice. The separate
opt-in Rust Mongo listener also exposes batch inserts/deletes, retained finds
and aggregation, `estimated_document_count()`, and aggregation-backed
`count_documents()` through PyMongo. These share the native document engine.

## Attached listeners

- `db.serve(*, http="127.0.0.1:0", admin="127.0.0.1:0", postgres=None, postgres_tls_cert=None, postgres_tls_key=None, postgres_user="briskdb", postgres_password_file=None) -> Server`; pass `admin=None` to disable administration
- `await async_db.serve(...) -> AsyncServer`
- `Server.data_address` reports the data address and `Server.http_address`
  remains its compatibility alias. `.admin_address` and `.postgres_address`
  report optional bound addresses.
- `Server.close()` is idempotent; server context exit closes only listeners.
- Database close first drains every attached server, then stops the engine.

Data HTTP, administration HTTP, and unauthenticated PostgreSQL accept only
numeric loopback addresses. The data address serves `/v1` discovery, query, and
execute. The optional administration address serves `/health`, `/metrics`,
`/v1/health`, `/v1/admin/*`, and `/admin/*`; cross-plane paths return 404.
Certificate, key, and password-file arguments enable TLS/SCRAM PostgreSQL and
permit a non-loopback PostgreSQL address. This is single-identity
authentication, not roles or authorization. The PostgreSQL endpoint supports
BriskDB's documented bounded SQL subset. See the repository's
[HTTP listener contract](../docs/HTTP_LISTENERS.md).

## Results and errors

Queries return `shards`, `columns`, and tuple `rows`. Writes return `shard`,
`rows_affected`, and an optional `generated_key`. Cursors provide bounded
native streaming through `fetchone()`, `fetchmany()`, `fetchall()`, iteration,
and deterministic cancellation on close.

All native failures derive from `BriskDBError`; each has stable `code` and
`retryable` class attributes. Durable key reuse for a different operation maps
to the non-retryable `IdempotencyConflictError`, which derives from
`IntegrityError`. See [value and error conversions](VALUE_CONVERSIONS.md) and
[sync/async lifecycle details](ASYNC_API.md).

## Remote SQLite addon

`attach_remote(connection, url, *, token, schema="remote", tables=None,
timeout=15.0) -> RemoteAttachment` accepts a standard `sqlite3.Connection`.
It loads the native module from the installed BriskDB wheel and creates proxy
virtual tables in a new, in-memory attached database. Query `remote.users`, or
use an unqualified name when it does not collide with a local table. SQLite
executes joins, expressions, aggregates, sorting, and predicates locally.

`RemoteAttachment.schema`, `.tables`, `.scope` (`"logical"` or `"legacy-shard"`),
and `.closed` describe the attachment. `.close()` and context-manager exit
detach it without changing any remote table. Finish active cursors/transactions
before closing; a busy detach raises and can be retried. Dropping a local proxy
table never drops remote data. Neither attachment nor detachment commits or
rolls back caller-owned work. Attachment rejects an active local transaction.
The connection's ordinary thread-ownership rules still apply. This is a
synchronous client; no async `sqlite3` API is implied.

Server: `db.serve(..., sqlite_remote_token=token,
sqlite_remote_tables=["users"], sqlite_remote_routing_key=None)` replaces the
data listener's normal SQL endpoints with `GET /sqlite/v1/catalog` and
`POST /sqlite/v1/scan`. Both require the bearer credential, with a 1..256-table
allowlist in the default logical database. Each table has at most 256 columns.
Use a separate `serve()` handle for PostgreSQL. The optional admin listener is
unchanged; use `admin=None` unless needed, and never publish it through the proxy.
The same options are forwarded by `AsyncDatabase.serve()`.

Registered tables use engine placement and global/sharded reads. An uncataloged
database is rejected unless the server explicitly supplies a legacy routing key;
that mode exposes all rows on the selected physical shard, not all shards and
not just rows belonging to that key. Internal tables and views are not exposed.
Schema generation and a per-listener instance nonce fence stale attachments:
after migration or restart, close and reattach. Metadata discovery checks every
target shard's column shape. Declared affinities are retained; remote constraints,
indexes, default collations, primary-key promises and hidden rowids are not copied.
Use explicit key columns and explicit collations in local SQL where needed.

Reads are capped at 4,096 rows and 1 MiB of engine result budget per scan, plus
an 8 MiB binary frame/HTTP response cap. Smaller engine settings still apply.
The server admits eight concurrent connector requests, bounds request bodies
to 4 KiB, uses a ten-second engine deadline and a fifteen-second HTTP-handler
deadline. Client timeout is configurable from >0 to 60 seconds. It bounds idle
socket operations and checks elapsed time between response-body reads; it is
not a strict wall-clock bound on DNS resolution or slowly arriving HTTP headers.
Use a trusted HTTPS endpoint/proxy with its own header/connection deadlines.
No automatic retries occur. There is no paging or pushdown in this preview:
`WHERE`, projections and `LIMIT` do not make an oversized full-table scan valid.
Remote cancellation on `Connection.interrupt()` is not yet provided during a
blocking network callback; request deadlines bound that wait.

NULL, signed int64, real, UTF-8 text (including embedded NUL), and blob values
are transported without JSON number loss. Unrepresentable unsigned/decimal,
invalid UTF-8 and NaN values fail rather than coerce silently. Reads from
different cursors/tables/shards need not observe the same committed snapshot,
even inside a local `BEGIN` or savepoint. Remote writes, DDL, transaction
mapping, stable row locators and distributed atomicity are not implemented.

Network access requires HTTPS with normal certificate verification. Literal
loopback IPs may use HTTP for local development. Redirects and environment
proxies are disabled; credentials are never embedded in SQLite schema SQL or
returned error messages. The native module uses SQLite's `DIRECTONLY` safety
flag (non-TEMP persistent views/triggers cannot activate it). It does not
sandbox SQL executed directly by the connection's owner. After loading,
extension loading is always disabled. Use only trusted SQL on this connection.
Failures use standard `sqlite3` exceptions, not native `BriskDBError` subclasses.
