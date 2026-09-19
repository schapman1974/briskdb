# Python API

`briskdb` is a typed wrapper around the in-process Rust engine. Public
classes and functions are covered by the packaged `py.typed` marker and `.pyi`
files; the signatures below are the compact API map.

## Open and configure

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
- `create_index(database, collection, keys, *, name, unique=False, ...)`
- `list_indexes(database, collection, *, skip=0, limit=None, batch_size=101, ...)`
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
`$unset`, `$min`, `$max`, `$pop`, `$rename`, `$addToSet`, and `$pullAll`, with dotted paths and numeric
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
applies the same eight operators through the shared update engine, retaining untouched
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
| `index_name` | `index_name` and `lifecycle="pending_build"` |
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
`built_in`, and `lifecycle`. Secondary index declarations currently remain
`pending_build`; the ready built-in `_id_` index is authoritative.

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
