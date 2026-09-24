# Mongo compatibility parity contract

Status: TinyMongo v1 contract frozen for issue
[#161](https://github.com/schapman1974/briskdb/issues/161); BriskDB candidate
endpoint has an opt-in shared document command slice and aggregation candidate gate

BriskDB uses a versioned differential contract to define the document behavior
that its Rust and Python APIs, and later its MongoDB listener, must preserve.
The first contract is source-locked to
[TinyMongo v1.3.0](https://github.com/schapman1974/tinymongo/releases/tag/v1.3.0)
at commit `53cbf44e98b8caa036163725d195fd29592e1cc0`. It covers 228 logical cases
through both synchronous and asynchronous APIs. Across TinyMongo's seven
contract backends, that is a 3,192-execution discovery matrix.

This is a frozen behavioral input, not a claim that BriskDB already has MongoDB
parity. The checked-in report contains only the 456 sync/async executions from
the `tinymongo-memory` reference. BriskDB's owned runner reproduces all 456 in
CI and byte-compares the normalized result with the checked-in reference. The
checked-in report remains `reference-only`; candidate results are published
separately and are not treated as complete TinyMongo parity.

Required CI runs all 456 exact frozen sync/async executions (228 logical cases)
against a real four-shard BriskDB listener. It uses the unchanged BriskDB PyMongo
adapter, including ordinary database-drop cleanup. The JUnit result is checked
against the complete locked case/API set: missing, substituted, duplicate,
skipped or failed cases reject the gate.

The full candidate job runs independently of the reference/oracle job so neither
loses coverage or needs a longer timeout. Its `mongo-full-candidate-results`
artifact contains `candidate-full.xml` and normalized `candidate-full.json`.
The immutable reference report remains separate. Frozen sources, corpus,
adapters, reference results and intentional-difference allowances are unchanged.
Passing this frozen slice does not complete the larger TinyMongo test inventory,
application matrices, bulk-write boundaries, security or hardening work; issues
#174/#181/#183/#184/#186/#187/#188 retain their separate acceptance criteria.

To reproduce with the frozen runner's test dependencies installed:

```sh
BRISKDB_MONGO_CONTRACT_PYTHON=python3 cargo test --locked \
  --no-default-features --features mongo --test mongo_candidate_contract \
  -- --ignored --nocapture
```

### Real ODM application checkpoint

A separate required CI job runs the actual locked TinyMongo Beanie and MongoEngine
application fixtures, not only the frozen command contracts with similar names.
Beanie 2.1.0, MongoEngine 0.29.3 and stock PyMongo 4.17.0 use a real four-shard
BriskDB listener. Fixture source hashes are checked before execution; only client
construction/connection configuration changes. Models, data-access calls and
application assertions stay unchanged, without monkeypatching driver methods or
rewriting replies. TinyMongo supplies test-only fixture code and an ID factory;
it is not the candidate storage or client implementation.

Both application bodies run before and after a full engine/listener restart.
The gate also verifies persisted application data and the ODM-created index.
Missing dependencies, modified sources, omitted/duplicated/skipped/failed cases,
or a child exceeding its 60-second phase bound fail the gate. The separate
`mongo-real-odm-results` artifact contains `initial.json` and `reopened.json`.
This is coverage for two baseline CRUD fixtures, not all ODM features, Talk Python
applications or the larger #181 acceptance matrix; it does not change the frozen
corpus, reports or difference policy.

To reproduce, install `tests/mongo_odm_requirements.txt` and the source-locked
TinyMongo package in a separate Python 3.13 environment, then point to its checkout:

```sh
BRISKDB_MONGO_ODM_PYTHON=python3 \
BRISKDB_MONGO_ODM_SOURCE_ROOT=/path/to/locked/tinymongo \
  cargo test --locked --no-default-features --features mongo --test mongo_odm \
  -- --ignored --nocapture
```

## Current wire checkpoint

The daemon accepts `--mongo-listen SOCKET_ADDR|disabled` and
`BRISKDB_MONGO_LISTEN` (CLI takes precedence). Both default to `disabled`;
activation requires building with `--features mongo`. For example:

```bash
cargo run --locked --features mongo --bin briskdb -- \
  --data-dir ./briskdb-data --shards 4 --mongo-listen 127.0.0.1:27017
```

The daemon explicitly enables document support only when Mongo is requested.
Numeric IPv4/IPv6 loopback addresses are accepted; port zero chooses an OS port
reported in the readiness log. Non-loopback addresses and fixed-port collisions
with the configured HTTP/admin/PostgreSQL listeners are rejected before opening
the database. Builds without `mongo` reject activation before creating files.
Every configured socket is bound before serving begins; a bind failure releases
the sockets and closes the process-owned database. SIGINT/SIGTERM drains all
listeners; an unexpected Mongo exit stops the common server and reports failure.
Mongo has no authentication/TLS boundary yet: keep it local, including when
PostgreSQL uses TLS/SCRAM. Do not publicly proxy the Mongo listener.

Zlib transport is negotiated only when a valid hello/isMaster offers `zlib`.
For example, pass `compressors="zlib"` to `MongoClient` / `AsyncMongoClient`,
or append `&compressors=zlib` to the README's connection URI. Ordinary clients
remain uncompressed. Snappy, zstd and the testing-only noop codec are not
negotiated. Successful negotiation is local to that connection; failed or
unsupported offers cannot enable compressed requests on it or another socket.

Both the compressed wire packet and the expanded original message retain the
1 MiB hard ceiling. A zlib handshake advertises 1 MiB minus 1 KiB, reserving
space for wrapper/stored-block overhead when drivers size batches before
compression (including PyMongo's `zlibCompressionLevel=0`). The 512 KiB BSON,
decoded-heap, batch, connection and frame-read limits are unchanged. Inflation
checks the declared size before allocation, uses a fixed-size output buffer,
and requires one complete zlib stream with exact consumed/produced lengths.
Truncation, bad checksums, trailing streams/data, nested/unsupported opcodes,
unnegotiated codecs and size mismatches close only the offending connection.
Original OP_MSG CRC-32C checks still cover the reconstructed original header.
Handshake/authentication commands cannot be compressed. Replies use zlib when
it reduces a compressed request's response size, otherwise remain plain.
Compression runs in the same bounded, joined blocking-parser slots as BSON.
Raw framing, malformed/bomb cases and real sync/async PyMongo CRUD/cursor/index
and restart tests cover the transport separately from the frozen semantic corpus.

Rust hosts using `listeners,mongo` can call
`server::AttachedServer::start_with_mongo(&database, listener_config, address)`
and obtain the actual address with `server.addresses().mongo()`. This requires
an already-running, explicitly document-enabled database. Closing/dropping the
attached server stops its listeners without closing that borrowed engine;
`close().await` joins cleanup and can be retried after cancellation. The
process-owned equivalent is `server::run_with_mongo(config, options, address)`
(`server,mongo`). Existing `Config` / `ListenerConfig` literals, and existing
entry points with Mongo disabled, remain source-compatible. The corresponding
`start_secure_with_mongo` and `start_sqlite_remote_with_mongo` entry points keep
Mongo loopback-only alongside their separately secured SQL connectors.

Python's synchronous and asyncio `db.serve(mongo="127.0.0.1:0")` now attaches
this same listener to an explicitly `documents=True` database. Both server
handles expose `mongo_address`; `mongo=None` remains the default. The wheel
includes the Rust transport without requiring a Python Mongo client dependency.
Native document methods and stock PyMongo clients access the same collections.
Server close drains listeners without closing the database; database close drains
all registered servers first. Startup validates numeric loopback addresses,
document enablement and fixed-port collisions, and releases sockets on bind
failure. PostgreSQL TLS/SCRAM and SQLite remote bearer tokens do not authenticate
Mongo or permit exposing its port. SQL tables and BSON collections stay distinct.

The non-default `mongo` Cargo feature exposes `protocol::mongo::MongoServer`.
The host explicitly starts it on a loopback address and closes it before
closing the borrowed database. Data commands require opening `BriskDb` with
`DocumentSupport::Enabled`; discovery still works with document support disabled.

```rust,ignore
let database = briskdb::BriskDb::builder("./data")
    .with_document_support(briskdb::DocumentSupport::Enabled)
    .open().await?;
let mut mongo = briskdb::protocol::mongo::MongoServer::start(
    &database, "127.0.0.1:27017".parse()?,
).await?;
// Keep the host runtime alive while clients use mongo.address().
mongo.close().await?;
database.close().await?;
```

Ordinary PyMongo 4.17.0 synchronous and asynchronous clients can discover,
ping, inspect build information, insert one or many documents, and run bounded
find queries through the [shared BSON matcher](DOCUMENT_ENGINE.md), including
multi-batch reads with `getMore` and explicit `killCursors` cleanup. The legacy
`count` command, PyMongo `estimated_document_count()`, and `distinct()` also use
this engine through both synchronous and asynchronous clients. Basic aggregation
pipelines also use retained cursors over the shared global document engine.
Exact `_id` and `_id: {$eq: value}` filters keep single-shard routing.
A safe `_id: {$in: [literal, ...]}` scans only its distinct
owning shards, using the same canonical BSON identities as storage. Numeric
aliases and repeated IDs do not duplicate output or shard access. The complete
matcher still checks every candidate; this is shard pruning, not a multi-key
index lookup. Find/getMore, legacy count, distinct, update, replace, delete and
find-and-modify share the restriction, including sorting and global pagination.
Compound filters and positive `$and` clauses intersect proven exact-ID/list
owners; `$or` unions owners only when every alternative is bounded. These keep
the full matcher even with one owner, including nonmatching upsert conflicts.
Canonical-ID work is limited to 1024 values across the filter. Empty/oversized
lists, regex members, negations and dotted IDs provide no restriction; unproven
queries (and empty owner intersections) retain ordinary scans.
Aggregation (including PyMongo `count_documents()`) uses the same shard restriction
for a safe first-stage `$match`: sole exact IDs use one owner, and proven logical
or list constraints use their selected owners. Every original stage remains in
the pipeline, and subset sources do not prefilter rows before aggregation's
cumulative input/work limits.
Matches after transformations or other preceding stages do not establish a route.
Scans merge matching documents in durable
natural order unless an explicit sort is supplied. Storage format, cursor
budgets and existing per-shard mutation/transaction boundaries are unchanged.
Missing IDs are generated by the shared engine when the client has
not already supplied them. Explicit null and other supported BSON IDs are preserved.
Duplicate IDs produce write errors with code 11000 and original input indices.
Ordered batches stop at the first duplicate; unordered batches continue after
safe duplicate failures. First writes create the
collection through the shared engine's durable catalog; reads of absent
collections return an exhausted empty cursor without creating anything.
Namespace checks target one collection through the engine, so reads and inserts
remain usable beyond 101 collections and with large unrelated catalog metadata.

PyMongo sync/async collection and database drops now use the shared durable
`DropCollection` / `DropDatabase` engine commands. Raw `drop` takes a collection
string and reports NamespaceNotFound (26) when absent; `dropDatabase` takes
integer 1 and succeeds even when absent. Success replies contain `ok: 1.0` only.
Drops preserve unrelated namespaces and SQL storage, and stale cursors cannot
read a recreated collection. Interruption after durable intent requires reopen
to finish deletion before normal operations resume; this is not rollback.
Plain explicit wire `create` is also supported: the same name and empty options
are idempotent; PyMongo's default `check_exists=True` detects duplicates through
collection discovery. Capped collections, validators, collation, views, time
series, and other nonempty wire creation options fail before mutation. See
[the recovery contract](DOCUMENT_STORAGE.md#namespace-deletion-and-restart).

`listCollections` and sync/async PyMongo `list_collections()` /
`list_collection_names()` return engine-owned metadata cursors. The optional
`cursor` document accepts `batchSize` (0–1000); for example,
`db.list_collections(cursor={"batchSize": 1})`. Shared BSON filters are compiled
even for absent databases and zero-sized initial batches. `nameOnly: true`
returns only `name`/`type` and filters those fields; full results additionally
contain persisted `options`, `info.readOnly: false`, `info.uuid`, and the actual
unique `_id_` index definition (without a fictitious Mongo index format version).
No SQL or internal catalog tables appear. Missing databases return empty without
creation. `authorizedCollections` accepts a boolean: this unauthenticated
standalone listener has no collection-level privileges to filter.

Metadata cursors use namespace `database.$cmd.listCollections`, shared quotas,
byte limits, expiry/cancellation cleanup, and pooled-socket getMore/killCursors.
They scan a validated manifest snapshot per page, retaining only bounded filter
and position state. An opening allocation ceiling excludes later creations;
deleted rows can disappear, and a dropped/recreated database invalidates the
cursor. There is no cross-batch snapshot promise. Collection UUIDv8 identity
survives reopen and backup, changes on drop/recreate, and differs for independent
roots.

`listIndexes` and sync/async PyMongo `list_indexes()` / `index_information()`
expose the built-in `_id_` followed by Ready secondary indexes in name order.
Rows contain ordered `key` and `name`, plus applicable `sparse` and
`partialFilterExpression` options. Pending native declarations are omitted:
they are not built indexes, including declarations marked unique. No internal
identity, lifecycle, or fictitious Mongo index version is exposed. Raw missing
collections return code 26; PyMongo returns empty discovery without creating them.
The `cursor` option accepts `batchSize` 0–1000; `maxTimeMS`, byte caps, cursor
ownership, pooled-socket continuation and cleanup use the shared engine path.
`includeBuildUUIDs` / `includeIndexBuildInfo` and other unsupported options are
rejected rather than fabricated. The cursor namespace is `database.collection`,
as defined by the [Mongo command](https://www.mongodb.com/docs/manual/reference/command/listIndexes/).

Index cursors retain only a collection identity, opening index-ID ceiling and
bounded name position, never definitions or SQLite handles. New/recreated index
identities are excluded, drops may disappear, and collection drop/recreation
invalidates continuation. Pages are not a snapshot: an existing pending declaration
built between pages may become visible if its name is still ahead of the cursor.
Native Rust `ListIndexMetadata` and Python `list_index_metadata` share this path;
the older native `ListIndexes` / `list_indexes` still include pending metadata.

Mongo `createIndexes` and sync/async PyMongo `create_index` / `create_indexes`
now build ordinary, compound, sparse and partial indexes, including unique indexes, through native
`CreateIndexes`. Batches are limited to 1,000 entries and existing BSON/request
budgets. Every shape is validated before implicit collection creation; builds then
run in order under one exclusive schema/sole-process admission. A runtime failure
can retain a completed prefix. Actual Ready counts include `_id_` and exclude
Pending declarations. Matching retries keep identities and metadata; same-name
definition conflicts return 86, equivalent definitions with another name return
85, and explicit uniqueness options on ascending `_id` return 197. Valid ascending
`_id` requests are no-ops even when PyMongo supplies its default `_id_1` name.
Duplicate unique builds/writes return 11000; unknown options (including
collation and commit quorum), descending `_id` creation and document sequences are
not supported. Ready indexes also provide conservative equality/membership candidates.
Ready unique indexes enforce cross-shard ownership for inserts, replacements,
updates and upserts. Missing/null, numeric aliases, multikey deduplication and
sparse/partial membership use the shared canonical key generator. Enforcement
survives reopening and ends with recoverable index removal. Bulk writes retain
the existing per-input/per-shard commit boundary: they are not globally atomic,
and an otherwise unique final bulk image can fail on a transient collision.
TinyMongo's memory and SQLite backends differ on transient unique collisions;
the explicit bulk-policy decision remains tracked in #183.

Mixed PyMongo `IndexModel` batches now accept selected TinyMongo-style reduced
behavior: non-unique `"hashed"` components become ascending equality keys;
`expireAfterSeconds` accepts a finite nonnegative integer/double but **does not
expire documents**; `background=True` still builds synchronously. Existing numeric
directions and names are preserved, and generated hashed names retain `_hashed`.
The catalog reports only effective keys/options, never fictitious hashed or TTL
support. Unique hashed/TTL combinations and those options on the built-in `_id`
index are rejected before collection creation. Background unique builds still
enforce uniqueness normally. Non-unique text declarations are accepted but the
**entire index is skipped**, including any other keys/options in that declaration.
No text index or placeholder is stored, and `$text` queries remain unsupported.
Unique text declarations are rejected. A skipped declaration cannot replace an
existing index of the same name or weaken its uniqueness. Degraded-equivalent-name
reuse remains unsupported; differently named equivalent definitions still return 85.
Native Rust/Python index-request semantics are unchanged.

Successful commands with reduced behavior include `briskdbIndexWarnings`, an
ordered array of `{name, reducedBehavior}` documents (plus `skipped: true` for
text declarations). PyMongo's `create_index()` /
`create_indexes()` helpers return names and discard these extra reply fields;
inspect a raw command reply or use PyMongo command monitoring to see them:

```python
result = client.app.command(
    "createIndexes", "events",
    indexes=[{"key": {"created": 1}, "name": "created_lookup", "expireAfterSeconds": 60}],
)
assert result["briskdbIndexWarnings"] == [{
    "name": "created_lookup",
    "reducedBehavior": ["ttl: expiration is not performed"],
}]
```

For `{"key": {"body": "text"}}`, the warning is
`{"name": "body_text", "skipped": true, "reducedBehavior": ["text: entire index is skipped; $text queries are not supported"]}`.
PyMongo still returns the requested name even though no index is created; consult
the warnings and catalog rather than treating that name as proof of index support.
All-text batches retain the Ready index count and, on an absent collection,
create only the empty collection/built-in ID index, matching the frozen TinyMongo
memory/JSON behavior (its SQLite backends leave that namespace absent). Key/name
and option-shape validation still runs eagerly. Skipped partial predicates are not
compiled or applied, but sparse plus partial remains invalid.

Malformed options, unsafe unique combinations and oversized warning replies fail
before any mutation. Warnings are not durable index metadata; retries return them
again. These compatibility options do not add a background worker, hashing,
expiration, full-text search, ordered index scans or distributed transactions.

Mongo `dropIndexes` and sync/async PyMongo `drop_index` / `drop_indexes` now use
the shared engine's exclusive removal path. String selectors accept an exact
name, an unambiguous legacy single-field alias, or `*` for every secondary
definition (including native Pending declarations). `_id` / `_id_` are protected
with code 72; missing namespaces/indexes report 26/27. The reply's `nIndexesWas`
counts Ready indexes under the same admission, including the built-in ID index.
No namespace is implicitly created, no document is deleted, and allocator history
is retained. Validated options include bounded `maxTimeMS`, acknowledged write
concern, and null comments; document sequences and unsupported options fail before
removal. Key documents and name arrays are not implemented (code 14); see the
broader [Mongo command syntax](https://www.mongodb.com/docs/manual/reference/command/dropindexes/).
Exact names take priority over aliases; ambiguous aliases return 115 rather than
depending on backend-specific ordering. Field/name selectors are bounded to 255
UTF-8 bytes. Completed removals remain committed after an error; recovery finishes
only the currently admitted drop, leaving later indexes intact. Wildcard removal
is not a cross-index atomic transaction.

The shared Rust index catalog now assigns stable, root-wide IDs to built-in and
pending secondary indexes. The version-17 manifest upgrade preserves existing
specification bytes; declarations and allocation commit together, and committed
namespace drops never recycle IDs. These IDs are not new wire/Python fields and
do not activate physical indexes, uniqueness enforcement, or index cursors.
Native Rust/Python can remove a pending declaration by exact name, with built-in
ID protection, transactional identity cleanup and non-reuse on recreation. This
is independent of the wire selection/removal path described above.

The version-18 upgrade adds empty physical secondary-index storage with a
checksummed, restartable per-shard layout upgrade and an older-binary fence.
Namespace creation/deletion owns the reserved tables; existing records, index
specifications and IDs are preserved. This is storage groundwork, not index
activation by itself. Version 19 adds explicit native Rust/Python non-unique
builds with journaled shard progress, atomic Ready publication, transactional
entry maintenance and restart coverage/checksum validation. Unpublished builds
are discarded on reopen without changing BSON or declaration IDs. Version 20
adds the older-writer fence for secondary uniqueness using the same entry format
and lifecycle. Broader planner use, whole-bulk post-image parity and selector compatibility remain
open under #174. Native
Rust and sync/async Python can also drop built indexes through the existing exact
name API. A journaled, sole-process cleanup removes derived entries and metadata,
preserves BSON/other Ready indexes/allocator history, and finishes on reopen after
an interruption. Pending drops retain their lightweight concurrent path.

`listDatabases` on `admin` supports `nameOnly: true`, including ordinary
sync/async PyMongo `list_database_names()` and
`list_databases(nameOnly=True, filter={"name": ...})`. It returns only
`databases: [{name: ...}]` and `ok`, without a cursor, size fields, or invented
admin/local databases. The catalog contains at most 64 logical document
databases with names up to 63 UTF-8 bytes, so this reply is intrinsically bounded.
SQL namespaces/internal tables are hidden. Discovery never creates a namespace;
creation and dropping the last collection determine catalog membership. Each
request reads one validated snapshot, with the same cancellation/deadline and
hard result limits as other shared-engine reads.

Filters support shared matcher predicates on `name`, including `$and`/`$or`/
`$nor` combinations; other output fields are rejected, even on an empty catalog.
`authorizedDatabases` accepts a boolean with no additional filtering on this
unauthenticated listener. Comments must be omitted/null. A positive `maxTimeMS`
narrows the ordinary deadline. Full `listDatabases` (omitted/false `nameOnly`)
and statistics-dependent filters are unsupported, not empty/zero estimates.
Mongo defines `sizeOnDisk` as database file bytes; BriskDB's logical databases
share files, so per-database physical accounting remains separate work.

For these data commands, unknown fields and unsupported option values fail
before storage admission. The current option contract is:

| Option | Accepted behavior |
| --- | --- |
| `maxTimeMS` | Nonnegative integer; a positive value narrows the 15-second command deadline. Find, aggregate, and collection/index metadata retain the remaining execution budget across batches; client idle time is not charged. Positive getMore values require unsupported tailable/awaitData semantics and are rejected. |
| `$readPreference` | A document containing only a recognized `mode`; the standalone engine serves the request |
| `ordered` | Boolean; defaults to `true`, with ordered/unordered partial-failure behavior |
| Insert `writeConcern` | Omitted/empty, or `w` equal to 0 or 1, `j: false`, and `wtimeout: 0`; no replication or stronger durability is promised |
| Drop/create `writeConcern` / `comment` | Same concern subset except `w: 0` is rejected; comment must be omitted or null (PyMongo's default). No replication or unacknowledged namespace mutation. Metadata discovery also accepts only omitted/null comments. |
| `bypassDocumentValidation` | Only `false` |
| Find `skip` / `limit` | Nonnegative integers; zero limit means no additional limit |
| Count `query` / `skip` / `limit` | Shared BSON matcher with global skip/limit; nonnegative integers, zero limit unbounded; absent collection returns zero |
| Distinct `key` / `query` | BSON string key and optional document filter; shared identity and global encounter order, absent collection returns an empty values array |
| Find `projection` | Basic inclusion/exclusion document, dotted/nested paths, arrays, and `_id` rules; validated before missing-collection handling |
| Find `sort` | Up to 32 ordinary fields with numeric `1`/`-1` directions; global BSON order with stable natural-order ties. Empty document preserves natural order. Metadata/expression sorts are unsupported. |
| Find `batchSize` | Integer from 0 through 1000; zero opens an empty initial batch. Default 101. |
| Aggregate `pipeline` / `cursor` | Required stage array and cursor document; cursor accepts only `batchSize` from 0 through 1000 (default 101). Basic stages plus project/set/addFields/unset; absent collection returns empty after validation. |
| Aggregate `allowDiskUse` | Only `false`; there is no disk spill |
| `getMore` `batchSize` | Integer from 1 through 1000; default 101. Pages also end at the wire byte budget. |
| Find `singleBatch` | Boolean; `true` intentionally returns only the first batch, with cursor ID zero |

Insert commands accept up to 1000 documents per wire batch, within the advertised
1-MiB message and 512-KiB document limits. Sequence decoding shares one 4-MiB
decoded-memory budget. BSON validation, generated-ID size checks, and engine
result limits are checked before any document write. Direct non-ID zero
timestamps are server-stamped; nested timestamps and IDs are preserved. Each
write commits separately, including within one shard; cancellation or a storage
failure can leave prior successes committed. No batch transaction is promised.

Retained find cursors use positive opaque IDs scoped to the listener and exact
namespace. A dedicated engine session lets a cursor move between pooled TCP
sockets; disconnect cleanup follows the last socket that used it. This is an
anonymous loopback capability, not authenticated logical-session support. At most
32 wire cursors and 8 per socket may be retained, within the shared engine quota.
Idle cursors expire after 10 minutes; failed continuations/response encoding,
explicit kills, disconnect, listener close, and engine shutdown release resources.
An idle socket may wait 10 minutes, while partial frames and commands remain
bounded to 15 seconds. Exhaustion returns ID zero; stale or wrong-namespace IDs
return code 43. Simultaneous use returns code 237. No SQLite lease is held between
batches, and no cross-batch snapshot is promised under concurrent writes.

Projection uses the shared engine transform after filtering; projected fields
retain BSON types and stored field order. The same projection persists across
getMore batches. Path collisions, mixed modes, and unsupported operators fail
explicitly; numeric array-index and positional output paths are not supported.
Wire byte limits apply to returned documents after projection. Required CI adds
4,865 projection comparisons against the source-locked oracle and real-driver
checks for nested arrays, continuation, unchanged storage, and restart.

The shared Rust `DocumentSorter` derives bounded BSON ordering keys, with
4,654 source-locked differential cases in required CI. Sorted find applies
filter, global sort, skip/limit, and projection in that order; continuations
retain the sort key and natural-order tie-breaker. It scans the routed shards for each
bounded top-key window until sorted indexes exist. The window holds at most
1024 keys and a conservative 64-MiB heap charge, not all matching documents.
Large skips may require repeated scans and pages may be short at an internal
window/memory boundary. Cursor key growth shares the existing retention quota.
Real-driver tests cover chained sorts, find_one, projected-away sort fields,
compound array keys, stable ties, byte-bounded batches, and restart.

Counts are exact over the documents observed during the operation; the
`estimated_document_count()` driver method currently uses this same count path,
not an approximate cached statistic. Exact-ID queries remain point-routed; other
queries use the shared scatter matcher, with literal-ID-list shard pruning
(empty filters use per-shard row counts).
Concurrent writes do not have a cross-shard snapshot guarantee. Count does not
open a cursor. Invalid queries/options fail before absent-collection handling.
Negative legacy count limits, hints, collation, comments, and read concern are
explicitly rejected. PyMongo `count_documents()` sends an aggregation pipeline
ending in `$group` with a literal `_id: 1`; it now works through the shared
aggregation core, including sync/async filtering, skip/limit, missing namespaces,
and restart. A safe leading ID match also narrows its source shards. It inherits
aggregation's consumed-row/work bounds and rejects an
explicit `limit=0` (15958), unlike legacy count. Unsupported aggregate options
remain rejected. Native embedded `Session.count_documents()` uses the separate
engine count command.

Distinct uses the [shared extractor and resource bounds](DOCUMENT_ENGINE.md#distinct-values).
It retains the first exact BSON representation, skips missing paths, includes
null, and flattens only the final array by one level. Dotted components traverse
documents, not intermediate arrays: this follows the frozen TinyMongo source.
Invalid keys/filters/options fail before absent-collection handling. Distinct is
a single bounded reply, not a cursor; bootstrap row/byte limits also apply.
Real-driver tests cover semantic aliases, arrays, filtered point reads, global
order, async calls, restart, unchanged storage, and whole-result byte rejection.
Required CI adds 4,888 frozen-oracle extraction/identity cases without modifying
the full candidate command corpus or its allowlists.

The shared [basic aggregation core](DOCUMENT_ENGINE.md#basic-aggregation-core)
validates `$match`/`$sort`/`$skip`/`$limit`/`$count` before reading storage. Native
and wire aggregate commands now use incremental global execution and the normal
cursor registry. Streaming stages retain counters, count avoids retaining source
documents, and sort uses bounded materialization; no spill or indexed aggregation
is promised. Required CI compares 5,134 complete pipelines in both execution
modes against the frozen implementation, separately from the full candidate
command corpus. Real-driver tests cover sync/async paging, empty initial batches,
pooled-socket handoff, byte caps, cleanup, validation and restart. Hints, comments,
collation, read concern, sessions and other unimplemented options fail explicitly.
Projection stages now share `$project`, `$set`, `$addFields`, and `$unset`, with
`$literal`/`$ifNull`/`$size`, field references, and `$$REMOVE`. Required CI compares
another 7,037 source-locked whole pipelines in both modes. Exact field order,
original-input assignment semantics, nested arrays/missing values, eager errors,
and lazy limit consumption are preserved. Transform allocation/work/depth limits
bound computed-output amplification before delivery. Real sync/async driver tests
cover paging, expanded result byte caps, runtime-error cleanup, and restart.
Shared `$group` now supports literal, field, and computed keys (including objects/arrays)
and all eight planned accumulators: addToSet, avg, first, last, max, min, push,
and sum. Required CI covers 9,509 accumulator and 5,663 key pipelines in both modes;
5,424 explicitly compare against frozen expression-plus-group composition, since
the frozen group grammar itself rejects non-null literal/computed keys. Grouping follows
the global input order and retains bounded accumulator state, with exact BSON
identity and Decimal128/mixed-numeric semantics. Arithmetic Double NaN payloads
are deliberately canonicalized; other representations remain exact. See the
[group contract](DOCUMENT_ENGINE.md#aggregation-groups-and-numeric-accumulators)
for integer result-type/overflow boundaries versus the frozen reference and
MongoDB. Real-driver tests cover structured/numeric results, byte paging,
whole-group size/memory rejection, no partial group replies, cleanup and restart.
Group keys use the existing expression subset without `$$REMOVE`/`$$ROOT` variables.
Partial-shard accumulator merging, additional expressions, and full
candidate-corpus acceptance remain open.

Mongo `delete` now supports filtered `delete_one`/`delete_many`, ordered and
unordered selector batches, array and OP_MSG sequence forms, indexed validation
errors, acknowledged results, and the existing unacknowledged write subset.
Selectors use the shared BSON matcher and exact-ID routing. Missing collections
return zero without creation. `limit` is exactly 0 (many) or 1 (one); collation,
hint, let, retryable writes, and stronger write concerns remain unsupported.
Operational errors abort the command and can follow committed shard writes;
only prevalidated statement errors participate in ordered/unordered continuation.
See [delete commit boundaries](DOCUMENT_ENGINE.md#filtered-deletion-and-commit-boundaries).
Mongo `findAndModify` now supports `remove: true`: shared filter, sort, projection
(`fields`), pre-delete `value`, `lastErrorObject.n`, and null on no match.
Runtime sort checks apply even to exact-ID routes. Return-value budgets fail
before deletion, including documents inserted through the larger native BSON
interface. Missing collections return null without creation after eager semantic
validation. Upsert, hint/collation/let,
and unacknowledged findAndModify remain unsupported. Native Python exposes
`find_one_and_delete` with the same shared execution and request controls.

Replacement `findAndModify` accepts a replacement document in `update`, optional
`remove:false`, `query`, `fields`, `sort`, and boolean `new` (default false).
The shared `FindOneAndReplace` command validates both the normalized stored
post-image and the selected before/after reply before mutation. Replies include
`value` (null on no match) and `lastErrorObject.n`/`updatedExisting`; a projected
empty document still reports a match. Without upsert, missing collections are not created, and
validation remains eager. `remove:true` cannot be combined with `update` or
`new:true` or `upsert:true`. Pipeline updates remain unsupported. Native
sync/async Python exposes `find_one_and_replace` with the same semantics.

Operator `findAndModify` accepts `$set`/`$unset`/`$min`/`$max`/`$pop`/`$rename`/`$addToSet`/`$pullAll`/`$push`/`$pull`/`$inc`
documents in `update`, with the
same query/sort/projection and boolean `new` options, through `FindOneAndUpdate`.
It preserves untouched fields and shares operator validation, immutable-ID
checks, post-image limits, and pre-commit return size/depth checks. No-ops still
return an image; projected `{}` still sets `n:1` and `updatedExisting:true`.
Without upsert, missing namespaces return null without creation after eager validation. Native
sync/async Python exposes `find_one_and_update` with identical image semantics.

Replacement and operator `findAndModify` accept boolean `upsert:true`, including
creation of a missing namespace. An insertion returns `lastErrorObject` with
`n:1`, `updatedExisting:false`, and `upserted` containing the exact inserted ID
(including null). `value` is null for `new:false` and the projected inserted
document for `new:true`. Existing matches retain `updatedExisting:true` without
`upserted`, even for projected `{}` or no-op updates. Inserted-ID metadata plus
the optional image are budgeted together before commit. Rechecks under the target
shard write lock return the actual atomic image. `remove:true` plus `upsert:true`
and unacknowledged findAndModify remain explicitly unsupported.

Wire `update` supports replacement statements and `$set`/`$unset`/`$min`/`$max`/`$pop`/`$rename`/`$addToSet`/`$pullAll`/`$push`/`$pull`/`$inc` operator
statements (`q` and a document `u`) through shared `Replace`/`Update`.
Replacement and operator statements accept boolean `upsert:true`. Only operator documents accept
`multi:true`; replacement remains single-document. Both body arrays and
OP_MSG `updates` sequences preserve ordered/unordered per-statement errors and
`n`/`nModified`; an immutable-ID violation is code 66. Missing collections return
zero only after eager validation, without creating metadata, unless an
upsert requests namespace creation. Normalized
post-images, including retained IDs, must fit the advertised 512 KiB BSON cap
before mutation. Existing bounded write-concern and one-way write handling apply.
Batch statements commit independently; operational failure may leave previous
commits, so no all-or-nothing batch or retryable-write guarantee is implied.

Replacement upserts retain a direct/sole-`$eq` query ID (not regex), prefer a
BSON-equal replacement ID's exact representation, or generate an ObjectId.
Other query fields are not copied. Insertions report `n:1`, `nModified:0`, and
`upserted:[{index, _id}]`, including null IDs; matches report normal counts without
an upsert entry. PyMongo's null-ID `matched_count` is 1 despite an insertion;
`did_upsert` and the raw result distinguish it. Native counts remain 0/0 on insert.
Same-ID races recheck under the target shard's write lock. Duplicate errors are
indexed code 11000 and follow ordered/unordered continuation. Whole-batch reply
headroom and returned-ID depth are preflighted before each document commit;
oversized IDs cannot silently insert and then fail reply encoding. Tests cover
null/generated IDs, exact BSON, concurrency, mixed batches, large-ID sequence
budgets, reply-depth rejection, one-way writes, and restart. Non-ID predicates
do not gain cross-shard uniqueness or global snapshot semantics.

Operator upserts for both one/many statements derive a seed from positive direct
and `$eq` equalities, including `$and`, literal embedded documents and dotted
object paths, then run the operators before generating any missing ID. Query-bound
IDs remain immutable; the update can supply an unbound ID. Duplicate/overlapping
equalities fail with indexed code 54 before insertion. Dotted `_id` equalities
can seed an embedded ID; non-equality ID predicates do not supply values.
Range/regex/negation/alternative predicates do not supply values; singleton
`$in`/`$all` and other logical simplifications are not inferred. Query-seeded and
operator-assigned zero timestamps remain literal. Metadata, aggregate reply limits,
ID depth checks, one-way writes and duplicate handling share the replacement path.
Many-scope target-shard rechecks update all new matches there without promising a
global snapshot. Preparation failures before document writes and explicit successful
target-shard rollback can certify safe unordered continuation; failures after earlier
modified shards still abort. Another 3,544 source-locked executions compare exact
stored BSON, metadata, find-and-modify before/after images and errors with the unchanged upsert helper over its common
direct/sole-equality object-path behavior. Legacy AND/literal-document inference,
unsafe paths/IDs and numeric differences are independently tested, not waived.

Multi updates commit one immediate transaction per shard, scanning bounded
records in natural order. A runtime failure rolls back that shard but can leave
earlier shards committed. Only explicit successful rollback plus zero earlier
document modifications certifies a runtime statement failure as safe. For
certified validation/resource failures, indexed `writeErrors` now preserve
driver `WriteError` behavior: ordered batches stop; unordered batches may
continue. Earlier no-op shard matches do not count as persisted changes.
Earlier changed shards, uncertain commit/rollback outcomes, cancellation, and
operational failures still produce command errors with no fabricated partial
counts; unordered batches stop too. Parsing failures remain indexed as before.
Raw-wire tests force provisional writes before rollback and prior committed
shards, checking exact data after restart. The frozen add-to-set atomicity case
now passes unchanged in both API modes. This remains neither a global
transaction/snapshot nor exact MongoDB per-document or frozen TinyMongo
collection-wide failure atomicity. No reference/allowance is changed.
Other operators/pipeline updates and update-command
hint/sort/collation/arrayFilters remain explicit future work. Native sync/async
Python exposes `replace_one`, `update_one`, and `update_many` with the same engine semantics and
controls. The field-update subset includes bounded object/array paths, immutable
IDs, and exact modified counts. Min/max compare whole BSON values and preserve
equal-value representations; missing array slots differ from existing nulls.
Pop removes front/back array elements; rename moves fields but does not traverse
arrays. Missing pop targets or rename sources are no-ops; typed operand/path/ID errors precede
commit. Add-to-set supports literal values and `$each`, retaining existing
duplicates/types; pull-all removes all literal BSON-equal matches. Numeric aliases
compare equal, booleans stay distinct, and document field order matters. Strict
numeric paths, operand/target errors, growth, comparison work, and cancellation
are bounded. Push supports literal values and `$each`/`$position`/`$sort`/`$slice`,
always inserting, stably sorting, then slicing. Scalar sort compares whole BSON
values; compound sort follows the frozen document-only selectors, not query-sort
array selection. Integral numeric positions/slices clamp at array boundaries.
Scratch/comparison work and temporary growth remain bounded before final slicing.
Pull reuses shared literal/field/document matching with ordinary embedded-ID
semantics, strict update paths, missing-field no-ops, stable removal, and eager
validation even without matches. Query comparison, regex work, path allocation,
AST/program retention, and cancellation share update budgets. Context-specific
expression and regex errors remain indexed driver write errors when safe.
Increment implements numeric promotion, retained Int64 width, code-2 integer
overflow rejection, exact missing operands, and 15-digit Double-to-Decimal
promotion. Rounded Decimal and equal Double no-ops retain stored bits; executed
NaN arithmetic counts as modified even with identical output bytes. Its fixed
workspace, path growth and cancellation are bounded. See the shared
[numeric boundaries](DOCUMENT_ENGINE.md#field-updates-and-single-record-write-boundaries).
Its 30,489 source-locked update oracle cases include 4,008 object-only
set/unset, 4,719 min/max, 3,078 pop/rename, 4,440 array-membership, 4,459 push,
5,573 pull cases, and 4,212 increment cases. Increment compares the exact shared
non-ID object-path subset: frozen Python's Int64 shrinking, unencodable overflow,
missing/signed-zero behavior, strict path/ID differences, and unspecified newly
computed Double NaN bits are independently tested, never coerced in the oracle. The
membership subset excludes ID writes and uses object-only add-to-set paths:
push/pull subsets cover non-ID object/array paths and embedded-ID queries. These
legacy reference helpers restore IDs (and add-to-set overwrites scalar parents) instead of
enforcing BriskDB's stricter safety rules. These boundaries have independent
tests, not frozen allowances or rewritten expected results. Independent
tests check resource and commit limits. Additional update operators remain
unimplemented.

The native index-definition foundation now validates ordered keys, normalizes
numeric directions and generates bounded default names before catalog mutation.
Required CI compares 64 valid ascending integer-key definitions with the unchanged
frozen index model, including pending metadata after restart. Descending/numeric
aliases and invalid/resource-limited definitions have independent tests. This is
not itself physical index support: secondary declarations still enforce no uniqueness.
Ready indexes separately support conservative equality candidates.
Required CI also compares
six build/drop/recreation discovery states and the reopened result with the
source-locked TinyMongo client, including built-in/name order and exact options.
Only ordered key pairs are represented as BSON documents for transport. The full frozen
index suites remain open; no frozen expected result or allowance is changed.

The shared index-key foundation now checks 7,201 additional source-locked cases
(29,370 document evaluations) against unchanged index helpers. It compares exact
opaque-token equality partitions and encounter order, sparse/partial membership,
compound/multikey behavior and unsupported-value failures. It reuses canonical
BSON identities and the shared matcher. Every generated key is also serialized
and restored through the versioned tuple codec before oracle comparison; opaque
reference tokens and the frozen generator remain unchanged. Independent tests cover bounded work,
eager validation, input immutability and interruption without partial results.
This pure helper is not physical index activation, wire support or uniqueness enforcement.
The frozen helper subset rejects intermediate/parallel arrays, object/nested-array
keys, ObjectId/date keys and nonfinite numeric keys. Unique storage still uses
that strict subset. Non-unique storage now retains such records as checksummed
fallback candidates, preserving complete matcher results through reads, writes,
builds and restart. The three previously failing nested-value frozen cases are
included in the expanded required candidate gate; mixed IndexModel options and
the full parity release gate remain open. Native Rust and sync/async Python declarations now accept sparse or
partial options after shared eager validation. The complete retained envelope is
bounded; exact filter BSON, IDs and membership options survive restart. Existing
flat declarations remain byte-compatible, and unknown legacy envelopes remain
opaque/readable. These options are metadata-only until physical activation;
native declaration validation does not scan records; wire removal uses the separate
exclusive lifecycle described above.
The separate native `CreateBuiltIndex` / sync/async `create_built_index` helper
now combines declaration and physical build on an existing collection,
with exclusive before/after Ready counts. Preflight rejects unsupported data and
combined budgets without publishing metadata. Restart removes newly created
unfinished declarations and entries, but preserves preexisting Pending ones.
It reuses the v19 cleanup journal and does not change the frozen contract.
Version 20 additionally permits unique builds. Equality candidates use the separately validated
Ready-cache read path. Native batch and wire creation
now reuse that lifecycle, with completed-prefix recovery tests at every new-entry
commit boundary on two- and four-shard roots. TinyMongo's broader IndexModel
warning/degradation behavior and the full frozen index suites remain open.

Ready-index equality reads now have 1,087 additional source-locked probe groups:
201,349 matcher evaluations and 9,541 eligible matching candidates without false
negatives. Direct/positive-conjunctive complete scalar tuples use bound BDIK
probes, preserve natural paging and still run the full matcher. Partial indexes,
sparse all-null tuples and unsupported/incomplete shapes scan. Native/real-driver
tests compare filters, sorting, count/distinct, index drop/recreation between
batches, write maintenance and reopened results. This does not close #174 or
#178: bulk-policy decisions, broader candidate forms and plan diagnostics remain open.

Ready-index candidates also support necessary positive literal `$in` lists,
including complete compound tuples and residual predicates under `$and`.
Lists are limited to 128 scalar members, with at most 128 distinct candidate
tuples and 1 MiB of encoded keys across the selected probe. Existing complete
equality probes keep priority. Regex/array/object/unsupported members, empty or
oversized lists, partial indexes and possible sparse all-null tuples are not
eligible for finite-key probing.
Bound values and the existing non-unique fallback marker are checked through
the full matcher. Multiple matching array entries are deduplicated before
pagination, so reads and writes visit each logical record once. Independent
candidate comparisons, scan differentials, physical-selection and checksum
checks cover this extension without changing the frozen equality-probe oracle.

Necessary positive `$exists: true` conditions can also select all entries of a
current sparse index after finite key probes have been considered. Direct and
positive-conjunction predicates on any indexed path qualify, including compound
sparse indexes. Explicit null, empty arrays and conservative non-unique fallback
entries remain candidates; the full matcher removes false positives. Logical
negations, alternatives, partial-index and unproven presence shapes retain scans. Multikey
rows are grouped before pagination, current entry bindings are checked, and
field-removing mutations keep record/index changes in the same transaction.
This is not range pushdown, an ordering promise, or new aggregation filtering.

Necessary direct/positive-conjunction `$exists: false` clauses can provide null
keys in complete finite probes. Explicit-null candidates are removed by the full
matcher, and uncertain stored shapes retain fallback entries. A sparse all-null
tuple remains unsafe; a necessarily nonnull companion path can make a compound
sparse probe eligible. Singleton joins now preserve document-first streaming
even when statistics underestimate large null-key groups. This bounds frontier
sorting but can still walk nonmatching SQLite entries; it is not an index-only
or index-order scan. Existing format, routing and aggregation accounting remain.

Bounded positive `$or` combinations can also supply finite candidate tuples.
Each indexed path needs a necessary supported equality, literal membership or
absence witness in every alternative. AND chooses a necessary witness rather
than intersecting values that may match different array elements. A single
borrowed-value buffer caps each path at 128 raw operand occurrences, including
duplicates and failed attempts; existing tuple/byte budgets still apply. Compound
unions may admit cross-branch combinations, which the full matcher removes.
Unbounded/unsupported branches, partial indexes and possible sparse all-null
tuples remain conservative. Necessary finite probes keep priority across indexes,
then logical probes, then sparse-presence scans. Native and real-driver fixtures
cover overlapping multikey matches, residuals, mutation/upsert, churn, corruption
and restart without changing the frozen public equality-probe oracle.

The same candidates now narrow mutation selection for one/many updates and
deletes, replacements and sorted find-and-modify, including upsert rechecks.
Full predicate/identity rechecks and per-shard transactions remain authoritative.
Natural-order advancement avoids repeat updates when index entries change;
rollback and cancellation preserve earlier commits without claiming atomicity
across shards. Physical-selection tests, scan differentials and sync/async driver
coverage verify the write path independently of read acceleration.

Sessions, retryable writes, replication and change streams are not advertised.
Zlib is advertised only in response to a valid matching compression offer.
This is not full TinyMongo or MongoDB compatibility. Required
real-driver CI also verifies BSON fidelity, ordered/unordered duplicate failures,
driver batch splitting, cursor paging/closing, pooled-socket handoff, concurrent
reads/writes, byte-bounded batches, reconnection, resource limits, and
persistence after closing and reopening BriskDB. Raw socket tests independently
cover server-generated IDs, null preservation, and aggregate decode limits.
Filter CI additionally compares more than 20,000 generated BSON cases directly
against the source-locked TinyMongo matcher, including nested-array paths,
operator validation, regex boundaries, and numeric families. This separate
matcher matrix does not mark the full frozen candidate command corpus as passed.
The broader #167 consumer/dialect conformance work remains open.

## Versioned files

[`compat/mongo/v1/manifest.json`](../compat/mongo/v1/manifest.json) is the
entry point. It records the source commit and contract-tree digests, suite and
backend dimensions, file hashes, reference target, counts, and the reviewed
capability inventory. That inventory includes client, database, collection,
and cursor APIs; options; BSON values and ordering; query and update operators;
projections; indexes; aggregation; result shapes; warnings; errors and codes;
and unsupported behavior.

The remaining files have distinct roles:

- `corpus.json` assigns stable IDs, suites, APIs, and source provenance to each
  logical case. The source-file hashes cover the complete TinyMongo contract
  tree at the locked commit, and the harvest metadata pins Python, pytest, and
  PyMongo.
- `reference-results.json` holds the normalized `tinymongo-memory` outcomes for
  all 228 cases through both APIs.
- `semantic-variants.json` records reviewed backend-specific branches and skips
  already present in the locked TinyMongo tests. These entries explain the
  intended behavior; they do not permit a candidate result mismatch.
- `intentional-differences.json` is the strict candidate-difference allow-list.
  Every entry is scoped to one target, case, and API and requires a BriskDB
  issue. Its current sync and async entries record the `mongodb-mongodb` skip
  for a Regex `_id`, which real MongoDB forbids. When that target is present,
  an entry that no longer matches the observed result is stale and makes the
  comparison fail.
- `runner/contracts/` contains BriskDB-owned, target-neutral copies of the 228
  executable contract bodies. `runner/adapters/` is the only target-specific
  boundary, with modules for TinyMongo, real MongoDB, and a configurable
  BriskDB candidate endpoint.

The manifest hashes every checked-in contract input. Changing the source
commit, corpus, reference results, capability inventory, semantic variants, or
difference policy therefore requires a normal reviewed BriskDB change. Do not
silently refresh a snapshot while implementing a candidate.

## Process boundary

The BriskDB Rust library, Python wheel, and server do not declare or load
TinyMongo. CI installs TinyMongo, PyMongo, and pytest separately inside the
Mongo contract test environment. None is pulled in by BriskDB's Cargo or Python
dependency metadata, and production code does not import the oracle.

Source distributions, including the Rust `.crate` and Python sdist, may retain
the checked-in manifest, fixtures, adapters, and harness for provenance and
offline audit. Those files are inert package source: a normal build or install
does not execute them or install TinyMongo. Built wheels and binaries do not
contain the TinyMongo package.

The normalization harness in
[`scripts/mongo_parity.py`](../scripts/mongo_parity.py) uses only the Python
standard library. The executable fixtures use pytest plus PyMongo's public BSON
and exception types as the compatibility vocabulary. They do not import
TinyMongo. The only TinyMongo import in the owned runner is lazy and isolated
in `runner/adapters/tinymongo.py`; selecting the BriskDB or MongoDB adapter does
not load it.

A producer identifies each execution with the exact JUnit properties
`tinymongo.api`, `tinymongo.backend`, and `tinymongo.suite`. Its test name maps
to a stable corpus case ID. Assertions inside the producer cover ordered BSON,
document mutation, result and cursor behavior, warning categories, exception
classes and stable codes, and explicit unsupported operations. The normalized
result preserves the target, API, backend, suite, outcome, and a normalized
observation. The observation combines a category with a SHA-256 fingerprint of
the outcome, category, and redacted reason, so a different failure cannot pass
by sharing only the expected outcome.

This adapter boundary lets the owned fixtures exercise each implementation and
publish the same result envelope. TinyMongo remains an oracle-only test
dependency. Its adapter source may remain visible in a source distribution, but
the oracle is neither installed nor imported by building, installing, or using
BriskDB. An optional real MongoDB target follows the same boundary.

## Validate and report

Validate the locked files and hashes from the repository root:

```bash
python3 scripts/mongo_parity.py validate
```

If a TinyMongo Git checkout is available, verify the locked source objects
against it. This reads blobs from the manifest's commit and does not trust the
checkout's working tree:

```bash
python3 scripts/mongo_parity.py verify-source \
  --source-root /path/to/tinymongo
```

Install the pinned test tools and the locked TinyMongo checkout into a test
environment, then reproduce the reference with BriskDB's owned fixtures:

```bash
python3 -m pip install -r compat/mongo/v1/runner/requirements.txt
python3 -m pip install --no-build-isolation --no-deps /path/to/tinymongo

mkdir -p target/mongo-parity
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=tinymongo \
  --mongo-contract-backend=memory \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/tinymongo-memory.xml

python3 scripts/mongo_parity.py ingest \
  --implementation tinymongo \
  --junit target/mongo-parity/tinymongo-memory.xml \
  --output target/mongo-parity/tinymongo-memory.json

cmp compat/mongo/v1/reference-results.json \
  target/mongo-parity/tinymongo-memory.json
```

CI checks out commit `53cbf44e98b8caa036163725d195fd29592e1cc0`
under `target/`, installs it only in test jobs, runs the same 456 owned
fixture executions, and requires the byte comparison to pass. The source
snapshot records its original Python 3.9.6 harvest environment. CI replays it
with Python 3.9.25, the pinned 3.9 patch available for Ubuntu 24.04.

Generate the currently available reference report:

```bash
mkdir -p target/mongo-parity
python3 scripts/mongo_parity.py report \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md
```

The report should say `reference-only`. CI runs both commands, publishes the
Markdown report in the workflow summary, and uploads the JSON and Markdown as
the `mongo-parity-report` artifact. This baseline comparison permits absent
optional targets, including real MongoDB.

To normalize JUnit produced by a candidate adapter:

```bash
python3 scripts/mongo_parity.py ingest \
  --implementation briskdb \
  --junit target/mongo-parity/briskdb.xml \
  --output target/mongo-parity/briskdb.json

python3 scripts/mongo_parity.py report \
  --require-target briskdb-briskdb \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md \
  target/mongo-parity/briskdb.json
```

`--require-target` is repeatable for publish gates. `report` fails when a
required target is absent, or for an uncovered case, an unexpected observation
fingerprint, or a stale intentional difference. An intentional difference must
name the exact reference and candidate fingerprints as well as their outcomes.
Once a BriskDB candidate endpoint exists, its normalized result belongs in the
required CI comparison; the checked-in TinyMongo reference remains the
comparison baseline.

A publish gate that includes reviewed optional targets should also require
every target named by the allow-list and supply its normalized result:

```bash
python3 scripts/mongo_parity.py report \
  --require-target briskdb-briskdb \
  --require-allowlist-targets \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md \
  target/mongo-parity/briskdb.json \
  target/mongo-parity/mongodb.json
```

Without `--require-allowlist-targets`, absent optional targets do not fail a
reference-only or partial comparison. If an allow-listed target is present,
its stale entries always fail regardless of that flag.

## Runner targets and options

The owned pytest runner accepts these target controls:

| Option | Environment fallback | Behavior |
| --- | --- | --- |
| `--mongo-contract-target=tinymongo|mongodb|briskdb` | `BRISKDB_MONGO_CONTRACT_TARGET` | Selects one lazy-loaded adapter. |
| `--mongo-contract-api=sync|async|both` | `BRISKDB_MONGO_CONTRACT_API` | Runs one API or both; the default is both. |
| `--mongo-contract-backend=<id>` | `BRISKDB_MONGO_CONTRACT_BACKEND` | Selects a TinyMongo backend; the default is `memory`. |
| `--mongo-contract-mongodb-uri=<uri>` | `BRISKDB_MONGODB_URI` | Supplies the optional real MongoDB endpoint. |
| `--mongo-contract-briskdb-uri=<uri>` | `BRISKDB_MONGO_PARITY_BRISKDB_URI` | Supplies the BriskDB candidate endpoint. |
| `--mongo-contract-require-target` | none | Fails instead of skipping when an optional target is unavailable. |

For example, an available real MongoDB instance can produce its normalized
candidate this way:

```bash
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=mongodb \
  --mongo-contract-mongodb-uri="$BRISKDB_MONGODB_URI" \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/mongodb.xml

python3 scripts/mongo_parity.py ingest \
  --implementation mongodb \
  --junit target/mongo-parity/mongodb.xml \
  --output target/mongo-parity/mongodb.json
```

Run the same corpus against a BriskDB candidate endpoint when one is available:

```bash
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=briskdb \
  --mongo-contract-briskdb-uri='mongodb://127.0.0.1:27018/?directConnection=true' \
  --mongo-contract-api=both \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/briskdb.xml
```

The candidate adapter uses PyMongo's sync and async transports but retains the
implementation identity `briskdb`. The runner records transport separately so
UUID configuration, Regex decoding, bytearray encoding, client-side
validation, and warning behavior follow PyMongo without inheriting semantic
exemptions that belong only to real MongoDB. No candidate endpoint is built in
this issue, and the reference-only CI job does not attempt this command.

## Refreshing the source snapshot

Refreshing the corpus is a compatibility-policy change. Check out the reviewed
TinyMongo commit separately, run its entire contract matrix to JUnit, then use
`snapshot` with the full 40-character commit:

```bash
python3 scripts/mongo_parity.py snapshot \
  --junit /path/to/tinymongo-contract.xml \
  --source-root /path/to/tinymongo \
  --source-commit <reviewed-commit> \
  --python-version 3.9.6 \
  --pytest-version 8.4.2 \
  --pymongo-version 4.17.0 \
  --corpus-output compat/mongo/v1/corpus.json \
  --results-output compat/mongo/v1/reference-results.json
```

After generating a reviewed snapshot, repeat the same `snapshot` command with
`--check`; this regenerates the canonical bytes and fails on drift without
rewriting either output file.

Review the source-tree identity, capability inventory, case additions and
removals, target-specific semantics, pinned harvest toolchain, normalized
reference results, and every intentional difference. Then update the manifest
counts and hashes, run `snapshot --check`, `validate`, and `verify-source`
against the updated manifest before committing the refresh.
