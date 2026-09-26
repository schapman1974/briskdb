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
application matrices, security or hardening work. Index acceptance is accounted
for separately below; the reviewed bulk-write boundary is #74/#183. Issues
#181/#185/#186/#187/#188 retain their separate acceptance criteria.

The [stopped Mongo import/backup/restore and rollback drill](OFFLINE_BACKUP.md#mongo-import-restore-and-rollback-drill)
separately verifies real TinyMongo SQLite sources, four-shard BriskDB import,
explicit index builds, stock PyMongo reads/writes across restore, and an unchanged
original source for pre-cutover rollback. It does not provide online backup or
reverse migration of writes made after cutover.

`compat/mongo/query-suite-inventory.json` separately accounts for all 115 test
functions in the locked `test_query_more.py`,
`test_query_operator_coverage_edges.py`, `test_client_read_fidelity.py`,
`test_insert_many_semantics.py`, `test_client_configuration.py`,
`test_projection.py`, `test_projection_memory.py` and `test_unset_semantics.py`
suites (259 reference parameter cases). Sixty-seven owned wheel tests check public
query/write/index results, Mongo
error codes, numeric path fanout, missing versus zero candidates, Decimal128 and
regex behavior. Hashes, exact function membership, reference collection counts
and actual candidate test symbols are validated; these are adapted scenarios,
not 259 unchanged upstream candidate passes or complete #186 certification.
Private bulk-planner monkeypatches, fake backend retry counts and TinyMongo's
no-PyMongo fallback errors module are excluded with explicit rationales; real
BriskDB duplicate/concurrency outcomes and single-pass client encoders are tested.

Local sync/async client `find()` calls now snapshot caller-owned filters and
projections after driver option validation. Later mutations of those mappings
cannot change an already-created cursor, its clone or its rewind. Bare-string
projections are rejected instead of being interpreted as individual character
field names. These helpers retain real PyMongo cursors and do not modify ordinary
PyMongo classes; they snapshot query inputs, not database contents or transactions.
Projection coverage includes nested/scalar/array paths, exact conflict codes,
scan/index/sorted/reopened reads, BSON ID fidelity and sync/async `$unset` no-ops.
The 64 x 100KB projection/first/count/full-read memory check measures Python-client
heap only, not native Rust RSS. TinyMongo-only backend plugins, private SQL traces
and decoder call counts remain explicitly excluded from unchanged-pass claims.

Read-fidelity checks cover recursive OrderedDict/UserDict/SON and parameterized
mapping aliases, sync/async find/clone/projection/aggregate/distinct/find-and-modify,
client-local timezone options, persisted BSON millisecond precision (including
pre-epoch values), and zero modifications for same-millisecond writes. The
reference's five storage backends are not claimed as BriskDB backends. The full
database-statistics assertion remains an explicit #166 gap: `list_databases()`
without `nameOnly=True` still returns code 115, not fabricated storage statistics.

Local client construction now validates driver options before acquiring storage.
Invalid document classes, timezone options, URI database paths or pool settings
do not create files or reopen/recover an existing root. The pinned PyMongo
constructor is used once; sync/async clients bind their actual loopback endpoint
before topology/background work. Regression tests also check shared patch
ownership, valid codec forms, aliases and that no placeholder/original host is
contacted. TinyMongo-only backend knobs and duplicate folder aliases remain
explicitly rejected rather than silently ignored.

The inventory explicitly distinguishes modern PyMongo from TinyMongo's legacy
cursor `.count()`/negative indexing and list-shaped index metadata. It also
records Python values that BSON cannot encode: arbitrary objects, non-string
field names and integers outside Int64 fail in the driver, rather than becoming
stored Python values or receiving a server error code. Private Python helper
return objects are replaced by observable query outcomes; the two private tests
for injected Python conversion failure and custom missing-sentinel identity are
implementation-specific, not claimed as reproduced branches. Native limits,
query semantics and the immutable frozen v1 contract are unchanged.

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
This is coverage for two baseline CRUD fixtures, not all ODM features or the
larger #181 acceptance matrix; it does not change the frozen
corpus, reports or difference policy.

To reproduce, install `tests/mongo_odm_requirements.txt` and the source-locked
TinyMongo package in a separate Python 3.13 environment, then point to its checkout:

```sh
BRISKDB_MONGO_ODM_PYTHON=python3 \
BRISKDB_MONGO_ODM_SOURCE_ROOT=/path/to/locked/tinymongo \
  cargo test --locked --no-default-features --features mongo --test mongo_odm \
  -- --ignored --nocapture
```

### Unchanged Talk Python wire contracts

The same pinned application environment also runs the full locked
`tests/contracts/test_talkpython_contract.py` using its original pytest fixtures,
support code, parameterization and async adapter. Only `TINYMONGO_MONGODB_URI`
changes to the real four-shard BriskDB listener. The source's `mongodb` target
profile means stock-PyMongo wire behavior, not a claim that the candidate server
is MongoDB. All 58 cases (29 sync and 29 async) must pass before and after a full
engine/listener restart. The 348 other TinyMongo backend variants are outside
this wire target, not skipped candidate cases. No test globals, application
methods or driver replies are patched.

The harness checks five source/configuration hashes, exact case identities and
per-case API/backend/suite metadata. Missing, duplicate, failed, errored, skipped,
or unexpected cases fail acceptance. Each pytest phase has a 120-second bound;
the Rust parent has a 180-second cleanup bound. Existing ODM and driver timeouts
are unchanged. Upstream fixtures drop their own databases on teardown, so a
separate BSON/index sentinel verifies same-root restart; the harness does not
claim those dropped application documents persist. Fresh JUnit reports prevent
stale results from satisfying a new run. `mongo-real-talkpython-results` contains
phase JSON inventories and raw JUnit XML, separately from the frozen corpus.

This covers the application's wire contracts for query/projection/sort/cursors,
CRUD/upserts, indexes/uniqueness, BSON fidelity and errors. The source's original
wire profile deliberately excludes TinyMongo-client-only conveniences such as
bytearray encoding and Python index warnings; it does not test the entire Talk
Python application deployment or user-supplied large apps. Those remain #181
work, not hidden xfails or extra frozen-corpus allowances.

```sh
BRISKDB_MONGO_ODM_PYTHON=python3 \
BRISKDB_MONGO_ODM_SOURCE_ROOT=/path/to/locked/tinymongo \
BRISKDB_MONGO_TALKPYTHON_REPORT_DIR=target/mongo-talkpython \
  cargo test --locked --no-default-features --features mongo --test mongo_odm \
  unchanged_talkpython_contracts_use_real_pymongo_before_and_after_restart \
  -- --ignored --exact --nocapture
```

### Real-driver cancellation checkpoint

The real-wire gate also deliberately cancels an in-flight stock PyMongo 4.17.0
async `getMore`, on fresh and reopened storage. A bounded loopback test proxy
forwards original packets unchanged and holds one actual server reply. Native
metrics prove that the server owns a cursor before cancellation and that the
disconnected socket/cursor drain before the client may close its cursor or reuse
its pool. The same client then reconnects, queries and counts the unchanged data.
No driver method, frozen adapter or command/result is rewritten. A reply-delivery
stall is not evidence that cancellation interrupted SQLite mid-execution, and
this read-only test makes no cancelled-write rollback/commit-outcome promise.
Python and Rust process/protocol deadlines bound both success and failure paths.

```sh
BRISKDB_MONGO_WIRE_PYTHON=python3 cargo test --locked --no-default-features \
  --features mongo --test mongo_wire real_pymongo_async_cancellation \
  -- --ignored --exact --nocapture
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

Rust hosts can inspect `MongoServer::client_metadata()` for at most eight active
connections, sorted by listener-local connection ID. The first successful modern
or legacy handshake freezes each connection's metadata; later monitoring hellos
cannot replace it, and an initial hello without `client` remains unrecorded.
Only a closed driver-family enum (PyMongo sync/async or Other) and, for recognized
names, an exact three-component unsigned-16-bit numeric version are retained.
Unknown names, arbitrary version strings, application/OS/platform/environment
values and extra fields are discarded, not logged, hashed or metric labels.
This deliberately redacted view follows the `client.driver` field layout in the
[MongoDB handshake specification](https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/),
not MongoDB's full client-metadata logging surface. It is untrusted diagnostic
information, never authentication. Metadata disappears on disconnect, failure,
abort or listener close; the API retains no history and changes no wire replies.

Ordinary PyMongo 4.17.0 synchronous and asynchronous clients can discover,
ping, inspect build information, insert one or many documents, and run bounded
find queries through the [shared BSON matcher](DOCUMENT_ENGINE.md), including
multi-batch reads with `getMore` and explicit `killCursors` cleanup. The legacy
`count` command, PyMongo `estimated_document_count()`, and `distinct()` also use
this engine through both synchronous and asynchronous clients. Basic aggregation
pipelines also use retained cursors over the shared global document engine.
Exact `_id` and `_id: {$eq: value}` filters keep single-shard routing.
Thirty-eight frozen BSON-ID vectors pin canonical v1 bytes and BLAKE3 digest
prefixes, including numeric/NaN/UUID aliases, ordered objects, arrays, regex and
scoped code. Initial-owner checks cover every supported shard count (2–64).
On 3-, 8- and 64-shard roots, typed point reads/updates/deletes verify actual
pool access; read counters independently report one record read on one shard.
Record bytes, natural order, identity keys, checksums and physical owners survive
a validated synthetic v17-schema upgrade and reopening. This is not a fixture
created by an archived release binary or proof of arbitrary future upgrades.
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
existing index of the same name or weaken its uniqueness. Ordinary PyMongo still
returns its own requested names; differently named equivalent definitions return
85 unless the explicit local-client compatibility mode below is selected.
Native Rust/Python index-request defaults are unchanged.

The optional local clients now provide
[TinyMongo-style model inputs and returned names](../python/API.md#local-index-model-compatibility-source-builds).
Their `create_indexes` uses the boolean `briskdbIndexModelCompatibility: true`
command option. This mode normalizes descending models to ascending equality
keys and permits reduced models to reuse an equivalent Ready index under the
same exclusive admission as the whole build batch. It returns ordered
`briskdbIndexNames`, and adds `reusedIndex` to the relevant warning. Skipped text
names occupy their original result positions without becoming catalog entries.
Non-unique built-in models resolve to `_id_` without changing its authority.
Unique/sparse/partial/compound distinctions cannot be lost through reuse.
Worst-case resolved names and warning sizes are admitted before mutation; a
runtime error still follows the existing completed-prefix build policy.
The wrapper emits Python `IndexCompatibilityWarning` diagnostics, preserves
driver options and performs no client-side list-then-create race. Stock PyMongo
and native APIs do not implicitly opt in. Concurrent DDL retains bounded busy
rejection; the helper does not silently retry writes. Old TinyMongo private
catalog repair and ambiguous legacy alias ordering are not added.

Local sync/async `create_index()` also rejects repeated key fields before the
driver folds its key sequence into a mapping (code 115, no implicit namespace or
index). Valid direct calls retain the ordinary driver path and direction metadata;
they do not implicitly select model normalization or resolved-name reuse. The
source-locked public advanced-index wheel regressions cover compound tuples,
single-array multikey membership, sparse missing/null behavior, partial membership
transitions, insert/update/upsert uniqueness, eager invalid options, and metadata
after restart. These are candidate API scenarios, not an assertion that BriskDB
uses TinyMongo's private SQLite JSON-expression index format.
The companion durable-index wheel scenarios cover catalog isolation, recoverable
drop/recreate, failed unique builds, typed/null/array keys, restart enforcement,
single-shard mutation rollback, and four concurrent clients within listener
admission. PyMongo's BSON key-document/cursor and direct requested-name return
shapes are retained. Cross-shard bulk atomicity is not claimed; it follows the
decision and fault acceptance in #74/#183.

The [index-suite inventory](../compat/mongo/index-suite-inventory.json) accounts
for every function in the four source-locked suites named by #174: 73 functions,
expanded by TinyMongo into 217 reference cases across its backend variants.
It maps 59 functions to public candidate scenarios, seven to native equivalents,
three to implementation-specific exclusions, and four to explicit contract
differences. This is a coverage map, **not 217 unchanged candidate passes**.
The exclusions concern private warning-plan helper types and old TinyDB catalog
injection/repair; the differences retain real PyMongo results, direct
descending/background support, and the reviewed cross-shard commit boundary.
Every entry names executable evidence and a rationale. The validator checks all
four source hashes, exact function membership, collected reference case counts,
and that the named candidate tests still exist. Missing, duplicate, unknown or
unexplained entries fail; no frozen v1 corpus or allowance was rewritten.

Installed-wheel tests exercise the portable model/durable/advanced/exact-ID
outcomes. Native entry/probe tests replace TinyMongo-specific JSON-expression
introspection; crash/reopen and cross-process tests check BriskDB's actual storage.
The independent model, index-definition, key and matcher-probe oracles remain
separate execution gates. This completes the bounded index acceptance inventory,
not the full repository corpus under #186 or the release/security gates.

To check the mapping against the pinned checkout and its isolated test interpreter:

```sh
BRISKDB_MONGO_ORACLE_SOURCE_ROOT=/path/to/locked/tinymongo \
BRISKDB_MONGO_ORACLE_PYTHON=/path/to/oracle/bin/python \
  python -m unittest discover -s python/tests -p test_index_suite_inventory.py -v
```

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
and lifecycle. Conservative equality/membership/presence/absence/string-range
candidate paths retain residual matching. Whole-bulk post-image transitions are
governed by #74/#183; raw Mongo key-document/name-array drop selectors are beyond
TinyMongo's string-selector contract. Native
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
| Read `hint` | Find/count/distinct/aggregate accept a string or BSON document as a **no-effect TinyMongo compatibility option**, not a forced-index directive. Successful raw replies include the fixed `briskdbReadWarnings` message described below. |
| Read `comment` | Any wire-valid BSON value on find/count/distinct/aggregate/getMore; ignored, not logged, echoed or retained in cursors/metrics |
| Read `readConcern` | Empty document or exactly `{level: "local"}`; no majority, snapshot or causal/cluster-time guarantees |
| Read `collation` | Exactly `{locale: "simple"}` (existing binary string comparison); no locale-specific or additional collation options |
| `ordered` | Boolean; defaults to `true`, with ordered/unordered partial-failure behavior |
| Insert `writeConcern` | Omitted/empty, or `w` equal to 0 or 1, `j: false`, and `wtimeout: 0`; no replication or stronger durability is promised |
| Drop/create `writeConcern` / `comment` | Same concern subset except `w: 0` is rejected; comment must be omitted or null (PyMongo's default). No replication or unacknowledged namespace mutation. Metadata discovery also accepts only omitted/null comments. |
| `bypassDocumentValidation` | Boolean no-op for insert/update/find-and-modify: user collection validators are not supported; BSON validation, immutable IDs, indexes and resource checks still apply |
| Find `skip` / `limit` | Nonnegative integers; zero limit means no additional limit |
| Count `query` / `skip` / `limit` | Shared BSON matcher with global skip/limit; nonnegative integers, zero limit unbounded; absent collection returns zero |
| Distinct `key` / `query` | BSON string key and optional document filter; shared identity and global encounter order, absent collection returns an empty values array |
| Find `projection` | Basic inclusion/exclusion document, dotted/nested paths, arrays, and `_id` rules; validated before missing-collection handling |
| Find `sort` | Up to 32 ordinary fields with numeric `1`/`-1` directions; global BSON order with stable natural-order ties. Empty document preserves natural order. Metadata/expression sorts are unsupported. |
| Find `batchSize` | Integer from 0 through 1000; zero opens an empty initial batch. Default 101. |
| Aggregate `pipeline` / `cursor` | Required stage array and cursor document; cursor accepts only `batchSize` from 0 through 1000 (default 101). Basic stages plus project/set/addFields/unset; absent collection returns empty after validation. |
| Find/aggregate `allowDiskUse` | Only `false`; there is no disk spill |
| Find/aggregate `let` | Empty document only; command-level expression variables remain unsupported |
| Find `tailable`, `awaitData`, `noCursorTimeout`, `allowPartialResults`, `returnKey`, `showRecordId` | Only boolean `false`, preserving existing result, expiry and all-or-error behavior |
| Find `oplogReplay` | Boolean legacy no-op; does not enable an oplog |
| `getMore` `batchSize` | Integer from 1 through 1000; default 101. Pages also end at the wire byte budget. |
| Find `singleBatch` | Boolean; `true` intentionally returns only the first batch, with cursor ID zero |

For example, existing PyMongo code can chain ordinary read options:

```python
rows = list(db.users.find({"active": True}, comment="team-report")
            .hint("active_1").sort("name", 1).limit(20))
```

The hint does **not** require that index to exist or force it to be used. This
matches TinyMongo's accepted-no-effect keyword behavior, not MongoDB's
[forced-index hint semantics](https://www.mongodb.com/docs/manual/reference/command/find/).
Automatic planning and full matching remain authoritative; a `$natural` hint
does not change ordering either. Raw successful replies containing a hint add
`briskdbReadWarnings: ["hint: accepted for TinyMongo compatibility; index selection remains automatic"]`.
Ordinary PyMongo helpers may hide this extra reply field. Subsequent getMore
replies do not repeat it. Neither index names/patterns nor comments are retained
for diagnostics. PyMongo may reject malformed/empty hint arguments before a
request reaches the server. Unknown options still fail with code 72: this is
an explicit supported subset, not TinyMongo's blanket ignoring of arbitrary
keyword arguments. Write/metadata option rules are unchanged.

Insert commands accept up to 1000 documents per wire batch, within the advertised
1-MiB message and 512-KiB document limits. Sequence decoding shares one 4-MiB
decoded-memory budget. BSON validation, generated-ID size checks, and engine
result limits are checked before any document write. Direct non-ID zero
timestamps are server-stamped; nested timestamps and IDs are preserved. Each
write commits separately, including within one shard; cancellation or a storage
failure can leave prior successes committed. No batch transaction is promised.

The optional local Python clients additionally preflight all `insert_many()`
input before the first insert. Previously, a serialization failure after the
first 1,000 records could leave that earlier driver batch committed. The helper
now assigns IDs and encodes immutable BSON once, retaining all encoded bytes in
client memory and enforcing the existing 512-KiB document limit. Real sync/async
wire tests cover late invalid/oversized input, custom encoders, mapping/RawBSON
inputs, generated/null/embedded IDs, global duplicate indices and restart.
This does not make server/storage failures atomic or modify ordinary PyMongo.
Duplicate diagnostics remain intentionally payload-free: code 11000 and input
indices are present, but TinyMongo's `keyPattern`/`keyValue` and interpolated
index/key error strings are not exposed. PyMongo supplies the caller's own `op`.

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

Natural-order reads load initial source frontiers from at most eight targeted
shards concurrently, including find/getMore and distinct/aggregate source pages.
Global ordering, pagination, owner pruning and the shared frontier byte bound
remain authoritative. Failures cancel only local peer work, then drain started
children before returning one error; a query failure cannot cancel a shared
listener shutdown token. Point reads remain direct and natural-order frontier
refills remain sequential. Sorted key-window scans now use the same eight-child
coordinator and a single shared, bounded heap; failures discard the whole window.
See the [engine read boundaries](DOCUMENT_ENGINE.md).

The native/legacy `count` path (also used by `estimated_document_count`) runs
independent targeted shard counts through the same eight-child coordinator,
then sums checked scalars before global skip/limit. Exact-ID counts remain
single-owner reads. `count_documents` still uses the driver's aggregation
pipeline; this does not replace its matching, pagination or grouping semantics.

Rust hosts retaining a `MongoServer` can inspect `mongo.metrics()` without a
network administration endpoint. The listener-local snapshot includes accepted,
admitted/rejected, active/closed/peak connections, fatal transport/accept/task
failures, 22 fixed command families, 31 fixed error codes plus an unknown-code
counter, write-error occurrences and response-size rejections. Command counters
separate started, in-flight, completed, failed, aborted and deliberately suppressed
one-way responses. Unknown command names share `Other`; namespaces, query values,
identities and diagnostic text never become labels or retained metric data.
The `cursors` group exposes registered, active, peak and closed wire cursors,
idle-pruned entries and connection/registry capacity rejections (including failed
handoffs). Retained batches and socket handoffs do not re-register a cursor.
Every registry removal counts as closed: exhaustion, explicit kill, errors,
idle expiry, owning-socket disconnect and listener shutdown. Idle expiry does
not include execution-budget exhaustion (already reported as command code 50).
These are listener wire-registry counts, not all embedded engine cursors.

```rust,ignore
use briskdb::protocol::mongo::MongoCommandKind;

let snapshot = mongo.metrics();
let finds = snapshot.command(MongoCommandKind::Find);
println!("active={} find_completed={} find_failed={}",
    snapshot.active_connections, finds.completed, finds.failed);
```

Per-command admission occurs after frame decoding and request preparation return;
malformed frames/parser failures are transport failures, not invented command
outcomes. Completion means an encoded reply or suppressed one-way outcome, not
successful delivery, successful writes, or global atomicity. Failed counts include
top-level and embedded write/write-concern errors; code counters count individual
error occurrences in final outcomes. Eight disjoint latency buckets (exported
microsecond bounds) plus cumulative/max time include completed and aborted
commands, from complete frame through reply construction; socket framing and
delivery are excluded. Live snapshots sample atomics separately; accounting
identities are meaningful after drain, not during concurrent updates. Totals
saturate, gauges are admission-bounded, close retains final counters, and a new
listener starts at zero. Metrics remain separate from the request tracing below;
this is not a Prometheus endpoint or completion of the broader #187 hardening gate.

Decoded requests also have debug-level `mongo.command` spans and one final
structured event on the `briskdb::mongo` tracing target. Hosts own their subscriber
and exporter; opening a library listener installs neither a global logger nor an
export queue. The daemon's existing `RUST_LOG` filter can include
`briskdb::mongo=debug`. A listener retains the subscriber active when it starts,
including across connection tasks and blocking reply workers.

Events contain only the process-unique frontend `connection_id`, client-supplied
numeric `wire_request_id`, connection-local `sequence`, fixed command family,
`completed`/`failed`/`aborted` outcome, first classified error code/category,
write-error count, response-suppression flag and elapsed microseconds. Repeated
wire IDs are distinguished by the sequence; none of these fields authenticates
a caller. Unknown commands/codes become `other`; code zero with category `none`
or `other` is not a Mongo error code. Namespaces, documents, query/update/filter
values, comments, credentials, paths, client metadata and error text are never
recorded. Identities are event fields, not metric labels.

The same final-outcome boundary as metrics applies: completion is not proof of
socket delivery or a globally committed write. Spans start after preparation;
the explicit elapsed field includes preparation from the complete frame. Guard
drop emits an aborted outcome after releasing its in-flight gauge, including on worker
unwind. There is at most one live request guard per admitted connection; the host
must bound its own export queues. Malformed frames that never become commands
remain transport metrics, not fabricated request events. Engine/shard phase
tracing and broader fault/soak acceptance remain separate #187 work.

Rust hosts can inspect local document readiness without a network command:

```rust,ignore
let status = mongo.readiness();
println!("ready={} listener={} security={} reason={}",
    status.ready(), status.listener.code(), status.security.code(),
    status.reason().map_or("ready", |reason| reason.code()));
// status.engine contains the neighboring live engine/schema admission snapshot.
```

The primary reason is ordered listener closing/closed/failed, documents disabled,
engine unavailable/draining/stopped, then schema migrating/pending/degraded.
All component fields remain available to inspect simultaneous conditions. The
listener state is local to that handle; closing it does not close another listener
or the borrowed engine. A failed or aborted listener stays failed after close.
Engine state is observed through a weak internal handle: reads perform no I/O,
query admission, polling, retained sessions or retries. After the last engine
owner is released, `engine` is `None`; retaining closed listener handles or
snapshots cannot keep pools or the old database identity alive.

`ready()` means local document admission, not spare connection capacity or a
guarantee that the next operation will succeed. These neighboring live fields
are not one atomic system snapshot. The schema gate reflects **detected** catalog
or shard corruption as `schema_degraded` but does not identify the failed file
or probe for new on-disk corruption; global-index health is a separate engine
surface. `ping`/discovery can succeed when document support is disabled. Security
is explicitly `anonymous_loopback`: Mongo authentication and TLS are not implemented,
local processes must be trusted, and non-loopback binding still fails. This is
not readiness for exposing Mongo to a network. No admin endpoint, Python readiness
API, automatic repair or security-policy change is added.

The raw-wire resource-churn gate repeatedly fills all 32 shared native/wire cursor
slots with find and sorted-aggregation cursors, verifies rejection of the next
cursor, fills all eight socket slots, and verifies socket overflow rejection.
Each wave exercises cursor handoff, owner disconnect, exhausted and abandoned
continuations, rejected one-way reads, duplicate unacknowledged writes, unknown
commands, malformed lengths/BSON, and truncated-frame disconnects. After every
wave, connection/cursor ownership and command gauges must drain, exact failure
counts must agree, and the next wave must reacquire full native capacity.

The normal socket suite runs eight waves across two engine lifetimes. The explicit
CI tier runs 128 waves across four lifetimes: 4,096 full-capacity cursor registrations
and 1,024 admitted wave sockets, plus setup/shutdown. Every lifetime ends with a
retained cursor/partial-frame shutdown; reopen checks unchanged records and rejects
stale cursor IDs. Retained closed host handles must not retain the previous engine.
Run the larger tier locally, without Python, using:

```bash
cargo test --locked --no-default-features --features mongo --test mongo_wire \
  resource_churn::bounded_resource_soak -- --ignored --exact --nocapture
```

This is deterministic bounded resource stress, not a long-duration soak, allocator
or RSS leak proof, throughput threshold, power-loss/disk-full drill, or replacement
for driver cancellation, storage-corruption, security and release gates. No existing
caps or timeouts are raised. The broader #185/#186/#187 acceptance remains open.

Read-work telemetry is separately opt-in for Rust hosts:

```rust,ignore
mongo.set_read_metrics_enabled(true); // before the requests to observe
// Run normal PyMongo find/getMore/aggregate/distinct commands.
let reads = mongo.metrics().reads;
println!("examined={} candidates={} scans={} shard_visits={}",
    reads.documents_examined, reads.index_candidate_plans,
    reads.scan_plans, reads.shard_visits);
mongo.set_read_metrics_enabled(false); // counters are not reset
```

The default is off: ordinary reads allocate no execution collector and request
no extra access-plan diagnostics. An enabled complete frame samples the flag
before preparation; already prepared requests retain that choice when it changes.
The `reads` snapshot aggregates successful engine find/getMore/aggregate/distinct
executions, record-read calls (including misses/lookahead/rescans), examined BSON
documents, source matcher evaluations, and returned documents/distinct values.
Sorted output refetches and source-matcher rechecks count again, independently
of the preceding key-window scan; distinct also counts its internal lookahead.
It excludes partial work from failed engine calls, legacy count, mutations and
catalog reads. A successful engine result counts even if later reply encoding or
socket delivery fails; these are not successful-client-operation counters.

Point/index-candidate/scan/unclassified plan counts describe the selected access
path, not index-only reads or full matcher elimination. Planned shard targets
are distinct from measured per-request shard visits: buffered aggregate output
can read zero shards. Eight fixed fanout buckets (inclusive exported bounds
0/1/2/4/8/16/32/64), peak fanout and 64 fixed physical-ordinal request counters
make fanout and request distribution visible without namespace/identity labels.
They do not measure matched rows, per-shard row/CPU skew, physical SQLite page or
byte I/O, or pipeline-predicate evaluations. Enabled requests use the existing
bounded engine diagnostics and account for their result metadata; protocol replies
do not gain fields. Native Python/CLI metric controls, Mongo `explain`/`serverStatus`,
exporters, engine/shard phase tracing and broader #187 acceptance remain separate work.

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
1024 keys and a conservative 64-MiB heap charge shared across all shards, not all
matching documents or a separate heap per shard. At most eight admitted shard
workers decode/derive keys concurrently, under the existing BSON, per-key (8 MiB),
and derivation-work limits. Selected documents are still refetched in global order.
Large skips may require repeated scans and pages may be short at an internal
window/memory boundary. Worker arrival order can change the length of a
memory-trimmed page, never its globally sorted prefix or stable tie-breaker.
Cursor key growth shares the existing retention quota.
Real-driver tests cover chained sorts, find_one, projected-away sort fields,
compound array keys, stable ties, byte-bounded batches, and restart.

Counts are exact over the documents observed during the operation; the
`estimated_document_count()` driver method currently uses this same count path,
not an approximate cached statistic. Exact-ID queries remain point-routed; other
queries use the shared scatter matcher, with literal-ID-list shard pruning
(empty filters use per-shard row counts).
Concurrent writes do not have a cross-shard snapshot guarantee. Count does not
open a cursor. Invalid queries/options fail before absent-collection handling.
Negative legacy count limits remain explicitly rejected. Hints, comments,
collation and read concern use the bounded read-option contract above.
PyMongo `count_documents()` sends an aggregation pipeline
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
collation and read concern use the read-option contract above, including the
aggregation commands generated by PyMongo `count_documents()`. Sessions and
other unimplemented options still fail explicitly. TinyMongo's direct aggregate
API rejects keyword options; this wire subset additionally supports real PyMongo.
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
Pipelines beginning with `$group` can now merge shard-local exact states for
integer-literal `$sum` and `$first`/`$last`/`$min`/`$max`. Group/key encounter order
and equal-extremum representations use global source positions, not worker
arrival order. All child states and final merging share memory/input/work quotas;
at most eight shard tasks run, and failure/cancellation drains all of them.
Dynamic or rounded sums, `$avg`, `$push`, `$addToSet`, and pipelines with preceding
stages retain the original ordered executor. These are optimization fallbacks,
not unsupported queries. No frozen source, expectation, or adapter was changed.
All 456 frozen command executions are an independent required gate. The complete
TinyMongo source inventory, additional expressions and release acceptance are
separate from this bounded grouping contract.

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
commit boundary on two- and four-shard roots. Local-client model compatibility
has an additional 144 source-locked public outcome comparisons, plus real sync/
async wheel tests for mixed input types, name reuse, concurrent admission/retry,
membership distinctions, uniqueness and reopen. This is not a claim that all
TinyMongo private backend/helper tests run unchanged; that full inventory and
the release gate remain tracked separately in #186 and #185.

Ready-index equality reads now have 1,087 additional source-locked probe groups:
201,349 matcher evaluations and 9,541 eligible matching candidates without false
negatives. Direct/positive-conjunctive complete scalar tuples use bound BDIK
probes, preserve natural paging and still run the full matcher. Unproven partial indexes,
sparse all-null tuples and unsupported/incomplete shapes scan. Native/real-driver
tests compare filters, sorting, count/distinct, index drop/recreation between
batches, write maintenance and reopened results. #178's bounded candidate scope
also includes the membership/existence/logical/partial/string-range proofs below,
native diagnostics, and local scan-versus-index benchmarks. Further optimizations
are not required for query correctness: unproven forms use the full matcher scan.
Broader index API compatibility remains #174; bulk boundaries are documented in
#74/#183 and full-inventory/release acceptance remains #186/#185.

Ready-index candidates also support necessary positive literal `$in` lists,
including complete compound tuples and residual predicates under `$and`.
Lists are limited to 128 scalar members, with at most 128 distinct candidate
tuples and 1 MiB of encoded keys across the selected probe. Existing complete
equality probes keep priority. Regex/array/object/unsupported members, empty or
oversized lists, unproven partial indexes and possible sparse all-null tuples are not
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
Unbounded/unsupported branches, unproven partial indexes and possible sparse all-null
tuples remain conservative. Necessary finite probes keep priority across indexes,
then logical probes, then sparse-presence scans. Native and real-driver fixtures
cover overlapping multikey matches, residuals, mutation/upsert, churn, corruption
and restart without changing the frozen public equality-probe oracle.

Ready partial indexes can now supply equality, finite-membership/absence and
logical candidate keys when the query proves the index's membership filter.
The bounded proof accepts identical-representation scalar equalities and explicit
`$exists: true` facts, composing positive AND/OR without expanding the query.
Every query alternative must prove each required fact; a partial-filter OR needs
one sufficient branch. Numeric-alias, range, type, membership-list and negation
implications remain deliberately unproven and scan. The full matcher, fallback
records, work/cancellation limits and fresh Ready authority remain mandatory.
The public single-equality helper and frozen corpus are unchanged. Native
scan/read-counter differentials and sync/async PyMongo restart fixtures verify
selection, index churn and membership-changing writes. This partial-membership
proof does not infer stronger ranges.

String ranges now add one necessary single-component index-entry filter for
`$gt`, `$gte`, `$lt` and `$lte`. Bound parameters compare UTF-8 payload bytes only,
with equality-frame/type guards and unconditional fallback-entry inclusion;
length prefixes and numeric encodings are never treated as sort keys. Independent
array predicates are not intersected. Sparse membership follows necessary string
presence; partial membership still needs its separate proof. Numeric/non-string,
compound and unproven logical ranges retain scans/other proven probes. The full
matcher remains authoritative. Native diagnostics report `string_range`, and
record counters/benchmarks distinguish avoided BSON reads from physical SQLite
work. This is not a JSON coercion, B-tree range seek or Mongo `explain` capability.

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

## Reproducible public-client performance comparison

`scripts/mongo_benchmark.py` runs the same checked synchronous workloads in isolated
interpreters against the installed BriskDB wheel and the locked TinyMongo
`sqlite-sharded` backend. A disposable MongoDB reference is optional. It covers
seed inserts, point reads/inserts/updates/deletes, equality-index creation/queries,
scans, grouped aggregation, cursor iteration, sorted scatter queries and a
concurrent insert wave. Every operation's result is checked; individual command
checks are outside their timers, while per-insert checks inside the threaded wave
are included. All trials/backends must finish with identical document counts and
content hashes.

```sh
python scripts/mongo_benchmark.py \
  --briskdb-python /absolute/path/to/candidate/bin/python \
  --tinymongo-python /absolute/path/to/locked-oracle/bin/python \
  --documents 1000 --operations 64 --trials 3 --shards 4 --workers 4 \
  --output mongo-benchmark-new.json
```

Use the isolated oracle environment described below, with source commit
`53cbf44e98b8caa036163725d195fd29592e1cc0`, and a release-built candidate wheel.
The interpreters are invoked with `-I`; candidate code never imports TinyMongo.
An explicit `--mongodb-uri mongodb://127.0.0.1:PORT` plus
`--mongodb-environment 'version/image digest, storage, CPU/memory limits'` adds a
reference using the candidate environment's stock PyMongo. Remote addresses,
credentials, URI database names and options are rejected. The worker creates and
drops only a generated `briskdb_bench_<uuid>` database carrying the exact random
ownership claim; timeout cleanup is retried by the parent and refuses an
unclaimed/preexisting namespace. Local backend roots are fresh temporary directories, never an
application database. Output files are exclusive-created, not overwritten.

Raw reports retain every elapsed-nanosecond sample, operation units, runtime
versions, candidate binary/Python hashes, the full reference Python-package hash
and host configuration. Backend order rotates between trials. Startup/import
timing is separate; up to 16 exact-ID reads warm each trial. Seed inserts and
threaded waves use submitted-document units; scans/group/iteration use
fixture-document units; the other workloads use one command per unit. Thread
start/barrier/join and result materialization are included. These are end-to-end
client costs, not equivalent storage implementations: TinyMongo is in-process;
BriskDB and MongoDB use a wire driver, and Docker-hosted MongoDB uses VM storage.
Backend transaction, bulk-commit and durability policies are not normalized.
TinyMongo iteration has no server `getMore` batching; wire clients request
64-record batches. No automatic write retries, cold-cache, durability,
sustained-load or statistical speedup claim is implied.

The installed-wheel CI tier runs a small correctness smoke, not a noisy timing
threshold. For a controlled performance run, supply `--baseline previous.json
--maximum-regression-ratio 1.5`. This gate requires at least three trials and
matching workload/configuration/host/reference-runtime metadata, recomputes
summaries from raw measurements, and fails if any median time per unit exceeds
the selected ratio. Rebaseline deliberately when the workload or host changes;
do not use another machine's sample as an acceptance threshold. The broader
release/application/security/soak requirements of #185 remain separate.

The [2026-09-26 raw example](benchmarks/mongo-public-clients-2026-09-26.json)
records 1,000 seed documents, 64 operations, three rotating trials, four shards
and four writer threads on macOS ARM64. It uses a development release-built
BriskDB wheel from source at `aca9ea8e75dfc514e16a02c4db263408d1e649d3`, not the
published alpha.7 feature set; TinyMongo is source-locked 1.3.0, and MongoDB 7.0.43
runs in Docker Desktop with its image digest and resource limits recorded.
All nine runs finish with the same 1,256 documents and content hash.

| Median command latency in this small workload | BriskDB wire | TinyMongo in-process | MongoDB Docker wire |
| --- | ---: | ---: | ---: |
| Exact-ID read | 9.007 ms | 0.060 ms | 0.993 ms |
| Exact-ID update | 9.854 ms | 0.643 ms | 0.982 ms |
| Indexed equality query (materialized) | 41.531 ms | 4.543 ms | 2.261 ms |

This snapshot exposes substantial current BriskDB overhead to profile; it does
not demonstrate performance parity, a universal ranking, or a release pass.

Following that baseline, normal wire `find` commands resolve collection existence
inside their admitted engine operation instead of issuing a separate catalog
preflight. Only a typed missing-collection result becomes an empty cursor;
arbitrary validation, corruption, cancellation and storage errors remain errors.
The engine's verified manifest snapshot and schema/shard checks are unchanged.
Zero-sized `singleBatch` requests still perform the original admission/catalog
check because they intentionally skip the engine read. Regression coverage checks
drop/recreate, zero-batch cursor continuation, disabled document support and
post-startup manifest corruption for existing and absent collections.

The [same-host follow-up report](benchmarks/mongo-public-clients-find-2026-09-26.json)
retains the identical workload, worker hash and reference configuration. Median
BriskDB point-read latency fell from 9.007 ms to 4.757 ms (47%); materialized
indexed-equality latency fell from 41.531 ms to 37.628 ms (9%). All nine trials
retain identical final records, and all 36 baseline checks pass the 1.5x bound.
These are measurements of this small fixture, not a general speedup guarantee;
write latency and the remaining engine/storage overhead are not fixed by this
read-path change.

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
