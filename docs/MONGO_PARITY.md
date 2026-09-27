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

`compat/mongo/query-suite-inventory.json` separately accounts for all 883 test
functions in the locked `test_query_more.py`,
`test_query_operator_coverage_edges.py`, `test_client_read_fidelity.py`,
`test_insert_many_semantics.py`, `test_client_configuration.py`,
`test_projection.py`, `test_projection_memory.py`, `test_unset_semantics.py`,
`test_common_api.py`, `test_collection_attributes.py`, `test_thread_safety.py`,
`test_multi_write.py`, `test_update_operator_modifiers.py`,
`test_array_update_modifiers.py`, `test_bson_value_types.py`,
`test_uuid_regex.py`, `test_aggregation_basic_stages.py`,
`test_aggregation_projection_stages.py`, `test_aggregation.py`,
`test_decimal_coverage_edges.py`, `test_bson_codec_fast_path.py` and
`test_async_api.py`, `test_bson_registry_hardening.py`, `test_patching.py`,
`test_pymongo_dropin.py`, `test_pymongo_contract.py` and
`test_talkpython_regressions.py`, `test_sharded_sqlite_operation_atomicity.py` and
`test_bson_codec.py`, `test_typed_physical_ids.py` and
`test_sqlite_optimistic_inserts.py`, `test_sharded_sqlite_concurrency.py` and
`test_sqlite_unique_update_fast_path.py`, `test_sqlite_bulk_updates.py`,
`test_warning_attribution.py`, `test_mongo_like.py` and
`test_bulk_insert_planning.py`, `test_storage_backends.py` and
`test_sqlite_complex_read_candidates.py`, `test_sqlite_read_optimizations.py` and
`test_sharded_sqlite_coverage_mutations.py`, `test_table_backend_id_and_error_edges.py`
and `test_sharded_sqlite_point_read_optimizations.py`, `test_memory_backend.py`,
`test_tinymongo_coverage_edges.py`, `test_beanie_compat.py`,
`test_sharded_sqlite_backend.py`, `test_tinymongo.py`,
`test_remaining_coverage.py`, `test_sharded_sqlite_coverage_init.py`,
`test_coverage_edges.py`, `test_acceptance_runner.py`,
`test_cli_replacement_safety.py`, `test_cli.py`, `test_compatibility_report.py`,
`test_storage_benchmark.py`, `test_sqlite_comparison_benchmark.py`,
`test_table_backends.py` suites
(1,504 reference parameter cases). 320 added wheel scenarios and existing patch
regressions check public
query/write/index results, Mongo
error codes, numeric path fanout, missing versus zero candidates, Decimal128 and
regex behavior. Hashes, exact function membership, reference collection counts
and actual candidate test symbols are validated; these are adapted scenarios,
not 1,504 unchanged upstream candidate passes or complete #186 certification.
Private bulk-planner monkeypatches, fake backend retry counts and TinyMongo's
no-PyMongo fallback errors module are excluded with explicit rationales; real
BriskDB duplicate/concurrency outcomes and single-pass client encoders are tested.
An exact source-tree gate now requires every top-level test suite in the locked
checkout to appear in the query inventory, the index inventory, or the pinned
Beanie/MongoEngine application harness: 58 + 5 + 2 suites, respectively. It checks
the ODM source hashes/function names without loading reference dependencies into
the candidate wheel. This closes top-level source accounting, not the separate
fuzz, fault-soak, external application, CI-tier or release gates.

The original Talk Python regressions additionally cover every generated-ID write
path, no-match invalid-payload rejection and ObjectId/datetime/binary/compound
sorting. These seven adapted wheel scenarios are separate from the unchanged
Talk Python contract runner below. Explicit string IDs work, but TinyMongo's
`generate_id` export and no-BSON fallback allocator are not provided. Python-only
`date` values fail BSON encoding before a batch is sent, rather than entering a
warning-based in-memory cursor sorter.

Main codec-source coverage includes nested ObjectId/date/timezone persistence,
exact Decimal128 BID values, nonfinite doubles, binary subtype/legacy-length
ordering, a 100-KB binary document, and literal JSON-tag-shaped mappings. The
native format is not TinyMongo tagged JSON; bytearray needs explicit bytes
conversion, patterns decode as BSON Regex, and driver exceptions do not adopt
TinyMongo's extra inheritance, context or bounded value representation.
For both [BSON UUID subtypes](https://bsonspec.org/spec.html) (3 and 4), PyMongo
can encode arbitrary widths but its C decoder requires 16-byte payloads in
replies. Opaque input/storage and projected queries remain supported, preserving
the frozen binary-comparison contract. Replies containing other widths, including
nested arrays/documents and JavaScript scope, return a bounded code-22
`InvalidBSON` error without echoing payloads or closing the connection. Rejected
first/continuation batches release their cursors. This is a response boundary,
not rollback: a find-and-modify operation can commit before its returned image
is rejected. Project out the opaque field when requesting such an image; do not
blindly retry a mutation after a reply error. Generic native opaque-binary storage
is unchanged; no repair or migration of existing values is performed.

Typed-ID coverage checks binary/numeric/date/NaN/container aliases, distinct
boolean/string/ObjectId/infinity values, exact stored representations and all
five original escaped-string IDs through mutations and reopen. A separate
source-locked drill imports genuine legacy `str(_id)` SQLite keys for integer,
double and string IDs, exercises `$eq`/`$or`/`$nor`, replacement and deletion with
stock PyMongo, reopens the native root, and verifies the unchanged original source.
This requires explicit stopped import; TinyMongo's private key format, fallback
registry, SQL compiler text and DuckDB/Parquet legacy ingestion are not claimed.
Forged keys or damaged checksums fail closed as corruption rather than being
relabelled as ordinary duplicate-key errors.

Empty-insert coverage includes clean/reused collections, ordered/unordered
duplicate indices and original error operations, unique/nonunique constraints,
custom mappings and zero-timestamp caller preservation. Imported legacy numeric
IDs reject equivalent duplicate inserts without changing the original row.
Private optimistic retries, SQL probe counts and externally substituted SQLite
schemas are not BriskDB extension APIs. Real native disk-full and stale-transaction
guards supply separate fault evidence, with rollback scoped to the affected shard
transaction rather than a promise of whole-batch cross-shard atomicity.

Spawned-writer coverage now holds a real external SQLite write lock after two
independent BriskDB/PyMongo processes initialize: the unblocked shard commits
first, and the other writer finishes after release. Native reopen checks exact
rows/counts. A separate process writes deliberately invalid but uncommitted BSON
and readers still see only committed WAL data, followed by rollback and healthy
reopen. No test commits the damaged bytes. PyMongo/native-runtime background
threads are intentional, unlike TinyMongo's background-thread-free client; the
owned client does not add multiprocessing-managed worker processes. These are
bounded local concurrency checks, not long-duration soak certification.

Local sync/async clients check process ownership before the pinned driver's
topology lookup for reads, writes, commands and cursor getMore, including retained
collection/cursor handles. A real-fork regression verifies prompt refusal and
unchanged parent data/cursor state; simulated owner changes also check both driver
paths before topology work. Use multiprocessing `spawn` and open a fresh client
inside each child. This does not make inherited engines fork-safe or prohibit
inspection of already-decoded Python objects.
Fresh local client construction and patch entry/exit also reject inherited Mongo
state before startup/patch locks or async startup executors. Forked children do
not reset or reuse these locks even after all parent clients close: an empty
registry does not prove that inherited Python/native state is safe. Bounded
real-fork probes hold each lock in another parent thread and verify prompt child
refusal, no new storage and an unchanged, usable parent. Use `spawn`, including
when the child intends to open a different database folder.

Sharded point-read coverage adds four concurrent readers before/after a committed
update, detached nested copies, typed/projection/date fidelity, and permanent
closed-client refusal while live peers and new clients remain usable. After 50
point reads spanning an update, an independent SQLite process truncates the
manifest and shard WALs with the client still live. Native pool tests cover
bounded admission, generation retirement, broken leases and transaction cleanup;
they do not emulate TinyMongo's private Python pool/cache/SQL retry hooks. Native
startup and fresh opens reject swapped/missing files. Reused pooled connections
now also check the current regular path and SQLite's file-identity probe before
checkout. Deleted/replaced POSIX files retire the stale lease, return redacted
code-1 failures and degrade the root; symlink/directory substitutions also reject
without leaking capacity. A VFS without that probe gets a fresh validated handle
instead of unverified reuse. This fixes the previously reproduced warmed-reader
gap, but is a checkout boundary, not continuous protection against a file swap
during an in-flight operation. Stop clients before replacing database files.

Shared-root clients also check retained misses, peer index updates and current
find-and-modify preimages, detached nested values and four-way insert/duplicate
races with durable reopen. This is native SQLite, not TinyMongo's diskless memory
registry. Direct memory backend selection rejects before acquiring storage.
Explicit patch scopes instead own real temporary SQLite roots (or configured
persistent folders); per-client host/backend arguments do not override that
scope. Nested temporary scopes remain isolated and remove only their own roots.
Private memory-registry cleanup hooks and capability mappings are not exposed.

Discovery edge scenarios check real sync/async build information, ping,
authorized/name-only and filtered collection names, explicit unsupported commands
and closed-client refusal. Build information identifies BriskDB instead of
copying TinyMongo's hard-coded MongoDB 8.0.0 payload. Malformed inputs keep the
pinned driver's errors; detached private database objects and TinyDB condition
construction hooks are not APIs. Dropping one same-field nonunique index leaves
the unique index enforced after reopen. Legacy manual cursors/`hasNext`, foreign
Parquet directory statistics and remote cleanup hooks remain explicit differences.

Broader sharded-backend scenarios use plan-proven IDs on every shard of an
eleven-shard root, concurrent readers, global sorted windows and exact restart
results. Additional tests prove cross-shard compound/sparse/partial uniqueness,
ordered/unordered duplicate indices and original error-operation identity.
Native default layout/WAL checks include paths with question marks, hashes and
Unicode. Invalid shard options reject eagerly before storage; both SQLite aliases
use the same sharded engine. Logical database drops never delete shared root
files or allow changing its shard count. TinyMongo's ATTACH pool, URI flags,
private retry/poison/PID hooks and manually overwritten legacy order columns are
not native APIs; their exclusions and actual native lifecycle evidence are mapped.

The oldest `test_tinymongo.py` suite is not a passing current reference gate:
without an explicitly owned MongoDB comparison server, three tests pass, one
fails because handle selection no longer creates a collection, and 32 skip.
Its removed cursor/collection methods and stale assertions are mapped, not
counted as passes. Current TinyMongo and BriskDB both return 100 rows for the
legacy contradictory `$not` example (not 80) and 19 matches for its first regex
(not 11). Adapted 100-row query/CRUD regressions use modern driver methods, while
the hash-checked 90-row mixed-sort fixture matches the current reference's exact
order before/after native reopen. Inventory collection strips the ambient legacy
MongoDB URI, preventing collection-time connections to a user's server.
Remaining small backend cases distinguish real BSON/duplicate results from
private Python registries and retry counts. TinyMongo's live CLI replacement
and compensating rollback are not BriskDB's stopped-source, fresh-destination
import; native stage cleanup/source preservation and empty-collection import
have separate evidence.

Startup beside surviving shard files now requires the original manifest before
global-index upgrades or writable manifest open. If that file is missing, native
and sync/async Mongo constructors return a corruption error without creating an
empty replacement or changing shard bytes. The native open also omits SQLite's
create flag for nonempty layouts. Fresh empty roots can still initialize. Tests
cover repeated default/matching/mismatched shard-count attempts and reopening an
exactly restored stopped fixture; automatic manifest reconstruction is unsupported.

Sharded-initialization source accounting distinguishes native recovery contracts:
Pending index declarations require explicit builds, unfinished builds clean their
unpublished derived entries, and admitted index drops finish removal on reopen.
They do not use TinyMongo's automatic pending activation or compensating
child-index recreation. Actual process-exit tests cover collection/database drops,
index creation, restartable cleanup, index drops and build commits. READY native
indexes validate coverage/checksums and unique ownership; no private Python
no-rescan guarantee is promised. Native directory-policy tests preserve unrelated
root-level files, reject unclaimed shard contents without a replacement manifest,
and verify dotted unique index enforcement/drop across separate restarts. Private
cache/PID/retry/physical-index hooks remain explicit implementation differences.

Remaining public edge checks verify exact three-row query results/counts, lazy
unsupported-query errors without creating a collection, no-match writes,
unchanged rows after malformed/non-numeric/immutable-ID updates, replacement
no-op counts and find-and-modify images across restart. Closed sync/async metadata,
listing, drop and retained collection calls cannot revive a client. TinyMongo
result constructors, legacy cursor/collection helpers, placeholder GridFS wrappers,
alternate storage engines, CLI internals and acceptance-runner plugins are not
BriskDB APIs. Native staged import is not live CLI compensating replacement; the
source-accounted runner fixtures are not an external application acceptance pass.

Reporting regressions distinguish every pytest outcome (including both XPASS
forms), reject duplicated or unknown locked dimensions, redact Unix/Windows/UNC
paths while preserving URLs, and keep ingestion/report output deterministic.
Missing required targets, skipped candidates, absent references and duplicate
result artifacts cannot become passing evidence. Native exact-fingerprint
comparison is not TinyMongo's percentage-scoring/report schema. Native benchmark
raw-sample, trial-rotation, result-digest and baseline validation likewise does not
copy TinyMongo's private Markdown format or per-phase process runner. Migration
preserves recognized BSON/index metadata into a fresh staged destination; it does
not implement TinyMongo's live JSON import/replace or remote-backend CLI.

Table-backend checks exercise all eight null/missing/array negation rows and five
filters before/after indexing and restart, scalar/array equality unions, broader
partial-index queries, malformed-filter errors and exact numeric uniqueness.
Out-of-Int64 Python values reject at BSON encoding; Decimal128 and supported native
multikey indexes are not subject to TinyMongo remote-SQL backend exclusions.
Empty legacy/table-native SQLite collections survive staged import and repeated
reopen without source changes. SQL compiler strings, private type-index objects,
in-place foreign token migrations and DuckDB/Parquet/remote SQL hooks remain
explicit implementation or contract differences.

Unique-index update scenarios verify no-op/miss counts on 10/100/1000-row fixtures,
scalar conflict rollback, array-order changes with unchanged multikey entries,
sparse/partial membership transfers, compound parent-path collisions, and exactly
10 selected updates among 200 records. All stored rows survive native reopen.
TinyMongo's private Python JSON decoder counters are not native Rust BSON counters;
these scenarios do not claim its exact decode thresholds or serve as a native
performance benchmark.

Bulk-update scenarios verify matched/modified/no-op/upsert results, indexed
boolean versus numeric array membership, and Decimal128 quiet-NaN execution
counts with unchanged BID bytes. Same-shard invalid updates and unique conflicts
roll back that shard; a later-shard error preserves earlier shard commits and
returns terminal `OperationFailure`, not a safe-to-continue `WriteError`.
Six simultaneously retained managed clients now complete concurrent increments
across three reopen cycles. Their shared embedded listener reserves 32 sockets
for real driver monitor/pool connections, fixing resets under the former
eight-socket capacity; standalone listener defaults remain eight.

Legacy examples retain exact insert/update/projection/duplicate-ID results
through reopen. Sync and async index-compatibility warnings point to the exact
application call/await filename and line for mappings and real `IndexModel`
objects. Descending model declarations explicitly warn about their effective
ascending equality index, while descending query sorting remains correct.
Python `date` objects fail BSON encoding before write, instead of reaching
TinyMongo's private warning-only cursor/aggregation fallbacks; valid `datetime`
sorting, cloning and aggregation are verified without those fallback warnings.

Bulk-insert planning coverage retains all 3,100 rows through reopen, including
the source's 100-existing/2,000-new unique-index workload and a late duplicate
at global batch position 1,000. Numeric/document ID aliases and unique errors
retain exact input ordering and original operations; error key values stay
redacted. Arbitrary objects and integers outside BSON int64 reject before writes.
The genuine legacy-file import drill also checks negative-zero sign preservation,
decimal aliases and distinct string/number IDs after import and reopen, while
verifying the source remains unchanged. Private Python planner/decoder counters
and SQL trace patterns are not native performance guarantees.

Storage-backend coverage verifies default/`sqlite`/`sqlite-sharded` aliases,
logical collection separation, native manifest/shard layout and common query/
mutation results through reopen. Foreign backends reject before folder creation
or storage acquisition; their file formats and private client attributes are not
emulated. A separate real `SQLiteStorage` legacy single-blob import drill preserves
ObjectId, datetime, UUID-subtype Binary and empty collections through stock-driver
writes and reopen. TinyMongo's own in-place migration runs only on a disposable
copy for reference validation; the actual source remains byte-for-byte unchanged.
This does not add DuckDB/Parquet ingestion or automatic migration on client open.

Complex reads combine indexed membership, ranges and modulo with exact natural
ordering, bounds, projection and counts across index creation/drop and reopen.
Array candidates, mixed BSON types, embedded objects, Decimal128, negative
remainders, int64 boundaries, regex OR branches and 900-value membership retain
complete results. Oversized Python integers reject at BSON encoding. Physical
Ready index/catalog damage fails closed without automatic repair or empty-result
fallback; public logical index drops remain supported. TinyMongo's private SQL
planner thresholds and decoder-count optimizations are not native benchmarks.

Point-read cases check repeated scalar/ObjectId hits, misses, projection/count/
bounds and real async reads after a sync seed. Native counters on the reopened
root show one shard and one record-read call per point hit or miss, with one or
zero examined BSON records respectively. A 250-row indexed-array case examines
exactly its one matching record with an equality-candidate plan. These counters
are not SQLite page I/O or Python decoder counts. A retained collection handle
observes another client's logical drop/recreate without stale data/index metadata.
An unverifiable legacy container-ID alias is explicitly rejected during import
without writing to its source.

Routed mutation scenarios deliberately place the natural first document on a
higher-numbered shard, then verify no-op/single/multi updates, cross-shard unique
replacement conflicts, exact-ID point plans and grouped deletes through reopen.
Public empty insert batches reject; duplicates retain the documented per-document
commit behavior rather than private prepared-batch rollback. Existing native
SQLite FULL and before/after-commit process-death gates cover actual transaction
rollback and prior-commit durability; they do not impersonate Python connection
proxies or guarantee whole-batch atomicity.

ID/error-edge cases preserve nested numeric container and current datetime
aliases, distinct nonfinite IDs and exact stored representations through reopen.
Duplicates return redacted code 11000; missing replacements create no records
or namespaces. Legacy import rejects malformed/missing/mismatched IDs and wrapped
JSON documents as corruption, and unverifiable legacy datetime spellings as
unsupported, without source writes. This is deliberately stricter than private
TinyMongo scan/skip/unwrap fallbacks. Its mocked RemoteSQL/MySQL upgrade behavior
is not a BriskDB storage adapter or a claim about the separate MySQL connector.

Common-client coverage includes sync/async dotted collection selection, private-name
brackets and typo errors, sorted/projected find-and-modify, concern-dictionary
isolation, consumed cursor positions and logical database drop/reopen. Shared
collection threads and async tasks check competing increments without retry masking.
On Linux/macOS, six spawned local writer processes insert 50 records each and the
reopened root must contain all 300 values and unique generated IDs. The native
schema is prepared before overlapping roots; this is not TinyDB's concurrent
implicit-creation policy, a cross-shard transaction or long-duration soak.
The source's private database-object cache and exact Python lock/build counts have
no native counterpart. Real PyMongo cursors retain buffered rows after close and
permit rewind, unlike TinyMongo's permanent-close behavior; boolean argument
validation also stays with the pinned driver. These differences are explicit,
and full database statistics remain the #166 gap described below.

Five additional real-wire contention scenarios verify conditional version winners,
unique atomic counter before/after images, sorted replacement/deletion preimages,
global natural/sorted selection across physical shards, and exact-ID shard
independence while an external process holds a real SQLite write lock. Completed
outcomes are checked after reopen. The lock holder is a separate spawned process
because stdlib SQLite and the wheel bundle different SQLite builds; same-process
lock bookkeeping is not a reliable fault injection boundary. These tests replace
TinyMongo private Python pause hooks with native outcomes, not identical forced
instruction interleavings or long-running soak certification.

Update-modifier coverage checks BSON-order min/max with representation-preserving
numeric equality, sparse array paths, rename movement/errors, pop validation and
integral-float directions, equality-seeded upserts and no-match preflight errors.
Rejected single-record updates leave the record unchanged, including when an
earlier modifier would have changed it. Private Python helper inputs that cannot
be stored/transmitted as BSON remain explicitly classified in the inventory.
Array-update coverage adds whole-value/field push sorting, ordered-document and
numeric/boolean equality for addToSet/pullAll, pull membership/regex/elemMatch and
logical document predicates, sparse paths and malformed modifiers. Non-string
BSON keys and Python sets fail in the driver before transmission; private helper
exception messages/classes are not silently counted as identical wire behavior.

BSON value coverage adds exact scoped/unscoped JavaScript, timestamp and bound
roundtrips, whole-value type sorting, scope-order-sensitive identity, and Code
rejection wherever commands require ordinary strings. Native atomic-clock tests
cover backward time, rollover, exhaustion and concurrent uniqueness without
changing the process-global clock. Unlike TinyMongo's per-attempt allocation,
BriskDB reserves timestamps during full-batch native preflight, so unattempted
ordered tails can leave extra gaps; timestamps are not a gapless sequence.
PyMongo decodes Python patterns as `bson.Regex`, and its validation exceptions
do not reproduce TinyMongo's enriched error context. These differences and the
private tagged-JSON codec exclusions are explicit in the inventory.

Regex coverage checks predicate versus exact BSON identity, locale-flagged stored
values, membership/all/not, flag conflicts and context-sensitive nested literals.
Malformed BSON-serializable filters retain their server codes and do not create
collections; malformed regex cstrings fail during driver encoding. PyMongo's
lazy query evaluation and lack of TinyMongo's private regex-only preflight and
Remote SQL unique-value rejection hooks are explicit. Native UUID/subtype-4
binary uniqueness and recursive numeric/boolean distinct identity are tested.

Basic aggregation source coverage checks stage validation before catalog creation,
integral numeric arguments and int64 bounds, count/projection composition,
mixed-direction sorting, pagination position, numeric-path/parallel-array error
precedence, result isolation and a 1,000-element shared-array sort. Non-BSON
arguments fail driver encoding; private Python warning/helper-call counts are
not a native API guarantee. The native sorter has its own bounded-work regression.
Projection-stage coverage adds source/computed field ordering, nested inclusion
and computed array shape, literals and binary subtype decoding, REMOVE semantics,
set/addFields aliases, unset forms, validation precedence and async composition.
Wire unsupported errors, driver key validation, tuple encoding and real PyMongo
command-cursor types are explicitly distinguished from private Python helpers.
Main aggregation coverage adds BSON-sensitive grouping, dotted array references,
lazy ifNull/size semantics, error precedence, literal isolation and cursor cleanup.
Native routing regressions cover safe leading-match pruning without reordering
pipeline semantics. Constant group keys exceed this TinyMongo version's support;
unsorted group order across shards is not promised. Real command cursors lack
TinyMongo clone/rewind methods, and fake/unsupported sessions use driver errors.
TinyMongo-specific `capabilities()`/`supports()` client introspection is not
implemented: those names remain PyMongo database selectors. That API difference
is recorded explicitly, not counted as equivalent introspection or full parity.
Decimal/codec edge coverage checks exact numeric unique-index aliases,
double-versus-decimal distinctions, extreme decimal IDs across reopen, invalid
updates, nonfinite BSON roundtrips and builtin/native subclasses. TinyMongo's
tagged JSON, Python registry/optional-dependency hooks, lazy diagnostic callbacks
and legacy arbitrary-precision integer IDs are not native BSON interfaces.
Async API coverage verifies lazy independent find requests with command monitoring,
event-loop progress during an actual pending insert, modern mutation/index/bulk
operations and cancellation-drained native release. Legacy cursor/collection
helpers, database context managers, permanent-close semantics and metadata returns
differ from modern PyMongo. Full database statistics remain the #166 gap; no
TinyMongo private callback/cache or successful in-flight-call shutdown guarantee
is claimed.

Type-registry coverage checks BSON subtype/numeric/recursive identity, UUID and
regex ordering, stable explicitly ordered sort ties, signed UTC milliseconds,
tag-lookalike documents and sync/async persistence. The Mongo client addon needs
PyMongo; its encoder rejects `bytearray` (convert to `bytes`). Native values do
not use TinyMongo's optional Python registry or implicitly decode its JSON tags.
Patch-source accounting reuses constructor restoration, nesting, thread/task
exclusion, shared data and awaited cleanup tests. Explicit SQLite scopes without
a folder stay temporary, unlike TinyMongo's default persistent path; async
clients require `async with`. Unsupported older PyMongo versions reject before
engine acquisition, without changing constructors or retaining patch state.
Drop-in application coverage exercises both the BriskDB import alias and actual
PyMongo constructors inside a patch scope, including index/query/update/upsert
results and native folder/environment selection. `BRISKDB_HOME` and native
SQLite files do not emulate TinyMongo's environment/JSON layout or backend plugins.
Invalid sync/async find sessions now reject before PyMongo constructs a partial
cursor; positional/keyword and falsey invalid objects are checked. `None` and
genuine session objects retain driver handling. Other fake-session operations
retain driver-specific errors, and this does not enable server sessions.

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
The daemon's Mongo listener defaults to plaintext and unauthenticated: keep it
local, including when PostgreSQL uses TLS/SCRAM. Optional Mongo TLS configuration
below encrypts connections but adds no user authentication. Do not publicly proxy
either anonymous listener.

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

### Encrypted Rust Mongo listener

Unreleased source builds can select `mongo-tls` (which selects `mongo` and the
shared `transport-tls` loader, without PostgreSQL). The standalone Rust API
accepts PEM certificate/key files and uses direct TLS, not PostgreSQL negotiation
or ALPN:

```rust,ignore
use briskdb::protocol::mongo::{MongoServer, MongoTlsConfig};

let mut mongo = MongoServer::start_tls(
    &database, // Running BriskDb opened with DocumentSupport::Enabled.
    "127.0.0.1:27017".parse()?,
    MongoTlsConfig::new("./server.crt", "./server.key"),
).await?;
// Keep the runtime and listener alive while clients connect.
mongo.close().await?;
```

Use a certificate valid for the client's hostname, and trust its issuing CA.
For example, with a certificate whose SAN includes `localhost`:

```python
from pymongo import MongoClient

with MongoClient("mongodb://localhost:27017/?directConnection=true",
                 tls=True, tlsCAFile="./ca.crt") as client:
    print(client.demo.users.find_one({"_id": 123}))
```

`AsyncMongoClient` accepts the same TLS options. Do not disable certificate or
hostname verification. TLS authenticates the **server**, not Mongo users: local
processes must still be trusted, all non-loopback binds are rejected before
reading identity files, and public proxying remains unsafe. Unreleased Python
`db.serve(mongo=..., mongo_tls_cert=..., mongo_tls_key=...)` exposes this encrypted
transport in sync/async source builds; both paths are required together and
stock PyMongo must verify the server certificate/hostname. See the
[Python example](../python/README.md#encrypt-the-mongo-listener-unreleased).
Managed PyMongo patch hosts still use their existing plaintext loopback transport;
the daemon can opt into TLS as described below. Legacy Rust
`AttachedServer::*_with_mongo` constructors also remain plaintext; opt into
attached TLS with the options API below.
Explicitly activated Rust security roots can instead select the authenticated
mode below. TLS alone never enables it.

Certificate/key files use the shared bounded, opened-descriptor-validated loader.
On Unix, private keys must not be group-writable or accessible by others; `0600`
is suitable. A listener rejects invalid/mismatched identities before binding.
`with_handshake_timeout(Duration)` narrows the default 15-second deadline to a
positive value no greater than 15 seconds. `start_tls_with_limits` also accepts
the ordinary connection/command limits: incomplete TLS handshakes consume the
same finite socket slots, allocate no command session/client metadata, and are
cancelled during shutdown. TLS failures use existing transport failure counters.
For an ordinary root, readiness reports `anonymous_tls_loopback`.

### Authenticated Rust Mongo (unreleased)

The standalone Rust host supports `mongo-tls,auth-scram` with an explicitly
activated security root. This is **not** enabled in the published alpha.7 wheel,
Python `serve`/`patch`, daemon or composed `AttachedServer` paths. Those retain
their existing anonymous loopback boundaries. Secure roots currently support
document and scoped user commands, not SQL or shared PostgreSQL/HTTP/SQLite-remote authentication.

First, an offline trusted Rust host must initialize/close the ordinary root,
construct a `SecurityCatalog` containing SCRAM-SHA-256 verifiers and explicit
flat role policies, then call `Engine::provision_security(root, shards, catalog)`.
On Unix the root must be owner-only (`0700`); at least one user is required.
This is **one-way activation**: ordinary openers stop working. Close every root
handle/process first, back up the entire consistent root including its credential
store, and never delete/adopt an orphan store after an interrupted activation.
See [the provisioning contract](ARCHITECTURE.md#authenticated-document-engine-unreleased-opt-in-rust-api).
There is no implicit admin or public provisioning endpoint. Initial roles and an
operator account must be provisioned by the trusted host; authorized users can
then use the bounded wire commands below.

Open that already-provisioned root and explicitly start TLS:

```rust,ignore
use briskdb::{BriskDb, DocumentSupport};
use briskdb::protocol::mongo::{MongoServer, MongoTlsConfig};

let database = BriskDb::builder("./secured-data")
    .with_shard_count(4)
    .with_document_support(DocumentSupport::Enabled)
    .with_authenticated_root()
    .open().await?;
let mut mongo = MongoServer::start_tls(
    &database, "0.0.0.0:27017".parse()?,
    MongoTlsConfig::new("./server.crt", "./server.key"),
).await?;
// Keep the runtime alive while serving; close before dropping the database.
mongo.close().await?;
database.close().await?;
```

Given a provisioned `admin`-realm user named `app_reader` with `ConnectDatabase`
on `app` and `ReadData` on its `items` collection:

```python
import os
from pymongo import MongoClient

with MongoClient("mongodb://db.example.com:27017/?directConnection=true",
                 username="app_reader", password=os.environ["BRISKDB_PASSWORD"],
                 authSource="admin", authMechanism="SCRAM-SHA-256",
                 tls=True, tlsCAFile="./ca.crt") as client:
    print(list(client.app.items.find({}).batch_size(100)))
```

`AsyncMongoClient` accepts the same credentials/TLS options. Keep certificate and
hostname verification enabled. Authentication proves identity, not blanket access:
document/schema/metadata actions require explicit current privileges. Adapter
commands with collection-existence probes also require `ListObjects`; implicit
creation needs `CreateObject` and `CreateDatabase`, upserts need `InsertData`,
and returning mutations need `ReadData`. Domain-wide database-name discovery is
not yet filtered to the user's grants. Automatic built-in Mongo role resolution
is not implemented.
Trusted hosts can use `Scope::non_system_document_collections(db)` when preparing
flat data-role policies: it excludes `system.*` and the reserved `local.replset.*`
namespace, with explicit exact grants for exceptions such as `system.js`.
Database-connect/list privileges remain separate. This scope is groundwork for
built-in roles, not an implicit `read`/`readWrite` implementation; existing custom
database-wide grants keep their behavior. See the [security-catalog encoding boundary](ARCHITECTURE.md#named-security-catalog-and-principal-admission-unreleased)
before using a new scope with older authenticated hosts.
Permission checks also precede empty/missing-collection shortcuts and implicit
namespace creation; the eventual engine operation refreshes authority again.

#### Explicit read/readWrite profiles (unreleased)

The trusted Rust host can atomically install BriskDB's supported subset of Mongo's
database-local [data roles](https://www.mongodb.com/docs/manual/reference/built-in-roles/):

```rust,ignore
database.engine().update_security_catalog(|catalog| {
    catalog.provision_mongo_data_roles("app")
}).await?;
```

The same method works on an offline `SecurityCatalog` before provisioning a root.
It creates exactly `app.read` and `app.readWrite`, with no accounts or memberships.
An existing name (even with identical permissions), invalid database or insufficient
role capacity rejects the whole pair without edits. Profiles count toward the
1,024-role cap, remain explicitly stored flat roles, and trusted hosts may replace
or drop them. They are not automatically created by startup, `createUser` or a
grant command. Existing custom roles are never overwritten or broadened.

`read` permits collection listing, reads and index inspection in exactly `app`.
`readWrite` adds CRUD, collection creation/drop and index creation/drop. Both
cover non-system collections plus exactly `system.js`; other `system.*` names
and `local.replset.*` are denied. BriskDB's exact database-connect permission is
included in both; exact database-creation authority is included in `readWrite`
because creating a collection can create its logical database. Neither grants
database deletion, global database-name listing, SQL access or user/role/server
administration. Unsupported Mongo operations remain unsupported, including
server-side JavaScript execution despite the `system.js` data exception.

After host provisioning, an account administrator with `CreateUser` in `accounts`
and `GrantRole` in `app` can assign the ordinary role descriptor over TLS:

```python
operator.accounts.command("createUser", "app_writer", pwd=password,
                          roles=[{"role": "readWrite", "db": "app"}])
```

Use `authSource="accounts"` for that user's connection. Existing authorized
`grantRolesToUser`/`revokeRolesFromUser` also work with these names and apply to
already-open pools/cursors on the next permission admission. Other built-in
roles, inheritance and automatic built-in-name protection are not claimed.
`MongoDataRole::Read.policy("app")` / `ReadWrite.policy("app")` also construct
the same explicit policy without mutating a catalog.

Security contract:

- Secure roots reject plaintext even on loopback; only TLS plus enabled authentication
  permits an explicit non-loopback bind. Readiness reports `authenticated_tls`;
  it does not certify network deployment or a healthy credential store.
- Monitoring hello/ping/build-info remains available without login. Hello advertises
  SHA-256 without disclosing account existence. Speculative authentication is ignored
  with normal-SASL fallback; SHA-1, channel binding and same-socket reauthentication
  are unsupported. Modern `skipEmptyExchange` and the older final empty step work.
- Each socket retains at most one conversation, with a ten-second absolute deadline
  and 4-KiB SASL payload bound. Nonces are fresh, proofs bind the exact transcript,
  and authentication failures return a fixed redacted error then close the socket.
  A shared per-listener admission bucket allows a burst of 64 starts, refilling at
  32/second, alongside existing connection/parser/engine-worker limits. There is
  no per-user lockout or unbounded username map.
- Unknown users receive synthetic, stable-per-listener/name salted challenges and the same
  final failure shape. This is not a claim of indistinguishable timing or costs
  when real users have different configured iteration counts.
- Socket metadata/cleanup uses a stable connection ID even when successful login
  installs a new core session. Pooled cursors require the same engine/catalog/user/
  credential generation. Another user cannot take over or remove a cursor. Current
  roles and credential generations are refreshed at each engine admission; rotation,
  removal and revocation affect subsequent work, not already-admitted operations.
- Trusted hosts administer the catalog through `update_security_catalog`; the
  scoped user commands below use separate authorized engine paths. Broader
  user/role administration, automatic built-in roles, durable audit retention, full fault/soak
  acceptance and Python/daemon/composed-host configuration remain separate work.

Local gates include real PyMongo 4.17.0 sync/async SCRAM with verified TLS and zlib,
escaped usernames, least-privilege denial, same-user pooled continuation, cross-user
read/kill denial, legacy empty exchanges, live credential/role revocation, plus
bounded parser/nonce/replay/expiry/admission and core ownership regression tests.

#### Scoped user commands (unreleased)

Authenticated standalone Mongo supports `createUser`, `dropUser`, password-only
`updateUser`, `grantRolesToUser` and `revokeRolesFromUser`. Roles must already
exist in the host-provisioned catalog; names such as `read`/`readWrite` are **not**
automatically built-in roles. Here `client` is a verified TLS connection logged
in as an operator granted `CreateUser`/`DropUser`/`RotateCredentials` on `accounts`
and `GrantRole`/`RevokeRole` on `app`, where the host has created `app_reader`:

```python
client.accounts.command("createUser", "reader", pwd=os.environ["NEW_PASSWORD"],
                        roles=[{"role": "app_reader", "db": "app"}])
client.accounts.command("updateUser", "reader", pwd=os.environ["ROTATED_PASSWORD"])
client.accounts.command("revokeRolesFromUser", "reader",
                        roles=[{"role": "app_reader", "db": "app"}])
client.accounts.command("grantRolesToUser", "reader",
                        roles=[{"role": "app_reader", "db": "app"}])
client.accounts.command("dropUser", "reader")
```

Creation requires `roles` (an empty array is valid); string role names refer to
the command database. Grant/revoke arrays must be nonempty, with at most 64
references. Account commands cannot target `local`. Passwords are bounded to
1–1024 UTF-8 bytes and SASLprep-validated, then hashed with a random salt and
600,000 iterations. Optional `mechanisms` must be `["SCRAM-SHA-256"]` and
`digestPassword` must be `true`; write concern must be omitted, `{}` or `{w: 1}`.
Unknown/duplicate fields, unacknowledged writes, SHA-1, pre-digested passwords,
custom data, authentication restrictions, comments, role-array replacement in
`updateUser` and role-definition commands are unsupported. Catalog
existence/conflict errors currently use BriskDB's generic wire error mapping,
not every MongoDB administration-specific error code.

Privileges are derived by the engine, checked before password work or catalog
existence checks, and tied to the same revision as the write. A mixed allowed/
forbidden grant never partly applies. There is no implicit own-password right
or protection against an authorized operator dropping its last administrator;
retain trusted-host recovery access. Password rotation/drop invalidates existing
authenticated sockets; reconnect with the new credentials. Role changes apply
to subsequent operations, including on pooled sockets. Timeouts/disconnects
after blocking work starts can have an uncertain commit outcome: do not blindly
retry. Local tests cover these boundaries with real PyMongo and engine-level
concurrent-revocation, restart, cancellation and redaction checks.

`usersInfo` returns credential-free account metadata with the same current
authority checks. A current authenticated user can inspect itself; inspecting
another name (including a missing one) needs `ViewUsers` on that account's realm.
Listing an entire realm always needs that grant, even when it is empty or
contains only the caller. All selected realms are authorized before any lookup:

```python
# A logged-in accounts/reader can inspect its own assigned roles.
print(client.accounts.command("usersInfo", "reader")["users"])
# An operator with ViewUsers on accounts can list that realm.
print(operator.accounts.command("usersInfo", 1)["users"])
```

Selectors may be a string, `{user: "reader", db: "accounts"}`, an array of up to
64 string/object references, or integer `1` for the current realm. Duplicates
are removed; results use realm/name order and omit authorized missing names.
An empty array returns no accounts but still requires a current login. The
returned fields are `_id`, `user`, `db`, `roles` and `mechanisms`; no internal
account ID/`userId` UUID, credential generation, salt, hash, proof or password is
returned. Self-inspection fails after credential rotation or account removal,
and role updates appear on subsequent requests.

`showCredentials`, `showPrivileges` and `showAuthenticationRestrictions` may be
omitted or `false` only. `showCustomData` accepts a boolean, but no custom data is
stored; `filter` may be omitted or empty only. Credential export is deliberately
unavailable even to an administrator. All-realms selection, expanded privileges,
nonempty filters, comments and unknown/duplicate fields are rejected. Shared
engine/session ownership, cancellation/deadlines and conservative metadata
row/byte limits apply, followed by the normal wire response-size limit. This
subset is tested with synchronous and asynchronous PyMongo; it is not the full
MongoDB account-inspection surface.

An already-encrypted, running standalone listener can explicitly reload its
certificate, private key and handshake budget together, without rebinding:

```rust,ignore
mongo.reload_tls(MongoTlsConfig::new("./next.crt", "./next.key")).await?;
// Optional cancellation/deadline uses the ordinary host request controls.
mongo.reload_tls_with_context(
    MongoTlsConfig::new("./next.crt", "./next.key"),
    briskdb::RequestContext::new().with_timeout(std::time::Duration::from_secs(5))?,
).await?;
```

Prepare complete files before invoking reload; this is not a filesystem watcher
or an atomic multi-file deployment mechanism. Validation/file I/O runs on a
blocking worker, and invalid input or cancellation before publication preserves
the previous identity. Cancellation/deadline is checked before loading, during
preparation and immediately before publication (`Interrupted` / `TimedOut` I/O
errors). A cancelled worker may finish loading but cannot publish. Concurrent
successful reloads publish in completion order. Query result limits do not apply.
Closing/stopped/failed listeners and stopped engines reject reload; plaintext
listeners cannot be upgraded implicitly.

Every socket freezes one complete identity **at admission**, before the TLS
handshake task is polled. New admissions use the replacement, while established
sockets and pending handshakes retain their old certificate and timeout. This is
not immediate revocation; arrange connection draining separately if required.
Cancellation after publication cannot undo the replacement. No Mongo credentials,
roles, bind permissions or other listener configuration changes with this API.

Run the real-driver gate explicitly with pinned PyMongo 4.17.0 installed:

```sh
BRISKDB_MONGO_WIRE_PYTHON=python3 cargo test --locked --no-default-features \
  --features mongo-tls --test mongo_wire \
  tls::real_pymongo_sync_async_tls_validation_and_crud -- --ignored --exact
BRISKDB_MONGO_WIRE_PYTHON=python3 cargo test --locked --no-default-features \
  --features mongo-tls --test mongo_wire \
  tls::reload::real_pymongo_uses_rotated_certificate_with_full_validation -- --ignored --exact
```

### Encrypted daemon Mongo (unreleased)

Build the daemon with `mongo-tls` and pass both paths with an enabled Mongo
listener. Neither TLS nor Mongo is implicitly enabled:

```sh
cargo run --locked --features mongo-tls --bin briskdb -- \
  --data-dir ./briskdb-data --shards 4 --mongo-listen 127.0.0.1:27017 \
  --mongo-tls-cert ./server.crt --mongo-tls-key ./server.key
```

The environment equivalents are `BRISKDB_MONGO_TLS_CERT` and
`BRISKDB_MONGO_TLS_KEY`; explicit CLI values take precedence. Both paths must be
set together, and `--mongo-listen disabled` with TLS paths fails instead of
ignoring them. Builds without `mongo-tls` reject a complete TLS request instead
of silently serving plaintext. The existing default and `mongo`-only builds do
not gain TLS or a listener automatically.

Use stock PyMongo with `mongodb://localhost:27017/?directConnection=true`,
`tls=True` and `tlsCAFile="./ca.crt"`, trusting the issuer and checking a hostname
in the server certificate. Do not disable certificate/hostname validation.
The shared loader validates bounded certificate/key files before database creation
or socket binding; Unix keys must not be group-writable or accessible to others.
Invalid material fails startup. Non-loopback and fixed collision checks precede
file reads. TLS/SCRAM preparation runs off the async runtime. All listeners bind
before serving; failed binding cleans up the process-owned database and sockets.
SIGINT/SIGTERM also drains encrypted connections and incomplete TLS handshakes.
The readiness log reports `mongo_secure=true` for this encrypted but anonymous
transport, **not authentication or permission to expose it publicly**.

Rust process hosts can call
`server::run_with_mongo_tls(config, engine_options, address, tls_config).await`
with `server,mongo-tls`. Existing `Config` literals and legacy entry points are
unchanged. Unix daemons can explicitly opt into `--reload-on-sighup`
(`BRISKDB_RELOAD_ON_SIGHUP=true`), then replace the startup files and send SIGHUP
without rebinding. All configured Mongo/PostgreSQL and optional
[HTTP/admin identities](HTTP_LISTENERS.md#encrypt-daemon-http-planes-unreleased)
validate before any replacement; invalid preparation preserves every active
identity. Admitted sockets keep their
old identity. Only one preparation worker runs; a 15-second publication deadline
and shutdown/lifecycle guards prevent late publication. This is neither an atomic
multi-file deployment nor a cross-connector publication transaction. See the
[daemon reload contract](POSTGRES_LISTENER.md#daemon-security-reload-unreleased),
including fixed outcome logs, signal coalescing and timed-out worker handling.
TLS remains anonymous/loopback-only and managed-patch behavior is unchanged.

The real-process gate covers verified sync/async PyMongo, zlib, CRUD, indexes,
cursors, rejection/recovery, SIGTERM, persisted collections on restart and
SIGHUP rotation with live sessions and invalid Mongo/PostgreSQL replacements:

```sh
BRISKDB_MONGO_WIRE_PYTHON=python3 cargo test --locked --no-default-features \
  --features server-cli,mongo-tls --bin briskdb --test mongo_server -- --include-ignored
```

### Composed Rust listener startup

With `listeners,mongo-tls`, the unreleased `AttachedServer::start_with_options`
API attaches encrypted Mongo to the same lifecycle as the other connectors:

```rust,ignore
use briskdb::server::{AttachedServer, AttachedServerOptions, ListenerConfig};
use briskdb::protocol::mongo::MongoTlsConfig;

let options = AttachedServerOptions::new().with_mongo_tls(
    "127.0.0.1:27017".parse()?,
    MongoTlsConfig::new("./server.crt", "./server.key"),
);
let mut server = AttachedServer::start_with_options(&database, ListenerConfig {
    http_listen: "127.0.0.1:8080".parse()?,
    admin_listen: None,
    postgres_listen: None,
}, options).await?;
println!("Mongo: {:?}", server.addresses().mongo());
server.close().await?; // Joins all listeners; borrowed database stays open.
```

Add `.with_postgres_security(SecurityConfig)` for PostgreSQL TLS/SCRAM (and
enable its address in `ListenerConfig`), or `.with_sqlite_remote(Config)` to
replace ordinary SQL HTTP with the authenticated read-only SQLite-remote router.
They can be combined; each connector retains its own credentials and trust
boundary. This composed Mongo path still has **no user authentication** and remains loopback-only.
HTTP/admin remain loopback-only too; SQLite-remote network use still requires a
separately secured HTTPS proxy. SQL tables and BSON collections remain distinct.

Addresses, collisions and document enablement are checked before reading secrets.
All TLS/SCRAM preparation runs off the async runtime, and every configured socket
is bound before serving. Invalid security or bind failure releases sockets without
closing the caller's database. Closing/dropping the server also drains pending TLS
handshakes. `with_mongo(address)` preserves an already-selected Mongo TLS identity
when changing its address. Existing `ListenerConfig` literals and legacy start
methods remain compatible; defaults do not enable Mongo or TLS implicitly.
An encrypted composed handle exposes `reload_mongo_tls(config).await` and
`reload_mongo_tls_with_context(config, request_context).await`. Both validate the
replacement off-runtime and publish only after rechecking cancellation, deadlines
and listener/engine lifecycle. The reload target retains only a weak engine probe;
keeping a closed server does not keep database pools alive. Existing sockets keep
their original identity/budget, as with standalone reload. Plaintext listeners
cannot be upgraded this way. Concurrent successful Rust reloads publish in
completion order; a published replacement cannot be undone by cancellation.
PostgreSQL and Mongo identities remain independent. Python sync/async wrappers
expose the same operation with queued controls and serialized close/reload; see
[Mongo certificate rotation](../python/README.md#reload-mongo-tls-unreleased).

The real composed gate requires PyMongo 4.17.0 and `psycopg[binary]` 3.2.13:

```sh
BRISKDB_MONGO_WIRE_PYTHON=python3 cargo test --locked --no-default-features \
  --features listeners,mongo-tls,tokio/rt-multi-thread --lib \
  server::options::tests::real_clients_compose_mongo_tls_postgres_scram_and_sqlite_remote \
  -- --ignored --exact
```

### Bounded driver metadata

Rust hosts can inspect `MongoServer::client_metadata()` for at most 32 active
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
for every function in the four source-locked suites named by #174 plus
`test_index_helpers.py`: 107 functions, expanded by TinyMongo into 285 reference
cases across its backend variants. It maps 75 functions to public candidate
scenarios, nine to native equivalents, seven to implementation-specific
exclusions, and 16 to explicit contract differences. This is a coverage map,
**not 285 unchanged candidate passes**. The exclusions concern private helper
types and old TinyDB catalog injection/repair; the differences retain real
PyMongo results, direct descending/background support, BSON input rules and
the reviewed cross-shard commit boundary.
Every entry names executable evidence and a rationale. The validator checks all
five source hashes, exact function membership, collected reference case counts,
and that the named candidate tests still exist. Missing, duplicate, unknown or
unexplained entries fail; no frozen v1 corpus or allowance was rewritten.

Four additional helper-derived wheel scenarios verify Unicode/canonical names,
eager invalid definitions, nested missing/null uniqueness, numeric/bool/string
distinctions, UUID subtype/regex identity, multiple constraints and v1/v2 metadata
protocol inputs through reopen. Bare key pairs are not public PyMongo
sequence-of-pairs inputs; tuples encode as BSON arrays, and redacted duplicate
errors do not echo private index names. Supported metadata protocol inputs may
omit sparse/partial defaults even for v2; this is not `IndexSpec.from_metadata`.
Direct calls retain the existing reduced hashed/text/TTL behavior described
above. Raw command replies contain reduction warnings; stock direct PyMongo
does not turn those extra fields into Python warnings. The local `create_indexes`
wrapper does. No TTL expiration or text search is implied by accepted options.

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

The complete query/source-tree inventory uses the same variables with
`-p test_query_suite_inventory.py`. Its isolated **reference** interpreter also
needs `duckdb==1.4.4` to collect the locked table-backend module (this version
[retains Python 3.9 support](https://pypi.org/project/duckdb/1.4.4/)). This is a
test-only dependency: do not add DuckDB or TinyMongo to the BriskDB
candidate/runtime environment.

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

Rust hosts can narrow connection admission and per-command execution time for
one listener without relaxing any existing ceiling:

```rust,ignore
use briskdb::protocol::mongo::{MongoResourceLimits, MongoServer};
use std::time::Duration;

let limits = MongoResourceLimits::new(4, Duration::from_secs(3))?
    .with_cursor_limits(12, 3)?; // listener total, per connection
let mut mongo = MongoServer::start_with_limits(
    &database, "127.0.0.1:0".parse()?, limits,
).await?;
assert_eq!(mongo.resource_limits(), limits);
```

The immutable policy accepts 1–32 connections and a positive timeout up to 15
seconds. Retained cursor limits may be narrowed to 1–32 per listener and 1–8 per
connection (the latter cannot exceed the total). `start` and daemon listeners
retain the existing eight-connection default. The wheel's managed shared-root
listener explicitly selects 32 slots for independent PyMongo monitor/pool
connections. Both retain 15-second deadlines, 32 total cursors and 8 cursors per
connection. The 32-connection ceiling remains finite; frame/document/decoded
budgets do not change. Tests fill the expanded limit twice, reject overflow,
and reclaim all slots and redacted metadata without affecting another listener.
Overflow sockets are rejected immediately, not queued. The command deadline
starts when a complete frame is received, charges blocking-parser queue/decode
time, and is never restarted between preparation and engine admission/execution.
Client `maxTimeMS` can only narrow it; zero does not disable the host deadline.
Each getMore is bounded anew by the host timeout and any remaining client cursor
budget. Discovery/handshake, reply construction/delivery and socket idle/frame I/O
retain their independent bounds. Expiry reports code 50 through existing metrics;
the policy is listener-local, not a new authenticated per-user quota. Engine,
BSON, cursor and result limits remain independently authoritative. Per-user
governance awaits the shared authentication/authorization work.

Cursor registration and pooled-socket handoff both enforce the configured
quotas. A rejected new cursor releases its native session; a rejected handoff
leaves the existing cursor with its prior owner. Explicit kill, exhaustion,
disconnect and listener shutdown reclaim quota through the existing registry.
The code-10334 rejection and cursor metrics remain unchanged. These are retained
wire-cursor counts, not per-user quotas or a replacement for engine-wide limits.

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
failures, 32 fixed command families, 32 fixed error codes plus an unknown-code
counter, write-error occurrences and response-size rejections. Command counters
separate started, in-flight, completed, failed, aborted and deliberately suppressed
one-way responses. Unknown command names share `Other`; namespaces, query values,
identities and diagnostic text never become labels or retained metric data.
The fixed families include SASL start/continue, legacy authentication/logout
(including their rejected outcomes), five user-management commands and `usersInfo`.
Code 18 (`AuthenticationFailed`) has its own fixed error counter; command counts
do not represent distinct users or successful logins, since a SASL exchange can
span multiple commands.
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
Commands with no subscriber skip trace construction/emission entirely while
retaining normal metrics. This also prevents an unsubscribed listener's first
request from initializing the locked tracing dependency's shared callsite cache
in a way that suppresses another host's subscribed events. A fresh-process
regression covers this startup order, worker-thread completion and abort cleanup;
it neither installs a global subscriber nor changes host filter policy.

Events contain only the process-unique frontend `connection_id`, client-supplied
numeric `wire_request_id`, connection-local `sequence`, fixed command family,
`completed`/`failed`/`aborted` outcome, first classified error code/category,
write-error count, response-suppression flag and elapsed microseconds, plus the
established session's authentication context below. Repeated
wire IDs are distinguished by the sequence; none of these fields authenticates
a caller. Unknown commands/codes become `other`; code zero with category `none`
or `other` is not a Mongo error code. Namespaces, documents, query/update/filter
values, comments, account/realm names, credentials, paths, client metadata and
error text are never recorded. Identity correlation is in event fields, not metric labels.

Authenticated standalone listeners add three bounded fields to spans and final
events: `authentication` (`anonymous`, `unauthenticated`, or `authenticated`),
`audit_user` (empty or a 64-character opaque hexadecimal label), and
`credential_generation` (zero without an established principal). Ordinary roots
report `anonymous`; secure sockets start `unauthenticated`. Only a successful
final SASL acknowledgement installs an authenticated identity; failed logins never
record the attempted username or a user label. SASL success is observable as a
completed `saslContinue` with `authentication=authenticated`.

The label is a keyed hash of the immutable catalog account ID, using a fresh
random, zeroizing key per listener and no per-user map. The same account has
the same label across its pooled sockets and password rotations; rotation changes
`credential_generation`. Deletion/recreation gets a new account ID and label,
and separate/restarted listeners use different labels. These fields describe the
socket's established login, **not** a fresh authorization decision: a revoked
socket keeps its original context on subsequent denied requests. Names/credentials
never become labels, and audit metadata never grants access or replaces the
engine's current-privilege checks. Native and real PyMongo TLS tests cover pooled
correlation, rotation, stale denials, user commands, failed login, aborted outcomes,
redaction and listener separation.

This is opt-in diagnostic tracing through the host's subscriber, not a durable,
tamper-evident or complete compliance audit log. It deliberately cannot resolve
labels back to usernames, correlate identities across restarts, or retain an
attempted login name. Malformed frames rejected before command admission have
no request event; decoded command-validation errors do. Delivery failures and
uncertain commits retain the existing boundaries below. Hosts remain
responsible for access control, bounded export queues, retention and monitoring.

The same final-outcome boundary as metrics applies: completion is not proof of
socket delivery or a globally committed write. Spans start after preparation;
the explicit elapsed field includes preparation from the complete frame. Guard
drop emits an aborted outcome after releasing its in-flight gauge, including on worker
unwind. There is at most one live request guard per admitted connection; the host
must bound its own export queues. Malformed frames that never become commands
remain transport metrics, not fabricated request events. Engine/shard phase
tracing and broader fault/soak acceptance remain separate #187 work.

The native document storage fault tier also exercises real `SQLITE_FULL` failures
using a connection-local `max_page_count` on owned temporary shards. Four cases
cover insert and replacement failing either during BSON-record allocation or
later secondary-index allocation. An earlier record/index mutation in the same
transaction must roll back too; exact record and index bytes/checksums must match
their pre-transaction snapshots. The unique-writer fence remains held through
cleanup, then is reusable. Restoring the owned connection's page budget permits
the formerly oversized write with a fresh natural-order reservation, and reopen
verifies data plus cross-shard uniqueness enforcement.
This bounded drill does not exhaust the host filesystem or simulate WAL/fsync
I/O failure, process power loss, the Mongo wire error envelope or long-duration
soak. Run it locally with:

```bash
cargo test --locked --all-features --lib \
  storage::document::enabled::fault_tests
```

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
is explicitly `anonymous_loopback` for plaintext, `anonymous_tls_loopback` for
ordinary-root TLS, or `authenticated_tls` for the opt-in secured standalone Rust
listener. Anonymous modes still require trusted local processes and reject
non-loopback binding. These observations alone do not certify network deployment
or credential-store health. No admin endpoint, Python readiness
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

An additional opt-in timed tier repeats those capacity/failure waves while a
retained cursor is live. Every wave inserts and reads a temporary BSON record,
deletes it and verifies absence, then increments and reads a durable revision.
The working set stays at twelve durable records (thirteen during an insert).
At most 32 waves run per engine lifetime; exact BSON content and the revision
must survive every reopen, including a final read-only reopen after the last
write. Shutdown still drains a retained cursor and partial frame; stale cursor
IDs must fail in the new listener. Only one old listener observer is retained,
and it must not keep its engine alive.

```bash
# Default: ten minutes. Explicit overrides must be 10..3600 seconds.
cargo test --locked --no-default-features --features mongo --test mongo_wire \
  resource_churn::extended::timed_mixed_crud_restart_soak -- --ignored --exact --nocapture
```

`BRISKDB_MONGO_SOAK_SECONDS=10` is a short harness check, not ten-minute soak
evidence. Each run prints actual elapsed time, completed waves/engine lifetimes,
and cursor/socket counts. The duration is an admission bound with a 60-second
cleanup allowance; stalls fail instead of waiting indefinitely. CI exposes the
ten-minute tier only through the `mongo_soak` workflow-dispatch input, retaining
its log; normal PR/push runs are unchanged. The existing workflow remains paused
during the local-only implementation batch. This finite test does not measure
RSS/allocator leaks, simulate power loss or I/O failure, or certify production
load, authenticated deployments, or the separate external-application gate.

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
documents, source matcher evaluations, `source_matches`, and returned
documents/distinct values. A source match counts a record observation accepted
by the source predicate, including direct-ID hits and unfiltered reads, before
sort-position rechecks, projection, skip/limit and pipeline stages. It is not
a unique-match count or final output count.
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
The fixed 64-slot `shard_documents_examined` and `shard_source_matches` arrays
also aggregate observed read-row work by physical ordinal, including rereads.
This exposes uneven row work separately from request distribution; zero-row
probes and buffered pages do not fabricate row observations. All totals saturate,
and live listener snapshots are best-effort independent atomic loads, not an
atomic multi-counter transaction. Native/Python per-request `shard_work` reports
only actually-read shards and charges a bounded 4,096-byte diagnostic budget.
The additional `storage_read_nanos` total and fixed 64-slot
`shard_storage_read_nanos` array measure elapsed monotonic time inside record-read
storage calls, including SQLite execution and BSON decoding, misses and rereads.
Native/Python snapshots also expose this integer timing on each shard summary.
Pool/worker admission, catalog checks, candidate-probe selection, matching,
engine sort-key/merge work and later result work are excluded. Concurrent shard
calls overlap, so the sum is not query wall time or physical disk latency. Empty/buffered pages
have zero time, and disabled requests read no timing clock. Error/unwind paths
drop their timer but failed engine requests still expose no snapshot or metrics.
They do not measure unique matched rows, per-shard CPU skew, physical SQLite page or
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
Only ordered key pairs are represented as BSON documents for transport. The
broader source accounting is tracked in the index inventory above; these oracle
runs do not change a frozen expected result or allowance.

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

## Update/routing property and fuzz checks

The document updater has four shrinkable property tests (256 generated cases
each): checked Int64 arithmetic/overflow, ordered array-operation sequences,
nested set/unset idempotence with exact unrelated BSON retention, and error or
cancellation atomicity. Generated values include arbitrary double/Decimal128
encodings and bounded nested documents/arrays. These tests run in the ordinary
native unit suite; they compare arithmetic and array results with independent
models, not with a second call to the same updater.

Routing has three additional 256-case properties: full-width hashes retain the
generation-one owner and bucket bounds, arbitrary keys consult a changed owner
map without mutating the original snapshot, and numeric BSON aliases route
identically as scalars and inside arrays/documents. A separate shrinkable matcher
check generates 256 logical ID filters for each of 2, 3, 8 and 64 shards. Every
selected bitmap must retain all authoritative matches across 81 BSON fixtures,
including numeric aliases, typed IDs, negation, nested logic and non-ID clauses.
It uses real native routing metadata but in-memory matcher fixtures, not a new
concurrent/resharding or wire-compatibility guarantee. Existing real-engine tests
separately verify actual shard checkouts, mutations, pipeline results and restart.

The `document_update` libFuzzer target accepts a BSON envelope with `update` and
`document` fields. Every input also exercises all eleven supported update
operators without needing a valid envelope. It checks deterministic compilation
and execution, callback cancellation, immutable ID representation, BSON round
trips and unchanged input/specification bytes. Input and generated arrays are
bounded. This complements the BSON, comparison, matcher, projection, sorting,
distinct and aggregation targets; it does not establish long-running fault-soak
or a complete differential/release certification.

The `mongo_wire` target additionally feeds arbitrary envelopes and raw request
payloads through the public uncompressed codec/parser. Whole-buffer and fragmented
decoding must agree on frames and terminal errors; incomplete/error input cannot
be consumed or cause the decoder to reserve input capacity. Every input also
constructs valid checksummed OP_MSG document sequences and a legacy handshake,
checking coalesced frames plus a partial successor at EOF. A separate bitwise
CRC32C implementation checks generated checksums and header/checksum tampering.
Mutated/truncated checksum-free scaffolds also reach section/BSON error paths.
Rejected encodes must leave pre-existing output bytes untouched. The same harness
runs as 256 shrinkable ordinary test cases plus deterministic seeds, including
oversized/negative length prefixes. It caps fuzz input at 16 KiB without changing
the product limits, opens no sockets/storage and makes no claims about command
execution, compressed transport or authenticated state machines. Existing socket
and compression tests remain separate gates.

```sh
cargo test --locked --all-features --lib document::update::properties
cargo test --locked --all-features --lib core::routing::tests
cargo test --locked --all-features --lib id_routing::tests
cargo test --locked --no-default-features --features mongo --test mongo_wire_properties
cargo check --locked --manifest-path fuzz/Cargo.toml --bins
cargo install cargo-fuzz --version 0.13.2 --locked
rustup toolchain install nightly --profile minimal
cargo +nightly fuzz run document_update -- \
  -max_total_time=60 -max_len=4096 -timeout=10 -rss_limit_mb=2048 -seed=186463
cargo +nightly fuzz run mongo_wire -- \
  -max_total_time=60 -max_len=4096 -timeout=10 -rss_limit_mb=2048 -seed=186468
```

Keep a failing artifact and reproduce it before reducing it. For an actual
failure, `cargo +nightly fuzz tmin document_update <artifact-path>` minimizes the
input; rerun the resulting artifact with `cargo +nightly fuzz run
document_update <minimized-artifact-path>`. Promote confirmed minimal failures
to deterministic native regressions. Property tests use proptest's shrinking and
failure-seed persistence. These mechanisms do not substitute for minimizing
cross-implementation differential mismatches; the reducer below adds that support
for filters, projections and field updates, while other differential surfaces remain separate work.

### Matcher, projection and update differential reproducers

The full source-locked matcher, projection and field-update matrices save confirmed mismatches under
`target/mongo-parity/reproducers` (override with `BRISKDB_MONGO_REPRO_DIR`). It keeps
the original before trying BSON field/array deletions. Every probe recomputes the
reference result with the isolated, source-hash-checked interpreter and must retain
the same reference/candidate outcome pair; stale expected values are never reused.
The search admits up to 128 probes within a 20-second admission budget; an
in-flight worker has a separate 10-second deadline. The diagnostic input limit is
64 KiB. A confirmed original remains available if
reduction fails or becomes unstable. Budget exhaustion is recorded explicitly;
deletion reduction is not a claim of globally minimal input.

Artifacts retain exact input/reference BSON, source commit, original candidate
outcome summary and probe metadata. Projection/update outcomes are compared byte-for-byte
during reduction (including field order and BSON numeric representations); their
saved candidate summary uses byte length and BLAKE3, not an unbounded debug dump.
They are atomically published without overwriting existing files, and
the existing CI parity-artifact upload retains them on failure. They contain input
data; review before sharing. The unchanged full matrix remains the conformance
gate. To replay one saved case after a fix:

```sh
BRISKDB_MONGO_ORACLE_PYTHON=/path/to/locked-oracle/bin/python \
BRISKDB_MONGO_REPLAY_BSON=/path/to/matcher-reproducer.bson \
cargo test --locked --all-features --test mongo_matcher_reproducer \
  replay_saved_matcher_case -- --exact --ignored --nocapture

BRISKDB_MONGO_ORACLE_PYTHON=/path/to/locked-oracle/bin/python \
BRISKDB_MONGO_REPLAY_BSON=/path/to/projection-reproducer.bson \
cargo test --locked --all-features --test mongo_projection_reproducer \
  replay_saved_projection_case -- --exact --ignored --nocapture

BRISKDB_MONGO_ORACLE_PYTHON=/path/to/locked-oracle/bin/python \
BRISKDB_MONGO_REPLAY_BSON=/path/to/update-reproducer.bson \
cargo test --locked --all-features --test mongo_update_reproducer \
  replay_saved_update_case -- --exact --ignored --nocapture
```

Replay refreshes the reference result, rejects changed expectations and checks
the current native matcher/projector/updater. Passing one saved case is diagnostic evidence,
not a full-matrix pass. Source-backed fault-injection self-tests deliberately
use wrong callbacks to exercise reduction, stale-expectation rejection, actual
subprocess replay, original retention and non-overwrite behavior; it does not
claim a real BriskDB mismatch.

Update reduction retains an `_id` and a nonempty operator document because the
unchanged reference helper requires them. Missing IDs, replacement/empty update
documents and non-operator names are rejected before probing. It does not expand
the 30,489-case matrix's documented per-operator intersection, rewrite intentional
TinyMongo/native differences, or claim upsert/replacement coverage. Native update,
mutation and embedded matcher errors retain their distinct Mongo codes; source
failure still preserves the confirmed original rather than accepting a stale result.

The existing CI workflow also has an opt-in `mongo_fuzz` dispatch input. Once CI
is re-enabled, selecting it runs every declared fuzz target with AddressSanitizer
on pinned `nightly-2026-09-27`/cargo-fuzz 0.13.2, with 30 seconds per target,
10 seconds per input and a 2 GiB RSS guard. Target discovery is checked and a
target failure stops the job; corpora and crash reproducers are retained for
seven days even on failure. Lockfile drift fails the run. This tier does not run
on ordinary PRs/pushes and does not re-enable a disabled workflow. It is a bounded
smoke tier, not a long-running soak or a Linux result until actually dispatched.

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

### Current hardening baseline (2026-09-27)

The [current raw baseline](benchmarks/mongo-public-clients-hardening-2026-09-27.json)
uses a freshly release-built development wheel from main
`02772e01e88117cacb8d5df4eecf133096066ec3`, after the storage-integrity and
Mongo test-hardening changes. Its package version remains `0.1.0a7`; it is **not**
the published alpha.7 artifact or a new release. The wheel SHA-256 is
`32554ed229f9dc010a8afb2642dbcbd4782c7ce2a4ba01a1ebad614c9df77d12`;
the report separately identifies its loaded native library and Python sources.
Artifact validation and all 545 installed-wheel tests passed before timing.

This run retains 1,000 seed documents, 64 operations, three rotating trials,
four BriskDB/TinyMongo shards and four writer threads. The host is macOS ARM64
(Darwin 25.6.0, 10 logical CPUs); all clients use Python 3.13.5 and PyMongo
4.17.0. The locked TinyMongo source remains unchanged. MongoDB 7.0.43 runs in
Docker Desktop on fresh VM-backed anonymous volumes, limited to two CPUs and
2 GiB RAM with a 0.25 GiB WiredTiger cache. Its immutable image digest is in the
report. Only literal-loopback access is exposed. This differs from the older
MongoDB resource configuration, so the older reports are not its regression
baseline or evidence of a controlled before/after speedup.

| Median command latency in this current small workload | BriskDB wire | TinyMongo in-process | MongoDB Docker wire |
| --- | ---: | ---: | ---: |
| Exact-ID read | 4.777 ms | 0.064 ms | 0.871 ms |
| Exact-ID update | 9.947 ms | 0.617 ms | 0.946 ms |
| Indexed equality query (materialized) | 39.939 ms | 4.660 ms | 1.968 ms |
| Sorted scatter window (materialized) | 35.278 ms | 14.261 ms | 1.408 ms |

All nine baseline trials produce the same 1,256 final documents with SHA-256
`ecdbda7a756269f0d4c41d60ead14c70390a9b41745d06b4b658ca6eaf2da1d5`.
The complete twelve-workload report, not just these command examples, retains
raw timings and units. BriskDB still has substantial end-to-end overhead in
this fixture; semantic agreement is not performance parity. This is finite
local evidence, not sustained-load, cross-platform or release acceptance.

The [matched repeat](benchmarks/mongo-public-clients-hardening-repeat-2026-09-27.json)
uses the same wheel, workload, interpreters, host and MongoDB environment. All
18 trials across both runs agree on final records; all 36 median regression
checks pass the unchanged 1.5x bound. The highest ratio is 1.083x rounded up
(MongoDB point reads); BriskDB's highest is 1.025x rounded up (cursor iteration).
This validates repeatability of this pair, not a code-change speedup. Reproduce
the repeat with the same command/configuration plus:

```sh
--baseline docs/benchmarks/mongo-public-clients-hardening-2026-09-27.json \
--maximum-regression-ratio 1.5 --output mongo-benchmark-new-repeat.json
```

Benchmark namespaces were confirmed absent afterwards, and the owned scratch
MongoDB container and its anonymous volumes were removed. No application data,
published wheel, release tag, or deployment was changed.

### Removing duplicate physical-schema inspection

The next controlled pair isolates a small storage-path change:
[before](benchmarks/mongo-public-clients-schema-before-2026-09-27.json) at
`8f778885e113aebb89ce44297d08d3c2458f1895` and
[after](benchmarks/mongo-public-clients-schema-after-2026-09-27.json) at
`83faf9a73fa7625020fe3daa37fed07d00ea4d4b`. Both are development release-built
wheels, not published artifacts. The candidate wheel SHA-256 is
`d2d539d11f1b6e464c6b1b6a8227707cb51248502dc1160335413691059b66f8`.
These runs use BriskDB and locked TinyMongo only; they do not rerun or compare
against the MongoDB container. Workload, worker hash, host, reference package,
interpreters, four-shard/four-writer configuration and three trials match.
Compiles and correctness suites finished before each timing run.

Each record operation formerly inspected the exact secondary-entry schema twice.
A shared presence result now distinguishes absent, legacy records-only and
complete storage, inspecting the records and secondary schemas once each.
Ordinary operations still require both exact schemas; there is no cached
authority or omitted integrity check. A deterministic authorizer-based test
counts two schema SELECTs per call, including repeat calls. Malformed/orphaned
schemas and failed inspection still reject without repair; explicit legacy
provisioning retains transactional rechecks.

| Median elapsed time per command in this fixture | Before | After |
| --- | ---: | ---: |
| Exact-ID read | 4.731 ms | 4.739 ms |
| Indexed equality query (materialized) | 39.494 ms | 36.634 ms |
| Scan query (materialized) | 169.354 ms | 132.649 ms |
| Grouped aggregation (materialized) | 288.828 ms | 250.799 ms |
| Sorted scatter window (materialized) | 35.458 ms | 33.255 ms |

Scan/group values above convert the report's per-fixture-document units back
to full commands using the fixed 1,000-document fixture. Observed scan, group
and indexed-query medians are approximately 22%, 13% and 7% lower respectively;
point-read overhead is essentially unchanged. These are measurements of one
matched small-workload pair, not statistical or general speedup guarantees.
All twelve trials across both runs retain the same 1,256 records/content hash,
and all 24 median regression checks pass the unchanged 1.5x bound. The full
report also includes workloads not improved by this change, including index
creation. Stable/MSRV schema tests, nine index-storage upgrade/recovery tests,
43 index/read tests, 46 wire tests and 545 installed-wheel tests pass locally.
Six native manual timing tests and seven explicit wire driver/soak tiers remain
opt-in; broader release/security/application acceptance is not implied.

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
