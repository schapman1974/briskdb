# Protocol-neutral document engine

Status: engine slice implemented by roadmap issue #162; embedded facade
implemented by issue #191

The opt-in `documents` feature adds an asynchronous document command boundary
to `core::Engine`. A caller submits an owned `DocumentRequest` to
`Engine::execute_document`; no HTTP, PostgreSQL, MongoDB, or other network
listener participates. `BriskDb` and `BriskSession` expose the same operation
to embedded Rust callers. This is the shared execution boundary for future
Python and MongoDB adapters.

Every request carries a nonzero 128-bit request identity and the same
`RequestContext` controls as SQL operations: cancellation, an absolute
deadline, and result limits that may narrow the engine defaults. The returned
`DocumentExecution` echoes the request identity and, for routed data commands,
the selected point or scatter plan. Reusing an identity helps correlate logs;
it does not make a write idempotent.

## Embedded facade

With the `documents` feature, the listener-free API adds these exact methods:

```text
BriskDb::execute_document(
    &self,
    session: &Session,
    request: DocumentRequest,
) -> EngineResult<DocumentExecution>

BriskSession::execute_document(
    &self,
    request: DocumentRequest,
) -> EngineResult<DocumentExecution>
```

The database form is useful to hosts that already manage core `Session`
values. The owned-session form keeps the database identity and shared
serialized session state in one cloneable handle. Neither method translates
BSON, reconstructs requests, or owns document semantics; after checking
per-handle enablement, it forwards the request unchanged to
`Engine::execute_document`.

The embedded facade has a build-time and a per-handle gate. Applications
compile with `default-features = false, features = ["documents"]`, then open
through `BriskDb::builder(...)` with `DocumentSupport::Enabled`.
Without the Cargo feature, these methods are not compiled and builder
validation of the enabled setting returns `Unsupported` before filesystem
access. With the feature present but support disabled on the handle, a facade
call returns `FailedPrecondition` before engine admission. See
[Embedded Rust](EMBEDDED_RUST.md#native-document-commands) for a complete
example.

## Implemented commands

The current engine executes:

| Command | Current behavior |
| --- | --- |
| `CreateCollection` | Provisions the catalog entry and fixed document table on every shard, then returns the active collection metadata |
| `CollectionExists` | Checks one exact namespace without enumerating unrelated collections; returns a boolean without creating metadata |
| `ListCollections` | Returns collection metadata for one exact database name |
| `CreateIndex` | Declares index metadata and returns its name; non-built-in indexes remain pending until physical index work lands |
| `ListIndexes` | Returns the built-in `_id_` definition and declared secondary-index metadata |
| `Insert` | Inserts ordered/unordered batches; generates missing ObjectIds, preserves explicit null IDs, and reports safe per-input duplicate failures |
| `Find` | Evaluates BSON match expressions and returns a bounded batch with a continuation ID when needed |
| `Aggregate` | Executes shared basic/projection stages over global natural-order input and returns a retained cursor |
| `ContinueCursor` | Resumes a session-owned find or aggregate cursor |
| `KillCursor` | Releases a session-owned cursor; reports whether it existed |
| `Count` | Evaluates the same match expressions, then applies global skip/limit |
| `Distinct` | Uses the same filters and global encounter order, with shared BSON identity and bounded unique values |
| `Delete` | Deletes one document selected by exact `_id` |

`CollectionExists` uses an admitted, controlled manifest lookup and scalar result
accounting. Its result is independent of catalog page size and unrelated metadata
size. Missing databases/collections return false; names are exact and case-sensitive.
The wire adapter uses this same command for absent-collection handling instead of
listing the catalog. Existence and a later read/write are not one atomic operation.

An exact `_id` filter, including `{_id: {$eq: value}}`, produces a
`DocumentPlan::Point` with one collection and one physical shard. Other filters
produce a deterministic `DocumentPlan::Scatter` over every shard. The shared
Rust matcher runs before scatter reads merge by the durable
cross-shard natural-order value, so insertion order remains stable across
restarts. `skip` and `limit` apply once across the whole cursor, after the
merge; `batch_size` bounds each returned page. An initial batch size of zero
opens a cursor without reading documents. Continuations require a positive
batch size and cannot change the original skip or limit.

Find cursors retain the compiled query, collection identity, and last consumed
natural-order position—not SQLite connections, transactions, schema guards, or
result documents. Pages are not a snapshot across concurrent writes. The engine
allows at most 32 cursors total and 8 per session, with a conservative 64-MiB
query-retention accounting quota. Idle cursors expire after 10 minutes, checked
on registry access. Session close/drop and engine shutdown release their cursors;
failed admitted continuations release theirs too. Foreign sessions/namespaces
cannot read or kill another cursor. Invalid/stale IDs report `DocumentCursorError`.

`DocumentReadOptions::with_batch_byte_limit` supplies an optional soft page
boundary including engine result overhead: a page ends before the next document
would exceed it. A single document that cannot fit fails. This does not weaken
the request's hard `ResultLimits`. A continuation may narrow this byte ceiling,
but cannot widen it. The alpha API's `DocumentReadOptions::into_parts` now returns
this sixth component, and `with_batch_size(0)` is valid for initial find.

### Projection

`DocumentReadOptions::with_projection` uses the shared Rust `DocumentProjector`.
Basic inclusion/exclusion, dotted paths, nested-mapping shorthand, array
traversal, and explicit `_id` rules follow the source-locked TinyMongo contract.
Boolean and BSON numeric flags are accepted: zero excludes, nonzero includes.
An empty document is an identity projection. Conflicting paths and mixed modes
(except the separate `_id` flag) fail eagerly with payload-free query errors.
Numeric array-index output paths, positional operators, `$slice`, `$elemMatch`,
and expression projections are explicitly unsupported.

Filtering uses original values. Projection preserves the surviving fields'
original order and exact BSON representations without changing stored BSON.
Returned-byte limits apply after projection, while the original read still obeys
storage/merge memory bounds. A cursor retains its initial projection and rejects
attempts to change it on continuation. Compilation is limited to 1 MiB, 4096
path components, and depth 100; evaluation is bounded to one million traversal
steps with cancellation/deadline checks in admitted blocking workers. Compiled
projection state counts against the existing cursor retention quota.
Required CI compares 4,865 generated BSON projection cases with the locked
oracle, separately from the full frozen command corpus.

### Global sorting and retained pages

`DocumentSorter` compiles an ordinary nonempty BSON sort specification with up
to 32 fields and numeric directions exactly `1` or `-1`. `key` returns an owned
`DocumentSortKey` that compares using BSON semantics; callers must add a stable
natural-order tie-breaker. Neither compilation nor key generation mutates the
input, and debug/error messages do not expose document values. Metadata and
expression sort specifications are explicitly rejected. Engine `find` accepts
`DocumentReadOptions::with_sort`; wire clients can use ordinary PyMongo
`find(...).sort(...)` and `find_one(..., sort=...)`. An empty find sort document
leaves natural order unchanged.

Sorting uses original matched values before global skip/limit and projection.
Equal BSON keys use durable natural order as the tie-breaker across shards and
pages. Exact-ID queries retain point routing while still validating their sort
keys. Cursors retain the compiled sort and last consumed key/position, and
reject attempts to change the sort on continuation. Growing keys are charged
again against the shared cursor retention quota; an over-quota continuation
fails and releases its cursor.

Until sorted indexes are available, each bounded window rescans matching
documents on all shards. A window retains at most 1024 keys and a conservative
64-MiB heap charge (plus the current bounded input/key while considering it),
then fetches only selected documents. Large skips can span several windows;
memory-bound or window-bound pages may be shorter than the requested batch.
Result byte limits apply after projection. Cancellation/deadlines cover scans,
key derivation, heap extraction, and fetches in admitted workers. No result
documents or SQLite leases are retained between requests. Selected rows are
rechecked after fetch, so deletion, a changed filter match, or a changed sort
key cannot return an unrelated/nonmatching row at the old position. Concurrent
writes do not have cross-batch snapshot semantics; moved sort keys can be missed
or encountered again at a later position. This is bounded in-memory sorting,
not indexed sorting or external spill-to-disk execution.

The shared implementation handles missing/null ties, the empty-array position
between MinKey and null, direction-sensitive array member selection, dotted
paths, and canonical numeric array indexes. Compound keys correlate values from
the same array element. Parallel arrays and ambiguous numeric paths produce
the locked contract's errors, including their precedence. Nonempty arrays
selected by a numeric endpoint compare as whole BSON arrays.

Compilation is capped at 1 MiB and 100 path components per field. Per-document
key generation is limited to 16,384 total path candidates, one million work
steps, a 64-MiB conservative temporary-allocation charge, and an 8-MiB owned-key
charge. Candidate joins use an array-provenance index instead of an unbounded
cross product. Cooperative check callbacks support cancellation/deadlines;
storage integrations must invoke them in admitted blocking work. Both compiled
specifications and owned keys expose conservative retention charges.

Required CI compares 4,654 generated sorting cases with source-locked TinyMongo,
including specification validation, all supported BSON families, stable ties,
compound arrays, and error precedence. Native and real sync/async driver tests
also cover global paging, stable ties, projection, byte bounds, and restart.
This does not claim that the full frozen command corpus passes.

### Matching and writes

The matcher supports dotted paths, missing/null distinctions, numeric BSON
equivalence, type-bracketed comparisons, array membership, `$eq`, `$ne`, `$gt`,
`$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`, `$not`,
`$all`, `$elemMatch`, `$size`, `$type`, `$mod`, and bounded regex predicates with
`i/m/s/x/u` options. Logical branches are validated eagerly. Regex predicates
are distinct from literal regex equality; ordinary backreferences and lookarounds
are supported, with a backtracking budget. Python-style final-newline anchors,
Unicode word/space classes, and dotted/dotless-I case folding are normalized.
Engine-specific regex recursion/control verbs are rejected. Broader regex
dialect conformance remains part of #167, not a claim of complete PCRE support.
Following the frozen TinyMongo contract, `$type: "int"` includes Int64 values
within the Int32 range; stored/returned BSON still preserves the Int64 tag.

Compilation limits queries to 1 MiB, 4096 nodes, 100 levels, and 32 regexes of
at most 4096 bytes each. Per-document execution limits path candidates and
evaluation steps, checks cancellation/deadlines, and executes only inside
admitted blocking workers. The matcher is authoritative; no SQL prefilter is
used in this slice. Required CI compares a generated BSON matrix against the
source-locked TinyMongo oracle without adding a production Python dependency.

The engine preserves BSON field order and exact BSON representations in stored
and returned documents. It accounts returned rows and encoded BSON bytes
against the effective `ResultLimits`; exceeding either bound fails the whole
command rather than returning a partial batch.

Insert batches preflight every document and the result budget before writing.
Each document routes by its canonical BSON `_id`. Contiguous same-shard inputs
share one connection lease and worker without changing input order. Writes
commit individually: ordered batches stop at the first duplicate; unordered
batches continue after duplicates and return every error's input index alongside
successful IDs. A single-document duplicate remains a `UniqueViolation` engine
error. Other failures, including cancellation and storage errors, stop the
command; earlier successful writes may remain. No cross-shard atomicity is
promised.

Missing IDs become BSON ObjectIds in the engine-owned copy. Explicit null IDs
are preserved. Direct, non-`_id` `Timestamp(0, 0)` fields receive distinct server
timestamps, while nested/array timestamps and timestamp-valued IDs are untouched.
Callers' BSON documents are not mutated. Inspect `DocumentInsertResult::write_errors`
before treating a batch result as wholly successful.

## Execution and storage boundary

Document commands use the same engine lifecycle, session serialization,
bounded blocking-worker admission, per-shard connection pools, cancellation,
deadline, shutdown, and classified `EngineError` behavior as SQL commands.
Protocol code is not allowed to open SQLite connections or call the document
storage layer directly.

The reserved `briskdb_documents_v1` table remains unavailable to ordinary SQL.
The engine grants its pooled connection only the narrow read/write operations
needed for one admitted document storage primitive, then restores the ordinary
authorizer before that connection can be reused.

### Distinct values

`DocumentDistinct` is the shared incremental extractor/deduplicator used by
`DocumentCommand::Distinct`, native Python `Session.distinct`, and the wire
`distinct` command. Required CI compares 4,888 cases with the source-locked
TinyMongo implementation. Missing paths contribute nothing; null is a value.
A final array contributes its immediate members, including nested arrays as
values. Dotted components traverse mappings only, not intermediate arrays or
numeric array positions. Empty/dollar/numeric components are literal mapping
keys. A NUL-containing key cannot match valid stored BSON and yields no values.
These are frozen TinyMongo semantics, not a claim of all MongoDB distinct rules.

Deduplication uses the shared `BsonValue` identity: boolean and numeric values
are different families, numeric aliases compare equal, and embedded document
field order matters. The first encountered exact BSON representation is kept.
The engine reads in global durable natural order and filters original documents;
exact-ID filters retain point routing. Internal one-document pages reuse the
bounded merge without allocating a retained cursor, even if a session already
has its maximum public cursors. There is no cross-shard snapshot guarantee or
indexed distinct optimization; filtering/frontier probes can be repeated between
internal pages. Request deadlines bound that work.

Collector bounds are 65,536 unique values, 8 MiB conservative heap charge per
value, 64 MiB retained state, 1 MiB/100 components per field, and one million
checked extraction steps per source document. Request row/byte limits are charged
for each new output value before retaining it, not for unrelated source payloads.
Exceeded limits or cancellation fail the whole command, never return a partial
success, and leave the session reusable. The public collector is unusable after
any failed push. Native distinct rejects projection/sort/pagination options;
wire hints, collation, read concern, and comments are not implemented yet.

### Basic aggregation core

`DocumentAggregator` compiles a `DocumentPipeline` into shared `$match`, `$sort`,
`$skip`, `$limit`, `$count`, `$group`, and the projection stages described below. Every stage is validated before execution,
even behind an empty-producing stage. Empty pipelines preserve inputs; stages
execute in declaration order without mutating their source documents. Matching
uses `DocumentMatcher`; sorting uses `DocumentSorter` with stable ties relative
to the preceding stage. Repeated sorts therefore preserve the earlier stage's
ordering where the later keys tie. Exact BSON types and field order survive.

Skip/limit accept finite, exactly integral BSON numbers through signed 64-bit
maximum, including Double/Decimal128; booleans and fractions are invalid. Skip
allows zero; limit requires a positive value. Count emits no document for empty
input, otherwise one document with the validated field name and an Int32 count
under the current row bound. Empty, dollar-prefixed, NUL/dotted and `_id` count
fields retain their distinct frozen validation errors. Generic stage-shape and
non-document match errors have no numeric code in the reference; the typed core
represents those as BadValue (2). Unimplemented stages remain explicitly unsupported.

The borrowed `DocumentAggregator::execute` API materializes input. It admits at most
65,536 input rows and 64 MiB of conservative working-data retention, including
sort keys, plus a separate 64 MiB compiled-plan quota. BSON is structurally
validated and its retained size checked before cloning. All stages share a
four-million-step execution budget and cancellation callback, including key
extraction, selection, count and result collection; compilation has its own
four-million-step bound. Existing matcher/sorter per-document limits also apply.
The shared BSON heap estimator is reused by sorting, distinct and aggregation.
Sorts use a bounded key heap with input-position ties; no disk spill or index
optimization is implied. A later limit cannot bypass initial materialization
limits. Errors return no partial success and leave the compiled plan reusable.

Required CI compares 5,134 whole-pipeline cases with the source-locked TinyMongo
runner, including stage permutations, BSON families, repeated sorts, numeric
boundaries, and eager errors. Unit tests cover each cancellation/deadline
checkpoint, memory/row/work limits, immutable inputs and redacted diagnostics.
Both materialized and incremental modes run against every oracle case.

### Aggregation projection and expression stages

`$project`, `$set`, `$addFields`, and `$unset` share one Rust transformation layer
across native and wire APIs. Basic include/exclude and unset operations reuse
`DocumentProjector`. Computed projections retain traversed source fields in source
order, then append direct computed fields in specification order. Nested output
paths preserve array shape; computed descendants create object shells for missing
or scalar parents. Set/addFields evaluate every assignment against the original
document before applying any changes, preserve replaced field positions, and
broadcast dotted assignments through arrays. No stage mutates stored input.

Supported expressions are field references, literal values/arrays/documents,
`$literal`, `$ifNull`, `$size`, and `$$REMOVE` (including validated suffixes).
Missing values remain distinct from null: object fields omit missing results,
expression arrays replace them with null, and ifNull skips both missing and null.
Field references traverse arrays of documents but do not descend through raw
nested arrays at the same path component. Output numeric/positional paths and
other variables/operators remain unsupported. Path collisions, mixed projection
modes, eager expression validation, and error-code precedence follow the frozen
implementation; code-less expression-shape failures map to BadValue (2).

Nonblocking stages are fused in both execution modes, including after sort/count.
A later limit stops evaluating preceding expressions on unconsumed rows; skip
still consumes/evaluates skipped rows. Each transform specification is bounded to
1 MiB encoded BSON, 4,096 charged syntax/path nodes, and depth 100. Each row's
transform has a one-million-step budget, nested within the pipeline's persistent
four-million-step budget. A 64 MiB cumulative allocation-work cap includes the
source and all copied/generated values, even discarded fallbacks, so broadcast
amplification is rejected before allocating the full result. Available allocation
headroom is reduced by buffered and unconsumed rows already in the pipeline.
Generated rows are
codec-validated before later stages; normal BSON depth/size and retained-output
limits still apply. These are conservative quotas, not RSS measurements.

Required CI adds 7,037 source-locked whole-pipeline transform cases in both modes,
checking exact BSON/order, missing/null/array behavior, lazy consumption, and
validation. Unit tests cover amplification, depth/nodes/bytes, every cancellation
and deadline checkpoint, input immutability, and failed-stream poisoning. Native
and real-driver tests cover paged transforms, runtime-error cursor cleanup,
output expansion/byte caps, sync/async calls, and restart. Projection-to-group
identity now uses the shared group stage below. Full candidate-corpus acceptance
still requires collection lifecycle support for the frozen harness.

### Aggregation groups and numeric accumulators

`$group` supports `_id: null` or a field reference, including fields containing
compound documents or arrays. Missing keys become null. Groups retain first
encounter order and the first exact key representation using shared recursive
BSON identity; numeric aliases compare equal while document field order matters.
Computed/constant keys other than null remain unsupported under the frozen
reference grammar. Accumulator expressions reuse the transformation evaluator
without `$$REMOVE`. All shapes, output names, expressions, and error precedence
are validated even for absent collections and empty inputs.

Supported accumulators are `$addToSet`, `$avg`, `$first`, `$last`, `$max`, `$min`,
`$push`, and `$sum`. First/last include missing as null; first still evaluates
later operands and can therefore fail on a later invalid expression. Push and
addToSet omit missing, retain explicit null and whole arrays, and preserve input
order; addToSet retains the first representation of each BSON identity. Min/max
ignore missing/null, return null if there are no comparable values, and retain
the last representation on equal extrema. Results put `_id` first, followed by
accumulator fields in specification order. Source documents remain unchanged.

Sum/average ignore nonnumeric values (including booleans and arrays). Integer
totals remain exact within the 65,536-input bound. As in frozen Python, an
integer-only result uses Int32 if representable, otherwise Int64; this is not
a claim of MongoDB's original-Int64 type retention. Frozen Python cannot BSON
encode totals outside Int64; BriskDB returns Double for that explicit boundary,
consistent with the [documented overflow result type](https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/#result-data-type).
This does not promise identical intermediate-overflow behavior for every MongoDB
version. Decimal128 arithmetic uses a 34-digit, half-even, clamped IEEE context
with exact binary64 operands before addition, retaining decimal scale. Average
uses separate decimal and compensated two-double totals with split Int64 input;
it follows the frozen cancellation, mixed-number, and nonfinite rules. Empty
numeric sums return Int32 zero, empty numeric averages null. Newly computed
double NaNs use a canonical quiet NaN; their arithmetic sign/payload is not
specified. Pass-through values, keys, extrema, and sets preserve original bits.

Grouping is blocking but retains states, not source documents. The global
natural or preceding sort order feeds the states, so first/last and ordered
numeric addition do not depend on physical shard boundaries or cursor batches.
Rounded shard-local totals are **not** merged: partial aggregation pushdown is
still open because rounding is not associative. Group keys/states/output and
per-row expression allocation share conservative 64 MiB working bounds,
reduced by unconsumed rows already retained upstream. Specifications are capped
at 1 MiB/4,096 charged syntax/path nodes. Pipeline row/work/cancellation limits
remain cumulative. Every completed group document is BSON-size/depth validated
before any result is delivered, even if a later limit/project would shrink it.
Failures poison the execution and release its cursor without partial group
results. No disk spill, indexed grouping, or snapshot is promised.

Required CI adds 9,509 source-locked grouping pipelines, each tested in both
execution modes. Comparisons retain exact BSON except arithmetic Double NaN
bits in explicitly tagged numeric output fields; input/pass-through NaNs remain
byte-for-byte comparisons. The unchanged reference produces all expectations;
unencodable integer totals are separate Rust edge tests, not coerced oracle
outputs. Tests also cover structured identity, stage ordering, numeric quantum,
resource limits, every cancellation/deadline checkpoint, cursor cleanup,
cross-shard byte paging, sync/async native and wire clients, and restart. Full
frozen command-corpus acceptance and partial-shard state merging remain open.

### Aggregate commands and cursors

`DocumentCommand::Aggregate`, native Python `Session.aggregate`/`AsyncSession.aggregate`,
and sync/async PyMongo `aggregate()` now share that compiled core. The engine's
`DocumentAggregationStream` moves owned source documents through a streaming
match/skip/limit/transform prefix. The first count retains only a counter; the first sort
retains bounded input; the first group retains bounded accumulator state.
Finalization feeds blocking-stage output through the
remaining shared executor. A prefix limit stops further source consumption.
Streams admit at most 65,536 consumed inputs and four million checked steps
over their entire lifetime, including finalization; bounds do not reset per
cursor batch. A failed push poisons the stream. Simple pipelines are not fully
buffered merely for wire delivery. The borrowed `DocumentAggregator::execute`
API above remains an explicitly materialized alternative.

Collection reads currently use controlled one-document source pages in global
durable natural order, with a bounded shard frontier independent of caller
output limits. All pipeline CPU work runs in admitted workers with cancellation
and deadlines. This first integration uses scatter plans even for exact-ID
matches; predicate/index pushdown and more efficient frontier reuse remain later
optimizations. Repeated frontier probes can increase read work. No cross-shard
snapshot is promised. Streaming batches can see concurrent changes; once a
blocking stage has produced retained results, that buffered remainder is fixed.

Aggregate cursors use the same 8-per-session/32-global registry, 64 MiB aggregate
retention quota, namespace ownership, 600-second idle expiry, and error/close/
kill/shutdown cleanup as find. Retention includes the compiled pipeline, sort
input, group state and buffered results, including queue allocation left after popping rows.
Empty initial batches defer source reads. Continuations enforce unchanged query
semantics, positive batch sizes, and retained soft byte caps. Hard result limits
fail the current request and discard its cursor; previous delivered batches
cannot be retracted. No SQLite lease or schema gate survives a request.

Native aggregate read options accept only batch size/byte cap; skip/limit belong
in the pipeline and find-style sort/projection options are rejected. Python
accepts a list of stage mappings and returns the existing cursor result shape;
`get_more`/`kill_cursor` work unchanged. Its pipeline conversion shares one BSON
wrapper budget (16 MiB encoded/64 MiB conservative heap), rather than one allowance
per stage. Tests exercise both driver styles, exact BSON, repeated sorts,
cross-shard counters, source data exceeding sort retention with tiny count output,
restart, shared cursor quotas, byte paging, and deterministic admission interruption.

## Current boundary

Update expressions, replacements,
multi-document deletion, upsert, additional aggregation expressions/group-key forms,
metadata cursors, and physical secondary-index builds remain later roadmap work.
Unsupported command shapes return the stable `EngineErrorKind::Unsupported`
category.

The opt-in Mongo listener and embedded document adapters translate into this
engine boundary instead of implementing routing or storage behavior themselves.
See [the Mongo parity contract](MONGO_PARITY.md),
[the BSON contract](BSON.md), and [document storage](DOCUMENT_STORAGE.md) for
the adjacent contracts.
