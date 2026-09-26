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

The [alpha transaction policy](../ROADMAP.md#cross-shard-transaction-policy--alpha-decision-74)
keeps general distributed transactions unsupported. Document commands require a
ready session and cannot join a caller's SQL transaction. Inserts commit per input;
many-scope updates/deletes commit per targeted shard. The detailed failure rules
below remain authoritative: earlier commits can survive a later failure, and
global unique enforcement is not whole-batch atomicity. A disconnect/crash after
commit but before delivery can leave an unknown outcome; a repeated request ID
does not deduplicate it. Stable IDs or application-level conditional updates can
help reconcile/retry, but there is no generic exactly-once or retryable-write promise.

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
| `DropCollection` / `DropDatabase` | Durably removes the exact collection or logical document database; returns `NamespaceDropped(bool)` indicating whether it existed |
| `CollectionExists` | Checks one exact namespace without enumerating unrelated collections; returns a boolean without creating metadata |
| `ListCollections` | Returns collection metadata for one exact database name |
| `ListCollectionMetadata` | Filters and pages BSON collection metadata through the shared cursor registry; `name_only` restricts both output and filtering to name/type |
| `ListDatabaseNames` | Returns `DatabaseNames(Box<[String]>)` from a validated document catalog snapshot; shared filters on `name` and logical combinations, normal request controls, no disk statistics |
| `CreateIndex` | Validates and normalizes ordered keys, resolves a bounded default/explicit name, and declares pending index metadata; it does not build or enforce a secondary index |
| `BuildIndex` | Explicitly builds a declared index under sole-process/exclusive schema admission; returns `IndexReady(name)` after complete publication |
| `CreateBuiltIndex` | Creates and builds one index on an existing collection; returns `IndexBuilt { name, before, after }` with Ready counts under the same exclusive admission |
| `CreateIndexes` | Eagerly validates up to 1,000 definitions, then creates them in order under one exclusive admission; logical-definition conflicts, built-in ID no-ops, and `IndexesBuilt { before, after }` counts |
| `DropIndex` | Removes an exact pending declaration or recoverably removes a built index and its derived entries; never removes BSON records |
| `DropIndexes` | Removes an exact name, an unambiguous single-field alias, or every secondary definition under one exclusive admission; returns Ready `before` / `after` counts and protects the built-in ID index |
| `ListIndexes` | Returns the built-in `_id_` definition and declared secondary-index metadata |
| `ListIndexMetadata` | Pages BSON metadata for built indexes only, built-in first then by name; shares cursor controls and excludes pending declarations |
| `Insert` | Inserts ordered/unordered batches; generates missing ObjectIds, preserves explicit null IDs, and reports safe per-input duplicate failures |
| `Find` | Evaluates BSON match expressions and returns a bounded batch with a continuation ID when needed |
| `Aggregate` | Executes shared basic/projection stages over global natural-order input and returns a retained cursor |
| `ContinueCursor` | Resumes a session-owned find or aggregate cursor |
| `KillCursor` | Releases a session-owned cursor; reports whether it existed |
| `Count` | Evaluates the same match expressions, then applies global skip/limit |
| `Distinct` | Uses the same filters and global encounter order, with shared BSON identity and bounded unique values |
| `Delete` | Deletes one or many matches; exact `_id` routes to one shard, other filters use the shared matcher |
| `FindOneAndDelete` | Atomically deletes one shard-local selection and returns its projected pre-delete document |
| `FindOneAndReplace` | Replaces one shard-local selection or upserts, returning its projected before/after document and insertion metadata |
| `FindOneAndUpdate` | Applies the supported field/array operators below to one shard-local selection or upserts, returning its projected before/after document and insertion metadata |
| `Replace` | Replaces one match, preserving `_id` and natural order, or inserts a replacement upsert; returns counts and optional inserted ID |
| `Update` | Applies the supported field/array operators below to one or many matches; returns matched/modified counts, with one transaction per shard for many |

`CollectionExists` uses an admitted, controlled manifest lookup and scalar result
accounting. Its result is independent of catalog page size and unrelated metadata
size. Missing databases/collections return false; names are exact and case-sensitive.
The wire adapter uses this same command for absent-collection handling instead of
listing the catalog. Existence and a later read/write are not one atomic operation.

Index declarations validate ordered, distinct dotted field paths before namespace
lookup or catalog mutation. Empty path components and `$`-prefixed components are
rejected. Ordinary numeric directions exactly equal to `1` or `-1` normalize to
Int32; booleans, other numbers, and special index types are rejected. Key order
remains significant. Bounds are 32 fields, 100 path components, 1 MiB encoded
specification, and 255 UTF-8 bytes for an explicit or generated name. Validation
runs on a controlled worker and observes cancellation/deadlines.

Omitting the name produces ordered `field_direction` pairs joined by underscores,
following the ordinary [MongoDB naming convention](https://www.mongodb.com/docs/manual/indexes/).
An explicit short name can accommodate a long valid field path. `_id`/`_id_`
remain reserved; this checkpoint does not add built-in-index redeclaration.
Same-name declarations with identical ordered keys and uniqueness are idempotent;
conflicts fail without catalog changes. Legacy numeric direction aliases compare
semantically without rewriting their original stored BSON. Opaque legacy
specification envelopes retain byte-exact conflict checks. Existing metadata is
still readable; new validation does not rewrite or activate older declarations.
`DocumentIndexMetadata::id()` exposes a durable `DocumentIndexId`, unique within
the database root and never reused after committed drops. The version-17 manifest
migration assigns IDs without altering existing BSON specifications or lifecycles.
Declarations and ID allocation share one transaction; idempotent calls retain
the same ID. IDs remain internal to Rust metadata for now: Python/wire result
shapes are unchanged, and SQL indexes use a separate identity space.

Native declarations also accept sparse or partial membership, using the shared
source-locked key generator to validate the predicate eagerly before catalog
mutation. The supported partial subset includes nonempty `$and`/`$or`, equality,
ordered comparisons, `$in`, `$type`, and `$exists: true`. Empty partial filters,
unsupported branches and combining sparse with partial are rejected. The complete
retained specification, including keys, name and filter, is bounded to 1 MiB.

Ordinary declarations retain their existing flat-key encoding. Sparse/partial
declarations use the exact ordered v2 envelope already used by TinyMongo import;
the filter's BSON representation is preserved. Repeating the same normalized
envelope is idempotent and keeps its ID. Advanced envelopes retain byte-exact
conflict checks; semantic predicate equivalence is not inferred. No older
specification is rewritten; declarations reuse the existing specification encoding.
`DocumentIndexMetadata::definition()` offers a borrowed keys/options view for
recognized flat or v2 encodings; unknown legacy envelopes remain readable through
`specification()` and return no interpreted view. The view itself is not a
membership validator or authority: consumers must compile it before execution.

New secondary declarations start `PendingBuild`, including `unique` ones:
they are not query authorities or uniqueness constraints. Declarations do not
scan or validate existing records. Explicit builds and transactional
maintenance and wire creation/discovery/removal are implemented separately below,
along with conservative equality candidates. Required CI compares 64 valid ascending integer-key definitions
against unchanged TinyMongo index source, including names, key order, flags and
restart metadata. Descending/numeric-alias normalization, invalid inputs,
resource limits and legacy metadata preservation are independently tested; no
frozen expectations or compatibility allowances are changed.

`DropIndex` removes one pending or built index by its exact, case-sensitive name.
It protects both `_id` and `_id_`, reports a typed not-found error for an absent
index, and never interprets field aliases, key patterns or `*` as bulk selectors
(`*` can still be an exact native index name). It neither creates a missing
collection nor touches document rows or SQL indexes. For pending declarations, deletion, identity-map
cascade and checksum refresh share one manifest transaction; the allocation
high-water mark is retained, so recreating the name gets a new index ID.
Result-budget and request-control failures before commit leave the declaration
unchanged. A successful commit returns `Acknowledged(true)` without a later
cancellation check turning that committed removal into an apparent failure.
Crash tests cover both sides of commit. Built indexes instead require
sole-process ownership and exclusive schema admission: intent atomically changes
the target to PendingBuild and installs the existing version-19 Drop journal.
Cleanup removes only that globally unique index ID from each shard, then deletes
its declaration/identity mapping and publishes the surviving compiled cache.
The permanent allocator is never reset. Cancellation after intent leaves schema
admission fenced until reopening completes the drop; it is not a rollback promise.
Process-exit tests cover both sides of intent, shard, cursor and completion commits.
Other Ready indexes and exact BSON remain unchanged. A schema-guarded, bounded
Ready-name cache selects this path without adding I/O to pending drops, preserving
their concurrent behavior.

`DropIndexes(DocumentDropIndexesRequest)` is the separate exclusive selection
path used by Mongo `dropIndexes`. An exact name takes priority over field aliases;
without an exact name, only one recognized single-field match is accepted.
Ambiguous aliases fail before any removal (use an exact name); selectors are
bounded to 255 UTF-8 bytes. `DocumentDropIndexesRequest::all` selects every
secondary definition, including non-enforcing Pending declarations, never `_id_`.
The complete selection and Ready counts are resolved under one schema/process
guard, and fixed-size result limits precede mutation. Completed removals survive
an error; an admitted unfinished drop completes on reopen, while later unstarted
indexes remain. This is not batch rollback or a durable all-index transaction.
The permanent allocator and document rows are unchanged. Named `*` remains a
literal in the native constructor; only the wire adapter translates `*` to `all`.

`DocumentIndexKeyGenerator` is the shared, immutable secondary-key foundation,
not a physical index. It validates key definitions and compiles optional partial
membership predicates with the existing matcher. Keys expose equality and hashing,
not ordering; callers must scope identities to an index and collection.
Ascending/descending directions do not change equality. The versioned `BDIK`
byte codec preserves tuple identities for later physical storage; see the
[secondary-index key format](BSON.md#secondary-index-tuple-keys). It does not
activate catalog entries or promise physical index coverage.

`DocumentIndexPreparation::compile(&collection_metadata)` compiles all secondary
declarations in one catalog snapshot, including pending indexes, for future build
and write preflight. Its `prepare(&document)` returns collection/index-scoped
BDIK frames only after every index succeeds. The built-in `_id_` remains owned
by record storage; sparse/partial exclusions retain an empty list for that index.
Unknown imported definitions fail explicitly instead of losing their options.
Compilation and preparation each share a 64 MiB conservative work budget and
one million checkpoints; preparation allows at most 16,384 total keys across
at most 64 secondary indexes. Encoded outputs and vector overhead are charged
before allocation, and repeated scalar/path work does not reset per index.
Input BSON is validated once, including when there are no secondary indexes.
The controlled methods discard all partial work on cancellation or error and
remain reusable. Debug output omits definitions and values.

This pure helper does not touch storage, enforce declared uniqueness, change
index lifecycles, or establish catalog freshness. A compiled snapshot may be
stale after a drop; physical callers must fence metadata and maintain entries
atomically with the document. Ordinary writes still ignore non-enforcing pending
declarations. `DocumentCommand::BuildIndex(DocumentBuildIndexRequest)` now builds
one declared index offline under exclusive schema admission and
sole-process ownership. It returns `DocumentResult::IndexReady(name)` after all
shards commit and the checksummed manifest publishes Ready. Repeated builds and
matching declarations preserve that Ready lifecycle. Ready entries are maintained
transactionally by every record write and verified on reopen. Interrupted builds
require reopening; startup discards the unpublished derived entries, leaving the
declaration pending. Shared preparation bounds apply across all Ready indexes,
not independently per index. Unique builds validate every prospective key
across all shards before durable intent; duplicate data returns `UniqueViolation`
(Mongo code 11000), without activating the constraint.
`CreateBuiltIndex(DocumentCreateIndexRequest)` combines normalization, declaration
and build under that admission. Preflight failures leave no new declaration or
allocated identity. Matching Ready retries are idempotent; matching Pending
declarations keep their original identity and abort behavior. For a new index,
the declaration and v19 DROP cleanup obligation commit together, with the cleanup
cursor held at zero until final activation cancels the obligation. Reopening an
interrupted operation removes both the new declaration and its derived entries,
without reusing its committed identity. `before` and
`after` count Ready indexes (including `_id_`, excluding unrelated Pending
declarations); result limits are checked before durable intent.
`CreateIndexes(DocumentCreateIndexesRequest)` adds bounded ordered batches and
logical-definition conflict checks for Mongo `createIndexes`. It validates every
shape before mutation, retains one schema/process guard across the entire batch,
and checks the fixed-size response budget before any durable intent. A runtime
failure can retain a completed prefix; this is not atomic batch rollback.
Matching names take precedence over legacy equivalent duplicates; a changed
definition under the same name is code 86, an equivalent definition under another
name is code 85. Recognized flat/v2 encodings compare keys and membership options
without rewriting stored BSON or IDs; partial filters retain exact BSON comparison.
Pending declarations participate in conflict checks and matching ones are built.
The ascending built-in ID request is a no-op with actual Ready counts; descending
ID creation is unsupported. The older singleton declaration/build APIs retain
their existing permissive naming behavior.
Broader planner candidates and selector compatibility remain open under #178/#174.
Whole-bulk post-image uniqueness is not promised by the alpha transaction policy.
Ready-index discovery is
implemented through `ListIndexMetadata` and Mongo `listIndexes`.

### Equality index candidates

Find (including sorted/paged reads), filtered count/distinct and mutation
selection can use a current Ready index when every indexed
path has a necessary supported scalar equality, bounded literal `$in` list, or
explicit field-absence condition.
Direct equality, `$eq` and positive `$and` clauses are recognized; the entire BSON matcher still verifies
each candidate. Compound paths and scalar membership in final arrays reuse the
same canonical BDIK keys as transactional write maintenance. Missing/null and
numeric cohorts keep their BSON semantics; booleans remain distinct from numbers.

Partial indexes are not selected. Sparse all-null tuples, incomplete compound
constraints, array/object operands and ObjectId/date/nonfinite operands are not
eligible for finite-key probing. Logical negations and ranges alone cannot
establish a finite witness. Membership lists
must be nonempty and contain at most 128 supported scalar literals each, with
at most 128 distinct compound tuples and 1 MiB of encoded keys in total. Regex
members and unsafe/oversized lists fall back. Numeric aliases are deduplicated;
complete equality probes retain their existing preference across indexes.
A sparse compound probe is eligible only when every possible tuple has at least
one nonnull component. Optional probe preparation exceeding its work budget also falls back;
cancellation and integrity errors are not swallowed. There is no native forced-index
hint API; wire read hints are accepted only as documented TinyMongo-compatible no-ops.

Storage selects from the root-shared Ready cache under the request's schema
admission and binds collection/index/key values in SQLite. Candidates preserve
shard-local natural order, validate the entry-to-record checksum binding and then
pass the full matcher. A cursor does not retain index authority between requests:
drop/recreation selects current authority or resumes scanning from the existing
natural-order frontier. This does not add a cross-request snapshot. Shard routing
and the public `Point`/`Scatter` plan remain unchanged; access-method reasons and
row counters, index-only reads, ordering/range pushdown and aggregation pushdown
remain work under #178. No format migration is needed.

Multi-key probes bind every encoded value (including the non-unique fallback
marker) and deduplicate matching entries by the collection's unique natural-order
identity before SQL pagination. A multikey document is returned or mutated once,
even if several array elements match the list. The document-first join preserves
the natural-order range and streams grouping without an all-candidate temporary
sort, including after SQLite statistics are collected. Aggregation still receives its
original unfiltered source rows; membership probes do not bypass its accounting.

After equality and finite membership candidates, a necessary positive
`$exists: true` clause can select all entries of a non-partial sparse Ready index.
Direct fields and positive `$and` clauses qualify; alternatives, negations and
`$exists: false` alone do not. For a compound sparse index, one qualifying indexed
path suffices because sparse membership requires any indexed field to exist.
Explicit null and empty arrays are present, not missing. Non-unique fallback
entries remain included, and the full matcher removes other compound-path or
residual matches. The same document-first, grouped natural-order pagination and
checksum validation apply, without retaining all keys or index rows in memory.
No sparse authority is retained across cursor requests, and mutations maintain
entry membership transactionally when fields are removed.

Necessary direct/positive-conjunction `$exists: false` clauses contribute the
ordinary null key to a complete finite tuple. Missing and explicit null share
that physical key; the full matcher excludes explicit null. Compound equality
or membership constraints can complete the tuple. Possible sparse all-null
tuples still cannot exclude entirely absent records and remain ineligible;
another necessarily nonnull component can establish sparse compound membership.
Uncertain stored paths retain non-unique fallback entries. Public single-equality
inference remains unchanged.

After necessary equality/membership/absence probes, bounded positive `$or`
combinations can supply finite keys before sparse-presence scans. For each index
path, every OR branch must provide a supported equality, literal membership or
absence witness; a conjunction can choose any one necessary witness. AND values
are never intersected, because different array elements can satisfy different
clauses. Nested combinations use one borrowed-value buffer per path, capped at
128 raw operand occurrences across attempted branches (duplicates included;
failed branches do not refund work). Compound paths form conservative Cartesian
supersets; full matching removes cross-branch and null false positives. Existing
128-tuple/1-MiB limits, sparse all-null rejection, partial-index exclusion,
fallback entries and request controls apply. An unbounded OR branch, regex
membership or unsupported value cannot silently disappear from the union.
Public single-equality inference and aggregate source accounting are unchanged.

Necessary string `$gt`/`$gte`/`$lt`/`$lte` predicates can now filter entries of a
single-component Ready index before BSON decoding. Direct fields and positive
ANDs select **one** necessary bound, never intersecting independent array-member
matches. The SQL binds the encoded bound and fallback marker, guards the versioned
single-string frame, and compares only its UTF-8 payload as a BLOB. Length prefixes,
numeric encodings and SQLite text coercion do not define order. NUL, unequal-length
and non-ASCII strings retain the matcher's ordering. Multikey candidates are grouped
before pagination, and fallback records remain included for the full matcher.
Sparse indexes are safe because a string match requires presence; partial indexes
still require the existing independent membership proof. Equality/finite candidates
remain preferred. Numeric and other non-string ranges, compound ranges, unproven
OR/NOT/elemMatch and partial implications retain existing scans/other proven probes.
No index format changes or new JSON shadow representation are required.

This is a **candidate-entry filter**, not an ordered B-tree range seek or index-only
read: SQLite can walk nonmatching entries, while BriskDB avoids fetching/decoding
their BSON. Existing natural-order streaming, checksum checks, cursor-page authority,
request limits, cancellation and atomic index maintenance remain in force. Unit/SQL
properties cover arbitrary Unicode/multikey inputs and no false negatives; native
and real-driver comparisons cover reads, mutations, restart, index churn and
corruption. The manual `string_range_candidate_benchmark` records timing and actual
BSON-examination counts on a same-root scan/index comparison.

Local evidence (2026-09-26, macOS ARM64, Cargo dev profile): three independent
same-root trials, each with 1,000 documents, 4-KiB payloads, four shards, one warmup
and ten measured finds per path. Raw total microseconds:

| Trial | 10-match scan | 10-match indexed | No-match scan | No-match indexed |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1,236,385 | 518,672 | 1,186,448 | 473,400 |
| 2 | 1,323,236 | 516,914 | 1,216,873 | 476,140 |
| 3 | 1,225,570 | 532,212 | 1,230,285 | 489,461 |

Every trial asserted identical result cardinality and reduced BSON examinations
from 10,000 to 100 (selective) or zero (no match). These are local debug-build
measurements demonstrating this candidate-filter benefit, not release throughput,
physical-I/O counts, a cross-platform threshold or a substitute for #185.

Native find, get-more, distinct and aggregation can opt into payload-free
read-plan diagnostics with `DocumentReadOptions::with_plan_diagnostics(true)`
(Python: `plan_diagnostics=True`). `DocumentScatterPlan::read_access()` then
reports `DocumentReadAccess::IndexCandidates` with a numeric index identity,
proof kind and key count (one bound for `StringRange`), or `Scan` with an unfiltered/no-ready-index/
no-safe-probe/probe-work-limit/aggregation-input reason. The selector is shared
with actual reads and runs under that request's schema admission, cancellation
and deadline. It retains no probe authority between cursor pages. Defaults and
exact-ID point plans are unchanged; each continuation opts in separately.
The fixed 32-byte diagnostic charge participates in result limits and page
packing, including sorted/aggregate pages. No BSON keys, filters or index names
are exposed. These are planned access paths, not measured row/shard visits,
SQLite I/O or index-only reads. Aggregation preserves its routed source scan
and pipeline work accounting. Counts and find-and-modify reject this native
option; catalog commands have no data access path. MongoDB `explain` and actual
SQLite-level execution counters remain separate work from these native planner
diagnostics; no Mongo `explain` response or physical-page accounting is implied.

`DocumentReadOptions::with_execution_stats(true)` independently enables native
per-request `DocumentExecution::read_stats()` (`execution_stats=True` /
`result["read_stats"]` in sync/async Python). The payload-free snapshot contains
record-read call counts, BSON documents examined, source-matcher
evaluations, and actual distinct read shards. Exact-ID, pruned-shard, natural,
sorted, distinct and aggregation source reads share the collector. Lookahead
and repeated sorting/source reads count again, including sorted-output refetches
and their matcher rechecks; buffered aggregation pages can
correctly report zero source work. Pipeline predicates, catalog/index-entry
work and physical SQLite rows/pages/bytes are excluded. Each requested page
starts fresh, detaches the collector before cursor retention, and charges a
fixed conservative 160 bytes in output limits/page packing. Unrequested reads
allocate no collector or update counters. Failed/aborted requests return no
snapshot; counters saturate rather than wrap. This does not change filtering,
routing, transaction boundaries or MongoDB wire explain support.

Singleton candidates also pin the document-first join, preventing stale SQLite
statistics from sorting an entire large equality/null-key group for each one-row
frontier. These joins prioritize bounded pagination memory: SQLite can still walk
nonmatching document/index entries even though BSON decoding skips noncandidates.
They are not index-order scans or an index-only read promise.

Required CI checks 1,087 source-locked probe groups (201,349 matcher evaluations,
9,541 eligible matching candidates) without false negatives, plus native and
real-driver scan differentials, residual filters, paging, index churn, write
maintenance, restart and damaged candidate checks. The manual same-root benchmark
is `cargo test --locked --features documents --test document_index_reads
equality_candidate_benchmark -- --ignored --exact --nocapture`; timings are local
measurements, not CI performance assertions.

Update/delete, replacements and find-and-modify use the same candidate helper,
including sorted selection and upsert rechecks. Each shard-local write retains
its existing transaction and full predicate/identity recheck. Multi-document
operations advance by immutable natural order, not mutable index position, so
changing a searched key cannot cause the same record to be updated twice.
Records and derived entries roll back together on the failing shard; earlier
shard commits remain visible as before. Cancellation and task abort preserve
that boundary and release admission after worker cleanup. This adds no unique
constraint, global snapshot or cross-shard atomicity guarantee.

Every record mutation now requires a root/collection/shard-bound
`DocumentWriteTransaction`, including imports and upsert rechecks. With a Ready
unique secondary index, it owns a bounded collection-writer
stripe acquired before `BEGIN IMMEDIATE`. Only the schema-admitted Ready index
cache may request that fence: non-unique indexes and pending unique
declarations do not acquire it. Insert and replacement post-images probe all
shards for conflicting canonical keys before any record mutation. Foreign-shard
probes use dedicated validated read-only connections, not nested pool leases.
The blocking worker retains the fence until SQLite commits or rolls back, even
if its async parent is abandoned. Unproven rollback degrades the root and retains
the fence and degraded root lease until process exit. Cross-process stripes use
at most 256 retained, owner-only lock files in a separate document-write namespace; stripe collisions
only serialize unrelated collections. Contention is cancellable and bounded by
the existing storage busy timeout. Record/index formats and shard-local commit
boundaries are unchanged.

Manifest version 20 fences older writers before unique activation. The existing
entry format covers ordinary, compound, one-level multikey, sparse and partial
unique indexes. Repeated keys within one record do not conflict; BSON numeric
aliases collide, booleans remain distinct, missing and null share a key, and
empty arrays have their own key. Same-owner replacements are legal; deletes
release ownership in the same transaction as record/entry removal. Dropping the
index removes enforcement through the existing recoverable lifecycle.
Offline builds and startup use a private disk-backed SQLite key set with a
bounded cache, not an unbounded collection-wide in-memory map. Startup holds
the relevant writer stripes in deterministic order while checking global keys;
duplicate stored owners are corruption, not an automatic repair opportunity.

This is **per-record/per-shard enforcement**, not globally atomic bulk updates.
Earlier shard/input commits can survive a later conflict. A multi-update whose
eventual post-image is unique can still fail if an intermediate key belongs to
another record (for example, shifting unique values `[1, 2]` to `[2, 3]`). The
locked TinyMongo backends disagree here: the memory backend accepts that shift,
whereas SQLite and same-shard sqlite-sharded reject it and preserve the old image.
The source-locked `test_mongo_write_boundaries.py` records that distinction without
changing the frozen corpus or its allowances. BriskDB retains incremental unique
validation and shard-local rollback, not a new cross-shard coordinator, in line
with the [#74 decision](../ROADMAP.md#cross-shard-transaction-policy--alpha-decision-74).

The #183 process-death matrix terminates immediately before and after **every**
commit in a six-input insert and four-shard update-many/delete-many (28 crash
positions). Inputs deliberately interleave physical shards; one shard is empty
and others have unequal record counts. Reopen checks exact BSON/natural order,
Ready unique and multikey index queries for both removed and surviving keys,
and released writer/pool/schema resources. Stable-ID unordered insert replay
reports duplicate indices for committed inputs; a version-filtered update retry
changes only remaining records; repeated deletion removes only survivors. A
second replay and another reopen verify records and indexes remain consistent.
These are application-controlled reconciliation patterns, **not** Mongo retryable
writes, request-ID deduplication, or a safe replay promise for arbitrary `$inc`.

Existing tests separately cover concurrent independent-process unique writers,
known rollback versus prior-shard commits in native/wire errors, ordered/unordered
insert errors, cancellation and failed commit/rollback. Together they establish
the selected non-atomic contract: successful replies have exact committed counts;
indexed errors only certify their documented scope; command failures or lost
replies never imply global rollback or carry fabricated partial counts. Abrupt
process exit does not simulate power loss or every filesystem fault; broader
filesystem fault injection, soak and release drills remain #68/#185/#187.

`DocumentIndexKeyGenerator` generates ordered compound tuples with at most one final array field, removes
duplicate array entries in encounter order, equates missing with null, and gives
empty arrays a separate identity. Sparse compound membership requires any indexed
field to exist (including explicit null). Partial membership is evaluated before
key extraction; sparse and partial cannot be combined. The partial subset permits
nonempty `$and`/`$or`, equality, ranges, `$in`, `$type`, and `$exists: true`, with
all branches eagerly validated. See MongoDB's [multikey](https://www.mongodb.com/docs/manual/core/indexes/index-types/index-multikey/),
[sparse](https://www.mongodb.com/docs/manual/core/index-sparse/), and
[partial](https://www.mongodb.com/docs/manual/core/index-partial/) index descriptions.

The exact frozen helper subset rejects intermediate array traversal (including
numeric path components), parallel arrays, object/nested-array key values,
ObjectId/date key values, and nonfinite numbers. This is narrower than full MongoDB
indexing; the built-in ID authority is separate. Code-with-scope keeps recursive,
ordered BSON identity. Finite numeric aliases share canonical keys while booleans,
strings, binary subtypes, and code remain distinct. No options are silently degraded.

Non-unique physical indexes do not impose that strict value subset on documents.
Storage preparation uses a record-bound `BDIF` fallback marker for values outside
the equality-token subset, including intermediate/nested/parallel arrays,
objects, ObjectId/date values and nonfinite numbers. A record contributes either
its complete ordinary key set or one fallback marker per affected index, never
a partial key set. Equality candidates include these fallback records in natural
order and validate their entry/record checksums before the full BSON matcher.
This may scan more candidates but cannot discard a possible match. Partial
nonmembers stay excluded; uncertain sparse path membership is conservatively
included. Unique indexes still reject unsupported values. Cancellation, malformed
definitions, corruption and resource limits never become successful fallbacks.
The pure public key/preparation helpers remain strict and source-oracle compatible.
Manifest version 21 fences older writers/readers before these durable markers can
appear. Inserts, updates, replacements, deletes, builds and restart validation
share the same storage preparation; existing `BDIK` keys are not rewritten.

The private Ready-index planner can use partial indexes for equality and bounded
finite/logical candidates when a query proves the membership filter. Proofs use
identical-representation scalar equality or explicit positive existence facts,
composed through positive AND/OR without query expansion. Every query alternative
must establish the required fact; unproven numeric aliases, ranges, type/list
implications and negations keep scans. Proof work shares existing index budgets
and cancellation checks. Candidates still include applicable fallback records,
run the full matcher and require current schema-admitted Ready authority on each
request. This does not change the public `equality_key` helper, durable formats,
the frozen oracle, or the prohibition on combining sparse and partial options.

Generation validates input BSON and has independent per-call limits: 16,384 keys,
8 MiB conservative charge per scalar/scope, 64 MiB cumulative work/retention charge,
and one million traversal/work steps. Duplicate values still consume work. Shared
scalar allocations avoid copying a large compound component into every tuple.
Expanded tuple bytes are still charged, so sharing cannot hide excessive future
hashing or persistence work for a large scalar paired with an array.
Partial definitions are bounded to 1 MiB and 4,096 validation nodes, in addition
to the matcher's own limits. Controlled entry points honor interruption during
compilation, membership, traversal, deduplication and tuple assembly. Errors return
no partial key set and do not mutate input or poison the immutable compiler.
Required CI checks 7,201 cases (29,370 document evaluations) against unchanged
frozen token equality partitions, order, membership and failures. This does not
extend the frozen candidate command coverage or turn pending declarations ready.

Namespace drops share the schema-migration gate and require sole-process
ownership. Preflight errors leave data unchanged; interruption after the durable
deletion intent requires reopen to finish the drop before ordinary operations
resume. See [deletion recovery](DOCUMENT_STORAGE.md#namespace-deletion-and-restart).
Missing targets return false without creating metadata. Dropping the last
collection removes its empty logical database. Monotonic catalog identities
prevent stale find/aggregate cursors from reading a recreated namespace; their
next admitted continuation fails and releases state (or idle expiry cleans it).

An exact `_id` filter, including `{_id: {$eq: value}}`, produces a
`DocumentPlan::Point` with one collection and one physical shard. A safe
`{_id: {$in: [literal, ...]}}` produces a deterministic
`DocumentPlan::Scatter` over only the distinct owning shards. It uses storage's
versioned canonical BSON encoding, so numeric aliases share ownership and
document/array IDs retain exact-value semantics. The complete matcher still
checks candidates; this prunes shards without promising a multi-key index lookup.
The restriction is shared by find/continuations, count, distinct and mutations.
Compound filters and positive `$and` clauses intersect proven exact-ID/list owner
sets; `$or` unions them only when every branch has a proven ID restriction.
These remain matcher-backed scans even with one owner: other conditions still
govern reads, writes and upserts. Canonical-ID work is capped at 1024 values across
the entire filter. Empty/oversized lists, regex members, negations, dotted IDs
and unproven branches provide no restriction. Without another necessary bound,
these scan every shard; empty owner intersections also use the ordinary scan
and matcher rather than introducing an empty-source plan.
Aggregation uses the same shard restriction for a safe first-stage match, while
keeping its original matcher and cumulative accounting in the pipeline. The shared
Rust matcher runs before scatter reads merge by the durable
cross-shard natural-order value, so insertion order remains stable across
restarts. `skip` and `limit` apply once across the whole cursor, after the
merge; `batch_size` bounds each returned page. An initial batch size of zero
opens a cursor without reading documents. Continuations require a positive
batch size and cannot change the original skip or limit.

Initial natural-order merge frontiers now load across at most eight target shards
concurrently, under the existing connection/worker admission limits. Completed
frontiers retain physical shard positions before the same natural-order merge;
empty shards and pruned-owner subsets do not alter output order. A shared checked
byte budget charges each record before publishing its frontier or admitting more
work. In-flight decoding is separately bounded by eight records and the existing
BSON allocation limits. Point reads still use one shard directly. Refilling the
selected natural-order frontier remains sequential. Sorted key-window scans
use the same bounded coordinator and one shared heap, not per-shard heaps.

Native `Count` uses the same eight-child coordinator for independent shard
counts, including owner-pruned `_id` sets. An empty filter uses each shard's
storage count; filtered counts retain the existing full matcher/candidate path.
Only one checked scalar per shard is retained, and global skip/limit applies
once after summation. Exact-ID counts still read their one owner directly.

Child failure, task panic, caller cancellation, deadline or engine shutdown stops
new read admission and drains every started child before returning one error,
without publishing a partial page or count. Peer cancellation uses a local token
and never cancels the caller's potentially shared request/listener token. The owning engine
operation retains its schema/session/lifecycle guards while children drain, even
when the calling task is abandoned. This adds concurrency, not cross-shard or
cross-batch snapshot isolation.

### Field updates and single-record write boundaries

`Update` executes scopes `One` and `Many` with `$set`, `$unset`, `$min`, `$max`,
`$pop`, `$rename`, `$addToSet`, `$pullAll`, `$push`, `$pull`, and `$inc`, using the same
locked selection/reselection and preflighted write path as replacement. A shared
`DocumentUpdater` validates every operator, operand, path, and prefix conflict
before namespace lookup or matching. It bounds specifications to 1 MiB/4,096
operations, paths to 100 components, conservative retained values to 64 MiB,
and traversal to one million steps. Cancellation/deadline checks run during
compilation and application; array extension is charged before allocation.

`$min`/`$max` compare complete BSON values, including null, arrays, nested
documents, and mixed numeric types. They reuse the shared BSON total order, not
query-sort array-element selection. Equal values preserve the stored type and
bytes. Missing fields (including new array slots) receive the candidate even
when it compares above/below null; existing nulls participate in comparison.
Blocked paths fail with code 28 and changed IDs with code 66. Comparison work
has a separate 64-MiB conservative value-traversal budget and shares the million
step/cancellation budget, including no-ops. See Mongo's
[$min](https://www.mongodb.com/docs/manual/reference/operator/update/min/) and
[$max](https://www.mongodb.com/docs/manual/reference/operator/update/max/)
definitions; specification-order field processing still follows the frozen
TinyMongo input rather than claiming MongoDB 5+ lexicographic processing.

`$pop` accepts numeric values exactly -1 (front) or 1 (back), including numeric
BSON aliases but not booleans. It removes one array element without creating
missing paths; empty or absent arrays are no-ops. Non-array targets fail with
code 14, blocked paths with code 28, and invalid operands with code 9. Numeric
array paths are supported. Element traversal is charged before mutation.

`$rename` moves a present field to a different, non-overlapping string path.
Existing destinations are overwritten in place; new destinations append in
specification order. Empty source parents remain. Missing sources are no-ops,
even if the destination would be blocked. Both paths participate in eager
cross-operation conflict checks. Rename can move whole array values but cannot
traverse arrays: numeric array components fail with code 2, nonnumeric ones with
code 28. Self/prefix renames, non-string/NUL destinations, and positional paths
fail eagerly. Present-source moves from/to `_id` or its children fail with code
66 even when the destination value would compare equal. Source cloning and both
paths are bounded, and errors discard the private post-image before any SQL
write. See Mongo's [$pop](https://www.mongodb.com/docs/manual/reference/operator/update/pop/)
and [$rename](https://www.mongodb.com/docs/manual/reference/operator/update/rename/)
definitions; exact field ordering follows the frozen reference.

`$addToSet` adds absent literal values or the individual candidates in `$each`.
It retains existing duplicates, element order, and stored BSON representations;
a plain array is one element. Missing fields become arrays, including an empty
`$each`. `$pullAll` removes every literal match without treating documents as
query expressions; missing targets are no-ops. Both use BSON equality (numeric
aliases equal, booleans distinct, document field order significant) and strict
numeric array paths. Non-array targets and malformed operands/modifiers use code
2. Unsupported modifiers are rejected eagerly even with no matches. Equality
shares the 64-MiB comparison-work and million-step cancellation limits, including
no-ops. Growth is charged before allocation; removal compacts the private array
in order. See Mongo's [$addToSet](https://www.mongodb.com/docs/manual/reference/operator/update/addtoset/)
and [$pullAll](https://www.mongodb.com/docs/manual/reference/operator/update/pullall/)
definitions. Specification/element encounter order follows the frozen contract.

`$push` appends one literal value (including a whole array), or uses `$each` with
optional `$position`, `$sort`, and `$slice`. Processing is always insertion,
stable sorting, then slicing, regardless of modifier field order. Missing targets
become arrays, including empty `$each`; non-array targets and malformed modifiers
fail with code 2. Positions count from the beginning or backward from the old
array's end and clamp at the boundaries. Positive slices keep the prefix,
negative slices keep the suffix, and zero clears the array. Finite integral BSON
numbers are accepted, excluding booleans; huge integers clamp without expansion.
Scalar sort compares whole BSON values. Compound sort supports at most 32 fields
with 100 components each, follows documents only, and treats missing/scalar/array
traversal as null, matching the frozen helper rather than query-sort array
selection. Stable ties preserve stored types and encounter order. A fallible
index merge charges scratch/output slots, comparisons, traversal, and cancellation
before moving values. Temporary growth is bounded even when slicing discards it;
the persisted document-size cap applies to the final post-image. See Mongo's
[$push](https://www.mongodb.com/docs/manual/reference/operator/update/push/)
definition; exact sorting and field-processing behavior follows the frozen input.

`$pull` removes all matching members without creating missing fields. Non-document
conditions use literal BSON equality (a scalar does not implicitly match an array
containing it). Operator documents use the shared query field predicates, including
equality/ranges, `$in`/`$nin`, regex, `$elemMatch`, `$all`, `$size`, `$type`, `$mod`,
and `$exists`. Document conditions use shared dotted-path and logical matching;
embedded `_id` fields are ordinary fields, not collection primary keys. An empty
condition document matches document members only. Top-level `$not` is rejected,
but `$not` in a document field is supported. `$expr` at the condition/logical-clause
level fails with code 224; nested field/element uses fail with code 2. Malformed
conditions are validated eagerly even when the collection/filter/array is empty;
regex error codes 51075/51091/51108 are preserved as update errors.
The matcher borrows members without wrapping or cloning them. Its path-candidate
allocations, literal/range comparisons, regex input/pattern work, and traversal
share the update's growth/comparison/step budgets. Conservative AST and potential
regex-program retention is preflighted before compilation; existing query depth,
node, regex-size/count/backtracking limits still apply. Stable in-place compaction
affects only the private post-image, discarded on any error. See Mongo's
[$pull](https://www.mongodb.com/docs/manual/reference/operator/update/pull/)
definition; exact predicate validation follows the frozen input.

`$inc` eagerly requires numeric operands (not booleans); existing null/nonnumeric
targets fail with code 14. Missing fields/array slots receive the exact operand,
including Int64 width, signed zero and signaling NaN. Existing Int32 sums promote
to Int64 on overflow; any Int64 operand retains Int64, and signed-64-bit overflow
fails atomically with code 2. Double takes precedence over integers; Decimal128
takes precedence over Double. Update Double-to-Decimal promotion uses 15 significant
digits, unlike aggregation's exact binary conversion. Decimal arithmetic shares
the 34-digit half-even clamped context; rounded equal Decimal values retain the
original BID/quantum. Equal Double results retain original bits, including signed
zero. Arithmetic on an existing Double or Decimal NaN counts as modified even when
its stored bytes are identical; newly computed Double NaNs have canonical bits.
These width/overflow/missing/no-op rules follow MongoDB's
[numeric implementation](https://github.com/mongodb/mongo/blob/master/src/mongo/util/safe_num.cpp)
and [arithmetic update node](https://github.com/mongodb/mongo/blob/master/src/mongo/db/update/arithmetic_node.cpp).
Each increment charges 4 KiB of conservative fixed arithmetic workspace before
evaluation, with cancellation checks before/after; ordinary path/growth limits
also apply. This is a logical accounting bound, not a process-RSS claim.

Only changed paths are edited. Untouched fields retain order and exact BSON
types; new fields follow specification order. Missing `$set` parents become
objects. Numeric paths traverse existing arrays (zero-based canonical ASCII
indices); extension fills gaps with null. `$unset` removes object fields or
sets an array slot to null without shifting positions; missing paths are no-ops.
Scalar `$set` parents fail with code 28. Positional paths are unsupported.
Conflicts/empty path components use codes 40/56; changed or removed `_id` uses
code 66. Semantically equal ID aliases retain the original representation.
Operator-assigned zero timestamps remain literal, unlike insert/replacement.
Stored BSON byte comparison determines modified counts, except executed NaN
arithmetic as described above. No-op writes skip SQL.

`DocumentUpdateRequest::with_max_document_bytes` caps the combined post-image,
including retained fields. Result/plan/post-image checks precede SQL, so validation
failures leave the record unchanged. Concurrent updates read the current document
under the write lock rather than applying a stale client-side replacement.
Other operators and secondary-index maintenance remain
unimplemented. Of 30,489 source-locked update oracle cases, the original 4,008
set/unset cases intentionally cover non-ID object paths only: frozen TinyMongo's
legacy scalar/array/ID behavior differs. The 4,719 min/max cases additionally
cover whole-value ordering, numeric array paths, scalar/null path errors,
immutable IDs, and conflicts. Another 3,078 cases cover pop/rename values, paths,
operand errors, missing fields, and identity. The additional 4,440 array-membership
cases exclude ID writes; add-to-set cases use object paths only. Frozen legacy
membership helpers silently restore changed IDs, and add-to-set overwrites
scalar parents/does not implement numeric array paths. Another 4,459 push cases
cover object/array paths, blocked parents, values, fixed modifier
order, numeric boundaries, stable compound sorting, slicing, and invalid operands
without ID writes because frozen push also silently restores IDs. Another 5,573
pull cases cover non-ID object/array paths, literal and query conditions, embedded
IDs, logical clauses, ranges, regex, and eager errors. Frozen pull also restores
collection IDs; independent tests enforce their immutability.
Another 4,212 increment cases compare the exact common semantics on non-ID object
paths, including 2,000 random Double/Decimal promotion cases. Frozen Python shrinks
small Int64 results, produces unencodable wide integers, adds zero to missing
operands, rewrites signed-zero no-ops, and uses legacy path/ID behavior. Those
differences, plus unspecified arithmetic Double NaN bits, are outside that exact
matrix, not coerced or waived. Independent Rust/storage/driver tests enforce the
documented Mongo numeric boundaries, NaN counts, strict paths and immutable IDs.
Across these legacy differences, BriskDB rejects changed
IDs and blocked parents; independent tests cover those stricter boundaries,
without changing frozen corpus/allowances. Independent unit, transaction, and real-wire tests
check resource and commit boundaries. Frozen source, corpus, and intentional-
difference allowances have not been changed.

Scope `Many` streams matching records in natural order within ascending shard
IDs, holding one immediate write transaction per shard. Exact-ID predicates
remain point-routed. Original IDs/order are unchanged, so even a no-op or a
still-matching post-image is visited only once per scan. Counts include every
match and only byte-different writes; the fixed-size result/plan is preflighted
before the first commit. The transformation and post-image checks are shared
with scope `One`, with one current record/post-image retained at a time.

On validation, cancellation, or storage failure, the current shard rolls back;
earlier committed shards remain changed. There is no cross-shard snapshot or
all-or-nothing transaction, nor a claim of MongoDB's individual-document failure
boundary or a uniform collection-wide validation boundary across TinyMongo backends. Successful
final commits are not reclassified by late cancellation. A runtime failure is
certified as having no committed document changes only after an explicit
rollback succeeds and earlier shards reported zero modifications (earlier
no-op matches are allowed). This internal evidence preserves the original error
kind/cause chain. The wire adapter reports certified validation/resource errors
as indexed write errors, stopping ordered batches but allowing unordered ones
to continue. Parsing errors remain indexed as before. Earlier changed shards,
commit/rollback failures, cancellation, and other operational or uncertified
failures still abort the whole command without fabricated partial counts.
Native callers receive an error, not a successful result with guessed counts.
Tests deterministically cover first-shard rollback after provisional writes,
earlier no-op shards, later-shard failure after committed changes, ordered and
unordered continuation, cancellation/task abort, restart, and session/lock reuse.

### Replacement and returned-image boundaries

`Replace` shares the controlled single-record selection/reselection path with
`FindOneAndDelete`. Exact-ID routes use one immediate write transaction. Other
filters scan bounded records in global natural order and reselect the winning
shard under its write lock, retrying if the local winner changed. This is atomic
shard-local selection and replacement, not a cross-shard snapshot. A no-match
returns zero matched/modified counts without creating a document. The original
natural-order identity and stored `_id` representation survive replacement.
An omitted ID is retained; a semantically equal numeric alias is accepted; a
conflicting ID fails with payload-free `DocumentMutationError::ImmutableId`.

The normalized post-image places `_id` first and replaces all other fields.
Only direct non-ID `Timestamp(0, 0)` fields receive server timestamps, matching
insert normalization; nested timestamps and explicit nulls remain unchanged.
Modification counts compare encoded BSON bytes, including type and field order,
not query equality. An identical post-image skips the SQL write. Replacement
documents reject top-level update operators before execution. Replacement upserts
are supported as described below; other non-default write options remain unsupported.

Request/plan/result budgets and normalized post-image size are checked before
commit. `DocumentReplaceRequest::with_max_document_bytes` lets wire adapters
enforce their smaller advertised BSON limit, including a retained ID larger
than the incoming replacement. Validation failure rolls back the local
transaction. Successful commits are not reclassified by late cancellation.
Pending secondary declarations remain non-enforcing. Ready indexes
validate the post-image against the combined index-key bounds and replace their
entries in the same record transaction. Ready unique indexes additionally check
cross-shard ownership under the collection-writer fence described above.

`Replace` with `DocumentWriteOptions::with_upsert(true)` first follows the normal
replacement path. On no match, it inserts a normalized replacement: an explicit
replacement `_id` wins; otherwise a top-level literal or sole `$eq` query `_id`
is retained, even with other query fields; otherwise an ObjectId is generated.
Regex predicates do not supply IDs. Conflicting query/replacement IDs fail with
code 66; BSON-equal aliases retain the replacement's representation. Explicit
null IDs are preserved. Other query fields are not copied into the replacement.
The inserted ID is first; direct zero timestamps receive server values.

An inserted replacement returns `matched_count: 0`, `modified_count: 0`, and
`upserted_id: Some(id)`. Rust and native Python expose `did_upsert`, so an inserted
null ID is distinguishable from no upsert. Native commands require an existing
collection; the wire adapter creates missing namespaces as for inserts. Failed
insert-on-miss attempts can therefore leave an empty collection, but insertion
preparation/result failure never commits a document. Returned IDs reserve two reply-container levels,
and post-image/result limits are checked before natural-order reservation or SQL.

The insertion shard rechecks the original predicate under an immediate write
transaction. Exact-ID plans remain point lookups (including native IDs larger
than the general predicate budget), and concurrent same-ID upserts
update the winner instead of inserting twice. Natural order is reserved outside
the shard lock to preserve manifest/shard lock ordering; losing races may leave
unused order numbers. Non-ID filters do not gain a global snapshot or uniqueness
across shards.

`Update` also accepts `with_upsert(true)` for both one/many scopes. A no-match
search derives a seed from positive direct/`$eq` equality clauses, including
literal embedded documents and nested `$and` clauses. Dotted object paths use
the same bounded, strict path rules as updates. Duplicate/overlapping equality
paths fail with code 54 (`NotSingleValueField`). Dotted `_id` equalities can seed
an embedded identifier; non-equality ID predicates supply no seed values.
Ranges, regex predicates, negations, and alternatives are not copied; advanced
logical simplifications such as singleton `$in`/`$all` are not inferred.
Inference only runs after no match; it does not reject an otherwise valid
matched update. Existing matcher and updater work/retention/growth budgets apply.

The operators run on that seed before missing-ID generation. A query-bound ID
is immutable; `$set` can provide an unbound ID. All zero timestamps remain literal,
including query-seeded values. The result is normalized ID-first and shares the
replacement-upsert identity/result/depth/post-image preflight, natural-order
reservation, target-shard recheck and metadata. Many-scope rechecks update every
new match on the target shard; they do not acquire a cross-shard snapshot.
No-write insertion preparation failures and explicitly rolled-back target-shard
failures may be certified for safe unordered continuation; an initial many-update
failure after earlier shard commits cannot receive that certificate.

Required CI checks 3,544 source-locked upsert executions (886 cases in both count
scopes and both find-and-modify return modes)
against unchanged TinyMongo `_document_for_upsert`, including exact persisted BSON,
metadata and atomic errors for all eleven operators. That matrix covers the common
direct/sole-`$eq`, non-overlapping object-path behavior and small integer increments.
Legacy literal-document/AND inference, scalar-parent overwrite, silent ID restoration,
and numeric differences are outside the matrix, not rewritten or waived. Independent
tests cover strict inference, supplied/generated/null/large IDs, concurrent counters,
limits, native/wire clients and restart.

`DocumentFindOneAndReplaceRequest` wraps a validated `DocumentReplaceRequest`
plus projection/sort read options. It defaults to the before-image;
`with_return_after(true)` selects the post-image. `FindOneAndReplace` returns
`Document(Some(image))` or `Document(None)` without creating a missing document
when upsert is disabled.
Sorting always uses the original stored values; projection only changes the
returned image, never the persisted replacement. No-ops still return the selected
image. Both forms share the same local reselection, ID/natural-order preservation,
and write-option boundaries as `Replace`, including replacement upserts.

The normalized post-image and prepared write are validated before projection of
the selected return image. Exact response size/plan budgets and a reserved
return-envelope nesting level are checked before the SQL write. A response
failure therefore leaves the old document intact, for both return modes. A
projection may make a valid large/deep stored image returnable, but cannot bypass
the normalized post-image cap. The successful commit is not reclassified by late
cancellation. The shared tests cover before/after concurrency, original-field
sorting, projection/storage separation, native depth-100 records, wire response
headroom, no-match/no-op results, and restart.

`DocumentFindOneAndUpdateRequest` wraps an `Update` request with scope `One`
and the same projection/sort and before/after options. It reuses `DocumentUpdater`
and the preflighted returned-image write path rather than replacing unaffected
fields. Sort reads original values, projection affects only the returned image,
and a projected empty document is still a match. No-op updates return the chosen
image; no match returns `Document(None)`. Invalid scopes, unsupported read/write
options, and update specifications fail eagerly. The post-image cap, response
size/depth checks, shard-local reselection, and late-cancellation commit boundary
above apply equally to operator updates, including upserts.

Both find-and-modify requests honor the wrapped write option `with_upsert(true)`.
No-match synthesis follows the replacement/operator rules above. An insertion
returns `DocumentResult::UpsertedDocument(DocumentUpsertedDocument)`, whose
`upserted_id()` retains the exact BSON ID, including null. `document()` is `None`
for the default before-image and the projected inserted document for the after-image.
Matched operations, including projected-empty images, retain the ordinary
`Document(Some(image))` result. Native Python keeps `kind: "document"` and exposes
`did_upsert` plus `upserted_id` on every single-document result; no match and an
inserted null ID are therefore unambiguous without a second read.

The inserted ID and optional image share one result budget, even if projection
excludes the ID or no image is returned. Metadata reserves one reply-container
level; the image reserves its normal envelope level. All checks precede insertion.
Sort controls selection of existing records, not synthesis of an inserted record.
Concurrent same-ID rechecks retain projection/sort and return the actual atomic
mutation image. Native namespaces must already exist; wire upserts may create them.

### Filtered deletion and commit boundaries

`Delete` validates its matcher, options, and fixed-size result/plan budget before
writing. Exact-ID filters retain the direct one-shard path for both scopes,
with an explicit immediate transaction and a control check before commit.
Filtered `One` scans each shard in natural order, retaining only the earliest
candidate identity. It then acquires an immediate write transaction on that
shard and rechecks both natural-order identity and the predicate. Concurrently
deleted, replaced, or recreated candidates trigger reselection rather than a
stale delete. With no concurrent writes, this removes the first globally
inserted match. This does not promise a global concurrent snapshot.

Filtered `Many` visits shards in ascending shard-ID order. Each shard scans and
deletes inside one immediate transaction, reading one record at a time without
materializing the matching set. A failure rolls back that shard; earlier shard
commits remain. Cancellation, deadlines, shutdown, and task abort use the same
worker/lease controls, including CPU-bound matcher checks. A failure or lost
reply can therefore have a partial/unknown outcome; do not assume global
rollback or blindly retry. Successful results report the total committed count.
SQLite leases/transactions are released before returning. Namespace schema
guards exclude concurrent DDL for the admitted shared command. Native Rust and
Python retain their missing-collection precondition; Mongo wire deletes on an
absent collection return zero after eager selector validation, without creating it.

`FindOneAndDelete` accepts projection and sort (not cursor/skip/limit options),
returning `Document(Some(pre_image))` or `Document(None)`. Sort uses original
fields with durable natural order breaking ties. The scatter scan retains only
one candidate identity/sort key and visits one record at a time; BSON comparisons
remain on blocking workers. The winning shard reselects under an immediate
write transaction. A different identity or sort key triggers a fresh global
selection, so another newly better row on that shard is not overlooked. This
makes local selection/deletion atomic, not a global cross-shard snapshot.
Exact-ID routes use one write transaction and still validate runtime sort rules.

The current pre-image is projected and its exact result/plan budget validated
inside the transaction **before deletion**. Invalid filters, sort/projection
errors, and oversized known return values do not delete documents. Shared
document results reserve one nesting level for their return-value envelope;
a depth-limit rejection does not delete data or degrade healthy storage. Mongo wire
also narrows this operation's result budget to fit its advertised BSON limit,
including reply-envelope headroom, before committing. A successful commit is
not changed into a late cancellation error. Transport loss can still leave an
unknown outcome, as with other non-retryable writes. No cursor is retained.

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
documents on the routed shards. A window retains at most 1024 keys and a conservative
64-MiB heap charge across all shards. At most eight admitted shard workers scan
and derive keys concurrently, each under the existing BSON allocation, 8-MiB
key, and derivation-work limits. Heap comparison/updates hold a short shared
mutex only inside blocking workers, never across an await. All children drain
before extracting a window; errors discard it without publishing partial rows.
Selected documents are then refetched in global order. Large skips can span several windows;
memory-bound or window-bound pages may be shorter than the requested batch.
Arrival order may shorten a byte-trimmed page but cannot change its sorted prefix.
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
share one connection lease and worker without changing input order. Every input
owns an explicit immediate shard transaction, with cancellation/deadline checks
before admission and before commit. Ordered batches stop at the first duplicate;
unordered batches continue only after the failed input's rollback succeeds and
return every error's input index alongside successful IDs. Rollback evidence is
local to that input, not a no-changes certificate for the enclosing batch. Begin,
commit and rollback failures stop the command. A successful commit is not changed
into a late cancellation error. A single-document duplicate remains a `UniqueViolation` engine
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
wire reads accept advisory/no-effect hints and opaque comments, simple binary
collation and empty/local read concern through the adapter. They do not alter
the shared extractor or force a native access path; stronger options fail
explicitly. See the [wire option contract](MONGO_PARITY.md).

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
identity now uses the shared group stage below. All frozen basic, projection-stage,
and application aggregation cases pass through the real four-shard endpoint in
both API modes. Required CI verifies exactly 112 executions, with no skipped or
missing cases, using unchanged fixtures and their ordinary drop cleanup. This
does not mark the full Mongo command corpus or group-accumulator suite complete.

### Aggregation groups and numeric accumulators

`$group` supports literal BSON, field references, and computed keys using the
existing `$literal`/`$ifNull`/`$size`, object, and array expressions. Missing keys
become null; missing object members are omitted and missing array elements
become null. Groups retain first
encounter order and the first exact key representation using shared recursive
BSON identity; numeric aliases compare equal while document field order matters.
Keys and accumulator expressions reuse the transformation evaluator without
variables such as `$$REMOVE` or `$$ROOT`; `$literal` can retain those strings as
data. All shapes, output names, expressions, and error precedence
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
Noncanonical finite Decimal128 BID payloads are interpreted as zero, consistent
with [shared BSON identity](BSON.md#equality-representation-equality-hashing-and-order) and MongoDB's BID
arithmetic. Python's `to_decimal()` recovers some malformed payloads differently;
this boundary is covered by explicit Rust vectors, not a claim of reference
agreement on those malformed inputs. Pass-through results retain the raw BID.

Grouping is blocking but retains states, not source documents. The global
natural or preceding sort order feeds the states, so first/last and ordered
numeric addition do not depend on physical shard boundaries or cursor batches.
Rounded shard-local totals are **not** merged because rounding is not associative.
Group keys/states/output and
per-row expression allocation share conservative 64 MiB working bounds,
reduced by unconsumed rows already retained upstream. Specifications are capped
at 1 MiB/4,096 charged syntax/path nodes. Pipeline row/work/cancellation limits
remain cumulative. Every completed group document is BSON-size/depth validated
before any result is delivered, even if a later limit/project would shrink it.
Failures poison the execution and release its cursor without partial group
results. No disk spill, indexed grouping, or snapshot is promised.

Pipelines whose first stage is `$group` use exact shard-local partial states when
every accumulator is an integer-literal `$sum` or `$first`/`$last`/`$min`/`$max`.
The existing expression evaluator still handles the group key and operands.
Integer totals merge in i128 and select their BSON result type only after the
final merge. Group order and key representation use the earliest global source
position; first/last use the corresponding positions, and equal min/max values
retain the latest representation. Completion order therefore cannot alter any
of these BSON results. Remaining pipeline stages execute only after global merge.

Up to eight admitted shard workers scan concurrently. One shared budget accounts
for every active/completed partial and the final merge: 65,536 consumed rows,
four million work checkpoints, and 64 MiB of conservatively charged aggregation
state/working allocation. There is no fresh quota per shard. At most eight source
BSON decodes can be in flight separately, each under the existing BSON decode
limits. State accounting includes conversion-container capacity; BSON payloads
move without cloning during merge. Cancellation/failure drains all children
before the operation releases schema/session guards or returns an error. A zero
first batch defers execution, and normal cursor/result budgets still apply.

Dynamic or noninteger sums, averages, push/addToSet, and any preceding pipeline
stage select the original globally ordered stream. This conservative eligibility
is not a loss of query support: it avoids reordering floating/Decimal arithmetic,
array encounter order, or an earlier match/skip/limit/sort/transform. Partial
state duplication can reach the bounded memory ceiling earlier than a single
state map; neither executor promises unbounded grouping or spilling.

Required CI covers 9,509 accumulator pipelines and 5,663 additional key pipelines,
each tested in both execution modes. Of these, 24 and 5,400 respectively use
explicit composition: the unchanged frozen `$set` evaluator calculates a key
into a collision-free temporary field before its unchanged field-key `$group`.
Rust executes the original pipeline; both forms are retained in each case. This
checks expression/group composition, not support for the extended key grammar
in the frozen implementation. All other cases execute identical pipelines.
Comparisons retain exact BSON except arithmetic Double NaN
bits in explicitly tagged numeric output fields; input/pass-through NaNs remain
byte-for-byte comparisons. The unchanged reference produces all expectations;
unencodable integer totals are separate Rust edge tests, not coerced oracle
outputs. Tests also cover structured identity, stage ordering, numeric quantum,
resource limits, every cancellation/deadline checkpoint, cursor cleanup,
cross-shard byte paging, sync/async native and wire clients, and restart. Exact-BSON
partial-versus-stream properties cover 1–64 partitions, reordered completion,
numeric/structured key aliases and extremum ties. Native worker tests cover
bounded waves, failed admission, cancellation, caller abort and resource reuse.
The full frozen 456-execution command corpus is required independently; broader
source-inventory coverage and release acceptance remain separate work.

PyMongo `count_documents()` now works through its actual `$match`, optional
`$skip`/`$limit`, and constant-key `$group` pipeline, without adapter rewrites.
Sync/async calls, absent namespaces, filtering, eager option errors, and restart
are tested. It inherits aggregation's consumed-row/work bounds; unlike the
legacy count command, explicit `limit=0` is an invalid `$limit` (15958).
Native Python's count helper continues to use the separate engine count command.

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
and deadlines. A first-stage `$match` with a sole exact `_id`/`$eq` uses a point
route; proven compound, positive-AND, bounded-OR and literal-list ID constraints
use only their selected owning shards. The entire
pipeline is validated first and every original stage stays in place. Subset routes
deliver unfiltered source rows from those shards into the original aggregation
matcher, so unmatched inputs still consume its cumulative input/work budgets.
No match is moved across a preceding transform, skip, limit, or other stage.
Unproven shapes keep full-shard scans. General predicate/index pushdown and more
efficient frontier reuse remain later optimizations. Repeated frontier probes
can increase read work. No cross-shard
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

Other update operators,
additional aggregation expressions/group-key forms,
database statistics, whole-bulk unique post-image semantics and
broader index-backed query plans
remain later roadmap work. Collection and Ready-index metadata cursors are implemented.
Unsupported command shapes return the stable `EngineErrorKind::Unsupported`
category.

The opt-in Mongo listener and embedded document adapters translate into this
engine boundary instead of implementing routing or storage behavior themselves.
See [the Mongo parity contract](MONGO_PARITY.md),
[the BSON contract](BSON.md), and [document storage](DOCUMENT_STORAGE.md) for
the adjacent contracts.
