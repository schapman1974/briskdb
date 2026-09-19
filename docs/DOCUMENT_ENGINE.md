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
| `ListCollections` | Returns collection metadata for one exact database name |
| `CreateIndex` | Declares index metadata and returns its name; non-built-in indexes remain pending until physical index work lands |
| `ListIndexes` | Returns the built-in `_id_` definition and declared secondary-index metadata |
| `Insert` | Inserts ordered/unordered batches; generates missing ObjectIds, preserves explicit null IDs, and reports safe per-input duplicate failures |
| `Find` | Evaluates BSON match expressions and returns one exhausted cursor batch |
| `Count` | Evaluates the same match expressions, then applies global skip/limit |
| `Delete` | Deletes one document selected by exact `_id` |

An exact `_id` filter, including `{_id: {$eq: value}}`, produces a
`DocumentPlan::Point` with one collection and one physical shard. Other filters
produce a deterministic `DocumentPlan::Scatter` over every shard. The shared
Rust matcher runs before scatter reads merge by the durable
cross-shard natural-order value, so insertion order remains stable across
restarts. `skip`, `limit`, and `batch_size` are applied after that merge. A
result that would require cursor continuation currently fails unless the caller
uses a limit that fits in one batch.

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

## Current boundary

Projection, sort, update expressions, replacements,
multi-document deletion, upsert, distinct, aggregation, retained
cursors, and physical secondary-index builds remain later roadmap work.
Unsupported command shapes return the stable `EngineErrorKind::Unsupported`
category.

The opt-in Mongo listener and embedded document adapters translate into this
engine boundary instead of implementing routing or storage behavior themselves.
See [the Mongo parity contract](MONGO_PARITY.md),
[the BSON contract](BSON.md), and [document storage](DOCUMENT_STORAGE.md) for
the adjacent contracts.
