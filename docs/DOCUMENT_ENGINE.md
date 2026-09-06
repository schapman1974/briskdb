# Protocol-neutral document engine

Status: implemented by roadmap issue #162

The opt-in `documents` feature adds an asynchronous document command boundary
to `core::Engine`. A caller submits an owned `DocumentRequest` to
`Engine::execute_document`; no HTTP, PostgreSQL, MongoDB, or other network
listener participates. This is the shared execution boundary for future Rust,
Python, and MongoDB adapters.

Every request carries a nonzero 128-bit request identity and the same
`RequestContext` controls as SQL operations: cancellation, an absolute
deadline, and result limits that may narrow the engine defaults. The returned
`DocumentExecution` echoes the request identity and, for routed data commands,
the selected point or scatter plan. Reusing an identity helps correlate logs;
it does not make a write idempotent.

## Implemented commands

This first engine slice executes:

| Command | Current behavior |
| --- | --- |
| `CreateCollection` | Provisions the catalog entry and fixed document table on every shard, then returns the active collection metadata |
| `ListCollections` | Returns collection metadata for one exact database name |
| `CreateIndex` | Declares index metadata and returns its name; non-built-in indexes remain pending until physical index work lands |
| `ListIndexes` | Returns the built-in `_id_` definition and declared secondary-index metadata |
| `Insert` | Inserts one document containing an explicit `_id`; the typed request retains batch shape for later bulk-write semantics |
| `Find` | Supports an empty filter or an exact top-level `{_id: value}` filter and returns one exhausted cursor batch |
| `Count` | Supports an empty filter or an exact top-level `{_id: value}` filter |
| `Delete` | Deletes one document selected by exact `_id` |

An exact `_id` filter produces a `DocumentPlan::Point` with one collection and
one physical shard. An empty filter produces a deterministic
`DocumentPlan::Scatter` over every shard. Scatter reads merge by the durable
cross-shard natural-order value, so insertion order remains stable across
restarts. `skip`, `limit`, and `batch_size` are applied after that merge. A
result that would require cursor continuation currently fails unless the caller
uses a limit that fits in one batch.

The engine preserves BSON field order and exact BSON representations in stored
and returned documents. It accounts returned rows and encoded BSON bytes
against the effective `ResultLimits`; exceeding either bound fails the whole
command rather than returning a partial batch.

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

General match expressions, projection, sort, update expressions, replacements,
multi-document insertion or deletion, upsert, distinct, aggregation, retained
cursors, and physical secondary-index builds remain later roadmap work.
Unsupported command shapes return the stable `EngineErrorKind::Unsupported`
category. Inserts require caller-supplied `_id` values until generated document
IDs land.

This release does not add a MongoDB listener or the higher-level embedded Rust
and Python collection APIs. Those adapters will translate into this engine
boundary instead of implementing routing or storage behavior themselves. See
[the Mongo parity contract](MONGO_PARITY.md), [the BSON contract](BSON.md), and
[document storage](DOCUMENT_STORAGE.md) for the adjacent contracts.
