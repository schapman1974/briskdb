# Experimental sharded virtual-table facade

BriskDB's no-fork direction is a statically registered SQLite virtual-table
module named `brisk_shard`. The prototype is compiled only with the
`experimental-vtab` Cargo feature and does not replace the current logical
scatter/gather executor.

The coordinator is a separate SQLite connection. It contains one virtual table
for each catalog-authoritative `Sharded` or `Global` application table:

- a `Sharded` cursor visits the validated physical shard files in shard order;
- a `Global` cursor visits canonical shard 0 once, even though the data is
  replicated physically; and
- `Catalog` placement is never declared in the coordinator schema.

Normal SQLite SQL runs above those tables. The prototype therefore proves that
filtering, ordering, aggregates, limits, joins, and other SQLite operators can
have global semantics without teaching a frontend about shard files. Constraint
routing and pushdown are intentionally deferred to issue #126; the initial
`xBestIndex` consumes no predicates, so SQLite rechecks every condition.

## Ownership and lifecycle

The coordinator never `ATTACH`es a shard. It registers `brisk_shard` through
rusqlite's compiled virtual-table API and passes an immutable registry as module
auxiliary data. SQLite calls the module synchronously on the coordinator
connection's owning thread. The production prototype uses an ephemeral
coordinator; a file-backed coordinator exists only in lifecycle tests and
rejects every shadowing or foreign schema object before creating anything.

Each cursor:

1. acquires an owned BriskDB schema-operation guard;
2. verifies that the coordinator's declared schema generation is still current;
3. opens one child shard through `Storage::open_shard`, retaining all identity,
   schema-digest, authorizer, and fail-closed checks;
4. marks that private child connection query-only and materializes a bounded
   shard batch while preserving all five SQLite storage classes, non-UTF-8
   `TEXT` bytes, column affinity, and declared collation;
5. closes the child before advancing to the next file; and
6. releases schema admission at EOF, error, cancellation, or cursor close.

No child `Connection`, `Statement`, or `Rows` object crosses a callback or
thread boundary. A cursor is not self-referential, and the coordinator is never
called recursively from a child scan. The spike bounds a complete cursor to
65,536 rows and a conservative 64 MiB allocation budget. It checks cell sizes
before copying payloads, uses fallible allocation, and discards an exhausted
shard batch before allocating the next. Later streaming work must preserve
operation-wide result budgets while replacing this bounded materialization.

A cancellation handle increments a scan epoch observed by child progress
handlers. It interrupts the coordinator only while a child callback is active;
the same mutex that publishes active child handles prevents a late interrupt
from reaching the next query. Cancellation of outer SQLite work after every
child callback has completed belongs with the future request-scoped query API.

The module uses `SQLITE_VTAB_DIRECTONLY` and rusqlite's read-only module table,
which has no `xUpdate`. The coordinator also enables `query_only` and disables
trusted schema execution after bootstrap. A fail-closed authorizer denies DML,
DDL, `ATTACH`, `DETACH`, unsafe PRAGMAs, and the SQL `load_extension` function,
including attempts to turn `query_only` back off. The Cargo feature adds only
rusqlite's virtual-table bindings; it does not add loader bindings or enable the
bundled SQLite library's disabled-by-default extension-loading capability.

## Why the physical format stays ordinary SQLite

All virtual objects live in the coordinator database. A physical shard still
contains the same application tables and BriskDB metadata it contained before
the feature was enabled. Stock SQLite can open and inspect each file, and the
manifest format is unchanged.

This is why several alternatives are excluded:

- a VFS can redirect pages and locks, but it cannot turn multiple independent
  databases into one SQL table or create multiple SQLite writers to one file;
- a side-effecting `DEFAULT` function cannot override SQLite's special rowid
  allocation rules and would allocate identifiers before statement success;
- runtime loadable extensions add deployment and trust boundaries that a
  statically linked Rust module does not need; and
- an SQLite fork would make every upstream security and correctness update a
  permanent merge obligation while still leaving BriskDB-specific routing and
  metadata policy to implement.

## Current non-goals

This boundary does not yet provide writes, generated IDs, distributed
transactions, predicate routing, aggregate pushdown, parallel shard scans, or a
public query API. It does not change protocol behavior. Those capabilities are
split across issues #125 through #131 so each can be tested and reviewed without
silently replacing the established executor.

Because this spike consumes no `xBestIndex` constraints, even a point predicate
or `LIMIT 1` materializes each reached shard before SQLite applies the outer
operation. A shard over the prototype budget can therefore fail a small-looking
query. Separate child connections also observe independent SQLite snapshots, so
the facade does not yet promise one cross-shard snapshot while writers commit.
It must not be wired to the query app until routing, streaming, request controls,
and consistency policy are completed and gated in #126 and #131.
