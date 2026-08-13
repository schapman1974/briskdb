# Experimental sharded virtual-table facade

BriskDB's no-fork direction is a statically registered SQLite virtual-table
module named `brisk_shard`. The prototype is compiled only with the
`experimental-vtab` Cargo feature. Its writable coordinator has a separate
runtime gate; its read-only coordinator does not replace the current logical
scatter/gather executor.

The coordinator is a separate, ephemeral stock-SQLite connection. It contains
one virtual table for each catalog-authoritative `Sharded` or `Global`
application table. `xConnect` declares the exact column schema derived from
trusted, already-validated catalog and shard metadata; submitted SQL never
supplies a shard filename, table declaration, or module argument.

The read contract is:

- a `Sharded` scan visits the validated physical shard files in ascending
  shard order and preserves duplicates as deterministic `UNION ALL`;
- a `Global` cursor visits canonical shard 0 once, even though the data is
  replicated physically; and
- `Catalog` placement is never declared in the coordinator schema.

Issue #127 adds a separate internal writable coordinator. Its explicit-key
contract is deliberately narrower:

- `INSERT` routes from an exact catalog shard-key value and returns both the
  SQLite affected-row count and the caller's explicit key to BriskDB;
- `UPDATE` and `DELETE` require an exact shard-key equality, read the selected
  row through the pinned writable child transaction, and identify it with a
  hidden, versioned, table-bound locator containing the shard and physical
  rowid or complete `WITHOUT ROWID` primary key;
- the first read or write pins the transaction to one validated physical shard,
  and any attempt to enlist a second shard or move a shard key aborts the whole
  logical transaction without a partial durable effect; and
- explicit transactions, nested savepoints, rollback, SQLite conflict modes,
  cancellation, connection loss, and child commit failures are reconciled by a
  wrapper that never exposes the raw writable coordinator connection.

Cancellation and child commit have one explicit linearization boundary. If
cancellation claims that decision first, the child rolls back. Once finalization
claims the commit decision, it wins and is allowed to finish, so BriskDB never
reports a cancelled result for a write that may already be durable.

## Engine and HTTP opt-in

Compiling `experimental-vtab` makes the coordinator available but does not
change execution by itself. The runtime default remains off. Rust embedders
enable it with `EngineOptions::with_experimental_vtab_writes(true)`; the server
maps `--experimental-vtab-writes` and
`BRISKDB_EXPERIMENTAL_VTAB_WRITES=true` to that same option.

With both gates enabled, `Engine::execute`, `Engine::execute_write`, and HTTP
`/v1/execute` dispatch a write through the coordinator only after the
authoritative catalog, common SQL frontend, bound values, routing policy, and
generated-key policy have accepted either one explicit Sharded target or one
omitted-key allocator intent. For an explicit key, returned `shard` remains the
planner's assigned owner; for an omitted key, it is the allocator's actual
owner. `rows_affected` comes from the reconciled physical child, and a
successful response is produced only after the autocommit child transaction
commits under BriskDB's configured SQLite WAL and synchronous policy. Existing
HTTP responses retain their shape; only a generated insert adds the optional
`generated_key` object documented in [generated keys](GENERATED_KEYS.md).

The Engine caches the validated physical table descriptors for the current
schema generation. One serialized cold bootstrap discovers them through a
cancellable, shard-0-capacity-accounted read-only handle. Warm coordinator
opens reuse those immutable descriptors. Explicit-key DML reserves only its
already planned target shard, preserving independent-shard writer progress.
Public native generated writes do not preselect an exact owner in Engine. The
writable registry builds a per-table candidate list whose start rotates with a
round-robin cursor. After its worker starts, Engine attempts a non-waiting
bounded-pool reservation for one candidate at a time; a busy candidate is
skipped without queuing, and its permit is never held alongside another
candidate's. Hi/lo learns its hash-routed owner only after consuming a lease,
so the runner instead reserves capacity on every shard in stable order before
entering the coordinator. Schema-generation publication invalidates the cache
and makes the next write rediscover descriptors.

For a target table declaring `native_range_v1`, an explicit valid native ID is
routed through its persisted allocation owner at planning, pool admission,
coordinator execution, and returned-shard reporting. Marker-clear and negative
legacy IDs retain the exact ordinary Int64 hash route. A reserved sequence
floor or an owner absent from the active map is rejected before mutation.
For `hilo_v1`, negative and positive values below
`0x2000_0000_0000_0000` remain explicit legacy IDs and use that ordinary hash
route. Every value at or above the marker belongs to a generated-ID namespace
and a caller-authored INSERT is rejected; only the allocator may introduce a
hi/lo ID.

The coordinator also has the narrow generated-insert seam consumed by issue
#130's planner. After the shared AST and catalog policy prove exactly one row
whose generated column is absent from the column list, execution arms exactly
one callback. For `native_range_v1`, the registry visits active owners from its
per-table rotating start. Once one candidate has immediate pool capacity, the
coordinator pins that child and checks its `sqlite_sequence` range under the
child lock. An exhausted candidate is released without mutation before another
candidate is admitted. The first active, immediately admitted, non-exhausted
owner uses `INSERT ... RETURNING id`; ownership and sequence are validated on
that same handle, and the generated key is retained only after successful
reconciliation. For `hilo_v1`, the coordinator first irrevocably consumes an
ID from a durable manifest-leased block, hashes that encoded value to its target
shard, inserts it explicitly with `RETURNING`, and verifies the returned value.
Leasing occurs before any target-shard write lock, but only after Engine has
reserved every possible target's pool capacity. A transaction that already
pinned a child therefore rejects later hi/lo generation instead of moving to
another shard. `Engine::execute_write`, prepared portal execution, and HTTP
`/v1/execute` carry the same protocol-neutral result. The exact syntax, result
shape, and gates are in [generated keys](GENERATED_KEYS.md).

Each hi/lo manifest write reserves 4,096 global per-table sequence values and
records a monotonic fence plus a random 32-byte process incarnation. There is no
clock, expiry, or lease reclamation. The fence identifies one committed block;
it does not invalidate IDs from an older block. A restart or crash abandons the
unconsumed tail, and an ID consumed before a rollback, cancellation, ignored
insert, or constraint failure is never returned. Gaps are therefore normal.
Numeric ID order is allocation order, not transaction commit order, and the
contract does not promise a gapless sequence or global commit ordering.

The integration opens an ephemeral coordinator for one statement. It does not
retain a coordinator in `Session`, and it does not add `BEGIN`, `COMMIT`,
`ROLLBACK`, read-your-writes, or any transaction spanning requests. Those need
the later protocol-neutral transaction state machine and shard-pinning policy.
Prepared portals remain logical cached objects rather than coordinator handles;
a generated portal opens a fresh coordinator for each execution and returns
`PreparedExecution::GeneratedWrite`.

Reads deliberately stay on the established paths. Engine logical reads and
HTTP `/v1/query` still select physical targets from catalog metadata and use
the bounded scatter/gather executor; the admin browser keeps its specialized
logical count and page readers. The internal read-only virtual-table facade is
not exposed to those surfaces yet.

For a usable equality constraint on the exact declared shard-key column,
`xBestIndex` requests one argument but deliberately does not set `omit`.
`xFilter` prunes to one physical child only when the bound SQLite storage class
exactly matches the catalog key type: `INTEGER` for `Int64`, valid UTF-8 `TEXT`
for `Text`, or `BLOB` for `Binary`. The child statement binds that same value to
`WHERE <physical-shard-key> = ?1`, allowing ordinary per-file SQLite indexes to
serve the lookup. The coordinator rechecks the predicate because the virtual
table did not claim omission.

An equality argument of `NULL` produces an empty cursor. A non-null storage
class mismatch is not a routing proof: it conservatively visits every normal
target and leaves SQLite to apply its affinity and comparison rules. With
`native_range_v1`, a valid encoded native integer maps its immutable owner slot
through the catalog's allocation-owner map. An integer classified as legacy
under that policy uses the ordinary canonical Int64 hash route instead. Under
`hilo_v1`, valid hi/lo IDs and accepted pre-marker legacy IDs both use that
canonical hash route; reserved or incompatible namespaces fail closed. No ID
allocation occurs on this read path.

Normal SQLite evaluates remaining filtering, aggregation, ordering, limits, and
feature-local joins above the virtual table. None of those operations is pushed
down, so every row reached by the routed or full scan still counts against the
facade's bounded cursor budgets before an aggregate, `LIMIT`, or join can reduce
the result. This is delegation to stock SQLite inside the experimental
coordinator, not an expansion of BriskDB's supported Engine or protocol SQL
surface. Advanced multi-shard forms rejected or delegated by that existing
surface remain rejected or delegated there.

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
3. opens the next selected child shard through a validated OS-level SQLite
   `READ_ONLY | NOFOLLOW` handle, retaining all identity, schema-digest,
   authorizer, and fail-closed checks;
4. also marks that private child connection query-only, optionally binds the
   exact shard-key equality to its indexed physical column, and materializes a
   bounded shard batch while preserving all five SQLite storage classes,
   non-UTF-8 `TEXT` result bytes, column affinity, and declared collation;
5. closes the child before advancing to the next file; and
6. releases schema admission at EOF, error, cancellation, or cursor close.

No child `Connection`, `Statement`, or `Rows` object crosses a callback or
thread boundary. A cursor is not self-referential, and the coordinator is never
called recursively from a child scan. The spike bounds a complete cursor to
65,536 rows and a conservative 64 MiB allocation budget. It checks cell sizes
before copying payloads, charges an owned equality argument to the same budget,
uses fallible result allocation, and discards an exhausted shard batch before
allocating the next. Later streaming work must preserve
operation-wide result budgets while replacing this bounded materialization.

A cancellation handle increments a scan epoch observed by child progress
handlers and by `xNext` before it serves another materialized row. It always
interrupts the coordinator as well, covering stock-SQLite filtering, sorting,
aggregation, and join work after a child handle has closed; SQLite specifies
that an interrupt issued while idle does not affect statements started later.
The epoch is rechecked after every child, including an empty child that did not
invoke the progress hook. Cancellation releases both the child and schema guard,
and the coordinator can be reused because cancellation is scoped to one scan
epoch.

The module uses `SQLITE_VTAB_DIRECTONLY` and rusqlite's read-only module table,
which has no `xUpdate`. Every physical child is opened with SQLite's OS-level
read-only flag, then also receives `query_only`; the coordinator disables
trusted schema execution after bootstrap. A fail-closed authorizer denies DML,
DDL, `ATTACH`, `DETACH`, unsafe PRAGMAs, and the SQL `load_extension` function,
including attempts to turn `query_only` back off. The Cargo feature adds only
rusqlite's virtual-table bindings; it does not add loader bindings or enable the
bundled SQLite library's disabled-by-default extension-loading capability.

The writable coordinator instead registers a stock-SQLite version-2 module.
rusqlite supplies the normal `xUpdate` and transaction callbacks; a small
in-tree bridge fills SQLite's public `xSavepoint`, `xRelease`, and
`xRollbackTo` slots. This is an API adapter, not an SQLite patch or fork. The
writable coordinator enables virtual-table constraint support, leaves
`query_only` off only on itself, and opens exactly one validated writable child
with `BEGIN IMMEDIATE`, `synchronous=FULL`, WAL, and `foreign_keys=ON` verified.
Its fail-closed authorizer permits top-level DML only against registered virtual
tables. DDL, `ATTACH`, `DETACH`, caller transaction SQL, unsafe PRAGMAs,
indirect trigger/view execution, and extension loading remain denied.

All physical SQLite constraints and indexes remain authoritative. Registration
accepts a local foreign key only when placement proves co-location: a Sharded
child may reference a co-sharded parent through corresponding shard keys in the
same generated-ID routing domain or a Global parent, and a Global child may
reference only a Global parent. Unsafe, missing, Catalog, cross-placement, or
SQLite-unenforceable relationships fail closed. Physical
`UNIQUE`, primary-key, `NOT NULL`, `CHECK`, strict typing, and admitted local
foreign-key errors retain their SQLite result codes through the facade.

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

The writable coordinator provides explicit-key DML and a preflighted
single-row generated allocation seam that mutates exactly one pinned Sharded
child. Native selection may pin and release earlier unmutated exhausted
candidates, but never retains or writes two children. Opt-in Engine,
prepared-portal, and HTTP execution expose that exact omitted-key shape. They do
not provide arbitrary missing-key/default behavior, generated
multi-row inserts, replicated Global writes, multi-shard or session
transactions, caller-authored
`RETURNING`, physical defaults, generated columns, physical triggers,
client-created virtual-table indexes or triggers, `ALTER TABLE`,
aggregate/order/limit/join pushdown, parallel shard scans, or a public
virtual-table query API. The physical-default restriction is intentional:
SQLite's `xUpdate` arguments do not distinguish an omitted column from explicit
`NULL`, so only an AST-preflighted intent may enable allocation. Omitted-key SQL
is limited to the issue #130 contract. Issue #131 completed the broader
[rollout gate](VTAB_ROLLOUT.md) with a hold decision: the facade remains
experimental and off by default.

The established protocol planner stays authoritative. The writable coordinator
is kept behind both `experimental-vtab` and the runtime option and cannot be
reached through a raw connection. Additional indexes continue to live on the
ordinary physical tables and are used by child SQLite statements; SQLite does
not permit adding indexes or triggers directly to a virtual table. Schema
changes remain the journaled migration system's responsibility.

Only the exact typed shard-key equality described above narrows a scan. Other
predicates, and a type-mismatched equality whose SQLite affinity semantics
might still match, materialize every reached shard before SQLite applies the
outer operation. A shard over the prototype budget can therefore fail a
small-looking aggregate or limited query. Separate child connections also
observe independent SQLite snapshots, so the facade does not promise one
cross-shard snapshot while writers commit. The issue #131 gate therefore did
not approve wiring it to the query app; streaming, request controls, a
consistency policy, measured performance, and live wire-protocol parity remain
required by [the decision record](VTAB_ROLLOUT.md).
