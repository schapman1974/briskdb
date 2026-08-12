# Experimental sharded virtual-table facade

BriskDB's no-fork direction is a statically registered SQLite virtual-table
module named `brisk_shard`. The prototype is compiled only with the
`experimental-vtab` Cargo feature and does not replace the current logical
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
under that policy uses the ordinary canonical Int64 hash route instead. No ID
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

This boundary does not provide writes, generated-ID allocation, distributed
transactions, aggregate/order/limit/join pushdown, parallel shard scans, or a
public query API. It does not change protocol behavior. The current `Engine`,
HTTP surface, and protocol planner remain authoritative. Advanced filter,
aggregate, order, and limit pushdown remains owned by issues #58 through #61.
Issues #127 through #131 separately cover explicit-key writes, native and
optional hi/lo generated IDs, generated-key SQL integration, and the final
rollout gate.

Only the exact typed shard-key equality described above narrows a scan. Other
predicates, and a type-mismatched equality whose SQLite affinity semantics
might still match, materialize every reached shard before SQLite applies the
outer operation. A shard over the prototype budget can therefore fail a
small-looking aggregate or limited query. Separate child connections also
observe independent SQLite snapshots, so the facade does not promise one
cross-shard snapshot while writers commit. It must not be wired to the query
app until streaming, request controls, consistency policy, and the rollout gate
in #131 are complete.
