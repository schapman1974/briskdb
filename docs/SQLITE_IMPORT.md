# Standard SQLite import

Status: implemented by roadmap issue #115

`briskdb-import` converts one ordinary SQLite database into a new BriskDB data
directory. It is an offline initialization command, not a server endpoint and
not an in-place conversion. The source is opened read-only, the destination
must not exist, and no running BriskDB process may use the destination while it
is being published.

```bash
cargo run --bin briskdb-import -- \
  --source /path/to/source.db \
  --data-dir /path/to/new-briskdb-data \
  --shards 4 \
  --plan /path/to/import-plan.json
```

The command prints the verified import report as JSON after publication. If
that final output stream is closed, it reports the already-committed destination
on standard error and still exits successfully so a completed import is not
mistaken for a retryable failure.

## Explicit placement plan

The plan must declare every ordinary source table exactly once. It never treats
a table as replicated because it is small. `Sharded` sends each row to exactly
one owner using the committed BriskDB key encoding and virtual-bucket map.
`Global` is the only replicated placement and copies every row to every shard
because the plan explicitly requested it.

```json
{
  "version": 1,
  "tables": [
    {
      "name": "orders",
      "placement": "sharded",
      "shard_key": {
        "strategy": "column",
        "column": "id",
        "key_type": "int64"
      }
    },
    {
      "name": "countries",
      "placement": "global"
    }
  ]
}
```

A Sharded declaration may omit `shard_key`; that is shorthand for
`{"strategy":"primary_key"}` and succeeds only for one physically non-null,
supported primary-key column that also satisfies the authoritative uniqueness
rules. Composite, nullable legacy, incompatible, or nonlocal unique-key shapes
fail preflight. An explicit key uses one of `int64`, `text`, or `binary`.
Runtime values must have the matching SQLite storage class, and Text must be
valid UTF-8 with SQLite `BINARY` collation. The importer does not cast keys.

Every primary or unique key on a Sharded table must contain the shard key. If
two independent unique domains have no common column, preserving those
constraints requires explicit Global placement or a separate schema redesign;
the importer does not weaken uniqueness silently.

## Foreign-key normalization

Authoritative foreign-key placement is not implemented yet. A source table
with any foreign key is rejected by default before a staging directory is
created. A plan may explicitly select `"foreign_keys":"omit"` for that table:

```json
{
  "name": "legacy_children",
  "placement": "sharded",
  "shard_key": {
    "strategy": "column",
    "column": "id",
    "key_type": "int64"
  },
  "foreign_keys": "omit"
}
```

That policy removes only supported table-level foreign-key clauses from the
staged `CREATE TABLE`; it does not change a source row. Every omitted clause is
recorded in `briskdb-import-receipt.json` and in the returned report. Inline or
otherwise unprovable rewrites are rejected. This is an explicit schema
normalization, not a claim that the imported schema is byte-for-byte identical.

The checked-in [`LARGE_Data` plan](../examples/LARGE_Data.import.json) uses this
policy for five legacy constraints. Four point at missing `__old_*` tables and
the source already reports foreign-key violations. The plan also marks tables
with independent unique keys Global. `cb_accounts` is Sharded by `id`, so its
three logical rows have three total Sharded physical rows rather than one copy
per shard.

## Snapshot, copy, and verification

Import holds one SQLite read transaction for a consistent source snapshot. It
checks source integrity, schema object support, exact plan coverage, declared
key structure, every key value, indexes, and `sqlite_sequence` before creating
staging. The source file identity is retained and rechecked so a path
replacement cannot silently change which database is imported. Cancellation
interrupts SQLite work during this preflight. Tables and supported indexes are
then created identically on every target shard and the complete authoritative
catalog is registered while all tables are empty. Every imported table is
registered with the explicit `GeneratedIdPolicy::None` policy. The importer
never infers generated-ID authority from `INTEGER PRIMARY KEY`,
`AUTOINCREMENT`, a `DEFAULT` expression, or `sqlite_sequence`.

Rows retain their SQLite storage classes. Sharded rows use separate physical
connections and visit one selected shard; the implementation does not use
`ATTACH`, does not compute `hash % shard_count`, and does not describe multiple
SQLite commits as one transaction. Global rows visit every shard only by their
declaration. Generated columns are recomputed from copied ordinary columns.
The source `sqlite_sequence` high-water mark is installed on every physical
copy as source-schema state; it is not generated-ID catalog metadata and does
not opt the table into `native_range_v1`. Existing integer keys, including
values copied from an `AUTOINCREMENT` table, remain explicit legacy values and
route through the ordinary Int64 shard-key encoding. Enabling a generated-ID
policy for imported data requires a future explicit conversion that proves the
key domain and seeds every allocator; import never guesses.
Preserved implicit `rowid` values remain shard-local physical locators after
import; they are neither globally unique logical identities nor authoritative
shard keys. Declared primary and unique constraints retain global meaning
because the importer requires every such key to contain the shard key.
One source row or individual TEXT/BLOB value is limited to 64 MiB so a single
cell cannot exhaust the importer process.

Before publication, BriskDB verifies source and physical counts, Global copy
counts, every Sharded row's owner, exact value digests, schema consensus,
sequences, `quick_check`, the supported foreign-key result, and a normal
manifest/catalog reopen. The durable receipt contains the normalized plan and
source-schema digests, persisted routing versions and map generation, per-shard
counts, and every explicit foreign-key omission. Receipt format version 2 also
records each table's lowercase BLAKE3 logical-contents digest and its exact
source `sqlite_sequence` high-water mark, or `null` when no sequence row exists.
The logical digest covers the verified row multiset, including SQLite storage
classes, generated values, and preserved implicit rowids; it is independent of
row order and physical SQLite page layout.

## Publication and interruption

The importer builds a cryptographically named hidden sibling directory on the
same filesystem as the destination. Errors or cancellation before publication
leave source database content and the final destination untouched and clean
staging on a best-effort basis. SQLite can create or update its transient
`-shm` sidecar while reading a WAL-mode source; the main database and WAL are
never opened for writing. A process abort can leave an unpublished hidden
stage; a retry uses a fresh stage and never adopts it implicitly.

After all SQLite WALs are checkpointed and files and directories are synced,
one atomic no-replace directory rename publishes the completed layout. The
destination is never overwritten. The staging directory's retained file
identity is checked before and after synchronization and immediately before
publication; a replaced path is neither published nor recursively cleaned.
Once the rename succeeds, late cancellation does not turn a committed import
into a cancellation result. A crash observer therefore sees either no final
destination or a layout that passed full reopen validation. Local filesystems
with working SQLite locks, atomic same-filesystem rename, and durable
synchronization are required; network and synchronized filesystems remain
unsupported.

## Current exclusions

The first importer accepts ordinary tables and their SQLite-created or explicit
indexes. Views, triggers, virtual tables, source schema changes during the
snapshot, cross-database objects, custom collation dependencies, and live or
incremental imports are rejected. A destination that already exists is never
overwritten or treated as an import stage. Unknown `sqlite_*` objects,
including persisted `sqlite_stat*` analysis tables, are rejected rather than
silently omitted.
