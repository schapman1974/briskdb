# Document catalog, storage, and TinyMongo import

Status: implemented by roadmap issue #163

The opt-in `documents` feature stores ordered BSON documents in BriskDB's
ordinary sharded SQLite layout. This is the persistence boundary below the
[protocol-neutral document engine](DOCUMENT_ENGINE.md). Protocol adapters use
that engine rather than this storage API. This layer does not expose a MongoDB
listener or a high-level embedded collection API.

## Logical catalog

Manifest format 14 introduced document namespaces separate from the SQL table
catalog, and the current format 17 retains that separation. A SQL table can
never become a collection through schema discovery. The manifest stores:

- exact, case-sensitive database and collection names;
- ordered BSON collection options;
- BSON, storage, catalog, index, placement-policy, and placement-algorithm
  versions;
- one mandatory unique `_id_` definition per active collection;
- user index declarations and their `PendingBuild` or `Ready` lifecycle; and
- checksummed, mutually exclusive provisioning and deletion journals; and
- permanent database/collection and index identity high-water marks, never reset
  by drops.

Database names contain 1 to 63 UTF-8 bytes. A complete
`database.collection` namespace contains at most 255 UTF-8 bytes. Names are
compared byte-for-byte and may not contain NUL. Mongo names are not normalized
through BriskDB's lowercase SQL identifier rules.

All eight document catalog tables participate in semantic manifest digest
version 9. Every supported mutation uses an immediate SQLite transaction,
validates the complete catalog, refreshes the digest, and commits the metadata
as one unit. Startup validates exact table definitions, foreign keys, row and
byte bounds, supported versions, namespace limits, built-in index state, and
collection/provisioning lifecycle relationships before publishing Ready.

Collection discovery exposes an opaque UUIDv8 without adding a persisted field
or changing format 16. Its frozen v1 derivation uses BLAKE3's derive-key mode
with context `briskdb.collection-metadata.uuid.v1`, then hashes the 16 raw durable
root layout-ID bytes followed by the collection ID as eight little-endian bytes.
Take the first 16 hash bytes, set byte 6's high nibble to `8` and byte 8's high two
bits to `10`, and encode as BSON binary subtype 4. Independent roots and
non-reused collection IDs distinguish identities; reopen and complete backups
preserve them. This is an identity, not an authentication token or Mongo storage
format claim.

## Shard records and placement

Every shard that serves an active collection has one storage-owned table. Its
abridged shape is:

```sql
CREATE TABLE briskdb_documents_v1 (
    collection_id INTEGER NOT NULL,
    id_key BLOB NOT NULL,
    natural_order INTEGER NOT NULL,
    document_bson BLOB NOT NULL,
    document_checksum BLOB NOT NULL,
    storage_format_version INTEGER NOT NULL,
    PRIMARY KEY (collection_id, id_key),
    UNIQUE (collection_id, natural_order)
) STRICT, WITHOUT ROWID;
```

The implementation validates the full definition, types, and byte bounds.
Ordinary SQL is denied access to this reserved table. The table remains
directly inspectable with `sqlite3` by an operator who opens the physical file
outside BriskDB.

`document_bson` is the complete BSON document. Encoding preserves field order,
wire families, binary subtype, Decimal128 BID bytes, timestamps, scoped code,
and the other representation guarantees in [the BSON contract](BSON.md).
`natural_order` is allocated monotonically per collection and provides stable
cross-shard insertion order after restart. `document_checksum` is a
domain-separated BLAKE3 checksum over collection ID, physical shard,
natural-order value, canonical ID key, and exact BSON bytes. Checksums detect
damage; they are not authentication.

`id_key` is `CanonicalBsonKey(_id)`. BriskDB routes those bytes through the
persisted virtual-bucket map using immutable placement policy
`HashByIdV1`. Semantically equal IDs therefore always select the same shard,
and the shard-local primary key enforces collection-wide `_id` uniqueness.
For example, BSON integer `1` and double `1.0` collide, as do a Standard UUID
and binary subtype 4 containing the same 16 bytes.

The fixed table and exact canonical-ID key provide safe `_id` candidate
filtering today. Later query and index work may add conservative candidate
structures, but BSON matching remains authoritative; SQLite candidates may
never exclude a true Mongo match. Secondary index declarations remain
`PendingBuild` until issue #174 installs and verifies their physical authority.

All shard-local insert, replace and delete storage primitives require a
caller-owned Rust transaction and reject handles whose SQLite transaction has
already rolled back. Ordinary inserts commit one immediate transaction per
input; exact-ID deletes also use an explicit immediate transaction. This closes
the autocommit paths before future index-entry maintenance adds more statements
to each mutation. It does not create physical entries or activate indexes.
Cancellation before commit rolls back the current input; earlier batch commits
remain. Subprocess tests cover process death after the write, before commit and
after commit, including the durable prefix of an interrupted insert batch.

The pure `DocumentIndexPreparation` helper now prepares all secondary definitions
from a collection snapshot under one bounded budget, returning scoped encoded
keys only if every index succeeds. It preserves empty sparse/partial membership
and excludes the built-in ID index. It does not access storage or certify current
catalog authority; the future physical path must fence the snapshot and commit
entries with the document. Pending declarations remain non-enforcing. See the
[shared-engine preparation contract](DOCUMENT_ENGINE.md#implemented-commands).

## Provisioning and restart

Collection creation first commits the database, provisioning collection,
pending built-in `_id_` index, operation identity, target shard count, and
`next_shard = 0` under the manifest checksum. It then creates or verifies the
fixed table on each shard and advances the checksummed cursor. Only after every
shard is durable does one final transaction mark the collection and built-in
index Ready and remove the cursor.

A retained cursor forces sole-process startup ownership. Restart resumes the
exact remaining shard prefix idempotently. An active collection with a missing
or incompatible table is corruption. An exact document table without catalog
authority is also rejected. Builds without the `documents` feature still
understand current manifest format 17 and validate its physical schema, but
refuse to open a root containing collections or a pending deletion.

Every built-in and declared index also has a durable root-wide `DocumentIndexId`.
The manifest-only version-17 migration assigns IDs without changing existing
specification bytes or lifecycles. New IDs and declarations commit together;
idempotent declarations retain their IDs. Namespace deletion cascades the mapping
but never reduces the allocation high-water mark, including when the catalog
becomes empty. Restart validates mapping coverage and checksums. These identities
do not activate secondary indexes or change native Python/wire metadata shapes.

Native sparse/partial declarations retain the same six-field v2 BSON envelope as
imported TinyMongo indexes: `v`, `name`, `key`, `unique`, `sparse`, and
`partialFilterExpression` (a document or null). Ordinary native declarations keep
their flat-key encoding. The engine validates membership expressions and the
complete 1 MiB envelope before catalog mutation; declaration bytes, identity and
checksum still commit together. No existing metadata is rewritten. Interpreting
supported keys/options does not mark a pending index Ready or touch shard data.

## Namespace deletion and restart

Dropping a collection or logical document database requires sole-process schema
ownership and drains admitted operations before mutation. One manifest transaction
records the exact database ID, optional collection ID (null means the whole
database), operation identity, shard count, and `next_shard = 0`. Each shard
deletes only the targeted collection IDs in one transaction; a separate sealed
manifest transaction acknowledges its progress. Replay of an unacknowledged
shard is idempotent. If no collections will remain anywhere, each shard removes
the exact optional document table instead. SQL tables, shard files, and other
document databases are untouched.

After all shards commit, one manifest transaction removes the targeted index
and collection metadata, the now-empty logical database, and the journal.
Database and collection IDs are allocated from durable high-water marks in the
same transaction as catalog creation. Exhaustion fails instead of reusing IDs.
Dropping and recreating a name therefore cannot make a retained cursor read the
new collection, including cursors held by another in-process engine.

Cancellation before intent commit leaves data unchanged. Interruption after
that commit leaves the root fenced until reopen completes the authorized drop;
it is not a rollback guarantee. Startup requires sole-process ownership and
finishes the remaining shard prefix before publishing Ready. The crash suite
exits a child process before/after intent, each shard transaction, each progress
acknowledgement, and finalization, for both drop scopes with/without survivors.

Pending secondary declarations can also be removed individually by exact name.
The built-in index is protected. Declaration deletion, its identity-map cascade
and semantic-root refresh commit atomically; the permanent allocation head does
not decrease. No shard row or schema changes. A pre-commit process exit restores
the declaration and ID on reopen; a post-commit exit preserves their absence.
This metadata-only path rejects non-pending secondary lifecycles and does not
stand in for a future recoverable physical-index drop.

## TinyMongo SQLite import

Feature `tinymongo-import` selects `documents` and `sqlite-import`. Its public
entry points are `TinyMongoImportPlan`, `read_tinymongo_source`,
`TinyMongoImportOptions`, and `import_tinymongo_database`.

The plan requires an exact database name and collection allowlist. This is
mandatory because TinyMongo's unsharded SQLite format has no application ID or
authoritative collection catalog; a normal SQLite table can otherwise have the
same two-column shape. The reader understands the pinned TinyMongo v1.3 forms:

1. the legacy `tinydb(id, data)` single-row JSON root;
2. table-native `<database>.sqlite` collection tables; and
3. a version-1 `.sqlite-sharded` directory with a manifest and 2 to 64 shards.

Preflight opens each source through SQLite's read-only immutable URI, disables
trusted-schema execution, runs `quick_check`, and installs a cancellation
progress handler before reading rows. The source must be stopped and fully
checkpointed: a `-wal`, `-shm`, or rollback-journal sidecar makes the import
fail before any destination path is created. Preflight validates exact schemas,
shard identity, manifest state, collection allowlist coverage, physical-ID
syntax and shard route, logical `_id` presence and semantic uniqueness, durable
logical index metadata, tagged JSON values, per-document limits, and aggregate
document, physical-metadata, and catalog budgets. Internal tables, physical
TinyMongo indexes, hidden order columns, and arbitrary non-allowlisted SQL
tables never become user collections.

For pre-v2 table keys, the importer proves that each reproducible Python scalar
spelling agrees with the logical `_id`. It parses the bounded literal form of
legacy mapping, list, and tuple IDs, compares it recursively with Mongo numeric
semantics, and restores mapping order from that physical key. Mismatches are
corruption. Python representations that cannot be reproduced losslessly from
the stored BSON family fail as unsupported instead of trusting an unrelated
physical key.

The reader does not recompute TinyMongo's Python-specific SHA-256 physical-ID
payload. That algorithm depends on Python JSON and its complete recursive BSON
identity registry. A syntactically valid digest that disagrees with logical
`_id` but still routes to the same source shard is therefore not diagnosed.
The destination never trusts that digest: it derives a new canonical key and
route from the validated logical BSON `_id`, so this limitation cannot change
destination identity or placement.

TinyMongo JSON persistence cannot recover information it did not store. Plain
JSON integers become Int32 when possible and otherwise Int64; out-of-Int64
values fail. Tagged Decimal128, ObjectId, binary subtype, UUID, date,
timestamp, regex, code/scope, MinKey, and MaxKey values retain their encoded
meaning. Python-only regex representations or unsupported flags fail instead
of being coerced. Secondary index specifications are imported as logical
`PendingBuild` metadata and rebuilt later; source SQLite expression indexes are
never trusted.

The complete source is validated before a destination staging directory is
created. A directory destination may not be placed inside its source. The
importer creates collections, writes routed documents in the source's durable
natural order, records pending indexes, closes and reopens the new layout, and
compares the catalog and exact ordered BSON sequence with the preflight
material. It then writes a versioned receipt, checkpoints and synchronizes
every SQLite file, and atomically renames the absent destination into place.
Cancellation is checked during SQLite work and materialization as well as
before publication. Cancellation or any error before the rename removes the
private stage, leaving the source and destination untouched and allowing a
clean retry.

```rust,no_run
use briskdb::import::{
    TinyMongoImportOptions, TinyMongoImportPlan, import_tinymongo_database,
};

let plan = TinyMongoImportPlan::new("app", ["users", "orders"])?;
let report = import_tinymongo_database(
    "tinydb/app.sqlite",
    "data/briskdb",
    &plan,
    TinyMongoImportOptions::new(4)?,
)?;
assert_eq!(report.target_shards(), Some(4));
# Ok::<(), briskdb::EngineError>(())
```

The importer is a library API in this release. The existing `briskdb-import`
binary continues to handle standard relational SQLite plans.
