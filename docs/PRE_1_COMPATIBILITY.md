# Pre-1.0 compatibility policy

BriskDB 0.x releases are experimental. The command-line interface, HTTP
contract, Rust API, accepted SQL subset, and on-disk format may change between
0.x releases. A 0.x release does not promise that a newer layout can be opened
by an older binary or that every future binary will migrate every historical
prototype layout.

The [versioned HTTP transport](HTTP_API.md) has its own compatibility boundary:
breaking request/response or default value-encoding changes require a new API
major version and a documented migration. This does not stabilize the alpha
SQL subset, storage layout, browser API, or public Rust library.
The checked [OpenAPI v1 artifact](OPENAPI.md) makes that machine-readable
surface reviewable and is tested against the live router. Its documented
representation limits do not weaken the duplicate-member, raw-byte, header, or
NDJSON rules in the HTTP contract.

This policy is narrower than the stable storage-format commitment planned for
1.0 in issue #77.

## Format and forward migration

The current manifest version is recorded in [the storage-format
contract](STORAGE_FORMAT.md). Each release that changes the format must:

- use an ordered, transactional, and tested migration;
- list the old manifest versions that the new binary accepts;
- describe any application-visible compatibility effect in its release notes;
- preserve fail-closed behavior for unknown, newer, malformed, or partially
  migrated layouts; and
- update the documented current version and its executable consistency test.

A supported forward migration runs during startup before any configured
listener binds.
It may update `manifest.sqlite`, shard metadata, schema generations, or other
files in the data directory. Treat startup of a newer binary as a storage
mutation even when application rows do not change.

The current version-18 migration accepts every exact historical version 1
through 17. The v14-to-v15 transaction installs the idempotency-receipt
downgrade fence; later eligible keyed writes may lazily create the optional
shard-local receipt table. The v15-to-v16 transaction adds document identity
high-water marks initialized from existing IDs, an empty deletion journal,
semantic digest version 8, and the version-16 fence. It changes no shard or
application row. Later explicit namespace drops use the recoverable deletion
journal; interruption after durable intent may complete the drop on restart.
Version-15 and older binaries refuse an upgraded root before they can ignore
that intent or recycle a dropped identity.
The v16-to-v17 transaction adds permanent document-index IDs and an allocation
high-water mark, preserving existing index specifications byte-for-byte. It
installs digest version 9 and the version-17 fence without changing shards or
activating indexes. Version-16 and older binaries refuse an upgraded root.
The v17-to-v18 transaction adds the document-index storage journal, digest
version 10 and version-18 fence. Sole-process startup then creates empty physical
entry tables and by-record indexes on document shards, with checksummed progress
and crash recovery. BSON records and existing index definitions/IDs are unchanged;
secondary indexes remain pending and non-enforcing. Empty roots acquire no new
shard tables until collection creation. Version-17 and older binaries refuse
the upgraded root, including an interrupted physical upgrade.

## Required upgrade procedure

Before starting a newer BriskDB release against an existing data directory:

1. Stop every BriskDB process using the directory.
2. Record the current BriskDB release and configured shard count.
3. Create and retain a complete backup using the [stopped-server backup
   procedure](OFFLINE_BACKUP.md).
4. Read the target release notes and confirm that the source manifest version
   is accepted.
5. Start the new release and verify readiness, catalog contents, and known rows
   before returning it to service.

Skipping the backup makes rollback unsupported.

## Downgrade and rollback

In-place downgrade is unsupported. BriskDB persists a downgrade fence, and an
older binary must refuse a layout that requires a newer manifest version. Do
not bypass that refusal by editing `user_version`, manifest rows, SQLite
headers, schema generations, or checksums.

Rollback means stopping the new binary, preserving its directory separately,
and restoring the complete pre-upgrade backup into a new empty directory. Start
the prior binary with the recorded shard count against that restored copy. Do
not combine the pre-upgrade manifest with post-upgrade shards or restore over
the migrated directory.

## Release-note contract

Until 1.0, every release must state one of the following:

- no on-disk format change; or
- the new manifest version, accepted source versions, automatic migration
  behavior, downgrade refusal, and any additional backup or validation steps.

Release notes must also call out breaking CLI, HTTP, Rust API, and SQL-subset
changes. Absence of a noted format change does not turn the 0.x format into a
1.0 stability promise.
