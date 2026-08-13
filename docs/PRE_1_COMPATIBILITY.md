# Pre-1.0 compatibility policy

BriskDB 0.x releases are experimental. The command-line interface, HTTP
contract, Rust API, accepted SQL subset, and on-disk format may change between
0.x releases. A 0.x release does not promise that a newer layout can be opened
by an older binary or that every future binary will migrate every historical
prototype layout.

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

A supported forward migration runs during startup before either listener binds.
It may update `manifest.sqlite`, shard metadata, schema generations, or other
files in the data directory. Treat startup of a newer binary as a storage
mutation even when application rows do not change.

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
