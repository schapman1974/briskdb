# Stopped-server backup and restore

BriskDB's supported alpha backup is a complete copy made while every BriskDB
process using the data directory is stopped. This procedure preserves one
consistent manifest, shard set, schema generation, routing map, generated-ID
allocator state, and any SQLite sidecar files as a unit.

This is not an online backup. Do not copy a live data directory, even when no
application writes are expected. Coordinated online backup remains tracked by
issue #67.

## Backup

1. Record the BriskDB version, configured shard count, and absolute data
   directory path.
2. Stop the BriskDB process cleanly and wait for it to exit. A service manager
   must report the process stopped before copying begins.
3. Verify that no other BriskDB process or embedder is using the directory.
4. Copy the complete data directory into a new backup directory. Include
   `manifest.sqlite`, the entire `shards` directory, the import receipt when
   present, and every SQLite `-wal`, `-shm`, or journal sidecar that exists.
   Do not select individual database files.
5. Make the backup durable using the snapshot, archive, or copy tool's normal
   completion and sync guarantees. Retain the recorded BriskDB version and
   shard count with the backup.
6. Restart the original server only after the copy has completed.

On the supported Linux platform, one simple local-filesystem copy is:

```bash
mkdir -- /var/backups/briskdb-alpha-1
cp -a -- /srv/briskdb-data/. /var/backups/briskdb-alpha-1/
```

The destination must be new and must not be inside the source data directory.
Archive or snapshot tools are acceptable only when they preserve the complete
stopped directory as one recovery point.

## Restore and validate

1. Stop BriskDB and preserve the failed or current data directory separately
   for diagnosis. Never restore files over an existing layout.
2. Create a new empty destination and copy the complete backup into it. Do not
   combine a backup manifest with shards from another backup or time.
3. Start the same BriskDB release with the recorded shard count and the restored
   directory. Startup must reach `Ready` without a migration, integrity, shard
   identity, WAL-mode, or schema-generation error.
4. Check `/health`, inspect the expected logical catalog, and read known rows
   from representative shard keys before returning the service to use.
5. Keep the prior directory and backup unchanged until validation succeeds.
   If validation fails, stop the process and investigate; do not edit manifest
   state, SQLite headers, or checksums to force startup.

Example restore copy:

```bash
mkdir -- /srv/briskdb-restored
cp -a -- /var/backups/briskdb-alpha-1/. /srv/briskdb-restored/
cargo run --locked -- --data-dir /srv/briskdb-restored --shards 4
```

Restoring into a different absolute path is supported. Restoring only one
shard, mixing recovery points, copying while the server is running, or using an
older BriskDB binary against a newer format is unsupported.

## Automated evidence

`tests/offline_backup.rs` creates schema and one routed row on every shard,
performs explicit engine shutdown, copies the complete layout through a backup
directory into a new root, reopens it, and verifies the schema generation,
physical route, and row value for every shard. The test freezes the stopped-copy
contract but does not claim online snapshot safety or certify a particular
third-party backup product.
