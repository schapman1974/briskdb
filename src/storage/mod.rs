//! SQLite file layout, manifest initialization, and connection configuration.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use rusqlite::{Connection, OptionalExtension};

pub use crate::core::Database;

const SCHEMA_VERSION: &str = "1";

#[derive(Debug)]
pub(crate) struct Storage {
    root: PathBuf,
    shard_count: u16,
}

impl Storage {
    pub(crate) fn open(root: impl AsRef<Path>, requested_shards: u16) -> anyhow::Result<Self> {
        if !(2..=64).contains(&requested_shards) {
            bail!("shard count must be between 2 and 64");
        }

        let root = root.as_ref().to_path_buf();
        let shards_dir = root.join("shards");
        fs::create_dir_all(&shards_dir)
            .with_context(|| format!("failed to create {}", shards_dir.display()))?;

        let manifest_path = root.join("manifest.sqlite");
        let mut manifest = Connection::open(&manifest_path)
            .with_context(|| format!("failed to open {}", manifest_path.display()))?;
        configure_connection(&manifest)?;
        manifest.execute_batch(
            "CREATE TABLE IF NOT EXISTS briskdb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;

        let existing: Option<String> = manifest
            .query_row(
                "SELECT value FROM briskdb_metadata WHERE key = 'shard_count'",
                [],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(existing) = existing {
            let existing: u16 = existing
                .parse()
                .context("manifest has an invalid shard count")?;
            if existing != requested_shards {
                bail!(
                    "database was created with {existing} shards, but {requested_shards} were requested"
                );
            }
        } else {
            let transaction = manifest.transaction()?;
            transaction.execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('shard_count', ?1)",
                [requested_shards.to_string()],
            )?;
            transaction.execute(
                "INSERT INTO briskdb_metadata (key, value) VALUES ('schema_version', ?1)",
                [SCHEMA_VERSION],
            )?;
            transaction.commit()?;
        }

        let storage = Self {
            root,
            shard_count: requested_shards,
        };
        for shard in 0..requested_shards {
            storage.open_shard(shard)?;
        }
        Ok(storage)
    }

    pub(crate) fn shard_count(&self) -> u16 {
        self.shard_count
    }

    fn shard_path(&self, shard: u16) -> PathBuf {
        self.root.join("shards").join(format!("{shard:04}.sqlite"))
    }

    pub(crate) fn open_shard(&self, shard: u16) -> anyhow::Result<Connection> {
        if shard >= self.shard_count {
            bail!("shard {shard} is outside the configured range");
        }
        let path = self.shard_path(shard);
        let connection = Connection::open(&path)
            .with_context(|| format!("failed to open shard {}", path.display()))?;
        configure_connection(&connection)?;
        Ok(connection)
    }
}

fn configure_connection(connection: &Connection) -> anyhow::Result<()> {
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_layout_and_reopens_with_the_same_shard_count() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        assert_eq!(storage.shard_count(), 4);
        assert!(temp.path().join("manifest.sqlite").exists());
        assert!(temp.path().join("shards/0003.sqlite").exists());
        assert!(Storage::open(temp.path(), 4).is_ok());

        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        assert_eq!(
            manifest
                .query_row(
                    "SELECT value FROM briskdb_metadata WHERE key = 'schema_version'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    #[test]
    fn rejects_invalid_or_changed_shard_counts() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            Storage::open(temp.path(), 1).unwrap_err().to_string(),
            "shard count must be between 2 and 64"
        );
        assert_eq!(
            Storage::open(temp.path(), 65).unwrap_err().to_string(),
            "shard count must be between 2 and 64"
        );

        Storage::open(temp.path(), 4).unwrap();
        assert_eq!(
            Storage::open(temp.path(), 8).unwrap_err().to_string(),
            "database was created with 4 shards, but 8 were requested"
        );
    }

    #[test]
    fn rejects_opening_a_shard_outside_the_layout() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        assert_eq!(
            storage.open_shard(4).unwrap_err().to_string(),
            "shard 4 is outside the configured range"
        );
    }

    #[test]
    fn shard_connections_keep_the_durability_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();
        let connection = storage.open_shard(0).unwrap();

        assert_eq!(
            connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            connection
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            5_000
        );
    }
}
