//! SQLite file layout, manifest initialization, and connection configuration.

use std::{
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{Connection, OptionalExtension};

pub use crate::core::Database;
use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

const SCHEMA_VERSION: &str = "1";

#[derive(Debug)]
pub(crate) struct Storage {
    root: PathBuf,
    shard_count: u16,
}

impl Storage {
    pub(crate) fn open(root: impl AsRef<Path>, requested_shards: u16) -> EngineResult<Self> {
        if !(2..=64).contains(&requested_shards) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "shard count must be between 2 and 64",
            ));
        }

        let root = root.as_ref().to_path_buf();
        let shards_dir = root.join("shards");
        fs::create_dir_all(&shards_dir).map_err(|error| {
            sqlite_error::storage_io(error, format!("failed to create {}", shards_dir.display()))
        })?;

        let manifest_path = root.join("manifest.sqlite");
        let mut manifest = Connection::open(&manifest_path).map_err(|error| {
            sqlite_error::storage(error)
                .context(format!("failed to open {}", manifest_path.display()))
        })?;
        configure_connection(&manifest)?;
        manifest
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS briskdb_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
            )
            .map_err(sqlite_error::storage)?;

        let existing: Option<String> = manifest
            .query_row(
                "SELECT value FROM briskdb_metadata WHERE key = 'shard_count'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error::storage)?;

        if let Some(existing) = existing {
            let existing: u16 = existing.parse().map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::DataCorruption,
                    "manifest has an invalid shard count",
                    error,
                )
            })?;
            if !(2..=64).contains(&existing) {
                return Err(EngineError::new(
                    EngineErrorKind::DataCorruption,
                    format!("manifest shard count {existing} is outside the supported range"),
                ));
            }
            if existing != requested_shards {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    format!(
                        "database was created with {existing} shards, but {requested_shards} were requested"
                    ),
                ));
            }
        } else {
            let transaction = manifest.transaction().map_err(sqlite_error::storage)?;
            transaction
                .execute(
                    "INSERT INTO briskdb_metadata (key, value) VALUES ('shard_count', ?1)",
                    [requested_shards.to_string()],
                )
                .map_err(sqlite_error::storage)?;
            transaction
                .execute(
                    "INSERT INTO briskdb_metadata (key, value) VALUES ('schema_version', ?1)",
                    [SCHEMA_VERSION],
                )
                .map_err(sqlite_error::storage)?;
            transaction.commit().map_err(sqlite_error::storage)?;
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

    pub(crate) fn open_shard(&self, shard: u16) -> EngineResult<Connection> {
        if shard >= self.shard_count {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                format!("shard {shard} is outside the configured range"),
            ));
        }
        let path = self.shard_path(shard);
        let connection = Connection::open(&path).map_err(|error| {
            sqlite_error::storage(error).context(format!("failed to open shard {}", path.display()))
        })?;
        configure_connection(&connection)?;
        Ok(connection)
    }
}

fn configure_connection(connection: &Connection) -> EngineResult<()> {
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(sqlite_error::storage)?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(sqlite_error::storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

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
        let too_few = Storage::open(temp.path(), 1).unwrap_err();
        assert_eq!(too_few.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(too_few.to_string(), "shard count must be between 2 and 64");

        let too_many = Storage::open(temp.path(), 65).unwrap_err();
        assert_eq!(too_many.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(too_many.to_string(), "shard count must be between 2 and 64");

        Storage::open(temp.path(), 4).unwrap();
        let changed = Storage::open(temp.path(), 8).unwrap_err();
        assert_eq!(changed.kind(), EngineErrorKind::FailedPrecondition);
        assert_eq!(
            changed.to_string(),
            "database was created with 4 shards, but 8 were requested"
        );
    }

    #[test]
    fn rejects_opening_a_shard_outside_the_layout() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::open(temp.path(), 4).unwrap();

        let error = storage.open_shard(4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(error.to_string(), "shard 4 is outside the configured range");
    }

    #[test]
    fn rejects_malformed_manifest_metadata_as_data_corruption() {
        let temp = tempfile::tempdir().unwrap();
        Storage::open(temp.path(), 4).unwrap();
        let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
        manifest
            .execute(
                "UPDATE briskdb_metadata SET value = 'not-a-number' WHERE key = 'shard_count'",
                [],
            )
            .unwrap();

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert_eq!(error.to_string(), "manifest has an invalid shard count");
        assert!(error.source().is_some());
    }

    #[test]
    fn rejects_impossible_numeric_manifest_counts_as_data_corruption() {
        for invalid in ["0", "1", "65", "65535"] {
            let temp = tempfile::tempdir().unwrap();
            Storage::open(temp.path(), 4).unwrap();
            let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            manifest
                .execute(
                    "UPDATE briskdb_metadata SET value = ?1 WHERE key = 'shard_count'",
                    [invalid],
                )
                .unwrap();

            let error = Storage::open(temp.path(), 4).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_eq!(
                error.to_string(),
                format!("manifest shard count {invalid} is outside the supported range")
            );
        }
    }

    #[test]
    fn rejects_a_corrupt_manifest_database() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(
            temp.path().join("manifest.sqlite"),
            b"not a sqlite database",
        )
        .unwrap();

        let error = Storage::open(temp.path(), 4).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        assert!(error.source().is_some());
    }

    #[test]
    fn classifies_invalid_storage_path_shape_as_a_failed_precondition() {
        let root_file = tempfile::NamedTempFile::new().unwrap();
        let error = Storage::open(root_file.path(), 4).unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(!error.is_retryable());
        assert!(error.source().is_some());
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
