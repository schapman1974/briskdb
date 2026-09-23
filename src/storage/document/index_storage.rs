//! Downgrade-fenced physical entry schema and restartable empty-table upgrade.
//! Ready non-unique authority is managed by the separate operation journal.

use rusqlite::Connection;

use super::{corrupt, normalize_schema_sql, shard_read_error};
use crate::core::EngineResult;

#[cfg(feature = "documents")]
mod entries;
#[cfg(feature = "documents")]
pub(super) use entries::{
    insert_entries, insert_selected_entries, remove_record_entries, require_no_orphans,
    validate_record_entries,
};

pub(super) const ENTRIES_TABLE: &str = "briskdb_document_index_entries_v1";
const BY_RECORD: &str = "briskdb_document_index_entries_by_record_v1";
const ENTRIES_SQL: &str = "CREATE TABLE briskdb_document_index_entries_v1 (
    collection_id INTEGER NOT NULL CHECK (collection_id > 0),
    index_id INTEGER NOT NULL CHECK (index_id > 0),
    id_key BLOB NOT NULL CHECK (typeof(id_key) = 'blob' AND length(id_key) BETWEEN 9 AND 16777216),
    index_key BLOB NOT NULL CHECK (typeof(index_key) = 'blob' AND length(index_key) BETWEEN 13 AND 67108864),
    entry_checksum BLOB NOT NULL CHECK (typeof(entry_checksum) = 'blob' AND length(entry_checksum) = 32),
    entry_format_version INTEGER NOT NULL CHECK (entry_format_version = 1),
    PRIMARY KEY (collection_id, index_id, index_key, id_key),
    FOREIGN KEY (collection_id, id_key) REFERENCES briskdb_documents_v1 (collection_id, id_key) ON DELETE CASCADE
) STRICT, WITHOUT ROWID";
const BY_RECORD_SQL: &str = "CREATE INDEX briskdb_document_index_entries_by_record_v1
    ON briskdb_document_index_entries_v1 (collection_id, id_key, index_id)";

pub(super) fn is_exact_schema_object(
    kind: &str,
    name: &str,
    table: &str,
    sql: Option<&str>,
) -> bool {
    let expected = match (kind, name, table) {
        ("table", ENTRIES_TABLE, ENTRIES_TABLE) => ENTRIES_SQL,
        ("index", BY_RECORD, ENTRIES_TABLE) => BY_RECORD_SQL,
        _ => return false,
    };
    sql.is_some_and(|sql| normalize_schema_sql(sql) == normalize_schema_sql(expected))
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let objects = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
         WHERE name = ?1 COLLATE NOCASE OR name = ?2 COLLATE NOCASE
         ORDER BY name LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([ENTRIES_TABLE, BY_RECORD], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| {
            shard_read_error(error, "failed to inspect document index entry schema")
        })?;
    if objects.is_empty() {
        return Ok(false);
    }
    if objects.len() != 2
        || objects.iter().any(|(kind, name, table, sql)| {
            !is_exact_schema_object(kind, name, table, sql.as_deref())
        })
    {
        return Err(corrupt(
            "document index entry storage has an incompatible schema",
        ));
    }
    Ok(true)
}

#[cfg(feature = "documents")]
pub(super) fn ensure_schema(transaction: &rusqlite::Transaction<'_>) -> EngineResult<()> {
    if !validate_optional_schema(transaction)? {
        transaction
            .execute_batch(ENTRIES_SQL)
            .map_err(crate::sqlite_error::storage)?;
        transaction
            .execute_batch(BY_RECORD_SQL)
            .map_err(crate::sqlite_error::storage)?;
    }
    if !validate_optional_schema(transaction)? {
        return Err(corrupt(
            "document index entry creation did not produce its exact schema",
        ));
    }
    Ok(())
}

#[cfg(feature = "documents")]
pub(super) fn drop_schema(transaction: &rusqlite::Transaction<'_>) -> EngineResult<()> {
    if validate_optional_schema(transaction)? {
        transaction
            .execute_batch("DROP TABLE briskdb_document_index_entries_v1")
            .map_err(crate::sqlite_error::storage)?;
    }
    Ok(())
}

#[cfg(test)]
fn crash_checkpoint(point: &str, shard: u16) {
    if std::env::var("BRISKDB_TEST_DOCUMENT_INDEX_STORAGE_CRASH")
        .ok()
        .as_deref()
        == Some(format!("{point}:{shard}").as_str())
    {
        std::process::exit(74);
    }
}

#[cfg(feature = "documents")]
pub(super) fn recover_layout(
    storage: &super::Storage,
    manifest_connection: &mut Connection,
) -> EngineResult<()> {
    use crate::{sqlite_error, storage::manifest};
    use rusqlite::{TransactionBehavior, params};

    manifest::current_integrity(manifest_connection, storage.shard_count())?;
    let (state, mut next): (u16, u16) = manifest_connection.query_row(
        "SELECT lifecycle_state, next_shard FROM briskdb_document_index_storage WHERE singleton = 1",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(sqlite_error::storage)?;
    if state == 2 {
        // Startup has sole-process ownership for this checksummed upgrade.
        // Namespace recovery has already finished, so a remaining collection
        // requires its record table on every shard; an empty catalog does not.
        let populated: bool = manifest_connection
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM briskdb_document_collections)",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error::storage)?;
        #[cfg(test)]
        crash_checkpoint("after-intent", 0);
        while next < storage.shard_count() {
            let shard = next;
            let mut connection = storage.open_unconfigured_shard(shard)?;
            storage.validate_unconfigured_shard(&connection, shard)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            let present = super::validate_optional_schema(&transaction)?;
            if present != populated {
                return Err(corrupt(
                    "document index storage upgrade found missing or orphaned records",
                ));
            }
            if populated {
                ensure_schema(&transaction)?;
                require_empty(&transaction)?;
            }
            #[cfg(test)]
            crash_checkpoint("before-shard-commit", shard);
            transaction.commit().map_err(sqlite_error::storage)?;
            #[cfg(test)]
            crash_checkpoint("after-shard-commit", shard);

            let transaction = manifest_connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            let changed = transaction.execute(
                "UPDATE briskdb_document_index_storage SET next_shard = ?1
                 WHERE singleton = 1 AND lifecycle_state = 2 AND next_shard = ?2 AND shard_count = ?3",
                params![shard + 1, shard, storage.shard_count()],
            ).map_err(sqlite_error::storage)?;
            if changed != 1 {
                return Err(corrupt(
                    "document index storage upgrade cursor did not advance exactly once",
                ));
            }
            manifest::refresh_manifest_digest(&transaction)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            #[cfg(test)]
            crash_checkpoint("before-cursor-commit", shard);
            transaction.commit().map_err(sqlite_error::storage)?;
            #[cfg(test)]
            crash_checkpoint("after-cursor-commit", shard);
            next = shard + 1;
        }
        let transaction = manifest_connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        let changed = transaction
            .execute(
                "UPDATE briskdb_document_index_storage SET lifecycle_state = 1
             WHERE singleton = 1 AND lifecycle_state = 2 AND next_shard = shard_count",
                [],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(corrupt(
                "document index storage completion lost its journal",
            ));
        }
        manifest::refresh_manifest_digest(&transaction)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        #[cfg(test)]
        crash_checkpoint("before-completion", 0);
        transaction.commit().map_err(sqlite_error::storage)?;
        #[cfg(test)]
        crash_checkpoint("after-completion", 0);
    }
    // The ordinary startup record-validation pass checks the complete shard
    // prefix (including entries), before the root is published to callers.
    Ok(())
}

#[cfg(feature = "documents")]
pub(super) fn require_empty(connection: &Connection) -> EngineResult<()> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM briskdb_document_index_entries_v1)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            shard_read_error(error, "failed to validate pending document index storage")
        })?;
    if exists {
        return Err(corrupt(
            "document index entries exist without physical index authority",
        ));
    }
    Ok(())
}

#[cfg(all(test, feature = "documents"))]
mod tests;
