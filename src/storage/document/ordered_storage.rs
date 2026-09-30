//! Optional v24-fenced ordered entries. Physical creation belongs to the same
//! per-shard transaction as an index build; unpublished entries are journaled.

use super::{corrupt, normalize_schema_sql, shard_read_error};
use crate::core::EngineResult;
use rusqlite::Connection;

pub(super) const TABLE: &str = "briskdb_document_ordered_entries_v1";
const SCAN: &str = "briskdb_document_ordered_scan_v1";
const TABLE_SQL: &str = "CREATE TABLE briskdb_document_ordered_entries_v1 (
    collection_id INTEGER NOT NULL CHECK (collection_id > 0),
    id_key BLOB NOT NULL CHECK (typeof(id_key) = 'blob' AND length(id_key) BETWEEN 9 AND 16777216),
    index_id INTEGER NOT NULL CHECK (index_id > 0),
    direction INTEGER NOT NULL CHECK (direction IN (0, 1)),
    sort_key BLOB CHECK (sort_key IS NULL OR (typeof(sort_key) = 'blob' AND length(sort_key) BETWEEN 9 AND 65536)),
    natural_order INTEGER NOT NULL CHECK (natural_order > 0),
    entry_checksum BLOB NOT NULL CHECK (typeof(entry_checksum) = 'blob' AND length(entry_checksum) = 32),
    entry_format_version INTEGER NOT NULL CHECK (entry_format_version = 1),
    PRIMARY KEY (collection_id, id_key, index_id, direction),
    FOREIGN KEY (collection_id, id_key) REFERENCES briskdb_documents_v1 (collection_id, id_key) ON DELETE CASCADE
) STRICT, WITHOUT ROWID";
const SCAN_SQL: &str = "CREATE INDEX briskdb_document_ordered_scan_v1
    ON briskdb_document_ordered_entries_v1 (collection_id, index_id, direction, sort_key, natural_order, id_key, entry_checksum, entry_format_version)";

pub(super) fn is_exact_schema_object(
    kind: &str,
    name: &str,
    table: &str,
    sql: Option<&str>,
) -> bool {
    let expected = match (kind, name, table) {
        ("table", TABLE, TABLE) => TABLE_SQL,
        ("index", SCAN, TABLE) => SCAN_SQL,
        _ => return false,
    };
    sql.is_some_and(|sql| normalize_schema_sql(sql) == normalize_schema_sql(expected))
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let objects = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
         WHERE name = ?1 COLLATE NOCASE OR name = ?2 COLLATE NOCASE ORDER BY name LIMIT 3",
        )
        .and_then(|mut statement| {
            statement
                .query_map([TABLE, SCAN], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect ordered index storage"))?;
    if objects.is_empty() {
        return Ok(false);
    }
    if objects.len() != 2
        || objects.iter().any(|(kind, name, table, sql)| {
            !is_exact_schema_object(kind, name, table, sql.as_deref())
        })
    {
        return Err(corrupt(
            "ordered document index storage has an incompatible schema",
        ));
    }
    Ok(true)
}

/// One metadata query covers both optional index families, keeping ordinary
/// record-schema validation at its existing two-inspection budget.
pub(super) fn inspect_index_schemas(connection: &Connection) -> EngineResult<(bool, bool)> {
    let objects = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
         WHERE name = ?1 COLLATE NOCASE OR name = ?2 COLLATE NOCASE
            OR name = ?3 COLLATE NOCASE OR name = ?4 COLLATE NOCASE ORDER BY name LIMIT 5",
        )
        .and_then(|mut statement| {
            statement
                .query_map(
                    [
                        super::index_storage::ENTRIES_TABLE,
                        super::index_storage::BY_RECORD,
                        TABLE,
                        SCAN,
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect document index schemas"))?;
    let mut counts = [0, 0];
    for (kind, name, table, sql) in objects {
        let ordered = name.eq_ignore_ascii_case(TABLE) || name.eq_ignore_ascii_case(SCAN);
        let exact = if ordered {
            is_exact_schema_object(&kind, &name, &table, sql.as_deref())
        } else {
            super::index_storage::is_exact_schema_object(&kind, &name, &table, sql.as_deref())
        };
        if !exact {
            return Err(corrupt("document index storage has an incompatible schema"));
        }
        counts[usize::from(ordered)] += 1;
    }
    if counts.iter().any(|count| !matches!(count, 0 | 2)) {
        return Err(corrupt("document index storage schema is incomplete"));
    }
    Ok((counts[0] == 2, counts[1] == 2))
}

#[cfg(feature = "documents")]
mod audit;
#[cfg(feature = "documents")]
mod entries;
#[cfg(feature = "documents")]
pub(super) use audit::{STARTUP_ORDERED_SQL, StartupAudit};
#[cfg(feature = "documents")]
pub(super) use entries::*;

#[cfg(feature = "documents")]
pub(super) fn ensure_schema(transaction: &rusqlite::Transaction<'_>) -> EngineResult<()> {
    if !validate_optional_schema(transaction)? {
        transaction
            .execute_batch(TABLE_SQL)
            .map_err(crate::sqlite_error::storage)?;
        transaction
            .execute_batch(SCAN_SQL)
            .map_err(crate::sqlite_error::storage)?;
    }
    Ok(())
}

#[cfg(feature = "documents")]
pub(super) fn drop_schema(transaction: &rusqlite::Transaction<'_>) -> EngineResult<()> {
    if validate_optional_schema(transaction)? {
        transaction
            .execute_batch("DROP TABLE briskdb_document_ordered_entries_v1")
            .map_err(crate::sqlite_error::storage)?;
    }
    Ok(())
}
