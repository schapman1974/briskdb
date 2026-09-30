//! Bounded index-first unions; the broad-group path remains document-first.

use rusqlite::{
    Rows, Statement, params_from_iter,
    types::{ToSqlOutput, ValueRef},
};

use super::{MAX_SELECTIVE_ENTRIES, MAX_SELECTIVE_ID_BYTES};

pub(in super::super) fn membership_selectivity_sql(key_count: usize) -> String {
    let placeholders = (4..=4 + key_count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "SELECT count(*), coalesce(sum(length(id_key) + length(index_key)), 0) FROM (
            SELECT id_key, index_key FROM briskdb_document_index_entries_v1
            INDEXED BY sqlite_autoindex_briskdb_document_index_entries_v1_1
            WHERE collection_id = ?1 AND index_id = ?2 AND index_key IN ({placeholders})
            LIMIT ?3)"
    )
}

/// As for singleton seeks, leave the aggregate row active to pin the snapshot.
/// Permit 32 entries per literal, but never more than 64 KiB of combined ID/key
/// bytes. The latter includes the representative key retained for deduplication.
/// Fallback entries and repeated multikey identities consume these same bounds.
pub(in super::super) fn selective_membership_snapshot<'s>(
    statement: &'s mut Statement<'_>,
    collection: i64,
    index: i64,
    keys: &[Vec<u8>],
    fallback: &[u8],
) -> rusqlite::Result<(Rows<'s>, bool)> {
    // Probe preparation already bounds and deduplicates the literal list.
    let max_entries = MAX_SELECTIVE_ENTRIES * keys.len() as i64;
    let values = [
        ValueRef::Integer(collection),
        ValueRef::Integer(index),
        ValueRef::Integer(max_entries + 1),
    ]
    .into_iter()
    .chain(keys.iter().map(|key| ValueRef::Blob(key)))
    .chain(std::iter::once(ValueRef::Blob(fallback)))
    .map(ToSqlOutput::Borrowed);
    let mut rows = statement.query(params_from_iter(values))?;
    let row = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let entries: i64 = row.get(0)?;
    let bytes: i64 = row.get(1)?;
    Ok((
        rows,
        entries <= max_entries && bytes <= MAX_SELECTIVE_ID_BYTES,
    ))
}

pub(in super::super) fn selective_membership(key_count: usize) -> String {
    let placeholders = (5..=5 + key_count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(",");
    // Deduplicate BEFORE LIMIT and retain only bounded identities/order/key.
    // min(index_key) chooses one real matching entry; the outer primary-key
    // fetch obtains that exact entry and its checksum together with the BSON.
    format!(
        "WITH frontier AS MATERIALIZED (
            SELECT e.id_key, d.natural_order, min(e.index_key) AS index_key
            FROM briskdb_document_index_entries_v1 AS e
            INDEXED BY sqlite_autoindex_briskdb_document_index_entries_v1_1
            CROSS JOIN briskdb_documents_v1 AS d
              ON d.collection_id = e.collection_id AND d.id_key = e.id_key
            WHERE e.collection_id = ?1 AND e.index_id = ?4 AND e.index_key IN ({placeholders})
              AND d.natural_order > ?2
            GROUP BY d.natural_order ORDER BY d.natural_order LIMIT ?3)
         SELECT d.natural_order, d.id_key, d.document_bson, d.document_checksum,
                d.storage_format_version, e.entry_checksum, e.entry_format_version, e.index_key
         FROM frontier AS f
         CROSS JOIN briskdb_documents_v1 AS d
           ON d.collection_id = ?1 AND d.id_key = f.id_key
         CROSS JOIN briskdb_document_index_entries_v1 AS e
         INDEXED BY sqlite_autoindex_briskdb_document_index_entries_v1_1
           ON e.collection_id = ?1 AND e.index_id = ?4
              AND e.index_key = f.index_key AND e.id_key = f.id_key
         ORDER BY f.natural_order"
    )
}

#[cfg(test)]
mod tests;
