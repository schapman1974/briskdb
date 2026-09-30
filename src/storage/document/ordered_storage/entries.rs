use super::{corrupt, shard_read_error, validate_optional_schema};
use crate::{
    core::EngineResult,
    document::{DocumentCollectionId, DocumentIndexId, PreparedDocumentIndexEntries},
    sqlite_error,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params, types::ValueRef};

#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn checksum(
    collection: DocumentCollectionId,
    index: DocumentIndexId,
    shard: u16,
    id: &[u8],
    direction: u8,
    key: Option<&[u8]>,
    natural: i64,
    record: &[u8; 32],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"briskdb.document-ordered-entry.v1\0");
    hash.update(&1_u32.to_le_bytes());
    hash.update(&collection.get().to_le_bytes());
    hash.update(&index.get().to_le_bytes());
    hash.update(&shard.to_le_bytes());
    hash.update(&(id.len() as u64).to_le_bytes());
    hash.update(id);
    hash.update(&[direction, u8::from(key.is_some())]);
    if let Some(key) = key {
        hash.update(&(key.len() as u64).to_le_bytes());
        hash.update(key);
    }
    hash.update(&natural.to_le_bytes());
    hash.update(record);
    *hash.finalize().as_bytes()
}

fn natural_order(
    connection: &Connection,
    collection: DocumentCollectionId,
    id: &[u8],
) -> EngineResult<i64> {
    connection.query_row(
        "SELECT natural_order FROM briskdb_documents_v1 WHERE collection_id = ?1 AND id_key = ?2",
        params![collection.get() as i64, id], |row| row.get(0),
    ).map_err(|error| shard_read_error(error, "ordered index record is missing"))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn insert_selected(
    transaction: &Transaction<'_>,
    collection: DocumentCollectionId,
    shard: u16,
    id: &[u8],
    record: &[u8; 32],
    prepared: &PreparedDocumentIndexEntries,
    only: Option<DocumentIndexId>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    if !prepared
        .indexes()
        .iter()
        .any(|index| index.ordered_keys().is_some() && only.is_none_or(|id| id == index.index_id()))
    {
        return check();
    }
    let natural = natural_order(transaction, collection, id)?;
    let mut statement = transaction.prepare_cached(
        "INSERT INTO briskdb_document_ordered_entries_v1
        (collection_id, id_key, index_id, direction, sort_key, natural_order, entry_checksum, entry_format_version)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)"
    ).map_err(sqlite_error::storage)?;
    for index in prepared.indexes() {
        if only.is_some_and(|id| id != index.index_id()) {
            continue;
        }
        let Some(keys) = index.ordered_keys() else {
            continue;
        };
        for (direction, key) in keys.iter().enumerate() {
            check()?;
            let digest = checksum(
                collection,
                index.index_id(),
                shard,
                id,
                direction as u8,
                key.as_deref(),
                natural,
                record,
            );
            statement
                .execute(params![
                    collection.get() as i64,
                    id,
                    index.index_id().get() as i64,
                    direction as i64,
                    key,
                    natural,
                    digest.as_slice()
                ])
                .map_err(|error| {
                    shard_read_error(error, "failed to store ordered document index entry")
                })?;
        }
    }
    check()
}

pub(in crate::storage::document) fn remove_record(
    transaction: &Transaction<'_>,
    collection: DocumentCollectionId,
    id: &[u8],
) -> EngineResult<()> {
    if validate_optional_schema(transaction)? {
        transaction.execute("DELETE FROM briskdb_document_ordered_entries_v1 WHERE collection_id = ?1 AND id_key = ?2", params![collection.get() as i64, id])
            .map_err(sqlite_error::storage)?;
    }
    Ok(())
}

pub(in crate::storage::document) fn remove_index(
    transaction: &Transaction<'_>,
    index: DocumentIndexId,
) -> EngineResult<()> {
    if validate_optional_schema(transaction)? {
        transaction
            .execute(
                "DELETE FROM briskdb_document_ordered_entries_v1 WHERE index_id = ?1",
                [index.get() as i64],
            )
            .map_err(sqlite_error::storage)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn validate_record(
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    id: &[u8],
    record: &[u8; 32],
    expected: Option<&PreparedDocumentIndexEntries>,
    ordered: bool,
    record_natural_order: Option<i64>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    let expected_count = expected_ordered_count(expected);
    if !ordered {
        return if expected_count == 0 {
            Ok(())
        } else {
            Err(corrupt("ordered index coverage is missing"))
        };
    }
    let mut statement = connection.prepare_cached(
        "SELECT index_id, direction, sort_key, natural_order, entry_checksum, entry_format_version
         FROM briskdb_document_ordered_entries_v1 WHERE collection_id = ?1 AND id_key = ?2
         ORDER BY index_id, direction LIMIT 129"
    ).map_err(sqlite_error::storage)?;
    let mut rows = statement
        .query(params![collection.get() as i64, id])
        .map_err(sqlite_error::storage)?;
    let mut count = 0_usize;
    let mut natural = record_natural_order;
    while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
        check()?;
        count += 1;
        let natural = match natural {
            Some(value) => value,
            None => {
                let value = natural_order(connection, collection, id)?;
                natural = Some(value);
                value
            }
        };
        validate_ordered_row(row, collection, shard, id, record, expected, natural, count)?;
    }
    if count != expected_count {
        return Err(corrupt("ordered document index coverage is incomplete"));
    }
    check()
}

pub(super) fn expected_ordered_count(expected: Option<&PreparedDocumentIndexEntries>) -> usize {
    expected
        .into_iter()
        .flat_map(|entries| entries.indexes())
        .filter(|index| index.ordered_keys().is_some())
        .count()
        * 2
}

/// Shared by point/build validation and the startup stream. Borrow keys and
/// checksums directly from SQLite; validate both independently derived directions.
#[allow(clippy::too_many_arguments)]
pub(super) fn validate_ordered_row(
    row: &rusqlite::Row<'_>,
    collection: DocumentCollectionId,
    shard: u16,
    id: &[u8],
    record: &[u8; 32],
    expected: Option<&PreparedDocumentIndexEntries>,
    natural: i64,
    count: usize,
) -> EngineResult<()> {
    let index: i64 = row.get(0).map_err(sqlite_error::storage)?;
    let direction: i64 = row.get(1).map_err(sqlite_error::storage)?;
    let key = match row.get_ref(2).map_err(sqlite_error::storage)? {
        ValueRef::Null => None,
        ValueRef::Blob(key) if (9..=65536).contains(&key.len()) => Some(key),
        _ => return Err(corrupt("ordered index key has an invalid type or size")),
    };
    let stored_natural: i64 = row.get(3).map_err(sqlite_error::storage)?;
    let digest = row
        .get_ref(4)
        .and_then(|value| value.as_blob().map_err(Into::into))
        .map_err(sqlite_error::storage)?;
    let version: i64 = row.get(5).map_err(sqlite_error::storage)?;
    let Some((index, keys)) = expected
        .into_iter()
        .flat_map(|entries| entries.indexes())
        .find(|candidate| candidate.index_id().get() as i64 == index)
        .and_then(|index| index.ordered_keys().map(|keys| (index.index_id(), keys)))
    else {
        return Err(corrupt("ordered index entry has no active authority"));
    };
    if !(0..=1).contains(&direction)
        || natural <= 0
        || count > 128
        || version != 1
        || stored_natural != natural
        || key != keys[direction as usize].as_deref()
        || digest
            != checksum(
                collection,
                index,
                shard,
                id,
                direction as u8,
                key,
                natural,
                record,
            )
    {
        return Err(corrupt("ordered document index entry is stale or damaged"));
    }
    Ok(())
}

pub(in crate::storage::document) fn require_no_orphans(
    connection: &Connection,
    collection: Option<DocumentCollectionId>,
) -> EngineResult<()> {
    if !validate_optional_schema(connection)? {
        return Ok(());
    }
    let sql = if collection.is_some() {
        "SELECT collection_id, id_key FROM briskdb_document_ordered_entries_v1 NOT INDEXED WHERE collection_id = ?1
         EXCEPT SELECT collection_id, id_key FROM briskdb_documents_v1 NOT INDEXED WHERE collection_id = ?1
         ORDER BY collection_id, id_key LIMIT 1"
    } else {
        "SELECT collection_id, id_key FROM briskdb_document_ordered_entries_v1 NOT INDEXED
         EXCEPT SELECT collection_id, id_key FROM briskdb_documents_v1 NOT INDEXED
         ORDER BY collection_id, id_key LIMIT 1"
    };
    let orphan = connection
        .query_row(
            sql,
            rusqlite::params_from_iter(collection.map(|id| id.get() as i64)),
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error::storage)?;
    if orphan.is_some() {
        return Err(corrupt("ordered index entry references a missing document"));
    }
    Ok(())
}
