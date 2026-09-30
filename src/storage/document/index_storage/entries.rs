//! Record-bound secondary entries. Only callers holding schema authority may
//! choose the index set; preparation alone is not a catalog-freshness proof.

use std::collections::{HashMap, HashSet};

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    document::{DocumentCollectionId, DocumentIndexId, PreparedDocumentIndexEntries},
    sqlite_error,
};

use super::{corrupt, shard_read_error};

const ENTRY_DOMAIN: &[u8] = b"briskdb.document-index-entry.v1\0";
const MAX_RECORD_ENTRIES: usize = 16_384;
// The primary key's ordering can tempt SQLite into scanning the collection
// for every record. The validated by-record index bounds this integrity check
// to this record's entries even while a new index is being built (#553).
const RECORD_ENTRIES_SQL: &str = "SELECT index_id, index_key, entry_checksum, entry_format_version
    FROM briskdb_document_index_entries_v1
    INDEXED BY briskdb_document_index_entries_by_record_v1
    WHERE collection_id = ?1 AND id_key = ?2
    ORDER BY index_id, index_key LIMIT 16385";

#[cfg(test)]
mod tests;

fn sqlite_id(id: u64) -> EngineResult<i64> {
    i64::try_from(id).map_err(|_| corrupt("document index identity exceeds its storage range"))
}

fn require_transaction(transaction: &Transaction<'_>) -> EngineResult<()> {
    if transaction.is_autocommit() {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "document index maintenance requires an active write transaction",
        ));
    }
    Ok(())
}

fn checksum(
    collection: DocumentCollectionId,
    index: DocumentIndexId,
    shard: u16,
    id_key: &[u8],
    index_key: &[u8],
    record_checksum: &[u8; 32],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(ENTRY_DOMAIN);
    hash.update(&1_u32.to_le_bytes());
    hash.update(&collection.get().to_le_bytes());
    hash.update(&index.get().to_le_bytes());
    hash.update(&shard.to_le_bytes());
    hash.update(&(id_key.len() as u64).to_le_bytes());
    hash.update(id_key);
    hash.update(&(index_key.len() as u64).to_le_bytes());
    hash.update(index_key);
    // Binding the exact record checksum catches stale entries even when an
    // update leaves its indexed semantic key unchanged.
    hash.update(record_checksum);
    *hash.finalize().as_bytes()
}

/// Validate the returned candidate's entry against the same SQLite snapshot's
/// exact record. An equality index narrows candidates, never trusts stale keys.
#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn validate_probe_entry(
    collection: DocumentCollectionId,
    index: DocumentIndexId,
    shard: u16,
    id_key: &[u8],
    index_key: &[u8],
    record_checksum: &[u8; 32],
    stored_checksum: &[u8],
    version: i64,
) -> EngineResult<()> {
    if version != 1
        || stored_checksum != checksum(collection, index, shard, id_key, index_key, record_checksum)
    {
        return Err(corrupt("document index candidate is stale or damaged"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn insert_entries(
    transaction: &Transaction<'_>,
    collection: DocumentCollectionId,
    shard: u16,
    id_key: &[u8],
    record_checksum: &[u8; 32],
    prepared: &PreparedDocumentIndexEntries,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    insert_selected_entries(
        transaction,
        collection,
        shard,
        id_key,
        record_checksum,
        prepared,
        None,
        check,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn insert_selected_entries(
    transaction: &Transaction<'_>,
    collection: DocumentCollectionId,
    shard: u16,
    id_key: &[u8],
    record_checksum: &[u8; 32],
    prepared: &PreparedDocumentIndexEntries,
    only: Option<DocumentIndexId>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    require_transaction(transaction)?;
    check()?;
    if prepared.collection_id() != collection {
        return Err(corrupt(
            "prepared document index entries belong to another collection",
        ));
    }
    if only.is_some_and(|id| {
        !prepared
            .indexes()
            .iter()
            .any(|index| index.index_id() == id)
    }) {
        return Err(corrupt(
            "document index build preparation omitted its target",
        ));
    }
    let mut statement = transaction
        .prepare_cached(
            "INSERT INTO briskdb_document_index_entries_v1
         (collection_id, index_id, id_key, index_key, entry_checksum, entry_format_version)
         VALUES (?1, ?2, ?3, ?4, ?5, 1)",
        )
        .map_err(sqlite_error::storage)?;
    for index in prepared.indexes() {
        if only.is_some_and(|id| index.index_id() != id) {
            continue;
        }
        for key in index.keys() {
            check()?;
            let digest = checksum(
                collection,
                index.index_id(),
                shard,
                id_key,
                key,
                record_checksum,
            );
            statement
                .execute(params![
                    sqlite_id(collection.get())?,
                    sqlite_id(index.index_id().get())?,
                    id_key,
                    key,
                    digest.as_slice()
                ])
                .map_err(|error| {
                    shard_read_error(error, "failed to store a document index entry")
                })?;
        }
    }
    super::super::ordered_storage::insert_selected(
        transaction,
        collection,
        shard,
        id_key,
        record_checksum,
        prepared,
        only,
        check,
    )
}

pub(in crate::storage::document) fn remove_record_entries(
    transaction: &Transaction<'_>,
    collection: DocumentCollectionId,
    id_key: &[u8],
) -> EngineResult<()> {
    require_transaction(transaction)?;
    transaction.execute(
        "DELETE FROM briskdb_document_index_entries_v1 WHERE collection_id = ?1 AND id_key = ?2",
        params![sqlite_id(collection.get())?, id_key],
    ).map_err(sqlite_error::storage)?;
    super::super::ordered_storage::remove_record(transaction, collection, id_key)
}

/// Check both directions: every expected key is present, and every stored key
/// is expected and bound to the current exact BSON record. No repair occurs.
#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn validate_record_entries(
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    id_key: &[u8],
    record_checksum: &[u8; 32],
    expected: Option<&PreparedDocumentIndexEntries>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    let ordered = super::super::ordered_storage::validate_optional_schema(connection)?;
    validate_record_entries_with_layout(
        connection,
        collection,
        shard,
        id_key,
        record_checksum,
        expected,
        ordered,
        None,
        check,
    )
}

/// Audits/builds inspect the schema once per admitted shard, not once per row.
/// `record_natural_order` must come from the checksum-validated source record,
/// never from an index entry. Reuse it to avoid seeking that record again.
#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn validate_record_entries_with_layout(
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    id_key: &[u8],
    record_checksum: &[u8; 32],
    expected: Option<&PreparedDocumentIndexEntries>,
    ordered: bool,
    record_natural_order: Option<i64>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    validate_equality_record_entries(
        connection,
        collection,
        shard,
        id_key,
        record_checksum,
        expected,
        check,
    )?;
    super::super::ordered_storage::validate_record(
        connection,
        collection,
        shard,
        id_key,
        record_checksum,
        expected,
        ordered,
        record_natural_order,
        check,
    )
}

/// Equality coverage only; startup separately consumes the ordered-entry stream
/// in the same shard snapshot. Point/build callers use the combined wrapper.
#[allow(clippy::too_many_arguments)]
pub(in crate::storage::document) fn validate_equality_record_entries(
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    id_key: &[u8],
    record_checksum: &[u8; 32],
    expected: Option<&PreparedDocumentIndexEntries>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    let mut missing = HashMap::<u64, HashSet<&[u8]>>::new();
    let mut missing_count = 0;
    if let Some(expected) = expected {
        if expected.collection_id() != collection {
            return Err(corrupt(
                "document index validation used a different collection",
            ));
        }
        for index in expected.indexes() {
            missing.try_reserve(1).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::OutOfMemory,
                    "unable to validate bounded document indexes",
                    error,
                )
            })?;
            let mut keys = HashSet::new();
            for key in index.keys() {
                check()?;
                if missing_count >= MAX_RECORD_ENTRIES {
                    return Err(corrupt("document index entry coverage exceeds its bound"));
                }
                keys.try_reserve(1).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::OutOfMemory,
                        "unable to validate bounded document index coverage",
                        error,
                    )
                })?;
                if !keys.insert(key.as_slice()) {
                    return Err(corrupt("prepared document index keys contain a duplicate"));
                }
                missing_count += 1;
            }
            if missing.insert(index.index_id().get(), keys).is_some() {
                return Err(corrupt(
                    "prepared document index identities contain a duplicate",
                ));
            }
        }
    }
    let mut statement = connection
        .prepare_cached(RECORD_ENTRIES_SQL)
        .map_err(|error| shard_read_error(error, "failed to inspect document index entries"))?;
    let mut rows = statement
        .query(params![sqlite_id(collection.get())?, id_key])
        .map_err(|error| shard_read_error(error, "failed to inspect document index entries"))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to read document index entry"))?
    {
        check()?;
        let index: i64 = row
            .get(0)
            .map_err(|error| shard_read_error(error, "invalid document index identity"))?;
        let index = u64::try_from(index)
            .ok()
            .filter(|id| *id > 0)
            .ok_or_else(|| corrupt("invalid document index identity"))?;
        // Borrow frame/checksum bytes from SQLite; never allocate a second
        // copy of a large stored frame just to reject it.
        let key = row
            .get_ref(1)
            .and_then(|value| value.as_blob().map_err(Into::into))
            .map_err(|error| shard_read_error(error, "invalid document index key"))?;
        let stored = row
            .get_ref(2)
            .and_then(|value| value.as_blob().map_err(Into::into))
            .map_err(|error| shard_read_error(error, "invalid document index checksum"))?;
        let version: i64 = row
            .get(3)
            .map_err(|error| shard_read_error(error, "invalid document index entry version"))?;
        if version != 1
            || !missing.get_mut(&index).is_some_and(|keys| keys.remove(key))
            || stored
                != checksum(
                    collection,
                    DocumentIndexId::from_validated(index),
                    shard,
                    id_key,
                    key,
                    record_checksum,
                )
        {
            return Err(corrupt(
                "document index entries are stale, damaged, or lack catalog authority",
            ));
        }
        missing_count -= 1;
    }
    if missing_count != 0 {
        return Err(corrupt(
            "document index is missing authoritative record entries",
        ));
    }
    check()
}

/// Startup audits every collection; an index build audits only its own.
/// Use separate SQL shapes so the scoped case seeks the collection key prefix
/// instead of scanning unrelated entries through an optional-filter predicate.
/// Ordered EXCEPT merges the two key streams instead of seeking into the large
/// WITHOUT ROWID record for every secondary entry (and rereading overflow pages).
/// These sources are validated by require_schema before use; select their
/// physical key order explicitly so no temporary sort is needed.
/// Query the first orphan directly: EXISTS can discard ORDER BY and switch the
/// compound query back to a temporary B-tree instead of a streaming merge.
fn orphan_check_sql(scoped: bool) -> &'static str {
    if scoped {
        "SELECT collection_id, id_key FROM briskdb_document_index_entries_v1
         INDEXED BY briskdb_document_index_entries_by_record_v1 WHERE collection_id = ?1
         EXCEPT
         SELECT collection_id, id_key FROM briskdb_documents_v1 NOT INDEXED
         WHERE collection_id = ?1
         ORDER BY collection_id, id_key LIMIT 1"
    } else {
        "SELECT collection_id, id_key FROM briskdb_document_index_entries_v1
         INDEXED BY briskdb_document_index_entries_by_record_v1
         EXCEPT
         SELECT collection_id, id_key FROM briskdb_documents_v1 NOT INDEXED
         ORDER BY collection_id, id_key LIMIT 1"
    }
}

pub(in crate::storage::document) fn require_no_orphans(
    connection: &Connection,
    collection: Option<DocumentCollectionId>,
) -> EngineResult<()> {
    let sql = orphan_check_sql(collection.is_some());
    let sqlite_collection = collection.map(|id| sqlite_id(id.get())).transpose()?;
    let orphan = connection
        .query_row(sql, rusqlite::params_from_iter(sqlite_collection), |_| {
            Ok(())
        })
        .optional()
        .map_err(|error| {
            shard_read_error(error, "failed to validate document index record ownership")
        })?
        .is_some();
    if orphan {
        return Err(corrupt("document index entry has no owning record"));
    }
    super::super::ordered_storage::require_no_orphans(connection, collection)
}
