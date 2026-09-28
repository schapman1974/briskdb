//! Global secondary-key validation. Callers own collection or schema fencing.

use super::*;
use std::sync::{Mutex, MutexGuard};

#[cfg(test)]
mod tests;

/// Private OS-read-only handles, never public SQL connections or cached read
/// authority. At most one idle handle per shard and sixteen per Storage root.
/// Active readers are still bounded by admitted document workers; checking out
/// a child never waits for another shard's connection permit while holding a
/// collection fence. No filesystem I/O occurs while holding this mutex.
#[derive(Debug, Default)]
pub(in crate::storage) struct PeerReaders {
    idle: Mutex<Vec<(u16, Connection)>>,
    #[cfg(test)]
    opened: std::sync::atomic::AtomicUsize,
}

const MAX_IDLE_PEER_READERS: usize = 16;

impl PeerReaders {
    fn lock(&self) -> EngineResult<MutexGuard<'_, Vec<(u16, Connection)>>> {
        self.idle.lock().map_err(|_| {
            EngineError::new(
                EngineErrorKind::Internal,
                "document peer reader pool is poisoned",
            )
        })
    }

    fn take(&self, shard: u16) -> EngineResult<Option<Connection>> {
        let mut idle = self.lock()?;
        Ok(idle
            .iter()
            .position(|(id, _)| *id == shard)
            .map(|index| idle.swap_remove(index).1))
    }

    fn put(&self, shard: u16, connection: Connection) {
        // Allocation pressure or a poisoned cache must not turn a completed
        // read into an error. Drop the handle instead. A caller only returns
        // successful, idle handles with its request's progress hook removed.
        let mut idle = match self.lock() {
            Ok(idle) => idle,
            Err(_) => return,
        };
        if idle.len() < MAX_IDLE_PEER_READERS
            && !idle.iter().any(|(id, _)| *id == shard)
            && idle.try_reserve(1).is_ok()
        {
            idle.push((shard, connection));
        }
    }

    pub(in crate::storage) fn close_idle(&self) -> EngineResult<usize> {
        let closing = std::mem::take(&mut *self.lock()?);
        let count = closing.len();
        drop(closing);
        Ok(count)
    }
}

/// Private temporary SQLite storage bounds the resident key set during offline
/// build/startup validation. The empty filename is a disk-backed temporary
/// database, deleted by SQLite on close; it is not a persistent root authority.
pub(super) struct UniqueKeyScratch {
    connection: Connection,
}

impl UniqueKeyScratch {
    pub(super) fn new(control: Option<Arc<OperationControl>>) -> EngineResult<Self> {
        let connection = Connection::open("").map_err(scratch_error)?;
        connection
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(scratch_error)?;
        if let Some(control) = control {
            connection
                .progress_handler(1_000, Some(move || control.should_stop()))
                .map_err(scratch_error)?;
        }
        connection
            .execute_batch(
                "PRAGMA cache_size = -1024;
             CREATE TABLE unique_keys (
                 index_id INTEGER NOT NULL,
                 index_key BLOB NOT NULL,
                 owner BLOB NOT NULL,
                 PRIMARY KEY (index_id, index_key)
             ) STRICT, WITHOUT ROWID;
             BEGIN IMMEDIATE",
            )
            .map_err(scratch_error)?;
        Ok(Self { connection })
    }

    /// Return the conflicting index, without exposing document IDs or values.
    /// An index's multikey duplicates for the same record are not conflicts.
    pub(super) fn add(
        &self,
        entries: &PreparedDocumentIndexEntries,
        owner: &[u8],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<DocumentIndexId>> {
        check()?;
        let mut insert = self
            .connection
            .prepare_cached(
                "INSERT INTO unique_keys VALUES (?1, ?2, ?3)
             ON CONFLICT (index_id, index_key) DO NOTHING",
            )
            .map_err(scratch_error)?;
        let mut existing = self
            .connection
            .prepare_cached(
                "SELECT owner = ?3 FROM unique_keys WHERE index_id = ?1 AND index_key = ?2",
            )
            .map_err(scratch_error)?;
        for index in entries.indexes().iter().filter(|index| index.is_unique()) {
            for key in index.keys() {
                check()?;
                let parameters = params![index.index_id().get() as i64, key, owner];
                if insert.execute(parameters).map_err(scratch_error)? == 0
                    && !existing
                        .query_row(parameters, |row| row.get::<_, bool>(0))
                        .map_err(scratch_error)?
                {
                    return Ok(Some(index.index_id()));
                }
            }
        }
        check()?;
        Ok(None)
    }
}

fn scratch_error(error: rusqlite::Error) -> EngineError {
    let error = sqlite_error::storage(error);
    if error.kind() == EngineErrorKind::DataCorruption {
        // This file is private derived scratch, never evidence that the user's
        // durable root is damaged. In particular do not degrade a healthy root.
        EngineError::from_source(
            EngineErrorKind::StorageUnavailable,
            "temporary unique-index validation storage failed",
            error,
        )
    } else {
        error.context("temporary unique-index validation storage failed")
    }
}

pub(super) fn duplicate() -> EngineError {
    EngineError::new(
        EngineErrorKind::UniqueViolation,
        "duplicate key violates a document secondary unique index",
    )
}

/// A child read never re-arms the parent's interrupt handle or installs another
/// thread-local busy registration. Its own progress hook observes the same
/// cancellation/deadline, and lock contention fails promptly with Busy.
/// Validation and the subsequent key probe share one fresh read transaction.
/// Otherwise every metadata query starts and ends its own SQLite read, repeating
/// file/lock checks and permitting validation and the probe to see different
/// snapshots. Only a successfully completed read can return its handle to the
/// bounded cache: no snapshot, lock, or cancellation hook survives that read.
pub(super) fn with_peer<T>(
    storage: &Storage,
    shard: u16,
    control: Option<Arc<OperationControl>>,
    cancellation: CancellationToken,
    read: impl FnOnce(&Connection) -> EngineResult<T>,
) -> EngineResult<T> {
    storage.ensure_shard_in_range(shard)?;
    check_active(control.as_deref(), &cancellation)?;
    let path = storage.shard_path(shard);
    let cached = storage.document_peer_readers.take(shard)?;
    let connection = match cached {
        Some(connection)
            if connection.is_autocommit()
                && crate::storage::shard::pooled_file_is_current(&connection, &path)? =>
        {
            connection
        }
        _ => {
            // An unavailable VFS identity probe falls back to a strict fresh
            // open. A missing/replaced/symlinked path fails rather than trusting
            // the old descriptor. Never cache a file-identity failure.
            let connection = crate::storage::shard::open_required_file_read_only(&path)?;
            #[cfg(test)]
            storage
                .document_peer_readers
                .opened
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            connection
        }
    };
    connection
        .busy_timeout(std::time::Duration::ZERO)
        .map_err(sqlite_error::storage)?;
    let progress_control = control.clone();
    let progress_cancellation = cancellation.clone();
    connection
        .progress_handler(
            1_000,
            Some(move || {
                progress_cancellation.is_cancelled()
                    || progress_control
                        .as_ref()
                        .is_some_and(|control| control.should_stop())
            }),
        )
        .map_err(sqlite_error::storage)?;
    // This dedicated OS-read-only handle executes only storage-owned bound
    // queries. Do not install the public SQL authorizer: standalone imports do
    // not carry the engine pool's thread-local document callback registration.
    let result = (|| {
        storage
            .shard_layout
            .journal()
            .configure_durability(&connection)?;
        let transaction = Transaction::new_unchecked(&connection, TransactionBehavior::Deferred)
            .map_err(sqlite_error::storage)?;
        let generation = storage.catalog.logical().schema_generation();
        crate::storage::shard::validate_open_read_only_connection(
            &transaction,
            &path,
            shard,
            generation,
            &storage.shard_layout,
        )?;
        let digest = storage.schema_coordination.committed_schema_digest()?;
        crate::storage::shard::verify_schema_digest(&transaction, generation, &digest)?;
        storage.validate_native_range_v1_state(&transaction, shard)?;
        require_schema(&transaction)?;
        let value = read(&transaction)?;
        check_active(control.as_deref(), &cancellation)?;
        transaction.commit().map_err(sqlite_error::storage)?;
        Ok(value)
    })();
    let value = normalize(result, control.as_deref(), &cancellation)?;
    connection
        .progress_handler(0, None::<fn() -> bool>)
        .map_err(sqlite_error::storage)?;
    check_active(control.as_deref(), &cancellation)?;
    if connection.is_autocommit() {
        storage.document_peer_readers.put(shard, connection);
    }
    Ok(value)
}

pub(super) fn check_active(
    control: Option<&OperationControl>,
    cancellation: &CancellationToken,
) -> EngineResult<()> {
    if let Some(reason) = control.and_then(OperationControl::reason) {
        return Err(reason.error());
    }
    ensure_document_operation_not_cancelled(cancellation, "while validating document uniqueness")
}

pub(super) fn normalize<T>(
    result: EngineResult<T>,
    control: Option<&OperationControl>,
    cancellation: &CancellationToken,
) -> EngineResult<T> {
    match result {
        Err(error)
            if error.kind() == EngineErrorKind::DataCorruption
                && !pool::has_generic_sqlite_failure(&error) =>
        {
            Err(error)
        }
        Err(error) => check_active(control, cancellation).and(Err(error)),
        Ok(value) => {
            check_active(control, cancellation)?;
            Ok(value)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_on_shard(
    storage: &Storage,
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    owner_shard: u16,
    owner: &CanonicalBsonKey,
    entries: &PreparedDocumentIndexEntries,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    // One key at a time bounds SQLite variables and avoids retaining additional
    // key copies. A conflict terminates immediately; the physical PK probes
    // directly by collection/index/key rather than scanning the collection.
    let mut statement = connection
        .prepare_cached(
            "SELECT e.id_key, d.natural_order, d.document_bson, d.document_checksum,
                d.storage_format_version, e.entry_checksum, e.entry_format_version
         FROM briskdb_document_index_entries_v1 AS e
         LEFT JOIN briskdb_documents_v1 AS d
           ON d.collection_id = e.collection_id AND d.id_key = e.id_key
         WHERE e.collection_id = ?1 AND e.index_id = ?2 AND e.index_key = ?3
           AND (?5 <> ?6 OR e.id_key <> ?4) LIMIT 1",
        )
        .map_err(|error| shard_read_error(error, "failed to prepare document unique-key lookup"))?;
    for index in entries.indexes().iter().filter(|index| index.is_unique()) {
        for key in index.keys() {
            check()?;
            let mut rows = statement
                .query(params![
                    collection.get() as i64,
                    index.index_id().get() as i64,
                    key,
                    owner.as_bytes(),
                    shard,
                    owner_shard
                ])
                .map_err(|error| {
                    shard_read_error(error, "failed to read document unique-key candidates")
                })?;
            if let Some(row) = rows.next().map_err(|error| {
                shard_read_error(error, "failed to read document unique-key candidate")
            })? {
                let read = || -> rusqlite::Result<_> {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                };
                let (id, order, bson, checksum, version, entry_checksum, entry_version) = read()
                    .map_err(|error| {
                        shard_read_error(error, "invalid document unique-key candidate")
                    })?;
                let canonical = CanonicalBsonKey::from_bytes(&id)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
                if storage.shard_for_key(canonical.as_bytes()) != shard {
                    return Err(corrupt(
                        "document unique-key candidate is on the wrong shard",
                    ));
                }
                let record =
                    decode_storage_record(collection, shard, order, id, bson, checksum, version)?;
                super::super::index_storage::validate_probe_entry(
                    collection,
                    index.index_id(),
                    shard,
                    record.id_key.as_bytes(),
                    key,
                    &record.checksum,
                    &entry_checksum,
                    entry_version,
                )?;
                check()?;
                return Err(duplicate());
            }
        }
    }
    check()
}
