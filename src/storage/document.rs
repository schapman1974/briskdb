//! Versioned document catalog lifecycle and shard-local BSON records.

use rusqlite::Connection;
#[cfg(feature = "documents")]
use rusqlite::TransactionBehavior;

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

use super::Storage;

mod index_storage;

pub(super) const RECORDS_TABLE: &str = "briskdb_documents_v1";
const RECORDS_SCHEMA_SQL: &str = "CREATE TABLE briskdb_documents_v1 (
    collection_id INTEGER NOT NULL CHECK (collection_id > 0),
    id_key BLOB NOT NULL CHECK (
        typeof(id_key) = 'blob' AND length(id_key) BETWEEN 9 AND 16777216
    ),
    natural_order INTEGER NOT NULL CHECK (natural_order > 0),
    document_bson BLOB NOT NULL CHECK (
        typeof(document_bson) = 'blob' AND length(document_bson) BETWEEN 5 AND 16777216
    ),
    document_checksum BLOB NOT NULL CHECK (
        typeof(document_checksum) = 'blob' AND length(document_checksum) = 32
    ),
    storage_format_version INTEGER NOT NULL CHECK (storage_format_version = 1),
    PRIMARY KEY (collection_id, id_key),
    UNIQUE (collection_id, natural_order)
) STRICT, WITHOUT ROWID";

pub(super) fn is_exact_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> bool {
    (object_type == "table"
        && name == RECORDS_TABLE
        && table_name == RECORDS_TABLE
        && sql.is_some_and(|sql| {
            normalize_schema_sql(sql) == normalize_schema_sql(RECORDS_SCHEMA_SQL)
        }))
        || index_storage::is_exact_schema_object(object_type, name, table_name, sql)
}

pub(super) fn is_storage_table(name: &str) -> bool {
    name.eq_ignore_ascii_case(RECORDS_TABLE)
        || name.eq_ignore_ascii_case(index_storage::ENTRIES_TABLE)
}

pub(super) fn validate_optional_schema(connection: &Connection) -> EngineResult<bool> {
    let objects = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
             WHERE name = ?1 COLLATE NOCASE
             ORDER BY type, name, tbl_name LIMIT 2",
        )
        .and_then(|mut statement| {
            statement
                .query_map([RECORDS_TABLE], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|error| shard_read_error(error, "failed to inspect document storage schema"))?;
    if objects.is_empty() {
        if index_storage::validate_optional_schema(connection)? {
            return Err(corrupt(
                "document index storage exists without document records",
            ));
        }
        return Ok(false);
    }
    if objects.len() != 1
        || !is_exact_schema_object(
            &objects[0].0,
            &objects[0].1,
            &objects[0].2,
            objects[0].3.as_deref(),
        )
    {
        return Err(corrupt(
            "shard document storage table has an incompatible schema",
        ));
    }
    index_storage::validate_optional_schema(connection)?;
    Ok(true)
}

#[cfg(feature = "documents")]
fn ensure_schema(connection: &mut Connection) -> EngineResult<()> {
    if validate_optional_schema(connection)? && index_storage::validate_optional_schema(connection)?
    {
        return Ok(());
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error::storage)?;
    if !validate_optional_schema(&transaction)? {
        transaction
            .execute_batch(RECORDS_SCHEMA_SQL)
            .map_err(sqlite_error::storage)?;
    }
    index_storage::ensure_schema(&transaction)?;
    if !validate_optional_schema(&transaction)? {
        return Err(corrupt(
            "document storage table creation did not produce its exact schema",
        ));
    }
    transaction.commit().map_err(sqlite_error::storage)
}

#[cfg(feature = "documents")]
fn require_schema(connection: &Connection) -> EngineResult<()> {
    if validate_optional_schema(connection)? && index_storage::validate_optional_schema(connection)?
    {
        Ok(())
    } else {
        Err(corrupt(
            "an active document collection is missing shard storage",
        ))
    }
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn corrupt(diagnostic: impl Into<String>) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, diagnostic)
}

fn shard_read_error(error: rusqlite::Error, diagnostic: &'static str) -> EngineError {
    let classified = sqlite_error::storage(error);
    if matches!(
        classified.kind(),
        EngineErrorKind::Busy
            | EngineErrorKind::Cancelled
            | EngineErrorKind::PermissionDenied
            | EngineErrorKind::ReadOnly
            | EngineErrorKind::StorageFull
            | EngineErrorKind::OutOfMemory
            | EngineErrorKind::StorageUnavailable
    ) {
        classified.context(diagnostic)
    } else {
        EngineError::from_source(EngineErrorKind::DataCorruption, diagnostic, classified)
    }
}

#[cfg(feature = "documents")]
mod enabled {
    mod candidate_sql;
    mod index_metadata;
    mod index_operations;
    mod unique;
    mod write_transaction;
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };
    pub(crate) use write_transaction::DocumentWriteTransaction;

    use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

    use crate::{
        core::{CancellationToken, EngineError, EngineErrorKind, EngineResult, OperationControl},
        document::{
            BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey, DocumentCatalog,
            DocumentCollectionId, DocumentCollectionMetadata, DocumentCollectionOptions,
            DocumentDatabaseId, DocumentIndexError, DocumentIndexId, DocumentIndexLifecycle,
            DocumentIndexMetadata, DocumentIndexPreparation, DocumentIndexProbe,
            DocumentIndexSelection, DocumentMatcher, DocumentNamespace, DocumentPlacement,
            PreparedDocumentIndexEntries, encode_document,
        },
        sqlite_error,
    };

    use super::{Storage, corrupt, ensure_schema, require_schema, shard_read_error};
    use crate::storage::{
        SchemaMigrationGuard, configure_journal_mode, configure_manifest_connection,
        configure_manifest_connection_after_busy_setup, manifest, open_existing_manifest, pool,
    };

    const COLLECTION_PROVISIONING: i64 = manifest::DOCUMENT_COLLECTION_PROVISIONING;
    const COLLECTION_ACTIVE: i64 = manifest::DOCUMENT_COLLECTION_ACTIVE;
    const INDEX_READY: i64 = manifest::DOCUMENT_INDEX_READY;
    const INDEX_PENDING_BUILD: i64 = manifest::DOCUMENT_INDEX_PENDING_BUILD;
    const RECORD_CHECKSUM_DOMAIN: &[u8] = b"briskdb.document-record.v1\0";
    pub(crate) const MAX_DOCUMENT_SHARD_SCAN_RECORDS: usize = 4_096;
    const _: () = assert!(
        manifest::MAX_DOCUMENT_INDEXES * (manifest::MAX_DOCUMENT_INDEX_NAME_BYTES + 128)
            + manifest::MAX_DOCUMENT_COLLECTIONS * (255 + 256)
            <= 32 * 1024 * 1024
    );

    pub(in crate::storage) struct DocumentIndexPreparations {
        collections: HashMap<DocumentCollectionId, Arc<DocumentIndexPreparation>>,
        ready_names: HashMap<DocumentNamespace, HashSet<String>>,
    }

    impl std::fmt::Debug for DocumentIndexPreparations {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DocumentIndexPreparations")
                .field("collection_count", &self.collections.len())
                .finish_non_exhaustive()
        }
    }

    impl DocumentIndexPreparations {
        fn get(&self, collection: &DocumentCollectionId) -> Option<&Arc<DocumentIndexPreparation>> {
            self.collections.get(collection)
        }
    }

    fn compile_ready_indexes(
        catalog: &DocumentCatalog,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<DocumentIndexPreparations> {
        compile_indexes_with_candidate(catalog, None, None, check).map_err(stored_index_error)
    }

    fn stored_index_error(error: EngineError) -> EngineError {
        if matches!(
            error.kind(),
            EngineErrorKind::InvalidArgument
                | EngineErrorKind::Unsupported
                | EngineErrorKind::LimitExceeded
                | EngineErrorKind::FailedPrecondition
        ) {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "stored document index authority cannot reproduce its bounded entries",
                error,
            )
        } else {
            error
        }
    }

    fn compile_indexes_with_candidate(
        catalog: &DocumentCatalog,
        candidate: Option<DocumentIndexId>,
        excluded: Option<DocumentIndexId>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<DocumentIndexPreparations> {
        compile_indexes_with_addition(catalog, candidate, excluded, None, check)
    }

    fn compile_indexes_with_addition(
        catalog: &DocumentCatalog,
        candidate: Option<DocumentIndexId>,
        excluded: Option<DocumentIndexId>,
        addition: Option<(DocumentCollectionId, &DocumentIndexMetadata)>,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<DocumentIndexPreparations> {
        let mut compiled = DocumentIndexPreparations {
            collections: HashMap::new(),
            ready_names: HashMap::new(),
        };
        let mut retained = 0_usize;
        let mut names_retained = 0_usize;
        for collection in catalog.collections() {
            check()?;
            // A prospective declaration participates in the same combined
            // bounds without cloning the catalog or publishing draft metadata.
            let metadata = || {
                collection.indexes().iter().chain(
                    addition
                        .filter(|(id, _)| *id == collection.id())
                        .map(|(_, index)| index),
                )
            };
            let preparation = DocumentIndexPreparation::compile_selected_with_check(
                collection.id(),
                metadata(),
                |index| {
                    Some(index.id()) != excluded
                        && (index.lifecycle() == DocumentIndexLifecycle::Ready
                            || Some(index.id()) == candidate)
                },
                check,
            )?;
            if preparation.is_empty() {
                continue;
            }
            retained = retained
                .checked_add(preparation.retained_bytes())
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "compiled document indexes exceed the root memory bound",
                    )
                })?;
            let selected = || {
                metadata().filter(|index| {
                    !index.is_built_in()
                        && Some(index.id()) != excluded
                        && (index.lifecycle() == DocumentIndexLifecycle::Ready
                            || Some(index.id()) == candidate)
                })
            };
            // Dispatch metadata has its own 32-MiB charge ceiling, sufficient
            // for every valid catalog. Do not lower v19's existing 64-MiB
            // definition allowance when opening previously built roots.
            // Admitted lookups perform no I/O or allocation.
            let name_bytes = selected()
                .map(|index| index.name().len() + 128)
                .sum::<usize>()
                + collection.database_name().len()
                + collection.name().len()
                + 256;
            names_retained = names_retained.checked_add(name_bytes).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "compiled document indexes exceed the root memory bound",
                )
            })?;
            if retained > 64 * 1024 * 1024 || names_retained > 32 * 1024 * 1024 {
                return Err(EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "compiled document indexes exceed the root memory bound",
                ));
            }
            let allocation = |error| {
                EngineError::from_source(
                    EngineErrorKind::OutOfMemory,
                    "unable to retain compiled document indexes",
                    error,
                )
            };
            compiled.collections.try_reserve(1).map_err(allocation)?;
            compiled.ready_names.try_reserve(1).map_err(allocation)?;
            let mut names = HashSet::new();
            names.try_reserve(selected().count()).map_err(allocation)?;
            for index in selected() {
                check()?;
                names.insert(index.name().to_owned());
            }
            let namespace = DocumentNamespace::new(collection.database_name(), collection.name())?;
            compiled.ready_names.insert(namespace, names);
            compiled
                .collections
                .insert(collection.id(), Arc::new(preparation));
        }
        Ok(compiled)
    }

    impl Storage {
        /// The caller holds shared schema admission while choosing whether a
        /// drop needs exclusive physical cleanup. Pending drops retain that
        /// shared guard, so a build cannot race this cached decision.
        pub(crate) fn document_index_is_ready(
            &self,
            namespace: &DocumentNamespace,
            name: &str,
        ) -> EngineResult<bool> {
            let current = self
                .schema_coordination
                .document_indexes
                .lock()
                .map_err(|_| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "document index coordination is poisoned",
                    )
                })?;
            let current = current.as_ref().ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "document index authority has not been validated",
                )
            })?;
            Ok(current
                .ready_names
                .get(namespace)
                .is_some_and(|names| names.contains(name)))
        }

        fn publish_document_indexes(&self, indexes: DocumentIndexPreparations) -> EngineResult<()> {
            let mut current = self
                .schema_coordination
                .document_indexes
                .lock()
                .map_err(|_| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "document index coordination is poisoned",
                    )
                })?;
            *current = Some(Arc::new(indexes));
            Ok(())
        }

        /// The caller retains schema admission for the entire operation. Build/drop
        /// publication drains that admission before replacing this shared cache.
        fn active_document_indexes(
            &self,
            collection: DocumentCollectionId,
        ) -> EngineResult<Option<Arc<DocumentIndexPreparation>>> {
            let current = self
                .schema_coordination
                .document_indexes
                .lock()
                .map_err(|_| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "document index coordination is poisoned",
                    )
                })?;
            let current = current.as_ref().ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "document index authority has not been validated",
                )
            })?;
            Ok(current.get(&collection).cloned())
        }

        /// Derive a request-local probe only from the validated Ready cache.
        /// The caller retains schema admission until its final candidate read.
        pub(crate) fn document_equality_probe(
            &self,
            collection: DocumentCollectionId,
            matcher: &DocumentMatcher,
            check: &mut dyn FnMut() -> EngineResult<()>,
        ) -> EngineResult<Option<DocumentIndexProbe>> {
            check()?;
            let Some(indexes) = self.active_document_indexes(collection)? else {
                return Ok(None);
            };
            match indexes.equality_probe_with_check(matcher, check) {
                // Optional optimization work has its own shared bound. Running
                // out of that budget must not reject an otherwise valid scan.
                Err(error) if error.kind() == EngineErrorKind::LimitExceeded => {
                    check()?;
                    Ok(None)
                }
                result => result,
            }
        }
    }

    fn prepare_active_entries(
        preparation: Option<&DocumentIndexPreparation>,
        document: &BsonDocument,
        cancellation: &CancellationToken,
    ) -> EngineResult<Option<PreparedDocumentIndexEntries>> {
        preparation
            .map(|preparation| {
                preparation.prepare_for_storage_with_check(document, &mut || {
                    ensure_document_operation_not_cancelled(
                        cancellation,
                        "while preparing document index entries",
                    )
                })
            })
            .transpose()
    }

    fn prepare_write_entries(
        preparation: Option<&DocumentIndexPreparation>,
        prepared: &PreparedDocumentWrite,
        cancellation: &CancellationToken,
    ) -> EngineResult<Option<PreparedDocumentIndexEntries>> {
        if preparation.is_none() {
            return Ok(None);
        }
        let document = crate::document::decode_document(&prepared.document_bson)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        prepare_active_entries(preparation, &document, cancellation)
    }

    fn validate_record_index_coverage(
        connection: &Connection,
        preparation: Option<&DocumentIndexPreparation>,
        record: &DocumentStorageRecord,
        cancellation: &CancellationToken,
    ) -> EngineResult<()> {
        let expected = prepare_active_entries(preparation, &record.document, cancellation)
            .map_err(stored_index_error)?;
        super::index_storage::validate_record_entries(
            connection,
            record.collection_id,
            record.shard,
            record.id_key.as_bytes(),
            &record.checksum,
            expected.as_ref(),
            &mut || {
                ensure_document_operation_not_cancelled(
                    cancellation,
                    "while validating document index entries",
                )
            },
        )
    }

    /// Exact, validated bytes prepared for one shard-local document write.
    ///
    /// The command engine can retain this value between routing and execution
    /// without exposing SQLite or re-encoding BSON on a blocking worker.
    #[derive(Debug, Clone)]
    pub(crate) struct PreparedDocumentWrite {
        id_key: CanonicalBsonKey,
        document_bson: Vec<u8>,
        shard: u16,
    }

    impl PreparedDocumentWrite {
        pub(crate) const fn shard(&self) -> u16 {
            self.shard
        }

        pub(crate) const fn id_key(&self) -> &CanonicalBsonKey {
            &self.id_key
        }

        pub(crate) fn document_bson_len(&self) -> usize {
            self.document_bson.len()
        }
    }

    /// One checksum-validated shard record returned to the document engine.
    #[derive(Debug, Clone)]
    pub(crate) struct DocumentStorageRecord {
        collection_id: DocumentCollectionId,
        shard: u16,
        natural_order: u64,
        id_key: CanonicalBsonKey,
        document: BsonDocument,
        encoded_len: usize,
        checksum: [u8; 32],
    }

    impl DocumentStorageRecord {
        pub(crate) const fn collection_id(&self) -> DocumentCollectionId {
            self.collection_id
        }

        pub(crate) const fn shard(&self) -> u16 {
            self.shard
        }

        pub(crate) const fn natural_order(&self) -> u64 {
            self.natural_order
        }

        pub(crate) const fn id_key(&self) -> &CanonicalBsonKey {
            &self.id_key
        }

        pub(crate) const fn document(&self) -> &BsonDocument {
            &self.document
        }

        pub(crate) const fn encoded_len(&self) -> usize {
            self.encoded_len
        }

        pub(crate) fn into_document(self) -> BsonDocument {
            self.document
        }
    }

    fn require_ready_manifest(connection: &Connection, shard_count: u16) -> EngineResult<()> {
        let integrity = manifest::current_integrity(connection, shard_count)?;
        if load_deletion(connection)?.is_some() {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "document deletion is incomplete; reopen the database to recover it",
            ));
        }
        let pending: bool = connection.query_row(
            "SELECT lifecycle_state = 2 OR EXISTS (SELECT 1 FROM briskdb_document_index_operation)
             FROM briskdb_document_index_storage WHERE singleton = 1",
            [], |row| row.get(0),
        ).map_err(sqlite_error::storage)?;
        if pending {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "document index storage operation is incomplete; reopen the database to recover it",
            ));
        }
        match integrity.state() {
            manifest::DatabaseIntegrityState::Ready => Ok(()),
            manifest::DatabaseIntegrityState::Degraded => Err(corrupt(
                "document operation found a persistently degraded database",
            )),
            manifest::DatabaseIntegrityState::Verifying
            | manifest::DatabaseIntegrityState::Migrating => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "document operation requires a checksum-verified ready database",
            )),
        }
    }

    #[derive(Debug, Clone)]
    struct Provisioning {
        collection_id: DocumentCollectionId,
        operation_id: [u8; 32],
        shard_count: u16,
        next_shard: u16,
    }

    #[derive(Debug, Clone)]
    struct Deletion {
        database_id: i64,
        collection_id: Option<i64>,
        operation_id: [u8; 32],
        shard_count: u16,
        next_shard: u16,
    }

    // Only the isolated crash-test child configures this hook. Exit without
    // destructors exercises SQLite/WAL and OS-lock recovery, not an error return.
    #[cfg(test)]
    fn deletion_crash_checkpoint(point: &str, shard: u16) {
        if std::env::var("BRISKDB_TEST_DOCUMENT_DROP_CRASH")
            .ok()
            .as_deref()
            == Some(format!("{point}:{shard}").as_str())
        {
            std::process::exit(73);
        }
    }

    enum CreateCollectionStart {
        Existing(DocumentCollectionMetadata),
        Provisioning(Provisioning),
    }

    fn run_dedicated_controlled<T>(
        connection: &mut Connection,
        control: Arc<OperationControl>,
        work: impl FnOnce(&mut Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        pool::run_dedicated_connection_controlled(connection, control, work)
    }

    fn run_manifest_controlled<T>(
        connection: &mut Connection,
        control: Arc<OperationControl>,
        work: impl FnOnce(&mut Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        run_dedicated_controlled(connection, control, |connection| {
            // The controlled helper already owns the busy handler. Calling the
            // ordinary configurator here would replace it with a fixed timeout.
            configure_manifest_connection_after_busy_setup(connection)?;
            work(connection)
        })
    }

    fn ensure_control_active(
        control: &OperationControl,
        boundary: &'static str,
    ) -> EngineResult<()> {
        match control.reason() {
            Some(reason) => Err(reason.error().context(boundary)),
            None => Ok(()),
        }
    }

    fn read_ready_manifest_snapshot<T>(
        connection: &mut Connection,
        shard_count: u16,
        read: impl FnOnce(&Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(sqlite_error::storage)?;
        require_ready_manifest(&transaction, shard_count)?;
        let value = read(&transaction)?;
        transaction.commit().map_err(sqlite_error::storage)?;
        Ok(value)
    }

    impl Storage {
        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn document_catalog(&self) -> EngineResult<DocumentCatalog> {
            let result = self.document_catalog_inner();
            self.fail_closed_on_corruption(result)
        }

        #[cfg(test)]
        pub(crate) fn document_catalog_controlled(
            &self,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentCatalog> {
            // Engine callers already own one schema-operation admission for
            // the complete logical command. Reacquiring here could fail after
            // a migration starts behind that admitted command.
            let result = self.document_catalog_controlled_inner(control);
            self.fail_closed_on_corruption(result)
        }

        /// Load one active collection without materializing unrelated catalog
        /// metadata. Engine callers already own schema-operation admission.
        pub(crate) fn document_collection_controlled(
            &self,
            database: &str,
            collection: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<Option<DocumentCollectionMetadata>> {
            let result = self.document_collection_controlled_inner(
                database,
                collection,
                Arc::clone(&control),
            );
            self.fail_closed_on_corruption(result)
        }

        /// Load active collections in one database without decoding metadata
        /// owned by other namespaces. Engine callers own schema admission.
        pub(crate) fn document_collections_for_database_controlled(
            &self,
            database: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<Vec<DocumentCollectionMetadata>> {
            let result = self.document_collections_for_database_controlled_inner(database, control);
            self.fail_closed_on_corruption(result)
        }

        /// Only document database names; no SQL namespaces, internal tables,
        /// collection metadata, or physical-size estimates. The catalog caps
        /// this result at 64 names of at most 63 UTF-8 bytes each.
        pub(crate) fn document_database_names_controlled(
            &self,
            control: Arc<OperationControl>,
        ) -> EngineResult<Vec<String>> {
            let result = (|| {
                let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                let read_control = Arc::clone(&control);
                run_manifest_controlled(&mut connection, control, |connection| {
                    read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                        let mut statement = connection.prepare(
                            "SELECT database_name FROM briskdb_document_databases ORDER BY database_id",
                        ).map_err(sqlite_error::storage)?;
                        let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
                        let mut names = Vec::new();
                        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
                            ensure_control_active(
                                &read_control,
                                "while reading document database names",
                            )?;
                            names.push(row.get(0).map_err(sqlite_error::storage)?);
                        }
                        ensure_control_active(
                            &read_control,
                            "after reading document database names",
                        )?;
                        Ok(names)
                    })
                })
            })();
            self.fail_closed_on_corruption(result)
        }

        /// Capture a database identity and collection allocation ceiling in one
        /// validated snapshot. An absent database is never created by discovery.
        pub(crate) fn document_metadata_identity_controlled(
            &self,
            database: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<Option<(DocumentDatabaseId, u64)>> {
            let result = (|| {
                let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                run_manifest_controlled(&mut connection, control, |connection| {
                    read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                        connection.query_row(
                            "SELECT database_id, collection_high_water FROM briskdb_document_databases
                             CROSS JOIN briskdb_document_identities WHERE database_name = ?1 AND singleton = 1",
                            [database],
                            |row| Ok((DocumentDatabaseId::from_validated(row.get::<_, i64>(0)? as u64), row.get::<_, i64>(1)? as u64)),
                        ).optional().map_err(sqlite_error::storage)
                    })
                })
            })();
            self.fail_closed_on_corruption(result)
        }

        /// Stream a single metadata page from one manifest snapshot. The visitor
        /// stops before the first matching row that belongs in the next page.
        /// No indexes or unrelated collection options are decoded/materialized.
        pub(crate) fn scan_document_collection_metadata_controlled(
            &self,
            database_id: DocumentDatabaseId,
            bounds: (u64, u64),
            name_only: bool,
            control: Arc<OperationControl>,
            mut visit: impl FnMut(u64, String, Option<BsonDocument>, [u8; 16]) -> EngineResult<bool>,
        ) -> EngineResult<()> {
            let result = (|| {
                let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                let read_control = Arc::clone(&control);
                run_manifest_controlled(&mut connection, control, |connection| {
                    read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                        let exists: bool = connection.query_row(
                            "SELECT EXISTS(SELECT 1 FROM briskdb_document_databases WHERE database_id = ?1)",
                            [database_id.get() as i64], |row| row.get(0),
                        ).map_err(sqlite_error::storage)?;
                        if !exists {
                            return Err(
                                crate::document::DocumentCursorError::NotFound.into_engine_error()
                            );
                        }
                        // Preserve rowid order without sorting large BSON options via
                        // the (database_id, collection_name) secondary index.
                        let mut statement = connection
                            .prepare(
                                "SELECT collection_id, collection_name,
                                    CASE WHEN ?4 THEN NULL ELSE options_bson END
                             FROM briskdb_document_collections NOT INDEXED
                             WHERE database_id = ?1 AND collection_id > ?2 AND collection_id <= ?3
                               AND lifecycle_state = 2 ORDER BY collection_id",
                            )
                            .map_err(sqlite_error::storage)?;
                        let mut rows = statement
                            .query(params![
                                database_id.get() as i64,
                                bounds.0 as i64,
                                bounds.1 as i64,
                                name_only
                            ])
                            .map_err(sqlite_error::storage)?;
                        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
                            ensure_control_active(
                                &read_control,
                                "while reading collection metadata",
                            )?;
                            let id = row.get::<_, i64>(0).map_err(sqlite_error::storage)? as u64;
                            let name = row.get(1).map_err(sqlite_error::storage)?;
                            let options: Option<Vec<u8>> =
                                row.get(2).map_err(sqlite_error::storage)?;
                            let options = options
                                .map(|bytes| {
                                    decode_metadata_document(
                                        &bytes,
                                        "stored collection options are invalid",
                                    )
                                })
                                .transpose()?;
                            let uuid = collection_metadata_uuid(self.shard_layout.layout_id(), id);
                            if !visit(id, name, options, uuid)? {
                                break;
                            }
                        }
                        ensure_control_active(&read_control, "after reading collection metadata")
                    })
                })
            })();
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn document_catalog_inner(&self) -> EngineResult<DocumentCatalog> {
            let _operation = self.enter_schema_operation()?;
            self.document_catalog_controlled_inner(OperationControl::new(None))
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn document_catalog_controlled_inner(
            &self,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentCatalog> {
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            run_manifest_controlled(&mut connection, control, |connection| {
                read_ready_manifest_snapshot(connection, self.shard_count(), load_catalog_rows)
            })
        }

        fn document_collection_controlled_inner(
            &self,
            database: &str,
            collection: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<Option<DocumentCollectionMetadata>> {
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            let read_control = Arc::clone(&control);
            run_manifest_controlled(&mut connection, control, |connection| {
                read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                    load_collection_row(connection, database, collection, read_control.as_ref())
                })
            })
        }

        fn document_collections_for_database_controlled_inner(
            &self,
            database: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<Vec<DocumentCollectionMetadata>> {
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            let read_control = Arc::clone(&control);
            run_manifest_controlled(&mut connection, control, |connection| {
                read_ready_manifest_snapshot(connection, self.shard_count(), |connection| {
                    load_collection_rows_for_database(connection, database, read_control.as_ref())
                })
            })
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn create_document_collection(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
        ) -> EngineResult<DocumentCollectionMetadata> {
            let result = self.create_document_collection_inner(database, collection, options);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn create_document_collection_inner(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
        ) -> EngineResult<DocumentCollectionMetadata> {
            let migration =
                SchemaMigrationGuard::new(self.schema_coordination.gate.begin_new_migration()?);
            migration.wait_for_quiescence_blocking();
            self.create_document_collection_controlled_inner(
                database,
                collection,
                options,
                migration,
                OperationControl::new(None),
            )
        }

        /// Create and provision a collection under a migration guard acquired
        /// before an engine session is leased.
        ///
        /// The caller must acquire the migration guard while holding no
        /// session, preflight and release its target session, await
        /// `migration.wait_for_quiescence()`, then reacquire the session and
        /// move the guard into this method.
        pub(crate) fn create_document_collection_controlled(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
            migration: SchemaMigrationGuard,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentCollectionMetadata> {
            let result = self.create_document_collection_controlled_inner(
                database, collection, options, migration, control,
            );
            self.fail_closed_on_corruption(result)
        }

        fn create_document_collection_controlled_inner(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
            mut migration: SchemaMigrationGuard,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentCollectionMetadata> {
            ensure_control_active(&control, "before creating document collection")?;
            crate::document::validate_namespace(database, collection)?;
            let options_bson = encode_document(options.document())
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            debug_assert!(options_bson.len() <= manifest::MAX_DOCUMENT_METADATA_BSON_BYTES);
            let id_specification = builtin_id_specification()?;
            let id_specification_bson = encode_document(&id_specification)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;

            let result = (|| {
                let manifest_path = self.root.join("manifest.sqlite");
                let mut connection = open_existing_manifest(&manifest_path)?;
                let start = run_manifest_controlled(
                    &mut connection,
                    Arc::clone(&control),
                    |connection| {
                        configure_journal_mode(connection)?;
                        require_ready_manifest(connection, self.shard_count())?;

                        if let Some(existing) =
                            existing_collection(connection, database, collection)?
                        {
                            if existing.1 != options_bson {
                                return Err(EngineError::new(
                                    EngineErrorKind::FailedPrecondition,
                                    "document collection already exists with different options",
                                ));
                            }
                            if existing.0 != COLLECTION_ACTIVE {
                                return Err(EngineError::new(
                                    EngineErrorKind::FailedPrecondition,
                                    "document collection has incomplete durable provisioning; reopen the database to recover it",
                                ));
                            }
                            let metadata = load_catalog_rows(connection)?
                                .collection(database, collection)
                                .cloned()
                                .ok_or_else(|| {
                                    corrupt(
                                        "active document collection disappeared from its catalog",
                                    )
                                })?;
                            return Ok(CreateCollectionStart::Existing(metadata));
                        }
                        if load_provisioning(connection)?.is_some() {
                            return Err(corrupt(
                                "document catalog retained provisioning after startup recovery",
                            ));
                        }

                        let transaction = connection
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(sqlite_error::storage)?;
                        require_ready_manifest(&transaction, self.shard_count())?;
                        let database_id = ensure_database(&transaction, database)?;
                        let collection_id = next_positive_id(
                            &transaction,
                            "collection_high_water",
                            "document collection",
                        )?;
                        let operation_id = provisioning_id(database, collection, &options_bson);
                        transaction
                            .execute(
                                "INSERT INTO briskdb_document_collections (
                                    collection_id, database_id, collection_name, options_bson,
                                    bson_schema_version, storage_format_version,
                                    placement_policy, placement_version, next_natural_order,
                                    lifecycle_state
                                 ) VALUES (?1, ?2, ?3, ?4, 1, 1, 1, 1, 1, ?5)",
                                params![
                                    collection_id,
                                    database_id,
                                    collection,
                                    options_bson,
                                    COLLECTION_PROVISIONING
                                ],
                            )
                            .map_err(sqlite_error::storage)?;
                        transaction
                            .execute(
                                "INSERT INTO briskdb_document_indexes (
                                    collection_id, index_name, spec_bson, is_unique, is_builtin,
                                    index_format_version, lifecycle_state
                                 ) VALUES (?1, '_id_', ?2, 1, 1, 1, ?3)",
                                params![collection_id, id_specification_bson, INDEX_PENDING_BUILD],
                            )
                            .map_err(sqlite_error::storage)?;
                        allocate_index_identity(&transaction, collection_id, "_id_")?;
                        transaction
                            .execute(
                                "INSERT INTO briskdb_document_provisioning (
                                    singleton, collection_id, operation_id, shard_count, next_shard
                                 ) VALUES (1, ?1, ?2, ?3, 0)",
                                params![collection_id, operation_id.as_slice(), self.shard_count()],
                            )
                            .map_err(sqlite_error::storage)?;
                        manifest::validate_document_catalog(&transaction, self.shard_count())?;
                        manifest::refresh_manifest_digest(&transaction)?;
                        require_ready_manifest(&transaction, self.shard_count())?;
                        ensure_control_active(
                            &control,
                            "before committing document collection provisioning",
                        )?;
                        migration.mark_pending_on_drop();
                        transaction.commit().map_err(sqlite_error::storage)?;

                        Ok(CreateCollectionStart::Provisioning(Provisioning {
                            collection_id: DocumentCollectionId::from_validated(
                                u64::try_from(collection_id)
                                    .expect("positive SQLite document ID fits u64"),
                            ),
                            operation_id,
                            shard_count: self.shard_count(),
                            next_shard: 0,
                        }))
                    },
                )?;

                let metadata = match start {
                    CreateCollectionStart::Existing(metadata) => metadata,
                    CreateCollectionStart::Provisioning(provisioning) => {
                        recover_provisioning_controlled(
                            self,
                            &mut connection,
                            provisioning,
                            &control,
                        )?
                    }
                };
                Ok(metadata)
            })();

            match result {
                Ok(metadata) => {
                    migration.publish_ready()?;
                    Ok(metadata)
                }
                Err(error) => Err(error),
            }
        }

        /// Journal a namespace drop while holding the same exclusive schema
        /// admission used by creation. None selects the entire logical database.
        pub(crate) fn drop_document_namespace_controlled(
            &self,
            database: &str,
            collection: Option<&str>,
            mut migration: SchemaMigrationGuard,
            control: Arc<OperationControl>,
        ) -> EngineResult<bool> {
            let result = (|| {
                ensure_control_active(&control, "before dropping document namespace")?;
                crate::document::validate_namespace(database, collection.unwrap_or("_"))?;
                migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;
                let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                let deletion =
                    run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                        configure_journal_mode(connection)?;
                        let transaction = connection
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(sqlite_error::storage)?;
                        require_ready_manifest(&transaction, self.shard_count())?;
                        if load_provisioning(&transaction)?.is_some() {
                            return Err(EngineError::new(
                                EngineErrorKind::FailedPrecondition,
                                "document provisioning must finish before deletion",
                            ));
                        }
                        let database_id: Option<i64> = transaction
                            .query_row(
                                "SELECT database_id FROM briskdb_document_databases
                                 WHERE database_name = ?1",
                                [database],
                                |row| row.get(0),
                            )
                            .optional()
                            .map_err(sqlite_error::storage)?;
                        let Some(database_id) = database_id else {
                            return Ok(None);
                        };
                        let collection_id = if let Some(collection) = collection {
                            let id: Option<i64> = transaction
                                .query_row(
                                    "SELECT collection_id FROM briskdb_document_collections
                                     WHERE database_id = ?1 AND collection_name = ?2
                                       AND lifecycle_state = 2",
                                    params![database_id, collection],
                                    |row| row.get(0),
                                )
                                .optional()
                                .map_err(sqlite_error::storage)?;
                            let Some(id) = id else {
                                return Ok(None);
                            };
                            Some(id)
                        } else {
                            None
                        };
                        let mut hasher = blake3::Hasher::new();
                        hasher.update(b"briskdb.document-deletion.v1\0");
                        hasher.update(&database_id.to_le_bytes());
                        hasher.update(&collection_id.unwrap_or(0).to_le_bytes());
                        hasher.update(&self.shard_count().to_le_bytes());
                        let operation_id = *hasher.finalize().as_bytes();
                        transaction
                            .execute(
                                "INSERT INTO briskdb_document_deletion
                                     (singleton, database_id, collection_id, operation_id,
                                      shard_count, next_shard)
                                 VALUES (1, ?1, ?2, ?3, ?4, 0)",
                                params![
                                    database_id,
                                    collection_id,
                                    operation_id.as_slice(),
                                    self.shard_count()
                                ],
                            )
                            .map_err(sqlite_error::storage)?;
                        manifest::validate_document_catalog(&transaction, self.shard_count())?;
                        manifest::refresh_manifest_digest(&transaction)?;
                        manifest::current_integrity(&transaction, self.shard_count())?;
                        ensure_control_active(
                            &control,
                            "before committing document deletion intent",
                        )?;
                        migration.mark_pending_on_drop();
                        #[cfg(test)]
                        deletion_crash_checkpoint("before-intent", 0);
                        transaction.commit().map_err(sqlite_error::storage)?;
                        #[cfg(test)]
                        deletion_crash_checkpoint("after-intent", 0);
                        Ok(Some(Deletion {
                            database_id,
                            collection_id,
                            operation_id,
                            shard_count: self.shard_count(),
                            next_shard: 0,
                        }))
                    })?;
                let existed = deletion.is_some();
                if let Some(deletion) = deletion {
                    recover_deletion(self, &mut connection, deletion, Some(&control))?;
                }
                let catalog = load_catalog_rows(&connection)?;
                self.publish_document_indexes(compile_ready_indexes(&catalog, &mut || {
                    ensure_control_active(&control, "before publishing document index catalog")
                })?)?;
                migration.publish_ready()?;
                Ok(existed)
            })();
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn declare_document_index(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            specification: &BsonDocument,
            unique: bool,
        ) -> EngineResult<DocumentIndexMetadata> {
            let result =
                self.declare_document_index_inner(collection_id, name, specification, unique);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn declare_document_index_inner(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            specification: &BsonDocument,
            unique: bool,
        ) -> EngineResult<DocumentIndexMetadata> {
            let _operation = self.enter_schema_operation()?;
            self.declare_document_index_controlled_inner(
                collection_id,
                name,
                specification,
                unique,
                OperationControl::new(None),
            )
        }

        pub(crate) fn declare_document_index_controlled(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            specification: &BsonDocument,
            unique: bool,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentIndexMetadata> {
            // The Engine retains schema admission across catalog resolution
            // and this manifest mutation; standalone wrappers acquire it once
            // before delegating to the same controlled implementation.
            let result = self.declare_document_index_controlled_inner(
                collection_id,
                name,
                specification,
                unique,
                control,
            );
            self.fail_closed_on_corruption(result)
        }

        fn declare_document_index_controlled_inner(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            specification: &BsonDocument,
            unique: bool,
            control: Arc<OperationControl>,
        ) -> EngineResult<DocumentIndexMetadata> {
            ensure_control_active(&control, "before declaring document index")?;
            if name.is_empty()
                || name.len() > manifest::MAX_DOCUMENT_INDEX_NAME_BYTES
                || name.contains('\0')
                || name == "_id_"
            {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document index name is invalid or reserved",
                ));
            }
            let spec_bson = encode_document(specification)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            debug_assert!(spec_bson.len() <= manifest::MAX_DOCUMENT_METADATA_BSON_BYTES);
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            let (stored_spec, index_id, stored_lifecycle) =
                run_manifest_controlled(&mut connection, control.clone(), |connection| {
                    configure_journal_mode(connection)?;
                    let transaction = connection
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(sqlite_error::storage)?;
                    require_ready_manifest(&transaction, self.shard_count())?;
                    require_active_collection(&transaction, collection_id)?;
                    let existing = transaction
                        .query_row(
                            "SELECT spec_bson, is_unique, lifecycle_state
                         FROM briskdb_document_indexes
                         WHERE collection_id = ?1 AND index_name = ?2",
                            params![to_sqlite_id(collection_id)?, name],
                            |row| {
                                Ok((
                                    row.get::<_, Vec<u8>>(0)?,
                                    row.get::<_, i64>(1)?,
                                    row.get::<_, i64>(2)?,
                                ))
                            },
                        )
                        .optional()
                        .map_err(sqlite_error::storage)?;
                    let mut stored_lifecycle = DocumentIndexLifecycle::PendingBuild;
                    let stored_spec = if let Some((existing_spec, existing_unique, lifecycle)) =
                        existing
                    {
                        // Legacy pending declarations can retain numeric direction
                        // aliases. Normalizing a new request must not rewrite or
                        // conflict with a semantically identical existing key list.
                        let canonical_keys = !specification.is_empty()
                            && specification
                                .iter()
                                .all(|(_, value)| matches!(value, BsonValue::Int32(1 | -1)));
                        let same_spec = existing_spec == spec_bson
                            || (canonical_keys
                                && decode_metadata_document(
                                    &existing_spec,
                                    "document index specification",
                                )? == *specification);
                        if !same_spec || existing_unique != i64::from(unique) {
                            return Err(EngineError::new(
                                EngineErrorKind::FailedPrecondition,
                                "document index name already has a different declaration",
                            ));
                        }
                        stored_lifecycle = match lifecycle {
                            INDEX_PENDING_BUILD => DocumentIndexLifecycle::PendingBuild,
                            INDEX_READY => DocumentIndexLifecycle::Ready,
                            _ => return Err(corrupt("invalid document index lifecycle")),
                        };
                        existing_spec
                    } else {
                        transaction
                            .execute(
                                "INSERT INTO briskdb_document_indexes (
                                collection_id, index_name, spec_bson, is_unique, is_builtin,
                                index_format_version, lifecycle_state
                             ) VALUES (?1, ?2, ?3, ?4, 0, 1, ?5)",
                                params![
                                    to_sqlite_id(collection_id)?,
                                    name,
                                    spec_bson,
                                    i64::from(unique),
                                    INDEX_PENDING_BUILD
                                ],
                            )
                            .map_err(sqlite_error::storage)?;
                        allocate_index_identity(&transaction, to_sqlite_id(collection_id)?, name)?;
                        manifest::validate_document_catalog(&transaction, self.shard_count())?;
                        manifest::refresh_manifest_digest(&transaction)?;
                        require_ready_manifest(&transaction, self.shard_count())?;
                        spec_bson.clone()
                    };
                    let index_id = transaction
                        .query_row(
                            "SELECT index_id FROM briskdb_document_index_identities
                         WHERE collection_id = ?1 AND index_name = ?2",
                            params![to_sqlite_id(collection_id)?, name],
                            |row| row.get::<_, i64>(0),
                        )
                        .map_err(sqlite_error::storage)?;
                    let index_id = DocumentIndexId::from_validated(positive_u64(
                        index_id,
                        "document index identity",
                    )?);
                    ensure_control_active(
                        &control,
                        "before committing document index declaration",
                    )?;
                    transaction.commit().map_err(sqlite_error::storage)?;
                    Ok((stored_spec, index_id, stored_lifecycle))
                })?;
            let decoded = decode_metadata_document(&stored_spec, "document index specification")?;
            Ok(DocumentIndexMetadata::from_validated_parts(
                index_id,
                name.to_owned(),
                decoded,
                unique,
                false,
                stored_lifecycle,
            ))
        }

        /// Remove one pending declaration by exact name under Engine-held schema
        /// admission. The Engine routes Ready indexes to exclusive journaled cleanup.
        pub(crate) fn drop_pending_document_index_controlled(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            control: Arc<OperationControl>,
        ) -> EngineResult<()> {
            let result = (|| {
                ensure_control_active(&control, "before dropping pending document index")?;
                if matches!(name, "_id" | "_id_") {
                    return Err(DocumentIndexError::Protected.into_engine_error());
                }
                if name.is_empty()
                    || name.len() > manifest::MAX_DOCUMENT_INDEX_NAME_BYTES
                    || name.contains('\0')
                {
                    return Err(EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        "document index name is invalid",
                    ));
                }
                let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
                run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                    configure_journal_mode(connection)?;
                    let transaction = connection
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(sqlite_error::storage)?;
                    require_ready_manifest(&transaction, self.shard_count())?;
                    require_active_collection(&transaction, collection_id)?;
                    let state = transaction
                        .query_row(
                            "SELECT is_builtin, lifecycle_state FROM briskdb_document_indexes
                         WHERE collection_id = ?1 AND index_name = ?2",
                            params![to_sqlite_id(collection_id)?, name],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                        )
                        .optional()
                        .map_err(sqlite_error::storage)?;
                    let (built_in, lifecycle) =
                        state.ok_or_else(|| DocumentIndexError::NotFound.into_engine_error())?;
                    if built_in != 0 {
                        return Err(DocumentIndexError::Protected.into_engine_error());
                    }
                    if lifecycle != INDEX_PENDING_BUILD {
                        return Err(EngineError::new(
                            EngineErrorKind::Unsupported,
                            "dropping a built document index requires exclusive schema admission",
                        ));
                    }
                    let changed = transaction
                        .execute(
                            "DELETE FROM briskdb_document_indexes
                         WHERE collection_id = ?1 AND index_name = ?2
                           AND is_builtin = 0 AND lifecycle_state = ?3",
                            params![to_sqlite_id(collection_id)?, name, INDEX_PENDING_BUILD],
                        )
                        .map_err(sqlite_error::storage)?;
                    if changed != 1 {
                        return Err(corrupt(
                            "pending document index removal did not remove exactly one declaration",
                        ));
                    }
                    // The identity mapping cascades; its permanent allocator is
                    // intentionally untouched, including when the last secondary disappears.
                    manifest::validate_document_catalog(&transaction, self.shard_count())?;
                    manifest::refresh_manifest_digest(&transaction)?;
                    require_ready_manifest(&transaction, self.shard_count())?;
                    ensure_control_active(
                        &control,
                        "before committing pending document index removal",
                    )?;
                    #[cfg(test)]
                    deletion_crash_checkpoint("index-before-commit", 0);
                    transaction.commit().map_err(sqlite_error::storage)?;
                    #[cfg(test)]
                    deletion_crash_checkpoint("index-after-commit", 0);
                    Ok(())
                })
            })();
            self.fail_closed_on_corruption(result)
        }

        /// Validate and encode one document once before routing its write.
        pub(crate) fn prepare_document_write(
            &self,
            document: &BsonDocument,
        ) -> EngineResult<PreparedDocumentWrite> {
            prepare_document(self, document)
        }

        /// Canonicalize an exact `_id` value and return its one owning shard.
        pub(crate) fn prepare_document_id(
            &self,
            id: &BsonValue,
        ) -> EngineResult<(CanonicalBsonKey, u16)> {
            let id_key = CanonicalBsonKey::encode(id)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            let shard = self.shard_for_key(id_key.as_bytes());
            Ok((id_key, shard))
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn insert_document(
            &self,
            collection_id: DocumentCollectionId,
            document: &BsonDocument,
        ) -> EngineResult<u16> {
            let result = self.insert_document_inner(collection_id, document);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn insert_document_inner(
            &self,
            collection_id: DocumentCollectionId,
            document: &BsonDocument,
        ) -> EngineResult<u16> {
            let _operation = self.enter_schema_operation()?;
            let cancellation = CancellationToken::new();
            let prepared = self.prepare_document_write(document)?;
            let natural_order =
                self.reserve_document_natural_orders_for_engine(collection_id, 1, &cancellation)?;
            self.insert_prepared_document(collection_id, natural_order, &prepared, &cancellation)?;
            Ok(prepared.shard())
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn insert_documents(
            &self,
            collection_id: DocumentCollectionId,
            documents: &[BsonDocument],
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            let result = self.insert_documents_inner(collection_id, documents, cancellation);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn insert_documents_inner(
            &self,
            collection_id: DocumentCollectionId,
            documents: &[BsonDocument],
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            let _operation = self.enter_schema_operation()?;
            ensure_document_write_not_cancelled(cancellation)?;
            if documents.is_empty() {
                self.require_active_document_collection(collection_id)?;
                return Ok(());
            }
            let natural_order = self.reserve_document_natural_orders_for_engine(
                collection_id,
                u64::try_from(documents.len()).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::LimitExceeded,
                        "document batch length exceeds its supported range",
                        error,
                    )
                })?,
                cancellation,
            )?;
            // A committed range is never reused. Cancellation or a later shard
            // failure may leave gaps, preserving monotonic order after retry.
            for (offset, document) in documents.iter().enumerate() {
                ensure_document_write_not_cancelled(cancellation)?;
                let offset = u64::try_from(offset).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::LimitExceeded,
                        "document batch offset exceeds its supported range",
                        error,
                    )
                })?;
                let order = natural_order.checked_add(offset).ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "document natural-order identity space is exhausted",
                    )
                })?;
                let prepared = self.prepare_document_write(document)?;
                self.insert_prepared_document(collection_id, order, &prepared, cancellation)?;
            }
            Ok(())
        }

        /// Reserve a durable, never-reused natural-order range for engine writes.
        ///
        /// The caller must hold the request's schema-operation guard while this
        /// manifest transaction runs. Cancellation is checked before the lock,
        /// by SQLite's progress hook, and immediately before commit.
        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn reserve_document_natural_orders_for_engine(
            &self,
            collection_id: DocumentCollectionId,
            count: u64,
            cancellation: &CancellationToken,
        ) -> EngineResult<u64> {
            let result =
                self.reserve_document_natural_orders_inner(collection_id, count, cancellation);
            self.fail_closed_on_corruption(result)
        }

        pub(crate) fn reserve_document_natural_orders_controlled(
            &self,
            collection_id: DocumentCollectionId,
            count: u64,
            control: Arc<OperationControl>,
        ) -> EngineResult<u64> {
            let result = self.reserve_document_natural_orders_controlled_inner(
                collection_id,
                count,
                control,
            );
            self.fail_closed_on_corruption(result)
        }

        fn reserve_document_natural_orders_controlled_inner(
            &self,
            collection_id: DocumentCollectionId,
            count: u64,
            control: Arc<OperationControl>,
        ) -> EngineResult<u64> {
            ensure_control_active(&control, "before reserving document natural order")?;
            let count = validate_natural_order_reservation_count(count)?;
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            run_manifest_controlled(&mut connection, control.clone(), |connection| {
                configure_journal_mode(connection)?;
                self.reserve_document_natural_orders_on_connection(
                    connection,
                    collection_id,
                    count,
                    || {
                        ensure_control_active(
                            &control,
                            "before committing document natural-order reservation",
                        )
                    },
                )
            })
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn reserve_document_natural_orders_inner(
            &self,
            collection_id: DocumentCollectionId,
            count: u64,
            cancellation: &CancellationToken,
        ) -> EngineResult<u64> {
            ensure_document_operation_not_cancelled(
                cancellation,
                "before reserving document natural order",
            )?;
            let count = validate_natural_order_reservation_count(count)?;
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&connection)?;
            configure_journal_mode(&connection)?;
            let progress_cancellation = cancellation.clone();
            connection
                .progress_handler(1_000, Some(move || progress_cancellation.is_cancelled()))
                .map_err(sqlite_error::storage)?;
            let result = self.reserve_document_natural_orders_on_connection(
                &mut connection,
                collection_id,
                count,
                || {
                    ensure_document_operation_not_cancelled(
                        cancellation,
                        "before committing document natural-order reservation",
                    )
                },
            );
            let cleanup = connection
                .progress_handler(0, None::<fn() -> bool>)
                .map_err(sqlite_error::storage);
            match (result, cleanup) {
                (Ok(first), Ok(())) => Ok(first),
                (Ok(_), Err(error)) => Err(error
                    .context("failed to remove the document allocator cancellation progress hook")),
                (Err(error), _) => Err(error),
            }
        }

        fn reserve_document_natural_orders_on_connection(
            &self,
            connection: &mut Connection,
            collection_id: DocumentCollectionId,
            count: i64,
            before_commit: impl FnOnce() -> EngineResult<()>,
        ) -> EngineResult<u64> {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            require_ready_manifest(&transaction, self.shard_count())?;
            let state = transaction
                .query_row(
                    "SELECT next_natural_order, lifecycle_state
                     FROM briskdb_document_collections WHERE collection_id = ?1",
                    [to_sqlite_id(collection_id)?],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()
                .map_err(sqlite_error::storage)?;
            let (first, lifecycle) = state.ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document collection does not exist",
                )
            })?;
            if lifecycle != COLLECTION_ACTIVE {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "document collection is not active",
                ));
            }
            let next = first.checked_add(count).ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::LimitExceeded,
                    "document natural-order identity space is exhausted",
                )
            })?;
            let changed = transaction
                .execute(
                    "UPDATE briskdb_document_collections SET next_natural_order = ?1
                     WHERE collection_id = ?2 AND next_natural_order = ?3
                       AND lifecycle_state = ?4",
                    params![next, to_sqlite_id(collection_id)?, first, COLLECTION_ACTIVE],
                )
                .map_err(sqlite_error::storage)?;
            if changed != 1 {
                return Err(corrupt(
                    "document natural-order allocator did not advance exactly once",
                ));
            }
            manifest::validate_document_catalog(&transaction, self.shard_count())?;
            manifest::refresh_manifest_digest(&transaction)?;
            require_ready_manifest(&transaction, self.shard_count())?;
            before_commit()?;
            transaction.commit().map_err(sqlite_error::storage)?;
            document_natural_order_from_sqlite(first)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn insert_prepared_document(
            &self,
            collection_id: DocumentCollectionId,
            natural_order: u64,
            prepared: &PreparedDocumentWrite,
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            let shard = prepared.shard();
            let connection = self.open_unconfigured_shard(shard)?;
            self.validate_unconfigured_shard(&connection, shard)?;
            require_schema(&connection)?;
            let transaction =
                self.begin_document_write(&connection, collection_id, shard, cancellation, None)?;
            self.insert_prepared_document_on_connection(
                &transaction,
                collection_id,
                natural_order,
                shard,
                prepared,
                cancellation,
            )?;
            ensure_document_operation_not_cancelled(cancellation, "before committing document")?;
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(())
        }

        /// Insert one prepared record within a caller-owned shard transaction.
        ///
        /// Transaction ownership remains with the engine. The caller supplies
        /// the lease's physical shard identity and arms SQLite's progress and
        /// interrupt hooks around this call.
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn insert_prepared_document_on_connection(
            &self,
            connection: &DocumentWriteTransaction<'_>,
            collection_id: DocumentCollectionId,
            natural_order: u64,
            shard: u16,
            prepared: &PreparedDocumentWrite,
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            connection.require_scope(self, collection_id, shard)?;
            ensure_document_operation_not_cancelled(cancellation, "before inserting document")?;
            self.validate_prepared_document_route(shard, prepared)?;
            require_schema(connection)?;
            let indexes = self.active_document_indexes(collection_id)?;
            let entries = prepare_write_entries(indexes.as_deref(), prepared, cancellation)?;
            connection.validate_unique_entries(&prepared.id_key, entries.as_ref())?;
            let natural_order = document_natural_order_to_sqlite(natural_order)?;
            let checksum = record_checksum(
                collection_id,
                shard,
                natural_order,
                prepared.id_key.as_bytes(),
                &prepared.document_bson,
            );
            connection
                .execute(
                    "INSERT INTO briskdb_documents_v1 (
                        collection_id, id_key, natural_order, document_bson,
                        document_checksum, storage_format_version
                     ) VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                    params![
                        to_sqlite_id(collection_id)?,
                        prepared.id_key.as_bytes(),
                        natural_order,
                        prepared.document_bson,
                        checksum.as_slice()
                    ],
                )
                .map_err(sqlite_error::statement)?;
            if let Some(entries) = entries.as_ref() {
                super::index_storage::insert_entries(
                    connection,
                    collection_id,
                    shard,
                    prepared.id_key.as_bytes(),
                    &checksum,
                    entries,
                    &mut || {
                        ensure_document_operation_not_cancelled(
                            cancellation,
                            "while inserting document index entries",
                        )
                    },
                )?;
            }
            Ok(())
        }

        /// Replace one exact `_id` record while preserving its natural order.
        ///
        /// `false` means the target record no longer exists at the supplied
        /// natural order. A replacement cannot change the semantic `_id`.
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn replace_document_on_connection(
            &self,
            connection: &DocumentWriteTransaction<'_>,
            collection_id: DocumentCollectionId,
            shard: u16,
            id_key: &CanonicalBsonKey,
            natural_order: u64,
            replacement: &PreparedDocumentWrite,
            cancellation: &CancellationToken,
        ) -> EngineResult<bool> {
            connection.require_scope(self, collection_id, shard)?;
            ensure_document_operation_not_cancelled(cancellation, "before replacing document")?;
            self.validate_document_key_route(shard, id_key)?;
            if replacement.id_key != *id_key {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document replacement cannot change the semantic _id",
                ));
            }
            self.validate_prepared_document_route(shard, replacement)?;
            require_schema(connection)?;
            let Some(current) = self.get_document_on_connection(
                connection,
                collection_id,
                shard,
                id_key,
                cancellation,
            )?
            else {
                return Ok(false);
            };
            if current.natural_order != natural_order {
                return Ok(false);
            }
            let indexes = self.active_document_indexes(collection_id)?;
            validate_record_index_coverage(connection, indexes.as_deref(), &current, cancellation)?;
            let entries = prepare_write_entries(indexes.as_deref(), replacement, cancellation)?;
            connection.validate_unique_entries(id_key, entries.as_ref())?;
            let natural_order = document_natural_order_to_sqlite(natural_order)?;
            let checksum = record_checksum(
                collection_id,
                shard,
                natural_order,
                id_key.as_bytes(),
                &replacement.document_bson,
            );
            let changed = connection
                .execute(
                    "UPDATE briskdb_documents_v1
                     SET document_bson = ?1, document_checksum = ?2
                     WHERE collection_id = ?3 AND id_key = ?4 AND natural_order = ?5",
                    params![
                        replacement.document_bson,
                        checksum.as_slice(),
                        to_sqlite_id(collection_id)?,
                        id_key.as_bytes(),
                        natural_order
                    ],
                )
                .map_err(sqlite_error::statement)?;
            if changed > 1 {
                return Err(corrupt(
                    "exact document replacement changed more than one stored record",
                ));
            }
            if changed == 1 {
                super::index_storage::remove_record_entries(
                    connection,
                    collection_id,
                    id_key.as_bytes(),
                )?;
                if let Some(entries) = entries.as_ref() {
                    super::index_storage::insert_entries(
                        connection,
                        collection_id,
                        shard,
                        id_key.as_bytes(),
                        &checksum,
                        entries,
                        &mut || {
                            ensure_document_operation_not_cancelled(
                                cancellation,
                                "while replacing document index entries",
                            )
                        },
                    )?;
                }
            }
            Ok(changed == 1)
        }

        /// Delete one exact canonical `_id` within a caller-owned transaction.
        pub(crate) fn delete_document_on_connection(
            &self,
            connection: &DocumentWriteTransaction<'_>,
            collection_id: DocumentCollectionId,
            shard: u16,
            id_key: &CanonicalBsonKey,
            cancellation: &CancellationToken,
        ) -> EngineResult<bool> {
            connection.require_scope(self, collection_id, shard)?;
            ensure_document_operation_not_cancelled(cancellation, "before deleting document")?;
            self.validate_document_key_route(shard, id_key)?;
            require_schema(connection)?;
            let Some(current) = self.get_document_on_connection(
                connection,
                collection_id,
                shard,
                id_key,
                cancellation,
            )?
            else {
                return Ok(false);
            };
            let indexes = self.active_document_indexes(collection_id)?;
            validate_record_index_coverage(connection, indexes.as_deref(), &current, cancellation)?;
            let changed = connection
                .execute(
                    "DELETE FROM briskdb_documents_v1
                     WHERE collection_id = ?1 AND id_key = ?2",
                    params![to_sqlite_id(collection_id)?, id_key.as_bytes()],
                )
                .map_err(sqlite_error::statement)?;
            if changed > 1 {
                return Err(corrupt(
                    "exact document deletion changed more than one stored record",
                ));
            }
            if changed == 1 {
                super::index_storage::remove_record_entries(
                    connection,
                    collection_id,
                    id_key.as_bytes(),
                )?;
            }
            Ok(changed == 1)
        }

        fn validate_prepared_document_route(
            &self,
            shard: u16,
            prepared: &PreparedDocumentWrite,
        ) -> EngineResult<()> {
            if prepared.shard != shard {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "prepared document was sent to a different shard lease",
                ));
            }
            self.validate_document_key_route(shard, &prepared.id_key)
        }

        fn validate_document_key_route(
            &self,
            shard: u16,
            id_key: &CanonicalBsonKey,
        ) -> EngineResult<()> {
            self.ensure_shard_in_range(shard)?;
            if self.shard_for_key(id_key.as_bytes()) != shard {
                return Err(EngineError::new(
                    EngineErrorKind::Internal,
                    "canonical document _id was sent to a non-owning shard lease",
                ));
            }
            Ok(())
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn get_document(
            &self,
            collection_id: DocumentCollectionId,
            id: &BsonValue,
        ) -> EngineResult<Option<BsonDocument>> {
            let result = self.get_document_inner(collection_id, id);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn get_document_inner(
            &self,
            collection_id: DocumentCollectionId,
            id: &BsonValue,
        ) -> EngineResult<Option<BsonDocument>> {
            let _operation = self.enter_schema_operation()?;
            self.require_active_document_collection(collection_id)?;
            let cancellation = CancellationToken::new();
            let (id_key, shard) = self.prepare_document_id(id)?;
            let connection = self.open_unconfigured_shard(shard)?;
            self.validate_unconfigured_shard(&connection, shard)?;
            self.get_document_on_connection(
                &connection,
                collection_id,
                shard,
                &id_key,
                &cancellation,
            )
            .map(|record| record.map(DocumentStorageRecord::into_document))
        }

        /// Read one exact canonical `_id` through an already-leased shard.
        pub(crate) fn get_document_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            id_key: &CanonicalBsonKey,
            cancellation: &CancellationToken,
        ) -> EngineResult<Option<DocumentStorageRecord>> {
            ensure_document_operation_not_cancelled(cancellation, "before reading document")?;
            self.validate_document_key_route(shard, id_key)?;
            require_schema(connection)?;
            let row = connection
                .query_row(
                    "SELECT natural_order, document_bson, document_checksum,
                            storage_format_version
                     FROM briskdb_documents_v1
                     WHERE collection_id = ?1 AND id_key = ?2",
                    params![to_sqlite_id(collection_id)?, id_key.as_bytes()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, Vec<u8>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| shard_read_error(error, "failed to read stored BSON document"))?;
            let record = row
                .map(|(natural_order, bson, checksum, version)| {
                    decode_storage_record(
                        collection_id,
                        shard,
                        natural_order,
                        id_key.as_bytes().to_vec(),
                        bson,
                        checksum,
                        version,
                    )
                })
                .transpose()?;
            ensure_document_operation_not_cancelled(cancellation, "after reading document")?;
            Ok(record)
        }

        /// Count one collection on one already-leased shard.
        ///
        /// This deliberately counts rows without decoding every BSON payload.
        /// Startup validation and point/scan reads remain the checksum boundary.
        pub(crate) fn count_document_shard_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            cancellation: &CancellationToken,
        ) -> EngineResult<u64> {
            ensure_document_operation_not_cancelled(cancellation, "before counting documents")?;
            self.ensure_shard_in_range(shard)?;
            require_schema(connection)?;
            let count = connection
                .query_row(
                    "SELECT count(*) FROM briskdb_documents_v1
                     WHERE collection_id = ?1",
                    [to_sqlite_id(collection_id)?],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(|error| {
                    shard_read_error(error, "failed to count stored BSON documents")
                })?;
            ensure_document_operation_not_cancelled(cancellation, "after counting documents")?;
            u64::try_from(count)
                .map_err(|_| corrupt("stored BSON document count is outside its range"))
        }

        /// Return one bounded, ascending shard-local natural-order page.
        pub(crate) fn scan_document_shard_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            after_natural_order: Option<u64>,
            limit: usize,
            cancellation: &CancellationToken,
        ) -> EngineResult<Vec<DocumentStorageRecord>> {
            self.scan_document_candidates_on_connection(
                connection,
                collection_id,
                shard,
                after_natural_order,
                limit,
                None,
                cancellation,
            )
        }

        /// The probe was selected under this request's schema admission. It is
        /// never cursor-retained authority; an absent/dropped index falls back
        /// when the next request selects from the current Ready cache.
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn scan_document_candidates_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            after_natural_order: Option<u64>,
            limit: usize,
            probe: Option<&DocumentIndexProbe>,
            cancellation: &CancellationToken,
        ) -> EngineResult<Vec<DocumentStorageRecord>> {
            ensure_document_operation_not_cancelled(cancellation, "before scanning documents")?;
            self.ensure_shard_in_range(shard)?;
            if probe.is_some_and(|probe| probe.collection_id() != collection_id) {
                return Err(EngineError::new(
                    EngineErrorKind::FailedPrecondition,
                    "document index probe belongs to another collection",
                ));
            }
            if !(1..=MAX_DOCUMENT_SHARD_SCAN_RECORDS).contains(&limit) {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "document shard scan limit must be between 1 and {MAX_DOCUMENT_SHARD_SCAN_RECORDS}"
                    ),
                ));
            }
            let after_natural_order = after_natural_order
                .map(document_natural_order_to_sqlite)
                .transpose()?
                .unwrap_or(0);
            let sqlite_limit =
                i64::try_from(limit).expect("bounded document scan limit fits SQLite");
            require_schema(connection)?;
            let grouped_sql = match probe.map(DocumentIndexProbe::selection) {
                Some(DocumentIndexSelection::Keys(keys)) if keys.len() > 1 => {
                    Some(candidate_sql::membership(keys.len()))
                }
                Some(DocumentIndexSelection::SparseEntries) => Some(candidate_sql::sparse()),
                _ => None,
            };
            let sql = if let Some(sql) = grouped_sql.as_deref() {
                sql
            } else if probe.is_some() {
                // Keep natural-order pagination and let SQLite choose join
                // order. Forcing an index-first join would repeatedly sort
                // large equality groups for each one-record merge frontier.
                "SELECT d.natural_order, d.id_key, d.document_bson, d.document_checksum,
                        d.storage_format_version, e.entry_checksum, e.entry_format_version,
                        e.index_key
                 FROM briskdb_documents_v1 AS d
                 JOIN briskdb_document_index_entries_v1 AS e
                   ON e.collection_id = d.collection_id AND e.id_key = d.id_key
                 WHERE d.collection_id = ?1 AND d.natural_order > ?2
                   AND e.index_id = ?4 AND (e.index_key = ?5 OR e.index_key = ?6)
                 ORDER BY d.natural_order LIMIT ?3"
            } else {
                "SELECT natural_order, id_key, document_bson, document_checksum,
                        storage_format_version
                 FROM briskdb_documents_v1
                 WHERE collection_id = ?1 AND natural_order > ?2
                 ORDER BY natural_order LIMIT ?3"
            };
            let mut statement = connection.prepare(sql).map_err(|error| {
                shard_read_error(error, "failed to prepare stored BSON document scan")
            })?;
            let mut rows = match probe.map(|probe| (probe, probe.selection())) {
                Some((probe, DocumentIndexSelection::SparseEntries)) => statement.query(params![
                    to_sqlite_id(collection_id)?,
                    after_natural_order,
                    sqlite_limit,
                    probe.index_id().get() as i64,
                ]),
                Some((probe, DocumentIndexSelection::Keys(keys))) if keys.len() > 1 => {
                    use rusqlite::types::{ToSqlOutput, ValueRef};
                    // Borrow the existing encoded keys; do not make another
                    // per-shard copy of a potentially large membership list.
                    let values = [
                        ValueRef::Integer(to_sqlite_id(collection_id)?),
                        ValueRef::Integer(after_natural_order),
                        ValueRef::Integer(sqlite_limit),
                        ValueRef::Integer(probe.index_id().get() as i64),
                    ]
                    .into_iter()
                    .chain(keys.iter().map(|key| ValueRef::Blob(key)))
                    .chain(std::iter::once(ValueRef::Blob(
                        crate::document::NON_UNIQUE_FALLBACK_KEY,
                    )))
                    .map(ToSqlOutput::Borrowed);
                    statement.query(rusqlite::params_from_iter(values))
                }
                Some((probe, DocumentIndexSelection::Keys(keys))) => statement.query(params![
                    to_sqlite_id(collection_id)?,
                    after_natural_order,
                    sqlite_limit,
                    probe.index_id().get() as i64,
                    &keys[0],
                    crate::document::NON_UNIQUE_FALLBACK_KEY,
                ]),
                None => statement.query(params![
                    to_sqlite_id(collection_id)?,
                    after_natural_order,
                    sqlite_limit
                ]),
            }
            .map_err(|error| {
                shard_read_error(error, "failed to start stored BSON document scan")
            })?;
            let mut records = Vec::with_capacity(limit);
            while let Some(row) = rows.next().map_err(|error| {
                shard_read_error(error, "failed while scanning stored BSON documents")
            })? {
                ensure_document_operation_not_cancelled(cancellation, "while scanning documents")?;
                let natural_order = row.get::<_, i64>(0).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON natural order")
                })?;
                let id_key = row.get::<_, Vec<u8>>(1).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON canonical key")
                })?;
                let document_bson = row.get::<_, Vec<u8>>(2).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON payload")
                })?;
                let checksum = row.get::<_, Vec<u8>>(3).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON checksum")
                })?;
                let version = row.get::<_, i64>(4).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON format version")
                })?;
                let canonical = CanonicalBsonKey::from_bytes(&id_key)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
                if self.shard_for_key(canonical.as_bytes()) != shard {
                    return Err(corrupt(
                        "stored BSON document is on a shard that disagrees with its canonical _id route",
                    ));
                }
                let record = decode_storage_record(
                    collection_id,
                    shard,
                    natural_order,
                    id_key,
                    document_bson,
                    checksum,
                    version,
                )?;
                if let Some(probe) = probe {
                    let stored = row
                        .get_ref(5)
                        .and_then(|value| value.as_blob().map_err(Into::into))
                        .map_err(|error| {
                            shard_read_error(error, "invalid document index checksum")
                        })?;
                    let version = row.get::<_, i64>(6).map_err(|error| {
                        shard_read_error(error, "invalid document index entry version")
                    })?;
                    let index_key = row
                        .get_ref(7)
                        .and_then(|value| value.as_blob().map_err(Into::into))
                        .map_err(|error| {
                            shard_read_error(error, "invalid document index candidate key")
                        })?;
                    super::index_storage::validate_probe_entry(
                        collection_id,
                        probe.index_id(),
                        shard,
                        record.id_key.as_bytes(),
                        index_key,
                        &record.checksum,
                        stored,
                        version,
                    )?;
                }
                records.push(record);
            }
            ensure_document_operation_not_cancelled(cancellation, "after scanning documents")?;
            Ok(records)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        pub(crate) fn document_count(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<u64> {
            let result = self.document_count_inner(collection_id);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(any(feature = "tinymongo-import", test))]
        fn document_count_inner(&self, collection_id: DocumentCollectionId) -> EngineResult<u64> {
            let _operation = self.enter_schema_operation()?;
            self.require_active_document_collection(collection_id)?;
            let cancellation = CancellationToken::new();
            let mut total = 0_u64;
            for shard in 0..self.shard_count() {
                let connection = self.open_unconfigured_shard(shard)?;
                self.validate_unconfigured_shard(&connection, shard)?;
                let count = self.count_document_shard_on_connection(
                    &connection,
                    collection_id,
                    shard,
                    &cancellation,
                )?;
                total = total.checked_add(count).ok_or_else(|| {
                    corrupt("stored BSON document count exceeds its supported range")
                })?;
            }
            Ok(total)
        }

        #[allow(dead_code)]
        pub(crate) fn scan_documents(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<Vec<BsonDocument>> {
            let result = self.scan_documents_inner(collection_id);
            self.fail_closed_on_corruption(result)
        }

        fn scan_documents_inner(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<Vec<BsonDocument>> {
            let _operation = self.enter_schema_operation()?;
            self.require_active_document_collection(collection_id)?;
            let cancellation = CancellationToken::new();
            let mut documents = Vec::new();
            for shard in 0..self.shard_count() {
                let connection = self.open_unconfigured_shard(shard)?;
                self.validate_unconfigured_shard(&connection, shard)?;
                let mut after = None;
                loop {
                    let page = self.scan_document_shard_on_connection(
                        &connection,
                        collection_id,
                        shard,
                        after,
                        MAX_DOCUMENT_SHARD_SCAN_RECORDS,
                        &cancellation,
                    )?;
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(DocumentStorageRecord::natural_order);
                    documents.extend(
                        page.into_iter()
                            .map(|record| (record.natural_order(), record.into_document())),
                    );
                }
            }
            documents.sort_by_key(|(natural_order, _)| *natural_order);
            if documents.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(corrupt(
                    "stored BSON documents have duplicate natural-order values",
                ));
            }
            let next_natural_order = self.next_document_natural_order(collection_id)?;
            if documents
                .last()
                .is_some_and(|(natural_order, _)| *natural_order >= next_natural_order)
            {
                return Err(corrupt(
                    "stored BSON document natural order exceeds its durable allocator",
                ));
            }
            Ok(documents
                .into_iter()
                .map(|(_, document)| document)
                .collect())
        }

        fn next_document_natural_order(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<u64> {
            let path = self.root.join("manifest.sqlite");
            let connection = open_existing_manifest(&path)?;
            configure_manifest_connection(&connection)?;
            require_ready_manifest(&connection, self.shard_count())?;
            let next = connection
                .query_row(
                    "SELECT next_natural_order FROM briskdb_document_collections
                     WHERE collection_id = ?1 AND lifecycle_state = ?2",
                    params![to_sqlite_id(collection_id)?, COLLECTION_ACTIVE],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sqlite_error::storage)?
                .ok_or_else(|| {
                    corrupt("active document collection disappeared from its catalog")
                })?;
            u64::try_from(next)
                .ok()
                .filter(|next| *next > 0)
                .ok_or_else(|| corrupt("document natural-order allocator is not positive"))
        }

        fn require_active_document_collection(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<()> {
            let path = self.root.join("manifest.sqlite");
            let connection = open_existing_manifest(&path)?;
            configure_manifest_connection(&connection)?;
            require_ready_manifest(&connection, self.shard_count())?;
            require_active_collection(&connection, collection_id)
        }
    }

    pub(in crate::storage) fn recover_or_validate(
        storage: &Storage,
        manifest_connection: &mut Connection,
    ) -> EngineResult<()> {
        manifest::validate_document_catalog(manifest_connection, storage.shard_count())?;
        if let Some(deletion) = load_deletion(manifest_connection)? {
            recover_deletion(storage, manifest_connection, deletion, None)?;
        }
        if let Some(provisioning) = load_provisioning(manifest_connection)? {
            recover_provisioning(storage, manifest_connection, provisioning)?;
        }
        super::index_storage::recover_layout(storage, manifest_connection)?;
        index_operations::recover(storage, manifest_connection)?;
        let catalog = load_catalog_rows(manifest_connection)?;
        let indexes = compile_ready_indexes(&catalog, &mut || Ok(()))?;
        if !catalog.collections().is_empty() {
            validate_stored_records(storage, manifest_connection, &catalog, &indexes)?;
        } else {
            for shard in 0..storage.shard_count() {
                let connection = storage.open_unconfigured_shard(shard)?;
                storage.validate_unconfigured_shard(&connection, shard)?;
                if super::validate_optional_schema(&connection)? {
                    return Err(corrupt(
                        "document storage exists without an active or provisioning collection",
                    ));
                }
            }
        }
        storage.publish_document_indexes(indexes)
    }

    fn load_deletion(connection: &Connection) -> EngineResult<Option<Deletion>> {
        let row = connection.query_row(
            "SELECT database_id, collection_id, operation_id, shard_count, next_shard FROM briskdb_document_deletion WHERE singleton = 1",
            [], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?, row.get::<_, Vec<u8>>(2)?, row.get::<_, u16>(3)?, row.get::<_, u16>(4)?)),
        ).optional().map_err(|error| shard_read_error(error, "failed to load document deletion journal"))?;
        row.map(
            |(database_id, collection_id, operation_id, shard_count, next_shard)| {
                Ok(Deletion {
                    database_id,
                    collection_id,
                    operation_id: operation_id
                        .try_into()
                        .map_err(|_| corrupt("invalid document deletion identity"))?,
                    shard_count,
                    next_shard,
                })
            },
        )
        .transpose()
    }

    fn recover_deletion(
        storage: &Storage,
        manifest_connection: &mut Connection,
        mut deletion: Deletion,
        control: Option<&Arc<OperationControl>>,
    ) -> EngineResult<()> {
        manifest::current_integrity(manifest_connection, storage.shard_count())?;
        if deletion.shard_count != storage.shard_count() {
            return Err(corrupt(
                "document deletion shard count differs from routing metadata",
            ));
        }
        let collections = manifest_connection.prepare(
            "SELECT collection_id FROM briskdb_document_collections WHERE database_id = ?1 AND (?2 IS NULL OR collection_id = ?2) ORDER BY collection_id"
        ).and_then(|mut statement| statement.query_map(params![deletion.database_id, deletion.collection_id], |row| row.get::<_, i64>(0))?.collect::<Result<Vec<_>, _>>()).map_err(sqlite_error::storage)?;
        let total: i64 = manifest_connection
            .query_row(
                "SELECT count(*) FROM briskdb_document_collections",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error::storage)?;
        let remove_schema = total == collections.len() as i64;
        while deletion.next_shard < deletion.shard_count {
            let shard = deletion.next_shard;
            let mut connection = storage.open_unconfigured_shard(shard)?;
            run_provisioning_step(&mut connection, control, |connection| {
                storage.validate_unconfigured_shard_nonterminal(connection, shard)?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                let present = super::validate_optional_schema(&transaction)?;
                if remove_schema {
                    // This shard may have committed before its journal cursor.
                    // The absence of the exact optional table is then success.
                    if present {
                        super::index_storage::drop_schema(&transaction)?;
                        transaction
                            .execute_batch("DROP TABLE briskdb_documents_v1")
                            .map_err(sqlite_error::storage)?;
                    }
                } else {
                    // A v17 deletion journal can precede the v18 storage
                    // upgrade. Its old record table is sufficient for cleanup.
                    if !present {
                        return Err(corrupt("document deletion is missing shard records"));
                    }
                    for id in &collections {
                        if let Some(control) = control {
                            ensure_control_active(control, "during document shard cleanup")?;
                        }
                        transaction
                            .execute(
                                "DELETE FROM briskdb_documents_v1 WHERE collection_id = ?1",
                                [id],
                            )
                            .map_err(sqlite_error::storage)?;
                    }
                }
                if let Some(control) = control {
                    ensure_control_active(control, "before committing document shard cleanup")?;
                }
                #[cfg(test)]
                deletion_crash_checkpoint("before-shard", shard);
                transaction.commit().map_err(sqlite_error::storage)
            })?;
            #[cfg(test)]
            deletion_crash_checkpoint("after-shard", shard);
            run_provisioning_step(manifest_connection, control, |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                manifest::current_integrity(&transaction, storage.shard_count())?;
                let changed = transaction.execute(
                    "UPDATE briskdb_document_deletion SET next_shard = ?1 WHERE singleton = 1 AND operation_id = ?2 AND next_shard = ?3",
                    params![shard + 1, deletion.operation_id.as_slice(), shard],
                ).map_err(sqlite_error::storage)?;
                if changed != 1 {
                    return Err(corrupt(
                        "document deletion journal did not advance exactly once",
                    ));
                }
                manifest::validate_document_catalog(&transaction, storage.shard_count())?;
                manifest::refresh_manifest_digest(&transaction)?;
                manifest::current_integrity(&transaction, storage.shard_count())?;
                if let Some(control) = control {
                    ensure_control_active(control, "before committing document deletion cursor")?;
                }
                #[cfg(test)]
                deletion_crash_checkpoint("before-progress", shard);
                transaction.commit().map_err(sqlite_error::storage)
            })?;
            #[cfg(test)]
            deletion_crash_checkpoint("after-progress", shard);
            deletion.next_shard = shard + 1;
        }
        run_provisioning_step(manifest_connection, control, |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            let removed = transaction.execute(
                "DELETE FROM briskdb_document_deletion WHERE singleton = 1 AND operation_id = ?1 AND next_shard = shard_count",
                [deletion.operation_id.as_slice()],
            ).map_err(sqlite_error::storage)?;
            if removed != 1 {
                return Err(corrupt(
                    "document deletion journal did not finalize exactly once",
                ));
            }
            transaction.execute(
                "DELETE FROM briskdb_document_indexes WHERE collection_id IN (
                    SELECT collection_id FROM briskdb_document_collections WHERE database_id = ?1 AND (?2 IS NULL OR collection_id = ?2))",
                params![deletion.database_id, deletion.collection_id],
            ).map_err(sqlite_error::storage)?;
            let removed = transaction.execute(
                "DELETE FROM briskdb_document_collections WHERE database_id = ?1 AND (?2 IS NULL OR collection_id = ?2)",
                params![deletion.database_id, deletion.collection_id],
            ).map_err(sqlite_error::storage)?;
            if removed != collections.len() {
                return Err(corrupt("document deletion target changed during cleanup"));
            }
            transaction.execute(
                "DELETE FROM briskdb_document_databases WHERE database_id = ?1 AND NOT EXISTS (SELECT 1 FROM briskdb_document_collections WHERE database_id = ?1)",
                [deletion.database_id],
            ).map_err(sqlite_error::storage)?;
            manifest::validate_document_catalog(&transaction, storage.shard_count())?;
            manifest::refresh_manifest_digest(&transaction)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            if let Some(control) = control {
                ensure_control_active(control, "before committing document deletion completion")?;
            }
            #[cfg(test)]
            deletion_crash_checkpoint("before-completion", 0);
            transaction.commit().map_err(sqlite_error::storage)?;
            #[cfg(test)]
            deletion_crash_checkpoint("after-completion", 0);
            Ok(())
        })
    }

    fn recover_provisioning(
        storage: &Storage,
        manifest_connection: &mut Connection,
        provisioning: Provisioning,
    ) -> EngineResult<()> {
        recover_provisioning_inner(storage, manifest_connection, provisioning, None).map(|_| ())
    }

    fn recover_provisioning_controlled(
        storage: &Storage,
        manifest_connection: &mut Connection,
        provisioning: Provisioning,
        control: &Arc<OperationControl>,
    ) -> EngineResult<DocumentCollectionMetadata> {
        ensure_control_active(control, "before provisioning document collection")?;
        recover_provisioning_inner(storage, manifest_connection, provisioning, Some(control))
    }

    fn run_provisioning_step<T>(
        connection: &mut Connection,
        control: Option<&Arc<OperationControl>>,
        work: impl FnOnce(&mut Connection) -> EngineResult<T>,
    ) -> EngineResult<T> {
        match control {
            Some(control) => run_dedicated_controlled(connection, Arc::clone(control), work),
            None => work(connection),
        }
    }

    fn recover_provisioning_inner(
        storage: &Storage,
        manifest_connection: &mut Connection,
        mut provisioning: Provisioning,
        control: Option<&Arc<OperationControl>>,
    ) -> EngineResult<DocumentCollectionMetadata> {
        if provisioning.shard_count != storage.shard_count() {
            return Err(corrupt(
                "document provisioning shard count differs from routing metadata",
            ));
        }
        while provisioning.next_shard < provisioning.shard_count {
            let shard = provisioning.next_shard;
            let mut connection = storage.open_unconfigured_shard(shard)?;
            match control {
                Some(control) => {
                    run_dedicated_controlled(&mut connection, Arc::clone(control), |connection| {
                        storage.validate_unconfigured_shard_nonterminal(connection, shard)?;
                        ensure_schema(connection)
                    })?
                }
                None => {
                    storage.validate_unconfigured_shard(&connection, shard)?;
                    ensure_schema(&mut connection)?;
                }
            }
            run_provisioning_step(manifest_connection, control, |manifest_connection| {
                let transaction = manifest_connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                manifest::current_integrity(&transaction, storage.shard_count())?;
                let changed = transaction
                    .execute(
                        "UPDATE briskdb_document_provisioning SET next_shard = ?1
                         WHERE singleton = 1 AND collection_id = ?2
                           AND operation_id = ?3 AND next_shard = ?4",
                        params![
                            shard + 1,
                            to_sqlite_id(provisioning.collection_id)?,
                            provisioning.operation_id.as_slice(),
                            shard
                        ],
                    )
                    .map_err(sqlite_error::storage)?;
                if changed != 1 {
                    return Err(corrupt(
                        "document provisioning journal did not advance exactly once",
                    ));
                }
                manifest::validate_document_catalog(&transaction, storage.shard_count())?;
                manifest::refresh_manifest_digest(&transaction)?;
                manifest::current_integrity(&transaction, storage.shard_count())?;
                if let Some(control) = control {
                    ensure_control_active(
                        control,
                        "before committing document collection provisioning cursor",
                    )?;
                }
                transaction.commit().map_err(sqlite_error::storage)
            })?;
            provisioning.next_shard = shard + 1;
        }

        run_provisioning_step(manifest_connection, control, |manifest_connection| {
            let transaction = manifest_connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            let activated_index = transaction
                .execute(
                    "UPDATE briskdb_document_indexes SET lifecycle_state = ?1
                     WHERE collection_id = ?2 AND index_name = '_id_'
                       AND is_unique = 1 AND is_builtin = 1 AND lifecycle_state = ?3",
                    params![
                        INDEX_READY,
                        to_sqlite_id(provisioning.collection_id)?,
                        INDEX_PENDING_BUILD
                    ],
                )
                .map_err(sqlite_error::storage)?;
            if activated_index != 1 {
                return Err(corrupt(
                    "document collection activation did not update its built-in _id index",
                ));
            }
            let changed = transaction
                .execute(
                    "UPDATE briskdb_document_collections SET lifecycle_state = ?1
                     WHERE collection_id = ?2 AND lifecycle_state = ?3",
                    params![
                        COLLECTION_ACTIVE,
                        to_sqlite_id(provisioning.collection_id)?,
                        COLLECTION_PROVISIONING
                    ],
                )
                .map_err(sqlite_error::storage)?;
            if changed != 1 {
                return Err(corrupt(
                    "document collection activation did not update exactly one row",
                ));
            }
            let deleted = transaction
                .execute(
                    "DELETE FROM briskdb_document_provisioning
                     WHERE singleton = 1 AND collection_id = ?1 AND operation_id = ?2
                       AND next_shard = shard_count",
                    params![
                        to_sqlite_id(provisioning.collection_id)?,
                        provisioning.operation_id.as_slice()
                    ],
                )
                .map_err(sqlite_error::storage)?;
            if deleted != 1 {
                return Err(corrupt(
                    "document provisioning journal did not finalize exactly once",
                ));
            }
            manifest::validate_document_catalog(&transaction, storage.shard_count())?;
            manifest::refresh_manifest_digest(&transaction)?;
            manifest::current_integrity(&transaction, storage.shard_count())?;
            let metadata = load_catalog_rows(&transaction)?
                .collection_by_id(provisioning.collection_id)
                .cloned()
                .ok_or_else(|| {
                    corrupt("completed document collection is missing from its catalog")
                })?;
            if let Some(control) = control {
                ensure_control_active(control, "before committing document collection activation")?;
            }
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(metadata)
        })
    }

    fn validate_stored_records(
        storage: &Storage,
        manifest_connection: &Connection,
        catalog: &DocumentCatalog,
        indexes: &DocumentIndexPreparations,
    ) -> EngineResult<()> {
        manifest::current_integrity(manifest_connection, storage.shard_count())?;
        // A cross-shard uniqueness snapshot must not combine an old owner on
        // one shard with a new owner committed later on another shard. Hold
        // every participating writer stripe in deterministic order throughout
        // validation. Deduplicate collisions to avoid acquiring our own lock.
        let mut fenced_collections = indexes
            .collections
            .iter()
            .filter(|(_, indexes)| indexes.has_unique_secondary())
            .map(|(collection, _)| *collection)
            .collect::<Vec<_>>();
        let stripe = |collection: &DocumentCollectionId| {
            collection.get() % crate::storage::process_lock::document_write::STRIPES as u64
        };
        fenced_collections.sort_unstable_by_key(stripe);
        fenced_collections.dedup_by_key(|collection| stripe(collection));
        let cancellation = CancellationToken::new();
        let _fences = fenced_collections
            .iter()
            .map(|collection| {
                write_transaction::acquire_fence(storage, *collection, &cancellation, None)
            })
            .collect::<EngineResult<Vec<_>>>()?;
        let unique_keys = (!fenced_collections.is_empty())
            .then(|| unique::UniqueKeyScratch::new(None))
            .transpose()?;
        let active_collections = catalog
            .collections()
            .iter()
            .map(DocumentCollectionMetadata::id)
            .collect::<HashSet<_>>();
        let mut natural_orders = HashSet::new();
        let mut maximum_orders = HashMap::<DocumentCollectionId, i64>::new();
        for shard in 0..storage.shard_count() {
            let connection = storage.open_unconfigured_shard(shard)?;
            storage.validate_unconfigured_shard(&connection, shard)?;
            require_schema(&connection)?;
            super::index_storage::require_no_orphans(&connection)?;
            let mut statement = connection
                .prepare(
                    "SELECT collection_id, natural_order, id_key, document_bson,
                            document_checksum, storage_format_version
                     FROM briskdb_documents_v1
                     ORDER BY collection_id, natural_order, id_key",
                )
                .map_err(|error| {
                    shard_read_error(error, "failed to inspect stored BSON documents")
                })?;
            let mut rows = statement.query([]).map_err(|error| {
                shard_read_error(error, "failed to inspect stored BSON documents")
            })?;
            while let Some(row) = rows.next().map_err(|error| {
                shard_read_error(error, "failed to inspect stored BSON documents")
            })? {
                let collection_id = row.get::<_, i64>(0).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON collection ID")
                })?;
                let collection_id = DocumentCollectionId::from_validated(positive_u64(
                    collection_id,
                    "stored BSON document collection ID",
                )?);
                if !active_collections.contains(&collection_id) {
                    return Err(corrupt(
                        "stored BSON document references a missing or inactive collection",
                    ));
                }
                let natural_order = row.get::<_, i64>(1).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON natural order")
                })?;
                if natural_order <= 0 {
                    return Err(corrupt(
                        "stored BSON document natural order is not positive",
                    ));
                }
                let id_key = row.get::<_, Vec<u8>>(2).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON canonical key")
                })?;
                CanonicalBsonKey::from_bytes(&id_key)
                    .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
                if storage.shard_for_key(&id_key) != shard {
                    return Err(corrupt(
                        "stored BSON document is on a shard that disagrees with its canonical _id route",
                    ));
                }
                let bson = row.get::<_, Vec<u8>>(3).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON bytes")
                })?;
                let checksum = row.get::<_, Vec<u8>>(4).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON checksum")
                })?;
                let version = row.get::<_, i64>(5).map_err(|error| {
                    shard_read_error(error, "failed to decode stored BSON format version")
                })?;
                let record_checksum: [u8; 32] = checksum
                    .as_slice()
                    .try_into()
                    .map_err(|_| corrupt("stored BSON checksum has an invalid length"))?;
                let document = decode_record(
                    collection_id,
                    shard,
                    natural_order,
                    &id_key,
                    bson,
                    checksum,
                    version,
                )?;
                let expected = indexes
                    .get(&collection_id)
                    .map(|preparation| {
                        preparation
                            .prepare_for_storage_with_check(&document, &mut || Ok(()))
                            .map_err(stored_index_error)
                    })
                    .transpose()?;
                if let (Some(unique_keys), Some(expected)) = (&unique_keys, &expected) {
                    if unique_keys
                        .add(expected, &id_key, &mut || Ok(()))?
                        .is_some()
                    {
                        return Err(corrupt(
                            "stored document unique index contains duplicate keys",
                        ));
                    }
                }
                super::index_storage::validate_record_entries(
                    &connection,
                    collection_id,
                    shard,
                    &id_key,
                    &record_checksum,
                    expected.as_ref(),
                    &mut || Ok(()),
                )?;
                if !natural_orders.insert((collection_id, natural_order)) {
                    return Err(corrupt(
                        "stored BSON documents have duplicate natural-order values",
                    ));
                }
                maximum_orders
                    .entry(collection_id)
                    .and_modify(|maximum| *maximum = (*maximum).max(natural_order))
                    .or_insert(natural_order);
            }
        }

        let allocators = load_natural_order_allocators(manifest_connection)?;
        if allocators.len() != active_collections.len()
            || allocators
                .keys()
                .any(|collection_id| !active_collections.contains(collection_id))
        {
            return Err(corrupt(
                "document natural-order allocators disagree with the active catalog",
            ));
        }
        for (collection_id, maximum_order) in maximum_orders {
            let next = allocators.get(&collection_id).ok_or_else(|| {
                corrupt("stored BSON document collection is missing its natural-order allocator")
            })?;
            if maximum_order >= *next {
                return Err(corrupt(
                    "stored BSON document natural order exceeds its durable allocator",
                ));
            }
        }
        Ok(())
    }

    fn load_natural_order_allocators(
        connection: &Connection,
    ) -> EngineResult<HashMap<DocumentCollectionId, i64>> {
        let rows = connection
            .prepare(
                "SELECT collection_id, next_natural_order
                 FROM briskdb_document_collections
                 WHERE lifecycle_state = 2 ORDER BY collection_id",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(sqlite_error::storage)?;
        rows.into_iter()
            .map(|(collection_id, next_natural_order)| {
                if next_natural_order <= 0 {
                    return Err(corrupt("document natural-order allocator is not positive"));
                }
                Ok((
                    DocumentCollectionId::from_validated(positive_u64(
                        collection_id,
                        "document natural-order allocator collection ID",
                    )?),
                    next_natural_order,
                ))
            })
            .collect()
    }

    fn load_provisioning(connection: &Connection) -> EngineResult<Option<Provisioning>> {
        connection
            .query_row(
                "SELECT collection_id, operation_id, shard_count, next_shard
                 FROM briskdb_document_provisioning WHERE singleton = 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error::storage)?
            .map(|(collection_id, operation_id, shard_count, next_shard)| {
                let operation_id: [u8; 32] = operation_id.try_into().map_err(|_| {
                    corrupt("document provisioning operation ID has an invalid width")
                })?;
                Ok(Provisioning {
                    collection_id: DocumentCollectionId::from_validated(positive_u64(
                        collection_id,
                        "document provisioning collection ID",
                    )?),
                    operation_id,
                    shard_count: bounded_u16(shard_count, "document provisioning shard count")?,
                    next_shard: bounded_u16(next_shard, "document provisioning cursor")?,
                })
            })
            .transpose()
    }

    fn load_catalog_rows(connection: &Connection) -> EngineResult<DocumentCatalog> {
        let databases = connection
            .prepare(
                "SELECT database_id, database_name FROM briskdb_document_databases
                 ORDER BY database_id",
            )
            .and_then(|mut statement| {
                statement
                    .query_map([], |row| {
                        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(sqlite_error::storage)?;
        let databases = databases
            .into_iter()
            .map(|(id, name)| Ok((positive_u64(id, "document database ID")?, name)))
            .collect::<EngineResult<HashMap<_, _>>>()?;
        let mut collections = Vec::new();
        let mut statement = connection
            .prepare(
                "SELECT collection_id, database_id, collection_name, options_bson,
                        placement_policy, placement_version
                 FROM briskdb_document_collections
                 WHERE lifecycle_state = 2 ORDER BY collection_id",
            )
            .map_err(sqlite_error::storage)?;
        let mut rows = statement.query([]).map_err(sqlite_error::storage)?;
        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
            let collection_id = positive_u64(
                row.get::<_, i64>(0).map_err(sqlite_error::storage)?,
                "document collection ID",
            )?;
            let database_id = positive_u64(
                row.get::<_, i64>(1).map_err(sqlite_error::storage)?,
                "document database ID",
            )?;
            let name = row.get::<_, String>(2).map_err(sqlite_error::storage)?;
            let options_bson = row.get::<_, Vec<u8>>(3).map_err(sqlite_error::storage)?;
            let placement_policy = row.get::<_, i64>(4).map_err(sqlite_error::storage)?;
            let placement_version = row.get::<_, i64>(5).map_err(sqlite_error::storage)?;
            if placement_policy != 1 || placement_version != 1 {
                return Err(corrupt("document collection has an unsupported placement"));
            }
            let database_name = databases.get(&database_id).cloned().ok_or_else(|| {
                corrupt("document collection references a missing logical database")
            })?;
            let options = DocumentCollectionOptions::new(decode_metadata_document(
                &options_bson,
                "document collection options",
            )?)
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::DataCorruption,
                    "stored document collection options are invalid",
                    error,
                )
            })?;
            let id = DocumentCollectionId::from_validated(collection_id);
            let indexes = load_indexes(connection, id)?;
            collections.push(DocumentCollectionMetadata::from_validated_parts(
                id,
                DocumentDatabaseId::from_validated(database_id),
                database_name,
                name,
                options,
                DocumentPlacement::HashByIdV1,
                indexes,
            ));
        }
        Ok(DocumentCatalog::from_validated_collections(
            collections.into_boxed_slice(),
        ))
    }

    // Frozen UUIDv8 derivation: domain-separated BLAKE3 over the durable random
    // root layout ID and never-reused little-endian collection ID. Backups and
    // reopen preserve identity; drop/recreate and independent roots do not.
    fn collection_metadata_uuid(layout_id: [u8; 16], collection_id: u64) -> [u8; 16] {
        let mut hash = blake3::Hasher::new_derive_key("briskdb.collection-metadata.uuid.v1");
        hash.update(&layout_id);
        hash.update(&collection_id.to_le_bytes());
        let mut uuid = [0; 16];
        uuid.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        uuid[6] = (uuid[6] & 0x0f) | 0x80;
        uuid[8] = (uuid[8] & 0x3f) | 0x80;
        uuid
    }

    type StoredCollectionRow = (i64, i64, String, String, Vec<u8>, i64, i64);

    #[test]
    fn collection_metadata_uuid_v1_is_frozen() {
        assert_eq!(
            collection_metadata_uuid([7; 16], 42),
            [
                120, 58, 80, 207, 9, 43, 140, 200, 170, 85, 175, 1, 132, 122, 162, 148
            ]
        );
        assert_ne!(
            collection_metadata_uuid([7; 16], 42),
            collection_metadata_uuid([8; 16], 42)
        );
        assert_ne!(
            collection_metadata_uuid([7; 16], 42),
            collection_metadata_uuid([7; 16], 43)
        );
    }

    fn load_collection_row(
        connection: &Connection,
        database: &str,
        collection: &str,
        control: &OperationControl,
    ) -> EngineResult<Option<DocumentCollectionMetadata>> {
        ensure_control_active(control, "before reading document collection metadata")?;
        let row = connection
            .query_row(
                "SELECT c.collection_id, c.database_id, d.database_name,
                        c.collection_name, c.options_bson, c.placement_policy,
                        c.placement_version
                 FROM briskdb_document_collections AS c
                 JOIN briskdb_document_databases AS d
                   ON d.database_id = c.database_id
                 WHERE c.lifecycle_state = 2
                   AND d.database_name = ?1
                   AND c.collection_name = ?2",
                params![database, collection],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error::storage)?;
        let metadata = row
            .map(|row| decode_collection_row(connection, row, Some(control)))
            .transpose()?;
        ensure_control_active(control, "after reading document collection metadata")?;
        Ok(metadata)
    }

    fn load_collection_rows_for_database(
        connection: &Connection,
        database: &str,
        control: &OperationControl,
    ) -> EngineResult<Vec<DocumentCollectionMetadata>> {
        ensure_control_active(control, "before listing document collection metadata")?;
        let mut statement = connection
            .prepare(
                "SELECT c.collection_id, c.database_id, d.database_name,
                        c.collection_name, c.options_bson, c.placement_policy,
                        c.placement_version
                 FROM briskdb_document_collections AS c
                 JOIN briskdb_document_databases AS d
                   ON d.database_id = c.database_id
                 WHERE c.lifecycle_state = 2 AND d.database_name = ?1
                 ORDER BY c.collection_id",
            )
            .map_err(sqlite_error::storage)?;
        let mut rows = statement.query([database]).map_err(sqlite_error::storage)?;
        let mut stored = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
            ensure_control_active(control, "while listing document collection metadata")?;
            stored.push((
                row.get(0).map_err(sqlite_error::storage)?,
                row.get(1).map_err(sqlite_error::storage)?,
                row.get(2).map_err(sqlite_error::storage)?,
                row.get(3).map_err(sqlite_error::storage)?,
                row.get(4).map_err(sqlite_error::storage)?,
                row.get(5).map_err(sqlite_error::storage)?,
                row.get(6).map_err(sqlite_error::storage)?,
            ));
        }
        drop(rows);
        drop(statement);
        let mut collections = Vec::new();
        collections
            .try_reserve_exact(stored.len())
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::OutOfMemory,
                    "unable to reserve bounded document collection metadata",
                    error,
                )
            })?;
        for row in stored {
            ensure_control_active(control, "while decoding document collection metadata")?;
            collections.push(decode_collection_row(connection, row, Some(control))?);
        }
        ensure_control_active(control, "after listing document collection metadata")?;
        Ok(collections)
    }

    fn decode_collection_row(
        connection: &Connection,
        row: StoredCollectionRow,
        control: Option<&OperationControl>,
    ) -> EngineResult<DocumentCollectionMetadata> {
        let (
            collection_id,
            database_id,
            database_name,
            name,
            options_bson,
            placement_policy,
            placement_version,
        ) = row;
        if let Some(control) = control {
            ensure_control_active(control, "before decoding document collection metadata")?;
        }
        let collection_id = positive_u64(collection_id, "document collection ID")?;
        let database_id = positive_u64(database_id, "document database ID")?;
        if placement_policy != 1 || placement_version != 1 {
            return Err(corrupt("document collection has an unsupported placement"));
        }
        let options = DocumentCollectionOptions::new(decode_metadata_document(
            &options_bson,
            "document collection options",
        )?)
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "stored document collection options are invalid",
                error,
            )
        })?;
        if let Some(control) = control {
            ensure_control_active(control, "after decoding document collection options")?;
        }
        let id = DocumentCollectionId::from_validated(collection_id);
        let indexes = load_indexes_with_control(connection, id, control)?;
        Ok(DocumentCollectionMetadata::from_validated_parts(
            id,
            DocumentDatabaseId::from_validated(database_id),
            database_name,
            name,
            options,
            DocumentPlacement::HashByIdV1,
            indexes,
        ))
    }

    fn load_indexes(
        connection: &Connection,
        collection_id: DocumentCollectionId,
    ) -> EngineResult<Box<[DocumentIndexMetadata]>> {
        load_indexes_with_control(connection, collection_id, None)
    }

    fn load_indexes_with_control(
        connection: &Connection,
        collection_id: DocumentCollectionId,
        control: Option<&OperationControl>,
    ) -> EngineResult<Box<[DocumentIndexMetadata]>> {
        if let Some(control) = control {
            ensure_control_active(control, "before reading document index metadata")?;
        }
        let mut statement = connection
            .prepare(
                "SELECT i.index_name, i.spec_bson, i.is_unique, i.is_builtin, i.lifecycle_state, d.index_id
                 FROM briskdb_document_indexes AS i
                 LEFT JOIN briskdb_document_index_identities AS d
                   ON d.collection_id = i.collection_id AND d.index_name = i.index_name
                 WHERE i.collection_id = ?1
                 ORDER BY i.index_name COLLATE BINARY",
            )
            .map_err(sqlite_error::storage)?;
        let rows = statement
            .query_map([to_sqlite_id(collection_id)?], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        let expected_id = builtin_id_specification()?;
        let mut indexes = Vec::with_capacity(rows.len());
        for (name, spec_bson, unique, built_in, lifecycle, index_id) in rows {
            if let Some(control) = control {
                ensure_control_active(control, "while decoding document index metadata")?;
            }
            let specification =
                decode_metadata_document(&spec_bson, "document index specification")?;
            let lifecycle = DocumentIndexLifecycle::from_code(
                u32::try_from(lifecycle)
                    .map_err(|_| corrupt("document index lifecycle is outside its range"))?,
            )?;
            let built_in = built_in != 0;
            let unique = unique != 0;
            if built_in
                && (name != "_id_"
                    || !unique
                    || lifecycle != DocumentIndexLifecycle::Ready
                    || !specification.representation_eq(&expected_id))
            {
                return Err(corrupt("built-in document _id index metadata is invalid"));
            }
            indexes.push(DocumentIndexMetadata::from_validated_parts(
                DocumentIndexId::from_validated(positive_u64(
                    index_id.ok_or_else(|| corrupt("document index identity is missing"))?,
                    "document index identity",
                )?),
                name,
                specification,
                unique,
                built_in,
                lifecycle,
            ));
        }
        if let Some(control) = control {
            ensure_control_active(control, "after reading document index metadata")?;
        }
        Ok(indexes.into_boxed_slice())
    }

    fn builtin_id_specification() -> EngineResult<BsonDocument> {
        let key = BsonDocument::from_entries([("_id", BsonValue::Int32(1))])
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        BsonDocument::from_entries([
            ("v", BsonValue::Int32(2)),
            ("name", BsonValue::String("_id_".to_owned())),
            ("key", BsonValue::Document(key)),
            ("unique", BsonValue::Boolean(true)),
        ])
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))
    }

    fn decode_metadata_document(bytes: &[u8], context: &'static str) -> EngineResult<BsonDocument> {
        crate::document::decode_document(bytes).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                format!("stored {context} is invalid"),
                error,
            )
        })
    }

    fn ensure_database(connection: &Connection, name: &str) -> EngineResult<i64> {
        if let Some(id) = connection
            .query_row(
                "SELECT database_id FROM briskdb_document_databases WHERE database_name = ?1",
                [name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sqlite_error::storage)?
        {
            return Ok(id);
        }
        // Reject caller-induced capacity exhaustion before inserting a row.
        // The later manifest validator must reserve DataCorruption for invalid
        // stored state, not a rolled-back attempt to exceed a supported limit.
        let count: i64 = connection
            .query_row(
                "SELECT count(*) FROM briskdb_document_databases",
                [],
                |row| row.get(0),
            )
            .map_err(sqlite_error::storage)?;
        if count >= manifest::MAX_DOCUMENT_DATABASES as i64 {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "document database catalog is full",
            ));
        }
        let id = next_positive_id(connection, "database_high_water", "document database")?;
        connection
            .execute(
                "INSERT INTO briskdb_document_databases (
                    database_id, database_name, catalog_version
                 ) VALUES (?1, ?2, 1)",
                params![id, name],
            )
            .map_err(sqlite_error::storage)?;
        Ok(id)
    }

    fn allocate_index_identity(
        transaction: &rusqlite::Transaction<'_>,
        collection_id: i64,
        name: &str,
    ) -> EngineResult<()> {
        // Called only after insertion of a new declaration, in the same
        // IMMEDIATE transaction. Rollback discards both the ID and declaration;
        // committed drops remove mappings but never lower this high-water mark.
        let current = transaction
            .query_row(
                "SELECT index_high_water FROM briskdb_document_index_allocator WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sqlite_error::storage)?;
        if current < 0 {
            return Err(corrupt(
                "document index identity high-water mark is negative",
            ));
        }
        let next = current.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "document index identity space is exhausted",
            )
        })?;
        let changed = transaction
            .execute(
                "UPDATE briskdb_document_index_allocator SET index_high_water = ?1
             WHERE singleton = 1 AND index_high_water = ?2",
                params![next, current],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(corrupt(
                "document index identity allocation did not advance exactly once",
            ));
        }
        transaction.execute(
            "INSERT INTO briskdb_document_index_identities (index_id, collection_id, index_name)
             VALUES (?1, ?2, ?3)",
            params![next, collection_id, name],
        ).map_err(sqlite_error::storage)?;
        Ok(())
    }

    fn next_positive_id(connection: &Connection, column: &str, kind: &str) -> EngineResult<i64> {
        // Only these static callers select a column. Allocation and catalog
        // insertion share the caller's IMMEDIATE manifest transaction.
        let sql = format!("SELECT {column} FROM briskdb_document_identities WHERE singleton = 1");
        let current = connection
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .map_err(sqlite_error::storage)?;
        let next = current.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("{kind} identity space is exhausted"),
            )
        })?;
        let changed = connection.execute(
            &format!("UPDATE briskdb_document_identities SET {column} = ?1 WHERE singleton = 1 AND {column} = ?2"),
            params![next, current],
        ).map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(corrupt(
                "document identity allocation did not advance exactly once",
            ));
        }
        Ok(next)
    }

    fn existing_collection(
        connection: &Connection,
        database: &str,
        collection: &str,
    ) -> EngineResult<Option<(i64, Vec<u8>)>> {
        connection
            .query_row(
                "SELECT c.lifecycle_state, c.options_bson
                 FROM briskdb_document_collections AS c
                 JOIN briskdb_document_databases AS d ON d.database_id = c.database_id
                 WHERE d.database_name = ?1 AND c.collection_name = ?2",
                params![database, collection],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sqlite_error::storage)
    }

    fn require_active_collection(
        connection: &Connection,
        id: DocumentCollectionId,
    ) -> EngineResult<()> {
        let state = connection
            .query_row(
                "SELECT lifecycle_state FROM briskdb_document_collections
                 WHERE collection_id = ?1",
                [to_sqlite_id(id)?],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(sqlite_error::storage)?;
        match state {
            Some(COLLECTION_ACTIVE) => Ok(()),
            Some(_) => Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "document collection is not active",
            )),
            None => Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document collection does not exist",
            )),
        }
    }

    fn required_document_id(document: &BsonDocument) -> EngineResult<&BsonValue> {
        document
            .get_unique("_id")
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document storage requires exactly one top-level _id field",
                )
            })
    }

    fn prepare_document(
        storage: &Storage,
        document: &BsonDocument,
    ) -> EngineResult<PreparedDocumentWrite> {
        let id = required_document_id(document)?;
        let id_key = CanonicalBsonKey::encode(id)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let document_bson = encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let shard = storage.shard_for_key(id_key.as_bytes());
        Ok(PreparedDocumentWrite {
            id_key,
            document_bson,
            shard,
        })
    }

    #[cfg(any(feature = "tinymongo-import", test))]
    fn ensure_document_write_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
        ensure_document_operation_not_cancelled(cancellation, "while inserting document batch")
    }

    fn ensure_document_operation_not_cancelled(
        cancellation: &CancellationToken,
        boundary: &'static str,
    ) -> EngineResult<()> {
        if cancellation.is_cancelled() {
            Err(EngineError::new(
                EngineErrorKind::Cancelled,
                format!("document operation was cancelled {boundary}"),
            ))
        } else {
            Ok(())
        }
    }

    fn validate_natural_order_reservation_count(count: u64) -> EngineResult<i64> {
        if count == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document natural-order reservation count must be greater than zero",
            ));
        }
        i64::try_from(count).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::LimitExceeded,
                "document natural-order reservation exceeds SQLite's supported range",
                error,
            )
        })
    }

    fn document_natural_order_to_sqlite(natural_order: u64) -> EngineResult<i64> {
        if natural_order == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "document natural order must be greater than zero",
            ));
        }
        i64::try_from(natural_order).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::NumericOutOfRange,
                "document natural order does not fit SQLite",
                error,
            )
        })
    }

    fn document_natural_order_from_sqlite(natural_order: i64) -> EngineResult<u64> {
        u64::try_from(natural_order)
            .ok()
            .filter(|order| *order > 0)
            .ok_or_else(|| corrupt("stored BSON document natural order is not positive"))
    }

    fn provisioning_id(database: &str, collection: &str, options: &[u8]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"briskdb.document-provisioning.v1\0");
        hash_bytes(&mut hasher, database.as_bytes());
        hash_bytes(&mut hasher, collection.as_bytes());
        hash_bytes(&mut hasher, options);
        *hasher.finalize().as_bytes()
    }

    fn record_checksum(
        collection_id: DocumentCollectionId,
        shard: u16,
        natural_order: i64,
        id_key: &[u8],
        document_bson: &[u8],
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(RECORD_CHECKSUM_DOMAIN);
        hasher.update(&collection_id.get().to_le_bytes());
        hasher.update(&shard.to_le_bytes());
        hasher.update(&natural_order.to_le_bytes());
        hash_bytes(&mut hasher, id_key);
        hash_bytes(&mut hasher, document_bson);
        *hasher.finalize().as_bytes()
    }

    fn decode_record(
        collection_id: DocumentCollectionId,
        shard: u16,
        natural_order: i64,
        id_key: &[u8],
        bson: Vec<u8>,
        checksum: Vec<u8>,
        version: i64,
    ) -> EngineResult<BsonDocument> {
        if natural_order <= 0
            || version != 1
            || checksum.as_slice()
                != record_checksum(collection_id, shard, natural_order, id_key, &bson).as_slice()
        {
            return Err(corrupt(
                "stored BSON document checksum or format is invalid",
            ));
        }
        let document = crate::document::decode_document(&bson)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        let id = required_document_id(&document).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::DataCorruption,
                "stored BSON document has no unambiguous _id",
                error,
            )
        })?;
        let expected = CanonicalBsonKey::encode(id)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        if expected.as_bytes() != id_key {
            return Err(corrupt(
                "stored BSON document _id does not match its canonical key",
            ));
        }
        Ok(document)
    }

    fn decode_storage_record(
        collection_id: DocumentCollectionId,
        shard: u16,
        natural_order: i64,
        id_key: Vec<u8>,
        bson: Vec<u8>,
        checksum: Vec<u8>,
        version: i64,
    ) -> EngineResult<DocumentStorageRecord> {
        let canonical_id = CanonicalBsonKey::from_bytes(&id_key)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        let encoded_len = bson.len();
        let record_checksum: [u8; 32] = checksum
            .as_slice()
            .try_into()
            .map_err(|_| corrupt("stored BSON checksum has an invalid length"))?;
        let document = decode_record(
            collection_id,
            shard,
            natural_order,
            &id_key,
            bson,
            checksum,
            version,
        )?;
        Ok(DocumentStorageRecord {
            collection_id,
            shard,
            natural_order: document_natural_order_from_sqlite(natural_order)?,
            id_key: canonical_id,
            document,
            encoded_len,
            checksum: record_checksum,
        })
    }

    // A Transaction handle can outlive a SQL ROLLBACK (including SQLite's
    // automatic rollback on some failures). Never let a subsequent mutation
    // silently fall back to autocommit, even when the Rust type is correct.
    fn require_write_transaction(transaction: &Transaction<'_>) -> EngineResult<()> {
        if transaction.is_autocommit() {
            Err(EngineError::new(
                EngineErrorKind::Internal,
                "document mutation requires an active shard transaction",
            ))
        } else {
            Ok(())
        }
    }

    fn hash_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) {
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }

    fn to_sqlite_id(id: DocumentCollectionId) -> EngineResult<i64> {
        i64::try_from(id.get()).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::NumericOutOfRange,
                "document collection ID does not fit SQLite",
                error,
            )
        })
    }

    fn positive_u64(value: i64, field: &str) -> EngineResult<u64> {
        u64::try_from(value)
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| corrupt(format!("{field} is not a positive integer")))
    }

    fn bounded_u16(value: i64, field: &str) -> EngineResult<u16> {
        u16::try_from(value).map_err(|_| corrupt(format!("{field} is outside its range")))
    }

    #[cfg(test)]
    mod tests {
        use rusqlite::Connection;

        use super::*;
        use crate::{
            core::{CancellationReason, Database},
            document::{
                BsonBinary, BsonUuid, DocumentPlacement, UuidRepresentation, encode_document,
            },
            sql::SqlDialect,
        };

        fn shard_path(root: &std::path::Path, shard: u16) -> std::path::PathBuf {
            root.join("shards").join(format!("{shard:04}.sqlite"))
        }

        fn document(entries: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
            BsonDocument::from_entries(entries).unwrap()
        }

        #[test]
        fn pending_index_drop_crash_child() {
            let Ok(root) = std::env::var("BRISKDB_TEST_DOCUMENT_INDEX_DROP_ROOT") else {
                return;
            };
            let storage = Storage::open(root, 2).unwrap();
            let collection = storage
                .document_catalog()
                .unwrap()
                .collection("app", "one")
                .unwrap()
                .id();
            let _admission = storage.enter_schema_operation().unwrap();
            storage
                .drop_pending_document_index_controlled(
                    collection,
                    "pending",
                    OperationControl::new(None),
                )
                .unwrap();
            panic!("configured index drop crash checkpoint was not reached");
        }

        #[test]
        fn pending_index_drop_crash_boundaries_preserve_rows_and_allocator() {
            for (checkpoint, committed) in [
                ("index-before-commit:0", false),
                ("index-after-commit:0", true),
            ] {
                let temp = tempfile::tempdir().unwrap();
                let storage = Storage::open(temp.path(), 2).unwrap();
                let collection = storage
                    .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                    .unwrap();
                let spec = document([("a", BsonValue::Int64(1))]);
                let index = storage
                    .declare_document_index(collection.id(), "pending", &spec, true)
                    .unwrap();
                storage
                    .insert_document(
                        collection.id(),
                        &document([("_id", BsonValue::Int32(7)), ("a", BsonValue::Int32(8))]),
                    )
                    .unwrap();
                drop(storage);
                let result = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "storage::document::enabled::tests::pending_index_drop_crash_child",
                        "--nocapture",
                    ])
                    .env("BRISKDB_TEST_DOCUMENT_INDEX_DROP_ROOT", temp.path())
                    .env("BRISKDB_TEST_DOCUMENT_DROP_CRASH", checkpoint)
                    .output()
                    .unwrap();
                assert_eq!(
                    result.status.code(),
                    Some(73),
                    "{}",
                    String::from_utf8_lossy(&result.stderr)
                );
                let storage = Storage::open(temp.path(), 2).unwrap();
                let catalog = storage.document_catalog().unwrap();
                let recovered = catalog.collection("app", "one").unwrap();
                assert_eq!(recovered.indexes()[0].id(), collection.indexes()[0].id());
                assert_eq!(storage.document_count(collection.id()).unwrap(), 1);
                let retained = recovered
                    .indexes()
                    .iter()
                    .find(|index| index.name() == "pending");
                assert_eq!(retained.is_none(), committed);
                if let Some(retained) = retained {
                    assert_eq!(retained.id(), index.id());
                    assert!(retained.specification().representation_eq(&spec));
                }
                let redeclared = storage
                    .declare_document_index(collection.id(), "pending", &spec, true)
                    .unwrap();
                if committed {
                    assert!(redeclared.id() > index.id());
                } else {
                    assert_eq!(redeclared.id(), index.id());
                }
            }
        }

        #[test]
        fn document_drop_crash_child() {
            let Ok(root) = std::env::var("BRISKDB_TEST_DOCUMENT_DROP_ROOT") else {
                return;
            };
            let storage = Storage::open(root, 4).unwrap();
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            let database = std::env::var("BRISKDB_TEST_DOCUMENT_DROP_MODE").unwrap() == "database";
            storage
                .drop_document_namespace_controlled(
                    "app",
                    (!database).then_some("one"),
                    migration,
                    OperationControl::new(None),
                )
                .unwrap();
            panic!("configured crash checkpoint was not reached");
        }

        #[test]
        fn every_document_drop_commit_boundary_recovers_after_process_exit() {
            let mut checkpoints = vec![
                "before-intent:0".to_owned(),
                "after-intent:0".to_owned(),
                "before-completion:0".to_owned(),
                "after-completion:0".to_owned(),
            ];
            for shard in 0..4 {
                for point in [
                    "before-shard",
                    "after-shard",
                    "before-progress",
                    "after-progress",
                ] {
                    checkpoints.push(format!("{point}:{shard}"));
                }
            }
            // Both selective deletion and removal of the last optional shard
            // table must recover; database mode also removes multiple targets.
            for (database, keep_other) in
                [(false, true), (true, true), (true, false), (false, false)]
            {
                for checkpoint in &checkpoints {
                    let temp = tempfile::tempdir().unwrap();
                    let storage = Storage::open(temp.path(), 4).unwrap();
                    let one = storage
                        .create_document_collection(
                            "app",
                            "one",
                            &DocumentCollectionOptions::empty(),
                        )
                        .unwrap();
                    for id in 0..8 {
                        storage
                            .insert_document(one.id(), &document([("_id", BsonValue::Int32(id))]))
                            .unwrap();
                    }
                    if database {
                        storage
                            .create_document_collection(
                                "app",
                                "two",
                                &DocumentCollectionOptions::empty(),
                            )
                            .unwrap();
                    }
                    let other = keep_other.then(|| {
                        storage
                            .create_document_collection(
                                "other",
                                "keep",
                                &DocumentCollectionOptions::empty(),
                            )
                            .unwrap()
                    });
                    if let Some(other) = &other {
                        storage
                            .insert_document(other.id(), &document([("_id", BsonValue::Int32(1))]))
                            .unwrap();
                    }
                    let before_catalog = storage.document_catalog().unwrap();
                    let highest_index = before_catalog
                        .collections()
                        .iter()
                        .flat_map(|collection| collection.indexes())
                        .map(|index| index.id())
                        .max()
                        .unwrap();
                    let highest = before_catalog
                        .collections()
                        .iter()
                        .map(|c| c.id().get())
                        .max()
                        .unwrap();
                    drop(storage);
                    let output = std::process::Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "storage::document::enabled::tests::document_drop_crash_child",
                            "--nocapture",
                        ])
                        .env("BRISKDB_TEST_DOCUMENT_DROP_ROOT", temp.path())
                        .env(
                            "BRISKDB_TEST_DOCUMENT_DROP_MODE",
                            if database { "database" } else { "collection" },
                        )
                        .env("BRISKDB_TEST_DOCUMENT_DROP_CRASH", checkpoint)
                        .output()
                        .unwrap();
                    assert_eq!(
                        output.status.code(),
                        Some(73),
                        "{checkpoint}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let recovered = Storage::open(temp.path(), 4).unwrap();
                    let catalog = recovered.document_catalog().unwrap();
                    let before_intent = checkpoint == "before-intent:0";
                    assert_eq!(
                        catalog.collection("app", "one").is_some(),
                        before_intent,
                        "{checkpoint}"
                    );
                    if database {
                        assert_eq!(catalog.collection("app", "two").is_some(), before_intent);
                    }
                    assert_eq!(catalog.collection("other", "keep").is_some(), keep_other);
                    if let Some(other) = &other {
                        assert_eq!(recovered.document_count(other.id()).unwrap(), 1);
                        assert_eq!(
                            catalog.collection("other", "keep").unwrap().indexes(),
                            other.indexes()
                        );
                    }
                    if before_intent {
                        assert_eq!(recovered.document_count(one.id()).unwrap(), 8);
                    } else {
                        let fresh = recovered
                            .create_document_collection(
                                "app",
                                "one",
                                &DocumentCollectionOptions::empty(),
                            )
                            .unwrap();
                        assert!(fresh.id().get() > highest);
                        assert!(fresh.indexes()[0].id() > highest_index);
                        assert_eq!(recovered.document_count(fresh.id()).unwrap(), 0);
                    }
                    drop(recovered);
                    drop(Storage::open(temp.path(), 4).unwrap());
                }
            }
        }

        #[test]
        fn document_identity_exhaustion_is_atomic_and_never_recycles_a_deleted_maximum() {
            for column in ["database_high_water", "collection_high_water"] {
                let temp = tempfile::tempdir().unwrap();
                let storage = Storage::open(temp.path(), 4).unwrap();
                let connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
                connection
                    .execute(
                        &format!("UPDATE briskdb_document_identities SET {column} = ?1"),
                        [i64::MAX - 1],
                    )
                    .unwrap();
                manifest::refresh_manifest_digest(&connection).unwrap();
                let last = storage
                    .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                    .unwrap();
                assert_eq!(
                    if column == "database_high_water" {
                        last.database_id().get()
                    } else {
                        last.id().get()
                    },
                    i64::MAX as u64
                );
                let migration = storage.begin_schema_migration().unwrap();
                migration.wait_for_quiescence_blocking();
                assert!(
                    storage
                        .drop_document_namespace_controlled(
                            "app",
                            None,
                            migration,
                            OperationControl::new(None)
                        )
                        .unwrap()
                );
                assert_eq!(
                    storage
                        .create_document_collection(
                            "app",
                            "one",
                            &DocumentCollectionOptions::empty()
                        )
                        .unwrap_err()
                        .kind(),
                    EngineErrorKind::LimitExceeded
                );
                assert!(storage.document_catalog().unwrap().collections().is_empty());
                let value: i64 = connection
                    .query_row(
                        &format!("SELECT {column} FROM briskdb_document_identities"),
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(value, i64::MAX);
            }
        }

        #[test]
        fn controlled_catalog_mutations_honor_preaccepted_cancellation() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();

            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            let cancelled_create = OperationControl::new(None);
            assert!(cancelled_create.request_cancel(CancellationReason::Cancelled));
            assert_eq!(
                storage
                    .create_document_collection_controlled(
                        "engine_db",
                        "cancelled",
                        &DocumentCollectionOptions::empty(),
                        migration,
                        cancelled_create,
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );

            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            let collection = storage
                .create_document_collection_controlled(
                    "engine_db",
                    "events",
                    &DocumentCollectionOptions::empty(),
                    migration,
                    OperationControl::new(None),
                )
                .unwrap();

            for result in [
                storage
                    .document_catalog_controlled({
                        let control = OperationControl::new(None);
                        assert!(control.request_cancel(CancellationReason::Cancelled));
                        control
                    })
                    .map(|_| ()),
                storage
                    .declare_document_index_controlled(
                        collection.id(),
                        "value_1",
                        &document([("value", BsonValue::Int32(1))]),
                        false,
                        {
                            let control = OperationControl::new(None);
                            assert!(control.request_cancel(CancellationReason::Cancelled));
                            control
                        },
                    )
                    .map(|_| ()),
                storage
                    .reserve_document_natural_orders_controlled(collection.id(), 1, {
                        let control = OperationControl::new(None);
                        assert!(control.request_cancel(CancellationReason::Cancelled));
                        control
                    })
                    .map(|_| ()),
            ] {
                assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled);
            }
        }

        #[test]
        fn manifest_snapshot_reads_remain_coherent_across_concurrent_catalog_commit() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "engine_db",
                    "snapshot_events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let manifest_path = temp.path().join("manifest.sqlite");
            let (snapshot_ready_tx, snapshot_ready_rx) = std::sync::mpsc::sync_channel(0);
            let (continue_tx, continue_rx) = std::sync::mpsc::sync_channel(0);

            let reader = std::thread::spawn(move || {
                let mut connection = open_existing_manifest(&manifest_path).unwrap();
                let control = OperationControl::new(None);
                read_ready_manifest_snapshot(&mut connection, 2, |connection| {
                    snapshot_ready_tx.send(()).unwrap();
                    continue_rx.recv().unwrap();
                    load_collection_row(
                        connection,
                        "engine_db",
                        "snapshot_events",
                        control.as_ref(),
                    )
                })
                .unwrap()
                .unwrap()
            });

            snapshot_ready_rx.recv().unwrap();
            storage
                .declare_document_index(
                    collection.id(),
                    "value_1",
                    &document([("value", BsonValue::Int32(1))]),
                    false,
                )
                .unwrap();
            continue_tx.send(()).unwrap();

            let snapshot = reader.join().unwrap();
            assert_eq!(snapshot.indexes().len(), 1);
            let current = storage
                .document_collection_controlled(
                    "engine_db",
                    "snapshot_events",
                    OperationControl::new(None),
                )
                .unwrap()
                .unwrap();
            assert_eq!(current.indexes().len(), 2);
        }

        #[test]
        fn controlled_natural_order_reservation_interrupts_manifest_lock_wait() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "engine_db",
                    "locked_allocator",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let _operation = storage.enter_schema_operation().unwrap();

            let mut blocker = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            let blocker_transaction = blocker
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let worker_storage = storage.clone();
            let control = OperationControl::new(None);
            let worker_control = Arc::clone(&control);
            let collection_id = collection.id();
            let started = std::time::Instant::now();
            let worker = std::thread::spawn(move || {
                worker_storage.reserve_document_natural_orders_controlled(
                    collection_id,
                    1,
                    worker_control,
                )
            });

            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(control.request_cancel(CancellationReason::Cancelled));
            let error = worker.join().unwrap().unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Cancelled);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "manifest lock wait did not respond promptly to cancellation"
            );
            blocker_transaction.rollback().unwrap();

            assert_eq!(
                storage
                    .reserve_document_natural_orders_controlled(
                        collection.id(),
                        1,
                        OperationControl::new(None),
                    )
                    .unwrap(),
                1
            );
        }

        #[test]
        fn document_mutations_reject_transaction_handles_after_rollback() {
            for automatic in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let storage = Storage::open(temp.path(), 2).unwrap();
                let collection = storage
                    .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
                    .unwrap();
                let original = document([("_id", BsonValue::Int32(1))]);
                storage.insert_document(collection.id(), &original).unwrap();
                let prepared = storage.prepare_document_write(&original).unwrap();
                let connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
                let transaction = storage
                    .begin_document_write(
                        &connection,
                        collection.id(),
                        prepared.shard(),
                        &CancellationToken::new(),
                        None,
                    )
                    .unwrap();
                if automatic {
                    let error = transaction.execute_batch(
                        "INSERT OR ROLLBACK INTO briskdb_documents_v1 SELECT * FROM briskdb_documents_v1"
                    ).unwrap_err();
                    assert_eq!(
                        sqlite_error::statement(error).kind(),
                        EngineErrorKind::UniqueViolation
                    );
                } else {
                    transaction.execute_batch("ROLLBACK").unwrap();
                }
                assert!(transaction.is_autocommit());
                let cancellation = CancellationToken::new();
                let insert = storage
                    .insert_prepared_document_on_connection(
                        &transaction,
                        collection.id(),
                        2,
                        prepared.shard(),
                        &prepared,
                        &cancellation,
                    )
                    .unwrap_err();
                let replace = storage
                    .replace_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        1,
                        &prepared,
                        &cancellation,
                    )
                    .unwrap_err();
                let delete = storage
                    .delete_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        &cancellation,
                    )
                    .unwrap_err();
                for error in [insert, replace, delete] {
                    assert_eq!(error.kind(), EngineErrorKind::Internal);
                }
                drop(transaction);
                assert!(
                    storage
                        .get_document(collection.id(), &BsonValue::Int32(1))
                        .unwrap()
                        .unwrap()
                        .representation_eq(&original)
                );
            }
        }

        #[test]
        fn connection_bound_point_operations_preserve_identity_order_and_exact_bson() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "engine_db",
                    "point_records",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let cancellation = CancellationToken::new();
            let original = document([
                ("before_id", BsonValue::String("first".to_owned())),
                ("_id", BsonValue::Int32(17)),
                ("number", BsonValue::Double(-0.0)),
                ("after_id", BsonValue::Int64(17)),
            ]);
            let prepared = storage.prepare_document_write(&original).unwrap();
            let natural_order = storage
                .reserve_document_natural_orders_for_engine(collection.id(), 1, &cancellation)
                .unwrap();
            let connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
            storage
                .validate_unconfigured_shard(&connection, prepared.shard())
                .unwrap();
            let transaction = storage
                .begin_document_write(
                    &connection,
                    collection.id(),
                    prepared.shard(),
                    &cancellation,
                    None,
                )
                .unwrap();

            storage
                .insert_prepared_document_on_connection(
                    &transaction,
                    collection.id(),
                    natural_order,
                    prepared.shard(),
                    &prepared,
                    &cancellation,
                )
                .unwrap();
            let stored = storage
                .get_document_on_connection(
                    &transaction,
                    collection.id(),
                    prepared.shard(),
                    prepared.id_key(),
                    &cancellation,
                )
                .unwrap()
                .unwrap();
            assert_eq!(stored.collection_id(), collection.id());
            assert_eq!(stored.shard(), prepared.shard());
            assert_eq!(stored.natural_order(), natural_order);
            assert_eq!(stored.id_key(), prepared.id_key());
            assert_eq!(
                stored.encoded_len(),
                encode_document(&original).unwrap().len()
            );
            assert!(stored.document().representation_eq(&original));

            let replacement = document([
                ("_id", BsonValue::Int64(17)),
                ("number", BsonValue::Double(0.0)),
                ("replacement", BsonValue::Boolean(true)),
            ]);
            let prepared_replacement = storage.prepare_document_write(&replacement).unwrap();
            assert!(
                storage
                    .replace_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        natural_order,
                        &prepared_replacement,
                        &cancellation,
                    )
                    .unwrap()
            );
            let replaced = storage
                .get_document_on_connection(
                    &transaction,
                    collection.id(),
                    prepared.shard(),
                    prepared.id_key(),
                    &cancellation,
                )
                .unwrap()
                .unwrap();
            assert_eq!(replaced.natural_order(), natural_order);
            assert!(replaced.document().representation_eq(&replacement));

            let changed_id = storage
                .prepare_document_write(&document([
                    ("_id", BsonValue::Int32(18)),
                    ("replacement", BsonValue::Boolean(true)),
                ]))
                .unwrap();
            assert_eq!(
                storage
                    .replace_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        natural_order,
                        &changed_id,
                        &cancellation,
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::InvalidArgument
            );
            assert!(
                storage
                    .delete_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        &cancellation,
                    )
                    .unwrap()
            );
            assert!(
                storage
                    .get_document_on_connection(
                        &transaction,
                        collection.id(),
                        prepared.shard(),
                        prepared.id_key(),
                        &cancellation,
                    )
                    .unwrap()
                    .is_none()
            );
            transaction.commit().unwrap();
        }

        #[test]
        fn connection_bound_shard_scans_are_bounded_resumable_and_cancellable() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "engine_db",
                    "scan_records",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let cancellation = CancellationToken::new();
            let mut selected_shard = None;
            let mut prepared = Vec::new();
            for id in 0..100 {
                let document = document([
                    ("_id", BsonValue::Int32(id)),
                    ("ordinal", BsonValue::Int32(id)),
                ]);
                let candidate = storage.prepare_document_write(&document).unwrap();
                if selected_shard.is_none() {
                    selected_shard = Some(candidate.shard());
                }
                if Some(candidate.shard()) == selected_shard {
                    prepared.push(candidate);
                }
                if prepared.len() == 3 {
                    break;
                }
            }
            assert_eq!(prepared.len(), 3);
            let shard = selected_shard.unwrap();
            let first_order = storage
                .reserve_document_natural_orders_for_engine(
                    collection.id(),
                    prepared.len() as u64,
                    &cancellation,
                )
                .unwrap();
            let connection = storage.open_unconfigured_shard(shard).unwrap();
            storage
                .validate_unconfigured_shard(&connection, shard)
                .unwrap();
            let transaction = storage
                .begin_document_write(&connection, collection.id(), shard, &cancellation, None)
                .unwrap();
            for (offset, document) in prepared.iter().enumerate() {
                storage
                    .insert_prepared_document_on_connection(
                        &transaction,
                        collection.id(),
                        first_order + offset as u64,
                        shard,
                        document,
                        &cancellation,
                    )
                    .unwrap();
            }

            assert_eq!(
                storage
                    .count_document_shard_on_connection(
                        &transaction,
                        collection.id(),
                        shard,
                        &cancellation,
                    )
                    .unwrap(),
                3
            );
            let first_page = storage
                .scan_document_shard_on_connection(
                    &transaction,
                    collection.id(),
                    shard,
                    None,
                    2,
                    &cancellation,
                )
                .unwrap();
            assert_eq!(first_page.len(), 2);
            assert_eq!(first_page[0].natural_order(), first_order);
            assert_eq!(first_page[1].natural_order(), first_order + 1);
            let second_page = storage
                .scan_document_shard_on_connection(
                    &transaction,
                    collection.id(),
                    shard,
                    Some(first_page[1].natural_order()),
                    2,
                    &cancellation,
                )
                .unwrap();
            assert_eq!(second_page.len(), 1);
            assert_eq!(second_page[0].natural_order(), first_order + 2);

            for limit in [0, MAX_DOCUMENT_SHARD_SCAN_RECORDS + 1] {
                assert_eq!(
                    storage
                        .scan_document_shard_on_connection(
                            &transaction,
                            collection.id(),
                            shard,
                            None,
                            limit,
                            &cancellation,
                        )
                        .unwrap_err()
                        .kind(),
                    EngineErrorKind::InvalidArgument
                );
            }
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            assert_eq!(
                storage
                    .scan_document_shard_on_connection(
                        &transaction,
                        collection.id(),
                        shard,
                        None,
                        1,
                        &cancelled,
                    )
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Cancelled
            );
            transaction.commit().unwrap();
        }

        #[test]
        fn index_identities_survive_reopen_and_are_never_reused_after_namespace_drops() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let one = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            let two = storage
                .create_document_collection("app", "two", &DocumentCollectionOptions::empty())
                .unwrap();
            let spec = document([("value", BsonValue::Int32(1))]);
            let first = storage
                .declare_document_index(one.id(), "same_name", &spec, false)
                .unwrap();
            let second = storage
                .declare_document_index(two.id(), "same_name", &spec, false)
                .unwrap();
            let ids = [
                one.indexes()[0].id(),
                two.indexes()[0].id(),
                first.id(),
                second.id(),
            ];
            assert_eq!(ids.into_iter().collect::<HashSet<_>>().len(), 4);
            assert!(ids.into_iter().all(|id| id.get() > 0));
            assert_eq!(
                storage
                    .declare_document_index(one.id(), "same_name", &spec, false)
                    .unwrap(),
                first
            );
            drop(storage);
            let storage = Storage::open(temp.path(), 2).unwrap();
            let catalog = storage.document_catalog().unwrap();
            assert_eq!(
                catalog.collection("app", "one").unwrap().indexes()[1],
                first
            );
            assert_eq!(
                catalog.collection("app", "two").unwrap().indexes()[1],
                second
            );
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .drop_document_namespace_controlled(
                    "app",
                    Some("one"),
                    migration,
                    OperationControl::new(None),
                )
                .unwrap();
            let fresh = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            assert!(fresh.indexes()[0].id() > second.id());
            let fresh_index = storage
                .declare_document_index(fresh.id(), "same_name", &spec, false)
                .unwrap();
            assert!(fresh_index.id() > fresh.indexes()[0].id());
            assert_eq!(
                storage
                    .document_catalog()
                    .unwrap()
                    .collection("app", "two")
                    .unwrap()
                    .indexes()[1],
                second
            );
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            storage
                .drop_document_namespace_controlled(
                    "app",
                    None,
                    migration,
                    OperationControl::new(None),
                )
                .unwrap();
            assert!(storage.document_catalog().unwrap().collections().is_empty());
            drop(storage);
            let storage = Storage::open(temp.path(), 2).unwrap();
            let recreated = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            assert!(recreated.indexes()[0].id() > fresh_index.id());
        }

        #[test]
        fn concurrent_index_declarations_share_one_durable_allocator() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            let id = collection.id();
            let returned = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..4)
                    .map(|worker| {
                        let storage = &storage;
                        scope.spawn(move || {
                            (0..4)
                                .map(|ordinal| {
                                    storage
                                        .declare_document_index(
                                            id,
                                            &format!("w{worker}_{ordinal}"),
                                            &document([("a", BsonValue::Int32(1))]),
                                            false,
                                        )
                                        .unwrap()
                                        .id()
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().unwrap())
                    .collect::<HashSet<_>>()
            });
            assert_eq!(returned.len(), 16);
            assert!(!returned.contains(&collection.indexes()[0].id()));
            drop(storage);
            let storage = Storage::open(temp.path(), 2).unwrap();
            let catalog = storage.document_catalog().unwrap();
            let indexes = catalog.collection("app", "one").unwrap().indexes();
            assert_eq!(indexes.len(), 17);
            assert_eq!(
                indexes
                    .iter()
                    .filter(|index| !index.is_built_in())
                    .map(|index| index.id())
                    .collect::<HashSet<_>>(),
                returned
            );
        }

        #[test]
        fn index_identity_exhaustion_rolls_back_declarations_and_new_namespaces() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            let connection = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            connection
                .execute(
                    "UPDATE briskdb_document_index_allocator SET index_high_water = ?1",
                    [i64::MAX - 1],
                )
                .unwrap();
            manifest::refresh_manifest_digest(&connection).unwrap();
            let spec = document([("value", BsonValue::Int32(1))]);
            let last = storage
                .declare_document_index(collection.id(), "last", &spec, false)
                .unwrap();
            assert_eq!(last.id().get(), i64::MAX as u64);
            let before = storage.document_catalog().unwrap();
            let root: Vec<u8> = connection
                .query_row("SELECT manifest_digest FROM briskdb_integrity", [], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(
                storage
                    .declare_document_index(collection.id(), "last", &spec, false)
                    .unwrap(),
                last
            );
            for result in [
                storage
                    .declare_document_index(collection.id(), "too_late", &spec, false)
                    .map(|_| ()),
                storage
                    .create_document_collection(
                        "new_database",
                        "new_collection",
                        &DocumentCollectionOptions::empty(),
                    )
                    .map(|_| ()),
            ] {
                assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
            }
            assert_eq!(storage.document_catalog().unwrap(), before);
            assert_eq!(
                connection
                    .query_row("SELECT manifest_digest FROM briskdb_integrity", [], |r| r
                        .get::<_, Vec<
                        u8,
                    >>(
                        0
                    ))
                    .unwrap(),
                root
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT index_high_water FROM briskdb_document_index_allocator",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                i64::MAX
            );
            drop(storage);
            assert_eq!(
                Storage::open(temp.path(), 2)
                    .unwrap()
                    .document_catalog()
                    .unwrap(),
                before
            );
        }

        #[test]
        fn index_identity_allocation_and_mapping_roll_back_together() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection("app", "one", &DocumentCollectionOptions::empty())
                .unwrap();
            let mut connection =
                open_existing_manifest(&temp.path().join("manifest.sqlite")).unwrap();
            let original: i64 = connection
                .query_row(
                    "SELECT index_high_water FROM briskdb_document_index_allocator",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                transaction.execute("INSERT INTO briskdb_document_indexes VALUES (?1, 'rolled_back', x'0500000000', 0, 0, 1, 2)", [to_sqlite_id(collection.id()).unwrap()]).unwrap();
                allocate_index_identity(
                    &transaction,
                    to_sqlite_id(collection.id()).unwrap(),
                    "rolled_back",
                )
                .unwrap();
                manifest::validate_document_catalog(&transaction, 2).unwrap();
                manifest::refresh_manifest_digest(&transaction).unwrap();
                // Simulate cancellation/failure after allocation but before commit.
                transaction.rollback().unwrap();
            }
            assert_eq!(
                connection
                    .query_row(
                        "SELECT index_high_water FROM briskdb_document_index_allocator",
                        [],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                original
            );
            assert_eq!(
                storage
                    .document_catalog()
                    .unwrap()
                    .collection("app", "one")
                    .unwrap()
                    .indexes()
                    .len(),
                1
            );
            let next = storage
                .declare_document_index(
                    collection.id(),
                    "committed",
                    &document([("a", BsonValue::Int32(1))]),
                    false,
                )
                .unwrap();
            assert_eq!(next.id().get(), (original + 1) as u64);
        }

        #[test]
        fn idempotent_index_declaration_preserves_legacy_numeric_direction_bytes() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
                .unwrap();
            let legacy = document([("a", BsonValue::Int64(1)), ("b", BsonValue::Double(-1.0))]);
            storage
                .declare_document_index(collection.id(), "legacy", &legacy, false)
                .unwrap();
            let canonical = document([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(-1))]);
            let result = storage
                .declare_document_index(collection.id(), "legacy", &canonical, false)
                .unwrap();
            assert!(result.specification().representation_eq(&legacy));
            // Storage also retains older opaque specification envelopes. Do
            // not broaden their existing byte-exact conflict behavior while
            // recognizing normalized ordinary key directions.
            let opaque = document([(
                "key",
                BsonValue::Document(document([("a", BsonValue::Int64(1))])),
            )]);
            storage
                .declare_document_index(collection.id(), "opaque", &opaque, false)
                .unwrap();
            let different = document([(
                "key",
                BsonValue::Document(document([("a", BsonValue::Int32(1))])),
            )]);
            assert_eq!(
                storage
                    .declare_document_index(collection.id(), "opaque", &different, false)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition
            );
            drop(storage);
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .document_collection_controlled("app", "items", OperationControl::new(None))
                .unwrap()
                .unwrap();
            let index = collection
                .indexes()
                .iter()
                .find(|index| index.name() == "legacy")
                .unwrap();
            assert!(index.specification().representation_eq(&legacy));
        }

        #[test]
        fn restart_preserves_catalog_metadata_and_exact_bson_representation() {
            let temp = tempfile::tempdir().unwrap();
            let options_document = document([
                (
                    "validator",
                    BsonValue::Document(document([
                        ("second", BsonValue::Int64(2)),
                        ("first", BsonValue::Int32(1)),
                    ])),
                ),
                ("capped", BsonValue::Boolean(false)),
            ]);
            let options = DocumentCollectionOptions::new(options_document.clone()).unwrap();
            let stored = document([
                ("tail_first", BsonValue::String("kept-first".to_owned())),
                ("_id", BsonValue::Int32(17)),
                ("i64", BsonValue::Int64(17)),
                ("double", BsonValue::Double(-0.0)),
                ("binary", BsonValue::Binary(BsonBinary::new(0x80, [0, 255]))),
                ("tail_second", BsonValue::String("kept-second".to_owned())),
            ]);
            let encoded = encode_document(&stored).unwrap();

            let storage = Storage::open(temp.path(), 4).unwrap();
            let collection = storage
                .create_document_collection("ordered_db", "ordered_collection", &options)
                .unwrap();
            let secondary_specification = document([
                (
                    "key",
                    BsonValue::Document(document([
                        ("i64", BsonValue::Int32(1)),
                        ("tail_first", BsonValue::Int32(-1)),
                    ])),
                ),
                ("sparse", BsonValue::Boolean(true)),
            ]);
            let secondary = storage
                .declare_document_index(
                    collection.id(),
                    "i64_and_tail",
                    &secondary_specification,
                    false,
                )
                .unwrap();
            assert_eq!(secondary.lifecycle(), DocumentIndexLifecycle::PendingBuild);
            assert!(!secondary.is_built_in());
            assert!(!secondary.is_unique());
            let shard = storage.insert_document(collection.id(), &stored).unwrap();
            let scanned = storage.scan_documents(collection.id()).unwrap();
            assert_eq!(scanned.len(), 1);
            assert!(scanned[0].representation_eq(&stored));

            assert_eq!(collection.namespace(), "ordered_db.ordered_collection");
            assert_eq!(collection.placement(), DocumentPlacement::HashByIdV1);
            assert!(
                collection
                    .options()
                    .document()
                    .representation_eq(&options_document)
            );
            assert_eq!(collection.indexes().len(), 1);
            let id_index = &collection.indexes()[0];
            assert_eq!(id_index.name(), "_id_");
            assert!(id_index.is_unique());
            assert!(id_index.is_built_in());
            assert_eq!(id_index.lifecycle(), DocumentIndexLifecycle::Ready);
            assert!(
                id_index
                    .specification()
                    .representation_eq(&builtin_id_specification().unwrap())
            );
            assert_eq!(
                id_index
                    .specification()
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
                ["v", "name", "key", "unique"]
            );

            let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
            let (
                sqlite_collection_id,
                id_key,
                natural_order,
                bson,
                checksum,
                format,
                order_type,
                bson_type,
                key_type,
            ) = connection
                .query_row(
                    "SELECT collection_id, id_key, natural_order, document_bson,
                                document_checksum, storage_format_version,
                                typeof(natural_order), typeof(document_bson), typeof(id_key)
                         FROM briskdb_documents_v1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                            row.get::<_, Vec<u8>>(4)?,
                            row.get::<_, i64>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(sqlite_collection_id, collection.id().get() as i64);
            assert_eq!(
                id_key,
                CanonicalBsonKey::encode(&BsonValue::Int32(17))
                    .unwrap()
                    .as_bytes()
            );
            assert_eq!(natural_order, 1);
            assert_eq!(bson, encoded);
            assert_eq!(checksum.len(), 32);
            assert_eq!(format, 1);
            assert_eq!(
                (order_type.as_str(), bson_type.as_str(), key_type.as_str()),
                ("integer", "blob", "blob")
            );
            drop(connection);
            drop(storage);

            let reopened = Storage::open(temp.path(), 4).unwrap();
            let catalog = reopened.document_catalog().unwrap();
            let reopened_collection = catalog
                .collection("ordered_db", "ordered_collection")
                .unwrap();
            assert_eq!(reopened_collection.id(), collection.id());
            assert!(
                reopened_collection
                    .options()
                    .document()
                    .representation_eq(&options_document)
            );
            assert_eq!(reopened_collection.indexes().len(), 2);
            let reopened_secondary = reopened_collection
                .indexes()
                .iter()
                .find(|index| index.name() == "i64_and_tail")
                .unwrap();
            assert_eq!(
                reopened_secondary.lifecycle(),
                DocumentIndexLifecycle::PendingBuild
            );
            assert!(
                reopened_secondary
                    .specification()
                    .representation_eq(&secondary_specification)
            );
            let restored = reopened
                .get_document(collection.id(), &BsonValue::Int32(17))
                .unwrap()
                .unwrap();
            assert!(restored.representation_eq(&stored));
            assert_eq!(encode_document(&restored).unwrap(), encoded);
        }

        #[test]
        fn canonical_id_aliases_route_together_and_enforce_unique_id() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 8).unwrap();
            let collection = storage
                .create_document_collection(
                    "identity_db",
                    "values",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();

            let integer = document([
                ("_id", BsonValue::Int32(1)),
                ("kind", BsonValue::String("integer".to_owned())),
            ]);
            let integer_shard = storage.insert_document(collection.id(), &integer).unwrap();
            let integer_alias = BsonValue::Double(1.0);
            assert_eq!(
                integer_shard,
                storage.shard_for_key(CanonicalBsonKey::encode(&integer_alias).unwrap().as_bytes())
            );
            assert!(
                storage
                    .get_document(collection.id(), &integer_alias)
                    .unwrap()
                    .unwrap()
                    .representation_eq(&integer)
            );
            let duplicate_integer = document([
                ("_id", integer_alias),
                ("kind", BsonValue::String("double".to_owned())),
            ]);
            assert_eq!(
                storage
                    .insert_document(collection.id(), &duplicate_integer)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::UniqueViolation
            );

            let uuid_bytes = [0x5a; 16];
            let uuid = BsonValue::Uuid(BsonUuid::new(uuid_bytes, UuidRepresentation::Standard));
            let binary = BsonValue::Binary(BsonBinary::new(4, uuid_bytes));
            let uuid_document = document([
                ("_id", uuid.clone()),
                ("kind", BsonValue::String("uuid".to_owned())),
            ]);
            let uuid_shard = storage
                .insert_document(collection.id(), &uuid_document)
                .unwrap();
            assert_eq!(
                uuid_shard,
                storage.shard_for_key(CanonicalBsonKey::encode(&binary).unwrap().as_bytes())
            );
            let restored_uuid = storage
                .get_document(collection.id(), &binary)
                .unwrap()
                .unwrap();
            assert_eq!(restored_uuid, uuid_document);
            assert_eq!(
                encode_document(&restored_uuid).unwrap(),
                encode_document(&uuid_document).unwrap()
            );
            let duplicate_binary = document([
                ("_id", binary),
                ("kind", BsonValue::String("binary".to_owned())),
            ]);
            assert_eq!(
                storage
                    .insert_document(collection.id(), &duplicate_binary)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::UniqueViolation
            );
        }

        #[test]
        fn ordinary_sql_tables_do_not_become_document_collections() {
            let temp = tempfile::tempdir().unwrap();
            let database = Database::open(temp.path(), 2).unwrap();
            database
                .broadcast("CREATE TABLE sql_only (id INTEGER PRIMARY KEY, value TEXT)")
                .unwrap();
            drop(database);

            let storage = Storage::open(temp.path(), 2).unwrap();
            assert!(storage.document_catalog().unwrap().collections().is_empty());
            storage
                .create_document_collection("app", "sql_only", &DocumentCollectionOptions::empty())
                .unwrap();
            for shard in 0..storage.shard_count() {
                let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
                connection
                    .execute("INSERT INTO sql_only VALUES (1, 'preserved')", [])
                    .unwrap();
            }
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            assert!(
                storage
                    .drop_document_namespace_controlled(
                        "app",
                        None,
                        migration,
                        OperationControl::new(None)
                    )
                    .unwrap()
            );
            drop(storage);
            let storage = Storage::open(temp.path(), 2).unwrap();
            assert!(storage.document_catalog().unwrap().collections().is_empty());
            for shard in 0..storage.shard_count() {
                let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
                assert_eq!(
                    connection
                        .query_row("SELECT value FROM sql_only WHERE id = 1", [], |row| row
                            .get::<_, String>(
                            0
                        ))
                        .unwrap(),
                    "preserved"
                );
                assert_eq!(
                    connection
                        .query_row(
                            "SELECT type FROM sqlite_schema WHERE name = 'sql_only'",
                            [],
                            |row| row.get::<_, String>(0),
                        )
                        .unwrap(),
                    "table"
                );
            }
        }

        #[test]
        fn active_document_storage_stays_outside_generated_sql_table_inventory() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            storage
                .create_document_collection(
                    "documents",
                    "users",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            drop(storage);

            let mut database = Database::open(temp.path(), 2).unwrap();
            database
                .apply_generated_table_ddl(
                    SqlDialect::Sqlite,
                    "CREATE TABLE events (
                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                         payload TEXT NOT NULL
                     )",
                )
                .unwrap();
            assert!(
                database
                    .catalog()
                    .table("default", "events")
                    .unwrap()
                    .is_some()
            );
            drop(database);

            let reopened = Storage::open(temp.path(), 2).unwrap();
            assert!(
                reopened
                    .document_catalog()
                    .unwrap()
                    .collection("documents", "users")
                    .is_some()
            );
        }

        #[test]
        fn record_checksum_tampering_is_rejected_on_startup() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "tamper_db",
                    "checksums",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let stored = document([
                ("_id", BsonValue::String("record-1".to_owned())),
                ("value", BsonValue::Int32(42)),
            ]);
            let shard = storage.insert_document(collection.id(), &stored).unwrap();
            drop(storage);

            let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
            connection
                .execute(
                    "UPDATE briskdb_documents_v1 SET document_checksum = zeroblob(32)",
                    [],
                )
                .unwrap();
            drop(connection);

            let error = Storage::open(temp.path(), 2).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        }

        #[test]
        fn batch_insert_persists_global_natural_order_across_shards_and_restart() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 4).unwrap();
            let collection = storage
                .create_document_collection(
                    "ordered_import",
                    "events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let documents = (0..32)
                .map(|value| {
                    document([
                        ("_id", BsonValue::String(format!("event-{value}"))),
                        ("source_order", BsonValue::Int32(value)),
                    ])
                })
                .collect::<Vec<_>>();
            storage
                .insert_documents(collection.id(), &documents, &CancellationToken::new())
                .unwrap();
            let appended = document([
                ("_id", BsonValue::String("event-appended".to_owned())),
                ("source_order", BsonValue::Int32(32)),
            ]);
            storage.insert_document(collection.id(), &appended).unwrap();

            let expected = documents
                .iter()
                .chain(std::iter::once(&appended))
                .collect::<Vec<_>>();
            let scanned = storage.scan_documents(collection.id()).unwrap();
            assert_eq!(scanned.len(), expected.len());
            assert!(
                scanned
                    .iter()
                    .zip(&expected)
                    .all(|(actual, expected)| actual.representation_eq(expected))
            );

            let mut persisted_orders = Vec::new();
            for shard in 0..storage.shard_count() {
                let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
                let mut orders = connection
                    .prepare(
                        "SELECT natural_order FROM briskdb_documents_v1
                         WHERE collection_id = ?1 ORDER BY natural_order",
                    )
                    .unwrap()
                    .query_map([collection.id().get() as i64], |row| row.get::<_, i64>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                persisted_orders.append(&mut orders);
            }
            persisted_orders.sort_unstable();
            assert_eq!(persisted_orders, (1_i64..=33).collect::<Vec<_>>());
            let manifest = Connection::open(temp.path().join("manifest.sqlite")).unwrap();
            assert_eq!(
                manifest
                    .query_row(
                        "SELECT next_natural_order FROM briskdb_document_collections
                         WHERE collection_id = ?1",
                        [collection.id().get() as i64],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap(),
                34
            );
            drop(manifest);
            drop(storage);

            let reopened = Storage::open(temp.path(), 4).unwrap();
            let scanned = reopened.scan_documents(collection.id()).unwrap();
            assert!(
                scanned
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| actual.representation_eq(expected))
            );
        }

        #[test]
        fn document_mutations_refuse_to_reseal_unrelated_manifest_tampering() {
            fn tamper_manifest(root: &std::path::Path) -> Vec<u8> {
                let connection = Connection::open(root.join("manifest.sqlite")).unwrap();
                connection
                    .execute(
                        "INSERT INTO briskdb_logical_databases (database_id, database_name)
                         VALUES (2, 'unrelated_tamper')",
                        [],
                    )
                    .unwrap();
                connection
                    .query_row(
                        "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                        [],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .unwrap()
            }

            fn assert_root_unchanged(root: &std::path::Path, expected: &[u8]) {
                let connection = Connection::open(root.join("manifest.sqlite")).unwrap();
                let actual = connection
                    .query_row(
                        "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                        [],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .unwrap();
                assert_eq!(actual, expected);
            }

            let create = tempfile::tempdir().unwrap();
            let storage = Storage::open(create.path(), 2).unwrap();
            let root = tamper_manifest(create.path());
            let error = storage
                .create_document_collection("new_db", "events", &DocumentCollectionOptions::empty())
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_root_unchanged(create.path(), &root);

            let deletion = tempfile::tempdir().unwrap();
            let storage = Storage::open(deletion.path(), 2).unwrap();
            storage
                .create_document_collection(
                    "existing_db",
                    "events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let root = tamper_manifest(deletion.path());
            let migration = storage.begin_schema_migration().unwrap();
            migration.wait_for_quiescence_blocking();
            let error = storage
                .drop_document_namespace_controlled(
                    "existing_db",
                    None,
                    migration,
                    OperationControl::new(None),
                )
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_root_unchanged(deletion.path(), &root);
            let connection = Connection::open(deletion.path().join("manifest.sqlite")).unwrap();
            assert_eq!(
                connection
                    .query_row(
                        "SELECT count(*) FROM briskdb_document_deletion",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row(
                        "SELECT count(*) FROM briskdb_document_collections",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                1
            );

            let index = tempfile::tempdir().unwrap();
            let storage = Storage::open(index.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "existing_db",
                    "events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let root = tamper_manifest(index.path());
            let error = storage
                .declare_document_index(
                    collection.id(),
                    "value_1",
                    &document([("value", BsonValue::Int32(1))]),
                    false,
                )
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_root_unchanged(index.path(), &root);

            let insert = tempfile::tempdir().unwrap();
            let storage = Storage::open(insert.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "existing_db",
                    "events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let root = tamper_manifest(insert.path());
            let error = storage
                .insert_document(
                    collection.id(),
                    &document([("_id", BsonValue::String("event-1".to_owned()))]),
                )
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            assert_root_unchanged(insert.path(), &root);
        }

        #[test]
        fn persisted_degraded_state_fences_document_reads_and_mutations() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "degraded_db",
                    "events",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let stored = document([
                ("_id", BsonValue::String("event-1".to_owned())),
                ("value", BsonValue::Int32(1)),
            ]);
            storage.insert_document(collection.id(), &stored).unwrap();

            let manifest_path = temp.path().join("manifest.sqlite");
            let mut manifest_connection = open_existing_manifest(&manifest_path).unwrap();
            configure_manifest_connection(&manifest_connection).unwrap();
            manifest::mark_degraded(
                &mut manifest_connection,
                storage.shard_count(),
                &storage.shard_layout,
            )
            .unwrap();
            let degraded_digest = manifest_connection
                .query_row(
                    "SELECT manifest_digest FROM briskdb_integrity WHERE singleton = 1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .unwrap();
            let catalog_counts = manifest_connection
                .query_row(
                    "SELECT
                         (SELECT count(*) FROM briskdb_document_collections),
                         (SELECT count(*) FROM briskdb_document_indexes),
                         (SELECT next_natural_order FROM briskdb_document_collections
                          WHERE collection_id = ?1)",
                    [collection.id().get() as i64],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .unwrap();
            drop(manifest_connection);

            let id = BsonValue::String("event-1".to_owned());
            for error in [
                storage.document_catalog_inner().unwrap_err(),
                storage
                    .get_document_inner(collection.id(), &id)
                    .unwrap_err(),
                storage
                    .insert_document_inner(
                        collection.id(),
                        &document([("_id", BsonValue::String("event-2".to_owned()))]),
                    )
                    .unwrap_err(),
                storage
                    .declare_document_index_inner(
                        collection.id(),
                        "value_1",
                        &document([("value", BsonValue::Int32(1))]),
                        false,
                    )
                    .unwrap_err(),
                storage
                    .create_document_collection_inner(
                        "degraded_db",
                        "other",
                        &DocumentCollectionOptions::empty(),
                    )
                    .unwrap_err(),
            ] {
                assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
            }

            let manifest_connection = Connection::open(&manifest_path).unwrap();
            let unchanged = manifest_connection
                .query_row(
                    "SELECT
                         manifest_digest,
                         (SELECT count(*) FROM briskdb_document_collections),
                         (SELECT count(*) FROM briskdb_document_indexes),
                         (SELECT next_natural_order FROM briskdb_document_collections
                          WHERE collection_id = ?1)
                     FROM briskdb_integrity WHERE singleton = 1",
                    [collection.id().get() as i64],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(unchanged.0, degraded_digest);
            assert_eq!((unchanged.1, unchanged.2, unchanged.3), catalog_counts);
            let document_rows = (0..storage.shard_count())
                .map(|shard| {
                    Connection::open(shard_path(temp.path(), shard))
                        .unwrap()
                        .query_row("SELECT count(*) FROM briskdb_documents_v1", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .unwrap()
                })
                .sum::<i64>();
            assert_eq!(document_rows, 1);

            assert_eq!(
                storage
                    .get_document(collection.id(), &id)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
            assert_eq!(
                storage.schema_gate_snapshot().state,
                crate::storage::SchemaGateState::Degraded
            );
        }

        #[test]
        fn startup_rejects_natural_order_and_shard_route_tampering() {
            let order = tempfile::tempdir().unwrap();
            let storage = Storage::open(order.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "tamper_db",
                    "order",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let shard = storage
                .insert_document(
                    collection.id(),
                    &document([("_id", BsonValue::String("event-1".to_owned()))]),
                )
                .unwrap();
            drop(storage);
            let connection = Connection::open(shard_path(order.path(), shard)).unwrap();
            connection
                .execute(
                    "UPDATE briskdb_documents_v1 SET natural_order = natural_order + 1",
                    [],
                )
                .unwrap();
            drop(connection);
            assert_eq!(
                Storage::open(order.path(), 2).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );

            let route = tempfile::tempdir().unwrap();
            let storage = Storage::open(route.path(), 2).unwrap();
            let collection = storage
                .create_document_collection(
                    "tamper_db",
                    "route",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            let source_shard = storage
                .insert_document(
                    collection.id(),
                    &document([("_id", BsonValue::String("event-1".to_owned()))]),
                )
                .unwrap();
            drop(storage);
            let source = Connection::open(shard_path(route.path(), source_shard)).unwrap();
            let record = source
                .query_row(
                    "SELECT collection_id, id_key, natural_order, document_bson,
                            document_checksum, storage_format_version
                     FROM briskdb_documents_v1",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Vec<u8>>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Vec<u8>>(3)?,
                            row.get::<_, Vec<u8>>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    },
                )
                .unwrap();
            source
                .execute("DELETE FROM briskdb_documents_v1", [])
                .unwrap();
            drop(source);
            let destination_shard = 1 - source_shard;
            let destination =
                Connection::open(shard_path(route.path(), destination_shard)).unwrap();
            destination
                .execute(
                    "INSERT INTO briskdb_documents_v1 (
                        collection_id, id_key, natural_order, document_bson,
                        document_checksum, storage_format_version
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![record.0, record.1, record.2, record.3, record.4, record.5],
                )
                .unwrap();
            drop(destination);
            assert_eq!(
                Storage::open(route.path(), 2).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
        }

        #[test]
        fn record_checksum_binds_collection_shard_order_key_and_payload() {
            let collection = DocumentCollectionId::from_validated(7);
            let baseline = record_checksum(collection, 0, 1, b"key", b"document");
            assert_ne!(
                baseline,
                record_checksum(
                    DocumentCollectionId::from_validated(8),
                    0,
                    1,
                    b"key",
                    b"document"
                )
            );
            assert_ne!(
                baseline,
                record_checksum(collection, 1, 1, b"key", b"document")
            );
            assert_ne!(
                baseline,
                record_checksum(collection, 0, 2, b"key", b"document")
            );
            assert_ne!(
                baseline,
                record_checksum(collection, 0, 1, b"key2", b"document")
            );
            assert_ne!(
                baseline,
                record_checksum(collection, 0, 1, b"key", b"document2")
            );
        }

        #[test]
        fn document_table_schema_tampering_is_rejected_on_startup() {
            let temp = tempfile::tempdir().unwrap();
            let storage = Storage::open(temp.path(), 2).unwrap();
            storage
                .create_document_collection(
                    "tamper_db",
                    "schema",
                    &DocumentCollectionOptions::empty(),
                )
                .unwrap();
            drop(storage);

            let connection = Connection::open(shard_path(temp.path(), 0)).unwrap();
            connection
                .execute_batch("ALTER TABLE briskdb_documents_v1 ADD COLUMN injected INTEGER;")
                .unwrap();
            drop(connection);

            let error = Storage::open(temp.path(), 2).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
        }

        #[test]
        fn startup_resumes_document_collection_provisioning_from_durable_cursor() {
            for legacy in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                drop(Storage::open(temp.path(), 4).unwrap());

                let options_bson =
                    encode_document(DocumentCollectionOptions::empty().document()).unwrap();
                let id_specification_bson =
                    encode_document(&builtin_id_specification().unwrap()).unwrap();
                let operation_id = provisioning_id("recovery_db", "events", &options_bson);
                let manifest_path = temp.path().join("manifest.sqlite");
                let mut manifest_connection = open_existing_manifest(&manifest_path).unwrap();
                configure_manifest_connection(&manifest_connection).unwrap();
                configure_journal_mode(&manifest_connection).unwrap();
                let transaction = manifest_connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .unwrap();
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_databases (
                        database_id, database_name, catalog_version
                     ) VALUES (1, 'recovery_db', 1)",
                        [],
                    )
                    .unwrap();
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_collections (
                        collection_id, database_id, collection_name, options_bson,
                        bson_schema_version, storage_format_version,
                        placement_policy, placement_version, next_natural_order,
                        lifecycle_state
                     ) VALUES (1, 1, 'events', ?1, 1, 1, 1, 1, 1, 1)",
                        [options_bson],
                    )
                    .unwrap();
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_indexes (
                        collection_id, index_name, spec_bson, is_unique, is_builtin,
                        index_format_version, lifecycle_state
                     ) VALUES (1, '_id_', ?1, 1, 1, 1, ?2)",
                        rusqlite::params![id_specification_bson, INDEX_PENDING_BUILD],
                    )
                    .unwrap();
                allocate_index_identity(&transaction, 1, "_id_").unwrap();
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_provisioning (
                        singleton, collection_id, operation_id, shard_count, next_shard
                     ) VALUES (1, 1, ?1, 4, 1)",
                        [operation_id.as_slice()],
                    )
                    .unwrap();
                transaction.execute("UPDATE briskdb_document_identities SET database_high_water = 1, collection_high_water = 1 WHERE singleton = 1", []).unwrap();
                manifest::validate_document_catalog(&transaction, 4).unwrap();
                manifest::refresh_manifest_digest(&transaction).unwrap();
                transaction.commit().unwrap();
                drop(manifest_connection);

                let mut first_shard = Connection::open(shard_path(temp.path(), 0)).unwrap();
                ensure_schema(&mut first_shard).unwrap();
                if legacy {
                    first_shard
                        .execute_batch("DROP TABLE briskdb_document_index_entries_v1")
                        .unwrap();
                    manifest::downgrade_v18_manifest_to_v17_for_test(
                        &Connection::open(&manifest_path).unwrap(),
                        4,
                    )
                    .unwrap();
                }
                drop(first_shard);

                let storage = Storage::open(temp.path(), 4).unwrap();
                let catalog = storage.document_catalog().unwrap();
                let collection = catalog.collection("recovery_db", "events").unwrap();
                assert_eq!(collection.id().get(), 1);
                assert_eq!(collection.indexes().len(), 1);
                assert_eq!(collection.indexes()[0].name(), "_id_");
                for shard in 0..4 {
                    let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
                    require_schema(&connection).unwrap();
                }
                let manifest_connection = Connection::open(manifest_path).unwrap();
                assert_eq!(
                    manifest_connection
                        .query_row(
                            "SELECT COUNT(*) FROM briskdb_document_provisioning",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    0
                );
                assert_eq!(
                    manifest_connection
                        .query_row(
                            "SELECT lifecycle_state FROM briskdb_document_collections
                         WHERE collection_id = 1",
                            [],
                            |row| row.get::<_, i64>(0),
                        )
                        .unwrap(),
                    COLLECTION_ACTIVE
                );
            }
        }
    }
}

#[cfg(feature = "documents")]
pub(super) use enabled::DocumentIndexPreparations;
#[cfg(feature = "documents")]
pub(super) use enabled::recover_or_validate;
#[cfg(feature = "documents")]
pub(crate) use enabled::{
    DocumentStorageRecord, DocumentWriteTransaction, MAX_DOCUMENT_SHARD_SCAN_RECORDS,
    PreparedDocumentWrite,
};

#[cfg(not(feature = "documents"))]
pub(super) fn recover_or_validate(
    storage: &Storage,
    manifest_connection: &mut Connection,
) -> EngineResult<()> {
    super::manifest::validate_document_catalog(manifest_connection, storage.shard_count())?;
    let has_catalog = manifest_connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM briskdb_document_collections)
                OR EXISTS (SELECT 1 FROM briskdb_document_deletion)
                OR EXISTS (SELECT 1 FROM briskdb_document_index_storage WHERE lifecycle_state = 2)
                OR EXISTS (SELECT 1 FROM briskdb_document_index_operation)",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(sqlite_error::storage)?;
    if has_catalog {
        return Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "this data directory contains document collections; enable the documents feature",
        ));
    }
    for shard in 0..storage.shard_count() {
        let connection = storage.open_unconfigured_shard(shard)?;
        storage.validate_unconfigured_shard(&connection, shard)?;
        if validate_optional_schema(&connection)? {
            return Err(corrupt(
                "document storage exists without document catalog metadata",
            ));
        }
    }
    Ok(())
}
