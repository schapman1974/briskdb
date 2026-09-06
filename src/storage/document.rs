//! Versioned document catalog lifecycle and shard-local BSON records.

use rusqlite::Connection;
#[cfg(feature = "documents")]
use rusqlite::TransactionBehavior;

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

use super::Storage;

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
    object_type == "table"
        && name == RECORDS_TABLE
        && table_name == RECORDS_TABLE
        && sql.is_some_and(|sql| {
            normalize_schema_sql(sql) == normalize_schema_sql(RECORDS_SCHEMA_SQL)
        })
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
    Ok(true)
}

#[cfg(feature = "documents")]
fn ensure_schema(connection: &mut Connection) -> EngineResult<()> {
    if validate_optional_schema(connection)? {
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
    if !validate_optional_schema(&transaction)? {
        return Err(corrupt(
            "document storage table creation did not produce its exact schema",
        ));
    }
    transaction.commit().map_err(sqlite_error::storage)
}

#[cfg(feature = "documents")]
fn require_schema(connection: &Connection) -> EngineResult<()> {
    if validate_optional_schema(connection)? {
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
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

    use crate::{
        core::{CancellationToken, EngineError, EngineErrorKind, EngineResult, OperationControl},
        document::{
            BsonDocument, BsonErrorContext, BsonValue, CanonicalBsonKey, DocumentCatalog,
            DocumentCollectionId, DocumentCollectionMetadata, DocumentCollectionOptions,
            DocumentDatabaseId, DocumentIndexLifecycle, DocumentIndexMetadata, DocumentPlacement,
            encode_document,
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

        #[cfg(test)]
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
        match manifest::current_integrity(connection, shard_count)?.state() {
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
                            "briskdb_document_collections",
                            "collection_id",
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
                if let Some((existing_spec, existing_unique, lifecycle)) = existing {
                    if existing_spec != spec_bson
                        || existing_unique != i64::from(unique)
                        || lifecycle != INDEX_PENDING_BUILD
                    {
                        return Err(EngineError::new(
                            EngineErrorKind::FailedPrecondition,
                            "document index name already has a different declaration",
                        ));
                    }
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
                    manifest::validate_document_catalog(&transaction, self.shard_count())?;
                    manifest::refresh_manifest_digest(&transaction)?;
                    require_ready_manifest(&transaction, self.shard_count())?;
                }
                ensure_control_active(&control, "before committing document index declaration")?;
                transaction.commit().map_err(sqlite_error::storage)
            })?;
            let decoded = decode_metadata_document(&spec_bson, "document index specification")?;
            Ok(DocumentIndexMetadata::from_validated_parts(
                name.to_owned(),
                decoded,
                unique,
                false,
                DocumentIndexLifecycle::PendingBuild,
            ))
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
            let mut connection = self.open_unconfigured_shard(shard)?;
            self.validate_unconfigured_shard(&connection, shard)?;
            require_schema(&connection)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            self.insert_prepared_document_on_connection(
                &transaction,
                collection_id,
                natural_order,
                shard,
                prepared,
                cancellation,
            )?;
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(())
        }

        /// Insert one prepared record through an already-leased shard handle.
        ///
        /// Transaction ownership remains with the engine. The caller supplies
        /// the lease's physical shard identity and arms SQLite's progress and
        /// interrupt hooks around this call.
        #[allow(clippy::too_many_arguments)]
        pub(crate) fn insert_prepared_document_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            natural_order: u64,
            shard: u16,
            prepared: &PreparedDocumentWrite,
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            ensure_document_operation_not_cancelled(cancellation, "before inserting document")?;
            self.validate_prepared_document_route(shard, prepared)?;
            require_schema(connection)?;
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
            Ok(())
        }

        /// Replace one exact `_id` record while preserving its natural order.
        ///
        /// `false` means the target record no longer exists at the supplied
        /// natural order. A replacement cannot change the semantic `_id`.
        #[allow(clippy::too_many_arguments)]
        // The protocol-neutral command model already reserves replacement;
        // matcher/update semantics will consume this atomic storage primitive.
        #[allow(dead_code)]
        pub(crate) fn replace_document_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            id_key: &CanonicalBsonKey,
            natural_order: u64,
            replacement: &PreparedDocumentWrite,
            cancellation: &CancellationToken,
        ) -> EngineResult<bool> {
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
            Ok(changed == 1)
        }

        /// Delete one exact canonical `_id` through an already-leased handle.
        pub(crate) fn delete_document_on_connection(
            &self,
            connection: &Connection,
            collection_id: DocumentCollectionId,
            shard: u16,
            id_key: &CanonicalBsonKey,
            cancellation: &CancellationToken,
        ) -> EngineResult<bool> {
            ensure_document_operation_not_cancelled(cancellation, "before deleting document")?;
            self.validate_document_key_route(shard, id_key)?;
            require_schema(connection)?;
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
            ensure_document_operation_not_cancelled(cancellation, "before scanning documents")?;
            self.ensure_shard_in_range(shard)?;
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
            let mut statement = connection
                .prepare(
                    "SELECT natural_order, id_key, document_bson, document_checksum,
                            storage_format_version
                     FROM briskdb_documents_v1
                     WHERE collection_id = ?1 AND natural_order > ?2
                     ORDER BY natural_order LIMIT ?3",
                )
                .map_err(|error| {
                    shard_read_error(error, "failed to prepare stored BSON document scan")
                })?;
            let mut rows = statement
                .query(params![
                    to_sqlite_id(collection_id)?,
                    after_natural_order,
                    sqlite_limit
                ])
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
                records.push(decode_storage_record(
                    collection_id,
                    shard,
                    natural_order,
                    id_key,
                    document_bson,
                    checksum,
                    version,
                )?);
            }
            ensure_document_operation_not_cancelled(cancellation, "after scanning documents")?;
            Ok(records)
        }

        #[cfg(feature = "tinymongo-import")]
        pub(crate) fn document_count(
            &self,
            collection_id: DocumentCollectionId,
        ) -> EngineResult<u64> {
            let result = self.document_count_inner(collection_id);
            self.fail_closed_on_corruption(result)
        }

        #[cfg(feature = "tinymongo-import")]
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
        if let Some(provisioning) = load_provisioning(manifest_connection)? {
            recover_provisioning(storage, manifest_connection, provisioning)?;
        }
        let catalog = load_catalog_rows(manifest_connection)?;
        if !catalog.collections().is_empty() {
            validate_stored_records(storage, manifest_connection, &catalog)?;
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
        Ok(())
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
    ) -> EngineResult<()> {
        manifest::current_integrity(manifest_connection, storage.shard_count())?;
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
                decode_record(
                    collection_id,
                    shard,
                    natural_order,
                    &id_key,
                    bson,
                    checksum,
                    version,
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

    type StoredCollectionRow = (i64, i64, String, String, Vec<u8>, i64, i64);

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
                "SELECT index_name, spec_bson, is_unique, is_builtin, lifecycle_state
                 FROM briskdb_document_indexes WHERE collection_id = ?1
                 ORDER BY index_name COLLATE BINARY",
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
                ))
            })
            .map_err(sqlite_error::storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqlite_error::storage)?;
        let expected_id = builtin_id_specification()?;
        let mut indexes = Vec::with_capacity(rows.len());
        for (name, spec_bson, unique, built_in, lifecycle) in rows {
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
            if !built_in && lifecycle == DocumentIndexLifecycle::Ready {
                return Err(corrupt(
                    "secondary document index is marked ready before physical index support",
                ));
            }
            indexes.push(DocumentIndexMetadata::from_validated_parts(
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
        let id = next_positive_id(
            connection,
            "briskdb_document_databases",
            "database_id",
            "document database",
        )?;
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

    fn next_positive_id(
        connection: &Connection,
        table: &str,
        column: &str,
        kind: &str,
    ) -> EngineResult<i64> {
        let sql = format!("SELECT COALESCE(MAX({column}), 0) FROM {table}");
        let current = connection
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .map_err(sqlite_error::storage)?;
        current.checked_add(1).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                format!("{kind} identity space is exhausted"),
            )
        })
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
        })
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
            let mut connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
            storage
                .validate_unconfigured_shard(&connection, prepared.shard())
                .unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
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
            let mut connection = storage.open_unconfigured_shard(shard).unwrap();
            storage
                .validate_unconfigured_shard(&connection, shard)
                .unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
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
            for shard in 0..storage.shard_count() {
                let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
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
            transaction
                .execute(
                    "INSERT INTO briskdb_document_provisioning (
                        singleton, collection_id, operation_id, shard_count, next_shard
                     ) VALUES (1, 1, ?1, 4, 1)",
                    [operation_id.as_slice()],
                )
                .unwrap();
            manifest::validate_document_catalog(&transaction, 4).unwrap();
            manifest::refresh_manifest_digest(&transaction).unwrap();
            transaction.commit().unwrap();
            drop(manifest_connection);

            let mut first_shard = Connection::open(shard_path(temp.path(), 0)).unwrap();
            ensure_schema(&mut first_shard).unwrap();
            drop(first_shard);

            let storage = Storage::open(temp.path(), 4).unwrap();
            let catalog = storage.document_catalog().unwrap();
            let collection = catalog.collection("recovery_db", "events").unwrap();
            assert_eq!(collection.id().get(), 1);
            assert_eq!(collection.indexes().len(), 1);
            assert_eq!(collection.indexes()[0].name(), "_id_");
            for shard in 0..4 {
                let connection = Connection::open(shard_path(temp.path(), shard)).unwrap();
                assert!(super::super::validate_optional_schema(&connection).unwrap());
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

#[cfg(feature = "documents")]
pub(super) use enabled::recover_or_validate;
#[cfg(feature = "documents")]
pub(crate) use enabled::{
    DocumentStorageRecord, MAX_DOCUMENT_SHARD_SCAN_RECORDS, PreparedDocumentWrite,
};

#[cfg(not(feature = "documents"))]
pub(super) fn recover_or_validate(
    storage: &Storage,
    manifest_connection: &mut Connection,
) -> EngineResult<()> {
    super::manifest::validate_document_catalog(manifest_connection, storage.shard_count())?;
    let has_catalog = manifest_connection
        .query_row(
            "SELECT EXISTS (SELECT 1 FROM briskdb_document_collections)",
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
