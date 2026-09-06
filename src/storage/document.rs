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
    use std::collections::{HashMap, HashSet};

    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

    use crate::{
        core::{CancellationToken, EngineError, EngineErrorKind, EngineResult},
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
        configure_journal_mode, configure_manifest_connection, manifest, open_existing_manifest,
    };

    const COLLECTION_PROVISIONING: i64 = manifest::DOCUMENT_COLLECTION_PROVISIONING;
    const COLLECTION_ACTIVE: i64 = manifest::DOCUMENT_COLLECTION_ACTIVE;
    const INDEX_READY: i64 = manifest::DOCUMENT_INDEX_READY;
    const INDEX_PENDING_BUILD: i64 = manifest::DOCUMENT_INDEX_PENDING_BUILD;
    const RECORD_CHECKSUM_DOMAIN: &[u8] = b"briskdb.document-record.v1\0";

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

    impl Storage {
        pub(crate) fn document_catalog(&self) -> EngineResult<DocumentCatalog> {
            let result = self.document_catalog_inner();
            self.fail_closed_on_corruption(result)
        }

        fn document_catalog_inner(&self) -> EngineResult<DocumentCatalog> {
            let _operation = self.enter_schema_operation()?;
            self.load_document_catalog()
        }

        fn load_document_catalog(&self) -> EngineResult<DocumentCatalog> {
            let manifest_path = self.root.join("manifest.sqlite");
            let connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&connection)?;
            require_ready_manifest(&connection, self.shard_count())?;
            load_catalog_rows(&connection)
        }

        pub(crate) fn create_document_collection(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
        ) -> EngineResult<DocumentCollectionMetadata> {
            let result = self.create_document_collection_inner(database, collection, options);
            self.fail_closed_on_corruption(result)
        }

        fn create_document_collection_inner(
            &self,
            database: &str,
            collection: &str,
            options: &DocumentCollectionOptions,
        ) -> EngineResult<DocumentCollectionMetadata> {
            crate::document::validate_namespace(database, collection)?;
            let options_bson = encode_document(options.document())
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            debug_assert!(options_bson.len() <= manifest::MAX_DOCUMENT_METADATA_BSON_BYTES);
            let id_specification = builtin_id_specification()?;
            let id_specification_bson = encode_document(&id_specification)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;

            let mut migration = super::super::SchemaMigrationGuard::new(
                self.schema_coordination.gate.begin_new_migration()?,
            );
            migration.wait_for_quiescence_blocking();
            migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;

            let result = (|| {
                let manifest_path = self.root.join("manifest.sqlite");
                let mut connection = open_existing_manifest(&manifest_path)?;
                configure_manifest_connection(&connection)?;
                configure_journal_mode(&connection)?;
                require_ready_manifest(&connection, self.shard_count())?;

                if let Some(existing) = existing_collection(&connection, database, collection)? {
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
                    return load_catalog_rows(&connection)?
                        .collection(database, collection)
                        .cloned()
                        .ok_or_else(|| {
                            corrupt("active document collection disappeared from its catalog")
                        });
                }
                if load_provisioning(&connection)?.is_some() {
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
                migration.mark_pending_on_drop();
                transaction.commit().map_err(sqlite_error::storage)?;

                let provisioning = Provisioning {
                    collection_id: DocumentCollectionId::from_validated(
                        u64::try_from(collection_id).expect("positive SQLite document ID fits u64"),
                    ),
                    operation_id,
                    shard_count: self.shard_count(),
                    next_shard: 0,
                };
                recover_provisioning(self, &mut connection, provisioning)?;
                load_catalog_rows(&connection)?
                    .collection(database, collection)
                    .cloned()
                    .ok_or_else(|| {
                        corrupt("completed document collection is missing from its catalog")
                    })
            })();

            match result {
                Ok(metadata) => {
                    migration.publish_ready()?;
                    Ok(metadata)
                }
                Err(error) => Err(error),
            }
        }

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

        fn declare_document_index_inner(
            &self,
            collection_id: DocumentCollectionId,
            name: &str,
            specification: &BsonDocument,
            unique: bool,
        ) -> EngineResult<DocumentIndexMetadata> {
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
            let _operation = self.enter_schema_operation()?;
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&connection)?;
            configure_journal_mode(&connection)?;
            require_ready_manifest(&connection, self.shard_count())?;
            require_active_collection(&connection, collection_id)?;
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
            transaction.commit().map_err(sqlite_error::storage)?;
            let decoded = decode_metadata_document(&spec_bson, "document index specification")?;
            Ok(DocumentIndexMetadata::from_validated_parts(
                name.to_owned(),
                decoded,
                unique,
                false,
                DocumentIndexLifecycle::PendingBuild,
            ))
        }

        pub(crate) fn insert_document(
            &self,
            collection_id: DocumentCollectionId,
            document: &BsonDocument,
        ) -> EngineResult<u16> {
            let result = self.insert_document_inner(collection_id, document);
            self.fail_closed_on_corruption(result)
        }

        fn insert_document_inner(
            &self,
            collection_id: DocumentCollectionId,
            document: &BsonDocument,
        ) -> EngineResult<u16> {
            let _operation = self.enter_schema_operation()?;
            let (id_key, document_bson, shard) = prepare_document(self, document)?;
            let natural_order = self.reserve_document_natural_orders(collection_id, 1)?;
            self.insert_prepared_document(
                collection_id,
                natural_order,
                shard,
                &id_key,
                &document_bson,
            )?;
            Ok(shard)
        }

        pub(crate) fn insert_documents(
            &self,
            collection_id: DocumentCollectionId,
            documents: &[BsonDocument],
            cancellation: &CancellationToken,
        ) -> EngineResult<()> {
            let result = self.insert_documents_inner(collection_id, documents, cancellation);
            self.fail_closed_on_corruption(result)
        }

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
            let natural_order = self.reserve_document_natural_orders(
                collection_id,
                u64::try_from(documents.len()).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::LimitExceeded,
                        "document batch length exceeds its supported range",
                        error,
                    )
                })?,
            )?;
            // A committed range is never reused. Cancellation or a later shard
            // failure may leave gaps, preserving monotonic order after retry.
            for (offset, document) in documents.iter().enumerate() {
                ensure_document_write_not_cancelled(cancellation)?;
                let offset = i64::try_from(offset).map_err(|error| {
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
                let (id_key, document_bson, shard) = prepare_document(self, document)?;
                self.insert_prepared_document(
                    collection_id,
                    order,
                    shard,
                    &id_key,
                    &document_bson,
                )?;
            }
            Ok(())
        }

        fn reserve_document_natural_orders(
            &self,
            collection_id: DocumentCollectionId,
            count: u64,
        ) -> EngineResult<i64> {
            debug_assert!(count > 0);
            let count = i64::try_from(count).map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::LimitExceeded,
                    "document natural-order reservation exceeds SQLite's supported range",
                    error,
                )
            })?;
            let manifest_path = self.root.join("manifest.sqlite");
            let mut connection = open_existing_manifest(&manifest_path)?;
            configure_manifest_connection(&connection)?;
            configure_journal_mode(&connection)?;
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
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(first)
        }

        fn insert_prepared_document(
            &self,
            collection_id: DocumentCollectionId,
            natural_order: i64,
            shard: u16,
            id_key: &CanonicalBsonKey,
            document_bson: &[u8],
        ) -> EngineResult<()> {
            debug_assert_eq!(self.shard_for_key(id_key.as_bytes()), shard);
            let mut connection = self.open_unconfigured_shard(shard)?;
            self.validate_unconfigured_shard(&connection, shard)?;
            require_schema(&connection)?;
            let checksum = record_checksum(
                collection_id,
                shard,
                natural_order,
                id_key.as_bytes(),
                document_bson,
            );
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            transaction
                .execute(
                    "INSERT INTO briskdb_documents_v1 (
                        collection_id, id_key, natural_order, document_bson,
                        document_checksum, storage_format_version
                     ) VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                    params![
                        to_sqlite_id(collection_id)?,
                        id_key.as_bytes(),
                        natural_order,
                        document_bson,
                        checksum.as_slice()
                    ],
                )
                .map_err(sqlite_error::statement)?;
            transaction.commit().map_err(sqlite_error::storage)?;
            Ok(())
        }

        pub(crate) fn get_document(
            &self,
            collection_id: DocumentCollectionId,
            id: &BsonValue,
        ) -> EngineResult<Option<BsonDocument>> {
            let result = self.get_document_inner(collection_id, id);
            self.fail_closed_on_corruption(result)
        }

        fn get_document_inner(
            &self,
            collection_id: DocumentCollectionId,
            id: &BsonValue,
        ) -> EngineResult<Option<BsonDocument>> {
            let _operation = self.enter_schema_operation()?;
            self.require_active_document_collection(collection_id)?;
            let id_key = CanonicalBsonKey::encode(id)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            let shard = self.shard_for_key(id_key.as_bytes());
            let connection = self.open_unconfigured_shard(shard)?;
            self.validate_unconfigured_shard(&connection, shard)?;
            require_schema(&connection)?;
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
            row.map(|(natural_order, bson, checksum, version)| {
                decode_record(
                    collection_id,
                    shard,
                    natural_order,
                    id_key.as_bytes(),
                    bson,
                    checksum,
                    version,
                )
            })
            .transpose()
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
            let mut total = 0_u64;
            for shard in 0..self.shard_count() {
                let connection = self.open_unconfigured_shard(shard)?;
                self.validate_unconfigured_shard(&connection, shard)?;
                require_schema(&connection)?;
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
                let count = u64::try_from(count)
                    .map_err(|_| corrupt("stored BSON document count is outside its range"))?;
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
            let mut documents = Vec::new();
            for shard in 0..self.shard_count() {
                let connection = self.open_unconfigured_shard(shard)?;
                self.validate_unconfigured_shard(&connection, shard)?;
                require_schema(&connection)?;
                let mut statement = connection
                    .prepare(
                        "SELECT natural_order, id_key, document_bson, document_checksum,
                                storage_format_version
                         FROM briskdb_documents_v1 WHERE collection_id = ?1
                         ORDER BY natural_order",
                    )
                    .map_err(sqlite_error::storage)?;
                let mut rows = statement
                    .query([to_sqlite_id(collection_id)?])
                    .map_err(sqlite_error::storage)?;
                while let Some(row) = rows.next().map_err(sqlite_error::storage)? {
                    let natural_order = row.get::<_, i64>(0).map_err(sqlite_error::storage)?;
                    let key = row.get::<_, Vec<u8>>(1).map_err(sqlite_error::storage)?;
                    let bson = row.get::<_, Vec<u8>>(2).map_err(sqlite_error::storage)?;
                    let checksum = row.get::<_, Vec<u8>>(3).map_err(sqlite_error::storage)?;
                    let version = row.get::<_, i64>(4).map_err(sqlite_error::storage)?;
                    CanonicalBsonKey::from_bytes(&key)
                        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
                    if self.shard_for_key(&key) != shard {
                        return Err(corrupt(
                            "stored BSON document is on a shard that disagrees with its canonical _id route",
                        ));
                    }
                    documents.push((
                        natural_order,
                        decode_record(
                            collection_id,
                            shard,
                            natural_order,
                            &key,
                            bson,
                            checksum,
                            version,
                        )?,
                    ));
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
        ) -> EngineResult<i64> {
            let path = self.root.join("manifest.sqlite");
            let connection = open_existing_manifest(&path)?;
            configure_manifest_connection(&connection)?;
            require_ready_manifest(&connection, self.shard_count())?;
            connection
                .query_row(
                    "SELECT next_natural_order FROM briskdb_document_collections
                     WHERE collection_id = ?1 AND lifecycle_state = ?2",
                    params![to_sqlite_id(collection_id)?, COLLECTION_ACTIVE],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sqlite_error::storage)?
                .ok_or_else(|| corrupt("active document collection disappeared from its catalog"))
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
        mut provisioning: Provisioning,
    ) -> EngineResult<()> {
        if provisioning.shard_count != storage.shard_count() {
            return Err(corrupt(
                "document provisioning shard count differs from routing metadata",
            ));
        }
        while provisioning.next_shard < provisioning.shard_count {
            let shard = provisioning.next_shard;
            let mut connection = storage.open_unconfigured_shard(shard)?;
            storage.validate_unconfigured_shard(&connection, shard)?;
            ensure_schema(&mut connection)?;
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
            transaction.commit().map_err(sqlite_error::storage)?;
            provisioning.next_shard = shard + 1;
        }

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
        transaction.commit().map_err(sqlite_error::storage)
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

    fn load_indexes(
        connection: &Connection,
        collection_id: DocumentCollectionId,
    ) -> EngineResult<Box<[DocumentIndexMetadata]>> {
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
    ) -> EngineResult<(CanonicalBsonKey, Vec<u8>, u16)> {
        let id = required_document_id(document)?;
        let id_key = CanonicalBsonKey::encode(id)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let document_bson = encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        let shard = storage.shard_for_key(id_key.as_bytes());
        Ok((id_key, document_bson, shard))
    }

    fn ensure_document_write_not_cancelled(cancellation: &CancellationToken) -> EngineResult<()> {
        if cancellation.is_cancelled() {
            Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "document batch insertion was cancelled",
            ))
        } else {
            Ok(())
        }
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
            core::Database,
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
