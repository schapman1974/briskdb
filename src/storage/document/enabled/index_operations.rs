//! Offline non-unique build/drop authority. A build is published only after
//! every shard commits; restart discards an unpublished build, never activates it.

use super::*;

#[cfg(test)]
mod tests;

const BUILD: i64 = 1;
const DROP: i64 = 2;
const ABORT: i64 = 3;

struct Journal {
    index: DocumentIndexId,
    kind: i64,
    operation: Vec<u8>,
    next: u16,
}

#[cfg(test)]
fn checkpoint(point: &str, shard: u16) {
    if std::env::var("BRISKDB_TEST_DOCUMENT_INDEX_OPERATION_CRASH")
        .ok()
        .as_deref()
        == Some(format!("{point}:{shard}").as_str())
    {
        std::process::exit(75);
    }
}

fn load(connection: &Connection) -> EngineResult<Option<Journal>> {
    connection.query_row(
        "SELECT index_id, operation_kind, operation_id, next_shard FROM briskdb_document_index_operation WHERE singleton = 1", [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, Vec<u8>>(2)?, row.get::<_, i64>(3)?)),
    ).optional().map_err(sqlite_error::storage)?.map(|(id, kind, operation, next)| Ok(Journal {
        index: DocumentIndexId::from_validated(positive_u64(id, "document index operation identity")?),
        kind, operation, next: bounded_u16(next, "document index operation cursor")?,
    })).transpose()
}

fn advance(
    storage: &Storage,
    connection: &mut Connection,
    journal: &Journal,
    next: u16,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<()> {
    run_provisioning_step(connection, control, |connection| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        let changed = transaction.execute(
            "UPDATE briskdb_document_index_operation SET next_shard = ?1
             WHERE singleton = 1 AND index_id = ?2 AND operation_id = ?3 AND operation_kind = ?4 AND next_shard = ?5",
            params![next, journal.index.get() as i64, journal.operation, journal.kind, journal.next],
        ).map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(corrupt("document index operation lost its exact cursor"));
        }
        manifest::refresh_manifest_digest(&transaction)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        if let Some(control) = control {
            ensure_control_active(control, "before committing document index progress")?;
        }
        #[cfg(test)]
        checkpoint("before-cursor", journal.next);
        transaction.commit().map_err(sqlite_error::storage)?;
        #[cfg(test)]
        checkpoint("after-cursor", journal.next);
        Ok(())
    })
}

/// Startup has sole-process ownership whenever this checksummed journal exists.
pub(super) fn recover(storage: &Storage, connection: &mut Connection) -> EngineResult<()> {
    manifest::current_integrity(connection, storage.shard_count())?;
    let Some(mut journal) = load(connection)? else {
        return Ok(());
    };
    if journal.kind == BUILD {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        let changed = transaction
            .execute(
                "UPDATE briskdb_document_index_operation SET operation_kind = 3, next_shard = 0
             WHERE singleton = 1 AND operation_kind = 1 AND operation_id = ?1",
                [&journal.operation],
            )
            .map_err(sqlite_error::storage)?;
        if changed != 1 {
            return Err(corrupt("unpublished document index build lost its journal"));
        }
        manifest::refresh_manifest_digest(&transaction)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        transaction.commit().map_err(sqlite_error::storage)?;
        journal.kind = ABORT;
        journal.next = 0;
    }
    cleanup(storage, connection, journal, None)
}

fn cleanup(
    storage: &Storage,
    connection: &mut Connection,
    mut journal: Journal,
    control: Option<&Arc<OperationControl>>,
) -> EngineResult<()> {
    if !matches!(journal.kind, DROP | ABORT) {
        return Err(corrupt("invalid document index cleanup mode"));
    }
    while journal.next < storage.shard_count() {
        let shard = journal.next;
        let mut shard_connection = storage.open_unconfigured_shard(shard)?;
        run_provisioning_step(&mut shard_connection, control, |connection| {
            storage.validate_unconfigured_shard_nonterminal(connection, shard)?;
            require_schema(connection)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sqlite_error::storage)?;
            // The durable journal owns this globally non-reused index ID.
            // Cleanup removes only its derived entries, never any BSON record.
            transaction
                .execute(
                    "DELETE FROM briskdb_document_index_entries_v1 WHERE index_id = ?1",
                    [journal.index.get() as i64],
                )
                .map_err(sqlite_error::storage)?;
            if let Some(control) = control {
                ensure_control_active(control, "before committing document index cleanup")?;
            }
            #[cfg(test)]
            checkpoint("cleanup-before-shard", shard);
            transaction.commit().map_err(sqlite_error::storage)?;
            #[cfg(test)]
            checkpoint("cleanup-after-shard", shard);
            Ok(())
        })?;
        advance(storage, connection, &journal, shard + 1, control)?;
        journal.next = shard + 1;
    }
    run_provisioning_step(connection, control, |connection| {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error::storage)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        let target: (i64, String) = transaction.query_row(
            "SELECT collection_id, index_name FROM briskdb_document_index_identities WHERE index_id = ?1",
            [journal.index.get() as i64], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(sqlite_error::storage)?;
        let removed = transaction
            .execute(
                "DELETE FROM briskdb_document_index_operation WHERE singleton = 1 AND index_id = ?1
             AND operation_id = ?2 AND operation_kind = ?3 AND next_shard = shard_count",
                params![journal.index.get() as i64, journal.operation, journal.kind],
            )
            .map_err(sqlite_error::storage)?;
        if removed != 1 {
            return Err(corrupt(
                "document index cleanup lost its completion journal",
            ));
        }
        if journal.kind == DROP {
            let removed = transaction.execute("DELETE FROM briskdb_document_indexes
                WHERE collection_id = ?1 AND index_name = ?2 AND is_builtin = 0 AND lifecycle_state = 2",
                params![target.0, target.1]).map_err(sqlite_error::storage)?;
            if removed != 1 {
                return Err(corrupt("document index drop lost its declaration"));
            }
        }
        manifest::refresh_manifest_digest(&transaction)?;
        manifest::current_integrity(&transaction, storage.shard_count())?;
        if let Some(control) = control {
            ensure_control_active(control, "before completing document index cleanup")?;
        }
        #[cfg(test)]
        checkpoint("cleanup-before-completion", 0);
        transaction.commit().map_err(sqlite_error::storage)?;
        #[cfg(test)]
        checkpoint("cleanup-after-completion", 0);
        Ok(())
    })
}

fn visit_records(
    storage: &Storage,
    connection: &Connection,
    collection: DocumentCollectionId,
    shard: u16,
    check: &mut dyn FnMut() -> EngineResult<()>,
    mut visit: impl FnMut(DocumentStorageRecord) -> EngineResult<()>,
) -> EngineResult<()> {
    let mut statement = connection
        .prepare(
            "SELECT natural_order, id_key, document_bson, document_checksum, storage_format_version
         FROM briskdb_documents_v1 WHERE collection_id = ?1 ORDER BY natural_order, id_key",
        )
        .map_err(sqlite_error::storage)?;
    let mut rows = statement
        .query([to_sqlite_id(collection)?])
        .map_err(sqlite_error::storage)?;
    while let Some(row) = rows
        .next()
        .map_err(|error| shard_read_error(error, "failed to scan document index build source"))?
    {
        check()?;
        let record = decode_storage_record(
            collection,
            shard,
            row.get(0).map_err(sqlite_error::storage)?,
            row.get(1).map_err(sqlite_error::storage)?,
            row.get(2).map_err(sqlite_error::storage)?,
            row.get(3).map_err(sqlite_error::storage)?,
            row.get(4).map_err(sqlite_error::storage)?,
        )?;
        if storage.shard_for_key(record.id_key.as_bytes()) != shard {
            return Err(corrupt(
                "document index build source is routed to the wrong shard",
            ));
        }
        visit(record)?;
    }
    check()
}

impl Storage {
    pub(crate) fn drop_built_document_index_controlled(
        &self,
        database: &str,
        collection_name: &str,
        name: &str,
        mut migration: SchemaMigrationGuard,
        control: Arc<OperationControl>,
    ) -> EngineResult<()> {
        let result = (|| {
            ensure_control_active(&control, "before dropping built document index")?;
            migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;
            let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
            let catalog =
                run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                    configure_journal_mode(connection)?;
                    require_ready_manifest(connection, self.shard_count())?;
                    load_catalog_rows(connection)
                })?;
            let collection = catalog
                .collection(database, collection_name)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        "document collection does not exist",
                    )
                })?;
            let target = collection
                .indexes()
                .iter()
                .find(|index| index.name() == name)
                .ok_or_else(|| DocumentIndexError::NotFound.into_engine_error())?;
            if target.is_built_in() || matches!(name, "_id" | "_id_") {
                return Err(DocumentIndexError::Protected.into_engine_error());
            }
            // Resolve afresh under exclusive admission. The initial cached
            // dispatch decision was protected, but another DDL operation may
            // have completed between releasing shared and acquiring exclusive.
            if target.lifecycle() == DocumentIndexLifecycle::PendingBuild {
                self.drop_pending_document_index_controlled(
                    collection.id(),
                    name,
                    Arc::clone(&control),
                )?;
                migration.publish_ready()?;
                return Ok(());
            }
            let future =
                compile_indexes_with_candidate(&catalog, None, Some(target.id()), &mut || {
                    ensure_control_active(&control, "while preparing surviving document indexes")
                })
                .map_err(stored_index_error)?;
            let mut operation = vec![0_u8; 32];
            getrandom::fill(&mut operation).map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("unable to allocate document index operation identity: {error}"),
                )
            })?;
            run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                require_ready_manifest(&transaction, self.shard_count())?;
                let changed = transaction.execute("UPDATE briskdb_document_indexes SET lifecycle_state = 2
                    WHERE collection_id = ?1 AND index_name = ?2 AND is_builtin = 0 AND is_unique = 0 AND lifecycle_state = 1",
                    params![to_sqlite_id(collection.id())?, name]).map_err(sqlite_error::storage)?;
                if changed != 1 {
                    return Err(corrupt("document index drop lost its Ready declaration"));
                }
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_index_operation VALUES (1, ?1, 2, ?2, ?3, 0)",
                        params![target.id().get() as i64, operation, self.shard_count()],
                    )
                    .map_err(sqlite_error::storage)?;
                manifest::refresh_manifest_digest(&transaction)?;
                manifest::current_integrity(&transaction, self.shard_count())?;
                ensure_control_active(&control, "before committing document index drop intent")?;
                migration.mark_pending_on_drop();
                #[cfg(test)]
                checkpoint("drop-before-intent", 0);
                transaction.commit().map_err(sqlite_error::storage)?;
                #[cfg(test)]
                checkpoint("drop-after-intent", 0);
                Ok(())
            })?;
            cleanup(
                self,
                &mut connection,
                Journal {
                    index: target.id(),
                    kind: DROP,
                    operation,
                    next: 0,
                },
                Some(&control),
            )?;
            self.publish_document_indexes(future)?;
            migration.publish_ready()
        })();
        self.fail_closed_on_corruption(result)
    }

    pub(crate) fn build_document_index_controlled(
        &self,
        database: &str,
        collection_name: &str,
        name: &str,
        mut migration: SchemaMigrationGuard,
        control: Arc<OperationControl>,
    ) -> EngineResult<DocumentIndexMetadata> {
        let result = (|| {
            ensure_control_active(&control, "before building document index")?;
            migration.acquire_process_ownership(&self.schema_coordination.process_lease)?;
            let mut connection = open_existing_manifest(&self.root.join("manifest.sqlite"))?;
            let catalog =
                run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                    configure_journal_mode(connection)?;
                    require_ready_manifest(connection, self.shard_count())?;
                    load_catalog_rows(connection)
                })?;
            let collection = catalog
                .collection(database, collection_name)
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        "document collection does not exist",
                    )
                })?;
            let target = collection
                .indexes()
                .iter()
                .find(|index| index.name() == name)
                .ok_or_else(|| DocumentIndexError::NotFound.into_engine_error())?;
            if target.is_built_in() {
                return Err(DocumentIndexError::Protected.into_engine_error());
            }
            if target.is_unique() {
                return Err(EngineError::new(
                    EngineErrorKind::Unsupported,
                    "physical unique document indexes require global uniqueness authority",
                ));
            }
            let current = compile_ready_indexes(&catalog, &mut || {
                ensure_control_active(&control, "while preparing document index authority")
            })?;
            let future =
                compile_indexes_with_candidate(&catalog, Some(target.id()), None, &mut || {
                    ensure_control_active(&control, "while preparing document index build")
                })?;
            let prepared = future
                .get(&collection.id())
                .ok_or_else(|| corrupt("document index build omitted its collection"))?;
            // Validate all current entries and the combined future write budget
            // before accepting durable intent. Data cannot change under this guard.
            for shard in 0..self.shard_count() {
                let mut source = self.open_unconfigured_shard(shard)?;
                run_provisioning_step(&mut source, Some(&control), |source| {
                    self.validate_unconfigured_shard_nonterminal(source, shard)?;
                    require_schema(source)?;
                    super::super::index_storage::require_no_orphans(source)?;
                    visit_records(
                        self,
                        source,
                        collection.id(),
                        shard,
                        &mut || {
                            ensure_control_active(
                                &control,
                                "while validating document index build source",
                            )
                        },
                        |record| {
                            let expected = current
                                .get(&collection.id())
                                .map(|index| {
                                    index.prepare_with_check(record.document(), &mut || {
                                        ensure_control_active(
                                            &control,
                                            "while validating existing document indexes",
                                        )
                                    })
                                })
                                .transpose()
                                .map_err(stored_index_error)?;
                            super::super::index_storage::validate_record_entries(
                                source,
                                collection.id(),
                                shard,
                                record.id_key.as_bytes(),
                                &record.checksum,
                                expected.as_ref(),
                                &mut || {
                                    ensure_control_active(
                                        &control,
                                        "while validating existing document index entries",
                                    )
                                },
                            )?;
                            prepared.prepare_with_check(record.document(), &mut || {
                                ensure_control_active(
                                    &control,
                                    "while validating future document index entries",
                                )
                            })?;
                            Ok(())
                        },
                    )
                })?;
            }
            if target.lifecycle() == DocumentIndexLifecycle::Ready {
                migration.publish_ready()?;
                return Ok(target.clone());
            }
            let mut operation = vec![0_u8; 32];
            getrandom::fill(&mut operation).map_err(|error| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    format!("unable to allocate document index operation identity: {error}"),
                )
            })?;
            run_manifest_controlled(&mut connection, Arc::clone(&control), |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sqlite_error::storage)?;
                require_ready_manifest(&transaction, self.shard_count())?;
                transaction
                    .execute(
                        "INSERT INTO briskdb_document_index_operation VALUES (1, ?1, 1, ?2, ?3, 0)",
                        params![target.id().get() as i64, operation, self.shard_count()],
                    )
                    .map_err(sqlite_error::storage)?;
                manifest::refresh_manifest_digest(&transaction)?;
                manifest::current_integrity(&transaction, self.shard_count())?;
                ensure_control_active(&control, "before committing document index build intent")?;
                migration.mark_pending_on_drop();
                #[cfg(test)]
                checkpoint("before-intent", 0);
                transaction.commit().map_err(sqlite_error::storage)?;
                #[cfg(test)]
                checkpoint("after-intent", 0);
                Ok(())
            })?;
            let mut journal = Journal {
                index: target.id(),
                kind: BUILD,
                operation,
                next: 0,
            };
            while journal.next < self.shard_count() {
                let shard = journal.next;
                let mut source = self.open_unconfigured_shard(shard)?;
                run_provisioning_step(&mut source, Some(&control), |source| {
                    self.validate_unconfigured_shard_nonterminal(source, shard)?;
                    require_schema(source)?;
                    let transaction = source
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(sqlite_error::storage)?;
                    visit_records(
                        self,
                        &transaction,
                        collection.id(),
                        shard,
                        &mut || {
                            ensure_control_active(
                                &control,
                                "while scanning document index build source",
                            )
                        },
                        |record| {
                            let entries =
                                prepared.prepare_with_check(record.document(), &mut || {
                                    ensure_control_active(
                                        &control,
                                        "while generating document index build entries",
                                    )
                                })?;
                            super::super::index_storage::insert_selected_entries(
                                &transaction,
                                collection.id(),
                                shard,
                                record.id_key.as_bytes(),
                                &record.checksum,
                                &entries,
                                Some(target.id()),
                                &mut || {
                                    ensure_control_active(
                                        &control,
                                        "while storing document index build entries",
                                    )
                                },
                            )?;
                            super::super::index_storage::validate_record_entries(
                                &transaction,
                                collection.id(),
                                shard,
                                record.id_key.as_bytes(),
                                &record.checksum,
                                Some(&entries),
                                &mut || {
                                    ensure_control_active(
                                        &control,
                                        "while validating completed document index entries",
                                    )
                                },
                            )
                        },
                    )?;
                    ensure_control_active(&control, "before committing document index shard")?;
                    #[cfg(test)]
                    checkpoint("before-shard", shard);
                    transaction.commit().map_err(sqlite_error::storage)?;
                    #[cfg(test)]
                    checkpoint("after-shard", shard);
                    Ok(())
                })?;
                advance(self, &mut connection, &journal, shard + 1, Some(&control))?;
                journal.next = shard + 1;
            }
            let metadata = run_manifest_controlled(
                &mut connection,
                Arc::clone(&control),
                |connection| {
                    let transaction = connection
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(sqlite_error::storage)?;
                    manifest::current_integrity(&transaction, self.shard_count())?;
                    let removed = transaction.execute("DELETE FROM briskdb_document_index_operation WHERE singleton = 1
                    AND index_id = ?1 AND operation_id = ?2 AND operation_kind = 1 AND next_shard = shard_count",
                    params![target.id().get() as i64, journal.operation]).map_err(sqlite_error::storage)?;
                    let activated = transaction.execute("UPDATE briskdb_document_indexes SET lifecycle_state = 1
                    WHERE collection_id = ?1 AND index_name = ?2 AND is_builtin = 0 AND is_unique = 0 AND lifecycle_state = 2",
                    params![to_sqlite_id(collection.id())?, name]).map_err(sqlite_error::storage)?;
                    if removed != 1 || activated != 1 {
                        return Err(corrupt(
                            "document index activation lost its exact journal or declaration",
                        ));
                    }
                    manifest::refresh_manifest_digest(&transaction)?;
                    require_ready_manifest(&transaction, self.shard_count())?;
                    let metadata = load_indexes(&transaction, collection.id())?
                        .into_vec()
                        .into_iter()
                        .find(|index| index.id() == target.id())
                        .ok_or_else(|| corrupt("activated document index disappeared"))?;
                    ensure_control_active(&control, "before publishing document index authority")?;
                    #[cfg(test)]
                    checkpoint("before-activation", 0);
                    transaction.commit().map_err(sqlite_error::storage)?;
                    #[cfg(test)]
                    checkpoint("after-activation", 0);
                    Ok(metadata)
                },
            )?;
            self.publish_document_indexes(future)?;
            migration.publish_ready()?;
            Ok(metadata)
        })();
        self.fail_closed_on_corruption(result)
    }
}
