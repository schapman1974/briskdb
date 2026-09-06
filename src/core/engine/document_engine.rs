//! Protocol-neutral document command execution through engine-owned resources.

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, time::Instant};

use tokio::task::JoinHandle;

use super::{Engine, Operation, flatten_join, pending_cancellation_reason, retire_if_broken};
use crate::{
    core::{
        CancelOnDrop, CancellationToken, EngineError, EngineErrorKind, EngineResult,
        OperationControl, ResultLimits, Session, SessionInner, SessionState, wait_for_cancellation,
        wait_pending,
    },
    document::{
        BSON_MAX_DECODED_BYTES, BsonDocument, BsonErrorContext, BsonValue, DocumentCollectionId,
        DocumentCollectionMetadata, DocumentCollectionOptions, DocumentCommand,
        DocumentDeleteResult, DocumentExecution, DocumentFilter, DocumentIndexMetadata,
        DocumentInsertResult, DocumentMutationScope, DocumentNamespace, DocumentPlan,
        DocumentPointPlan, DocumentReadOptions, DocumentRequest, DocumentResult,
        DocumentScatterPlan, DocumentWriteOptions, MAX_DOCUMENT_REQUEST_BYTES, encode_document,
    },
    storage::{
        ConnectionOwner, DocumentStorageRecord, MAX_DOCUMENT_SHARD_SCAN_RECORDS, PooledConnection,
        PreparedDocumentWrite, SchemaOperationGuard, Storage,
    },
};

const DOCUMENT_RESULT_ENVELOPE_BYTES: u64 = 16;
const DOCUMENT_RESULT_ROW_BYTES: u64 = 8;
const DOCUMENT_RESULT_VALUE_BYTES: u64 = 9;
const DOCUMENT_MERGE_PAGE_SIZE: usize = 1;
const _: () = assert!(DOCUMENT_MERGE_PAGE_SIZE <= MAX_DOCUMENT_SHARD_SCAN_RECORDS);

impl Engine {
    /// Execute one owned document command through the same lifecycle, session,
    /// cancellation, deadline, worker, connection-pool, and result-limit
    /// boundaries used by SQL protocols.
    pub async fn execute_document(
        &self,
        session: &Session,
        request: DocumentRequest,
    ) -> EngineResult<DocumentExecution> {
        let (request_id, context, command) = request.into_parts();
        let mut operation = self.operation(context)?;
        if session.owner != self.inner.id {
            return operation.finish(Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "the session belongs to a different engine",
            )));
        }
        let result = match command {
            DocumentCommand::CreateCollection(request) => {
                let (namespace, options, write_options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(write_options) {
                    Err(error)
                } else {
                    self.run_document_create_collection(
                        &mut operation,
                        session,
                        request_id,
                        namespace,
                        options,
                    )
                    .await
                }
            }
            command => {
                let schema_operation = match self.inner.database.storage.enter_schema_operation() {
                    Ok(guard) => guard,
                    Err(error) => return operation.finish(Err(error)),
                };
                let session_guard = match operation.wait_pending(self.ready_session(session)).await
                {
                    Ok(guard) => guard,
                    Err(error) => return operation.finish(Err(error)),
                };
                if let Err(error) = require_document_session_ready(&session_guard) {
                    return operation.finish(Err(error));
                }
                let owner = ConnectionOwner::new(session.id().get());
                self.run_document_command(
                    &mut operation,
                    owner,
                    session_guard,
                    schema_operation,
                    request_id,
                    command,
                )
                .await
            }
        };
        if operation.lease.is_some() {
            operation.finish(result)
        } else {
            operation.finish_started(result)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_document_command(
        &self,
        operation: &mut Operation,
        owner: ConnectionOwner,
        session: tokio::sync::OwnedMutexGuard<SessionInner>,
        schema_operation: SchemaOperationGuard,
        request_id: crate::document::DocumentRequestId,
        command: DocumentCommand,
    ) -> EngineResult<DocumentExecution> {
        operation.check_before_start()?;
        let cancellation = CancellationToken::new();
        let cancel_operation = cancellation.clone();
        if let Err(reason) = operation.control.arm(Arc::new(move || {
            cancel_operation.cancel();
        })) {
            return operation.control.complete(Err(reason.error()));
        }

        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let worker_control = Arc::clone(&control);
        let engine = self.clone();
        let deadline = operation.deadline;
        let result_limits = operation.result_limits;
        let join = tokio::spawn(async move {
            let _lease = lease;
            let _schema_operation = schema_operation;
            let _session = session;
            let result = engine
                .coordinate_document_command(
                    owner,
                    request_id,
                    command,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await;
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn coordinate_document_command(
        &self,
        owner: ConnectionOwner,
        request_id: crate::document::DocumentRequestId,
        command: DocumentCommand,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        result_limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let storage = self.inner.database.storage.clone();
        let result_cancellation = cancellation.clone();
        let execution = match command {
            DocumentCommand::ListCollections(request) => {
                let (database, options) = request.into_parts();
                require_catalog_read_options(&options)?;
                let collections = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collections = storage
                                .document_collections_for_database_controlled(
                                    &database,
                                    Arc::clone(&control),
                                )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            let collections = apply_slice_options(&collections, &options)?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok(collections)
                        },
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::Collections(collections.into_boxed_slice()),
                ))
            }
            DocumentCommand::CreateIndex(request) => {
                let (namespace, index, write_options) = request.into_parts();
                require_catalog_write_options(write_options)?;
                if index.sparse() || index.partial_filter().is_some() {
                    return Err(unsupported(
                        "sparse and partial document indexes require the document-index execution milestone",
                    ));
                }
                let name = index.name().ok_or_else(|| {
                    unsupported("unnamed document indexes require deterministic name generation")
                })?;
                let name = name.to_owned();
                enforce_execution_result_limits(
                    &DocumentExecution::new(
                        request_id,
                        None,
                        DocumentResult::IndexName(name.clone()),
                    ),
                    result_limits,
                )?;
                let (keys, _, unique, _, _) = index.into_parts();
                let catalog_storage = storage.clone();
                let collection_id = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collection = catalog_storage.document_collection_controlled(
                                namespace.database(),
                                namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = require_collection(collection)?.id();
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok(collection_id)
                        },
                    )
                    .await?;
                let metadata_storage = storage.clone();
                let metadata_name = name.clone();
                self.run_document_storage_task(
                    cancellation,
                    deadline,
                    move |_cancellation, control| {
                        metadata_storage.declare_document_index_controlled(
                            collection_id,
                            &metadata_name,
                            &keys,
                            unique,
                            control,
                        )
                    },
                )
                .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::IndexName(name),
                ))
            }
            DocumentCommand::ListIndexes(request) => {
                let (namespace, options) = request.into_parts();
                require_catalog_read_options(&options)?;
                let indexes = self
                    .run_document_storage_task(
                        cancellation,
                        deadline,
                        move |cancellation, control| {
                            let collection = storage.document_collection_controlled(
                                namespace.database(),
                                namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection = require_collection(collection)?;
                            let indexes = apply_slice_options(collection.indexes(), &options)?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok(indexes)
                        },
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::Indexes(indexes.into_boxed_slice()),
                ))
            }
            DocumentCommand::Insert(request) => {
                let (namespace, documents, options) = request.into_parts();
                require_insert_options(options)?;
                if documents.len() != 1 {
                    return Err(unsupported(
                        "multi-document inserts require the bulk-write result semantics milestone",
                    ));
                }
                let catalog_storage = storage.clone();
                let collection_id = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collection = catalog_storage.document_collection_controlled(
                                namespace.database(),
                                namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = require_collection(collection)?.id();
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok(collection_id)
                        },
                    )
                    .await?;
                let prepare_storage = storage.clone();
                let (prepared, ids, plan) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let (prepared, ids) = prepare_documents(
                                &prepare_storage,
                                &documents,
                                cancellation,
                                &control,
                            )?;
                            enforce_prepared_write_budget(&prepared, cancellation, &control)?;
                            let plan =
                                insert_plan(collection_id, &prepared, cancellation, &control)?;
                            enforce_insert_execution_limits(
                                &plan,
                                &ids,
                                result_limits,
                                cancellation,
                                &control,
                            )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok((prepared, ids, plan))
                        },
                    )
                    .await?;
                let count = u64::try_from(prepared.len()).map_err(|_| {
                    EngineError::new(
                        EngineErrorKind::LimitExceeded,
                        "document insert count exceeds the supported range",
                    )
                })?;
                let reserve_storage = storage.clone();
                let first_order = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |_cancellation, control| {
                            reserve_storage.reserve_document_natural_orders_controlled(
                                collection_id,
                                count,
                                control,
                            )
                        },
                    )
                    .await?;
                for (offset, prepared_document) in prepared.into_iter().enumerate() {
                    let natural_order = first_order
                        .checked_add(u64::try_from(offset).expect("bounded insert count fits u64"))
                        .ok_or_else(|| {
                            EngineError::new(
                                EngineErrorKind::LimitExceeded,
                                "document natural-order identity overflowed",
                            )
                        })?;
                    let shard = prepared_document.shard();
                    let write = prepared_document;
                    self.run_document_shard(
                        shard,
                        owner,
                        cancellation.clone(),
                        deadline,
                        move |storage, connection, cancellation| {
                            storage.insert_prepared_document_on_connection(
                                connection,
                                collection_id,
                                natural_order,
                                shard,
                                &write,
                                cancellation,
                            )
                        },
                    )
                    .await?;
                }
                Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    DocumentResult::Insert(DocumentInsertResult::from_validated(ids)),
                ))
            }
            DocumentCommand::Find(request) => {
                let (namespace, filter, options) = request.into_parts();
                require_find_options(&options)?;
                let catalog_storage = storage.clone();
                let catalog_namespace = namespace.clone();
                let (collection_id, route) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collection = catalog_storage.document_collection_controlled(
                                catalog_namespace.database(),
                                catalog_namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = require_collection(collection)?.id();
                            let route = prepare_filter_route(
                                &catalog_storage,
                                filter,
                                cancellation,
                                &control,
                            )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok((collection_id, route))
                        },
                    )
                    .await?;
                let (plan, documents) = match route {
                    PreparedFilterRoute::Point { id_key, shard } => {
                        let (id_key, record) = self
                            .run_document_shard(
                                shard,
                                owner,
                                cancellation,
                                deadline,
                                move |storage, connection, cancellation| {
                                    let record = storage.get_document_on_connection(
                                        connection,
                                        collection_id,
                                        shard,
                                        &id_key,
                                        cancellation,
                                    )?;
                                    Ok((id_key, record))
                                },
                            )
                            .await?;
                        if let Some(record) = &record {
                            validate_point_record(record, collection_id, shard, &id_key)?;
                        }
                        let documents = apply_point_read(record, &options, result_limits)?;
                        (
                            DocumentPlan::Point(DocumentPointPlan::new(
                                collection_id,
                                shard,
                                id_key,
                            )?),
                            documents,
                        )
                    }
                    PreparedFilterRoute::Scatter => {
                        let documents = self
                            .scan_document_collection(
                                owner,
                                collection_id,
                                cancellation,
                                deadline,
                                &options,
                                result_limits,
                            )
                            .await?;
                        (scatter_plan(collection_id, self.shard_count())?, documents)
                    }
                };
                let batch = crate::document::DocumentCursorBatch::from_validated(
                    namespace, None, documents,
                );
                Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    DocumentResult::Cursor(batch),
                ))
            }
            DocumentCommand::Count(request) => {
                let (namespace, filter, options) = request.into_parts();
                require_count_options(&options)?;
                let catalog_storage = storage.clone();
                let (collection_id, route) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collection = catalog_storage.document_collection_controlled(
                                namespace.database(),
                                namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = require_collection(collection)?.id();
                            let route = prepare_filter_route(
                                &catalog_storage,
                                filter,
                                cancellation,
                                &control,
                            )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok((collection_id, route))
                        },
                    )
                    .await?;
                let (plan, count) = match route {
                    PreparedFilterRoute::Point { id_key, shard } => {
                        let (id_key, found) = self
                            .run_document_shard(
                                shard,
                                owner,
                                cancellation,
                                deadline,
                                move |storage, connection, cancellation| {
                                    let found = storage.get_document_on_connection(
                                        connection,
                                        collection_id,
                                        shard,
                                        &id_key,
                                        cancellation,
                                    )?;
                                    Ok((id_key, found))
                                },
                            )
                            .await?;
                        if let Some(record) = &found {
                            validate_point_record(record, collection_id, shard, &id_key)?;
                        }
                        let found = found.is_some();
                        (
                            DocumentPlan::Point(DocumentPointPlan::new(
                                collection_id,
                                shard,
                                id_key,
                            )?),
                            u64::from(found),
                        )
                    }
                    PreparedFilterRoute::Scatter => {
                        let mut count = 0_u64;
                        for shard in 0..self.shard_count() {
                            let shard_count = self
                                .run_document_shard(
                                    shard,
                                    owner,
                                    cancellation.clone(),
                                    deadline,
                                    move |storage, connection, cancellation| {
                                        storage.count_document_shard_on_connection(
                                            connection,
                                            collection_id,
                                            shard,
                                            cancellation,
                                        )
                                    },
                                )
                                .await?;
                            count = count.checked_add(shard_count).ok_or_else(|| {
                                EngineError::new(
                                    EngineErrorKind::LimitExceeded,
                                    "document count exceeded the supported range",
                                )
                            })?;
                        }
                        (scatter_plan(collection_id, self.shard_count())?, count)
                    }
                };
                let count = apply_count_options(count, &options);
                enforce_scalar_result_limit(result_limits)?;
                Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    DocumentResult::Count(count),
                ))
            }
            DocumentCommand::Delete(request) => {
                let (namespace, filter, scope, options) = request.into_parts();
                require_delete_options(options)?;
                if scope != DocumentMutationScope::One {
                    return Err(unsupported(
                        "multi-document deletes require the matcher semantics milestone",
                    ));
                }
                let catalog_storage = storage.clone();
                let (collection_id, id_key, shard, plan) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let collection = catalog_storage.document_collection_controlled(
                                namespace.database(),
                                namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = require_collection(collection)?.id();
                            let id = require_point_filter(filter, cancellation, &control)?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            let (id_key, shard) = catalog_storage.prepare_document_id(&id)?;
                            let plan = DocumentPlan::Point(DocumentPointPlan::new(
                                collection_id,
                                shard,
                                id_key.clone(),
                            )?);
                            enforce_execution_result_limits(
                                &DocumentExecution::new(
                                    request_id,
                                    Some(plan.clone()),
                                    DocumentResult::Delete(DocumentDeleteResult::new(0)),
                                ),
                                result_limits,
                            )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok((collection_id, id_key, shard, plan))
                        },
                    )
                    .await?;
                let delete_key = id_key;
                let deleted = self
                    .run_document_shard(
                        shard,
                        owner,
                        cancellation,
                        deadline,
                        move |storage, connection, cancellation| {
                            storage.delete_document_on_connection(
                                connection,
                                collection_id,
                                shard,
                                &delete_key,
                                cancellation,
                            )
                        },
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    DocumentResult::Delete(DocumentDeleteResult::new(u64::from(deleted))),
                ))
            }
            DocumentCommand::DropCollection(_)
            | DocumentCommand::Aggregate(_)
            | DocumentCommand::Distinct(_)
            | DocumentCommand::Update(_)
            | DocumentCommand::Replace(_)
            | DocumentCommand::DropIndex(_)
            | DocumentCommand::ContinueCursor(_)
            | DocumentCommand::KillCursor(_) => Err(unsupported(
                "this document command is modeled but requires a later document-semantics milestone",
            )),
            DocumentCommand::CreateCollection(_) => Err(EngineError::new(
                EngineErrorKind::Internal,
                "document collection creation reached the data-command coordinator",
            )),
        }?;
        if execution_result_is_mutation(execution.result()) {
            // Every supported mutation preflights this exact result shape
            // before its first durable write. Once that write commits, return
            // success even if cancellation wins the race to result delivery.
            Ok(execution)
        } else {
            self.run_document_storage_task(
                result_cancellation,
                deadline,
                move |cancellation, control| {
                    enforce_execution_result_limits_controlled(
                        &execution,
                        result_limits,
                        cancellation,
                        &control,
                    )?;
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok(execution)
                },
            )
            .await
        }
    }

    async fn run_document_create_collection(
        &self,
        operation: &mut Operation,
        session: &Session,
        request_id: crate::document::DocumentRequestId,
        namespace: DocumentNamespace,
        options: DocumentCollectionOptions,
    ) -> EngineResult<DocumentExecution> {
        let migration = self.inner.database.storage.begin_schema_migration()?;
        // Close the same-session transaction race before waiting for existing
        // schema operations to drain. Once migration admission succeeds, no
        // new transaction can retain a schema-operation guard behind this
        // session check.
        let session_preflight = operation.wait_pending(self.ready_session(session)).await?;
        require_document_session_ready(&session_preflight)?;
        drop(session_preflight);
        operation
            .wait_pending(async {
                migration.wait_for_quiescence().await;
                Ok(())
            })
            .await?;
        let session = operation.wait_pending(self.ready_session(session)).await?;
        require_document_session_ready(&session)?;
        let worker = operation.wait_pending(self.inner.workers.acquire()).await?;
        operation.check_before_start()?;
        let lease = operation.take_lease();
        let worker_control = Arc::clone(&operation.control);
        let result_limits = operation.result_limits;
        let storage = self.inner.database.storage.clone();
        let storage_for_corruption = storage.clone();
        let connections = self.inner.connections.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session;
            let result =
                enforce_create_collection_result_limits(&namespace, &options, result_limits)
                    .and_then(|_| connections.retire_idle_for_schema_migration())
                    .and_then(|_| {
                        storage.create_document_collection_controlled(
                            namespace.database(),
                            namespace.collection(),
                            &options,
                            migration,
                            Arc::clone(&worker_control),
                        )
                    })
                    .map(|metadata| {
                        DocumentExecution::new(
                            request_id,
                            None,
                            DocumentResult::Collection(metadata),
                        )
                    })
                    .and_then(|execution| {
                        enforce_execution_result_limits(&execution, result_limits)?;
                        Ok(execution)
                    });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage_for_corruption.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
    }

    async fn run_document_storage_task<T, F>(
        &self,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&CancellationToken, Arc<OperationControl>) -> EngineResult<T> + Send + 'static,
    {
        let control = OperationControl::new(deadline);
        let mut cancel_on_drop = CancelOnDrop::new(Arc::clone(&control));
        let shutdown = self.inner.shutdown_cancel.clone();
        let worker = wait_pending(
            self.inner.workers.acquire(),
            &cancellation,
            &shutdown,
            deadline,
            &control,
        )
        .await?;
        if let Some(reason) = pending_cancellation_reason(&cancellation, &shutdown, deadline) {
            control.request_cancel(reason);
            let result = control.complete(Err(reason.error()));
            cancel_on_drop.disarm();
            return result;
        }
        let worker_control = Arc::clone(&control);
        let storage = self.inner.database.storage.clone();
        let task_cancellation = cancellation.clone();
        let mut join: JoinHandle<EngineResult<T>> = worker.spawn(move || {
            let result = work(&task_cancellation, Arc::clone(&worker_control));
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        let result = tokio::select! {
            biased;
            result = &mut join => flatten_join(result),
            reason = wait_for_cancellation(&cancellation, &shutdown, deadline) => {
                control.request_cancel(reason);
                flatten_join(join.await)
            }
        };
        let result = control.complete(result);
        cancel_on_drop.disarm();
        result
    }

    async fn run_document_shard<T, F>(
        &self,
        shard: u16,
        owner: ConnectionOwner,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Storage, &mut PooledConnection, &CancellationToken) -> EngineResult<T>
            + Send
            + 'static,
    {
        let control = OperationControl::new(deadline);
        let mut cancel_on_drop = CancelOnDrop::new(Arc::clone(&control));
        let shutdown = self.inner.shutdown_cancel.clone();
        let permit = wait_pending(
            self.inner.connections.acquire_for_owner(shard, owner),
            &cancellation,
            &shutdown,
            deadline,
            &control,
        )
        .await?;
        let worker = wait_pending(
            self.inner.workers.acquire(),
            &cancellation,
            &shutdown,
            deadline,
            &control,
        )
        .await?;
        if let Some(reason) = pending_cancellation_reason(&cancellation, &shutdown, deadline) {
            control.request_cancel(reason);
            let result = control.complete(Err(reason.error()));
            cancel_on_drop.disarm();
            return result;
        }
        let worker_control = Arc::clone(&control);
        let storage = self.inner.database.storage.clone();
        let error_storage = storage.clone();
        let task_cancellation = cancellation.clone();
        let mut join = worker.spawn(move || {
            let result = permit
                .checkout_controlled(Arc::clone(&worker_control))
                .and_then(|mut connection| {
                    let result = connection
                        .run_document_controlled(Arc::clone(&worker_control), |connection| {
                            work(&storage, connection, &task_cancellation)
                        });
                    retire_if_broken(&mut connection, &result);
                    result
                });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                error_storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        let result = tokio::select! {
            biased;
            result = &mut join => flatten_join(result),
            reason = wait_for_cancellation(&cancellation, &shutdown, deadline) => {
                control.request_cancel(reason);
                flatten_join(join.await)
            }
        };
        let result = control.complete(result);
        cancel_on_drop.disarm();
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_document_collection(
        &self,
        owner: ConnectionOwner,
        collection_id: DocumentCollectionId,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<Vec<BsonDocument>> {
        enforce_empty_result_limit(limits)?;
        let frontier_limit = limits
            .max_bytes()
            .max(u64::try_from(BSON_MAX_DECODED_BYTES).unwrap_or(u64::MAX));
        let mut frontiers: Vec<Option<DocumentStorageRecord>> =
            Vec::with_capacity(usize::from(self.shard_count()));
        let mut retained_bytes = 0_u64;
        for shard in 0..self.shard_count() {
            let page = self
                .run_document_shard(
                    shard,
                    owner,
                    cancellation.clone(),
                    deadline,
                    move |storage, connection, cancellation| {
                        storage.scan_document_shard_on_connection(
                            connection,
                            collection_id,
                            shard,
                            None,
                            DOCUMENT_MERGE_PAGE_SIZE,
                            cancellation,
                        )
                    },
                )
                .await?;
            let record = page.into_iter().next();
            if let Some(record) = &record {
                validate_point_record(record, collection_id, shard, record.id_key())?;
                retained_bytes = retained_bytes
                    .checked_add(u64::try_from(record.encoded_len()).unwrap_or(u64::MAX))
                    .ok_or_else(result_size_overflow)?;
                if retained_bytes > frontier_limit {
                    return Err(limit_exceeded(
                        "document scatter merge frontier exceeds its bounded memory limit",
                    ));
                }
            }
            frontiers.push(record);
        }

        let mut heap = BinaryHeap::new();
        for (shard, record) in frontiers.iter().enumerate() {
            if let Some(record) = record {
                heap.push(Reverse((record.natural_order(), shard)));
            }
        }

        let mut skipped = 0_u64;
        let mut documents = Vec::new();
        let mut result_bytes = DOCUMENT_RESULT_ENVELOPE_BYTES;
        let requested = options
            .limit()
            .unwrap_or(u64::MAX)
            .min(options.batch_size());
        let retained_documents = requested.min(limits.max_rows());
        documents
            .try_reserve_exact(usize::try_from(retained_documents).unwrap_or(usize::MAX))
            .map_err(|error| {
                EngineError::from_source(
                    EngineErrorKind::OutOfMemory,
                    "unable to reserve bounded document result storage",
                    error,
                )
            })?;
        let mut has_more = false;

        while let Some(Reverse((_natural_order, shard_index))) = heap.pop() {
            let record = frontiers[shard_index]
                .take()
                .expect("the document frontier heap references a record");
            let natural_order = record.natural_order();
            retained_bytes = retained_bytes
                .saturating_sub(u64::try_from(record.encoded_len()).unwrap_or(u64::MAX));

            if skipped < options.skip() {
                skipped += 1;
            } else if u64::try_from(documents.len()).unwrap_or(u64::MAX) < requested {
                add_document_result_budget(&mut result_bytes, record.encoded_len(), limits)?;
                if u64::try_from(documents.len()).unwrap_or(u64::MAX) >= limits.max_rows() {
                    return Err(limit_exceeded(
                        "document result exceeds the request row limit",
                    ));
                }
                documents.push(record.into_document());
            } else {
                has_more = true;
                break;
            }

            let shard = u16::try_from(shard_index).expect("document shard index fits u16");
            let after = Some(natural_order);
            let page = self
                .run_document_shard(
                    shard,
                    owner,
                    cancellation.clone(),
                    deadline,
                    move |storage, connection, cancellation| {
                        storage.scan_document_shard_on_connection(
                            connection,
                            collection_id,
                            shard,
                            after,
                            DOCUMENT_MERGE_PAGE_SIZE,
                            cancellation,
                        )
                    },
                )
                .await?;
            let next = page.into_iter().next();
            if let Some(next) = &next {
                validate_point_record(next, collection_id, shard, next.id_key())?;
                retained_bytes = retained_bytes
                    .checked_add(u64::try_from(next.encoded_len()).unwrap_or(u64::MAX))
                    .ok_or_else(result_size_overflow)?;
                if retained_bytes > frontier_limit {
                    return Err(limit_exceeded(
                        "document scatter merge frontier exceeds its bounded memory limit",
                    ));
                }
                heap.push(Reverse((next.natural_order(), shard_index)));
            }
            frontiers[shard_index] = next;
        }

        if has_more
            && options
                .limit()
                .is_none_or(|limit| limit > options.batch_size())
        {
            return Err(unsupported(
                "document cursor continuation is not available yet; use a limit within one batch",
            ));
        }
        Ok(documents)
    }
}

fn require_document_session_ready(session: &SessionInner) -> EngineResult<()> {
    match session.state() {
        SessionState::Ready => Ok(()),
        SessionState::InTransaction => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "document commands cannot run inside a SQL transaction",
        )),
        SessionState::FailedTransaction => Err(EngineError::new(
            EngineErrorKind::TransactionAborted,
            "document commands cannot run in a failed SQL transaction",
        )),
        SessionState::Closed => Err(EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "the session is closed",
        )),
    }
}

fn require_catalog_write_options(options: DocumentWriteOptions) -> EngineResult<()> {
    if !options.ordered() || options.upsert() || options.bypass_document_validation() {
        return Err(unsupported(
            "non-default document catalog write options are not supported",
        ));
    }
    Ok(())
}

fn require_insert_options(options: DocumentWriteOptions) -> EngineResult<()> {
    if !options.ordered() {
        return Err(unsupported(
            "unordered document inserts require the bulk-write semantics milestone",
        ));
    }
    if options.upsert() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "upsert is not an insert option",
        ));
    }
    if options.bypass_document_validation() {
        return Err(unsupported(
            "bypassing document validation requires the validation semantics milestone",
        ));
    }
    Ok(())
}

fn require_delete_options(options: DocumentWriteOptions) -> EngineResult<()> {
    if !options.ordered() || options.upsert() || options.bypass_document_validation() {
        return Err(unsupported(
            "non-default document delete options are not supported",
        ));
    }
    Ok(())
}

fn require_find_options(options: &DocumentReadOptions) -> EngineResult<()> {
    if options.projection().is_some() {
        return Err(unsupported(
            "document projections require the projection semantics milestone",
        ));
    }
    if options.sort().is_some() {
        return Err(unsupported(
            "document sort expressions require the query semantics milestone",
        ));
    }
    Ok(())
}

fn require_count_options(options: &DocumentReadOptions) -> EngineResult<()> {
    require_find_options(options)
}

fn require_catalog_read_options(options: &DocumentReadOptions) -> EngineResult<()> {
    require_find_options(options)
}

fn require_collection(
    collection: Option<DocumentCollectionMetadata>,
) -> EngineResult<DocumentCollectionMetadata> {
    collection.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::InvalidArgument,
            "document collection does not exist",
        )
    })
}

enum FilterRoute {
    Point(BsonValue),
    Scatter,
}

enum PreparedFilterRoute {
    Point {
        id_key: crate::document::CanonicalBsonKey,
        shard: u16,
    },
    Scatter,
}

fn classify_filter(
    filter: DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<FilterRoute> {
    ensure_document_cpu_active(cancellation, control)?;
    if filter.is_empty() {
        return Ok(FilterRoute::Scatter);
    }
    let mut entries = filter.into_document().into_entries().into_iter();
    match (entries.next(), entries.next()) {
        (Some((name, value)), None)
            if name == "_id" && is_literal_id_filter(&value, cancellation, control)? =>
        {
            ensure_document_cpu_active(cancellation, control)?;
            Ok(FilterRoute::Point(value))
        }
        _ => Err(unsupported(
            "document matcher expressions require the query semantics milestone",
        )),
    }
}

fn is_literal_id_filter(
    value: &BsonValue,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<bool> {
    match value {
        BsonValue::RegularExpression(_) => Ok(false),
        BsonValue::Document(document) => {
            for (name, _) in document.iter() {
                ensure_document_cpu_active(cancellation, control)?;
                if name.starts_with('$') {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(true),
    }
}

fn prepare_filter_route(
    storage: &Storage,
    filter: DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<PreparedFilterRoute> {
    match classify_filter(filter, cancellation, control)? {
        FilterRoute::Point(id) => {
            let (id_key, shard) = storage.prepare_document_id(&id)?;
            ensure_document_cpu_active(cancellation, control)?;
            Ok(PreparedFilterRoute::Point { id_key, shard })
        }
        FilterRoute::Scatter => Ok(PreparedFilterRoute::Scatter),
    }
}

fn require_point_filter(
    filter: DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<BsonValue> {
    match classify_filter(filter, cancellation, control)? {
        FilterRoute::Point(id) => Ok(id),
        FilterRoute::Scatter => Err(unsupported("this mutation requires an exact _id filter")),
    }
}

fn validate_point_record(
    record: &DocumentStorageRecord,
    collection_id: DocumentCollectionId,
    shard: u16,
    id_key: &crate::document::CanonicalBsonKey,
) -> EngineResult<()> {
    if record.collection_id() != collection_id
        || record.shard() != shard
        || record.id_key() != id_key
    {
        return Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "document point read returned inconsistent storage identity",
        ));
    }
    // The storage decoder already validates the checksum, BSON payload,
    // canonical `_id`, and shard route while running on the blocking worker.
    // Repeating canonical BSON work here would move attacker-sized CPU work
    // onto the async coordinator.
    Ok(())
}

fn scatter_plan(
    collection_id: DocumentCollectionId,
    shard_count: u16,
) -> EngineResult<DocumentPlan> {
    DocumentScatterPlan::new(collection_id, (0..shard_count).collect::<Vec<_>>())
        .map(DocumentPlan::Scatter)
}

fn insert_plan(
    collection_id: DocumentCollectionId,
    prepared: &[PreparedDocumentWrite],
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentPlan> {
    ensure_document_cpu_active(cancellation, control)?;
    if prepared.len() == 1 {
        let plan = DocumentPointPlan::new(
            collection_id,
            prepared[0].shard(),
            prepared[0].id_key().clone(),
        )
        .map(DocumentPlan::Point)?;
        ensure_document_cpu_active(cancellation, control)?;
        return Ok(plan);
    }
    let mut shards = prepared
        .iter()
        .map(PreparedDocumentWrite::shard)
        .collect::<Vec<_>>();
    ensure_document_cpu_active(cancellation, control)?;
    shards.sort_unstable();
    shards.dedup();
    let plan = DocumentScatterPlan::new(collection_id, shards).map(DocumentPlan::Scatter)?;
    ensure_document_cpu_active(cancellation, control)?;
    Ok(plan)
}

fn prepare_documents(
    storage: &Storage,
    documents: &[BsonDocument],
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<(Vec<PreparedDocumentWrite>, Vec<BsonValue>)> {
    ensure_document_cpu_active(cancellation, control)?;
    let mut prepared = Vec::new();
    let mut ids = Vec::new();
    prepared
        .try_reserve_exact(documents.len())
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::OutOfMemory,
                "unable to reserve prepared document write storage",
                error,
            )
        })?;
    ids.try_reserve_exact(documents.len()).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::OutOfMemory,
            "unable to reserve document insert identity storage",
            error,
        )
    })?;
    for document in documents {
        ensure_document_cpu_active(cancellation, control)?;
        let id = document
            .get_unique("_id")
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document inserts require an explicit _id",
                )
            })?;
        let write = storage.prepare_document_write(document)?;
        ensure_document_cpu_active(cancellation, control)?;
        prepared.push(write);
        ids.push(id.clone());
    }
    ensure_document_cpu_active(cancellation, control)?;
    Ok((prepared, ids))
}

fn apply_point_read(
    record: Option<DocumentStorageRecord>,
    options: &DocumentReadOptions,
    limits: ResultLimits,
) -> EngineResult<Vec<BsonDocument>> {
    let Some(record) = record else {
        enforce_empty_result_limit(limits)?;
        return Ok(Vec::new());
    };
    if options.skip() > 0 {
        enforce_empty_result_limit(limits)?;
        return Ok(Vec::new());
    }
    let mut bytes = DOCUMENT_RESULT_ENVELOPE_BYTES;
    add_document_result_budget(&mut bytes, record.encoded_len(), limits)?;
    if limits.max_rows() < 1 {
        return Err(limit_exceeded(
            "document result exceeds the request row limit",
        ));
    }
    Ok(vec![record.into_document()])
}

fn apply_count_options(count: u64, options: &DocumentReadOptions) -> u64 {
    count
        .saturating_sub(options.skip())
        .min(options.limit().unwrap_or(u64::MAX))
}

fn apply_slice_options<T: Clone>(
    values: &[T],
    options: &DocumentReadOptions,
) -> EngineResult<Vec<T>> {
    let skip = usize::try_from(options.skip()).unwrap_or(usize::MAX);
    let take = usize::try_from(options.limit().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
    let batch = usize::try_from(options.batch_size()).expect("bounded batch size fits usize");
    if values.len().saturating_sub(skip) > batch && take > batch {
        return Err(unsupported(
            "document cursor continuation is not available yet; use a limit within one batch",
        ));
    }
    Ok(values
        .iter()
        .skip(skip)
        .take(take.min(batch))
        .cloned()
        .collect())
}

struct DocumentResultBudget {
    rows: u64,
    bytes: u64,
    limits: ResultLimits,
}

impl DocumentResultBudget {
    fn new(limits: ResultLimits) -> Self {
        Self {
            rows: 0,
            bytes: DOCUMENT_RESULT_ENVELOPE_BYTES,
            limits,
        }
    }

    fn add_rows(&mut self, rows: usize) -> EngineResult<()> {
        self.rows = self
            .rows
            .checked_add(u64::try_from(rows).unwrap_or(u64::MAX))
            .ok_or_else(result_size_overflow)?;
        if self.rows > self.limits.max_rows() {
            return Err(limit_exceeded(
                "document result exceeds the request row limit",
            ));
        }
        Ok(())
    }

    fn add_bytes(&mut self, bytes: u64) -> EngineResult<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(result_size_overflow)?;
        if self.bytes > self.limits.max_bytes() {
            return Err(limit_exceeded(
                "document result exceeds the request byte limit",
            ));
        }
        Ok(())
    }

    fn add_document(
        &mut self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let encoded = encode_document(document)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        check()?;
        self.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES)?;
        self.add_bytes(u64::try_from(encoded.len()).unwrap_or(u64::MAX))
    }

    fn add_value(
        &mut self,
        value: &BsonValue,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let wrapper = BsonDocument::from_entries([("value", value.clone())])
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        check()?;
        self.add_document(&wrapper, check)
    }

    fn add_index(
        &mut self,
        index: &DocumentIndexMetadata,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let specification = encode_document(index.specification())
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        check()?;
        self.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + 3)?;
        self.add_bytes(u64::try_from(index.name().len()).unwrap_or(u64::MAX))?;
        self.add_bytes(u64::try_from(specification.len()).unwrap_or(u64::MAX))
    }

    fn add_collection(
        &mut self,
        collection: &DocumentCollectionMetadata,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        let options = encode_document(collection.options().document())
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
        check()?;
        self.add_bytes(DOCUMENT_RESULT_ROW_BYTES + 24)?;
        self.add_bytes(u64::try_from(collection.namespace().len()).unwrap_or(u64::MAX))?;
        self.add_bytes(u64::try_from(options.len()).unwrap_or(u64::MAX))?;
        for index in collection.indexes() {
            self.add_index(index, check)?;
        }
        Ok(())
    }

    fn add_plan(&mut self, plan: &DocumentPlan) -> EngineResult<()> {
        match plan {
            DocumentPlan::Point(plan) => {
                self.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + 10)?;
                self.add_bytes(u64::try_from(plan.id_key().as_bytes().len()).unwrap_or(u64::MAX))
            }
            DocumentPlan::Scatter(plan) => {
                self.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + 8)?;
                self.add_bytes(
                    u64::try_from(plan.shards().len())
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2),
                )
            }
        }
    }

    fn finish(self) -> EngineResult<()> {
        if self.bytes > self.limits.max_bytes() {
            Err(limit_exceeded(
                "document result exceeds the request byte limit",
            ))
        } else {
            Ok(())
        }
    }
}

fn enforce_execution_result_limits(
    execution: &DocumentExecution,
    limits: ResultLimits,
) -> EngineResult<()> {
    enforce_execution_result_limits_with_check(execution, limits, &mut || Ok(()))
}

fn enforce_execution_result_limits_controlled(
    execution: &DocumentExecution,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<()> {
    enforce_execution_result_limits_with_check(execution, limits, &mut || {
        ensure_document_cpu_active(cancellation, control)
    })
}

fn enforce_execution_result_limits_with_check(
    execution: &DocumentExecution,
    limits: ResultLimits,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    let mut budget = DocumentResultBudget::new(limits);
    if let Some(plan) = execution.plan() {
        budget.add_plan(plan)?;
        check()?;
    }
    match execution.result() {
        DocumentResult::Acknowledged(_) | DocumentResult::CursorKilled(_) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 1)?;
        }
        DocumentResult::Collection(collection) => {
            budget.add_rows(1)?;
            budget.add_collection(collection, check)?;
        }
        DocumentResult::Collections(collections) => {
            budget.add_rows(collections.len())?;
            for collection in collections {
                budget.add_collection(collection, check)?;
            }
        }
        DocumentResult::Document(document) => {
            if let Some(document) = document {
                budget.add_rows(1)?;
                budget.add_document(document, check)?;
            }
        }
        DocumentResult::Cursor(batch) => {
            let namespace_bytes = batch
                .namespace()
                .database()
                .len()
                .saturating_add(1)
                .saturating_add(batch.namespace().collection().len());
            budget.add_bytes(u64::try_from(namespace_bytes).unwrap_or(u64::MAX) + 8)?;
            budget.add_rows(batch.documents().len())?;
            for document in batch.documents() {
                budget.add_document(document, check)?;
            }
        }
        DocumentResult::Count(_) | DocumentResult::Delete(_) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 8)?;
        }
        DocumentResult::Distinct(values) => {
            budget.add_rows(values.len())?;
            for value in values {
                budget.add_value(value, check)?;
            }
        }
        DocumentResult::Insert(result) => {
            budget.add_rows(result.inserted_ids().len())?;
            for id in result.inserted_ids() {
                budget.add_value(id, check)?;
            }
        }
        DocumentResult::Update(result) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 17)?;
            if let Some(id) = result.upserted_id() {
                budget.add_value(id, check)?;
            }
        }
        DocumentResult::IndexName(name) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES)?;
            budget.add_bytes(u64::try_from(name.len()).unwrap_or(u64::MAX))?;
        }
        DocumentResult::Indexes(indexes) => {
            budget.add_rows(indexes.len())?;
            for index in indexes {
                budget.add_index(index, check)?;
            }
        }
    }
    let result = budget.finish();
    check()?;
    result
}

fn enforce_create_collection_result_limits(
    namespace: &DocumentNamespace,
    options: &DocumentCollectionOptions,
    limits: ResultLimits,
) -> EngineResult<()> {
    let mut budget = DocumentResultBudget::new(limits);
    budget.add_rows(1)?;
    let options = encode_document(options.document())
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    let id_specification = BsonDocument::from_entries([("_id", BsonValue::Int32(1))])
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    let id_specification = encode_document(&id_specification)
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    let namespace_bytes = namespace
        .database()
        .len()
        .saturating_add(1)
        .saturating_add(namespace.collection().len());
    budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + 24)?;
    budget.add_bytes(u64::try_from(namespace_bytes).unwrap_or(u64::MAX))?;
    budget.add_bytes(u64::try_from(options.len()).unwrap_or(u64::MAX))?;
    budget.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + 3)?;
    budget.add_bytes(4)?;
    budget.add_bytes(u64::try_from(id_specification.len()).unwrap_or(u64::MAX))?;
    budget.finish()
}

fn enforce_prepared_write_budget(
    prepared: &[PreparedDocumentWrite],
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<()> {
    let mut bytes = 0_usize;
    for prepared in prepared {
        ensure_document_cpu_active(cancellation, control)?;
        bytes = bytes
            .checked_add(prepared.document_bson_len())
            .filter(|bytes| *bytes <= MAX_DOCUMENT_REQUEST_BYTES)
            .ok_or_else(|| {
                limit_exceeded("prepared document writes exceed the request memory limit")
            })?;
    }
    ensure_document_cpu_active(cancellation, control)?;
    Ok(())
}

fn enforce_insert_execution_limits(
    plan: &DocumentPlan,
    ids: &[BsonValue],
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<()> {
    let mut budget = DocumentResultBudget::new(limits);
    let mut check = || ensure_document_cpu_active(cancellation, control);
    ensure_document_cpu_active(cancellation, control)?;
    budget.add_plan(plan)?;
    budget.add_rows(ids.len())?;
    for id in ids {
        ensure_document_cpu_active(cancellation, control)?;
        budget.add_value(id, &mut check)?;
        ensure_document_cpu_active(cancellation, control)?;
    }
    let result = budget.finish();
    ensure_document_cpu_active(cancellation, control)?;
    result
}

fn add_document_result_budget(
    bytes: &mut u64,
    document_len: usize,
    limits: ResultLimits,
) -> EngineResult<()> {
    *bytes = bytes
        .checked_add(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES)
        .and_then(|value| value.checked_add(u64::try_from(document_len).ok()?))
        .ok_or_else(result_size_overflow)?;
    if *bytes > limits.max_bytes() {
        return Err(limit_exceeded(
            "document result exceeds the request byte limit",
        ));
    }
    Ok(())
}

fn enforce_empty_result_limit(limits: ResultLimits) -> EngineResult<()> {
    if DOCUMENT_RESULT_ENVELOPE_BYTES > limits.max_bytes() {
        Err(limit_exceeded(
            "document result exceeds the request byte limit",
        ))
    } else {
        Ok(())
    }
}

fn enforce_scalar_result_limit(limits: ResultLimits) -> EngineResult<()> {
    if DOCUMENT_RESULT_ENVELOPE_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 8 > limits.max_bytes() {
        Err(limit_exceeded(
            "document scalar result exceeds the request byte limit",
        ))
    } else {
        Ok(())
    }
}

fn execution_result_is_mutation(result: &DocumentResult) -> bool {
    matches!(
        result,
        DocumentResult::Acknowledged(_)
            | DocumentResult::Collection(_)
            | DocumentResult::Insert(_)
            | DocumentResult::Update(_)
            | DocumentResult::Delete(_)
            | DocumentResult::IndexName(_)
            | DocumentResult::CursorKilled(_)
    )
}

fn ensure_document_cpu_active(
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<()> {
    if let Some(reason) = control.reason() {
        return Err(reason.error());
    }
    if cancellation.is_cancelled() {
        return Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "the document request was cancelled during CPU-bound work",
        ));
    }
    Ok(())
}

fn limit_exceeded(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::LimitExceeded, message)
}

fn result_size_overflow() -> EngineError {
    limit_exceeded("document result byte accounting overflowed")
}

fn unsupported(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::{sync::oneshot, time::timeout};

    use super::*;
    use crate::{
        core::{EngineOptions, RequestContext},
        document::{DocumentCollectionOptions, DocumentCreateCollectionRequest, DocumentRequestId},
        storage::SchemaGateState,
    };

    #[tokio::test]
    async fn collection_creation_excludes_schema_operations_before_waiting_for_its_session() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, EngineOptions::new(1, 1).unwrap())
            .await
            .unwrap();
        let first_holder_session = Arc::new(engine.session());
        let second_holder_session = Arc::new(engine.session());
        let create_session = Arc::new(engine.session());
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (first_release_tx, first_release_rx) = std::sync::mpsc::channel();
        let first_engine = engine.clone();
        let first_session_for_task = Arc::clone(&first_holder_session);
        let first_holder = tokio::spawn(async move {
            first_engine
                .hold_session_for_test(
                    &first_session_for_task,
                    0,
                    first_started_tx,
                    first_release_rx,
                )
                .await
        });
        let (second_started_tx, second_started_rx) = oneshot::channel();
        let (second_release_tx, second_release_rx) = std::sync::mpsc::channel();
        let second_engine = engine.clone();
        let second_session_for_task = Arc::clone(&second_holder_session);
        let second_holder = tokio::spawn(async move {
            second_engine
                .hold_session_for_test(
                    &second_session_for_task,
                    1,
                    second_started_tx,
                    second_release_rx,
                )
                .await
        });
        timeout(Duration::from_secs(2), first_started_rx)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), second_started_rx)
            .await
            .unwrap()
            .unwrap();

        let create_engine = engine.clone();
        let create_session_for_task = Arc::clone(&create_session);
        let create = tokio::spawn(async move {
            let namespace = DocumentNamespace::new("app", "events").unwrap();
            let command = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                namespace,
                DocumentCollectionOptions::empty(),
                DocumentWriteOptions::new(),
            ));
            create_engine
                .execute_document(
                    &create_session_for_task,
                    DocumentRequest::new(
                        DocumentRequestId::new([1; 16]).unwrap(),
                        RequestContext::new(),
                        command,
                    ),
                )
                .await
        });

        timeout(Duration::from_secs(2), async {
            loop {
                if engine.inner.database.storage.schema_gate_snapshot().state
                    == SchemaGateState::Migrating
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("collection creation should exclude new schema operations before session wait");

        let busy = timeout(Duration::from_secs(2), engine.status(&create_session))
            .await
            .expect("schema admission must fail before waiting for the creation session")
            .unwrap_err();
        assert_eq!(busy.kind(), EngineErrorKind::Busy);

        first_release_tx.send(()).unwrap();
        second_release_tx.send(()).unwrap();
        first_holder.await.unwrap().unwrap();
        second_holder.await.unwrap().unwrap();
        let created = timeout(Duration::from_secs(2), create)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(created.result(), DocumentResult::Collection(_)));
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn collection_creation_rejects_its_own_transaction_before_schema_quiescence() {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open(temp.path(), 2).await.unwrap();
        let session = engine.session();
        let schema = engine
            .inner
            .database
            .storage
            .enter_schema_operation()
            .unwrap();
        let lifecycle = engine.inner.lifecycle.try_acquire().unwrap();
        session
            .inner
            .lock()
            .await
            .begin_transaction(crate::core::session::TransactionState::new(
                lifecycle, schema,
            ));

        let command = DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
            DocumentNamespace::new("app", "events").unwrap(),
            DocumentCollectionOptions::empty(),
            DocumentWriteOptions::new(),
        ));
        let error = timeout(
            Duration::from_secs(2),
            engine.execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([2; 16]).unwrap(),
                    RequestContext::new(),
                    command,
                ),
            ),
        )
        .await
        .expect("collection creation must not wait on its own transaction schema guard")
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);

        let transaction = session
            .inner
            .lock()
            .await
            .finish_transaction()
            .expect("test session owns transaction state");
        transaction.finish(false, None).unwrap();
        engine.shutdown().await.unwrap();
    }
}
