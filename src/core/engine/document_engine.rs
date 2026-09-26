//! Protocol-neutral document command execution through engine-owned resources.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::BinaryHeap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use tokio::task::JoinHandle;

mod aggregation;
mod deletion;
mod distinct;
mod fanout;
mod id_routing;
mod index_metadata;
mod metadata;
mod single_mutation;
mod sorting;
mod update_many;
#[cfg(test)]
mod write_recovery;
mod write_transaction;

use write_transaction::write_transaction;

use super::document_cursor::{
    AggregateCursor, AggregateRow, CursorSource as PreparedFilterRoute, CursorState,
    IndexMetadataCursorState, MetadataCursorState, ReadStats, RetainedCursorState,
};
use super::{Engine, Operation, flatten_join, pending_cancellation_reason, retire_if_broken};
use crate::{
    core::{
        CancelOnDrop, CancellationToken, EngineError, EngineErrorKind, EngineResult,
        OperationControl, ResultLimits, Session, SessionInner, SessionState, wait_for_cancellation,
        wait_pending,
    },
    document::{
        BSON_MAX_DECODED_BYTES, BsonDocument, BsonErrorContext, BsonObjectId, BsonTimestamp,
        BsonValue, DocumentCollectionId, DocumentCollectionMetadata, DocumentCollectionOptions,
        DocumentCommand, DocumentCursorError, DocumentDeleteResult, DocumentExecution,
        DocumentFilter, DocumentIndexError, DocumentIndexMetadata, DocumentInsertResult,
        DocumentMatcher, DocumentMutationScope, DocumentNamespace, DocumentPlan, DocumentPointPlan,
        DocumentProjector, DocumentReadOptions, DocumentRequest, DocumentResult,
        DocumentScatterPlan, DocumentSorter, DocumentWriteError, DocumentWriteOptions,
        MAX_DOCUMENT_REQUEST_BYTES, encode_document,
    },
    storage::{
        ConnectionOwner, DocumentStorageRecord, MAX_DOCUMENT_SHARD_SCAN_RECORDS, PooledConnection,
        PreparedDocumentWrite, SchemaOperationGuard, Storage,
    },
};

const DOCUMENT_RESULT_ENVELOPE_BYTES: u64 = 16;
const DOCUMENT_RESULT_ROW_BYTES: u64 = 8;
const DOCUMENT_RESULT_VALUE_BYTES: u64 = 9;
const DOCUMENT_READ_ACCESS_BYTES: u64 = 32;
// Aggregate counters, shard IDs and at most 64 bounded per-shard row summaries.
const DOCUMENT_READ_STATS_BYTES: u64 = 2048;
const DOCUMENT_MERGE_PAGE_SIZE: usize = 1;
const DOCUMENT_WRITE_ERROR_BYTES: u64 = 64;
static SERVER_TIMESTAMP: AtomicU64 = AtomicU64::new(0);
const _: () = assert!(DOCUMENT_MERGE_PAGE_SIZE <= MAX_DOCUMENT_SHARD_SCAN_RECORDS);

enum DocumentIndexOperation {
    Build(String),
    Drop(String),
    Create(crate::document::DocumentIndexRequest),
    CreateBatch {
        indexes: Box<[crate::document::DocumentIndexRequest]>,
        resolve_names: bool,
    },
    DropBatch(Option<String>),
}

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
            DocumentCommand::BuildIndex(request) => {
                let (namespace, name, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_index_operation(
                        &mut operation,
                        session,
                        request_id,
                        namespace,
                        DocumentIndexOperation::Build(name),
                    )
                    .await
                }
            }
            DocumentCommand::CreateBuiltIndex(request) => {
                let (namespace, index, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_index_operation(
                        &mut operation,
                        session,
                        request_id,
                        namespace,
                        DocumentIndexOperation::Create(index),
                    )
                    .await
                }
            }
            DocumentCommand::DropIndexes(request) => {
                let (namespace, name, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_index_operation(
                        &mut operation,
                        session,
                        request_id,
                        namespace,
                        DocumentIndexOperation::DropBatch(name),
                    )
                    .await
                }
            }
            DocumentCommand::CreateIndexes(request) => {
                let resolve_names = request.resolve_names();
                let (namespace, indexes, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_index_operation(
                        &mut operation,
                        session,
                        request_id,
                        namespace,
                        DocumentIndexOperation::CreateBatch {
                            indexes,
                            resolve_names,
                        },
                    )
                    .await
                }
            }
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
            DocumentCommand::DropCollection(request) => {
                let (namespace, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_drop_namespace(
                        &mut operation,
                        session,
                        request_id,
                        namespace.database().to_owned(),
                        Some(namespace.collection().to_owned()),
                    )
                    .await
                }
            }
            DocumentCommand::DropDatabase(request) => {
                let (database, options) = request.into_parts();
                if let Err(error) = require_catalog_write_options(options) {
                    Err(error)
                } else {
                    self.run_document_drop_namespace(
                        &mut operation,
                        session,
                        request_id,
                        database,
                        None,
                    )
                    .await
                }
            }
            command => {
                let schema_operation = match self.inner.database.storage.enter_schema_operation() {
                    Ok(guard) => guard,
                    Err(error) => return operation.finish(Err(error)),
                };
                let ready_drop = if let DocumentCommand::DropIndex(request) = &command {
                    match self
                        .inner
                        .database
                        .storage
                        .document_index_is_ready(request.namespace(), request.name())
                    {
                        Ok(ready) => ready,
                        Err(error) => return operation.finish(Err(error)),
                    }
                } else {
                    false
                };
                if ready_drop {
                    drop(schema_operation);
                    let DocumentCommand::DropIndex(request) = command else {
                        unreachable!("only index drops select physical cleanup");
                    };
                    let (namespace, name, options) = request.into_parts();
                    if let Err(error) = require_catalog_write_options(options) {
                        Err(error)
                    } else {
                        self.run_document_index_operation(
                            &mut operation,
                            session,
                            request_id,
                            namespace,
                            DocumentIndexOperation::Drop(name),
                        )
                        .await
                    }
                } else {
                    let session_guard =
                        match operation.wait_pending(self.ready_session(session)).await {
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
            let mut session = session;
            let result = engine
                .coordinate_document_command(
                    owner,
                    &mut session,
                    request_id,
                    command,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await;
            let cursor = result.as_ref().ok().and_then(execution_cursor);
            let result = worker_control.complete(result);
            if result.is_err() {
                if let Some((namespace, id)) = cursor {
                    engine.inner.document_cursors.kill(owner, &namespace, id);
                }
            }
            result
        });
        operation.wait_started(join).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn coordinate_document_command(
        &self,
        owner: ConnectionOwner,
        session: &mut SessionInner,
        request_id: crate::document::DocumentRequestId,
        command: DocumentCommand,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        result_limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let storage = self.inner.database.storage.clone();
        let result_cancellation = cancellation.clone();
        let mutation_returns_document = matches!(
            &command,
            DocumentCommand::FindOneAndDelete(_)
                | DocumentCommand::FindOneAndReplace(_)
                | DocumentCommand::FindOneAndUpdate(_)
        );
        let execution = match command {
            DocumentCommand::ListDatabaseNames(request) => {
                let names = self
                    .list_document_database_names(
                        request.into_filter(),
                        cancellation,
                        deadline,
                        result_limits,
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::DatabaseNames(names.into_boxed_slice()),
                ))
            }
            DocumentCommand::ListCollectionMetadata(request) => {
                self.start_collection_metadata_cursor(
                    owner,
                    session,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::ListIndexMetadata(request) => {
                self.start_index_metadata_cursor(
                    owner,
                    session,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::CollectionExists(request) => {
                let namespace = request.into_namespace();
                let exists = self
                    .run_document_storage_task(
                        cancellation,
                        deadline,
                        move |cancellation, control| {
                            let exists = storage
                                .document_collection_controlled(
                                    namespace.database(),
                                    namespace.collection(),
                                    Arc::clone(&control),
                                )?
                                .is_some();
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok(exists)
                        },
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::CollectionExists(exists),
                ))
            }
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
                let (specification, name, unique) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            crate::document::normalize_index_request(index, &mut || {
                                ensure_document_cpu_active(cancellation, &control)
                            })
                        },
                    )
                    .await?;
                enforce_execution_result_limits(
                    &DocumentExecution::new(
                        request_id,
                        None,
                        DocumentResult::IndexName(name.clone()),
                    ),
                    result_limits,
                )?;
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
                let metadata = self
                    .run_document_storage_task(
                        cancellation,
                        deadline,
                        move |_cancellation, control| {
                            metadata_storage.declare_document_index_controlled(
                                collection_id,
                                &metadata_name,
                                &specification,
                                unique,
                                control,
                            )
                        },
                    )
                    .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    if metadata.lifecycle() == crate::document::DocumentIndexLifecycle::Ready {
                        DocumentResult::IndexReady(name)
                    } else {
                        DocumentResult::IndexName(name)
                    },
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
                let batch = documents.len() > 1;
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
                // Allocate all result bookkeeping before the first write. A
                // duplicate is a safe per-item failure; interruption and storage
                // failures still abort without claiming multi-shard atomicity.
                let mut inserted_ids = Vec::new();
                let mut write_errors = Vec::new();
                inserted_ids.try_reserve_exact(ids.len()).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::OutOfMemory,
                        "unable to reserve insert results",
                        error,
                    )
                })?;
                write_errors.try_reserve_exact(ids.len()).map_err(|error| {
                    EngineError::from_source(
                        EngineErrorKind::OutOfMemory,
                        "unable to reserve insert errors",
                        error,
                    )
                })?;
                let mut pending = prepared.into_iter().zip(ids).enumerate().peekable();
                while let Some((_, (first, _))) = pending.peek() {
                    let shard = first.shard();
                    // One lease/worker handles each contiguous same-shard run.
                    // This groups an entire single-shard batch without reordering
                    // cross-shard inputs or promising transactional batch writes.
                    let (remaining, successes, failures, stopped) = self
                        .run_document_shard_controlled(
                            shard,
                            owner,
                            cancellation.clone(),
                            deadline,
                            move |storage, connection, cancellation, control| {
                                let mut stopped = false;
                                while pending
                                    .peek()
                                    .is_some_and(|(_, (write, _))| write.shard() == shard)
                                {
                                    let (offset, (write, id)) =
                                        pending.next().expect("peeked insert");
                                    let natural_order = first_order
                                        .checked_add(
                                            u64::try_from(offset)
                                                .expect("bounded insert count fits u64"),
                                        )
                                        .ok_or_else(|| {
                                            limit_exceeded(
                                                "document natural-order identity overflowed",
                                            )
                                        })?;
                                    // One transaction per input, not per batch: future
                                    // index maintenance must commit with this record,
                                    // while earlier successful inputs remain committed.
                                    match write_transaction(
                                        storage,
                                        collection_id,
                                        shard,
                                        connection,
                                        cancellation,
                                        control,
                                        |transaction| {
                                            storage.insert_prepared_document_on_connection(
                                                transaction,
                                                collection_id,
                                                natural_order,
                                                shard,
                                                &write,
                                                cancellation,
                                            )
                                        },
                                    ) {
                                        Ok(()) => inserted_ids.push(id),
                                        Err(error) if batch && error.is_rolled_back_duplicate() => {
                                            write_errors.push(DocumentWriteError::new(
                                                offset,
                                                EngineErrorKind::UniqueViolation,
                                            ));
                                            if options.ordered() {
                                                stopped = true;
                                                break;
                                            }
                                        }
                                        Err(error) => return Err(error.into_engine_error()),
                                    }
                                }
                                Ok((pending, inserted_ids, write_errors, stopped))
                            },
                        )
                        .await?;
                    pending = remaining;
                    inserted_ids = successes;
                    write_errors = failures;
                    if stopped {
                        break;
                    }
                }
                Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    DocumentResult::Insert(DocumentInsertResult::from_batch(
                        inserted_ids,
                        write_errors,
                    )),
                ))
            }
            DocumentCommand::Find(request) => {
                let (namespace, filter, options) = request.into_parts();
                let catalog_storage = storage.clone();
                let catalog_namespace = namespace.clone();
                let projection = options.projection().cloned();
                let sort = options.sort().cloned();
                let (collection_id, route, projection, sorter) = self
                    .run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            let projection = projection
                                .as_ref()
                                .map(|spec| {
                                    DocumentProjector::compile_with_check(
                                        spec.document(),
                                        &mut || ensure_document_cpu_active(cancellation, &control),
                                    )
                                    .map(Arc::new)
                                })
                                .transpose()?;
                            let sorter = sort
                                .as_ref()
                                .filter(|spec| !spec.document().is_empty())
                                .map(|spec| {
                                    DocumentSorter::compile_with_check(spec.document(), &mut || {
                                        ensure_document_cpu_active(cancellation, &control)
                                    })
                                    .map(Arc::new)
                                })
                                .transpose()?;
                            let collection = catalog_storage.document_collection_controlled(
                                catalog_namespace.database(),
                                catalog_namespace.collection(),
                                Arc::clone(&control),
                            )?;
                            let collection_id = collection
                                .ok_or_else(|| {
                                    crate::document::DocumentCollectionNotFound.into_engine_error()
                                })?
                                .id();
                            let route = prepare_filter_route(
                                &catalog_storage,
                                &filter,
                                cancellation,
                                &control,
                            )?;
                            ensure_document_cpu_active(cancellation, &control)?;
                            Ok((collection_id, route, projection, sorter))
                        },
                    )
                    .await?;
                let mut state = CursorState {
                    namespace: namespace.clone(),
                    collection_id,
                    source: route,
                    read_stats: ReadStats::for_options(&options),
                    aggregation: None,
                    projection,
                    sorter,
                    sort_after: None,
                    after: None,
                    skip: options.skip(),
                    remaining: options.limit(),
                    batch_byte_limit: options.batch_byte_limit(),
                };
                let plan = self
                    .document_cursor_plan(&state, &options, cancellation.clone(), deadline)
                    .await?;
                let (documents, has_more) = self
                    .read_document_page(
                        owner,
                        &mut state,
                        cancellation,
                        deadline,
                        &options,
                        result_limits,
                    )
                    .await?;
                let read_stats = state.finish_read_stats();
                let cursor_id = if has_more {
                    session
                        .document_cursor_owner
                        .get_or_insert_with(|| self.inner.document_cursors.owner(owner));
                    Some(self.inner.document_cursors.insert(owner, state)?)
                } else {
                    None
                };
                let batch = crate::document::DocumentCursorBatch::from_validated(
                    namespace, cursor_id, documents,
                );
                Ok(
                    DocumentExecution::new(request_id, Some(plan), DocumentResult::Cursor(batch))
                        .with_read_stats(read_stats),
                )
            }
            DocumentCommand::ContinueCursor(request) => {
                let (namespace, id, options) = request.into_parts();
                if options.batch_size() == 0
                    || options.skip() != 0
                    || options.limit().is_some()
                    || options.projection().is_some()
                    || options.sort().is_some()
                {
                    return Err(EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        "cursor continuation requires a positive batch size and cannot change skip/limit/projection/sort",
                    ));
                }
                let mut lease = self
                    .inner
                    .document_cursors
                    .checkout(owner, &namespace, id)?;
                match lease.state.take().expect("checked-out cursor owns state") {
                    RetainedCursorState::Collections(mut state) => {
                        if let Some(bytes) = options.batch_byte_limit() {
                            state.batch_byte_limit =
                                Some(state.batch_byte_limit.unwrap_or(u64::MAX).min(bytes));
                        }
                        let (documents, has_more) = self
                            .read_collection_metadata_page(
                                &mut state,
                                cancellation,
                                deadline,
                                options.batch_size(),
                                result_limits,
                            )
                            .await?;
                        let cursor_id = lease.complete(
                            has_more.then_some(RetainedCursorState::Collections(state)),
                        )?;
                        Ok(DocumentExecution::new(
                            request_id,
                            None,
                            DocumentResult::Cursor(
                                crate::document::DocumentCursorBatch::from_validated(
                                    namespace, cursor_id, documents,
                                ),
                            ),
                        ))
                    }
                    RetainedCursorState::Indexes(mut state) => {
                        if let Some(bytes) = options.batch_byte_limit() {
                            state.batch_byte_limit =
                                Some(state.batch_byte_limit.unwrap_or(u64::MAX).min(bytes));
                        }
                        let (documents, has_more) = self
                            .read_index_metadata_page(
                                &mut state,
                                cancellation,
                                deadline,
                                options.batch_size(),
                                result_limits,
                            )
                            .await?;
                        let cursor_id = lease
                            .complete(has_more.then_some(RetainedCursorState::Indexes(state)))?;
                        Ok(DocumentExecution::new(
                            request_id,
                            None,
                            DocumentResult::Cursor(
                                crate::document::DocumentCursorBatch::from_validated(
                                    namespace, cursor_id, documents,
                                ),
                            ),
                        ))
                    }
                    RetainedCursorState::Documents(mut state) => {
                        state.read_stats = ReadStats::for_options(&options);
                        let catalog_namespace = namespace.clone();
                        let collection_id = state.collection_id;
                        self.run_document_storage_task(
                            cancellation.clone(),
                            deadline,
                            move |cancellation, control| {
                                let current = storage.document_collection_controlled(
                                    catalog_namespace.database(),
                                    catalog_namespace.collection(),
                                    Arc::clone(&control),
                                )?;
                                if current.is_none_or(|collection| collection.id() != collection_id)
                                {
                                    return Err(DocumentCursorError::NotFound.into_engine_error());
                                }
                                ensure_document_cpu_active(cancellation, &control)
                            },
                        )
                        .await?;
                        if let Some(bytes) = options.batch_byte_limit() {
                            state.batch_byte_limit =
                                Some(state.batch_byte_limit.unwrap_or(u64::MAX).min(bytes));
                        }
                        let plan = self
                            .document_cursor_plan(&state, &options, cancellation.clone(), deadline)
                            .await?;
                        let (documents, has_more) = self
                            .read_document_page(
                                owner,
                                &mut state,
                                cancellation,
                                deadline,
                                &options,
                                result_limits,
                            )
                            .await?;
                        let read_stats = state.finish_read_stats();
                        let cursor_id = lease.complete(has_more.then_some(state.into()))?;
                        Ok(DocumentExecution::new(
                            request_id,
                            Some(plan),
                            DocumentResult::Cursor(
                                crate::document::DocumentCursorBatch::from_validated(
                                    namespace, cursor_id, documents,
                                ),
                            ),
                        )
                        .with_read_stats(read_stats))
                    }
                }
            }
            DocumentCommand::KillCursor(request) => {
                let (namespace, id, options) = request.into_parts();
                require_catalog_write_options(options)?;
                enforce_execution_result_limits(
                    &DocumentExecution::new(request_id, None, DocumentResult::CursorKilled(true)),
                    result_limits,
                )?;
                let killed = self.inner.document_cursors.kill(owner, &namespace, id);
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::CursorKilled(killed),
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
                                &filter,
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
                    route @ (PreparedFilterRoute::Scatter(_)
                    | PreparedFilterRoute::ShardSubset { .. }) => {
                        let count = self
                            .count_document_shards(
                                owner,
                                collection_id,
                                &route,
                                cancellation,
                                deadline,
                            )
                            .await?;
                        (route.plan(collection_id, self.shard_count())?, count)
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
                self.run_document_delete(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::FindOneAndReplace(request) => {
                self.run_document_find_replace(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::FindOneAndUpdate(request) => {
                self.run_document_find_update(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::FindOneAndDelete(request) => {
                self.run_document_find_delete(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::Distinct(request) => {
                self.run_document_distinct(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::Aggregate(request) => {
                self.run_document_aggregate(
                    owner,
                    session,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::Replace(request) => {
                self.run_document_replace(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::Update(request) => {
                self.run_document_update(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    result_limits,
                )
                .await
            }
            DocumentCommand::DropIndex(request) => {
                let (namespace, name, options) = request.into_parts();
                require_catalog_write_options(options)?;
                if matches!(name.as_str(), "_id" | "_id_") {
                    return Err(DocumentIndexError::Protected.into_engine_error());
                }
                // Preflight the exact acknowledgement before any manifest mutation.
                enforce_execution_result_limits(
                    &DocumentExecution::new(request_id, None, DocumentResult::Acknowledged(true)),
                    result_limits,
                )?;
                self.run_document_storage_task(
                    cancellation,
                    deadline,
                    move |cancellation, control| {
                        let collection = storage.document_collection_controlled(
                            namespace.database(),
                            namespace.collection(),
                            Arc::clone(&control),
                        )?;
                        let collection_id = require_collection(collection)?.id();
                        ensure_document_cpu_active(cancellation, &control)?;
                        storage.drop_pending_document_index_controlled(
                            collection_id,
                            &name,
                            control,
                        )
                    },
                )
                .await?;
                Ok(DocumentExecution::new(
                    request_id,
                    None,
                    DocumentResult::Acknowledged(true),
                ))
            }
            DocumentCommand::CreateCollection(_)
            | DocumentCommand::CreateBuiltIndex(_)
            | DocumentCommand::CreateIndexes(_)
            | DocumentCommand::DropIndexes(_)
            | DocumentCommand::BuildIndex(_)
            | DocumentCommand::DropCollection(_)
            | DocumentCommand::DropDatabase(_) => Err(EngineError::new(
                EngineErrorKind::Internal,
                "document namespace mutation reached the data-command coordinator",
            )),
        }?;
        if mutation_returns_document || execution_result_is_mutation(execution.result()) {
            // Every supported mutation preflights this exact result shape
            // before its first durable write. Once that write commits, return
            // success even if cancellation wins the race to result delivery.
            Ok(execution)
        } else {
            let cursor = execution_cursor(&execution);
            let result = self
                .run_document_storage_task(
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
                .await;
            if result.is_err() {
                if let Some((namespace, id)) = cursor {
                    self.inner.document_cursors.kill(owner, &namespace, id);
                }
            }
            result
        }
    }

    async fn run_document_index_operation(
        &self,
        operation: &mut Operation,
        session: &Session,
        request_id: crate::document::DocumentRequestId,
        namespace: DocumentNamespace,
        action: DocumentIndexOperation,
    ) -> EngineResult<DocumentExecution> {
        let migration = self.inner.database.storage.begin_schema_migration()?;
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
        let cancellation = operation.cancellation.clone();
        let result_limits = operation.result_limits;
        let storage = self.inner.database.storage.clone();
        let connections = self.inner.connections.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session;
            let result: EngineResult<DocumentExecution> = (|| {
                if let DocumentIndexOperation::DropBatch(name) = action {
                    let execution = DocumentExecution::new(
                        request_id,
                        None,
                        DocumentResult::IndexesDropped {
                            before: 0,
                            after: 0,
                        },
                    );
                    enforce_execution_result_limits(&execution, result_limits)?;
                    connections.retire_idle_for_schema_migration()?;
                    let (before, after) = storage.drop_document_indexes_controlled(
                        &namespace,
                        name.as_deref(),
                        migration,
                        Arc::clone(&worker_control),
                    )?;
                    return Ok(DocumentExecution::new(
                        request_id,
                        None,
                        DocumentResult::IndexesDropped { before, after },
                    ));
                }
                if let DocumentIndexOperation::CreateBatch {
                    indexes,
                    resolve_names,
                } = action
                {
                    let definitions = crate::document::normalize_index_batch(indexes, &mut || {
                        ensure_document_cpu_active(&cancellation, &worker_control)
                    })?;
                    let execution = DocumentExecution::new(
                        request_id,
                        None,
                        if resolve_names {
                            // Reuse may resolve to a longer existing name. Admit
                            // the bounded worst case before the first mutation.
                            DocumentResult::IndexModelsBuilt {
                                names: vec![
                                    "x".repeat(
                                        crate::document::MAX_DOCUMENT_INDEX_NAME_BYTES
                                    );
                                    definitions.len()
                                ]
                                .into_boxed_slice(),
                                before: 0,
                                after: 0,
                            }
                        } else {
                            DocumentResult::IndexesBuilt {
                                before: 0,
                                after: 0,
                            }
                        },
                    );
                    enforce_execution_result_limits(&execution, result_limits)?;
                    connections.retire_idle_for_schema_migration()?;
                    let (before, after, names) = storage.create_document_indexes_controlled(
                        &namespace,
                        definitions,
                        migration,
                        Arc::clone(&worker_control),
                    )?;
                    return Ok(DocumentExecution::new(
                        request_id,
                        None,
                        if resolve_names {
                            DocumentResult::IndexModelsBuilt {
                                before,
                                after,
                                names,
                            }
                        } else {
                            DocumentResult::IndexesBuilt { before, after }
                        },
                    ));
                }
                let (name, declaration, response) = match action {
                    DocumentIndexOperation::CreateBatch { .. }
                    | DocumentIndexOperation::DropBatch(_) => unreachable!("batch handled above"),
                    DocumentIndexOperation::Create(index) => {
                        let (specification, name, unique) =
                            crate::document::normalize_index_request(index, &mut || {
                                ensure_document_cpu_active(&cancellation, &worker_control)
                            })?;
                        let response = DocumentResult::IndexBuilt {
                            name: name.clone(),
                            before: 0,
                            after: 0,
                        };
                        (name, Some((specification, unique)), response)
                    }
                    DocumentIndexOperation::Build(name) => {
                        let response = DocumentResult::IndexReady(name.clone());
                        (name, None, response)
                    }
                    DocumentIndexOperation::Drop(name) => {
                        (name, None, DocumentResult::Acknowledged(true))
                    }
                };
                // Allocate and validate the exact response before durable intent;
                // filling two fixed-width counters after commit cannot fail.
                let execution = DocumentExecution::new(request_id, None, response);
                enforce_execution_result_limits(&execution, result_limits)?;
                connections.retire_idle_for_schema_migration()?;
                let (_, _, mut response) = execution.into_parts();
                match &mut response {
                    DocumentResult::IndexBuilt { before, after, .. } => {
                        let (specification, unique) =
                            declaration.as_ref().expect("create definition");
                        (*before, *after) = storage.create_built_document_index_controlled(
                            &namespace,
                            &name,
                            specification,
                            *unique,
                            migration,
                            Arc::clone(&worker_control),
                        )?;
                    }
                    DocumentResult::IndexReady(_) => {
                        storage.build_document_index_controlled(
                            namespace.database(),
                            namespace.collection(),
                            &name,
                            migration,
                            Arc::clone(&worker_control),
                        )?;
                    }
                    DocumentResult::Acknowledged(_) => storage
                        .drop_built_document_index_controlled(
                            namespace.database(),
                            namespace.collection(),
                            &name,
                            migration,
                            Arc::clone(&worker_control),
                        )?,
                    _ => unreachable!("only index build/drop responses are constructed"),
                }
                Ok(DocumentExecution::new(request_id, None, response))
            })();
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
            }
            worker_control.complete(result)
        });
        operation.wait_started(join).await
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

    async fn run_document_drop_namespace(
        &self,
        operation: &mut Operation,
        session: &Session,
        request_id: crate::document::DocumentRequestId,
        database: String,
        collection: Option<String>,
    ) -> EngineResult<DocumentExecution> {
        let migration = self.inner.database.storage.begin_schema_migration()?;
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
        let connections = self.inner.connections.clone();
        let join = worker.spawn(move || {
            let _lease = lease;
            let _session = session;
            let result = enforce_execution_result_limits(
                &DocumentExecution::new(request_id, None, DocumentResult::NamespaceDropped(true)),
                result_limits,
            )
            .and_then(|_| connections.retire_idle_for_schema_migration())
            .and_then(|_| {
                storage.drop_document_namespace_controlled(
                    &database,
                    collection.as_deref(),
                    migration,
                    Arc::clone(&worker_control),
                )
            })
            .map(|existed| {
                DocumentExecution::new(request_id, None, DocumentResult::NamespaceDropped(existed))
            });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == EngineErrorKind::DataCorruption)
            {
                storage.record_schema_degraded();
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
        self.run_document_shard_controlled(
            shard,
            owner,
            cancellation,
            deadline,
            move |storage, connection, cancellation, _control| {
                work(storage, connection, cancellation)
            },
        )
        .await
    }

    async fn run_document_shard_controlled<T, F>(
        &self,
        shard: u16,
        owner: ConnectionOwner,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(
                &Storage,
                &mut PooledConnection,
                &CancellationToken,
                &Arc<OperationControl>,
            ) -> EngineResult<T>
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
                    let result = connection.run_document_controlled(
                        Arc::clone(&worker_control),
                        |connection| {
                            work(&storage, connection, &task_cancellation, &worker_control)
                        },
                    );
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

    async fn document_cursor_plan(
        &self,
        state: &CursorState,
        options: &DocumentReadOptions,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
    ) -> EngineResult<DocumentPlan> {
        let plan = state.source.plan(state.collection_id, self.shard_count())?;
        if !options.plan_diagnostics() {
            return Ok(plan);
        }
        // Point plans already describe a canonical-ID lookup. Inserts and
        // other commands without read diagnostics keep their existing plans.
        let DocumentPlan::Scatter(plan) = plan else {
            return Ok(plan);
        };
        let storage = self.inner.database.storage.clone();
        let source = state.source.clone();
        let collection = state.collection_id;
        let aggregation = state.aggregation.is_some();
        self.run_document_storage_task(cancellation, deadline, move |cancellation, control| {
            use crate::document::{DocumentReadAccess, DocumentScanReason};
            let mut check = || ensure_document_cpu_active(cancellation, &control);
            check()?;
            // The request still owns schema admission. This uses the same
            // Ready cache and bounded selector as its actual reads, but keeps
            // only payload-free diagnostics, never retained probe authority.
            let access = if aggregation {
                DocumentReadAccess::Scan {
                    reason: DocumentScanReason::AggregationInput,
                }
            } else if let Some(matcher) = source.matcher() {
                storage
                    .document_candidate_selection(collection, matcher, &mut check)?
                    .1
            } else {
                DocumentReadAccess::Scan {
                    reason: DocumentScanReason::Unfiltered,
                }
            };
            check()?;
            Ok(DocumentPlan::Scatter(plan.with_read_access(access)))
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_document_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        if state.aggregation.is_some() {
            self.read_aggregate_page(owner, state, cancellation, deadline, options, limits)
                .await
        } else {
            self.read_document_source_page(owner, state, cancellation, deadline, options, limits)
                .await
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_document_source_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        enforce_empty_result_limit(limits)?;
        if state
            .batch_byte_limit
            .is_some_and(|limit| cursor_page_base_bytes(state, self.shard_count(), options) > limit)
        {
            return Err(limit_exceeded(
                "cursor envelope cannot fit the batch byte limit",
            ));
        }
        if state.remaining == Some(0) {
            return Ok((Vec::new(), false));
        }
        if options.batch_size() == 0 {
            return Ok((Vec::new(), true));
        }
        match &state.source {
            PreparedFilterRoute::Point { id_key, shard } => {
                let id_key = id_key.clone();
                let shard = *shard;
                let collection_id = state.collection_id;
                let stats = state.read_stats.clone();
                let (id_key, record) = self
                    .run_document_shard(
                        shard,
                        owner,
                        cancellation.clone(),
                        deadline,
                        move |storage, connection, cancellation| {
                            if let Some(stats) = &stats {
                                stats.storage_read(shard);
                            }
                            let record = storage.get_document_on_connection(
                                connection,
                                collection_id,
                                shard,
                                &id_key,
                                cancellation,
                            )?;
                            if let Some(stats) = &stats {
                                stats.examine(shard, u64::from(record.is_some()));
                            }
                            Ok((id_key, record))
                        },
                    )
                    .await?;
                if let Some(record) = &record {
                    validate_point_record(record, collection_id, shard, &id_key)?;
                }
                let Some(record) = record else {
                    return Ok((Vec::new(), false));
                };
                if let Some(stats) = &state.read_stats {
                    stats.source_match(shard);
                }
                // Sorting validates the original value even when skip removes
                // this point result; array-key errors must not be hidden.
                let record = if let Some(sorter) = state.sorter.clone() {
                    self.run_document_storage_task(
                        cancellation.clone(),
                        deadline,
                        move |cancellation, control| {
                            sorter.key_validated_with_check(record.document(), &mut || {
                                ensure_document_cpu_active(cancellation, &control)
                            })?;
                            Ok(record)
                        },
                    )
                    .await?
                } else {
                    record
                };
                if state.skip != 0 {
                    return Ok((Vec::new(), false));
                }
                let (document, encoded_len) = self
                    .cursor_output_document(
                        record,
                        state.projection.clone(),
                        cancellation,
                        deadline,
                    )
                    .await?;
                let mut bytes = cursor_page_base_bytes(state, self.shard_count(), options);
                if state.batch_byte_limit.is_some_and(|limit| {
                    bytes
                        + DOCUMENT_RESULT_ROW_BYTES
                        + DOCUMENT_RESULT_VALUE_BYTES
                        + encoded_len as u64
                        > limit
                }) {
                    return Err(limit_exceeded(
                        "document cannot fit the cursor batch byte limit",
                    ));
                }
                add_document_result_budget(&mut bytes, encoded_len, limits)?;
                Ok((vec![document], false))
            }
            PreparedFilterRoute::Scatter(_) | PreparedFilterRoute::ShardSubset { .. } => {
                let matcher = state.source.matcher().cloned();
                if state.sorter.is_some() {
                    return self
                        .scan_sorted_document_page(
                            owner,
                            state,
                            cancellation,
                            deadline,
                            matcher,
                            options,
                            limits,
                        )
                        .await;
                }
                self.scan_document_page(
                    owner,
                    state,
                    cancellation,
                    deadline,
                    matcher,
                    options,
                    limits,
                )
                .await
            }
        }
    }

    async fn cursor_output_document(
        &self,
        record: DocumentStorageRecord,
        projection: Option<Arc<DocumentProjector>>,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
    ) -> EngineResult<(BsonDocument, usize)> {
        let Some(projection) = projection else {
            let bytes = record.encoded_len();
            return Ok((record.into_document(), bytes));
        };
        self.run_document_storage_task(cancellation, deadline, move |cancellation, control| {
            let document = projection
                .project_owned_validated_with_check(record.into_document(), &mut || {
                    ensure_document_cpu_active(cancellation, &control)
                })?;
            let bytes = encode_document(&document)
                .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?
                .len();
            ensure_document_cpu_active(cancellation, &control)?;
            Ok((document, bytes))
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_document_page(
        &self,
        owner: ConnectionOwner,
        state: &mut CursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        matcher: Option<Arc<DocumentMatcher>>,
        options: &DocumentReadOptions,
        limits: ResultLimits,
    ) -> EngineResult<(Vec<BsonDocument>, bool)> {
        enforce_empty_result_limit(limits)?;
        let collection_id = state.collection_id;
        let frontier_limit = limits
            .max_bytes()
            .max(u64::try_from(BSON_MAX_DECODED_BYTES).unwrap_or(u64::MAX));
        let (mut frontiers, mut retained_bytes) = self
            .initial_document_frontiers(
                owner,
                state,
                cancellation.clone(),
                deadline,
                matcher.clone(),
                frontier_limit,
            )
            .await?;

        let mut heap = BinaryHeap::new();
        for (shard, record) in frontiers.iter().enumerate() {
            if let Some(record) = record {
                heap.push(Reverse((record.natural_order(), shard)));
            }
        }

        let mut documents = Vec::new();
        let mut result_bytes = cursor_page_base_bytes(state, self.shard_count(), options);
        let requested = state
            .remaining
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

            if state.skip > 0 {
                state.skip -= 1;
            } else if u64::try_from(documents.len()).unwrap_or(u64::MAX) < requested {
                let (document, encoded_len) = self
                    .cursor_output_document(
                        record,
                        state.projection.clone(),
                        cancellation.clone(),
                        deadline,
                    )
                    .await?;
                let next_bytes = result_bytes
                    .checked_add(DOCUMENT_RESULT_ROW_BYTES)
                    .and_then(|bytes| bytes.checked_add(DOCUMENT_RESULT_VALUE_BYTES))
                    .and_then(|bytes| bytes.checked_add(encoded_len as u64))
                    .ok_or_else(result_size_overflow)?;
                if state
                    .batch_byte_limit
                    .is_some_and(|limit| next_bytes > limit)
                {
                    if documents.is_empty() {
                        return Err(limit_exceeded(
                            "document cannot fit the cursor batch byte limit",
                        ));
                    }
                    has_more = true;
                    break;
                }
                add_document_result_budget(&mut result_bytes, encoded_len, limits)?;
                if u64::try_from(documents.len()).unwrap_or(u64::MAX) >= limits.max_rows() {
                    return Err(limit_exceeded(
                        "document result exceeds the request row limit",
                    ));
                }
                documents.push(document);
                if let Some(remaining) = &mut state.remaining {
                    *remaining = remaining.saturating_sub(1);
                }
            } else {
                has_more = true;
                break;
            }

            state.after = Some(natural_order);
            if state.remaining == Some(0) {
                break;
            }

            let shard = u16::try_from(shard_index).expect("document shard index fits u16");
            let after = Some(natural_order);
            let matcher = matcher.clone();
            let stats = state.read_stats.clone();
            let next = self
                .run_document_shard(
                    shard,
                    owner,
                    cancellation.clone(),
                    deadline,
                    move |storage, connection, cancellation| {
                        next_matching_document(
                            storage,
                            connection,
                            collection_id,
                            shard,
                            after,
                            matcher.as_deref(),
                            cancellation,
                            deadline,
                            stats.as_deref(),
                        )
                    },
                )
                .await?;
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

        Ok((documents, has_more))
    }
}

fn execution_cursor(
    execution: &DocumentExecution,
) -> Option<(DocumentNamespace, crate::document::DocumentCursorId)> {
    match execution.result() {
        DocumentResult::Cursor(batch) => {
            batch.cursor_id().map(|id| (batch.namespace().clone(), id))
        }
        _ => None,
    }
}

fn cursor_envelope_bytes(namespace: &DocumentNamespace) -> u64 {
    DOCUMENT_RESULT_ENVELOPE_BYTES
        + 8
        + namespace.database().len() as u64
        + 1
        + namespace.collection().len() as u64
}

fn cursor_page_base_bytes(state: &CursorState, shards: u16, options: &DocumentReadOptions) -> u64 {
    cursor_envelope_bytes(&state.namespace)
        + if options.execution_stats() {
            DOCUMENT_READ_STATS_BYTES
        } else {
            0
        }
        + match &state.source {
            PreparedFilterRoute::Point { id_key, .. } => {
                DOCUMENT_RESULT_VALUE_BYTES + 10 + id_key.as_bytes().len() as u64
            }
            PreparedFilterRoute::Scatter(_) | PreparedFilterRoute::ShardSubset { .. } => {
                DOCUMENT_RESULT_VALUE_BYTES
                    + 8
                    + state.source.shards(shards).count() as u64 * 2
                    + if options.plan_diagnostics() {
                        DOCUMENT_READ_ACCESS_BYTES
                    } else {
                        0
                    }
            }
        }
}

#[allow(clippy::too_many_arguments)]
fn next_matching_document(
    storage: &Storage,
    connection: &mut PooledConnection,
    collection_id: DocumentCollectionId,
    shard: u16,
    mut after: Option<u64>,
    matcher: Option<&DocumentMatcher>,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
    stats: Option<&ReadStats>,
) -> EngineResult<Option<DocumentStorageRecord>> {
    let mut check = || {
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(EngineError::deadline_exceeded(
                "document matcher deadline exceeded",
            ));
        }
        if cancellation.is_cancelled() {
            return Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "document matcher cancelled",
            ));
        }
        Ok(())
    };
    let probe = matcher
        .map(|matcher| storage.document_equality_probe(collection_id, matcher, &mut check))
        .transpose()?
        .flatten();
    loop {
        check()?;
        if let Some(stats) = stats {
            stats.storage_read(shard);
        }
        let record = storage
            .scan_document_candidates_on_connection(
                connection,
                collection_id,
                shard,
                after,
                DOCUMENT_MERGE_PAGE_SIZE,
                probe.as_ref(),
                cancellation,
            )?
            .into_iter()
            .next();
        let Some(record) = record else {
            return Ok(None);
        };
        if let Some(stats) = stats {
            stats.examine(shard, 1);
        }
        if let Some(matcher) = matcher {
            if let Some(stats) = stats {
                stats.match_document();
            }
            if !matcher.matches_with_check(record.document(), &mut check)? {
                after = Some(record.natural_order());
                continue;
            }
        }
        if let Some(stats) = stats {
            stats.source_match(shard);
        }
        check()?;
        return Ok(Some(record));
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

fn require_count_options(options: &DocumentReadOptions) -> EngineResult<()> {
    if options.plan_diagnostics() || options.execution_stats() {
        return Err(unsupported("read diagnostics are not available for count"));
    }
    require_catalog_read_options(options)
}

fn require_catalog_read_options(options: &DocumentReadOptions) -> EngineResult<()> {
    if options.projection().is_some() {
        return Err(unsupported(
            "projection is only supported for document find",
        ));
    }
    if options.sort().is_some() {
        return Err(unsupported("sorting is only supported for document find"));
    }
    Ok(())
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

enum FilterRoute<'a> {
    Point(BsonValue),
    Scatter,
    Filtered(&'a DocumentFilter),
}

fn classify_filter<'a>(
    filter: &'a DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<FilterRoute<'a>> {
    ensure_document_cpu_active(cancellation, control)?;
    if filter.is_empty() {
        return Ok(FilterRoute::Scatter);
    }
    if filter.document().len() == 1 {
        if let Some(value) = filter.document().get_first("_id") {
            if is_literal_id_filter(value, cancellation, control)? {
                return Ok(FilterRoute::Point(value.clone()));
            }
            if let BsonValue::Document(expression) = value {
                if expression.len() == 1 {
                    if let Some(value) = expression.get_first("$eq") {
                        return Ok(FilterRoute::Point(value.clone()));
                    }
                }
            }
        }
    }
    Ok(FilterRoute::Filtered(filter))
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
    filter: &DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<PreparedFilterRoute> {
    match classify_filter(filter, cancellation, control)? {
        FilterRoute::Point(id) => {
            let (id_key, shard) = storage.prepare_document_id(&id)?;
            ensure_document_cpu_active(cancellation, control)?;
            Ok(PreparedFilterRoute::Point { id_key, shard })
        }
        FilterRoute::Scatter => Ok(PreparedFilterRoute::Scatter(None)),
        FilterRoute::Filtered(filter) => {
            let matcher = DocumentMatcher::compile_with_check(filter.document(), &mut || {
                ensure_document_cpu_active(cancellation, control)
            })?;
            let matcher = Arc::new(matcher);
            // Validate every query branch before deriving any routing shortcut.
            if let Some(shards) =
                id_routing::proven_id_shards(storage, filter, cancellation, control)?
            {
                return Ok(PreparedFilterRoute::ShardSubset {
                    matcher: Some(matcher),
                    shards,
                });
            }
            Ok(PreparedFilterRoute::Scatter(Some(matcher)))
        }
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

#[cfg(test)]
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
        let document = prepare_insert_document(document)?;
        let id = document
            .get_unique("_id")
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?
            .ok_or_else(|| {
                EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "document inserts require an explicit _id",
                )
            })?;
        let write = storage.prepare_document_write(&document)?;
        ensure_document_cpu_active(cancellation, control)?;
        prepared.push(write);
        ids.push(id.clone());
    }
    ensure_document_cpu_active(cancellation, control)?;
    Ok((prepared, ids))
}

/// Insert write normalization, independent of any wire protocol.
/// An absent ID is generated; explicit null and all nested timestamps survive.
fn prepare_insert_document(document: &BsonDocument) -> EngineResult<Cow<'_, BsonDocument>> {
    let missing_id = document.get_first("_id").is_none();
    let stamp = |name: &str, value: &BsonValue| {
        name != "_id"
            && matches!(value, BsonValue::Timestamp(value) if value.time() == 0 && value.increment() == 0)
    };
    if !missing_id && !document.iter().any(|(name, value)| stamp(name, value)) {
        return Ok(Cow::Borrowed(document));
    }
    let mut normalized = BsonDocument::new();
    normalized
        .try_reserve(document.len() + usize::from(missing_id))
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::OutOfMemory,
                "unable to normalize insert document",
                error,
            )
        })?;
    if missing_id {
        normalized
            .push(
                "_id",
                BsonValue::ObjectId(BsonObjectId::from_bytes(bson::oid::ObjectId::new().bytes())),
            )
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    }
    for (name, value) in document.iter() {
        let value = if stamp(name, value) {
            BsonValue::Timestamp(next_server_timestamp()?)
        } else {
            value.clone()
        };
        normalized
            .push(name, value)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    }
    Ok(Cow::Owned(normalized))
}

fn next_server_timestamp() -> EngineResult<BsonTimestamp> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u32::try_from(elapsed.as_secs()).ok())
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "server clock exceeds BSON timestamp range",
            )
        })?;
    let mut next = 0;
    SERVER_TIMESTAMP
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
            let old_seconds = (previous >> 32) as u32;
            let increment = previous as u32;
            let (seconds, increment) = if seconds > old_seconds {
                (seconds, 1)
            } else if increment == u32::MAX {
                (old_seconds.checked_add(1)?, 1)
            } else {
                (old_seconds, increment + 1)
            };
            next = (u64::from(seconds) << 32) | u64::from(increment);
            Some(next)
        })
        .map_err(|_| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "server timestamp exhausted",
            )
        })?;
    Ok(BsonTimestamp::new((next >> 32) as u32, next as u32))
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
        // Include the durable u64 identity retained by Rust metadata even
        // though native Python/wire metadata shapes do not expose it yet.
        self.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + 3 + 8)?;
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
                if plan.read_access().is_some() {
                    // Fixed-size enum/index identity/count metadata, no BSON or
                    // index names. Default plans retain their previous budget.
                    self.add_bytes(DOCUMENT_READ_ACCESS_BYTES)?;
                }
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
    if execution.read_stats().is_some() {
        budget.add_bytes(DOCUMENT_READ_STATS_BYTES)?;
    }
    if let Some(plan) = execution.plan() {
        budget.add_plan(plan)?;
        check()?;
    }
    match execution.result() {
        DocumentResult::Acknowledged(_)
        | DocumentResult::CollectionExists(_)
        | DocumentResult::NamespaceDropped(_)
        | DocumentResult::CursorKilled(_) => {
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
        DocumentResult::DatabaseNames(names) => {
            budget.add_rows(names.len())?;
            for name in names {
                check()?;
                budget.add_bytes(
                    DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + name.len() as u64,
                )?;
            }
        }
        DocumentResult::Document(document) => {
            if let Some(document) = document {
                budget.add_rows(1)?;
                budget.add_document(document, check)?;
            }
        }
        DocumentResult::UpsertedDocument(result) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 1)?;
            budget.add_value(result.upserted_id(), check)?;
            if let Some(document) = result.document() {
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
            budget.add_rows(result.inserted_ids().len() + result.write_errors().len())?;
            budget.add_bytes(result.write_errors().len() as u64 * DOCUMENT_WRITE_ERROR_BYTES)?;
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
        DocumentResult::IndexName(name) | DocumentResult::IndexReady(name) => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES)?;
            budget.add_bytes(u64::try_from(name.len()).unwrap_or(u64::MAX))?;
        }
        DocumentResult::IndexBuilt { name, .. } => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 16)?;
            budget.add_bytes(u64::try_from(name.len()).unwrap_or(u64::MAX))?;
        }
        DocumentResult::IndexesBuilt { .. } | DocumentResult::IndexesDropped { .. } => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + DOCUMENT_RESULT_VALUE_BYTES + 16)?;
        }
        DocumentResult::IndexModelsBuilt { names, .. } => {
            budget.add_rows(1)?;
            budget.add_bytes(DOCUMENT_RESULT_ROW_BYTES + 2 * DOCUMENT_RESULT_VALUE_BYTES + 16)?;
            for name in names {
                check()?;
                budget.add_bytes(DOCUMENT_RESULT_VALUE_BYTES + name.len() as u64)?;
            }
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
    if ids.len() > 1 {
        budget.add_bytes(ids.len() as u64 * DOCUMENT_WRITE_ERROR_BYTES)?;
    }
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
            | DocumentResult::NamespaceDropped(_)
            | DocumentResult::Collection(_)
            | DocumentResult::Insert(_)
            | DocumentResult::Update(_)
            | DocumentResult::Delete(_)
            | DocumentResult::IndexName(_)
            | DocumentResult::IndexReady(_)
            | DocumentResult::IndexBuilt { .. }
            | DocumentResult::IndexesBuilt { .. }
            | DocumentResult::IndexModelsBuilt { .. }
            | DocumentResult::IndexesDropped { .. }
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

    #[cfg(feature = "mongo")]
    #[tokio::test]
    async fn index_model_names_preflight_worst_case_result_before_building() {
        use crate::document::{DocumentCreateIndexesRequest, DocumentIndexRequest};
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = engine.session();
        let identity = DocumentRequestId::new([9; 16]).unwrap();
        let namespace = DocumentNamespace::new("app", "items").unwrap();
        engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    identity,
                    RequestContext::new(),
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        namespace.clone(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        let command = || {
            DocumentCommand::CreateIndexes(
                DocumentCreateIndexesRequest::new(
                    namespace.clone(),
                    vec![
                        DocumentIndexRequest::new(
                            BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                        )
                        .unwrap(),
                    ],
                    DocumentWriteOptions::new(),
                )
                .unwrap()
                .with_resolved_names(),
            )
        };
        // A short requested name may resolve to a 255-byte existing name.
        let required = DOCUMENT_RESULT_ENVELOPE_BYTES
            + DOCUMENT_RESULT_ROW_BYTES
            + 3 * DOCUMENT_RESULT_VALUE_BYTES
            + 16
            + crate::document::MAX_DOCUMENT_INDEX_NAME_BYTES as u64;
        let before = engine.inner.database.storage.document_catalog().unwrap();
        let rejected = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    identity,
                    RequestContext::new()
                        .with_result_limits(ResultLimits::new(1, required - 1).unwrap()),
                    command(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(rejected.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            engine.inner.database.storage.document_catalog().unwrap(),
            before
        );
        let token = CancellationToken::new();
        token.cancel();
        assert!(
            engine
                .execute_document(
                    &session,
                    DocumentRequest::new(
                        identity,
                        RequestContext::new().with_cancellation_token(token),
                        command()
                    )
                )
                .await
                .is_err()
        );
        assert_eq!(
            engine.inner.database.storage.document_catalog().unwrap(),
            before
        );
        let result = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    identity,
                    RequestContext::new()
                        .with_result_limits(ResultLimits::new(1, required).unwrap()),
                    command(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            result.result(),
            &DocumentResult::IndexModelsBuilt {
                names: vec!["value_1".into()].into_boxed_slice(),
                before: 1,
                after: 2
            }
        );
        engine.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_drop_recovers_after_cancel_deadline_and_task_abort() {
        use crate::document::DocumentDropDatabaseRequest;
        use rusqlite::{Connection, TransactionBehavior};

        for mode in 0..3 {
            let root = tempfile::tempdir().unwrap();
            let engine = Engine::open(root.path(), 2).await.unwrap();
            let session = Arc::new(engine.session());
            let identity = DocumentRequestId::new([9; 16]).unwrap();
            for database in ["drop_me", "keep_me"] {
                engine
                    .execute_document(
                        &session,
                        DocumentRequest::new(
                            identity,
                            RequestContext::new(),
                            DocumentCommand::CreateCollection(
                                DocumentCreateCollectionRequest::new(
                                    DocumentNamespace::new(database, "items").unwrap(),
                                    DocumentCollectionOptions::empty(),
                                    DocumentWriteOptions::new(),
                                ),
                            ),
                        ),
                    )
                    .await
                    .unwrap();
            }
            let mut blocker = Connection::open(root.path().join("shards/0000.sqlite")).unwrap();
            let transaction = blocker
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            let token = CancellationToken::new();
            let mut context = RequestContext::new().with_cancellation_token(token.clone());
            if mode == 1 {
                context = context.with_timeout(Duration::from_secs(2)).unwrap();
            }
            let task_engine = engine.clone();
            let task_session = Arc::clone(&session);
            let task = tokio::spawn(async move {
                task_engine
                    .execute_document(
                        &task_session,
                        DocumentRequest::new(
                            identity,
                            context,
                            DocumentCommand::DropDatabase(
                                DocumentDropDatabaseRequest::new(
                                    "drop_me",
                                    DocumentWriteOptions::new(),
                                )
                                .unwrap(),
                            ),
                        ),
                    )
                    .await
            });
            let manifest = Connection::open(root.path().join("manifest.sqlite")).unwrap();
            timeout(Duration::from_secs(1), async {
                loop {
                    let pending: i64 = manifest
                        .query_row(
                            "SELECT COUNT(*) FROM briskdb_document_deletion",
                            [],
                            |row| row.get(0),
                        )
                        .unwrap();
                    if pending == 1 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("drop intent must be durable before interruption");
            match mode {
                0 => {
                    token.cancel();
                }
                2 => task.abort(),
                _ => (),
            }
            let result = timeout(Duration::from_secs(3), task).await.unwrap();
            if mode == 2 {
                assert!(result.unwrap_err().is_cancelled());
            } else {
                assert_eq!(
                    result.unwrap().unwrap_err().kind(),
                    if mode == 0 {
                        EngineErrorKind::Cancelled
                    } else {
                        EngineErrorKind::DeadlineExceeded
                    }
                );
            }
            // Wait for a detached worker to release its session and migration guard.
            drop(
                timeout(Duration::from_secs(2), session.inner.lock())
                    .await
                    .unwrap(),
            );
            assert_eq!(
                engine
                    .inner
                    .database
                    .storage
                    .enter_schema_operation()
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition
            );
            let pending: i64 = manifest
                .query_row(
                    "SELECT COUNT(*) FROM briskdb_document_deletion",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(pending, 1);
            transaction.rollback().unwrap();
            drop(blocker);
            drop(manifest);
            engine.shutdown().await.unwrap();
            drop(session);
            drop(engine);
            let reopened = Engine::open(root.path(), 2).await.unwrap();
            let storage = &reopened.inner.database.storage;
            assert!(
                storage
                    .document_collection_controlled("drop_me", "items", OperationControl::new(None))
                    .unwrap()
                    .is_none()
            );
            assert!(
                storage
                    .document_collection_controlled("keep_me", "items", OperationControl::new(None))
                    .unwrap()
                    .is_some()
            );
            reopened.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn distinct_releases_admission_on_cancel_deadline_and_task_abort() {
        use crate::document::DocumentDistinctRequest;
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open_with_options(root.path(), 2, EngineOptions::new(1, 1).unwrap())
            .await
            .unwrap();
        let session = Arc::new(engine.session());
        let namespace = DocumentNamespace::new("app", "distinct_cancel").unwrap();
        let identity = DocumentRequestId::new([8; 16]).unwrap();
        engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    identity,
                    RequestContext::new(),
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        namespace.clone(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        for mode in 0..3 {
            let permit = engine
                .inner
                .connections
                .acquire_for_owner(0, ConnectionOwner::new(engine.session().id().get()))
                .await
                .unwrap();
            let token = CancellationToken::new();
            let mut context = RequestContext::new().with_cancellation_token(token.clone());
            if mode == 1 {
                context = context.with_timeout(Duration::from_secs(2)).unwrap();
            }
            let request = DocumentRequest::new(
                identity,
                context,
                DocumentCommand::Distinct(
                    DocumentDistinctRequest::new(
                        namespace.clone(),
                        "v",
                        DocumentFilter::empty(),
                        DocumentReadOptions::new(),
                    )
                    .unwrap(),
                ),
            );
            let task_engine = engine.clone();
            let task_session = Arc::clone(&session);
            let task =
                tokio::spawn(
                    async move { task_engine.execute_document(&task_session, request).await },
                );
            timeout(Duration::from_secs(1), async {
                while engine.inner.connections.snapshot().unwrap().shards[0].queued != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("distinct must reach pool admission");
            match mode {
                0 => {
                    token.cancel();
                }
                2 => task.abort(),
                _ => (),
            }
            let result = timeout(Duration::from_secs(3), task).await.unwrap();
            if mode == 2 {
                assert!(result.unwrap_err().is_cancelled());
            } else {
                assert_eq!(
                    result.unwrap().unwrap_err().kind(),
                    if mode == 0 {
                        EngineErrorKind::Cancelled
                    } else {
                        EngineErrorKind::DeadlineExceeded
                    }
                );
            }
            let session_guard = timeout(Duration::from_secs(2), session.inner.lock())
                .await
                .unwrap();
            assert_eq!(
                engine.inner.connections.snapshot().unwrap().shards[0].queued,
                0
            );
            drop(session_guard);
            drop(permit);
            let result = engine
                .execute_document(
                    &session,
                    DocumentRequest::new(
                        identity,
                        RequestContext::new(),
                        DocumentCommand::Distinct(
                            DocumentDistinctRequest::new(
                                namespace.clone(),
                                "v",
                                DocumentFilter::empty(),
                                DocumentReadOptions::new(),
                            )
                            .unwrap(),
                        ),
                    ),
                )
                .await
                .unwrap();
            assert!(
                matches!(result.result(), DocumentResult::Distinct(values) if values.is_empty())
            );
        }
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_continuations_release_state_on_cancel_deadline_and_task_abort() {
        use crate::document::{DocumentContinueCursorRequest, DocumentFindRequest};

        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open_with_options(root.path(), 2, EngineOptions::new(1, 1).unwrap())
            .await
            .unwrap();
        let session = Arc::new(engine.session());
        let namespace = DocumentNamespace::new("app", "cursor_cancel").unwrap();
        let identity = DocumentRequestId::new([9; 16]).unwrap();
        engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    identity,
                    RequestContext::new(),
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        namespace.clone(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();

        for mode in 0..15 {
            let mut options = DocumentReadOptions::new().with_batch_size(0).unwrap();
            if (3..6).contains(&mode) {
                options = options.with_sort(
                    crate::document::DocumentSort::new(
                        BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap(),
                    )
                    .unwrap(),
                );
            }
            let command = if mode >= 6 {
                let stages = if mode >= 12 {
                    vec![
                        BsonDocument::from_entries([(
                            "$group",
                            BsonValue::Document(
                                BsonDocument::from_entries([
                                    ("_id", BsonValue::Null),
                                    (
                                        "n",
                                        BsonValue::Document(
                                            BsonDocument::from_entries([(
                                                "$sum",
                                                BsonValue::Int32(1),
                                            )])
                                            .unwrap(),
                                        ),
                                    ),
                                ])
                                .unwrap(),
                            ),
                        )])
                        .unwrap(),
                    ]
                } else if mode >= 9 {
                    vec![
                        BsonDocument::from_entries([(
                            "$sort",
                            BsonValue::Document(
                                BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap(),
                            ),
                        )])
                        .unwrap(),
                    ]
                } else {
                    Vec::new()
                };
                DocumentCommand::Aggregate(
                    crate::document::DocumentAggregateRequest::new(
                        namespace.clone(),
                        crate::document::DocumentPipeline::new(stages).unwrap(),
                        options,
                    )
                    .unwrap(),
                )
            } else {
                DocumentCommand::Find(DocumentFindRequest::new(
                    namespace.clone(),
                    DocumentFilter::empty(),
                    options,
                ))
            };
            let opened = engine
                .execute_document(
                    &session,
                    DocumentRequest::new(identity, RequestContext::new(), command),
                )
                .await
                .unwrap();
            let (_, id) = execution_cursor(&opened).unwrap();
            // Reserve the only shard-0 permit without occupying a worker. The
            // continuation can check out its cursor and read catalog metadata,
            // then blocks at an observable, deterministic admission boundary.
            let permit = engine
                .inner
                .connections
                .acquire_for_owner(0, ConnectionOwner::new(engine.session().id().get()))
                .await
                .unwrap();
            let token = CancellationToken::new();
            let mut context = RequestContext::new().with_cancellation_token(token.clone());
            if mode % 3 == 1 {
                context = context.with_timeout(Duration::from_secs(2)).unwrap();
            }
            let request = DocumentRequest::new(
                identity,
                context,
                DocumentCommand::ContinueCursor(DocumentContinueCursorRequest::new(
                    namespace.clone(),
                    id,
                    DocumentReadOptions::new(),
                )),
            );
            let task_engine = engine.clone();
            let task_session = Arc::clone(&session);
            let task =
                tokio::spawn(
                    async move { task_engine.execute_document(&task_session, request).await },
                );
            timeout(Duration::from_secs(1), async {
                while engine.inner.connections.snapshot().unwrap().shards[0].queued != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("continuation must reach pool admission");
            match mode % 3 {
                0 => {
                    token.cancel();
                }
                2 => task.abort(),
                _ => (),
            }
            let result = timeout(Duration::from_secs(3), task).await.unwrap();
            if mode % 3 == 2 {
                assert!(result.unwrap_err().is_cancelled());
            } else {
                assert_eq!(
                    result.unwrap().unwrap_err().kind(),
                    if mode % 3 == 0 {
                        EngineErrorKind::Cancelled
                    } else {
                        EngineErrorKind::DeadlineExceeded
                    }
                );
            }
            // Acquiring session state waits for any detached cancellation
            // cleanup to finish before observing the registry.
            let _session = timeout(Duration::from_secs(2), session.inner.lock())
                .await
                .unwrap();
            assert!(!engine.inner.document_cursors.kill(
                ConnectionOwner::new(session.id().get()),
                &namespace,
                id
            ));
            assert_eq!(
                engine.inner.connections.snapshot().unwrap().shards[0].queued,
                0
            );
            drop(permit);
        }
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn collection_creation_excludes_schema_operations_before_waiting_for_its_session() {
        lifecycle_excludes_schema_operations_before_waiting_for_its_session(false).await;
    }

    #[tokio::test]
    async fn collection_drop_excludes_schema_operations_before_waiting_for_its_session() {
        lifecycle_excludes_schema_operations_before_waiting_for_its_session(true).await;
    }

    async fn lifecycle_excludes_schema_operations_before_waiting_for_its_session(drop: bool) {
        let temp = tempfile::tempdir().unwrap();
        let engine = Engine::open_with_options(temp.path(), 2, EngineOptions::new(1, 1).unwrap())
            .await
            .unwrap();
        let first_holder_session = Arc::new(engine.session());
        let second_holder_session = Arc::new(engine.session());
        let create_session = Arc::new(engine.session());
        if drop {
            engine
                .execute_document(
                    &create_session,
                    DocumentRequest::new(
                        DocumentRequestId::new([1; 16]).unwrap(),
                        RequestContext::new(),
                        DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                            DocumentNamespace::new("app", "events").unwrap(),
                            DocumentCollectionOptions::empty(),
                            DocumentWriteOptions::new(),
                        )),
                    ),
                )
                .await
                .unwrap();
        }
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
            let command = if drop {
                DocumentCommand::DropCollection(
                    crate::document::DocumentDropCollectionRequest::new(
                        namespace,
                        DocumentWriteOptions::new(),
                    ),
                )
            } else {
                DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                    namespace,
                    DocumentCollectionOptions::empty(),
                    DocumentWriteOptions::new(),
                ))
            };
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
        if drop {
            assert_eq!(created.result(), &DocumentResult::NamespaceDropped(true));
        } else {
            assert!(matches!(created.result(), DocumentResult::Collection(_)));
        }
        engine.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn collection_creation_rejects_its_own_transaction_before_schema_quiescence() {
        lifecycle_rejects_its_own_transaction_before_schema_quiescence(0).await;
    }

    #[tokio::test]
    async fn database_drop_rejects_its_own_transaction_before_schema_quiescence() {
        lifecycle_rejects_its_own_transaction_before_schema_quiescence(1).await;
    }

    #[tokio::test]
    async fn index_batch_rejects_its_own_transaction_before_schema_quiescence() {
        lifecycle_rejects_its_own_transaction_before_schema_quiescence(2).await;
    }

    #[tokio::test]
    async fn index_removal_rejects_its_own_transaction_before_schema_quiescence() {
        lifecycle_rejects_its_own_transaction_before_schema_quiescence(3).await;
    }

    async fn lifecycle_rejects_its_own_transaction_before_schema_quiescence(mode: u8) {
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

        let command = if mode == 1 {
            DocumentCommand::DropDatabase(
                crate::document::DocumentDropDatabaseRequest::new(
                    "app",
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            )
        } else if mode == 3 {
            DocumentCommand::DropIndexes(crate::document::DocumentDropIndexesRequest::all(
                DocumentNamespace::new("app", "events").unwrap(),
                DocumentWriteOptions::new(),
            ))
        } else if mode == 2 {
            DocumentCommand::CreateIndexes(
                crate::document::DocumentCreateIndexesRequest::new(
                    DocumentNamespace::new("app", "events").unwrap(),
                    vec![
                        crate::document::DocumentIndexRequest::new(
                            BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                        )
                        .unwrap(),
                    ],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            )
        } else {
            DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                DocumentNamespace::new("app", "events").unwrap(),
                DocumentCollectionOptions::empty(),
                DocumentWriteOptions::new(),
            ))
        };
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
