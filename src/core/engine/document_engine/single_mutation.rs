//! Atomic shard-local single-record mutations with preflighted results.

mod upsert;

use super::*;
use crate::{
    document::{
        BSON_MAX_NESTING_DEPTH, BsonCodecOptions, CanonicalBsonKey, DEFAULT_DOCUMENT_BATCH_SIZE,
        DocumentFindOneAndDeleteRequest, DocumentFindOneAndReplaceRequest,
        DocumentFindOneAndUpdateRequest, DocumentMutationError, DocumentReplaceRequest,
        DocumentRequestId, DocumentSortKey, DocumentUpdateRequest, DocumentUpdateResult,
        DocumentUpdater, encode_document_with_options,
    },
    sqlite_error,
};
use rusqlite::{Connection, Transaction, TransactionBehavior};

#[derive(PartialEq, Eq)]
struct Candidate {
    shard: u16,
    natural_order: u64,
    key: CanonicalBsonKey,
    sort: Option<DocumentSortKey>,
}

#[derive(Clone)]
enum Mutation {
    Delete,
    Update {
        updater: Arc<DocumentUpdater>,
        max_document_bytes: usize,
        returns: MutationReturn,
    },
    Replace {
        document: Arc<BsonDocument>,
        max_document_bytes: usize,
        returns: MutationReturn,
    },
}

#[derive(Clone, Copy)]
pub(super) enum MutationReturn {
    Counts,
    Before,
    After,
}

impl MutationReturn {
    fn no_match(self) -> DocumentResult {
        match self {
            Self::Counts => DocumentResult::Update(
                DocumentUpdateResult::new(0, 0, None).expect("zero update counts"),
            ),
            Self::Before | Self::After => DocumentResult::Document(None),
        }
    }
}

impl Mutation {
    fn no_match(&self) -> DocumentResult {
        match self {
            Self::Delete => DocumentResult::Document(None),
            Self::Update { returns, .. } | Self::Replace { returns, .. } => returns.no_match(),
        }
    }
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_find_update(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentFindOneAndUpdateRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (update, read_options, return_after) = request.into_parts();
        require_single_mutation_read_options(&read_options)?;
        let max_document_bytes = update.max_document_bytes();
        let (namespace, filter, update, scope, write_options) = update.into_parts();
        if scope != DocumentMutationScope::One {
            return Err(unsupported(
                "find-one-and-update requires single-document scope",
            ));
        }
        require_replacement_options(write_options)?;
        let updater = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    DocumentUpdater::compile_with_check(update.document(), &mut || {
                        ensure_document_cpu_active(cancellation, &control)
                    })
                },
            )
            .await?;
        self.run_document_single_mutation(
            owner,
            request_id,
            namespace,
            Arc::new(filter),
            read_options,
            Mutation::Update {
                updater: Arc::new(updater),
                max_document_bytes,
                returns: if return_after {
                    MutationReturn::After
                } else {
                    MutationReturn::Before
                },
            },
            cancellation,
            deadline,
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_update(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentUpdateRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let max_document_bytes = request.max_document_bytes();
        let (namespace, filter, update, scope, options) = request.into_parts();
        require_replacement_options(options.with_upsert(false))?;
        let updater = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    DocumentUpdater::compile_with_check(update.document(), &mut || {
                        ensure_document_cpu_active(cancellation, &control)
                    })
                },
            )
            .await?;
        let updater = Arc::new(updater);
        if options.upsert() {
            return self
                .run_document_upsert(
                    owner,
                    request_id,
                    namespace,
                    Arc::new(filter),
                    Mutation::Update {
                        updater,
                        max_document_bytes,
                        returns: MutationReturn::Counts,
                    },
                    scope,
                    cancellation,
                    deadline,
                    limits,
                )
                .await;
        }
        if scope == DocumentMutationScope::Many {
            return self
                .run_document_update_many(
                    owner,
                    request_id,
                    namespace,
                    Arc::new(filter),
                    updater,
                    max_document_bytes,
                    cancellation,
                    deadline,
                    limits,
                )
                .await;
        }
        self.run_document_single_mutation(
            owner,
            request_id,
            namespace,
            Arc::new(filter),
            DocumentReadOptions::new(),
            Mutation::Update {
                updater,
                max_document_bytes,
                returns: MutationReturn::Counts,
            },
            cancellation,
            deadline,
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_find_delete(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentFindOneAndDeleteRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, filter, options) = request.into_parts();
        require_single_mutation_read_options(&options)?;
        self.run_document_single_mutation(
            owner,
            request_id,
            namespace,
            Arc::new(filter),
            options,
            Mutation::Delete,
            cancellation,
            deadline,
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_replace(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentReplaceRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        if request.write_options().upsert() {
            return self
                .run_document_replacement_upsert(
                    owner,
                    request_id,
                    request,
                    cancellation,
                    deadline,
                    limits,
                )
                .await;
        }
        let max_document_bytes = request.max_document_bytes();
        let (namespace, filter, replacement, options) = request.into_parts();
        require_replacement_options(options)?;
        self.run_document_single_mutation(
            owner,
            request_id,
            namespace,
            Arc::new(filter),
            DocumentReadOptions::new(),
            Mutation::Replace {
                document: Arc::new(replacement),
                max_document_bytes,
                returns: MutationReturn::Counts,
            },
            cancellation,
            deadline,
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_find_replace(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentFindOneAndReplaceRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (replacement, read_options, return_after) = request.into_parts();
        require_single_mutation_read_options(&read_options)?;
        let max_document_bytes = replacement.max_document_bytes();
        let (namespace, filter, document, write_options) = replacement.into_parts();
        require_replacement_options(write_options)?;
        self.run_document_single_mutation(
            owner,
            request_id,
            namespace,
            Arc::new(filter),
            read_options,
            Mutation::Replace {
                document: Arc::new(document),
                max_document_bytes,
                returns: if return_after {
                    MutationReturn::After
                } else {
                    MutationReturn::Before
                },
            },
            cancellation,
            deadline,
            limits,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_document_single_mutation(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        namespace: DocumentNamespace,
        filter: Arc<DocumentFilter>,
        options: DocumentReadOptions,
        mutation: Mutation,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let storage = self.inner.database.storage.clone();
        let shard_count = self.shard_count();
        let no_match = mutation.no_match();
        let (collection_id, route, projection, sorter, plan) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let mut check = || ensure_document_cpu_active(cancellation, &control);
                    let route = prepare_filter_route(&storage, &filter, cancellation, &control)?;
                    let projection = options
                        .projection()
                        .map(|spec| {
                            DocumentProjector::compile_with_check(spec.document(), &mut check)
                                .map(Arc::new)
                        })
                        .transpose()?;
                    let sorter = options
                        .sort()
                        .filter(|spec| !spec.document().is_empty())
                        .map(|spec| {
                            DocumentSorter::compile_with_check(spec.document(), &mut check)
                                .map(Arc::new)
                        })
                        .transpose()?;
                    let collection_id =
                        require_collection(storage.document_collection_controlled(
                            namespace.database(),
                            namespace.collection(),
                            Arc::clone(&control),
                        )?)?
                        .id();
                    let plan = match &route {
                        PreparedFilterRoute::Point { id_key, shard } => DocumentPlan::Point(
                            DocumentPointPlan::new(collection_id, *shard, id_key.clone())?,
                        ),
                        PreparedFilterRoute::Scatter(_) => {
                            scatter_plan(collection_id, shard_count)?
                        }
                    };
                    enforce_execution_result_limits_with_check(
                        &DocumentExecution::new(request_id, Some(plan.clone()), no_match),
                        limits,
                        &mut check,
                    )?;
                    check()?;
                    Ok((collection_id, route, projection, sorter, plan))
                },
            )
            .await?;
        match route {
            PreparedFilterRoute::Point { id_key, shard } => {
                self.run_document_shard_controlled(
                    shard,
                    owner,
                    cancellation,
                    deadline,
                    move |storage, connection, cancellation, control| {
                        let transaction =
                            Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                                .map_err(sqlite_error::statement)?;
                        let record = storage.get_document_on_connection(
                            &transaction,
                            collection_id,
                            shard,
                            &id_key,
                            cancellation,
                        )?;
                        // Even a one-row point route must validate runtime
                        // sort semantics (for example parallel arrays), just
                        // like ordinary find, before deleting anything.
                        if let (Some(sorter), Some(record)) = (sorter.as_deref(), record.as_ref()) {
                            sorter.key_validated_with_check(record.document(), &mut || {
                                ensure_document_cpu_active(cancellation, control)
                            })?;
                        }
                        let execution = mutate_record(
                            &mutation,
                            storage,
                            &transaction,
                            record,
                            request_id,
                            plan,
                            projection.as_deref(),
                            limits,
                            cancellation,
                            control,
                        )?;
                        ensure_document_cpu_active(cancellation, control)?;
                        transaction.commit().map_err(sqlite_error::statement)?;
                        Ok(execution)
                    },
                )
                .await
            }
            PreparedFilterRoute::Scatter(matcher) => {
                loop {
                    let mut best = None;
                    for shard in 0..shard_count {
                        let matcher = matcher.clone();
                        let sorter = sorter.clone();
                        // Comparing potentially large BSON sort keys stays on
                        // blocking workers, never the async coordinator.
                        best = self
                            .run_document_shard_controlled(
                                shard,
                                owner,
                                cancellation.clone(),
                                deadline,
                                move |storage, connection, cancellation, control| {
                                    select_candidate(
                                        storage,
                                        connection,
                                        collection_id,
                                        shard,
                                        matcher.as_deref(),
                                        sorter.as_deref(),
                                        best,
                                        cancellation,
                                        control,
                                    )
                                },
                            )
                            .await?;
                    }
                    let Some(candidate) = best else {
                        return Ok(DocumentExecution::new(
                            request_id,
                            Some(plan),
                            mutation.no_match(),
                        ));
                    };
                    let shard = candidate.shard;
                    let matcher = matcher.clone();
                    let sorter = sorter.clone();
                    let projection = projection.clone();
                    let plan = plan.clone();
                    let mutation = mutation.clone();
                    let execution = self
                        .run_document_shard_controlled(
                            shard,
                            owner,
                            cancellation.clone(),
                            deadline,
                            move |storage, connection, cancellation, control| {
                                let transaction = Transaction::new_unchecked(
                                    connection,
                                    TransactionBehavior::Immediate,
                                )
                                .map_err(sqlite_error::statement)?;
                                // Reselect, not merely reread: another row may have
                                // become the best match on this shard meanwhile.
                                let current = select_candidate(
                                    storage,
                                    &transaction,
                                    collection_id,
                                    shard,
                                    matcher.as_deref(),
                                    sorter.as_deref(),
                                    None,
                                    cancellation,
                                    control,
                                )?;
                                if current.as_ref() != Some(&candidate) {
                                    transaction.rollback().map_err(sqlite_error::statement)?;
                                    return Ok(None);
                                }
                                let record = storage.get_document_on_connection(
                                    &transaction,
                                    collection_id,
                                    shard,
                                    &candidate.key,
                                    cancellation,
                                )?;
                                let execution = mutate_record(
                                    &mutation,
                                    storage,
                                    &transaction,
                                    record,
                                    request_id,
                                    plan,
                                    projection.as_deref(),
                                    limits,
                                    cancellation,
                                    control,
                                )?;
                                ensure_document_cpu_active(cancellation, control)?;
                                transaction.commit().map_err(sqlite_error::statement)?;
                                Ok(Some(execution))
                            },
                        )
                        .await?;
                    if let Some(execution) = execution {
                        return Ok(execution);
                    }
                }
            }
        }
    }
}

fn require_single_mutation_read_options(options: &DocumentReadOptions) -> EngineResult<()> {
    if options.skip() != 0
        || options.limit().is_some()
        || options.batch_size() != DEFAULT_DOCUMENT_BATCH_SIZE
        || options.batch_byte_limit().is_some()
    {
        return Err(unsupported(
            "single-record mutations accept only projection and sort read options",
        ));
    }
    Ok(())
}

fn require_replacement_options(options: DocumentWriteOptions) -> EngineResult<()> {
    if !options.ordered() || options.upsert() || options.bypass_document_validation() {
        return Err(unsupported(
            "replacement upsert and non-default write options are not implemented",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn mutate_record(
    mutation: &Mutation,
    storage: &Storage,
    transaction: &Transaction<'_>,
    record: Option<DocumentStorageRecord>,
    request_id: DocumentRequestId,
    plan: DocumentPlan,
    projection: Option<&DocumentProjector>,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentExecution> {
    match mutation {
        Mutation::Update {
            updater,
            max_document_bytes,
            returns,
        } => {
            let Some(record) = record else {
                return Ok(DocumentExecution::new(
                    request_id,
                    Some(plan),
                    mutation.no_match(),
                ));
            };
            update_record(
                storage,
                transaction,
                record,
                updater,
                *max_document_bytes,
                *returns,
                projection,
                request_id,
                plan,
                limits,
                cancellation,
                control,
            )
        }
        Mutation::Delete => return_and_delete(
            storage,
            transaction,
            record,
            request_id,
            plan,
            projection,
            limits,
            cancellation,
            control,
        ),
        Mutation::Replace {
            document,
            max_document_bytes,
            returns,
        } => replace_record(
            storage,
            transaction,
            record,
            document,
            *max_document_bytes,
            *returns,
            projection,
            request_id,
            plan,
            limits,
            cancellation,
            control,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn update_record(
    storage: &Storage,
    transaction: &Transaction<'_>,
    record: DocumentStorageRecord,
    updater: &DocumentUpdater,
    max_document_bytes: usize,
    returns: MutationReturn,
    projection: Option<&DocumentProjector>,
    request_id: DocumentRequestId,
    plan: DocumentPlan,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentExecution> {
    let (post_image, force_modified) = updater.apply_for_write(record.document(), &mut || {
        ensure_document_cpu_active(cancellation, control)
    })?;
    write_post_image(
        storage,
        transaction,
        record,
        post_image,
        force_modified,
        max_document_bytes,
        returns,
        projection,
        request_id,
        plan,
        limits,
        cancellation,
        control,
    )
}

#[allow(clippy::too_many_arguments)]
fn replace_record(
    storage: &Storage,
    transaction: &Transaction<'_>,
    record: Option<DocumentStorageRecord>,
    replacement: &BsonDocument,
    max_document_bytes: usize,
    returns: MutationReturn,
    projection: Option<&DocumentProjector>,
    request_id: DocumentRequestId,
    plan: DocumentPlan,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentExecution> {
    let check = || ensure_document_cpu_active(cancellation, control);
    check()?;
    let Some(record) = record else {
        return Ok(DocumentExecution::new(
            request_id,
            Some(plan),
            returns.no_match(),
        ));
    };
    if let Some(id) = replacement.get_first("_id") {
        let key = CanonicalBsonKey::encode(id)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        if &key != record.id_key() {
            return Err(DocumentMutationError::ImmutableId.into_engine_error());
        }
    }
    check()?;
    // Canonical field order and the original ID representation survive even
    // when the caller supplied a numerically equal ID of another BSON type.
    let mut post_image = BsonDocument::new();
    post_image
        .try_reserve(replacement.len() + 1)
        .map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::OutOfMemory,
                "unable to prepare replacement",
                error,
            )
        })?;
    post_image
        .push(
            "_id",
            record
                .document()
                .get_first("_id")
                .expect("validated stored ID")
                .clone(),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    for (name, value) in replacement.iter().filter(|(name, _)| *name != "_id") {
        check()?;
        let value = match value {
            BsonValue::Timestamp(value) if value.time() == 0 && value.increment() == 0 => {
                BsonValue::Timestamp(next_server_timestamp()?)
            }
            value => value.clone(),
        };
        post_image
            .push(name, value)
            .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    }
    write_post_image(
        storage,
        transaction,
        record,
        post_image,
        false,
        max_document_bytes,
        returns,
        projection,
        request_id,
        plan,
        limits,
        cancellation,
        control,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_post_image(
    storage: &Storage,
    transaction: &Transaction<'_>,
    record: DocumentStorageRecord,
    post_image: BsonDocument,
    force_modified: bool,
    max_document_bytes: usize,
    returns: MutationReturn,
    projection: Option<&DocumentProjector>,
    request_id: DocumentRequestId,
    plan: DocumentPlan,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentExecution> {
    let mut check = || ensure_document_cpu_active(cancellation, control);
    check()?;
    let bytes = encode_document_with_options(
        &post_image,
        &BsonCodecOptions::new().with_max_document_bytes(max_document_bytes),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    check()?;
    // Query equality intentionally merges numeric types. Modification counts
    // instead compare the bytes that storage persists, including field order.
    let before = encode_document(record.document())
        .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))?;
    // NaN arithmetic is executed even when its resulting encoding is identical.
    let modified = force_modified || bytes != before;
    drop(bytes);
    drop(before);
    // Prepare the write before consuming either image for projection. Retain
    // its identity separately so returning the pre-image needs no extra clone.
    let prepared = if modified {
        Some(storage.prepare_document_write(&post_image)?)
    } else {
        None
    };
    let identity = (
        record.collection_id(),
        record.shard(),
        record.id_key().clone(),
        record.natural_order(),
    );
    let result = match returns {
        MutationReturn::Counts => {
            DocumentResult::Update(DocumentUpdateResult::new(1, u64::from(modified), None)?)
        }
        MutationReturn::Before => DocumentResult::Document(Some(project_return_document(
            record.into_document(),
            projection,
            &mut check,
        )?)),
        MutationReturn::After => DocumentResult::Document(Some(project_return_document(
            post_image, projection, &mut check,
        )?)),
    };
    let execution = DocumentExecution::new(request_id, Some(plan), result);
    enforce_execution_result_limits_with_check(&execution, limits, &mut check)?;
    check()?;
    if let Some(prepared) = prepared {
        if !storage.replace_document_on_connection(
            transaction,
            identity.0,
            identity.1,
            &identity.2,
            identity.3,
            &prepared,
            cancellation,
        )? {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "selected document vanished inside its write transaction",
            ));
        }
    }
    Ok(execution)
}

#[allow(clippy::too_many_arguments)]
fn select_candidate(
    storage: &Storage,
    connection: &Connection,
    collection_id: DocumentCollectionId,
    shard: u16,
    matcher: Option<&DocumentMatcher>,
    sorter: Option<&DocumentSorter>,
    mut best: Option<Candidate>,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<Option<Candidate>> {
    let mut after = None;
    while let Some(record) = deletion::next_match(
        storage,
        connection,
        collection_id,
        shard,
        &mut after,
        matcher,
        cancellation,
        control,
    )? {
        let mut check = || ensure_document_cpu_active(cancellation, control);
        let sort = sorter
            .map(|sorter| sorter.key_validated_with_check(record.document(), &mut check))
            .transpose()?;
        check()?;
        if best.as_ref().is_none_or(|best| {
            (sort.as_ref(), record.natural_order()) < (best.sort.as_ref(), best.natural_order)
        }) {
            best = Some(Candidate {
                shard,
                natural_order: record.natural_order(),
                key: record.id_key().clone(),
                sort,
            });
        }
        check()?;
        if sorter.is_none() {
            break;
        }
    }
    Ok(best)
}

#[allow(clippy::too_many_arguments)]
fn return_and_delete(
    storage: &Storage,
    transaction: &Transaction<'_>,
    record: Option<DocumentStorageRecord>,
    request_id: DocumentRequestId,
    plan: DocumentPlan,
    projection: Option<&DocumentProjector>,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<DocumentExecution> {
    let mut check = || ensure_document_cpu_active(cancellation, control);
    check()?;
    let identity = record.as_ref().map(|record| {
        (
            record.collection_id(),
            record.shard(),
            record.id_key().clone(),
        )
    });
    let document = record
        .map(|record| project_return_document(record.into_document(), projection, &mut check))
        .transpose()?;
    let execution =
        DocumentExecution::new(request_id, Some(plan), DocumentResult::Document(document));
    // Validate the exact returned pre-image before the first durable write.
    enforce_execution_result_limits_with_check(&execution, limits, &mut check)?;
    check()?;
    if let Some((collection_id, shard, key)) = identity {
        if !storage.delete_document_on_connection(
            transaction,
            collection_id,
            shard,
            &key,
            cancellation,
        )? {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "selected document vanished inside its write transaction",
            ));
        }
    }
    Ok(execution)
}

fn project_return_document(
    document: BsonDocument,
    projection: Option<&DocumentProjector>,
    check: &mut impl FnMut() -> EngineResult<()>,
) -> EngineResult<BsonDocument> {
    let document = match projection {
        Some(projection) => projection.project_owned_validated_with_check(document, check)?,
        None => document,
    };
    // The returned image is nested once in its result envelope. Reject a
    // depth overflow before mutation, without classifying it as corruption.
    encode_document_with_options(
        &document,
        &BsonCodecOptions::new().with_max_nesting_depth(BSON_MAX_NESTING_DEPTH - 1),
    )
    .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
    check()?;
    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::RequestContext, document::DocumentCreateCollectionRequest};

    #[tokio::test]
    async fn reselection_observes_a_better_row_and_output_failure_keeps_both_rows() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = engine.session();
        let request_id = DocumentRequestId::new([1; 16]).unwrap();
        let execution = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    request_id,
                    RequestContext::new(),
                    DocumentCommand::CreateCollection(DocumentCreateCollectionRequest::new(
                        DocumentNamespace::new("app", "items").unwrap(),
                        DocumentCollectionOptions::empty(),
                        DocumentWriteOptions::new(),
                    )),
                ),
            )
            .await
            .unwrap();
        let DocumentResult::Collection(collection) = execution.result() else {
            panic!("collection")
        };
        let collection_id = collection.id();
        engine
            .run_document_shard_controlled(
                0,
                ConnectionOwner::new(session.id().get()),
                CancellationToken::new(),
                None,
                move |storage, connection, cancellation, control| {
                    let ids: Vec<_> = (0..100)
                        .filter(|id| {
                            storage
                                .prepare_document_id(&BsonValue::Int32(*id))
                                .unwrap()
                                .1
                                == 0
                        })
                        .take(2)
                        .collect();
                    assert_eq!(ids.len(), 2);
                    let row = |id, rank| {
                        BsonDocument::from_entries([
                            ("_id", BsonValue::Int32(id)),
                            ("rank", BsonValue::Int32(rank)),
                        ])
                        .unwrap()
                    };
                    let first = storage.prepare_document_write(&row(ids[0], 0))?;
                    let second = storage.prepare_document_write(&row(ids[1], 1))?;
                    let transaction =
                        Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                            .map_err(sqlite_error::statement)?;
                    storage.insert_prepared_document_on_connection(
                        &transaction,
                        collection_id,
                        1,
                        0,
                        &first,
                        cancellation,
                    )?;
                    storage.insert_prepared_document_on_connection(
                        &transaction,
                        collection_id,
                        2,
                        0,
                        &second,
                        cancellation,
                    )?;
                    let sorter = DocumentSorter::compile(
                        &BsonDocument::from_entries([("rank", BsonValue::Int32(1))]).unwrap(),
                    )?;
                    let selected = select_candidate(
                        storage,
                        &transaction,
                        collection_id,
                        0,
                        None,
                        Some(&sorter),
                        None,
                        cancellation,
                        control,
                    )?
                    .unwrap();
                    assert_eq!(selected.key, *first.id_key());
                    let better = storage.prepare_document_write(&row(ids[1], -1))?;
                    assert!(storage.replace_document_on_connection(
                        &transaction,
                        collection_id,
                        0,
                        second.id_key(),
                        2,
                        &better,
                        cancellation
                    )?);
                    let current = select_candidate(
                        storage,
                        &transaction,
                        collection_id,
                        0,
                        None,
                        Some(&sorter),
                        None,
                        cancellation,
                        control,
                    )?
                    .unwrap();
                    assert!(
                        selected != current,
                        "rechecking only the old row would miss a new shard-local winner"
                    );
                    assert_eq!(current.key, *second.id_key());
                    let record = storage.get_document_on_connection(
                        &transaction,
                        collection_id,
                        0,
                        &current.key,
                        cancellation,
                    )?;
                    let plan = scatter_plan(collection_id, 2)?;
                    assert_eq!(
                        return_and_delete(
                            storage,
                            &transaction,
                            record,
                            request_id,
                            plan,
                            None,
                            ResultLimits::new(1, 1).unwrap(),
                            cancellation,
                            control
                        )
                        .unwrap_err()
                        .kind(),
                        EngineErrorKind::LimitExceeded
                    );
                    assert!(
                        storage
                            .get_document_on_connection(
                                &transaction,
                                collection_id,
                                0,
                                first.id_key(),
                                cancellation
                            )?
                            .is_some()
                    );
                    assert!(
                        storage
                            .get_document_on_connection(
                                &transaction,
                                collection_id,
                                0,
                                second.id_key(),
                                cancellation
                            )?
                            .is_some()
                    );
                    transaction.rollback().map_err(sqlite_error::statement)
                },
            )
            .await
            .unwrap();
    }
}
