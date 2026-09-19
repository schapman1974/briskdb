//! Atomic shard-local selection and deletion with a preflighted pre-image.

use super::*;
use crate::{
    document::{
        BSON_MAX_NESTING_DEPTH, BsonCodecOptions, CanonicalBsonKey, DEFAULT_DOCUMENT_BATCH_SIZE,
        DocumentFindOneAndDeleteRequest, DocumentRequestId, DocumentSortKey,
        encode_document_with_options,
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

impl Engine {
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
        if options.skip() != 0
            || options.limit().is_some()
            || options.batch_size() != DEFAULT_DOCUMENT_BATCH_SIZE
            || options.batch_byte_limit().is_some()
        {
            return Err(unsupported(
                "find-one-and-delete accepts only projection and sort read options",
            ));
        }
        let storage = self.inner.database.storage.clone();
        let shard_count = self.shard_count();
        let (collection_id, route, projection, sorter, plan) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let mut check = || ensure_document_cpu_active(cancellation, &control);
                    let route = prepare_filter_route(&storage, filter, cancellation, &control)?;
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
                        &DocumentExecution::new(
                            request_id,
                            Some(plan.clone()),
                            DocumentResult::Document(None),
                        ),
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
                        let execution = return_and_delete(
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
                            DocumentResult::Document(None),
                        ));
                    };
                    let shard = candidate.shard;
                    let matcher = matcher.clone();
                    let sorter = sorter.clone();
                    let projection = projection.clone();
                    let plan = plan.clone();
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
                                let execution = return_and_delete(
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
        .map(|record| {
            let document = record.into_document();
            match projection {
                Some(projection) => {
                    projection.project_owned_validated_with_check(document, &mut check)
                }
                None => Ok(document),
            }
        })
        .transpose()?;
    if let Some(document) = document.as_ref() {
        // The returned value is nested once in its result envelope. A stored
        // document at the root depth ceiling must fail before mutation, not
        // during reply serialization. This is an output limit, not corruption.
        encode_document_with_options(
            document,
            &BsonCodecOptions::new().with_max_nesting_depth(BSON_MAX_NESTING_DEPTH - 1),
        )
        .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
        check()?;
    }
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
