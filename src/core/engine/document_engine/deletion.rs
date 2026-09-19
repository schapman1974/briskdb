//! Filtered deletion: bounded scans, natural-order selection, shard-local commits.

use super::*;
use crate::{
    document::{CanonicalBsonKey, DocumentDeleteRequest, DocumentRequestId},
    sqlite_error,
};
use rusqlite::{Connection, TransactionBehavior};

struct Candidate {
    shard: u16,
    natural_order: u64,
    key: CanonicalBsonKey,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_delete(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        request: DocumentDeleteRequest,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let (namespace, filter, scope, options) = request.into_parts();
        require_delete_options(options)?;
        let storage = self.inner.database.storage.clone();
        let shard_count = self.shard_count();
        let (collection_id, route, plan) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let route = prepare_filter_route(&storage, filter, cancellation, &control)?;
                    let collection = storage.document_collection_controlled(
                        namespace.database(),
                        namespace.collection(),
                        Arc::clone(&control),
                    )?;
                    let collection_id = require_collection(collection)?.id();
                    let plan = match &route {
                        PreparedFilterRoute::Point { id_key, shard } => DocumentPlan::Point(
                            DocumentPointPlan::new(collection_id, *shard, id_key.clone())?,
                        ),
                        PreparedFilterRoute::Scatter(_) => {
                            scatter_plan(collection_id, shard_count)?
                        }
                    };
                    // The result has fixed size regardless of the eventual count.
                    // Reject delivery limits before admitting any writes.
                    enforce_execution_result_limits(
                        &DocumentExecution::new(
                            request_id,
                            Some(plan.clone()),
                            DocumentResult::Delete(DocumentDeleteResult::new(0)),
                        ),
                        limits,
                    )?;
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok((collection_id, route, plan))
                },
            )
            .await?;
        let count = match route {
            PreparedFilterRoute::Point { id_key, shard } => {
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
                                &id_key,
                                cancellation,
                            )
                        },
                    )
                    .await?;
                u64::from(deleted)
            }
            PreparedFilterRoute::Scatter(matcher) if scope == DocumentMutationScope::Many => {
                let mut count = 0u64;
                for shard in 0..shard_count {
                    let matcher = matcher.clone();
                    let deleted = self
                        .run_document_shard_controlled(
                            shard,
                            owner,
                            cancellation.clone(),
                            deadline,
                            move |storage, connection, cancellation, control| {
                                let transaction = rusqlite::Transaction::new_unchecked(
                                    connection,
                                    TransactionBehavior::Immediate,
                                )
                                .map_err(sqlite_error::statement)?;
                                let mut after = None;
                                let mut count = 0u64;
                                while let Some(record) = next_match(
                                    storage,
                                    &transaction,
                                    collection_id,
                                    shard,
                                    &mut after,
                                    matcher.as_deref(),
                                    cancellation,
                                    control,
                                )? {
                                    ensure_document_cpu_active(cancellation, control)?;
                                    if storage.delete_document_on_connection(
                                        &transaction,
                                        collection_id,
                                        shard,
                                        record.id_key(),
                                        cancellation,
                                    )? {
                                        count = count
                                            .checked_add(1)
                                            .ok_or_else(result_size_overflow)?;
                                    }
                                }
                                ensure_document_cpu_active(cancellation, control)?;
                                transaction.commit().map_err(sqlite_error::statement)?;
                                // No cancellation check after a known successful commit.
                                Ok(count)
                            },
                        )
                        .await?;
                    count = count
                        .checked_add(deleted)
                        .ok_or_else(result_size_overflow)?;
                }
                count
            }
            PreparedFilterRoute::Scatter(matcher) => {
                loop {
                    // Retain only one canonical identity, never a document per shard.
                    let mut earliest: Option<Candidate> = None;
                    for shard in 0..shard_count {
                        let matcher = matcher.clone();
                        let candidate = self
                            .run_document_shard_controlled(
                                shard,
                                owner,
                                cancellation.clone(),
                                deadline,
                                move |storage, connection, cancellation, control| {
                                    let record = next_match(
                                        storage,
                                        connection,
                                        collection_id,
                                        shard,
                                        &mut None,
                                        matcher.as_deref(),
                                        cancellation,
                                        control,
                                    )?;
                                    Ok(record.map(|record| Candidate {
                                        shard,
                                        natural_order: record.natural_order(),
                                        key: record.id_key().clone(),
                                    }))
                                },
                            )
                            .await?;
                        if let Some(candidate) = candidate {
                            if earliest
                                .as_ref()
                                .is_none_or(|old| candidate.natural_order < old.natural_order)
                            {
                                earliest = Some(candidate);
                            }
                        }
                    }
                    let Some(candidate) = earliest else {
                        break 0;
                    };
                    let matcher = matcher.clone();
                    let deleted = self
                        .run_document_shard_controlled(
                            candidate.shard,
                            owner,
                            cancellation.clone(),
                            deadline,
                            move |storage, connection, cancellation, control| {
                                let transaction = rusqlite::Transaction::new_unchecked(
                                    connection,
                                    TransactionBehavior::Immediate,
                                )
                                .map_err(sqlite_error::statement)?;
                                let deleted = delete_candidate(
                                    storage,
                                    &transaction,
                                    collection_id,
                                    &candidate,
                                    matcher.as_deref(),
                                    cancellation,
                                    control,
                                )?;
                                ensure_document_cpu_active(cancellation, control)?;
                                transaction.commit().map_err(sqlite_error::statement)?;
                                Ok(deleted)
                            },
                        )
                        .await?;
                    if deleted {
                        break 1;
                    }
                    // A concurrent change invalidated selection. Select again;
                    // never delete a recreated same-_id or a no-longer-matching row.
                }
            }
        };
        Ok(DocumentExecution::new(
            request_id,
            Some(plan),
            DocumentResult::Delete(DocumentDeleteResult::new(count)),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn next_match(
    storage: &Storage,
    connection: &Connection,
    collection_id: DocumentCollectionId,
    shard: u16,
    after: &mut Option<u64>,
    matcher: Option<&DocumentMatcher>,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<Option<DocumentStorageRecord>> {
    loop {
        let mut check = || ensure_document_cpu_active(cancellation, control);
        check()?;
        let Some(record) = storage
            .scan_document_shard_on_connection(
                connection,
                collection_id,
                shard,
                *after,
                1,
                cancellation,
            )?
            .pop()
        else {
            return Ok(None);
        };
        *after = Some(record.natural_order());
        if matcher.map_or(Ok(true), |matcher| {
            matcher.matches_with_check(record.document(), &mut check)
        })? {
            check()?;
            return Ok(Some(record));
        }
    }
}

fn delete_candidate(
    storage: &Storage,
    transaction: &rusqlite::Transaction<'_>,
    collection_id: DocumentCollectionId,
    candidate: &Candidate,
    matcher: Option<&DocumentMatcher>,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<bool> {
    let mut check = || ensure_document_cpu_active(cancellation, control);
    check()?;
    let Some(record) = storage.get_document_on_connection(
        transaction,
        collection_id,
        candidate.shard,
        &candidate.key,
        cancellation,
    )?
    else {
        return Ok(false);
    };
    if record.natural_order() != candidate.natural_order
        || !matcher.map_or(Ok(true), |matcher| {
            matcher.matches_with_check(record.document(), &mut check)
        })?
    {
        return Ok(false);
    }
    check()?;
    storage.delete_document_on_connection(
        transaction,
        collection_id,
        candidate.shard,
        &candidate.key,
        cancellation,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{core::RequestContext, document::DocumentCreateCollectionRequest};

    #[tokio::test]
    async fn candidate_recheck_rejects_deleted_recreated_and_changed_records() {
        let root = tempfile::tempdir().unwrap();
        let engine = Engine::open(root.path(), 2).await.unwrap();
        let session = engine.session();
        let execution = engine
            .execute_document(
                &session,
                DocumentRequest::new(
                    DocumentRequestId::new([1; 16]).unwrap(),
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
        let shard = engine
            .inner
            .database
            .storage
            .prepare_document_id(&BsonValue::Int32(1))
            .unwrap()
            .1;
        engine
            .run_document_shard_controlled(
                shard,
                ConnectionOwner::new(session.id().get()),
                CancellationToken::new(),
                None,
                move |storage, connection, cancellation, control| {
                    let row = |group| {
                        BsonDocument::from_entries([
                            ("_id", BsonValue::Int32(1)),
                            ("group", BsonValue::Int32(group)),
                        ])
                        .unwrap()
                    };
                    let prepared = storage.prepare_document_write(&row(1))?;
                    let mut candidate = Candidate {
                        shard,
                        natural_order: 1,
                        key: prepared.id_key().clone(),
                    };
                    let transaction = rusqlite::Transaction::new_unchecked(
                        connection,
                        TransactionBehavior::Immediate,
                    )
                    .map_err(sqlite_error::statement)?;
                    storage.insert_prepared_document_on_connection(
                        &transaction,
                        collection_id,
                        1,
                        shard,
                        &prepared,
                        cancellation,
                    )?;
                    storage.delete_document_on_connection(
                        &transaction,
                        collection_id,
                        shard,
                        &candidate.key,
                        cancellation,
                    )?;
                    assert!(!delete_candidate(
                        storage,
                        &transaction,
                        collection_id,
                        &candidate,
                        None,
                        cancellation,
                        control
                    )?);
                    storage.insert_prepared_document_on_connection(
                        &transaction,
                        collection_id,
                        2,
                        shard,
                        &prepared,
                        cancellation,
                    )?;
                    assert!(
                        !delete_candidate(
                            storage,
                            &transaction,
                            collection_id,
                            &candidate,
                            None,
                            cancellation,
                            control
                        )?,
                        "same _id is a new incarnation"
                    );
                    candidate.natural_order = 2;
                    let replacement = storage.prepare_document_write(&row(0))?;
                    assert!(storage.replace_document_on_connection(
                        &transaction,
                        collection_id,
                        shard,
                        &candidate.key,
                        2,
                        &replacement,
                        cancellation
                    )?);
                    let matcher = DocumentMatcher::compile_with_check(
                        &BsonDocument::from_entries([("group", BsonValue::Int32(1))]).unwrap(),
                        &mut || Ok(()),
                    )?;
                    assert!(
                        !delete_candidate(
                            storage,
                            &transaction,
                            collection_id,
                            &candidate,
                            Some(&matcher),
                            cancellation,
                            control
                        )?,
                        "current record must still match"
                    );
                    assert!(delete_candidate(
                        storage,
                        &transaction,
                        collection_id,
                        &candidate,
                        None,
                        cancellation,
                        control
                    )?);
                    transaction.rollback().map_err(sqlite_error::statement)
                },
            )
            .await
            .unwrap();
    }
}
