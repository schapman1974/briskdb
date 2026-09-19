//! Bounded operator updates, committed one shard at a time.

use super::*;
use crate::{
    document::{DocumentRequestId, DocumentUpdateResult, DocumentUpdater},
    sqlite_error,
};
use rusqlite::{Transaction, TransactionBehavior};

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_update_many(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        namespace: DocumentNamespace,
        filter: DocumentFilter,
        updater: Arc<DocumentUpdater>,
        max_document_bytes: usize,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        limits: ResultLimits,
    ) -> EngineResult<DocumentExecution> {
        let storage = self.inner.database.storage.clone();
        let shard_count = self.shard_count();
        let (collection_id, route, plan) = self
            .run_document_storage_task(
                cancellation.clone(),
                deadline,
                move |cancellation, control| {
                    let route = prepare_filter_route(&storage, filter, cancellation, &control)?;
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
                    // Counts occupy fixed-width fields. Preflight the final delivery
                    // shape before the first shard can commit anything.
                    enforce_execution_result_limits_with_check(
                        &DocumentExecution::new(request_id, Some(plan.clone()), counts(0, 0)?),
                        limits,
                        &mut || ensure_document_cpu_active(cancellation, &control),
                    )?;
                    ensure_document_cpu_active(cancellation, &control)?;
                    Ok((collection_id, route, plan))
                },
            )
            .await?;
        let (shards, key, matcher) = match route {
            PreparedFilterRoute::Point { id_key, shard } => (shard..shard + 1, Some(id_key), None),
            PreparedFilterRoute::Scatter(matcher) => (0..shard_count, None, matcher),
        };
        let mut totals = (0u64, 0u64);
        for shard in shards {
            let updater = Arc::clone(&updater);
            let matcher = matcher.clone();
            let plan = plan.clone();
            let mut key = key.clone();
            totals = self
                .run_document_shard_controlled(
                    shard,
                    owner,
                    cancellation.clone(),
                    deadline,
                    move |storage, connection, cancellation, control| {
                        let transaction =
                            Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
                                .map_err(sqlite_error::statement)?;
                        let point = key.is_some();
                        let mut after = None;
                        let (mut matched, mut modified) = totals;
                        loop {
                            ensure_document_cpu_active(cancellation, control)?;
                            let record = if point {
                                match key.take() {
                                    Some(key) => storage.get_document_on_connection(
                                        &transaction,
                                        collection_id,
                                        shard,
                                        &key,
                                        cancellation,
                                    )?,
                                    None => None,
                                }
                            } else {
                                deletion::next_match(
                                    storage,
                                    &transaction,
                                    collection_id,
                                    shard,
                                    &mut after,
                                    matcher.as_deref(),
                                    cancellation,
                                    control,
                                )?
                            };
                            let Some(record) = record else { break };
                            let execution = single_mutation::update_record(
                                storage,
                                &transaction,
                                record,
                                &updater,
                                max_document_bytes,
                                request_id,
                                plan.clone(),
                                limits,
                                cancellation,
                                control,
                            )?;
                            let DocumentResult::Update(result) = execution.into_parts().2 else {
                                return Err(EngineError::new(
                                    EngineErrorKind::Internal,
                                    "unexpected field-update result",
                                ));
                            };
                            matched = matched
                                .checked_add(result.matched_count())
                                .ok_or_else(result_size_overflow)?;
                            modified = modified
                                .checked_add(result.modified_count())
                                .ok_or_else(result_size_overflow)?;
                        }
                        ensure_document_cpu_active(cancellation, control)?;
                        transaction.commit().map_err(sqlite_error::statement)?;
                        // Known commits are not reclassified by late cancellation.
                        Ok((matched, modified))
                    },
                )
                .await?;
        }
        Ok(DocumentExecution::new(
            request_id,
            Some(plan),
            counts(totals.0, totals.1)?,
        ))
    }
}

fn counts(matched: u64, modified: u64) -> EngineResult<DocumentResult> {
    Ok(DocumentResult::Update(DocumentUpdateResult::new(
        matched, modified, None,
    )?))
}
