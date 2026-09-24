//! Bounded operator updates, committed one shard at a time.

use super::*;
use crate::storage::DocumentWriteTransaction;
use crate::{
    document::{DocumentRequestId, DocumentUpdateResult, DocumentUpdater, DocumentWriteRollback},
    sqlite_error,
};

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_document_update_many(
        &self,
        owner: ConnectionOwner,
        request_id: DocumentRequestId,
        namespace: DocumentNamespace,
        filter: Arc<DocumentFilter>,
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
                    let route = prepare_filter_route(&storage, &filter, cancellation, &control)?;
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
            let key = key.clone();
            totals = self
                .run_document_shard_controlled(
                    shard,
                    owner,
                    cancellation.clone(),
                    deadline,
                    move |storage, connection, cancellation, control| {
                        let transaction = storage.begin_document_write(
                            connection,
                            collection_id,
                            shard,
                            cancellation,
                            Some(control),
                        )?;
                        let outcome = update_shard_matches(
                            storage,
                            &transaction,
                            collection_id,
                            shard,
                            key,
                            matcher.as_deref(),
                            &updater,
                            max_document_bytes,
                            request_id,
                            &plan,
                            limits,
                            cancellation,
                            control,
                            totals,
                        );
                        match outcome {
                            Ok(counts) => {
                                // A commit/cleanup failure is never certified as a
                                // rolled-back statement. Known successful commits
                                // are not reclassified by late cancellation.
                                transaction.commit().map_err(sqlite_error::statement)?;
                                Ok(counts)
                            }
                            Err(error) => {
                                // Do not rely on Drop's best-effort rollback when
                                // certifying an error as safe for batch continuation.
                                transaction.rollback().map_err(sqlite_error::statement)?;
                                Err(if totals.1 == 0 {
                                    DocumentWriteRollback::wrap(error)
                                } else {
                                    error
                                })
                            }
                        }
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

// Shared with the insertion-shard recheck of update-many upserts. The caller
// owns commit/rollback certification; this helper never commits independently.
#[allow(clippy::too_many_arguments)]
pub(super) fn update_shard_matches(
    storage: &Storage,
    transaction: &DocumentWriteTransaction<'_>,
    collection_id: DocumentCollectionId,
    shard: u16,
    mut key: Option<crate::document::CanonicalBsonKey>,
    matcher: Option<&DocumentMatcher>,
    updater: &DocumentUpdater,
    max_document_bytes: usize,
    request_id: DocumentRequestId,
    plan: &DocumentPlan,
    limits: ResultLimits,
    cancellation: &CancellationToken,
    control: &OperationControl,
    totals: (u64, u64),
) -> EngineResult<(u64, u64)> {
    let point = key.is_some();
    let mut after = None;
    let (mut matched, mut modified) = totals;
    loop {
        ensure_document_cpu_active(cancellation, control)?;
        let record = if point {
            match key.take() {
                Some(key) => storage.get_document_on_connection(
                    transaction,
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
                transaction,
                collection_id,
                shard,
                &mut after,
                matcher,
                cancellation,
                control,
            )?
        };
        let Some(record) = record else { break };
        let execution = single_mutation::update_record(
            storage,
            transaction,
            record,
            updater,
            max_document_bytes,
            single_mutation::MutationReturn::Counts,
            None,
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
    Ok((matched, modified))
}

fn counts(matched: u64, modified: u64) -> EngineResult<DocumentResult> {
    Ok(DocumentResult::Update(DocumentUpdateResult::new(
        matched, modified, None,
    )?))
}
