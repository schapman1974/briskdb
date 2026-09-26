//! Bounded shard scans with exact partial groups. One shared quota spans all
//! active/completed states and the final merge. The coordinator drains every
//! child before releasing the owning operation's schema/session guards.

use super::*;
use crate::document::{DocumentPartialAggregation, DocumentPartialBudget};
use std::sync::Mutex;

impl Engine {
    pub(super) async fn read_partial_groups(
        &self,
        owner: ConnectionOwner,
        state: &CursorState,
        plan: Arc<DocumentPartialAggregation>,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
    ) -> EngineResult<Vec<BsonDocument>> {
        let budget = Arc::new(Mutex::new(DocumentPartialBudget::default()));
        let child_budget = Arc::clone(&budget);
        let child_plan = Arc::clone(&plan);
        let engine = self.clone();
        let collection_id = state.collection_id;
        let stats = state.read_stats.clone();
        // Only pipelines beginning with a group are eligible; there is no
        // source filter/limit/sort whose position can move across this boundary.
        let results = super::super::fanout::coordinate(
            state.source.shards(self.shard_count()).collect(),
            cancellation.clone(),
            self.inner.shutdown_cancel.clone(),
            deadline,
            move |shard, cancellation| {
                let engine = engine.clone();
                let plan = Arc::clone(&child_plan);
                let budget = Arc::clone(&child_budget);
                let stats = stats.clone();
                async move {
                    engine
                        .run_document_shard(
                            shard,
                            owner,
                            cancellation,
                            deadline,
                            move |storage, connection, cancellation| {
                                let mut groups = budget.lock().map_err(|_| poisoned())?.groups()?;
                                let mut after = None;
                                while let Some(record) = next_matching_document(
                                    storage,
                                    connection,
                                    collection_id,
                                    shard,
                                    after,
                                    None,
                                    cancellation,
                                    deadline,
                                    stats.as_deref(),
                                )? {
                                    validate_point_record(
                                        &record,
                                        collection_id,
                                        shard,
                                        record.id_key(),
                                    )?;
                                    after = Some(record.natural_order());
                                    // Only admitted blocking workers hold this lock, never
                                    // across await or storage access. At most eight bounded
                                    // BSON decodes can be in flight independently of the
                                    // single 64-MiB aggregation working-state quota.
                                    let mut budget = budget.lock().map_err(|_| poisoned())?;
                                    plan.push(
                                        &mut groups,
                                        record.document(),
                                        (record.natural_order(), shard),
                                        &mut budget,
                                        &mut || check(cancellation, deadline),
                                    )?;
                                }
                                check(cancellation, deadline)?;
                                Ok(groups)
                            },
                        )
                        .await
                }
            },
        )
        .await?;
        let budget = Arc::try_unwrap(budget)
            .map_err(|_| poisoned())?
            .into_inner()
            .map_err(|_| poisoned())?;
        self.run_document_storage_task(cancellation, deadline, move |cancellation, control| {
            plan.finish(
                results.into_iter().map(|(_, groups)| groups).collect(),
                budget,
                &mut || ensure_document_cpu_active(cancellation, &control),
            )
        })
        .await
    }
}

fn poisoned() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "document partial-group budget unavailable",
    )
}

fn check(cancellation: &CancellationToken, deadline: Option<Instant>) -> EngineResult<()> {
    if cancellation.is_cancelled() {
        return Err(EngineError::new(
            EngineErrorKind::Cancelled,
            "document grouping cancelled",
        ));
    }
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(EngineError::deadline_exceeded(
            "document grouping deadline exceeded",
        ));
    }
    Ok(())
}
