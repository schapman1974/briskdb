//! Bounded initial natural-order frontiers. Every started child drains before
//! the owning document operation releases its schema/session/lifecycle guards.

use std::{future::Future, sync::atomic::AtomicU64};

use tokio::task::JoinSet;

use super::super::MAX_SCATTER_CONCURRENCY;
use super::*;

struct CancelChildren(CancellationToken);
impl Drop for CancelChildren {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Do not cancel the caller's token on a child error: callers may share it
/// across commands, or use their listener shutdown token as request control.
async fn coordinate<T, F, Fut>(
    shards: Vec<u16>,
    parent: CancellationToken,
    shutdown: CancellationToken,
    deadline: Option<Instant>,
    work: F,
) -> EngineResult<Vec<(u16, T)>>
where
    T: Send + 'static,
    F: Fn(u16, CancellationToken) -> Fut,
    Fut: Future<Output = EngineResult<T>> + Send + 'static,
{
    if let Some(reason) = pending_cancellation_reason(&parent, &shutdown, deadline) {
        return Err(reason.error());
    }
    let children = CancellationToken::new();
    let _cancel_children = CancelChildren(children.clone());
    let mut results = Vec::with_capacity(shards.len());
    let mut remaining = shards.into_iter();
    let mut running = JoinSet::new();
    for shard in remaining.by_ref().take(MAX_SCATTER_CONCURRENCY) {
        let future = work(shard, children.clone());
        running.spawn(async move { (shard, future.await) });
    }
    let mut first_error = None;
    while !running.is_empty() {
        let joined = tokio::select! {
            biased;
            reason = wait_for_cancellation(&parent, &shutdown, deadline), if first_error.is_none() => {
                first_error = Some(reason.error());
                children.cancel();
                results.clear();
                continue;
            }
            joined = running.join_next() => joined,
        };
        match joined {
            Some(Ok((shard, Ok(result)))) if first_error.is_none() => results.push((shard, result)),
            Some(Ok((_, Err(error)))) if first_error.is_none() => {
                first_error = Some(error);
                children.cancel();
                results.clear();
            }
            Some(Err(error)) if first_error.is_none() => {
                first_error = Some(EngineError::from_source(
                    EngineErrorKind::Internal,
                    "document frontier task failed",
                    error,
                ));
                children.cancel();
                results.clear();
            }
            _ => {}
        }
        if first_error.is_none() {
            if let Some(shard) = remaining.next() {
                let future = work(shard, children.clone());
                running.spawn(async move { (shard, future.await) });
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(results),
    }
}

struct FrontierBudget {
    retained: AtomicU64,
    limit: u64,
}

impl FrontierBudget {
    fn reserve(&self, bytes: u64) -> EngineResult<()> {
        self.retained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |retained| {
                retained
                    .checked_add(bytes)
                    .filter(|total| *total <= self.limit)
            })
            .map(|_| ())
            .map_err(|retained| {
                if retained.checked_add(bytes).is_none() {
                    result_size_overflow()
                } else {
                    limit_exceeded(
                        "document scatter merge frontier exceeds its bounded memory limit",
                    )
                }
            })
    }
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn initial_document_frontiers(
        &self,
        owner: ConnectionOwner,
        state: &CursorState,
        cancellation: CancellationToken,
        deadline: Option<Instant>,
        matcher: Option<Arc<DocumentMatcher>>,
        frontier_limit: u64,
    ) -> EngineResult<(Vec<Option<DocumentStorageRecord>>, u64)> {
        let collection_id = state.collection_id;
        let after = state.after;
        let stats = state.read_stats.clone();
        let shard_count = self.shard_count();
        let shards = state.source.shards(shard_count).collect();
        let budget = Arc::new(FrontierBudget {
            retained: AtomicU64::new(0),
            limit: frontier_limit,
        });
        let child_budget = Arc::clone(&budget);
        let engine = self.clone();
        let results = coordinate(
            shards,
            cancellation,
            self.inner.shutdown_cancel.clone(),
            deadline,
            move |shard, cancellation| {
                let engine = engine.clone();
                let matcher = matcher.clone();
                let stats = stats.clone();
                let budget = Arc::clone(&child_budget);
                async move {
                    engine
                        .run_document_shard(
                            shard,
                            owner,
                            cancellation,
                            deadline,
                            move |storage, connection, cancellation| {
                                let record = next_matching_document(
                                    storage,
                                    connection,
                                    collection_id,
                                    shard,
                                    after,
                                    matcher.as_deref(),
                                    cancellation,
                                    deadline,
                                    stats.as_deref(),
                                )?;
                                if let Some(record) = &record {
                                    validate_point_record(
                                        record,
                                        collection_id,
                                        shard,
                                        record.id_key(),
                                    )?;
                                    // Charge before publishing a completed result or admitting
                                    // another shard. In-flight decodes remain capped at eight
                                    // records, each with the existing BSON allocation limits.
                                    budget.reserve(
                                        u64::try_from(record.encoded_len()).unwrap_or(u64::MAX),
                                    )?;
                                }
                                Ok(record)
                            },
                        )
                        .await
                }
            },
        )
        .await?;
        let mut frontiers: Vec<_> = (0..shard_count).map(|_| None).collect();
        for (shard, record) in results {
            frontiers[usize::from(shard)] = record;
        }
        Ok((frontiers, budget.retained.load(Ordering::Acquire)))
    }
}

#[cfg(test)]
mod engine_tests;
#[cfg(test)]
mod tests;
