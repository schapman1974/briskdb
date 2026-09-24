//! Conservative multi-ID shard pruning. Matching, natural order, and mutation
//! semantics remain in the existing shared execution paths.

use super::*;
use crate::document::DocumentPipeline;

const MAX_ROUTED_IDS: usize = 1024;

#[cfg(test)]
mod tests;

/// Called only after the entire pipeline has compiled successfully. Keep every
/// stage (including the first match) in its original place. Unfiltered source
/// rows from the selected shards still pass through the runner's cumulative
/// input/work limits before matching; there is no prefilter or duplicate matcher.
pub(super) fn leading_match_source(
    storage: &Storage,
    pipeline: &DocumentPipeline,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<PreparedFilterRoute> {
    ensure_document_cpu_active(cancellation, control)?;
    let Some(first) = pipeline.stages().first().filter(|stage| stage.len() == 1) else {
        return Ok(PreparedFilterRoute::Scatter(None));
    };
    let Some(BsonValue::Document(predicate)) = first.get_first("$match") else {
        return Ok(PreparedFilterRoute::Scatter(None));
    };
    let filter = DocumentFilter::new(predicate.clone())?;
    match classify_filter(&filter, cancellation, control)? {
        FilterRoute::Point(id) => {
            let (id_key, shard) = storage.prepare_document_id(&id)?;
            ensure_document_cpu_active(cancellation, control)?;
            Ok(PreparedFilterRoute::Point { id_key, shard })
        }
        FilterRoute::Filtered(filter) => Ok(
            match literal_in_shards(storage, filter, cancellation, control)? {
                Some(shards) => PreparedFilterRoute::ShardSubset {
                    matcher: None,
                    shards,
                },
                None => PreparedFilterRoute::Scatter(None),
            },
        ),
        FilterRoute::Scatter => Ok(PreparedFilterRoute::Scatter(None)),
    }
}

/// Called only after the complete filter has compiled successfully. Restrict
/// only a sole `_id: {$in: [literal, ...]}`; never reinterpret a regex, logical
/// alternative, duplicate field, or operator document as an exact identity.
pub(super) fn literal_in_shards(
    storage: &Storage,
    filter: &DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<Option<u64>> {
    if filter.document().len() != 1 {
        return Ok(None);
    }
    let Some(BsonValue::Document(expression)) = filter.document().get_first("_id") else {
        return Ok(None);
    };
    if expression.len() != 1 {
        return Ok(None);
    }
    let Some(BsonValue::Array(ids)) = expression.get_first("$in") else {
        return Ok(None);
    };
    if ids.is_empty() || ids.len() > MAX_ROUTED_IDS {
        return Ok(None);
    }
    let mut shards = 0_u64;
    for id in ids {
        ensure_document_cpu_active(cancellation, control)?;
        if !is_literal_id_filter(id, cancellation, control)? {
            return Ok(None);
        }
        // Use exactly the storage writer's versioned canonical BSON encoding
        // and immutable bucket ownership, including numeric-equivalent IDs.
        // Keep only a fixed bitmap, not another retained copy of every key.
        let (_, shard) = storage.prepare_document_id(id)?;
        shards |= 1_u64 << shard;
    }
    ensure_document_cpu_active(cancellation, control)?;
    Ok(Some(shards))
}
