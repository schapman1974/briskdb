//! Conservative multi-ID shard pruning. Matching, natural order, and mutation
//! semantics remain in the existing shared execution paths.

use super::*;

const MAX_ROUTED_IDS: usize = 1024;

#[cfg(test)]
mod tests;

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
