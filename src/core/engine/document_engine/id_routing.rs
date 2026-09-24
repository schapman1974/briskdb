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
            match proven_id_shards(storage, filter, cancellation, control)? {
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

/// Called only after the complete filter has compiled successfully. Recognize
/// necessary ID constraints, not a second query language: ordinary fields and
/// negations never provide routes. AND intersects proven owner sets; OR can
/// union them only when every alternative is proven. The full matcher remains
/// authoritative, including when only one owner is left.
pub(super) fn proven_id_shards(
    storage: &Storage,
    filter: &DocumentFilter,
    cancellation: &CancellationToken,
    control: &OperationControl,
) -> EngineResult<Option<u64>> {
    let mut routing = IdConstraints {
        storage,
        cancellation,
        control,
        remaining_ids: MAX_ROUTED_IDS,
    };
    let shards = routing.document(filter.document())?;
    ensure_document_cpu_active(cancellation, control)?;
    // An empty intersection proves no match, but the established cursor/plan
    // API requires nonempty sources. Preserve its ordinary matcher fallback.
    Ok(shards.filter(|shards| *shards != 0))
}

struct IdConstraints<'a> {
    storage: &'a Storage,
    cancellation: &'a CancellationToken,
    control: &'a OperationControl,
    remaining_ids: usize,
}

impl IdConstraints<'_> {
    fn check(&self) -> EngineResult<()> {
        ensure_document_cpu_active(self.cancellation, self.control)
    }

    fn document(&mut self, filter: &BsonDocument) -> EngineResult<Option<u64>> {
        self.check()?;
        let mut owners = None;
        // Full compilation above bounds logical recursion and validates every
        // branch, even if this conservative recognizer cannot use it.
        for (field, value) in filter.iter() {
            self.check()?;
            let constraint = match field {
                "_id" => self.id(value)?,
                "$and" => self.logical(value, false)?,
                "$or" => self.logical(value, true)?,
                // Never descend into ordinary fields, dotted IDs, $nor, $not,
                // $elemMatch or literal ID documents looking for predicates.
                _ => None,
            };
            if let Some(constraint) = constraint {
                owners = Some(owners.map_or(constraint, |owners| owners & constraint));
            }
        }
        self.check()?;
        Ok(owners)
    }

    fn logical(&mut self, value: &BsonValue, union: bool) -> EngineResult<Option<u64>> {
        let BsonValue::Array(children) = value else {
            return Ok(None);
        };
        let mut owners = None;
        for child in children {
            self.check()?;
            let BsonValue::Document(child) = child else {
                return Ok(None);
            };
            match self.document(child)? {
                Some(child) => {
                    owners = Some(owners.map_or(child, |owners| {
                        if union {
                            owners | child
                        } else {
                            owners & child
                        }
                    }));
                }
                // One unrestricted alternative can match any shard. An
                // unrestricted conjunct does not invalidate other necessities.
                None if union => return Ok(None),
                None => (),
            }
        }
        Ok(owners)
    }

    fn id(&mut self, value: &BsonValue) -> EngineResult<Option<u64>> {
        if is_literal_id_filter(value, self.cancellation, self.control)? {
            return self.literal(value);
        }
        let BsonValue::Document(expression) = value else {
            return Ok(None);
        };
        // Other predicates on this field still belong to the full matcher.
        // Explicit $eq treats its operand literally, just like the existing
        // sole-ID point route, including a BSON regex or operator-named object.
        if let Some(id) = expression.get_first("$eq") {
            return self.literal(id);
        }
        let Some(BsonValue::Array(ids)) = expression.get_first("$in") else {
            return Ok(None);
        };
        if ids.is_empty() || ids.len() > self.remaining_ids {
            return Ok(None);
        }
        for id in ids {
            self.check()?;
            if !is_literal_id_filter(id, self.cancellation, self.control)? {
                return Ok(None);
            }
        }
        let mut owners = 0;
        for id in ids {
            let Some(owner) = self.literal(id)? else {
                return Ok(None);
            };
            owners |= owner;
        }
        Ok(Some(owners))
    }

    fn literal(&mut self, id: &BsonValue) -> EngineResult<Option<u64>> {
        self.check()?;
        if self.remaining_ids == 0 {
            return Ok(None);
        }
        self.remaining_ids -= 1;
        // Use exactly the storage writer's versioned canonical BSON encoding
        // and immutable ownership. The work bound spans all logical branches;
        // only fixed bitmaps are retained, not another copy of the ID list.
        let (_, shard) = self.storage.prepare_document_id(id)?;
        self.check()?;
        Ok(Some(1_u64 << shard))
    }
}
