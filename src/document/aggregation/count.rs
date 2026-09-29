//! Conservative scalar-count lowering, after every pipeline stage is validated.
//! Unlike general grouping, this retains no input rows or document-valued state.

use super::*;
use std::sync::Arc;

#[cfg(test)]
mod tests;

enum Window {
    Skip(u64),
    Limit(u64),
}

pub(crate) struct DocumentCountAggregation {
    matcher: Option<Arc<DocumentMatcher>>,
    windows: Vec<Window>,
    field: String,
    key: Option<BsonValue>,
    native: bool,
    count: u64,
    exhausted: bool,
    retained_bytes: usize,
}

impl DocumentAggregator {
    /// Only an optional leading match, skip/limit, and a terminal count (or
    /// literal-key group with exactly one integer-unit sum) are scalar counts.
    /// No stage is silently discarded, reordered, or validated lazily.
    pub(crate) fn into_count(self) -> Result<DocumentCountAggregation, Self> {
        let Some((last, prefix)) = self.stages.split_last() else {
            return Err(self);
        };
        if !matches!(last, Stage::Count(_))
            && !matches!(last, Stage::Group(group) if group.is_unit_count())
        {
            return Err(self);
        }
        if !prefix.iter().enumerate().all(|(i, stage)| {
            matches!(stage, Stage::Skip(_) | Stage::Limit(_))
                || i == 0 && matches!(stage, Stage::Match(_))
        }) {
            return Err(self);
        }
        let mut stages = self.stages;
        let (field, key) = match stages.pop().expect("terminal count") {
            Stage::Count(field) => (field, None),
            Stage::Group(group) => {
                let (field, key) = group.into_unit_count();
                (field, Some(key))
            }
            _ => unreachable!("proven count terminal"),
        };
        let mut matcher = None;
        let mut windows = Vec::new();
        let mut has_limit = false;
        for stage in stages {
            match stage {
                Stage::Match(compiled) if !compiled.is_unconditional() => {
                    matcher = Some(Arc::new(compiled));
                }
                Stage::Match(_) => (),
                Stage::Skip(amount) => windows.push(Window::Skip(amount)),
                Stage::Limit(amount) => {
                    has_limit = true;
                    windows.push(Window::Limit(amount));
                }
                _ => unreachable!("proven count prefix"),
            }
        }
        Ok(DocumentCountAggregation {
            native: matcher.is_none() || !has_limit,
            matcher,
            windows,
            field,
            key,
            count: 0,
            exhausted: false,
            // The compiled-stage charge conservatively covers moved fields,
            // window counters and Arc overhead. No input documents are retained.
            retained_bytes: self.retained_bytes,
        })
    }
}

impl DocumentCountAggregation {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Filtered limits must stop matching in global encounter order. Counting
    /// every shard first could evaluate later predicates after an early limit.
    pub(crate) fn uses_native_count(&self) -> bool {
        self.native
    }

    pub(crate) fn take_source_matcher(&mut self) -> Option<Arc<DocumentMatcher>> {
        assert!(self.native);
        let matcher = self.matcher.take();
        if let Some(matcher) = &matcher {
            self.retained_bytes -= matcher.retained_bytes();
        }
        matcher
    }

    pub(crate) fn finish_total(mut self, mut total: u64) -> EngineResult<Option<BsonDocument>> {
        for window in &self.windows {
            total = match window {
                Window::Skip(skip) => total.saturating_sub(*skip),
                Window::Limit(limit) => total.min(*limit),
            };
        }
        self.count = total;
        self.finish()
    }

    pub(crate) fn is_input_exhausted(&self) -> bool {
        self.exhausted
    }

    pub(crate) fn push(
        &mut self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        if self.exhausted {
            return Ok(());
        }
        if let Some(matcher) = &self.matcher {
            if !matcher.matches_with_check(document, check)? {
                return Ok(());
            }
        }
        for window in &mut self.windows {
            check()?;
            match window {
                Window::Skip(remaining) if *remaining != 0 => {
                    *remaining -= 1;
                    return Ok(());
                }
                Window::Skip(_) => (),
                Window::Limit(remaining) => {
                    if *remaining == 0 {
                        self.exhausted = true;
                        return Ok(());
                    }
                    *remaining -= 1;
                    self.exhausted |= *remaining == 0;
                }
            }
        }
        self.count = self.count.checked_add(1).ok_or_else(limit)?;
        check()
    }

    pub(crate) fn finish(self) -> EngineResult<Option<BsonDocument>> {
        if self.count == 0 {
            return Ok(None);
        }
        // Share the integer-sum representation, including Int32/Int64/Double
        // promotion. A literal Int64(1) does not force an Int64 small result.
        let value =
            super::super::aggregation_numeric::Sum::from_integer(i128::from(self.count)).finish();
        let mut fields = Vec::with_capacity(2);
        if let Some(key) = self.key {
            fields.push(("_id".to_owned(), key));
        }
        fields.push((self.field, value));
        BsonDocument::from_entries(fields)
            .map(Some)
            .map_err(|error| error.into_engine_error(BsonErrorContext::StoredData))
    }
}
