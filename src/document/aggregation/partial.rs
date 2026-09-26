//! Shared quotas and conservative eligibility for exact shard-local grouping.

pub(crate) use super::super::aggregation_group::partial::DocumentPartialGroups;
use super::*;

#[cfg(test)]
mod tests;

pub(crate) struct DocumentPartialAggregation {
    plan: DocumentAggregator,
}

#[derive(Default)]
pub(crate) struct DocumentPartialBudget {
    bytes: usize,
    rows: usize,
    steps: usize,
}

impl DocumentAggregator {
    pub(crate) fn can_partition(&self) -> bool {
        matches!(self.stages.first(), Some(Stage::Group(group)) if group.can_partition())
    }

    pub(crate) fn into_partial(self) -> DocumentPartialAggregation {
        assert!(self.can_partition());
        DocumentPartialAggregation { plan: self }
    }
}

impl DocumentPartialBudget {
    pub(crate) fn groups(&mut self) -> EngineResult<DocumentPartialGroups> {
        let groups = DocumentPartialGroups::default();
        add_bytes(&mut self.bytes, groups.retained_bytes())?;
        Ok(groups)
    }
}

impl DocumentPartialAggregation {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.plan.retained_bytes()
    }

    fn group(&self) -> &Group {
        let Stage::Group(group) = &self.plan.stages[0] else {
            unreachable!("proven group")
        };
        group
    }

    pub(crate) fn push(
        &self,
        groups: &mut DocumentPartialGroups,
        document: &BsonDocument,
        position: (u64, u16),
        shared: &mut DocumentPartialBudget,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        if shared.rows == MAX_ROWS {
            return Err(limit());
        }
        shared.rows += 1;
        let before = groups.retained_bytes();
        let mut work = Budget {
            check,
            steps: shared.steps,
        };
        let result = groups.push(
            self.group(),
            document,
            position,
            shared.bytes - before,
            &mut || work.step(),
        );
        shared.bytes = shared.bytes - before + groups.retained_bytes();
        shared.steps = work.steps;
        result
    }

    pub(crate) fn finish(
        &self,
        partials: Vec<DocumentPartialGroups>,
        mut shared: DocumentPartialBudget,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<BsonDocument>> {
        let mut work = Budget {
            check,
            steps: shared.steps,
        };
        work.step()?;
        let mut partials = partials.into_iter();
        let mut merged = partials.next().unwrap_or_default();
        for partial in partials {
            let before = merged.retained_bytes() + partial.retained_bytes();
            merged.merge(self.group(), partial, shared.bytes - before, &mut || {
                work.step()
            })?;
            shared.bytes = shared.bytes - before + merged.retained_bytes();
        }
        let groups = merged.into_groups(&mut || work.step())?;
        let rows = group_rows(groups, self.group(), &mut work)?;
        let rows = execute_stages(&self.plan.stages[1..], rows, &mut work)?;
        output_documents(rows, &mut work)
    }
}
