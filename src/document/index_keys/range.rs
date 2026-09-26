//! Bounded range inference for the string payload of a single-component key.

use super::*;

#[cfg(test)]
mod tests;

impl DocumentIndexKeyGenerator {
    pub(super) fn string_range_with_budget(
        &self,
        matcher: &DocumentMatcher,
        budget: &mut Budget<'_>,
    ) -> EngineResult<Option<DocumentIndexSelection>> {
        budget.step()?;
        if self.paths.len() != 1 || !self.proves_partial_membership(matcher, budget)? {
            return Ok(None);
        }
        let Some((operand, greater, inclusive)) =
            matcher.string_range_for_index_path(&self.paths[0], &mut || budget.step())?
        else {
            return Ok(None);
        };
        // A string range necessarily requires a present string or an array
        // member, so sparse exclusion of entirely missing values is safe.
        // Uncertain stored shapes retain their normal BDIF fallback entry.
        let key = DocumentIndexKey {
            components: component_keys(Some(operand), budget)?,
        };
        budget.keys(1)?;
        let bytes = key.encoded_len_with_check(&mut || budget.step())?;
        budget.charge(bytes)?;
        Ok(Some(DocumentIndexSelection::StringRange {
            key: key.to_bytes_with_check(&mut || budget.step())?,
            greater,
            inclusive,
        }))
    }
}
