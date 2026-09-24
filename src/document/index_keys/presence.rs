//! Necessary field presence can select all entries of a sparse Ready index.

use super::*;

impl DocumentIndexKeyGenerator {
    pub(super) fn sparse_presence_with_budget(
        &self,
        matcher: &DocumentMatcher,
        budget: &mut Budget<'_>,
    ) -> EngineResult<bool> {
        budget.step()?;
        if !self.sparse || self.partial.is_some() {
            return Ok(false);
        }
        // Sparse compound membership requires ANY indexed field to exist.
        // A necessary presence predicate on one path therefore suffices even
        // when other indexed paths have no query constraint. Explicit null and
        // empty arrays are present; uncertain stored shapes retain BDIF entries.
        for path in &self.paths {
            if matcher.requires_index_path_existence(path, true, &mut || budget.step())? {
                return Ok(true);
            }
        }
        budget.step()?;
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
