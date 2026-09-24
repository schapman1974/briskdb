//! Bounded necessary scalar membership tuples for existing Ready indexes.
//! This is only candidate inference; the complete matcher remains authoritative.

use super::*;

pub(super) const MAX_PROBE_KEYS: usize = 128;
const MAX_PROBE_BYTES: usize = 1024 * 1024;

#[cfg(test)]
mod tests;

impl DocumentIndexKeyGenerator {
    pub(super) fn membership_keys_with_budget(
        &self,
        matcher: &DocumentMatcher,
        budget: &mut Budget<'_>,
    ) -> EngineResult<Option<Vec<DocumentIndexKey>>> {
        budget.step()?;
        if self.partial.is_some() {
            return Ok(None);
        }
        budget.charge(128 + self.paths.len() * 32)?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(self.paths.len())
            .map_err(allocation)?;
        let mut count = 1_usize;
        let mut may_be_all_null = true;
        for path in &self.paths {
            let values = if let Some(value) =
                matcher.equality_for_index_path(path, &mut || budget.step())?
            {
                vec![value]
            } else if let Some(values) =
                matcher.membership_for_index_path(path, MAX_PROBE_KEYS, &mut || budget.step())?
            {
                values
            } else if matcher.requires_index_path_existence(path, false, &mut || budget.step())? {
                // Missing fields have the ordinary null key. Explicit null is
                // only a false-positive candidate, removed by the full matcher.
                // Uncertain stored array paths retain their BDIF fallback key.
                // Existing sparse all-null rejection below remains mandatory.
                vec![&NULL]
            } else {
                return Ok(None);
            };
            let mut selected = Vec::new();
            selected
                .try_reserve_exact(values.len())
                .map_err(allocation)?;
            budget.charge(values.len() * 64)?;
            let mut includes_null = false;
            for value in values {
                budget.step()?;
                if !supported_scalar(value) {
                    return Ok(None);
                }
                includes_null |= matches!(value, BsonValue::Null);
                let mut keys = component_keys(Some(value), budget)?;
                let key = keys.pop().expect("a supported scalar has one equality key");
                // Numeric aliases and repeated members must not enlarge the
                // cartesian product or produce duplicate SQL candidates.
                if !selected.contains(&key) {
                    selected.push(key);
                }
            }
            may_be_all_null &= includes_null;
            let Some(next) = count
                .checked_mul(selected.len())
                .filter(|n| *n <= MAX_PROBE_KEYS)
            else {
                return Ok(None);
            };
            count = next;
            components.push(selected);
        }
        if self.sparse && may_be_all_null {
            // An all-null tuple can match an entirely absent sparse record.
            return Ok(None);
        }
        budget.keys(count)?;
        budget.charge(count * (128 + self.paths.len() * 32))?;
        let mut output = Vec::new();
        output.try_reserve_exact(count).map_err(allocation)?;
        let mut bytes = 0_usize;
        for index in 0..count {
            budget.step()?;
            let mut position = index;
            let mut tuple = Vec::new();
            tuple
                .try_reserve_exact(components.len())
                .map_err(allocation)?;
            for values in &components {
                budget.step()?;
                tuple.push(Arc::clone(&values[position % values.len()]));
                position /= values.len();
            }
            let key = DocumentIndexKey { components: tuple };
            let length = key.encoded_len_with_check(&mut || budget.step())?;
            let Some(next) = bytes.checked_add(length).filter(|n| *n <= MAX_PROBE_BYTES) else {
                return Ok(None);
            };
            bytes = next;
            output.push(key);
        }
        // No repeated key bytes are allocated until every tuple has passed the
        // cumulative serialized-size bound. Components remain Arc-shared here.
        budget.charge(bytes)?;
        budget.step()?;
        Ok(Some(output))
    }
}
