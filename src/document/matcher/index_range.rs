//! One necessary string range, never an intersection of multikey predicates.

use super::*;

impl DocumentMatcher {
    pub(in crate::document) fn string_range_for_index_path<'a>(
        &'a self,
        requested: &[String],
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Option<(&'a BsonValue, bool, bool)>> {
        for clause in &self.clauses {
            check()?;
            match clause {
                Clause::Field {
                    path, predicates, ..
                } if path == requested => {
                    for predicate in predicates {
                        check()?;
                        if let Predicate::Range {
                            operand: operand @ BsonValue::String(_),
                            greater,
                            inclusive,
                        } = predicate
                        {
                            return Ok(Some((operand, *greater, *inclusive)));
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if let Some(range) = child.string_range_for_index_path(requested, check)? {
                            return Ok(Some(range));
                        }
                    }
                }
                // OR/NOR/NOT/elemMatch need separate proofs. Numeric ranges
                // cannot be compared using the equality frame's byte ordering.
                _ => (),
            }
        }
        Ok(None)
    }
}
