//! Sufficient, bounded partial-index membership proofs, never query rewriting.

use super::*;

#[cfg(test)]
mod tests;

impl DocumentMatcher {
    /// Every matching query document must satisfy this membership filter.
    /// Only identical scalar equality and explicit positive existence facts
    /// participate. Numeric aliases/stronger ranges are deliberately not inferred.
    /// Positive AND/OR compose proofs without cloning or expanding either AST.
    pub(in crate::document) fn is_implied_by_index_query(
        &self,
        query: &DocumentMatcher,
        control: &mut dyn MatchControl,
    ) -> EngineResult<bool> {
        control.step()?;
        for clause in &self.clauses {
            control.step()?;
            let proven = match clause {
                Clause::Field {
                    path,
                    exact_id,
                    predicates,
                } => {
                    let mut all = true;
                    for predicate in predicates {
                        control.step()?;
                        if !query.requires_partial_fact(path, *exact_id, predicate, control)? {
                            all = false;
                            break;
                        }
                    }
                    all
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    let mut all = true;
                    for child in children {
                        if !child.is_implied_by_index_query(query, control)? {
                            all = false;
                            break;
                        }
                    }
                    all
                }
                Clause::Logical {
                    kind: Logical::Or,
                    children,
                } => {
                    let mut any = false;
                    for child in children {
                        if child.is_implied_by_index_query(query, control)? {
                            any = true;
                            break;
                        }
                    }
                    any
                }
                _ => false,
            };
            if !proven {
                return Ok(false);
            }
        }
        control.step()?;
        Ok(true)
    }

    fn requires_partial_fact(
        &self,
        requested: &[String],
        requested_exact_id: bool,
        required: &Predicate,
        control: &mut dyn MatchControl,
    ) -> EngineResult<bool> {
        control.step()?;
        if !matches!(required, Predicate::Equal(value) if partial_scalar(value))
            && !matches!(required, Predicate::Exists(true))
        {
            return Ok(false);
        }
        for clause in &self.clauses {
            control.step()?;
            match clause {
                Clause::Field {
                    path,
                    exact_id,
                    predicates,
                } if *exact_id == requested_exact_id && same_path(path, requested, control)? => {
                    for predicate in predicates {
                        control.step()?;
                        let same = match (required, predicate) {
                            (Predicate::Exists(true), Predicate::Exists(true)) => true,
                            (Predicate::Equal(left), Predicate::Equal(right))
                                if partial_scalar(right) =>
                            {
                                control.comparison_bytes(
                                    256 + scalar_bytes(left) + scalar_bytes(right),
                                )?;
                                let same = left.representation_eq(right);
                                control.step()?;
                                same
                            }
                            _ => false,
                        };
                        if same {
                            return Ok(true);
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if child.requires_partial_fact(
                            requested,
                            requested_exact_id,
                            required,
                            control,
                        )? {
                            return Ok(true);
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::Or,
                    children,
                } => {
                    let mut all = !children.is_empty();
                    for child in children {
                        if !child.requires_partial_fact(
                            requested,
                            requested_exact_id,
                            required,
                            control,
                        )? {
                            all = false;
                            break;
                        }
                    }
                    if all {
                        return Ok(true);
                    }
                }
                _ => (),
            }
        }
        control.step()?;
        Ok(false)
    }
}

fn partial_scalar(value: &BsonValue) -> bool {
    !matches!(
        value,
        BsonValue::Array(_)
            | BsonValue::Document(_)
            | BsonValue::JavaScript(_)
            | BsonValue::RegularExpression(_)
    )
}

fn scalar_bytes(value: &BsonValue) -> usize {
    match value {
        BsonValue::String(value) => value.len(),
        BsonValue::Binary(value) => value.bytes().len(),
        _ => 0,
    }
}

fn same_path(
    left: &[String],
    right: &[String],
    control: &mut dyn MatchControl,
) -> EngineResult<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        control.step()?;
        control.comparison_bytes(left.len() + right.len())?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
