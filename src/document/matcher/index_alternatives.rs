//! Necessary per-path witnesses, not boolean simplification or index authority.

use super::*;

static ABSENT: BsonValue = BsonValue::Null;

impl DocumentMatcher {
    pub(crate) fn has_index_alternatives(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<bool> {
        for clause in &self.clauses {
            check()?;
            match clause {
                Clause::Logical {
                    kind: Logical::Or, ..
                } => return Ok(true),
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if child.has_index_alternatives(check)? {
                            return Ok(true);
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(false)
    }

    /// A conjunction can use any necessary witness, but an OR needs one from
    /// every branch. Never intersect values: an array may match different AND
    /// operands through different elements. All returned values are borrowed;
    /// the index compiler still validates scalar/key/sparse/tuple eligibility.
    pub(in crate::document) fn alternatives_for_index_path<'a>(
        &'a self,
        requested: &[String],
        max_values: usize,
        control: &mut dyn MatchControl,
    ) -> EngineResult<Option<Vec<&'a BsonValue>>> {
        let mut witness = Witness {
            values: Vec::new(),
            remaining: max_values,
            control,
        };
        witness.control.step()?;
        if witness.collect(self, requested)? {
            Ok(Some(witness.values))
        } else {
            Ok(None)
        }
    }
}

struct Witness<'a, 'b> {
    values: Vec<&'a BsonValue>,
    remaining: usize,
    control: &'b mut dyn MatchControl,
}

impl<'a> Witness<'a, '_> {
    fn reserve(&mut self, count: usize) -> EngineResult<bool> {
        self.control.step()?;
        if count == 0 || count > self.remaining {
            return Ok(false);
        }
        // Failed branches do not refund work. The one shared vector bounds
        // live references without cloning the AST or expanding it into DNF.
        self.remaining -= count;
        self.control
            .allocation(32 + count * std::mem::size_of::<&BsonValue>())?;
        self.values.try_reserve_exact(count).map_err(|_| limit())?;
        Ok(true)
    }

    fn collect(
        &mut self,
        matcher: &'a DocumentMatcher,
        requested: &[String],
    ) -> EngineResult<bool> {
        let start = self.values.len();
        for clause in &matcher.clauses {
            self.control.step()?;
            match clause {
                Clause::Field {
                    path, predicates, ..
                } if path == requested => {
                    for predicate in predicates {
                        self.control.step()?;
                        match predicate {
                            Predicate::Equal(value) => {
                                if self.reserve(1)? {
                                    self.values.push(value);
                                    return Ok(true);
                                }
                            }
                            Predicate::Exists(false) => {
                                if self.reserve(1)? {
                                    self.values.push(&ABSENT);
                                    return Ok(true);
                                }
                            }
                            Predicate::In {
                                members,
                                negative: false,
                            } => {
                                if !self.reserve(members.len())? {
                                    continue;
                                }
                                let mut literals = true;
                                for member in members {
                                    self.control.step()?;
                                    if let Member::Literal(value) = member {
                                        self.values.push(value);
                                    } else {
                                        literals = false;
                                        break;
                                    }
                                }
                                if literals {
                                    return Ok(true);
                                }
                                self.values.truncate(start);
                            }
                            _ => (),
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::And,
                    children,
                } => {
                    for child in children {
                        if self.collect(child, requested)? {
                            return Ok(true);
                        }
                    }
                }
                Clause::Logical {
                    kind: Logical::Or,
                    children,
                } => {
                    let mut complete = !children.is_empty();
                    for child in children {
                        if !self.collect(child, requested)? {
                            complete = false;
                            break;
                        }
                    }
                    if complete {
                        return Ok(true);
                    }
                    self.values.truncate(start);
                }
                _ => (),
            }
        }
        debug_assert_eq!(self.values.len(), start);
        Ok(false)
    }
}
