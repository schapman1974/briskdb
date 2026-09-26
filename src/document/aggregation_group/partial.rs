//! Exact partial states only. Rounded arithmetic and encounter-ordered arrays
//! remain on the original ordered stream, never sums of rounded shard totals.

use super::*;

type Position = (u64, u16);

impl Group {
    pub(in crate::document) fn can_partition(&self) -> bool {
        self.accumulators.iter().all(|(_, operator, expression)| {
            matches!(
                operator,
                Operator::First | Operator::Last | Operator::Min | Operator::Max
            ) || matches!(
                (operator, expression),
                (
                    Operator::Sum,
                    Expression::Literal(BsonValue::Int32(_) | BsonValue::Int64(_))
                )
            )
        })
    }
}

enum PartialState {
    Integer(i128),
    One {
        value: Option<BsonValue>,
        position: Position,
    },
}

// Include old/new enum vectors and their spare capacity while handing off to
// the shared finalizer. BSON payloads are moved, never cloned at this boundary.
const STATE_BYTES: usize =
    2 * (std::mem::size_of::<PartialState>() + std::mem::size_of::<State>()) + 128;

struct PartialEntry {
    key: Arc<BsonValue>,
    first: Position,
    states: Vec<PartialState>,
    bytes: usize,
}

pub(crate) struct DocumentPartialGroups {
    lookup: HashMap<Arc<BsonValue>, usize>,
    entries: Vec<PartialEntry>,
    bytes: usize,
}

impl Default for DocumentPartialGroups {
    fn default() -> Self {
        Self {
            lookup: HashMap::new(),
            entries: Vec::new(),
            bytes: 512,
        }
    }
}

impl DocumentPartialGroups {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub(in crate::document) fn push(
        &mut self,
        plan: &Group,
        document: &BsonDocument,
        position: Position,
        retained_elsewhere: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        let mut budget = Budget::new(check);
        budget.charge(self.bytes)?;
        budget.charge(retained_elsewhere)?;
        let source_bytes = memory::document_bytes(document, MAX_BYTES, &mut || budget.step())?;
        budget.charge(source_bytes)?;
        let key = plan
            .key
            .evaluate(document, &mut budget, 1)?
            .unwrap_or(BsonValue::Null);
        let index = if let Some(index) = self.lookup.get(&key) {
            *index
        } else {
            if self.entries.len() == 65_536 {
                return Err(limit());
            }
            let bytes = memory::value_bytes(&key, MAX_BYTES, &mut || budget.step())?
                + 768
                + plan.accumulators.len() * STATE_BYTES;
            budget.charge(bytes)?;
            let key = Arc::new(key);
            let index = self.entries.len();
            self.lookup.insert(Arc::clone(&key), index);
            self.entries.push(PartialEntry {
                key,
                first: position,
                bytes,
                states: plan
                    .accumulators
                    .iter()
                    .map(|(_, operator, _)| {
                        if matches!(operator, Operator::Sum) {
                            PartialState::Integer(0)
                        } else {
                            PartialState::One {
                                value: None,
                                position,
                            }
                        }
                    })
                    .collect(),
            });
            self.bytes += bytes;
            index
        };
        let entry = &mut self.entries[index];
        for ((_, operator, expression), state) in plan.accumulators.iter().zip(&mut entry.states) {
            let value = expression.evaluate(document, &mut budget, 1)?;
            match state {
                PartialState::Integer(total) => {
                    let next = match value {
                        Some(BsonValue::Int32(value)) => i128::from(value),
                        Some(BsonValue::Int64(value)) => i128::from(value),
                        _ => unreachable!("statically proven integer literal"),
                    };
                    *total = total.checked_add(next).ok_or_else(limit)?;
                }
                PartialState::One {
                    value: current,
                    position: current_position,
                } => {
                    // First/last include missing as null; min/max ignore both.
                    let value = if matches!(operator, Operator::First | Operator::Last) {
                        Some(value.unwrap_or(BsonValue::Null))
                    } else {
                        value.filter(|value| !matches!(value, BsonValue::Null))
                    };
                    if select(
                        *operator,
                        current.as_ref(),
                        *current_position,
                        value.as_ref(),
                        position,
                    ) {
                        let added = value_bytes(value.as_ref(), &mut || budget.step())?;
                        budget.charge(added)?;
                        let removed = value_bytes(current.as_ref(), &mut || budget.step())?;
                        *current = value;
                        *current_position = position;
                        entry.bytes = entry.bytes - removed + added;
                        self.bytes = self.bytes - removed + added;
                    }
                }
            }
            budget.step()?;
        }
        budget.step()
    }

    /// Consume partials without copying BSON. `retained_elsewhere` includes all
    /// not-yet-merged shard states, so merging never gets a fresh memory quota.
    pub(in crate::document) fn merge(
        &mut self,
        plan: &Group,
        other: Self,
        retained_elsewhere: usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        check()?;
        if self
            .bytes
            .saturating_add(other.bytes)
            .saturating_add(retained_elsewhere)
            > MAX_BYTES
        {
            return Err(limit());
        }
        drop(other.lookup);
        for incoming in other.entries {
            check()?;
            if let Some(&index) = self.lookup.get(&incoming.key) {
                let entry = &mut self.entries[index];
                let old_bytes = entry.bytes;
                if incoming.first < entry.first {
                    self.lookup.remove(&entry.key);
                    entry.key = Arc::clone(&incoming.key);
                    self.lookup.insert(Arc::clone(&entry.key), index);
                    entry.first = incoming.first;
                }
                for ((_, operator, _), (current, incoming)) in plan
                    .accumulators
                    .iter()
                    .zip(entry.states.iter_mut().zip(incoming.states))
                {
                    check()?;
                    match (current, incoming) {
                        (PartialState::Integer(total), PartialState::Integer(next)) => {
                            *total = total.checked_add(next).ok_or_else(limit)?;
                        }
                        (
                            PartialState::One { value, position },
                            PartialState::One {
                                value: next,
                                position: next_position,
                            },
                        ) => {
                            if select(
                                *operator,
                                value.as_ref(),
                                *position,
                                next.as_ref(),
                                next_position,
                            ) {
                                *value = next;
                                *position = next_position;
                            }
                        }
                        _ => unreachable!("partials share one compiled plan"),
                    }
                }
                entry.bytes = memory::value_bytes(&entry.key, MAX_BYTES, check)?
                    + 768
                    + plan.accumulators.len() * STATE_BYTES;
                for state in &entry.states {
                    check()?;
                    if let PartialState::One { value, .. } = state {
                        entry.bytes += value_bytes(value.as_ref(), check)?;
                    }
                }
                self.bytes = self.bytes - old_bytes + entry.bytes;
            } else {
                if self.entries.len() == 65_536 {
                    return Err(limit());
                }
                self.lookup
                    .insert(Arc::clone(&incoming.key), self.entries.len());
                self.bytes += incoming.bytes;
                self.entries.push(incoming);
            }
        }
        check()
    }

    pub(in crate::document) fn into_groups(
        mut self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Groups> {
        drop(self.lookup);
        // Unstable in-place sort avoids another group-sized allocation. Unique
        // source positions impose the same first-encounter order as the stream.
        let mut error = None;
        self.entries.sort_unstable_by(|left, right| {
            if error.is_none() {
                error = check().err();
            }
            left.first.cmp(&right.first)
        });
        if let Some(error) = error {
            return Err(error);
        }
        let mut entries = Vec::new();
        for entry in self.entries {
            check()?;
            entries.push(Entry {
                key: entry.key,
                bytes: entry.bytes,
                states: entry
                    .states
                    .into_iter()
                    .map(|state| match state {
                        PartialState::Integer(total) => State::Sum(Sum::from_integer(total)),
                        PartialState::One { value, .. } => State::One(value),
                    })
                    .collect(),
            });
        }
        Ok(Groups {
            lookup: HashMap::new(),
            entries,
            bytes: self.bytes,
        })
    }
}

fn value_bytes(
    value: Option<&BsonValue>,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<usize> {
    value
        .map(|value| memory::value_bytes(value, MAX_BYTES, check))
        .transpose()
        .map(|bytes| bytes.unwrap_or(0))
}

fn select(
    operator: Operator,
    current: Option<&BsonValue>,
    current_position: Position,
    next: Option<&BsonValue>,
    next_position: Position,
) -> bool {
    let Some(next) = next else {
        return false;
    };
    let Some(current) = current else {
        return true;
    };
    match operator {
        Operator::First => next_position < current_position,
        Operator::Last => next_position > current_position,
        Operator::Min | Operator::Max => {
            let order = next.cmp(current);
            (order.is_eq() && next_position > current_position)
                || (matches!(operator, Operator::Min) && order.is_lt())
                || (matches!(operator, Operator::Max) && order.is_gt())
        }
        _ => unreachable!("only proven single-value accumulators"),
    }
}
