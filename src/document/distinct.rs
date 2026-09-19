//! Source-locked distinct extraction and BSON identity, shared by all adapters.

use std::{collections::HashSet, fmt, sync::Arc};

use super::{BsonDocument, BsonErrorContext, BsonValue, encode_document};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

const MAX_FIELD_BYTES: usize = 1024 * 1024;
const MAX_DEPTH: usize = 100;
const MAX_STEPS: usize = 1_000_000;
const MAX_VALUES: usize = 65_536;
const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn validate_field(field: &str) -> EngineResult<()> {
    if field.len() > MAX_FIELD_BYTES || field.split('.').count() > MAX_DEPTH {
        return Err(limit());
    }
    // Empty components, dollar-prefixed names and numeric components are
    // literal mapping keys in the frozen contract. NUL simply cannot match a
    // valid stored BSON field; it is legal in this command's string value.
    Ok(())
}

/// Incrementally collect distinct values in input encounter order. Missing
/// paths contribute nothing; a final array contributes its immediate members.
/// Intermediate arrays are not traversed. BSON-equal values retain the first
/// representation, including numeric aliases. Inputs are never mutated.
///
/// Retention is bounded to 65,536 values, 8 MiB conservative heap charge per
/// value and 64 MiB total. Any failed push poisons the collector: a partial
/// result cannot subsequently be returned as success.
pub struct DocumentDistinct {
    parts: Vec<String>,
    seen: HashSet<Arc<BsonValue>>,
    values: Vec<Arc<BsonValue>>,
    retained_bytes: usize,
    failed: bool,
}

impl fmt::Debug for DocumentDistinct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocumentDistinct")
            .field("values", &self.values.len())
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl DocumentDistinct {
    pub fn new(field: &str) -> EngineResult<Self> {
        validate_field(field)?;
        Ok(Self {
            parts: field.split('.').map(str::to_owned).collect(),
            seen: HashSet::new(),
            values: Vec::new(),
            retained_bytes: field.len() + MAX_DEPTH * 32 + 512,
            failed: false,
        })
    }

    pub fn push(&mut self, document: &BsonDocument) -> EngineResult<()> {
        self.push_with_check(document, &mut || Ok(()))
    }

    pub fn push_with_check(
        &mut self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<()> {
        let result = (|| {
            self.require_ready()?;
            check()?;
            encode_document(document)
                .map_err(|error| error.into_engine_error(BsonErrorContext::ClientInput))?;
            check()?;
            self.push_inner(document, check, &mut |_| Ok(()))
        })();
        self.failed |= result.is_err();
        result
    }

    pub(crate) fn push_validated_with_check(
        &mut self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
        admit_value: &mut dyn FnMut(&BsonValue) -> EngineResult<()>,
    ) -> EngineResult<()> {
        let result = self
            .require_ready()
            .and_then(|()| self.push_inner(document, check, admit_value));
        self.failed |= result.is_err();
        result
    }

    fn push_inner(
        &mut self,
        document: &BsonDocument,
        check: &mut dyn FnMut() -> EngineResult<()>,
        admit_value: &mut dyn FnMut(&BsonValue) -> EngineResult<()>,
    ) -> EngineResult<()> {
        let mut budget = Budget { steps: 0, check };
        budget.step()?;
        let mut current = document;
        let mut selected = None;
        for (index, part) in self.parts.iter().enumerate() {
            let mut found = None;
            for (name, value) in current.iter() {
                budget.step()?;
                if name == part {
                    found = Some(value);
                    break;
                }
            }
            let Some(value) = found else {
                return Ok(());
            };
            if index + 1 == self.parts.len() {
                selected = Some(value);
            } else if let BsonValue::Document(nested) = value {
                current = nested;
            } else {
                return Ok(());
            }
        }
        if let Some(value) = selected {
            match value {
                BsonValue::Array(values) => {
                    for value in values {
                        self.insert(value, &mut budget, admit_value)?;
                    }
                }
                value => self.insert(value, &mut budget, admit_value)?,
            }
        }
        budget.step()
    }

    fn insert(
        &mut self,
        value: &BsonValue,
        budget: &mut Budget<'_>,
        admit_value: &mut dyn FnMut(&BsonValue) -> EngineResult<()>,
    ) -> EngineResult<()> {
        // Preflight borrowed values before hashing or cloning attacker-sized
        // nested data. The semantic Hash/Eq implementation is engine-owned.
        let bytes = value_bytes(value, budget)? + 128;
        budget.step()?;
        if self.seen.contains(value) {
            return budget.step();
        }
        if self.values.len() >= MAX_VALUES || self.retained_bytes + bytes > MAX_RETAINED_BYTES {
            return Err(limit());
        }
        admit_value(value)?;
        budget.step()?;
        self.seen.try_reserve(1).map_err(allocation)?;
        self.values.try_reserve(1).map_err(allocation)?;
        let value = Arc::new(value.clone());
        budget.step()?;
        self.seen.insert(Arc::clone(&value));
        self.values.push(value);
        self.retained_bytes += bytes;
        budget.step()
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn into_values(self) -> EngineResult<Vec<BsonValue>> {
        self.into_values_with_check(&mut || Ok(()))
    }

    pub fn into_values_with_check(
        self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<BsonValue>> {
        self.require_ready()?;
        check()?;
        drop(self.seen);
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.values.len())
            .map_err(allocation)?;
        for value in self.values {
            check()?;
            values.push(Arc::try_unwrap(value).map_err(|_| {
                EngineError::new(
                    EngineErrorKind::Internal,
                    "distinct value unexpectedly has another owner",
                )
            })?);
        }
        check()?;
        Ok(values)
    }

    fn require_ready(&self) -> EngineResult<()> {
        if self.failed {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "distinct collector cannot continue after a failed input",
            ));
        }
        Ok(())
    }
}

fn allocation(error: std::collections::TryReserveError) -> EngineError {
    EngineError::from_source(
        EngineErrorKind::OutOfMemory,
        "unable to reserve bounded distinct results",
        error,
    )
}

fn limit() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "distinct resource limit exceeded",
    )
}

struct Budget<'a> {
    steps: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl Budget<'_> {
    fn step(&mut self) -> EngineResult<()> {
        (self.check)()?;
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        Ok(())
    }
}

fn document_bytes(document: &BsonDocument, budget: &mut Budget<'_>) -> EngineResult<usize> {
    let mut bytes = 128;
    for (name, value) in document.iter() {
        bytes += name.len() + value_bytes(value, budget)?;
        if bytes > MAX_VALUE_BYTES {
            return Err(limit());
        }
    }
    Ok(bytes)
}

fn value_bytes(value: &BsonValue, budget: &mut Budget<'_>) -> EngineResult<usize> {
    budget.step()?;
    let bytes = 128
        + match value {
            BsonValue::String(value) => value.len(),
            BsonValue::Document(value) => document_bytes(value, budget)?,
            BsonValue::Array(values) => {
                let mut bytes = 0;
                for value in values {
                    bytes += value_bytes(value, budget)?;
                    if bytes > MAX_VALUE_BYTES {
                        return Err(limit());
                    }
                }
                bytes
            }
            BsonValue::Binary(value) => value.bytes().len(),
            BsonValue::RegularExpression(value) => value.pattern().len() + value.options().len(),
            BsonValue::JavaScript(value) => {
                value.code().len()
                    + value
                        .scope()
                        .map_or(Ok(0), |scope| document_bytes(scope, budget))?
            }
            _ => 0,
        };
    if bytes > MAX_VALUE_BYTES {
        return Err(limit());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(value: BsonValue) -> BsonDocument {
        BsonDocument::from_entries([("private-field", value)]).unwrap()
    }

    #[test]
    fn first_bson_representation_order_and_array_boundaries_are_preserved() {
        let original = doc(BsonValue::Array(vec![
            BsonValue::Int64(1),
            BsonValue::Double(1.0),
            BsonValue::Boolean(true),
            BsonValue::Array(vec![BsonValue::Int32(2)]),
            BsonValue::Null,
        ]));
        let before = encode_document(&original).unwrap();
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        distinct.push(&original).unwrap();
        distinct.push(&original).unwrap();
        assert!(!format!("{distinct:?}").contains("private-field"));
        let values = distinct.into_values().unwrap();
        assert!(matches!(values[0], BsonValue::Int64(1)));
        assert_eq!(values.len(), 4);
        assert_eq!(encode_document(&original).unwrap(), before);
        let mut nested = DocumentDistinct::new("private-field.0").unwrap();
        nested.push(&original).unwrap();
        assert!(nested.into_values().unwrap().is_empty());
        let mut missing = DocumentDistinct::new("private-field\0").unwrap();
        missing.push(&original).unwrap();
        assert!(missing.into_values().unwrap().is_empty());
    }

    #[test]
    fn interruptions_poison_the_collector_without_successful_partial_results() {
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        distinct.push(&doc(BsonValue::Int32(1))).unwrap();
        let error = distinct
            .push_with_check(&doc(BsonValue::Int32(2)), &mut || {
                Err(EngineError::new(EngineErrorKind::Cancelled, "cancelled"))
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(
            distinct.push(&doc(BsonValue::Int32(3))).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            distinct.into_values().unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        let distinct = DocumentDistinct::new("private-field").unwrap();
        assert_eq!(
            distinct
                .into_values_with_check(&mut || Err(EngineError::deadline_exceeded("expired")))
                .unwrap_err()
                .kind(),
            EngineErrorKind::DeadlineExceeded
        );
    }

    #[test]
    fn count_memory_field_and_work_limits_are_hard_failures() {
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        distinct
            .push(&doc(BsonValue::Array(
                (0..MAX_VALUES)
                    .map(|index| BsonValue::Int32(index as i32))
                    .collect(),
            )))
            .unwrap();
        // Duplicates do not spend another result slot at the boundary.
        distinct.push(&doc(BsonValue::Int64(0))).unwrap();
        assert_eq!(
            distinct
                .push(&doc(BsonValue::Int32(MAX_VALUES as i32)))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(distinct.into_values().is_err());
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        assert_eq!(
            distinct
                .push(&doc(BsonValue::from("x".repeat(MAX_VALUE_BYTES))))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        distinct.retained_bytes = MAX_RETAINED_BYTES;
        assert_eq!(
            distinct.push(&doc(BsonValue::Int32(1))).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert!(DocumentDistinct::new(&"x".repeat(MAX_FIELD_BYTES + 1)).is_err());
        assert!(DocumentDistinct::new(&"x.".repeat(MAX_DEPTH)).is_err());
        let mut budget = Budget {
            steps: MAX_STEPS,
            check: &mut || Ok(()),
        };
        assert_eq!(
            budget.step().unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn output_admission_failure_never_returns_a_partial_collection() {
        let mut distinct = DocumentDistinct::new("private-field").unwrap();
        let mut admitted = 0;
        let result = distinct.push_validated_with_check(
            &doc(BsonValue::Array(vec![
                BsonValue::Int32(1),
                BsonValue::Int32(2),
            ])),
            &mut || Ok(()),
            &mut |_| {
                admitted += 1;
                if admitted > 1 { Err(limit()) } else { Ok(()) }
            },
        );
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
        assert!(distinct.into_values().is_err());
    }
}
