//! Bounded insertion, stable whole-value/field sorting, and slicing for $push.

use std::cmp::Ordering;

use super::{
    Action, BsonDocument, BsonValue, Budget, DocumentUpdateError, EngineResult, MAX_RETAINED_BYTES,
    existing_document_value, limit, write_document,
};
use crate::document::number::CanonicalNumber;

const MAX_SORT_FIELDS: usize = 32;
static NULL: BsonValue = BsonValue::Null;

// Array indices/slices only need a sign and a magnitude capped at usize::MAX.
// Large finite integer Decimal128/double values clamp without BigInt expansion.
#[derive(Clone, Copy)]
struct Integer {
    negative: bool,
    magnitude: usize,
}

impl Integer {
    fn compile(value: &BsonValue) -> EngineResult<Self> {
        let Some(CanonicalNumber::Finite(number)) = value.canonical_number() else {
            return Err(DocumentUpdateError::BadValue.error());
        };
        if number.exponent_two() < 0 || number.exponent_five() < 0 {
            return Err(DocumentUpdateError::BadValue.error());
        }
        let magnitude = usize::try_from(number.coefficient())
            .ok()
            .and_then(|coefficient| {
                2usize
                    .checked_pow(number.exponent_two() as u32)
                    .and_then(|power| coefficient.checked_mul(power))
            })
            .and_then(|value| {
                5usize
                    .checked_pow(number.exponent_five() as u32)
                    .and_then(|power| value.checked_mul(power))
            })
            .unwrap_or(usize::MAX);
        Ok(Self {
            negative: number.is_negative(),
            magnitude,
        })
    }

    fn position(self, length: usize) -> usize {
        if self.negative {
            length.saturating_sub(self.magnitude)
        } else {
            self.magnitude.min(length)
        }
    }
}

enum Sort {
    Value { descending: bool },
    Fields(Vec<(Vec<String>, bool)>),
}

fn direction(value: &BsonValue) -> Option<bool> {
    if *value == BsonValue::Int32(1) {
        Some(false)
    } else if *value == BsonValue::Int32(-1) {
        Some(true)
    } else {
        None
    }
}

impl Sort {
    fn compile(
        value: &BsonValue,
        retained: &mut usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        if let Some(descending) = direction(value) {
            return Ok(Self::Value { descending });
        }
        let BsonValue::Document(fields) = value else {
            return Err(DocumentUpdateError::BadValue.error());
        };
        if fields.is_empty() {
            return Err(DocumentUpdateError::BadValue.error());
        }
        if fields.len() > MAX_SORT_FIELDS {
            return Err(limit());
        }
        let mut result = Vec::new();
        for (field, value) in fields.iter() {
            check()?;
            let descending =
                direction(value).ok_or_else(|| DocumentUpdateError::BadValue.error())?;
            let components = field.split('.').count();
            if components > 100 {
                return Err(limit());
            }
            *retained = retained
                .checked_add(components * 128 + field.len() + 128)
                .filter(|bytes| *bytes <= MAX_RETAINED_BYTES)
                .ok_or_else(limit)?;
            // Sort selectors are not update paths: the frozen reference accepts
            // empty/$-prefixed field names and follows documents, not arrays.
            result.push((field.split('.').map(str::to_owned).collect(), descending));
        }
        Ok(Self::Fields(result))
    }

    fn compare(
        &self,
        left: &BsonValue,
        right: &BsonValue,
        budget: &mut Budget<'_>,
    ) -> EngineResult<Ordering> {
        let compare =
            |left: &BsonValue, right: &BsonValue, descending: bool, budget: &mut Budget<'_>| {
                budget.comparison_value(left)?;
                budget.comparison_value(right)?;
                let order = left.cmp(right);
                budget.step()?;
                Ok(if descending { order.reverse() } else { order })
            };
        match self {
            Self::Value { descending } => compare(left, right, *descending, budget),
            Self::Fields(fields) => {
                for (path, descending) in fields {
                    let left = field_value(left, path, budget)?;
                    let right = field_value(right, path, budget)?;
                    let order = compare(left, right, *descending, budget)?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(Ordering::Equal)
            }
        }
    }

    fn apply(&self, values: &mut Vec<BsonValue>, budget: &mut Budget<'_>) -> EngineResult<()> {
        let length = values.len();
        if length < 2 {
            return Ok(());
        }
        // A fallible stable merge of indices avoids using an error-swallowing or
        // inconsistent sort_by comparator. Charge scratch and output slots first.
        budget.charge(
            length
                .checked_mul(2 * size_of::<usize>() + 128)
                .ok_or_else(limit)?,
        )?;
        let mut source = Vec::new();
        let mut target = Vec::new();
        source.try_reserve_exact(length).map_err(|_| limit())?;
        target.try_reserve_exact(length).map_err(|_| limit())?;
        for index in 0..length {
            budget.step()?;
            source.push(index);
            target.push(0);
        }
        let mut width = 1;
        while width < length {
            let mut start = 0;
            while start < length {
                let middle = start.saturating_add(width).min(length);
                let end = middle.saturating_add(width).min(length);
                let (mut left, mut right) = (start, middle);
                for slot in &mut target[start..end] {
                    budget.step()?;
                    let take_left = right == end
                        || (left < middle
                            && self.compare(
                                &values[source[left]],
                                &values[source[right]],
                                budget,
                            )? != Ordering::Greater);
                    if take_left {
                        *slot = source[left];
                        left += 1;
                    } else {
                        *slot = source[right];
                        right += 1;
                    }
                }
                start = end;
            }
            std::mem::swap(&mut source, &mut target);
            width = width.checked_mul(2).unwrap_or(length);
        }
        let mut sorted = Vec::new();
        sorted.try_reserve_exact(length).map_err(|_| limit())?;
        for index in source {
            budget.step()?;
            sorted.push(std::mem::replace(&mut values[index], BsonValue::Null));
        }
        *values = sorted;
        Ok(())
    }
}

fn field_value<'a>(
    mut value: &'a BsonValue,
    path: &[String],
    budget: &mut Budget<'_>,
) -> EngineResult<&'a BsonValue> {
    for component in path {
        budget.step()?;
        let BsonValue::Document(document) = value else {
            return Ok(&NULL);
        };
        let mut found = None;
        for (field, candidate) in document.iter() {
            budget.step()?;
            if field == component {
                found = Some(candidate);
                break;
            }
        }
        let Some(next) = found else {
            return Ok(&NULL);
        };
        value = next;
    }
    Ok(value)
}

pub(super) struct Push {
    additions: Vec<BsonValue>,
    position: Option<Integer>,
    sort: Option<Sort>,
    slice: Option<Integer>,
}

impl Push {
    pub(super) fn compile(
        value: &BsonValue,
        retained: &mut usize,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Self> {
        let mut push = Self {
            additions: Vec::new(),
            position: None,
            sort: None,
            slice: None,
        };
        if let BsonValue::Document(modifiers) = value {
            if modifiers.iter().any(|(name, _)| name.starts_with('$')) {
                for (name, _) in modifiers.iter() {
                    check()?;
                    if !matches!(name, "$each" | "$position" | "$sort" | "$slice") {
                        return Err(DocumentUpdateError::BadValue.error());
                    }
                }
                let Some(BsonValue::Array(additions)) = modifiers.get_first("$each") else {
                    return Err(DocumentUpdateError::BadValue.error());
                };
                push.additions = additions.clone();
                push.position = modifiers
                    .get_first("$position")
                    .map(Integer::compile)
                    .transpose()?;
                push.slice = modifiers
                    .get_first("$slice")
                    .map(Integer::compile)
                    .transpose()?;
                push.sort = modifiers
                    .get_first("$sort")
                    .map(|value| Sort::compile(value, retained, check))
                    .transpose()?;
                return Ok(push);
            }
        }
        push.additions.push(value.clone());
        Ok(push)
    }

    pub(super) fn apply_document(
        &self,
        document: &mut BsonDocument,
        path: &[String],
        budget: &mut Budget<'_>,
    ) -> EngineResult<()> {
        match existing_document_value(document, path, true, budget)? {
            Some(BsonValue::Array(values)) => self.apply_array(values, budget),
            Some(_) => Err(DocumentUpdateError::BadValue.error()),
            None => {
                let mut values = Vec::new();
                self.apply_array(&mut values, budget)?;
                write_document(
                    document,
                    path,
                    &Action::Set(BsonValue::Array(values)),
                    true,
                    budget,
                )
            }
        }
    }

    fn apply_array(
        &self,
        values: &mut Vec<BsonValue>,
        budget: &mut Budget<'_>,
    ) -> EngineResult<()> {
        let position = self
            .position
            .map_or(values.len(), |position| position.position(values.len()));
        let added = self.additions.len();
        budget.charge(added.checked_mul(128).ok_or_else(limit)?)?;
        values.try_reserve_exact(added).map_err(|_| limit())?;
        for value in &self.additions {
            budget.step()?;
            values.push(budget.clone_value(value)?);
        }
        for _ in position..values.len() {
            budget.step()?;
        }
        values[position..].rotate_right(added);
        if let Some(sort) = &self.sort {
            sort.apply(values, budget)?;
        }
        if let Some(slice) = self.slice {
            let keep = slice.magnitude.min(values.len());
            for _ in values.iter() {
                budget.step()?;
            }
            if slice.negative {
                values.drain(..values.len() - keep);
            } else {
                values.truncate(keep);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{EngineError, EngineErrorKind},
        document::{BsonDecimal128, DocumentMutationError, DocumentUpdater, encode_document},
    };
    use std::error::Error;

    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }
    fn spec(path: &str, value: BsonValue) -> BsonDocument {
        doc([("$push", BsonValue::Document(doc([(path, value)])))])
    }
    fn array(values: &[i32]) -> BsonValue {
        BsonValue::Array(values.iter().copied().map(BsonValue::Int32).collect())
    }
    fn code(error: EngineError) -> i32 {
        let mut source = error.source();
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<DocumentUpdateError>() {
                return error.mongo_code();
            }
            if let Some(error) = cause.downcast_ref::<DocumentMutationError>() {
                return error.mongo_code();
            }
            source = cause.source();
        }
        panic!("untyped error: {error}")
    }

    #[test]
    fn push_modifiers_apply_insert_sort_slice_in_fixed_order_and_preserve_literals() {
        let original = doc([("_id", BsonValue::Int64(7)), ("v", array(&[3, 1]))]);
        let result = DocumentUpdater::compile(&spec(
            "v",
            BsonValue::Document(doc([
                ("$slice", BsonValue::Int32(3)),
                ("$sort", BsonValue::Int32(1)),
                ("$each", array(&[2, 0])),
                ("$position", BsonValue::Double(1.0)),
            ])),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(result.get_first("v"), Some(&array(&[0, 1, 2])));
        assert_eq!(original.get_first("v"), Some(&array(&[3, 1])));
        for (position, expected) in [
            (-1, vec![3, 9, 8, 1]),
            (-99, vec![9, 8, 3, 1]),
            (99, vec![3, 1, 9, 8]),
        ] {
            let result = DocumentUpdater::compile(&spec(
                "v",
                BsonValue::Document(doc([
                    ("$each", array(&[9, 8])),
                    ("$position", BsonValue::Int32(position)),
                ])),
            ))
            .unwrap()
            .apply(&original)
            .unwrap();
            assert_eq!(result.get_first("v"), Some(&array(&expected)));
        }
        let result = DocumentUpdater::compile(&spec("v", array(&[2, 4])))
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            result.get_first("v"),
            Some(&BsonValue::Array(vec![
                BsonValue::Int32(3),
                BsonValue::Int32(1),
                array(&[2, 4])
            ]))
        );
        let result = DocumentUpdater::compile(&spec(
            "new",
            BsonValue::Document(doc([("$each", array(&[]))])),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(result.get_first("new"), Some(&array(&[])));
        for (slice, expected) in [
            (0, vec![]),
            (-1, vec![1]),
            (1, vec![3]),
            (-99, vec![3, 1]),
            (99, vec![3, 1]),
        ] {
            let result = DocumentUpdater::compile(&spec(
                "v",
                BsonValue::Document(doc([
                    ("$each", array(&[])),
                    ("$slice", BsonValue::Int32(slice)),
                ])),
            ))
            .unwrap()
            .apply(&original)
            .unwrap();
            assert_eq!(result.get_first("v"), Some(&array(&expected)));
        }
    }

    #[test]
    fn push_sort_is_stable_with_whole_values_and_frozen_document_selectors() {
        let original = doc([(
            "v",
            BsonValue::Array(vec![
                BsonValue::Int64(1),
                BsonValue::Double(1.0),
                BsonValue::Boolean(true),
            ]),
        )]);
        let result = DocumentUpdater::compile(&spec(
            "v",
            BsonValue::Document(doc([("$each", array(&[])), ("$sort", BsonValue::Int32(1))])),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(
            encode_document(&result).unwrap(),
            encode_document(&original).unwrap()
        );
        let item = |group: &str, score: i32, serial: i32| {
            BsonValue::Document(doc([
                ("group", BsonValue::from(group)),
                ("score", BsonValue::Int32(score)),
                ("serial", BsonValue::Int32(serial)),
            ]))
        };
        let values = vec![
            item("b", 1, 0),
            item("a", 1, 1),
            item("a", 2, 2),
            item("a", 2, 3),
        ];
        let original = doc([("v", BsonValue::Array(values.clone()))]);
        let result = DocumentUpdater::compile(&spec(
            "v",
            BsonValue::Document(doc([
                ("$each", array(&[])),
                (
                    "$sort",
                    BsonValue::Document(doc([
                        ("group", BsonValue::Int32(1)),
                        ("score", BsonValue::Int32(-1)),
                    ])),
                ),
            ])),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(
            result.get_first("v"),
            Some(&BsonValue::Array(vec![
                values[2].clone(),
                values[3].clone(),
                values[1].clone(),
                values[0].clone()
            ]))
        );
        for field in ["", "$x"] {
            let original = doc([(
                "v",
                BsonValue::Array(vec![
                    BsonValue::Document(doc([(field, BsonValue::Int32(2))])),
                    BsonValue::Document(doc([(field, BsonValue::Int32(1))])),
                ]),
            )]);
            let result = DocumentUpdater::compile(&spec(
                "v",
                BsonValue::Document(doc([
                    ("$each", array(&[])),
                    (
                        "$sort",
                        BsonValue::Document(doc([(field, BsonValue::Int32(1))])),
                    ),
                ])),
            ))
            .unwrap()
            .apply(&original)
            .unwrap();
            assert_eq!(
                result.get_first("v"),
                Some(&BsonValue::Array(vec![
                    BsonValue::Document(doc([(field, BsonValue::Int32(1))])),
                    BsonValue::Document(doc([(field, BsonValue::Int32(2))]))
                ]))
            );
        }
        let original = doc([(
            "v",
            BsonValue::Array(vec![
                BsonValue::Document(doc([("a", array(&[2]))])),
                BsonValue::Document(doc([("a", array(&[1]))])),
            ]),
        )]);
        let result = DocumentUpdater::compile(&spec(
            "v",
            BsonValue::Document(doc([
                ("$each", array(&[])),
                (
                    "$sort",
                    BsonValue::Document(doc([("a.0", BsonValue::Int32(1))])),
                ),
            ])),
        ))
        .unwrap()
        .apply(&original)
        .unwrap();
        assert_eq!(
            encode_document(&result).unwrap(),
            encode_document(&original).unwrap()
        );
    }

    #[test]
    fn push_integer_modifiers_clamp_without_expansion_and_validate_eagerly() {
        for (value, negative) in [
            (BsonValue::Double(f64::MAX), false),
            (BsonValue::Double(-f64::MAX), true),
            (
                BsonValue::Decimal128(BsonDecimal128::parse("1E6144").unwrap()),
                false,
            ),
            (
                BsonValue::Decimal128(BsonDecimal128::parse("-1E6144").unwrap()),
                true,
            ),
        ] {
            let integer = Integer::compile(&value).unwrap();
            assert_eq!(integer.negative, negative);
            assert_eq!(integer.magnitude, usize::MAX);
            assert_eq!(integer.position(4), if negative { 0 } else { 4 });
        }
        assert_eq!(
            Integer::compile(&BsonValue::Int64(i64::MIN))
                .unwrap()
                .position(4),
            0
        );
        for value in [
            BsonValue::Boolean(true),
            BsonValue::Double(0.5),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::INFINITY),
            BsonValue::Decimal128(BsonDecimal128::parse("1E-6176").unwrap()),
        ] {
            assert_eq!(code(Integer::compile(&value).err().unwrap()), 2);
        }
        for operand in [
            doc([("$slice", BsonValue::Int32(1))]),
            doc([("$each", BsonValue::Null)]),
            doc([("$each", array(&[])), ("$extra", BsonValue::Int32(1))]),
            doc([
                ("$each", array(&[])),
                ("$position", BsonValue::Boolean(true)),
            ]),
            doc([
                ("$each", array(&[])),
                ("$sort", BsonValue::Document(BsonDocument::new())),
            ]),
            doc([("$each", array(&[])), ("$sort", BsonValue::Int32(0))]),
        ] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&spec("v", BsonValue::Document(operand))).unwrap_err()
                ),
                2
            );
        }
        let fields = BsonDocument::from_entries(
            (0..33).map(|index| (format!("x{index}"), BsonValue::Int32(1))),
        )
        .unwrap();
        assert_eq!(
            DocumentUpdater::compile(&spec(
                "v",
                BsonValue::Document(doc([
                    ("$each", array(&[])),
                    ("$sort", BsonValue::Document(fields))
                ]))
            ))
            .unwrap_err()
            .kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn push_strict_paths_ids_growth_sort_work_and_cancellation_leave_original_untouched() {
        let original = doc([
            ("_id", BsonValue::Document(doc([("v", array(&[1]))]))),
            ("grid", BsonValue::Array(vec![array(&[1])])),
            ("scalar", BsonValue::Null),
        ]);
        let before = encode_document(&original).unwrap();
        for (path, expected) in [
            ("scalar", 2),
            ("scalar.x", 28),
            ("grid.01", 28),
            ("grid.0.x", 28),
            ("_id.v", 66),
        ] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&spec(path, BsonValue::Int32(2)))
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                ),
                expected
            );
            assert_eq!(encode_document(&original).unwrap(), before);
        }
        let result = DocumentUpdater::compile(&spec("grid.3", BsonValue::Int32(2)))
            .unwrap()
            .apply(&original)
            .unwrap();
        assert_eq!(
            result.get_first("grid"),
            Some(&BsonValue::Array(vec![
                array(&[1]),
                BsonValue::Null,
                BsonValue::Null,
                array(&[2])
            ]))
        );
        let mut check = || Ok(());
        let mut budget = Budget {
            bytes: 0,
            comparison_bytes: super::super::MAX_COMPARISON_BYTES - 1,
            steps: 0,
            check: &mut check,
        };
        let mut values = vec![BsonValue::Int32(2), BsonValue::Int32(1)];
        assert_eq!(
            Sort::Value { descending: false }
                .apply(&mut values, &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(values, vec![BsonValue::Int32(2), BsonValue::Int32(1)]);
        let push = Push::compile(&BsonValue::Int32(3), &mut 0, &mut check).unwrap();
        let mut budget = Budget {
            bytes: MAX_RETAINED_BYTES - 1,
            comparison_bytes: 0,
            steps: 0,
            check: &mut check,
        };
        assert_eq!(
            push.apply_array(&mut values, &mut budget)
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(values, vec![BsonValue::Int32(2), BsonValue::Int32(1)]);
        let updater = DocumentUpdater::compile(&spec(
            "grid.0",
            BsonValue::Document(doc([
                ("$each", array(&[4, 3, 2])),
                ("$sort", BsonValue::Int32(1)),
            ])),
        ))
        .unwrap();
        let mut visits = 0;
        let error = updater
            .apply_with_check(&original, &mut || {
                visits += 1;
                if visits == 50 {
                    Err(EngineError::new(EngineErrorKind::Cancelled, "test"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(encode_document(&original).unwrap(), before);
    }
}
