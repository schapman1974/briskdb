use super::*;
use crate::document::{aggregation_numeric::increment_decimal, number::CanonicalNumber};

/// Arithmetic has a fixed-size workspace; no coefficient grows with the update
/// count. Retention is conservatively charged by the caller before evaluation.
fn add(left: &BsonValue, right: &BsonValue) -> EngineResult<BsonValue> {
    use BsonValue::{Decimal128, Double, Int32, Int64};
    Ok(match (left, right) {
        (Decimal128(_), _) | (_, Decimal128(_)) => increment_decimal(left, right),
        (Double(_), _) | (_, Double(_)) => {
            let double = |value: &BsonValue| match value {
                Int32(value) => f64::from(*value),
                Int64(value) => *value as f64,
                Double(value) => *value,
                _ => unreachable!("numeric update"),
            };
            let sum = double(left) + double(right);
            if let Double(original) = left {
                if sum == *original {
                    return Ok(left.clone()); // Includes signed-zero no-ops.
                }
            }
            // New arithmetic NaN bits are not portable across architectures.
            Double(if sum.is_nan() { f64::NAN } else { sum })
        }
        (Int32(left), Int32(right)) => {
            let sum = i64::from(*left) + i64::from(*right);
            i32::try_from(sum).map_or(Int64(sum), Int32)
        }
        _ => {
            let integer = |value: &BsonValue| match value {
                Int32(value) => i64::from(*value),
                Int64(value) => *value,
                _ => unreachable!("numeric update"),
            };
            Int64(
                integer(left)
                    .checked_add(integer(right))
                    .ok_or_else(|| DocumentUpdateError::BadValue.error())?,
            )
        }
    })
}

pub(super) fn apply(
    document: &mut BsonDocument,
    path: &[String],
    operand: &BsonValue,
    budget: &mut Budget<'_>,
) -> EngineResult<bool> {
    budget.step()?;
    // Two bounded decimal operands, result, conversion text and context. Charge
    // before any formatting/allocation. This is a logical bound, not process RSS.
    budget.charge(4096)?;
    if let Some(current) = existing_document_value(document, path, true, budget)? {
        if current.canonical_number().is_none() {
            return Err(DocumentUpdateError::TypeMismatch.error());
        }
        let force_modified = current.canonical_number() == Some(CanonicalNumber::NaN);
        let result = add(current, operand)?;
        budget.step()?;
        *current = result;
        Ok(force_modified)
    } else {
        // A missing field receives the operand itself, retaining Int64 width,
        // negative zero and signaling NaN; it is not arithmetic against zero.
        write_document(document, path, &Action::Set(operand.clone()), true, budget)?;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::BsonDecimal128;

    fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }
    fn decimal(value: &str) -> BsonValue {
        BsonValue::Decimal128(BsonDecimal128::parse(value).unwrap())
    }
    fn updater(path: &str, value: BsonValue) -> DocumentUpdater {
        DocumentUpdater::compile(&doc([("$inc", BsonValue::Document(doc([(path, value)])))]))
            .unwrap()
    }
    fn same(left: BsonValue, right: BsonValue) {
        assert_eq!(
            encode_document(&doc([("v", left)])).unwrap(),
            encode_document(&doc([("v", right)])).unwrap()
        );
    }
    fn code(error: EngineError) -> i32 {
        error
            .source()
            .unwrap()
            .downcast_ref::<DocumentUpdateError>()
            .unwrap()
            .mongo_code()
    }

    #[test]
    fn numeric_promotion_preserves_width_and_rejects_integer_overflow() {
        use BsonValue::{Double, Int32, Int64};
        // Noncanonical finite BID coefficients denote zero. An equal rounded
        // result retains the original encoding, rather than canonicalizing it.
        for sign in [0, 1_u128 << 127] {
            let value = BsonValue::Decimal128(BsonDecimal128::from_bid(
                (sign | (6176_u128 << 113) | ((1_u128 << 113) - 1)).to_le_bytes(),
            ));
            same(add(&value, &Int32(0)).unwrap(), value.clone());
            same(add(&value, &Int32(1)).unwrap(), decimal("1"));
        }
        for (left, right, expected) in [
            (Int32(1), Int32(2), Int32(3)),
            (Int32(i32::MAX), Int32(1), Int64(i64::from(i32::MAX) + 1)),
            (Int32(i32::MIN), Int32(-1), Int64(i64::from(i32::MIN) - 1)),
            (Int64(1), Int32(0), Int64(1)),
            (Int32(1), Int64(-1), Int64(0)),
            (Int64(i64::MAX), Int64(-i64::MAX), Int64(0)),
            (Int64(2), Double(0.5), Double(2.5)),
            (Double(-0.0), Double(0.0), Double(-0.0)),
            (Double(1e20), Int32(1), Double(1e20)),
            (Double(f64::MAX), Double(f64::MAX), Double(f64::INFINITY)),
        ] {
            same(add(&left, &right).unwrap(), expected);
        }
        for (left, right) in [
            (Int64(i64::MAX), Int32(1)),
            (Int64(i64::MIN), Int32(-1)),
            (Int64(i64::MAX), Int64(i64::MAX)),
        ] {
            assert_eq!(code(add(&left, &right).unwrap_err()), 2);
        }
        for (left, right, expected) in [
            (decimal("1.00"), decimal("2.5"), decimal("3.50")),
            (decimal("2"), Double(0.1), decimal("2.100000000000000")),
            (decimal("1.00"), decimal("0.000"), decimal("1.00")),
            (decimal("2"), decimal("1E-300"), decimal("2")),
            (decimal("-0E-6176"), Int32(0), decimal("-0E-6176")),
            (
                Int64(i64::MAX),
                decimal("1"),
                decimal("9223372036854775808"),
            ),
            (
                decimal("9.999999999999999999999999999999999E6144"),
                decimal("9.999999999999999999999999999999999E6144"),
                decimal("Infinity"),
            ),
        ] {
            same(add(&left, &right).unwrap(), expected);
        }
    }

    #[test]
    fn missing_paths_copy_operand_and_existing_nans_are_executed() {
        for operand in [
            BsonValue::Int64(1),
            BsonValue::Double(-0.0),
            decimal("sNaN"),
            decimal("-0.00"),
        ] {
            let result = updater("missing.0.v", operand.clone())
                .apply(&BsonDocument::new())
                .unwrap();
            assert_eq!(
                encode_document(&result).unwrap(),
                encode_document(&doc([(
                    "missing",
                    BsonValue::Document(doc([("0", BsonValue::Document(doc([("v", operand)])))]))
                )]))
                .unwrap()
            );
        }
        let original = doc([("a", BsonValue::Array(vec![BsonValue::Int32(2)]))]);
        let result = updater("a.3", BsonValue::Int64(7))
            .apply(&original)
            .unwrap();
        assert_eq!(
            encode_document(&result).unwrap(),
            encode_document(&doc([(
                "a",
                BsonValue::Array(vec![
                    BsonValue::Int32(2),
                    BsonValue::Null,
                    BsonValue::Null,
                    BsonValue::Int64(7)
                ])
            )]))
            .unwrap()
        );
        for value in [
            decimal("NaN"),
            decimal("sNaN"),
            decimal("-NaN"),
            BsonValue::Double(f64::NAN),
            BsonValue::Double(f64::from_bits(0xfff800000000002a)),
        ] {
            let input = doc([("v", value)]);
            let (result, executed) = updater("v", BsonValue::Int32(0))
                .apply_for_write(&input, &mut || Ok(()))
                .unwrap();
            assert!(executed);
            assert_eq!(
                result.get_first("v").unwrap().canonical_number(),
                Some(CanonicalNumber::NaN)
            );
        }
        let input = doc([("v", decimal("1.00"))]);
        let (result, executed) = updater("v", decimal("0.000"))
            .apply_for_write(&input, &mut || Ok(()))
            .unwrap();
        assert!(!executed);
        assert_eq!(
            encode_document(&result).unwrap(),
            encode_document(&input).unwrap()
        );
    }

    #[test]
    fn invalid_numeric_updates_are_eager_atomic_and_keep_identity() {
        for value in [
            BsonValue::Boolean(true),
            BsonValue::Null,
            BsonValue::from("1"),
            BsonValue::Array(vec![]),
            BsonValue::Document(BsonDocument::new()),
        ] {
            assert_eq!(
                code(
                    DocumentUpdater::compile(&doc([(
                        "$inc",
                        BsonValue::Document(doc([("v", value.clone())]))
                    )]))
                    .unwrap_err()
                ),
                14
            );
            let original = doc([("_id", BsonValue::Int64(7)), ("v", value)]);
            let before = encode_document(&original).unwrap();
            let expression = doc([
                (
                    "$set",
                    BsonValue::Document(doc([("marker", BsonValue::Boolean(true))])),
                ),
                (
                    "$inc",
                    BsonValue::Document(doc([("v", BsonValue::Int32(1))])),
                ),
            ]);
            assert_eq!(
                code(
                    DocumentUpdater::compile(&expression)
                        .unwrap()
                        .apply(&original)
                        .unwrap_err()
                ),
                14
            );
            assert_eq!(encode_document(&original).unwrap(), before);
        }
        for (value, path) in [
            (BsonValue::Null, "a.x"),
            (BsonValue::Int32(1), "a.x"),
            (BsonValue::Array(vec![BsonValue::Null]), "a.0.x"),
            (BsonValue::Array(vec![]), "a.01"),
        ] {
            assert_eq!(
                code(
                    updater(path, BsonValue::Int32(1))
                        .apply(&doc([("a", value)]))
                        .unwrap_err()
                ),
                28
            );
        }
        let original = doc([("_id", BsonValue::Int64(7))]);
        assert_eq!(
            encode_document(
                &updater("_id", BsonValue::Double(0.0))
                    .apply(&original)
                    .unwrap()
            )
            .unwrap(),
            encode_document(&original).unwrap()
        );
        let error = updater("_id", BsonValue::Int32(1))
            .apply(&original)
            .unwrap_err();
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<DocumentMutationError>()
                .unwrap()
                .mongo_code(),
            66
        );
        let conflicting = doc([
            (
                "$set",
                BsonValue::Document(doc([("a", BsonValue::Int32(1))])),
            ),
            (
                "$inc",
                BsonValue::Document(doc([("a.v", BsonValue::Int32(1))])),
            ),
        ]);
        assert_eq!(
            code(DocumentUpdater::compile(&conflicting).unwrap_err()),
            40
        );
    }

    #[test]
    fn increment_workspace_and_cancellation_are_preflighted() {
        let original = doc([("v", decimal("1.00"))]);
        let before = encode_document(&original).unwrap();
        let mut check = || Ok(());
        let mut budget = Budget {
            bytes: MAX_RETAINED_BYTES - 4095,
            comparison_bytes: 0,
            steps: 0,
            check: &mut check,
        };
        let mut private = original.clone();
        let error = apply(&mut private, &["v".into()], &decimal("1"), &mut budget).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(encode_document(&private).unwrap(), before);
        let error = updater("v", decimal("1"))
            .apply_with_check(&original, &mut || {
                Err(EngineError::new(EngineErrorKind::Cancelled, "cancelled"))
            })
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert_eq!(encode_document(&original).unwrap(), before);
        assert_eq!(
            updater("a.9999999999999999999999", BsonValue::Int32(1))
                .apply(&doc([("a", BsonValue::Array(vec![]))]))
                .unwrap_err()
                .kind(),
            EngineErrorKind::LimitExceeded
        );
    }
}
