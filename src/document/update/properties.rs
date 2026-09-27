//! Independent, shrinkable models for update transforms and failure boundaries.
use super::*;
use crate::document::{BsonDecimal128, decode_document};
use proptest::prelude::*;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn spec(operator: &str, path: &str, value: BsonValue) -> BsonDocument {
    doc([(operator, BsonValue::Document(doc([(path, value)])))])
}

fn value() -> BoxedStrategy<BsonValue> {
    prop_oneof![
        Just(BsonValue::Null),
        any::<bool>().prop_map(BsonValue::Boolean),
        any::<i32>().prop_map(BsonValue::Int32),
        any::<i64>().prop_map(BsonValue::Int64),
        any::<u64>().prop_map(|bits| BsonValue::Double(f64::from_bits(bits))),
        any::<[u8; 16]>().prop_map(|bits| BsonValue::Decimal128(BsonDecimal128::from_bid(bits))),
        "[a-zA-Z0-9]{0,24}".prop_map(BsonValue::String),
    ]
    .prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(BsonValue::Array),
            prop::collection::vec(inner, 0..4).prop_map(|values| BsonValue::Document(
                BsonDocument::from_entries(
                    values
                        .into_iter()
                        .enumerate()
                        .map(|(i, value)| (format!("field{i}"), value))
                )
                .unwrap()
            )),
        ]
    })
    .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn integer_increment_matches_checked_arithmetic(left in any::<i64>(), right in any::<i64>()) {
        let source = doc([("_id", BsonValue::Int64(7)), ("value", BsonValue::Int64(left))]);
        let before = encode_document(&source).unwrap();
        let updater = DocumentUpdater::compile(&spec("$inc", "value", BsonValue::Int64(right))).unwrap();
        let result = updater.apply(&source);
        match left.checked_add(right) {
            Some(expected) => prop_assert!(result.unwrap().get_first("value").unwrap()
                .representation_eq(&BsonValue::Int64(expected))),
            None => {
                let error = result.unwrap_err();
                prop_assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
                prop_assert_eq!(error.source().unwrap().downcast_ref::<DocumentUpdateError>(),
                    Some(&DocumentUpdateError::BadValue));
            }
        }
        prop_assert_eq!(encode_document(&source).unwrap(), before);
    }

    #[test]
    fn array_update_sequences_match_ordered_vector_model(
        initial in prop::collection::vec(-4_i16..5, 0..24),
        operations in prop::collection::vec((0_u8..5, -4_i16..5), 0..32),
    ) {
        let mut expected = initial;
        let array = |values: &[i16]| BsonValue::Array(values.iter()
            .map(|v| BsonValue::Int32(i32::from(*v))).collect());
        let mut source = doc([("_id", BsonValue::Double(-0.0)), ("items", array(&expected))]);
        for (operation, number) in operations {
            let value = BsonValue::Int32(i32::from(number));
            let (operator, operand) = match operation {
                0 => { expected.push(number); ("$push", value) }
                1 => {
                    if !expected.contains(&number) { expected.push(number); }
                    ("$addToSet", value)
                }
                2 => { expected.retain(|v| *v != number); ("$pullAll", BsonValue::Array(vec![value])) }
                3 => { expected.pop(); ("$pop", BsonValue::Int32(1)) }
                _ => {
                    if !expected.is_empty() { expected.remove(0); }
                    ("$pop", BsonValue::Int32(-1))
                }
            };
            let before = encode_document(&source).unwrap();
            let updater = DocumentUpdater::compile(&spec(operator, "items", operand)).unwrap();
            let result = updater.apply(&source).unwrap();
            prop_assert!(result.get_first("items").unwrap().representation_eq(&array(&expected)));
            prop_assert!(result.get_first("_id").unwrap().representation_eq(&BsonValue::Double(-0.0)));
            prop_assert_eq!(encode_document(&source).unwrap(), before);
            source = result;
        }
    }

    #[test]
    fn set_and_unset_are_idempotent_and_preserve_unrelated_bson(payload in value(), untouched in value()) {
        let source = doc([("_id", BsonValue::Int64(9)), ("untouched", untouched.clone())]);
        let before = encode_document(&source).unwrap();
        let setter = DocumentUpdater::compile(&spec("$set", "nested.value", payload.clone())).unwrap();
        let first = setter.apply(&source).unwrap();
        let bytes = encode_document(&first).unwrap();
        prop_assert_eq!(&encode_document(&setter.apply(&first).unwrap()).unwrap(), &bytes);
        prop_assert_eq!(encode_document(&decode_document(&bytes).unwrap()).unwrap(), bytes);
        prop_assert!(first.get_first("untouched").unwrap().representation_eq(&untouched));
        let BsonValue::Document(nested) = first.get_first("nested").unwrap() else { unreachable!() };
        prop_assert!(nested.get_first("value").unwrap().representation_eq(&payload));
        let unsetter = DocumentUpdater::compile(&spec("$unset", "nested.value", BsonValue::Null)).unwrap();
        let removed = unsetter.apply(&first).unwrap();
        prop_assert_eq!(encode_document(&unsetter.apply(&removed).unwrap()).unwrap(), encode_document(&removed).unwrap());
        prop_assert!(removed.get_first("untouched").unwrap().representation_eq(&untouched));
        prop_assert_eq!(encode_document(&source).unwrap(), before);
    }

    #[test]
    fn mid_update_errors_and_cancellation_never_mutate_input(payload in value(), stop in any::<u16>()) {
        let source = doc([("_id", BsonValue::Int64(1)), ("bad", BsonValue::Boolean(true)),
            ("payload", payload.clone())]);
        let before = encode_document(&source).unwrap();
        let bad = doc([
            ("$set", BsonValue::Document(doc([("first", payload.clone())]))),
            ("$inc", BsonValue::Document(doc([("bad", BsonValue::Int32(1))]))),
        ]);
        let error = DocumentUpdater::compile(&bad).unwrap().apply(&source).unwrap_err();
        prop_assert_eq!(error.source().unwrap().downcast_ref::<DocumentUpdateError>(),
            Some(&DocumentUpdateError::TypeMismatch));
        prop_assert_eq!(&encode_document(&source).unwrap(), &before);
        let update = doc([
            ("$set", BsonValue::Document(doc([("first", payload)]))),
            ("$unset", BsonValue::Document(doc([("payload", BsonValue::Null)]))),
        ]);
        let updater = DocumentUpdater::compile(&update).unwrap();
        // Sample the full execution, including checkpoints after mutations,
        // even when a nested input requires many preliminary validation calls.
        let mut total = 0;
        let expected = updater.apply_with_check(&source, &mut || { total += 1; Ok(()) }).unwrap();
        let stop = usize::from(stop) % (total + 1);
        let mut checks = 0;
        let result = updater.apply_with_check(&source, &mut || {
            checks += 1;
            if checks > stop { Err(EngineError::new(EngineErrorKind::Cancelled, "property cancellation")) }
            else { Ok(()) }
        });
        if checks > stop { prop_assert_eq!(result.unwrap_err().kind(), EngineErrorKind::Cancelled); }
        else { prop_assert_eq!(encode_document(&result.unwrap()).unwrap(), encode_document(&expected).unwrap()); }
        prop_assert_eq!(encode_document(&source).unwrap(), before);
    }
}
