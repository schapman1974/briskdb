#![no_main]

use briskdb::{
    core::{EngineError, EngineErrorKind, EngineResult},
    document::{
        BsonDecimal128, BsonDocument, BsonValue, DocumentUpdater, decode_document, encode_document,
    },
};
use libfuzzer_sys::fuzz_target;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}

fn same(left: EngineResult<BsonDocument>, right: EngineResult<BsonDocument>) {
    match (left, right) {
        (Ok(left), Ok(right)) => {
            assert_eq!(
                encode_document(&left).unwrap(),
                encode_document(&right).unwrap()
            );
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.kind(), right.kind());
            assert_eq!(left.to_string(), right.to_string());
        }
        _ => panic!("update must be deterministic"),
    }
}

fn exercise(spec: &BsonDocument, source: &BsonDocument, cancel_after: usize) {
    let before = encode_document(source).unwrap();
    let spec_before = encode_document(spec).unwrap();
    let plain = DocumentUpdater::compile(spec);
    let checked = DocumentUpdater::compile_with_check(spec, &mut || Ok(()));
    match (plain, checked) {
        (Ok(updater), Ok(checked)) => {
            let first = updater.apply(source);
            if let Ok(result) = &first {
                let bytes = encode_document(result).unwrap();
                assert_eq!(
                    encode_document(&decode_document(&bytes).unwrap()).unwrap(),
                    bytes
                );
                if let Some(id) = source.get_first("_id") {
                    assert!(id.representation_eq(result.get_first("_id").unwrap()));
                }
            }
            same(first, checked.apply_with_check(source, &mut || Ok(())));
            let mut checks = 0;
            let cancelled = updater.apply_with_check(source, &mut || {
                checks += 1;
                if checks > cancel_after {
                    Err(EngineError::new(
                        EngineErrorKind::Cancelled,
                        "fuzz cancellation",
                    ))
                } else {
                    Ok(())
                }
            });
            if checks > cancel_after {
                assert_eq!(cancelled.unwrap_err().kind(), EngineErrorKind::Cancelled);
            } else {
                same(cancelled, updater.apply(source));
            }
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.kind(), right.kind());
            assert_eq!(left.to_string(), right.to_string());
        }
        _ => panic!("update compilation must be deterministic"),
    }
    assert_eq!(encode_document(source).unwrap(), before);
    assert_eq!(encode_document(spec).unwrap(), spec_before);
}

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    let byte = |i| data.get(i).copied().unwrap_or(0);
    let cancel_after = usize::from(byte(0) % 64);
    if let Ok(envelope) = decode_document(data) {
        if let (Some(BsonValue::Document(spec)), Some(BsonValue::Document(source))) =
            (envelope.get_first("update"), envelope.get_first("document"))
        {
            exercise(spec, source, cancel_after);
        }
    }

    // Reach every supported operator on every input, without needing the fuzzer
    // to first discover a valid BSON envelope. Keep generated arrays bounded.
    let number = BsonValue::Int32(i32::from(byte(1)) - 128);
    let bits = std::array::from_fn(|i| byte(i + 2));
    let numeric = match byte(10) % 4 {
        0 => BsonValue::Double(f64::from_bits(u64::from_le_bytes(bits))),
        1 => BsonValue::Int64(i64::from_le_bytes(bits)),
        2 => BsonValue::Decimal128(BsonDecimal128::from_bid(std::array::from_fn(|i| {
            byte(i + 2)
        }))),
        _ => number.clone(),
    };
    let values: Vec<_> = data
        .iter()
        .take(32)
        .map(|v| BsonValue::Int32(i32::from(*v) - 128))
        .collect();
    let source = doc([
        ("_id", BsonValue::Int64(i64::from(byte(2)))),
        ("value", numeric),
        ("items", BsonValue::Array(values.clone())),
        (
            "nested",
            BsonValue::Document(doc([("value", number.clone())])),
        ),
    ]);
    let each = BsonValue::Document(doc([("$each", BsonValue::Array(values))]));
    let push = BsonValue::Document(doc([
        ("$each", BsonValue::Array(vec![number.clone()])),
        ("$position", BsonValue::Int32(i32::from(byte(3)) - 128)),
        ("$slice", BsonValue::Int32(i32::from(byte(4)) - 128)),
        (
            "$sort",
            BsonValue::Int32(if byte(5) & 1 == 0 { 1 } else { -1 }),
        ),
    ]));
    for (operator, path, value) in [
        ("$set", "nested.value", number.clone()),
        ("$unset", "nested.value", BsonValue::Null),
        ("$min", "value", number.clone()),
        ("$max", "value", number.clone()),
        ("$inc", "value", number.clone()),
        (
            "$pop",
            "items",
            BsonValue::Int32(if byte(6) & 1 == 0 { 1 } else { -1 }),
        ),
        ("$rename", "nested.value", BsonValue::from("renamed")),
        ("$addToSet", "items", each),
        ("$pullAll", "items", BsonValue::Array(vec![number.clone()])),
        (
            "$pull",
            "items",
            BsonValue::Document(doc([("$gte", number.clone())])),
        ),
        ("$push", "items", push),
    ] {
        exercise(
            &doc([(operator, BsonValue::Document(doc([(path, value)])))]),
            &source,
            cancel_after,
        );
    }
    // Arbitrary paths include numeric array offsets, positional markers,
    // immutable _id and invalid traversals. Envelopes also reach path conflicts.
    let path = String::from_utf8_lossy(&data[..data.len().min(256)]);
    if let Ok(fields) = BsonDocument::from_entries([(path.as_ref(), number)]) {
        exercise(
            &doc([("$set", BsonValue::Document(fields))]),
            &source,
            cancel_after,
        );
    }
});
