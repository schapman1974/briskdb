#![no_main]

use briskdb::document::{
    BsonDocument, BsonValue, DocumentDistinct, decode_document, encode_document,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(envelope) = decode_document(data) {
        if let (
            Some(BsonValue::String(field)),
            Some(BsonValue::Document(left)),
            Some(BsonValue::Document(right)),
        ) = (
            envelope.get_first("field"),
            envelope.get_first("left"),
            envelope.get_first("right"),
        ) {
            let before = encode_document(&envelope).unwrap();
            if let (Ok(mut once), Ok(mut repeated)) =
                (DocumentDistinct::new(field), DocumentDistinct::new(field))
            {
                let first = once.push(left).and_then(|()| once.push(right));
                let second = repeated
                    .push(left)
                    .and_then(|()| repeated.push(left))
                    .and_then(|()| repeated.push(right))
                    .and_then(|()| repeated.push(right));
                if first.is_ok() && second.is_ok() {
                    let encode = |collector: DocumentDistinct| {
                        encode_document(
                            &BsonDocument::from_entries([(
                                "values",
                                BsonValue::Array(collector.into_values().unwrap()),
                            )])
                            .unwrap(),
                        )
                        .unwrap()
                    };
                    assert_eq!(encode(once), encode(repeated));
                }
            }
            assert_eq!(before, encode_document(&envelope).unwrap());
        }
    }
    if let Ok(mut distinct) = DocumentDistinct::new(&String::from_utf8_lossy(data)) {
        distinct.push(&BsonDocument::new()).unwrap();
        assert!(distinct.into_values().unwrap().is_empty());
    }
});
