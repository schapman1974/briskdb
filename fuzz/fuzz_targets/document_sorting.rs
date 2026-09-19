#![no_main]

use briskdb::document::{
    BsonDocument, BsonValue, DocumentSorter, decode_document, encode_document,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(envelope) = decode_document(data) {
        if let (
            Some(BsonValue::Document(spec)),
            Some(BsonValue::Document(left)),
            Some(BsonValue::Document(right)),
        ) = (
            envelope.get_first("sort"),
            envelope.get_first("left"),
            envelope.get_first("right"),
        ) {
            let before = encode_document(&envelope).unwrap();
            if let Ok(sorter) = DocumentSorter::compile(spec) {
                if let (Ok(a), Ok(b)) = (sorter.key(left), sorter.key(right)) {
                    assert_eq!(a.cmp(&b), b.cmp(&a).reverse());
                    assert_eq!(a, a.clone());
                    assert_eq!(sorter.key(left).unwrap(), a);
                    assert_eq!(a == b, a.cmp(&b).is_eq());
                }
            }
            assert_eq!(encode_document(&envelope).unwrap(), before);
        }
    }
    let name = String::from_utf8_lossy(&data[..data.len().min(1024)]);
    if let Ok(spec) = BsonDocument::from_entries([(name.as_ref(), BsonValue::Int32(1))]) {
        if let Ok(sorter) = DocumentSorter::compile(&spec) {
            let _ = sorter.key(&BsonDocument::new());
        }
    }
});
