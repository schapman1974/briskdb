#![no_main]

use briskdb::document::{BsonDocument, BsonValue, DocumentMatcher, decode_document};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(envelope) = decode_document(data) {
        if let (Some(BsonValue::Document(query)), Some(BsonValue::Document(document))) =
            (envelope.get_first("query"), envelope.get_first("document"))
        {
            if let Ok(matcher) = DocumentMatcher::compile(query) {
                let _ = matcher.matches(document);
            }
        }
    }
    // Also reach the regex parser/normalizer without needing valid BSON bytes.
    let split = data.len().min(256) / 2;
    let pattern = String::from_utf8_lossy(&data[..split]).into_owned();
    let value = String::from_utf8_lossy(&data[split..data.len().min(512)]).into_owned();
    let expression = BsonDocument::from_entries([
        ("$regex", BsonValue::String(pattern)),
        ("$options", BsonValue::String("i".into())),
    ])
    .unwrap();
    let query = BsonDocument::from_entries([("v", BsonValue::Document(expression))]).unwrap();
    let document = BsonDocument::from_entries([("v", BsonValue::String(value))]).unwrap();
    if let Ok(matcher) = DocumentMatcher::compile(&query) {
        let _ = matcher.matches(&document);
    }
});
