#![no_main]

use briskdb::document::{
    BsonDocument, BsonValue, DocumentProjector, decode_document, encode_document,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > 1024 * 1024 {
        return;
    }
    if let Ok(envelope) = decode_document(data) {
        if let (Some(BsonValue::Document(spec)), Some(BsonValue::Document(document))) = (
            envelope.get_first("projection"),
            envelope.get_first("document"),
        ) {
            if let Ok(projector) = DocumentProjector::compile(spec) {
                let before = encode_document(document).unwrap();
                if let Ok(projected) = projector.project(document) {
                    let bytes = encode_document(&projected).unwrap();
                    assert!(bytes.len() <= before.len());
                    assert!(
                        decode_document(&bytes)
                            .unwrap()
                            .representation_eq(&projected)
                    );
                }
                assert_eq!(encode_document(document).unwrap(), before);
            }
        }
    }
    // Reach path validation and collisions without a valid BSON envelope.
    let name = String::from_utf8_lossy(&data[..data.len().min(1024)]);
    if let Ok(spec) = BsonDocument::from_entries([(name.as_ref(), BsonValue::Int32(1))]) {
        if let Ok(projector) = DocumentProjector::compile(&spec) {
            let _ = projector.project(&BsonDocument::new());
        }
    }
});
