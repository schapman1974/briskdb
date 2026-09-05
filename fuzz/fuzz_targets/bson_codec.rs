#![no_main]

use briskdb::document::{
    BsonCodecOptions, BsonDocument, BsonErrorKind, BsonValue, CanonicalBsonKey,
    DuplicateFieldPolicy, UuidRepresentation, decode_document_with_options,
    encode_document_with_options,
};
use libfuzzer_sys::fuzz_target;

fn check_value(value: &BsonValue) {
    match value {
        BsonValue::Document(document) => check_document(document),
        BsonValue::Array(values) => values.iter().for_each(check_value),
        BsonValue::JavaScript(code) => {
            if let Some(scope) = code.scope() {
                check_document(scope);
            }
        }
        _ => {}
    }

    let key = match CanonicalBsonKey::encode(value) {
        Ok(key) => key,
        Err(error) => {
            assert_eq!(error.kind(), BsonErrorKind::Oversized);
            return;
        }
    };
    let parsed = CanonicalBsonKey::from_bytes(key.as_bytes()).expect("encoded key parses");
    assert_eq!(parsed, key);
}

fn check_document(document: &BsonDocument) {
    for (_, value) in document.iter() {
        check_value(value);
    }
}

fuzz_target!(|data: &[u8]| {
    let _ = CanonicalBsonKey::from_bytes(data);

    let representations = [
        None,
        Some(UuidRepresentation::Standard),
        Some(UuidRepresentation::PythonLegacy),
        Some(UuidRepresentation::JavaLegacy),
        Some(UuidRepresentation::CSharpLegacy),
    ];

    for representation in representations {
        let options = BsonCodecOptions::default()
            .with_duplicate_field_policy(DuplicateFieldPolicy::Preserve)
            .with_uuid_representation(representation);
        let Ok(document) = decode_document_with_options(data, &options) else {
            continue;
        };

        check_document(&document);
        let encoded = encode_document_with_options(&document, &options)
            .expect("a decoded bounded document re-encodes");
        let decoded = decode_document_with_options(&encoded, &options)
            .expect("a re-encoded document decodes");
        assert!(document.representation_eq(&decoded));
    }
});
