#![cfg(feature = "documents")]

use briskdb::document::{
    BsonDocument, BsonValue, DocumentDistinct, decode_document, encode_document,
};
use std::process::Command;

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn distinct_matches_the_locked_tinymongo_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_distinct_oracle.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() < 32 * 1024 * 1024);
    let mut bytes = output.stdout.as_slice();
    let mut count = 0;
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let Some(BsonValue::String(field)) = case.get_first("field") else {
            panic!("field");
        };
        let Some(BsonValue::Array(documents)) = case.get_first("documents") else {
            panic!("documents");
        };
        let before = encode_document(&case).unwrap();
        let mut distinct = DocumentDistinct::new(field).unwrap();
        for document in documents {
            let BsonValue::Document(document) = document else {
                panic!("document");
            };
            distinct
                .push(document)
                .unwrap_or_else(|error| panic!("case {count}: {error:?}"));
        }
        let actual = BsonValue::Array(distinct.into_values().unwrap());
        let encode = |value| {
            encode_document(&BsonDocument::from_entries([("values", value)]).unwrap()).unwrap()
        };
        assert_eq!(
            encode(actual),
            encode(case.get_first("result").unwrap().clone()),
            "case {count}: {case:?}"
        );
        assert_eq!(before, encode_document(&case).unwrap());
        count += 1;
    }
    assert!(
        count > 4500,
        "expected the full distinct matrix, got {count}"
    );
    println!("{count} source-locked TinyMongo distinct cases passed");
}
