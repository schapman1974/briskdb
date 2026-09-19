#![cfg(feature = "documents")]

use briskdb::document::{
    BsonValue, DocumentUpdateError, DocumentUpdater, decode_document, encode_document,
};
use std::{error::Error, process::Command};

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn field_updates_match_locked_object_path_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_update_oracle.py"
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
        let Some(BsonValue::Document(document)) = case.get_first("document") else {
            panic!("document")
        };
        let Some(BsonValue::Document(update)) = case.get_first("update") else {
            panic!("update")
        };
        let before = encode_document(document).unwrap();
        let result = DocumentUpdater::compile(update).and_then(|updater| updater.apply(document));
        if let Some(BsonValue::Int32(expected)) = case.get_first("error") {
            let error = result.unwrap_err();
            let actual = error
                .source()
                .and_then(|e| e.downcast_ref::<DocumentUpdateError>())
                .map(|e| e.mongo_code());
            assert_eq!(actual, Some(*expected), "case {count}: {error}");
        } else {
            let Some(BsonValue::Document(expected)) = case.get_first("result") else {
                panic!("result")
            };
            assert_eq!(
                encode_document(&result.unwrap_or_else(|e| panic!("case {count}: {e}"))).unwrap(),
                encode_document(expected).unwrap(),
                "case {count}"
            );
        }
        assert_eq!(encode_document(document).unwrap(), before);
        count += 1;
    }
    assert_eq!(count, 4008);
    println!("{count} source-locked object-path field update cases passed");
}
