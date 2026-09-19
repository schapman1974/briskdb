#![cfg(feature = "documents")]

use std::{error::Error, process::Command};

use briskdb::document::{BsonValue, DocumentMatcher, DocumentQueryError, decode_document};

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn matcher_matches_the_locked_tinymongo_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_matcher_oracle.py"
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
        let Some(BsonValue::Document(query)) = case.get_first("query") else {
            panic!("query");
        };
        let Some(BsonValue::Document(document)) = case.get_first("document") else {
            panic!("document");
        };
        let result = DocumentMatcher::compile(query).and_then(|matcher| matcher.matches(document));
        if let Some(BsonValue::Int32(expected)) = case.get_first("error") {
            let error = result.expect_err(&format!("case {count}: {query:?}"));
            let code = error
                .source()
                .and_then(|source| source.downcast_ref::<DocumentQueryError>())
                .map(|error| error.mongo_code());
            assert_eq!(code, Some(*expected), "case {count}: {query:?}: {error:?}");
        } else {
            let Some(BsonValue::Boolean(expected)) = case.get_first("matches") else {
                panic!("expected match");
            };
            assert_eq!(
                result.unwrap_or_else(|error| panic!("case {count}: {query:?}: {error:?}")),
                *expected,
                "case {count}: {query:?} against {document:?}"
            );
        }
        count += 1;
    }
    assert!(
        count > 20_000,
        "expected full generated matrix, found {count}"
    );
    println!("{count} source-locked TinyMongo matcher cases passed");
}
