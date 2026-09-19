#![cfg(feature = "documents")]

use std::{error::Error, process::Command};

use briskdb::document::{
    BsonValue, DocumentQueryError, DocumentSorter, decode_document, encode_document,
};

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn sorting_matches_the_locked_tinymongo_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_sorting_oracle.py"
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
        let Some(BsonValue::Document(spec)) = case.get_first("sort") else {
            panic!("sort");
        };
        let Some(BsonValue::Array(documents)) = case.get_first("documents") else {
            panic!("documents");
        };
        let before = encode_document(&case).unwrap();
        let actual = DocumentSorter::compile(spec).and_then(|sorter| {
            let mut keyed = Vec::new();
            for document in documents {
                let BsonValue::Document(document) = document else {
                    panic!("document");
                };
                keyed.push((sorter.key(document)?, document.get_first("_id").unwrap()));
            }
            // Stable sorting must leave semantically equal keys in input order.
            keyed.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(BsonValue::Array(
                keyed.into_iter().map(|(_, id)| id.clone()).collect(),
            ))
        });
        if let Some(BsonValue::Int32(expected)) = case.get_first("error") {
            let error = actual.expect_err(&format!("case {count}: {spec:?}"));
            let code = error
                .source()
                .and_then(|source| source.downcast_ref::<DocumentQueryError>())
                .map(|error| error.mongo_code());
            assert_eq!(
                code,
                Some(*expected),
                "case {count}: {spec:?} against {documents:?}: {error:?}"
            );
        } else {
            let actual = actual.unwrap_or_else(|error| {
                panic!("case {count}: {spec:?} against {documents:?}: {error:?}")
            });
            assert_eq!(
                &actual,
                case.get_first("result").unwrap(),
                "case {count}: {spec:?} against {documents:?}"
            );
        }
        assert_eq!(encode_document(&case).unwrap(), before);
        count += 1;
    }
    assert!(count > 4500, "expected full sorting matrix, got {count}");
    println!("{count} source-locked TinyMongo sorting cases passed");
}
