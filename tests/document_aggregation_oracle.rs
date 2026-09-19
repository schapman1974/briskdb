#![cfg(feature = "documents")]

use std::{error::Error, process::Command};

use briskdb::document::{
    BsonDocument, BsonValue, DocumentAggregator, DocumentPipeline, DocumentQueryError,
    decode_document, encode_document,
};

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_matches_the_locked_tinymongo_oracle() {
    compare("document_aggregation_oracle.py", 5000, 32 * 1024 * 1024);
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_transforms_match_the_locked_tinymongo_oracle() {
    compare(
        "document_aggregation_transforms_oracle.py",
        5000,
        64 * 1024 * 1024,
    );
}

fn compare(script: &str, minimum: usize, maximum_bytes: usize) {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(script),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.len() < maximum_bytes);
    let mut bytes = output.stdout.as_slice();
    let mut count = 0;
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let documents = |field| {
            let Some(BsonValue::Array(values)) = case.get_first(field) else {
                panic!("array");
            };
            values
                .iter()
                .map(|value| {
                    let BsonValue::Document(document) = value else {
                        panic!("document");
                    };
                    document.clone()
                })
                .collect::<Vec<_>>()
        };
        let pipeline = DocumentPipeline::new(documents("pipeline")).unwrap();
        let source = documents("documents");
        let before = encode_document(&case).unwrap();
        let actual =
            DocumentAggregator::compile(&pipeline).and_then(|runner| runner.execute(&source));
        let streamed = DocumentAggregator::compile(&pipeline).and_then(|runner| {
            let mut stream = runner.into_stream();
            let mut result = Vec::new();
            for document in source.iter().cloned() {
                if stream.is_input_exhausted() {
                    break;
                }
                if let Some(document) = stream.push(document)? {
                    result.push(document);
                }
            }
            result.extend(stream.finish()?);
            Ok(result)
        });
        for actual in [actual, streamed] {
            if let Some(BsonValue::Int32(expected)) = case.get_first("error") {
                let error = actual.expect_err(&format!("case {count}: {case:?}"));
                let code = error
                    .source()
                    .and_then(|source| source.downcast_ref::<DocumentQueryError>())
                    .map(|error| error.mongo_code());
                assert_eq!(code, Some(*expected), "case {count}: {case:?}: {error:?}");
            } else {
                let actual =
                    actual.unwrap_or_else(|error| panic!("case {count}: {case:?}: {error:?}"));
                let encode = |value| {
                    encode_document(&BsonDocument::from_entries([("result", value)]).unwrap())
                        .unwrap()
                };
                // Compare exact BSON, including field order and numeric variants.
                assert_eq!(
                    encode(BsonValue::Array(
                        actual.into_iter().map(BsonValue::Document).collect()
                    )),
                    encode(case.get_first("result").unwrap().clone()),
                    "case {count}: {case:?}"
                );
            }
        }
        assert_eq!(encode_document(&case).unwrap(), before);
        assert_eq!(source, documents("documents"));
        count += 1;
    }
    assert!(
        count > minimum,
        "expected the complete pipeline matrix, got {count}"
    );
    println!("{count} source-locked TinyMongo aggregation cases passed ({script})");
}
