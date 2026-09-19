#![cfg(feature = "documents")]

use std::{error::Error, process::Command};

use briskdb::document::{
    BsonDocument, BsonValue, DocumentAggregator, DocumentPipeline, DocumentQueryError,
    decode_document, encode_document,
};

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_matches_the_locked_tinymongo_oracle() {
    compare("document_aggregation_oracle.py", 5000, 32 * 1024 * 1024, 0);
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_transforms_match_the_locked_tinymongo_oracle() {
    compare(
        "document_aggregation_transforms_oracle.py",
        5000,
        64 * 1024 * 1024,
        0,
    );
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_groups_match_the_locked_tinymongo_oracle() {
    compare(
        "document_aggregation_groups_oracle.py",
        8000,
        96 * 1024 * 1024,
        24,
    );
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn aggregation_group_keys_match_frozen_expression_group_composition() {
    compare(
        "document_aggregation_keys_oracle.py",
        5000,
        128 * 1024 * 1024,
        5400,
    );
}

fn compare(script: &str, minimum: usize, maximum_bytes: usize, expected_composed: usize) {
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
    let mut composed = 0;
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        composed += usize::from(case.get_first("reference_pipeline").is_some());
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
                    let value =
                        normalize_arithmetic_nans(value, case.get_first("numeric_nan_fields"));
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
    assert_eq!(
        composed, expected_composed,
        "composition coverage must remain explicit"
    );
    println!(
        "{count} source-locked TinyMongo aggregation cases passed ({script}); {composed} use explicit frozen-stage composition for extended group keys"
    );
}

fn normalize_arithmetic_nans(value: BsonValue, fields: Option<&BsonValue>) -> BsonValue {
    let Some(BsonValue::Array(fields)) = fields else {
        return value;
    };
    let BsonValue::Array(rows) = value else {
        panic!("result rows");
    };
    BsonValue::Array(
        rows.into_iter()
            .map(|row| {
                let BsonValue::Document(row) = row else {
                    panic!("result document");
                };
                BsonValue::Document(
                    BsonDocument::from_entries(row.into_entries().into_iter().map(
                        |(name, value)| {
                            let numeric = fields.iter().any(
                                |field| matches!(field, BsonValue::String(field) if field == &name),
                            );
                            let value = match value {
                                BsonValue::Double(value) if numeric && value.is_nan() => {
                                    BsonValue::Double(f64::NAN)
                                }
                                value => value,
                            };
                            (name, value)
                        },
                    ))
                    .unwrap(),
                )
            })
            .collect(),
    )
}
