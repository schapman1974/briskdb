#![cfg(feature = "documents")]

use std::{collections::HashMap, process::Command};

use briskdb::{
    core::EngineErrorKind,
    document::{
        BsonDocument, BsonValue, DocumentIndexKey, DocumentIndexKeyGenerator, DocumentMatcher,
        decode_document, encode_document,
    },
};

fn document<'a>(case: &'a BsonDocument, field: &str) -> &'a BsonDocument {
    let Some(BsonValue::Document(value)) = case.get_first(field) else {
        panic!("{field}")
    };
    value
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn index_equality_probes_never_exclude_locked_matcher_results() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_index_probe_oracle.py"
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
    let (mut count, mut checked, mut hits) = (0, 0, 0);
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let before = encode_document(&case).unwrap();
        let partial = match case.get_first("partial") {
            Some(BsonValue::Document(value)) => Some(value),
            Some(BsonValue::Null) => None,
            _ => panic!("partial"),
        };
        let generator = DocumentIndexKeyGenerator::compile(
            document(&case, "keys"),
            case.get_first("sparse") == Some(&BsonValue::Boolean(true)),
            partial,
        )
        .unwrap();
        let matcher = DocumentMatcher::compile(document(&case, "query")).unwrap();
        let probe = generator.equality_key(&matcher).unwrap();
        assert_eq!(
            probe.is_some(),
            case.get_first("probe") == Some(&BsonValue::Boolean(true)),
            "selection case {count}: {case:?}"
        );
        let Some(BsonValue::Array(documents)) = case.get_first("documents") else {
            panic!("documents")
        };
        let Some(BsonValue::Array(expected)) = case.get_first("expected") else {
            panic!("expected")
        };
        assert_eq!(documents.len(), expected.len());
        for (input, expected) in documents.iter().zip(expected) {
            let BsonValue::Document(input) = input else {
                panic!("input")
            };
            let actual = matcher.matches(input).unwrap();
            assert_eq!(
                BsonValue::Boolean(actual),
                *expected,
                "matcher case {count}: {input:?}"
            );
            if let Some(probe) = &probe {
                if actual {
                    assert!(
                        generator.keys(input).unwrap().contains(probe),
                        "false negative case {count}: {case:?}; input: {input:?}"
                    );
                    hits += 1;
                }
            }
            checked += 1;
        }
        assert_eq!(before, encode_document(&case).unwrap());
        count += 1;
    }
    assert_eq!(count, 1_087);
    assert!(
        checked > 180_000 && hits > 5_000,
        "{checked} evaluations, {hits} hits"
    );
    println!(
        "{count} source-locked probe groups: {checked} matcher evaluations, {hits} indexed matches without false negatives"
    );
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn index_keys_match_locked_membership_order_and_equality_partitions() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/document_index_keys_oracle.py"
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
    let mut documents_checked = 0;
    while !bytes.is_empty() {
        let length = i32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let case = decode_document(&bytes[..length]).unwrap();
        bytes = &bytes[length..];
        let before = encode_document(&case).unwrap();
        let partial = match case.get_first("partial") {
            Some(BsonValue::Document(value)) => Some(value),
            Some(BsonValue::Null) => None,
            _ => panic!("partial"),
        };
        let compiled = DocumentIndexKeyGenerator::compile(
            document(&case, "keys"),
            case.get_first("sparse") == Some(&BsonValue::Boolean(true)),
            partial,
        );
        let compile_error = case.get_first("compile_error") == Some(&BsonValue::Boolean(true));
        assert_eq!(
            compiled.is_err(),
            compile_error,
            "compile case {count}: {case:?}"
        );
        if let Ok(compiled) = compiled {
            let Some(BsonValue::Array(documents)) = case.get_first("documents") else {
                panic!("documents")
            };
            let Some(BsonValue::Array(expected)) = case.get_first("expected") else {
                panic!("expected")
            };
            assert_eq!(documents.len(), expected.len());
            let mut seen = HashMap::new();
            for (input, expected) in documents.iter().zip(expected) {
                let BsonValue::Document(input) = input else {
                    panic!("input")
                };
                let result = compiled.keys(input);
                if expected == &BsonValue::Null {
                    assert_eq!(
                        result.unwrap_err().kind(),
                        EngineErrorKind::Unsupported,
                        "case {count}"
                    );
                } else {
                    let keys =
                        result.unwrap_or_else(|error| panic!("case {count}: {case:?}: {error:?}"));
                    let actual = BsonValue::Array(
                        keys.into_iter()
                            .map(|key| {
                                let bytes = key.to_bytes().unwrap();
                                let restored = DocumentIndexKey::from_bytes(&bytes).unwrap();
                                assert_eq!(restored, key);
                                assert_eq!(restored.to_bytes().unwrap(), bytes);
                                let next = seen.len() as i32;
                                BsonValue::Int32(*seen.entry(restored).or_insert(next))
                            })
                            .collect(),
                    );
                    assert_eq!(&actual, expected, "case {count}: {case:?}");
                }
                documents_checked += 1;
            }
        }
        assert_eq!(before, encode_document(&case).unwrap());
        count += 1;
    }
    assert_eq!(count, 7_201);
    assert_eq!(documents_checked, 29_370);
    println!(
        "{count} source-locked index-key cases passed ({documents_checked} document evaluations)"
    );
}
