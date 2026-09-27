#![cfg(feature = "documents")]

#[path = "support/mongo_matcher_reproducer.rs"]
mod reproducer;

use briskdb::document::{BsonDocument, BsonValue, decode_document, encode_document};
use reproducer::{Outcome, reduce};
use std::{fs, io::Read, time::Duration};

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn case(document: BsonDocument, query: BsonDocument) -> BsonDocument {
    doc([
        ("document", BsonValue::Document(document)),
        ("query", BsonValue::Document(query)),
    ])
}

#[test]
fn deletion_reduction_keeps_the_witness_and_removes_nested_noise() {
    let original = case(
        doc([
            ("noise", BsonValue::from("unrelated")),
            (
                "items",
                BsonValue::Array(vec![
                    BsonValue::Int32(0),
                    BsonValue::Int32(7),
                    BsonValue::Int32(3),
                ]),
            ),
            (
                "nested",
                BsonValue::Document(doc([("unused", BsonValue::Boolean(true))])),
            ),
        ]),
        doc([
            ("items", BsonValue::Int32(7)),
            ("noise", BsonValue::Boolean(true)),
        ]),
    );
    let before = encode_document(&original).unwrap();
    let reduced = reduce(&original, 128, Duration::from_secs(1), |candidate| {
        let (row, query) = reproducer::inputs(candidate)?;
        let has_witness = matches!(row.get_first("items"), Some(BsonValue::Array(values)) if values.contains(&BsonValue::Int32(7)));
        Ok(has_witness && query.get_first("items") == Some(&BsonValue::Int32(7)))
    }).unwrap();
    assert!(!reduced.exhausted);
    assert_eq!(
        encode_document(&reduced.case).unwrap(),
        encode_document(&case(
            doc([("items", BsonValue::Array(vec![BsonValue::Int32(7)]))]),
            doc([("items", BsonValue::Int32(7))]),
        ))
        .unwrap()
    );
    assert_eq!(encode_document(&original).unwrap(), before);
}

#[test]
fn budget_exhaustion_is_explicit_and_never_returns_an_unchecked_case() {
    let original = case(doc([("v", BsonValue::Int32(7))]), BsonDocument::new());
    let reduced = reduce(&original, 1, Duration::from_secs(1), |_| Ok(true)).unwrap();
    assert!(reduced.exhausted);
    assert_eq!(reduced.probes, 1);
    assert_eq!(
        encode_document(&reduced.case).unwrap(),
        encode_document(&original).unwrap()
    );
    let timed = reduce(&original, 128, Duration::ZERO, |_| Ok(true)).unwrap();
    assert!(timed.exhausted);
    assert_eq!(timed.probes, 1);
}

#[test]
fn a_nonreproducer_or_probe_error_cannot_be_reported_as_reduced() {
    let original = case(doc([("v", BsonValue::Int32(7))]), BsonDocument::new());
    assert!(reduce(&original, 0, Duration::from_secs(1), |_| Ok(true)).is_err());
    assert!(reduce(&original, 128, Duration::from_secs(1), |_| Ok(false)).is_err());
    let mut calls = 0;
    let error = reduce(&original, 128, Duration::from_secs(1), |_| {
        calls += 1;
        if calls > 1 {
            Err("reference unavailable".into())
        } else {
            Ok(true)
        }
    })
    .err()
    .unwrap();
    assert_eq!(error, "reference unavailable");
    assert_eq!(calls, 2);
}

#[test]
fn input_bounds_and_shape_fail_before_any_reference_probe() {
    let too_large = case(
        doc([("v", BsonValue::from("x".repeat(65536)))]),
        BsonDocument::new(),
    );
    for malformed in [
        too_large,
        BsonDocument::new(),
        doc([("document", BsonValue::Null)]),
    ] {
        assert!(
            reduce(&malformed, 128, Duration::from_secs(1), |_| panic!(
                "must validate before probing"
            ))
            .is_err()
        );
    }
}

#[test]
fn retained_atomic_bson_encodings_are_not_normalized() {
    let value = BsonValue::Double(f64::from_bits(0xfff800000000002a));
    let original = case(
        doc([("v", value.clone()), ("unused", BsonValue::Int32(1))]),
        BsonDocument::new(),
    );
    let reduced = reduce(&original, 128, Duration::from_secs(1), |candidate| {
        Ok(reproducer::inputs(candidate)?
            .0
            .get_first("v")
            .is_some_and(|actual| actual.representation_eq(&value)))
    })
    .unwrap();
    assert_eq!(
        encode_document(&reduced.case).unwrap(),
        encode_document(&case(doc([("v", value)]), BsonDocument::new())).unwrap()
    );
}

#[test]
#[ignore = "diagnostic replay only: requires BRISKDB_MONGO_REPLAY_BSON and the locked oracle"]
fn replay_saved_matcher_case() {
    let path = std::env::var_os("BRISKDB_MONGO_REPLAY_BSON").expect("set the saved artifact path");
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .unwrap()
        .take(68 * 1024)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() < 68 * 1024);
    let artifact = decode_document(&bytes).unwrap();
    assert_eq!(artifact.get_first("schema"), Some(&BsonValue::Int32(1)));
    assert_eq!(
        artifact.get_first("kind"),
        Some(&BsonValue::from("matcher"))
    );
    assert_eq!(
        artifact.get_first("sourceCommit"),
        Some(&BsonValue::from(reproducer::SOURCE_COMMIT))
    );
    let Some(BsonValue::Document(case)) = artifact.get_first("case") else {
        panic!("missing case");
    };
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    let refreshed = reproducer::reference(&python, case).unwrap();
    assert_eq!(
        reproducer::expected(case).unwrap(),
        reproducer::expected(&refreshed).unwrap(),
        "saved reference outcome changed"
    );
    assert_eq!(
        reproducer::candidate(&refreshed),
        reproducer::expected(&refreshed).unwrap(),
        "saved mismatch still reproduces"
    );
    println!("single saved matcher case passes; this is diagnostic replay, not the full matrix");
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; explicitly run as a harness self-test"]
fn source_backed_fault_injection_reduces_and_replays_without_stale_expectations() {
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    let original = doc([
        (
            "document",
            BsonValue::Document(doc([
                ("v", BsonValue::Int32(7)),
                ("noise", BsonValue::Int32(9)),
            ])),
        ),
        (
            "query",
            BsonValue::Document(doc([("v", BsonValue::Int32(7))])),
        ),
        ("matches", BsonValue::Boolean(false)), // Deliberately stale; must be recomputed.
    ]);
    let root = tempfile::tempdir().unwrap();
    let fault = |_case: &BsonDocument| Outcome::Match(false);
    let path = reproducer::save_mismatch(&python, &original, root.path(), fault).unwrap();
    let artifact = decode_document(&fs::read(&path).unwrap()).unwrap();
    let Some(BsonValue::Document(reduced)) = artifact.get_first("case") else {
        panic!("case");
    };
    let (row, query) = reproducer::inputs(reduced).unwrap();
    assert!(row.is_empty() && query.is_empty());
    assert_eq!(reproducer::expected(reduced).unwrap(), Outcome::Match(true));
    assert_eq!(reproducer::candidate(reduced), Outcome::Match(true));
    let replay = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replay_saved_matcher_case",
            "--ignored",
            "--nocapture",
        ])
        .env("BRISKDB_MONGO_REPLAY_BSON", &path)
        .env("BRISKDB_MONGO_ORACLE_PYTHON", &python)
        .output()
        .unwrap();
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        reproducer::save_mismatch(&python, &original, root.path(), fault).unwrap(),
        path
    );
    assert!(
        reproducer::save_mismatch(&python, &original, root.path(), reproducer::candidate).is_err()
    );
    // A corrupt existing artifact is never silently overwritten.
    fs::write(&path, b"corrupt").unwrap();
    assert!(reproducer::save_mismatch(&python, &original, root.path(), fault).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"corrupt");

    let flaky_root = tempfile::tempdir().unwrap();
    let calls = std::cell::Cell::new(0);
    let failure = reproducer::save_mismatch(&python, &original, flaky_root.path(), |case| {
        calls.set(calls.get() + 1);
        if calls.get() <= 2 {
            Outcome::Match(false)
        } else {
            reproducer::candidate(case)
        }
    })
    .err()
    .unwrap();
    assert!(failure.contains("confirmed original retained"));
    let retained = fs::read_dir(flaky_root.path())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(retained.len(), 1);
    let retained = decode_document(&fs::read(retained[0].path()).unwrap()).unwrap();
    assert_eq!(
        retained.get_first("phase"),
        Some(&BsonValue::from("original"))
    );
    // The helper tests a deliberately faulty callback, not a real BriskDB bug.
}
