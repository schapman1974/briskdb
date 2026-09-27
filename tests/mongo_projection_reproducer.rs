#![cfg(feature = "documents")]

#[path = "support/mongo_matcher_reproducer.rs"]
mod reproducer;

use briskdb::document::{BsonDocument, BsonValue, decode_document, encode_document};
use reproducer::{Outcome, Surface};
use std::{fs, time::Duration};

const SURFACE: Surface = Surface::Projection;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn case(row: BsonDocument, spec: BsonDocument) -> BsonDocument {
    doc([
        ("document", BsonValue::Document(row)),
        ("projection", BsonValue::Document(spec)),
    ])
}

#[test]
fn projection_reduction_retains_its_own_shape_and_exact_bson_witness() {
    let value = BsonValue::Double(f64::from_bits(0xfff800000000002a));
    let original = case(
        doc([("v", value.clone()), ("noise", BsonValue::Int32(9))]),
        doc([("v", BsonValue::Int32(1)), ("unused", BsonValue::Int32(1))]),
    );
    let before = encode_document(&original).unwrap();
    let reduced = SURFACE
        .reduce(&original, 128, Duration::from_secs(1), |candidate| {
            let (row, spec) = SURFACE.inputs(candidate)?;
            Ok(row
                .get_first("v")
                .is_some_and(|actual| actual.representation_eq(&value))
                && spec.get_first("v") == Some(&BsonValue::Int32(1)))
        })
        .unwrap();
    assert!(!reduced.exhausted);
    assert_eq!(
        encode_document(&reduced.case).unwrap(),
        encode_document(&case(
            doc([("v", value)]),
            doc([("v", BsonValue::Int32(1))]),
        ))
        .unwrap()
    );
    assert_eq!(encode_document(&original).unwrap(), before);
    assert!(Surface::Matcher.inputs(&reduced.case).is_err());
}

#[test]
fn projection_outcomes_compare_exact_representation_and_reject_ambiguous_expectations() {
    let row = doc([
        ("nan", BsonValue::Double(f64::from_bits(0xfff800000000002a))),
        ("zero", BsonValue::Double(-0.0)),
        ("integer", BsonValue::Int64(7)),
    ]);
    let mut input = case(row.clone(), BsonDocument::new());
    input
        .push("result", BsonValue::Document(row.clone()))
        .unwrap();
    let expected = Outcome::Document(encode_document(&row).unwrap());
    assert_eq!(SURFACE.candidate(&input), expected);
    assert_eq!(SURFACE.expected(&input).unwrap(), expected);
    input.push("error", BsonValue::Int32(115)).unwrap();
    assert!(SURFACE.expected(&input).is_err());
    assert!(SURFACE.expected(&BsonDocument::new()).is_err());
}

#[test]
fn projection_diagnostics_reject_bad_shapes_sizes_and_metadata_before_reference_execution() {
    for malformed in [
        BsonDocument::new(),
        doc([("document", BsonValue::Null)]),
        case(
            doc([("large", BsonValue::from("x".repeat(65536)))]),
            BsonDocument::new(),
        ),
    ] {
        assert!(
            SURFACE
                .reduce(&malformed, 128, Duration::from_secs(1), |_| panic!(
                    "no probes"
                ))
                .is_err()
        );
        let error = SURFACE
            .reference("/no-such-oracle", &malformed)
            .unwrap_err();
        assert!(
            error.contains("requires") || error.contains("exceeds"),
            "{error}"
        );
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("case.bson");
    for (schema, kind, source) in [
        (2, "projection", reproducer::SOURCE_COMMIT),
        (1, "matcher", reproducer::SOURCE_COMMIT),
        (1, "projection", "wrong-source"),
    ] {
        fs::write(
            &path,
            encode_document(&doc([
                ("schema", BsonValue::Int32(schema)),
                ("kind", BsonValue::from(kind)),
                ("sourceCommit", BsonValue::from(source)),
            ]))
            .unwrap(),
        )
        .unwrap();
        assert!(
            SURFACE
                .replay("/no-such-oracle", &path)
                .unwrap_err()
                .contains("metadata")
        );
    }
    fs::write(&path, vec![0; 68 * 1024]).unwrap();
    assert!(
        SURFACE
            .replay("/no-such-oracle", &path)
            .unwrap_err()
            .contains("bounded envelope")
    );
}

#[test]
#[ignore = "diagnostic replay only: requires BRISKDB_MONGO_REPLAY_BSON and the locked oracle"]
fn replay_saved_projection_case() {
    let path = std::env::var_os("BRISKDB_MONGO_REPLAY_BSON").expect("set the saved artifact path");
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    SURFACE
        .replay(&python, std::path::Path::new(&path))
        .unwrap();
    println!("single saved projection case passes; diagnostic replay is not the full matrix");
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; explicitly run as a harness self-test"]
fn source_backed_projection_fault_reduces_and_replays_exact_output() {
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    let mut original = case(
        doc([
            ("keep", BsonValue::Int64(7)),
            ("noise", BsonValue::Int32(9)),
        ]),
        doc([("keep", BsonValue::Int32(1)), ("_id", BsonValue::Int32(0))]),
    );
    original
        .push("result", BsonValue::Document(BsonDocument::new()))
        .unwrap(); // Stale.
    let root = tempfile::tempdir().unwrap();
    let wrong =
        |_case: &BsonDocument| Outcome::Document(encode_document(&BsonDocument::new()).unwrap());
    let path = SURFACE
        .save_mismatch(&python, &original, root.path(), wrong)
        .unwrap();
    let artifact = decode_document(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        artifact.get_first("kind"),
        Some(&BsonValue::from("projection"))
    );
    assert!(
        matches!(artifact.get_first("originalCandidateOutcome"), Some(BsonValue::String(value)) if value.contains("blake3="))
    );
    let Some(BsonValue::Document(reduced)) = artifact.get_first("case") else {
        panic!("case");
    };
    let (row, spec) = SURFACE.inputs(reduced).unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get_first("keep"), Some(&BsonValue::Int64(7)));
    assert!(spec.is_empty());
    let expected =
        Outcome::Document(encode_document(&doc([("keep", BsonValue::Int64(7))])).unwrap());
    assert_eq!(SURFACE.expected(reduced).unwrap(), expected);
    assert_eq!(SURFACE.candidate(reduced), expected);
    let replay = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replay_saved_projection_case",
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
        SURFACE
            .save_mismatch(&python, &original, root.path(), wrong)
            .unwrap(),
        path
    );
    assert!(
        SURFACE
            .save_mismatch(&python, &original, root.path(), |case| SURFACE
                .candidate(case))
            .is_err()
    );

    // Even a wildly incorrect candidate output must not inflate the saved
    // envelope beyond the replay limit: only its length and digest are stored.
    let large_output = SURFACE
        .save_mismatch(&python, &original, root.path(), |_| {
            Outcome::Document(vec![255; 128 * 1024])
        })
        .unwrap();
    assert!(fs::metadata(&large_output).unwrap().len() < 2048);
    SURFACE.replay(&python, &large_output).unwrap();

    let stale = doc([
        ("schema", BsonValue::Int32(1)),
        ("kind", BsonValue::from("projection")),
        ("sourceCommit", BsonValue::from(reproducer::SOURCE_COMMIT)),
        ("case", BsonValue::Document(original.clone())),
    ]);
    let stale_path = root.path().join("stale.bson");
    fs::write(&stale_path, encode_document(&stale).unwrap()).unwrap();
    assert!(
        SURFACE
            .replay(&python, &stale_path)
            .unwrap_err()
            .contains("saved reference outcome changed")
    );
    fs::write(&path, b"corrupt").unwrap();
    assert!(
        SURFACE
            .save_mismatch(&python, &original, root.path(), wrong)
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), b"corrupt");

    let flaky_root = tempfile::tempdir().unwrap();
    let calls = std::cell::Cell::new(0);
    let failure = SURFACE
        .save_mismatch(&python, &original, flaky_root.path(), |case| {
            calls.set(calls.get() + 1);
            if calls.get() <= 2 {
                wrong(case)
            } else {
                SURFACE.candidate(case)
            }
        })
        .unwrap_err();
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
    // Deliberately wrong callback: this is a harness test, not a real product bug.
}
