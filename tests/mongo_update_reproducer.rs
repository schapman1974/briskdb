#![cfg(feature = "documents")]

#[path = "support/mongo_matcher_reproducer.rs"]
mod reproducer;

use briskdb::document::{BsonDocument, BsonValue, decode_document, encode_document};
use reproducer::{Outcome, Surface};
use std::{fs, time::Duration};

const SURFACE: Surface = Surface::Update;

fn doc<const N: usize>(fields: [(&str, BsonValue); N]) -> BsonDocument {
    BsonDocument::from_entries(fields).unwrap()
}
fn case(row: BsonDocument, update: BsonDocument) -> BsonDocument {
    doc([
        ("document", BsonValue::Document(row)),
        ("update", BsonValue::Document(update)),
    ])
}
fn operator(name: &str, field: &str, value: BsonValue) -> BsonDocument {
    BsonDocument::from_entries([(
        name,
        BsonValue::Document(BsonDocument::from_entries([(field, value)]).unwrap()),
    )])
    .unwrap()
}

#[test]
fn update_reduction_never_removes_required_identity_or_all_operator_names() {
    let original = case(
        doc([("_id", BsonValue::Int64(7)), ("noise", BsonValue::Null)]),
        operator("$set", "v", BsonValue::Int32(1)),
    );
    let reduced = SURFACE
        .reduce(&original, 128, Duration::from_secs(1), |input| {
            let (row, update) = SURFACE.inputs(input)?;
            assert!(row.get_first("_id").is_some());
            assert!(!update.is_empty());
            Ok(true)
        })
        .unwrap();
    assert!(!reduced.exhausted);
    let (row, update) = SURFACE.inputs(&reduced.case).unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row.get_first("_id"), Some(&BsonValue::Int64(7)));
    assert_eq!(
        update.get_first("$set"),
        Some(&BsonValue::Document(BsonDocument::new()))
    );
    for invalid in [
        case(
            BsonDocument::new(),
            operator("$set", "v", BsonValue::Int32(1)),
        ),
        case(row.clone(), BsonDocument::new()),
        case(row.clone(), doc([("v", BsonValue::Int32(1))])),
    ] {
        assert!(
            SURFACE
                .reduce(&invalid, 128, Duration::from_secs(1), |_| panic!(
                    "invalid probe"
                ))
                .is_err()
        );
        assert!(
            SURFACE
                .reference("/no-such-oracle", &invalid)
                .unwrap_err()
                .contains("requires")
        );
    }
}

#[test]
fn update_outcomes_keep_typed_bson_and_classify_each_native_error_family() {
    let value = BsonValue::Double(f64::from_bits(0xfff800000000002a));
    let row = doc([("_id", BsonValue::Int64(7)), ("v", value.clone())]);
    let input = case(row.clone(), operator("$set", "v", value));
    assert_eq!(
        SURFACE.candidate(&input),
        Outcome::Document(encode_document(&row).unwrap())
    );
    assert_eq!(
        SURFACE.candidate(&case(
            row.clone(),
            operator("$inc", "v", BsonValue::from("bad"))
        )),
        Outcome::Code(14)
    );
    assert_eq!(
        SURFACE.candidate(&case(
            row.clone(),
            operator("$set", "_id", BsonValue::Int32(8))
        )),
        Outcome::Code(66)
    );
    assert_eq!(
        SURFACE.candidate(&case(
            row.clone(),
            operator(
                "$pull",
                "v",
                BsonValue::Document(doc([("$where", BsonValue::from("bad"))]),)
            )
        )),
        Outcome::Code(2) // Pull's field-operator validation, not a top-level query.
    );
    assert_eq!(
        SURFACE.candidate(&case(
            row,
            operator(
                "$pull",
                "v",
                BsonValue::Document(doc([("$expr", BsonValue::Null)]),)
            )
        )),
        Outcome::Code(224)
    );
}

#[test]
#[ignore = "diagnostic replay only: requires BRISKDB_MONGO_REPLAY_BSON and the locked oracle"]
fn replay_saved_update_case() {
    let path = std::env::var_os("BRISKDB_MONGO_REPLAY_BSON").expect("set the saved artifact path");
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    SURFACE
        .replay(&python, std::path::Path::new(&path))
        .unwrap();
    println!("single saved update case passes; diagnostic replay is not the full matrix");
}

#[test]
#[ignore = "requires source-locked test-only TinyMongo; explicitly run as a harness self-test"]
fn source_backed_update_fault_reduces_and_replays_without_changing_the_outcome_pair() {
    let python =
        std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").expect("set the locked oracle interpreter");
    let mut update = operator("$inc", "v", BsonValue::Int32(1));
    update
        .push(
            "$unset",
            BsonValue::Document(doc([("noise", BsonValue::Int32(1))])),
        )
        .unwrap();
    let mut original = case(
        doc([
            ("_id", BsonValue::Int64(7)),
            ("v", BsonValue::Int32(1)),
            ("noise", BsonValue::Int32(9)),
        ]),
        update,
    );
    original.push("error", BsonValue::Int32(999)).unwrap(); // Stale; must be discarded.
    let root = tempfile::tempdir().unwrap();
    let wrong = |_input: &BsonDocument| Outcome::Code(999);
    let path = SURFACE
        .save_mismatch(&python, &original, root.path(), wrong)
        .unwrap();
    let artifact = decode_document(&fs::read(&path).unwrap()).unwrap();
    assert_eq!(artifact.get_first("kind"), Some(&BsonValue::from("update")));
    let Some(BsonValue::Document(reduced)) = artifact.get_first("case") else {
        panic!("case");
    };
    let (row, update) = SURFACE.inputs(reduced).unwrap();
    assert!(row.get_first("noise").is_none());
    assert_eq!(row.get_first("_id"), Some(&BsonValue::Int64(7)));
    assert!(update.get_first("$unset").is_none());
    assert_eq!(
        update.get_first("$inc"),
        Some(&BsonValue::Document(doc([("v", BsonValue::Int32(1))])))
    );
    let expected = Outcome::Document(
        encode_document(&doc([
            ("_id", BsonValue::Int64(7)),
            ("v", BsonValue::Int32(2)),
        ]))
        .unwrap(),
    );
    assert_eq!(SURFACE.expected(reduced).unwrap(), expected);
    assert_eq!(SURFACE.candidate(reduced), expected);
    let replay = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "replay_saved_update_case",
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
    // This intentionally wrong callback is not a newly discovered product bug.
}
