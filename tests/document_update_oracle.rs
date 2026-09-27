#![cfg(feature = "documents")]

use briskdb::document::{BsonValue, decode_document, encode_document};
use std::{path::PathBuf, process::Command};

#[path = "support/mongo_matcher_reproducer.rs"]
mod reproducer;
use reproducer::Surface;

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn field_updates_match_locked_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(&python)
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
        let before = encode_document(document).unwrap();
        let expected = Surface::Update.expected(&case).unwrap();
        let actual = Surface::Update.candidate(&case);
        if actual != expected {
            let directory = std::env::var_os("BRISKDB_MONGO_REPRO_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("target/mongo-parity/reproducers")
                });
            let artifact = Surface::Update.save_mismatch(&python, &case, &directory, |case| {
                Surface::Update.candidate(case)
            });
            panic!(
                "update case {count}: expected {expected:?}, got {actual:?}; reproducer: {artifact:?}; original: {case:?}"
            );
        }
        assert_eq!(encode_document(document).unwrap(), before);
        count += 1;
    }
    assert_eq!(count, 30489);
    println!(
        "{count} source-locked field update cases passed (4008 object-only set/unset; 4719 min/max; 3078 pop/rename; 4440 non-ID membership with object-only add-to-set; 4459 non-ID push; 5573 non-ID pull; 4212 intersecting non-ID object-path increment cases)"
    );
}
