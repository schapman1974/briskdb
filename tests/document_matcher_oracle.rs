#![cfg(feature = "documents")]

use std::{path::PathBuf, process::Command};

use briskdb::document::decode_document;

#[path = "support/mongo_matcher_reproducer.rs"]
mod reproducer;

#[test]
#[ignore = "requires source-locked test-only TinyMongo; CI runs this explicitly"]
fn matcher_matches_the_locked_tinymongo_oracle() {
    let python = std::env::var("BRISKDB_MONGO_ORACLE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = Command::new(&python)
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
        let expected = reproducer::expected(&case).unwrap();
        let actual = reproducer::candidate(&case);
        if actual != expected {
            let directory = std::env::var_os("BRISKDB_MONGO_REPRO_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("target/mongo-parity/reproducers")
                });
            let artifact =
                reproducer::save_mismatch(&python, &case, &directory, reproducer::candidate);
            panic!(
                "matcher case {count}: expected {expected:?}, got {actual:?}; reproducer: {artifact:?}; original: {case:?}"
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
