use briskdb::core::Database;
use rusqlite::{Connection, OpenFlags};

#[test]
fn pre_one_policy_matches_the_manifest_created_by_this_package() {
    let package_major = env!("CARGO_PKG_VERSION")
        .split('.')
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(package_major, 0, "pre-1.0 policy applied to a 1.x package");

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("database");
    drop(Database::open(&root, 2).unwrap());

    let manifest = Connection::open_with_flags(
        root.join("manifest.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let manifest_version = manifest
        .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
        .unwrap();

    let storage_contract = include_str!("../docs/STORAGE_FORMAT.md");
    assert!(storage_contract.contains(&format!("## Current format: version {manifest_version}")));

    let release_policy = include_str!("../docs/PRE_1_COMPATIBILITY.md");
    for required_boundary in [
        "BriskDB 0.x releases are experimental",
        "In-place downgrade is unsupported",
        "complete pre-upgrade backup",
        "accepted source versions",
        "no on-disk format change",
    ] {
        assert!(
            release_policy.contains(required_boundary),
            "pre-1.0 policy is missing required boundary: {required_boundary}"
        );
    }
}
