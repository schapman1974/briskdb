use super::*;
use crate::core::{Database, security_catalog::tests as fixtures};
use std::{fs, os::unix::fs::PermissionsExt};

fn root() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    drop(Database::open(temp.path(), 2).unwrap());
    temp
}

#[test]
fn activation_binds_committed_store_and_ordinary_reopen_fails_without_mutation() {
    let temp = root();
    let (catalog, _, _) = fixtures::setup();
    let id = provision(temp.path(), 2, &catalog).unwrap();
    let authority = open(temp.path(), 2).unwrap();
    assert_eq!(authority.store_id(), id);
    assert_eq!(authority.revision(), 1);
    let manifest_path = temp.path().join("manifest.sqlite");
    let before = fs::read(&manifest_path).unwrap();
    assert_eq!(
        Database::open(temp.path(), 2).unwrap_err().kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert_eq!(fs::read(manifest_path).unwrap(), before);
    assert!(provision(temp.path(), 2, &catalog).is_err());
    assert_eq!(open(temp.path(), 2).unwrap().store_id(), id);
    assert_eq!(
        fs::metadata(temp.path().join(FILE_NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn activation_excludes_idle_local_owners_and_does_not_write_a_store() {
    let temp = root();
    let owner = Database::open(temp.path(), 2).unwrap();
    let (catalog, _, _) = fixtures::setup();
    assert_eq!(
        provision(temp.path(), 2, &catalog).unwrap_err().kind(),
        EngineErrorKind::Busy
    );
    assert!(!temp.path().join(FILE_NAME).exists());
    drop(owner);
    provision(temp.path(), 2, &catalog).unwrap();
}

#[test]
fn activation_excludes_peer_process_owners() {
    let temp = root();
    let peer = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::security_root::tests::peer_process_activation_probe",
            "--nocapture",
        ])
        .env("BRISKDB_SECURITY_ROOT_PARENT_TEST", temp.path())
        .spawn()
        .unwrap();
    // The child performs the activation while this process retains the root lease.
    // A file handshake keeps the check independent of scheduling speed.
    let owner = Database::open(temp.path(), 2).unwrap();
    fs::write(temp.path().join("owner-ready"), b"ready").unwrap();
    let status = peer.wait_with_output().unwrap().status;
    drop(owner);
    assert!(status.success());
    assert!(!temp.path().join(FILE_NAME).exists());
}

#[test]
fn peer_process_activation_probe() {
    let Some(path) = std::env::var_os("BRISKDB_SECURITY_ROOT_PARENT_TEST") else {
        return;
    };
    let root = std::path::PathBuf::from(path);
    let started = std::time::Instant::now();
    while !root.join("owner-ready").exists() {
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let (catalog, _, _) = fixtures::setup();
    assert_eq!(
        provision(&root, 2, &catalog).unwrap_err().kind(),
        EngineErrorKind::Busy
    );
}

#[test]
fn orphan_and_wrong_store_are_never_adopted_or_overwritten() {
    let temp = root();
    let path = temp.path().join(FILE_NAME);
    let (catalog, _, _) = fixtures::setup();
    let id = SecurityStoreId::generate().unwrap();
    drop(SecurityCatalogStore::create(&path, id, &catalog).unwrap());
    let before = fs::read(&path).unwrap();
    assert!(provision(temp.path(), 2, &catalog).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(open(temp.path(), 2).is_err());
    drop(Database::open(temp.path(), 2).unwrap());
    // Retain the orphan rather than deleting it, then activate a new store.
    fs::rename(&path, path.with_extension("orphan")).unwrap();
    provision(temp.path(), 2, &catalog).unwrap();
    fs::rename(&path, path.with_extension("activated")).unwrap();
    fs::rename(path.with_extension("orphan"), &path).unwrap();
    assert!(open(temp.path(), 2).is_err());
    assert!(Database::open(temp.path(), 2).is_err());
}

#[test]
fn missing_store_never_falls_back_or_recreates_it() {
    let temp = root();
    let (catalog, _, _) = fixtures::setup();
    provision(temp.path(), 2, &catalog).unwrap();
    let path = temp.path().join(FILE_NAME);
    fs::rename(&path, path.with_extension("retained")).unwrap();
    assert!(open(temp.path(), 2).is_err());
    assert!(!path.exists());
    assert!(Database::open(temp.path(), 2).is_err());
}

#[test]
fn activation_rejects_nonprivate_empty_and_missing_roots_without_creating_storage() {
    let temp = root();
    assert!(provision(temp.path(), 2, &SecurityCatalog::new()).is_err());
    assert!(!temp.path().join(FILE_NAME).exists());
    let (catalog, _, _) = fixtures::setup();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(provision(temp.path(), 2, &catalog).is_err());
    assert!(!temp.path().join(FILE_NAME).exists());
    let missing = temp.path().join("missing");
    assert!(provision(&missing, 2, &catalog).is_err());
    assert!(!missing.exists());
}
