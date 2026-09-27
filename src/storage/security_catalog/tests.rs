use super::*;
use crate::core::{
    authentication::ScramSha256Verifier,
    authorization::{Action, DataDomain, Policy, Privilege, Scope},
    security_catalog::SecurityName,
};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

fn name(value: &str) -> SecurityName {
    SecurityName::new("app", value).unwrap()
}
fn catalog() -> SecurityCatalog {
    let mut catalog = SecurityCatalog::new();
    let policy = Policy::new([Privilege::new(
        Action::ReadData,
        Scope::database(DataDomain::Document, "app").unwrap(),
    )
    .unwrap()])
    .unwrap();
    catalog.create_role(name("reader"), policy).unwrap();
    catalog
        .create_user(
            name("alice"),
            ScramSha256Verifier::from_password_with_iterations("test password", 4096).unwrap(),
            [name("reader")],
        )
        .unwrap();
    catalog
}
fn setup() -> (
    tempfile::TempDir,
    PathBuf,
    SecurityStoreId,
    SecurityCatalogStore,
) {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = temp.path().join("security.sqlite");
    let id = SecurityStoreId::generate().unwrap();
    let store = SecurityCatalogStore::create(&path, id, &catalog()).unwrap();
    (temp, path, id, store)
}

#[test]
fn replacement_cannot_reset_identity_history_or_bypass_credential_rotation() {
    let (_temp, path, id, mut store) = setup();
    let (_, original) = store.load().unwrap().into_parts();
    let before = fs::read(&path).unwrap();
    // A fresh salt without advancing the generation is not a valid rotation.
    // A newly constructed empty catalog also resets the identity allocator.
    for invalid in [catalog(), SecurityCatalog::new()] {
        let error = store.replace(1, &invalid).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
        assert!(error.to_string().contains("identity or credential history"));
        assert!(!store.fenced);
        assert_eq!(store.load().unwrap().revision(), 1);
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    let mut valid = SecurityCatalog::from_record(original.to_record().unwrap().as_bytes()).unwrap();
    valid
        .rotate_credentials(
            &name("alice"),
            ScramSha256Verifier::from_password_with_iterations("new password", 4096).unwrap(),
        )
        .unwrap();
    assert_eq!(store.replace(1, &valid).unwrap(), 2);
    assert!(store.replace(2, &original).is_err());
    assert!(!store.fenced);
    drop(store);
    let mut reopened = SecurityCatalogStore::open(&path, id).unwrap();
    let (_, restored) = reopened.load().unwrap().into_parts();
    assert_eq!(
        restored.to_record().unwrap().as_bytes(),
        valid.to_record().unwrap().as_bytes()
    );
    let removed = without_users(&mut reopened);
    assert_eq!(reopened.replace(2, &removed).unwrap(), 3);
    assert!(reopened.replace(3, &valid).is_err());
}

fn without_users(store: &mut SecurityCatalogStore) -> SecurityCatalog {
    let (_, mut state) = store.load().unwrap().into_parts();
    state.drop_user(&name("alice")).unwrap();
    state
}

#[test]
fn create_reopen_replace_preserves_complete_catalog_and_private_permissions() {
    let (_temp, path, id, mut store) = setup();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let (revision, mut original) = store.load().unwrap().into_parts();
    assert_eq!(revision, 1);
    assert_eq!(original.user_count(), 1);
    assert_eq!(store.id(), id);
    let record = original.to_record().unwrap();
    drop(store);
    let mut store = SecurityCatalogStore::open(&path, id).unwrap();
    let (_, loaded) = store.load().unwrap().into_parts();
    assert_eq!(loaded.to_record().unwrap().as_bytes(), record.as_bytes());
    original.drop_user(&name("alice")).unwrap();
    assert_eq!(store.replace(1, &original).unwrap(), 2);
    drop(store);
    let mut store = SecurityCatalogStore::open(&path, id).unwrap();
    let (revision, loaded) = store.load().unwrap().into_parts();
    assert_eq!(revision, 2);
    assert_eq!(loaded.user_count(), 0);
    assert_eq!(loaded.role_count(), 1);
}

#[test]
fn stale_writer_never_overwrites_a_newer_catalog() {
    let (_temp, path, id, mut first) = setup();
    let mut second = SecurityCatalogStore::open(&path, id).unwrap();
    let (version, mut stale) = second.load().unwrap().into_parts();
    let (_, mut current) = first.load().unwrap().into_parts();
    current.drop_user(&name("alice")).unwrap();
    first.replace(version, &current).unwrap();
    stale.create_role(name("stale"), Policy::default()).unwrap();
    assert_eq!(
        second.replace(version, &stale).unwrap_err().kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert!(!second.fenced);
    let (version, current) = second.load().unwrap().into_parts();
    assert_eq!(version, 2);
    assert_eq!(current.user_count(), 0);
    assert_eq!(current.role_count(), 1);
    assert_eq!(second.replace(version, &current).unwrap(), 3);
}

#[test]
fn independent_writers_serialize_and_exactly_one_wins_the_same_revision() {
    let (_temp, path, id, store) = setup();
    drop(store);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let workers = (0..2)
        .map(|number| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut store = SecurityCatalogStore::open(path, id).unwrap();
                let (revision, mut state) = store.load().unwrap().into_parts();
                state
                    .create_role(name(&format!("writer{number}")), Policy::default())
                    .unwrap();
                barrier.wait();
                store
                    .replace(revision, &state)
                    .map_err(|error| error.kind())
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| **result == Ok(2)).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(EngineErrorKind::FailedPrecondition))
            .count(),
        1
    );
    let (_, result) = SecurityCatalogStore::open(path, id)
        .unwrap()
        .load()
        .unwrap()
        .into_parts();
    assert_eq!(result.role_count(), 2);
}

#[test]
fn failed_update_rolls_back_and_fences_handle_until_explicit_reopen() {
    let (_temp, path, id, mut store) = setup();
    let (_, mut changed) = store.load().unwrap().into_parts();
    changed.drop_user(&name("alice")).unwrap();
    let error = store
        .replace_with_hook(1, &changed, |_| {
            Err(failure(
                EngineErrorKind::Cancelled,
                "injected precommit failure",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert!(store.fenced);
    assert!(store.load().is_err());
    assert!(store.replace(1, &changed).is_err());
    drop(store);
    let mut reopened = SecurityCatalogStore::open(path, id).unwrap();
    let (revision, state) = reopened.load().unwrap().into_parts();
    assert_eq!(revision, 1);
    assert_eq!(state.user_count(), 1);
    assert_eq!(reopened.replace(1, &changed).unwrap(), 2);
}

#[test]
fn invalid_ids_missing_and_existing_paths_do_not_initialize_or_truncate() {
    assert!(SecurityStoreId::from_bytes([0; 16]).is_err());
    let (temp, path, id, store) = setup();
    drop(store);
    let before = fs::read(&path).unwrap();
    assert!(SecurityCatalogStore::create(&path, id, &SecurityCatalog::new()).is_err());
    assert!(SecurityCatalogStore::open(&path, SecurityStoreId::generate().unwrap()).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    let missing = temp.path().join("missing.sqlite");
    assert!(SecurityCatalogStore::open(&missing, id).is_err());
    assert!(!missing.exists());
    let empty = temp.path().join("empty.sqlite");
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&empty)
        .unwrap();
    assert!(SecurityCatalogStore::open(&empty, id).is_err());
    assert_eq!(fs::metadata(empty).unwrap().len(), 0);
}

#[test]
fn unrelated_sqlite_wrong_version_and_schema_tampering_fail_closed() {
    for mutation in [
        "PRAGMA application_id=1",
        "PRAGMA user_version=2",
        "CREATE TABLE surprise(x)",
        "DELETE FROM briskdb_security_state",
        "UPDATE briskdb_security_state SET record=zeroblob(52)",
    ] {
        let (_temp, path, id, store) = setup();
        drop(store);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(mutation).unwrap();
        drop(connection);
        let before = fs::read(&path).unwrap();
        assert!(
            SecurityCatalogStore::open(&path, id).is_err(),
            "accepted {mutation}"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}

#[test]
fn public_or_nonregular_paths_are_rejected_without_permission_repair() {
    let (temp, path, id, store) = setup();
    drop(store);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        SecurityCatalogStore::open(&path, id).unwrap_err().kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(SecurityCatalogStore::open(&path, id).is_err());
    assert!(
        SecurityCatalogStore::create(temp.path().join("new.sqlite"), id, &SecurityCatalog::new())
            .is_err()
    );
    assert!(!temp.path().join("new.sqlite").exists());
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let link = temp.path().join("link.sqlite");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    assert!(SecurityCatalogStore::open(link, id).is_err());
    let directory = temp.path().join("directory");
    fs::create_dir(&directory).unwrap();
    assert!(SecurityCatalogStore::open(directory, id).is_err());
    let fifo = temp.path().join("fifo");
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: c_path is NUL-terminated and remains live for this call.
    assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
    let began = std::time::Instant::now();
    assert!(SecurityCatalogStore::open(fifo, id).is_err());
    assert!(began.elapsed() < Duration::from_secs(1));
}

#[test]
fn retained_handle_rejects_replaced_file_and_permissions_changes() {
    let (temp, path, id, mut store) = setup();
    fs::rename(&path, temp.path().join("original.sqlite")).unwrap();
    let replacement = SecurityCatalogStore::create(&path, id, &SecurityCatalog::new()).unwrap();
    assert!(store.load().is_err());
    assert!(store.replace(1, &catalog()).is_err());
    drop(replacement);
    drop(store);
    let mut store = SecurityCatalogStore::open(&path, id).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(store.load().is_err());
}

#[test]
fn revision_overflow_and_observed_rollback_are_rejected() {
    let (_temp, path, _id, mut store) = setup();
    let (_, state) = store.load().unwrap().into_parts();
    for revision in [0, i64::MAX as u64, u64::MAX] {
        assert!(store.replace(revision, &state).is_err());
    }
    assert_eq!(store.load().unwrap().revision(), 1);
    store.replace(1, &state).unwrap();
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("UPDATE briskdb_security_state SET revision = 1", [])
        .unwrap();
    assert_eq!(
        store.load().unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn store_and_snapshot_debug_do_not_expose_names_paths_or_credentials() {
    let (_temp, path, id, mut store) = setup();
    let snapshot = store.load().unwrap();
    for text in [
        format!("{store:?}"),
        format!("{snapshot:?}"),
        format!("{id:?}"),
    ] {
        for secret in [path.to_str().unwrap(), "alice", "reader", "test password"] {
            assert!(!text.contains(secret));
        }
    }
    // Data remains a salted verifier after an actual file reopen, not a password.
    drop(store);
    assert!(
        !fs::read(path)
            .unwrap()
            .windows(b"test password".len())
            .any(|bytes| bytes == b"test password")
    );
}

#[test]
fn conflicting_update_still_advances_the_observed_rollback_floor() {
    let (_temp, path, id, mut first) = setup();
    let mut second = SecurityCatalogStore::open(&path, id).unwrap();
    let (_, state) = first.load().unwrap().into_parts();
    first.replace(1, &state).unwrap();
    assert!(second.replace(1, &state).is_err());
    assert_eq!(second.observed_revision, 2);
    Connection::open(&path)
        .unwrap()
        .execute("UPDATE briskdb_security_state SET revision = 1", [])
        .unwrap();
    assert_eq!(
        second.load().unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn file_replacement_during_a_write_rolls_back_and_fences_the_original_handle() {
    let (temp, path, id, mut store) = setup();
    let original = temp.path().join("original.sqlite");
    let changed = without_users(&mut store);
    let error = store
        .replace_with_hook(1, &changed, |_| {
            fs::rename(&path, &original).unwrap();
            // Keep the replacement independent of the original SQLite journal.
            let mut sentinel = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .unwrap();
            std::io::Write::write_all(&mut sentinel, b"replacement sentinel").unwrap();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    assert!(store.fenced);
    assert!(store.load().is_err());
    drop(store);
    assert_eq!(fs::read(&path).unwrap(), b"replacement sentinel");
    let (revision, recovered) = SecurityCatalogStore::open(&original, id)
        .unwrap()
        .load()
        .unwrap()
        .into_parts();
    assert_eq!(revision, 1);
    assert_eq!(recovered.user_count(), 1);
}

#[test]
fn busy_commit_is_bounded_fences_the_handle_and_reopens_the_old_revision() {
    let (_temp, path, id, mut store) = setup();
    let changed = without_users(&mut store);
    let mut reader = Connection::open(&path).unwrap();
    let transaction = reader.transaction().unwrap();
    let revision: i64 = transaction
        .query_row("SELECT revision FROM briskdb_security_state", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(revision, 1);
    let began = std::time::Instant::now();
    let error = store.replace(1, &changed).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Busy);
    assert!(began.elapsed() < Duration::from_secs(6));
    assert!(store.fenced);
    assert!(store.load().is_err());
    transaction.rollback().unwrap();
    drop(reader);
    drop(store);
    let mut reopened = SecurityCatalogStore::open(path, id).unwrap();
    let (revision, catalog) = reopened.load().unwrap().into_parts();
    assert_eq!(revision, 1);
    assert_eq!(catalog.user_count(), 1);
    assert_eq!(
        reopened
            .connection
            .limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH)
            .unwrap(),
        (MAX_SECURITY_CATALOG_RECORD_BYTES + 4096) as i32
    );
}

#[test]
fn crash_writer_child() {
    let Some(path) = std::env::var_os("BRISKDB_CATALOG_STORE_TEST_CRASH_PATH") else {
        return;
    };
    let id = SecurityStoreId::from_bytes([7; 16]).unwrap();
    let mut store = SecurityCatalogStore::open(PathBuf::from(path), id).unwrap();
    let changed = without_users(&mut store);
    store
        .replace_with_hook(1, &changed, |_| std::process::exit(79))
        .unwrap();
    panic!("crash hook did not exit");
}

#[test]
fn process_exit_before_commit_recovers_the_complete_previous_catalog() {
    let temp = tempfile::tempdir().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let path = temp.path().join("security.sqlite");
    let id = SecurityStoreId::from_bytes([7; 16]).unwrap();
    drop(SecurityCatalogStore::create(&path, id, &catalog()).unwrap());
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::security_catalog::tests::crash_writer_child",
            "--nocapture",
        ])
        .env("BRISKDB_CATALOG_STORE_TEST_CRASH_PATH", &path)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(79),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut store = SecurityCatalogStore::open(&path, id).unwrap();
    let (revision, recovered) = store.load().unwrap().into_parts();
    assert_eq!(revision, 1);
    assert_eq!(recovered.user_count(), 1);
    assert_eq!(recovered.role_count(), 1);
    let changed = without_users(&mut store);
    assert_eq!(store.replace(1, &changed).unwrap(), 2);
    drop(store);
    assert_eq!(
        SecurityCatalogStore::open(path, id)
            .unwrap()
            .load()
            .unwrap()
            .revision(),
        2
    );
}
