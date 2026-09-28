use super::*;
use crate::{core::CancellationReason, document::DocumentCollectionOptions};
use std::sync::atomic::Ordering;

fn setup() -> (tempfile::TempDir, Storage) {
    let root = tempfile::tempdir().unwrap();
    let storage = Storage::open(root.path(), 2).unwrap();
    storage
        .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
        .unwrap();
    storage.manifest_readers.close_idle().unwrap();
    (root, storage)
}

fn lookup(storage: &Storage, name: &str) -> EngineResult<bool> {
    let _admission = storage.enter_schema_operation()?;
    storage
        .document_collection_controlled("app", name, OperationControl::new(None))
        .map(|collection| collection.is_some())
}

#[test]
fn repeated_collection_reads_reuse_one_handle_without_caching_absence() {
    let (root, storage) = setup();
    let baseline = storage.manifest_readers.opened.load(Ordering::Relaxed);
    for _ in 0..10 {
        assert!(lookup(&storage, "items").unwrap());
        assert!(!lookup(&storage, "later").unwrap());
    }
    assert_eq!(
        storage.manifest_readers.opened.load(Ordering::Relaxed),
        baseline + 1
    );
    assert_eq!(storage.manifest_readers.lock().unwrap().len(), 1);
    assert!(storage.manifest_readers.lock().unwrap()[0].is_autocommit());

    // An independent engine handle publishes through the normal schema gate.
    // Warm readers must observe its commit, even after a cached negative lookup.
    let peer = Storage::open(root.path(), 2).unwrap();
    peer.create_document_collection("app", "later", &DocumentCollectionOptions::empty())
        .unwrap();
    assert!(lookup(&storage, "later").unwrap());
    assert_eq!(
        storage.manifest_readers.opened.load(Ordering::Relaxed),
        baseline + 1
    );
    assert_eq!(storage.document_catalog().unwrap().collections().len(), 2);
    let _admission = storage.enter_schema_operation().unwrap();
    assert_eq!(
        storage
            .document_collections_for_database_controlled("app", OperationControl::new(None))
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn warm_manifest_reads_revalidate_semantic_metadata_and_fail_closed() {
    let (root, storage) = setup();
    assert!(lookup(&storage, "items").unwrap());
    let raw = Connection::open(root.path().join("manifest.sqlite")).unwrap();
    raw.execute(
        "UPDATE briskdb_document_collections SET collection_name = 'tampered'",
        [],
    )
    .unwrap();
    assert_eq!(
        lookup(&storage, "items").unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(storage.manifest_readers.lock().unwrap().is_empty());
    assert!(storage.enter_schema_operation().is_err());
}

#[cfg(unix)]
#[test]
fn warm_manifest_reads_reject_missing_replaced_and_nonregular_paths() {
    for mode in [
        "missing",
        "replacement",
        "symlink",
        "directory",
        "parent-symlink",
    ] {
        let (root, storage) = setup();
        assert!(lookup(&storage, "items").unwrap());
        let path = root.path().join("manifest.sqlite");
        let raw = Connection::open(&path).unwrap();
        let checkpoint: (i64, i64, i64) = raw
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(checkpoint, (0, 0, 0));
        drop(raw);
        let relocated = tempfile::tempdir().unwrap();
        if mode == "parent-symlink" {
            let target = relocated.path().join("root");
            std::fs::rename(root.path(), &target).unwrap();
            std::os::unix::fs::symlink(&target, root.path()).unwrap();
        } else {
            let target = root.path().join("original.sqlite");
            std::fs::rename(&path, &target).unwrap();
            match mode {
                "replacement" => {
                    std::fs::copy(&target, &path).unwrap();
                }
                "symlink" => std::os::unix::fs::symlink(&target, &path).unwrap(),
                "directory" => std::fs::create_dir(&path).unwrap(),
                _ => {}
            }
        }
        let error = lookup(&storage, "items").unwrap_err();
        assert_eq!(
            error.kind(),
            if matches!(mode, "missing" | "replacement") {
                EngineErrorKind::DataCorruption
            } else {
                EngineErrorKind::FailedPrecondition
            },
            "{mode}"
        );
        assert!(storage.manifest_readers.lock().unwrap().is_empty());
        if mode == "missing" {
            assert!(!path.exists());
        }
        // Drop live SQLite handles before the relocated fixture is removed.
        drop(storage);
    }
}

#[test]
fn idle_manifest_readers_do_not_retain_controls_or_read_locks() {
    let (root, storage) = setup();
    let path = root.path().join("manifest.sqlite");
    let control = OperationControl::new(None);
    storage
        .manifest_readers
        .read(&path, Arc::clone(&control), |_| Ok(()))
        .unwrap();
    control.request_cancel(CancellationReason::DeadlineExceeded);
    let opened = storage.manifest_readers.opened.load(Ordering::Relaxed);
    storage
        .manifest_readers
        .read(&path, OperationControl::new(None), |connection| {
            let count: i64 = connection
                .query_row(
                    "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<2000)
             SELECT count(*) FROM n",
                    [],
                    |row| row.get(0),
                )
                .map_err(sqlite_error::storage)?;
            assert_eq!(count, 2001);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        storage.manifest_readers.opened.load(Ordering::Relaxed),
        opened
    );
    let writer = Connection::open(&path).unwrap();
    let checkpoint: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(checkpoint, (0, 0, 0));
}

#[test]
fn errors_panics_cancellation_and_open_transactions_retire_manifest_readers() {
    let (root, storage) = setup();
    let path = root.path().join("manifest.sqlite");
    for mode in ["error", "cancel", "panic", "transaction"] {
        assert!(lookup(&storage, "items").unwrap());
        let control = OperationControl::new(None);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            storage
                .manifest_readers
                .read(&path, Arc::clone(&control), |connection| match mode {
                    "error" => Err(EngineError::new(
                        EngineErrorKind::Busy,
                        "injected read error",
                    )),
                    "cancel" => {
                        control.request_cancel(CancellationReason::DeadlineExceeded);
                        Ok(())
                    }
                    "panic" => panic!("injected read panic"),
                    "transaction" => connection
                        .execute_batch("BEGIN DEFERRED")
                        .map_err(sqlite_error::storage),
                    _ => unreachable!(),
                })
        }));
        match mode {
            "error" => assert_eq!(result.unwrap().unwrap_err().kind(), EngineErrorKind::Busy),
            "cancel" => assert_eq!(
                result.unwrap().unwrap_err().kind(),
                EngineErrorKind::DeadlineExceeded
            ),
            "panic" => assert!(result.is_err()),
            "transaction" => result.unwrap().unwrap(),
            _ => unreachable!(),
        }
        assert!(
            storage.manifest_readers.lock().unwrap().is_empty(),
            "{mode}"
        );
    }
    assert!(lookup(&storage, "items").unwrap());
}

#[test]
fn cancelled_reads_do_not_open_or_create_a_missing_manifest() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("missing.sqlite");
    let readers = ManifestReaders::default();
    let control = OperationControl::new(None);
    control.request_cancel(CancellationReason::DeadlineExceeded);
    assert_eq!(
        readers.read(&path, control, |_| Ok(())).unwrap_err().kind(),
        EngineErrorKind::DeadlineExceeded
    );
    assert_eq!(readers.opened.load(Ordering::Relaxed), 0);
    assert!(!path.exists());
}

#[test]
fn idle_manifest_readers_are_bounded_and_drained_at_shutdown_and_schema_retirement() {
    let (root, storage) = setup();
    let path = root.path().join("manifest.sqlite");
    // Independent threads model overlapping controlled readers: the dedicated
    // control helper intentionally prohibits nesting its TLS busy owner.
    let (started, ready) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Vec::new();
        for _ in 0..4 {
            let (release, wait) = std::sync::mpsc::channel();
            releases.push(release);
            let started = started.clone();
            let readers = &storage.manifest_readers;
            let path = &path;
            scope.spawn(move || {
                readers
                    .read(path, OperationControl::new(None), |_| {
                        started.send(()).unwrap();
                        wait.recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                        Ok(())
                    })
                    .unwrap()
            });
        }
        for _ in 0..4 {
            ready
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        for release in releases {
            release.send(()).unwrap();
        }
    });
    assert_eq!(
        storage.manifest_readers.lock().unwrap().len(),
        MAX_IDLE_READERS
    );
    let pools = pool::ConnectionPools::new(storage.clone(), 1, 0).unwrap();
    assert_eq!(pools.close_idle().unwrap(), MAX_IDLE_READERS);
    assert!(lookup(&storage, "items").unwrap());
    assert_eq!(pools.retire_idle_for_schema_migration().unwrap(), 1);
    assert!(storage.manifest_readers.lock().unwrap().is_empty());
}

#[test]
fn identity_probe_fails_closed_or_requires_a_fresh_open() {
    assert!(interpret_identity_probe(ffi::SQLITE_OK, 0).unwrap());
    assert_eq!(
        interpret_identity_probe(ffi::SQLITE_OK, 1)
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(!interpret_identity_probe(ffi::SQLITE_NOTFOUND, 0).unwrap());
    assert!(!interpret_identity_probe(ffi::SQLITE_NOTFOUND, 1).unwrap());
    assert_eq!(
        interpret_identity_probe(ffi::SQLITE_IOERR, 0)
            .unwrap_err()
            .kind(),
        EngineErrorKind::StorageUnavailable
    );
}
