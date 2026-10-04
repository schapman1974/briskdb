use super::*;
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, memory::InMemory, path::Path as ObjectPath,
};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct FaultStore {
    inner: InMemory,
    // 1: payload write failure; 2: head failure before commit; 3: lost reply
    // after committing a head; 4: one definite CAS conflict; 5: corrupt reads;
    // 6: ambiguous SDK retry after the receipt retention window advances.
    fault: AtomicU8,
    gets: AtomicUsize,
    lists: AtomicUsize,
    head_gate: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fault-injection store")
    }
}
fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "fault test",
        source: "injected failure".into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let fault = self.fault.load(Ordering::SeqCst);
        let updating = matches!(options.mode, PutMode::Update(_));
        let gate = if updating {
            self.head_gate.lock().unwrap().take()
        } else {
            None
        };
        if let Some((entered, release)) = gate {
            entered.send(()).unwrap();
            release
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
        if updating && fault == 8 {
            self.fault.store(0, Ordering::SeqCst);
            let result = self.inner.put_opts(path, payload, options).await?;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            return Ok(result);
        }
        if updating && fault == 9 {
            return Err(object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: "injected denied head publication".into(),
            });
        }
        if fault == 7 && path.as_ref().ends_with("/result.json") {
            self.fault.store(0, Ordering::SeqCst);
            return Err(failure());
        }
        if updating && fault == 6 {
            self.fault.store(0, Ordering::SeqCst);
            self.inner.put_opts(path, payload, options).await?;
            let bytes = self.inner.get(path).await?.bytes().await?;
            let mut head: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let sequence = head["sequence"].as_u64().unwrap() + 256;
            head["sequence"] = sequence.into();
            head["revision"] = sequence.into();
            head["base_through"] = sequence.into();
            head["deltas"] = serde_json::json!([]);
            head["receipts"] = serde_json::json!([]);
            self.inner
                .put(path, Bytes::from(serde_json::to_vec(&head).unwrap()).into())
                .await?;
            return Err(object_store::Error::Precondition {
                path: path.to_string(),
                source: "SDK retry after lost reply".into(),
            });
        }
        if fault == 1 && path.as_ref().ends_with(".parquet") {
            self.fault.store(0, Ordering::SeqCst);
            return Err(failure());
        }
        if updating && matches!(fault, 2..=4) {
            self.fault.store(0, Ordering::SeqCst);
            if fault == 3 {
                self.inner.put_opts(path, payload, options).await?;
                return Err(failure());
            }
            if fault == 4 {
                return Err(object_store::Error::Precondition {
                    path: path.to_string(),
                    source: "injected race".into(),
                });
            }
            return Err(failure());
        }
        self.inner.put_opts(path, payload, options).await
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.fault.load(Ordering::SeqCst) == 5 && path.as_ref().ends_with(".parquet") {
            return Err(failure());
        }
        self.inner.get_opts(path, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(path).await
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn fixture() -> (tempfile::TempDir, Arc<FaultStore>, Database) {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(FaultStore::default());
    let config = super::tests::config();
    let database = Database::create(
        root.path().join("db"),
        config,
        store.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    (root, store, database)
}

fn point_request(value: &str) -> UpdateRequest {
    UpdateRequest {
        operation_id: nonce().unwrap(),
        table: "items".into(),
        key: BTreeMap::from([("id".into(), Cell::Text("a".into()))]),
        set: BTreeMap::from([("value".into(), Cell::Text(value.into()))]),
        increment: BTreeMap::new(),
        expected: BTreeMap::new(),
    }
}

fn update_race(disjoint: bool, rebase: bool, expected: bool) -> UpdateResult {
    let (root, store, mut other) = fixture();
    other
        .execute("INSERT INTO items VALUES ('a','original')", &[])
        .unwrap();
    let partition = other.config().partition(&Cell::Text("a".into())).unwrap();
    let other_id = (0..100)
        .map(|n| format!("other-{n}"))
        .find(|id| other.config().partition(&Cell::Text(id.clone())).unwrap() == partition)
        .unwrap();
    other
        .execute(
            "INSERT INTO items VALUES (?, 'unrelated')",
            &[Cell::Text(other_id.clone())],
        )
        .unwrap();
    let mut update = point_request("new");
    if expected {
        update
            .expected
            .insert("value".into(), Cell::Text("original".into()));
    }
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *store.head_gate.lock().unwrap() = Some((entered_tx, release_rx));
    let path = root.path().join("db");
    let worker = std::thread::spawn(move || {
        let mut db = Database::open(path, store).unwrap();
        db.update(
            &update,
            RetryOptions {
                timeout_ms: 10_000,
                rebase_disjoint: rebase,
                allow_compaction: true,
                ..Default::default()
            },
        )
        .unwrap()
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    let target = if disjoint { other_id } else { "a".into() };
    other
        .execute(
            "UPDATE items SET value='competing' WHERE id=?",
            &[Cell::Text(target)],
        )
        .unwrap();
    release_tx.send(()).unwrap();
    worker.join().unwrap()
}

#[test]
fn conflicting_point_update_replays_only_after_definite_no_commit() {
    let result = update_race(false, true, false);
    assert_eq!(result.statement_retries, 1);
    assert_eq!(result.status, "committed");
    assert_eq!(result.affected_rows, 1);
}

#[test]
fn stale_editor_condition_is_not_removed_by_retry() {
    let result = update_race(false, true, true);
    assert_eq!(result.statement_retries, 1);
    assert_eq!(result.status, "condition_not_met");
    assert_eq!(result.affected_rows, 0);
}

#[test]
fn disjoint_record_update_rebases_without_reexecuting_the_statement() {
    let result = update_race(true, true, false);
    assert_eq!(result.statement_retries, 0);
    assert_eq!(result.publication_retries, 1);
    assert_eq!(result.status, "committed");
    let conservative = update_race(true, false, false);
    assert_eq!(conservative.statement_retries, 1);
}

#[test]
fn update_deadline_after_publication_is_reconciled_by_operation_id() {
    let (_root, store, mut db) = fixture();
    db.execute("INSERT INTO items VALUES ('a','original')", &[])
        .unwrap();
    let update = point_request("new");
    store.fault.store(8, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let error = db
        .update(
            &update,
            RetryOptions {
                timeout_ms: 150,
                ..Default::default()
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
    assert!(started.elapsed() < std::time::Duration::from_millis(900));
    let result = db
        .update_status(&update.operation_id, 1000)
        .unwrap()
        .unwrap();
    assert_eq!(result.status, "committed");
    assert_eq!(
        db.update(&update, RetryOptions::default())
            .unwrap()
            .commit_id,
        result.commit_id
    );
}

#[test]
fn denied_head_publication_is_a_permission_failure_not_a_transient_queue_trigger() {
    let (_root, store, mut db) = fixture();
    db.execute("INSERT INTO items VALUES ('a','original')", &[])
        .unwrap();
    let update = point_request("not-authorized");
    store.fault.store(9, Ordering::SeqCst);
    assert_eq!(
        db.update(&update, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    assert!(
        db.update_status(&update.operation_id, 1000)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Text("original".into())]]
    );
}

#[test]
fn receipt_archive_failure_never_forgets_a_committed_operation() {
    let (_root, store, mut db) = fixture();
    // No matching row: cheap metadata-only commits also need durable dedup.
    let first_request = point_request("first");
    let first = db.update(&first_request, RetryOptions::default()).unwrap();
    for _ in 0..255 {
        db.update(&point_request("unused"), RetryOptions::default())
            .unwrap();
    }
    store.fault.store(7, Ordering::SeqCst);
    let last = point_request("last");
    assert!(db.update(&last, RetryOptions::default()).is_err());
    assert_eq!(
        db.update_status(&first_request.operation_id, 1000)
            .unwrap()
            .unwrap()
            .commit_id,
        first.commit_id
    );
    db.update(&last, RetryOptions::default()).unwrap();
    assert_eq!(
        db.update(&first_request, RetryOptions::default())
            .unwrap()
            .commit_id,
        first.commit_id
    );
}

#[test]
fn failed_payload_and_failed_head_do_not_acknowledge_or_publish() {
    for fault in [1, 2] {
        let (_root, store, mut database) = fixture();
        store.fault.store(fault, Ordering::SeqCst);
        let error = database
            .execute("INSERT INTO items VALUES ('x','value')", &[])
            .unwrap_err();
        assert_ne!(error.kind(), EngineErrorKind::Busy); // Unknown is never safe-to-retry Busy.
        assert!(
            database
                .query("SELECT * FROM items", &[])
                .unwrap()
                .rows
                .is_empty()
        );
    }
}

#[test]
fn lost_success_reply_is_reconciled_without_duplicate_sql() {
    let (_root, store, mut database) = fixture();
    store.fault.store(3, Ordering::SeqCst);
    let result = database
        .execute("INSERT INTO items VALUES ('x','value')", &[])
        .unwrap();
    assert_eq!(result.affected_rows, 1);
    assert!(result.commit_id.is_some());
    assert_eq!(
        database
            .query("SELECT * FROM items", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn definite_conflict_retries_publication_only() {
    let (_root, store, mut database) = fixture();
    store.fault.store(4, Ordering::SeqCst);
    let result = database
        .execute("INSERT INTO items VALUES ('x','value')", &[])
        .unwrap();
    assert_eq!(result.publication_retries, 1);
    assert_eq!(
        database
            .query("SELECT * FROM items", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn expired_receipt_window_is_unknown_not_safe_to_replay() {
    let (_root, store, mut database) = fixture();
    store.fault.store(6, Ordering::SeqCst);
    let error = database
        .execute("INSERT INTO items VALUES ('x','value')", &[])
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::StorageUnavailable);
    assert!(error.to_string().contains("commit outcome unknown"));
}

#[test]
fn a_missing_pending_object_fails_closed() {
    let (_root, store, mut database) = fixture();
    database
        .execute("INSERT INTO items VALUES ('x','value')", &[])
        .unwrap();
    store.fault.store(5, Ordering::SeqCst);
    assert!(database.query("SELECT * FROM items", &[]).is_err());
}

#[test]
fn unrelated_table_writes_add_no_objects_to_an_indexed_read() {
    let (_root, store, mut database) = fixture();
    for i in 0..40 {
        database
            .execute(
                "INSERT INTO items VALUES (?,?)",
                &super::tests::row(&format!("id-{i}"), "value"),
            )
            .unwrap();
    }
    let before = store.gets.load(Ordering::SeqCst);
    assert!(
        database
            .query("SELECT * FROM labels WHERE id='chapter'", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(store.gets.load(Ordering::SeqCst) - before, 1); // Exactly one head, no LIST and no unrelated Parquet GET.
    assert_eq!(store.lists.load(Ordering::SeqCst), 0);
}

#[test]
fn new_writes_survive_concurrent_compactors() {
    let (root, store, mut database) = fixture();
    for i in 0..8 {
        database
            .execute(
                "INSERT INTO items VALUES (?,?)",
                &super::tests::row(&format!("before-{i}"), "old"),
            )
            .unwrap();
    }
    let path = root.path().join("db");
    let writer_path = path.clone();
    let writer_store = store.clone();
    let writer = std::thread::spawn(move || {
        let mut database = Database::open(writer_path, writer_store).unwrap();
        for i in 0..40 {
            database
                .execute(
                    "INSERT INTO items VALUES (?,?)",
                    &super::tests::row(&format!("during-{i}"), "new"),
                )
                .unwrap();
        }
    });
    let compactor_path = path.clone();
    let compactor_store = store.clone();
    let compactor = std::thread::spawn(move || {
        let mut database = Database::open(compactor_path, compactor_store).unwrap();
        for _ in 0..5 {
            database.compact_all().unwrap();
        }
    });
    for _ in 0..5 {
        database.compact_all().unwrap();
    }
    writer.join().unwrap();
    compactor.join().unwrap();
    let mut database = Database::open(path, store).unwrap();
    assert_eq!(
        database
            .query("SELECT * FROM items", &[])
            .unwrap()
            .rows
            .len(),
        48
    );
}
