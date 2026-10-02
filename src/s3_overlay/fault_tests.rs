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
