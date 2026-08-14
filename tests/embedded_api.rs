use std::{path::PathBuf, time::Duration};

use briskdb::{
    BriskDb, BriskDbBuilder, DocumentSupport, EngineErrorKind, EngineOptions, EngineState,
    RuntimeBehavior, Statement, Value,
};
use rusqlite::Connection;

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

async fn create_notes(db: &BriskDb, route: &str, body: &str) {
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    db.migrate(
        &session,
        "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await
    .unwrap();
    db.execute_write(
        &session,
        Statement::new(
            "INSERT INTO notes (id, body) VALUES (?1, ?2)",
            vec![Value::from(1_i64), Value::from(body)],
        ),
    )
    .await
    .unwrap();
    session.close().await.unwrap();
}

async fn note_body(db: &BriskDb, route: &str) -> String {
    let session = db.session();
    session.set_routing_key(route).await.unwrap();
    let result = db
        .query(
            &session,
            Statement::new("SELECT body FROM notes WHERE id = ?1", vec![1_i64.into()]),
        )
        .await
        .unwrap();
    let body = result.value.rows()[0]
        .get(0)
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();
    session.close().await.unwrap();
    body
}

#[test]
fn builder_defaults_detect_existing_storage_and_are_host_safe() {
    assert_send_sync_static::<BriskDb>();
    assert_send_sync_static::<BriskDbBuilder>();

    let builder = BriskDb::builder("data");
    assert_eq!(builder.root(), PathBuf::from("data"));
    assert_eq!(builder.shard_count(), None);
    assert_eq!(builder.engine_options(), EngineOptions::default());
    assert_eq!(builder.runtime_behavior(), RuntimeBehavior::CallerManaged);
    assert_eq!(builder.document_support(), DocumentSupport::Disabled);
}

#[tokio::test]
async fn omitted_shards_require_initialized_storage_without_creating_files() {
    let parent = tempfile::tempdir().unwrap();
    let missing = parent.path().join("missing");
    let error = BriskDb::open(&missing).await.unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(error.to_string().contains("set a shard count to create it"));
    assert!(!missing.exists());

    let empty = parent.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let error = BriskDb::open(&empty).await.unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0);
}

#[tokio::test]
async fn omitted_shards_detect_existing_data_and_explicit_mismatch_is_stable() {
    let temp = tempfile::tempdir().unwrap();
    let created = BriskDb::builder(temp.path())
        .with_shard_count(6)
        .open()
        .await
        .unwrap();
    created.close().await.unwrap();

    let mismatch = BriskDb::builder(temp.path())
        .with_shard_count(4)
        .open()
        .await
        .unwrap_err();
    assert_eq!(mismatch.kind(), EngineErrorKind::FailedPrecondition);
    assert_eq!(
        mismatch.to_string(),
        "database was created with 6 shards, but 4 were requested"
    );

    let detected = BriskDb::open(temp.path()).await.unwrap();
    assert_eq!(detected.shard_count(), 6);
    detected.close().await.unwrap();
}

#[tokio::test]
async fn detected_shards_validate_count_dependent_resource_limits() {
    let temp = tempfile::tempdir().unwrap();
    BriskDb::builder(temp.path())
        .with_shard_count(33)
        .open()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    let options = EngineOptions::new(16, 1).unwrap();
    let error = BriskDb::builder(temp.path())
        .with_engine_options(options)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    assert!(error.to_string().contains("512 total active connections"));
}

#[tokio::test]
async fn shard_detection_fails_closed_for_corrupt_foreign_and_newer_manifests() {
    let parent = tempfile::tempdir().unwrap();

    let corrupt = parent.path().join("corrupt");
    std::fs::create_dir(&corrupt).unwrap();
    let corrupt_manifest = corrupt.join("manifest.sqlite");
    let corrupt_bytes = b"not a SQLite database";
    std::fs::write(&corrupt_manifest, corrupt_bytes).unwrap();
    let error = BriskDb::open(&corrupt).await.unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    assert_eq!(std::fs::read(&corrupt_manifest).unwrap(), corrupt_bytes);
    assert_eq!(std::fs::read_dir(&corrupt).unwrap().count(), 1);

    let foreign = parent.path().join("foreign");
    std::fs::create_dir(&foreign).unwrap();
    Connection::open(foreign.join("manifest.sqlite"))
        .unwrap()
        .execute_batch("CREATE TABLE application_data (value TEXT)")
        .unwrap();
    let error = BriskDb::open(&foreign).await.unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(error.to_string().contains("not an empty or recognized"));

    let newer = parent.path().join("newer");
    std::fs::create_dir(&newer).unwrap();
    Connection::open(newer.join("manifest.sqlite"))
        .unwrap()
        .execute_batch(
            "PRAGMA application_id = 1112687682;
             PRAGMA user_version = 999;",
        )
        .unwrap();
    let error = BriskDb::open(&newer).await.unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(error.to_string().contains("newer than this BriskDB build"));
}

#[tokio::test]
async fn invalid_complete_configuration_fails_before_creating_storage() {
    let parent = tempfile::tempdir().unwrap();
    let invalid_shards = parent.path().join("invalid-shards");
    let error = BriskDb::builder(&invalid_shards)
        .with_shard_count(1)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    assert!(!invalid_shards.exists());

    let unsupported_runtime = parent.path().join("unsupported-runtime");
    let error = BriskDb::builder(&unsupported_runtime)
        .with_runtime_behavior(RuntimeBehavior::Dedicated)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    assert!(!unsupported_runtime.exists());

    let unsupported_documents = parent.path().join("unsupported-documents");
    let error = BriskDb::builder(&unsupported_documents)
        .with_document_support(DocumentSupport::Enabled)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Unsupported);
    assert!(!unsupported_documents.exists());

    let empty = BriskDb::builder("").validate().unwrap_err();
    assert_eq!(empty.kind(), EngineErrorKind::InvalidArgument);
}

#[tokio::test]
async fn embedded_handle_opens_writes_queries_reports_status_closes_and_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let options = EngineOptions::new(1, 2)
        .unwrap()
        .with_request_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let db = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .with_engine_options(options)
        .open()
        .await
        .unwrap();
    let session = db.session();
    session.set_routing_key("account-1").await.unwrap();

    db.migrate(
        &session,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
    )
    .await
    .unwrap();
    let written = db
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO notes (id, body) VALUES (?1, ?2)",
                vec![1_i64.into(), "persistent".into()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(written.value.rows_affected, 1);
    assert_eq!(note_body(&db, "account-1").await, "persistent");
    assert_eq!(
        db.status(&session).await.unwrap().connections_per_shard(),
        1
    );
    assert_eq!(db.root(), temp.path());
    assert_eq!(db.shard_count(), 2);
    assert_eq!(db.state(), EngineState::Running);

    session.close().await.unwrap();
    db.close().await.unwrap();
    assert_eq!(db.state(), EngineState::Stopped);
    db.close().await.unwrap();

    let reopened = BriskDb::open(temp.path()).await.unwrap();
    assert_eq!(note_body(&reopened, "account-1").await, "persistent");
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn independent_instances_coexist_without_state_or_data_leakage() {
    let parent = tempfile::tempdir().unwrap();
    let first = BriskDb::builder(parent.path().join("first"))
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let second = BriskDb::builder(parent.path().join("second"))
        .with_shard_count(2)
        .open()
        .await
        .unwrap();

    create_notes(&first, "same-route", "first").await;
    create_notes(&second, "same-route", "second").await;
    assert_eq!(note_body(&first, "same-route").await, "first");
    assert_eq!(note_body(&second, "same-route").await, "second");

    first.close().await.unwrap();
    assert_eq!(second.state(), EngineState::Running);
    assert_eq!(note_body(&second, "same-route").await, "second");
    second.close().await.unwrap();
}
