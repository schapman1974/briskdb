use std::{path::PathBuf, time::Duration};

use briskdb::{
    BriskDb, BriskDbBuilder, DEFAULT_EMBEDDED_SHARDS, DocumentSupport, EngineErrorKind,
    EngineOptions, EngineState, RuntimeBehavior, Statement, Value,
};

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
fn builder_defaults_are_public_deterministic_and_host_safe() {
    assert_send_sync_static::<BriskDb>();
    assert_send_sync_static::<BriskDbBuilder>();

    let builder = BriskDb::builder("data");
    assert_eq!(builder.root(), PathBuf::from("data"));
    assert_eq!(builder.shard_count(), DEFAULT_EMBEDDED_SHARDS);
    assert_eq!(builder.engine_options(), EngineOptions::default());
    assert_eq!(builder.runtime_behavior(), RuntimeBehavior::CallerManaged);
    assert_eq!(builder.document_support(), DocumentSupport::Disabled);
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

    let reopened = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
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
