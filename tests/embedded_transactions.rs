use std::time::Duration;

use briskdb::{
    BriskCursor, BriskDb, BriskSession, BriskTransaction, EngineErrorKind, PrepareRequest,
    SessionState, SqlDialect, SqlTranslationMode, Statement, TransactionExecution, Value,
    core::{Database, ShardKeyMetadata, ShardKeyType, TableDeclaration},
};

async fn open_database() -> (tempfile::TempDir, BriskDb) {
    let directory = tempfile::tempdir().unwrap();
    let mut storage = Database::open(directory.path(), 2).unwrap();
    storage
        .broadcast("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        .unwrap();
    let logical_database = storage.catalog().default_database().id();
    storage
        .register_tables(vec![
            TableDeclaration::sharded(
                logical_database,
                "records",
                ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    drop(storage);
    let database = BriskDb::open(directory.path()).await.unwrap();
    (directory, database)
}

async fn row_count(database: &BriskDb, id: i64) -> i64 {
    let session = database.owned_session();
    let result = session
        .query_logical(Statement::new(
            "SELECT COUNT(*) FROM records WHERE id = ?1",
            vec![Value::from(id)],
        ))
        .await
        .unwrap();
    let count = result.value.rows()[0]
        .get(0)
        .and_then(Value::as_i64)
        .unwrap();
    session.close().await.unwrap();
    count
}

async fn ids_on_different_shards(database: &BriskDb) -> (i64, i64) {
    let session = database.owned_session();
    let mut first = None;
    for id in 1_i64..=128 {
        let result = session
            .query_logical(Statement::new(
                "SELECT id FROM records WHERE id = ?1",
                vec![Value::from(id)],
            ))
            .await
            .unwrap();
        let shard = result.shards[0];
        match first {
            None => first = Some((id, shard)),
            Some((first_id, first_shard)) if first_shard != shard => {
                session.close().await.unwrap();
                return (first_id, id);
            }
            Some(_) => {}
        }
    }
    panic!("test keys did not cover two shards")
}

#[test]
fn embedded_handles_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<BriskDb>();
    assert_send_sync::<BriskSession>();
    assert_send_sync::<BriskTransaction>();
    assert_send_sync::<BriskCursor>();
}

#[tokio::test]
async fn owned_transaction_commits_and_drop_rolls_back() {
    let (_directory, database) = open_database().await;

    let dropped = database.begin_transaction().await.unwrap();
    dropped
        .execute_write(Statement::new(
            "INSERT INTO records (id, value) VALUES (?1, ?2)",
            vec![Value::from(1_i64), Value::from("rolled back")],
        ))
        .await
        .unwrap();
    assert_eq!(dropped.state().await, SessionState::InTransaction);
    drop(dropped);
    assert_eq!(row_count(&database, 1).await, 0);

    let committed = database.begin_transaction().await.unwrap();
    committed
        .execute_write(Statement::new(
            "INSERT INTO records (id, value) VALUES (?1, ?2)",
            vec![Value::from(2_i64), Value::from("committed")],
        ))
        .await
        .unwrap();
    assert_eq!(
        committed.commit().await.unwrap(),
        TransactionExecution::Committed
    );
    assert_eq!(row_count(&database, 2).await, 1);

    database.close().await.unwrap();
}

#[tokio::test]
async fn cross_shard_failure_puts_transaction_in_failed_state_and_commit_rolls_back() {
    let (_directory, database) = open_database().await;
    let (first_id, second_id) = ids_on_different_shards(&database).await;

    let transaction = database.begin_transaction().await.unwrap();
    transaction
        .execute_write(Statement::new(
            "INSERT INTO records (id, value) VALUES (?1, ?2)",
            vec![Value::from(first_id), Value::from("first shard")],
        ))
        .await
        .unwrap();
    let error = transaction
        .execute_write(Statement::new(
            "INSERT INTO records (id, value) VALUES (?1, ?2)",
            vec![Value::from(second_id), Value::from("second shard")],
        ))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert_eq!(transaction.state().await, SessionState::FailedTransaction);
    assert_eq!(
        transaction.commit().await.unwrap(),
        TransactionExecution::RolledBack
    );
    assert_eq!(row_count(&database, first_id).await, 0);
    assert_eq!(row_count(&database, second_id).await, 0);

    database.close().await.unwrap();
}

#[tokio::test]
async fn cursor_streams_metadata_and_rows_and_drop_does_not_block_shutdown() {
    let (_directory, database) = open_database().await;
    let session = database.owned_session();
    for id in 1_i64..=32 {
        session
            .execute_write(Statement::new(
                "INSERT INTO records (id, value) VALUES (?1, ?2)",
                vec![Value::from(id), Value::from(format!("value-{id}"))],
            ))
            .await
            .unwrap();
    }

    let statement = session
        .prepare(PrepareRequest::new(
            database.catalog().default_database().id(),
            SqlDialect::Sqlite,
            SqlTranslationMode::StrictSqlite,
            "SELECT id, value FROM records",
        ))
        .await
        .unwrap();
    let portal = session.bind(statement, vec![]).await.unwrap();
    let mut cursor = session.stream_bound_logical(portal).await.unwrap();
    assert_eq!(cursor.shards().len(), 2);
    assert_eq!(cursor.columns().len(), 2);
    assert_eq!(cursor.columns()[0].name, "id");
    assert!(cursor.next_row().await.unwrap().is_ok());
    drop(cursor);

    let retry_portal = session.bind(statement, vec![]).await.unwrap();
    let mut retry = session.stream_bound_logical(retry_portal).await.unwrap();
    let mut rows = 0;
    while let Some(row) = retry.next_row().await {
        row.unwrap();
        rows += 1;
    }
    assert_eq!(rows, 32);
    session.close_prepared(statement).await.unwrap();

    session.close().await.unwrap();
    let report = database
        .close_with_grace(Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!report.forced());
}
