use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use briskdb::{
    BriskDb, DataType, DescribeTarget, PrepareRequest, PreparedExecution, SqlDialect,
    SqlTranslationMode, Statement, Value,
};
use serde_json::{Value as JsonValue, json};
use tower::ServiceExt;

async fn request_json(router: axum::Router, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn embedded_prepared_commands_preserve_order_values_and_handle_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let session = database.session();
    session.set_routing_key("prepared-owner").await.unwrap();

    let statement = database
        .prepare(
            &session,
            PrepareRequest::new(
                database.catalog().default_database().id(),
                SqlDialect::Sqlite,
                SqlTranslationMode::StrictSqlite,
                "SELECT ?1 AS duplicate, ?2 AS duplicate, ?3 AS nullable, ?4 AS happened_at",
            ),
        )
        .await
        .unwrap();
    let description = database
        .describe(&session, DescribeTarget::Statement(statement))
        .await
        .unwrap();
    assert_eq!(
        description
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["duplicate", "duplicate", "nullable", "happened_at"]
    );
    assert_eq!(description.parameter_types(), &[DataType::Unknown; 4]);

    let decimal_error = database
        .bind(
            &session,
            statement,
            vec![
                Value::Binary(vec![0, 0xff]),
                Value::decimal("12.3400").unwrap(),
                Value::Null,
                Value::Text("2026-08-14T00:00:00Z".to_owned()),
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(decimal_error.kind(), briskdb::EngineErrorKind::Unsupported);

    let portal = database
        .bind(
            &session,
            statement,
            vec![
                Value::Binary(vec![0, 0xff]),
                Value::Text("12.3400".into()),
                Value::Null,
                Value::Text("2026-08-14T00:00:00Z".to_owned()),
            ],
        )
        .await
        .unwrap();
    let executed = database
        .execute_bound_logical(&session, portal)
        .await
        .unwrap();
    assert_eq!(executed.shards.len(), 1);
    let PreparedExecution::Rows(rows) = executed.value else {
        panic!("read portal must return rows");
    };
    assert_eq!(rows.columns()[0].name, "duplicate");
    assert_eq!(rows.columns()[1].name, "duplicate");
    assert_eq!(rows.rows()[0].get(0), Some(&Value::Binary(vec![0, 0xff])));
    assert_eq!(rows.rows()[0].get(1), Some(&Value::Text("12.3400".into())));
    assert_eq!(rows.rows()[0].get(2), Some(&Value::Null));
    assert_eq!(
        rows.rows()[0].get(3),
        Some(&Value::Text("2026-08-14T00:00:00Z".into()))
    );

    assert!(database.close_bound(&session, portal).await.unwrap());
    assert!(database.close_prepared(&session, statement).await.unwrap());
    session.close().await.unwrap();
    database.close().await.unwrap();
}

#[tokio::test]
async fn embedded_and_http_calls_observe_the_same_engine_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let database = BriskDb::builder(temp.path())
        .with_shard_count(2)
        .open()
        .await
        .unwrap();
    let session = database.session();
    session.set_routing_key("shared-outcome").await.unwrap();
    database
        .migrate(
            &session,
            "CREATE TABLE records (id TEXT PRIMARY KEY, payload BLOB, note TEXT)",
        )
        .await
        .unwrap();
    let written = database
        .execute_write(
            &session,
            Statement::new(
                "INSERT INTO records (id, payload, note) VALUES (?1, ?2, ?3)",
                vec![
                    Value::from("shared-outcome"),
                    Value::Binary(vec![1, 2, 255]),
                    Value::Null,
                ],
            ),
        )
        .await
        .unwrap();
    assert_eq!(written.value.rows_affected, 1);

    let embedded = database
        .query_logical(
            &session,
            Statement::new(
                "SELECT id, payload, note FROM records WHERE id = ?1",
                vec![Value::from("shared-outcome")],
            ),
        )
        .await
        .unwrap();
    let application = briskdb::api::router_with_engine(database.engine().clone());
    let http = request_json(
        application,
        "/v1/query",
        json!({
            "shard_key": "shared-outcome",
            "sql": "SELECT id, payload, note FROM records WHERE id = ?1",
            "params": ["shared-outcome"]
        }),
    )
    .await;

    assert_eq!(http.0, StatusCode::OK);
    assert_eq!(http.1["shard"], json!(embedded.shards[0]));
    assert_eq!(
        http.1["rows"],
        json!([["shared-outcome", [1, 2, 255], null]])
    );
    assert_eq!(
        embedded.value.rows()[0].values(),
        &[
            Value::Text("shared-outcome".into()),
            Value::Binary(vec![1, 2, 255]),
            Value::Null,
        ]
    );

    session.close().await.unwrap();
    database.close().await.unwrap();
}
