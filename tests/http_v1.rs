#![cfg(feature = "http")]

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode},
};
use briskdb::{
    Statement,
    core::{
        Database, Engine, EngineOptions, ResultLimits, ShardKeyMetadata, ShardKeyType,
        TableDeclaration, Value as CoreValue,
    },
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn application() -> (tempfile::TempDir, Engine, Router) {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    database
        .broadcast("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
        .unwrap();
    database
        .broadcast(
            "CREATE TABLE binary_notes (
                id INTEGER PRIMARY KEY,
                payload BLOB NOT NULL
             )",
        )
        .unwrap();
    let engine = Engine::from_database(database);
    let router = briskdb::api::router_with_engine(engine.clone());
    (temp, engine, router)
}

fn scatter_application() -> (tempfile::TempDir, Engine, Router, [String; 2]) {
    let temp = tempfile::tempdir().unwrap();
    let mut database = Database::open(temp.path(), 2).unwrap();
    database
        .broadcast(
            "CREATE TABLE events (
                tenant_key TEXT NOT NULL PRIMARY KEY,
                ordinal INTEGER NOT NULL,
                payload BLOB NOT NULL
             )",
        )
        .unwrap();
    let logical_database = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical_database,
                "events",
                ShardKeyMetadata::new("tenant_key", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let tenant_keys = [0_u16, 1_u16].map(|expected_shard| {
        (0_u64..)
            .map(|candidate| format!("lossless-{expected_shard}-{candidate}"))
            .find(|candidate| database.shard_for_key(candidate.as_bytes()) == expected_shard)
            .unwrap()
    });
    for (shard, tenant_key, payload) in [
        (0_u16, tenant_keys[0].as_str(), vec![0_u8]),
        (1_u16, tenant_keys[1].as_str(), vec![0xff_u8]),
    ] {
        let inserted = database
            .execute_routed(
                tenant_key,
                "INSERT INTO events (tenant_key, ordinal, payload) VALUES (?1, ?2, ?3)",
                &[
                    CoreValue::from(tenant_key),
                    CoreValue::from(i64::from(shard)),
                    CoreValue::from(payload),
                ],
            )
            .unwrap();
        assert_eq!(inserted.shard, shard);
        assert_eq!(inserted.value, 1);
    }
    let engine = Engine::from_database(Arc::new(database));
    let router = briskdb::api::router_with_engine(engine.clone());
    (temp, engine, router, tenant_keys)
}

async fn request(
    app: &Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = app
        .clone()
        .oneshot(request.body(body.into()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    (
        parts.status,
        parts.headers,
        to_bytes(body, 4 * 1024 * 1024).await.unwrap().to_vec(),
    )
}

fn assert_problem(
    response: (StatusCode, HeaderMap, Vec<u8>),
    expected_status: StatusCode,
    expected_code: &str,
) -> Value {
    let (status, headers, body) = response;
    assert_eq!(status, expected_status);
    assert_eq!(headers["briskdb-api-version"], "1");
    assert_eq!(headers["content-type"], "application/problem+json");
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], status.as_u16());
    assert_eq!(body["code"], expected_code);
    assert_eq!(body.as_object().unwrap().len(), 5);
    assert!(body["type"].as_str().unwrap().contains(':'));
    assert!(!body.to_string().contains("private-sentinel"));
    body
}

fn tagged(tag: &str, value: &str) -> Value {
    json!({"$briskdb_type": tag, "value": value})
}

async fn wait_for_active_query_count(app: &Router, expected: usize) -> Vec<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (status, headers, body) =
                request(app, Method::GET, "/v1/admin/queries", None, Body::empty()).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(headers["briskdb-api-version"], "1");
            let body = serde_json::from_slice::<Value>(&body).unwrap();
            let queries = body["queries"].as_array().unwrap();
            if queries.len() == expected {
                return queries.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active-query registry should reach the expected size")
}

#[tokio::test]
async fn discovery_health_alias_and_head_identify_the_contract() {
    let (_temp, _engine, app) = application();
    for uri in ["/v1", "/v1/"] {
        let (status, headers, body) = request(&app, Method::GET, uri, None, Body::empty()).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(headers["briskdb-api-version"], "1");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "api_version": "1",
                "value_encoding": "legacy-json-v1",
                "supported_value_encodings": ["legacy-json-v1", "lossless-json-v1"],
                "session_scope": "request",
                "sql_dialect": "sqlite",
                "max_request_bytes": 2097152
            })
        );
        let (status, headers, body) = request(&app, Method::HEAD, uri, None, Body::empty()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["briskdb-api-version"], "1");
        assert!(body.is_empty());
    }
    let versioned = request(&app, Method::GET, "/v1/health", None, Body::empty()).await;
    let legacy = request(&app, Method::GET, "/health", None, Body::empty()).await;
    assert_eq!(versioned.0, StatusCode::OK);
    assert_eq!(versioned.1["briskdb-api-version"], "1");
    assert_eq!(versioned.2, legacy.2);
    assert!(!legacy.1.contains_key("briskdb-api-version"));
}

#[tokio::test]
async fn readiness_has_one_exact_versioned_and_unversioned_probe_shape() {
    let (_temp, engine, app) = application();
    let expected = json!({
        "status": "ready",
        "ready": true,
        "reasons": [],
        "engine_state": "running",
        "schema_state": "ready",
        "schema_generation": engine.catalog().schema_generation().to_string(),
        "active_schema_operations": 0
    });

    let versioned = request(&app, Method::GET, "/v1/ready", None, Body::empty()).await;
    let unversioned = request(&app, Method::GET, "/ready", None, Body::empty()).await;
    assert_eq!(versioned.0, StatusCode::OK);
    assert_eq!(versioned.1["briskdb-api-version"], "1");
    assert_eq!(versioned.1["content-type"], "application/json");
    assert_eq!(
        serde_json::from_slice::<Value>(&versioned.2).unwrap(),
        expected
    );
    assert_eq!(unversioned.0, StatusCode::OK);
    assert!(!unversioned.1.contains_key("briskdb-api-version"));
    assert_eq!(unversioned.2, versioned.2);

    let head = request(&app, Method::HEAD, "/v1/ready", None, Body::empty()).await;
    assert_eq!(head.0, StatusCode::OK);
    assert_eq!(head.1["briskdb-api-version"], "1");
    assert!(head.2.is_empty());

    assert_eq!(
        engine.begin_shutdown(),
        briskdb::core::EngineState::Draining
    );
    let unavailable = request(&app, Method::GET, "/v1/ready", None, Body::empty()).await;
    assert_eq!(unavailable.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable.1["briskdb-api-version"], "1");
    assert_eq!(unavailable.1["content-type"], "application/json");
    assert_eq!(
        serde_json::from_slice::<Value>(&unavailable.2).unwrap(),
        json!({
            "status": "not_ready",
            "ready": false,
            "reasons": ["engine_draining"],
            "engine_state": "draining",
            "schema_state": "ready",
            "schema_generation": engine.catalog().schema_generation().to_string(),
            "active_schema_operations": 0
        })
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn relational_catalog_preserves_ids_order_and_placement_without_physical_details() {
    let (_temp, engine, app, _tenant_keys) = scatter_application();
    let (status, headers, body) =
        request(&app, Method::GET, "/v1/admin/catalog", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({
            "identifier_encoding_version": 1,
            "schema_generation": engine.catalog().schema_generation().to_string(),
            "default_database_id": "1",
            "databases": [{"id":"1", "name":"default"}],
            "tables": [{
                "id": "1",
                "database_id": "1",
                "name": "events",
                "placement": {
                    "kind": "sharded",
                    "shard_key": {"column":"tenant_key", "data_type":"text"}
                },
                "generated_id": {"policy":"none"}
            }],
            "global_indexes": []
        })
    );
    let rendered = String::from_utf8(body).unwrap();
    for forbidden in [
        "manifest.sqlite",
        "shard-",
        "CREATE TABLE",
        "private-sentinel",
    ] {
        assert!(!rendered.contains(forbidden));
    }
}

#[tokio::test]
async fn operational_reports_are_bounded_redacted_and_checkpoint_input_is_strict() {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    let engine = Engine::from_database(database);
    let app = briskdb::api::router_with_engine(engine.clone());

    let (status, _, body) = request(
        &app,
        Method::GET,
        "/v1/admin/migrations",
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"schema_generation":"0", "active":null, "latest_complete":null})
    );

    let migration_sql = "CREATE TABLE private_sentinel_items (id INTEGER PRIMARY KEY)";
    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/admin/broadcast",
        Some("application/json"),
        serde_json::to_vec(&json!({"sql":migration_sql})).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"completed_shards":[0, 1]})
    );

    let (status, headers, body) = request(
        &app,
        Method::GET,
        "/v1/admin/migrations",
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    let summary = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(summary["schema_generation"], "1");
    assert!(summary["active"].is_null());
    let completed = summary["latest_complete"].clone();
    assert_eq!(completed["generation"], "1");
    assert_eq!(completed["source_generation"], "0");
    assert_eq!(completed["target_generation"], "1");
    assert_eq!(completed["state"], "complete");
    assert_eq!(completed["shard_count"], 2);
    assert_eq!(completed["next_shard"], 2);
    assert_eq!(completed["completed_shards"], 2);
    assert_eq!(completed["sql_bytes"], migration_sql.len());
    assert_eq!(completed.as_object().unwrap().len(), 8);
    assert!(completed.get("id").is_none());
    assert!(completed.get("digest").is_none());
    assert!(!String::from_utf8(body).unwrap().contains(migration_sql));

    let (status, _, body) = request(
        &app,
        Method::GET,
        "/v1/admin/migrations/1",
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap(), completed);
    for generation in [
        "0",
        "01",
        "-1",
        "2",
        "18446744073709551616",
        "private-sentinel",
    ] {
        assert_problem(
            request(
                &app,
                Method::GET,
                &format!("/v1/admin/migrations/{generation}"),
                None,
                Body::empty(),
            )
            .await,
            StatusCode::NOT_FOUND,
            "not_found",
        );
    }

    let (status, _, body) =
        request(&app, Method::GET, "/v1/admin/shards", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({
            "schema_generation": "1",
            "shards": [{"id":0,"state":"ready"},{"id":1,"state":"ready"}]
        })
    );

    let (status, _, body) =
        request(&app, Method::GET, "/v1/admin/backup", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({
            "mode": "stopped_directory_copy",
            "online": false,
            "requires_all_processes_stopped": true,
            "checkpoint_endpoint": "/v1/admin/maintenance/checkpoint",
            "checkpoint_role": "preparation_only",
            "checkpoint_is_recovery_point": false
        })
    );

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/admin/maintenance/checkpoint",
        Some("application/json"),
        "{}",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    let checkpoint = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(checkpoint["operation"], "passive_checkpoint");
    assert_eq!(checkpoint["busy"], false);
    assert_eq!(checkpoint["complete"], true);
    assert_eq!(checkpoint["recovery_point"], false);
    assert_eq!(checkpoint["shards"].as_array().unwrap().len(), 2);
    assert_eq!(checkpoint["shards"][0]["shard"], 0);
    assert_eq!(checkpoint["shards"][1]["shard"], 1);
    assert_eq!(checkpoint["databases"].as_array().unwrap().len(), 1);
    assert_eq!(checkpoint["databases"][0]["database"], "manifest");
    for row in checkpoint["shards"]
        .as_array()
        .unwrap()
        .iter()
        .chain(checkpoint["databases"].as_array().unwrap())
    {
        assert_eq!(row.as_object().unwrap().len(), 6);
        assert!(row["busy"].is_boolean());
        assert!(row["counts_available"].is_boolean());
        assert!(row["wal_frames"].is_u64());
        assert!(row["checkpointed_frames"].is_u64());
        assert!(row["complete"].is_boolean());
    }

    for uri in [
        "/v1/admin/catalog",
        "/v1/admin/migrations",
        "/v1/admin/migrations/1",
        "/v1/admin/shards",
        "/v1/admin/queries",
        "/v1/admin/backup",
    ] {
        let (status, headers, body) = request(&app, Method::HEAD, uri, None, Body::empty()).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(headers["briskdb-api-version"], "1", "{uri}");
        assert!(body.is_empty(), "{uri}");
    }

    for body in ["", "[]", "null", r#"{"private-sentinel":true}"#] {
        assert_problem(
            request(
                &app,
                Method::POST,
                "/v1/admin/maintenance/checkpoint",
                Some("application/json"),
                body,
            )
            .await,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        );
    }
    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/admin/maintenance/checkpoint",
            None,
            "{}",
        )
        .await,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    );
}

#[tokio::test]
async fn active_query_handles_cancel_exact_work_and_disappear_after_cleanup() {
    let (_temp, _engine, app) = application();
    let sql = "WITH RECURSIVE numbers(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000) SELECT sum(value) FROM numbers";
    let query_app = app.clone();
    let query_body = serde_json::to_vec(&json!({"shard_key":"cancel-owner", "sql":sql})).unwrap();
    let query = tokio::spawn(async move {
        request(
            &query_app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            query_body,
        )
        .await
    });

    let active = wait_for_active_query_count(&app, 1).await;
    let query_status = active[0].as_object().unwrap();
    assert_eq!(query_status.len(), 4);
    let operation_id = query_status["operation_id"].as_str().unwrap().to_owned();
    assert_eq!(operation_id.len(), 32);
    assert!(
        operation_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert!(query_status["elapsed_ms"].is_u64());
    assert_eq!(query_status["sql_bytes"], sql.len());
    assert_eq!(query_status["cancellation_requested"], false);
    let rendered = serde_json::to_string(&active).unwrap();
    assert!(!rendered.contains("WITH RECURSIVE"));
    assert!(!rendered.contains("sql_digest"));

    assert_problem(
        request(
            &app,
            Method::POST,
            &format!("/v1/admin/queries/{operation_id}/cancel"),
            None,
            "private-sentinel",
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
    );
    let still_active = wait_for_active_query_count(&app, 1).await;
    assert_eq!(still_active[0]["operation_id"], operation_id);
    assert_eq!(still_active[0]["cancellation_requested"], false);

    let oversized_cancel_body = "private-sentinel".repeat(150_000);
    assert_problem(
        request(
            &app,
            Method::POST,
            &format!("/v1/admin/queries/{operation_id}/cancel"),
            None,
            oversized_cancel_body,
        )
        .await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_too_large",
    );
    let still_active = wait_for_active_query_count(&app, 1).await;
    assert_eq!(still_active[0]["operation_id"], operation_id);
    assert_eq!(still_active[0]["cancellation_requested"], false);

    let cancellation = request(
        &app,
        Method::POST,
        &format!("/v1/admin/queries/{operation_id}/cancel"),
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(cancellation.0, StatusCode::ACCEPTED);
    assert_eq!(cancellation.1["briskdb-api-version"], "1");
    assert_eq!(cancellation.1["content-type"], "application/json");
    assert_eq!(
        serde_json::from_slice::<Value>(&cancellation.2).unwrap(),
        json!({"operation_id":operation_id, "newly_requested":true})
    );

    let query_response = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .expect("the cancelled query should stop promptly")
        .unwrap();
    assert_problem(
        query_response,
        StatusCode::INTERNAL_SERVER_ERROR,
        "cancelled",
    );
    assert!(wait_for_active_query_count(&app, 0).await.is_empty());

    for stale_or_invalid in [
        operation_id.as_str(),
        "00000000000000000000000000000000",
        "0123456789ABCDEF0123456789ABCDEF",
        "private-sentinel",
    ] {
        assert_problem(
            request(
                &app,
                Method::POST,
                &format!("/v1/admin/queries/{stale_or_invalid}/cancel"),
                None,
                Body::empty(),
            )
            .await,
            StatusCode::NOT_FOUND,
            "not_found",
        );
    }

    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        r#"{"shard_key":"cancel-owner","sql":"SELECT 1 AS value"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows"],
        json!([[1]])
    );
}

#[tokio::test]
async fn omitted_and_explicit_legacy_encoding_have_the_exact_same_response() {
    let (_temp, _engine, app) = application();
    let omitted = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        r#"{"shard_key":"legacy-owner","sql":"SELECT ?1 AS object_text, X'00ff' AS data, 9007199254740993 AS large_integer","params":[{"z":0,"a":1}]}"#,
    )
    .await;
    let explicit = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        r#"{"shard_key":"legacy-owner","sql":"SELECT ?1 AS object_text, X'00ff' AS data, 9007199254740993 AS large_integer","params":[{"z":0,"a":1}],"value_encoding":"legacy-json-v1"}"#,
    )
    .await;

    assert_eq!(omitted.0, StatusCode::OK);
    assert_eq!(explicit.0, StatusCode::OK);
    assert_eq!(omitted.1["content-type"], "application/json");
    assert_eq!(omitted.1["briskdb-api-version"], "1");
    assert_eq!(omitted.2, explicit.2);
    let mut body = serde_json::from_slice::<Value>(&omitted.2).unwrap();
    assert!(body["shard"].as_u64().is_some());
    assert!(body.get("value_encoding").is_none());
    body.as_object_mut().unwrap().remove("shard");
    assert_eq!(
        body,
        json!({
            "columns": [
                {"name": "object_text", "data_type": "unknown"},
                {"name": "data", "data_type": "unknown"},
                {"name": "large_integer", "data_type": "unknown"}
            ],
            "rows": [["{\"a\":1,\"z\":0}", [0, 255], 9007199254740993_i64]]
        })
    );

    let omitted_execute = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        r#"{"shard_key":"legacy-owner","sql":"INSERT INTO notes VALUES (?1)","params":[100]}"#,
    )
    .await;
    let explicit_execute = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        r#"{"shard_key":"legacy-owner","sql":"INSERT INTO notes VALUES (?1)","params":[101],"value_encoding":"legacy-json-v1"}"#,
    )
    .await;
    assert_eq!(omitted_execute.0, StatusCode::OK);
    assert_eq!(explicit_execute.0, StatusCode::OK);
    assert_eq!(omitted_execute.2, explicit_execute.2);
}

#[tokio::test]
async fn lossless_binary_can_be_written_queried_and_reused_without_becoming_text() {
    let (_temp, _engine, app) = application();
    let insert = json!({
        "shard_key": "binary-owner",
        "sql": "INSERT INTO binary_notes (id, payload) VALUES (?1, ?2)",
        "params": [tagged("int64", "1"), tagged("binary", "AAEC/w==")],
        "value_encoding": "lossless-json-v1"
    });
    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        serde_json::to_vec(&insert).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(headers["briskdb-api-version"], "1");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows_affected"],
        1
    );

    let query = json!({
        "shard_key": "binary-owner",
        "sql": "SELECT id, payload, typeof(payload) AS storage_class, hex(payload) AS payload_hex FROM binary_notes WHERE id = ?1",
        "params": [tagged("int64", "1")],
        "value_encoding": "lossless-json-v1"
    });
    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&query).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(first["value_encoding"], "lossless-json-v1");
    assert_eq!(
        first["rows"],
        json!([[
            tagged("int64", "1"),
            tagged("binary", "AAEC/w=="),
            "blob",
            "000102FF"
        ]])
    );

    let returned_binary = first["rows"][0][1].clone();
    let reuse = json!({
        "shard_key": "binary-owner",
        "sql": "INSERT INTO binary_notes (id, payload) VALUES (?1, ?2)",
        "params": [tagged("int64", "2"), returned_binary],
        "value_encoding": "lossless-json-v1"
    });
    let (status, _, _) = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        serde_json::to_vec(&reuse).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let verify = json!({
        "shard_key": "binary-owner",
        "sql": "SELECT typeof(payload), hex(payload) FROM binary_notes WHERE id = ?1",
        "params": [tagged("int64", "2")],
        "value_encoding": "lossless-json-v1"
    });
    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&verify).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows"],
        json!([["blob", "000102FF"]])
    );
}

#[tokio::test]
async fn invalid_lossless_tags_and_encoding_selection_fail_before_mutation() {
    let (_temp, engine, app) = application();
    let invalid_bodies = [
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"1"},{"$briskdb_type":"private-sentinel","value":"AA=="}],"value_encoding":"lossless-json-v1"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"2"},{"$briskdb_type":"binary","value":"private-sentinel"}],"value_encoding":"lossless-json-v1"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"3"},{"$briskdb_type":"binary","value":"AA==","private-sentinel":true}],"value_encoding":"lossless-json-v1"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"4"},{"$briskdb_type":"binary","value":"AA==","value":"private-sentinel"}],"value_encoding":"lossless-json-v1"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"5"},{"$briskdb_type":"binary"}],"value_encoding":"lossless-json-v1"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"6"},"private-sentinel"],"value_encoding":"future-json-v9"}"#,
        r#"{"shard_key":"invalid-owner","sql":"INSERT INTO binary_notes VALUES (?1,?2)","params":[{"$briskdb_type":"int64","value":"7"},"private-sentinel"],"value_encoding":"legacy-json-v1","value_encoding":"lossless-json-v1"}"#,
    ];

    for body in invalid_bodies {
        assert_problem(
            request(
                &app,
                Method::POST,
                "/v1/execute",
                Some("application/json"),
                body,
            )
            .await,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        );
    }

    let session = engine.session();
    session.set_routing_key("invalid-owner").await.unwrap();
    let rows = engine
        .query(
            &session,
            Statement::new("SELECT COUNT(*) FROM binary_notes", vec![]),
        )
        .await
        .unwrap();
    assert_eq!(rows.value.rows()[0].get(0), Some(&CoreValue::from(0_i64)));
}

#[tokio::test]
async fn lossless_scatter_keeps_shard_row_and_duplicate_column_order() {
    let (_temp, _engine, app, tenant_keys) = scatter_application();
    let query = json!({
        "sql": "SELECT tenant_key AS duplicate, ordinal AS duplicate, payload AS \"\" FROM events",
        "value_encoding": "lossless-json-v1"
    });
    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&query).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({
            "shards": [0, 1],
            "value_encoding": "lossless-json-v1",
            "columns": [
                {"name": "duplicate", "data_type": "text"},
                {"name": "duplicate", "data_type": "int64"},
                {"name": "", "data_type": "binary"}
            ],
            "rows": [
                [tenant_keys[0], tagged("int64", "0"), tagged("binary", "AA==")],
                [tenant_keys[1], tagged("int64", "1"), tagged("binary", "/w==")]
            ]
        })
    );

    let empty = json!({
        "sql": "SELECT tenant_key AS duplicate, ordinal AS duplicate, payload AS \"\" FROM events WHERE ordinal = ?1",
        "params": [tagged("int64", "99")],
        "value_encoding": "lossless-json-v1"
    });
    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&empty).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({
            "shards": [0, 1],
            "value_encoding": "lossless-json-v1",
            "columns": [
                {"name": "duplicate", "data_type": "text"},
                {"name": "duplicate", "data_type": "int64"},
                {"name": "", "data_type": "binary"}
            ],
            "rows": []
        })
    );
}

#[tokio::test]
async fn invalid_envelopes_fail_before_sql_or_schema_mutation() {
    let (_temp, engine, app) = application();
    for body in [
        r#"{"sql": "INSERT INTO notes VALUES (1)", "shard_key":"owner", "private-sentinel": true}"#,
        r#"{"sql": "INSERT INTO notes VALUES (1)", "sql": "private-sentinel", "shard_key":"owner"}"#,
        r#"{"sql": "INSERT INTO notes VALUES (1)", "shard_key":"owner", "params": null}"#,
        r#"{"sql": "INSERT INTO notes VALUES (1)", "shard_key":"owner"} {}"#,
        r#"{"sql": "private-sentinel""#,
        r#"{"params": []}"#,
        r#"{"sql": 1}"#,
        r#"{"sql": "SELECT 1", "shard_key": 1}"#,
        r#"[]"#,
        r#"null"#,
        "",
    ] {
        for uri in ["/v1/execute", "/v1/query"] {
            assert_problem(
                request(&app, Method::POST, uri, Some("application/json"), body).await,
                StatusCode::BAD_REQUEST,
                "invalid_argument",
            );
        }
    }
    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/admin/broadcast",
            Some("application/json"),
            r#"{"sql":"DROP TABLE notes", "params":["private-sentinel"]}"#,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
    );

    let session = engine.session();
    session.set_routing_key("owner").await.unwrap();
    let rows = engine
        .query(&session, Statement::new("SELECT id FROM notes", vec![]))
        .await
        .unwrap();
    assert!(rows.value.rows().is_empty());
}

#[tokio::test]
async fn transport_rejections_have_redacted_versioned_problems_and_allow_headers() {
    let (_temp, _engine, app) = application();
    for media_type in [None, Some("text/plain")] {
        assert_problem(
            request(
                &app,
                Method::POST,
                "/v1/query",
                media_type,
                r#"{"sql":"private-sentinel"}"#,
            )
            .await,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        );
    }
    for (uri, method, allow) in [
        ("/v1/query", Method::GET, "POST"),
        ("/v1/admin/broadcast", Method::PUT, "POST"),
        ("/v1/admin/global-indexes", Method::POST, "GET,HEAD"),
        ("/v1/ready", Method::POST, "GET,HEAD"),
        ("/v1/admin/catalog", Method::POST, "GET,HEAD"),
        ("/v1/admin/migrations", Method::POST, "GET,HEAD"),
        ("/v1/admin/migrations/1", Method::POST, "GET,HEAD"),
        ("/v1/admin/shards", Method::POST, "GET,HEAD"),
        ("/v1/admin/queries", Method::POST, "GET,HEAD"),
        (
            "/v1/admin/queries/0123456789abcdef0123456789abcdef/cancel",
            Method::GET,
            "POST",
        ),
        ("/v1/admin/backup", Method::POST, "GET,HEAD"),
        ("/v1/admin/maintenance/checkpoint", Method::GET, "POST"),
        ("/v1", Method::POST, "GET,HEAD"),
    ] {
        let response = request(&app, method, uri, None, Body::empty()).await;
        assert_eq!(response.1["allow"], allow);
        assert_problem(
            response,
            StatusCode::METHOD_NOT_ALLOWED,
            "method_not_allowed",
        );
    }
    assert_problem(
        request(
            &app,
            Method::GET,
            "/v1/private-sentinel",
            None,
            Body::empty(),
        )
        .await,
        StatusCode::NOT_FOUND,
        "not_found",
    );
    let response = request(&app, Method::GET, "/v2/query", None, Body::empty()).await;
    assert_eq!(response.0, StatusCode::NOT_FOUND);
    assert!(!response.1.contains_key("briskdb-api-version"));
}

#[tokio::test]
async fn oversized_json_is_rejected_and_later_requests_still_work() {
    let (_temp, _engine, app) = application();
    let oversized = json!({"sql": "private-sentinel".repeat(150_000)}).to_string();
    for uri in [
        "/v1/query",
        "/v1/execute",
        "/v1/admin/broadcast",
        "/v1/admin/maintenance/checkpoint",
    ] {
        assert_problem(
            request(
                &app,
                Method::POST,
                uri,
                Some("application/json"),
                oversized.clone(),
            )
            .await,
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
        );
    }
    let (status, headers, body) = request(&app, Method::POST, "/v1/query",
        Some("application/json; charset=utf-8"),
        r#"{"shard_key":"owner", "sql":"SELECT ?1 AS duplicate, ?2 AS duplicate", "params":["hello",null]}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["rows"], json!([["hello", null]]));
    assert_eq!(body["columns"][0]["name"], "duplicate");
    assert_eq!(body["columns"][1]["name"], "duplicate");
    assert!(body.get("shard").is_some());
    assert!(body.get("shards").is_none());
}

#[tokio::test]
async fn concurrent_requests_keep_value_encoding_request_local() {
    let (_temp, _engine, app) = application();
    let mut requests = tokio::task::JoinSet::new();
    for value in 0_i64..24 {
        let app = app.clone();
        requests.spawn(async move {
            let lossless = value % 2 == 1;
            let body = if lossless {
                json!({
                    "shard_key": format!("concurrent-{value}"),
                    "sql": "SELECT ?1 AS value",
                    "params": [tagged("int64", &value.to_string())],
                    "value_encoding": "lossless-json-v1"
                })
            } else {
                json!({
                    "shard_key": format!("concurrent-{value}"),
                    "sql": "SELECT ?1 AS value",
                    "params": [value],
                    "value_encoding": "legacy-json-v1"
                })
            };
            let response = request(
                &app,
                Method::POST,
                "/v1/query",
                Some("application/json"),
                serde_json::to_vec(&body).unwrap(),
            )
            .await;
            (value, lossless, response)
        });
    }

    let mut completed = 0;
    while let Some(joined) = requests.join_next().await {
        let (value, lossless, (status, headers, body)) = joined.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["briskdb-api-version"], "1");
        let body: Value = serde_json::from_slice(&body).unwrap();
        if lossless {
            assert_eq!(body["value_encoding"], "lossless-json-v1");
            assert_eq!(body["rows"], json!([[tagged("int64", &value.to_string())]]));
        } else {
            assert!(body.get("value_encoding").is_none());
            assert_eq!(body["rows"], json!([[value]]));
        }
        completed += 1;
    }
    assert_eq!(completed, 24);
}

#[tokio::test]
async fn lossless_query_result_limit_returns_only_the_problem_document() {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    let options = EngineOptions::default().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
    let app = briskdb::api::router_with_engine(
        Engine::from_database_with_options(database, options).unwrap(),
    );
    let body = json!({
        "shard_key": "limited-lossless",
        "sql": "SELECT 1 AS value UNION ALL SELECT 2",
        "value_encoding": "lossless-json-v1"
    });
    let problem = assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            serde_json::to_vec(&body).unwrap(),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "limit_exceeded",
    );
    assert!(problem.get("value_encoding").is_none());
    assert!(problem.get("columns").is_none());
    assert!(problem.get("rows").is_none());
}

#[tokio::test]
async fn engine_errors_and_request_routing_remain_session_local() {
    let (_temp, engine, app) = application();
    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        r#"{"shard_key":"owner", "sql":"INSERT INTO notes VALUES (?1)", "params":[7]}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["briskdb-api-version"], "1");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows_affected"],
        1
    );

    let session = engine.session();
    let core = engine
        .query_logical(&session, Statement::new("SELECT id FROM notes", vec![]))
        .await
        .unwrap_err();
    let problem = assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            r#"{"sql":"SELECT id FROM notes"}"#,
        )
        .await,
        StatusCode::from_u16(briskdb::protocol::error::http_error(core.kind()).status).unwrap(),
        core.code(),
    );
    assert_eq!(problem["code"], core.code());

    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/execute",
            Some("application/json"),
            r#"{"shard_key":"owner", "sql":"INSERT INTO notes VALUES (7)"}"#,
        )
        .await,
        StatusCode::CONFLICT,
        "unique_violation",
    );
    let (status, _, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        r#"{"shard_key":"owner", "sql":"SELECT id FROM notes"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows"],
        json!([[7]])
    );
}
