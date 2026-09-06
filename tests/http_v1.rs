#![cfg(feature = "http")]

use std::sync::Arc;

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

fn scatter_application() -> (tempfile::TempDir, Router, [String; 2]) {
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
    let router = briskdb::api::router(Arc::new(database));
    (temp, router, tenant_keys)
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
    let (_temp, app, tenant_keys) = scatter_application();
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
    for uri in ["/v1/query", "/v1/execute", "/v1/admin/broadcast"] {
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
