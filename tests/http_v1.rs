#![cfg(feature = "http")]

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode},
};
use briskdb::{
    Statement,
    core::{Database, Engine},
};
use serde_json::{Value, json};
use tower::ServiceExt;

fn application() -> (tempfile::TempDir, Engine, Router) {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    database
        .broadcast("CREATE TABLE notes (id INTEGER PRIMARY KEY)")
        .unwrap();
    let engine = Engine::from_database(database);
    let router = briskdb::api::router_with_engine(engine.clone());
    (temp, engine, router)
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
