#![cfg(feature = "http")]

use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode},
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
    request_with_headers(app, method, uri, content_type, body, &[]).await
}

async fn request_with_headers(
    app: &Router,
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let mut request = request.body(body.into()).unwrap();
    for (name, value) in headers {
        request.headers_mut().append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    let response = app.clone().oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    request_id(&parts.headers);
    (
        parts.status,
        parts.headers,
        to_bytes(body, 4 * 1024 * 1024).await.unwrap().to_vec(),
    )
}

async fn idempotent_execute(
    app: &Router,
    key: &str,
    body: &Value,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    request_with_headers(
        app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        serde_json::to_vec(body).unwrap(),
        &[("BriskDB-Idempotency-Key", key)],
    )
    .await
}

fn request_id(headers: &HeaderMap) -> &str {
    let values = headers
        .get_all("briskdb-request-id")
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 1);
    let value = values[0].to_str().unwrap();
    assert_eq!(value.len(), 32);
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_ne!(value, "00000000000000000000000000000000");
    value
}

fn assert_problem(
    response: (StatusCode, HeaderMap, Vec<u8>),
    expected_status: StatusCode,
    expected_code: &str,
) -> Value {
    let (status, headers, body) = response;
    assert_eq!(status, expected_status);
    assert_eq!(headers["briskdb-api-version"], "1");
    request_id(&headers);
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

fn ndjson_records(body: &[u8]) -> Vec<Value> {
    assert_eq!(body.last(), Some(&b'\n'));
    body.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
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
        let representation_length = body.len().to_string();
        assert_eq!(headers["content-length"], representation_length);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({
                "api_version": "1",
                "value_encoding": "legacy-json-v1",
                "supported_value_encodings": ["legacy-json-v1", "lossless-json-v1"],
                "session_scope": "request",
                "sql_dialect": "sqlite",
                "max_request_bytes": 2097152,
                "max_result_rows": 10000,
                "max_result_logical_bytes": 16777216,
                "stream_buffer_rows": 16,
                "request_id_header": "BriskDB-Request-ID",
                "idempotency_key_header": "BriskDB-Idempotency-Key",
                "stream_media_type": "application/x-ndjson; charset=utf-8"
            })
        );
        request_id(&headers);
        let (status, headers, body) = request(&app, Method::HEAD, uri, None, Body::empty()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["briskdb-api-version"], "1");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["content-length"], representation_length);
        request_id(&headers);
        assert!(body.is_empty());
    }
    let versioned = request(&app, Method::GET, "/v1/health", None, Body::empty()).await;
    let legacy = request(&app, Method::GET, "/health", None, Body::empty()).await;
    assert_eq!(versioned.0, StatusCode::OK);
    assert_eq!(versioned.1["briskdb-api-version"], "1");
    request_id(&versioned.1);
    request_id(&legacy.1);
    assert_eq!(versioned.2, legacy.2);
    assert!(!legacy.1.contains_key("briskdb-api-version"));
}

#[tokio::test]
async fn request_and_idempotency_headers_are_strict_distinct_and_outermost() {
    let (_temp, engine, app) = application();
    let supplied = "0123456789abcdef0123456789abcdef";
    for uri in [
        "/v1",
        "/health",
        "/admin",
        "/admin/assets/app.js",
        "/private-sentinel",
    ] {
        let response = request_with_headers(
            &app,
            Method::GET,
            uri,
            None,
            Body::empty(),
            &[("BriskDB-Request-ID", supplied)],
        )
        .await;
        assert_eq!(request_id(&response.1), supplied);
    }

    let mut generated = HashSet::new();
    for _ in 0..64 {
        let response = request(&app, Method::GET, "/v1", None, Body::empty()).await;
        assert!(generated.insert(request_id(&response.1).to_owned()));
    }

    for invalid in [
        "",
        "0123456789abcdef0123456789abcde",
        "0123456789abcdef0123456789abcdef0",
        "0123456789ABCDEF0123456789ABCDEF",
        "g123456789abcdef0123456789abcdef",
        "00000000000000000000000000000000",
        "0123456789abcdef,0123456789abcdef",
        " 0123456789abcdef0123456789abcdef ",
    ] {
        let response = request_with_headers(
            &app,
            Method::GET,
            "/v1",
            None,
            Body::empty(),
            &[("BriskDB-Request-ID", invalid)],
        )
        .await;
        let returned = request_id(&response.1).to_owned();
        assert_ne!(returned, invalid);
        assert_problem(response, StatusCode::BAD_REQUEST, "invalid_argument");
    }

    for name in ["BriskDB-Request-ID", "BriskDB-Idempotency-Key"] {
        let response = request_with_headers(
            &app,
            Method::POST,
            "/v1/execute",
            Some("application/json"),
            r#"{"shard_key":"owner","sql":"INSERT INTO notes VALUES (1)"}"#,
            &[(name, supplied), (name, supplied)],
        )
        .await;
        if name == "BriskDB-Request-ID" {
            assert_ne!(request_id(&response.1), supplied);
        }
        assert_problem(response, StatusCode::BAD_REQUEST, "invalid_argument");
    }

    for (method, uri, content_type, body) in [
        (Method::GET, "/v1/execute", None, ""),
        (Method::PUT, "/v1/execute", None, ""),
        (
            Method::POST,
            "/v1/execute/",
            Some("application/json"),
            r#"{"shard_key":"owner","sql":"INSERT INTO notes VALUES (1)"}"#,
        ),
        (
            Method::POST,
            "/v1/query",
            Some("application/json"),
            r#"{"shard_key":"owner","sql":"SELECT 1"}"#,
        ),
        (Method::GET, "/v1/admin/catalog", None, ""),
    ] {
        assert_problem(
            request_with_headers(
                &app,
                method,
                uri,
                content_type,
                body,
                &[("BriskDB-Idempotency-Key", supplied)],
            )
            .await,
            StatusCode::NOT_IMPLEMENTED,
            "unsupported",
        );
    }

    let unversioned = request_with_headers(
        &app,
        Method::GET,
        "/health",
        None,
        Body::empty(),
        &[("BriskDB-Idempotency-Key", supplied)],
    )
    .await;
    assert_eq!(unversioned.0, StatusCode::NOT_IMPLEMENTED);
    assert!(!unversioned.1.contains_key("briskdb-api-version"));
    request_id(&unversioned.1);
    assert_eq!(unversioned.1["content-type"], "application/problem+json");
    let problem: Value = serde_json::from_slice(&unversioned.2).unwrap();
    assert_eq!(problem["code"], "unsupported");
    assert_eq!(problem.as_object().unwrap().len(), 5);

    for invalid in [
        "",
        "0123456789abcdef0123456789abcde",
        "0123456789abcdef0123456789abcdef0",
        "0123456789ABCDEF0123456789ABCDEF",
        "g123456789abcdef0123456789abcdef",
        "00000000000000000000000000000000",
        "0123456789abcdef,0123456789abcdef",
        " 0123456789abcdef0123456789abcdef ",
    ] {
        let response = request_with_headers(
            &app,
            Method::POST,
            "/v1/execute",
            Some("application/json"),
            r#"{"shard_key":"owner","sql":"INSERT INTO notes VALUES (1)"}"#,
            &[
                ("BriskDB-Request-ID", supplied),
                ("BriskDB-Idempotency-Key", invalid),
            ],
        )
        .await;
        assert_eq!(request_id(&response.1), supplied);
        assert!(!response.1.contains_key("briskdb-idempotency-status"));
        assert_problem(response, StatusCode::BAD_REQUEST, "invalid_argument");
    }

    for uri in [
        "/health",
        "/admin",
        "/admin/assets/app.js",
        "/private-sentinel",
    ] {
        let invalid_unversioned = request_with_headers(
            &app,
            Method::GET,
            uri,
            None,
            Body::empty(),
            &[("BriskDB-Request-ID", "ABC")],
        )
        .await;
        assert_eq!(invalid_unversioned.0, StatusCode::BAD_REQUEST, "{uri}");
        assert!(
            !invalid_unversioned.1.contains_key("briskdb-api-version"),
            "{uri}"
        );
        assert_ne!(request_id(&invalid_unversioned.1), "ABC", "{uri}");
        let body: Value = serde_json::from_slice(&invalid_unversioned.2).unwrap();
        assert_eq!(body["code"], "invalid_argument", "{uri}");
        assert_eq!(body.as_object().unwrap().len(), 5, "{uri}");
    }

    let unsupported_write = request_with_headers(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        r#"{"shard_key":"owner","sql":"INSERT INTO notes VALUES (1)"}"#,
        &[("BriskDB-Idempotency-Key", supplied)],
    )
    .await;
    assert!(
        !unsupported_write
            .1
            .contains_key("briskdb-idempotency-status")
    );
    assert_problem(
        unsupported_write,
        StatusCode::NOT_IMPLEMENTED,
        "unsupported",
    );
    let session = engine.session();
    session.set_routing_key("owner").await.unwrap();
    let count = engine
        .query(
            &session,
            Statement::new("SELECT COUNT(*) FROM notes", vec![]),
        )
        .await
        .unwrap();
    assert_eq!(count.value.rows()[0].get(0), Some(&CoreValue::from(0_i64)));
}

#[tokio::test]
async fn middleware_head_rejections_preserve_headers_and_suppress_problem_bodies() {
    let (_temp, _engine, app) = application();
    let supplied = "0123456789abcdef0123456789abcdef";
    let cases = [
        (
            vec![("BriskDB-Request-ID", "ABC")],
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            vec![
                ("BriskDB-Request-ID", supplied),
                ("BriskDB-Request-ID", supplied),
            ],
            StatusCode::BAD_REQUEST,
            None,
        ),
        (
            vec![
                ("BriskDB-Request-ID", supplied),
                ("BriskDB-Idempotency-Key", supplied),
            ],
            StatusCode::NOT_IMPLEMENTED,
            Some(supplied),
        ),
    ];

    for (headers, expected_status, expected_request_id) in cases {
        let (status, response_headers, body) = request_with_headers(
            &app,
            Method::HEAD,
            "/v1/query",
            None,
            Body::empty(),
            &headers,
        )
        .await;
        assert_eq!(status, expected_status);
        assert_eq!(response_headers["briskdb-api-version"], "1");
        assert_eq!(response_headers["content-type"], "application/problem+json");
        assert!(
            response_headers["content-length"]
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap()
                > 0
        );
        assert!(!response_headers.contains_key("briskdb-idempotency-status"));
        let returned = request_id(&response_headers);
        if let Some(expected_request_id) = expected_request_id {
            assert_eq!(returned, expected_request_id);
        } else {
            assert_ne!(returned, supplied);
            assert_ne!(returned, "ABC");
        }
        assert!(body.is_empty());
    }
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
    assert_eq!(head.1["content-type"], "application/json");
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
    let unavailable_representation_length = unavailable.2.len().to_string();
    let unavailable_head = request(&app, Method::HEAD, "/v1/ready", None, Body::empty()).await;
    assert_eq!(unavailable_head.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(unavailable_head.1["briskdb-api-version"], "1");
    assert_eq!(unavailable_head.1["content-type"], "application/json");
    assert_eq!(
        unavailable_head.1["content-length"],
        unavailable_representation_length
    );
    assert!(unavailable_head.2.is_empty());
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
        assert_eq!(headers["content-type"], "application/json", "{uri}");
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

    let stream_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/query/stream")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "shard_key": "cancel-stream",
                        "sql": sql
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stream_response.status(), StatusCode::OK);
    assert_eq!(
        stream_response.headers()["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    request_id(stream_response.headers());
    let active = wait_for_active_query_count(&app, 1).await;
    let stream_id = active[0]["operation_id"].as_str().unwrap().to_owned();
    let cancellation = request(
        &app,
        Method::POST,
        &format!("/v1/admin/queries/{stream_id}/cancel"),
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(cancellation.0, StatusCode::ACCEPTED);
    let stream_body = to_bytes(stream_response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let records = ndjson_records(&stream_body);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["kind"], "meta");
    assert_eq!(records[1]["kind"], "error");
    assert_eq!(records[1]["code"], "cancelled");
    assert_eq!(records[1]["status"], 500);
    assert_eq!(records[1].as_object().unwrap().len(), 6);
    assert!(records[1].get("request_id").is_none());
    assert!(wait_for_active_query_count(&app, 0).await.is_empty());

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

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query/stream",
        Some("application/json"),
        serde_json::to_vec(&query).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    let records = ndjson_records(&body);
    assert_eq!(records.len(), 4);
    assert_eq!(
        records[0],
        json!({
            "kind": "meta",
            "shards": [0, 1],
            "value_encoding": "lossless-json-v1",
            "columns": [
                {"name": "duplicate", "data_type": "text"},
                {"name": "duplicate", "data_type": "int64"},
                {"name": "", "data_type": "binary"}
            ]
        })
    );
    assert_eq!(
        records[1],
        json!({
            "kind":"row",
            "values":[tenant_keys[0], tagged("int64", "0"), tagged("binary", "AA==")]
        })
    );
    assert_eq!(
        records[2],
        json!({
            "kind":"row",
            "values":[tenant_keys[1], tagged("int64", "1"), tagged("binary", "/w==")]
        })
    );
    assert_eq!(records[3], json!({"kind":"complete", "rows":2}));

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
        for uri in ["/v1/execute", "/v1/query", "/v1/query/stream"] {
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
        ("/v1/query/stream", Method::GET, "POST"),
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
    request_id(&response.1);
}

#[tokio::test]
async fn oversized_json_is_rejected_and_later_requests_still_work() {
    let (_temp, _engine, app) = application();
    let oversized = json!({"sql": "private-sentinel".repeat(150_000)}).to_string();
    for uri in [
        "/v1/query",
        "/v1/query/stream",
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
async fn query_result_limit_overrides_are_strict_narrowing_and_request_local() {
    let (_temp, _engine, app) = application();
    let query = |limits: Value| {
        json!({
            "shard_key": "limited-request",
            "sql": "SELECT 1 AS v UNION ALL SELECT 2",
            "result_limits": limits
        })
    };

    for limits in [
        json!({}),
        json!({"max_rows": 0}),
        json!({"max_rows": -1}),
        json!({"max_rows": 1.5}),
        json!({"max_rows": "1"}),
        json!({"max_logical_bytes": 0}),
        json!({"private-sentinel": 1}),
    ] {
        assert_problem(
            request(
                &app,
                Method::POST,
                "/v1/query",
                Some("application/json"),
                serde_json::to_vec(&query(limits)).unwrap(),
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
            "/v1/query",
            Some("application/json"),
            r#"{"shard_key":"limited-request","sql":"SELECT 1","result_limits":null}"#,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
    );
    for endpoint in ["/v1/query", "/v1/query/stream"] {
        for limits in [
            r#"{"max_rows":null,"max_logical_bytes":1000}"#,
            r#"{"max_rows":1,"max_logical_bytes":null}"#,
        ] {
            let body = format!(
                r#"{{"shard_key":"limited-request","sql":"SELECT 1","result_limits":{limits}}}"#
            );
            assert_problem(
                request(&app, Method::POST, endpoint, Some("application/json"), body).await,
                StatusCode::BAD_REQUEST,
                "invalid_argument",
            );
        }
    }
    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/execute",
            Some("application/json"),
            r#"{"shard_key":"owner","sql":"INSERT INTO notes VALUES (99)","result_limits":{"max_rows":1}}"#,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
    );

    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            serde_json::to_vec(&query(json!({"max_rows": 1}))).unwrap(),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "limit_exceeded",
    );
    let exact = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&query(json!({"max_rows": 2}))).unwrap(),
    )
    .await;
    assert_eq!(exact.0, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&exact.2).unwrap()["rows"],
        json!([[1], [2]])
    );

    for (bytes, status) in [
        (51_u64, StatusCode::OK),
        (50, StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let response = request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            serde_json::to_vec(&json!({
                "shard_key": "limited-bytes",
                "sql": "SELECT 1 AS v",
                "result_limits": {"max_logical_bytes": bytes}
            }))
            .unwrap(),
        )
        .await;
        assert_eq!(response.0, status);
        if status != StatusCode::OK {
            assert_problem(response, status, "limit_exceeded");
        }
    }

    let recovered = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        r#"{"shard_key":"limited-request","sql":"SELECT 3 AS v"}"#,
    )
    .await;
    assert_eq!(recovered.0, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&recovered.2).unwrap()["rows"],
        json!([[3]])
    );
}

#[tokio::test]
async fn invalid_query_limits_are_rejected_before_engine_registry_admission() {
    let (_temp, engine, app) = application();
    let tracked = (0..briskdb::core::MAX_ACTIVE_QUERIES)
        .map(|ordinal| {
            engine
                .begin_tracked_query(&format!("SELECT private_sentinel_{ordinal}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        engine.active_queries().len(),
        briskdb::core::MAX_ACTIVE_QUERIES
    );

    for uri in ["/v1/query", "/v1/query/stream"] {
        assert_problem(
            request(
                &app,
                Method::POST,
                uri,
                Some("application/json"),
                r#"{"shard_key":"limit-order","sql":"private-sentinel","result_limits":{}}"#,
            )
            .await,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        );
        assert_eq!(
            engine.active_queries().len(),
            briskdb::core::MAX_ACTIVE_QUERIES,
            "{uri}"
        );
    }

    drop(tracked);
    assert!(engine.active_queries().is_empty());
}

#[tokio::test]
async fn ndjson_stream_has_exact_frames_encodings_and_late_limit_error() {
    let (_temp, _engine, app) = application();
    assert_problem(
        request(
            &app,
            Method::POST,
            "/v1/query/stream",
            Some("application/json"),
            serde_json::to_vec(&json!({
                "shard_key": "stream-metadata-limit",
                "sql": "SELECT 1 AS value",
                "result_limits": {"max_logical_bytes": 1}
            }))
            .unwrap(),
        )
        .await,
        StatusCode::UNPROCESSABLE_ENTITY,
        "limit_exceeded",
    );

    let complete = request_with_headers(
        &app,
        Method::POST,
        "/v1/query/stream",
        Some("application/json"),
        serde_json::to_vec(&json!({
            "shard_key": "stream-owner",
            "sql": "SELECT ?1 AS exact, ?2 AS payload UNION ALL SELECT ?3, ?4",
            "params": [
                tagged("int64", "9007199254740993"),
                tagged("binary", "AAH/"),
                tagged("int64", "-9007199254740993"),
                tagged("binary", "Cg0e")
            ],
            "value_encoding": "lossless-json-v1"
        }))
        .unwrap(),
        &[("BriskDB-Request-ID", "1234567890abcdef1234567890abcdef")],
    )
    .await;
    assert_eq!(complete.0, StatusCode::OK);
    assert_eq!(
        complete.1["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    assert_eq!(request_id(&complete.1), "1234567890abcdef1234567890abcdef");
    assert!(!complete.1.contains_key("content-length"));
    let records = ndjson_records(&complete.2);
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["kind"], "meta");
    assert_eq!(records[0]["value_encoding"], "lossless-json-v1");
    assert_eq!(records[0]["columns"].as_array().unwrap().len(), 2);
    assert!(records[0].get("shard").is_some());
    assert!(records[0].get("shards").is_none());
    assert_eq!(
        records[1],
        json!({
            "kind": "row",
            "values": [tagged("int64", "9007199254740993"), tagged("binary", "AAH/")]
        })
    );
    assert_eq!(
        records[2],
        json!({
            "kind": "row",
            "values": [tagged("int64", "-9007199254740993"), tagged("binary", "Cg0e")]
        })
    );
    assert_eq!(records[3], json!({"kind": "complete", "rows": 2}));

    let limited = request(
        &app,
        Method::POST,
        "/v1/query/stream",
        Some("application/json"),
        serde_json::to_vec(&json!({
            "shard_key": "stream-limit",
            "sql": "SELECT 1 AS v UNION ALL SELECT 2",
            "result_limits": {"max_rows": 1}
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(limited.0, StatusCode::OK);
    assert_eq!(
        limited.1["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    let records = ndjson_records(&limited.2);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["kind"], "meta");
    assert_eq!(records[1], json!({"kind":"row", "values":[1]}));
    assert_eq!(
        records[2],
        json!({
            "kind": "error",
            "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#limit-exceeded",
            "title": "Limit exceeded",
            "status": 422,
            "detail": "The request exceeds an engine limit.",
            "code": "limit_exceeded"
        })
    );
    assert_eq!(records[2].as_object().unwrap().len(), 6);
}

#[tokio::test]
async fn eligible_http_write_is_created_replayed_conflicted_and_durable() {
    let (temp, engine, app, _tenant_keys) = scatter_application();
    let key = "fedcba9876543210fedcba9876543210";
    let body = json!({
        "sql": "INSERT INTO events (tenant_key, ordinal, payload) VALUES (?1, ?2, ?3)",
        "params": ["http-idempotent", 41, "one"]
    });
    let (first, concurrent_retry) = tokio::join!(
        idempotent_execute(&app, key, &body),
        idempotent_execute(&app, key, &body),
    );
    let created_count = [&first, &concurrent_retry]
        .into_iter()
        .filter(|response| {
            response
                .1
                .get("briskdb-idempotency-status")
                .is_some_and(|status| status == "created")
        })
        .count();
    assert_eq!(created_count, 1);
    let original_body = [&first, &concurrent_retry]
        .into_iter()
        .find(|response| response.0 == StatusCode::OK)
        .unwrap()
        .2
        .clone();
    for response in [first, concurrent_retry] {
        match response.0 {
            StatusCode::OK => {
                let status = response.1["briskdb-idempotency-status"].to_str().unwrap();
                assert!(matches!(status, "created" | "replayed"));
                assert_eq!(response.2, original_body);
                assert_eq!(
                    serde_json::from_slice::<Value>(&response.2).unwrap()["rows_affected"],
                    1
                );
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                assert!(!response.1.contains_key("briskdb-idempotency-status"));
                assert_problem(response, StatusCode::SERVICE_UNAVAILABLE, "busy");
            }
            status => panic!("unexpected concurrent idempotency status {status}"),
        }
    }

    let replayed = idempotent_execute(&app, key, &body).await;
    assert_eq!(replayed.0, StatusCode::OK);
    assert_eq!(replayed.1["briskdb-idempotency-status"], "replayed");
    assert_eq!(replayed.2, original_body);

    let mut conflict_body = body.clone();
    conflict_body["params"][1] = json!(42);
    let conflict = idempotent_execute(&app, key, &conflict_body).await;
    assert!(!conflict.1.contains_key("briskdb-idempotency-status"));
    assert_problem(conflict, StatusCode::CONFLICT, "idempotency_conflict");

    let unkeyed = request(
        &app,
        Method::POST,
        "/v1/execute",
        Some("application/json"),
        serde_json::to_vec(&json!({
            "sql": "UPDATE events SET ordinal = ordinal WHERE tenant_key = ?1",
            "params": ["http-idempotent"]
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(unkeyed.0, StatusCode::OK);
    assert!(!unkeyed.1.contains_key("briskdb-idempotency-status"));

    drop(app);
    drop(engine);
    let reopened = Arc::new(Database::open(temp.path(), 2).unwrap());
    let reopened_engine = Engine::from_database(reopened);
    let reopened_app = briskdb::api::router_with_engine(reopened_engine.clone());
    let after_restart = idempotent_execute(&reopened_app, key, &body).await;
    assert_eq!(after_restart.0, StatusCode::OK);
    assert_eq!(after_restart.1["briskdb-idempotency-status"], "replayed");
    assert_eq!(after_restart.2, original_body);

    let selected = request(
        &reopened_app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&json!({
            "sql": "SELECT ordinal FROM events WHERE tenant_key = ?1",
            "params": ["http-idempotent"]
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(selected.0, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&selected.2).unwrap()["rows"],
        json!([[41]])
    );
    reopened_engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn lossless_query_result_limit_returns_only_the_problem_document() {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    let options = EngineOptions::default().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
    let app = briskdb::api::router_with_engine(
        Engine::from_database_with_options(database, options).unwrap(),
    );
    let discovery = request(&app, Method::GET, "/v1", None, Body::empty()).await;
    assert_eq!(discovery.0, StatusCode::OK);
    let discovery: Value = serde_json::from_slice(&discovery.2).unwrap();
    assert_eq!(discovery["max_result_rows"], 1);
    assert_eq!(discovery["max_result_logical_bytes"], 1_024);
    let body = json!({
        "shard_key": "limited-lossless",
        "sql": "SELECT 1 AS value UNION ALL SELECT 2",
        "value_encoding": "lossless-json-v1",
        "result_limits": {"max_rows": 100, "max_logical_bytes": 1_000_000}
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
