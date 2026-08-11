//! Experimental HTTP adapter.

mod admin;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::{
    core::{DataType, Database, Engine, EngineError, ResultSet, Routed, Statement, Value},
    protocol::error::http_error,
};

/// Build an HTTP router from the legacy synchronous database handle.
///
/// New callers should construct one shared [`Engine`] and use
/// [`router_with_engine`]. This wrapper preserves the pre-engine Rust API while
/// still sending every request through the shared asynchronous engine.
pub fn router(database: Arc<Database>) -> Router {
    router_with_engine(Engine::from_database(database))
}

/// Build an HTTP router backed by the protocol-neutral asynchronous engine.
pub fn router_with_engine(engine: Engine) -> Router {
    let state = HttpState {
        engine,
        admin_sessions: admin::SessionStore::new(),
    };
    Router::new()
        .route("/health", get(health))
        .route("/v1/execute", post(execute))
        .route("/v1/query", post(query))
        .route("/v1/admin/broadcast", post(broadcast))
        .merge(admin::routes(state.clone()))
        .with_state(state)
}

#[derive(Clone)]
struct HttpState {
    engine: Engine,
    admin_sessions: admin::SessionStore,
}

async fn health(State(state): State<HttpState>) -> Result<Json<JsonValue>, ApiError> {
    let engine = state.engine;
    let session = engine.session();
    let status = engine.status(&session).await?;

    Ok(Json(json!({
        "status": "ok",
        "shards": status.shard_count(),
    })))
}

#[derive(Debug, Deserialize)]
struct RoutedSqlRequest {
    shard_key: String,
    sql: String,
    #[serde(default)]
    params: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BroadcastRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    shard: u16,
    rows_affected: usize,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    shard: u16,
    columns: Vec<QueryColumn>,
    rows: Vec<Vec<JsonValue>>,
}

#[derive(Debug, Serialize)]
struct QueryColumn {
    name: String,
    data_type: &'static str,
}

async fn execute(
    State(state): State<HttpState>,
    Json(request): Json<RoutedSqlRequest>,
) -> Result<Json<ExecuteResponse>, ApiError> {
    let engine = state.engine;
    let params = request
        .params
        .into_iter()
        .map(json_to_value)
        .collect::<Vec<_>>();
    let session = engine.session();
    session.set_routing_key(request.shard_key).await?;
    let Routed {
        shard,
        value: rows_affected,
    } = engine
        .execute(&session, Statement::new(request.sql, params))
        .await?;

    Ok(Json(ExecuteResponse {
        shard,
        rows_affected,
    }))
}

async fn query(
    State(state): State<HttpState>,
    Json(request): Json<RoutedSqlRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let engine = state.engine;
    let params = request
        .params
        .into_iter()
        .map(json_to_value)
        .collect::<Vec<_>>();
    let session = engine.session();
    session.set_routing_key(request.shard_key).await?;
    let Routed {
        shard,
        value: result,
    } = engine
        .query(&session, Statement::new(request.sql, params))
        .await?;
    let response = result_set_to_query_response(shard, result);

    Ok(Json(response))
}

async fn broadcast(
    State(state): State<HttpState>,
    Json(request): Json<BroadcastRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let engine = state.engine;
    let session = engine.session();
    let shards = engine.broadcast(&session, request.sql).await?;
    Ok(Json(json!({"completed_shards": shards})))
}

fn json_to_value(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(value),
        JsonValue::Number(value) => value
            .as_i64()
            .map(Value::Int64)
            .or_else(|| value.as_u64().map(Value::UInt64))
            .or_else(|| value.as_f64().map(Value::Float64))
            .unwrap_or_else(|| {
                Value::decimal(value.to_string())
                    .expect("a serde_json Number always has valid decimal syntax")
            }),
        JsonValue::String(value) => Value::Text(value),
        JsonValue::Array(value) => Value::Text(JsonValue::Array(value).to_string()),
        JsonValue::Object(value) => Value::Text(JsonValue::Object(value).to_string()),
    }
}

fn value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => JsonValue::Bool(value),
        Value::Int64(value) => json!(value),
        Value::UInt64(value) => json!(value),
        Value::Float64(value) => {
            serde_json::Number::from_f64(value).map_or(JsonValue::Null, JsonValue::Number)
        }
        Value::Decimal(value) => JsonValue::String(value.into_string()),
        Value::Text(value) => JsonValue::String(value),
        Value::InvalidText(value) => {
            JsonValue::String(String::from_utf8_lossy(&value).into_owned())
        }
        Value::Binary(value) => {
            JsonValue::Array(value.into_iter().map(|byte| json!(byte)).collect())
        }
    }
}

fn result_set_to_query_response(shard: u16, result: ResultSet) -> QueryResponse {
    let (columns, rows) = result.into_parts();
    let columns = columns
        .into_iter()
        .map(|column| QueryColumn {
            name: column.name,
            data_type: data_type_name(column.data_type),
        })
        .collect();
    let rows = rows
        .into_iter()
        .map(|row| row.into_values().into_iter().map(value_to_json).collect())
        .collect();

    QueryResponse {
        shard,
        columns,
        rows,
    }
}

const fn data_type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Unknown => "unknown",
        DataType::Null => "null",
        DataType::Boolean => "boolean",
        DataType::Int64 => "int64",
        DataType::UInt64 => "uint64",
        DataType::Float64 => "float64",
        DataType::Decimal => "decimal",
        DataType::Text => "text",
        DataType::Binary => "binary",
    }
}

#[derive(Debug)]
struct ApiError(EngineError);

impl From<EngineError> for ApiError {
    fn from(error: EngineError) -> Self {
        Self(error)
    }
}

#[derive(Debug, Serialize)]
struct ProblemDetails {
    #[serde(rename = "type")]
    problem_type: &'static str,
    title: &'static str,
    status: u16,
    detail: &'static str,
    code: &'static str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mapping = http_error(self.0.kind());
        tracing::error!(
            error = ?self.0,
            error_code = self.0.code(),
            "engine request failed"
        );
        let status = StatusCode::from_u16(mapping.status)
            .expect("the exhaustive HTTP error mapping contains valid status codes");
        let mut response = (
            status,
            Json(ProblemDetails {
                problem_type: mapping.problem_type,
                title: mapping.title,
                status: mapping.status,
                detail: mapping.detail,
                code: self.0.code(),
            }),
        )
            .into_response();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::{
        core::{Column, DataType, EngineErrorKind, EngineOptions, ResultLimits, Row},
        sql::{
            MAX_PARSED_SQL_BYTES, SqlDialect, normalize_placeholders, parse, validate_common_subset,
        },
    };

    fn engine_router(database: Arc<Database>) -> Router {
        router_with_engine(Engine::from_database(database))
    }

    async fn send_json(
        router: &Router,
        method: Method,
        uri: &str,
        body: Option<JsonValue>,
    ) -> Response {
        let mut request = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(value) => {
                request = request.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&value).unwrap())
            }
            None => Body::empty(),
        };
        router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap()
    }

    async fn response_json(response: Response) -> (StatusCode, JsonValue) {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn request_json(
        router: &Router,
        method: Method,
        uri: &str,
        body: Option<JsonValue>,
    ) -> (StatusCode, JsonValue) {
        response_json(send_json(router, method, uri, body).await).await
    }

    #[tokio::test]
    async fn all_http_endpoints_follow_the_current_contract_through_the_engine() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"widget-1");
        let application = engine_router(database);

        assert_eq!(
            request_json(&application, Method::GET, "/health", None).await,
            (StatusCode::OK, json!({"status": "ok", "shards": 4}))
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/admin/broadcast",
                Some(json!({
                    "sql": "CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT NOT NULL)"
                })),
            )
            .await,
            (StatusCode::OK, json!({"completed_shards": [0, 1, 2, 3]}))
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/execute",
                Some(json!({
                    "shard_key": "widget-1",
                    "sql": "CREATE TABLE bypassed_migration (id INTEGER)"
                })),
            )
            .await,
            (
                StatusCode::FORBIDDEN,
                json!({
                    "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#permission-denied",
                    "title": "Permission denied",
                    "status": 403,
                    "detail": "The operation is not permitted.",
                    "code": "permission_denied"
                }),
            )
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/execute",
                Some(json!({
                    "shard_key": "widget-1",
                    "sql": "INSERT INTO widgets (id, name) VALUES (?1, ?2)",
                    "params": ["widget-1", "First widget"]
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({"shard": expected_shard, "rows_affected": 1}),
            )
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "widget-1",
                    "sql": "SELECT id, name FROM widgets WHERE id = ?1",
                    "params": ["widget-1"]
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": expected_shard,
                    "columns": [
                        {"name": "id", "data_type": "unknown"},
                        {"name": "name", "data_type": "unknown"}
                    ],
                    "rows": [["widget-1", "First widget"]]
                }),
            )
        );
    }

    #[tokio::test]
    async fn detected_schema_drift_makes_health_fail_closed_with_a_redacted_problem() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let application = engine_router(database);
        for shard_id in 0..2 {
            rusqlite::Connection::open(temp.path().join(format!("shards/{shard_id:04}.sqlite")))
                .unwrap()
                .execute_batch("CREATE TABLE secret_drift(value TEXT)")
                .unwrap();
        }

        let expected = json!({
            "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#data-corruption",
            "title": "Data corruption",
            "status": 500,
            "detail": "Stored data failed an integrity check.",
            "code": "data_corruption"
        });
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "detect-drift",
                    "sql": "SELECT 1",
                    "params": []
                })),
            )
            .await,
            (StatusCode::INTERNAL_SERVER_ERROR, expected.clone())
        );
        let (status, body) = request_json(&application, Method::GET, "/health", None).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, expected);
        let serialized = body.to_string();
        assert!(!serialized.contains("secret_drift"));
        assert!(!serialized.contains(temp.path().to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn legacy_database_router_is_a_behavior_preserving_engine_wrapper() {
        let temp = tempfile::tempdir().unwrap();
        let application = router(Arc::new(Database::open(temp.path(), 4).unwrap()));

        assert_eq!(
            request_json(&application, Method::GET, "/health", None).await,
            (StatusCode::OK, json!({"status": "ok", "shards": 4}))
        );
        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "compatibility-request",
                "sql": "SELECT 42 AS answer"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["columns"],
            json!([{"name": "answer", "data_type": "unknown"}])
        );
        assert_eq!(body["rows"], json!([[42]]));
    }

    #[tokio::test]
    async fn empty_catalog_http_sql_retains_raw_sqlite_compatibility() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let expected_shard = database.shard_for_key(b"raw-http-parser-boundary");
        let application = engine_router(database);
        let mut raw_sql = "SELECT 7 AS value".to_owned();
        raw_sql.push_str(&" ".repeat(MAX_PARSED_SQL_BYTES));
        assert!(raw_sql.len() > MAX_PARSED_SQL_BYTES);

        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "raw-http-parser-boundary",
                    "sql": raw_sql
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": expected_shard,
                    "columns": [{"name": "value", "data_type": "unknown"}],
                    "rows": [[7]]
                })
            )
        );
    }

    #[tokio::test]
    async fn common_subset_validation_does_not_change_the_current_http_query_path() {
        let source = "WITH answer(value) AS (VALUES (9)) SELECT value FROM answer";
        let validation_error =
            validate_common_subset(parse(SqlDialect::Sqlite, source).unwrap()).unwrap_err();
        assert_eq!(validation_error.kind(), EngineErrorKind::Unsupported);

        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let expected_shard = database.shard_for_key(b"subset-http-boundary");
        let application = engine_router(database);

        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "subset-http-boundary",
                    "sql": source
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": expected_shard,
                    "columns": [{"name": "value", "data_type": "unknown"}],
                    "rows": [[9]]
                })
            )
        );
    }

    #[tokio::test]
    async fn placeholder_normalization_does_not_change_the_current_http_parameter_path() {
        let source = "SELECT :value AS value";
        let common = validate_common_subset(parse(SqlDialect::Sqlite, source).unwrap()).unwrap();
        let normalization_error = normalize_placeholders(common).unwrap_err();
        assert_eq!(normalization_error.kind(), EngineErrorKind::Unsupported);

        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let expected_shard = database.shard_for_key(b"normalizer-http-boundary");
        let application = engine_router(database);

        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "normalizer-http-boundary",
                    "sql": source,
                    "params": [9]
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": expected_shard,
                    "columns": [{"name": "value", "data_type": "unknown"}],
                    "rows": [[9]]
                })
            )
        );
    }

    #[tokio::test]
    async fn a_failed_http_request_does_not_poison_the_next_session() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"recovery-request");
        let application = engine_router(database);

        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "recovery-request",
                    "sql": "SELECT * FROM missing_table"
                })),
            )
            .await
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": "recovery-request",
                    "sql": "SELECT 'recovered' AS state"
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": expected_shard,
                    "columns": [{"name": "state", "data_type": "unknown"}],
                    "rows": [["recovered"]]
                })
            )
        );
    }

    #[tokio::test]
    async fn concurrent_http_requests_use_the_shared_engine_without_value_leakage() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let application = engine_router(Arc::clone(&database));
        let mut requests = tokio::task::JoinSet::new();

        for value in 0_i64..16 {
            let application = application.clone();
            let shard_key = format!("concurrent-{value}");
            let expected_shard = database.shard_for_key(shard_key.as_bytes());
            requests.spawn(async move {
                let response = request_json(
                    &application,
                    Method::POST,
                    "/v1/query",
                    Some(json!({
                        "shard_key": shard_key,
                        "sql": "SELECT ?1 AS value",
                        "params": [value]
                    })),
                )
                .await;
                (value, expected_shard, response)
            });
        }

        let mut completed = 0;
        while let Some(request) = requests.join_next().await {
            let (value, expected_shard, (status, body)) = request.unwrap();
            assert_eq!(status, StatusCode::OK, "request {value}: {body}");
            assert_eq!(body["shard"], json!(expected_shard));
            assert_eq!(
                body["columns"],
                json!([{"name": "value", "data_type": "unknown"}])
            );
            assert_eq!(body["rows"], json!([[value]]));
            completed += 1;
        }
        assert_eq!(completed, 16);
    }

    #[tokio::test]
    async fn invalid_queries_use_safe_problem_details() {
        let temp = tempfile::tempdir().unwrap();
        let application = engine_router(Arc::new(Database::open(temp.path(), 4).unwrap()));

        let response = send_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "widget-1",
                "sql": "SELECT * FROM missing_table"
            })),
        )
        .await;
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
        let (status, body) = response_json(response).await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body,
            json!({
                "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-query",
                "title": "Invalid query",
                "status": 422,
                "detail": "The query could not be processed.",
                "code": "invalid_query"
            })
        );
        assert!(!body.to_string().contains("missing_table"));
    }

    #[tokio::test]
    async fn result_limit_failures_return_only_a_safe_problem_without_partial_rows() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let options =
            EngineOptions::default().with_result_limits(ResultLimits::new(1, 1_024).unwrap());
        let application =
            router_with_engine(Engine::from_database_with_options(database, options).unwrap());

        let response = send_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "limited-query",
                "sql": "SELECT 1 AS value UNION ALL SELECT 2"
            })),
        )
        .await;
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
        let (status, body) = response_json(response).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body,
            json!({
                "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#limit-exceeded",
                "title": "Limit exceeded",
                "status": 422,
                "detail": "The request exceeds an engine limit.",
                "code": "limit_exceeded"
            })
        );
        assert!(body.get("columns").is_none());
        assert!(body.get("rows").is_none());
    }

    #[tokio::test]
    async fn engine_deadlines_reach_http_as_safe_gateway_timeout_problems() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let options = EngineOptions::default()
            .with_request_timeout(Some(Duration::from_millis(5)))
            .unwrap();
        let application =
            router_with_engine(Engine::from_database_with_options(database, options).unwrap());

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "deadline-query",
                "sql": "WITH RECURSIVE numbers(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000) SELECT sum(value) FROM numbers"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            body,
            json!({
                "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#deadline-exceeded",
                "title": "Request deadline exceeded",
                "status": 504,
                "detail": "The operation exceeded its request deadline.",
                "code": "deadline_exceeded"
            })
        );
    }

    #[tokio::test]
    async fn unsigned_parameters_outside_sqlite_range_fail_instead_of_rounding() {
        let temp = tempfile::tempdir().unwrap();
        let application = engine_router(Arc::new(Database::open(temp.path(), 4).unwrap()));
        let too_large = u64::try_from(i64::MAX).unwrap() + 1;

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "typed-row",
                "sql": "SELECT ?1 AS value",
                "params": [too_large]
            })),
        )
        .await;

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body,
            json!({
                "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#numeric-out-of-range",
                "title": "Numeric value out of range",
                "status": 422,
                "detail": "A numeric value is outside the supported range.",
                "code": "numeric_out_of_range"
            })
        );
        assert!(!body.to_string().contains(&too_large.to_string()));
    }

    #[tokio::test]
    async fn constraint_failures_keep_their_precise_safe_kind() {
        let temp = tempfile::tempdir().unwrap();
        let application = engine_router(Arc::new(Database::open(temp.path(), 4).unwrap()));
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/admin/broadcast",
                Some(json!({"sql": "CREATE TABLE widgets (id TEXT PRIMARY KEY)"})),
            )
            .await
            .0,
            StatusCode::OK
        );
        let insert = || {
            send_json(
                &application,
                Method::POST,
                "/v1/execute",
                Some(json!({
                    "shard_key": "widget-1",
                    "sql": "INSERT INTO widgets (id) VALUES (?1)",
                    "params": ["private-value"]
                })),
            )
        };
        assert_eq!(response_json(insert().await).await.0, StatusCode::OK);

        let response = insert().await;
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
        assert_eq!(
            response_json(response).await,
            (
                StatusCode::CONFLICT,
                json!({
                    "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#unique-violation",
                    "title": "Unique constraint violation",
                    "status": 409,
                    "detail": "A unique constraint was violated.",
                    "code": "unique_violation"
                })
            )
        );
    }

    #[tokio::test]
    async fn every_problem_kind_uses_its_exact_mapping_and_redacts_sources() {
        let secret = "password=hunter2 /private/customer.sqlite SELECT secret_value";
        for &kind in EngineErrorKind::ALL {
            let mapping = http_error(kind);
            let error = EngineError::from_source(kind, secret, io::Error::other(secret));
            let response = ApiError(error).into_response();

            assert_eq!(
                response.headers().get(CONTENT_TYPE).unwrap(),
                "application/problem+json"
            );
            let (status, body) = response_json(response).await;
            assert_eq!(status.as_u16(), mapping.status, "{} status", kind.code());
            assert_eq!(
                body,
                json!({
                    "type": mapping.problem_type,
                    "title": mapping.title,
                    "status": mapping.status,
                    "detail": mapping.detail,
                    "code": kind.code()
                }),
                "{} body",
                kind.code()
            );
            assert!(!body.to_string().contains(secret));
        }
    }

    #[tokio::test]
    async fn internal_engine_errors_become_redacted_internal_problems() {
        let secret = "password=hunter2 /private/customer.sqlite SELECT secret_value";
        let response = ApiError(EngineError::from_source(
            EngineErrorKind::Internal,
            secret,
            io::Error::other(secret),
        ))
        .into_response();

        assert_eq!(
            response_json(response).await,
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#internal",
                    "title": "Internal error",
                    "status": 500,
                    "detail": "An internal engine error occurred.",
                    "code": "internal"
                })
            )
        );
    }

    #[tokio::test]
    async fn non_finite_sqlite_reals_keep_the_legacy_json_null_encoding() {
        let temp = tempfile::tempdir().unwrap();
        let application = engine_router(Arc::new(Database::open(temp.path(), 4).unwrap()));

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "typed-row",
                "sql": "SELECT 1e999 AS positive_infinity, -1e999 AS negative_infinity"
            })),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["columns"],
            json!([
                {"name": "positive_infinity", "data_type": "unknown"},
                {"name": "negative_infinity", "data_type": "unknown"}
            ])
        );
        assert_eq!(body["rows"], json!([[null, null]]));
    }

    #[tokio::test]
    async fn typed_core_keeps_legacy_http_parameter_and_blob_shapes() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"typed-row");
        let application = engine_router(database);

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "typed-row",
                "sql": "SELECT ?1 AS enabled, ?2 AS object_text, ?3 AS array_text, X'00ff' AS data",
                "params": [true, {"nested": true}, [1, "two"]]
            })),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "shard": expected_shard,
                "columns": [
                    {"name": "enabled", "data_type": "unknown"},
                    {"name": "object_text", "data_type": "unknown"},
                    {"name": "array_text", "data_type": "unknown"},
                    {"name": "data", "data_type": "unknown"}
                ],
                "rows": [[1, "{\"nested\":true}", "[1,\"two\"]", [0, 255]]]
            })
        );
    }

    #[tokio::test]
    async fn query_preserves_duplicate_column_names_and_positions() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"duplicate-row");
        let application = engine_router(database);

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "duplicate-row",
                "sql": "SELECT 1 AS duplicate, 2 AS middle, 3 AS duplicate, 4 AS \"\""
            })),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "shard": expected_shard,
                "columns": [
                    {"name": "duplicate", "data_type": "unknown"},
                    {"name": "middle", "data_type": "unknown"},
                    {"name": "duplicate", "data_type": "unknown"},
                    {"name": "", "data_type": "unknown"}
                ],
                "rows": [[1, 2, 3, 4]]
            })
        );
    }

    #[tokio::test]
    async fn empty_query_results_keep_duplicate_column_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"empty-row");
        let application = engine_router(database);

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "empty-row",
                "sql": "SELECT 1 AS duplicate, 2 AS duplicate WHERE 0"
            })),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "shard": expected_shard,
                "columns": [
                    {"name": "duplicate", "data_type": "unknown"},
                    {"name": "duplicate", "data_type": "unknown"}
                ],
                "rows": []
            })
        );
    }

    #[test]
    fn legacy_api_module_reexports_the_router() {
        let _legacy_router: fn(Arc<Database>) -> Router = crate::api::router;
        let _engine_router: fn(Engine) -> Router = crate::api::router_with_engine;
    }

    #[test]
    fn production_http_adapter_has_no_blocking_or_shard_routing_escape_hatches() {
        let test_module_marker = ["#[cfg", "(test)]\nmod tests {"].concat();
        let (production_source, _) = include_str!("http.rs")
            .split_once(&test_module_marker)
            .expect("the HTTP unit-test module has a cfg(test) boundary");

        assert_eq!(
            production_source.matches("Database").count(),
            2,
            "Database may appear only in the compatibility import and router signature"
        );

        for forbidden in [
            "spawn_blocking",
            "block_in_place",
            "execute_routed(",
            "query_routed(",
            "shard_for_key(",
            "database.execute(",
            "database.query(",
            "database.broadcast(",
            "open_shard(",
            "crate::sql",
            "crate::storage",
            "blake3",
            "rusqlite",
            "ConnectionPools",
            "PooledConnection",
            "BlockingPool",
            "EngineOptions",
            "Semaphore",
        ] {
            assert!(
                !production_source.contains(forbidden),
                "HTTP production code contains forbidden backend escape hatch {forbidden}"
            );
        }
    }

    #[test]
    fn json_parameters_keep_the_existing_binding_contract() {
        assert_eq!(json_to_value(JsonValue::Null), Value::Null);
        assert_eq!(json_to_value(json!(true)), Value::from(true));
        assert_eq!(json_to_value(json!(42)), Value::from(42_i64));
        assert_eq!(json_to_value(json!(1.5)), Value::from(1.5_f64));
        assert_eq!(json_to_value(json!("text")), Value::from("text"));
        assert_eq!(json_to_value(json!([1, "two"])), Value::from("[1,\"two\"]"));
        assert_eq!(
            json_to_value(json!({"nested": true})),
            Value::from("{\"nested\":true}")
        );

        let above_signed_i64_range = json!(9_223_372_036_854_775_809_u64);
        assert_eq!(
            json_to_value(above_signed_i64_range),
            Value::from(9_223_372_036_854_775_809_u64)
        );
    }

    #[test]
    fn typed_values_encode_to_explicit_legacy_json_shapes() {
        assert_eq!(
            value_to_json(Value::from(u64::MAX)),
            json!(18_446_744_073_709_551_615_u64)
        );
        assert_eq!(
            value_to_json(Value::decimal("12.3400").unwrap()),
            json!("12.3400")
        );
        assert_eq!(value_to_json(Value::from(f64::INFINITY)), JsonValue::Null);
        assert_eq!(value_to_json(Value::from(f64::NAN)), JsonValue::Null);
        assert_eq!(
            value_to_json(Value::InvalidText(vec![b'f', 0x80])),
            JsonValue::String("f\u{fffd}".to_owned())
        );
    }

    #[test]
    fn every_data_type_has_a_stable_http_metadata_name() {
        assert_eq!(
            [
                DataType::Unknown,
                DataType::Null,
                DataType::Boolean,
                DataType::Int64,
                DataType::UInt64,
                DataType::Float64,
                DataType::Decimal,
                DataType::Text,
                DataType::Binary,
            ]
            .map(data_type_name),
            [
                "unknown", "null", "boolean", "int64", "uint64", "float64", "decimal", "text",
                "binary",
            ]
        );
    }

    #[test]
    fn result_encoding_keeps_column_order_duplicate_names_and_row_positions() {
        let result = ResultSet::new(
            vec![
                Column::new("duplicate", DataType::Unknown),
                Column::new("duplicate", DataType::Text),
                Column::new("blob", DataType::Binary),
                Column::new("flag", DataType::Boolean),
                Column::new("", DataType::Null),
            ],
            vec![
                Row::new(vec![
                    Value::from(1_i64),
                    Value::from("second position"),
                    Value::from(vec![0_u8, 255]),
                    Value::from(true),
                    Value::Null,
                ]),
                Row::new(vec![
                    Value::from(2_i64),
                    Value::from("still separate"),
                    Value::from(vec![1_u8, 2]),
                    Value::from(false),
                    Value::Null,
                ]),
            ],
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(result_set_to_query_response(3, result)).unwrap(),
            json!({
                "shard": 3,
                "columns": [
                    {"name": "duplicate", "data_type": "unknown"},
                    {"name": "duplicate", "data_type": "text"},
                    {"name": "blob", "data_type": "binary"},
                    {"name": "flag", "data_type": "boolean"},
                    {"name": "", "data_type": "null"}
                ],
                "rows": [
                    [1, "second position", [0, 255], true, null],
                    [2, "still separate", [1, 2], false, null]
                ]
            })
        );
    }

    #[test]
    fn result_encoding_keeps_valid_zero_column_shapes() {
        let empty = ResultSet::new(Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            serde_json::to_value(result_set_to_query_response(1, empty)).unwrap(),
            json!({"shard": 1, "columns": [], "rows": []})
        );

        let empty_row = ResultSet::new(Vec::new(), vec![Row::new(Vec::new())]).unwrap();
        assert_eq!(
            serde_json::to_value(result_set_to_query_response(2, empty_row)).unwrap(),
            json!({"shard": 2, "columns": [], "rows": [[]]})
        );
    }
}
