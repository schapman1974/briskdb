//! Experimental HTTP adapter.

mod admin;

use std::{fmt::Write as _, sync::Arc};

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
    core::{
        DataType, Database, Engine, EngineError, EngineErrorKind, Executed, GeneratedKey,
        GlobalIndexHealthState, GlobalIndexLifecycle, GlobalIndexOperationalReport,
        GlobalIndexOperationalStatus, ResultSet, Routed, Statement, Value,
    },
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
        .route("/metrics", get(metrics))
        .route("/v1/execute", post(execute))
        .route("/v1/query", post(query))
        .route("/v1/admin/broadcast", post(broadcast))
        .route("/v1/admin/global-indexes", get(global_indexes))
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
    let indexes = engine.global_index_operational_report().await?;
    let service_status = if indexes.state() == GlobalIndexHealthState::Healthy {
        "ok"
    } else {
        "degraded"
    };
    tracing::debug!(
        global_index_state = indexes.state().code(),
        global_indexes = indexes.indexes().len(),
        degraded_global_indexes = indexes.degraded_indexes(),
        unavailable_global_indexes = indexes.unavailable_indexes(),
        global_index_async_lag = indexes.async_lag(),
        global_index_outbox_events = indexes.retained_outbox_events(),
        global_index_outbox_bytes = indexes.retained_outbox_bytes(),
        global_index_backpressured_shards = indexes.backpressured_outbox_shards(),
        "global-index operational health"
    );

    Ok(Json(json!({
        "status": service_status,
        "shards": status.shard_count(),
        "global_indexes": {
            "state": indexes.state().code(),
            "total": indexes.indexes().len(),
            "healthy": indexes.healthy_indexes(),
            "degraded": indexes.degraded_indexes(),
            "unavailable": indexes.unavailable_indexes(),
            "async_lag": indexes.async_lag(),
            "retained_outbox_events": indexes.retained_outbox_events(),
            "retained_outbox_bytes": indexes.retained_outbox_bytes(),
            "backpressured_outbox_shards": indexes.backpressured_outbox_shards(),
        },
    })))
}

async fn metrics(State(state): State<HttpState>) -> Result<Response, ApiError> {
    let report = state.engine.global_index_operational_report().await?;
    let mut response = prometheus_metrics(&report).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok(response)
}

#[derive(Debug, Deserialize)]
struct RoutedSqlRequest {
    #[serde(default)]
    shard_key: Option<String>,
    sql: String,
    #[serde(default)]
    params: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct QueryRequest {
    /// Retained for empty-catalog compatibility. Registered-table reads are
    /// routed from catalog metadata and SQL predicates instead.
    #[serde(default)]
    shard_key: Option<String>,
    sql: String,
    #[serde(default)]
    params: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BroadcastRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct GlobalIndexesResponse {
    state: &'static str,
    retained_outbox_events: u64,
    retained_outbox_bytes: u64,
    backpressured_outbox_shards: u16,
    indexes: Vec<GlobalIndexStatus>,
}

#[derive(Debug, Serialize)]
struct GlobalIndexStatus {
    id: String,
    name: String,
    unique: bool,
    lifecycle: &'static str,
    health: &'static str,
    available: bool,
    recovery: &'static str,
    authority_entries: u64,
    unique_keys: u64,
    active_operations: u64,
    active_unique_reservations: u64,
    active_value_leases: u64,
    pending_read_repairs: u64,
    applied_read_repairs: u64,
    async_lag: u64,
    async_failures: u64,
    poisoned_shards: u16,
    leased_shards: u16,
    async_paused: bool,
    rebuild_required: bool,
    summary_ready_shards: u16,
    summary_degraded_shards: u16,
    summary_saturated_shards: u16,
}

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    shard: u16,
    rows_affected: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    generated_key: Option<ExecuteGeneratedKey>,
}

#[derive(Debug, Serialize)]
struct ExecuteGeneratedKey {
    column: String,
    data_type: &'static str,
    value: String,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    shard: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shards: Option<Vec<u16>>,
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
    if let Some(shard_key) = request.shard_key {
        session.set_routing_key(shard_key).await?;
    }
    let Routed {
        shard,
        value: write_result,
    } = engine
        .execute_http_request(&session, Statement::new(request.sql, params))
        .await?;
    let rows_affected = write_result.rows_affected;
    let generated_key = write_result
        .generated_key
        .map(execute_generated_key)
        .transpose()?;

    Ok(Json(ExecuteResponse {
        shard,
        rows_affected,
        generated_key,
    }))
}

fn execute_generated_key(generated: GeneratedKey) -> Result<ExecuteGeneratedKey, EngineError> {
    let (data_type, value) = match generated.value {
        Value::Int64(value) => ("int64", value.to_string()),
        Value::UInt64(value) => ("uint64", value.to_string()),
        _ => {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "the engine returned a non-integer generated key to the HTTP adapter",
            ));
        }
    };
    Ok(ExecuteGeneratedKey {
        column: generated.column,
        data_type,
        value,
    })
}

async fn query(
    State(state): State<HttpState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let engine = state.engine;
    let params = request
        .params
        .into_iter()
        .map(json_to_value)
        .collect::<Vec<_>>();
    let session = engine.session();
    if engine.catalog().tables().is_empty() {
        if let Some(shard_key) = request.shard_key {
            session.set_routing_key(shard_key).await?;
        }
    }
    let Executed {
        shards,
        value: result,
    } = engine
        .query_logical(&session, Statement::new(request.sql, params))
        .await?;
    let response = result_set_to_query_response(shards, result);

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

async fn global_indexes(
    State(state): State<HttpState>,
) -> Result<Json<GlobalIndexesResponse>, ApiError> {
    let report = state.engine.global_index_operational_report().await?;
    let indexes = report
        .indexes()
        .iter()
        .map(|status| {
            let metadata = state
                .engine
                .catalog()
                .global_indexes()
                .iter()
                .find(|index| index.id() == status.index_id())
                .ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::Internal,
                        "global-index operational report does not match the catalog",
                    )
                })?;
            let (lifecycle, available, recovery) = lifecycle_status(status.lifecycle());
            Ok(GlobalIndexStatus {
                id: status.index_id().to_string(),
                name: metadata.name().to_owned(),
                unique: status.is_unique(),
                lifecycle,
                health: status.state().code(),
                available,
                recovery,
                authority_entries: status.authority_entries(),
                unique_keys: status.unique_keys(),
                active_operations: status.active_operations(),
                active_unique_reservations: status.active_unique_reservations(),
                active_value_leases: status.active_value_leases(),
                pending_read_repairs: status.pending_read_repairs(),
                applied_read_repairs: status.applied_read_repairs(),
                async_lag: status.async_lag(),
                async_failures: status.async_failures(),
                poisoned_shards: status.poisoned_shards(),
                leased_shards: status.leased_shards(),
                async_paused: status.async_paused(),
                rebuild_required: status.rebuild_required(),
                summary_ready_shards: status.summary_ready_shards(),
                summary_degraded_shards: status.summary_degraded_shards(),
                summary_saturated_shards: status.summary_saturated_shards(),
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(Json(GlobalIndexesResponse {
        state: report.state().code(),
        retained_outbox_events: report.retained_outbox_events(),
        retained_outbox_bytes: report.retained_outbox_bytes(),
        backpressured_outbox_shards: report.backpressured_outbox_shards(),
        indexes,
    }))
}

fn lifecycle_status(lifecycle: GlobalIndexLifecycle) -> (&'static str, bool, &'static str) {
    match lifecycle {
        GlobalIndexLifecycle::Creating => ("creating", false, "build"),
        GlobalIndexLifecycle::Ready => ("ready", true, "none"),
        GlobalIndexLifecycle::Invalid => ("invalid", false, "rebuild"),
        GlobalIndexLifecycle::Rebuilding => ("rebuilding", false, "resume_rebuild"),
        GlobalIndexLifecycle::Dropping => ("dropping", false, "none"),
    }
}

fn prometheus_metrics(report: &GlobalIndexOperationalReport) -> String {
    let mut output = String::from(
        "# HELP briskdb_global_indexes Global indexes by operational state.\n\
         # TYPE briskdb_global_indexes gauge\n",
    );
    for state in [
        GlobalIndexHealthState::Healthy,
        GlobalIndexHealthState::Degraded,
        GlobalIndexHealthState::Unavailable,
    ] {
        let value = report
            .indexes()
            .iter()
            .filter(|index| index.state() == state)
            .count();
        let _ = writeln!(
            output,
            "briskdb_global_indexes{{state=\"{}\"}} {value}",
            state.code()
        );
    }
    let _ = writeln!(
        output,
        "briskdb_global_index_outbox_retained_events {}",
        report.retained_outbox_events()
    );
    let _ = writeln!(
        output,
        "briskdb_global_index_outbox_retained_bytes {}",
        report.retained_outbox_bytes()
    );
    let _ = writeln!(
        output,
        "briskdb_global_index_outbox_backpressured_shards {}",
        report.backpressured_outbox_shards()
    );
    for index in report.indexes() {
        write_index_metrics(&mut output, index);
    }
    output
}

fn write_index_metrics(output: &mut String, index: &GlobalIndexOperationalStatus) {
    let id = index.index_id();
    for (name, value) in [
        ("authority_entries", index.authority_entries()),
        ("unique_keys", index.unique_keys()),
        ("active_operations", index.active_operations()),
        (
            "active_unique_reservations",
            index.active_unique_reservations(),
        ),
        ("active_value_leases", index.active_value_leases()),
        ("pending_read_repairs", index.pending_read_repairs()),
        ("applied_read_repairs", index.applied_read_repairs()),
        ("async_lag", index.async_lag()),
        ("async_failures", index.async_failures()),
        ("poisoned_shards", u64::from(index.poisoned_shards())),
        ("leased_shards", u64::from(index.leased_shards())),
        (
            "summary_ready_shards",
            u64::from(index.summary_ready_shards()),
        ),
        (
            "summary_degraded_shards",
            u64::from(index.summary_degraded_shards()),
        ),
        (
            "summary_saturated_shards",
            u64::from(index.summary_saturated_shards()),
        ),
    ] {
        let _ = writeln!(
            output,
            "briskdb_global_index_{name}{{index_id=\"{id}\"}} {value}"
        );
    }
    for (name, value) in [
        ("async_paused", index.async_paused()),
        ("rebuild_required", index.rebuild_required()),
    ] {
        let _ = writeln!(
            output,
            "briskdb_global_index_{name}{{index_id=\"{id}\"}} {}",
            u8::from(value)
        );
    }
    let _ = writeln!(
        output,
        "briskdb_global_index_state{{index_id=\"{id}\",state=\"{}\"}} 1",
        index.state().code()
    );
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

fn result_set_to_query_response(shards: Vec<u16>, result: ResultSet) -> QueryResponse {
    let (shard, shards) = match shards.as_slice() {
        [shard] => (Some(*shard), None),
        _ => (None, Some(shards)),
    };
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
        shards,
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
        core::{
            Column, DataType, EngineErrorKind, EngineOptions, GlobalIndexDeclaration,
            GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType,
            GlobalIndexStorageTopology, ResultLimits, Row, ShardKeyMetadata, ShardKeyType,
            TableDeclaration,
        },
        sql::{
            MAX_PARSED_SQL_BYTES, SqlDialect, normalize_placeholders, parse, validate_common_subset,
        },
    };

    fn engine_router(database: Arc<Database>) -> Router {
        router_with_engine(Engine::from_database(database))
    }

    fn healthy_without_global_indexes(shards: u16) -> JsonValue {
        json!({
            "status": "ok",
            "shards": shards,
            "global_indexes": {
                "state": "healthy",
                "total": 0,
                "healthy": 0,
                "degraded": 0,
                "unavailable": 0,
                "async_lag": 0,
                "retained_outbox_events": 0,
                "retained_outbox_bytes": 0,
                "backpressured_outbox_shards": 0
            }
        })
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
            (StatusCode::OK, healthy_without_global_indexes(4))
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
                        {"name": "id", "data_type": "text"},
                        {"name": "name", "data_type": "text"}
                    ],
                    "rows": [["widget-1", "First widget"]]
                }),
            )
        );
    }

    #[tokio::test]
    async fn global_index_status_is_machine_readable_for_service_callers() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE events (tenant_id TEXT NOT NULL, email TEXT NOT NULL)")
            .unwrap();
        let logical = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical,
                    "events",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let table = database
            .catalog()
            .table("default", "events")
            .unwrap()
            .unwrap()
            .id();
        let index_id = database
            .create_global_index(
                GlobalIndexDeclaration::new(
                    table,
                    "events_email_lookup",
                    vec![GlobalIndexKeyPart::new(
                        GlobalIndexKeySource::column("email").unwrap(),
                        GlobalIndexKeyType::Text,
                    )],
                )
                .unwrap()
                .with_topology(GlobalIndexStorageTopology::selected_v1()),
            )
            .unwrap();
        database.build_global_index(index_id).unwrap();

        let engine = Engine::from_database(Arc::new(database));
        let application = router_with_engine(engine.clone());
        assert_eq!(
            request_json(&application, Method::GET, "/v1/admin/global-indexes", None,).await,
            (
                StatusCode::OK,
                json!({
                    "state": "healthy",
                    "retained_outbox_events": 0,
                    "retained_outbox_bytes": 0,
                    "backpressured_outbox_shards": 0,
                    "indexes": [{
                        "id": index_id.to_string(),
                        "name": "events_email_lookup",
                        "unique": false,
                        "lifecycle": "ready",
                        "health": "healthy",
                        "available": true,
                        "recovery": "none",
                        "authority_entries": 0,
                        "unique_keys": 0,
                        "active_operations": 0,
                        "active_unique_reservations": 0,
                        "active_value_leases": 0,
                        "pending_read_repairs": 0,
                        "applied_read_repairs": 0,
                        "async_lag": 0,
                        "async_failures": 0,
                        "poisoned_shards": 0,
                        "leased_shards": 0,
                        "async_paused": false,
                        "rebuild_required": false,
                        "summary_ready_shards": 4,
                        "summary_degraded_shards": 0,
                        "summary_saturated_shards": 0
                    }]
                }),
            )
        );

        let metrics = send_json(&application, Method::GET, "/metrics", None).await;
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(
            metrics.headers()[CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let body = String::from_utf8(
            to_bytes(metrics.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(body.contains("briskdb_global_indexes{state=\"healthy\"} 1"));
        assert!(body.contains(&format!(
            "briskdb_global_index_summary_ready_shards{{index_id=\"{index_id}\"}} 4"
        )));

        let session = engine.session();
        session.set_routing_key("http-lag").await.unwrap();
        engine
            .execute_write(
                &session,
                Statement::new(
                    "INSERT INTO events (tenant_id, email) VALUES (?1, ?2)",
                    vec!["http-lag".into(), "lag@example.test".into()],
                ),
            )
            .await
            .unwrap();
        let (health_status, health) =
            request_json(&application, Method::GET, "/health", None).await;
        assert_eq!(health_status, StatusCode::OK);
        assert_eq!(health["status"], "degraded");
        assert_eq!(health["global_indexes"]["state"], "degraded");
        assert_eq!(health["global_indexes"]["degraded"], 1);
        assert_eq!(health["global_indexes"]["async_lag"], 1);
        assert_eq!(health["global_indexes"]["retained_outbox_events"], 1);
        let (_, admin) =
            request_json(&application, Method::GET, "/v1/admin/global-indexes", None).await;
        assert_eq!(admin["state"], "degraded");
        assert_eq!(admin["indexes"][0]["health"], "degraded");
        assert_eq!(admin["indexes"][0]["async_lag"], 1);
    }

    #[tokio::test]
    async fn validated_catalog_http_writes_reuse_one_handle_and_keep_counters_isolated() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE widgets (
                    id TEXT NOT NULL PRIMARY KEY,
                    value INTEGER NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "widgets",
                    ShardKeyMetadata::new("id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let database = Arc::new(database);
        let routing_key = "reused-http-write";
        let expected_shard = database.shard_for_key(routing_key.as_bytes());
        // The experimental virtual-table write gate is off by default, even
        // in an all-features build. This existing pooling assertion therefore
        // also protects parity for the established physical-shard path.
        let options = EngineOptions::new(1, 64).unwrap();
        let engine = Engine::from_database_with_options(Arc::clone(&database), options).unwrap();
        let application = router_with_engine(engine.clone());

        let rejected = request_json(
            &application,
            Method::POST,
            "/v1/execute",
            Some(json!({
                "shard_key": routing_key,
                "sql": "INSERT INTO widgets (id, value) VALUES (?1, total_changes())",
                "params": [routing_key]
            })),
        )
        .await;
        assert_eq!(rejected.0, StatusCode::NOT_IMPLEMENTED);
        assert!(
            engine
                .pool_snapshot_for_test()
                .unwrap()
                .shards
                .iter()
                .all(|shard| shard.checkouts == 0 && shard.opened == 0),
            "connection-local functions must be rejected before pool checkout"
        );

        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/execute",
                Some(json!({
                    "shard_key": routing_key,
                    "sql": "INSERT INTO widgets (id, value) VALUES (?1, ?2)",
                    "params": [routing_key, 0]
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({"shard": expected_shard, "rows_affected": 1})
            )
        );
        for _ in 0..99 {
            assert_eq!(
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": routing_key,
                        "sql": "UPDATE widgets SET value = value + 1 WHERE id = ?1",
                        "params": [routing_key]
                    })),
                )
                .await,
                (
                    StatusCode::OK,
                    json!({"shard": expected_shard, "rows_affected": 1})
                )
            );
        }

        let mut concurrent = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let application = application.clone();
            concurrent.spawn(async move {
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": "reused-http-write",
                        "sql": "UPDATE widgets SET value = value + 1 WHERE id = ?1",
                        "params": ["reused-http-write"]
                    })),
                )
                .await
            });
        }
        while let Some(result) = concurrent.join_next().await {
            assert_eq!(
                result.unwrap(),
                (
                    StatusCode::OK,
                    json!({"shard": expected_shard, "rows_affected": 1})
                )
            );
        }

        let snapshot = engine.pool_snapshot_for_test().unwrap();
        let shard = snapshot.shards[usize::from(expected_shard)];
        assert_eq!(shard.opened, 1);
        assert_eq!(shard.checkouts, 132);
        assert_eq!(shard.reused, 131);
        assert_eq!(shard.retired, 0);
        assert_eq!(shard.active, 0);
        assert_eq!(shard.queued, 0);
        assert_eq!(shard.idle, 1);

        let stored = database
            .query_routed(
                routing_key,
                "SELECT value FROM widgets WHERE id = ?1",
                &[Value::from(routing_key)],
            )
            .unwrap();
        assert_eq!(stored.value.rows()[0].get(0), Some(&Value::from(131_i64)));

        let observer = engine.session();
        let counters = engine
            .inspect_shard(
                &observer,
                expected_shard,
                Statement::new(
                    "SELECT last_insert_rowid(), changes(), total_changes()",
                    vec![],
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            counters.rows()[0].values(),
            [Value::from(0_i64), Value::from(0_i64), Value::from(0_i64)]
        );
        let isolated = engine.pool_snapshot_for_test().unwrap().shards[usize::from(expected_shard)];
        assert_eq!(isolated.opened, 2);
        assert_eq!(isolated.retired, 1);
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn opted_in_vtab_http_autocommit_dml_places_each_row_on_exactly_one_shard() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                    tenant_id TEXT NOT NULL,
                    record_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, record_id)
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "records",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let tenant_keys = (0..4_u16)
            .map(|expected_shard| {
                (0_u64..)
                    .map(|candidate| format!("vtab-http-{expected_shard}-{candidate}"))
                    .find(|key| database.shard_for_key(key.as_bytes()) == expected_shard)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let database = Arc::new(database);
        let options = EngineOptions::new(2, 16)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::clone(&database), options).unwrap();
        let application = router_with_engine(engine.clone());

        for (shard, tenant_key) in tenant_keys.iter().enumerate() {
            assert_eq!(
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": tenant_key,
                        "sql": "INSERT INTO records (tenant_id, record_id, payload) VALUES (?1, ?2, ?3)",
                        "params": [tenant_key, 1, format!("inserted-{shard}")]
                    })),
                )
                .await,
                (
                    StatusCode::OK,
                    json!({"shard": shard, "rows_affected": 1})
                )
            );
        }

        // Inspect every physical file after INSERT, rather than accepting a
        // successful logical read as proof of placement. Each owner has its
        // one row and no other shard has a duplicate.
        let observer = engine.session();
        for (shard, tenant_key) in tenant_keys.iter().enumerate() {
            let physical = engine
                .inspect_shard(
                    &observer,
                    u16::try_from(shard).unwrap(),
                    Statement::new("SELECT tenant_id, record_id, payload FROM records", vec![]),
                )
                .await
                .unwrap();
            assert_eq!(physical.rows().len(), 1, "physical shard {shard}");
            assert_eq!(
                physical.rows()[0].values(),
                [
                    Value::from(tenant_key.clone()),
                    Value::from(1_i64),
                    Value::from(format!("inserted-{shard}")),
                ],
                "physical shard {shard}"
            );
        }

        for (shard, tenant_key) in tenant_keys.iter().enumerate() {
            assert_eq!(
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": tenant_key,
                        "sql": "UPDATE records SET payload = ?1 WHERE tenant_id = ?2 AND record_id = ?3",
                        "params": [format!("updated-{shard}"), tenant_key, 1]
                    })),
                )
                .await,
                (
                    StatusCode::OK,
                    json!({"shard": shard, "rows_affected": 1})
                )
            );
        }
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/execute",
                Some(json!({
                    "shard_key": tenant_keys[0],
                    "sql": "UPDATE records SET payload = 'missing' WHERE tenant_id = ?1 AND record_id = ?2",
                    "params": [tenant_keys[0], 99]
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({"shard": 0, "rows_affected": 0})
            )
        );

        for expected_rows in [1, 0] {
            assert_eq!(
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": tenant_keys[2],
                        "sql": "DELETE FROM records WHERE tenant_id = ?1 AND record_id = ?2",
                        "params": [tenant_keys[2], 1]
                    })),
                )
                .await,
                (
                    StatusCode::OK,
                    json!({"shard": 2, "rows_affected": expected_rows})
                )
            );
        }

        for (shard, tenant_key) in tenant_keys.iter().enumerate() {
            let physical = engine
                .inspect_shard(
                    &observer,
                    u16::try_from(shard).unwrap(),
                    Statement::new("SELECT tenant_id, record_id, payload FROM records", vec![]),
                )
                .await
                .unwrap();
            if shard == 2 {
                assert!(physical.is_empty(), "deleted owner shard must be empty");
            } else {
                assert_eq!(physical.rows().len(), 1, "physical shard {shard}");
                assert_eq!(
                    physical.rows()[0].values(),
                    [
                        Value::from(tenant_key.clone()),
                        Value::from(1_i64),
                        Value::from(format!("updated-{shard}")),
                    ],
                    "physical shard {shard}"
                );
            }
        }

        // Opting writes into the coordinator does not replace logical reads:
        // the existing metadata-driven scatter path still visits all shards.
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "sql": "SELECT tenant_id, record_id, payload FROM records"
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shards": [0, 1, 2, 3],
                    "columns": [
                        {"name": "tenant_id", "data_type": "text"},
                        {"name": "record_id", "data_type": "int64"},
                        {"name": "payload", "data_type": "text"}
                    ],
                    "rows": [
                        [tenant_keys[0], 1, "updated-0"],
                        [tenant_keys[1], 1, "updated-1"],
                        [tenant_keys[3], 1, "updated-3"]
                    ]
                })
            )
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn opted_in_vtab_http_rejects_transactional_and_unsupported_sql_without_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE records (
                    tenant_id TEXT NOT NULL PRIMARY KEY,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "records",
                    ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let database = Arc::new(database);
        let options = EngineOptions::new(2, 16)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::clone(&database), options).unwrap();
        let before = engine.pool_snapshot_for_test().unwrap();
        let application = router_with_engine(engine.clone());
        let mapping = http_error(EngineErrorKind::Unsupported);
        let expected = json!({
            "type": mapping.problem_type,
            "title": mapping.title,
            "status": mapping.status,
            "detail": mapping.detail,
            "code": EngineErrorKind::Unsupported.code()
        });

        let cases = [
            (
                "/v1/execute",
                json!({"sql": "BEGIN", "shard_key": "blocked-tenant"}),
            ),
            (
                "/v1/execute",
                json!({"sql": "COMMIT", "shard_key": "blocked-tenant"}),
            ),
            (
                "/v1/execute",
                json!({"sql": "ROLLBACK", "shard_key": "blocked-tenant"}),
            ),
            (
                "/v1/execute",
                json!({"sql": "SAVEPOINT private_savepoint", "shard_key": "blocked-tenant"}),
            ),
            (
                "/v1/execute",
                json!({"sql": "ATTACH DATABASE ':memory:' AS private_db", "shard_key": "blocked-tenant"}),
            ),
            (
                "/v1/execute",
                json!({
                    "sql": "INSERT INTO records (tenant_id, payload) VALUES (?1, ?2) RETURNING payload",
                    "params": ["blocked-tenant", "must-not-be-stored"],
                    "shard_key": "blocked-tenant"
                }),
            ),
            (
                "/v1/query",
                json!({
                    "sql": "INSERT INTO records (tenant_id, payload) VALUES (?1, ?2) RETURNING payload",
                    "params": ["blocked-tenant", "must-not-be-stored"],
                    "shard_key": "blocked-tenant"
                }),
            ),
        ];

        for (uri, body) in cases {
            assert_eq!(
                request_json(&application, Method::POST, uri, Some(body)).await,
                (StatusCode::NOT_IMPLEMENTED, expected.clone()),
                "unsupported request through {uri}"
            );
        }

        assert_eq!(
            engine.pool_snapshot_for_test().unwrap(),
            before,
            "unsupported SQL must fail before pool admission"
        );
        for shard in 0..database.shard_count() {
            let rows =
                rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM records", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .unwrap();
            assert_eq!(rows, 0, "unsupported SQL mutated physical shard {shard}");
        }
    }

    #[cfg(feature = "experimental-vtab")]
    #[tokio::test]
    async fn http_omitted_key_insert_returns_exact_generated_id_and_actual_owner() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 4).unwrap();
        database
            .broadcast(
                "CREATE TABLE native_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT NOT NULL
                 )",
            )
            .unwrap();
        let logical_database = database.catalog().default_database().id();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    logical_database,
                    "native_events",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap()
                .with_generated_id_policy(
                    crate::core::GeneratedIdPolicy::native_range_v1("id").unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let database = Arc::new(database);
        let options = EngineOptions::new(2, 16)
            .unwrap()
            .with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(Arc::clone(&database), options).unwrap();
        let application = router_with_engine(engine);

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/execute",
            Some(json!({
                "sql": "INSERT INTO native_events (payload) VALUES (?1)",
                "params": ["from-http"]
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rows_affected"], json!(1));
        assert_eq!(body["generated_key"]["column"], json!("id"));
        assert_eq!(body["generated_key"]["data_type"], json!("int64"));
        let encoded = body["generated_key"]["value"]
            .as_str()
            .expect("generated integer is rendered as an exact decimal string");
        let id = encoded.parse::<i64>().unwrap();
        assert_eq!(encoded, id.to_string());
        let shard = u16::try_from(body["shard"].as_u64().unwrap()).unwrap();

        for candidate in 0..database.shard_count() {
            assert_eq!(
                rusqlite::Connection::open(
                    temp.path().join(format!("shards/{candidate:04}.sqlite"))
                )
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM native_events WHERE id = ?1 AND payload = 'from-http'",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                i64::from(candidate == shard),
                "physical shard {candidate}"
            );
        }
    }

    #[tokio::test]
    async fn empty_catalog_http_writes_keep_unique_owners_and_raw_sqlite_isolation() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        database
            .broadcast("CREATE TABLE raw_items (id INTEGER PRIMARY KEY)")
            .unwrap();
        let routing_key = "raw-http-write";
        let expected_shard = database.shard_for_key(routing_key.as_bytes());
        let options = EngineOptions::new(1, 4).unwrap();
        #[cfg(feature = "experimental-vtab")]
        let options = options.with_experimental_vtab_writes(true);
        let engine = Engine::from_database_with_options(database, options).unwrap();
        let application = router_with_engine(engine.clone());

        for id in [1, 2] {
            assert_eq!(
                request_json(
                    &application,
                    Method::POST,
                    "/v1/execute",
                    Some(json!({
                        "shard_key": routing_key,
                        "sql": "INSERT INTO raw_items (id) VALUES (?1)",
                        "params": [id]
                    })),
                )
                .await,
                (
                    StatusCode::OK,
                    json!({"shard": expected_shard, "rows_affected": 1})
                )
            );
        }
        let after_writes =
            engine.pool_snapshot_for_test().unwrap().shards[usize::from(expected_shard)];
        assert_eq!(after_writes.opened, 2);
        assert_eq!(after_writes.checkouts, 2);
        assert_eq!(after_writes.reused, 0);
        assert_eq!(after_writes.retired, 1);

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": routing_key,
                "sql": "SELECT last_insert_rowid(), changes(), total_changes(), (SELECT COUNT(*) FROM raw_items)"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["rows"], json!([[0, 0, 0, 2]]));

        let after_observer =
            engine.pool_snapshot_for_test().unwrap().shards[usize::from(expected_shard)];
        assert_eq!(after_observer.opened, 3);
        assert_eq!(after_observer.checkouts, 3);
        assert_eq!(after_observer.retired, 2);
    }

    #[tokio::test]
    async fn registered_sharded_query_needs_no_shard_key_and_returns_all_shards() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (
                    tenant_key TEXT NOT NULL PRIMARY KEY,
                    payload TEXT NOT NULL
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
                .map(|candidate| format!("tenant-{expected_shard}-{candidate}"))
                .find(|candidate| database.shard_for_key(candidate.as_bytes()) == expected_shard)
                .unwrap()
        });

        for (shard, tenant_key, payload) in [
            (0_u16, tenant_keys[0].as_str(), "zero payload"),
            (1_u16, tenant_keys[1].as_str(), "one payload"),
        ] {
            let inserted = database
                .execute_routed(
                    tenant_key,
                    "INSERT INTO events (tenant_key, payload) VALUES (?1, ?2)",
                    &[Value::from(tenant_key), Value::from(payload)],
                )
                .unwrap();
            assert_eq!(inserted.shard, shard);
            assert_eq!(inserted.value, 1);
        }

        let application = engine_router(Arc::new(database));
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "sql": "SELECT tenant_key, payload FROM events"
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shards": [0, 1],
                    "columns": [
                        {"name": "tenant_key", "data_type": "text"},
                        {"name": "payload", "data_type": "text"}
                    ],
                    "rows": [
                        [tenant_keys[0], "zero payload"],
                        [tenant_keys[1], "one payload"]
                    ]
                })
            )
        );
        assert_eq!(
            request_json(
                &application,
                Method::POST,
                "/v1/query",
                Some(json!({
                    "shard_key": tenant_keys[0],
                    "sql": "SELECT tenant_key, payload FROM events"
                })),
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shards": [0, 1],
                    "columns": [
                        {"name": "tenant_key", "data_type": "text"},
                        {"name": "payload", "data_type": "text"}
                    ],
                    "rows": [
                        [tenant_keys[0], "zero payload"],
                        [tenant_keys[1], "one payload"]
                    ]
                })
            ),
            "registered logical reads must not be narrowed by a legacy shard key"
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
            (StatusCode::OK, healthy_without_global_indexes(4))
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
    fn http_parameters_use_the_shared_canonical_index_key_encoding() {
        let through_http = [
            json_to_value(json!(true)),
            json_to_value(json!(-42)),
            json_to_value(json!(9_223_372_036_854_775_809_u64)),
            json_to_value(json!(1.5)),
            json_to_value(json!("shared")),
        ];
        let direct = [
            Value::Boolean(true),
            Value::Int64(-42),
            Value::UInt64(9_223_372_036_854_775_809),
            Value::Float64(1.5),
            Value::Text("shared".to_owned()),
        ];
        assert_eq!(
            crate::core::CanonicalIndexKey::encode_values(&through_http).unwrap(),
            crate::core::CanonicalIndexKey::encode_values(&direct).unwrap()
        );
    }

    #[test]
    fn routed_requests_preserve_or_omit_the_optional_shard_key() {
        let read = serde_json::from_value::<QueryRequest>(json!({
            "sql": "SELECT payload FROM events"
        }))
        .unwrap();
        assert_eq!(read.shard_key, None);

        let generated = serde_json::from_value::<RoutedSqlRequest>(json!({
            "sql": "INSERT INTO events (payload) VALUES (?1)"
        }))
        .unwrap();
        assert_eq!(generated.shard_key, None);

        let explicit = serde_json::from_value::<RoutedSqlRequest>(json!({
            "shard_key": "tenant-42",
            "sql": "INSERT INTO events (tenant_key, payload) VALUES (?1, ?2)"
        }))
        .unwrap();
        assert_eq!(explicit.shard_key.as_deref(), Some("tenant-42"));
    }

    #[test]
    fn execute_responses_omit_absent_keys_and_encode_generated_integers_exactly() {
        assert_eq!(
            serde_json::to_value(ExecuteResponse {
                shard: 2,
                rows_affected: 1,
                generated_key: None,
            })
            .unwrap(),
            json!({"shard": 2, "rows_affected": 1})
        );

        for (value, data_type, expected) in [
            (Value::Int64(i64::MAX), "int64", i64::MAX.to_string()),
            (Value::UInt64(u64::MAX), "uint64", u64::MAX.to_string()),
        ] {
            let generated_key =
                execute_generated_key(GeneratedKey::new("event_id", value)).unwrap();
            assert_eq!(
                serde_json::to_value(ExecuteResponse {
                    shard: 3,
                    rows_affected: 1,
                    generated_key: Some(generated_key),
                })
                .unwrap(),
                json!({
                    "shard": 3,
                    "rows_affected": 1,
                    "generated_key": {
                        "column": "event_id",
                        "data_type": data_type,
                        "value": expected,
                    }
                })
            );
        }
    }

    #[test]
    fn execute_response_rejects_impossible_non_integer_generated_values() {
        for value in [
            Value::Null,
            Value::Boolean(true),
            Value::Float64(1.5),
            Value::Text("not-an-id".to_owned()),
            Value::Binary(vec![1, 2, 3]),
        ] {
            let error = execute_generated_key(GeneratedKey::new("id", value)).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::Internal);
            assert_eq!(
                error.diagnostic(),
                "the engine returned a non-integer generated key to the HTTP adapter"
            );
        }
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
            serde_json::to_value(result_set_to_query_response(vec![3], result)).unwrap(),
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
            serde_json::to_value(result_set_to_query_response(vec![1], empty)).unwrap(),
            json!({"shard": 1, "columns": [], "rows": []})
        );

        let empty_row = ResultSet::new(Vec::new(), vec![Row::new(Vec::new())]).unwrap();
        assert_eq!(
            serde_json::to_value(result_set_to_query_response(vec![2], empty_row)).unwrap(),
            json!({"shard": 2, "columns": [], "rows": [[]]})
        );
    }

    #[test]
    fn scatter_result_encoding_reports_every_visited_shard() {
        let result = ResultSet::new(
            vec![Column::new("payload", DataType::Text)],
            vec![
                Row::new(vec![Value::from("from shard zero")]),
                Row::new(vec![Value::from("from shard one")]),
            ],
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(result_set_to_query_response(vec![0, 1], result)).unwrap(),
            json!({
                "shards": [0, 1],
                "columns": [{"name": "payload", "data_type": "text"}],
                "rows": [["from shard zero"], ["from shard one"]]
            })
        );
    }
}
