//! Versioned HTTP adapter over the protocol-neutral engine.

mod admin;
mod v1;

use std::{fmt::Write as _, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as STANDARD_BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use v1::{
    BroadcastRequest, RawJsonParameter, SqlRequest as QueryRequest, SqlRequest as RoutedSqlRequest,
    V1Json, ValueEncoding,
};

use crate::{
    core::{
        DataType, Database, Decimal, Engine, EngineError, EngineErrorKind, Executed, GeneratedKey,
        GlobalIndexHealthState, GlobalIndexLifecycle, GlobalIndexOperationalReport,
        GlobalIndexOperationalStatus, ResultSet, Routed, Statement, Value,
    },
    protocol::error::http_error,
};

/// Build an HTTP router from the legacy synchronous database handle.
///
/// New callers should construct one shared [`Engine`] and use
/// [`router_with_engine`]. This wrapper preserves the pre-engine Rust API while
/// still sending every request through the shared asynchronous engine. It also
/// preserves the original combined data/admin router; network servers should
/// use [`data_router_with_engine`] and [`admin_router_with_engine`] instead.
pub fn router(database: Arc<Database>) -> Router {
    router_with_engine(Engine::from_database(database))
}

/// Build the original combined data/admin HTTP router.
///
/// This compatibility constructor is useful to in-process Tower hosts. The
/// BriskDB server binds the two planes separately.
pub fn router_with_engine(engine: Engine) -> Router {
    let state = HttpState::new(engine);
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .merge(v1::routes())
        .merge(admin::routes(state.clone()))
        .with_state(state)
}

/// Build the data-plane router from the legacy synchronous database handle.
pub fn data_router(database: Arc<Database>) -> Router {
    data_router_with_engine(Engine::from_database(database))
}

/// Build the data-plane router backed by the protocol-neutral engine.
///
/// This plane contains only HTTP v1 discovery and SQL query/execute routes.
pub fn data_router_with_engine(engine: Engine) -> Router {
    Router::new()
        .merge(v1::data_routes())
        .with_state(HttpState::new(engine))
}

/// Build the admin-plane router from the legacy synchronous database handle.
pub fn admin_router(database: Arc<Database>) -> Router {
    admin_router_with_engine(Engine::from_database(database))
}

/// Build the admin-plane router backed by the protocol-neutral engine.
///
/// This plane contains operational health and metrics, the versioned
/// administration routes, and the embedded browser.
pub fn admin_router_with_engine(engine: Engine) -> Router {
    let state = HttpState::new(engine);
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .merge(v1::admin_routes())
        .merge(admin::routes(state.clone()))
        .with_state(state)
}

#[derive(Clone)]
struct HttpState {
    engine: Engine,
    admin_sessions: admin::SessionStore,
}

impl HttpState {
    fn new(engine: Engine) -> Self {
        Self {
            engine,
            admin_sessions: admin::SessionStore::new(),
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    value_encoding: Option<ValueEncoding>,
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
    V1Json(request): V1Json<RoutedSqlRequest>,
) -> Result<Json<ExecuteResponse>, ApiError> {
    let engine = state.engine;
    let value_encoding = request.value_encoding;
    let params = request
        .params
        .into_iter()
        .map(|value| parameter_to_value(value, value_encoding))
        .collect::<Result<Vec<_>, _>>()?;
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
    V1Json(request): V1Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let engine = state.engine;
    let value_encoding = request.value_encoding;
    let params = request
        .params
        .into_iter()
        .map(|value| parameter_to_value(value, value_encoding))
        .collect::<Result<Vec<_>, _>>()?;
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
    let response = result_set_to_query_response(shards, result, value_encoding);

    Ok(Json(response))
}

async fn broadcast(
    State(state): State<HttpState>,
    V1Json(request): V1Json<BroadcastRequest>,
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

fn parameter_to_value(
    value: RawJsonParameter,
    encoding: ValueEncoding,
) -> Result<Value, EngineError> {
    match encoding {
        ValueEncoding::LegacyJsonV1 => legacy_json_to_value(value.get()),
        ValueEncoding::LosslessJsonV1 => lossless_json_to_value(value.get()),
    }
}

#[derive(Deserialize)]
struct LegacyParameterEnvelope {
    params: [JsonValue; 1],
}

fn legacy_json_to_value(raw: &str) -> Result<Value, EngineError> {
    // RawValue is required so lossless tags retain duplicate members for strict
    // rejection. Reparse each legacy value beneath the same root-object and
    // params-array nesting as the original Vec<JsonValue> envelope so opting
    // into RawValue does not relax serde_json's established recursion limit.
    let mut envelope = String::with_capacity(raw.len().saturating_add(13));
    envelope.push_str("{\"params\":[");
    envelope.push_str(raw);
    envelope.push_str("]}");
    let LegacyParameterEnvelope { params: [value] } =
        serde_json::from_str(&envelope).map_err(|error| {
            EngineError::from_source(
                EngineErrorKind::InvalidArgument,
                "an HTTP JSON parameter could not be decoded",
                error,
            )
        })?;
    json_to_value(value)
}

fn json_to_value(value: JsonValue) -> Result<Value, EngineError> {
    validate_json_numbers(&value)?;
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(value),
        JsonValue::Number(value) => value
            .as_i64()
            .map(Value::Int64)
            .or_else(|| value.as_u64().map(Value::UInt64))
            .or_else(|| value.as_f64().map(Value::Float64))
            .expect("validated JSON numbers fit the HTTP v1 numeric contract"),
        JsonValue::String(value) => Value::Text(value),
        JsonValue::Array(value) => Value::Text(legacy_json_text(JsonValue::Array(value))),
        JsonValue::Object(value) => Value::Text(legacy_json_text(JsonValue::Object(value))),
    })
}

fn legacy_json_text(value: JsonValue) -> String {
    fn sort_object_keys(value: JsonValue) -> JsonValue {
        match value {
            JsonValue::Array(values) => {
                JsonValue::Array(values.into_iter().map(sort_object_keys).collect())
            }
            JsonValue::Object(values) => {
                let mut entries = values.into_iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                JsonValue::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key, sort_object_keys(value)))
                        .collect(),
                )
            }
            value => value,
        }
    }

    sort_object_keys(value).to_string()
}

fn validate_json_numbers(value: &JsonValue) -> Result<(), EngineError> {
    match value {
        JsonValue::Number(number) => {
            let rendered = number.to_string();
            let valid = if rendered
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, b'.' | b'e' | b'E'))
            {
                number.as_f64().is_some_and(f64::is_finite)
            } else {
                number.as_i64().is_some() || number.as_u64().is_some()
            };
            if !valid {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    "an HTTP JSON parameter contains a number outside the v1 numeric range",
                ));
            }
            Ok(())
        }
        JsonValue::Array(values) => values.iter().try_for_each(validate_json_numbers),
        JsonValue::Object(values) => values.values().try_for_each(validate_json_numbers),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::String(_) => Ok(()),
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LosslessTaggedValue {
    #[serde(rename = "$briskdb_type")]
    kind: LosslessValueKind,
    value: String,
}

#[derive(Debug, Deserialize)]
enum LosslessValueKind {
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint64")]
    UInt64,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "decimal")]
    Decimal,
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "invalid_text")]
    InvalidText,
}

fn lossless_json_to_value(raw: &str) -> Result<Value, EngineError> {
    let value = serde_json::from_str::<JsonValue>(raw).map_err(|_| invalid_lossless_value())?;
    match value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(value) => Ok(Value::Boolean(value)),
        JsonValue::String(value) => Ok(Value::Text(value)),
        JsonValue::Object(_) => {
            // Deserialize the raw object again so serde observes repeated fields
            // instead of accepting serde_json::Map's last-value-wins projection.
            let tagged = serde_json::from_str::<LosslessTaggedValue>(raw)
                .map_err(|_| invalid_lossless_value())?;
            decode_lossless_tag(tagged)
        }
        JsonValue::Number(_) | JsonValue::Array(_) => Err(invalid_lossless_value()),
    }
}

fn decode_lossless_tag(tagged: LosslessTaggedValue) -> Result<Value, EngineError> {
    let LosslessTaggedValue { kind, value } = tagged;
    match kind {
        LosslessValueKind::Int64 => value
            .parse::<i64>()
            .ok()
            .filter(|parsed| parsed.to_string() == value)
            .map(Value::Int64)
            .ok_or_else(invalid_lossless_value),
        LosslessValueKind::UInt64 => value
            .parse::<u64>()
            .ok()
            .filter(|parsed| parsed.to_string() == value)
            .map(Value::UInt64)
            .ok_or_else(invalid_lossless_value),
        LosslessValueKind::Float64 => decode_float64_bits(&value).map(Value::Float64),
        LosslessValueKind::Decimal => Decimal::parse(value)
            .map(Value::Decimal)
            .map_err(|_| invalid_lossless_value()),
        LosslessValueKind::Binary => decode_canonical_base64(&value).map(Value::Binary),
        LosslessValueKind::InvalidText => decode_canonical_base64(&value).map(Value::InvalidText),
    }
}

fn decode_float64_bits(value: &str) -> Result<f64, EngineError> {
    if value.len() != 16
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(invalid_lossless_value());
    }
    u64::from_str_radix(value, 16)
        .map(f64::from_bits)
        .map_err(|_| invalid_lossless_value())
}

fn decode_canonical_base64(value: &str) -> Result<Vec<u8>, EngineError> {
    let decoded = STANDARD_BASE64
        .decode(value)
        .map_err(|_| invalid_lossless_value())?;
    if STANDARD_BASE64.encode(&decoded) != value {
        return Err(invalid_lossless_value());
    }
    Ok(decoded)
}

fn invalid_lossless_value() -> EngineError {
    EngineError::new(
        EngineErrorKind::InvalidArgument,
        "an HTTP JSON parameter does not match lossless-json-v1",
    )
}

fn lossless_value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(value) => JsonValue::Bool(value),
        Value::Text(value) => JsonValue::String(value),
        Value::Int64(value) => tagged_lossless_value("int64", value.to_string()),
        Value::UInt64(value) => tagged_lossless_value("uint64", value.to_string()),
        Value::Float64(value) => {
            tagged_lossless_value("float64", format!("{:016x}", value.to_bits()))
        }
        Value::Decimal(value) => tagged_lossless_value("decimal", value.into_string()),
        Value::Binary(value) => tagged_lossless_value("binary", STANDARD_BASE64.encode(value)),
        Value::InvalidText(value) => {
            tagged_lossless_value("invalid_text", STANDARD_BASE64.encode(value))
        }
    }
}

fn tagged_lossless_value(kind: &'static str, value: String) -> JsonValue {
    json!({
        "$briskdb_type": kind,
        "value": value,
    })
}

fn value_to_json_with_encoding(value: Value, encoding: ValueEncoding) -> JsonValue {
    match encoding {
        ValueEncoding::LegacyJsonV1 => value_to_json(value),
        ValueEncoding::LosslessJsonV1 => lossless_value_to_json(value),
    }
}

fn result_set_to_query_response(
    shards: Vec<u16>,
    result: ResultSet,
    value_encoding: ValueEncoding,
) -> QueryResponse {
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
        .map(|row| {
            row.into_values()
                .into_iter()
                .map(|value| value_to_json_with_encoding(value, value_encoding))
                .collect()
        })
        .collect();

    QueryResponse {
        shard,
        shards,
        value_encoding: (!value_encoding.is_legacy()).then_some(value_encoding),
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
        problem_response(ProblemDetails {
            problem_type: mapping.problem_type,
            title: mapping.title,
            status: mapping.status,
            detail: mapping.detail,
            code: self.0.code(),
        })
    }
}

fn problem_response(problem: ProblemDetails) -> Response {
    let status = StatusCode::from_u16(problem.status)
        .expect("the HTTP contract contains valid status codes");
    let mut response = (status, Json(problem)).into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    response
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
        let response = router
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        if uri.starts_with("/v1/") {
            assert_eq!(response.headers()["briskdb-api-version"], "1");
        }
        response
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

    async fn request_raw_json(router: &Router, uri: &str, body: &str) -> (StatusCode, JsonValue) {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["briskdb-api-version"], "1");
        response_json(response).await
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
    async fn raw_numbers_outside_json_v1_range_are_rejected_at_every_depth() {
        let temp = tempfile::tempdir().unwrap();
        let application = engine_router(Arc::new(Database::open(temp.path(), 4).unwrap()));
        let expected = (
            StatusCode::BAD_REQUEST,
            json!({
                "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-argument",
                "title": "Invalid argument",
                "status": 400,
                "detail": "The request contains an invalid argument.",
                "code": "invalid_argument"
            }),
        );

        for body in [
            r#"{"shard_key":"numeric-boundary","sql":"SELECT ?1","params":[18446744073709551616]}"#,
            r#"{"shard_key":"numeric-boundary","sql":"SELECT ?1","params":[[18446744073709551616]]}"#,
            r#"{"shard_key":"numeric-boundary","sql":"SELECT ?1","params":[{"nested":-9223372036854775809}]}"#,
            r#"{"shard_key":"numeric-boundary","sql":"SELECT ?1","params":[1e400]}"#,
            r#"{"shard_key":"numeric-boundary","sql":"SELECT ?1","params":[{"nested":[1e400]}]}"#,
        ] {
            assert_eq!(
                request_raw_json(&application, "/v1/query", body).await,
                expected
            );
        }
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
        let _data_router: fn(Arc<Database>) -> Router = crate::api::data_router;
        let _data_engine_router: fn(Engine) -> Router = crate::api::data_router_with_engine;
        let _admin_router: fn(Arc<Database>) -> Router = crate::api::admin_router;
        let _admin_engine_router: fn(Engine) -> Router = crate::api::admin_router_with_engine;
    }

    #[test]
    fn production_http_adapter_has_no_blocking_or_shard_routing_escape_hatches() {
        let test_module_marker = ["#[cfg", "(test)]\nmod tests {"].concat();
        let (production_source, _) = include_str!("http.rs")
            .split_once(&test_module_marker)
            .expect("the HTTP unit-test module has a cfg(test) boundary");
        let production_source = [production_source, include_str!("http/v1.rs")].concat();

        assert_eq!(
            production_source.matches("Database").count(),
            4,
            "Database may appear only in the compatibility import and three router signatures"
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
        assert_eq!(json_to_value(JsonValue::Null).unwrap(), Value::Null);
        assert_eq!(json_to_value(json!(true)).unwrap(), Value::from(true));
        assert_eq!(json_to_value(json!(42)).unwrap(), Value::from(42_i64));
        assert_eq!(json_to_value(json!(1.5)).unwrap(), Value::from(1.5_f64));
        assert_eq!(json_to_value(json!("text")).unwrap(), Value::from("text"));
        assert_eq!(
            json_to_value(json!([1, "two"])).unwrap(),
            Value::from("[1,\"two\"]")
        );
        assert_eq!(
            json_to_value(json!({"nested": true})).unwrap(),
            Value::from("{\"nested\":true}")
        );
        let insertion_ordered: JsonValue =
            serde_json::from_str(r#"{"z":0,"a":{"z":1,"a":2},"m":[{"z":3,"a":4}]}"#).unwrap();
        assert_eq!(
            json_to_value(insertion_ordered).unwrap(),
            Value::from(r#"{"a":{"a":2,"z":1},"m":[{"a":4,"z":3}],"z":0}"#)
        );

        let above_signed_i64_range = json!(9_223_372_036_854_775_809_u64);
        assert_eq!(
            json_to_value(above_signed_i64_range).unwrap(),
            Value::from(9_223_372_036_854_775_809_u64)
        );
    }

    #[test]
    fn raw_parameter_capture_preserves_the_legacy_envelope_recursion_limit() {
        #[derive(Deserialize)]
        struct PreviousSqlRequest {
            #[serde(rename = "sql")]
            _sql: String,
            params: Vec<JsonValue>,
        }

        for depth in 120..=132 {
            let nested = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
            let body = format!(r#"{{"sql":"SELECT ?1","params":[{nested}]}}"#);
            let previous_accepts = serde_json::from_str::<PreviousSqlRequest>(&body)
                .is_ok_and(|request| request.params.len() == 1);
            let current_accepts = serde_json::from_str::<RoutedSqlRequest>(&body)
                .ok()
                .and_then(|mut request| request.params.pop())
                .is_some_and(|parameter| {
                    parameter_to_value(parameter, ValueEncoding::LegacyJsonV1).is_ok()
                });

            assert_eq!(
                current_accepts, previous_accepts,
                "legacy recursion-depth behavior changed at parameter depth {depth}"
            );
        }
    }

    #[test]
    fn http_parameters_use_the_shared_canonical_index_key_encoding() {
        let through_http = [
            json_to_value(json!(true)).unwrap(),
            json_to_value(json!(-42)).unwrap(),
            json_to_value(json!(9_223_372_036_854_775_809_u64)).unwrap(),
            json_to_value(json!(1.5)).unwrap(),
            json_to_value(json!("shared")).unwrap(),
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

    fn decode_lossless_parameter(raw: &str) -> Result<Value, EngineError> {
        let parameter = serde_json::from_str::<RawJsonParameter>(raw).unwrap();
        parameter_to_value(parameter, ValueEncoding::LosslessJsonV1)
    }

    #[test]
    fn lossless_json_round_trips_every_protocol_neutral_value_representation() {
        for value in [
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Int64(i64::MIN),
            Value::Int64(i64::MAX),
            Value::UInt64(u64::MAX),
            Value::decimal("+0012.3400E-02").unwrap(),
            Value::Text("plain text".to_owned()),
            Value::Text("{\"$briskdb_type\":\"binary\"}".to_owned()),
            Value::InvalidText(vec![b'f', 0x80, 0xff]),
            Value::Binary(vec![]),
            Value::Binary(vec![0, 0xff]),
            Value::Binary((0..=u8::MAX).collect()),
        ] {
            let encoded = lossless_value_to_json(value.clone()).to_string();
            let decoded = decode_lossless_parameter(&encoded)
                .unwrap_or_else(|error| panic!("failed to decode {encoded}: {error:?}"));
            assert_eq!(decoded, value);
        }

        for bits in [
            0_u64,
            (-0.0_f64).to_bits(),
            1.5_f64.to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            0x7ff8_0000_0000_0042,
        ] {
            let encoded = lossless_value_to_json(Value::Float64(f64::from_bits(bits))).to_string();
            let Value::Float64(decoded) = decode_lossless_parameter(&encoded).unwrap() else {
                panic!("float64 tag decoded as another value type");
            };
            assert_eq!(decoded.to_bits(), bits);
        }
    }

    #[test]
    fn lossless_json_has_stable_exact_tag_shapes() {
        for (value, expected) in [
            (
                Value::Int64(i64::MIN),
                json!({"$briskdb_type": "int64", "value": "-9223372036854775808"}),
            ),
            (
                Value::UInt64(u64::MAX),
                json!({"$briskdb_type": "uint64", "value": "18446744073709551615"}),
            ),
            (
                Value::Float64(-0.0),
                json!({"$briskdb_type": "float64", "value": "8000000000000000"}),
            ),
            (
                Value::decimal("12.3400").unwrap(),
                json!({"$briskdb_type": "decimal", "value": "12.3400"}),
            ),
            (
                Value::Binary(vec![0, 255]),
                json!({"$briskdb_type": "binary", "value": "AP8="}),
            ),
            (
                Value::InvalidText(vec![b'f', 0x80]),
                json!({"$briskdb_type": "invalid_text", "value": "ZoA="}),
            ),
        ] {
            assert_eq!(lossless_value_to_json(value), expected);
        }
    }

    #[test]
    fn lossless_json_rejects_ambiguous_or_noncanonical_parameters() {
        for raw in [
            "0",
            "1.5",
            "[]",
            "{}",
            r#"{"value":"0","$briskdb_type":"int64","extra":true}"#,
            r#"{"$briskdb_type":"int64"}"#,
            r#"{"value":"0"}"#,
            r#"{"$briskdb_type":"unknown","value":"0"}"#,
            r#"{"$briskdb_type":1,"value":"0"}"#,
            r#"{"$briskdb_type":"int64","value":0}"#,
            r#"{"$briskdb_type":"int64","$briskdb_type":"int64","value":"0"}"#,
            r#"{"$briskdb_type":"int64","value":"0","value":"0"}"#,
            r#"{"$briskdb_type":"int64","value":"00"}"#,
            r#"{"$briskdb_type":"int64","value":"-0"}"#,
            r#"{"$briskdb_type":"int64","value":"9223372036854775808"}"#,
            r#"{"$briskdb_type":"uint64","value":"+1"}"#,
            r#"{"$briskdb_type":"uint64","value":"18446744073709551616"}"#,
            r#"{"$briskdb_type":"decimal","value":"NaN"}"#,
            r#"{"$briskdb_type":"float64","value":"000000000000000"}"#,
            r#"{"$briskdb_type":"float64","value":"3FF0000000000000"}"#,
            r#"{"$briskdb_type":"float64","value":"xxxxxxxxxxxxxxxx"}"#,
            r#"{"$briskdb_type":"binary","value":"AA"}"#,
            r#"{"$briskdb_type":"binary","value":"AB=="}"#,
            r#"{"$briskdb_type":"invalid_text","value":"!"}"#,
        ] {
            let error = decode_lossless_parameter(raw).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument, "{raw}");
            assert_eq!(
                error.diagnostic(),
                "an HTTP JSON parameter does not match lossless-json-v1"
            );
        }
    }

    #[test]
    fn lossless_query_response_echoes_encoding_and_keeps_ordered_rows() {
        let result = ResultSet::new(
            vec![
                Column::new("duplicate", DataType::Int64),
                Column::new("duplicate", DataType::Binary),
            ],
            vec![Row::new(vec![
                Value::Int64(9_007_199_254_740_993),
                Value::Binary(vec![0, 255]),
            ])],
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(result_set_to_query_response(
                vec![3],
                result,
                ValueEncoding::LosslessJsonV1,
            ))
            .unwrap(),
            json!({
                "shard": 3,
                "value_encoding": "lossless-json-v1",
                "columns": [
                    {"name": "duplicate", "data_type": "int64"},
                    {"name": "duplicate", "data_type": "binary"}
                ],
                "rows": [[
                    {"$briskdb_type": "int64", "value": "9007199254740993"},
                    {"$briskdb_type": "binary", "value": "AP8="}
                ]]
            })
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
            serde_json::to_value(result_set_to_query_response(
                vec![3],
                result,
                ValueEncoding::LegacyJsonV1,
            ))
            .unwrap(),
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
            serde_json::to_value(result_set_to_query_response(
                vec![1],
                empty,
                ValueEncoding::LegacyJsonV1,
            ))
            .unwrap(),
            json!({"shard": 1, "columns": [], "rows": []})
        );

        let empty_row = ResultSet::new(Vec::new(), vec![Row::new(Vec::new())]).unwrap();
        assert_eq!(
            serde_json::to_value(result_set_to_query_response(
                vec![2],
                empty_row,
                ValueEncoding::LegacyJsonV1,
            ))
            .unwrap(),
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
            serde_json::to_value(result_set_to_query_response(
                vec![0, 1],
                result,
                ValueEncoding::LegacyJsonV1,
            ))
            .unwrap(),
            json!({
                "shards": [0, 1],
                "columns": [{"name": "payload", "data_type": "text"}],
                "rows": [["from shard zero"], ["from shard one"]]
            })
        );
    }
}
