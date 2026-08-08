//! Experimental HTTP adapter.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue, json};

use crate::core::{Database, ResultSet, Routed, Value};

pub fn router(database: Arc<Database>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/execute", post(execute))
        .route("/v1/query", post(query))
        .route("/v1/admin/broadcast", post(broadcast))
        .with_state(database)
}

async fn health(State(database): State<Arc<Database>>) -> Json<JsonValue> {
    Json(json!({
        "status": "ok",
        "shards": database.shard_count(),
    }))
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

async fn execute(
    State(database): State<Arc<Database>>,
    Json(request): Json<RoutedSqlRequest>,
) -> Result<Json<ExecuteResponse>, ApiError> {
    let Routed {
        shard,
        value: rows_affected,
    } = tokio::task::spawn_blocking(move || {
        let params = request
            .params
            .into_iter()
            .map(json_to_value)
            .collect::<Vec<_>>();
        database.execute_routed(&request.shard_key, &request.sql, &params)
    })
    .await??;

    Ok(Json(ExecuteResponse {
        shard,
        rows_affected,
    }))
}

async fn query(
    State(database): State<Arc<Database>>,
    Json(request): Json<RoutedSqlRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let (shard, rows) = tokio::task::spawn_blocking(move || {
        let params = request
            .params
            .into_iter()
            .map(json_to_value)
            .collect::<Vec<_>>();
        let Routed {
            shard,
            value: result,
        } = database.query_routed(&request.shard_key, &request.sql, &params)?;
        Ok::<_, anyhow::Error>((shard, result_set_to_json_rows(result)))
    })
    .await??;

    Ok(Json(json!({"shard": shard, "rows": rows})))
}

async fn broadcast(
    State(database): State<Arc<Database>>,
    Json(request): Json<BroadcastRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let shards = tokio::task::spawn_blocking(move || database.broadcast(&request.sql)).await??;
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

fn result_set_to_json_rows(result: ResultSet) -> Vec<JsonValue> {
    let (columns, rows) = result.into_parts();
    rows.into_iter()
        .map(|row| {
            let mut object = Map::with_capacity(columns.len());
            for (column, value) in columns.iter().zip(row.into_values()) {
                object.insert(column.name.clone(), value_to_json(value));
            }
            JsonValue::Object(object)
        })
        .collect()
}

struct ApiError(anyhow::Error);

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": self.0.to_string()})),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::core::{Column, DataType, Row};

    async fn request_json(
        router: &Router,
        method: Method,
        uri: &str,
        body: Option<JsonValue>,
    ) -> (StatusCode, JsonValue) {
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
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn http_contract_is_preserved_across_all_endpoints() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 4).unwrap());
        let expected_shard = database.shard_for_key(b"widget-1");
        let application = router(database);

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
                    "rows": [{"id": "widget-1", "name": "First widget"}]
                }),
            )
        );
    }

    #[tokio::test]
    async fn sqlite_errors_keep_the_json_500_contract() {
        let temp = tempfile::tempdir().unwrap();
        let application = router(Arc::new(Database::open(temp.path(), 4).unwrap()));

        let (status, body) = request_json(
            &application,
            Method::POST,
            "/v1/query",
            Some(json!({
                "shard_key": "widget-1",
                "sql": "SELECT * FROM missing_table"
            })),
        )
        .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body["error"].as_str().unwrap().contains("no such table"));
    }

    #[tokio::test]
    async fn unsigned_parameters_outside_sqlite_range_fail_instead_of_rounding() {
        let temp = tempfile::tempdir().unwrap();
        let application = router(Arc::new(Database::open(temp.path(), 4).unwrap()));
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

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body["error"],
            format!("unsigned integer {too_large} exceeds SQLite INTEGER range")
        );
    }

    #[tokio::test]
    async fn non_finite_sqlite_reals_keep_the_legacy_json_null_encoding() {
        let temp = tempfile::tempdir().unwrap();
        let application = router(Arc::new(Database::open(temp.path(), 4).unwrap()));

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
            body["rows"],
            json!([{"positive_infinity": null, "negative_infinity": null}])
        );
    }

    #[tokio::test]
    async fn typed_core_keeps_legacy_http_parameter_and_blob_shapes() {
        let temp = tempfile::tempdir().unwrap();
        let application = router(Arc::new(Database::open(temp.path(), 4).unwrap()));

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
            body["rows"],
            json!([{
                "enabled": 1,
                "object_text": "{\"nested\":true}",
                "array_text": "[1,\"two\"]",
                "data": [0, 255]
            }])
        );
    }

    #[test]
    fn legacy_api_module_reexports_the_router() {
        let _legacy_router: fn(Arc<Database>) -> Router = crate::api::router;
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
    fn legacy_result_encoding_keeps_json_shapes_and_duplicate_overwrite() {
        let result = ResultSet::new(
            vec![
                Column::new("duplicate", DataType::Unknown),
                Column::new("duplicate", DataType::Unknown),
                Column::new("blob", DataType::Unknown),
                Column::new("flag", DataType::Unknown),
            ],
            vec![Row::new(vec![
                Value::from(1_i64),
                Value::from("last value wins"),
                Value::from(vec![0_u8, 255]),
                Value::from(true),
            ])],
        )
        .unwrap();

        assert_eq!(
            result_set_to_json_rows(result),
            vec![json!({
                "duplicate": "last value wins",
                "blob": [0, 255],
                "flag": true
            })]
        );
    }
}
