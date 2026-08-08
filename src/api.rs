use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::storage::Database;

pub fn router(database: Arc<Database>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/execute", post(execute))
        .route("/v1/query", post(query))
        .route("/v1/admin/broadcast", post(broadcast))
        .with_state(database)
}

async fn health(State(database): State<Arc<Database>>) -> Json<Value> {
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
    params: Vec<Value>,
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
    let shard = database.shard_for_key(request.shard_key.as_bytes());
    let rows_affected = tokio::task::spawn_blocking(move || {
        database.execute(&request.shard_key, &request.sql, &request.params)
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
) -> Result<Json<Value>, ApiError> {
    let shard = database.shard_for_key(request.shard_key.as_bytes());
    let rows = tokio::task::spawn_blocking(move || {
        database.query(&request.shard_key, &request.sql, &request.params)
    })
    .await??;

    Ok(Json(json!({"shard": shard, "rows": rows})))
}

async fn broadcast(
    State(database): State<Arc<Database>>,
    Json(request): Json<BroadcastRequest>,
) -> Result<Json<Value>, ApiError> {
    let shards = tokio::task::spawn_blocking(move || database.broadcast(&request.sql)).await??;
    Ok(Json(json!({"completed_shards": shards})))
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
