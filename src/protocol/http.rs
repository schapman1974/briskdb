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
use serde_json::{Value, json};

use crate::core::{Database, Routed};

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
    let Routed {
        shard,
        value: rows_affected,
    } = tokio::task::spawn_blocking(move || {
        database.execute_routed(&request.shard_key, &request.sql, &request.params)
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
    let Routed { shard, value: rows } = tokio::task::spawn_blocking(move || {
        database.query_routed(&request.shard_key, &request.sql, &request.params)
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

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use tower::ServiceExt;

    use super::*;

    async fn request_json(
        router: &Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
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

    #[test]
    fn legacy_api_module_reexports_the_router() {
        let _legacy_router: fn(Arc<Database>) -> Router = crate::api::router;
    }
}
