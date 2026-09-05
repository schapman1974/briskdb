//! Version 1 transport contract. Storage and SQL semantics remain in Engine.

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, Request},
    http::{HeaderValue, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;

use super::{
    HttpState, ProblemDetails, broadcast, execute, global_indexes, health, problem_response, query,
};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn routes() -> Router<HttpState> {
    let endpoints = Router::new()
        .route("/", get(discovery))
        .route("/health", get(health))
        .route("/execute", post(execute))
        .route("/query", post(query))
        .route("/admin/broadcast", post(broadcast))
        .route("/admin/global-indexes", get(global_indexes))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed);
    Router::new()
        .route("/v1/", get(discovery))
        .method_not_allowed_fallback(method_not_allowed)
        .nest("/v1", endpoints)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(middleware::map_response(version_header))
}

async fn version_header(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("briskdb-api-version", HeaderValue::from_static("1"));
    response
}

#[derive(Serialize)]
struct ApiVersion {
    api_version: &'static str,
    value_encoding: &'static str,
    session_scope: &'static str,
    sql_dialect: &'static str,
    max_request_bytes: usize,
}

async fn discovery() -> Json<ApiVersion> {
    Json(ApiVersion {
        api_version: "1",
        value_encoding: "legacy-json-v1",
        session_scope: "request",
        sql_dialect: "sqlite",
        max_request_bytes: MAX_REQUEST_BYTES,
    })
}

/// Shared SQL envelope; each handler creates a fresh core Session and Statement.
/// Unknown fields must fail rather than silently ignoring a client's routing,
/// transaction, dialect, or value-encoding expectations.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SqlRequest {
    #[serde(default)]
    pub(super) shard_key: Option<String>,
    pub(super) sql: String,
    #[serde(default)]
    pub(super) params: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BroadcastRequest {
    pub(super) sql: String,
}

pub(super) struct V1Json<T>(pub(super) T);

impl<T, S> FromRequest<S> for V1Json<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = TransportError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|rejection| match rejection.status() {
                StatusCode::PAYLOAD_TOO_LARGE => TransportError::BodyTooLarge,
                StatusCode::UNSUPPORTED_MEDIA_TYPE => TransportError::MediaType,
                _ => TransportError::InvalidRequest,
            })
    }
}

pub(super) enum TransportError {
    InvalidRequest,
    BodyTooLarge,
    MediaType,
    NotFound,
    MethodNotAllowed,
}

impl IntoResponse for TransportError {
    fn into_response(self) -> Response {
        let (status, code, title, detail) = match self {
            Self::InvalidRequest => (
                400,
                "invalid_argument",
                "Invalid argument",
                "The request contains an invalid argument.",
            ),
            Self::BodyTooLarge => (
                413,
                "request_too_large",
                "Request too large",
                "The request body exceeds the HTTP API limit.",
            ),
            Self::MediaType => (
                415,
                "unsupported_media_type",
                "Unsupported media type",
                "The request requires a JSON content type.",
            ),
            Self::NotFound => (
                404,
                "not_found",
                "Not found",
                "The requested API endpoint does not exist.",
            ),
            Self::MethodNotAllowed => (
                405,
                "method_not_allowed",
                "Method not allowed",
                "The method is not supported by this API endpoint.",
            ),
        };
        let problem_type = match self {
            Self::InvalidRequest => {
                super::http_error(crate::core::EngineErrorKind::InvalidArgument).problem_type
            }
            Self::BodyTooLarge => "urn:briskdb:http:v1:request-too-large",
            Self::MediaType => "urn:briskdb:http:v1:unsupported-media-type",
            Self::NotFound => "urn:briskdb:http:v1:not-found",
            Self::MethodNotAllowed => "urn:briskdb:http:v1:method-not-allowed",
        };
        problem_response(ProblemDetails {
            problem_type,
            title,
            status,
            detail,
            code,
        })
    }
}

async fn not_found() -> TransportError {
    TransportError::NotFound
}

async fn method_not_allowed() -> TransportError {
    TransportError::MethodNotAllowed
}
