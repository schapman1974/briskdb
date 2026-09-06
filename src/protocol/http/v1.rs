//! Version 1 transport contract. Storage and SQL semantics remain in Engine.

use std::fmt;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, Request},
    http::{HeaderValue, StatusCode, request::Parts},
    middleware,
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny, MapAccess, Visitor},
};
use serde_json::value::RawValue;

use super::{
    HttpState, IDEMPOTENCY_KEY_HEADER_NAME, ProblemDetails, REQUEST_ID_HEADER_NAME,
    STREAM_MEDIA_TYPE, active_queries, backup_capability, broadcast, cancel_query, catalog,
    checkpoint, execute, global_indexes, health, migration, migrations, problem_response, query,
    query_stream, ready, shard_status,
};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn routes() -> Router<HttpState> {
    versioned_routes(
        Router::new()
            .route("/", get(discovery))
            .route("/health", get(health))
            .route("/ready", get(ready))
            .route("/execute", post(execute))
            .route("/query", post(query))
            .route("/query/stream", post(query_stream))
            .route("/admin/broadcast", post(broadcast))
            .route("/admin/global-indexes", get(global_indexes))
            .route("/admin/catalog", get(catalog))
            .route("/admin/migrations", get(migrations))
            .route("/admin/migrations/{target_generation}", get(migration))
            .route("/admin/shards", get(shard_status))
            .route("/admin/queries", get(active_queries))
            .route("/admin/queries/{query_id}/cancel", post(cancel_query))
            .route("/admin/backup", get(backup_capability))
            .route("/admin/maintenance/checkpoint", post(checkpoint)),
        true,
    )
}

pub(super) fn data_routes() -> Router<HttpState> {
    versioned_routes(
        Router::new()
            .route("/", get(discovery))
            .route("/execute", post(execute))
            .route("/query", post(query))
            .route("/query/stream", post(query_stream)),
        true,
    )
}

pub(super) fn admin_routes() -> Router<HttpState> {
    versioned_routes(
        Router::new()
            .route("/health", get(health))
            .route("/ready", get(ready))
            .route("/admin/broadcast", post(broadcast))
            .route("/admin/global-indexes", get(global_indexes))
            .route("/admin/catalog", get(catalog))
            .route("/admin/migrations", get(migrations))
            .route("/admin/migrations/{target_generation}", get(migration))
            .route("/admin/shards", get(shard_status))
            .route("/admin/queries", get(active_queries))
            .route("/admin/queries/{query_id}/cancel", post(cancel_query))
            .route("/admin/backup", get(backup_capability))
            .route("/admin/maintenance/checkpoint", post(checkpoint)),
        false,
    )
}

fn versioned_routes(
    endpoints: Router<HttpState>,
    include_discovery_slash: bool,
) -> Router<HttpState> {
    let endpoints = endpoints
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(middleware::map_response(version_header));
    let router = Router::new().nest("/v1", endpoints);
    let slash = (if include_discovery_slash {
        get(discovery).fallback(method_not_allowed)
    } else {
        any(not_found)
    })
    .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
    .layer(middleware::map_response(version_header));
    router.route("/v1/", slash)
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
    value_encoding: ValueEncoding,
    supported_value_encodings: [ValueEncoding; 2],
    session_scope: &'static str,
    sql_dialect: &'static str,
    max_request_bytes: usize,
    max_result_rows: u64,
    max_result_logical_bytes: u64,
    stream_buffer_rows: usize,
    request_id_header: &'static str,
    idempotency_key_header: &'static str,
    stream_media_type: &'static str,
}

async fn discovery(
    axum::extract::State(state): axum::extract::State<HttpState>,
) -> Json<ApiVersion> {
    let result_limits = state.engine.options().result_limits();
    Json(ApiVersion {
        api_version: "1",
        value_encoding: ValueEncoding::LegacyJsonV1,
        supported_value_encodings: ValueEncoding::ALL,
        session_scope: "request",
        sql_dialect: "sqlite",
        max_request_bytes: MAX_REQUEST_BYTES,
        max_result_rows: result_limits.max_rows(),
        max_result_logical_bytes: result_limits.max_bytes(),
        stream_buffer_rows: crate::core::DEFAULT_STREAM_BUFFER_ROWS,
        request_id_header: REQUEST_ID_HEADER_NAME,
        idempotency_key_header: IDEMPOTENCY_KEY_HEADER_NAME,
        stream_media_type: STREAM_MEDIA_TYPE,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub(super) enum ValueEncoding {
    #[default]
    #[serde(rename = "legacy-json-v1")]
    LegacyJsonV1,
    #[serde(rename = "lossless-json-v1")]
    LosslessJsonV1,
}

impl ValueEncoding {
    pub(super) const ALL: [Self; 2] = [Self::LegacyJsonV1, Self::LosslessJsonV1];

    pub(super) const fn is_legacy(self) -> bool {
        matches!(self, Self::LegacyJsonV1)
    }
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(super) struct RawJsonParameter(Box<RawValue>);

impl RawJsonParameter {
    pub(super) fn get(&self) -> &str {
        self.0.get()
    }
}

/// Execute envelope; each handler creates a fresh core Session and Statement.
/// Unknown fields must fail rather than silently ignoring a client's routing,
/// transaction, dialect, value-encoding, or request-control expectations.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecuteRequest {
    #[serde(default)]
    pub(super) shard_key: Option<String>,
    pub(super) sql: String,
    #[serde(default)]
    pub(super) params: Vec<RawJsonParameter>,
    #[serde(default)]
    pub(super) value_encoding: ValueEncoding,
}

/// Query envelope with optional protocol-neutral result-limit narrowing.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryRequest {
    #[serde(default)]
    pub(super) shard_key: Option<String>,
    pub(super) sql: String,
    #[serde(default)]
    pub(super) params: Vec<RawJsonParameter>,
    #[serde(default)]
    pub(super) value_encoding: ValueEncoding,
    #[serde(default, deserialize_with = "deserialize_result_limits")]
    pub(super) result_limits: Option<QueryResultLimits>,
}

fn deserialize_result_limits<'de, D>(deserializer: D) -> Result<Option<QueryResultLimits>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    QueryResultLimits::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryResultLimits {
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    pub(super) max_rows: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_u64")]
    pub(super) max_logical_bytes: Option<u64>,
}

fn deserialize_optional_u64<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    u64::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BroadcastRequest {
    pub(super) sql: String,
}

#[derive(Debug)]
pub(super) struct EmptyRequest;

impl<'de> Deserialize<'de> for EmptyRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EmptyRequestVisitor;

        impl<'de> Visitor<'de> for EmptyRequestVisitor {
            type Value = EmptyRequest;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an empty JSON object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                if map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "the empty request must not contain fields",
                    ));
                }
                Ok(EmptyRequest)
            }
        }

        deserializer.deserialize_map(EmptyRequestVisitor)
    }
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

/// An exact empty request body under the shared v1 byte limit.
///
/// Routes without a JSON envelope still extract the body so Axum applies
/// [`DefaultBodyLimit`] before the handler can perform an operation.
pub(super) struct V1EmptyBody;

impl<S> FromRequest<S> for V1EmptyBody
where
    S: Send + Sync,
{
    type Rejection = TransportError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let body =
            Bytes::from_request(request, state)
                .await
                .map_err(|rejection| match rejection.status() {
                    StatusCode::PAYLOAD_TOO_LARGE => TransportError::BodyTooLarge,
                    _ => TransportError::InvalidRequest,
                })?;
        if body.is_empty() {
            Ok(Self)
        } else {
            Err(TransportError::InvalidRequest)
        }
    }
}

pub(super) struct V1Path<T>(pub(super) T);

impl<T, S> FromRequestParts<S> for V1Path<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = TransportError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|_| TransportError::NotFound)
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

#[cfg(test)]
mod tests {
    use super::{ExecuteRequest, QueryRequest, ValueEncoding};

    #[test]
    fn sql_request_defaults_and_explicit_legacy_selection_preserve_raw_parameters() {
        for (raw, expected_parameter) in [
            (
                r#"{"sql":"SELECT ?1","params":[{"z":0,"a":1}]}"#,
                r#"{"z":0,"a":1}"#,
            ),
            (
                r#"{"sql":"SELECT ?1","params":[ [ 1, 2 ] ],"value_encoding":"legacy-json-v1"}"#,
                "[ 1, 2 ]",
            ),
        ] {
            let request = serde_json::from_str::<ExecuteRequest>(raw).unwrap();

            assert_eq!(request.value_encoding, ValueEncoding::LegacyJsonV1);
            assert_eq!(request.params.len(), 1);
            assert_eq!(request.params[0].get(), expected_parameter);
        }
    }

    #[test]
    fn only_query_envelopes_accept_nonnull_result_limits() {
        let query = serde_json::from_str::<QueryRequest>(
            r#"{"sql":"SELECT 1","result_limits":{"max_rows":1}}"#,
        )
        .unwrap();
        assert_eq!(query.result_limits.unwrap().max_rows, Some(1));
        assert!(
            serde_json::from_str::<QueryRequest>(r#"{"sql":"SELECT 1","result_limits":null}"#)
                .is_err()
        );
        assert!(
            serde_json::from_str::<ExecuteRequest>(
                r#"{"sql":"DELETE FROM t","result_limits":{"max_rows":1}}"#
            )
            .is_err()
        );
        for raw in [
            r#"{"sql":"SELECT 1","result_limits":{"max_rows":null,"max_logical_bytes":1}}"#,
            r#"{"sql":"SELECT 1","result_limits":{"max_rows":1,"max_logical_bytes":null}}"#,
        ] {
            assert!(serde_json::from_str::<QueryRequest>(raw).is_err(), "{raw}");
        }
    }
}
