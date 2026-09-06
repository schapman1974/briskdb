//! Version 1 transport contract. Storage and SQL semantics remain in Engine.

use std::fmt;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, Request},
    http::{HeaderValue, StatusCode, request::Parts},
    middleware,
    response::{IntoResponse, Response},
    routing::{any, get},
};
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny, MapAccess, Visitor},
};
use serde_json::value::RawValue;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use super::{
    HttpState, IDEMPOTENCY_KEY_HEADER_NAME, ProblemDetails, REQUEST_ID_HEADER_NAME,
    STREAM_MEDIA_TYPE, problem_response,
};

pub(super) const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn routes() -> Router<HttpState> {
    let endpoints: Router<HttpState> = annotated_data_routes()
        .merge(annotated_admin_routes())
        .into();
    versioned_routes(endpoints, true)
}

pub(super) fn data_routes() -> Router<HttpState> {
    versioned_routes(annotated_data_routes().into(), true)
}

pub(super) fn admin_routes() -> Router<HttpState> {
    versioned_routes(annotated_admin_routes().into(), false)
}

pub(super) fn openapi() -> utoipa::openapi::OpenApi {
    annotated_data_routes()
        .merge(annotated_admin_routes())
        .into_openapi()
}

fn annotated_data_routes() -> OpenApiRouter<HttpState> {
    OpenApiRouter::new()
        .routes(routes!(discovery))
        .routes(routes!(super::execute))
        .routes(routes!(super::query))
        .routes(routes!(super::query_stream))
}

fn annotated_admin_routes() -> OpenApiRouter<HttpState> {
    OpenApiRouter::new()
        .routes(routes!(super::health))
        .routes(routes!(super::ready))
        .routes(routes!(super::broadcast))
        .routes(routes!(super::global_indexes))
        .routes(routes!(super::catalog))
        .routes(routes!(super::migrations))
        .routes(routes!(super::migration))
        .routes(routes!(super::shard_status))
        .routes(routes!(super::active_queries))
        .routes(routes!(super::cancel_query))
        .routes(routes!(super::backup_capability))
        .routes(routes!(super::checkpoint))
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

#[derive(Serialize, ToSchema)]
pub(super) struct ApiVersion {
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

#[utoipa::path(
    get,
    path = "/",
    operation_id = "getApiVersion",
    tag = "data",
    responses((status = 200, description = "HTTP API version", body = ApiVersion))
)]
pub(super) async fn discovery(
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
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
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ExecuteRequest {
    #[serde(default)]
    pub(super) shard_key: Option<String>,
    pub(super) sql: String,
    #[serde(default)]
    #[schema(value_type = Vec<serde_json::Value>)]
    pub(super) params: Vec<RawJsonParameter>,
    #[serde(default)]
    pub(super) value_encoding: ValueEncoding,
}

/// Query envelope with optional protocol-neutral result-limit narrowing.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryRequest {
    #[serde(default)]
    pub(super) shard_key: Option<String>,
    pub(super) sql: String,
    #[serde(default)]
    #[schema(value_type = Vec<serde_json::Value>)]
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

#[derive(Debug, Deserialize, ToSchema)]
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

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct BroadcastRequest {
    pub(super) sql: String,
}

#[derive(Debug, ToSchema)]
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

#[derive(Clone, Copy)]
pub(super) enum TransportError {
    InvalidRequest,
    BodyTooLarge,
    MediaType,
    NotFound,
    MethodNotAllowed,
}

#[derive(Clone, Copy)]
pub(super) struct TransportErrorMapping {
    pub(super) status: u16,
    pub(super) code: &'static str,
    pub(super) problem_type: &'static str,
    pub(super) title: &'static str,
    pub(super) detail: &'static str,
}

impl TransportError {
    pub(super) const ALL: [Self; 5] = [
        Self::InvalidRequest,
        Self::BodyTooLarge,
        Self::MediaType,
        Self::NotFound,
        Self::MethodNotAllowed,
    ];

    pub(super) const fn mapping(self) -> TransportErrorMapping {
        match self {
            Self::InvalidRequest => {
                let mapping = crate::protocol::error::http_error(
                    crate::core::EngineErrorKind::InvalidArgument,
                );
                TransportErrorMapping {
                    status: mapping.status,
                    code: "invalid_argument",
                    problem_type: mapping.problem_type,
                    title: mapping.title,
                    detail: mapping.detail,
                }
            }
            Self::BodyTooLarge => TransportErrorMapping {
                status: 413,
                code: "request_too_large",
                problem_type: "urn:briskdb:http:v1:request-too-large",
                title: "Request too large",
                detail: "The request body exceeds the HTTP API limit.",
            },
            Self::MediaType => TransportErrorMapping {
                status: 415,
                code: "unsupported_media_type",
                problem_type: "urn:briskdb:http:v1:unsupported-media-type",
                title: "Unsupported media type",
                detail: "The request requires a JSON content type.",
            },
            Self::NotFound => TransportErrorMapping {
                status: 404,
                code: "not_found",
                problem_type: "urn:briskdb:http:v1:not-found",
                title: "Not found",
                detail: "The requested API endpoint does not exist.",
            },
            Self::MethodNotAllowed => TransportErrorMapping {
                status: 405,
                code: "method_not_allowed",
                problem_type: "urn:briskdb:http:v1:method-not-allowed",
                title: "Method not allowed",
                detail: "The method is not supported by this API endpoint.",
            },
        }
    }
}

impl IntoResponse for TransportError {
    fn into_response(self) -> Response {
        let mapping = self.mapping();
        problem_response(ProblemDetails {
            problem_type: mapping.problem_type,
            title: mapping.title,
            status: mapping.status,
            detail: mapping.detail,
            code: mapping.code,
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
