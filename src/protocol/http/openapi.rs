//! Deterministic OpenAPI 3.1 finalization for the annotated v1 router.

use serde_json::{Map, Value, json};

use crate::{
    core::{
        DEFAULT_LOGICAL_DATABASE_ID, DEFAULT_LOGICAL_DATABASE_NAME, DEFAULT_STREAM_BUFFER_ROWS,
        EngineErrorKind, MAX_ACTIVE_QUERIES, MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD,
        MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD, MAX_GLOBAL_INDEXES, MAX_LOGICAL_DATABASES,
        MAX_RESULT_BYTES, MAX_RESULT_ROWS, MAX_TABLES,
    },
    protocol::error::http_error,
};

use super::{
    STREAM_MEDIA_TYPE,
    v1::{MAX_REQUEST_BYTES, TransportError},
};

const DATA_SERVER: &str = "http://127.0.0.1:7654";
const ADMIN_SERVER: &str = "http://127.0.0.1:7655";
const MIN_PHYSICAL_SHARDS: u64 = 2;
const MAX_PHYSICAL_SHARDS: u64 = 64;
const MAX_PHYSICAL_SHARD_ID: u64 = MAX_PHYSICAL_SHARDS - 1;
const MAX_SCHEMA_MIGRATION_SQL_BYTES: u64 = 65_536;
const MAX_RETAINED_OUTBOX_EVENTS: u64 =
    MAX_GLOBAL_INDEX_OUTBOX_EVENTS_PER_SHARD * MAX_PHYSICAL_SHARDS;
const MAX_RETAINED_OUTBOX_BYTES: u64 =
    MAX_GLOBAL_INDEX_OUTBOX_BYTES_PER_SHARD * MAX_PHYSICAL_SHARDS;
const CHECKPOINT_OPERATION_ID: &str = concat!("checkpoint", "Data", "bases");
const U64_DECIMAL_PATTERN: &str = "^(?:0|[1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|18446744073709550[0-9]{3}|18446744073709551[0-5][0-9]{2}|1844674407370955160[0-9]|1844674407370955161[0-4]|18446744073709551615)$";
const SCHEMA_GENERATION_PATTERN: &str = "^(?:0|[1-9][0-9]{0,8}|1[0-9]{9}|20[0-9]{8}|21[0-3][0-9]{7}|214[0-6][0-9]{6}|2147[0-3][0-9]{5}|21474[0-7][0-9]{4}|214748[0-2][0-9]{3}|2147483[0-5][0-9]{2}|21474836[0-3][0-9]|214748364[0-7])$";
const CANONICAL_BASE64_PATTERN: &str =
    r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/][AQgw]==|[A-Za-z0-9+/]{2}[AEIMQUYcgkosw048]=)?$";
// The round-to-nearest overflow midpoint `(2^54 - 1) * 2^970`. Decimal or
// exponent request tokens strictly inside these bounds convert to a finite
// binary64 value; tokens at either bound convert to infinity.
const F64_ROUNDING_OVERFLOW_MIDPOINT: &str = "179769313486231580793728971405303415079934132710037826936173778980444968292764750946649017977587207096330286416692887910946555547851940402630657488671505820681908902000708383676273854845817711531764475730270069855571366959622842914819860834936475292719074168444365510704342711559699508093042880177904174497792";

const ROUTES: &[&str] = &[
    "/",
    "/execute",
    "/query",
    "/query/stream",
    "/health",
    "/ready",
    "/admin/broadcast",
    "/admin/global-indexes",
    "/admin/catalog",
    "/admin/migrations",
    "/admin/migrations/{target_generation}",
    "/admin/shards",
    "/admin/queries",
    "/admin/queries/{operation_id}/cancel",
    "/admin/backup",
    "/admin/maintenance/checkpoint",
];

const ENGINE_ERROR_STATUSES: &[u16] = &[403, 409, 422, 500, 501, 503, 504, 507];

pub(super) fn document() -> Value {
    let generated = serde_json::to_value(super::v1::openapi())
        .expect("the annotated HTTP v1 OpenAPI model is serializable");
    let mut generated_paths = generated
        .get("paths")
        .and_then(Value::as_object)
        .cloned()
        .expect("the annotated HTTP v1 router has paths");

    let mut paths = Map::new();
    for relative_path in ROUTES {
        let mut item = generated_paths
            .remove(*relative_path)
            .unwrap_or_else(|| panic!("annotated HTTP v1 route {relative_path} is missing"));
        let operations = item
            .as_object_mut()
            .expect("an OpenAPI path item is an object");
        for method in ["get", "post"] {
            if let Some(operation) = operations.get_mut(method) {
                finalize_operation(relative_path, operation);
            }
        }
        if let Some(get) = operations.get("get").cloned() {
            operations.insert("head".to_owned(), head_operation(get));
        }

        let full_path = if *relative_path == "/" {
            "/v1".to_owned()
        } else {
            format!("/v1{relative_path}")
        };
        paths.insert(full_path, item);
    }
    assert!(
        generated_paths.is_empty(),
        "an annotated HTTP v1 route is missing from the OpenAPI scope: {:?}",
        generated_paths.keys().collect::<Vec<_>>()
    );

    let mut discovery_alias = paths
        .get("/v1")
        .cloned()
        .expect("the annotated discovery route exists");
    let alias = discovery_alias
        .as_object_mut()
        .expect("the discovery path item is an object");
    alias["get"]["operationId"] = json!("getApiVersionAlias");
    alias["get"]["summary"] = json!("Discover HTTP API version through the slash alias");
    alias["head"]["operationId"] = json!("headApiVersionAlias");
    alias["head"]["summary"] = json!("Inspect discovery headers through the slash alias");
    paths.insert("/v1/".to_owned(), discovery_alias);

    let mut schemas = generated
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    finalize_schemas(&mut schemas);

    json!({
        "openapi": "3.1.0",
        "jsonSchemaDialect": "https://json-schema.org/draft/2020-12/schema",
        "info": {
            "title": "BriskDB HTTP API",
            "version": "1",
            "description": "Versioned BriskDB data and administration machine API. The API version is independent of the Cargo package and storage-format versions.",
            "license": {"name": "MIT"}
        },
        "tags": [
            {"name": "data", "description": "SQL query and write operations on the data listener."},
            {"name": "administration", "description": "Operational inspection and maintenance on the administration listener."}
        ],
        "paths": Value::Object(paths),
        "components": {
            "schemas": Value::Object(schemas),
            "parameters": component_parameters(),
            "headers": component_headers(),
            "responses": component_responses()
        },
        "x-briskdb-engine-errors": engine_error_mappings(),
        "x-briskdb-transport-errors": transport_error_mappings(),
        "x-briskdb-wire-contract": {
            "maxRequestBytes": MAX_REQUEST_BYTES,
            "requestObjectsRejectUnknownAndDuplicateMembers": true,
            "requestControlHeadersRequireOneCanonicalValue": true,
            "openapiCannotValidateDuplicateJsonMembersOrRawHeaderMultiplicity": true,
            "idempotencyStatusHeaderIsConditional": true
        }
    })
}

fn finalize_operation(relative_path: &str, operation: &mut Value) {
    let object = operation
        .as_object_mut()
        .expect("an OpenAPI operation is an object");
    let operation_id = object
        .get("operationId")
        .and_then(Value::as_str)
        .expect("every annotated HTTP v1 operation has a stable ID")
        .to_owned();
    let listener = listener(relative_path);
    let (url, description) = match listener {
        "data" => (DATA_SERVER, "Data listener (default address)"),
        "administration" => (ADMIN_SERVER, "Administration listener (default address)"),
        _ => unreachable!(),
    };
    object.insert(
        "summary".to_owned(),
        json!(operation_summary(&operation_id)),
    );
    object.insert("tags".to_owned(), json!([listener]));
    object.insert(
        "servers".to_owned(),
        json!([{"url": url, "description": description}]),
    );
    object.insert("x-briskdb-plane".to_owned(), json!(listener));

    let mut parameters = vec![ref_value("#/components/parameters/BriskDBRequestId")];
    if let Some(existing) = object
        .remove("parameters")
        .and_then(|value| value.as_array().cloned())
    {
        parameters.extend(existing);
    }
    if operation_id == "executeStatement" {
        parameters.push(ref_value("#/components/parameters/BriskDBIdempotencyKey"));
    }
    for parameter in &mut parameters {
        let Some(parameter) = parameter.as_object_mut() else {
            continue;
        };
        match parameter.get("name").and_then(Value::as_str) {
            Some("target_generation") => {
                parameter.insert("required".to_owned(), json!(true));
                parameter.insert("schema".to_owned(), canonical_positive_u64_string());
            }
            Some("operation_id") => {
                parameter.insert("required".to_owned(), json!(true));
                parameter.insert(
                    "schema".to_owned(),
                    ref_value("#/components/schemas/OpaqueId"),
                );
            }
            _ => {}
        }
    }
    object.insert("parameters".to_owned(), Value::Array(parameters));

    if matches!(
        operation_id.as_str(),
        "executeStatement"
            | "queryRows"
            | "streamQueryRows"
            | "broadcastMigration"
            | CHECKPOINT_OPERATION_ID
    ) {
        let schema = match operation_id.as_str() {
            "executeStatement" => "ExecuteRequest",
            "queryRows" | "streamQueryRows" => "QueryRequest",
            "broadcastMigration" => "BroadcastRequest",
            CHECKPOINT_OPERATION_ID => "EmptyRequest",
            _ => unreachable!(),
        };
        object.insert(
            "requestBody".to_owned(),
            json!({
                "required": true,
                "content": {
                    "application/json": {
                        "schema": ref_value(&format!("#/components/schemas/{schema}"))
                    }
                }
            }),
        );
        object.insert(
            "x-briskdb-max-request-bytes".to_owned(),
            json!(MAX_REQUEST_BYTES),
        );
    } else if operation_id == "cancelQuery" {
        object.remove("requestBody");
        object.insert(
            "x-briskdb-request-body".to_owned(),
            json!({"mode": "exactly-zero-bytes", "maxBytes": MAX_REQUEST_BYTES}),
        );
    } else {
        object.remove("requestBody");
    }

    object.insert("responses".to_owned(), operation_responses(&operation_id));
}

fn head_operation(mut get: Value) -> Value {
    let object = get
        .as_object_mut()
        .expect("a generated GET operation is an object");
    let get_id = object["operationId"]
        .as_str()
        .expect("a generated GET operation has an ID")
        .to_owned();
    object.insert("operationId".to_owned(), json!(head_operation_id(&get_id)));
    object.insert(
        "summary".to_owned(),
        json!(format!(
            "Inspect headers for {}",
            operation_summary(&get_id).to_lowercase()
        )),
    );
    object.insert("x-briskdb-implicit-head".to_owned(), json!(true));
    object.remove("requestBody");
    if let Some(responses) = object.get_mut("responses").and_then(Value::as_object_mut) {
        for response in responses.values_mut() {
            if response.get("$ref").is_some() {
                let status = referenced_problem_status(response)
                    .expect("a referenced GET response is a reusable Problem response");
                *response = problem_response(status);
            }
            if let Some(response) = response.as_object_mut() {
                let content_type =
                    response
                        .get("content")
                        .and_then(Value::as_object)
                        .map(|content| {
                            assert_eq!(
                                content.len(),
                                1,
                                "a GET response has exactly one representation media type"
                            );
                            content
                                .keys()
                                .next()
                                .expect("a nonempty response content object")
                                .to_owned()
                        });
                response.remove("content");
                let headers = response
                    .entry("headers")
                    .or_insert_with(|| Value::Object(Map::new()))
                    .as_object_mut()
                    .expect("response headers are an object");
                headers.insert(
                    "Content-Length".to_owned(),
                    ref_value("#/components/headers/ContentLength"),
                );
                if let Some(content_type) = content_type {
                    headers.insert(
                        "Content-Type".to_owned(),
                        json!({
                            "description": "Media type of the corresponding GET representation.",
                            "required": true,
                            "schema": {"type": "string", "const": content_type}
                        }),
                    );
                }
            }
        }
    }
    get
}

fn listener(path: &str) -> &'static str {
    if matches!(path, "/" | "/execute" | "/query" | "/query/stream") {
        "data"
    } else {
        "administration"
    }
}

fn operation_summary(operation_id: &str) -> &'static str {
    match operation_id {
        "getApiVersion" => "Discover HTTP API version",
        "executeStatement" => "Execute one routed write",
        "queryRows" => "Query materialized rows",
        "streamQueryRows" => "Stream bounded query rows",
        "getHealth" => "Inspect engine and global-index health",
        "getReadiness" => "Inspect request-admission readiness",
        "broadcastMigration" => "Apply a journaled schema migration",
        "getGlobalIndexes" => "Inspect global-index operation",
        "getCatalog" => "Inspect the relational catalog",
        "listMigrations" => "Inspect migration summary",
        "getMigration" => "Inspect one migration generation",
        "listShards" => "Inspect validated physical shards",
        "listActiveQueries" => "List active HTTP queries",
        "cancelQuery" => "Cancel one active HTTP query",
        "getBackupCapability" => "Inspect backup capability",
        CHECKPOINT_OPERATION_ID => "Checkpoint every database",
        _ => panic!("unknown annotated HTTP v1 operation ID: {operation_id}"),
    }
}

fn head_operation_id(get_id: &str) -> &'static str {
    match get_id {
        "getApiVersion" => "headApiVersion",
        "getHealth" => "headHealth",
        "getReadiness" => "headReadiness",
        "getGlobalIndexes" => "headGlobalIndexes",
        "getCatalog" => "headCatalog",
        "listMigrations" => "headMigrations",
        "getMigration" => "headMigration",
        "listShards" => "headShards",
        "listActiveQueries" => "headActiveQueries",
        "getBackupCapability" => "headBackupCapability",
        _ => panic!("POST operation unexpectedly acquired HEAD: {get_id}"),
    }
}

fn operation_responses(operation_id: &str) -> Value {
    let (success_status, success_description, success_schema) = match operation_id {
        "getApiVersion" => (200, "HTTP API version", "ApiVersion"),
        "executeStatement" => (200, "Routed write result", "ExecuteResponse"),
        "queryRows" => (200, "Materialized query result", "QueryResponse"),
        "getHealth" => (200, "Engine and global-index health", "HealthResponse"),
        "getReadiness" => (200, "Ready for ordinary requests", "ReadyResponse"),
        "broadcastMigration" => (200, "Completed migration shards", "BroadcastResponse"),
        "getGlobalIndexes" => (
            200,
            "Global-index operational report",
            "GlobalIndexesResponse",
        ),
        "getCatalog" => (200, "Relational catalog", "CatalogResponse"),
        "listMigrations" => (200, "Migration summary", "MigrationsResponse"),
        "getMigration" => (200, "Migration generation", "MigrationResponse"),
        "listShards" => (200, "Validated physical shards", "ShardStatusResponse"),
        "listActiveQueries" => (200, "Active HTTP queries", "ActiveQueriesResponse"),
        "cancelQuery" => (202, "Cancellation requested", "CancelQueryResponse"),
        "getBackupCapability" => (200, "Backup capability", "BackupCapabilityResponse"),
        CHECKPOINT_OPERATION_ID => (200, "Passive checkpoint report", "CheckpointResponse"),
        "streamQueryRows" => {
            let mut responses = Map::new();
            responses.insert("200".to_owned(), stream_response());
            add_operation_errors(operation_id, &mut responses);
            return Value::Object(responses);
        }
        _ => panic!("unknown annotated HTTP v1 operation ID: {operation_id}"),
    };
    let mut responses = Map::new();
    responses.insert(
        success_status.to_string(),
        json_response(success_description, success_schema, operation_id),
    );
    if operation_id == "getReadiness" {
        responses.insert(
            "503".to_owned(),
            json_response(
                "Not ready for ordinary requests",
                "NotReadyResponse",
                operation_id,
            ),
        );
    }
    add_operation_errors(operation_id, &mut responses);
    Value::Object(responses)
}

fn add_operation_errors(operation_id: &str, responses: &mut Map<String, Value>) {
    responses
        .entry("400".to_owned())
        .or_insert_with(|| problem_response_ref(400));
    responses
        .entry("501".to_owned())
        .or_insert_with(|| problem_response_ref(501));

    if matches!(
        operation_id,
        "executeStatement"
            | "queryRows"
            | "streamQueryRows"
            | "broadcastMigration"
            | CHECKPOINT_OPERATION_ID
    ) {
        responses.insert("413".to_owned(), problem_response_ref(413));
        responses.insert("415".to_owned(), problem_response_ref(415));
    } else if operation_id == "cancelQuery" {
        responses.insert("413".to_owned(), problem_response_ref(413));
    }

    if matches!(operation_id, "getMigration" | "cancelQuery") {
        responses.insert("404".to_owned(), problem_response_ref(404));
    }

    if matches!(
        operation_id,
        "executeStatement"
            | "queryRows"
            | "streamQueryRows"
            | "getHealth"
            | "broadcastMigration"
            | "getGlobalIndexes"
            | "listMigrations"
            | "getMigration"
            | "listShards"
            | CHECKPOINT_OPERATION_ID
    ) {
        for status in ENGINE_ERROR_STATUSES {
            responses.insert(status.to_string(), problem_response_ref(*status));
        }
    }
}

fn problem_response_ref(status: u16) -> Value {
    ref_value(&format!("#/components/responses/Problem{status}"))
}

fn referenced_problem_status(response: &Value) -> Option<u16> {
    response
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.strip_prefix("#/components/responses/Problem"))
        .and_then(|status| status.parse().ok())
}

fn json_response(description: &str, schema: &str, operation_id: &str) -> Value {
    let mut headers = common_response_headers();
    if operation_id == "executeStatement" {
        headers.insert(
            "BriskDB-Idempotency-Status".to_owned(),
            ref_value("#/components/headers/BriskDBIdempotencyStatus"),
        );
    }
    json!({
        "description": description,
        "headers": Value::Object(headers),
        "content": {
            "application/json": {
                "schema": ref_value(&format!("#/components/schemas/{schema}"))
            }
        }
    })
}

fn stream_response() -> Value {
    let mut content = Map::new();
    content.insert(
        STREAM_MEDIA_TYPE.to_owned(),
        json!({
            "schema": {
                "type": "string",
                "description": "A byte sequence of compact JSON records, each terminated by LF. It is not one JSON instance. Every row uses the value encoding declared by the preceding metadata record."
            },
            "x-briskdb-ndjson": {
                "framing": "lf-delimited-json",
                "delimiter": "LF",
                "compact": true,
                "sequence": [
                    {
                        "schema": "#/components/schemas/QueryStreamMeta",
                        "minimum": 1,
                        "maximum": 1
                    },
                    {
                        "schema": "#/components/schemas/QueryStreamRow",
                        "minimum": 0,
                        "maximum": MAX_RESULT_ROWS
                    },
                    {
                        "oneOf": [
                            "#/components/schemas/QueryStreamComplete",
                            "#/components/schemas/QueryStreamError"
                        ],
                        "minimum": 1,
                        "maximum": 1
                    }
                ],
                "terminalRecordRequired": true,
                "eofBeforeTerminal": "indeterminate",
                "lateErrorsRetainHttpStatus": 200
            }
        }),
    );
    json!({
        "description": "Bounded NDJSON query stream",
        "headers": Value::Object(common_response_headers()),
        "content": Value::Object(content)
    })
}

fn problem_response(status: u16) -> Value {
    let mut headers = common_response_headers();
    if status == 405 {
        headers.insert("Allow".to_owned(), ref_value("#/components/headers/Allow"));
    }
    json!({
        "description": problem_description(status),
        "headers": Value::Object(headers),
        "content": {
            "application/problem+json": {
                "schema": ref_value(&format!("#/components/schemas/ProblemDetails{status}"))
            }
        }
    })
}

fn common_response_headers() -> Map<String, Value> {
    Map::from_iter([
        (
            "BriskDB-API-Version".to_owned(),
            ref_value("#/components/headers/BriskDBApiVersion"),
        ),
        (
            "BriskDB-Request-ID".to_owned(),
            ref_value("#/components/headers/BriskDBRequestId"),
        ),
    ])
}

fn problem_description(status: u16) -> &'static str {
    match status {
        400 => "Invalid request or argument",
        403 => "Permission denied or read-only storage",
        404 => "Resource not found",
        405 => "Method not allowed",
        409 => "State or constraint conflict",
        413 => "Request body too large",
        415 => "Unsupported request media type",
        422 => "Unprocessable value, query, type, or limit",
        500 => "Cancelled request, corruption, or internal error",
        501 => "Unsupported operation or globally rejected request control",
        503 => "Storage, memory, contention, or shutdown unavailable",
        504 => "Request deadline exceeded",
        507 => "Storage full",
        _ => panic!("undocumented HTTP problem status: {status}"),
    }
}

fn component_parameters() -> Value {
    json!({
        "BriskDBRequestId": {
            "name": "BriskDB-Request-ID",
            "in": "header",
            "required": false,
            "description": "Optional caller-supplied correlation ID. It must occur exactly once and use the canonical nonzero 128-bit lowercase hexadecimal form.",
            "schema": ref_value("#/components/schemas/OpaqueId")
        },
        "BriskDBIdempotencyKey": {
            "name": "BriskDB-Idempotency-Key",
            "in": "header",
            "required": false,
            "description": "Optional durable idempotency key for an eligible exact-target direct autocommit DML request.",
            "schema": ref_value("#/components/schemas/OpaqueId")
        }
    })
}

fn component_headers() -> Value {
    json!({
        "BriskDBApiVersion": {
            "description": "Version of the HTTP representation contract.",
            "required": true,
            "schema": {"type": "string", "const": "1"}
        },
        "BriskDBRequestId": {
            "description": "Canonical request correlation ID supplied or generated for this attempt.",
            "required": true,
            "schema": ref_value("#/components/schemas/OpaqueId")
        },
        "BriskDBIdempotencyStatus": {
            "description": "Present only for a successfully created or replayed durable idempotency receipt.",
            "required": false,
            "schema": {"type": "string", "enum": ["created", "replayed"]}
        },
        "ContentLength": {
            "description": "The size of the corresponding GET representation; a HEAD response carries no body.",
            "required": true,
            "schema": {"type": "integer", "minimum": 0, "maximum": u64::MAX}
        },
        "Allow": {
            "description": "Methods accepted by the matched HTTP v1 route family.",
            "required": true,
            "schema": {"type": "string", "enum": ["GET,HEAD", "POST"]}
        }
    })
}

fn component_responses() -> Value {
    let mut responses = Map::new();
    for status in [
        400_u16, 403, 404, 405, 409, 413, 415, 422, 500, 501, 503, 504, 507,
    ] {
        responses.insert(format!("Problem{status}"), problem_response(status));
    }
    Value::Object(responses)
}

fn engine_error_mappings() -> Value {
    Value::Array(
        EngineErrorKind::ALL
            .iter()
            .copied()
            .map(|kind| {
                let mapping = http_error(kind);
                json!({
                    "code": kind.code(),
                    "status": mapping.status,
                    "type": mapping.problem_type,
                    "title": mapping.title,
                    "detail": mapping.detail
                })
            })
            .collect(),
    )
}

fn transport_error_mappings() -> Value {
    Value::Array(
        TransportError::ALL
            .iter()
            .copied()
            .map(|error| {
                let mapping = error.mapping();
                json!({
                    "code": mapping.code,
                    "status": mapping.status,
                    "type": mapping.problem_type,
                    "title": mapping.title,
                    "detail": mapping.detail
                })
            })
            .collect(),
    )
}

fn ref_value(reference: &str) -> Value {
    json!({"$ref": reference})
}

fn finalize_schemas(schemas: &mut Map<String, Value>) {
    schemas.remove("Value");
    for schema in schemas.values_mut() {
        if schema.get("type").and_then(Value::as_str) == Some("object") {
            schema["additionalProperties"] = json!(false);
        }
    }

    let additions = [
        ("OpaqueId", opaque_id_schema()),
        (
            "LegacyJsonValue",
            described_ref(
                "#/components/schemas/LegacyJsonParameter",
                "The legacy parameter codec accepts recursively nested JSON and binds arrays and objects as compact JSON text.",
            ),
        ),
        ("LegacyJsonParameter", legacy_json_parameter_schema()),
        (
            "LegacyJsonParameterNumber",
            legacy_json_parameter_number_schema(),
        ),
        ("LegacyJsonNumber", legacy_json_number_schema()),
        ("LegacyJsonCell", legacy_json_cell_schema()),
        ("LosslessJsonValue", lossless_value_schema()),
        ("LosslessInt64", lossless_int64_schema()),
        ("LosslessUInt64", lossless_uint64_schema()),
        (
            "LosslessFloat64",
            tagged_value_schema("float64", "^[0-9a-f]{16}$"),
        ),
        (
            "LosslessDecimal",
            tagged_value_schema(
                "decimal",
                r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$",
            ),
        ),
        (
            "LosslessBinary",
            tagged_value_schema("binary", CANONICAL_BASE64_PATTERN),
        ),
        (
            "LosslessInvalidText",
            tagged_value_schema("invalid_text", CANONICAL_BASE64_PATTERN),
        ),
        ("LegacyExecuteRequest", sql_request_schema(false, false)),
        ("LosslessExecuteRequest", sql_request_schema(true, false)),
        ("LegacyQueryRequest", sql_request_schema(false, true)),
        ("LosslessQueryRequest", sql_request_schema(true, true)),
        (
            "LegacySingleShardQueryResponse",
            query_response_schema(false, true),
        ),
        (
            "LegacyMultiShardQueryResponse",
            query_response_schema(false, false),
        ),
        (
            "LosslessSingleShardQueryResponse",
            query_response_schema(true, true),
        ),
        (
            "LosslessMultiShardQueryResponse",
            query_response_schema(true, false),
        ),
        (
            "LegacySingleShardStreamMeta",
            stream_meta_schema(false, true),
        ),
        (
            "LegacyMultiShardStreamMeta",
            stream_meta_schema(false, false),
        ),
        (
            "LosslessSingleShardStreamMeta",
            stream_meta_schema(true, true),
        ),
        (
            "LosslessMultiShardStreamMeta",
            stream_meta_schema(true, false),
        ),
    ];
    for (name, schema) in additions {
        schemas.insert(name.to_owned(), schema);
    }

    schemas.insert(
        "ValueEncoding".to_owned(),
        json!({"type": "string", "enum": ["legacy-json-v1", "lossless-json-v1"], "default": "legacy-json-v1"}),
    );
    schemas.insert(
        "ExecuteRequest".to_owned(),
        refs_one_of(&["LegacyExecuteRequest", "LosslessExecuteRequest"]),
    );
    schemas.insert(
        "QueryRequest".to_owned(),
        refs_one_of(&["LegacyQueryRequest", "LosslessQueryRequest"]),
    );
    schemas.insert("QueryResultLimits".to_owned(), query_result_limits_schema());
    schemas.insert(
        "BroadcastRequest".to_owned(),
        strict_object(&["sql"], json!({"sql": {"type": "string"}})),
    );
    schemas.insert(
        "EmptyRequest".to_owned(),
        json!({"type": "object", "maxProperties": 0, "additionalProperties": false}),
    );
    schemas.insert("ApiVersion".to_owned(), api_version_schema());
    schemas.insert("HealthResponse".to_owned(), health_schema());
    schemas.insert(
        "HealthGlobalIndexesResponse".to_owned(),
        health_global_indexes_schema(),
    );
    schemas.insert("ReadinessResponse".to_owned(), readiness_schema());
    schemas.insert("ReadyResponse".to_owned(), ready_schema(true));
    schemas.insert("NotReadyResponse".to_owned(), ready_schema(false));
    schemas.insert("QueryColumn".to_owned(), query_column_schema());
    schemas.insert(
        "QueryResponse".to_owned(),
        refs_one_of(&[
            "LegacySingleShardQueryResponse",
            "LegacyMultiShardQueryResponse",
            "LosslessSingleShardQueryResponse",
            "LosslessMultiShardQueryResponse",
        ]),
    );
    schemas.insert(
        "QueryStreamMeta".to_owned(),
        refs_one_of(&[
            "LegacySingleShardStreamMeta",
            "LegacyMultiShardStreamMeta",
            "LosslessSingleShardStreamMeta",
            "LosslessMultiShardStreamMeta",
        ]),
    );
    schemas.insert("QueryStreamRow".to_owned(), stream_row_schema());
    schemas.insert("QueryStreamComplete".to_owned(), stream_complete_schema());
    schemas.insert("QueryStreamError".to_owned(), stream_error_schema());
    schemas.insert(
        "QueryStreamRecord".to_owned(),
        refs_one_of(&[
            "QueryStreamMeta",
            "QueryStreamRow",
            "QueryStreamComplete",
            "QueryStreamError",
        ]),
    );

    finalize_operational_schemas(schemas);
    finalize_problem_schemas(schemas);
}

fn opaque_id_schema() -> Value {
    json!({
        "allOf": [
            {"type": "string", "pattern": "^[0-9a-f]{32}$"},
            {"not": {"const": "00000000000000000000000000000000"}}
        ],
        "examples": ["0123456789abcdef0123456789abcdef"]
    })
}

fn legacy_json_parameter_schema() -> Value {
    json!({
        "description": "One legacy-json-v1 request parameter. Integer tokens fit i64 or u64; fractional or exponent tokens must convert to finite binary64. JSON Schema validation cannot retain the original number token spelling, so its numeric schema accepts every value whose conversion rounds to a finite binary64 value.",
        "oneOf": [
            {"type": "null"},
            {"type": "boolean"},
            ref_value("#/components/schemas/LegacyJsonParameterNumber"),
            {"type": "string"},
            {"type": "array", "items": ref_value("#/components/schemas/LegacyJsonParameter")},
            {"type": "object", "additionalProperties": ref_value("#/components/schemas/LegacyJsonParameter")}
        ],
        "x-briskdb-number-token-limit": "JSON Schema sees numeric value, while the decoder also distinguishes integer tokens from fractional or exponent tokens."
    })
}

fn legacy_json_parameter_number_schema() -> Value {
    let negative_overflow_midpoint = format!("-{F64_ROUNDING_OVERFLOW_MIDPOINT}");
    json!({
        "description": "A legacy request JSON number whose conversion rounds to a finite binary64 value. The decoder additionally restricts bare integer tokens to i64 or u64.",
        "type": "number",
        "exclusiveMinimum": exact_json_number(&negative_overflow_midpoint),
        "exclusiveMaximum": exact_json_number(F64_ROUNDING_OVERFLOW_MIDPOINT)
    })
}

fn legacy_json_number_schema() -> Value {
    json!({
        "description": "A legacy result JSON number within the finite binary64 range.",
        "type": "number",
        "minimum": -f64::MAX,
        "maximum": f64::MAX
    })
}

fn exact_json_number(number: &str) -> Value {
    serde_json::from_str(number).expect("a fixed OpenAPI numeric bound is valid JSON")
}

fn lossless_value_schema() -> Value {
    json!({
        "description": "One canonical lossless-json-v1 BriskDB value.",
        "oneOf": [
            {"type": "null"},
            {"type": "boolean"},
            {"type": "string"},
            ref_value("#/components/schemas/LosslessInt64"),
            ref_value("#/components/schemas/LosslessUInt64"),
            ref_value("#/components/schemas/LosslessFloat64"),
            ref_value("#/components/schemas/LosslessDecimal"),
            ref_value("#/components/schemas/LosslessBinary"),
            ref_value("#/components/schemas/LosslessInvalidText")
        ]
    })
}

fn tagged_value_schema(kind: &str, pattern: &str) -> Value {
    strict_object(
        &["$briskdb_type", "value"],
        json!({
            "$briskdb_type": {"type": "string", "const": kind},
            "value": {"type": "string", "pattern": pattern}
        }),
    )
}

fn lossless_int64_schema() -> Value {
    strict_object(
        &["$briskdb_type", "value"],
        json!({
            "$briskdb_type": {"type": "string", "const": "int64"},
            "value": {
                "type": "string",
                "oneOf": [
                    {"pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6]|9223372036854775807)$"},
                    {"pattern": "^-(?:[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-7]|9223372036854775808)$"}
                ]
            }
        }),
    )
}

fn lossless_uint64_schema() -> Value {
    tagged_value_schema("uint64", U64_DECIMAL_PATTERN)
}

fn sql_request_schema(lossless: bool, query: bool) -> Value {
    let mut properties = Map::from_iter([
        ("shard_key".to_owned(), json!({"type": ["string", "null"]})),
        ("sql".to_owned(), json!({"type": "string"})),
        (
            "params".to_owned(),
            json!({
                "type": "array",
                "items": ref_value(if lossless {
                    "#/components/schemas/LosslessJsonValue"
                } else {
                    "#/components/schemas/LegacyJsonParameter"
                }),
                "default": []
            }),
        ),
        (
            "value_encoding".to_owned(),
            if lossless {
                json!({"type": "string", "const": "lossless-json-v1"})
            } else {
                json!({
                    "type": "string",
                    "const": "legacy-json-v1",
                    "default": "legacy-json-v1"
                })
            },
        ),
    ]);
    if query {
        properties.insert(
            "result_limits".to_owned(),
            ref_value("#/components/schemas/QueryResultLimits"),
        );
    }
    let required = if lossless {
        vec!["sql", "value_encoding"]
    } else {
        vec!["sql"]
    };
    strict_object(&required, Value::Object(properties))
}

fn query_result_limits_schema() -> Value {
    json!({
        "type": "object",
        "minProperties": 1,
        "properties": {
            "max_rows": {"type": "integer", "minimum": 1, "maximum": MAX_RESULT_ROWS},
            "max_logical_bytes": {"type": "integer", "minimum": 1, "maximum": MAX_RESULT_BYTES}
        },
        "additionalProperties": false,
        "x-briskdb-number-token-limit": "max_rows and max_logical_bytes require unsigned-integer JSON token spelling; JSON Schema validators may also accept mathematically integral decimal or exponent tokens."
    })
}

fn refs_one_of(names: &[&str]) -> Value {
    json!({
        "oneOf": names
            .iter()
            .map(|name| ref_value(&format!("#/components/schemas/{name}")))
            .collect::<Vec<_>>()
    })
}

fn described_ref(reference: &str, description: &str) -> Value {
    json!({"$ref": reference, "description": description})
}

fn strict_object(required: &[&str], properties: Value) -> Value {
    let mut schema = Map::from_iter([
        ("type".to_owned(), json!("object")),
        ("properties".to_owned(), properties),
        ("additionalProperties".to_owned(), json!(false)),
    ]);
    if !required.is_empty() {
        schema.insert("required".to_owned(), json!(required));
    }
    Value::Object(schema)
}

fn unsigned_integer(maximum: Option<u64>) -> Value {
    let mut schema = Map::from_iter([
        ("type".to_owned(), json!("integer")),
        ("minimum".to_owned(), json!(0)),
    ]);
    if let Some(maximum) = maximum {
        schema.insert("maximum".to_owned(), json!(maximum));
    }
    Value::Object(schema)
}

fn canonical_positive_u64_string() -> Value {
    json!({
        "type": "string",
        "pattern": "^(?:[1-9][0-9]{0,18}|1[0-7][0-9]{18}|18[0-3][0-9]{17}|184[0-3][0-9]{16}|1844[0-5][0-9]{15}|18446[0-6][0-9]{14}|184467[0-3][0-9]{13}|1844674[0-3][0-9]{12}|184467440[0-6][0-9]{10}|1844674407[0-2][0-9]{9}|18446744073[0-6][0-9]{8}|1844674407370[0-8][0-9]{6}|18446744073709[0-4][0-9]{5}|184467440737095[0-4][0-9]{4}|1844674407370955[0][0-9]{3}|18446744073709551[0-5][0-9]{2}|184467440737095516[0][0-9]|1844674407370955161[0-4]|18446744073709551615)$"
    })
}

fn schema_generation_string() -> Value {
    json!({"type": "string", "pattern": SCHEMA_GENERATION_PATTERN})
}

fn positive_schema_generation_string() -> Value {
    let mut schema = schema_generation_string();
    schema["not"] = json!({"const": "0"});
    schema
}

fn array_of(reference: &str) -> Value {
    json!({"type": "array", "items": ref_value(reference)})
}

fn api_version_schema() -> Value {
    response_object(
        &[
            "api_version",
            "value_encoding",
            "supported_value_encodings",
            "session_scope",
            "sql_dialect",
            "max_request_bytes",
            "max_result_rows",
            "max_result_logical_bytes",
            "stream_buffer_rows",
            "request_id_header",
            "idempotency_key_header",
            "stream_media_type",
        ],
        json!({
            "api_version": {"type": "string", "const": "1"},
            "value_encoding": {"type": "string", "const": "legacy-json-v1"},
            "supported_value_encodings": {
                "type": "array",
                "prefixItems": [
                    {"type": "string", "const": "legacy-json-v1"},
                    {"type": "string", "const": "lossless-json-v1"}
                ],
                "items": false,
                "minItems": 2,
                "maxItems": 2
            },
            "session_scope": {"type": "string", "const": "request"},
            "sql_dialect": {"type": "string", "const": "sqlite"},
            "max_request_bytes": {"type": "integer", "const": MAX_REQUEST_BYTES},
            "max_result_rows": {"type": "integer", "minimum": 1, "maximum": MAX_RESULT_ROWS},
            "max_result_logical_bytes": {"type": "integer", "minimum": 1, "maximum": MAX_RESULT_BYTES},
            "stream_buffer_rows": {"type": "integer", "const": DEFAULT_STREAM_BUFFER_ROWS},
            "request_id_header": {"type": "string", "const": "BriskDB-Request-ID"},
            "idempotency_key_header": {"type": "string", "const": "BriskDB-Idempotency-Key"},
            "stream_media_type": {"type": "string", "const": STREAM_MEDIA_TYPE}
        }),
    )
}

fn health_schema() -> Value {
    response_object(
        &["status", "shards", "global_indexes"],
        json!({
            "status": {"type": "string", "enum": ["ok", "degraded"]},
            "shards": {"type": "integer", "minimum": MIN_PHYSICAL_SHARDS, "maximum": MAX_PHYSICAL_SHARDS},
            "global_indexes": ref_value("#/components/schemas/HealthGlobalIndexesResponse")
        }),
    )
}

fn health_global_indexes_schema() -> Value {
    response_object(
        &[
            "state",
            "total",
            "healthy",
            "degraded",
            "unavailable",
            "async_lag",
            "retained_outbox_events",
            "retained_outbox_bytes",
            "backpressured_outbox_shards",
        ],
        json!({
            "state": {"type": "string", "enum": ["healthy", "degraded", "unavailable"]},
            "total": unsigned_integer(Some(MAX_GLOBAL_INDEXES as u64)),
            "healthy": unsigned_integer(Some(MAX_GLOBAL_INDEXES as u64)),
            "degraded": unsigned_integer(Some(MAX_GLOBAL_INDEXES as u64)),
            "unavailable": unsigned_integer(Some(MAX_GLOBAL_INDEXES as u64)),
            "async_lag": unsigned_integer(Some(u64::MAX)),
            "retained_outbox_events": unsigned_integer(Some(MAX_RETAINED_OUTBOX_EVENTS)),
            "retained_outbox_bytes": unsigned_integer(Some(MAX_RETAINED_OUTBOX_BYTES)),
            "backpressured_outbox_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS))
        }),
    )
}

fn readiness_schema() -> Value {
    refs_one_of(&["ReadyResponse", "NotReadyResponse"])
}

fn ready_schema(ready: bool) -> Value {
    if ready {
        return readiness_response_branch("ready", true, "running", "ready", &[]);
    }

    let cases: [(&str, &str, &[&str]); 11] = [
        ("running", "migrating", &["schema_migrating"]),
        ("running", "pending", &["schema_recovery_pending"]),
        ("running", "degraded", &["schema_degraded"]),
        ("draining", "ready", &["engine_draining"]),
        (
            "draining",
            "migrating",
            &["engine_draining", "schema_migrating"],
        ),
        (
            "draining",
            "pending",
            &["engine_draining", "schema_recovery_pending"],
        ),
        (
            "draining",
            "degraded",
            &["engine_draining", "schema_degraded"],
        ),
        ("stopped", "ready", &["engine_stopped"]),
        (
            "stopped",
            "migrating",
            &["engine_stopped", "schema_migrating"],
        ),
        (
            "stopped",
            "pending",
            &["engine_stopped", "schema_recovery_pending"],
        ),
        (
            "stopped",
            "degraded",
            &["engine_stopped", "schema_degraded"],
        ),
    ];
    let mut response = response_object(
        &[
            "status",
            "ready",
            "reasons",
            "engine_state",
            "schema_state",
            "schema_generation",
            "active_schema_operations",
        ],
        json!({
            "status": {"type": "string", "const": "not_ready"},
            "ready": {"type": "boolean", "const": false},
            "reasons": {
                "type": "array",
                "minItems": 1,
                "maxItems": 2,
                "uniqueItems": true,
                "items": {"type": "string", "enum": [
                    "engine_draining",
                    "engine_stopped",
                    "schema_migrating",
                    "schema_recovery_pending",
                    "schema_degraded"
                ]}
            },
            "engine_state": {"type": "string", "enum": ["running", "draining", "stopped"]},
            "schema_state": {"type": "string", "enum": ["ready", "migrating", "pending", "degraded"]},
            "schema_generation": schema_generation_string(),
            "active_schema_operations": unsigned_integer(Some(u64::MAX))
        }),
    );
    response["oneOf"] = Value::Array(
        cases
            .into_iter()
            .map(|(engine_state, schema_state, reasons)| {
                not_ready_state_case(engine_state, schema_state, reasons)
            })
            .collect(),
    );
    response
}

fn not_ready_state_case(engine_state: &str, schema_state: &str, reasons: &[&str]) -> Value {
    json!({
        "type": "object",
        "required": ["engine_state", "schema_state", "reasons"],
        "properties": {
            "engine_state": {"type": "string", "const": engine_state},
            "schema_state": {"type": "string", "const": schema_state},
            "reasons": exact_readiness_reasons(reasons)
        }
    })
}

fn readiness_response_branch(
    status: &str,
    ready: bool,
    engine_state: &str,
    schema_state: &str,
    reasons: &[&str],
) -> Value {
    let reasons = exact_readiness_reasons(reasons);
    response_object(
        &[
            "status",
            "ready",
            "reasons",
            "engine_state",
            "schema_state",
            "schema_generation",
            "active_schema_operations",
        ],
        json!({
            "status": {"type": "string", "const": status},
            "ready": {"type": "boolean", "const": ready},
            "reasons": reasons,
            "engine_state": {"type": "string", "const": engine_state},
            "schema_state": {"type": "string", "const": schema_state},
            "schema_generation": schema_generation_string(),
            "active_schema_operations": unsigned_integer(Some(u64::MAX))
        }),
    )
}

fn exact_readiness_reasons(reasons: &[&str]) -> Value {
    let reason_count = reasons.len();
    json!({
        "type": "array",
        "prefixItems": reasons
            .iter()
            .map(|reason| json!({"type": "string", "const": reason}))
            .collect::<Vec<_>>(),
        "items": false,
        "minItems": reason_count,
        "maxItems": reason_count
    })
}

fn query_column_schema() -> Value {
    response_object(
        &["name", "data_type"],
        json!({
            "name": {"type": "string"},
            "data_type": {"type": "string", "enum": [
                "unknown", "null", "boolean", "int64", "uint64", "float64", "decimal", "text", "binary"
            ]}
        }),
    )
}

fn query_response_schema(lossless: bool, single_shard: bool) -> Value {
    let value_schema = if lossless {
        "#/components/schemas/LosslessJsonValue"
    } else {
        "#/components/schemas/LegacyJsonCell"
    };
    let mut properties = Map::from_iter([
        (
            "columns".to_owned(),
            array_of("#/components/schemas/QueryColumn"),
        ),
        (
            "rows".to_owned(),
            json!({
                "type": "array",
                "items": {"type": "array", "items": ref_value(value_schema)},
                "maxItems": MAX_RESULT_ROWS
            }),
        ),
    ]);
    let mut required = vec!["columns", "rows"];
    if single_shard {
        properties.insert(
            "shard".to_owned(),
            unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
        );
        required.push("shard");
    } else {
        properties.insert(
            "shards".to_owned(),
            json!({
                "type": "array",
                "minItems": MIN_PHYSICAL_SHARDS,
                "maxItems": MAX_PHYSICAL_SHARDS,
                "uniqueItems": true,
                "items": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID))
            }),
        );
        required.push("shards");
    }
    if lossless {
        properties.insert(
            "value_encoding".to_owned(),
            json!({"type": "string", "const": "lossless-json-v1"}),
        );
        required.push("value_encoding");
    }
    let schema = response_object(&required, Value::Object(properties));
    let mut forbidden = vec![if single_shard { "shards" } else { "shard" }];
    if !lossless {
        forbidden.push("value_encoding");
    }
    forbid_properties(schema, &forbidden)
}

fn stream_meta_schema(lossless: bool, single_shard: bool) -> Value {
    let mut properties = Map::from_iter([
        (
            "kind".to_owned(),
            json!({"type": "string", "const": "meta"}),
        ),
        (
            "columns".to_owned(),
            array_of("#/components/schemas/QueryColumn"),
        ),
    ]);
    let mut required = vec!["kind", "columns"];
    if single_shard {
        properties.insert(
            "shard".to_owned(),
            unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
        );
        required.push("shard");
    } else {
        properties.insert(
            "shards".to_owned(),
            json!({
                "type": "array",
                "minItems": MIN_PHYSICAL_SHARDS,
                "maxItems": MAX_PHYSICAL_SHARDS,
                "uniqueItems": true,
                "items": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID))
            }),
        );
        required.push("shards");
    }
    if lossless {
        properties.insert(
            "value_encoding".to_owned(),
            json!({"type": "string", "const": "lossless-json-v1"}),
        );
        required.push("value_encoding");
    }
    strict_object(&required, Value::Object(properties))
}

fn stream_row_schema() -> Value {
    strict_object(
        &["kind", "values"],
        json!({
            "kind": {"type": "string", "const": "row"},
            "values": {
                "type": "array",
                "items": {
                    "anyOf": [
                        ref_value("#/components/schemas/LegacyJsonCell"),
                        ref_value("#/components/schemas/LosslessJsonValue")
                    ]
                }
            }
        }),
    )
}

fn legacy_json_cell_schema() -> Value {
    json!({
        "description": "One legacy-json-v1 result cell. A byte array represents Binary; objects and nested arrays are never emitted.",
        "oneOf": [
            {"type": "null"},
            {"type": "boolean"},
            ref_value("#/components/schemas/LegacyJsonNumber"),
            {"type": "string"},
            {
                "type": "array",
                "items": {"type": "integer", "minimum": 0, "maximum": 255}
            }
        ]
    })
}

fn stream_complete_schema() -> Value {
    strict_object(
        &["kind", "rows"],
        json!({
            "kind": {"type": "string", "const": "complete"},
            "rows": unsigned_integer(Some(MAX_RESULT_ROWS))
        }),
    )
}

fn stream_error_schema() -> Value {
    strict_object(
        &["kind", "type", "title", "status", "detail", "code"],
        json!({
            "kind": {"type": "string", "const": "error"},
            "type": {"type": "string", "format": "uri"},
            "title": {"type": "string"},
            "status": {"type": "integer", "minimum": 100, "maximum": 599},
            "detail": {"type": "string"},
            "code": {"type": "string"}
        }),
    )
}

fn finalize_operational_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "BroadcastResponse".to_owned(),
        response_object(
            &["completed_shards"],
            json!({
                "completed_shards": {
                    "type": "array",
                    "minItems": MIN_PHYSICAL_SHARDS,
                    "maxItems": MAX_PHYSICAL_SHARDS,
                    "uniqueItems": true,
                    "items": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID))
                }
            }),
        ),
    );
    schemas.insert("ExecuteGeneratedKey".to_owned(), generated_key_schema());
    schemas.insert("ExecuteResponse".to_owned(), execute_response_schema());

    schemas.insert(
        "CatalogDb".to_owned(),
        response_object(
            &["id", "name"],
            json!({"id": canonical_positive_u64_string(), "name": {"type": "string"}}),
        ),
    );
    schemas.insert(
        "CatalogShardKey".to_owned(),
        response_object(
            &["column", "data_type"],
            json!({
                "column": {"type": "string"},
                "data_type": {"type": "string", "enum": ["int64", "text", "binary"]}
            }),
        ),
    );
    schemas.insert(
        "CatalogShardedPlacement".to_owned(),
        response_object(
            &["kind", "shard_key"],
            json!({
                "kind": {"type": "string", "const": "sharded"},
                "shard_key": ref_value("#/components/schemas/CatalogShardKey")
            }),
        ),
    );
    schemas.insert(
        "CatalogUnshardedPlacement".to_owned(),
        forbid_properties(
            response_object(
                &["kind"],
                json!({"kind": {"type": "string", "enum": ["global", "catalog", "unknown"]}}),
            ),
            &["shard_key"],
        ),
    );
    schemas.insert(
        "CatalogPlacement".to_owned(),
        refs_one_of(&["CatalogShardedPlacement", "CatalogUnshardedPlacement"]),
    );
    schemas.insert(
        "CatalogGeneratedIdDisabled".to_owned(),
        forbid_properties(
            response_object(
                &["policy"],
                json!({"policy": {"type": "string", "enum": ["none", "unknown"]}}),
            ),
            &["column", "encoding_version"],
        ),
    );
    schemas.insert(
        "CatalogGeneratedIdEnabled".to_owned(),
        response_object(
            &["policy", "column", "encoding_version"],
            json!({
                "policy": {"type": "string", "enum": ["native_range_v1", "hilo_v1"]},
                "column": {"type": "string"},
                "encoding_version": {"type": "integer", "const": 1}
            }),
        ),
    );
    schemas.insert(
        "CatalogGeneratedId".to_owned(),
        refs_one_of(&["CatalogGeneratedIdDisabled", "CatalogGeneratedIdEnabled"]),
    );
    schemas.insert("CatalogTable".to_owned(), catalog_table_schema());
    schemas.insert(
        "CatalogGlobalIndex".to_owned(),
        catalog_global_index_schema(),
    );
    schemas.insert("CatalogResponse".to_owned(), catalog_response_schema());

    schemas.insert("MigrationResponse".to_owned(), migration_schema());
    schemas.insert("MigrationsResponse".to_owned(), migrations_schema());
    schemas.insert("ShardStatus".to_owned(), shard_schema());
    schemas.insert("ShardStatusResponse".to_owned(), shards_schema());
    schemas.insert("ActiveQueryResponse".to_owned(), active_query_schema());
    schemas.insert(
        "ActiveQueriesResponse".to_owned(),
        response_object(
            &["queries"],
            json!({
                "queries": {
                    "type": "array",
                    "maxItems": MAX_ACTIVE_QUERIES,
                    "items": ref_value("#/components/schemas/ActiveQueryResponse")
                }
            }),
        ),
    );
    schemas.insert(
        "CancelQueryResponse".to_owned(),
        response_object(
            &["operation_id", "newly_requested"],
            json!({
                "operation_id": ref_value("#/components/schemas/OpaqueId"),
                "newly_requested": {"type": "boolean"}
            }),
        ),
    );
    schemas.insert(
        "BackupCapabilityResponse".to_owned(),
        backup_capability_schema(),
    );
    schemas.insert(
        "CheckpointShardResponse".to_owned(),
        checkpoint_shard_schema(),
    );
    schemas.insert(
        "CheckpointDbResponse".to_owned(),
        checkpoint_database_schema(),
    );
    schemas.insert("CheckpointResponse".to_owned(), checkpoint_schema());
    schemas.insert("GlobalIndexStatus".to_owned(), global_index_status_schema());
    schemas.insert(
        "GlobalIndexesResponse".to_owned(),
        global_indexes_response_schema(),
    );
}

fn generated_key_schema() -> Value {
    json!({
        "oneOf": [
            response_object(
                &["column", "data_type", "value"],
                json!({
                    "column": {"type": "string"},
                    "data_type": {"type": "string", "const": "int64"},
                    "value": ref_value("#/components/schemas/LosslessInt64/properties/value")
                }),
            ),
            response_object(
                &["column", "data_type", "value"],
                json!({
                    "column": {"type": "string"},
                    "data_type": {"type": "string", "const": "uint64"},
                    "value": ref_value("#/components/schemas/LosslessUInt64/properties/value")
                }),
            )
        ]
    })
}

fn execute_response_schema() -> Value {
    let without_key = response_object(
        &["shard", "rows_affected"],
        json!({
            "shard": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
            "rows_affected": unsigned_integer(Some(u64::MAX))
        }),
    );
    let with_key = response_object(
        &["shard", "rows_affected", "generated_key"],
        json!({
            "shard": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
            "rows_affected": unsigned_integer(Some(u64::MAX)),
            "generated_key": ref_value("#/components/schemas/ExecuteGeneratedKey")
        }),
    );
    json!({"oneOf": [forbid_properties(without_key, &["generated_key"]), with_key]})
}

fn catalog_table_schema() -> Value {
    response_object(
        &["id", "database_id", "name", "placement", "generated_id"],
        json!({
            "id": canonical_positive_u64_string(),
            "database_id": canonical_positive_u64_string(),
            "name": {"type": "string"},
            "placement": ref_value("#/components/schemas/CatalogPlacement"),
            "generated_id": ref_value("#/components/schemas/CatalogGeneratedId")
        }),
    )
}

fn catalog_global_index_schema() -> Value {
    response_object(
        &[
            "id",
            "table_id",
            "name",
            "unique",
            "lifecycle",
            "schema_generation",
            "key_encoding_version",
        ],
        json!({
            "id": canonical_positive_u64_string(),
            "table_id": canonical_positive_u64_string(),
            "name": {"type": "string"},
            "unique": {"type": "boolean"},
            "lifecycle": {"type": "string", "enum": ["creating", "ready", "invalid", "rebuilding", "dropping"]},
            "schema_generation": schema_generation_string(),
            "key_encoding_version": {"type": "integer", "const": 1}
        }),
    )
}

fn catalog_response_schema() -> Value {
    response_object(
        &[
            "identifier_encoding_version",
            "schema_generation",
            "default_database_id",
            "databases",
            "tables",
            "global_indexes",
        ],
        json!({
            "identifier_encoding_version": {"type": "integer", "const": 1},
            "schema_generation": schema_generation_string(),
            "default_database_id": {
                "type": "string",
                "const": DEFAULT_LOGICAL_DATABASE_ID.to_string()
            },
            "databases": {
                "type": "array",
                "items": ref_value("#/components/schemas/CatalogDb"),
                "minItems": 1,
                "maxItems": MAX_LOGICAL_DATABASES,
                "contains": {
                    "type": "object",
                    "required": ["id", "name"],
                    "properties": {
                        "id": {"const": DEFAULT_LOGICAL_DATABASE_ID.to_string()},
                        "name": {"const": DEFAULT_LOGICAL_DATABASE_NAME}
                    }
                },
                "minContains": 1,
                "maxContains": 1
            },
            "tables": {
                "type": "array",
                "items": ref_value("#/components/schemas/CatalogTable"),
                "maxItems": MAX_TABLES
            },
            "global_indexes": {
                "type": "array",
                "items": ref_value("#/components/schemas/CatalogGlobalIndex"),
                "maxItems": MAX_GLOBAL_INDEXES
            }
        }),
    )
}

fn migration_schema() -> Value {
    response_object(
        &[
            "generation",
            "source_generation",
            "target_generation",
            "state",
            "shard_count",
            "next_shard",
            "completed_shards",
            "sql_bytes",
        ],
        json!({
            "generation": positive_schema_generation_string(),
            "source_generation": schema_generation_string(),
            "target_generation": positive_schema_generation_string(),
            "state": {"type": "string", "enum": ["applying", "complete"]},
            "shard_count": {"type": "integer", "minimum": MIN_PHYSICAL_SHARDS, "maximum": MAX_PHYSICAL_SHARDS},
            "next_shard": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "completed_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "sql_bytes": {"type": "integer", "minimum": 1, "maximum": MAX_SCHEMA_MIGRATION_SQL_BYTES}
        }),
    )
}

fn migrations_schema() -> Value {
    response_object(
        &["schema_generation", "active", "latest_complete"],
        json!({
            "schema_generation": schema_generation_string(),
            "active": {"oneOf": [{"type": "null"}, ref_value("#/components/schemas/MigrationResponse")]},
            "latest_complete": {"oneOf": [{"type": "null"}, ref_value("#/components/schemas/MigrationResponse")]}
        }),
    )
}

fn shard_schema() -> Value {
    response_object(
        &["id", "state"],
        json!({
            "id": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
            "state": {"type": "string", "const": "ready"}
        }),
    )
}

fn shards_schema() -> Value {
    response_object(
        &["schema_generation", "shards"],
        json!({
            "schema_generation": schema_generation_string(),
            "shards": {
                "type": "array",
                "items": ref_value("#/components/schemas/ShardStatus"),
                "minItems": MIN_PHYSICAL_SHARDS,
                "maxItems": MAX_PHYSICAL_SHARDS,
                "uniqueItems": true
            }
        }),
    )
}

fn active_query_schema() -> Value {
    response_object(
        &[
            "operation_id",
            "elapsed_ms",
            "sql_bytes",
            "cancellation_requested",
        ],
        json!({
            "operation_id": ref_value("#/components/schemas/OpaqueId"),
            "elapsed_ms": unsigned_integer(Some(u64::MAX)),
            "sql_bytes": unsigned_integer(Some(u64::MAX)),
            "cancellation_requested": {"type": "boolean"}
        }),
    )
}

fn backup_capability_schema() -> Value {
    response_object(
        &[
            "mode",
            "online",
            "requires_all_processes_stopped",
            "checkpoint_endpoint",
            "checkpoint_role",
            "checkpoint_is_recovery_point",
        ],
        json!({
            "mode": {"type": "string", "const": "stopped_directory_copy"},
            "online": {"type": "boolean", "const": false},
            "requires_all_processes_stopped": {"type": "boolean", "const": true},
            "checkpoint_endpoint": {"type": "string", "const": "/v1/admin/maintenance/checkpoint"},
            "checkpoint_role": {"type": "string", "const": "preparation_only"},
            "checkpoint_is_recovery_point": {"type": "boolean", "const": false}
        }),
    )
}

fn checkpoint_shard_schema() -> Value {
    response_object(
        &[
            "shard",
            "busy",
            "counts_available",
            "wal_frames",
            "checkpointed_frames",
            "complete",
        ],
        json!({
            "shard": unsigned_integer(Some(MAX_PHYSICAL_SHARD_ID)),
            "busy": {"type": "boolean"},
            "counts_available": {"type": "boolean"},
            "wal_frames": unsigned_integer(Some(u64::MAX)),
            "checkpointed_frames": unsigned_integer(Some(u64::MAX)),
            "complete": {"type": "boolean"}
        }),
    )
}

fn checkpoint_database_schema() -> Value {
    response_object(
        &[
            "database",
            "busy",
            "counts_available",
            "wal_frames",
            "checkpointed_frames",
            "complete",
        ],
        json!({
            "database": {"type": "string", "enum": ["manifest", "global_index"]},
            "busy": {"type": "boolean"},
            "counts_available": {"type": "boolean"},
            "wal_frames": unsigned_integer(Some(u64::MAX)),
            "checkpointed_frames": unsigned_integer(Some(u64::MAX)),
            "complete": {"type": "boolean"}
        }),
    )
}

fn checkpoint_schema() -> Value {
    response_object(
        &[
            "operation",
            "busy",
            "complete",
            "recovery_point",
            "shards",
            "databases",
        ],
        json!({
            "operation": {"type": "string", "const": "passive_checkpoint"},
            "busy": {"type": "boolean"},
            "complete": {"type": "boolean"},
            "recovery_point": {"type": "boolean", "const": false},
            "shards": {
                "type": "array",
                "items": ref_value("#/components/schemas/CheckpointShardResponse"),
                "minItems": MIN_PHYSICAL_SHARDS,
                "maxItems": MAX_PHYSICAL_SHARDS,
                "uniqueItems": true
            },
            "databases": {
                "type": "array",
                "items": ref_value("#/components/schemas/CheckpointDbResponse"),
                "minItems": 1,
                "maxItems": 2,
                "uniqueItems": true
            }
        }),
    )
}

fn global_index_status_schema() -> Value {
    response_object(
        &[
            "id",
            "name",
            "unique",
            "lifecycle",
            "health",
            "available",
            "recovery",
            "authority_entries",
            "unique_keys",
            "active_operations",
            "active_unique_reservations",
            "active_value_leases",
            "pending_read_repairs",
            "applied_read_repairs",
            "async_lag",
            "async_failures",
            "poisoned_shards",
            "leased_shards",
            "async_paused",
            "rebuild_required",
            "summary_ready_shards",
            "summary_degraded_shards",
            "summary_saturated_shards",
        ],
        json!({
            "id": canonical_positive_u64_string(),
            "name": {"type": "string"},
            "unique": {"type": "boolean"},
            "lifecycle": {"type": "string", "enum": ["creating", "ready", "invalid", "rebuilding", "dropping"]},
            "health": {"type": "string", "enum": ["healthy", "degraded", "unavailable"]},
            "available": {"type": "boolean"},
            "recovery": {"type": "string", "enum": ["build", "none", "rebuild", "resume_rebuild"]},
            "authority_entries": unsigned_integer(Some(u64::MAX)),
            "unique_keys": unsigned_integer(Some(u64::MAX)),
            "active_operations": unsigned_integer(Some(u64::MAX)),
            "active_unique_reservations": unsigned_integer(Some(u64::MAX)),
            "active_value_leases": unsigned_integer(Some(u64::MAX)),
            "pending_read_repairs": unsigned_integer(Some(u64::MAX)),
            "applied_read_repairs": unsigned_integer(Some(u64::MAX)),
            "async_lag": unsigned_integer(Some(u64::MAX)),
            "async_failures": unsigned_integer(Some(u64::MAX)),
            "poisoned_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "leased_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "async_paused": {"type": "boolean"},
            "rebuild_required": {"type": "boolean"},
            "summary_ready_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "summary_degraded_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "summary_saturated_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS))
        }),
    )
}

fn global_indexes_response_schema() -> Value {
    response_object(
        &[
            "state",
            "retained_outbox_events",
            "retained_outbox_bytes",
            "backpressured_outbox_shards",
            "indexes",
        ],
        json!({
            "state": {"type": "string", "enum": ["healthy", "degraded", "unavailable"]},
            "retained_outbox_events": unsigned_integer(Some(MAX_RETAINED_OUTBOX_EVENTS)),
            "retained_outbox_bytes": unsigned_integer(Some(MAX_RETAINED_OUTBOX_BYTES)),
            "backpressured_outbox_shards": unsigned_integer(Some(MAX_PHYSICAL_SHARDS)),
            "indexes": {
                "type": "array",
                "items": ref_value("#/components/schemas/GlobalIndexStatus"),
                "maxItems": MAX_GLOBAL_INDEXES
            }
        }),
    )
}

fn finalize_problem_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert("ProblemDetails".to_owned(), problem_schema(None));
    for status in [
        400_u16, 403, 404, 405, 409, 413, 415, 422, 500, 501, 503, 504, 507,
    ] {
        schemas.insert(
            format!("ProblemDetails{status}"),
            problem_schema(Some(status)),
        );
    }
}

fn problem_schema(status: Option<u16>) -> Value {
    strict_object(
        &["type", "title", "status", "detail", "code"],
        json!({
            "type": {"type": "string", "format": "uri"},
            "title": {"type": "string"},
            "status": match status {
                Some(status) => json!({"type": "integer", "const": status}),
                None => json!({"type": "integer", "minimum": 100, "maximum": 599}),
            },
            "detail": {"type": "string"},
            "code": {"type": "string"}
        }),
    )
}

fn response_object(required: &[&str], properties: Value) -> Value {
    let mut schema = Map::from_iter([
        ("type".to_owned(), json!("object")),
        ("properties".to_owned(), properties),
    ]);
    if !required.is_empty() {
        schema.insert("required".to_owned(), json!(required));
    }
    Value::Object(schema)
}

fn forbid_properties(mut schema: Value, names: &[&str]) -> Value {
    let forbidden = names
        .iter()
        .map(|name| json!({"required": [name]}))
        .collect::<Vec<_>>();
    schema["not"] = if forbidden.len() == 1 {
        forbidden
            .into_iter()
            .next()
            .expect("one forbidden property")
    } else {
        json!({"anyOf": forbidden})
    };
    schema
}
