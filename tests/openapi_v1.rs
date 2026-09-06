#![cfg(feature = "http")]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode},
};
use briskdb::{
    core::{Database, Engine, EngineErrorKind},
    protocol::error::http_error,
};
use jsonschema::Validator;
use serde_json::{Map, Value, json};
use tower::ServiceExt as _;

const DATA_SERVER: &str = "http://127.0.0.1:7654";
const ADMIN_SERVER: &str = "http://127.0.0.1:7655";
const LEGACY_REQUEST_ROUNDING_LIMIT: &str = concat!(
    "179769313486231580793728971405303415079934132710037826936173778980444968292",
    "764750946649017977587207096330286416692887910946555547851940402630657488671",
    "505820681908902000708383676273854845817711531764475730270069855571366959622",
    "842914819860834936475292719074168444365510704342711559699508093042880177904",
    "174497792"
);
const LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE: &str = concat!(
    "179769313486231580793728971405303415079934132710037826936173778980444968292",
    "764750946649017977587207096330286416692887910946555547851940402630657488671",
    "505820681908902000708383676273854845817711531764475730270069855571366959622",
    "842914819860834936475292719074168444365510704342711559699508093042880177904",
    "174497791"
);

const OPERATION_IDS: &[&str] = &[
    "broadcastMigration",
    "cancelQuery",
    "checkpointDatabases",
    "executeStatement",
    "getApiVersion",
    "getApiVersionAlias",
    "getBackupCapability",
    "getCatalog",
    "getGlobalIndexes",
    "getHealth",
    "getMigration",
    "getReadiness",
    "headActiveQueries",
    "headApiVersion",
    "headApiVersionAlias",
    "headBackupCapability",
    "headCatalog",
    "headGlobalIndexes",
    "headHealth",
    "headMigration",
    "headMigrations",
    "headReadiness",
    "headShards",
    "listActiveQueries",
    "listMigrations",
    "listShards",
    "queryRows",
    "streamQueryRows",
];

const ROUTES: &[(&str, &[&str], &str)] = &[
    ("/v1", &["get", "head"], DATA_SERVER),
    ("/v1/", &["get", "head"], DATA_SERVER),
    ("/v1/execute", &["post"], DATA_SERVER),
    ("/v1/query", &["post"], DATA_SERVER),
    ("/v1/query/stream", &["post"], DATA_SERVER),
    ("/v1/health", &["get", "head"], ADMIN_SERVER),
    ("/v1/ready", &["get", "head"], ADMIN_SERVER),
    ("/v1/admin/broadcast", &["post"], ADMIN_SERVER),
    ("/v1/admin/global-indexes", &["get", "head"], ADMIN_SERVER),
    ("/v1/admin/catalog", &["get", "head"], ADMIN_SERVER),
    ("/v1/admin/migrations", &["get", "head"], ADMIN_SERVER),
    (
        "/v1/admin/migrations/{target_generation}",
        &["get", "head"],
        ADMIN_SERVER,
    ),
    ("/v1/admin/shards", &["get", "head"], ADMIN_SERVER),
    ("/v1/admin/queries", &["get", "head"], ADMIN_SERVER),
    (
        "/v1/admin/queries/{operation_id}/cancel",
        &["post"],
        ADMIN_SERVER,
    ),
    ("/v1/admin/backup", &["get", "head"], ADMIN_SERVER),
    ("/v1/admin/maintenance/checkpoint", &["post"], ADMIN_SERVER),
];

fn document() -> Value {
    briskdb::api::openapi_v1()
}

fn object<'a>(value: &'a Value, context: &str) -> &'a Map<String, Value> {
    value
        .as_object()
        .unwrap_or_else(|| panic!("{context} must be an object, got {value}"))
}

fn operation<'a>(document: &'a Value, path: &str, method: &str) -> &'a Value {
    document
        .pointer(&format!("/paths/{}/{method}", escape_pointer(path)))
        .unwrap_or_else(|| panic!("missing {method} {path}"))
}

fn escape_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn resolve<'a>(document: &'a Value, value: &'a Value) -> &'a Value {
    let Some(reference) = value.get("$ref").and_then(Value::as_str) else {
        return value;
    };
    let pointer = reference
        .strip_prefix('#')
        .unwrap_or_else(|| panic!("external OpenAPI reference is forbidden: {reference}"));
    document
        .pointer(pointer)
        .unwrap_or_else(|| panic!("unresolved OpenAPI reference: {reference}"))
}

fn response<'a>(document: &'a Value, path: &str, method: &str, status: &str) -> &'a Value {
    let response = operation(document, path, method)
        .pointer(&format!("/responses/{status}"))
        .unwrap_or_else(|| panic!("missing {status} response for {method} {path}"));
    resolve(document, response)
}

fn response_schema<'a>(
    document: &'a Value,
    path: &str,
    method: &str,
    status: &str,
    media_type: &str,
) -> &'a Value {
    response(document, path, method, status)
        .pointer(&format!("/content/{}/schema", escape_pointer(media_type)))
        .unwrap_or_else(|| {
            panic!("missing {media_type} schema for {status} response to {method} {path}")
        })
}

fn request_schema<'a>(document: &'a Value, path: &str) -> &'a Value {
    operation(document, path, "post")
        .pointer("/requestBody/content/application~1json/schema")
        .unwrap_or_else(|| panic!("missing JSON request schema for POST {path}"))
}

fn parameter<'a>(document: &'a Value, path: &str, method: &str, name: &str) -> &'a Value {
    operation(document, path, method)["parameters"]
        .as_array()
        .unwrap_or_else(|| panic!("{method} {path} must declare parameters"))
        .iter()
        .map(|parameter| resolve(document, parameter))
        .find(|parameter| parameter["name"] == name)
        .unwrap_or_else(|| panic!("{method} {path} omits parameter {name}"))
}

fn component<'a>(document: &'a Value, name: &str) -> &'a Value {
    document
        .pointer(&format!("/components/schemas/{name}"))
        .unwrap_or_else(|| panic!("missing {name} component schema"))
}

fn exact_json(source: &str) -> Value {
    serde_json::from_str(source)
        .unwrap_or_else(|error| panic!("invalid test JSON {source}: {error}"))
}

fn validator(document: &Value, schema: &Value) -> Validator {
    let mut root = document.clone();
    let root_object = root
        .as_object_mut()
        .expect("the OpenAPI document root must be an object");
    root_object.insert(
        "$schema".to_owned(),
        Value::String("https://json-schema.org/draft/2020-12/schema".to_owned()),
    );
    root_object.insert("allOf".to_owned(), Value::Array(vec![schema.clone()]));
    jsonschema::draft202012::new(&root).expect("component must compile as JSON Schema 2020-12")
}

fn assert_valid(document: &Value, schema: &Value, instance: &Value, context: &str) {
    let validator = validator(document, schema);
    if !validator.is_valid(instance) {
        let errors = validator
            .iter_errors(instance)
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        panic!("{context} does not match its OpenAPI schema: {errors}\ninstance: {instance}");
    }
}

fn assert_invalid(document: &Value, schema: &Value, instance: &Value, context: &str) {
    assert!(
        !validator(document, schema).is_valid(instance),
        "{context} unexpectedly matches its OpenAPI schema: {instance}"
    );
}

fn collect_references<'a>(value: &'a Value, references: &mut Vec<&'a str>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_references(value, references);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key == "$ref" {
                    references.push(value.as_str().expect("$ref values must be strings"));
                }
                collect_references(value, references);
            }
        }
        _ => {}
    }
}

#[test]
fn generated_document_is_the_deterministic_checked_openapi_31_artifact() {
    let first = document();
    let second = document();
    assert_eq!(
        first, second,
        "generation must be deterministic in one process"
    );

    let mut generated = serde_json::to_vec_pretty(&first).unwrap();
    generated.push(b'\n');
    assert_eq!(generated, include_bytes!("../docs/openapi-v1.json"));

    let checked = std::str::from_utf8(include_bytes!("../docs/openapi-v1.json")).unwrap();
    let parsed = oas3::from_json(checked).expect("the artifact must parse as OpenAPI 3.1");
    assert_eq!(parsed.openapi.to_string(), "3.1.0");
    assert_eq!(first["openapi"], "3.1.0");
    assert_eq!(first["info"]["version"], "1");
    assert!(
        first.get("servers").is_none(),
        "listener ownership belongs to each operation"
    );
    assert!(first.get("security").is_none());
    assert!(
        first
            .pointer("/components/securitySchemes")
            .is_none_or(|schemes| object(schemes, "securitySchemes").is_empty())
    );
    assert_eq!(
        first["x-briskdb-wire-contract"],
        json!({
            "maxRequestBytes": 2_097_152,
            "requestObjectsRejectUnknownAndDuplicateMembers": true,
            "requestControlHeadersRequireOneCanonicalValue": true,
            "openapiCannotValidateDuplicateJsonMembersOrRawHeaderMultiplicity": true,
            "idempotencyStatusHeaderIsConditional": true
        })
    );

    let mut references = Vec::new();
    collect_references(&first, &mut references);
    assert!(
        references.len() >= 20,
        "schemas should share component references"
    );
    for reference in references {
        let pointer = reference
            .strip_prefix('#')
            .unwrap_or_else(|| panic!("external reference is forbidden: {reference}"));
        assert!(
            first.pointer(pointer).is_some(),
            "unresolved reference: {reference}"
        );
    }
    let schemas = object(&first["components"]["schemas"], "component schemas");
    for removed in ["Value", "JsonValue"] {
        assert!(
            !schemas.contains_key(removed),
            "unused generated schema {removed} must not leak into the artifact"
        );
    }
    for retained in ["ValueEncoding", "ReadinessResponse", "QueryStreamRecord"] {
        assert!(
            schemas.contains_key(retained),
            "documented abstraction {retained} must remain available"
        );
    }
}

#[test]
fn path_operation_and_listener_matrix_is_exact() {
    let document = document();
    let paths = object(&document["paths"], "paths");
    let expected_paths = ROUTES
        .iter()
        .map(|(path, _, _)| *path)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        paths.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        expected_paths
    );

    let mut operation_ids = BTreeSet::new();
    let mut actual_operations = 0;
    for &(path, expected_methods, expected_server) in ROUTES {
        let path_item = object(&paths[path], path);
        let actual_methods = path_item
            .keys()
            .map(String::as_str)
            .filter(|key| {
                matches!(
                    *key,
                    "get" | "head" | "post" | "put" | "patch" | "delete" | "options" | "trace"
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual_methods,
            expected_methods.iter().copied().collect(),
            "wrong operations for {path}"
        );

        for &method in expected_methods {
            actual_operations += 1;
            let operation = operation(&document, path, method);
            let operation_id = operation["operationId"]
                .as_str()
                .unwrap_or_else(|| panic!("{method} {path} needs an operationId"));
            assert!(
                operation_ids.insert(operation_id),
                "duplicate operationId {operation_id}"
            );
            assert!(
                operation.get("security").is_none(),
                "{method} {path} must not anticipate authentication"
            );
            assert_eq!(
                operation.pointer("/servers/0/url").and_then(Value::as_str),
                Some(expected_server),
                "wrong listener owner for {method} {path}"
            );
            let expected_plane = if expected_server == DATA_SERVER {
                "data"
            } else {
                "administration"
            };
            assert_eq!(
                operation.get("x-briskdb-plane").and_then(Value::as_str),
                Some(expected_plane),
                "wrong plane extension for {method} {path}"
            );
            assert_eq!(
                operation["servers"].as_array().map(Vec::len),
                Some(1),
                "{method} {path} must identify exactly one listener"
            );
        }
    }
    assert_eq!(actual_operations, 28);
    assert_eq!(
        operation_ids,
        OPERATION_IDS.iter().copied().collect::<BTreeSet<_>>()
    );

    let encoded = serde_json::to_string(&document).unwrap();
    for excluded in [
        "\"/health\"",
        "\"/ready\"",
        "\"/metrics\"",
        "\"/admin\"",
        "\"/openapi",
    ] {
        assert!(
            !encoded.contains(excluded),
            "out-of-scope route leaked into the document: {excluded}"
        );
    }
}

#[test]
fn request_objects_are_strict_and_match_the_live_envelopes() {
    let document = document();
    let execute = request_schema(&document, "/v1/execute");
    let query = request_schema(&document, "/v1/query");
    let stream = request_schema(&document, "/v1/query/stream");
    let broadcast = request_schema(&document, "/v1/admin/broadcast");
    let checkpoint = request_schema(&document, "/v1/admin/maintenance/checkpoint");

    for component_name in ["LegacyExecuteRequest", "LegacyQueryRequest"] {
        let schema = component(&document, component_name);
        assert_eq!(
            schema["properties"]["params"]["default"],
            json!([]),
            "{component_name} must expose the runtime empty-parameter default"
        );
        assert_eq!(
            schema["properties"]["value_encoding"],
            json!({
                "type": "string",
                "const": "legacy-json-v1",
                "default": "legacy-json-v1"
            }),
            "{component_name} must expose the runtime legacy codec default inline"
        );
    }

    let result_limits = component(&document, "QueryResultLimits");
    assert_eq!(
        result_limits["x-briskdb-number-token-limit"],
        "max_rows and max_logical_bytes require unsigned-integer JSON token spelling; JSON Schema validators may also accept mathematically integral decimal or exponent tokens.",
    );
    for property in ["max_rows", "max_logical_bytes"] {
        for token in ["1.0", "1e0"] {
            let mathematically_integral = exact_json(token);
            assert_valid(
                &document,
                &result_limits["properties"][property],
                &mathematically_integral,
                &format!("JSON Schema integral {property} token {token}"),
            );
            assert_valid(
                &document,
                result_limits,
                &Value::Object(Map::from_iter([(
                    property.to_owned(),
                    mathematically_integral,
                )])),
                &format!("result limits schema with integral {property} token {token}"),
            );
        }
    }

    for path in [
        "/v1/execute",
        "/v1/query",
        "/v1/query/stream",
        "/v1/admin/broadcast",
        "/v1/admin/maintenance/checkpoint",
    ] {
        let request_body = resolve(
            &document,
            &operation(&document, path, "post")["requestBody"],
        );
        assert_eq!(
            request_body["required"], true,
            "POST {path} requires a body"
        );
        assert_eq!(
            object(&request_body["content"], "request content")
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            ["application/json"].into_iter().collect(),
            "POST {path} has the wrong documented request media types"
        );
        assert_eq!(
            operation(&document, path, "post")["x-briskdb-max-request-bytes"],
            2_097_152,
            "POST {path} must expose the raw request ceiling"
        );
    }

    for (schema, instance, context) in [
        (execute, json!({"sql": "DELETE FROM notes"}), "execute"),
        (
            query,
            json!({
                "shard_key": "tenant-a",
                "sql": "SELECT ?1",
                "params": [1, {"arbitrary": [true, null]}],
                "value_encoding": "legacy-json-v1",
                "result_limits": {"max_rows": 1}
            }),
            "query",
        ),
        (
            stream,
            json!({"sql": "SELECT 1", "result_limits": {"max_logical_bytes": 128}}),
            "stream query",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": {"max_rows": 1_000_000, "max_logical_bytes": 1_073_741_824}}),
            "maximum query limits",
        ),
        (
            broadcast,
            json!({"sql": "CREATE TABLE t (id INTEGER)"}),
            "broadcast",
        ),
        (checkpoint, json!({}), "checkpoint"),
    ] {
        assert_valid(&document, schema, &instance, context);
    }

    for (schema, instance, context) in [
        (execute, json!({}), "execute missing sql"),
        (
            execute,
            json!({"sql": "DELETE FROM notes", "result_limits": {"max_rows": 1}}),
            "execute-only field",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": null}),
            "null result limits",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": {}}),
            "empty result limits",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": {"max_rows": null}}),
            "null row limit",
        ),
        (
            query,
            json!({"sql": "SELECT ?1", "params": [1], "value_encoding": "lossless-json-v1"}),
            "native number in lossless query",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": {"max_rows": 1_000_001}}),
            "row limit above the engine maximum",
        ),
        (
            query,
            json!({"sql": "SELECT 1", "result_limits": {"max_logical_bytes": 1_073_741_825_u64}}),
            "byte limit above the engine maximum",
        ),
        (
            broadcast,
            json!({"sql": "CREATE TABLE t (id INTEGER)", "unknown": true}),
            "unknown broadcast field",
        ),
        (checkpoint, json!({"force": true}), "nonempty checkpoint"),
    ] {
        assert_invalid(&document, schema, &instance, context);
    }

    let boundary_request = json!({
        "sql": "SELECT ?1, ?2, ?3",
        "params": [{
            "nested": [
                exact_json("-9223372036854775808"),
                exact_json("18446744073709551615"),
                exact_json("1.7976931348623157e308")
            ]
        }]
    });
    assert_valid(
        &document,
        query,
        &boundary_request,
        "nested legacy parameter with supported numeric boundaries",
    );

    let parameter_number = component(&document, "LegacyJsonParameterNumber");
    assert_eq!(parameter_number["type"], "number");
    assert_eq!(
        parameter_number["exclusiveMinimum"],
        exact_json(&format!("-{LEGACY_REQUEST_ROUNDING_LIMIT}")),
    );
    assert_eq!(
        parameter_number["exclusiveMaximum"],
        exact_json(LEGACY_REQUEST_ROUNDING_LIMIT),
    );
    assert!(parameter_number.get("minimum").is_none());
    assert!(parameter_number.get("maximum").is_none());

    let accepted_parameter_tokens = [
        "1.7976931348623158e308".to_owned(),
        "-1.7976931348623158e308".to_owned(),
        format!("{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0"),
        format!("-{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0"),
    ];
    for token in accepted_parameter_tokens {
        let number = exact_json(&token);
        assert_valid(
            &document,
            parameter_number,
            &number,
            &format!("legacy request number just inside the binary64 rounding limit: {token}"),
        );
        for (schema, request_name) in [(execute, "execute"), (query, "query")] {
            assert_valid(
                &document,
                schema,
                &json!({"sql": "SELECT ?1", "params": [{"nested": [number.clone()]}]}),
                &format!("nested {request_name} parameter just inside the rounding limit: {token}"),
            );
        }
    }

    let rejected_parameter_tokens = [
        "1.7976931348623159e308".to_owned(),
        "-1.7976931348623159e308".to_owned(),
        format!("{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
        format!("-{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
    ];
    for token in rejected_parameter_tokens {
        let number = exact_json(&token);
        assert_invalid(
            &document,
            parameter_number,
            &number,
            &format!("legacy request number at or beyond the binary64 rounding limit: {token}"),
        );
        for (schema, request_name) in [(execute, "execute"), (query, "query")] {
            assert_invalid(
                &document,
                schema,
                &json!({"sql": "SELECT ?1", "params": [{"nested": [number.clone()]}]}),
                &format!("nested {request_name} parameter at the rounding limit: {token}"),
            );
        }
    }

    let overflowing_float_request = json!({
        "sql": "SELECT ?1",
        "params": [{"nested": [exact_json("1e400")]}]
    });
    assert_invalid(
        &document,
        query,
        &overflowing_float_request,
        "nested legacy float outside the finite f64 range",
    );

    assert_eq!(
        resolve(&document, query),
        resolve(&document, stream),
        "materialized and streaming queries accept one envelope"
    );
    assert!(
        operation(&document, "/v1/admin/queries/{operation_id}/cancel", "post",)
            .get("requestBody")
            .is_none(),
        "cancel accepts an exact empty byte body, not a JSON envelope"
    );
    assert_eq!(
        operation(&document, "/v1/admin/queries/{operation_id}/cancel", "post",)["x-briskdb-request-body"],
        json!({"mode": "exactly-zero-bytes", "maxBytes": 2_097_152})
    );
}

#[test]
fn header_and_path_parameter_schemas_capture_the_parsed_grammar() {
    let document = document();
    let request_id = parameter(&document, "/v1/query", "post", "BriskDB-Request-ID");
    assert_eq!(request_id["in"], "header");
    assert!(!request_id["required"].as_bool().unwrap_or(false));
    let request_id_schema = &request_id["schema"];
    assert_valid(
        &document,
        request_id_schema,
        &json!("0123456789abcdef0123456789abcdef"),
        "request ID",
    );
    for invalid in [
        json!("00000000000000000000000000000000"),
        json!("0123456789ABCDEF0123456789ABCDEF"),
        json!("short"),
        json!(null),
    ] {
        assert_invalid(&document, request_id_schema, &invalid, "invalid request ID");
    }

    let idempotency = parameter(&document, "/v1/execute", "post", "BriskDB-Idempotency-Key");
    assert_eq!(idempotency["in"], "header");
    assert!(!idempotency["required"].as_bool().unwrap_or(false));
    assert_valid(
        &document,
        &idempotency["schema"],
        &json!("fedcba9876543210fedcba9876543210"),
        "idempotency key",
    );
    for invalid in [
        json!("00000000000000000000000000000000"),
        json!("FEDCBA9876543210FEDCBA9876543210"),
        json!("fedcba9876543210fedcba987654321"),
    ] {
        assert_invalid(
            &document,
            &idempotency["schema"],
            &invalid,
            "invalid idempotency key",
        );
    }
    for &(path, methods, _) in ROUTES {
        for &method in methods {
            if path == "/v1/execute" && method == "post" {
                continue;
            }
            let names = operation(&document, path, method)["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .map(|parameter| resolve(&document, parameter)["name"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert!(
                !names.contains("BriskDB-Idempotency-Key"),
                "{method} {path} must not advertise execute idempotency"
            );
        }
    }

    for (path, name, valid, invalid) in [
        (
            "/v1/admin/migrations/{target_generation}",
            "target_generation",
            "18446744073709551615",
            ["", "0", "01", "+1", "18446744073709551616"],
        ),
        (
            "/v1/admin/queries/{operation_id}/cancel",
            "operation_id",
            "0123456789abcdef0123456789abcdef",
            [
                "",
                "00000000000000000000000000000000",
                "0123456789ABCDEF0123456789ABCDEF",
                "short",
                "0123456789abcdef0123456789abcdef0",
            ],
        ),
    ] {
        let parameter = parameter(
            &document,
            path,
            if name == "operation_id" {
                "post"
            } else {
                "get"
            },
            name,
        );
        assert_eq!(parameter["in"], "path");
        assert_eq!(parameter["required"], true);
        assert_valid(
            &document,
            &parameter["schema"],
            &json!(valid),
            &format!("valid {name}"),
        );
        for invalid in invalid {
            assert_invalid(
                &document,
                &parameter["schema"],
                &json!(invalid),
                &format!("invalid {name}"),
            );
        }
    }

    let api_version_header = resolve(
        &document,
        &response(&document, "/v1/query", "post", "200")["headers"]["BriskDB-API-Version"],
    );
    assert_valid(
        &document,
        &api_version_header["schema"],
        &json!("1"),
        "API version response header",
    );
    assert_invalid(
        &document,
        &api_version_header["schema"],
        &json!("2"),
        "wrong API version response header",
    );

    let idempotency_status = resolve(
        &document,
        &response(&document, "/v1/execute", "post", "200")["headers"]["BriskDB-Idempotency-Status"],
    );
    for status in ["created", "replayed"] {
        assert_valid(
            &document,
            &idempotency_status["schema"],
            &json!(status),
            "idempotency status response header",
        );
    }
    assert_invalid(
        &document,
        &idempotency_status["schema"],
        &json!("conflicted"),
        "unknown idempotency status response header",
    );
}

#[test]
fn value_and_conditional_response_schemas_preserve_wire_shapes() {
    let document = document();
    assert_valid(
        &document,
        component(&document, "QueryColumn"),
        &json!({"name": "expression", "data_type": "unknown"}),
        "unknown expression column type",
    );
    let query = component(&document, "QueryResponse");
    let base = json!({
        "columns": [{"name": "same", "data_type": "binary"}, {"name": "same", "data_type": "int64"}],
        "rows": [[{"$briskdb_type": "binary", "value": "AP8="}, {"$briskdb_type": "int64", "value": "9007199254740993"}]],
        "value_encoding": "lossless-json-v1"
    });
    let mut single = base.clone();
    single["shard"] = json!(0);
    assert_valid(&document, query, &single, "single-shard query response");
    let mut scatter = base.clone();
    scatter["shards"] = json!([0, 1]);
    assert_valid(&document, query, &scatter, "scatter query response");
    let mut additive_response = single.clone();
    additive_response["future_top_level"] = json!({"opaque": true});
    additive_response["columns"][0]["future_column_field"] = json!(17);
    assert_valid(
        &document,
        query,
        &additive_response,
        "additive query response fields",
    );
    let mut both = base.clone();
    both["shard"] = json!(0);
    both["shards"] = json!([0, 1]);
    assert_invalid(&document, query, &both, "ambiguous query shard shape");
    assert_invalid(&document, query, &base, "missing query shard shape");

    let legacy_response = json!({
        "shard": 0,
        "columns": [{"name": "value", "data_type": "unknown"}],
        "rows": [[1], [[0, 255]], ["compact object parameter returns as text"]]
    });
    assert_valid(
        &document,
        query,
        &legacy_response,
        "legacy query response with arbitrary JSON values",
    );
    let mut named_legacy_response = legacy_response.clone();
    named_legacy_response["value_encoding"] = json!("legacy-json-v1");
    assert_invalid(
        &document,
        query,
        &named_legacy_response,
        "legacy response must omit value_encoding",
    );
    let mut missing_lossless_encoding = single.clone();
    missing_lossless_encoding
        .as_object_mut()
        .unwrap()
        .remove("value_encoding");
    assert_invalid(
        &document,
        query,
        &missing_lossless_encoding,
        "lossless response without its encoding marker",
    );
    let mut numeric_lossless = single.clone();
    numeric_lossless["rows"] = json!([[1, 2]]);
    assert_invalid(
        &document,
        query,
        &numeric_lossless,
        "native numeric cells in a lossless response",
    );

    let legacy_result_number = component(&document, "LegacyJsonNumber");
    assert_eq!(legacy_result_number["type"], "number");
    assert_eq!(
        legacy_result_number["minimum"],
        exact_json("-1.7976931348623157e308"),
    );
    assert_eq!(
        legacy_result_number["maximum"],
        exact_json("1.7976931348623157e308"),
    );
    assert!(legacy_result_number.get("exclusiveMinimum").is_none());
    assert!(legacy_result_number.get("exclusiveMaximum").is_none());
    for token in ["1.7976931348623157e308", "-1.7976931348623157e308"] {
        assert_valid(
            &document,
            legacy_result_number,
            &exact_json(token),
            "legacy result number at the largest emitted binary64 magnitude",
        );
    }
    for token in ["1.7976931348623158e308", "-1.7976931348623158e308"] {
        assert_invalid(
            &document,
            legacy_result_number,
            &exact_json(token),
            "legacy result number beyond the largest emitted binary64 magnitude",
        );
    }

    let legacy_cell = component(&document, "LegacyJsonCell");
    for valid in [
        Value::Null,
        json!(true),
        json!(1),
        json!(1.5),
        json!("text"),
        json!([]),
        json!([0, 255]),
    ] {
        assert_valid(&document, legacy_cell, &valid, "legacy response cell");
    }
    for invalid in [
        json!([256]),
        json!([-1]),
        json!([[1]]),
        json!({"arbitrary": true}),
    ] {
        assert_invalid(
            &document,
            legacy_cell,
            &invalid,
            "invalid legacy response cell",
        );
    }
    for (number, context) in [
        (
            exact_json("-9223372036854775808"),
            "legacy cell at i64::MIN",
        ),
        (
            exact_json("18446744073709551615"),
            "legacy cell at u64::MAX",
        ),
        (
            exact_json("1.7976931348623157e308"),
            "legacy cell at maximum finite f64",
        ),
        (
            exact_json("-1.7976931348623157e308"),
            "legacy cell at minimum finite f64",
        ),
    ] {
        assert_valid(&document, legacy_cell, &number, context);
    }
    for token in [
        "1.7976931348623158e308",
        "-1.7976931348623158e308",
        "1e400",
        "-1e400",
    ] {
        assert_invalid(
            &document,
            legacy_cell,
            &exact_json(token),
            "legacy cell float outside the emitted finite f64 range",
        );
    }
    let stream_row = component(&document, "QueryStreamRow");
    for valid in [
        json!({"kind": "row", "values": [null, true, 1, "text", [0, 255]]}),
        json!({"kind": "row", "values": [{"$briskdb_type": "int64", "value": "1"}]}),
    ] {
        assert_valid(&document, stream_row, &valid, "stream row values");
    }
    for invalid in [
        json!({"kind": "row", "values": [{"arbitrary": true}]}),
        json!({"kind": "row", "values": [[[1]]]}),
        json!({"kind": "row", "values": [1], "future": true}),
    ] {
        assert_invalid(&document, stream_row, &invalid, "invalid stream row value");
    }
    let mut nested_legacy_response = legacy_response.clone();
    nested_legacy_response["rows"] = json!([[{"arbitrary": true}]]);
    assert_invalid(
        &document,
        query,
        &nested_legacy_response,
        "object in a legacy query response",
    );

    let legacy_value = component(&document, "LegacyJsonValue");
    for valid in [
        Value::Null,
        json!(true),
        json!(17),
        json!(1.5),
        json!("text"),
        json!([1, {"nested": true}]),
        json!({"arbitrary": [null]}),
    ] {
        assert_valid(&document, legacy_value, &valid, "legacy JSON value");
    }
    for (number, context) in [
        (
            exact_json("-9223372036854775808"),
            "legacy value at i64::MIN",
        ),
        (
            exact_json("18446744073709551615"),
            "legacy value at u64::MAX",
        ),
        (
            exact_json("1.7976931348623158e308"),
            "legacy parameter value that rounds to maximum finite f64",
        ),
        (
            exact_json("-1.7976931348623158e308"),
            "legacy parameter value that rounds to minimum finite f64",
        ),
        (
            exact_json(&format!("{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0")),
            "legacy parameter value just inside the positive rounding limit",
        ),
        (
            exact_json(&format!("-{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0")),
            "legacy parameter value just inside the negative rounding limit",
        ),
    ] {
        assert_valid(&document, legacy_value, &number, context);
    }
    for token in [
        "1.7976931348623159e308".to_owned(),
        "-1.7976931348623159e308".to_owned(),
        format!("{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
        format!("-{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
        "1e400".to_owned(),
        "-1e400".to_owned(),
    ] {
        assert_invalid(
            &document,
            legacy_value,
            &exact_json(&token),
            "legacy parameter value outside the finite binary64 rounding range",
        );
    }
    assert_eq!(
        component(&document, "LegacyJsonParameter")["x-briskdb-number-token-limit"],
        "JSON Schema sees numeric value, while the decoder also distinguishes integer tokens from fractional or exponent tokens.",
        "the schema must document its lexical-number validation limit",
    );

    let value = component(&document, "LosslessJsonValue");
    for valid in [
        Value::Null,
        json!(true),
        json!("text"),
        json!({"$briskdb_type": "uint64", "value": "18446744073709551615"}),
        json!({"$briskdb_type": "int64", "value": "9223372036854775807"}),
        json!({"$briskdb_type": "int64", "value": "-9223372036854775808"}),
        json!({"$briskdb_type": "float64", "value": "7ff8000000000000"}),
        json!({"$briskdb_type": "decimal", "value": "12.3400"}),
        json!({"$briskdb_type": "invalid_text", "value": "/w=="}),
    ] {
        assert_valid(&document, value, &valid, "BriskDB value");
    }
    for invalid in [
        json!(17),
        json!([]),
        json!({}),
        json!({"$briskdb_type": "binary"}),
        json!({"$briskdb_type": "unknown", "value": "x"}),
        json!({"$briskdb_type": "binary", "value": "AA==", "extra": true}),
        json!({"$briskdb_type": "int64", "value": "01"}),
        json!({"$briskdb_type": "int64", "value": "-0"}),
        json!({"$briskdb_type": "int64", "value": "9223372036854775808"}),
        json!({"$briskdb_type": "uint64", "value": "18446744073709551616"}),
        json!({"$briskdb_type": "float64", "value": "7FF8000000000000"}),
        json!({"$briskdb_type": "decimal", "value": "."}),
        json!({"$briskdb_type": "binary", "value": "not base64"}),
    ] {
        assert_invalid(&document, value, &invalid, "invalid BriskDB value");
    }
    for kind in ["binary", "invalid_text"] {
        for encoded in ["AA==", "AAA=", "/w==", "//8="] {
            assert_valid(
                &document,
                value,
                &json!({"$briskdb_type": kind, "value": encoded}),
                &format!("canonical {kind} Base64 with valid pad bits"),
            );
        }
        for encoded in ["AB==", "AAB=", "/x==", "//9="] {
            assert_invalid(
                &document,
                value,
                &json!({"$briskdb_type": kind, "value": encoded}),
                &format!("noncanonical {kind} Base64 pad bits"),
            );
        }
    }

    let placement = component(&document, "CatalogPlacement");
    assert_valid(
        &document,
        placement,
        &json!({"kind": "sharded", "shard_key": {"column": "tenant", "data_type": "text"}}),
        "sharded catalog placement",
    );
    assert_valid(
        &document,
        placement,
        &json!({"kind": "global"}),
        "global catalog placement",
    );
    assert_invalid(
        &document,
        placement,
        &json!({"kind": "sharded"}),
        "sharded placement without key",
    );
    assert_invalid(
        &document,
        placement,
        &json!({"kind": "global", "shard_key": {"column": "tenant", "data_type": "text"}}),
        "global placement with key",
    );

    for (component_name, property) in [
        ("CatalogDb", "id"),
        ("CatalogTable", "id"),
        ("CatalogTable", "database_id"),
        ("CatalogGlobalIndex", "id"),
        ("CatalogGlobalIndex", "table_id"),
        ("GlobalIndexStatus", "id"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        for valid in ["1", "18446744073709551615"] {
            assert_valid(
                &document,
                schema,
                &json!(valid),
                &format!("positive {component_name}.{property}"),
            );
        }
        for invalid in ["0", "01", "18446744073709551616"] {
            assert_invalid(
                &document,
                schema,
                &json!(invalid),
                &format!("invalid persisted identity {component_name}.{property}"),
            );
        }
    }
    for (component_name, property) in [
        ("CatalogResponse", "schema_generation"),
        ("CatalogGlobalIndex", "schema_generation"),
        ("MigrationsResponse", "schema_generation"),
        ("MigrationResponse", "source_generation"),
        ("ShardStatusResponse", "schema_generation"),
        ("ReadyResponse", "schema_generation"),
        ("NotReadyResponse", "schema_generation"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        for valid in ["0", "2147483647"] {
            assert_valid(
                &document,
                schema,
                &json!(valid),
                &format!("zero-capable {component_name}.{property}"),
            );
        }
        for invalid in ["00", "2147483648"] {
            assert_invalid(
                &document,
                schema,
                &json!(invalid),
                &format!("invalid generation {component_name}.{property}"),
            );
        }
    }
    for property in ["generation", "target_generation"] {
        let schema = &component(&document, "MigrationResponse")["properties"][property];
        for valid in ["1", "2147483647"] {
            assert_valid(
                &document,
                schema,
                &json!(valid),
                &format!("positive MigrationResponse.{property}"),
            );
        }
        for invalid in ["0", "01", "2147483648"] {
            assert_invalid(
                &document,
                schema,
                &json!(invalid),
                &format!("invalid MigrationResponse.{property}"),
            );
        }
    }
    assert_eq!(
        component(&document, "CatalogResponse")["properties"]["default_database_id"],
        json!({"type": "string", "const": "1"}),
        "the catalog default database identity is fixed by the v1 store contract",
    );

    let catalog_properties = &component(&document, "CatalogResponse")["properties"];
    assert_eq!(catalog_properties["databases"]["type"], "array");
    assert_eq!(catalog_properties["databases"]["minItems"], 1);
    assert_eq!(catalog_properties["databases"]["maxItems"], 64);
    assert_valid(
        &document,
        &catalog_properties["databases"],
        &json!([{"id": "1", "name": "default"}]),
        "catalog database list containing the fixed default database",
    );
    assert_invalid(
        &document,
        &catalog_properties["databases"],
        &json!([]),
        "catalog database list without any database",
    );
    assert_invalid(
        &document,
        &catalog_properties["databases"],
        &json!([{"id": "2", "name": "other"}]),
        "catalog database list without the fixed default database",
    );
    for property in ["tables", "global_indexes"] {
        assert_eq!(catalog_properties[property]["type"], "array");
        assert_eq!(catalog_properties[property]["maxItems"], 4_096);
    }
    assert_eq!(
        component(&document, "GlobalIndexesResponse")["properties"]["indexes"]["maxItems"],
        4_096,
    );

    for property in ["total", "healthy", "degraded", "unavailable"] {
        let schema = &component(&document, "HealthGlobalIndexesResponse")["properties"][property];
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 0);
        assert_eq!(schema["maximum"], 4_096);
        assert_valid(
            &document,
            schema,
            &json!(4_096),
            &format!("HealthGlobalIndexesResponse.{property} at the catalog index limit"),
        );
        assert_invalid(
            &document,
            schema,
            &json!(4_097),
            &format!("HealthGlobalIndexesResponse.{property} above the catalog index limit"),
        );
    }

    let migration_sql_bytes = &component(&document, "MigrationResponse")["properties"]["sql_bytes"];
    assert_eq!(migration_sql_bytes["type"], "integer");
    assert_eq!(migration_sql_bytes["minimum"], 1);
    assert_eq!(migration_sql_bytes["maximum"], 65_536);
    for valid in [1, 65_536] {
        assert_valid(
            &document,
            migration_sql_bytes,
            &json!(valid),
            "migration SQL byte length",
        );
    }
    for invalid in [0, 65_537] {
        assert_invalid(
            &document,
            migration_sql_bytes,
            &json!(invalid),
            "invalid migration SQL byte length",
        );
    }

    let not_ready_branches = component(&document, "NotReadyResponse")["oneOf"]
        .as_array()
        .expect("NotReadyResponse must enumerate its reachable state pairs");
    assert_eq!(not_ready_branches.len(), 11);
    assert_eq!(
        component(&document, "NotReadyResponse")["properties"]["reasons"]["maxItems"],
        2,
        "at most one engine and one schema reason can be emitted",
    );

    for component_name in ["GlobalIndexesResponse", "HealthGlobalIndexesResponse"] {
        for (property, maximum) in [
            ("retained_outbox_events", 64_000_000_u64),
            ("retained_outbox_bytes", 17_179_869_184_u64),
        ] {
            let schema = &component(&document, component_name)["properties"][property];
            assert_eq!(schema["minimum"], 0);
            assert_eq!(schema["maximum"], maximum);
            assert_valid(
                &document,
                schema,
                &json!(maximum),
                &format!("{component_name}.{property} at its retention limit"),
            );
            assert_invalid(
                &document,
                schema,
                &json!(maximum + 1),
                &format!("{component_name}.{property} above its retention limit"),
            );
        }
    }
    for (component_name, property) in [
        ("GlobalIndexStatus", "async_lag"),
        ("HealthGlobalIndexesResponse", "async_lag"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        assert_eq!(schema["maximum"], u64::MAX);
        assert_valid(
            &document,
            schema,
            &json!(u64::MAX),
            &format!("{component_name}.{property} at the u64 lifetime-watermark limit"),
        );
    }
    for (component_name, pointer) in [
        ("ActiveQueryResponse", "/properties/sql_bytes"),
        ("ExecuteResponse", "/oneOf/0/properties/rows_affected"),
        ("ExecuteResponse", "/oneOf/1/properties/rows_affected"),
        ("ReadyResponse", "/properties/active_schema_operations"),
        ("NotReadyResponse", "/properties/active_schema_operations"),
    ] {
        let schema = component(&document, component_name)
            .pointer(pointer)
            .unwrap_or_else(|| panic!("missing usize schema {component_name}{pointer}"));
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 0);
        assert_eq!(schema["maximum"], u64::MAX);
        assert_valid(
            &document,
            schema,
            &json!(u64::MAX),
            &format!("{component_name}{pointer} at the platform-independent wire maximum"),
        );
        assert_invalid(
            &document,
            schema,
            &exact_json("18446744073709551616"),
            &format!("{component_name}{pointer} above the u64 wire maximum"),
        );
    }

    for (component_name, pointer) in [
        ("ExecuteResponse", "/oneOf/0/properties/shard"),
        ("ExecuteResponse", "/oneOf/1/properties/shard"),
        ("LegacySingleShardQueryResponse", "/properties/shard"),
        ("LosslessSingleShardQueryResponse", "/properties/shard"),
        ("LegacyMultiShardQueryResponse", "/properties/shards/items"),
        (
            "LosslessMultiShardQueryResponse",
            "/properties/shards/items",
        ),
        ("LegacySingleShardStreamMeta", "/properties/shard"),
        ("LosslessSingleShardStreamMeta", "/properties/shard"),
        ("LegacyMultiShardStreamMeta", "/properties/shards/items"),
        ("LosslessMultiShardStreamMeta", "/properties/shards/items"),
        ("BroadcastResponse", "/properties/completed_shards/items"),
        ("ShardStatus", "/properties/id"),
        ("CheckpointShardResponse", "/properties/shard"),
    ] {
        let schema = component(&document, component_name)
            .pointer(pointer)
            .unwrap_or_else(|| panic!("missing shard ID schema {component_name}{pointer}"));
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 0);
        assert_eq!(schema["maximum"], 63);
        assert_valid(
            &document,
            schema,
            &json!(63),
            &format!("{component_name}{pointer} at the maximum physical shard ID"),
        );
        assert_invalid(
            &document,
            schema,
            &json!(64),
            &format!("{component_name}{pointer} beyond the maximum physical shard ID"),
        );
    }

    for (component_name, property) in [
        ("HealthGlobalIndexesResponse", "backpressured_outbox_shards"),
        ("GlobalIndexesResponse", "backpressured_outbox_shards"),
        ("GlobalIndexStatus", "poisoned_shards"),
        ("GlobalIndexStatus", "leased_shards"),
        ("GlobalIndexStatus", "summary_ready_shards"),
        ("GlobalIndexStatus", "summary_degraded_shards"),
        ("GlobalIndexStatus", "summary_saturated_shards"),
        ("MigrationResponse", "next_shard"),
        ("MigrationResponse", "completed_shards"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 0);
        assert_eq!(schema["maximum"], 64);
        assert_valid(
            &document,
            schema,
            &json!(64),
            &format!("{component_name}.{property} at the physical shard count limit"),
        );
        assert_invalid(
            &document,
            schema,
            &json!(65),
            &format!("{component_name}.{property} above the physical shard count limit"),
        );
    }

    for (component_name, property) in [
        ("HealthResponse", "shards"),
        ("MigrationResponse", "shard_count"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 2);
        assert_eq!(schema["maximum"], 64);
        for valid in [2, 64] {
            assert_valid(
                &document,
                schema,
                &json!(valid),
                &format!("valid {component_name}.{property}"),
            );
        }
        for invalid in [1, 65] {
            assert_invalid(
                &document,
                schema,
                &json!(invalid),
                &format!("invalid {component_name}.{property}"),
            );
        }
    }

    for (component_name, property) in [
        ("LegacyMultiShardQueryResponse", "shards"),
        ("LosslessMultiShardQueryResponse", "shards"),
        ("LegacyMultiShardStreamMeta", "shards"),
        ("LosslessMultiShardStreamMeta", "shards"),
        ("BroadcastResponse", "completed_shards"),
        ("ShardStatusResponse", "shards"),
        ("CheckpointResponse", "shards"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["minItems"], 2);
        assert_eq!(schema["maxItems"], 64);
        assert_eq!(schema["uniqueItems"], true);
    }

    let all_shard_ids = Value::Array((0..64).map(|shard| json!(shard)).collect());
    for (component_name, property) in [
        ("LegacyMultiShardQueryResponse", "shards"),
        ("LosslessMultiShardQueryResponse", "shards"),
        ("LegacyMultiShardStreamMeta", "shards"),
        ("LosslessMultiShardStreamMeta", "shards"),
        ("BroadcastResponse", "completed_shards"),
    ] {
        let schema = &component(&document, component_name)["properties"][property];
        assert_valid(
            &document,
            schema,
            &all_shard_ids,
            &format!("{component_name}.{property} with all 64 physical shards"),
        );
        assert_invalid(
            &document,
            schema,
            &json!([0]),
            &format!("{component_name}.{property} below its multi-shard minimum"),
        );
    }

    for component_name in [
        "LegacySingleShardQueryResponse",
        "LegacyMultiShardQueryResponse",
        "LosslessSingleShardQueryResponse",
        "LosslessMultiShardQueryResponse",
    ] {
        let rows = &component(&document, component_name)["properties"]["rows"];
        assert_eq!(rows["type"], "array");
        assert_eq!(rows["maxItems"], 1_000_000);
    }
    let completed_rows = &component(&document, "QueryStreamComplete")["properties"]["rows"];
    assert_eq!(completed_rows["type"], "integer");
    assert_eq!(completed_rows["minimum"], 0);
    assert_eq!(completed_rows["maximum"], 1_000_000);
    assert_valid(
        &document,
        completed_rows,
        &json!(1_000_000),
        "stream completion at the result row limit",
    );
    assert_invalid(
        &document,
        completed_rows,
        &json!(1_000_001),
        "stream completion above the result row limit",
    );

    let checkpoint_databases =
        &component(&document, "CheckpointResponse")["properties"]["databases"];
    assert_eq!(checkpoint_databases["type"], "array");
    assert_eq!(checkpoint_databases["minItems"], 1);
    assert_eq!(checkpoint_databases["maxItems"], 2);
    assert_eq!(checkpoint_databases["uniqueItems"], true);
    let shard_statuses = Value::Array(
        (0..64)
            .map(|id| json!({"id": id, "state": "ready"}))
            .collect(),
    );
    let shard_statuses_schema =
        &component(&document, "ShardStatusResponse")["properties"]["shards"];
    assert_valid(
        &document,
        shard_statuses_schema,
        &shard_statuses,
        "status response with all 64 physical shards",
    );
    assert_invalid(
        &document,
        shard_statuses_schema,
        &json!([{"id": 0, "state": "ready"}]),
        "status response below the physical shard minimum",
    );

    let checkpoint_shards = Value::Array(
        (0..64)
            .map(|shard| {
                json!({
                    "shard": shard,
                    "busy": false,
                    "counts_available": true,
                    "wal_frames": 0,
                    "checkpointed_frames": 0,
                    "complete": true
                })
            })
            .collect(),
    );
    let checkpoint_shards_schema =
        &component(&document, "CheckpointResponse")["properties"]["shards"];
    assert_valid(
        &document,
        checkpoint_shards_schema,
        &checkpoint_shards,
        "checkpoint response with all 64 physical shards",
    );
    assert_invalid(
        &document,
        checkpoint_shards_schema,
        &json!([{
            "shard": 0,
            "busy": false,
            "counts_available": true,
            "wal_frames": 0,
            "checkpointed_frames": 0,
            "complete": true
        }]),
        "checkpoint response below the physical shard minimum",
    );
    assert_valid(
        &document,
        checkpoint_databases,
        &json!([
            {
                "database": "manifest",
                "busy": false,
                "counts_available": true,
                "wal_frames": 0,
                "checkpointed_frames": 0,
                "complete": true
            },
            {
                "database": "global_index",
                "busy": false,
                "counts_available": true,
                "wal_frames": 0,
                "checkpointed_frames": 0,
                "complete": true
            }
        ]),
        "checkpoint response with both database families",
    );
    assert_invalid(
        &document,
        checkpoint_databases,
        &json!([]),
        "checkpoint response without a database result",
    );
    for (component_name, property) in [
        ("CatalogResponse", "identifier_encoding_version"),
        ("CatalogGeneratedIdEnabled", "encoding_version"),
        ("CatalogGlobalIndex", "key_encoding_version"),
    ] {
        assert_eq!(
            component(&document, component_name)["properties"][property],
            json!({"type": "integer", "const": 1}),
            "{component_name}.{property} is a frozen v1 encoding"
        );
    }

    let checkpoint_database = component(&document, "CheckpointDbResponse");
    for database in ["manifest", "global_index"] {
        assert_valid(
            &document,
            checkpoint_database,
            &json!({
                "database": database,
                "busy": false,
                "counts_available": true,
                "wal_frames": 0,
                "checkpointed_frames": 0,
                "complete": true
            }),
            "checkpoint database result",
        );
    }
    assert_invalid(
        &document,
        checkpoint_database,
        &json!({
            "database": "catalog",
            "busy": false,
            "counts_available": true,
            "wal_frames": 0,
            "checkpointed_frames": 0,
            "complete": true
        }),
        "invented checkpoint database name",
    );

    let generated_key = component(&document, "ExecuteGeneratedKey");
    for valid in [
        json!({"column": "id", "data_type": "int64", "value": "-9223372036854775808"}),
        json!({"column": "id", "data_type": "uint64", "value": "18446744073709551615"}),
    ] {
        assert_valid(&document, generated_key, &valid, "generated key");
    }
    for invalid in [
        json!({"column": "id", "data_type": "uint64", "value": "-1"}),
        json!({"column": "id", "data_type": "int64", "value": "01"}),
    ] {
        assert_invalid(&document, generated_key, &invalid, "invalid generated key");
    }
}

#[test]
fn response_headers_problems_head_and_stream_contract_are_explicit() {
    let document = document();

    for &(path, methods, _) in ROUTES {
        for &method in methods {
            let operation = operation(&document, path, method);
            let request_headers = operation["parameters"]
                .as_array()
                .unwrap_or_else(|| panic!("{method} {path} must declare request headers"))
                .iter()
                .map(|parameter| resolve(&document, parameter))
                .filter(|parameter| parameter["in"] == "header")
                .filter_map(|parameter| parameter["name"].as_str())
                .collect::<BTreeSet<_>>();
            assert!(
                request_headers.contains("BriskDB-Request-ID"),
                "{method} {path} omits the request identity header"
            );
            assert_eq!(
                request_headers.contains("BriskDB-Idempotency-Key"),
                path == "/v1/execute" && method == "post",
                "only exact POST /v1/execute accepts the idempotency header"
            );
            assert!(
                operation["responses"].get("501").is_some(),
                "{method} {path} must expose the global idempotency middleware rejection"
            );

            for (status, response) in object(&operation["responses"], "responses") {
                if method == "head" {
                    assert!(
                        response.get("$ref").is_none(),
                        "HEAD {path} {status} must materialize its response before removing content"
                    );
                } else if !(status.starts_with('2') || path == "/v1/ready" && status == "503") {
                    assert_eq!(
                        response["$ref"],
                        format!("#/components/responses/Problem{status}"),
                        "{method} {path} {status} must reuse its Problem component"
                    );
                }
                let response = resolve(&document, response);
                let headers = object(&response["headers"], "response headers");
                assert!(
                    headers.contains_key("BriskDB-Request-ID"),
                    "{method} {path} {status} omits BriskDB-Request-ID"
                );
                assert!(
                    headers.contains_key("BriskDB-API-Version"),
                    "{method} {path} {status} omits BriskDB-API-Version"
                );
            }
        }
    }

    let execute_headers = operation(&document, "/v1/execute", "post")["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|parameter| resolve(&document, parameter))
        .filter_map(|parameter| parameter["name"].as_str())
        .collect::<BTreeSet<_>>();
    assert!(execute_headers.contains("BriskDB-Idempotency-Key"));
    assert!(
        response(&document, "/v1/execute", "post", "200")["headers"]
            .get("BriskDB-Idempotency-Status")
            .is_some()
    );

    let content_length_schema = &document["components"]["headers"]["ContentLength"]["schema"];
    assert_valid(
        &document,
        content_length_schema,
        &json!(u64::MAX),
        "Content-Length at the platform-independent wire maximum",
    );
    assert_invalid(
        &document,
        content_length_schema,
        &exact_json("18446744073709551616"),
        "Content-Length above the u64 wire maximum",
    );

    for &(path, methods, _) in ROUTES {
        if methods == ["get", "head"] {
            let get_responses = object(
                &operation(&document, path, "get")["responses"],
                "GET responses",
            );
            let head_responses = object(
                &operation(&document, path, "head")["responses"],
                "HEAD responses",
            );
            assert_eq!(
                get_responses.keys().collect::<BTreeSet<_>>(),
                head_responses.keys().collect()
            );
            for (status, response) in head_responses {
                let get_response = resolve(&document, &get_responses[status]);
                let response = resolve(&document, response);
                assert!(
                    response.get("content").is_none(),
                    "HEAD {path} must describe no response body"
                );
                let headers = object(&response["headers"], "HEAD response headers");
                assert_eq!(
                    headers["Content-Length"],
                    json!({"$ref": "#/components/headers/ContentLength"}),
                    "HEAD {path} must reference the retained representation length"
                );
                assert_eq!(
                    resolve(&document, &headers["Content-Length"]),
                    &json!({
                        "description": "The size of the corresponding GET representation; a HEAD response carries no body.",
                        "required": true,
                        "schema": {"type": "integer", "minimum": 0, "maximum": u64::MAX}
                    }),
                    "HEAD {path} must require the retained representation length"
                );
                let get_content = object(&get_response["content"], "GET response content");
                assert_eq!(
                    get_content.len(),
                    1,
                    "GET {path} {status} must have one representation media type"
                );
                let expected_content_type = get_content.keys().next().unwrap();
                let content_type = resolve(
                    &document,
                    headers.get("Content-Type").unwrap_or_else(|| {
                        panic!("HEAD {path} {status} omits its representation Content-Type")
                    }),
                );
                assert_eq!(
                    content_type,
                    &json!({
                        "description": "Media type of the corresponding GET representation.",
                        "required": true,
                        "schema": {
                            "type": "string",
                            "const": expected_content_type
                        }
                    }),
                    "HEAD {path} {status} has the wrong representation Content-Type"
                );
            }
        }
    }

    let problem = resolve(
        &document,
        response_schema(
            &document,
            "/v1/query",
            "post",
            "400",
            "application/problem+json",
        ),
    );
    assert_eq!(problem["type"], "object");
    assert_eq!(problem["additionalProperties"], false);
    assert_eq!(
        problem["required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>(),
        ["code", "detail", "status", "title", "type"]
            .into_iter()
            .collect()
    );
    assert_eq!(
        object(&problem["properties"], "Problem properties")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        ["code", "detail", "status", "title", "type"]
            .into_iter()
            .collect()
    );
    let problem_code = resolve(&document, &problem["properties"]["code"]);
    assert_eq!(problem_code["type"], "string");
    assert!(
        problem_code.get("enum").is_none(),
        "Problem.code remains open to additive future error kinds"
    );
    let invalid_argument = http_error(EngineErrorKind::InvalidArgument);
    assert_invalid(
        &document,
        problem,
        &json!({
            "type": invalid_argument.problem_type,
            "title": invalid_argument.title,
            "status": invalid_argument.status,
            "detail": invalid_argument.detail,
            "code": EngineErrorKind::InvalidArgument.code(),
            "future": true
        }),
        "Problem with an extra member",
    );
    let stream_error_code = resolve(
        &document,
        &component(&document, "QueryStreamError")["properties"]["code"],
    );
    assert_eq!(stream_error_code["type"], "string");
    assert!(
        stream_error_code.get("enum").is_none(),
        "terminal stream error codes remain additive"
    );
    let problem_responses = object(
        &document["components"]["responses"],
        "reusable Problem responses",
    );
    for status in [
        400_u16, 403, 404, 405, 409, 413, 415, 422, 500, 501, 503, 504, 507,
    ] {
        let name = format!("Problem{status}");
        let response = resolve(
            &document,
            problem_responses
                .get(&name)
                .unwrap_or_else(|| panic!("missing reusable {name}")),
        );
        let headers = object(&response["headers"], "reusable Problem headers");
        if status == 405 {
            assert_eq!(
                headers["Allow"],
                json!({"$ref": "#/components/headers/Allow"})
            );
            assert_eq!(
                resolve(&document, &headers["Allow"]),
                &json!({
                    "description": "Methods accepted by the matched HTTP v1 route family.",
                    "required": true,
                    "schema": {"type": "string", "enum": ["GET,HEAD", "POST"]}
                })
            );
        } else {
            assert!(
                !headers.contains_key("Allow"),
                "{name} must not claim the status-specific Allow header"
            );
        }
    }
    for (component_name, instance) in [
        (
            "QueryStreamMeta",
            json!({"kind": "meta", "shard": 0, "columns": [], "future": true}),
        ),
        (
            "QueryStreamComplete",
            json!({"kind": "complete", "rows": 0, "future": true}),
        ),
        (
            "QueryStreamError",
            json!({
                "kind": "error",
                "type": invalid_argument.problem_type,
                "title": invalid_argument.title,
                "status": invalid_argument.status,
                "detail": invalid_argument.detail,
                "code": EngineErrorKind::InvalidArgument.code(),
                "future": true
            }),
        ),
    ] {
        assert_invalid(
            &document,
            component(&document, component_name),
            &instance,
            &format!("strict {component_name}"),
        );
    }

    let ready_schema = response_schema(&document, "/v1/ready", "get", "200", "application/json");
    let not_ready_schema =
        response_schema(&document, "/v1/ready", "get", "503", "application/json");
    assert_eq!(
        ready_schema,
        &json!({"$ref": "#/components/schemas/ReadyResponse"})
    );
    assert_eq!(
        not_ready_schema,
        &json!({"$ref": "#/components/schemas/NotReadyResponse"})
    );
    let ready = json!({
        "status": "ready",
        "ready": true,
        "reasons": [],
        "engine_state": "running",
        "schema_state": "ready",
        "schema_generation": "0",
        "active_schema_operations": 0
    });
    let not_ready = json!({
        "status": "not_ready",
        "ready": false,
        "reasons": ["engine_draining"],
        "engine_state": "draining",
        "schema_state": "ready",
        "schema_generation": "0",
        "active_schema_operations": 0
    });
    assert_valid(&document, ready_schema, &ready, "200 readiness response");
    assert_valid(
        &document,
        not_ready_schema,
        &not_ready,
        "503 readiness response",
    );
    assert_invalid(
        &document,
        ready_schema,
        &not_ready,
        "503 readiness body under the 200 schema",
    );
    assert_invalid(
        &document,
        not_ready_schema,
        &ready,
        "200 readiness body under the 503 schema",
    );

    assert_eq!(
        component(&document, "NotReadyResponse")["oneOf"]
            .as_array()
            .expect("NotReadyResponse must enumerate its reachable state pairs")
            .len(),
        11,
    );
    for (engine_state, engine_reason) in [
        ("running", None),
        ("draining", Some("engine_draining")),
        ("stopped", Some("engine_stopped")),
    ] {
        for (schema_state, schema_reason) in [
            ("ready", None),
            ("migrating", Some("schema_migrating")),
            ("pending", Some("schema_recovery_pending")),
            ("degraded", Some("schema_degraded")),
        ] {
            if engine_reason.is_none() && schema_reason.is_none() {
                continue;
            }
            let reasons = engine_reason
                .into_iter()
                .chain(schema_reason)
                .collect::<Vec<_>>();
            assert_valid(
                &document,
                not_ready_schema,
                &json!({
                    "status": "not_ready",
                    "ready": false,
                    "reasons": reasons,
                    "engine_state": engine_state,
                    "schema_state": schema_state,
                    "schema_generation": "0",
                    "active_schema_operations": 0
                }),
                &format!("reachable not-ready state pair {engine_state}/{schema_state}"),
            );
        }
    }
    for (instance, context) in [
        (
            json!({
                "status": "not_ready",
                "ready": false,
                "reasons": [],
                "engine_state": "running",
                "schema_state": "ready",
                "schema_generation": "0",
                "active_schema_operations": 0
            }),
            "running/ready is the 200 readiness state",
        ),
        (
            json!({
                "status": "not_ready",
                "ready": false,
                "reasons": ["engine_stopped"],
                "engine_state": "draining",
                "schema_state": "ready",
                "schema_generation": "0",
                "active_schema_operations": 0
            }),
            "mismatched engine readiness reason",
        ),
        (
            json!({
                "status": "not_ready",
                "ready": false,
                "reasons": ["schema_degraded", "engine_stopped"],
                "engine_state": "stopped",
                "schema_state": "degraded",
                "schema_generation": "0",
                "active_schema_operations": 0
            }),
            "reversed two-cause readiness reasons",
        ),
        (
            json!({
                "status": "not_ready",
                "ready": false,
                "reasons": ["engine_draining", "engine_stopped"],
                "engine_state": "draining",
                "schema_state": "ready",
                "schema_generation": "0",
                "active_schema_operations": 0
            }),
            "duplicate engine-cause category",
        ),
        (
            json!({
                "status": "not_ready",
                "ready": false,
                "reasons": ["schema_state_unknown"],
                "engine_state": "running",
                "schema_state": "migrating",
                "schema_generation": "0",
                "active_schema_operations": 0
            }),
            "unreachable fallback readiness reason",
        ),
    ] {
        assert_invalid(&document, not_ready_schema, &instance, context);
    }

    let stream_response = response(&document, "/v1/query/stream", "post", "200");
    let stream_media = &stream_response["content"]["application/x-ndjson; charset=utf-8"];
    assert!(stream_media.is_object());
    assert_eq!(stream_media["schema"]["type"], "string");
    assert_eq!(
        stream_media["x-briskdb-ndjson"],
        json!({
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
                    "maximum": 1_000_000
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
        })
    );
}

#[test]
fn every_engine_and_transport_error_mapping_is_present_verbatim() {
    let document = document();
    let mappings = document["x-briskdb-engine-errors"]
        .as_array()
        .expect("the document must expose its stable engine error mapping");
    assert_eq!(
        mappings
            .iter()
            .map(|mapping| mapping["code"].as_str().expect("mapping code"))
            .collect::<Vec<_>>(),
        EngineErrorKind::ALL
            .iter()
            .map(|kind| kind.code())
            .collect::<Vec<_>>()
    );
    let actual = mappings
        .iter()
        .map(|mapping| {
            (
                mapping["code"].as_str().expect("mapping code"),
                (
                    mapping["status"].as_u64().expect("mapping status") as u16,
                    mapping["type"].as_str().expect("mapping problem type"),
                    mapping["title"].as_str().expect("mapping title"),
                    mapping["detail"].as_str().expect("mapping detail"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual.len(), EngineErrorKind::ALL.len());
    for &kind in EngineErrorKind::ALL {
        let expected = http_error(kind);
        assert_eq!(
            actual.get(kind.code()),
            Some(&(
                expected.status,
                expected.problem_type,
                expected.title,
                expected.detail,
            )),
            "wrong OpenAPI mapping for {}",
            kind.code()
        );
        assert_valid(
            &document,
            component(&document, &format!("ProblemDetails{}", expected.status)),
            &json!({
                "type": expected.problem_type,
                "title": expected.title,
                "status": expected.status,
                "detail": expected.detail,
                "code": kind.code()
            }),
            &format!("{} Problem mapping", kind.code()),
        );
    }

    let transport_mappings = document["x-briskdb-transport-errors"]
        .as_array()
        .expect("the document must expose its stable transport error mapping");
    let expected_transport_mappings = json!([
        {
            "code": "invalid_argument",
            "status": 400,
            "type": "https://github.com/schapman1974/briskdb/blob/main/docs/ERRORS.md#invalid-argument",
            "title": "Invalid argument",
            "detail": "The request contains an invalid argument."
        },
        {
            "code": "request_too_large",
            "status": 413,
            "type": "urn:briskdb:http:v1:request-too-large",
            "title": "Request too large",
            "detail": "The request body exceeds the HTTP API limit."
        },
        {
            "code": "unsupported_media_type",
            "status": 415,
            "type": "urn:briskdb:http:v1:unsupported-media-type",
            "title": "Unsupported media type",
            "detail": "The request requires a JSON content type."
        },
        {
            "code": "not_found",
            "status": 404,
            "type": "urn:briskdb:http:v1:not-found",
            "title": "Not found",
            "detail": "The requested API endpoint does not exist."
        },
        {
            "code": "method_not_allowed",
            "status": 405,
            "type": "urn:briskdb:http:v1:method-not-allowed",
            "title": "Method not allowed",
            "detail": "The method is not supported by this API endpoint."
        }
    ]);
    assert_eq!(
        transport_mappings,
        expected_transport_mappings.as_array().unwrap(),
        "transport errors must retain their exact runtime order and mapping"
    );

    let engine_codes = mappings
        .iter()
        .map(|mapping| mapping["code"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    let transport_codes = transport_mappings
        .iter()
        .map(|mapping| mapping["code"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        engine_codes
            .intersection(&transport_codes)
            .copied()
            .collect::<BTreeSet<_>>(),
        ["invalid_argument"].into_iter().collect(),
        "transport-only errors must not be conflated with EngineErrorKind"
    );
    assert_eq!(
        transport_codes
            .difference(&engine_codes)
            .copied()
            .collect::<BTreeSet<_>>(),
        [
            "method_not_allowed",
            "not_found",
            "request_too_large",
            "unsupported_media_type",
        ]
        .into_iter()
        .collect(),
    );
    for mapping in transport_mappings {
        let status = mapping["status"].as_u64().unwrap();
        assert_valid(
            &document,
            component(&document, &format!("ProblemDetails{status}")),
            mapping,
            &format!("{} transport Problem mapping", mapping["code"]),
        );
    }
}

fn application() -> (tempfile::TempDir, Engine, Router) {
    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::open(temp.path(), 2).unwrap());
    database
        .broadcast("CREATE TABLE notes (id INTEGER PRIMARY KEY, body BLOB NOT NULL)")
        .unwrap();
    let engine = Engine::from_database(database);
    let app = briskdb::api::router_with_engine(engine.clone());
    (temp, engine, app)
}

async fn request(
    app: &Router,
    method: Method,
    path: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = Request::builder().method(method).uri(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = app
        .clone()
        .oneshot(request.body(body.into()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, 4 * 1024 * 1024).await.unwrap().to_vec();
    (parts.status, parts.headers, body)
}

#[tokio::test]
async fn live_json_problem_and_ndjson_records_match_component_schemas() {
    let document = document();
    let (_temp, engine, app) = application();

    for token in [
        "1.7976931348623158e308".to_owned(),
        "-1.7976931348623158e308".to_owned(),
        format!("{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0"),
        format!("-{LEGACY_REQUEST_ROUNDING_LIMIT_MINUS_ONE}.0"),
    ] {
        let body =
            format!(r#"{{"shard_key":"tenant-a","sql":"SELECT ?1 AS value","params":[{token}]}}"#);
        let (status, headers, response_body) = request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            body,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "finite-rounded legacy request token {token} must be accepted: {}",
            String::from_utf8_lossy(&response_body),
        );
        assert_eq!(headers["content-type"], "application/json");
        let response: Value = serde_json::from_slice(&response_body).unwrap();
        assert_valid(
            &document,
            response_schema(&document, "/v1/query", "post", "200", "application/json"),
            &response,
            &format!("live response for finite-rounded legacy request token {token}"),
        );
    }

    for token in [
        "1.7976931348623159e308".to_owned(),
        "-1.7976931348623159e308".to_owned(),
        format!("{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
        format!("-{LEGACY_REQUEST_ROUNDING_LIMIT}.0"),
    ] {
        let body =
            format!(r#"{{"shard_key":"tenant-a","sql":"SELECT ?1 AS value","params":[{token}]}}"#);
        let (status, headers, response_body) = request(
            &app,
            Method::POST,
            "/v1/query",
            Some("application/json"),
            body,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "overflowing legacy request token {token} must be rejected",
        );
        assert_eq!(headers["content-type"], "application/problem+json");
        let problem: Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(problem["code"], "invalid_argument");
        assert_valid(
            &document,
            response_schema(
                &document,
                "/v1/query",
                "post",
                "400",
                "application/problem+json",
            ),
            &problem,
            &format!("live Problem for overflowing legacy request token {token}"),
        );
    }

    for path in ["/v1/query", "/v1/query/stream"] {
        for property in ["max_rows", "max_logical_bytes"] {
            for token in ["1.0", "1e0"] {
                let body = format!(
                    r#"{{"shard_key":"tenant-a","sql":"SELECT 1","result_limits":{{"{property}":{token}}}}}"#
                );
                let (status, headers, response_body) =
                    request(&app, Method::POST, path, Some("application/json"), body).await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{path} must reject non-integer {property} token {token}",
                );
                assert_eq!(headers["content-type"], "application/problem+json");
                let problem: Value = serde_json::from_slice(&response_body).unwrap();
                assert_eq!(problem["code"], "invalid_argument");
                assert_valid(
                    &document,
                    response_schema(&document, path, "post", "400", "application/problem+json"),
                    &problem,
                    &format!("live Problem for {path} {property} token {token}"),
                );
            }
        }
    }

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        serde_json::to_vec(&json!({
            "shard_key": "tenant-a",
            "sql": "SELECT ?1 AS payload",
            "params": [{"$briskdb_type": "binary", "value": "AP8="}],
            "value_encoding": "lossless-json-v1"
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "application/json");
    let query: Value = serde_json::from_slice(&body).unwrap();
    assert_valid(
        &document,
        response_schema(&document, "/v1/query", "post", "200", "application/json"),
        &query,
        "live query response",
    );

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query",
        Some("application/json"),
        br#"{"sql":"SELECT 1","unknown":true}"#.as_slice(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers["content-type"], "application/problem+json");
    let problem: Value = serde_json::from_slice(&body).unwrap();
    assert_valid(
        &document,
        response_schema(
            &document,
            "/v1/query",
            "post",
            "400",
            "application/problem+json",
        ),
        &problem,
        "live Problem response",
    );

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query/stream",
        Some("application/json"),
        br#"{"shard_key":"tenant-a","sql":"SELECT 7 AS value"}"#.as_slice(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    assert_eq!(body.last(), Some(&b'\n'));
    let records = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .filter_map(|record| record["kind"].as_str())
            .collect::<Vec<_>>(),
        ["meta", "row", "complete"]
    );

    for record in &records {
        let kind = record["kind"].as_str().unwrap();
        let component = match kind {
            "meta" => "QueryStreamMeta",
            "row" => "QueryStreamRow",
            "complete" => "QueryStreamComplete",
            "error" => "QueryStreamError",
            other => panic!("unknown stream record kind: {other}"),
        };
        let schema = document
            .pointer(&format!("/components/schemas/{component}"))
            .unwrap_or_else(|| panic!("missing {component} component"));
        assert_valid(
            &document,
            schema,
            record,
            &format!("live {kind} stream record"),
        );
    }

    let (status, headers, body) = request(
        &app,
        Method::POST,
        "/v1/query/stream",
        Some("application/json"),
        br#"{"shard_key":"tenant-a","sql":"SELECT 1 AS value UNION ALL SELECT 2","result_limits":{"max_rows":1}}"#.as_slice(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers["content-type"],
        "application/x-ndjson; charset=utf-8"
    );
    let records = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        records
            .iter()
            .filter_map(|record| record["kind"].as_str())
            .collect::<Vec<_>>(),
        ["meta", "row", "error"]
    );
    let error_schema = &document["components"]["schemas"]["QueryStreamError"];
    assert_valid(
        &document,
        error_schema,
        &records[2],
        "live terminal stream error",
    );

    let (status, headers, body) =
        request(&app, Method::GET, "/v1/ready", None, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "application/json");
    let ready: Value = serde_json::from_slice(&body).unwrap();
    assert_valid(
        &document,
        response_schema(&document, "/v1/ready", "get", "200", "application/json"),
        &ready,
        "live 200 readiness response",
    );

    engine.begin_shutdown();
    let (status, headers, body) =
        request(&app, Method::GET, "/v1/ready", None, Body::empty()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["content-type"], "application/json");
    let not_ready: Value = serde_json::from_slice(&body).unwrap();
    assert_valid(
        &document,
        response_schema(&document, "/v1/ready", "get", "503", "application/json"),
        &not_ready,
        "live 503 readiness response",
    );
    assert_invalid(
        &document,
        response_schema(&document, "/v1/ready", "get", "200", "application/json"),
        &not_ready,
        "live 503 readiness body under the 200 schema",
    );
    engine.shutdown().await.unwrap();
}
