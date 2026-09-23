//! Opt-in, authenticated, bounded read-only connector for host SQLite.
//! This router intentionally has no SQL execution or administration endpoint.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::core::{Engine, RequestContext, ResultLimits, Statement, TablePlacement, Value};

const MAX_ROWS: u64 = 4096;
const MAX_BYTES: usize = 8 * 1024 * 1024;
type Failure = (StatusCode, &'static str);

/// One read-only credential and an explicit, default-database table allowlist.
/// Secret material is intentionally excluded from Debug output.
#[derive(Clone)]
pub struct Config {
    token_hash: blake3::Hash,
    tables: BTreeSet<String>,
    legacy_routing_key: Option<String>,
}

impl Config {
    pub fn new(token: &str, tables: Vec<String>) -> Result<Self, &'static str> {
        if !(32..=256).contains(&token.len())
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("SQLite remote token must contain 32..256 URL-safe ASCII characters");
        }
        if tables.is_empty() || tables.len() > 256 {
            return Err("SQLite remote requires an explicit allowlist of 1..256 tables");
        }
        if tables.iter().any(|name| {
            name.is_empty()
                || name.len() > 255
                || name.contains('\0')
                || name.to_ascii_lowercase().starts_with("sqlite_")
                || name.to_ascii_lowercase().starts_with("briskdb")
        }) {
            return Err("SQLite remote allowlist contains an invalid or reserved table name");
        }
        let unique: BTreeSet<_> = tables.iter().cloned().collect();
        if unique.len() != tables.len() {
            return Err("SQLite remote table allowlist contains duplicates");
        }
        Ok(Self {
            token_hash: blake3::hash(token.as_bytes()),
            tables: unique,
            legacy_routing_key: None,
        })
    }

    /// Explicitly opt an uncataloged database into a single routed shard view.
    /// Registered databases always use their logical placement instead.
    pub fn with_legacy_routing_key(mut self, key: String) -> Result<Self, &'static str> {
        if key.is_empty() || key.len() > 255 || key.contains('\0') {
            return Err("legacy remote routing key must contain 1..255 bytes and no NUL");
        }
        self.legacy_routing_key = Some(key);
        Ok(self)
    }
}

#[derive(Clone)]
struct Connector {
    engine: Engine,
    config: Config,
    instance: String,
    admission: Arc<tokio::sync::Semaphore>,
}

/// Build a dedicated router. Bind loopback and publish only this listener
/// through a trusted HTTPS reverse proxy; do not expose the admin listener.
pub fn router(engine: Engine, config: Config) -> Result<Router, &'static str> {
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(|_| "SQLite remote instance entropy unavailable")?;
    let state = Connector {
        engine,
        config,
        instance: blake3::hash(&nonce).to_hex().to_string(),
        admission: Arc::new(tokio::sync::Semaphore::new(8)),
    };
    Ok(Router::new()
        .route("/sqlite/v1/catalog", get(catalog))
        .route("/sqlite/v1/scan", post(scan))
        .layer(DefaultBodyLimit::max(4096))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state))
}

async fn authorize(State(state): State<Connector>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    // blake3::Hash equality is constant-time. Never log or echo credentials.
    if !supplied.is_some_and(|token| {
        token.len() <= 256 && blake3::hash(token.as_bytes()) == state.config.token_hash
    }) {
        return (
            StatusCode::UNAUTHORIZED,
            "BriskDB remote authentication failed",
        )
            .into_response();
    }
    let Ok(_permit) = state.admission.try_acquire() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "BriskDB remote request capacity exceeded",
        )
            .into_response();
    };
    let mut response = match tokio::time::timeout(Duration::from_secs(15), next.run(request)).await
    {
        Ok(response) => response,
        Err(_) => (
            StatusCode::REQUEST_TIMEOUT,
            "BriskDB remote request timed out",
        )
            .into_response(),
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

fn context() -> RequestContext {
    RequestContext::new()
        .with_timeout(Duration::from_secs(10))
        .expect("nonzero timeout")
        .with_result_limits(ResultLimits::new(MAX_ROWS, 1024 * 1024).expect("nonzero limits"))
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn query_failure(_: crate::core::EngineError) -> Failure {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        "BriskDB remote query failed or exceeded its limits",
    )
}

#[derive(Serialize, PartialEq, Eq)]
struct Column {
    name: String,
    declared_type: String,
}

#[derive(Serialize)]
struct Table {
    name: String,
    columns: Vec<Column>,
}

#[derive(Serialize)]
struct Catalog {
    version: u8,
    instance: String,
    generation: u64,
    max_rows: u64,
    max_bytes: usize,
    scope: &'static str,
    tables: Vec<Table>,
}

async fn describe(
    state: &Connector,
    name: &str,
    request: RequestContext,
) -> Result<Vec<Column>, Failure> {
    let catalog = state.engine.catalog();
    let shards = if catalog.tables().is_empty() {
        let key = state.config.legacy_routing_key.as_ref().ok_or((
            StatusCode::PRECONDITION_FAILED,
            "remote requires registered logical placement or an explicit legacy routing key",
        ))?;
        let session = state.engine.session();
        session.set_routing_key(key).await.map_err(query_failure)?;
        let target = state
            .engine
            .query_with_context(
                &session,
                Statement::new("SELECT 1", vec![]),
                request.clone(),
            )
            .await
            .map_err(query_failure)?;
        vec![target.shard]
    } else {
        let table = catalog
            .tables()
            .iter()
            .find(|table| {
                table.database_id() == catalog.default_database().id() && table.name() == name
            })
            .ok_or((StatusCode::NOT_FOUND, "BriskDB remote table is unavailable"))?;
        match table.placement() {
            TablePlacement::Sharded(_) => (0..state.engine.shard_count()).collect(),
            TablePlacement::Global => vec![0],
            TablePlacement::Catalog => {
                return Err((StatusCode::FORBIDDEN, "catalog tables are not exposed"));
            }
        }
    };
    let session = state.engine.session();
    let mut expected = None;
    for shard in shards {
        let result = state.engine.inspect_shard_with_context(&session, shard, Statement::new(
            "SELECT name FROM pragma_table_list WHERE schema='main' AND type='table' AND name=?1 COLLATE BINARY",
            vec![Value::Text(name.to_owned())]), request.clone()).await.map_err(query_failure)?;
        if result.into_parts().1.len() != 1 {
            return Err((
                StatusCode::NOT_FOUND,
                "BriskDB remote requires an ordinary table on every target shard",
            ));
        }
        let result = state
            .engine
            .inspect_shard_with_context(
                &session,
                shard,
                Statement::new(
                    "SELECT name, type FROM pragma_table_xinfo(?1) WHERE hidden != 1 ORDER BY cid",
                    vec![Value::Text(name.to_owned())],
                ),
                request.clone(),
            )
            .await
            .map_err(query_failure)?;
        let columns = result
            .into_parts()
            .1
            .into_iter()
            .map(|row| match row.into_values().as_slice() {
                [Value::Text(name), Value::Text(declared_type)]
                    if name.len() <= 255
                        && declared_type.len() <= 255
                        && !name.contains('\0')
                        && !declared_type.contains('\0') =>
                {
                    Ok(Column {
                        name: name.clone(),
                        declared_type: declared_type.clone(),
                    })
                }
                _ => Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "unsupported BriskDB remote schema",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.is_empty() || columns.len() > 256 {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                "BriskDB remote column limit exceeded",
            ));
        }
        match &expected {
            Some(previous) if *previous != columns => {
                return Err((StatusCode::CONFLICT, "remote shard schemas disagree"));
            }
            None => expected = Some(columns),
            _ => {}
        }
    }
    expected.ok_or((StatusCode::NOT_FOUND, "BriskDB remote has no target shards"))
}

fn fence(state: &Connector, instance: &str, generation: u64) -> Result<(), Failure> {
    if instance != state.instance || generation != state.engine.catalog().schema_generation() {
        Err((
            StatusCode::CONFLICT,
            "BriskDB remote schema/server changed; detach and attach again",
        ))
    } else {
        Ok(())
    }
}

async fn catalog(State(state): State<Connector>) -> Result<Response, Failure> {
    let generation = state.engine.catalog().schema_generation();
    let mut tables = Vec::new();
    let request = context();
    for name in &state.config.tables {
        tables.push(Table {
            name: name.clone(),
            columns: describe(&state, name, request.clone()).await?,
        });
    }
    fence(&state, &state.instance, generation)?;
    let bytes = serde_json::to_vec(&Catalog {
        version: 1,
        instance: state.instance,
        generation,
        max_rows: MAX_ROWS,
        max_bytes: MAX_BYTES,
        scope: if state.engine.catalog().tables().is_empty() {
            "legacy-shard"
        } else {
            "logical"
        },
        tables,
    })
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "remote catalog serialization failed",
        )
    })?;
    if bytes.len() > MAX_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "remote catalog is too large"));
    }
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scan {
    instance: String,
    generation: u64,
    table: String,
}

async fn scan(
    State(state): State<Connector>,
    Json(request): Json<Scan>,
) -> Result<Response, Failure> {
    if !state.config.tables.contains(&request.table) {
        return Err((StatusCode::FORBIDDEN, "BriskDB remote table is not allowed"));
    }
    fence(&state, &request.instance, request.generation)?;
    let controls = context();
    let expected = describe(&state, &request.table, controls.clone()).await?;
    let session = state.engine.session();
    if state.engine.catalog().tables().is_empty() {
        let key = state.config.legacy_routing_key.as_ref().ok_or((
            StatusCode::PRECONDITION_FAILED,
            "remote requires logical placement or a legacy routing key",
        ))?;
        session.set_routing_key(key).await.map_err(query_failure)?;
    }
    let result = state
        .engine
        .query_logical_with_context(
            &session,
            Statement::new(format!("SELECT * FROM {}", quote(&request.table)), vec![]),
            controls,
        )
        .await
        .map_err(query_failure)?;
    fence(&state, &request.instance, request.generation)?;
    let (columns, rows) = result.value.into_parts();
    if !columns
        .iter()
        .map(|column| &column.name)
        .eq(expected.iter().map(|column| &column.name))
    {
        return Err((StatusCode::CONFLICT, "BriskDB remote result schema changed"));
    }
    if columns.is_empty() || columns.len() > 256 || rows.len() > MAX_ROWS as usize {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "BriskDB remote result limit exceeded",
        ));
    }
    let mut frame = b"BRS1".to_vec();
    frame.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    frame.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        for value in row.into_values() {
            encode(&mut frame, value)?;
        }
    }
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], frame).into_response())
}

fn encode(frame: &mut Vec<u8>, value: Value) -> Result<(), Failure> {
    match value {
        Value::Null => frame.push(0),
        Value::Boolean(value) => {
            frame.push(1);
            frame.extend_from_slice(&i64::from(value).to_le_bytes());
        }
        Value::Int64(value) => {
            frame.push(1);
            frame.extend_from_slice(&value.to_le_bytes());
        }
        Value::UInt64(value) if value <= i64::MAX as u64 => {
            frame.push(1);
            frame.extend_from_slice(&(value as i64).to_le_bytes());
        }
        Value::Float64(value) if !value.is_nan() => {
            frame.push(2);
            frame.extend_from_slice(&value.to_le_bytes());
        }
        Value::Text(value) => encode_bytes(frame, 3, value.as_bytes())?,
        Value::Binary(value) => encode_bytes(frame, 4, &value)?,
        _ => {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                "remote value cannot be represented losslessly by SQLite",
            ));
        }
    }
    if frame.len() > MAX_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "remote frame limit exceeded"));
    }
    Ok(())
}

fn encode_bytes(frame: &mut Vec<u8>, tag: u8, value: &[u8]) -> Result<(), Failure> {
    if value.len() > MAX_BYTES {
        return Err((StatusCode::PAYLOAD_TOO_LARGE, "remote cell limit exceeded"));
    }
    frame.push(tag);
    frame.extend_from_slice(&(value.len() as u32).to_le_bytes());
    frame.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Database, ShardKeyMetadata, ShardKeyType, TableDeclaration};
    use axum::body::{Body, to_bytes};
    use serde_json::{Value as JsonValue, json};
    use tower::ServiceExt as _;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    async fn request(router: &Router, path: &str, body: Option<JsonValue>) -> Response {
        let method = if body.is_some() { "POST" } else { "GET" };
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        body.map_or_else(String::new, |body| body.to_string()),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn registered_logical_tables_read_all_shards_with_lossless_values() {
        let root = tempfile::tempdir().unwrap();
        let mut database = Database::open(root.path(), 4).unwrap();
        database
            .broadcast("CREATE TABLE users (id INTEGER PRIMARY KEY NOT NULL)")
            .unwrap();
        database
            .register_tables(vec![
                TableDeclaration::sharded(
                    database.catalog().default_database().id(),
                    "users",
                    ShardKeyMetadata::new("id", ShardKeyType::Int64).unwrap(),
                )
                .unwrap(),
            ])
            .unwrap();
        let mut physical = BTreeSet::new();
        for id in 0_i64..64 {
            let routed = database
                .execute_routed(
                    &id.to_string(),
                    "INSERT INTO users (id) VALUES (?1)",
                    &[Value::Int64(id)],
                )
                .unwrap();
            physical.insert(routed.shard);
        }
        assert_eq!(physical.len(), 4);
        let engine = Engine::from_database(Arc::new(database));
        let app = router(
            engine.clone(),
            Config::new(TOKEN, vec!["users".into()]).unwrap(),
        )
        .unwrap();
        let response = request(&app, "/sqlite/v1/catalog", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let catalog: JsonValue =
            serde_json::from_slice(&to_bytes(response.into_body(), MAX_BYTES).await.unwrap())
                .unwrap();
        assert_eq!(catalog["scope"], "logical");
        assert_eq!(catalog["tables"][0]["columns"][0]["name"], "id");
        let scan = json!({"instance": catalog["instance"], "generation": catalog["generation"], "table": "users"});
        let response = request(&app, "/sqlite/v1/scan", Some(scan.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let frame = to_bytes(response.into_body(), MAX_BYTES).await.unwrap();
        assert_eq!(&frame[..4], b"BRS1");
        assert_eq!(u32::from_le_bytes(frame[4..8].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(frame[8..12].try_into().unwrap()), 64);
        let values = frame[12..]
            .chunks_exact(9)
            .map(|cell| {
                assert_eq!(cell[0], 1);
                i64::from_le_bytes(cell[1..].try_into().unwrap())
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(values, (0..64).collect());
        let mut stale = scan.clone();
        stale["generation"] = json!(u64::MAX);
        assert_eq!(
            request(&app, "/sqlite/v1/scan", Some(stale)).await.status(),
            StatusCode::CONFLICT
        );
        let mut denied = scan.clone();
        denied["table"] = json!("sqlite_schema");
        assert_eq!(
            request(&app, "/sqlite/v1/scan", Some(denied))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        let mut injected = scan;
        injected["sql"] = json!("DROP TABLE users");
        assert_eq!(
            request(&app, "/sqlite/v1/scan", Some(injected))
                .await
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        engine.begin_shutdown();
    }

    #[tokio::test]
    async fn uncataloged_tables_require_explicit_scope_and_auth_precedes_body_parsing() {
        let root = tempfile::tempdir().unwrap();
        let database = Database::open(root.path(), 2).unwrap();
        database
            .broadcast("CREATE TABLE users (id INTEGER)")
            .unwrap();
        let engine = Engine::from_database(Arc::new(database));
        let app = router(
            engine.clone(),
            Config::new(TOKEN, vec!["users".into()]).unwrap(),
        )
        .unwrap();
        assert_eq!(
            request(&app, "/sqlite/v1/catalog", None).await.status(),
            StatusCode::PRECONDITION_FAILED
        );
        let request = Request::builder()
            .method("POST")
            .uri("/sqlite/v1/scan")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("x".repeat(8192)))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let request = Request::builder()
            .method("POST")
            .uri("/sqlite/v1/scan")
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("x".repeat(8192)))
            .unwrap();
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        engine.begin_shutdown();
    }

    #[test]
    fn credentials_allowlists_and_binary_encoding_fail_closed() {
        for token in ["short", "contains newline\n012345678901234567890123456789"] {
            assert!(Config::new(token, vec!["users".into()]).is_err());
        }
        for tables in [
            vec![],
            vec!["sqlite_schema".into()],
            vec!["users".into(), "users".into()],
        ] {
            assert!(Config::new(TOKEN, tables).is_err());
        }
        assert!(
            Config::new(TOKEN, vec!["users".into()])
                .unwrap()
                .with_legacy_routing_key(String::new())
                .is_err()
        );
        for value in [
            Value::UInt64(u64::MAX),
            Value::Float64(f64::NAN),
            Value::InvalidText(vec![255]),
            Value::decimal("1.25").unwrap(),
        ] {
            assert!(encode(&mut Vec::new(), value).is_err());
        }
        let mut frame = Vec::new();
        for value in [
            Value::Null,
            Value::Int64(i64::MIN),
            Value::Float64(-0.0),
            Value::Binary(vec![]),
            Value::Text("\0🍋".into()),
        ] {
            encode(&mut frame, value).unwrap();
        }
        assert_eq!(frame[0], 0);
        assert_eq!(&frame[2..10], &i64::MIN.to_le_bytes());
        assert_eq!(&frame[11..19], &(-0.0_f64).to_le_bytes());
    }
}
