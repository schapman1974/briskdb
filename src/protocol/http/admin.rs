use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, COOKIE, SET_COOKIE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use super::HttpState;
use crate::core::{
    Column, EngineError, EngineErrorKind, RequestContext, ResultLimits, ResultSet, Routed, Row,
    Statement, TablePlacement, Value, merge_scatter_results,
};

const INDEX_HTML: &str = include_str!("admin/index.html");
const STYLES_CSS: &str = include_str!("admin/styles.css");
const LOGIC_JS: &str = include_str!("admin/logic.js");
const APP_JS: &str = include_str!("admin/app.js");

const ADMIN_USERNAME: &str = "admin";
const ADMIN_PASSWORD: &str = "admin";
const SESSION_COOKIE: &str = "briskdb_admin_session";
const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_SESSIONS: usize = 128;
const DEFAULT_PAGE_LIMIT: u16 = 50;
const MAX_PAGE_LIMIT: u16 = 200;
const MAX_PAGE_OFFSET: u64 = 1_000_000;
const MAX_INSPECTION_CONCURRENCY: usize = 8;
const MAX_JAVASCRIPT_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const TABLE_DISCOVERY_SQL: &str = "SELECT name FROM pragma_table_list \
     WHERE schema = 'main' AND type = 'table' \
       AND lower(name) NOT GLOB 'sqlite_*' \
       AND lower(name) != 'briskdb' \
       AND lower(name) NOT GLOB 'briskdb_*' \
     ORDER BY name COLLATE BINARY";
const TABLE_LOOKUP_SQL: &str = "SELECT name FROM pragma_table_list \
     WHERE schema = 'main' AND type = 'table' \
       AND name = ?1 COLLATE BINARY \
       AND lower(name) NOT GLOB 'sqlite_*' \
       AND lower(name) != 'briskdb' \
       AND lower(name) NOT GLOB 'briskdb_*' \
     LIMIT 1";
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");
const CSP: HeaderValue = HeaderValue::from_static(
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
);

#[derive(Clone)]
pub(super) struct SessionStore {
    inner: Arc<Mutex<Sessions>>,
}

#[derive(Default)]
struct Sessions {
    by_token: HashMap<String, Instant>,
}

impl SessionStore {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Sessions::default())),
        }
    }

    fn issue(&self) -> Result<String, getrandom::Error> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)?;
        let token = hex_token(&bytes);
        self.issue_at(token.clone(), Instant::now());
        Ok(token)
    }

    fn issue_at(&self, token: String, now: Instant) {
        let mut sessions = self.lock();
        sessions.purge_expired(now);
        if sessions.by_token.len() >= MAX_SESSIONS {
            let evicted = sessions
                .by_token
                .iter()
                .min_by(|(left_token, left_expiry), (right_token, right_expiry)| {
                    left_expiry
                        .cmp(right_expiry)
                        .then_with(|| left_token.cmp(right_token))
                })
                .map(|(token, _)| token.clone());
            if let Some(token) = evicted {
                sessions.by_token.remove(&token);
            }
        }
        sessions.by_token.insert(token, now + SESSION_LIFETIME);
    }

    fn validate(&self, token: &str) -> bool {
        self.validate_at(token, Instant::now())
    }

    fn validate_at(&self, token: &str, now: Instant) -> bool {
        let mut sessions = self.lock();
        sessions.purge_expired(now);
        sessions.by_token.contains_key(token)
    }

    fn revoke(&self, token: &str) -> bool {
        self.lock().by_token.remove(token).is_some()
    }

    fn lock(&self) -> MutexGuard<'_, Sessions> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Sessions {
    fn purge_expired(&mut self, now: Instant) {
        self.by_token.retain(|_, expiry| *expiry > now);
    }
}

pub(super) fn routes(state: HttpState) -> Router<HttpState> {
    let protected = Router::new()
        .route("/admin/api/session", get(session))
        .route("/admin/api/overview", get(overview))
        .route("/admin/api/count", get(count))
        .route("/admin/api/rows", get(rows))
        .route_layer(middleware::from_fn_with_state(state, require_authenticated));

    Router::new()
        .route("/admin", get(index))
        .route("/admin/", get(index))
        .route("/admin/assets/styles.css", get(styles))
        .route("/admin/assets/logic.js", get(logic))
        .route("/admin/assets/app.js", get(script))
        .route("/admin/api/login", post(login))
        .route("/admin/api/logout", post(logout))
        .merge(protected)
        .layer(DefaultBodyLimit::max(1_024))
        .layer(middleware::from_fn(add_no_store))
}

async fn require_authenticated(
    State(state): State<HttpState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    if authenticate(&headers, &state.admin_sessions).is_err() {
        authentication_required()
    } else {
        next.run(request).await
    }
}

async fn add_no_store(request: Request, next: Next) -> Response {
    no_store(next.run(request).await)
}

async fn index() -> Response {
    let mut response = static_response("text/html; charset=utf-8", INDEX_HTML);
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        CSP.clone(),
    );
    response
}

async fn styles() -> Response {
    static_response("text/css; charset=utf-8", STYLES_CSS)
}

async fn logic() -> Response {
    static_response("text/javascript; charset=utf-8", LOGIC_JS)
}

async fn script() -> Response {
    static_response("text/javascript; charset=utf-8", APP_JS)
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

async fn login(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(request): Json<LoginRequest>,
) -> Response {
    if request.username != ADMIN_USERNAME || request.password != ADMIN_PASSWORD {
        return auth_json(
            StatusCode::UNAUTHORIZED,
            json!({
                "code": "invalid_credentials",
                "message": "Invalid username or password."
            }),
            None,
        );
    }

    for token in presented_valid_tokens(&headers) {
        state.admin_sessions.revoke(&token);
    }
    let token = match state.admin_sessions.issue() {
        Ok(token) => token,
        Err(error) => {
            tracing::error!(error = ?error, "could not create an admin browser session");
            return auth_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"code": "internal", "message": "The request could not be completed."}),
                None,
            );
        }
    };
    auth_json(
        StatusCode::OK,
        json!({"authenticated": true}),
        Some(session_cookie(&token)),
    )
}

async fn logout(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    for token in presented_valid_tokens(&headers) {
        state.admin_sessions.revoke(&token);
    }
    auth_json(
        StatusCode::OK,
        json!({"authenticated": false}),
        Some(clear_session_cookie()),
    )
}

async fn session() -> Response {
    auth_json(
        StatusCode::OK,
        json!({"authenticated": true, "username": ADMIN_USERNAME}),
        None,
    )
}

#[derive(Serialize)]
struct OverviewResponse {
    scope: &'static str,
    shard_count: u16,
    visited_shards: Vec<u16>,
    tables: Vec<String>,
}

async fn overview(State(state): State<HttpState>) -> Response {
    let catalog = state.engine.catalog();
    if !catalog.tables().is_empty() {
        let default_database = catalog.default_database().id();
        let tables = catalog
            .tables()
            .iter()
            .filter(|table| {
                table.database_id() == default_database
                    && !matches!(table.placement(), TablePlacement::Catalog)
            })
            .map(|table| table.name().to_owned())
            .collect();
        return admin_json(
            StatusCode::OK,
            &OverviewResponse {
                scope: "logical_default_database",
                shard_count: state.engine.shard_count(),
                visited_shards: vec![],
                tables,
            },
        );
    }

    let session = state.engine.session();
    match state
        .engine
        .inspect_shard(&session, 0, Statement::new(TABLE_DISCOVERY_SQL, vec![]))
        .await
        .and_then(table_names)
    {
        Ok(tables) => admin_json(
            StatusCode::OK,
            &OverviewResponse {
                scope: "empty_catalog_shard_zero_fallback",
                shard_count: state.engine.shard_count(),
                visited_shards: vec![0],
                tables,
            },
        ),
        Err(error) => admin_engine_error(error),
    }
}

#[derive(Deserialize)]
struct CountQuery {
    table: String,
}

#[derive(Serialize)]
struct CountResponse {
    table: String,
    scope: &'static str,
    visited_shards: Vec<u16>,
    total_rows: JsonValue,
}

async fn count(State(state): State<HttpState>, Query(query): Query<CountQuery>) -> Response {
    if !is_visible_table_name(&query.table) {
        return invalid_argument("admin table name is not browseable");
    }

    match inspect_logical_table(&state.engine, &query.table).await {
        Ok(inspection) => admin_json(
            StatusCode::OK,
            &CountResponse {
                table: query.table,
                scope: inspection.scope,
                visited_shards: inspection.visited_shards(),
                total_rows: admin_value_to_json(Value::UInt64(inspection.total_rows)),
            },
        ),
        Err(error) => admin_engine_error(error),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalTableScope {
    Sharded,
    Global,
    EmptyCatalog,
}

impl LogicalTableScope {
    const fn name(self) -> &'static str {
        match self {
            Self::Sharded => "logical_sharded_table",
            Self::Global => "logical_global_table",
            Self::EmptyCatalog => "empty_catalog_all_physical_shards",
        }
    }
}

struct TableTargets {
    scope: LogicalTableScope,
    shards: Vec<u16>,
}

struct ShardTableInspection {
    shard: u16,
    rows: u64,
    columns: Vec<Column>,
}

struct LogicalTableInspection {
    scope: &'static str,
    schema_generation: u64,
    context: RequestContext,
    result_limits: ResultLimits,
    shards: Vec<ShardTableInspection>,
    total_rows: u64,
}

impl LogicalTableInspection {
    fn visited_shards(&self) -> Vec<u16> {
        self.shards.iter().map(|shard| shard.shard).collect()
    }

    fn columns(&self) -> &[Column] {
        self.shards
            .first()
            .map_or(&[], |inspection| inspection.columns.as_slice())
    }
}

fn logical_table_targets(
    engine: &crate::core::Engine,
    table: &str,
) -> Result<TableTargets, EngineError> {
    let catalog = engine.catalog();
    if catalog.tables().is_empty() {
        return Ok(TableTargets {
            scope: LogicalTableScope::EmptyCatalog,
            shards: (0..engine.shard_count()).collect(),
        });
    }

    let default_database = catalog.default_database().id();
    let Some(metadata) = catalog
        .tables()
        .iter()
        .find(|metadata| metadata.database_id() == default_database && metadata.name() == table)
    else {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "admin table is not registered in the default logical database",
        ));
    };
    match metadata.placement() {
        TablePlacement::Sharded(_) => Ok(TableTargets {
            scope: LogicalTableScope::Sharded,
            shards: (0..engine.shard_count()).collect(),
        }),
        TablePlacement::Global => Ok(TableTargets {
            scope: LogicalTableScope::Global,
            shards: vec![0],
        }),
        TablePlacement::Catalog => Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "admin catalog tables are not browseable",
        )),
    }
}

async fn inspect_logical_table(
    engine: &crate::core::Engine,
    table: &str,
) -> Result<LogicalTableInspection, EngineError> {
    let targets = logical_table_targets(engine, table)?;
    let status_session = engine.session();
    let status = engine.status(&status_session).await?;
    let context = match status.request_timeout() {
        Some(timeout) => RequestContext::new().with_timeout(timeout)?,
        None => RequestContext::new(),
    };
    let result_limits = ResultLimits::new(status.max_result_rows(), status.max_result_bytes())?;
    let schema_generation = engine.catalog().schema_generation();
    let inspections = stream::iter(targets.shards.iter().copied())
        .map(|shard| {
            let engine = engine.clone();
            let table = table.to_owned();
            let context = context.clone();
            async move { inspect_table_on_shard(&engine, shard, &table, context).await }
        })
        .buffer_unordered(MAX_INSPECTION_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await;
    ensure_schema_generation(schema_generation, engine.catalog().schema_generation())?;
    let mut shards = inspections?;
    shards.sort_unstable_by_key(|inspection| inspection.shard);
    ensure_matching_columns(&shards)?;
    let total_rows = sum_row_counts(shards.iter().map(|inspection| inspection.rows))?;
    Ok(LogicalTableInspection {
        scope: targets.scope.name(),
        schema_generation,
        context,
        result_limits,
        shards,
        total_rows,
    })
}

fn sum_row_counts(counts: impl IntoIterator<Item = u64>) -> Result<u64, EngineError> {
    counts.into_iter().try_fold(0_u64, |total, count| {
        total.checked_add(count).ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "admin logical row count exceeds the supported unsigned range",
            )
        })
    })
}

fn ensure_schema_generation(expected: u64, observed: u64) -> Result<(), EngineError> {
    if observed == expected {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::Busy,
            "admin logical table inspection crossed a completed schema migration",
        ))
    }
}

async fn inspect_table_on_shard(
    engine: &crate::core::Engine,
    shard: u16,
    table: &str,
    context: RequestContext,
) -> Result<ShardTableInspection, EngineError> {
    let session = engine.session();
    ensure_browseable_table_with_context(engine, &session, shard, table, context.clone()).await?;
    let shape = engine
        .inspect_shard_with_context(
            &session,
            shard,
            Statement::new(
                format!("SELECT * FROM {} LIMIT 0", quote_identifier(table)),
                vec![],
            ),
            context.clone(),
        )
        .await?;
    let result = engine
        .inspect_shard_with_context(
            &session,
            shard,
            Statement::new(
                format!(
                    "SELECT COUNT(*) AS row_count FROM {}",
                    quote_identifier(table)
                ),
                vec![],
            ),
            context,
        )
        .await?;
    Ok(ShardTableInspection {
        shard,
        rows: row_count(result)?,
        columns: shape.into_parts().0,
    })
}

fn ensure_matching_columns(shards: &[ShardTableInspection]) -> Result<(), EngineError> {
    let Some(expected) = shards.first().map(|shard| shard.columns.as_slice()) else {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            "admin logical table has no physical targets",
        ));
    };
    if shards
        .iter()
        .all(|shard| shard.columns.as_slice() == expected)
    {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::DataCorruption,
            "admin logical table has incompatible physical schemas",
        ))
    }
}

async fn ensure_browseable_table_with_context(
    engine: &crate::core::Engine,
    session: &crate::core::Session,
    shard: u16,
    table: &str,
    context: RequestContext,
) -> Result<(), EngineError> {
    let tables = engine
        .inspect_shard_with_context(
            session,
            shard,
            Statement::new(TABLE_LOOKUP_SQL, vec![Value::Text(table.to_owned())]),
            context,
        )
        .await
        .and_then(table_names)?;
    if tables.as_slice() == [table] {
        Ok(())
    } else {
        Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            format!("admin table is not browseable on physical shard {shard}"),
        ))
    }
}

fn row_count(result: ResultSet) -> Result<u64, EngineError> {
    let (_, mut rows) = result.into_parts();
    if rows.len() != 1 {
        return Err(invalid_count_result());
    }
    let values = rows
        .pop()
        .expect("the exact row count was checked")
        .into_values();
    match values.as_slice() {
        [Value::Int64(value)] if *value >= 0 => Ok(*value as u64),
        [Value::UInt64(value)] => Ok(*value),
        _ => Err(invalid_count_result()),
    }
}

fn invalid_count_result() -> EngineError {
    EngineError::new(
        EngineErrorKind::Internal,
        "admin table count returned an unexpected result shape",
    )
}

#[derive(Deserialize)]
struct RowsQuery {
    table: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Serialize)]
struct RowsResponse {
    table: String,
    scope: &'static str,
    visited_shards: Vec<u16>,
    ordering: &'static str,
    limit: u16,
    offset: u64,
    has_more: bool,
    columns: Vec<super::QueryColumn>,
    rows: Vec<Vec<JsonValue>>,
}

async fn rows(State(state): State<HttpState>, Query(query): Query<RowsQuery>) -> Response {
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    let offset = query.offset.unwrap_or(0);
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) {
        return invalid_argument("admin page limit is outside the supported range");
    }
    if offset > MAX_PAGE_OFFSET {
        return invalid_argument("admin page offset is outside the supported range");
    }
    if !is_visible_table_name(&query.table) {
        return invalid_argument("admin table name is not browseable");
    }

    match logical_table_page(&state.engine, &query.table, limit, offset).await {
        Ok(page) => page_response(query.table, page, limit, offset),
        Err(error) => admin_engine_error(error),
    }
}

struct LogicalTablePage {
    scope: &'static str,
    visited_shards: Vec<u16>,
    columns: Vec<Column>,
    rows: Vec<Row>,
}

async fn logical_table_page(
    engine: &crate::core::Engine,
    table: &str,
    limit: u16,
    offset: u64,
) -> Result<LogicalTablePage, EngineError> {
    let inspection = inspect_logical_table(engine, table).await?;
    let page_slices = page_slices(&inspection.shards, offset, u64::from(limit) + 1);
    let quoted_table = quote_identifier(table);
    let order_by = deterministic_order_by(inspection.columns());
    let page_results = stream::iter(page_slices)
        .map(|slice| {
            let engine = engine.clone();
            let table = table.to_owned();
            let quoted_table = quoted_table.clone();
            let order_by = order_by.clone();
            let context = inspection.context.clone();
            async move {
                let session = engine.session();
                let result = engine
                    .inspect_shard_with_context(
                        &session,
                        slice.shard,
                        Statement::new(
                            format!(
                                "SELECT browse.* FROM {quoted_table} AS browse \
                                 WHERE EXISTS (\
                                     SELECT 1 FROM pragma_table_list \
                                     WHERE schema = 'main' AND type = 'table' \
                                       AND name = ?1 COLLATE BINARY \
                                       AND lower(name) NOT GLOB 'sqlite_*' \
                                       AND lower(name) != 'briskdb' \
                                       AND lower(name) NOT GLOB 'briskdb_*'\
                                 ) {order_by} LIMIT ?2 OFFSET ?3"
                            ),
                            vec![
                                Value::Text(table),
                                Value::Int64(slice.limit as i64),
                                Value::Int64(slice.offset as i64),
                            ],
                        ),
                        context,
                    )
                    .await?;
                Ok::<_, EngineError>(Routed {
                    shard: slice.shard,
                    value: result,
                })
            }
        })
        .buffer_unordered(MAX_INSPECTION_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await;
    ensure_schema_generation(
        inspection.schema_generation,
        engine.catalog().schema_generation(),
    )?;
    let mut page_results = page_results?;
    page_results.sort_unstable_by_key(|result| result.shard);
    let rows = if page_results.is_empty() {
        Vec::new()
    } else {
        let merged = merge_scatter_results(page_results, inspection.result_limits)?;
        if merged.columns() != inspection.columns() {
            return Err(EngineError::new(
                EngineErrorKind::DataCorruption,
                "admin logical page observed incompatible physical columns",
            ));
        }
        merged.into_parts().1
    };
    Ok(LogicalTablePage {
        scope: inspection.scope,
        visited_shards: inspection.visited_shards(),
        columns: inspection.columns().to_vec(),
        rows,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageSlice {
    shard: u16,
    offset: u64,
    limit: u64,
}

fn page_slices(shards: &[ShardTableInspection], offset: u64, limit: u64) -> Vec<PageSlice> {
    let mut remaining_offset = offset;
    let mut remaining_limit = limit;
    let mut slices = Vec::new();
    for shard in shards {
        if remaining_limit == 0 {
            break;
        }
        if remaining_offset >= shard.rows {
            remaining_offset -= shard.rows;
            continue;
        }
        let available = shard.rows - remaining_offset;
        let take = available.min(remaining_limit);
        slices.push(PageSlice {
            shard: shard.shard,
            offset: remaining_offset,
            limit: take,
        });
        remaining_limit -= take;
        remaining_offset = 0;
    }
    slices
}

fn deterministic_order_by(columns: &[Column]) -> String {
    if columns.is_empty() {
        String::new()
    } else {
        format!(
            "ORDER BY {}",
            columns
                .iter()
                .map(|column| format!("browse.{}", quote_identifier(&column.name)))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

fn page_response(table: String, page: LogicalTablePage, limit: u16, offset: u64) -> Response {
    let columns = page
        .columns
        .into_iter()
        .map(|column| super::QueryColumn {
            name: column.name,
            data_type: super::data_type_name(column.data_type),
        })
        .collect();
    let mut rows = page
        .rows
        .into_iter()
        .map(|row| {
            row.into_values()
                .into_iter()
                .map(admin_value_to_json)
                .collect()
        })
        .collect::<Vec<_>>();
    let has_more = rows.len() > usize::from(limit)
        && offset
            .checked_add(u64::from(limit))
            .is_some_and(|next_offset| next_offset <= MAX_PAGE_OFFSET);
    rows.truncate(usize::from(limit));
    admin_json(
        StatusCode::OK,
        &RowsResponse {
            table,
            scope: page.scope,
            visited_shards: page.visited_shards,
            ordering: "shard_major_then_all_columns",
            limit,
            offset,
            has_more,
            columns,
            rows,
        },
    )
}

fn admin_value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Int64(value) if value.unsigned_abs() > MAX_JAVASCRIPT_SAFE_INTEGER => {
            tagged_integer("int64", value)
        }
        Value::UInt64(value) if value > MAX_JAVASCRIPT_SAFE_INTEGER => {
            tagged_integer("uint64", value)
        }
        value => super::value_to_json(value),
    }
}

fn tagged_integer(kind: &'static str, value: impl ToString) -> JsonValue {
    json!({
        "$briskdb_type": kind,
        "value": value.to_string(),
    })
}

fn table_names(result: ResultSet) -> Result<Vec<String>, EngineError> {
    result
        .into_parts()
        .1
        .into_iter()
        .map(|row| match row.into_values().into_iter().next() {
            Some(Value::Text(name)) if is_visible_table_name(&name) => Ok(name),
            _ => Err(EngineError::new(
                EngineErrorKind::Internal,
                "admin table discovery returned an unexpected value",
            )),
        })
        .collect()
}

fn is_visible_table_name(name: &str) -> bool {
    if name.contains('\0') {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    lower != "briskdb" && !lower.starts_with("briskdb_") && !lower.starts_with("sqlite_")
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn hex_token(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity(64);
    for byte in bytes {
        token.push(char::from(HEX[usize::from(byte >> 4)]));
        token.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    token
}

enum PresentedSession {
    Missing,
    ValidFormat(String),
    Invalid,
}

fn presented_session(headers: &HeaderMap) -> PresentedSession {
    let mut found = None;
    for header in headers.get_all(COOKIE) {
        let Ok(header) = header.to_str() else {
            return PresentedSession::Invalid;
        };
        for pair in header.split(';') {
            let Some((name, value)) = pair.trim().split_once('=') else {
                continue;
            };
            if name.trim() != SESSION_COOKIE {
                continue;
            }
            if found.is_some() || !valid_token_format(value.trim()) {
                return PresentedSession::Invalid;
            }
            found = Some(value.trim().to_owned());
        }
    }
    found.map_or(PresentedSession::Missing, PresentedSession::ValidFormat)
}

fn presented_valid_tokens(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|header| header.to_str().ok())
        .flat_map(|header| header.split(';'))
        .filter_map(|pair| pair.trim().split_once('='))
        .filter_map(|(name, value)| {
            let value = value.trim();
            (name.trim() == SESSION_COOKIE && valid_token_format(value)).then(|| value.to_owned())
        })
        .collect()
}

fn valid_token_format(token: &str) -> bool {
    token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn authenticate(headers: &HeaderMap, sessions: &SessionStore) -> Result<String, ()> {
    match presented_session(headers) {
        PresentedSession::ValidFormat(token) if sessions.validate(&token) => Ok(token),
        PresentedSession::Missing
        | PresentedSession::ValidFormat(_)
        | PresentedSession::Invalid => Err(()),
    }
}

fn session_cookie(token: &str) -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={token}; Path=/admin; HttpOnly; SameSite=Strict; Max-Age=28800"
    ))
    .expect("a lowercase hexadecimal token is a valid cookie value")
}

fn clear_session_cookie() -> HeaderValue {
    HeaderValue::from_static(
        "briskdb_admin_session=; Path=/admin; HttpOnly; SameSite=Strict; Max-Age=0",
    )
}

fn static_response(content_type: &'static str, body: &'static str) -> Response {
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    no_store(response)
}

fn admin_json(status: StatusCode, body: &impl Serialize) -> Response {
    no_store((status, Json(body)).into_response())
}

fn auth_json(status: StatusCode, body: JsonValue, cookie: Option<HeaderValue>) -> Response {
    let mut response = admin_json(status, &body);
    if let Some(cookie) = cookie {
        response.headers_mut().insert(SET_COOKIE, cookie);
    }
    response
}

fn authentication_required() -> Response {
    auth_json(
        StatusCode::UNAUTHORIZED,
        json!({
            "code": "authentication_required",
            "message": "Log in to continue."
        }),
        None,
    )
}

fn invalid_argument(diagnostic: &'static str) -> Response {
    admin_engine_error(EngineError::new(
        EngineErrorKind::InvalidArgument,
        diagnostic,
    ))
}

fn admin_engine_error(error: EngineError) -> Response {
    no_store(super::ApiError(error).into_response())
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, NO_STORE.clone());
    response
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::core::{
        Column, DataType, Database, Row, ShardKeyMetadata, ShardKeyType, TableDeclaration,
    };

    fn token(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }

    fn application() -> (tempfile::TempDir, Router) {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        let router = super::super::router(database);
        (temp, router)
    }

    async fn request(
        app: &Router,
        method: Method,
        uri: &str,
        cookie: Option<&str>,
        body: Option<JsonValue>,
    ) -> Response {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(cookie) = cookie {
            builder = builder.header(COOKIE, cookie);
        }
        let body = if let Some(body) = body {
            builder = builder.header(CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&body).unwrap())
        } else {
            Body::empty()
        };
        app.clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap()
    }

    async fn body_json(response: Response) -> JsonValue {
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn login_cookie(app: &Router) -> HeaderValue {
        let response = request(
            app,
            Method::POST,
            "/admin/api/login",
            None,
            Some(json!({"username": "admin", "password": "admin"})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        response.headers()[SET_COOKIE].clone()
    }

    #[test]
    fn token_encoding_is_exact_lowercase_hex() {
        let bytes = std::array::from_fn(|index| index as u8);
        assert_eq!(
            hex_token(&bytes),
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        );
    }

    #[test]
    fn session_expiry_capacity_eviction_and_revocation_are_deterministic() {
        let sessions = SessionStore::new();
        let now = Instant::now();
        let first = token('0');
        sessions.issue_at(first.clone(), now);
        assert!(sessions.validate_at(&first, now + SESSION_LIFETIME - Duration::from_nanos(1)));
        assert!(!sessions.validate_at(&first, now + SESSION_LIFETIME));

        for index in 0..MAX_SESSIONS {
            sessions.issue_at(
                format!("{index:064x}"),
                now + Duration::from_nanos(index as u64),
            );
        }
        assert_eq!(sessions.lock().by_token.len(), MAX_SESSIONS);
        sessions.issue_at(token('f'), now + Duration::from_secs(1));
        assert_eq!(sessions.lock().by_token.len(), MAX_SESSIONS);
        assert!(!sessions.validate_at(&format!("{:064x}", 0), now));
        assert!(sessions.revoke(&token('f')));
        assert!(!sessions.revoke(&token('f')));
    }

    #[test]
    fn cloned_store_is_safe_for_independent_concurrent_sessions() {
        let sessions = SessionStore::new();
        let now = Instant::now();
        let mut workers = Vec::new();
        for index in 0..32 {
            let sessions = sessions.clone();
            workers.push(thread::spawn(move || {
                let token = format!("{index:064x}");
                sessions.issue_at(token.clone(), now);
                assert!(sessions.validate_at(&token, now));
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(sessions.lock().by_token.len(), 32);
    }

    #[test]
    fn cookie_parser_accepts_one_token_and_rejects_malformed_or_duplicate_values() {
        let value = token('a');
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("theme=dark; {SESSION_COOKIE}={value}; x=1")).unwrap(),
        );
        assert!(matches!(
            presented_session(&headers),
            PresentedSession::ValidFormat(found) if found == value
        ));

        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{SESSION_COOKIE}={value}; {SESSION_COOKIE}={value}"
            ))
            .unwrap(),
        );
        assert!(matches!(
            presented_session(&headers),
            PresentedSession::Invalid
        ));
        headers.insert(
            COOKIE,
            HeaderValue::from_static("briskdb_admin_session=UPPERCASE"),
        );
        assert!(matches!(
            presented_session(&headers),
            PresentedSession::Invalid
        ));
    }

    #[test]
    fn table_names_are_filtered_and_identifiers_are_quoted() {
        for hidden in ["briskdb", "BriskDB_meta", "SQLITE_sequence", "bad\0name"] {
            assert!(!is_visible_table_name(hidden));
        }
        assert!(is_visible_table_name(""));
        assert!(is_visible_table_name("orders"));
        assert!(is_visible_table_name("snowman_☃"));
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn typed_table_discovery_excludes_virtual_and_shadow_objects() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE ordinary (id INTEGER); \
                 CREATE VIRTUAL TABLE docs USING fts5(body);",
            )
            .unwrap();
        let mut statement = connection.prepare(TABLE_DISCOVERY_SQL).unwrap();
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(names, ["ordinary"]);
    }

    #[tokio::test]
    async fn embedded_shell_and_assets_are_public_deterministic_and_same_origin() {
        let (_temp, app) = application();
        for (uri, content_type, marker) in [
            ("/admin", "text/html; charset=utf-8", "BriskDB Data Browser"),
            (
                "/admin/assets/styles.css",
                "text/css; charset=utf-8",
                "--accent",
            ),
            (
                "/admin/assets/logic.js",
                "text/javascript; charset=utf-8",
                "createAuthEpoch",
            ),
            (
                "/admin/assets/app.js",
                "text/javascript; charset=utf-8",
                "textContent",
            ),
        ] {
            let response = request(&app, Method::GET, uri, None, None).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[CONTENT_TYPE], content_type);
            assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
            let text = String::from_utf8(
                to_bytes(response.into_body(), 1_048_576)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert!(text.contains(marker));
            assert!(!text.contains("https://"));
            assert!(!text.contains("http://"));
        }

        for id in [
            "login-form",
            "browser-view",
            "scope-kicker",
            "table-list",
            "record-count",
            "status",
            "data-head",
            "data-body",
            "previous-page",
            "next-page",
        ] {
            assert!(INDEX_HTML.contains(&format!("id=\"{id}\"")));
            assert!(APP_JS.contains(&format!("#{id}")));
        }
        assert!(!INDEX_HTML.contains("shard-select"));
        assert!(!APP_JS.contains("shard-select"));
        assert!(!APP_JS.contains("selected_shard"));
        assert!(!APP_JS.contains("innerHTML"));
        assert!(APP_JS.contains("(empty table name)"));
        assert!(APP_JS.contains("(whitespace-only table name)"));
        assert!(APP_JS.contains("state.table === null"));
        assert!(APP_JS.contains("logic.acceptAuthenticationFailure"));
        assert!(APP_JS.contains("logic.acceptsTableResponse"));
        assert!(APP_JS.contains("logic.cellPresentation"));
        assert!(APP_JS.contains("logic.pageSummary"));
        assert!(APP_JS.contains("logic.rowCountPresentation"));
        assert!(APP_JS.contains("state.countRequest"));
        assert!(APP_JS.contains("/admin/api/count?"));
        let logic_position = INDEX_HTML
            .find("/admin/assets/logic.js")
            .expect("the shell loads the tested browser logic");
        let app_position = INDEX_HTML
            .find("/admin/assets/app.js")
            .expect("the shell loads the application script");
        assert!(logic_position < app_position);
    }

    #[tokio::test]
    async fn exact_login_session_and_idempotent_logout_flow() {
        let (_temp, app) = application();
        for credentials in [
            json!({"username": "Admin", "password": "admin"}),
            json!({"username": "admin", "password": "Admin"}),
            json!({"username": "", "password": ""}),
        ] {
            let response = request(
                &app,
                Method::POST,
                "/admin/api/login",
                None,
                Some(credentials),
            )
            .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().get(SET_COOKIE).is_none());
            assert_eq!(
                body_json(response).await,
                json!({"code":"invalid_credentials","message":"Invalid username or password."})
            );
        }

        let set_cookie = login_cookie(&app).await;
        let set_cookie = set_cookie.to_str().unwrap();
        assert!(set_cookie.contains("Path=/admin"));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        assert!(set_cookie.contains("Max-Age=28800"));
        let cookie = set_cookie.split(';').next().unwrap();
        assert!(valid_token_format(cookie.split_once('=').unwrap().1));

        let response = request(&app, Method::GET, "/admin/api/session", Some(cookie), None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await,
            json!({"authenticated":true,"username":"admin"})
        );

        let response = request(&app, Method::POST, "/admin/api/logout", Some(cookie), None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[SET_COOKIE]
                .to_str()
                .unwrap()
                .contains("Max-Age=0")
        );

        let rejected = request(&app, Method::GET, "/admin/api/session", Some(cookie), None).await;
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            body_json(rejected).await,
            json!({"code":"authentication_required","message":"Log in to continue."})
        );

        let response = request(&app, Method::POST, "/admin/api/logout", None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({"authenticated":false}));
    }

    #[tokio::test]
    async fn overview_count_and_rows_require_authentication_and_validate_bounds() {
        let (_temp, app) = application();
        for uri in [
            "/admin/api/session",
            "/admin/api/overview",
            "/admin/api/count?table=widgets",
            "/admin/api/count",
            "/admin/api/rows?table=widgets",
            "/admin/api/rows?table=widgets&shard=not-a-number",
        ] {
            let response = request(&app, Method::GET, uri, None, None).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().get(SET_COOKIE).is_none());
        }

        let set_cookie = login_cookie(&app).await;
        let cookie = set_cookie.to_str().unwrap().split(';').next().unwrap();
        for uri in [
            "/admin/api/count",
            "/admin/api/rows?table=widgets&limit=0",
            "/admin/api/rows?table=widgets&limit=201",
            "/admin/api/rows?table=widgets&offset=1000001",
        ] {
            let response = request(&app, Method::GET, uri, Some(cookie), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    fn routing_key_for_shard(database: &Database, target: u16) -> String {
        (0_u64..)
            .map(|candidate| format!("admin-browser-{candidate}"))
            .find(|candidate| database.shard_for_key(candidate.as_bytes()) == target)
            .unwrap()
    }

    async fn response_json(response: Response) -> (StatusCode, JsonValue) {
        let status = response.status();
        (status, body_json(response).await)
    }

    #[tokio::test]
    async fn authenticated_browser_discovers_and_pages_the_logical_table_across_files() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        database
            .broadcast(
                "CREATE TABLE widgets (\
                    id INTEGER PRIMARY KEY, \
                    label TEXT NOT NULL, \
                    payload BLOB, \
                    note TEXT\
                 ); \
                 CREATE TABLE \"\" (value TEXT); \
                 CREATE TABLE \" \" (value TEXT); \
                 CREATE TABLE dupes (value TEXT); \
                 CREATE TABLE \"odd\"\"table\" (value TEXT); \
                 CREATE VIEW widget_view AS SELECT id FROM widgets;",
            )
            .unwrap();
        for shard in 0..2 {
            let routing_key = routing_key_for_shard(&database, shard);
            let row_count = if shard == 0 { 2 } else { 3 };
            for id in 1..=row_count {
                database
                    .execute(
                        &routing_key,
                        "INSERT INTO widgets (id, label, payload, note) VALUES (?1, ?2, ?3, ?4)",
                        &[
                            Value::Int64(id),
                            Value::Text(format!("shard-{shard}-row-{id}")),
                            Value::Binary(vec![shard as u8, id as u8]),
                            Value::Null,
                        ],
                    )
                    .unwrap();
            }
            database
                .execute(
                    &routing_key,
                    "INSERT INTO dupes (value) VALUES ('same')",
                    &[],
                )
                .unwrap();
        }
        let app = super::super::router(database);
        let set_cookie = login_cookie(&app).await;
        let cookie = set_cookie.to_str().unwrap().split(';').next().unwrap();

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/overview?shard=1",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "scope": "empty_catalog_shard_zero_fallback",
                    "shard_count": 2,
                    "visited_shards": [0],
                    "tables": ["", " ", "dupes", "odd\"table", "widgets"]
                })
            )
        );

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/count?table=widgets",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "widgets",
                    "scope": "empty_catalog_all_physical_shards",
                    "visited_shards": [0, 1],
                    "total_rows": 5
                })
            )
        );

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/count?table=odd%22table",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "odd\"table",
                    "scope": "empty_catalog_all_physical_shards",
                    "visited_shards": [0, 1],
                    "total_rows": 0
                })
            )
        );

        let (_, duplicates) = response_json(
            request(
                &app,
                Method::GET,
                "/admin/api/rows?table=dupes&limit=50&offset=0",
                Some(cookie),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(duplicates["rows"], json!([["same"], ["same"]]));

        let (status, first_page) = response_json(
            request(
                &app,
                Method::GET,
                "/admin/api/rows?table=widgets&limit=2&offset=0",
                Some(cookie),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first_page["table"], "widgets");
        assert_eq!(first_page["scope"], "empty_catalog_all_physical_shards");
        assert_eq!(first_page["visited_shards"], json!([0, 1]));
        assert_eq!(first_page["ordering"], "shard_major_then_all_columns");
        assert_eq!(first_page["limit"], 2);
        assert_eq!(first_page["offset"], 0);
        assert_eq!(first_page["has_more"], true);
        assert_eq!(
            first_page["columns"],
            json!([
                {"name":"id","data_type":"unknown"},
                {"name":"label","data_type":"unknown"},
                {"name":"payload","data_type":"unknown"},
                {"name":"note","data_type":"unknown"}
            ])
        );
        assert_eq!(
            first_page["rows"],
            json!([
                [1, "shard-0-row-1", [0, 1], null],
                [2, "shard-0-row-2", [0, 2], null]
            ])
        );

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/rows?table=widgets&limit=2&offset=2&shard=0",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "widgets",
                    "scope": "empty_catalog_all_physical_shards",
                    "visited_shards": [0, 1],
                    "ordering": "shard_major_then_all_columns",
                    "limit": 2,
                    "offset": 2,
                    "has_more": true,
                    "columns": [
                        {"name":"id","data_type":"unknown"},
                        {"name":"label","data_type":"unknown"},
                        {"name":"payload","data_type":"unknown"},
                        {"name":"note","data_type":"unknown"}
                    ],
                    "rows": [
                        [1, "shard-1-row-1", [1, 1], null],
                        [2, "shard-1-row-2", [1, 2], null]
                    ]
                })
            )
        );

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/rows?table=widgets&limit=2&offset=4",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await
            .1["rows"],
            json!([[3, "shard-1-row-3", [1, 3], null]])
        );

        let odd_table = "/admin/api/rows?table=odd%22table&limit=50&offset=0";
        let (status, empty) =
            response_json(request(&app, Method::GET, odd_table, Some(cookie), None).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["rows"], json!([]));
        assert_eq!(empty["has_more"], false);

        let empty_name = "/admin/api/rows?table=&limit=50&offset=0";
        let (status, empty) =
            response_json(request(&app, Method::GET, empty_name, Some(cookie), None).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["table"], "");
        assert_eq!(empty["rows"], json!([]));

        let whitespace_name = "/admin/api/rows?table=%20&limit=50&offset=0";
        let (status, whitespace) =
            response_json(request(&app, Method::GET, whitespace_name, Some(cookie), None).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(whitespace["table"], " ");

        let ignored_shard_zero = request(
            &app,
            Method::GET,
            "/admin/api/rows?shard=0&table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        );
        let ignored_shard_one = request(
            &app,
            Method::GET,
            "/admin/api/rows?shard=1&table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        );
        let (ignored_shard_zero, ignored_shard_one) =
            tokio::join!(ignored_shard_zero, ignored_shard_one);
        let (_, ignored_shard_zero) = response_json(ignored_shard_zero).await;
        let (_, ignored_shard_one) = response_json(ignored_shard_one).await;
        assert_eq!(ignored_shard_zero, ignored_shard_one);
        assert_eq!(ignored_shard_zero["rows"][0][1], "shard-0-row-1");

        for table in ["briskdb_shard_metadata", "widget_view", "guessed_table"] {
            let uri = format!("/admin/api/rows?table={table}");
            let response = request(&app, Method::GET, &uri, Some(cookie), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{table}");
            let uri = format!("/admin/api/count?table={table}");
            let response = request(&app, Method::GET, &uri, Some(cookie), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{table}");
        }
        let recovered = request(
            &app,
            Method::GET,
            "/admin/api/rows?table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        )
        .await;
        assert_eq!(recovered.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn populated_catalog_drives_sharded_and_global_browser_targets() {
        let temp = tempfile::tempdir().unwrap();
        let mut database = Database::open(temp.path(), 2).unwrap();
        database
            .broadcast(
                "CREATE TABLE events (\
                    tenant_key TEXT NOT NULL PRIMARY KEY, \
                    payload TEXT NOT NULL\
                 ); \
                 CREATE TABLE countries (\
                    code TEXT NOT NULL PRIMARY KEY, \
                    label TEXT NOT NULL\
                 );",
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
                TableDeclaration::global(logical_database, "countries").unwrap(),
                TableDeclaration::catalog(logical_database, "manifest_records").unwrap(),
            ])
            .unwrap();

        let tenant_keys = [0_u16, 1_u16].map(|shard| routing_key_for_shard(&database, shard));
        for (shard, tenant_key) in tenant_keys.iter().enumerate() {
            let inserted = database
                .execute_routed(
                    tenant_key,
                    "INSERT INTO events (tenant_key, payload) VALUES (?1, 'same payload')",
                    &[Value::Text(tenant_key.clone())],
                )
                .unwrap();
            assert_eq!(inserted.shard, shard as u16);
        }

        for (shard, label) in [(0_u16, "canonical"), (1_u16, "noncanonical copy")] {
            rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap()
                .execute(
                    "INSERT INTO countries (code, label) VALUES ('US', ?1)",
                    [label],
                )
                .unwrap();
        }

        let app = super::super::router(Arc::new(database));
        let set_cookie = login_cookie(&app).await;
        let cookie = set_cookie.to_str().unwrap().split(';').next().unwrap();

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/overview?shard=1",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "scope": "logical_default_database",
                    "shard_count": 2,
                    "visited_shards": [],
                    "tables": ["countries", "events"]
                })
            )
        );

        let (_, sharded) = response_json(
            request(
                &app,
                Method::GET,
                "/admin/api/rows?table=events&limit=50&offset=0&shard=1",
                Some(cookie),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(sharded["scope"], "logical_sharded_table");
        assert_eq!(sharded["visited_shards"], json!([0, 1]));
        assert_eq!(sharded["rows"].as_array().unwrap().len(), 2);
        assert_eq!(sharded["rows"][0][1], "same payload");
        assert_eq!(sharded["rows"][1][1], "same payload");

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/count?table=events",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "events",
                    "scope": "logical_sharded_table",
                    "visited_shards": [0, 1],
                    "total_rows": 2
                })
            )
        );

        let (_, global) = response_json(
            request(
                &app,
                Method::GET,
                "/admin/api/rows?table=countries&limit=50&offset=0",
                Some(cookie),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(global["scope"], "logical_global_table");
        assert_eq!(global["visited_shards"], json!([0]));
        assert_eq!(global["rows"], json!([["US", "canonical"]]));
        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/count?table=countries&shard=1",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "countries",
                    "scope": "logical_global_table",
                    "visited_shards": [0],
                    "total_rows": 1
                })
            )
        );

        for endpoint in ["rows", "count"] {
            let response = request(
                &app,
                Method::GET,
                &format!("/admin/api/{endpoint}?table=manifest_records"),
                Some(cookie),
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn logical_count_and_rows_return_no_partial_data_after_one_file_fails() {
        let temp = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::open(temp.path(), 2).unwrap());
        database
            .broadcast(
                "CREATE TABLE counted (id INTEGER PRIMARY KEY); \
                 CREATE TABLE control (id INTEGER PRIMARY KEY);",
            )
            .unwrap();
        let app = super::super::router(Arc::clone(&database));
        let set_cookie = login_cookie(&app).await;
        let cookie = set_cookie.to_str().unwrap().split(';').next().unwrap();

        let primed = request(
            &app,
            Method::GET,
            "/admin/api/count?table=counted",
            Some(cookie),
            None,
        )
        .await;
        assert_eq!(primed.status(), StatusCode::OK);

        rusqlite::Connection::open(temp.path().join("shards/0001.sqlite"))
            .unwrap()
            .execute_batch("DROP TABLE counted")
            .unwrap();

        let failed = request(
            &app,
            Method::GET,
            "/admin/api/count?table=counted",
            Some(cookie),
            None,
        )
        .await;
        assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
        let failed = body_json(failed).await;
        assert_eq!(failed["code"], "invalid_argument");
        assert!(failed.get("total_rows").is_none());

        let failed = request(
            &app,
            Method::GET,
            "/admin/api/rows?table=counted&limit=50&offset=0",
            Some(cookie),
            None,
        )
        .await;
        assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
        let failed = body_json(failed).await;
        assert_eq!(failed["code"], "invalid_argument");
        assert!(failed.get("rows").is_none());

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/count?table=control",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "table": "control",
                    "scope": "empty_catalog_all_physical_shards",
                    "visited_shards": [0, 1],
                    "total_rows": 0
                })
            )
        );
    }

    #[tokio::test]
    async fn browser_sessions_are_independent_and_unknown_tokens_are_rejected() {
        let (_temp, app) = application();
        let first = login_cookie(&app).await;
        let second = login_cookie(&app).await;
        let first = first.to_str().unwrap().split(';').next().unwrap();
        let second = second.to_str().unwrap().split(';').next().unwrap();
        assert_ne!(first, second);

        let duplicate_first = format!("{first}; {first}");
        let logged_out = request(
            &app,
            Method::POST,
            "/admin/api/logout",
            Some(&duplicate_first),
            None,
        )
        .await;
        assert_eq!(logged_out.status(), StatusCode::OK);
        assert_eq!(
            request(&app, Method::GET, "/admin/api/session", Some(first), None,)
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request(&app, Method::GET, "/admin/api/session", Some(second), None,)
                .await
                .status(),
            StatusCode::OK
        );

        for invalid in [
            "briskdb_admin_session=short",
            "briskdb_admin_session=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "briskdb_admin_session=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ] {
            let response =
                request(&app, Method::GET, "/admin/api/session", Some(invalid), None).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(response.headers().get(SET_COOKIE).is_none());
        }
    }

    #[tokio::test]
    async fn stale_authentication_failure_cannot_clear_a_newer_login_cookie() {
        let (_temp, app) = application();
        let stale = request(
            &app,
            Method::GET,
            "/admin/api/session",
            Some("briskdb_admin_session=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            None,
        )
        .await;
        let current = login_cookie(&app).await;
        let current = current.to_str().unwrap().split(';').next().unwrap();

        assert_eq!(stale.status(), StatusCode::UNAUTHORIZED);
        assert!(stale.headers().get(SET_COOKIE).is_none());
        assert_eq!(
            request(&app, Method::GET, "/admin/api/session", Some(current), None,)
                .await
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn framework_login_rejections_are_never_cached() {
        let (_temp, app) = application();
        for (content_type, body) in [
            ("application/json", "{".to_owned()),
            ("text/plain", "username=admin&password=admin".to_owned()),
            ("application/json", "x".repeat(1_025)),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/admin/api/login")
                        .header(CONTENT_TYPE, content_type)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(response.status().is_client_error());
            assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
            assert!(response.headers().get(SET_COOKIE).is_none());
        }
    }

    #[tokio::test]
    async fn maximum_offset_never_advertises_an_unrequestable_next_page() {
        let result = ResultSet::new(
            vec![Column::new("value", DataType::Unknown)],
            vec![
                Row::new(vec![Value::Int64(1)]),
                Row::new(vec![Value::Int64(2)]),
            ],
        )
        .unwrap();
        let (columns, rows) = result.into_parts();
        let body = body_json(page_response(
            "widgets".to_owned(),
            LogicalTablePage {
                scope: "logical_sharded_table",
                visited_shards: vec![0, 1],
                columns,
                rows,
            },
            1,
            MAX_PAGE_OFFSET,
        ))
        .await;
        assert_eq!(body["rows"], json!([[1]]));
        assert_eq!(body["has_more"], false);
    }

    #[tokio::test]
    async fn admin_page_response_preserves_extreme_integer_text() {
        let result = ResultSet::new(
            vec![
                Column::new("minimum", DataType::Int64),
                Column::new("maximum", DataType::Int64),
                Column::new("unsigned", DataType::UInt64),
            ],
            vec![Row::new(vec![
                Value::Int64(i64::MIN),
                Value::Int64(i64::MAX),
                Value::UInt64(u64::MAX),
            ])],
        )
        .unwrap();
        let (columns, rows) = result.into_parts();
        let body = body_json(page_response(
            "numbers".to_owned(),
            LogicalTablePage {
                scope: "logical_sharded_table",
                visited_shards: vec![0, 1],
                columns,
                rows,
            },
            50,
            0,
        ))
        .await;

        assert_eq!(
            body["rows"],
            json!([[{
                "$briskdb_type": "int64",
                "value": "-9223372036854775808"
            }, {
                "$briskdb_type": "int64",
                "value": "9223372036854775807"
            }, {
                "$briskdb_type": "uint64",
                "value": "18446744073709551615"
            }]])
        );
    }

    #[test]
    fn admin_integer_encoding_is_exact_at_javascript_boundaries() {
        let safe = MAX_JAVASCRIPT_SAFE_INTEGER as i64;
        for value in [-safe, safe] {
            assert_eq!(admin_value_to_json(Value::Int64(value)), json!(value));
        }
        assert_eq!(
            admin_value_to_json(Value::Int64(-safe - 1)),
            json!({"$briskdb_type":"int64","value":"-9007199254740992"})
        );
        assert_eq!(
            admin_value_to_json(Value::Int64(safe + 1)),
            json!({"$briskdb_type":"int64","value":"9007199254740992"})
        );
        assert_eq!(
            admin_value_to_json(Value::Int64(i64::MIN)),
            json!({"$briskdb_type":"int64","value":"-9223372036854775808"})
        );
        assert_eq!(
            admin_value_to_json(Value::Int64(i64::MAX)),
            json!({"$briskdb_type":"int64","value":"9223372036854775807"})
        );
        assert_eq!(
            admin_value_to_json(Value::UInt64(MAX_JAVASCRIPT_SAFE_INTEGER)),
            json!(MAX_JAVASCRIPT_SAFE_INTEGER)
        );
        assert_eq!(
            admin_value_to_json(Value::UInt64(MAX_JAVASCRIPT_SAFE_INTEGER + 1)),
            json!({"$briskdb_type":"uint64","value":"9007199254740992"})
        );
        assert_eq!(
            admin_value_to_json(Value::UInt64(u64::MAX)),
            json!({"$briskdb_type":"uint64","value":"18446744073709551615"})
        );
    }

    #[test]
    fn logical_count_and_page_helpers_validate_shapes_bounds_and_exact_json() {
        let result =
            |rows| ResultSet::new(vec![Column::new("row_count", DataType::Unknown)], rows).unwrap();
        assert_eq!(
            row_count(result(vec![Row::new(vec![Value::Int64(5)])])).unwrap(),
            5
        );
        assert_eq!(
            row_count(result(vec![Row::new(vec![Value::UInt64(u64::MAX)])])).unwrap(),
            u64::MAX
        );
        for malformed in [
            result(vec![]),
            result(vec![Row::new(vec![Value::Int64(-1)])]),
            result(vec![Row::new(vec![Value::Text("5".to_owned())])]),
            result(vec![
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)]),
            ]),
        ] {
            assert_eq!(
                row_count(malformed).unwrap_err().kind(),
                EngineErrorKind::Internal
            );
        }

        assert_eq!(sum_row_counts([2, 3, 5]).unwrap(), 10);
        assert_eq!(sum_row_counts(std::iter::empty()).unwrap(), 0);
        assert_eq!(
            sum_row_counts([u64::MAX, 1]).unwrap_err().kind(),
            EngineErrorKind::NumericOutOfRange
        );
        assert!(ensure_schema_generation(7, 7).is_ok());
        let changed = ensure_schema_generation(7, 8).unwrap_err();
        assert_eq!(changed.kind(), EngineErrorKind::Busy);
        assert!(changed.is_retryable());

        let body = serde_json::to_value(CountResponse {
            table: "events".to_owned(),
            scope: "logical_sharded_table",
            visited_shards: (0..64).collect(),
            total_rows: admin_value_to_json(Value::UInt64(MAX_JAVASCRIPT_SAFE_INTEGER + 1)),
        })
        .unwrap();
        assert_eq!(body["scope"], "logical_sharded_table");
        assert_eq!(body["visited_shards"].as_array().unwrap().len(), 64);
        assert_eq!(
            body["total_rows"],
            json!({"$briskdb_type":"uint64","value":"9007199254740992"})
        );

        let columns = vec![Column::new("value", DataType::Unknown)];
        let shards = [2_u64, 3, 0, 4]
            .into_iter()
            .enumerate()
            .map(|(shard, rows)| ShardTableInspection {
                shard: shard as u16,
                rows,
                columns: columns.clone(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            page_slices(&shards, 1, 5),
            vec![
                PageSlice {
                    shard: 0,
                    offset: 1,
                    limit: 1,
                },
                PageSlice {
                    shard: 1,
                    offset: 0,
                    limit: 3,
                },
                PageSlice {
                    shard: 3,
                    offset: 0,
                    limit: 1,
                },
            ]
        );
        assert_eq!(
            deterministic_order_by(&[
                Column::new("a", DataType::Unknown),
                Column::new("odd\"name", DataType::Unknown),
            ]),
            "ORDER BY browse.\"a\", browse.\"odd\"\"name\""
        );
    }

    #[tokio::test]
    async fn expired_session_is_rejected_and_successful_relogin_replaces_presented_session() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = SessionStore::new();
        let expired = token('e');
        sessions.issue_at(
            expired.clone(),
            Instant::now() - SESSION_LIFETIME - Duration::from_secs(1),
        );
        let state = HttpState {
            engine: crate::core::Engine::from_database(Arc::new(
                Database::open(temp.path(), 2).unwrap(),
            )),
            admin_sessions: sessions,
        };
        let app = routes(state.clone()).with_state(state);
        let expired_cookie = format!("{SESSION_COOKIE}={expired}");
        let response = request(
            &app,
            Method::GET,
            "/admin/api/session",
            Some(&expired_cookie),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let first = login_cookie(&app).await;
        let first = first.to_str().unwrap().split(';').next().unwrap();
        let replacement = request(
            &app,
            Method::POST,
            "/admin/api/login",
            Some(first),
            Some(json!({"username":"admin","password":"admin"})),
        )
        .await;
        assert_eq!(replacement.status(), StatusCode::OK);
        let replacement_cookie = replacement.headers()[SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        assert_ne!(first, replacement_cookie);
        assert_eq!(
            request(&app, Method::GET, "/admin/api/session", Some(first), None,)
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            request(
                &app,
                Method::GET,
                "/admin/api/session",
                Some(replacement_cookie),
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );
    }

    #[test]
    fn admin_adapter_source_stays_above_storage_and_routing_internals() {
        let source = include_str!("admin.rs");
        let production = source.split_once("#[cfg(test)]").unwrap().0;
        for pieces in [
            ["rusi", "qlite"],
            ["crate::", "storage"],
            ["std::", "fs"],
            ["shard_", "for_key"],
        ] {
            let forbidden = pieces.concat();
            assert!(!production.contains(&forbidden), "found {forbidden}");
        }
    }
}
