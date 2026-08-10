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
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use super::HttpState;
use crate::core::{EngineError, EngineErrorKind, ResultSet, Statement, Value};

const INDEX_HTML: &str = include_str!("admin/index.html");
const STYLES_CSS: &str = include_str!("admin/styles.css");
const APP_JS: &str = include_str!("admin/app.js");

const ADMIN_USERNAME: &str = "admin";
const ADMIN_PASSWORD: &str = "admin";
const SESSION_COOKIE: &str = "briskdb_admin_session";
const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_SESSIONS: usize = 128;
const DEFAULT_PAGE_LIMIT: u16 = 50;
const MAX_PAGE_LIMIT: u16 = 200;
const MAX_PAGE_OFFSET: u64 = 1_000_000;
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
        .route("/admin/api/rows", get(rows))
        .route_layer(middleware::from_fn_with_state(state, require_authenticated));

    Router::new()
        .route("/admin", get(index))
        .route("/admin/", get(index))
        .route("/admin/assets/styles.css", get(styles))
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

#[derive(Deserialize)]
struct OverviewQuery {
    #[serde(default)]
    shard: Option<u16>,
}

#[derive(Serialize)]
struct OverviewResponse {
    shard_count: u16,
    selected_shard: u16,
    tables: Vec<String>,
}

async fn overview(State(state): State<HttpState>, Query(query): Query<OverviewQuery>) -> Response {
    let shard = query.shard.unwrap_or(0);
    let session = state.engine.session();
    let result = state
        .engine
        .inspect_shard(&session, shard, Statement::new(TABLE_DISCOVERY_SQL, vec![]))
        .await;

    match result.and_then(table_names) {
        Ok(tables) => admin_json(
            StatusCode::OK,
            &OverviewResponse {
                shard_count: state.engine.shard_count(),
                selected_shard: shard,
                tables,
            },
        ),
        Err(error) => admin_engine_error(error),
    }
}

#[derive(Deserialize)]
struct RowsQuery {
    shard: u16,
    table: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Serialize)]
struct RowsResponse {
    shard: u16,
    table: String,
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

    let session = state.engine.session();
    let verified = state
        .engine
        .inspect_shard(
            &session,
            query.shard,
            Statement::new(TABLE_LOOKUP_SQL, vec![Value::Text(query.table.clone())]),
        )
        .await;
    let verified = match verified.and_then(table_names) {
        Ok(tables) if tables.as_slice() == [query.table.as_str()] => true,
        Ok(_) => false,
        Err(error) => return admin_engine_error(error),
    };
    if !verified {
        return invalid_argument("admin table name is not browseable");
    }

    let quoted_table = quote_identifier(&query.table);
    let result = state
        .engine
        .inspect_shard(
            &session,
            query.shard,
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
                     ) \
                     LIMIT ?2 OFFSET ?3"
                ),
                vec![
                    Value::Text(query.table.clone()),
                    Value::Int64(i64::from(limit) + 1),
                    Value::Int64(offset as i64),
                ],
            ),
        )
        .await;

    match result {
        Ok(result) => page_response(query.shard, query.table, limit, offset, result),
        Err(error) => admin_engine_error(error),
    }
}

fn page_response(
    shard: u16,
    table: String,
    limit: u16,
    offset: u64,
    result: ResultSet,
) -> Response {
    let super::QueryResponse {
        columns, mut rows, ..
    } = super::result_set_to_query_response(shard, result);
    let has_more = rows.len() > usize::from(limit)
        && offset
            .checked_add(u64::from(limit))
            .is_some_and(|next_offset| next_offset <= MAX_PAGE_OFFSET);
    rows.truncate(usize::from(limit));
    admin_json(
        StatusCode::OK,
        &RowsResponse {
            shard,
            table,
            limit,
            offset,
            has_more,
            columns,
            rows,
        },
    )
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
        Some(clear_session_cookie()),
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
    use crate::core::{Column, DataType, Database, Row};

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
            "shard-select",
            "table-list",
            "status",
            "data-head",
            "data-body",
            "previous-page",
            "next-page",
        ] {
            assert!(INDEX_HTML.contains(&format!("id=\"{id}\"")));
            assert!(APP_JS.contains(&format!("#{id}")));
        }
        assert!(!APP_JS.contains("innerHTML"));
        assert!(APP_JS.contains("(empty table name)"));
        assert!(APP_JS.contains("(whitespace-only table name)"));
        assert!(APP_JS.contains("state.table === null"));
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
    async fn overview_and_rows_require_authentication_and_validate_bounds() {
        let (_temp, app) = application();
        for uri in [
            "/admin/api/session",
            "/admin/api/overview?shard=0",
            "/admin/api/rows?shard=0&table=widgets",
            "/admin/api/overview?shard=not-a-number",
            "/admin/api/rows?shard=not-a-number",
        ] {
            let response = request(&app, Method::GET, uri, None, None).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(
                response.headers()[SET_COOKIE]
                    .to_str()
                    .unwrap()
                    .contains("Max-Age=0")
            );
        }

        let set_cookie = login_cookie(&app).await;
        let cookie = set_cookie.to_str().unwrap().split(';').next().unwrap();
        for uri in [
            "/admin/api/rows?shard=0&table=widgets&limit=0",
            "/admin/api/rows?shard=0&table=widgets&limit=201",
            "/admin/api/rows?shard=0&table=widgets&offset=1000001",
            "/admin/api/rows?shard=2&table=widgets",
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
    async fn authenticated_browser_discovers_and_pages_only_the_selected_shard() {
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
                 CREATE TABLE \"odd\"\"table\" (value TEXT); \
                 CREATE VIEW widget_view AS SELECT id FROM widgets;",
            )
            .unwrap();
        for shard in 0..2 {
            let routing_key = routing_key_for_shard(&database, shard);
            for id in 1..=3_i64 {
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
                    "shard_count": 2,
                    "selected_shard": 1,
                    "tables": ["", " ", "odd\"table", "widgets"]
                })
            )
        );

        let (status, first_page) = response_json(
            request(
                &app,
                Method::GET,
                "/admin/api/rows?shard=1&table=widgets&limit=2&offset=0",
                Some(cookie),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first_page["shard"], 1);
        assert_eq!(first_page["table"], "widgets");
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
                [1, "shard-1-row-1", [1, 1], null],
                [2, "shard-1-row-2", [1, 2], null]
            ])
        );

        assert_eq!(
            response_json(
                request(
                    &app,
                    Method::GET,
                    "/admin/api/rows?shard=1&table=widgets&limit=2&offset=2",
                    Some(cookie),
                    None,
                )
                .await
            )
            .await,
            (
                StatusCode::OK,
                json!({
                    "shard": 1,
                    "table": "widgets",
                    "limit": 2,
                    "offset": 2,
                    "has_more": false,
                    "columns": [
                        {"name":"id","data_type":"unknown"},
                        {"name":"label","data_type":"unknown"},
                        {"name":"payload","data_type":"unknown"},
                        {"name":"note","data_type":"unknown"}
                    ],
                    "rows": [[3, "shard-1-row-3", [1, 3], null]]
                })
            )
        );

        let odd_table = "/admin/api/rows?shard=0&table=odd%22table&limit=50&offset=0";
        let (status, empty) =
            response_json(request(&app, Method::GET, odd_table, Some(cookie), None).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["rows"], json!([]));
        assert_eq!(empty["has_more"], false);

        let empty_name = "/admin/api/rows?shard=0&table=&limit=50&offset=0";
        let (status, empty) =
            response_json(request(&app, Method::GET, empty_name, Some(cookie), None).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["table"], "");
        assert_eq!(empty["rows"], json!([]));

        let whitespace_name = "/admin/api/rows?shard=0&table=%20&limit=50&offset=0";
        let (status, whitespace) =
            response_json(request(&app, Method::GET, whitespace_name, Some(cookie), None).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(whitespace["table"], " ");

        let shard_zero = request(
            &app,
            Method::GET,
            "/admin/api/rows?shard=0&table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        );
        let shard_one = request(
            &app,
            Method::GET,
            "/admin/api/rows?shard=1&table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        );
        let (shard_zero, shard_one) = tokio::join!(shard_zero, shard_one);
        let (_, shard_zero) = response_json(shard_zero).await;
        let (_, shard_one) = response_json(shard_one).await;
        assert_eq!(shard_zero["rows"][0][1], "shard-0-row-1");
        assert_eq!(shard_one["rows"][0][1], "shard-1-row-1");

        for table in ["briskdb_shard_metadata", "widget_view", "guessed_table"] {
            let uri = format!("/admin/api/rows?shard=0&table={table}");
            let response = request(&app, Method::GET, &uri, Some(cookie), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{table}");
        }
        let recovered = request(
            &app,
            Method::GET,
            "/admin/api/rows?shard=0&table=widgets&limit=1&offset=0",
            Some(cookie),
            None,
        )
        .await;
        assert_eq!(recovered.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn browser_sessions_are_independent_and_unknown_tokens_are_cleared() {
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
            assert!(
                response.headers()[SET_COOKIE]
                    .to_str()
                    .unwrap()
                    .contains("Max-Age=0")
            );
        }
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
        let body = body_json(page_response(
            0,
            "widgets".to_owned(),
            1,
            MAX_PAGE_OFFSET,
            result,
        ))
        .await;
        assert_eq!(body["rows"], json!([[1]]));
        assert_eq!(body["has_more"], false);
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
