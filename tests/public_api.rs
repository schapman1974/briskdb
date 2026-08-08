use std::sync::Arc;

use axum::Router;
use briskdb::{api, core, protocol::http, storage};

#[test]
fn legacy_and_explicit_module_paths_are_both_available() {
    let _legacy_database: Option<storage::Database> = None;
    let _core_database: Option<core::Database> = None;
    let _legacy_router: fn(Arc<storage::Database>) -> Router = api::router;
    let _http_router: fn(Arc<core::Database>) -> Router = http::router;
}
