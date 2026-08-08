use std::sync::Arc;

use axum::Router;
use briskdb::{
    api, core,
    protocol::{error, http},
    storage,
};

#[test]
fn legacy_and_explicit_module_paths_are_both_available() {
    let _legacy_database: Option<storage::Database> = None;
    let _core_database: Option<core::Database> = None;
    let _engine: Option<core::Engine> = None;
    let _engine_status: Option<core::EngineStatus> = None;
    let _session: Option<core::Session> = None;
    let _ready = core::SessionState::Ready;
    let _closed = core::SessionState::Closed;
    let _statement = core::Statement::new("SELECT ?1", vec![core::Value::from(42_i64)]);
    let _legacy_router: fn(Arc<storage::Database>) -> Router = api::router;
    let _http_router: fn(Arc<core::Database>) -> Router = http::router;
    let _engine_router: fn(core::Engine) -> Router = http::router_with_engine;

    let result = core::ResultSet::new(
        vec![core::Column::new("value", core::DataType::Int64)],
        vec![core::Row::new(vec![core::Value::from(42_i64)])],
    )
    .unwrap();
    assert_eq!(result.rows()[0].get(0), Some(&core::Value::from(42_i64)));

    let decimal = "12.3400".parse::<core::Decimal>().unwrap();
    assert_eq!(core::Value::from(decimal).as_decimal(), Some("12.3400"));
    let _invalid_decimal: core::ParseDecimalError =
        "not-a-number".parse::<core::Decimal>().unwrap_err();

    let engine_error = core::EngineError::new(core::EngineErrorKind::InvalidArgument, "diagnostic");
    let _engine_result: core::EngineResult<()> = Err(engine_error);
    assert_eq!(
        error::http_error(core::EngineErrorKind::InvalidArgument).status,
        400
    );
    assert_eq!(
        error::postgres_error(core::EngineErrorKind::UniqueViolation).sqlstate,
        "23505"
    );
    assert_eq!(
        error::mysql_error(core::EngineErrorKind::UniqueViolation).error_number,
        1062
    );
}

#[tokio::test]
async fn protocol_neutral_async_engine_surface_is_available() {
    let temp = tempfile::tempdir().unwrap();
    let engine = core::Engine::open(temp.path(), 4).await.unwrap();
    let session: core::Session = engine.session();

    assert_eq!(session.state().await, core::SessionState::Ready);
    let status: core::EngineStatus = engine.status(&session).await.unwrap();
    assert_eq!(status.shard_count(), 4);
}
