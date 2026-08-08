use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::Router;
use briskdb::{
    api, core,
    protocol::{error, http},
    server, storage,
};

#[test]
fn legacy_and_explicit_module_paths_are_both_available() {
    let _legacy_database: Option<storage::Database> = None;
    let _core_database: Option<core::Database> = None;
    let _engine: Option<core::Engine> = None;
    let _engine_status: Option<core::EngineStatus> = None;
    let _engine_options: core::EngineOptions = core::EngineOptions::default();
    let _result_limits: core::ResultLimits = core::ResultLimits::default();
    let _request_context: core::RequestContext = core::RequestContext::new();
    let _cancellation_token: core::CancellationToken = core::CancellationToken::new();
    let _running = core::EngineState::Running;
    let _shutdown_report: Option<core::ShutdownReport> = None;
    let _session: Option<core::Session> = None;
    let _ready = core::SessionState::Ready;
    let _closed = core::SessionState::Closed;
    let _statement = core::Statement::new("SELECT ?1", vec![core::Value::from(42_i64)]);
    let _legacy_router: fn(Arc<storage::Database>) -> Router = api::router;
    let _http_router: fn(Arc<core::Database>) -> Router = http::router;
    let _engine_router: fn(core::Engine) -> Router = http::router_with_engine;
    let _default_server_entry_point = server::run;
    let _configured_server_entry_point = server::run_with_engine_options;

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

#[test]
fn engine_options_are_public_validated_and_have_stable_defaults() {
    let defaults = core::EngineOptions::default();
    assert_eq!(
        defaults.connections_per_shard(),
        core::DEFAULT_CONNECTIONS_PER_SHARD
    );
    assert_eq!(
        defaults.queue_capacity_per_shard(),
        core::DEFAULT_QUEUE_CAPACITY_PER_SHARD
    );
    assert_eq!(
        defaults.result_limits(),
        core::ResultLimits::new(
            core::DEFAULT_MAX_RESULT_ROWS,
            core::DEFAULT_MAX_RESULT_BYTES,
        )
        .unwrap()
    );
    assert_eq!(
        defaults.request_timeout(),
        Some(Duration::from_millis(core::DEFAULT_REQUEST_TIMEOUT_MS))
    );
    assert_eq!(
        defaults.shutdown_grace(),
        Duration::from_millis(core::DEFAULT_SHUTDOWN_GRACE_MS)
    );

    let minimum = core::EngineOptions::new(1, 1).unwrap();
    assert_eq!(minimum.connections_per_shard(), 1);
    assert_eq!(minimum.queue_capacity_per_shard(), 1);

    let maximum = core::EngineOptions::new(
        core::MAX_CONNECTIONS_PER_SHARD,
        core::MAX_QUEUE_CAPACITY_PER_SHARD,
    )
    .unwrap();
    assert_eq!(
        maximum.connections_per_shard(),
        core::MAX_CONNECTIONS_PER_SHARD
    );
    assert_eq!(
        maximum.queue_capacity_per_shard(),
        core::MAX_QUEUE_CAPACITY_PER_SHARD
    );

    for (connections, queue_capacity) in [
        (0, 1),
        (core::MAX_CONNECTIONS_PER_SHARD + 1, 1),
        (1, 0),
        (1, core::MAX_QUEUE_CAPACITY_PER_SHARD + 1),
    ] {
        assert_eq!(
            core::EngineOptions::new(connections, queue_capacity)
                .unwrap_err()
                .kind(),
            core::EngineErrorKind::InvalidArgument
        );
    }

    let limits = core::ResultLimits::new(37, 4_096).unwrap();
    let configured = minimum
        .with_result_limits(limits)
        .with_request_timeout(None)
        .unwrap()
        .with_shutdown_grace(Duration::from_millis(250))
        .unwrap();
    assert_eq!(configured.result_limits(), limits);
    assert_eq!(configured.request_timeout(), None);
    assert_eq!(configured.shutdown_grace(), Duration::from_millis(250));
}

#[tokio::test]
async fn protocol_neutral_async_engine_surface_is_available() {
    let temp = tempfile::tempdir().unwrap();
    let limits = core::ResultLimits::new(50, 8_192).unwrap();
    let options = core::EngineOptions::new(2, 7)
        .unwrap()
        .with_result_limits(limits)
        .with_request_timeout(Some(Duration::from_secs(5)))
        .unwrap()
        .with_shutdown_grace(Duration::from_millis(100))
        .unwrap();
    let engine = core::Engine::open_with_options(temp.path(), 4, options)
        .await
        .unwrap();
    let session: core::Session = engine.session();

    assert_eq!(session.state().await, core::SessionState::Ready);
    assert_eq!(engine.options(), options);
    let status: core::EngineStatus = engine.status(&session).await.unwrap();
    assert_eq!(status.shard_count(), 4);
    assert_eq!(status.max_blocking_workers(), 8);
    assert_eq!(status.connections_per_shard(), 2);
    assert_eq!(status.queue_capacity_per_shard(), 7);
    assert_eq!(status.max_result_rows(), 50);
    assert_eq!(status.max_result_bytes(), 8_192);
    assert_eq!(status.request_timeout(), Some(Duration::from_secs(5)));
    assert_eq!(status.shutdown_grace(), Duration::from_millis(100));

    session.set_routing_key("public-controls").await.unwrap();
    let token = core::CancellationToken::new();
    let context = core::RequestContext::new()
        .with_cancellation_token(token.clone())
        .with_deadline(Instant::now() + Duration::from_secs(1))
        .with_result_limits(core::ResultLimits::new(1, 512).unwrap());
    let result = engine
        .query_with_context(&session, core::Statement::new("SELECT 1", vec![]), context)
        .await
        .unwrap();
    assert_eq!(result.value.rows().len(), 1);
    token.cancel();
    assert!(token.is_cancelled());

    assert_eq!(engine.state(), core::EngineState::Running);
    assert_eq!(engine.begin_shutdown(), core::EngineState::Draining);
    let report = engine.shutdown().await.unwrap();
    assert!(!report.forced());
    assert_eq!(engine.state(), core::EngineState::Stopped);
}

#[tokio::test]
async fn default_and_wrapped_database_engine_constructors_remain_available() {
    let default_temp = tempfile::tempdir().unwrap();
    let default_engine = core::Engine::open(default_temp.path(), 2).await.unwrap();
    assert_eq!(default_engine.options(), core::EngineOptions::default());

    let wrapped_temp = tempfile::tempdir().unwrap();
    let database = Arc::new(core::Database::open(wrapped_temp.path(), 4).unwrap());
    let options = core::EngineOptions::new(3, 11).unwrap();
    let wrapped = core::Engine::from_database_with_options(database, options).unwrap();
    let session = wrapped.session();
    let status = wrapped.status(&session).await.unwrap();

    assert_eq!(wrapped.options(), options);
    assert_eq!(status.shard_count(), 4);
    assert_eq!(status.max_blocking_workers(), 12);
    assert_eq!(status.connections_per_shard(), 3);
    assert_eq!(status.queue_capacity_per_shard(), 11);
}
