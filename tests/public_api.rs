use std::{
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use axum::Router;
use briskdb::{
    api, core,
    protocol::{error, http},
    server, storage,
};

fn insert_catalog_fixture(root: &Path) {
    let manifest = rusqlite::Connection::open(root.join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             BEGIN IMMEDIATE;
             INSERT INTO briskdb_logical_databases (database_id, database_name)
             VALUES (9, 'tenant');
             INSERT INTO briskdb_tables (
                table_id,
                database_id,
                table_name,
                placement,
                shard_key_column,
                shard_key_type
             ) VALUES
                (20, 1, 'countries', 2, NULL, NULL),
                (30, 9, 'accounts', 1, 'tenant_id', 2),
                (40, 9, 'internal_catalog', 3, NULL, NULL);
             COMMIT;",
        )
        .unwrap();
}

fn assert_catalog_fixture(catalog: &core::Catalog) {
    assert_eq!(catalog.identifier_encoding_version(), 1);
    assert_eq!(catalog.schema_generation(), 0);
    assert_eq!(catalog.default_database().id().get(), 1);
    assert_eq!(catalog.default_database().name(), "default");
    assert_eq!(catalog.logical_databases().len(), 2);
    assert_eq!(catalog.tables().len(), 3);

    let tenant = catalog.database("tenant").unwrap().unwrap();
    assert_eq!(tenant.id().get(), 9);
    assert_eq!(tenant.id().to_string(), "9");
    assert_eq!(catalog.database_by_id(tenant.id()), Some(tenant));

    let countries = catalog.table("default", "countries").unwrap().unwrap();
    assert_eq!(countries.id().get(), 20);
    assert_eq!(countries.id().to_string(), "20");
    assert_eq!(countries.database_id().get(), 1);
    assert!(matches!(
        countries.placement(),
        core::TablePlacement::Global
    ));
    assert_eq!(
        catalog.table_by_id(core::TableId::new(countries.id().get()).unwrap()),
        Some(countries)
    );

    let accounts = catalog.table("tenant", "accounts").unwrap().unwrap();
    assert_eq!(accounts.id().get(), 30);
    assert_eq!(accounts.database_id(), tenant.id());
    match accounts.placement() {
        core::TablePlacement::Sharded(shard_key) => {
            assert_eq!(shard_key.column(), "tenant_id");
            assert_eq!(shard_key.key_type(), core::ShardKeyType::Text);
        }
        placement => panic!("unexpected accounts placement: {placement:?}"),
    }

    assert!(matches!(
        catalog
            .table("tenant", "internal_catalog")
            .unwrap()
            .unwrap()
            .placement(),
        core::TablePlacement::Catalog
    ));
    assert_eq!(
        catalog
            .tables()
            .iter()
            .map(|table| (table.database_id().get(), table.name()))
            .collect::<Vec<_>>(),
        [(1, "countries"), (9, "accounts"), (9, "internal_catalog")]
    );
}

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
fn logical_catalog_types_and_access_are_public_and_protocol_neutral() {
    fn assert_public_metadata<T: Clone + Send + Sync + 'static>() {}

    assert_public_metadata::<core::Catalog>();
    assert_public_metadata::<core::LogicalDatabaseId>();
    assert_public_metadata::<core::LogicalDatabaseMetadata>();
    assert_public_metadata::<core::TableId>();
    assert_public_metadata::<core::TableMetadata>();
    assert_public_metadata::<core::TablePlacement>();
    assert_public_metadata::<core::ShardKeyMetadata>();
    assert_public_metadata::<core::ShardKeyType>();

    let _signed = core::ShardKeyType::Int64;
    let _text = core::ShardKeyType::Text;
    let _binary = core::ShardKeyType::Binary;
    let _global = core::TablePlacement::Global;
    let _catalog = core::TablePlacement::Catalog;

    let temp = tempfile::tempdir().unwrap();
    let database = Arc::new(core::Database::open(temp.path(), 4).unwrap());
    let catalog: &core::Catalog = database.catalog();
    let default: &core::LogicalDatabaseMetadata = catalog.default_database();
    let default_id: core::LogicalDatabaseId = default.id();
    let reconstructed_default = core::LogicalDatabaseId::new(default_id.get()).unwrap();

    assert_eq!(catalog.identifier_encoding_version(), 1);
    assert_eq!(catalog.schema_generation(), 0);
    assert_eq!(default_id.get(), 1);
    assert_eq!(default_id.to_string(), "1");
    assert_eq!(catalog.database_by_id(reconstructed_default), Some(default));
    assert_eq!(default.name(), "default");
    assert_eq!(catalog.logical_databases(), std::slice::from_ref(default));
    assert!(catalog.tables().is_empty());
    assert_eq!(catalog.database_by_id(default_id), Some(default));
    assert_eq!(catalog.database("missing").unwrap(), None);
    assert_eq!(catalog.table("default", "missing").unwrap(), None);
    assert_eq!(
        core::LogicalDatabaseId::new(0).unwrap_err().kind(),
        core::EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        core::TableId::new(0).unwrap_err().kind(),
        core::EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        catalog.database("NotCanonical").unwrap_err().kind(),
        core::EngineErrorKind::InvalidArgument
    );

    let engine = core::Engine::from_database(Arc::clone(&database));
    assert!(std::ptr::eq(engine.catalog(), database.catalog()));
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

#[test]
fn catalog_snapshot_is_immutable_and_reopen_observes_only_valid_commits() {
    let temp = tempfile::tempdir().unwrap();
    let keys: [&[u8]; 5] = [
        b"",
        b"customer-42",
        b"a\0b",
        &[0, 1, 2, 0xff],
        "snowman-☃".as_bytes(),
    ];
    let database = core::Database::open(temp.path(), 10).unwrap();
    let expected_routes = keys.map(|key| database.shard_for_key(key));
    assert_eq!(database.catalog().logical_databases().len(), 1);
    assert!(database.catalog().tables().is_empty());

    insert_catalog_fixture(temp.path());

    assert_eq!(database.catalog().logical_databases().len(), 1);
    assert!(database.catalog().tables().is_empty());
    assert_eq!(database.catalog().database("tenant").unwrap(), None);
    assert_eq!(keys.map(|key| database.shard_for_key(key)), expected_routes);

    let reopened = core::Database::open(temp.path(), 10).unwrap();
    assert_catalog_fixture(reopened.catalog());
    assert_eq!(keys.map(|key| reopened.shard_for_key(key)), expected_routes);

    let manifest = rusqlite::Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch(
            "PRAGMA ignore_check_constraints = ON;
             UPDATE briskdb_tables SET placement = 99 WHERE table_id = 30;
             PRAGMA ignore_check_constraints = OFF;",
        )
        .unwrap();
    drop(manifest);

    assert_catalog_fixture(reopened.catalog());
    assert_eq!(keys.map(|key| reopened.shard_for_key(key)), expected_routes);
    let error = core::Database::open(temp.path(), 10).unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::DataCorruption);

    assert_eq!(database.catalog().logical_databases().len(), 1);
    assert!(database.catalog().tables().is_empty());
    assert_eq!(keys.map(|key| database.shard_for_key(key)), expected_routes);
}

#[test]
fn database_and_engine_catalog_reads_are_deterministic_in_parallel() {
    let temp = tempfile::tempdir().unwrap();
    drop(core::Database::open(temp.path(), 6).unwrap());
    insert_catalog_fixture(temp.path());

    let database = Arc::new(core::Database::open(temp.path(), 6).unwrap());
    let engine = core::Engine::from_database(Arc::clone(&database));
    assert!(std::ptr::eq(database.catalog(), engine.catalog()));
    assert_catalog_fixture(database.catalog());

    let expected_catalog = Arc::new(database.catalog().clone());
    let expected_shard = database.shard_for_key(b"parallel-catalog");
    let workers = (0..8)
        .map(|_| {
            let database = Arc::clone(&database);
            let engine = engine.clone();
            let expected_catalog = Arc::clone(&expected_catalog);
            thread::spawn(move || {
                for _ in 0..2_000 {
                    assert_eq!(database.catalog(), expected_catalog.as_ref());
                    assert_eq!(engine.catalog(), expected_catalog.as_ref());
                    assert_eq!(
                        database
                            .catalog()
                            .table("tenant", "accounts")
                            .unwrap()
                            .unwrap()
                            .id()
                            .get(),
                        30
                    );
                    assert_eq!(database.shard_for_key(b"parallel-catalog"), expected_shard);
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn broadcast_created_shard_tables_are_not_inferred_into_the_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let database = core::Database::open(temp.path(), 4).unwrap();
    database
        .broadcast(
            "CREATE TABLE physical_widgets (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL
             );",
        )
        .unwrap();
    database
        .execute(
            "tenant-1",
            "INSERT INTO physical_widgets (id, tenant_id) VALUES (?1, ?2)",
            &[core::Value::from("widget-1"), core::Value::from("tenant-1")],
        )
        .unwrap();

    assert!(database.catalog().tables().is_empty());
    assert_eq!(
        database
            .catalog()
            .table("default", "physical_widgets")
            .unwrap(),
        None
    );
    drop(database);

    let reopened = core::Database::open(temp.path(), 4).unwrap();
    assert!(reopened.catalog().tables().is_empty());
    assert_eq!(
        reopened
            .catalog()
            .table("default", "physical_widgets")
            .unwrap(),
        None
    );
    let rows = reopened
        .query(
            "tenant-1",
            "SELECT id, tenant_id FROM physical_widgets WHERE id = ?1",
            &[core::Value::from("widget-1")],
        )
        .unwrap();
    assert_eq!(rows.rows().len(), 1);
}
