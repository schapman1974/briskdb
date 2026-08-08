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
    server, sql, storage,
};

fn insert_catalog_fixture(root: &Path) {
    let manifest = rusqlite::Connection::open(root.join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             BEGIN IMMEDIATE;
             DROP TABLE briskdb_integrity;
             DROP TABLE briskdb_metadata;
             CREATE TABLE briskdb_metadata (
                 requires_manifest_version INTEGER NOT NULL
                     CHECK (requires_manifest_version >= 6)
             ) STRICT;
             INSERT INTO briskdb_metadata VALUES (6);
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
             PRAGMA user_version = 6;
             COMMIT;",
        )
        .unwrap();
}

fn insert_prepared_catalog_fixture(root: &Path) {
    let manifest = rusqlite::Connection::open(root.join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             BEGIN IMMEDIATE;
             DROP TABLE briskdb_integrity;
             DROP TABLE briskdb_metadata;
             CREATE TABLE briskdb_metadata (
                 requires_manifest_version INTEGER NOT NULL
                     CHECK (requires_manifest_version >= 6)
             ) STRICT;
             INSERT INTO briskdb_metadata VALUES (6);
             INSERT INTO briskdb_tables (
                table_id,
                database_id,
                table_name,
                placement,
                shard_key_column,
                shard_key_type
             ) VALUES (50, 1, 'prepared_events', 1, 'tenant_id', 1);
             PRAGMA user_version = 6;
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
    let _prepared_limits: core::PreparedStatementLimits = core::PreparedStatementLimits::default();
    let _request_context: core::RequestContext = core::RequestContext::new();
    let _cancellation_token: core::CancellationToken = core::CancellationToken::new();
    let _running = core::EngineState::Running;
    let _shutdown_report: Option<core::ShutdownReport> = None;
    let _session: Option<core::Session> = None;
    let _ready = core::SessionState::Ready;
    let _closed = core::SessionState::Closed;
    let _statement = core::Statement::new("SELECT ?1", vec![core::Value::from(42_i64)]);
    let _prepared_statement_id: Option<core::PreparedStatementId> = None;
    let _portal_id: Option<core::PortalId> = None;
    let _describe_target: Option<core::DescribeTarget> = None;
    let _description: Option<core::PreparedStatementDescription> = None;
    let _prepared_execution: Option<core::PreparedExecution> = None;
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
fn protocol_neutral_sql_parser_facade_is_public_bounded_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<sql::ParsedSql>();
    assert_owned_public::<sql::SqlDialect>();

    let cases = [
        (sql::SqlDialect::Sqlite, "SELECT ?1"),
        (sql::SqlDialect::PostgreSql, "SELECT $1"),
        (sql::SqlDialect::MySql, "SELECT ?"),
    ];
    for (dialect, source) in cases {
        let parsed = sql::parse(dialect, source.to_owned()).unwrap();
        assert_eq!(parsed.dialect(), dialect);
        assert_eq!(parsed.source(), source);
        assert_eq!(parsed.statement_count(), 1);
        assert!(!parsed.is_empty());
    }

    let empty = sql::parse(sql::SqlDialect::Sqlite, "-- comment only").unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.statement_count(), 0);

    let invalid = sql::parse(sql::SqlDialect::PostgreSql, "SELECT ?").unwrap_err();
    assert_eq!(invalid.kind(), core::EngineErrorKind::InvalidQuery);

    let too_long = " ".repeat(sql::MAX_PARSED_SQL_BYTES + 1);
    let limited = sql::parse(sql::SqlDialect::Sqlite, too_long).unwrap_err();
    assert_eq!(limited.kind(), core::EngineErrorKind::LimitExceeded);
    assert_eq!(sql::MAX_PARSED_SQL_STATEMENTS, 256);
    assert_eq!(sql::SQL_PARSE_RECURSION_LIMIT, 32);

    // Parsing and subset validation are opt-in infrastructure. The existing
    // raw SQLite path deliberately does not invoke either layer yet.
    let temp = tempfile::tempdir().unwrap();
    let database = core::Database::open(temp.path(), 2).unwrap();
    let mut raw_sql = "SELECT 1".to_owned();
    raw_sql.push_str(&" ".repeat(sql::MAX_PARSED_SQL_BYTES));
    let result = database.query("parser-opt-in", &raw_sql, &[]).unwrap();
    assert_eq!(result.rows()[0].get(0), Some(&core::Value::Int64(1)));
}

#[test]
fn common_sql_subset_validation_is_public_owned_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<sql::CommonSql>();
    assert_eq!(sql::MAX_COMMON_SQL_EXPRESSION_DEPTH, 128);

    for (dialect, source) in [
        (sql::SqlDialect::Sqlite, "SELECT ?1 AS value"),
        (sql::SqlDialect::PostgreSql, "SELECT $1 AS value"),
        (sql::SqlDialect::MySql, "SELECT ? AS value"),
    ] {
        let common = sql::validate_common_subset(sql::parse(dialect, source).unwrap()).unwrap();
        assert_eq!(common.dialect(), dialect);
        assert_eq!(common.source(), source);
        assert_eq!(common.statement_count(), 1);
        assert!(!common.is_empty());
    }

    let cte = "WITH answer(value) AS (VALUES (9)) SELECT value FROM answer";
    let unsupported =
        sql::validate_common_subset(sql::parse(sql::SqlDialect::Sqlite, cte.to_owned()).unwrap())
            .unwrap_err();
    assert_eq!(unsupported.kind(), core::EngineErrorKind::Unsupported);
    assert!(!unsupported.diagnostic().contains(cte));

    let malformed = sql::parse(sql::SqlDialect::Sqlite, "SELECT +").unwrap_err();
    assert_eq!(malformed.kind(), core::EngineErrorKind::InvalidQuery);

    // The structural validator does not alter the current execution path.
    let temp = tempfile::tempdir().unwrap();
    let database = core::Database::open(temp.path(), 2).unwrap();
    let result = database.query("subset-opt-in", cte, &[]).unwrap();
    assert_eq!(result.rows()[0].get(0), Some(&core::Value::Int64(9)));
}

#[test]
fn placeholder_normalization_is_public_owned_bounded_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<sql::NormalizedSql>();
    assert_owned_public::<sql::StatementParameters>();
    assert_eq!(sql::MAX_SQL_PARAMETERS, 32_766);

    for (dialect, source, expected, count, indices) in [
        (
            sql::SqlDialect::Sqlite,
            "SELECT ?2, ?, ?1",
            "SELECT ?2, ?3, ?1",
            3,
            vec![2, 3, 1],
        ),
        (
            sql::SqlDialect::PostgreSql,
            "SELECT $2, $1, $2",
            "SELECT ?2, ?1, ?2",
            2,
            vec![2, 1, 2],
        ),
        (
            sql::SqlDialect::MySql,
            "SELECT ?, ?",
            "SELECT ?1, ?2",
            2,
            vec![1, 2],
        ),
    ] {
        let common = sql::validate_common_subset(sql::parse(dialect, source).unwrap()).unwrap();
        let normalized = sql::normalize_placeholders(common).unwrap();
        let parameters = &normalized.statement_parameters()[0];

        assert_eq!(normalized.dialect(), dialect);
        assert_eq!(normalized.source(), source);
        assert_eq!(normalized.sqlite_parameter_sql(), expected);
        assert_eq!(normalized.statement_count(), 1);
        assert!(!normalized.is_empty());
        assert_eq!(parameters.parameter_count(), count);
        assert_eq!(parameters.occurrence_count(), indices.len());
        assert_eq!(parameters.parameter_indices(), indices);
    }

    let named =
        sql::validate_common_subset(sql::parse(sql::SqlDialect::Sqlite, "SELECT :value").unwrap())
            .unwrap();
    let unsupported = sql::normalize_placeholders(named).unwrap_err();
    assert_eq!(unsupported.kind(), core::EngineErrorKind::Unsupported);

    // Normalization is opt-in infrastructure. Existing raw SQLite callers keep
    // their current marker behavior until the planned prepare/bind path adopts
    // the normalized representation.
    let temp = tempfile::tempdir().unwrap();
    let database = core::Database::open(temp.path(), 2).unwrap();
    let result = database
        .query(
            "normalizer-opt-in",
            "SELECT :value",
            &[core::Value::Int64(9)],
        )
        .unwrap();
    assert_eq!(result.rows()[0].get(0), Some(&core::Value::Int64(9)));
}

#[test]
fn sql_translation_is_public_owned_dialect_equivalent_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<sql::SqlTranslationMode>();
    assert_owned_public::<sql::TranslatedSql>();

    let ddl = [
        (
            sql::SqlDialect::Sqlite,
            "CREATE TABLE \"typed\" (\"id\" INTEGER PRIMARY KEY, \"enabled\" BOOLEAN, \"payload\" BLOB)",
        ),
        (
            sql::SqlDialect::PostgreSql,
            "CREATE TABLE \"typed\" (\"id\" INT8 PRIMARY KEY, \"enabled\" BOOL, \"payload\" BYTEA)",
        ),
        (
            sql::SqlDialect::MySql,
            "CREATE TABLE `typed` (`id` BIGINT PRIMARY KEY, `enabled` TINYINT(1), `payload` VARBINARY(64))",
        ),
    ];
    for (dialect, source) in ddl {
        let normalized = sql::normalize_placeholders(
            sql::validate_common_subset(sql::parse(dialect, source).unwrap()).unwrap(),
        )
        .unwrap();
        let translated =
            sql::translate_sql(normalized, sql::SqlTranslationMode::Compatibility).unwrap();
        assert_eq!(translated.dialect(), dialect);
        assert_eq!(translated.mode(), sql::SqlTranslationMode::Compatibility);
        assert_eq!(translated.source(), source);
        assert_eq!(
            translated.sqlite_sql(),
            "CREATE TABLE \"typed\" (\"id\" BIGINT PRIMARY KEY, \"enabled\" BOOLEAN, \"payload\" BLOB)"
        );
    }

    let strict_source = "-- exact SQLite\r\nSELECT ?2, ?, 'private ?';\r\n";
    let strict = sql::translate_sql(
        sql::normalize_placeholders(
            sql::validate_common_subset(
                sql::parse(sql::SqlDialect::Sqlite, strict_source).unwrap(),
            )
            .unwrap(),
        )
        .unwrap(),
        sql::SqlTranslationMode::StrictSqlite,
    )
    .unwrap();
    assert_eq!(
        strict.sqlite_sql(),
        "-- exact SQLite\r\nSELECT ?2, ?3, 'private ?';\r\n"
    );
    assert!(!format!("{strict:?}").contains("private"));

    let temp = tempfile::tempdir().unwrap();
    drop(core::Database::open(temp.path(), 4).unwrap());
    insert_catalog_fixture(temp.path());
    let database = Arc::new(core::Database::open(temp.path(), 4).unwrap());
    let engine = core::Engine::from_database(Arc::clone(&database));
    let tenant = database.catalog().database("tenant").unwrap().unwrap();
    let parameter = [core::Value::Text("tenant-public-translation".to_owned())];
    let requests = [
        (
            sql::SqlDialect::Sqlite,
            "SELECT tenant_id FROM accounts WHERE tenant_id = ?1",
        ),
        (
            sql::SqlDialect::PostgreSql,
            "SELECT tenant_id FROM accounts WHERE tenant_id = $1",
        ),
        (
            sql::SqlDialect::MySql,
            "SELECT tenant_id FROM accounts WHERE tenant_id = ?",
        ),
    ];
    let plans = requests.map(|(dialect, source)| {
        let translated = sql::translate_sql(
            sql::normalize_placeholders(
                sql::validate_common_subset(sql::parse(dialect, source).unwrap()).unwrap(),
            )
            .unwrap(),
            sql::SqlTranslationMode::Compatibility,
        )
        .unwrap();
        assert_eq!(
            translated.sqlite_sql(),
            "SELECT tenant_id FROM accounts WHERE tenant_id = ?1"
        );
        engine
            .plan_bound_statement(
                tenant.id(),
                translated.normalized_sql(),
                0,
                &parameter,
                None,
            )
            .unwrap()
    });
    assert_eq!(plans[0], plans[1]);
    assert_eq!(plans[1], plans[2]);

    // Translation remains opt-in. Existing raw SQLite execution continues to
    // accept its current named-parameter behavior directly.
    let raw = database
        .query(
            "translation-opt-in",
            "SELECT :value",
            &[core::Value::Int64(29)],
        )
        .unwrap();
    assert_eq!(raw.rows()[0].get(0), Some(&core::Value::Int64(29)));
}

#[test]
fn shard_key_inference_is_public_typed_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<sql::ShardKeyInference>();
    assert_owned_public::<sql::ShardKeyInferenceKind>();
    assert_owned_public::<sql::ShardKeyValue>();

    let temp = tempfile::tempdir().unwrap();
    drop(core::Database::open(temp.path(), 4).unwrap());
    insert_catalog_fixture(temp.path());
    let database = core::Database::open(temp.path(), 4).unwrap();
    let tenant = database.catalog().database("tenant").unwrap().unwrap();

    let source = "INSERT INTO accounts (payload, tenant_id) VALUES ($1, $2), ($3, 'tenant-b')";
    let parsed = sql::parse(sql::SqlDialect::PostgreSql, source).unwrap();
    let common = sql::validate_common_subset(parsed).unwrap();
    let normalized = sql::normalize_placeholders(common).unwrap();
    let inference = sql::infer_shard_keys(
        database.catalog(),
        tenant.id(),
        &normalized,
        0,
        &[
            core::Value::Text("payload-a".to_owned()),
            core::Value::Text("tenant-a".to_owned()),
            core::Value::Text("payload-b".to_owned()),
        ],
    )
    .unwrap();

    assert_eq!(inference.table_id().unwrap().get(), 30);
    assert_eq!(inference.key_type(), Some(core::ShardKeyType::Text));
    assert_eq!(inference.kind(), sql::ShardKeyInferenceKind::Multiple);
    assert_eq!(inference.values().len(), 2);
    assert_eq!(inference.values()[0].key_type(), core::ShardKeyType::Text);
    assert_eq!(inference.values()[0].as_str(), Some("tenant-a"));
    assert_eq!(inference.values()[0].as_i64(), None);
    assert_eq!(inference.values()[0].as_bytes(), None);
    assert_eq!(inference.values()[1].as_str(), Some("tenant-b"));

    let debug = format!("{inference:?}");
    assert!(!debug.contains("tenant-a"));
    assert!(!debug.contains("tenant-b"));

    // Inference is read-only, protocol-neutral analysis. The existing raw
    // database interface still executes caller-provided SQLite directly.
    let raw = database
        .query("inference-opt-in", "SELECT 23", &[])
        .unwrap();
    assert_eq!(raw.rows()[0].get(0), Some(&core::Value::Int64(23)));
}

#[tokio::test]
async fn bound_statement_planning_is_public_owned_value_aware_and_opt_in() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    fn same_shard_text_pair(database: &core::Database, prefix: &str) -> ([String; 2], u16) {
        let first = format!("{prefix}-0");
        let shard = database.shard_for_key(first.as_bytes());
        let second = (1_u64..)
            .map(|candidate| format!("{prefix}-{candidate}"))
            .find(|candidate| database.shard_for_key(candidate.as_bytes()) == shard)
            .unwrap();
        ([first, second], shard)
    }
    fn binary_key_for_shard(database: &core::Database, shard: u16, prefix: &str) -> Vec<u8> {
        (0_u64..)
            .map(|candidate| {
                let mut key = format!("\0{prefix}-{candidate}").into_bytes();
                key.push(0xff);
                key
            })
            .find(|candidate| database.shard_for_key(candidate) == shard)
            .unwrap()
    }
    fn binary_key_for_other_shard(database: &core::Database, shard: u16, prefix: &str) -> Vec<u8> {
        (0_u64..)
            .map(|candidate| {
                let mut key = format!("\0{prefix}-{candidate}").into_bytes();
                key.push(0xff);
                key
            })
            .find(|candidate| database.shard_for_key(candidate) != shard)
            .unwrap()
    }

    assert_owned_public::<core::BoundStatementPlan>();
    assert_owned_public::<core::PlannedRoute>();

    let temp = tempfile::tempdir().unwrap();
    drop(core::Database::open(temp.path(), 8).unwrap());
    insert_catalog_fixture(temp.path());
    let database = Arc::new(core::Database::open(temp.path(), 8).unwrap());
    let engine = core::Engine::from_database(Arc::clone(&database));
    let tenant = database.catalog().database("tenant").unwrap().unwrap();

    let source = "INSERT INTO accounts (tenant_id) VALUES ($1), ($2), ($3)";
    let parsed = sql::parse(sql::SqlDialect::PostgreSql, source).unwrap();
    let common = sql::validate_common_subset(parsed).unwrap();
    let normalized = sql::normalize_placeholders(common).unwrap();
    let (first_distinct_keys, first_shard) =
        same_shard_text_pair(&database, "tenant-alpha-sensitive");
    let first_keys = [
        first_distinct_keys[0].as_str(),
        first_distinct_keys[1].as_str(),
        first_distinct_keys[0].as_str(),
    ];
    let first_parameters = first_keys
        .iter()
        .map(|key| core::Value::Text((*key).to_owned()))
        .collect::<Vec<_>>();
    let explicit_key = binary_key_for_shard(&database, first_shard, "explicit-route-sensitive");

    let plan = engine
        .plan_bound_statement(
            tenant.id(),
            &normalized,
            0,
            &first_parameters,
            Some(explicit_key.as_slice()),
        )
        .unwrap();

    assert_eq!(plan.database(), tenant.id());
    assert_eq!(
        plan.schema_generation(),
        database.catalog().schema_generation()
    );
    assert_eq!(plan.hash_version(), 1);
    assert_eq!(plan.key_encoding_version(), 1);
    assert_eq!(plan.bucket_algorithm_version(), 1);
    assert_eq!(plan.map_generation(), 1);
    assert_eq!(plan.statement_index(), 0);
    assert_eq!(
        plan.inference().kind(),
        sql::ShardKeyInferenceKind::Multiple
    );
    assert_eq!(plan.inference().values().len(), first_keys.len());
    assert_eq!(plan.inferred_routes().len(), first_keys.len());
    assert_eq!(plan.assigned_shard(), Some(first_shard));

    for ((value, route), expected_key) in plan
        .inference()
        .values()
        .iter()
        .zip(plan.inferred_routes())
        .zip(first_keys)
    {
        assert_eq!(value.as_str(), Some(expected_key));
        assert_eq!(route.key_bytes(), expected_key.as_bytes());
        assert_eq!(
            route.shard(),
            database.shard_for_key(expected_key.as_bytes())
        );
        assert_eq!(route.shard(), first_shard);
    }
    assert_eq!(plan.inferred_routes()[0], plan.inferred_routes()[2]);

    let explicit_route = plan.explicit_route().unwrap();
    assert_eq!(explicit_route.key_bytes(), explicit_key);
    assert_eq!(explicit_route.shard(), first_shard);
    assert!(
        plan.inferred_routes()
            .iter()
            .all(|route| route.key_bytes() != explicit_route.key_bytes())
    );

    let plan_debug = format!("{plan:?}");
    let route_debug = format!("{explicit_route:?}");
    for sensitive in ["tenant-alpha-sensitive", "explicit-route-sensitive"] {
        assert!(!plan_debug.contains(sensitive));
        assert!(!route_debug.contains(sensitive));
    }

    let cloned = plan.clone();
    assert_eq!(cloned, plan);
    assert_eq!(thread::spawn(move || cloned).join().unwrap(), plan);

    let conflicting_key =
        binary_key_for_other_shard(&database, first_shard, "conflicting-explicit-sensitive");
    assert_ne!(database.shard_for_key(&conflicting_key), first_shard);
    let conflict = engine
        .plan_bound_statement(
            tenant.id(),
            &normalized,
            0,
            &first_parameters,
            Some(conflicting_key.as_slice()),
        )
        .unwrap_err();
    assert_eq!(conflict.kind(), core::EngineErrorKind::InvalidArgument);
    assert_eq!(conflict.code(), "invalid_argument");
    let conflict_debug = format!("{conflict:?}");
    for sensitive in [
        first_distinct_keys[0].as_str(),
        first_distinct_keys[1].as_str(),
        "conflicting-explicit-sensitive",
    ] {
        assert!(!conflict.diagnostic().contains(sensitive));
        assert!(!conflict_debug.contains(sensitive));
    }

    let recovered = engine
        .plan_bound_statement(
            tenant.id(),
            &normalized,
            0,
            &first_parameters,
            Some(explicit_key.as_slice()),
        )
        .unwrap();
    assert_eq!(recovered, plan);

    let (second_distinct_keys, second_shard) =
        same_shard_text_pair(&database, "tenant-bravo-sensitive");
    let second_keys = [
        second_distinct_keys[0].as_str(),
        second_distinct_keys[1].as_str(),
        second_distinct_keys[0].as_str(),
    ];
    let second_parameters = second_keys
        .iter()
        .map(|key| core::Value::Text((*key).to_owned()))
        .collect::<Vec<_>>();
    let replanned = engine
        .plan_bound_statement(tenant.id(), &normalized, 0, &second_parameters, None)
        .unwrap();
    assert_eq!(replanned.inferred_routes().len(), second_keys.len());
    assert!(replanned.explicit_route().is_none());
    assert_eq!(replanned.assigned_shard(), Some(second_shard));
    for (route, expected_key) in replanned.inferred_routes().iter().zip(second_keys) {
        assert_eq!(route.key_bytes(), expected_key.as_bytes());
        assert_eq!(
            route.shard(),
            database.shard_for_key(expected_key.as_bytes())
        );
        assert_eq!(route.shard(), second_shard);
    }
    assert_ne!(replanned.inferred_routes()[0], plan.inferred_routes()[0]);

    // Planning remains opt-in analysis: existing raw Database and Engine
    // execution paths continue to execute caller-provided SQLite directly.
    let raw = database
        .query(
            "planner-database-regression",
            "SELECT ?1",
            &[core::Value::Int64(37)],
        )
        .unwrap();
    assert_eq!(raw.rows()[0].get(0), Some(&core::Value::Int64(37)));

    let session = engine.session();
    session
        .set_routing_key("planner-engine-regression")
        .await
        .unwrap();
    let routed = engine
        .query(
            &session,
            core::Statement::new("SELECT ?1", vec![core::Value::Int64(41)]),
        )
        .await
        .unwrap();
    assert_eq!(routed.value.rows()[0].get(0), Some(&core::Value::Int64(41)));
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
        defaults.prepared_statement_limits(),
        core::PreparedStatementLimits::new(
            core::DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION,
            core::DEFAULT_MAX_PORTALS_PER_SESSION,
            core::DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
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

    let maximum_prepared = core::PreparedStatementLimits::new(
        core::MAX_PREPARED_STATEMENTS_PER_SESSION,
        core::MAX_PORTALS_PER_SESSION,
        core::MAX_RETAINED_BOUND_VALUE_BYTES,
    )
    .unwrap();
    assert_eq!(
        maximum_prepared.max_statements_per_session(),
        core::MAX_PREPARED_STATEMENTS_PER_SESSION
    );
    assert_eq!(
        maximum_prepared.max_portals_per_session(),
        core::MAX_PORTALS_PER_SESSION
    );
    assert_eq!(
        maximum_prepared.max_retained_bound_value_bytes(),
        core::MAX_RETAINED_BOUND_VALUE_BYTES
    );
    for invalid in [
        core::PreparedStatementLimits::new(0, 1, 1),
        core::PreparedStatementLimits::new(core::MAX_PREPARED_STATEMENTS_PER_SESSION + 1, 1, 1),
        core::PreparedStatementLimits::new(1, 0, 1),
        core::PreparedStatementLimits::new(1, core::MAX_PORTALS_PER_SESSION + 1, 1),
        core::PreparedStatementLimits::new(1, 1, 0),
        core::PreparedStatementLimits::new(1, 1, core::MAX_RETAINED_BOUND_VALUE_BYTES + 1),
    ] {
        assert_eq!(
            invalid.unwrap_err().kind(),
            core::EngineErrorKind::InvalidArgument
        );
    }

    let limits = core::ResultLimits::new(37, 4_096).unwrap();
    let prepared_limits = core::PreparedStatementLimits::new(17, 19, 8_192).unwrap();
    let configured = minimum
        .with_result_limits(limits)
        .with_prepared_statement_limits(prepared_limits)
        .with_request_timeout(None)
        .unwrap()
        .with_shutdown_grace(Duration::from_millis(250))
        .unwrap();
    assert_eq!(configured.result_limits(), limits);
    assert_eq!(configured.prepared_statement_limits(), prepared_limits);
    assert_eq!(configured.request_timeout(), None);
    assert_eq!(configured.shutdown_grace(), Duration::from_millis(250));
}

#[tokio::test]
async fn protocol_neutral_async_engine_surface_is_available() {
    let temp = tempfile::tempdir().unwrap();
    let limits = core::ResultLimits::new(50, 8_192).unwrap();
    let prepared_limits = core::PreparedStatementLimits::new(13, 17, 32_768).unwrap();
    let options = core::EngineOptions::new(2, 7)
        .unwrap()
        .with_result_limits(limits)
        .with_prepared_statement_limits(prepared_limits)
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
    assert_eq!(status.prepared_statement_limits(), prepared_limits);
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
async fn prepared_lifecycle_is_public_owned_and_protocol_neutral() {
    fn assert_owned<T: Send + Sync + 'static>() {}
    assert_owned::<core::PrepareRequest>();
    assert_owned::<core::PreparedStatementId>();
    assert_owned::<core::PortalId>();
    assert_owned::<core::DescribeTarget>();
    assert_owned::<core::PreparedStatementDescription>();
    assert_owned::<core::PreparedExecution>();

    let temp = tempfile::tempdir().unwrap();
    drop(core::Database::open(temp.path(), 4).unwrap());
    insert_prepared_catalog_fixture(temp.path());
    let database = Arc::new(core::Database::open(temp.path(), 4).unwrap());
    database
        .broadcast(
            "CREATE TABLE prepared_events (
                tenant_id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
             )",
        )
        .unwrap();
    let logical_database = database.catalog().default_database().id();
    let engine = core::Engine::from_database(database);
    let session = engine.session();

    let private_request = core::PrepareRequest::new(
        logical_database,
        sql::SqlDialect::MySql,
        sql::SqlTranslationMode::Compatibility,
        "INSERT INTO prepared_events (tenant_id, payload) VALUES (?, 'private-literal')",
    );
    assert_eq!(private_request.database(), logical_database);
    assert_eq!(private_request.dialect(), sql::SqlDialect::MySql);
    assert_eq!(
        private_request.translation_mode(),
        sql::SqlTranslationMode::Compatibility
    );
    assert!(private_request.sql().contains("private-literal"));
    assert!(!format!("{private_request:?}").contains("private-literal"));

    let insert = engine
        .prepare_statement(&session, private_request)
        .await
        .unwrap();
    let insert_description = engine
        .describe_prepared(&session, core::DescribeTarget::Statement(insert))
        .await
        .unwrap();
    assert_eq!(
        insert_description.parameter_types(),
        [core::DataType::Unknown]
    );
    assert!(insert_description.columns().is_empty());
    let insert_portal = engine
        .bind_statement(&session, insert, vec![core::Value::from(42_i64)])
        .await
        .unwrap();
    let inserted = engine
        .execute_portal(&session, insert_portal)
        .await
        .unwrap();
    assert_eq!(inserted.value, core::PreparedExecution::AffectedRows(1));

    let select = engine
        .prepare_statement(
            &session,
            core::PrepareRequest::new(
                logical_database,
                sql::SqlDialect::PostgreSql,
                sql::SqlTranslationMode::Compatibility,
                "SELECT tenant_id, payload FROM prepared_events WHERE tenant_id = $1",
            ),
        )
        .await
        .unwrap();
    let select_portal = engine
        .bind_statement(&session, select, vec![core::Value::from(42_i64)])
        .await
        .unwrap();
    let description = engine
        .describe_prepared(&session, core::DescribeTarget::Portal(select_portal))
        .await
        .unwrap();
    assert_eq!(description.parameter_types(), [core::DataType::Unknown]);
    assert_eq!(
        description.columns(),
        [
            core::Column::new("tenant_id", core::DataType::Unknown),
            core::Column::new("payload", core::DataType::Unknown),
        ]
    );
    assert!(description.returns_rows());
    let selected = engine
        .execute_portal(&session, select_portal)
        .await
        .unwrap();
    let core::PreparedExecution::Rows(rows) = selected.value else {
        panic!("the public prepared SELECT should return rows");
    };
    assert_eq!(
        rows.rows()[0].values(),
        [
            core::Value::from(42_i64),
            core::Value::from("private-literal")
        ]
    );

    assert!(engine.close_portal(&session, select_portal).await.unwrap());
    assert!(
        engine
            .close_prepared_statement(&session, select)
            .await
            .unwrap()
    );
    assert!(
        engine
            .close_prepared_statement(&session, insert)
            .await
            .unwrap()
    );
    assert_eq!(
        engine
            .execute_portal(&session, insert_portal)
            .await
            .unwrap_err()
            .kind(),
        core::EngineErrorKind::FailedPrecondition
    );
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
