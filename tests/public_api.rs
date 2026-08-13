use std::{
    collections::BTreeSet,
    path::Path,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use axum::Router;
use briskdb::{
    api, core,
    protocol::{error, http, postgres},
    server, sql, storage,
};

fn register_catalog_fixture(database: &mut core::Database) {
    database
        .broadcast(
            "CREATE TABLE accounts (
                id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                payload TEXT,
                PRIMARY KEY (tenant_id, id)
             );
             CREATE TABLE countries (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL
             );",
        )
        .unwrap();
    let logical_database = database.catalog().default_database().id();
    database
        .register_tables(vec![
            core::TableDeclaration::global(logical_database, "countries").unwrap(),
            core::TableDeclaration::catalog(logical_database, "internal_catalog").unwrap(),
            core::TableDeclaration::sharded(
                logical_database,
                "accounts",
                core::ShardKeyMetadata::new("tenant_id", core::ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
}

fn register_prepared_catalog_fixture(database: &mut core::Database) {
    database
        .broadcast(
            "CREATE TABLE prepared_events (
                tenant_id INTEGER PRIMARY KEY,
                payload TEXT NOT NULL
             )",
        )
        .unwrap();
    let logical_database = database.catalog().default_database().id();
    database
        .register_tables(vec![
            core::TableDeclaration::sharded(
                logical_database,
                "prepared_events",
                core::ShardKeyMetadata::new("tenant_id", core::ShardKeyType::Int64).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
}

fn assert_catalog_fixture(catalog: &core::Catalog) {
    assert_eq!(catalog.identifier_encoding_version(), 1);
    assert_eq!(catalog.schema_generation(), 1);
    assert_eq!(catalog.default_database().id().get(), 1);
    assert_eq!(catalog.default_database().name(), "default");
    assert_eq!(catalog.logical_databases().len(), 1);
    assert_eq!(catalog.tables().len(), 3);
    assert!(
        catalog
            .tables()
            .iter()
            .all(|table| table.generated_id_policy() == &core::GeneratedIdPolicy::None)
    );

    let logical_database = catalog.default_database();
    assert_eq!(
        catalog.database_by_id(logical_database.id()),
        Some(logical_database)
    );

    let countries = catalog.table("default", "countries").unwrap().unwrap();
    assert_eq!(countries.id().get(), 2);
    assert_eq!(countries.id().to_string(), "2");
    assert_eq!(countries.database_id().get(), 1);
    assert!(matches!(
        countries.placement(),
        core::TablePlacement::Global
    ));
    assert_eq!(
        catalog.table_by_id(core::TableId::new(countries.id().get()).unwrap()),
        Some(countries)
    );

    let accounts = catalog.table("default", "accounts").unwrap().unwrap();
    assert_eq!(accounts.id().get(), 1);
    assert_eq!(accounts.database_id(), logical_database.id());
    match accounts.placement() {
        core::TablePlacement::Sharded(shard_key) => {
            assert_eq!(shard_key.column(), "tenant_id");
            assert_eq!(shard_key.key_type(), core::ShardKeyType::Text);
        }
        placement => panic!("unexpected accounts placement: {placement:?}"),
    }

    assert!(matches!(
        catalog
            .table("default", "internal_catalog")
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
        [(1, "accounts"), (1, "countries"), (1, "internal_catalog")]
    );
}

#[test]
fn legacy_and_explicit_module_paths_are_both_available() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}

    assert_owned_public::<core::GeneratedKey>();
    assert_owned_public::<core::WriteResult>();
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
    let generated_key = core::GeneratedKey::new("id", core::Value::Int64(41));
    let write_result = core::WriteResult::with_generated_key(1, generated_key);
    assert_eq!(write_result.rows_affected, 1);
    assert_eq!(write_result.generated_key.unwrap().column, "id");
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
    let server_config = server::Config {
        listen: "127.0.0.1:7654".parse().unwrap(),
        postgres_listen: Some("127.0.0.1:5433".parse().unwrap()),
        data_dir: std::path::PathBuf::from("./briskdb-data"),
        shards: 4,
    };
    assert_eq!(
        server_config.postgres_listen,
        Some("127.0.0.1:5433".parse().unwrap())
    );

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
fn sqlite_import_generated_id_opt_in_is_public_owned_and_explicit() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}

    assert_owned_public::<briskdb::import::SqliteGeneratedIdPlan>();
    assert_owned_public::<briskdb::import::SqliteTableImportPlan>();
    let ordinary = briskdb::import::SqliteTableImportPlan::sharded_by_primary_key("events");
    assert_eq!(
        ordinary.generated_id_plan(),
        &briskdb::import::SqliteGeneratedIdPlan::None
    );

    let native = ordinary.with_native_range_v1("id").unwrap();
    assert_eq!(native.generated_id_plan().column(), Some("id"));
    assert!(matches!(
        native.generated_id_plan(),
        briskdb::import::SqliteGeneratedIdPlan::NativeRangeV1 { column, .. }
            if column == "id"
    ));
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

    // An empty authoritative catalog deliberately retains the raw SQLite
    // compatibility path and therefore does not invoke either layer.
    let temp = tempfile::tempdir().unwrap();
    let database = core::Database::open(temp.path(), 2).unwrap();
    let mut raw_sql = "SELECT 1".to_owned();
    raw_sql.push_str(&" ".repeat(sql::MAX_PARSED_SQL_BYTES));
    let result = database.query("parser-opt-in", &raw_sql, &[]).unwrap();
    assert_eq!(result.rows()[0].get(0), Some(&core::Value::Int64(1)));
}

#[tokio::test]
async fn postgres_adapter_boundary_is_public_session_scoped_and_not_a_listener() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<postgres::Adapter>();
    assert_send_sync::<postgres::Connection>();

    let temp = tempfile::tempdir().unwrap();
    let engine = core::Engine::open(temp.path(), 2).await.unwrap();
    let adapter = postgres::Adapter::new(engine.clone());
    let first = adapter.open_connection();
    let second = adapter.open_connection();
    let selected = adapter
        .open_connection_for("public_client", "default")
        .unwrap();

    assert_ne!(first.session_id(), second.session_id());
    assert_ne!(first.session_id(), selected.session_id());
    assert_eq!(first.status().await.unwrap().shard_count(), 2);
    assert_eq!(second.status().await.unwrap().shard_count(), 2);
    assert_eq!(selected.user(), Some("public_client"));
    assert_eq!(selected.database(), "default");
    assert_eq!(
        selected.database_id(),
        engine.catalog().default_database().id()
    );
    assert!(format!("{adapter:?}").contains("shard_count"));
    assert!(format!("{first:?}").contains("session_id"));
    assert!(!format!("{selected:?}").contains("public_client"));
    assert_eq!(
        adapter
            .open_connection_for("Invalid", "default")
            .unwrap_err()
            .kind(),
        core::EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        adapter
            .open_connection_for("public_client", "missing")
            .unwrap_err()
            .kind(),
        core::EngineErrorKind::InvalidArgument
    );

    first.close().await.unwrap();
    assert_eq!(
        first.status().await.unwrap_err().kind(),
        core::EngineErrorKind::FailedPrecondition
    );
    assert_eq!(second.status().await.unwrap().shard_count(), 2);
    second.close().await.unwrap();
    selected.close().await.unwrap();
    engine.shutdown().await.unwrap();
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
fn statement_classification_is_public_typed_ordered_and_conservative() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    fn assert_copy_public<T: Copy + Eq + std::hash::Hash + Send + Sync + 'static>() {}

    assert_owned_public::<sql::StatementBatchClassification>();
    assert_copy_public::<sql::StatementBehavior>();
    assert_copy_public::<sql::WriteBehavior>();
    assert_copy_public::<sql::SchemaBehavior>();
    assert_copy_public::<sql::SessionBehavior>();

    let families = [
        ("SELECT 1", sql::StatementBehavior::Read),
        (
            "INSERT INTO widgets (id) VALUES (1)",
            sql::StatementBehavior::Write(sql::WriteBehavior::Insert),
        ),
        (
            "UPDATE widgets SET id = 2 WHERE id = 1",
            sql::StatementBehavior::Write(sql::WriteBehavior::Update),
        ),
        (
            "DELETE FROM widgets WHERE id = 1",
            sql::StatementBehavior::Write(sql::WriteBehavior::Delete),
        ),
        (
            "CREATE TABLE widgets (id INTEGER)",
            sql::StatementBehavior::Schema(sql::SchemaBehavior::CreateTable),
        ),
        (
            "CREATE INDEX widgets_id ON widgets (id)",
            sql::StatementBehavior::Schema(sql::SchemaBehavior::CreateIndex),
        ),
        (
            "BEGIN",
            sql::StatementBehavior::Session(sql::SessionBehavior::Begin),
        ),
        (
            "COMMIT",
            sql::StatementBehavior::Session(sql::SessionBehavior::Commit),
        ),
        (
            "ROLLBACK",
            sql::StatementBehavior::Session(sql::SessionBehavior::Rollback),
        ),
    ];

    for dialect in sql::SqlDialect::ALL.iter().copied() {
        for &(source, expected) in &families {
            let common = sql::validate_common_subset(sql::parse(dialect, source).unwrap()).unwrap();
            let classification = sql::classify_statements(&common).unwrap();
            assert_eq!(classification.statement_count(), 1);
            assert_eq!(classification.behaviors(), [expected]);
            assert_eq!(classification.behavior(0), Some(expected));
            assert_eq!(classification.behavior(1), None);
            assert_eq!(
                classification.is_read_only(),
                expected == sql::StatementBehavior::Read
            );
            assert_eq!(
                expected.is_read_only(),
                expected == sql::StatementBehavior::Read
            );
        }

        let private_source =
            "SELECT 'private-read-one' AS value; SELECT 'private-read-two' AS value";
        let common =
            sql::validate_common_subset(sql::parse(dialect, private_source).unwrap()).unwrap();
        let classification = sql::classify_statements(&common).unwrap();
        assert_eq!(
            classification.behaviors(),
            [sql::StatementBehavior::Read, sql::StatementBehavior::Read]
        );
        assert!(classification.is_read_only());
        let debug = format!("{classification:?}");
        assert!(!debug.contains("private-read-one"));
        assert!(!debug.contains("private-read-two"));

        let blocked_source =
            "SELECT 'private-batch-value'; INSERT INTO private_table (id) VALUES (1)";
        let blocked_common =
            sql::validate_common_subset(sql::parse(dialect, blocked_source).unwrap()).unwrap();
        let blocked = sql::classify_statements(&blocked_common).unwrap_err();
        assert_eq!(blocked.kind(), core::EngineErrorKind::Unsupported);
        assert!(blocked.diagnostic().contains("statement 2"));
        assert!(!blocked.diagnostic().contains("private-batch-value"));
        assert!(!blocked.diagnostic().contains("private_table"));
        assert_eq!(blocked_common.source(), blocked_source);
    }

    let empty = sql::validate_common_subset(
        sql::parse(sql::SqlDialect::Sqlite, "-- private empty batch").unwrap(),
    )
    .unwrap();
    let error = sql::classify_statements(&empty).unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::InvalidArgument);
    assert!(!error.diagnostic().contains("private empty batch"));
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
    let mut database = core::Database::open(temp.path(), 4).unwrap();
    register_catalog_fixture(&mut database);
    let database = Arc::new(database);
    let engine = core::Engine::from_database(Arc::clone(&database));
    let logical_database = database.catalog().default_database();
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
                logical_database.id(),
                translated.normalized_sql(),
                0,
                &parameter,
                None,
            )
            .unwrap()
    });
    assert_eq!(plans[0], plans[1]);
    assert_eq!(plans[1], plans[2]);

    // A populated authoritative catalog sends raw calls through the bounded
    // SQLite frontend, which accepts positional parameters and rejects named
    // parameters instead of retaining the empty-catalog pass-through.
    assert_eq!(
        database
            .query(
                "translation-opt-in",
                "SELECT :value",
                &[core::Value::Int64(29)],
            )
            .unwrap_err()
            .kind(),
        core::EngineErrorKind::Unsupported
    );
    let raw = database
        .query("translation-opt-in", "SELECT ?1", &[core::Value::Int64(29)])
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
    let mut database = core::Database::open(temp.path(), 4).unwrap();
    register_catalog_fixture(&mut database);
    let logical_database = database.catalog().default_database();

    let source = "INSERT INTO accounts (payload, tenant_id) VALUES ($1, $2), ($3, 'tenant-b')";
    let parsed = sql::parse(sql::SqlDialect::PostgreSql, source).unwrap();
    let common = sql::validate_common_subset(parsed).unwrap();
    let normalized = sql::normalize_placeholders(common).unwrap();
    let inference = sql::infer_shard_keys(
        database.catalog(),
        logical_database.id(),
        &normalized,
        0,
        &[
            core::Value::Text("payload-a".to_owned()),
            core::Value::Text("tenant-a".to_owned()),
            core::Value::Text("payload-b".to_owned()),
        ],
    )
    .unwrap();

    assert_eq!(inference.table_id().unwrap().get(), 1);
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
    let mut database = core::Database::open(temp.path(), 8).unwrap();
    register_catalog_fixture(&mut database);
    let database = Arc::new(database);
    let engine = core::Engine::from_database(Arc::clone(&database));
    let logical_database = database.catalog().default_database();

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
            logical_database.id(),
            &normalized,
            0,
            &first_parameters,
            Some(explicit_key.as_slice()),
        )
        .unwrap();

    assert_eq!(plan.database(), logical_database.id());
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
        plan.behavior(),
        sql::StatementBehavior::Write(sql::WriteBehavior::Insert)
    );
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
            logical_database.id(),
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
            logical_database.id(),
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
        .plan_bound_statement(
            logical_database.id(),
            &normalized,
            0,
            &second_parameters,
            None,
        )
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
    assert_public_metadata::<core::TableDeclaration>();
    assert_public_metadata::<core::TableMetadata>();
    assert_public_metadata::<core::TablePlacement>();
    assert_public_metadata::<core::ShardKeyMetadata>();
    assert_public_metadata::<core::ShardKeyType>();
    assert_public_metadata::<core::GeneratedIdPolicy>();

    let _signed = core::ShardKeyType::Int64;
    let _text = core::ShardKeyType::Text;
    let _binary = core::ShardKeyType::Binary;
    let _global = core::TablePlacement::Global;
    let _catalog = core::TablePlacement::Catalog;
    let generated_policy = core::GeneratedIdPolicy::native_range_v1("id").unwrap();
    assert_eq!(generated_policy.column(), Some("id"));
    assert_eq!(generated_policy.encoding_version(), Some(1));
    assert_eq!(core::GeneratedIdPolicy::None.column(), None);

    let generated_declaration = core::TableDeclaration::sharded(
        core::LogicalDatabaseId::new(1).unwrap(),
        "generated_rows",
        core::ShardKeyMetadata::new("id", core::ShardKeyType::Int64).unwrap(),
    )
    .unwrap()
    .with_generated_id_policy(generated_policy.clone())
    .unwrap();
    assert_eq!(
        generated_declaration.generated_id_policy(),
        &generated_policy
    );

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
    #[cfg(feature = "experimental-vtab")]
    assert!(!defaults.experimental_vtab_writes());

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

#[cfg(feature = "experimental-vtab")]
#[test]
fn experimental_vtab_write_option_is_public_and_opt_in() {
    let constructed = core::EngineOptions::new(2, 7).unwrap();
    assert!(!constructed.experimental_vtab_writes());

    let enabled = constructed.with_experimental_vtab_writes(true);
    assert!(enabled.experimental_vtab_writes());
    assert_eq!(enabled.connections_per_shard(), 2);
    assert_eq!(enabled.queue_capacity_per_shard(), 7);
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
    engine
        .broadcast(
            &session,
            "CREATE TABLE public_write_result (id INTEGER PRIMARY KEY)".to_owned(),
        )
        .await
        .unwrap();
    let write = engine
        .execute_write(
            &session,
            core::Statement::new("INSERT INTO public_write_result (id) VALUES (1)", vec![]),
        )
        .await
        .unwrap();
    assert_eq!(write.value, core::WriteResult::without_generated_key(1));
    let controlled_write = engine
        .execute_write_with_context(
            &session,
            core::Statement::new("INSERT INTO public_write_result (id) VALUES (2)", vec![]),
            core::RequestContext::new().with_deadline(Instant::now() + Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert_eq!(
        controlled_write.value,
        core::WriteResult::without_generated_key(1)
    );
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
    let mut database = core::Database::open(temp.path(), 4).unwrap();
    register_prepared_catalog_fixture(&mut database);
    let database = Arc::new(database);
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
        insert_description.behavior(),
        sql::StatementBehavior::Write(sql::WriteBehavior::Insert)
    );
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
    assert_eq!(description.behavior(), sql::StatementBehavior::Read);
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
    let mut database = core::Database::open(temp.path(), 10).unwrap();
    let expected_routes = keys.map(|key| database.shard_for_key(key));
    register_catalog_fixture(&mut database);
    assert_catalog_fixture(database.catalog());
    assert_eq!(keys.map(|key| database.shard_for_key(key)), expected_routes);

    let reopened = core::Database::open(temp.path(), 10).unwrap();
    assert_catalog_fixture(reopened.catalog());
    assert_eq!(keys.map(|key| reopened.shard_for_key(key)), expected_routes);

    let manifest = rusqlite::Connection::open(temp.path().join("manifest.sqlite")).unwrap();
    manifest
        .execute_batch(
            "PRAGMA ignore_check_constraints = ON;
             UPDATE briskdb_tables SET placement = 99 WHERE table_id = 1;
             PRAGMA ignore_check_constraints = OFF;",
        )
        .unwrap();
    drop(manifest);

    assert_catalog_fixture(reopened.catalog());
    assert_eq!(keys.map(|key| reopened.shard_for_key(key)), expected_routes);
    let error = core::Database::open(temp.path(), 10).unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::DataCorruption);

    assert_catalog_fixture(database.catalog());
    assert_eq!(keys.map(|key| database.shard_for_key(key)), expected_routes);
}

#[test]
fn database_and_engine_catalog_reads_are_deterministic_in_parallel() {
    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), 6).unwrap();
    register_catalog_fixture(&mut database);
    let database = Arc::new(database);
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
                            .table("default", "accounts")
                            .unwrap()
                            .unwrap()
                            .id()
                            .get(),
                        1
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

const GENERATED_DDL_TEST_SHARDS: u16 = 4;
const NATIVE_RANGE_V1_TEST_MARKER: i64 = 0x4000_0000_0000_0000;
const NATIVE_RANGE_V1_TEST_OWNER_STRIDE: i64 = 1_i64 << 52;

fn assert_generated_events_catalog(database: &core::Database, table_id: core::TableId) {
    let table = database
        .catalog()
        .table("default", "events")
        .unwrap()
        .expect("generated DDL must publish its table");
    assert_eq!(table.id(), table_id);
    assert_eq!(table.name(), "events");
    assert_eq!(
        table.generated_id_policy(),
        &core::GeneratedIdPolicy::native_range_v1("id").unwrap()
    );
    match table.placement() {
        core::TablePlacement::Sharded(shard_key) => {
            assert_eq!(shard_key.column(), "id");
            assert_eq!(shard_key.key_type(), core::ShardKeyType::Int64);
        }
        placement => panic!("unexpected generated table placement: {placement:?}"),
    }
    assert_eq!(database.catalog().tables().len(), 1);
}

fn assert_generated_events_physical_schema(root: &Path, local_sequence: i64) {
    for shard in 0..GENERATED_DDL_TEST_SHARDS {
        let connection =
            rusqlite::Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
        let schema_sql = connection
            .query_row(
                "SELECT sql FROM sqlite_schema
                 WHERE type = 'table' AND name = 'events'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert!(
            schema_sql.to_ascii_uppercase().contains("AUTOINCREMENT"),
            "shard {shard} has non-generated physical schema: {schema_sql}"
        );
        let id_shape = connection
            .query_row(
                "SELECT type, pk FROM pragma_table_info('events') WHERE name = 'id'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap();
        assert_eq!(id_shape, ("INTEGER".to_owned(), 1));
        let sequence = connection
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'events'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(
            sequence,
            NATIVE_RANGE_V1_TEST_MARKER
                + i64::from(shard) * NATIVE_RANGE_V1_TEST_OWNER_STRIDE
                + local_sequence,
            "shard {shard} must own a distinct native allocation range"
        );
    }
}

fn physical_table_exists(root: &Path, table: &str) -> bool {
    (0..GENERATED_DDL_TEST_SHARDS).any(|shard| {
        let connection =
            rusqlite::Connection::open(root.join(format!("shards/{shard:04}.sqlite"))).unwrap();
        connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_schema
                     WHERE type = 'table' AND name = ?1
                 )",
                [table],
                |row| row.get::<_, bool>(0),
            )
            .unwrap()
    })
}

fn generated_ddl_bridge_count(root: &Path) -> i64 {
    rusqlite::Connection::open(root.join("manifest.sqlite"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM briskdb_generated_table_ddl",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

fn schema_migration_count(root: &Path) -> i64 {
    rusqlite::Connection::open(root.join("manifest.sqlite"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM briskdb_schema_migrations",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn generated_table_ddl_accepts_documented_dialects_with_one_physical_contract() {
    fn assert_owned_public<T: Clone + Send + Sync + 'static>() {}
    assert_owned_public::<core::GeneratedTableDdlReceipt>();

    let cases = [
        (
            sql::SqlDialect::Sqlite,
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)",
        ),
        (
            sql::SqlDialect::MySql,
            "CREATE TABLE events (id BIGINT PRIMARY KEY AUTO_INCREMENT, payload TEXT NOT NULL)",
        ),
        (
            sql::SqlDialect::PostgreSql,
            "CREATE TABLE events (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL)",
        ),
        (
            sql::SqlDialect::PostgreSql,
            "CREATE TABLE events (id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, payload TEXT NOT NULL)",
        ),
    ];
    let mut receipts = Vec::new();
    let mut logical_ids = BTreeSet::new();

    for (dialect, source) in cases {
        let temp = tempfile::tempdir().unwrap();
        let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
        let receipt = database.apply_generated_table_ddl(dialect, source).unwrap();

        assert_generated_events_catalog(&database, receipt.table_id());
        assert_generated_events_physical_schema(temp.path(), 0);
        assert_ne!(receipt.logical_id(), receipt.physical_migration_id());
        assert!(
            logical_ids.insert(receipt.logical_id()),
            "each exact dialect source must retain a distinct logical identity"
        );
        receipts.push(receipt);
    }

    assert_eq!(logical_ids.len(), cases.len());
    assert_eq!(
        receipts[0].logical_id(),
        [
            0xed, 0x6d, 0x6c, 0x53, 0x10, 0xc7, 0x07, 0x6e, 0xec, 0x12, 0x03, 0x28, 0x98, 0xad,
            0xec, 0xea, 0xbf, 0x85, 0x93, 0xcb, 0x97, 0x4f, 0xaa, 0x73, 0xe9, 0x01, 0x48, 0xb2,
            0xc8, 0x78, 0x44, 0x07,
        ],
        "the version-1 SQLite logical-source identity is a storage-format vector"
    );
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].physical_migration_id() == pair[1].physical_migration_id())
    );
    assert!(
        receipts
            .windows(2)
            .all(|pair| pair[0].provisioning_id() == pair[1].provisioning_id())
    );

    let repeat = tempfile::tempdir().unwrap();
    let mut repeated_database =
        core::Database::open(repeat.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let repeated = repeated_database
        .apply_generated_table_ddl(cases[0].0, cases[0].1)
        .unwrap();
    assert_eq!(repeated, receipts[0]);
}

#[test]
fn generated_table_ddl_exact_retry_and_reopen_are_idempotent_and_owner_safe() {
    const SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let original = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    assert_eq!(
        database
            .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
            .unwrap(),
        original
    );
    assert_eq!(generated_ddl_bridge_count(temp.path()), 1);
    drop(database);

    let mut reopened = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let reopened_receipt = reopened
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    assert_eq!(reopened_receipt, original);
    assert_generated_events_catalog(&reopened, original.table_id());
    drop(reopened);

    for shard in 0..GENERATED_DDL_TEST_SHARDS {
        let connection =
            rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        connection
            .execute(
                "INSERT INTO events (payload) VALUES (?1)",
                [format!("physical-shard-{shard}")],
            )
            .unwrap();
        let expected =
            NATIVE_RANGE_V1_TEST_MARKER + i64::from(shard) * NATIVE_RANGE_V1_TEST_OWNER_STRIDE + 1;
        assert_eq!(connection.last_insert_rowid(), expected);
        assert_eq!(
            connection
                .query_row(
                    "SELECT payload FROM events WHERE id = ?1",
                    [expected],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            format!("physical-shard-{shard}")
        );
    }

    let validated = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    assert_generated_events_catalog(&validated, original.table_id());
    assert_generated_events_physical_schema(temp.path(), 1);
}

#[cfg(feature = "experimental-vtab")]
#[tokio::test]
async fn generated_table_ddl_catalog_drives_a_public_engine_omitted_key_insert() {
    const SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let receipt = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    let options = core::EngineOptions::default().with_experimental_vtab_writes(true);
    let engine = core::Engine::from_database_with_options(Arc::new(database), options).unwrap();
    assert_eq!(
        engine
            .catalog()
            .table_by_id(receipt.table_id())
            .unwrap()
            .name(),
        "events"
    );

    let inserted = engine
        .execute_write(
            &engine.session(),
            core::Statement::new(
                "INSERT INTO events (payload) VALUES (?1)",
                vec![core::Value::from("bridge-to-engine")],
            ),
        )
        .await
        .unwrap();
    assert_eq!(inserted.value.rows_affected, 1);
    let generated = inserted.value.generated_key.unwrap();
    assert_eq!(generated.column, "id");
    let core::Value::Int64(id) = generated.value else {
        panic!("native_range_v1 must return an Int64 generated key");
    };
    for shard in 0..GENERATED_DDL_TEST_SHARDS {
        assert_eq!(
            rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE id = ?1 AND payload = 'bridge-to-engine'",
                    [id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            i64::from(shard == inserted.shard),
            "physical shard {shard}"
        );
    }
}

#[test]
fn completed_generated_table_ddl_survives_later_schema_migrations() {
    const SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let original = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    assert_eq!(
        database
            .broadcast("ALTER TABLE events ADD COLUMN note TEXT")
            .unwrap(),
        (0..GENERATED_DDL_TEST_SHARDS).collect::<Vec<_>>()
    );
    assert_eq!(
        database
            .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
            .unwrap(),
        original
    );
    drop(database);

    let mut reopened = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    assert_eq!(
        reopened
            .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
            .unwrap(),
        original
    );
    assert_generated_events_catalog(&reopened, original.table_id());
    for shard in 0..GENERATED_DDL_TEST_SHARDS {
        let connection =
            rusqlite::Connection::open(temp.path().join(format!("shards/{shard:04}.sqlite")))
                .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT type FROM pragma_table_info('events') WHERE name = 'note'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "TEXT"
        );
    }
}

#[test]
fn generated_table_ddl_rejects_invalid_and_conflicting_requests_before_mutation() {
    const SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let invalid = database
        .apply_generated_table_ddl(
            sql::SqlDialect::Sqlite,
            "CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        )
        .unwrap_err();
    assert_eq!(invalid.kind(), core::EngineErrorKind::InvalidArgument);
    assert!(database.catalog().tables().is_empty());
    assert!(!physical_table_exists(temp.path(), "events"));
    assert_eq!(generated_ddl_bridge_count(temp.path()), 0);

    let original = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    let distinct_logical_source = format!("{SOURCE}\n");
    let logical_conflict = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, &distinct_logical_source)
        .unwrap_err();
    assert_eq!(
        logical_conflict.kind(),
        core::EngineErrorKind::FailedPrecondition
    );
    let table_conflict = database
        .apply_generated_table_ddl(
            sql::SqlDialect::MySql,
            "CREATE TABLE other_events (id BIGINT PRIMARY KEY AUTO_INCREMENT, payload TEXT NOT NULL)",
        )
        .unwrap_err();
    assert_eq!(
        table_conflict.kind(),
        core::EngineErrorKind::FailedPrecondition
    );

    assert_eq!(database.catalog().tables().len(), 1);
    assert_generated_events_catalog(&database, original.table_id());
    assert!(!physical_table_exists(temp.path(), "other_events"));
    assert_eq!(generated_ddl_bridge_count(temp.path()), 1);
    assert_eq!(
        database
            .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
            .unwrap(),
        original
    );
    let recorded_source = rusqlite::Connection::open(temp.path().join("manifest.sqlite"))
        .unwrap()
        .query_row(
            "SELECT source_sql FROM briskdb_generated_table_ddl",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(recorded_source, SOURCE);
}

#[test]
fn generated_table_ddl_preflights_authoritative_writable_contract_before_mutation() {
    const VALID_SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    database
        .broadcast("CREATE TABLE legacy (id INTEGER PRIMARY KEY, payload TEXT)")
        .unwrap();
    let generation = database.catalog().schema_generation();
    let migrations = schema_migration_count(temp.path());
    let error = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, VALID_SOURCE)
        .unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::FailedPrecondition);
    assert_eq!(database.catalog().schema_generation(), generation);
    assert_eq!(schema_migration_count(temp.path()), migrations);
    assert_eq!(generated_ddl_bridge_count(temp.path()), 0);
    assert!(physical_table_exists(temp.path(), "legacy"));
    assert!(!physical_table_exists(temp.path(), "events"));

    for source in [
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE)",
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT DEFAULT 'defaulted')",
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, __briskdb_locator BLOB)",
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, rowid TEXT, _rowid_ TEXT, oid TEXT)",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
        let generation = database.catalog().schema_generation();
        let migrations = schema_migration_count(temp.path());

        let error = database
            .apply_generated_table_ddl(sql::SqlDialect::Sqlite, source)
            .unwrap_err();
        assert_eq!(
            error.kind(),
            core::EngineErrorKind::FailedPrecondition,
            "source={source}; error={error}"
        );
        assert!(database.catalog().tables().is_empty(), "source={source}");
        assert_eq!(
            database.catalog().schema_generation(),
            generation,
            "source={source}"
        );
        assert_eq!(
            schema_migration_count(temp.path()),
            migrations,
            "source={source}"
        );
        assert_eq!(
            generated_ddl_bridge_count(temp.path()),
            0,
            "source={source}"
        );
        assert!(
            !physical_table_exists(temp.path(), "events"),
            "source={source}"
        );
    }

    let extra_columns = (1..2_000)
        .map(|index| format!("c{index} INTEGER"))
        .collect::<Vec<_>>()
        .join(", ");
    let source =
        format!("CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, {extra_columns})");
    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    let generation = database.catalog().schema_generation();
    let migrations = schema_migration_count(temp.path());
    let error = database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, &source)
        .unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::FailedPrecondition);
    assert!(database.catalog().tables().is_empty());
    assert_eq!(database.catalog().schema_generation(), generation);
    assert_eq!(schema_migration_count(temp.path()), migrations);
    assert_eq!(generated_ddl_bridge_count(temp.path()), 0);
    assert!(!physical_table_exists(temp.path(), "events"));
}

#[test]
fn generated_table_ddl_reopen_rejects_physical_schema_drift() {
    const SOURCE: &str =
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL)";

    let temp = tempfile::tempdir().unwrap();
    let mut database = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap();
    database
        .apply_generated_table_ddl(sql::SqlDialect::Sqlite, SOURCE)
        .unwrap();
    drop(database);

    rusqlite::Connection::open(temp.path().join("shards/0002.sqlite"))
        .unwrap()
        .execute_batch("ALTER TABLE events ADD COLUMN drift TEXT")
        .unwrap();

    let error = core::Database::open(temp.path(), GENERATED_DDL_TEST_SHARDS).unwrap_err();
    assert_eq!(error.kind(), core::EngineErrorKind::DataCorruption);
}
