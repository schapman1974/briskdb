use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
};

use briskdb::{
    core::{
        CancellationToken, Database, EngineErrorKind, GeneratedIdPolicy, ShardKeyType,
        TablePlacement,
    },
    import::{
        SQLITE_IMPORT_RECEIPT_VERSION, SqliteImportKeyType, SqliteImportOptions,
        SqliteImportPlacement, SqliteImportPlan, SqliteImportReport, SqliteImportTableReport,
        SqliteTableImportPlan, import_sqlite_database,
    },
};
use rusqlite::{Connection, OpenFlags, params, types::ValueRef};

const SHARD_COUNT: u16 = 3;

#[test]
fn explicit_native_range_import_preserves_legacy_rows_and_owner_floors() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("native-source.sqlite");
    let destination = temporary.path().join("native-imported");
    let source = Connection::open(&source_path).unwrap();
    source
        .execute_batch(
            "CREATE TABLE records(
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 value TEXT NOT NULL
             );
             INSERT INTO records(id, value) VALUES (-9, 'negative'), (17, 'legacy');",
        )
        .unwrap();
    drop(source);
    let plan = SqliteImportPlan::new(vec![
        SqliteTableImportPlan::sharded_by_primary_key("records")
            .with_native_range_v1("id")
            .unwrap(),
    ]);

    import_sqlite_database(
        &source_path,
        &destination,
        &plan,
        SqliteImportOptions::new(4).unwrap(),
    )
    .unwrap();

    let database = Database::open(&destination, 4).unwrap();
    let metadata = database
        .catalog()
        .tables()
        .iter()
        .find(|table| table.name() == "records")
        .unwrap();
    assert_eq!(
        metadata.generated_id_policy(),
        &GeneratedIdPolicy::native_range_v1("id").unwrap()
    );
    let routes = integer_routes(&database, [-9, 17]);
    drop(database);

    let shards = open_shards(&destination, 4);
    assert_integer_owners(&shards, "records", &routes);
    for (shard, connection) in shards.iter().enumerate() {
        let sequence = connection
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'records'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        let expected_floor =
            0x4000_0000_0000_0000_i64 + i64::try_from(shard).unwrap() * (1_i64 << 52);
        assert_eq!(sequence, expected_floor);
        assert_ne!(sequence, 17);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RawValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl RawValue {
    fn from_ref(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.to_bits()),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }
}

#[test]
fn public_import_preserves_exact_sharded_ownership_and_sqlite_values() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("source.sqlite");
    let destination = temporary.path().join("imported");
    create_full_source(&source_path);

    let plan = SqliteImportPlan::new(vec![
        SqliteTableImportPlan::sharded_by_primary_key("events"),
        SqliteTableImportPlan::global("global_settings"),
        SqliteTableImportPlan::sharded("typed_rows", "id", SqliteImportKeyType::Int64),
        SqliteTableImportPlan::sharded_by_primary_key("generated_rows"),
        SqliteTableImportPlan::sharded("rowid_rows", "logical_key", SqliteImportKeyType::Text),
        SqliteTableImportPlan::sharded("binary_key_rows", "key", SqliteImportKeyType::Binary),
        SqliteTableImportPlan::sharded_by_primary_key("sequence_rows"),
    ]);

    let report = import_sqlite_database(
        &source_path,
        &destination,
        &plan,
        SqliteImportOptions::new(SHARD_COUNT).unwrap(),
    )
    .unwrap();

    assert_eq!(SQLITE_IMPORT_RECEIPT_VERSION, 2);
    assert_eq!(report.receipt_version, SQLITE_IMPORT_RECEIPT_VERSION);
    assert_eq!(report.shard_count, SHARD_COUNT);
    assert_eq!(report.hash_version, 1);
    assert_eq!(report.key_encoding_version, 1);
    assert_eq!(report.bucket_algorithm_version, 1);
    assert_eq!(report.map_generation, 1);
    assert!(is_lower_hex_digest(&report.source_schema_blake3));
    assert!(is_lower_hex_digest(&report.plan_blake3));
    assert!(report.omitted_foreign_keys.is_empty());
    assert_eq!(report.tables.len(), 7);

    assert_sharded_report(report_for(&report, "events"), 120);
    assert_global_report(report_for(&report, "global_settings"), 2);
    assert_sharded_report(report_for(&report, "typed_rows"), 7);
    assert_sharded_report(report_for(&report, "generated_rows"), 2);
    assert_sharded_report(report_for(&report, "rowid_rows"), 3);
    assert_sharded_report(report_for(&report, "binary_key_rows"), 3);
    assert_sharded_report(report_for(&report, "sequence_rows"), 2);
    for table in &report.tables {
        assert!(is_lower_hex_digest(&table.logical_contents_blake3));
        assert_eq!(
            table.sqlite_sequence,
            (table.table == "sequence_rows").then_some(500),
            "wrong sqlite_sequence receipt value for {}",
            table.table,
        );
    }

    let receipt: SqliteImportReport = serde_json::from_reader(
        File::open(destination.join("briskdb-import-receipt.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt, report);

    let database = Database::open(&destination, SHARD_COUNT).unwrap();
    assert_eq!(database.catalog().tables().len(), 7);
    assert_sharded_catalog(&database, "events", "id", ShardKeyType::Int64);
    assert_global_catalog(&database, "global_settings");
    assert_sharded_catalog(&database, "typed_rows", "id", ShardKeyType::Int64);
    assert_sharded_catalog(&database, "generated_rows", "id", ShardKeyType::Int64);
    assert_sharded_catalog(&database, "rowid_rows", "logical_key", ShardKeyType::Text);
    assert_sharded_catalog(&database, "binary_key_rows", "key", ShardKeyType::Binary);
    assert_sharded_catalog(&database, "sequence_rows", "id", ShardKeyType::Int64);
    assert_eq!(
        database
            .catalog()
            .tables()
            .iter()
            .filter(|table| matches!(table.placement(), TablePlacement::Global))
            .count(),
        1,
        "only explicitly Global tables may be replicated",
    );

    let event_routes = integer_routes(&database, 1..=120);
    let typed_routes = integer_routes(&database, 1..=7);
    let generated_routes = integer_routes(&database, 1..=2);
    let sequence_routes = integer_routes(&database, [3, 40]);
    let rowid_routes = ["alpha", "beta", "gamma"]
        .into_iter()
        .map(|key| (key.to_owned(), database.shard_for_key(key.as_bytes())))
        .collect::<BTreeMap<_, _>>();
    let binary_routes = [Vec::new(), vec![0, 0xff], vec![b'a', 0, b'b']]
        .into_iter()
        .map(|key| {
            let shard = database.shard_for_key(&key);
            (key, shard)
        })
        .collect::<BTreeMap<_, _>>();
    drop(database);

    let shards = open_shards(&destination, SHARD_COUNT);
    assert_integer_owners(&shards, "events", &event_routes);
    assert_integer_owners(&shards, "typed_rows", &typed_routes);
    assert_integer_owners(&shards, "generated_rows", &generated_routes);
    assert_integer_owners(&shards, "sequence_rows", &sequence_routes);
    assert_event_values(&shards);
    assert_global_rows_are_replicated(&shards);
    assert_raw_storage_classes(&shards);
    assert_generated_columns(&shards);
    assert_implicit_rowids(&shards, &rowid_routes);
    assert_binary_key_owners(&shards, &binary_routes);
    assert_sequence_high_water(&shards);
}

#[test]
fn pre_cancelled_import_never_creates_the_destination_and_clean_retry_succeeds() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("source.sqlite");
    let destination = temporary.path().join("cancelled-destination");
    let connection = Connection::open(&source_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT);
             INSERT INTO records VALUES (1, 'one');",
        )
        .unwrap();
    drop(connection);

    let cancellation = CancellationToken::new();
    assert!(cancellation.cancel());
    let plan = SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded_by_primary_key(
        "records",
    )]);
    let error = import_sqlite_database(
        &source_path,
        &destination,
        &plan,
        SqliteImportOptions::new(2)
            .unwrap()
            .with_cancellation_token(cancellation),
    )
    .unwrap_err();

    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert!(!destination.exists());

    let retry = import_sqlite_database(
        &source_path,
        &destination,
        &plan,
        SqliteImportOptions::new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(retry.tables.len(), 1);
    assert_eq!(retry.tables[0].source_rows, 1);
    assert_eq!(retry.tables[0].physical_rows.iter().sum::<u64>(), 1);
    assert!(destination.join("manifest.sqlite").is_file());
}

#[test]
fn incomplete_and_invalid_plans_never_create_a_destination() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("source.sqlite");
    let connection = Connection::open(&source_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE alpha(id INTEGER PRIMARY KEY);
             CREATE TABLE beta(id INTEGER PRIMARY KEY);",
        )
        .unwrap();
    drop(connection);

    let incomplete_destination = temporary.path().join("incomplete-destination");
    let incomplete =
        SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded_by_primary_key("alpha")]);
    let error = import_sqlite_database(
        &source_path,
        &incomplete_destination,
        &incomplete,
        SqliteImportOptions::new(2).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert!(!incomplete_destination.exists());

    let invalid_destination = temporary.path().join("invalid-destination");
    let invalid = SqliteImportPlan::new(vec![
        SqliteTableImportPlan::sharded_by_primary_key("alpha"),
        SqliteTableImportPlan::global("alpha"),
        SqliteTableImportPlan::sharded_by_primary_key("beta"),
    ]);
    let error = import_sqlite_database(
        &source_path,
        &invalid_destination,
        &invalid,
        SqliteImportOptions::new(2).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    assert!(!invalid_destination.exists());
}

#[test]
fn implicit_rowid_is_a_shard_local_physical_locator() {
    let temporary = tempfile::tempdir().unwrap();
    let source_path = temporary.path().join("source.sqlite");
    let destination = temporary.path().join("imported");
    let connection = Connection::open(&source_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE records(
                 logical_key TEXT NOT NULL COLLATE BINARY PRIMARY KEY,
                 value TEXT NOT NULL
             );",
        )
        .unwrap();
    connection.close().unwrap();
    let plan = SqliteImportPlan::new(vec![SqliteTableImportPlan::sharded(
        "records",
        "logical_key",
        SqliteImportKeyType::Text,
    )]);
    import_sqlite_database(
        &source_path,
        &destination,
        &plan,
        SqliteImportOptions::new(2).unwrap(),
    )
    .unwrap();

    let database = Database::open(&destination, 2).unwrap();
    let mut keys = [None, None];
    for candidate in (0_u32..10_000).map(|index| format!("key_{index}")) {
        let shard = usize::from(database.shard_for_key(candidate.as_bytes()));
        keys[shard].get_or_insert(candidate);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }
    let [Some(first_key), Some(second_key)] = keys else {
        panic!("failed to find one routed key for each physical shard");
    };
    drop(database);

    for (shard, key) in [(0_u16, first_key), (1_u16, second_key)] {
        let connection = Connection::open(shard_path(&destination, shard)).unwrap();
        connection
            .execute(
                "INSERT INTO records(_rowid_, logical_key, value) VALUES (7, ?1, 'value')",
                [key],
            )
            .unwrap();
        connection.close().unwrap();
    }

    let shards = open_shards(&destination, 2);
    let rows = shards
        .iter()
        .map(|connection| {
            connection
                .query_row("SELECT _rowid_, logical_key FROM records", [], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows[0].0, 7);
    assert_eq!(rows[1].0, 7);
    assert_ne!(rows[0].1, rows[1].1);
}

fn create_full_source(path: &Path) {
    let mut source = Connection::open(path).unwrap();
    source
        .execute_batch(
            "CREATE TABLE events(
                 id INTEGER PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE global_settings(
                 code TEXT NOT NULL COLLATE BINARY PRIMARY KEY,
                 value BLOB
             );
             CREATE TABLE typed_rows(
                 id INTEGER PRIMARY KEY,
                 text_value TEXT,
                 blob_value BLOB,
                 nul_text TEXT,
                 dynamic_value
             );
             CREATE TABLE generated_rows(
                 id INTEGER PRIMARY KEY,
                 base INTEGER NOT NULL,
                 doubled INTEGER GENERATED ALWAYS AS (base * 2) STORED,
                 display TEXT GENERATED ALWAYS AS ('v=' || base) VIRTUAL
             );
             CREATE TABLE rowid_rows(
                 logical_key TEXT NOT NULL COLLATE BINARY PRIMARY KEY,
                 value TEXT
             );
             CREATE TABLE binary_key_rows(
                 key BLOB NOT NULL PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE sequence_rows(
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 payload TEXT
             );",
        )
        .unwrap();

    let transaction = source.transaction().unwrap();
    for id in 1_i64..=120 {
        transaction
            .execute(
                "INSERT INTO events(id, value) VALUES (?1, ?2)",
                params![id, format!("event-{id:03}")],
            )
            .unwrap();
    }
    transaction
        .execute(
            "INSERT INTO global_settings(code, value) VALUES (?1, ?2)",
            params!["region", b"us-east".as_slice()],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO global_settings(code, value) VALUES (?1, ?2)",
            params!["feature_flags", vec![0_u8, 0xff, 1]],
        )
        .unwrap();

    transaction
        .execute(
            "INSERT INTO typed_rows
             VALUES (1, CAST(X'FF0061' AS TEXT), X'00FF10', CAST(X'610062' AS TEXT), NULL)",
            [],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows VALUES (2, 'valid', X'', '', ?1)",
            [i64::MIN],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows VALUES (3, 'real', X'01', 'x', ?1)",
            [1.25_f64],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows
             VALUES (4, 'text', X'02', 'y', CAST(X'FE00' AS TEXT))",
            [],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows VALUES (5, 'blob', X'03', 'z', X'00FF')",
            [],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows VALUES (6, 'max-int', X'04', 'm', ?1)",
            [i64::MAX],
        )
        .unwrap();
    transaction
        .execute(
            "INSERT INTO typed_rows VALUES (7, 'negative-zero', X'05', 'n', ?1)",
            [-0.0_f64],
        )
        .unwrap();

    transaction
        .execute(
            "INSERT INTO generated_rows(id, base) VALUES (1, 7), (2, -3)",
            [],
        )
        .unwrap();
    for (rowid, logical_key, value) in [
        (7_i64, "alpha", "a"),
        (41_i64, "beta", "b"),
        (900_i64, "gamma", "c"),
    ] {
        transaction
            .execute(
                "INSERT INTO rowid_rows(_rowid_, logical_key, value) VALUES (?1, ?2, ?3)",
                params![rowid, logical_key, value],
            )
            .unwrap();
    }
    for (key, value) in [
        (Vec::new(), "empty"),
        (vec![0, 0xff], "binary"),
        (vec![b'a', 0, b'b'], "embedded-nul"),
    ] {
        transaction
            .execute(
                "INSERT INTO binary_key_rows(key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .unwrap();
    }
    transaction
        .execute(
            "INSERT INTO sequence_rows(id, payload) VALUES (3, 'three'), (40, 'forty')",
            [],
        )
        .unwrap();
    transaction
        .execute(
            "UPDATE sqlite_sequence SET seq = 500 WHERE name = 'sequence_rows'",
            [],
        )
        .unwrap();
    transaction.commit().unwrap();
    source.close().unwrap();
}

fn report_for<'a>(report: &'a SqliteImportReport, table: &str) -> &'a SqliteImportTableReport {
    report
        .tables
        .iter()
        .find(|candidate| candidate.table == table)
        .unwrap_or_else(|| panic!("missing report for {table}"))
}

fn assert_sharded_report(report: &SqliteImportTableReport, source_rows: u64) {
    assert!(matches!(
        &report.placement,
        SqliteImportPlacement::Sharded { .. }
    ));
    assert_eq!(report.source_rows, source_rows);
    assert_eq!(report.physical_rows.len(), usize::from(SHARD_COUNT));
    assert_eq!(report.physical_rows.iter().sum::<u64>(), source_rows);
}

fn assert_global_report(report: &SqliteImportTableReport, source_rows: u64) {
    assert_eq!(report.placement, SqliteImportPlacement::Global);
    assert_eq!(report.source_rows, source_rows);
    assert_eq!(
        report.physical_rows,
        vec![source_rows; usize::from(SHARD_COUNT)]
    );
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn assert_sharded_catalog(database: &Database, table: &str, column: &str, key_type: ShardKeyType) {
    let metadata = database
        .catalog()
        .tables()
        .iter()
        .find(|candidate| candidate.name() == table)
        .unwrap_or_else(|| panic!("missing catalog table {table}"));
    assert!(
        matches!(metadata.placement(), TablePlacement::Sharded(key)
            if key.column() == column && key.key_type() == key_type),
        "wrong catalog placement for {table}: {:?}",
        metadata.placement(),
    );
    assert_eq!(
        metadata.generated_id_policy(),
        &GeneratedIdPolicy::None,
        "SQLite import must never infer generated-ID authority for {table}",
    );
}

fn assert_global_catalog(database: &Database, table: &str) {
    let metadata = database
        .catalog()
        .tables()
        .iter()
        .find(|candidate| candidate.name() == table)
        .unwrap_or_else(|| panic!("missing catalog table {table}"));
    assert!(matches!(metadata.placement(), TablePlacement::Global));
    assert_eq!(
        metadata.generated_id_policy(),
        &GeneratedIdPolicy::None,
        "SQLite import must persist no generated-ID authority for {table}",
    );
}

fn integer_routes(database: &Database, ids: impl IntoIterator<Item = i64>) -> BTreeMap<i64, u16> {
    ids.into_iter()
        .map(|id| {
            let encoded = id.to_string();
            (id, database.shard_for_key(encoded.as_bytes()))
        })
        .collect()
}

fn shard_path(root: &Path, shard: u16) -> PathBuf {
    root.join("shards").join(format!("{shard:04}.sqlite"))
}

fn open_shards(root: &Path, shard_count: u16) -> Vec<Connection> {
    (0..shard_count)
        .map(|shard| {
            Connection::open_with_flags(
                shard_path(root, shard),
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .unwrap()
        })
        .collect()
}

fn assert_integer_owners(shards: &[Connection], table: &str, expected_routes: &BTreeMap<i64, u16>) {
    assert!(
        table
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
        "test table names must be trusted identifiers",
    );
    let query = format!("SELECT id FROM \"{table}\"");
    let mut observed = BTreeMap::<i64, Vec<u16>>::new();
    for (shard, connection) in shards.iter().enumerate() {
        let mut statement = connection.prepare(&query).unwrap();
        let ids = statement
            .query_map([], |row| row.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for id in ids {
            observed
                .entry(id)
                .or_default()
                .push(u16::try_from(shard).unwrap());
        }
    }

    let expected = expected_routes
        .iter()
        .map(|(&id, &shard)| (id, vec![shard]))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        observed, expected,
        "{table} rows must exist on exactly one routed owner"
    );
}

fn assert_event_values(shards: &[Connection]) {
    let mut observed = BTreeMap::new();
    for connection in shards {
        let mut statement = connection.prepare("SELECT id, value FROM events").unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (id, value) in rows {
            assert!(observed.insert(id, value).is_none());
        }
    }
    let expected = (1_i64..=120)
        .map(|id| (id, format!("event-{id:03}")))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed, expected);
}

fn assert_global_rows_are_replicated(shards: &[Connection]) {
    let expected = vec![
        ("feature_flags".to_owned(), RawValue::Blob(vec![0, 0xff, 1])),
        ("region".to_owned(), RawValue::Blob(b"us-east".to_vec())),
    ];
    for connection in shards {
        let mut statement = connection
            .prepare("SELECT code, value FROM global_settings ORDER BY code COLLATE BINARY")
            .unwrap();
        let observed = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    RawValue::from_ref(row.get_ref(1)?),
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(observed, expected);
    }
}

fn assert_raw_storage_classes(shards: &[Connection]) {
    let mut observed = Vec::new();
    for connection in shards {
        let mut statement = connection
            .prepare(
                "SELECT id, text_value, blob_value, nul_text, dynamic_value
                 FROM typed_rows",
            )
            .unwrap();
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    RawValue::from_ref(row.get_ref(1)?),
                    RawValue::from_ref(row.get_ref(2)?),
                    RawValue::from_ref(row.get_ref(3)?),
                    RawValue::from_ref(row.get_ref(4)?),
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        observed.extend(rows);
    }
    observed.sort_by_key(|row| row.0);

    assert_eq!(
        observed,
        vec![
            (
                1,
                RawValue::Text(vec![0xff, 0, b'a']),
                RawValue::Blob(vec![0, 0xff, 0x10]),
                RawValue::Text(vec![b'a', 0, b'b']),
                RawValue::Null,
            ),
            (
                2,
                RawValue::Text(b"valid".to_vec()),
                RawValue::Blob(Vec::new()),
                RawValue::Text(Vec::new()),
                RawValue::Integer(i64::MIN),
            ),
            (
                3,
                RawValue::Text(b"real".to_vec()),
                RawValue::Blob(vec![1]),
                RawValue::Text(b"x".to_vec()),
                RawValue::Real(1.25_f64.to_bits()),
            ),
            (
                4,
                RawValue::Text(b"text".to_vec()),
                RawValue::Blob(vec![2]),
                RawValue::Text(b"y".to_vec()),
                RawValue::Text(vec![0xfe, 0]),
            ),
            (
                5,
                RawValue::Text(b"blob".to_vec()),
                RawValue::Blob(vec![3]),
                RawValue::Text(b"z".to_vec()),
                RawValue::Blob(vec![0, 0xff]),
            ),
            (
                6,
                RawValue::Text(b"max-int".to_vec()),
                RawValue::Blob(vec![4]),
                RawValue::Text(b"m".to_vec()),
                RawValue::Integer(i64::MAX),
            ),
            (
                7,
                RawValue::Text(b"negative-zero".to_vec()),
                RawValue::Blob(vec![5]),
                RawValue::Text(b"n".to_vec()),
                RawValue::Real((-0.0_f64).to_bits()),
            ),
        ]
    );
}

fn assert_binary_key_owners(shards: &[Connection], expected_routes: &BTreeMap<Vec<u8>, u16>) {
    let mut observed = BTreeMap::<Vec<u8>, Vec<u16>>::new();
    for (shard, connection) in shards.iter().enumerate() {
        let keys = connection
            .prepare("SELECT key FROM binary_key_rows")
            .unwrap()
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for key in keys {
            observed
                .entry(key)
                .or_default()
                .push(u16::try_from(shard).unwrap());
        }
    }
    let expected = expected_routes
        .iter()
        .map(|(key, &shard)| (key.clone(), vec![shard]))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed, expected);
}

fn assert_generated_columns(shards: &[Connection]) {
    let mut rows = Vec::new();
    for connection in shards {
        let hidden = connection
            .prepare(
                "SELECT name, hidden
                 FROM pragma_table_xinfo('generated_rows')
                 WHERE hidden <> 0
                 ORDER BY cid",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            hidden,
            vec![("doubled".to_owned(), 3), ("display".to_owned(), 2)]
        );

        let shard_rows = connection
            .prepare("SELECT id, base, doubled, display FROM generated_rows")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.extend(shard_rows);
    }
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        vec![(1, 7, 14, "v=7".to_owned()), (2, -3, -6, "v=-3".to_owned())]
    );
}

fn assert_implicit_rowids(shards: &[Connection], expected_routes: &BTreeMap<String, u16>) {
    let mut observed = BTreeMap::<String, Vec<(i64, String, u16)>>::new();
    for (shard, connection) in shards.iter().enumerate() {
        let rows = connection
            .prepare("SELECT _rowid_, logical_key, value FROM rowid_rows")
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for (rowid, key, value) in rows {
            observed
                .entry(key)
                .or_default()
                .push((rowid, value, u16::try_from(shard).unwrap()));
        }
    }

    let expected_values = [
        ("alpha", 7_i64, "a"),
        ("beta", 41_i64, "b"),
        ("gamma", 900_i64, "c"),
    ];
    let expected = expected_values
        .into_iter()
        .map(|(key, rowid, value)| {
            (
                key.to_owned(),
                vec![(rowid, value.to_owned(), expected_routes[key])],
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed, expected);
}

fn assert_sequence_high_water(shards: &[Connection]) {
    for connection in shards {
        let high_water = connection
            .query_row(
                "SELECT seq FROM sqlite_sequence WHERE name = 'sequence_rows'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(high_water, 500);
    }
}
