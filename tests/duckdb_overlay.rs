#![cfg(all(unix, feature = "experimental-duckdb-reader"))]
use briskdb::s3_overlay::{Cell, Column, ColumnType, Config, Database, DuckDbReadOptions};
use object_store::memory::InMemory;
use std::{collections::BTreeMap, slice::from_ref, sync::Arc};

fn row(id: &str, text: &str) -> Vec<Cell> {
    vec![Cell::Text(id.into()), Cell::Text(text.into())]
}
fn options(threads: u16) -> DuckDbReadOptions {
    DuckDbReadOptions {
        library: std::env::var_os("BRISKDB_TEST_DUCKDB_LIBRARY")
            .expect("supply pinned DuckDB library")
            .into(),
        sqlite_extension: std::env::var_os("BRISKDB_TEST_DUCKDB_SQLITE")
            .expect("supply signed matching SQLite extension")
            .into(),
        threads,
        memory_mb: 256,
    }
}

#[test]
#[ignore = "requires explicitly supplied DuckDB 1.5.6 native artifacts"]
fn duckdb_reader_matches_sqlite_before_pending_after_update_delete_and_compaction() {
    let directory = tempfile::tempdir().unwrap();
    let table = briskdb::s3_overlay::Table {
        name: "items".into(),
        columns: vec![
            Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            },
            Column {
                name: "value".into(),
                kind: ColumnType::Text,
                nullable: false,
            },
        ],
        primary_key: vec!["id".into()],
        shard_key: "id".into(),
        indexes: vec![],
    };
    let mut config = Config::new("test", "us-east-1", "experimental", vec![table]).unwrap();
    config.partitions = 4;
    let mut db = Database::create(
        directory.path().join("db"),
        config,
        Arc::new(InMemory::new()),
        BTreeMap::from([(
            "items".into(),
            vec![row("one", "original"), row("two", "other")],
        )]),
    )
    .unwrap();
    let sql = "SELECT id,value FROM items WHERE id=? ORDER BY id";
    for threads in [1, 2, 4] {
        let opt = options(threads);
        for value in [None, Some("updated")] {
            if let Some(value) = value {
                db.execute("UPDATE items SET value=? WHERE id=?", &row(value, "one"))
                    .unwrap();
            }
            let params = [Cell::Text("one".into())];
            assert_eq!(
                db.query_partition_duckdb("items", &params[0], sql, &params, &opt)
                    .unwrap()
                    .rows,
                db.query(sql, &params).unwrap().rows
            );
        }
        db.execute(
            "INSERT INTO items VALUES (?,?)",
            &row(&format!("pending-{threads}"), "inserted"),
        )
        .unwrap();
        let params = [Cell::Text(format!("pending-{threads}"))];
        assert_eq!(
            db.query_partition_duckdb("items", &params[0], sql, &params, &opt)
                .unwrap()
                .rows,
            vec![row(&format!("pending-{threads}"), "inserted")]
        );
        db.execute("DELETE FROM items WHERE id=?", &params).unwrap();
        assert!(
            db.query_partition_duckdb("items", &params[0], sql, &params, &opt)
                .unwrap()
                .rows
                .is_empty()
        );
        db.compact_all().unwrap();
        let params = [Cell::Text("one".into())];
        assert_eq!(
            db.query_partition_duckdb("items", &params[0], sql, &params, &opt)
                .unwrap()
                .rows,
            db.query(sql, &params).unwrap().rows
        );
        assert!(
            db.query_partition_duckdb("items", &params[0], "DELETE FROM items", &[], &opt)
                .is_err()
        );
        assert!(
            db.query_partition_duckdb(
                "items",
                &params[0],
                "SELECT * FROM read_csv('/etc/passwd')",
                &[],
                &opt
            )
            .is_err()
        );
    }
}

#[test]
#[ignore = "requires explicitly supplied DuckDB 1.5.6 native artifacts"]
fn duckdb_preserves_scalar_types_empty_partitions_and_errors() {
    let directory = tempfile::tempdir().unwrap();
    let table = briskdb::s3_overlay::Table {
        name: "typed".into(),
        columns: [
            ("id", ColumnType::Integer),
            ("n", ColumnType::Integer),
            ("r", ColumnType::Real),
            ("text", ColumnType::Text),
            ("bytes", ColumnType::Blob),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (name, kind))| Column {
            name: name.into(),
            kind,
            nullable: i != 0,
        })
        .collect(),
        primary_key: vec!["id".into()],
        shard_key: "id".into(),
        indexes: vec![],
    };
    let mut config = Config::new("test", "us-east-1", "experimental", vec![table]).unwrap();
    config.partitions = 4;
    let mut db = Database::create(
        directory.path().join("db"),
        config,
        Arc::new(InMemory::new()),
        BTreeMap::new(),
    )
    .unwrap();
    let key = Cell::Integer(12);
    let sql = "SELECT * FROM typed WHERE id=?";
    let opt = options(4);
    assert!(
        db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &opt)
            .unwrap()
            .rows
            .is_empty()
    );
    let values = vec![
        key.clone(),
        Cell::Integer(i64::MIN),
        Cell::Real(1.25),
        Cell::Text("Unicode α\0quoted'".into()),
        Cell::Blob(vec![0, 255, 42]),
    ];
    db.execute("INSERT INTO typed VALUES (?,?,?,?,?)", &values)
        .unwrap();
    for compact in [false, true] {
        if compact {
            db.compact_all().unwrap();
        }
        assert_eq!(
            db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &opt)
                .unwrap()
                .rows,
            vec![values.clone()]
        );
        assert_eq!(
            db.query(sql, from_ref(&key)).unwrap().rows,
            vec![values.clone()]
        );
    }
    db.execute(
        "UPDATE typed SET n=NULL,r=NULL,text=NULL,bytes=NULL WHERE id=?",
        from_ref(&key),
    )
    .unwrap();
    assert_eq!(
        db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &opt)
            .unwrap()
            .rows,
        vec![vec![
            key.clone(),
            Cell::Null,
            Cell::Null,
            Cell::Null,
            Cell::Null
        ]]
    );
    assert!(
        db.query_partition_duckdb("typed", &key, sql, &[], &opt)
            .is_err()
    );
    assert!(
        db.query_partition_duckdb("typed", &Cell::Text("12".into()), sql, from_ref(&key), &opt)
            .is_err()
    );
    assert!(
        db.query_partition_duckdb("typed", &key, "SELECT 1; SELECT 2", &[], &opt)
            .is_err()
    );
    assert!(
        db.query_partition_duckdb("typed", &key, "SELECT 1.0 / 0.0", &[], &opt)
            .is_err()
    );
    let mut bad = opt.clone();
    bad.threads = 0;
    assert!(
        db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &bad)
            .is_err()
    );
    bad = opt.clone();
    bad.memory_mb = 1;
    assert!(
        db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &bad)
            .is_err()
    );
    bad = opt.clone();
    bad.library = directory.path().join("missing-library");
    assert!(
        db.query_partition_duckdb("typed", &key, sql, from_ref(&key), &bad)
            .is_err()
    );
    assert_eq!(
        db.query("SELECT id FROM typed WHERE id=?", from_ref(&key))
            .unwrap()
            .rows,
        vec![vec![key]]
    );
    let recursive = "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<4096) SELECT n, CASE WHEN n%3=0 THEN NULL ELSE 'Unicode α' END, CAST(n AS REAL), n%2=0 FROM t ORDER BY n";
    assert_eq!(
        db.query_partition_duckdb("typed", &Cell::Integer(12), recursive, &[], &opt)
            .unwrap()
            .rows,
        db.query(recursive, &[]).unwrap().rows,
        "multiple chunks and validity masks must preserve rows and scalar types",
    );
    let oversized = "WITH RECURSIVE t(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM t WHERE n<100000) SELECT n FROM t";
    assert!(
        db.query_partition_duckdb("typed", &Cell::Integer(12), oversized, &[], &opt)
            .is_err()
    );
}
