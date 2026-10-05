use super::*;
use object_store::memory::InMemory;
use std::thread;

fn table(name: &str) -> Table {
    Table {
        name: name.into(),
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
        indexes: vec![vec!["value".into()]],
    }
}
pub(super) fn row(id: &str, value: &str) -> Row {
    vec![Cell::Text(id.into()), Cell::Text(value.into())]
}
pub(super) fn config() -> Config {
    let mut config = Config::new(
        "test-bucket",
        "us-east-1",
        "experimental",
        vec![table("items"), table("labels")],
    )
    .unwrap();
    config.partitions = 4;
    config.compact_after_files = 8;
    config
}
fn fixture() -> (tempfile::TempDir, Arc<InMemory>, Database) {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let seed = BTreeMap::from([
        (
            "items".into(),
            vec![row("a", "original"), row("b", "base-b")],
        ),
        ("labels".into(), vec![row("a", "label-a")]),
    ]);
    let database = Database::create(root.path().join("db"), config(), store.clone(), seed).unwrap();
    (root, store, database)
}
fn all(database: &mut Database) -> Vec<Row> {
    database
        .query("SELECT id,value FROM items ORDER BY id", &[])
        .unwrap()
        .rows
}

#[test]
fn open_stats_report_one_catalog_snapshot_and_no_base_warmup() {
    let (root, store, database) = fixture();
    assert!(database.open_stats().is_none());
    drop(database);
    let mut database = Database::open(root.path().join("db"), store).unwrap();
    let stats = database.open_stats().unwrap().clone();
    assert_eq!(stats.catalog_file_opens, 2);
    assert_eq!(stats.catalog_root_reads, 1);
    assert_eq!(stats.catalog_lock_requests, 1);
    assert!(stats.catalog_page_reads >= 1);
    assert_eq!(stats.store_client_ms, 0.0);
    let stages = stats.root_path_ms
        + stats.catalog_ms
        + stats.store_client_ms
        + stats.runtime_ms
        + stats.connection_ms;
    assert!(stats.total_ms >= stages);
    assert_eq!(database.read_stats().sqlite_base_opens, 0);
    database
        .query("SELECT value FROM items WHERE id='a'", &[])
        .unwrap();
    assert_eq!(database.read_stats().sqlite_base_opens, 1);
    assert_eq!(database.open_stats().unwrap().total_ms, stats.total_ms);
}

#[test]
fn cached_bases_reuse_connections_but_refresh_heads_after_remote_writes_and_compaction() {
    let (root, store, mut writer) = fixture();
    let mut reader = Database::open(root.path().join("db"), store.clone()).unwrap();
    let query = "SELECT value FROM items WHERE id='a'";
    assert_eq!(
        reader.query(query, &[]).unwrap().rows,
        vec![vec![Cell::Text("original".into())]]
    );
    assert_eq!(
        (
            reader.read_stats().sqlite_base_opens,
            reader.read_stats().sqlite_base_cache_hits
        ),
        (1, 0)
    );
    reader.query(query, &[]).unwrap();
    assert_eq!(
        (
            reader.read_stats().sqlite_base_opens,
            reader.read_stats().sqlite_base_cache_hits
        ),
        (0, 1)
    );
    assert_eq!(reader.read_stats().heads_read, 1);

    // A different handle publishes deltas; the reader retains its connection.
    writer
        .execute("UPDATE items SET value='changed' WHERE id='a'", &[])
        .unwrap();
    assert_eq!(
        reader.query(query, &[]).unwrap().rows,
        vec![vec![Cell::Text("changed".into())]]
    );
    assert_eq!(reader.read_stats().heads_read, 1);
    assert_eq!(reader.read_stats().sqlite_base_cache_hits, 0);
    let partition = writer.config().partition(&Cell::Text("a".into())).unwrap();
    writer.compact("items", partition).unwrap();
    assert_eq!(
        reader.query(query, &[]).unwrap().rows,
        vec![vec![Cell::Text("changed".into())]]
    );
    assert_eq!(
        (
            reader.read_stats().sqlite_base_opens,
            reader.read_stats().sqlite_base_cache_hits
        ),
        (1, 0)
    );
    assert_eq!(reader.read_stats().heads_read, 1);
    writer
        .execute("DELETE FROM items WHERE id='a'", &[])
        .unwrap();
    assert!(reader.query(query, &[]).unwrap().rows.is_empty());
    assert_eq!(reader.read_stats().sqlite_base_cache_hits, 0);
    assert_eq!(reader.read_stats().heads_read, 1);

    let released = Arc::downgrade(&reader.registry);
    drop(reader);
    assert!(
        released.upgrade().is_none(),
        "closing releases registry and its base cache"
    );
    let mut reopened = Database::open(root.path().join("db"), store).unwrap();
    assert!(reopened.query(query, &[]).unwrap().rows.is_empty());
    assert_eq!(
        (
            reopened.read_stats().sqlite_base_opens,
            reopened.read_stats().sqlite_base_cache_hits
        ),
        (0, 0)
    );
}

#[test]
fn warm_base_cache_reads_across_64_partitions_do_not_reopen_bases() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config();
    config.partitions = 64;
    let mut keys = BTreeMap::new();
    for number in 0..10_000 {
        let key = Cell::Text(format!("key-{number}"));
        keys.entry(config.partition(&key).unwrap()).or_insert(key);
        if keys.len() == 64 {
            break;
        }
    }
    assert_eq!(keys.len(), 64);
    let seed = keys
        .values()
        .map(|key| vec![key.clone(), Cell::Text("base".into())])
        .collect();
    let mut database = Database::create(
        root.path().join("db"),
        config,
        Arc::new(InMemory::new()),
        BTreeMap::from([("items".into(), seed)]),
    )
    .unwrap();
    for pass in 0..3 {
        for key in keys.values() {
            assert_eq!(
                database
                    .query(
                        "SELECT value FROM items WHERE id=?",
                        std::slice::from_ref(key)
                    )
                    .unwrap()
                    .rows,
                vec![vec![Cell::Text("base".into())]]
            );
            let stats = database.read_stats();
            assert_eq!(stats.heads_read, 1);
            assert_eq!(stats.sqlite_base_opens, u64::from(pass == 0));
            assert_eq!(stats.sqlite_base_cache_hits, u64::from(pass != 0));
            assert_eq!(stats.sqlite_base_cache_evictions, 0);
        }
    }
}

#[test]
fn base_cache_reuses_probes_within_one_statement_without_mixing_tables() {
    let (_root, _store, mut database) = fixture();
    let result = database.query(
        "SELECT value FROM items WHERE id='a' UNION ALL SELECT value FROM items WHERE id='a' UNION ALL SELECT value FROM labels WHERE id='a'",
        &[],
    ).unwrap();
    assert_eq!(
        result.rows,
        vec![
            vec![Cell::Text("original".into())],
            vec![Cell::Text("original".into())],
            vec![Cell::Text("label-a".into())],
        ]
    );
    assert_eq!(database.read_stats().sqlite_base_opens, 2);
    assert_eq!(database.read_stats().sqlite_base_cache_hits, 1);
    assert_eq!(database.read_stats().heads_read, 2);
}

#[test]
fn explicit_open_flags_are_per_handle_and_read_only_blocks_all_writes() {
    let (root, store, database) = fixture();
    assert_eq!(database.options(), OpenOptions::default());
    drop(database);
    let options = OpenOptions {
        parquet_pruning: false,
        read_only: true,
    };
    let mut read =
        Database::open_with_options(root.path().join("db"), store.clone(), options).unwrap();
    assert_eq!(read.options(), options);
    assert_eq!(
        all(&mut read),
        vec![row("a", "original"), row("b", "base-b")]
    );
    assert_eq!(
        read.execute("DELETE FROM items WHERE id='a'", &[])
            .unwrap_err()
            .kind(),
        EngineErrorKind::ReadOnly
    );
    assert_eq!(
        read.compact("items", 0).unwrap_err().kind(),
        EngineErrorKind::ReadOnly
    );
    assert_eq!(
        read.compact_all().unwrap_err().kind(),
        EngineErrorKind::ReadOnly
    );
    read.set_parquet_pruning(true);
    assert!(read.options().read_only);
    drop(read);
    let mut normal = Database::open(root.path().join("db"), store.clone()).unwrap();
    assert_eq!(normal.options(), OpenOptions::default());
    assert_eq!(
        all(&mut normal),
        vec![row("a", "original"), row("b", "base-b")]
    );
    let absent = root.path().join("not-created");
    assert!(
        Database::create_with_options(&absent, config(), store, BTreeMap::new(), options).is_err()
    );
    assert!(!absent.exists());
}

#[test]
fn open_options_reject_unknown_flags_and_non_boolean_values() {
    assert_eq!(
        serde_json::from_str::<OpenOptions>("{}").unwrap(),
        OpenOptions::default()
    );
    for value in [
        r#"{"parquet_pruning": "false"}"#,
        r#"{"read_only": 1}"#,
        r#"{"parquet_prunign": false}"#,
    ] {
        assert!(serde_json::from_str::<OpenOptions>(value).is_err());
    }
}

#[test]
fn base_pending_updates_deletes_and_joins_are_one_sql_view() {
    let (root, store, mut database) = fixture();
    assert_eq!(
        all(&mut database),
        vec![row("a", "original"), row("b", "base-b")]
    );
    database
        .execute("INSERT INTO items VALUES (?,?)", &row("c", "pending"))
        .unwrap();
    database
        .execute("UPDATE items SET value=? WHERE id=?", &row("new", "a"))
        .unwrap();
    database
        .execute("DELETE FROM items WHERE id=?", &[Cell::Text("b".into())])
        .unwrap();
    assert_eq!(
        all(&mut database),
        vec![row("a", "new"), row("c", "pending")]
    );
    let joined = database
        .query(
            "SELECT i.id,i.value,l.value FROM items i JOIN labels l ON i.id=l.id",
            &[],
        )
        .unwrap();
    assert_eq!(
        joined.rows,
        vec![vec![
            Cell::Text("a".into()),
            Cell::Text("new".into()),
            Cell::Text("label-a".into())
        ]]
    );
    drop(database);
    let mut reopened = Database::open(root.path().join("db"), store).unwrap();
    assert_eq!(
        all(&mut reopened),
        vec![row("a", "new"), row("c", "pending")]
    );
}

#[test]
fn predicates_see_updated_values_not_stale_base_values() {
    let (_root, _store, mut database) = fixture();
    database
        .execute("UPDATE items SET value='changed' WHERE id='a'", &[])
        .unwrap();
    assert!(
        database
            .query("SELECT id FROM items WHERE value='original'", &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        database
            .query("SELECT id FROM items WHERE value='changed'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Text("a".into())]]
    );
}

#[test]
fn typed_parquet_round_trip_is_not_a_json_blob() {
    let mut schema = table("typed");
    schema.columns.extend([
        Column {
            name: "count".into(),
            kind: ColumnType::Integer,
            nullable: false,
        },
        Column {
            name: "ratio".into(),
            kind: ColumnType::Real,
            nullable: false,
        },
        Column {
            name: "binary".into(),
            kind: ColumnType::Blob,
            nullable: false,
        },
        Column {
            name: "optional".into(),
            kind: ColumnType::Text,
            nullable: true,
        },
    ]);
    let value = vec![
        Cell::Text("key".into()),
        Cell::Text("Unicode \u{1f600}".into()),
        Cell::Integer(i64::MAX),
        Cell::Real(-1.25),
        Cell::Blob(vec![0, 255, 42]),
        Cell::Null,
    ];
    let changes = BTreeMap::from([
        (schema.key(&value), Some(value)),
        (b"tombstone".to_vec(), None),
    ]);
    let bytes = parquet::encode(&schema, &changes).unwrap();
    assert_eq!(&bytes[..4], b"PAR1");
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    assert_eq!(parquet::decode(&schema, bytes).unwrap(), changes);
}

#[test]
fn failed_statements_never_publish_partial_changes() {
    let (_root, _store, mut database) = fixture();
    assert!(
        database
            .execute(
                "INSERT INTO items VALUES ('x','one'),('x','duplicate')",
                &[]
            )
            .is_err()
    );
    assert_eq!(
        all(&mut database),
        vec![row("a", "original"), row("b", "base-b")]
    );
    assert!(
        database
            .execute("UPDATE items SET id='b' WHERE id='a'", &[])
            .is_err()
    );
    assert_eq!(all(&mut database).len(), 2);
}

#[test]
fn cross_partition_write_is_rejected_atomically() {
    let (_root, _store, mut database) = fixture();
    let first = Cell::Text("new-0".into());
    let partition = database.config().partition(&first).unwrap();
    let other = (1..100)
        .map(|i| Cell::Text(format!("new-{i}")))
        .find(|v| database.config().partition(v).unwrap() != partition)
        .unwrap();
    assert!(
        database
            .execute(
                "INSERT INTO items VALUES (?,'one'),(?,'two')",
                &[first, other]
            )
            .is_err()
    );
    assert_eq!(all(&mut database).len(), 2);
}

#[test]
fn compaction_preserves_rows_and_leaves_old_snapshots_readable() {
    let (_root, _store, mut database) = fixture();
    let partition = database
        .config()
        .partition(&Cell::Text("a".into()))
        .unwrap();
    let (head_before, _) = database
        .registry
        .cloud
        .get(
            &registry::head_key(database.config(), 0, partition),
            1024 * 1024,
        )
        .unwrap();
    let head_before: registry::Head = serde_json::from_slice(&head_before).unwrap();
    database
        .execute("UPDATE items SET value='changed' WHERE id='a'", &[])
        .unwrap();
    let expected = all(&mut database);
    let report = database.compact("items", partition).unwrap();
    assert!(report.published);
    assert_eq!(report.merged_files, 1);
    assert_eq!(all(&mut database), expected);
    let old = open_base(
        &database.registry.root,
        database.config(),
        0,
        partition,
        head_before.base.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(
        old.query_row("SELECT value FROM items WHERE id='a'", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "original"
    );
    assert_eq!(
        database.compact("items", partition).unwrap().merged_files,
        0
    );
}

#[test]
fn independent_concurrent_inserts_are_durable_and_unique() {
    let (root, store, database) = fixture();
    drop(database);
    let threads = (0..20)
        .map(|i| {
            let path = root.path().join("db");
            let store = store.clone();
            thread::spawn(move || {
                let mut database = Database::open(path, store).unwrap();
                database
                    .execute(
                        "INSERT INTO items VALUES (?,?)",
                        &row(&format!("new-{i}"), "value"),
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        assert_eq!(thread.join().unwrap().affected_rows, 1);
    }
    let mut database = Database::open(root.path().join("db"), store).unwrap();
    assert_eq!(all(&mut database).len(), 22);
    database.compact_all().unwrap();
    assert_eq!(all(&mut database).len(), 22);
}

#[test]
fn same_key_concurrency_has_exactly_one_winner() {
    let (root, store, database) = fixture();
    drop(database);
    let threads = (0..8)
        .map(|_| {
            let path = root.path().join("db");
            let store = store.clone();
            thread::spawn(move || {
                Database::open(path, store)
                    .unwrap()
                    .execute("INSERT INTO items VALUES ('same','value')", &[])
                    .is_ok()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
    let mut database = Database::open(root.path().join("db"), store).unwrap();
    assert_eq!(all(&mut database).len(), 3);
}

#[test]
fn requests_refresh_snapshots_and_no_sqlite_metadata_is_created() {
    let (root, store, mut first) = fixture();
    let mut second = Database::open(root.path().join("db"), store).unwrap();
    assert_eq!(all(&mut first).len(), 2);
    second
        .execute("INSERT INTO items VALUES ('next','seen')", &[])
        .unwrap();
    assert_eq!(all(&mut first).len(), 3);
    assert!(root.path().join("db/overlay.isam").is_file());
    assert!(!root.path().join("db/manifest.sqlite").exists());
    assert!(crate::core::Database::open(root.path().join("db"), 4).is_err());
}

#[test]
fn sql_cannot_escape_overlay_or_silently_begin_transactions() {
    let (_root, _store, mut database) = fixture();
    for sql in [
        "ATTACH ':memory:' AS escape",
        "BEGIN",
        "CREATE TABLE escape (id)",
        "PRAGMA writable_schema=ON",
        "DELETE FROM items; DELETE FROM labels",
    ] {
        assert!(database.execute(sql, &[]).is_err(), "{sql}");
    }
    assert!(
        database
            .query("SELECT load_extension('anything')", &[])
            .is_err()
    );
    assert!(
        database
            .query("DELETE FROM items RETURNING id", &[])
            .is_err()
    );
    assert_eq!(all(&mut database).len(), 2);
}
