use super::*;
use object_store::memory::InMemory;

fn fixture() -> (tempfile::TempDir, Database) {
    let root = tempfile::tempdir().unwrap();
    let mut config = tests::config();
    config.compact_after_files = 128;
    config.max_pending_files = 128;
    let database = Database::create(
        root.path().join("db"),
        config,
        Arc::new(InMemory::new()),
        BTreeMap::from([("items".into(), vec![tests::row("a", "base")])]),
    )
    .unwrap();
    (root, database)
}

#[test]
fn newest_point_reads_one_payload_and_no_base_with_or_without_indexes() {
    for pruning in [false, true] {
        let (_root, mut database) = fixture();
        database.set_parquet_pruning(pruning);
        for value in ["first", "second", "third", "latest"] {
            database
                .execute(
                    "UPDATE items SET value=? WHERE id='a'",
                    &[Cell::Text(value.into())],
                )
                .unwrap();
        }
        assert_eq!(
            database
                .query("SELECT * FROM items WHERE id='a'", &[])
                .unwrap()
                .rows,
            vec![tests::row("a", "latest")]
        );
        let stats = database.read_stats();
        assert_eq!(stats.heads_read, 1);
        assert_eq!(stats.parquet_files_read, 1);
        assert_eq!(stats.parquet_files_skipped, 3);
        assert_eq!(stats.sqlite_base_opens + stats.sqlite_base_cache_hits, 0);
        assert!(
            database
                .query("SELECT * FROM items WHERE id='a' AND value='base'", &[])
                .unwrap()
                .rows
                .is_empty()
        );
        database
            .execute("DELETE FROM items WHERE id='a'", &[])
            .unwrap();
        assert!(
            database
                .query("SELECT * FROM items WHERE id='a'", &[])
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(database.read_stats().parquet_files_read, 1);
        assert_eq!(database.read_stats().sqlite_base_cache_hits, 0);
        database
            .execute("INSERT INTO items VALUES ('a','reborn')", &[])
            .unwrap();
        assert_eq!(
            database
                .query("SELECT * FROM items WHERE id='a'", &[])
                .unwrap()
                .rows,
            vec![tests::row("a", "reborn")]
        );
    }
}

#[test]
fn newest_point_does_not_read_superseded_corruption_but_checks_authoritative_payload() {
    let (_root, mut database) = fixture();
    database.set_parquet_pruning(false);
    for value in ["old", "new"] {
        database
            .execute(
                "UPDATE items SET value=? WHERE id='a'",
                &[Cell::Text(value.into())],
            )
            .unwrap();
    }
    let partition = database
        .config()
        .partition(&Cell::Text("a".into()))
        .unwrap();
    let head_key = registry::head_key(database.config(), 0, partition);
    let (bytes, _) = database.registry.cloud.get(&head_key, 1024 * 1024).unwrap();
    let head: registry::Head = serde_json::from_slice(&bytes).unwrap();
    for (index, delta) in head.deltas.iter().enumerate() {
        let key = format!(
            "{}/tables/0000/partitions/{partition:04}/deltas/{}.parquet",
            database.config().namespace(),
            delta.id
        );
        database
            .registry
            .cloud
            .put(
                &key,
                bytes::Bytes::from_static(b"damaged"),
                object_store::PutMode::Overwrite,
            )
            .unwrap();
        let result = database.query("SELECT * FROM items WHERE id='a'", &[]);
        if index == 0 {
            assert_eq!(result.unwrap().rows, vec![tests::row("a", "new")]);
            assert!(database.query("SELECT * FROM items", &[]).is_err());
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn newest_point_requires_every_composite_key_with_exact_types() {
    let root = tempfile::tempdir().unwrap();
    let mut config = tests::config();
    config.tables[0].columns[0].kind = ColumnType::Integer;
    config.tables[0].columns.push(Column {
        name: "suffix".into(),
        kind: ColumnType::Blob,
        nullable: false,
    });
    config.tables[0].primary_key = vec!["suffix".into(), "id".into()];
    let target = vec![
        Cell::Integer(7),
        Cell::Text("base".into()),
        Cell::Blob(vec![0]),
    ];
    let other = vec![
        Cell::Integer(7),
        Cell::Text("other".into()),
        Cell::Blob(vec![1]),
    ];
    let mut database = Database::create(
        root.path().join("db"),
        config,
        Arc::new(InMemory::new()),
        BTreeMap::from([("items".into(), vec![target, other.clone()])]),
    )
    .unwrap();
    database.set_parquet_pruning(false);
    for value in ["old", "new"] {
        database
            .execute(
                "UPDATE items SET value=? WHERE id=7 AND suffix=x'00'",
                &[Cell::Text(value.into())],
            )
            .unwrap();
    }
    let expected = vec![vec![
        Cell::Integer(7),
        Cell::Text("new".into()),
        Cell::Blob(vec![0]),
    ]];
    assert_eq!(
        database
            .query("SELECT * FROM items WHERE id=7 AND suffix=x'00'", &[])
            .unwrap()
            .rows,
        expected
    );
    assert_eq!(database.read_stats().parquet_files_read, 1);
    assert_eq!(database.read_stats().sqlite_base_cache_hits, 0);
    for key in ["'7'", "7.0"] {
        assert_eq!(
            database
                .query(
                    &format!("SELECT * FROM items WHERE id={key} AND suffix=x'00'"),
                    &[]
                )
                .unwrap()
                .rows,
            expected
        );
        assert_eq!(database.read_stats().parquet_files_read, 2);
    }
    let mut both = expected;
    both.push(other);
    assert_eq!(
        database
            .query("SELECT * FROM items WHERE id=7 ORDER BY suffix", &[])
            .unwrap()
            .rows,
        both
    );
    assert_eq!(database.read_stats().parquet_files_read, 2);
    assert!(
        database
            .query("SELECT * FROM items WHERE id=NULL AND suffix=x'00'", &[])
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn newest_point_probe_never_marks_a_partial_snapshot_complete() {
    let (_root, mut database) = fixture();
    database
        .execute("UPDATE items SET value='new' WHERE id='a'", &[])
        .unwrap();
    let partition = database
        .config()
        .partition(&Cell::Text("a".into()))
        .unwrap();
    let other = (0..100)
        .map(|number| format!("other-{number}"))
        .find(|key| {
            database
                .config()
                .partition(&Cell::Text(key.clone()))
                .unwrap()
                == partition
        })
        .unwrap();
    database
        .execute(
            "INSERT INTO items VALUES (?,?)",
            &tests::row(&other, "other"),
        )
        .unwrap();
    for pruning in [false, true] {
        database.set_parquet_pruning(pruning);
        let rows = database
            .query(
                "SELECT * FROM items WHERE id='a' UNION ALL SELECT * FROM items ORDER BY id,value",
                &[],
            )
            .unwrap()
            .rows;
        let mut expected = vec![
            tests::row("a", "new"),
            tests::row("a", "new"),
            tests::row(&other, "other"),
        ];
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
        assert_eq!(rows, expected);
        assert_eq!(
            database.read_stats().heads_read,
            u64::from(database.config().partitions)
        );
    }
}
