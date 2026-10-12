use super::*;
use object_store::memory::InMemory;

fn request(id: &str, value: &str) -> UpdateRequest {
    UpdateRequest {
        operation_id: id.into(),
        table: "items".into(),
        key: BTreeMap::from([("id".into(), Cell::Text("a".into()))]),
        set: BTreeMap::from([("value".into(), Cell::Text(value.into()))]),
        increment: BTreeMap::new(),
        expected: BTreeMap::new(),
    }
}

fn fixture() -> (tempfile::TempDir, Arc<InMemory>, Database) {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let database = Database::create(
        root.path().join("db"),
        tests::config(),
        store.clone(),
        BTreeMap::from([("items".into(), vec![tests::row("a", "original")])]),
    )
    .unwrap();
    (root, store, database)
}

#[test]
fn prepared_point_rows_are_only_reused_for_the_same_unmodified_probe() {
    let (_root, _store, mut db) = fixture();
    let prepared = request(&nonce().unwrap(), "new")
        .prepare(db.config())
        .unwrap();
    db.registry.reset(false).unwrap();
    assert!(
        db.registry
            .begin_update(&prepared, true, false)
            .unwrap()
            .is_none()
    );
    let rows = db.registry.scan(0, &prepared.predicates).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        db.registry
            .state
            .lock()
            .unwrap()
            .stats
            .sqlite_base_cache_hits,
        0
    );
    assert!(
        db.registry
            .scan(1, &prepared.predicates)
            .unwrap()
            .is_empty()
    );
    assert!(
        db.registry
            .scan(0, &[(0, Cell::Text("absent".into()))])
            .unwrap()
            .is_empty()
    );
    assert!(
        db.registry
            .scan(0, &[(0, Cell::Integer(7))])
            .unwrap()
            .is_empty()
    );
    assert_eq!(db.registry.scan(0, &[]).unwrap().len(), 1);
    db.registry
        .update(0, rows[0].0, tests::row("a", "staged"))
        .unwrap();
    let updated = db.registry.scan(0, &prepared.predicates).unwrap();
    assert_eq!(updated[0].1, tests::row("a", "staged"));
    db.finish_statement().unwrap();
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "original")]
    );
}

#[test]
fn composite_point_updates_preserve_typed_keys_null_guards_and_fresh_attempts() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let mut config = tests::config();
    let table = &mut config.tables[0];
    table.columns[0].kind = ColumnType::Integer;
    table.columns[1].nullable = true;
    table.columns.extend([
        Column {
            name: "suffix".into(),
            kind: ColumnType::Blob,
            nullable: false,
        },
        Column {
            name: "counter".into(),
            kind: ColumnType::Integer,
            nullable: false,
        },
    ]);
    table.primary_key.push("suffix".into());
    let target = vec![
        Cell::Integer(7),
        Cell::Null,
        Cell::Blob(vec![0, 255]),
        Cell::Integer(0),
    ];
    let other = vec![
        Cell::Integer(7),
        Cell::Text("other".into()),
        Cell::Blob(vec![1]),
        Cell::Integer(0),
    ];
    let mut db = Database::create(
        root.path().join("db"),
        config,
        store,
        BTreeMap::from([("items".into(), vec![target, other.clone()])]),
    )
    .unwrap();
    let mut update = UpdateRequest {
        operation_id: nonce().unwrap(),
        table: "items".into(),
        key: BTreeMap::from([
            ("id".into(), Cell::Integer(7)),
            ("suffix".into(), Cell::Blob(vec![0, 255])),
        ]),
        set: BTreeMap::from([("value".into(), Cell::Text("new".into()))]),
        increment: BTreeMap::from([("counter".into(), Cell::Integer(1))]),
        expected: BTreeMap::from([("value".into(), Cell::Null)]),
    };
    let options = RetryOptions {
        timeout_ms: 10_000,
        ..Default::default()
    };
    assert_eq!(db.update(&update, options).unwrap().affected_rows, 1);
    assert_eq!(db.read_stats().sqlite_base_opens, 1);
    assert_eq!(db.read_stats().sqlite_base_cache_hits, 0);
    update.operation_id = nonce().unwrap();
    assert_eq!(db.update(&update, options).unwrap().affected_rows, 0);
    assert_eq!(db.read_stats().sqlite_base_cache_hits, 0);
    update.operation_id = nonce().unwrap();
    update
        .expected
        .insert("value".into(), Cell::Text("new".into()));
    assert_eq!(db.update(&update, options).unwrap().affected_rows, 1);
    assert_eq!(db.read_stats().sqlite_base_cache_hits, 0);
    assert_eq!(
        db.query("SELECT * FROM items ORDER BY suffix", &[])
            .unwrap()
            .rows,
        vec![
            vec![
                Cell::Integer(7),
                Cell::Text("new".into()),
                Cell::Blob(vec![0, 255]),
                Cell::Integer(2)
            ],
            other
        ]
    );
}

#[test]
fn point_update_commits_and_repeated_operation_returns_original_result() {
    let (_root, _store, mut db) = fixture();
    let update = request(&nonce().unwrap(), "new");
    let first = db.update(&update, RetryOptions::default()).unwrap();
    assert_eq!(first.status, "committed");
    assert_eq!(first.affected_rows, 1);
    assert!(!first.deduplicated);
    db.execute("UPDATE items SET value='later' WHERE id='a'", &[])
        .unwrap();
    let repeated = db.update(&update, RetryOptions::default()).unwrap();
    assert!(repeated.deduplicated);
    assert_eq!(first.commit_id, repeated.commit_id);
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Text("later".into())]]
    );
    assert_eq!(
        db.update_status(&update.operation_id, 1000)
            .unwrap()
            .unwrap()
            .commit_id,
        first.commit_id
    );
}

#[test]
fn reused_operation_id_with_different_intent_is_rejected() {
    let (_root, _store, mut db) = fixture();
    let mut update = request(&nonce().unwrap(), "new");
    db.update(&update, RetryOptions::default()).unwrap();
    update
        .set
        .insert("value".into(), Cell::Text("wrong".into()));
    assert_eq!(
        db.update(&update, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::IdempotencyConflict
    );
}

#[test]
fn expected_value_failure_is_terminal_even_when_condition_later_matches() {
    let (_root, _store, mut db) = fixture();
    let mut update = request(&nonce().unwrap(), "new");
    update
        .expected
        .insert("value".into(), Cell::Text("future".into()));
    let first = db.update(&update, RetryOptions::default()).unwrap();
    assert_eq!(first.status, "condition_not_met");
    assert_eq!(first.affected_rows, 0);
    db.execute("UPDATE items SET value='future' WHERE id='a'", &[])
        .unwrap();
    assert!(
        db.update(&update, RetryOptions::default())
            .unwrap()
            .deduplicated
    );
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Text("future".into())]]
    );
}

#[test]
fn receipts_survive_reopening_compaction_and_the_256_receipt_window() {
    let (root, store, mut db) = fixture();
    let update = request(&nonce().unwrap(), "first");
    let options = RetryOptions {
        timeout_ms: 30_000,
        allow_compaction: true,
        ..Default::default()
    };
    let first = db.update(&update, options).unwrap();
    for i in 0..260 {
        db.update(&request(&nonce().unwrap(), &format!("later-{i}")), options)
            .unwrap();
    }
    drop(db);
    let mut db = Database::open(root.path().join("db"), store).unwrap();
    let repeated = db.update(&update, options).unwrap();
    assert_eq!(repeated.commit_id, first.commit_id);
    assert!(repeated.deduplicated);
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Text("later-259".into())]]
    );
}

#[test]
fn invalid_updates_and_read_only_are_rejected_before_claims() {
    let (_root, _store, mut db) = fixture();
    let mut update = request(&nonce().unwrap(), "new");
    update.key.clear();
    assert_eq!(
        db.update(&update, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    update = request(&nonce().unwrap(), "new");
    db.apply_options(OpenOptions {
        read_only: true,
        ..Default::default()
    });
    assert_eq!(
        db.update(&update, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::ReadOnly
    );
    assert_eq!(
        db.update_target(&update).unwrap_err().kind(),
        EngineErrorKind::ReadOnly
    );
}

#[test]
fn simultaneous_duplicate_increments_apply_once() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let mut config = tests::config();
    config.tables[0].columns[1].kind = ColumnType::Integer;
    let path = root.path().join("db");
    let mut db = Database::create(
        &path,
        config,
        store.clone(),
        BTreeMap::from([(
            "items".into(),
            vec![vec![Cell::Text("a".into()), Cell::Integer(0)]],
        )]),
    )
    .unwrap();
    let mut update = request(&nonce().unwrap(), "unused");
    update.set.clear();
    update.increment.insert("value".into(), Cell::Integer(1));
    let workers = (0..16)
        .map(|_| {
            let path = path.clone();
            let store = store.clone();
            let update = update.clone();
            std::thread::spawn(move || {
                Database::open(path, store)
                    .unwrap()
                    .update(
                        &update,
                        RetryOptions {
                            timeout_ms: 30_000,
                            ..Default::default()
                        },
                    )
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let results = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<Vec<_>>();
    assert!(results.iter().all(|r| r.commit_id == results[0].commit_id));
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Integer(1)]]
    );
}

#[test]
fn distinct_concurrent_increments_preserve_every_successful_update() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let mut config = tests::config();
    config.tables[0].columns[1].kind = ColumnType::Integer;
    let path = root.path().join("db");
    let mut db = Database::create(
        &path,
        config,
        store.clone(),
        BTreeMap::from([(
            "items".into(),
            vec![vec![Cell::Text("a".into()), Cell::Integer(0)]],
        )]),
    )
    .unwrap();
    let workers = (0..12)
        .map(|_| {
            let path = path.clone();
            let store = store.clone();
            std::thread::spawn(move || {
                let mut update = request(&nonce().unwrap(), "unused");
                update.set.clear();
                update.increment.insert("value".into(), Cell::Integer(1));
                let mut db = Database::open(path, store).unwrap();
                let options = RetryOptions {
                    timeout_ms: 30_000,
                    max_retries: 32,
                    allow_compaction: true,
                    ..Default::default()
                };
                let result = db.update(&update, options).unwrap();
                assert_eq!(result.affected_rows, 1);
                assert_eq!(
                    db.update(&update, options).unwrap().commit_id,
                    result.commit_id
                );
                result.commit_id
            })
        })
        .collect::<Vec<_>>();
    let ids = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 12);
    assert_eq!(
        db.query("SELECT value FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![vec![Cell::Integer(12)]]
    );
}

#[test]
fn operation_identity_is_global_to_the_database_and_compaction_is_explicit() {
    let (_root, _store, mut db) = fixture();
    let update = request(&nonce().unwrap(), "once");
    db.update(&update, RetryOptions::default()).unwrap();
    let mut wrong_target = update.clone();
    wrong_target
        .key
        .insert("id".into(), Cell::Text("another-row".into()));
    assert_eq!(
        db.update(&wrong_target, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::IdempotencyConflict
    );
    for _ in 1..db.config().compact_after_files {
        db.update(&request(&nonce().unwrap(), "next"), RetryOptions::default())
            .unwrap();
    }
    let pending = request(&nonce().unwrap(), "after-compaction");
    assert_eq!(
        db.update(&pending, RetryOptions::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::Busy
    );
    assert!(
        db.update_status(&pending.operation_id, 1000)
            .unwrap()
            .is_none()
    );
    let worker = RetryOptions {
        allow_compaction: true,
        ..Default::default()
    };
    assert_eq!(db.update(&pending, worker).unwrap().affected_rows, 1);
}
