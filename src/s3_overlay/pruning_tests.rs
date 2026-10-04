use super::*;
use object_store::memory::InMemory;

fn fixture() -> (tempfile::TempDir, Arc<InMemory>, Database, Vec<String>, u16) {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(InMemory::new());
    let mut config = tests::config();
    config.max_pending_files = 128;
    config.compact_after_files = 128;
    let partition = config.partition(&Cell::Text("a".into())).unwrap();
    let keys = (0..10_000)
        .map(|i| format!("key-{i:05}"))
        .filter(|k| config.partition(&Cell::Text(k.clone())).unwrap() == partition)
        .take(31)
        .collect::<Vec<_>>();
    let mut db = Database::create(
        root.path().join("db"),
        config,
        store.clone(),
        BTreeMap::from([("items".into(), vec![tests::row("a", "base")])]),
    )
    .unwrap();
    for key in &keys {
        db.execute(
            "INSERT INTO items VALUES (?,?)",
            &tests::row(key, "pending"),
        )
        .unwrap();
    }
    db.execute("UPDATE items SET value='changed' WHERE id='a'", &[])
        .unwrap();
    (root, store, db, keys, partition)
}

#[test]
fn same_partition_index_skips_31_of_32_files_and_preserves_results() {
    let (_root, _store, mut db, _, _) = fixture();
    let fast = db.query("SELECT * FROM items WHERE id='a'", &[]).unwrap();
    assert_eq!(fast.rows, vec![tests::row("a", "changed")]);
    assert_eq!(db.read_stats().parquet_files_read, 1);
    assert_eq!(db.read_stats().parquet_files_skipped, 31);
    assert_eq!(db.read_stats().index_files_opened, 1);
    db.set_parquet_pruning(false);
    let slow = db.query("SELECT * FROM items WHERE id='a'", &[]).unwrap();
    assert_eq!(slow.rows, fast.rows);
    assert_eq!(db.read_stats().parquet_files_read, 32);
    assert_eq!(db.read_stats().parquet_files_skipped, 0);
}

#[test]
fn different_probes_and_full_scans_share_head_not_pruned_row_cache() {
    let (_root, _store, mut db, keys, _) = fixture();
    for sql in [
        "SELECT id,value FROM items WHERE id=? UNION ALL SELECT id,value FROM items WHERE id=?",
        "SELECT a.id,b.value FROM items a JOIN items b ON b.id=? WHERE a.id=?",
        "SELECT id,value FROM items WHERE id=? UNION ALL SELECT id,value FROM items",
    ] {
        let count = if sql.contains("WHERE id=? UNION ALL SELECT id,value FROM items WHERE")
            || sql.contains("JOIN")
        {
            2
        } else {
            1
        };
        let params = [Cell::Text(keys[1].clone()), Cell::Text(keys[2].clone())];
        db.set_parquet_pruning(true);
        let fast = db.query(sql, &params[..count]).unwrap().rows;
        db.set_parquet_pruning(false);
        assert_eq!(db.query(sql, &params[..count]).unwrap().rows, fast);
    }
}

#[test]
fn deletion_and_key_move_do_not_resurrect_base_or_pending_rows() {
    let (_root, _store, mut db, keys, _) = fixture();
    db.execute(
        "DELETE FROM items WHERE id=?",
        &[Cell::Text(keys[0].clone())],
    )
    .unwrap();
    db.execute("DELETE FROM items WHERE id='a'", &[]).unwrap();
    for key in [&keys[0], "a"] {
        assert!(
            db.query("SELECT * FROM items WHERE id=?", &[Cell::Text(key.into())])
                .unwrap()
                .rows
                .is_empty()
        );
    }
    db.execute(
        "UPDATE items SET id=? WHERE id=?",
        &[Cell::Text("a".into()), Cell::Text(keys[1].clone())],
    )
    .unwrap();
    assert!(
        db.query(
            "SELECT * FROM items WHERE id=?",
            &[Cell::Text(keys[1].clone())]
        )
        .unwrap()
        .rows
        .is_empty()
    );
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "pending")]
    );
    // A non-key filter must still read every newer key version.
    db.execute("UPDATE items SET value='gone' WHERE id='a'", &[])
        .unwrap();
    assert!(
        db.query("SELECT * FROM items WHERE id='a' AND value='pending'", &[])
            .unwrap()
            .rows
            .is_empty()
    );
}

#[test]
fn absent_or_corrupt_index_and_old_unindexed_deltas_fall_back_safely() {
    let (root, store, mut db, _, partition) = fixture();
    let path = file_index::path(&root.path().join("db"), db.config(), 0, partition);
    // Damage advisory summaries but leave the authoritative head/payload intact.
    let mut index = crate::isam::Store::open(&path).unwrap();
    let records = index
        .read_batch()
        .unwrap()
        .range(&[0u8; 32], None, 128)
        .unwrap();
    for record in records {
        index
            .write_batch(&[crate::isam::Mutation::put(record.key, b"invalid summary")])
            .unwrap();
    }
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "changed")]
    );
    assert_eq!(db.read_stats().parquet_files_read, 32);
    assert_eq!(db.read_stats().index_fallback_files, 32);
    drop(index);
    // Move, don't delete: simulate a missing advisory index on reopen.
    std::fs::rename(&path, path.with_extension("saved")).unwrap();
    drop(db);
    let mut db = Database::open(root.path().join("db"), store).unwrap();
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "changed")]
    );
    assert_eq!(db.read_stats().parquet_files_read, 32);
    std::fs::write(&path, b"not a valid ISAM file").unwrap();
    db.execute("UPDATE items SET value='index-failed' WHERE id='a'", &[])
        .unwrap();
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "index-failed")]
    );
    db.set_parquet_pruning(false);
    db.execute("UPDATE items SET value='unindexed' WHERE id='a'", &[])
        .unwrap();
    db.set_parquet_pruning(true);
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "unindexed")]
    );
}

#[test]
fn index_is_advisory_during_compaction_and_does_not_change_sqlite_coercions() {
    let (_root, _store, mut db, _, partition) = fixture();
    db.execute("INSERT INTO items VALUES ('123','numeric')", &[])
        .unwrap();
    for sql in [
        "SELECT * FROM items WHERE id=123",
        "SELECT * FROM items WHERE id='A' COLLATE NOCASE",
        "SELECT * FROM items WHERE id IN ('a','123')",
        "SELECT * FROM items WHERE id>'a' ORDER BY id",
    ] {
        let fast = db.query(sql, &[]).unwrap().rows;
        db.set_parquet_pruning(false);
        assert_eq!(db.query(sql, &[]).unwrap().rows, fast);
        db.set_parquet_pruning(true);
    }
    assert!(db.compact("items", partition).unwrap().published);
    assert_eq!(
        db.query("SELECT * FROM items WHERE id='a'", &[])
            .unwrap()
            .rows,
        vec![tests::row("a", "changed")]
    );
    assert_eq!(db.read_stats().parquet_files_read, 0);
    assert_eq!(db.read_stats().index_files_opened, 0);
}
