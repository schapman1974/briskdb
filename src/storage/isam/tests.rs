//! Model comparisons, corruption checks, and local process-failure boundaries.
use super::*;
use std::{
    collections::BTreeMap,
    os::unix::fs::{FileExt, symlink},
    process::Command,
    time::Duration,
};

fn open_fixture(key_bytes: u16, value_bytes: u16) -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::create(
        directory.path().join("records.isam"),
        Layout::new(key_bytes, value_bytes).unwrap(),
    )
    .unwrap();
    (directory, store)
}

#[test]
fn layout_and_mutation_bounds_are_checked_without_publishing() {
    for (key, value) in [(0, 1), (129, 1), (1, 1025)] {
        assert!(Layout::new(key, value).is_err());
    }
    let (_directory, mut store) = open_fixture(2, 4);
    for mutations in [
        vec![Mutation::insert(b"a", b"ok")],
        vec![Mutation::insert(b"ab", b"12345")],
        vec![Mutation::delete(b"abc")],
        vec![Mutation::put(b"ab", b"x"); MAX_BATCH_RECORDS + 1],
    ] {
        assert!(matches!(
            store.write_batch(&mutations),
            Err(Error::Invalid(_))
        ));
        assert_eq!(store.read_batch().unwrap().generation(), 1);
    }
    let read = store.read_batch().unwrap();
    assert!(read.get(b"a").is_err());
    assert!(read.range(b"zz", Some(b"aa"), 1).is_err());
    assert!(read.range(b"aa", Some(b"x"), 1).is_err());
    assert!(read.range(b"aa", None, MAX_BATCH_RECORDS + 1).is_err());
    assert!(read.range(b"aa", None, 0).unwrap().is_empty());
    assert_eq!(read.verify().unwrap(), 0);
}

#[test]
fn accepts_the_batch_limit_and_orders_binary_composite_keys_bytewise() {
    let (_directory, mut store) = open_fixture(4, 4);
    let keys = [
        [0x00, 0x01, 0x00, 0xff],
        [0x00, 0x01, 0xff, 0x00],
        [0x00, 0x02, 0x00, 0x00],
        [0xff, 0x00, 0x00, 0x00],
    ];
    let mut model = BTreeMap::new();
    let mutations: Vec<_> = keys
        .into_iter()
        .enumerate()
        .map(|(index, key)| {
            let value = [index as u8];
            model.insert(key.to_vec(), value.to_vec());
            Mutation::insert(key, value)
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    let read = store.read_batch().unwrap();
    let actual = read.range(&[0; 4], None, MAX_BATCH_RECORDS).unwrap();
    let expected: Vec<_> = model
        .into_iter()
        .map(|(key, value)| Record { key, value })
        .collect();
    assert_eq!(actual, expected);

    let (directory, mut bounded) = open_fixture(4, 4);
    let mutations: Vec<_> = (0..MAX_BATCH_RECORDS as u32)
        .map(|id| Mutation::insert(id.to_be_bytes(), id.to_le_bytes()))
        .collect();
    bounded.write_batch(&mutations).unwrap();
    let read = bounded.read_batch().unwrap();
    assert_eq!(read.verify().unwrap(), MAX_BATCH_RECORDS as u64);
    assert_eq!(
        read.range(&[0; 4], None, MAX_BATCH_RECORDS).unwrap().len(),
        MAX_BATCH_RECORDS
    );
    drop(bounded);
    let mut reopened = Store::open(directory.path().join("records.isam")).unwrap();
    assert_eq!(
        reopened.read_batch().unwrap().verify().unwrap(),
        MAX_BATCH_RECORDS as u64
    );
}

#[test]
fn persists_ordered_ranges_and_reopens_without_sqlite() {
    let (directory, mut store) = open_fixture(9, 768);
    let mutations: Vec<_> = (1..=120)
        .rev()
        .map(|i| {
            Mutation::insert(
                format!("JHN003{i:03}").as_bytes(),
                format!("verse {i}").as_bytes(),
            )
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    let path = directory.path().join("records.isam");
    drop(store);
    let mut reopened = Store::open(&path).unwrap();
    let read = reopened.read_batch().unwrap();
    assert_eq!(read.get(b"JHN003016").unwrap(), Some(b"verse 16".to_vec()));
    assert_eq!(read.get(b"JHN004000").unwrap(), None);
    let rows = read.range(b"JHN003014", Some(b"JHN003018"), 100).unwrap();
    assert_eq!(
        rows.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
        [b"JHN003014", b"JHN003015", b"JHN003016", b"JHN003017"]
    );
    assert_eq!(read.range(b"JHN003014", None, 2).unwrap().len(), 2);
    assert!(
        read.range(b"JHN003014", Some(b"JHN003014"), 3)
            .unwrap()
            .is_empty()
    );
    assert_eq!(read.verify().unwrap(), 120);
    let mut names: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names, ["records.isam", "records.isam.writer.lock"]);
}

#[test]
fn a_failed_batch_is_atomic_and_orphan_pages_are_reusable() {
    let (_directory, mut store) = open_fixture(2, 8);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    assert!(matches!(
        store.write_batch(&[
            Mutation::put(b"aa", b"new"),
            Mutation::insert(b"bb", b"new"),
            Mutation::insert(b"aa", b"dup")
        ]),
        Err(Error::Duplicate)
    ));
    {
        let read = store.read_batch().unwrap();
        assert_eq!(read.generation(), 2);
        assert_eq!(read.get(b"aa").unwrap(), Some(b"old".to_vec()));
        assert_eq!(read.get(b"bb").unwrap(), None);
    }
    store
        .write_batch(&[
            Mutation::delete(b"aa"),
            Mutation::insert(b"aa", b"again"),
            Mutation::delete(b"zz"),
        ])
        .unwrap();
    assert_eq!(
        store.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"again".to_vec())
    );
    store.write_batch(&[Mutation::delete(b"aa")]).unwrap();
    assert_eq!(store.read_batch().unwrap().verify().unwrap(), 0);
}

#[test]
fn snapshots_do_not_block_writes_and_reused_handles_refresh() {
    let (directory, mut writer) = open_fixture(2, 8);
    writer
        .write_batch(&[Mutation::insert(b"aa", b"before")])
        .unwrap();
    let mut reader = Store::open_read_only(directory.path().join("records.isam")).unwrap();
    let read = reader.read_batch().unwrap();
    // The read scope deliberately stays live across two writer commits.
    writer
        .write_batch(&[
            Mutation::put(b"aa", b"after"),
            Mutation::insert(b"bb", b"new"),
        ])
        .unwrap();
    writer.write_batch(&[Mutation::delete(b"aa")]).unwrap();
    assert_eq!(read.get(b"aa").unwrap(), Some(b"before".to_vec()));
    assert_eq!(read.get(b"bb").unwrap(), None);
    let fresh = reader.read_batch().unwrap();
    assert_eq!(fresh.get(b"aa").unwrap(), None);
    assert_eq!(fresh.get(b"bb").unwrap(), Some(b"new".to_vec()));
    assert!(matches!(
        reader.write_batch(&[Mutation::delete(b"bb")]),
        Err(Error::ReadOnly)
    ));
}

#[test]
fn one_read_admission_covers_many_records_and_counts_logical_io() {
    let (_directory, mut store) = open_fixture(9, 32);
    let mutations: Vec<_> = (0..36_u16)
        .map(|verse| {
            Mutation::insert(
                format!("JHN003{verse:03}").into_bytes(),
                format!("verse {verse}").into_bytes(),
            )
        })
        .collect();
    store.write_batch(&mutations).unwrap();
    store.reset_operation_stats();

    let batch = store.read_batch().unwrap();
    let verses = batch.range(b"JHN003000", Some(b"JHN004000"), 36).unwrap();
    assert_eq!(verses.len(), 36);
    drop(batch);

    let stats = store.operation_stats();
    assert_eq!(stats.file_opens, 0);
    assert_eq!(stats.root_reads, 1);
    assert!(stats.root_read_ns > 0);
    assert_eq!(stats.lock_requests, 1);
    assert_eq!(stats.lock_retries, 0);
    assert!(stats.page_reads < 36);
    assert!(stats.page_read_ns > 0);
}

#[test]
fn operation_stats_handle_observes_close_after_store_drop() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"val")])
        .unwrap();
    drop(store);
    let store = Store::open(directory.path().join("records.isam")).unwrap();
    let stats = store.operation_stats_handle();
    assert_eq!(stats.snapshot().file_opens, 2);
    assert_eq!(stats.snapshot().file_stats, 3);
    assert!(stats.snapshot().root_read_ns > 0);
    assert!(stats.snapshot().page_read_ns > 0);
    assert_eq!(stats.snapshot().file_closes, 0);
    drop(store);
    assert_eq!(stats.snapshot().file_closes, 2);
}

#[test]
fn lock_wait_and_retries_are_included_in_operation_stats() {
    let (directory, store) = open_fixture(2, 4);
    let mut peer = Store::open_with_policy(
        directory.path().join("records.isam"),
        LockPolicy::new(Duration::from_millis(15), Duration::from_millis(2)).unwrap(),
    )
    .unwrap();
    let lock = Guard::acquire(&store.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(matches!(
        peer.write_batch(&[Mutation::insert(b"aa", b"val")]),
        Err(Error::Busy)
    ));
    drop(lock);

    let stats = peer.operation_stats();
    assert!(stats.lock_requests > 0);
    assert!(stats.lock_retries > 0);
    assert!(stats.lock_wait_ns > 0);
    peer.write_batch(&[Mutation::insert(b"aa", b"val")])
        .unwrap();
    let stats = peer.operation_stats();
    assert!(stats.root_write_ns > 0);
    assert!(stats.page_write_ns > 0);
    assert!(stats.sync_ns > 0);
    assert!(stats.publication_ns > 0);
}

#[test]
fn readers_and_independent_file_writers_progress_during_write_preparation() {
    let (directory, mut writer) = open_fixture(2, 8);
    writer
        .write_batch(&[Mutation::insert(b"aa", b"before")])
        .unwrap();
    let mut reader = Store::open_read_only(directory.path().join("records.isam")).unwrap();
    let (_other_directory, mut independent) = open_fixture(2, 8);
    let mut overlapped = false;
    writer
        .write_with_hook(&[Mutation::put(b"aa", b"after")], |point| {
            if point == CommitPoint::PagesWritten {
                assert_eq!(
                    reader.read_batch().unwrap().get(b"aa").unwrap(),
                    Some(b"before".to_vec())
                );
                independent
                    .write_batch(&[Mutation::insert(b"bb", b"other")])
                    .unwrap();
                overlapped = true;
            } else if point == CommitPoint::RootWritten {
                // Only this publication/sync interval excludes new read snapshots.
                assert!(matches!(reader.read_batch(), Err(Error::Busy)));
            }
            Ok(())
        })
        .unwrap();
    assert!(overlapped);
    assert_eq!(
        reader.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"after".to_vec())
    );
    assert_eq!(
        independent.read_batch().unwrap().get(b"bb").unwrap(),
        Some(b"other".to_vec())
    );
}

#[test]
fn interrupted_alternate_root_write_preserves_the_last_published_generation() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    let committed = read_snapshot(&store.file, None).unwrap();
    // Damage only the inactive slot as an interrupted next-generation write
    // would do. The previously acknowledged root must remain usable.
    let inactive = ((committed.generation + 1) % 2) * format::PAGE_BYTES as u64;
    store.file.write_all_at(&[0xff; 123], inactive).unwrap();
    drop(store);
    let mut reopened = Store::open(directory.path().join("records.isam")).unwrap();
    assert_eq!(
        reopened.read_batch().unwrap().generation(),
        committed.generation
    );
    assert_eq!(
        reopened.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"old".to_vec())
    );
    reopened
        .write_batch(&[Mutation::put(b"aa", b"new")])
        .unwrap();
    assert_eq!(
        reopened.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"new".to_vec())
    );
}

fn wide_key(value: u64) -> Vec<u8> {
    let mut key = vec![0; 128];
    key[..8].copy_from_slice(&value.to_be_bytes());
    key
}

#[test]
fn multilevel_splits_updates_and_deletes_match_an_independent_map() {
    let (directory, mut store) = open_fixture(128, 1024);
    let mut model = BTreeMap::new();
    let initial: Vec<_> = (0..600_u64)
        .rev()
        .map(|i| {
            model.insert(wide_key(i), i.to_le_bytes().to_vec());
            Mutation::insert(wide_key(i), i.to_le_bytes())
        })
        .collect();
    store.write_batch(&initial).unwrap();
    let initial_snapshot = read_snapshot(&store.file, None).unwrap();
    // Small leaf/branch fanout forces both kinds of split in this fixture.
    let mut root_level = [0];
    store
        .file
        .read_exact_at(&mut root_level, initial_snapshot.root + 24)
        .unwrap();
    assert!(root_level[0] >= 2);
    // Bulk construction must coalesce writes, not emit 600 copied root paths.
    assert!(initial_snapshot.end < 230 * format::PAGE_BYTES as u64);
    let mut seed = 19_u64;
    for batch in 0..30 {
        let mut operations = Vec::new();
        for _ in 0..25 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let key = wide_key(seed % 800);
            if seed % 3 == 0 {
                model.remove(&key);
                operations.push(Mutation::delete(key));
            } else {
                let value = seed.to_le_bytes().to_vec();
                model.insert(key.clone(), value.clone());
                operations.push(Mutation::put(key, value));
            }
        }
        store.write_batch(&operations).unwrap();
        let read = store.read_batch().unwrap();
        assert_eq!(read.verify().unwrap(), model.len() as u64, "batch {batch}");
        let actual = read.range(&wide_key(0), None, MAX_BATCH_RECORDS).unwrap();
        let expected: Vec<_> = model
            .iter()
            .map(|(k, v)| Record {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();
        assert_eq!(actual, expected, "batch {batch}");
        for (key, value) in &model {
            assert_eq!(read.get(key).unwrap().as_ref(), Some(value));
        }
    }
    let deletes: Vec<_> = model.keys().cloned().map(Mutation::delete).collect();
    store.write_batch(&deletes).unwrap();
    assert_eq!(store.read_batch().unwrap().verify().unwrap(), 0);
    drop(store);
    let mut reopened = Store::open(directory.path().join("records.isam")).unwrap();
    reopened
        .write_batch(&[Mutation::insert(wide_key(9999), b"reborn")])
        .unwrap();
    assert_eq!(reopened.read_batch().unwrap().verify().unwrap(), 1);
}

#[test]
fn shared_publication_locks_coexist_and_writer_lock_is_separate() {
    let (directory, writer) = open_fixture(2, 4);
    let mut reader = Store::open(directory.path().join("records.isam")).unwrap();
    let first = Guard::acquire(&writer.file, false, LockPolicy::default()).unwrap();
    let second = Guard::acquire(&reader.file, false, LockPolicy::default()).unwrap();
    assert!(matches!(
        Guard::acquire(&reader.file, true, LockPolicy::default()),
        Err(Error::Busy)
    ));
    drop(second);
    drop(first);
    let preparing = Guard::acquire(&writer.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(
        reader.read_batch().is_ok(),
        "writer preparation must not exclude readers"
    );
    assert!(matches!(
        reader.write_batch(&[Mutation::insert(b"aa", b"x")]),
        Err(Error::Busy)
    ));
    drop(preparing);
    reader
        .write_batch(&[Mutation::insert(b"aa", b"x")])
        .unwrap();
}

#[test]
fn lock_retries_are_bounded_and_can_succeed_after_release() {
    assert!(LockPolicy::new(Duration::from_secs(301), Duration::from_millis(1)).is_err());
    assert!(LockPolicy::new(Duration::ZERO, Duration::ZERO).is_err());
    let (directory, writer) = open_fixture(2, 4);
    let path = directory.path().join("records.isam");
    let mut peer = Store::open(&path).unwrap();
    peer.set_lock_policy(
        LockPolicy::new(Duration::from_millis(15), Duration::from_millis(2)).unwrap(),
    );
    let lock = Guard::acquire(&writer.writer_lock, true, LockPolicy::default()).unwrap();
    assert!(matches!(
        peer.write_batch(&[Mutation::insert(b"aa", b"x")]),
        Err(Error::Busy)
    ));
    drop(lock);
    peer.write_batch(&[Mutation::insert(b"aa", b"x")]).unwrap();
}

#[test]
fn rejects_foreign_truncated_symlinked_and_aliased_files_without_changing_them() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreign");
    std::fs::write(&path, b"SQLite format 3\0not our data").unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(Store::create(&path, Layout::new(2, 4).unwrap()).is_err());
    assert!(Store::open(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!writer_lock_path(&path).unwrap().exists());
    let link = directory.path().join("symlink");
    symlink(&path, &link).unwrap();
    assert!(Store::open(&link).is_err());
    let (_good_directory, store) = open_fixture(2, 4);
    store.file.set_len(10).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
    let (directory, _store) = open_fixture(2, 4);
    std::fs::hard_link(
        directory.path().join("records.isam"),
        directory.path().join("alias"),
    )
    .unwrap();
    assert!(Store::open(directory.path().join("records.isam")).is_err());
}

#[test]
fn rejects_group_or_world_access_on_data_and_lock_files() {
    for lock_file in [false, true] {
        let (directory, store) = open_fixture(2, 4);
        let path = directory.path().join(if lock_file {
            "records.isam.writer.lock"
        } else {
            "records.isam"
        });
        drop(store);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(matches!(
            Store::open(directory.path().join("records.isam")),
            Err(Error::Invalid(_))
        ));
    }
}

#[test]
fn rejects_checksummed_unknown_format_versions_and_reserved_header_bytes() {
    for version in [0_u16, 2_u16] {
        let (_directory, store) = open_fixture(2, 4);
        let snapshot = read_snapshot(&store.file, None).unwrap();
        let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
        let mut header = [0; format::PAGE_BYTES];
        store.file.read_exact_at(&mut header, slot).unwrap();
        header[8..10].copy_from_slice(&version.to_le_bytes());
        format::seal(&mut header);
        store.file.write_all_at(&header, slot).unwrap();
        assert!(matches!(
            read_snapshot(&store.file, None),
            Err(Error::Corrupt(_))
        ));
    }

    let (_directory, store) = open_fixture(2, 4);
    let snapshot = read_snapshot(&store.file, None).unwrap();
    let slot = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    let mut header = [0; format::PAGE_BYTES];
    store.file.read_exact_at(&mut header, slot).unwrap();
    header[40] = 1;
    format::seal(&mut header);
    store.file.write_all_at(&header, slot).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn failed_initialization_leaves_an_incomplete_file_that_open_does_not_adopt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("incomplete.isam");
    std::fs::write(directory.path().join("incomplete.isam.writer.lock"), []).unwrap();
    std::fs::set_permissions(
        directory.path().join("incomplete.isam.writer.lock"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert!(Store::create(&path, Layout::new(2, 4).unwrap()).is_err());
    assert!(path.exists());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert!(matches!(Store::open(&path), Err(Error::Corrupt(_))));
}

#[test]
fn refuses_corrupt_pages_instead_of_falling_back_to_old_committed_data() {
    let (directory, mut store) = open_fixture(2, 4);
    store
        .write_batch(&[Mutation::insert(b"aa", b"old")])
        .unwrap();
    store.write_batch(&[Mutation::put(b"aa", b"new")]).unwrap();
    let snapshot = read_snapshot(&store.file, None).unwrap();
    store
        .file
        .write_all_at(&[0xff], snapshot.root + 50)
        .unwrap();
    assert!(matches!(
        store.read_batch().unwrap().get(b"aa"),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(
        Store::open(directory.path().join("records.isam")),
        Err(Error::Corrupt(_))
    ));
    store
        .file
        .write_all_at(&[0xff; format::HEADER_BYTES as usize], 0)
        .unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn rejects_checksummed_invalid_root_bounds_and_page_cycles() {
    let (_directory, mut store) = open_fixture(128, 1024);
    let operations: Vec<_> = (0..30)
        .map(|i| Mutation::insert(wide_key(i), b"value"))
        .collect();
    store.write_batch(&operations).unwrap();
    let snapshot = read_snapshot(&store.file, None).unwrap();
    let mut page = [0; format::PAGE_BYTES];
    store.file.read_exact_at(&mut page, snapshot.root).unwrap();
    assert!(page[24] > 0);
    page[40 + 128..40 + 128 + 8].copy_from_slice(&snapshot.root.to_le_bytes());
    format::seal(&mut page);
    store.file.write_all_at(&page, snapshot.root).unwrap();
    assert!(matches!(
        store.read_batch().unwrap().verify(),
        Err(Error::Corrupt(_))
    ));
    let header_offset = snapshot.generation % 2 * format::PAGE_BYTES as u64;
    store.file.read_exact_at(&mut page, header_offset).unwrap();
    page[32..40].copy_from_slice(&1_u64.to_le_bytes());
    format::seal(&mut page);
    store.file.write_all_at(&page, header_offset).unwrap();
    assert!(matches!(
        read_snapshot(&store.file, None),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn publication_failures_distinguish_uncommitted_and_unknown_outcomes() {
    for point in [
        CommitPoint::PagesWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let (_directory, mut store) = open_fixture(2, 4);
        let result = store.write_with_hook(&[Mutation::insert(b"aa", b"new")], |at| {
            if at == point {
                Err(io::Error::other("injected I/O failure"))
            } else {
                Ok(())
            }
        });
        if point == CommitPoint::RootWritten {
            assert!(matches!(result, Err(Error::CommitUnknown(_))));
            assert_eq!(
                store.read_batch().unwrap().get(b"aa").unwrap(),
                Some(b"new".to_vec())
            );
        } else {
            assert!(matches!(result, Err(Error::Io(_))));
            assert_eq!(store.read_batch().unwrap().get(b"aa").unwrap(), None);
        }
    }
}

#[test]
fn crash_worker() {
    let Some(path) = std::env::var_os("BRISK_ISAM_CRASH_PATH") else {
        return;
    };
    let point = std::env::var("BRISK_ISAM_CRASH_POINT").unwrap();
    let mut store = Store::open(path).unwrap();
    store
        .write_with_hook(
            &[
                Mutation::put(b"aa", b"new"),
                Mutation::insert(b"bb", b"new"),
            ],
            |at| {
                if format!("{at:?}") == point {
                    std::process::exit(86);
                }
                Ok(())
            },
        )
        .unwrap();
    panic!("crash boundary not reached");
}

#[test]
fn process_exit_releases_locks_and_preserves_commit_boundary() {
    for point in [
        CommitPoint::PagesWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let (directory, mut parent) = open_fixture(2, 4);
        parent
            .write_batch(&[Mutation::insert(b"aa", b"old")])
            .unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storage::isam::tests::crash_worker"])
            .env(
                "BRISK_ISAM_CRASH_PATH",
                directory.path().join("records.isam"),
            )
            .env("BRISK_ISAM_CRASH_POINT", format!("{point:?}"))
            .output()
            .unwrap()
            .status;
        assert_eq!(status.code(), Some(86));
        // Process exit is not power loss: a written, unsynced root is visible
        // in the live OS cache. Do not infer fsync durability from this test.
        let published = point == CommitPoint::RootWritten;
        let read = parent.read_batch().unwrap();
        assert_eq!(
            read.get(b"aa").unwrap(),
            Some(if published {
                b"new".to_vec()
            } else {
                b"old".to_vec()
            })
        );
        assert_eq!(read.get(b"bb").unwrap().is_some(), published);
        parent.write_batch(&[Mutation::put(b"cc", b"ok")]).unwrap();
    }
}

#[test]
fn inherited_handle_and_snapshot_operations_are_rejected() {
    let (_directory, mut store) = open_fixture(2, 4);
    store.owner_pid = store.owner_pid.wrapping_add(1);
    assert!(matches!(store.read_batch(), Err(Error::WrongProcess)));
    assert!(matches!(store.write_batch(&[]), Err(Error::WrongProcess)));
    store.owner_pid = std::process::id();
    let mut read = store.read_batch().unwrap();
    read.owner_pid = read.owner_pid.wrapping_add(1);
    assert!(matches!(read.get(b"aa"), Err(Error::WrongProcess)));
    assert!(matches!(
        read.range(b"aa", None, 1),
        Err(Error::WrongProcess)
    ));
    assert!(matches!(read.verify(), Err(Error::WrongProcess)));
}

#[test]
fn empty_root_encoding_has_stable_fields_and_padding() {
    let (_directory, store) = open_fixture(9, 768);
    let mut bytes = [0; format::PAGE_BYTES];
    store
        .file
        .read_exact_at(&mut bytes, format::PAGE_BYTES as u64)
        .unwrap();
    assert_eq!(&bytes[..16], b"BRISAM01\x01\x00\x09\x00\x00\x03\x00\x00");
    assert_eq!(format::u64_at(&bytes, 16), 1);
    assert_eq!(format::u64_at(&bytes, 24), 0);
    assert_eq!(format::u64_at(&bytes, 32), 8192);
    assert!(bytes[40..format::CHECKSUM_START].iter().all(|b| *b == 0));
    assert!(format::checksum_valid(&bytes));
}
