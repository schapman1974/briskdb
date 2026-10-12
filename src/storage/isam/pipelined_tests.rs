//! Deterministic overlap, prefix ordering, and fail-closed recovery tests for v4.
//! Process exits and injected I/O errors do not simulate power loss or NFS.
use super::*;
use std::{
    collections::BTreeMap,
    os::unix::fs::FileExt,
    process::Command,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::Duration,
};

fn policy() -> LockPolicy {
    LockPolicy::new(Duration::from_secs(10), Duration::from_millis(1)).unwrap()
}

fn fixture() -> (tempfile::TempDir, Store, Store) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pipeline.isam");
    let mut a = Store::create_pipelined(&path, Layout::new(2, 128).unwrap()).unwrap();
    let b = Store::open_with_policy(&path, policy()).unwrap();
    a.set_lock_policy(policy());
    (directory, a, b)
}

fn disjoint_key() -> Vec<u8> {
    (0..=255_u8)
        .map(|n| vec![b'b', n])
        .find(|key| locking::key_lock_stripe(key) != locking::key_lock_stripe(b"aa"))
        .unwrap()
}

#[test]
fn staging_fences_the_data_inode_but_releases_it_before_durable_sync() {
    let (directory, mut writer, peer) = fixture();
    let mut reader = Store::open_read_only(directory.path().join("pipeline.isam")).unwrap();
    let old = reader.read_batch().unwrap();
    let mut fresh = Store::open_read_only(directory.path().join("pipeline.isam")).unwrap();
    writer
        .write_with_hook(&[Mutation::insert(b"aa", b"first")], |point| {
            let fence = Guard::acquire(&peer.file, true, LockPolicy::default());
            if matches!(point, CommitPoint::PlanPrepared | CommitPoint::PagesWritten) {
                // The sidecar alone cannot make another NFS client's cached
                // data fresh. Stage under the actual data inode's fence.
                assert!(matches!(fence, Err(Error::Busy)));
                // Staging changes only unpublished bytes: do not exclude
                // readers acquiring a new published snapshot on another handle.
                drop(Guard::acquire(&peer.file, false, LockPolicy::default()).unwrap());
                assert_eq!(fresh.read_batch().unwrap().verify().unwrap(), 0);
            } else {
                // Neither explicit durability sync retains this fence.
                drop(fence.unwrap());
            }
            assert_eq!(old.verify().unwrap(), 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(writer.read_batch().unwrap().verify().unwrap(), 1);
}

#[test]
fn retained_peer_rejects_a_duplicate_after_serial_acknowledged_insert() {
    let (directory, mut first, mut retained) = fixture();
    first
        .write_batch(&[Mutation::insert(b"aa", b"first")])
        .unwrap();
    assert!(matches!(
        retained.write_batch(&[Mutation::insert(b"aa", b"second")]),
        Err(Error::Duplicate)
    ));
    let mut reopened = Store::open(directory.path().join("pipeline.isam")).unwrap();
    assert!(matches!(
        reopened.write_batch(&[Mutation::insert(b"aa", b"third")]),
        Err(Error::Duplicate)
    ));
    assert_eq!(
        reopened.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"first".to_vec())
    );
}

#[test]
fn independent_writer_finishes_while_first_writer_is_paused_before_publication() {
    let (_dir, mut a, mut b) = fixture();
    let key = disjoint_key();
    let mut old = Store::open_read_only(_dir.path().join("pipeline.isam")).unwrap();
    let old = old.read_batch().unwrap();
    a.reset_operation_stats();
    b.reset_operation_stats();
    a.write_with_hook(&[Mutation::insert(b"aa", b"first")], |point| {
        if point == CommitPoint::PagesSynced {
            // A owns its key locks but neither shared commit/publication gate.
            b.write_batch(&[Mutation::insert(key.as_slice(), b"second")])
                .unwrap();
            let read = b.read_batch().unwrap();
            assert_eq!(read.get(b"aa").unwrap(), Some(b"first".to_vec()));
            assert_eq!(read.get(&key).unwrap(), Some(b"second".to_vec()));
        }
        Ok(())
    })
    .unwrap();
    let read = a.read_batch().unwrap();
    assert_eq!(read.generation(), 3);
    assert_eq!(read.verify().unwrap(), 2);
    assert_eq!(old.verify().unwrap(), 0);
    assert_eq!(a.operation_stats().syncs, 2);
    assert_eq!(b.operation_stats().syncs, 2);
    let latest = read_snapshot(&a.file, None).unwrap();
    assert_eq!((latest.generation, latest.publication), (3, 3));
    drop(a);
    drop(b);
    assert_eq!(
        Store::open(_dir.path().join("pipeline.isam"))
            .unwrap()
            .read_batch()
            .unwrap()
            .verify()
            .unwrap(),
        2
    );
}

#[test]
fn neither_sync_holds_the_global_gate_or_blocks_snapshot_readers() {
    for pause in [CommitPoint::WorkingRootWritten, CommitPoint::RootWritten] {
        let (dir, mut a, b) = fixture();
        let mut reader = Store::open_read_only(dir.path().join("pipeline.isam")).unwrap();
        a.write_with_hook(&[Mutation::insert(b"aa", b"new")], |point| {
            if point == pause {
                let guard = b
                    .key_locks
                    .as_ref()
                    .unwrap()
                    .acquire_commit(
                        LockPolicy::default().deadline(),
                        Duration::from_millis(1),
                        &b.counters,
                    )
                    .unwrap();
                drop(guard);
                let visible = reader.read_batch().unwrap().get(b"aa").unwrap();
                assert_eq!(visible.is_some(), pause == CommitPoint::RootWritten);
                // Conflicting keys must still wait, even though the gate is free.
                assert!(matches!(
                    b.key_locks.as_ref().unwrap().acquire_stripes(
                        &[b"aa"],
                        LockPolicy::default().deadline(),
                        Duration::from_millis(1),
                        &b.counters,
                    ),
                    Err(Error::Busy)
                ));
                assert!(matches!(
                    Store::open(dir.path().join("pipeline.isam")),
                    Err(Error::Busy)
                ));
            }
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn later_writer_stages_concurrently_but_cannot_publish_an_unflushed_dependency() {
    let (dir, mut a, mut b) = fixture();
    let mut reader = Store::open_read_only(dir.path().join("pipeline.isam")).unwrap();
    let key = disjoint_key();
    let (first_staged_tx, first_staged_rx) = mpsc::channel();
    let (second_staged_tx, second_staged_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    thread::scope(|scope| {
        let first = scope.spawn(move || {
            a.write_with_hook(&[Mutation::insert(b"aa", b"a")], |point| {
                if point == CommitPoint::WorkingRootWritten {
                    first_staged_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(())
            })
        });
        first_staged_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let second = scope.spawn(move || {
            let result = b.write_with_hook(&[Mutation::insert(key.as_slice(), b"b")], |point| {
                if point == CommitPoint::WorkingRootWritten {
                    second_staged_tx.send(()).unwrap();
                }
                Ok(())
            });
            done_tx.send(()).unwrap();
            result
        });
        second_staged_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert_eq!(reader.read_batch().unwrap().verify().unwrap(), 0);
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        resume_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
    });
    assert_eq!(reader.read_batch().unwrap().verify().unwrap(), 2);
}

#[test]
fn failures_after_staging_are_uncertain_and_reopen_discards_only_unpublished_work() {
    for pause in [
        CommitPoint::PlanPrepared,
        CommitPoint::PagesWritten,
        CommitPoint::WorkingRootWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let (dir, mut a, _b) = fixture();
        a.write_batch(&[Mutation::insert(b"zz", b"acknowledged")])
            .unwrap();
        let result = a.write_with_hook(&[Mutation::insert(b"aa", b"maybe")], |point| {
            if point == pause {
                Err(io::Error::other("injected failure"))
            } else {
                Ok(())
            }
        });
        if matches!(pause, CommitPoint::PlanPrepared | CommitPoint::PagesWritten) {
            assert!(matches!(result, Err(Error::Io(_))));
        } else {
            assert!(matches!(result, Err(Error::CommitUnknown(_))), "{result:?}");
        }
        let published = pause == CommitPoint::RootWritten;
        let mut reopened = Store::open(dir.path().join("pipeline.isam")).unwrap();
        assert_eq!(
            reopened.read_batch().unwrap().get(b"aa").unwrap().is_some(),
            published
        );
        assert_eq!(
            reopened.read_batch().unwrap().get(b"zz").unwrap(),
            Some(b"acknowledged".to_vec())
        );
        reopened
            .write_batch(&[Mutation::insert(b"cc", b"after")])
            .unwrap();
        assert_eq!(
            reopened.read_batch().unwrap().verify().unwrap(),
            if published { 3 } else { 2 }
        );
    }
}

#[test]
fn writable_reopen_ignores_damaged_working_state_but_not_damaged_published_roots() {
    let (dir, mut a, _b) = fixture();
    a.write_batch(&[Mutation::insert(b"aa", b"safe")]).unwrap();
    a.file
        .write_all_at(&[0xff; format::PAGE_BYTES], format::WORKING_ROOT_OFFSET)
        .unwrap();
    let path = dir.path().join("pipeline.isam");
    assert_eq!(
        Store::open_read_only(&path)
            .unwrap()
            .read_batch()
            .unwrap()
            .verify()
            .unwrap(),
        1
    );
    let mut reopened = Store::open(&path).unwrap();
    reopened
        .write_batch(&[Mutation::insert(b"bb", b"also safe")])
        .unwrap();
    let latest = read_snapshot(&a.file, None).unwrap();
    a.file
        .write_all_at(
            &[0xff],
            (latest.publication % 2) * format::PAGE_BYTES as u64 + 80,
        )
        .unwrap();
    assert!(matches!(Store::open(&path), Err(Error::Corrupt(_))));
    assert!(matches!(
        Store::open_read_only(&path),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn pipelined_variable_values_splits_deletes_and_window_wrap_match_a_map() {
    let (dir, mut a, _b) = fixture();
    let mut expected = BTreeMap::new();
    for wave in 0..160_u16 {
        let mut changes = Vec::new();
        for n in 0..24_u16 {
            let key = (wave.wrapping_mul(139).wrapping_add(n * 97) % 1200)
                .to_be_bytes()
                .to_vec();
            if (wave + n) % 5 == 0 {
                changes.push(Mutation::delete(key.as_slice()));
                expected.remove(&key);
            } else {
                let value = vec![(wave % 255) as u8; (wave as usize * 7 + n as usize) % 129];
                changes.push(Mutation::put(key.as_slice(), value.as_slice()));
                expected.insert(key, value);
            }
        }
        a.write_batch(&changes).unwrap();
    }
    let mut reopened = Store::open(dir.path().join("pipeline.isam")).unwrap();
    let read = reopened.read_batch().unwrap();
    assert_eq!(read.verify().unwrap(), expected.len() as u64);
    let actual: BTreeMap<_, _> = read
        .range(&[0, 0], None, 4096)
        .unwrap()
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect();
    assert_eq!(actual, expected);
    let state = format::read_working_state(&reopened.file, &reopened.counters).unwrap();
    assert_eq!(state.durable_generation, state.snapshot.generation);
    assert_eq!(state.ready, 0);
}

#[test]
fn pipelined_crash_worker() {
    let Some(path) = std::env::var_os("BRISK_PIPELINE_CRASH_PATH") else {
        return;
    };
    let pause = std::env::var("BRISK_PIPELINE_CRASH_POINT").unwrap();
    let mut store = Store::open(path).unwrap();
    store
        .write_with_hook(
            &[
                Mutation::put(b"aa", b"new"),
                Mutation::insert(b"bb", b"new"),
            ],
            |point| {
                if format!("{point:?}") == pause {
                    std::process::exit(86);
                }
                Ok(())
            },
        )
        .unwrap();
    panic!("crash boundary was not reached");
}

#[test]
fn pipelined_process_exit_boundaries_preserve_acknowledged_data() {
    for pause in [
        CommitPoint::PlanPrepared,
        CommitPoint::PagesWritten,
        CommitPoint::WorkingRootWritten,
        CommitPoint::PagesSynced,
        CommitPoint::RootWritten,
    ] {
        let (dir, mut a, _b) = fixture();
        a.write_batch(&[Mutation::insert(b"aa", b"old")]).unwrap();
        let path = dir.path().join("pipeline.isam");
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::isam::pipelined_tests::pipelined_crash_worker",
            ])
            .env("BRISK_PIPELINE_CRASH_PATH", &path)
            .env("BRISK_PIPELINE_CRASH_POINT", format!("{pause:?}"))
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut reopened = Store::open(&path).unwrap();
        let published = pause == CommitPoint::RootWritten;
        let read = reopened.read_batch().unwrap();
        assert_eq!(
            read.get(b"aa").unwrap(),
            Some(if published { b"new" } else { b"old" }.to_vec())
        );
        assert_eq!(read.get(b"bb").unwrap().is_some(), published);
        reopened
            .write_batch(&[Mutation::insert(b"cc", b"after")])
            .unwrap();
    }
}

#[test]
fn a_crashed_writer_leaves_a_hole_that_cannot_be_silently_skipped() {
    let (dir, mut a, mut b) = fixture();
    a.write_batch(&[Mutation::insert(b"aa", b"old")]).unwrap();
    let path = dir.path().join("pipeline.isam");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::isam::pipelined_tests::pipelined_crash_worker",
        ])
        .env("BRISK_PIPELINE_CRASH_PATH", &path)
        .env("BRISK_PIPELINE_CRASH_POINT", "WorkingRootWritten")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86));
    // Use an already-open handle: no recovery fence has reset the working root.
    b.set_lock_policy(LockPolicy::default());
    assert!(matches!(
        b.write_batch(&[Mutation::insert(b"cc", b"later")]),
        Err(Error::CommitUnknown(_))
    ));
    assert_eq!(
        a.read_batch().unwrap().get(b"aa").unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(a.read_batch().unwrap().get(b"bb").unwrap(), None);
    assert_eq!(a.read_batch().unwrap().get(b"cc").unwrap(), None);
    let mut recovered = Store::open(&path).unwrap();
    recovered
        .write_batch(&[Mutation::insert(b"cc", b"recovered")])
        .unwrap();
    assert_eq!(recovered.read_batch().unwrap().verify().unwrap(), 2);
}

#[test]
fn working_state_rejects_invalid_completion_windows_and_reserved_bytes() {
    let (dir, a, _b) = fixture();
    let mut original = [0; format::PAGE_BYTES];
    a.file
        .read_exact_at(&mut original, format::WORKING_ROOT_OFFSET)
        .unwrap();
    for (offset, value) in [(48, 0_u64), (48, 2), (56, 1), (64, 2), (72, 1)] {
        let mut damaged = original;
        damaged[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        format::seal(&mut damaged);
        a.file
            .write_all_at(&damaged, format::WORKING_ROOT_OFFSET)
            .unwrap();
        assert!(matches!(
            format::read_working_state(&a.file, &a.counters),
            Err(Error::Corrupt(_))
        ));
    }
    // Only writable recovery, not a read of the working page, can discard it.
    let mut recovered = Store::open(dir.path().join("pipeline.isam")).unwrap();
    recovered
        .write_batch(&[Mutation::insert(b"aa", b"valid")])
        .unwrap();
    assert_eq!(recovered.read_batch().unwrap().verify().unwrap(), 1);
}

#[test]
fn abandoned_speculation_does_not_cause_infinite_validation_retries_or_false_duplicates() {
    let (dir, mut a, mut b) = fixture();
    a.write_batch(&[Mutation::insert(b"aa", b"old")]).unwrap();
    let path = dir.path().join("pipeline.isam");
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::isam::pipelined_tests::pipelined_crash_worker",
        ])
        .env("BRISK_PIPELINE_CRASH_PATH", &path)
        .env("BRISK_PIPELINE_CRASH_POINT", "WorkingRootWritten")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86));
    b.reset_operation_stats();
    // A catalog retry would keep reading published "old" forever if the
    // validator only returned false against the abandoned working "new".
    assert!(matches!(
        b.write_batch_checked(&[Mutation::put(b"aa", b"replacement")], &[], |read| {
            Ok(read.get(b"aa")?.as_deref() == Some(b"old".as_slice()))
        }),
        Err(Error::Busy)
    ));
    assert!(matches!(
        b.write_batch(&[Mutation::insert(b"bb", b"new owner")]),
        Err(Error::Busy)
    ));
    assert!(matches!(
        b.write_batch(&[Mutation::insert(b"aa", b"duplicate")]),
        Err(Error::Duplicate)
    ));
    assert_eq!(b.operation_stats().page_writes, 0);
    assert_eq!(b.operation_stats().root_writes, 0);
    let mut recovered = Store::open(&path).unwrap();
    assert!(
        recovered
            .write_batch_checked(&[Mutation::put(b"aa", b"replacement")], &[], |read| {
                Ok(read.get(b"aa")?.as_deref() == Some(b"old".as_slice()))
            })
            .unwrap()
    );
    recovered
        .write_batch(&[Mutation::insert(b"bb", b"new owner")])
        .unwrap();
}

#[test]
fn pipelined_unique_index_claims_remain_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("catalog.isam");
    let mut catalog = NativeCatalog::create_pipelined(&path).unwrap();
    catalog
        .create_table(&TableDefinition {
            name: "items".into(),
            schema_version: 1,
            columns: vec![
                ColumnDefinition {
                    name: "id".into(),
                    column_type: ColumnType::UInt64,
                    nullable: false,
                },
                ColumnDefinition {
                    name: "tag".into(),
                    column_type: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".into()],
            indexes: vec![IndexDefinition {
                name: "by_tag".into(),
                columns: vec!["tag".into()],
                unique: true,
            }],
        })
        .unwrap();
    let peers: Vec<_> = (0..8)
        .map(|_| {
            let mut c = NativeCatalog::open(&path).unwrap();
            c.set_lock_policy(policy());
            c
        })
        .collect();
    let barrier = Arc::new(Barrier::new(peers.len()));
    let results = thread::scope(|scope| {
        peers
            .into_iter()
            .enumerate()
            .map(|(id, mut c)| {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    c.insert_rows(
                        "items",
                        &[
                            vec![
                                NativeValue::UInt64(id as u64 * 2),
                                NativeValue::Text("contested".into()),
                            ],
                            vec![
                                NativeValue::UInt64(id as u64 * 2 + 1),
                                NativeValue::Text(format!("companion-{id}")),
                            ],
                        ],
                    )
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(
        results.iter().filter(|r| r.is_ok()).count(),
        1,
        "{results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Duplicate)))
            .count(),
        7,
        "{results:?}"
    );
    for (id, result) in results.iter().enumerate() {
        for n in 0..2 {
            assert_eq!(
                catalog
                    .get_row("items", &[NativeValue::UInt64(id as u64 * 2 + n)])
                    .unwrap()
                    .is_some(),
                result.is_ok()
            );
        }
    }
    assert_eq!(
        catalog
            .lookup_index(
                "items",
                "by_tag",
                &[NativeValue::Text("contested".into())],
                10
            )
            .unwrap()
            .len(),
        1
    );
}
