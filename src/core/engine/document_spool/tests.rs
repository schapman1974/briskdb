use super::*;

fn position(id: u64) -> SpoolPosition {
    let mut key = vec![b'x'; 32];
    key[..8].copy_from_slice(&(id % 13).to_be_bytes());
    SpoolPosition {
        key,
        natural_order: id + 1,
        shard: (id % 4) as u16,
    }
}

fn builder(budget: &SpoolBudget) -> SpoolBuilder {
    let mut builder = SpoolBuilder::new(budget);
    builder.chunk_keys = 3;
    builder.chunk_bytes = 1024;
    builder.frontier_runs = 4;
    builder
}

fn build(
    budget: &SpoolBudget,
    count: u64,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<SortSpool> {
    let mut builder = builder(budget);
    for id in (0..count).rev() {
        builder.push(position(id), check)?;
    }
    builder.finish(check)
}

#[test]
fn binary_runs_preserve_keys_ties_and_shards_with_bounded_pages_and_release_disk() {
    for count in [0, 1, 2, 3, 4, 17, 97, 1001] {
        let budget = SpoolBudget::default();
        let mut spool = build(&budget, count, &mut || Ok(())).unwrap();
        assert_eq!(
            budget.0.used.load(Ordering::Acquire),
            count * (HEADER_BYTES + 32) as u64
        );
        let mut expected: Vec<_> = (0..count).map(position).collect();
        expected.sort();
        for expected in expected {
            spool.advance(&mut || Ok(())).unwrap();
            assert!(spool.pending.len() <= 256);
            let actual = spool.pending.pop_front().unwrap();
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.natural_order, expected.natural_order);
            assert_eq!(actual.shard, expected.shard);
        }
        spool.advance(&mut || Ok(())).unwrap();
        assert!(spool.pending.is_empty());
        drop(spool);
        assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
    }
}

#[test]
fn lazy_merge_preserves_multiple_runs_without_reserving_rewrite_copies() {
    let bytes = 19 * (HEADER_BYTES + 32) as u64;
    let budget = SpoolBudget(Counter::new(bytes));
    let mut builder = builder(&budget);
    builder.frontier_runs = FRONTIER_RUNS;
    for id in (0..19).rev() {
        builder.push(position(id), &mut || Ok(())).unwrap();
    }
    let mut spool = builder.finish(&mut || Ok(())).unwrap();
    assert_eq!(spool.runs.len(), 7);
    assert_eq!(budget.used_bytes(), bytes);
    let mut expected: Vec<_> = (0..19).map(position).collect();
    expected.sort();
    for expected in expected {
        spool.advance(&mut || Ok(())).unwrap();
        let actual = spool.pending.pop_front().unwrap();
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.natural_order, expected.natural_order);
        assert_eq!(actual.shard, expected.shard);
    }
    spool.advance(&mut || Ok(())).unwrap();
    assert!(spool.pending.is_empty());
    assert!(spool.frontiers.is_empty());
    drop(spool);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn frontier_byte_budget_forces_balanced_merge_passes() {
    let budget = SpoolBudget::default();
    let mut builder = builder(&budget);
    builder.frontier_runs = FRONTIER_RUNS;
    builder.frontier_bytes = 2 * (32 + 128);
    for id in 0..19 {
        builder.push(position(id), &mut || Ok(())).unwrap();
    }
    let mut spool = builder.finish(&mut || Ok(())).unwrap();
    assert_eq!(spool.runs.len(), 2);
    spool.advance(&mut || Ok(())).unwrap();
    assert_eq!(spool.pending.len(), 19);
    drop(spool);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn cancellation_during_buffering_spill_merge_and_read_releases_all_scratch() {
    let budget = SpoolBudget::default();
    let mut calls = 0;
    let spool = build(&budget, 19, &mut || {
        calls += 1;
        Ok(())
    })
    .unwrap();
    drop(spool);
    for cutoff in (0..calls).step_by(5) {
        let mut current = 0;
        let result = build(&budget, 19, &mut || {
            current += 1;
            if current > cutoff {
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "test cancellation",
                ))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(budget.0.used.load(Ordering::Acquire), 0, "cutoff {cutoff}");
    }
    let mut spool = build(&budget, 19, &mut || Ok(())).unwrap();
    let error = spool
        .advance(&mut || {
            Err(EngineError::new(
                EngineErrorKind::Cancelled,
                "test cancellation",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    drop(spool);
    assert_eq!(budget.0.used.load(Ordering::Acquire), 0);

    let mut spool = build(&budget, 19, &mut || Ok(())).unwrap();
    let mut read_checks = 0;
    spool
        .advance(&mut || {
            read_checks += 1;
            Ok(())
        })
        .unwrap();
    drop(spool);
    for cutoff in (0..read_checks).step_by(5) {
        let mut spool = build(&budget, 19, &mut || Ok(())).unwrap();
        let mut current = 0;
        assert!(
            spool
                .advance(&mut || {
                    current += 1;
                    if current > cutoff {
                        Err(EngineError::new(
                            EngineErrorKind::Cancelled,
                            "test frontier cancellation",
                        ))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        drop(spool);
        assert_eq!(budget.used_bytes(), 0, "frontier cutoff {cutoff}");
    }
}

#[test]
fn local_and_shared_quotas_cover_merge_copies_and_recover_after_failure_or_drop() {
    for local_limit in [false, true] {
        let budget = SpoolBudget(Counter::new(if local_limit { 10_000 } else { 500 }));
        let mut builder = builder(&budget);
        if local_limit {
            builder.local = Counter::new(500);
        }
        let result: EngineResult<()> = (|| {
            for id in 0..19 {
                builder.push(position(id), &mut || Ok(()))?;
            }
            drop(builder.finish(&mut || Ok(()))?);
            Ok(())
        })();
        assert_eq!(result.unwrap_err().kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
        let spool = build(&budget, 1, &mut || Ok(())).unwrap();
        assert_eq!(
            budget.0.used.load(Ordering::Acquire),
            (HEADER_BYTES + 32) as u64
        );
        drop(spool);
        assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
    }
    let budget = SpoolBudget(Counter::new(156));
    let first = build(&budget, 2, &mut || Ok(())).unwrap();
    assert!(build(&budget, 1, &mut || Ok(())).is_err());
    assert_eq!(budget.0.used.load(Ordering::Acquire), 156);
    drop(first);
    assert!(build(&budget, 1, &mut || Ok(())).is_ok());
    assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
}

#[test]
fn scratch_corruption_and_truncation_fail_without_poisoning_database_state() {
    for damage in 0..5 {
        let budget = SpoolBudget::default();
        let mut spool = build(&budget, 3, &mut || Ok(())).unwrap();
        let file = spool.runs.first_mut().unwrap().file.get_mut();
        match damage {
            0 => file.set_len(HEADER_BYTES as u64 - 1).unwrap(),
            1 => {
                file.seek(SeekFrom::Start(14)).unwrap();
                file.write_all(&[0; 32]).unwrap();
            }
            2 => {
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&u32::MAX.to_le_bytes()).unwrap();
            }
            3 => {
                // Below the global key limit and remaining file length, but
                // larger than this run's recorded frontier allocation bound.
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&64u32.to_le_bytes()).unwrap();
            }
            _ => {
                file.seek(SeekFrom::Start(HEADER_BYTES as u64)).unwrap();
                file.write_all(b"bad payload").unwrap();
            }
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(
            spool.advance(&mut || Ok(())).unwrap_err().kind(),
            EngineErrorKind::StorageUnavailable
        );
        drop(spool);
        assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
    }
}

#[test]
fn oversized_keys_fail_before_allocating_a_temporary_run() {
    let budget = SpoolBudget::default();
    let mut builder = builder(&budget);
    let position = SpoolPosition {
        key: vec![0; 1024],
        natural_order: 1,
        shard: 0,
    };
    assert_eq!(
        builder.push(position, &mut || Ok(())).unwrap_err().kind(),
        EngineErrorKind::LimitExceeded
    );
    assert!(builder.runs.is_empty());
    assert_eq!(budget.0.used.load(Ordering::Acquire), 0);
}

#[cfg(unix)]
#[test]
fn scratch_files_are_private_and_have_no_directory_entry() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let budget = SpoolBudget::default();
    let spool = build(&budget, 1, &mut || Ok(())).unwrap();
    let metadata = spool
        .runs
        .first()
        .unwrap()
        .file
        .get_ref()
        .metadata()
        .unwrap();
    assert_eq!(metadata.permissions().mode() & 0o077, 0);
    assert_eq!(metadata.nlink(), 0);
}
