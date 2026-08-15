use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use briskdb::core::{
    CancellationToken, CanonicalIndexKey, Database, EngineErrorKind, GlobalIndexDeclaration,
    GlobalIndexId, GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexOwner,
    GlobalIndexStorageTopology, GlobalOperationId, GlobalOperationState, GlobalUniqueMutation,
    ShardKeyMetadata, ShardKeyType, TableDeclaration, UniqueNullSemantics, Value,
};
use proptest::prelude::*;
use rusqlite::Connection;

const SHARDS: u16 = 4;
const CHILD_ROOT: &str = "BRISKDB_AUTHORITY_CHILD_ROOT";
const CHILD_MODE: &str = "BRISKDB_AUTHORITY_CHILD_MODE";
const CHILD_WORKER: &str = "BRISKDB_AUTHORITY_CHILD_WORKER";
const CHILD_RELEASE: &str = "BRISKDB_AUTHORITY_CHILD_RELEASE";
const CHILD_RESULT: &str = "BRISKDB_AUTHORITY_CHILD_RESULT";

fn setup(root: &Path) -> (Database, GlobalIndexId, GlobalIndexId) {
    let mut database = Database::open(root, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE authority_rows (
                 tenant_id TEXT NOT NULL,
                 row_id INTEGER NOT NULL,
                 email TEXT NOT NULL,
                 global_value INTEGER NOT NULL,
                 PRIMARY KEY (tenant_id, row_id)
             ) STRICT",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "authority_rows",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    let table = database
        .catalog()
        .table("default", "authority_rows")
        .unwrap()
        .unwrap()
        .id();
    let unique_id = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "authority_email_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("email").unwrap(),
                    GlobalIndexKeyType::Text,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::Distinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(unique_id).unwrap();
    let value_id = database
        .create_global_index(
            GlobalIndexDeclaration::new(
                table,
                "authority_global_value_unique",
                vec![GlobalIndexKeyPart::new(
                    GlobalIndexKeySource::column("global_value").unwrap(),
                    GlobalIndexKeyType::Int64,
                )],
            )
            .unwrap()
            .unique(UniqueNullSemantics::NotDistinct)
            .with_topology(GlobalIndexStorageTopology::selected_v1()),
        )
        .unwrap();
    database.build_global_index(value_id).unwrap();
    (database, unique_id, value_id)
}

fn operation(value: u128) -> GlobalOperationId {
    GlobalOperationId::new(value.to_le_bytes()).unwrap()
}

fn key(value: &str) -> CanonicalIndexKey {
    CanonicalIndexKey::encode_values(&[Value::from(value)]).unwrap()
}

fn owner(shard: u16, value: &str) -> GlobalIndexOwner {
    GlobalIndexOwner::new(shard, value.as_bytes().to_vec()).unwrap()
}

#[test]
fn unique_and_value_state_machines_are_idempotent_and_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let (database, unique_id, value_id) = setup(temp.path());
    let first =
        GlobalUniqueMutation::claim(unique_id, key("first@example.test"), owner(0, "row-1"));
    assert_eq!(
        database
            .reserve_global_unique(operation(1), &first)
            .unwrap()
            .state(),
        GlobalOperationState::Active
    );
    assert_eq!(
        database
            .reserve_global_unique(operation(1), &first)
            .unwrap()
            .state(),
        GlobalOperationState::Active
    );
    let changed_request =
        GlobalUniqueMutation::claim(unique_id, key("changed@example.test"), owner(0, "row-1"));
    assert_eq!(
        database
            .reserve_global_unique(operation(1), &changed_request)
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        database
            .reserve_global_unique(operation(2), &first)
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    assert_eq!(
        database
            .finalize_global_unique(operation(1))
            .unwrap()
            .state(),
        GlobalOperationState::Finalized
    );
    assert_eq!(
        database
            .finalize_global_unique(operation(1))
            .unwrap()
            .state(),
        GlobalOperationState::Finalized
    );

    let replacement = GlobalUniqueMutation::replace(
        unique_id,
        key("first@example.test"),
        owner(0, "row-1"),
        key("second@example.test"),
        owner(1, "row-1-moved"),
    );
    database
        .reserve_global_unique(operation(3), &replacement)
        .unwrap();
    for (id, candidate) in [
        (
            4,
            GlobalUniqueMutation::claim(unique_id, key("first@example.test"), owner(2, "steal")),
        ),
        (
            5,
            GlobalUniqueMutation::claim(unique_id, key("second@example.test"), owner(2, "steal")),
        ),
    ] {
        assert_eq!(
            database
                .reserve_global_unique(operation(id), &candidate)
                .unwrap_err()
                .kind(),
            EngineErrorKind::UniqueViolation
        );
    }
    database.finalize_global_unique(operation(3)).unwrap();
    let handoff = GlobalUniqueMutation::replace(
        unique_id,
        key("second@example.test"),
        owner(1, "row-1-moved"),
        key("second@example.test"),
        owner(2, "row-1-rewritten"),
    );
    database
        .reserve_global_unique(operation(8), &handoff)
        .unwrap();
    database.finalize_global_unique(operation(8)).unwrap();
    assert_eq!(
        database
            .reserve_global_unique(
                operation(9),
                &GlobalUniqueMutation::release(
                    unique_id,
                    key("second@example.test"),
                    owner(1, "row-1-moved"),
                ),
            )
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    let released_key =
        GlobalUniqueMutation::claim(unique_id, key("first@example.test"), owner(2, "new-row"));
    database
        .reserve_global_unique(operation(6), &released_key)
        .unwrap();
    assert_eq!(
        database
            .rollback_global_unique(operation(6))
            .unwrap()
            .state(),
        GlobalOperationState::RolledBack
    );
    assert_eq!(
        database
            .finalize_global_unique(operation(6))
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );

    for (id, value, row) in [(20, "alpha", "row-alpha"), (21, "beta", "row-beta")] {
        let mutation = GlobalUniqueMutation::claim(unique_id, key(value), owner(0, row));
        database
            .reserve_global_unique(operation(id), &mutation)
            .unwrap();
        database.finalize_global_unique(operation(id)).unwrap();
    }
    for (id, old, old_row, new, new_row) in [
        (22, "alpha", "row-alpha", "beta", "row-alpha"),
        (23, "beta", "row-beta", "alpha", "row-beta"),
    ] {
        let exchange = GlobalUniqueMutation::replace(
            unique_id,
            key(old),
            owner(0, old_row),
            key(new),
            owner(0, new_row),
        );
        assert_eq!(
            database
                .reserve_global_unique(operation(id), &exchange)
                .unwrap_err()
                .kind(),
            EngineErrorKind::UniqueViolation
        );
    }

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        database
            .reserve_global_unique_with_cancellation(operation(7), &released_key, &cancelled)
            .unwrap_err()
            .kind(),
        EngineErrorKind::Cancelled
    );

    let lease_one = database
        .lease_global_values(operation(100), value_id, 3)
        .unwrap();
    assert_eq!((lease_one.first(), lease_one.last()), (1, 3));
    assert_eq!(
        database
            .lease_global_values(operation(100), value_id, 3)
            .unwrap(),
        lease_one
    );
    assert_eq!(
        database
            .lease_global_values(operation(100), value_id, 4)
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        database
            .lease_global_values(operation(1), value_id, 1)
            .unwrap_err()
            .kind(),
        EngineErrorKind::InvalidArgument
    );
    assert_eq!(
        database
            .abandon_global_value_lease(operation(100))
            .unwrap()
            .state(),
        GlobalOperationState::RolledBack
    );
    let lease_two = database
        .lease_global_values(operation(101), value_id, 2)
        .unwrap();
    assert_eq!((lease_two.first(), lease_two.last()), (4, 5));
    assert!(lease_two.fence_token() > lease_one.fence_token());
    assert_eq!(
        database
            .finalize_global_value_lease(operation(101))
            .unwrap()
            .state(),
        GlobalOperationState::Finalized
    );

    Connection::open(temp.path().join("global-indexes/global.sqlite"))
        .unwrap()
        .execute(
            "UPDATE briskdb_global_value_sequences
             SET next_value = ?1, exhausted = 0 WHERE index_id = ?2",
            [i64::MAX, i64::try_from(value_id.get()).unwrap()],
        )
        .unwrap();
    assert_eq!(
        database
            .lease_global_values(operation(102), value_id, 2)
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(
        database
            .lease_global_values(operation(103), value_id, 1)
            .unwrap()
            .first(),
        i64::MAX as u64
    );
    assert_eq!(
        database
            .lease_global_values(operation(104), value_id, 1)
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
}

#[test]
fn validation_reports_active_reservations_and_rebuild_rolls_them_back() {
    let temp = tempfile::tempdir().unwrap();
    let (mut database, unique_id, _) = setup(temp.path());
    let mutation = GlobalUniqueMutation::claim(
        unique_id,
        key("interrupted@example.test"),
        owner(0, "interrupted-row"),
    );
    database
        .reserve_global_unique(operation(500), &mutation)
        .unwrap();
    let validation = database.validate_global_index(unique_id).unwrap();
    assert!(!validation.is_valid());
    assert!(
        validation
            .issues()
            .iter()
            .any(|issue| { issue.kind().code() == "active_unique_reservation" })
    );
    database.rebuild_global_index(unique_id).unwrap();
    assert_eq!(
        database
            .reserve_global_unique(operation(500), &mutation)
            .unwrap()
            .state(),
        GlobalOperationState::RolledBack
    );
    database
        .reserve_global_unique(operation(501), &mutation)
        .unwrap();
}

#[test]
fn authority_worker_child() {
    let Ok(root) = env::var(CHILD_ROOT) else {
        return;
    };
    let mode = env::var(CHILD_MODE).unwrap();
    let worker: u8 = env::var(CHILD_WORKER).unwrap().parse().unwrap();
    let release = PathBuf::from(env::var(CHILD_RELEASE).unwrap());
    let result = PathBuf::from(env::var(CHILD_RESULT).unwrap());
    let database = Database::open(root, SHARDS).unwrap();
    fs::write(result.with_extension("ready"), b"ready").unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !release.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for release");
        thread::sleep(Duration::from_millis(2));
    }
    let indexes = database.catalog().global_indexes();
    let unique_id = indexes
        .iter()
        .find(|index| index.name() == "authority_email_unique")
        .unwrap()
        .id();
    let value_id = indexes
        .iter()
        .find(|index| index.name() == "authority_global_value_unique")
        .unwrap()
        .id();
    match mode.as_str() {
        "unique" => {
            let mutation = GlobalUniqueMutation::claim(
                unique_id,
                key("hot@example.test"),
                owner(u16::from(worker), &format!("row-{worker}")),
            );
            let won = match database
                .reserve_global_unique(operation(1_000 + u128::from(worker)), &mutation)
            {
                Ok(_) => {
                    database
                        .finalize_global_unique(operation(1_000 + u128::from(worker)))
                        .unwrap();
                    true
                }
                Err(error) if error.kind() == EngineErrorKind::UniqueViolation => false,
                Err(error) => panic!("unexpected authority error: {error}"),
            };
            fs::write(
                result,
                if won {
                    b"won".as_slice()
                } else {
                    b"lost".as_slice()
                },
            )
            .unwrap();
        }
        "lease" => {
            for ordinal in 0_u8..20 {
                let id = u128::from_be_bytes([
                    2, worker, ordinal, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                ]);
                database
                    .lease_global_values(operation(id), value_id, 7)
                    .unwrap();
                database.finalize_global_value_lease(operation(id)).unwrap();
            }
            fs::write(result, b"done").unwrap();
        }
        _ => panic!("unknown child mode"),
    }
}

fn run_workers(root: &Path, mode: &str, workers: usize) -> Vec<Vec<u8>> {
    let release = root.join(format!("{mode}-release"));
    let mut children = Vec::new();
    let mut results = Vec::new();
    for worker in 0..workers {
        let result = root.join(format!("{mode}-{worker}-result"));
        let child = Command::new(env::current_exe().unwrap())
            .args(["--exact", "authority_worker_child", "--nocapture"])
            .env(CHILD_ROOT, root)
            .env(CHILD_MODE, mode)
            .env(CHILD_WORKER, worker.to_string())
            .env(CHILD_RELEASE, &release)
            .env(CHILD_RESULT, &result)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        children.push(child);
        results.push(result);
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while results
        .iter()
        .any(|result| !result.with_extension("ready").exists())
    {
        assert!(Instant::now() < deadline, "timed out waiting for workers");
        thread::sleep(Duration::from_millis(2));
    }
    fs::write(&release, b"release").unwrap();
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    results
        .into_iter()
        .map(|path| fs::read(path).unwrap())
        .collect()
}

#[test]
fn multiple_processes_choose_one_unique_owner_and_disjoint_value_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let (database, unique_id, value_id) = setup(temp.path());
    let unique_results = run_workers(temp.path(), "unique", 4);
    assert_eq!(
        unique_results
            .iter()
            .filter(|value| value.as_slice() == b"won")
            .count(),
        1
    );
    let connection = Connection::open(temp.path().join("global-indexes/global.sqlite")).unwrap();
    let owners: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM briskdb_global_index_unique_keys WHERE index_id = ?1",
            [i64::try_from(unique_id.get()).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owners, 1);

    assert!(
        run_workers(temp.path(), "lease", 4)
            .iter()
            .all(|value| value.as_slice() == b"done")
    );
    let mut statement = connection
        .prepare(
            "SELECT first_value, last_value FROM briskdb_global_value_leases
             WHERE index_id = ?1 ORDER BY first_value",
        )
        .unwrap();
    let ranges = statement
        .query_map([i64::try_from(value_id.get()).unwrap()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ranges.len(), 80);
    assert_eq!(ranges[0], (1, 7));
    for pair in ranges.windows(2) {
        assert_eq!(pair[0].1 + 1, pair[1].0);
    }
    drop(database);
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 12,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn unique_histories_match_a_serial_reference_model(
        history in prop::collection::vec((0_u8..8, any::<bool>()), 1..40)
    ) {
        let temp = tempfile::tempdir().unwrap();
        let (database, unique_id, _) = setup(temp.path());
        let mut model = BTreeSet::new();
        for (ordinal, (candidate, commit)) in history.into_iter().enumerate() {
            let candidate_key = key(&format!("key-{candidate}"));
            let mutation = GlobalUniqueMutation::claim(
                unique_id,
                candidate_key,
                owner(u16::from(candidate % SHARDS as u8), &format!("row-{ordinal}")),
            );
            let id = operation(10_000 + ordinal as u128);
            match database.reserve_global_unique(id, &mutation) {
                Ok(_) => {
                    prop_assert!(!model.contains(&candidate));
                    if commit {
                        database.finalize_global_unique(id).unwrap();
                        model.insert(candidate);
                    } else {
                        database.rollback_global_unique(id).unwrap();
                    }
                }
                Err(error) => {
                    prop_assert_eq!(error.kind(), EngineErrorKind::UniqueViolation);
                    prop_assert!(model.contains(&candidate));
                }
            }
        }
        let physical: i64 = Connection::open(temp.path().join("global-indexes/global.sqlite"))
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM briskdb_global_index_unique_keys WHERE index_id = ?1",
                [i64::try_from(unique_id.get()).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        prop_assert_eq!(physical as usize, model.len());
    }
}
