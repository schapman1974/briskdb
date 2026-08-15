use std::path::Path;

use briskdb::core::{
    CancellationToken, Database, EngineErrorKind, GlobalIndexDeclaration, GlobalIndexId,
    GlobalIndexKeyPart, GlobalIndexKeySource, GlobalIndexKeyType, GlobalIndexLifecycle,
    GlobalIndexStorageTopology, GlobalIndexValidationIssueKind, GlobalIndexValidationOptions,
    ShardKeyMetadata, ShardKeyType, TableDeclaration, UniqueNullSemantics, Value,
};
use rusqlite::{Connection, params};

const SHARDS: u16 = 4;

fn setup(root: &Path, unique: bool, rows: usize) -> (Database, GlobalIndexId) {
    let mut database = Database::open(root, SHARDS).unwrap();
    database
        .broadcast(
            "CREATE TABLE events (
                tenant_id TEXT NOT NULL,
                local_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                PRIMARY KEY (tenant_id, local_id)
             )",
        )
        .unwrap();
    let logical = database.catalog().default_database().id();
    database
        .register_tables(vec![
            TableDeclaration::sharded(
                logical,
                "events",
                ShardKeyMetadata::new("tenant_id", ShardKeyType::Text).unwrap(),
            )
            .unwrap(),
        ])
        .unwrap();
    for ordinal in 0..rows {
        let tenant = format!("tenant-{}", ordinal % 17);
        database
            .execute(
                &tenant,
                "INSERT INTO events (tenant_id, local_id, email) VALUES (?1, ?2, ?3)",
                &[
                    Value::from(tenant.as_str()),
                    Value::from(ordinal as i64),
                    Value::from(format!("user-{ordinal}@example.test")),
                ],
            )
            .unwrap();
    }
    let table = database
        .catalog()
        .table("default", "events")
        .unwrap()
        .unwrap()
        .id();
    let declaration = GlobalIndexDeclaration::new(
        table,
        if unique {
            "events_email_unique"
        } else {
            "events_email_lookup"
        },
        vec![GlobalIndexKeyPart::new(
            GlobalIndexKeySource::column("email").unwrap(),
            GlobalIndexKeyType::Text,
        )],
    )
    .unwrap()
    .with_topology(GlobalIndexStorageTopology::selected_v1());
    let declaration = if unique {
        declaration.unique(UniqueNullSemantics::Distinct)
    } else {
        declaration
    };
    let index_id = database.create_global_index(declaration).unwrap();
    database.build_global_index(index_id).unwrap();
    (database, index_id)
}

fn physical(root: &Path) -> Connection {
    Connection::open(root.join("global-indexes/global.sqlite")).unwrap()
}

fn assert_finding(
    database: &mut Database,
    index_id: GlobalIndexId,
    expected: GlobalIndexValidationIssueKind,
) {
    let report = database.validate_global_index(index_id).unwrap();
    assert!(!report.is_valid(), "corruption unexpectedly validated");
    assert!(
        report.issues().iter().any(|issue| issue.kind() == expected),
        "missing {expected:?} in {:?}",
        report.issues()
    );
    assert_eq!(
        database
            .catalog()
            .global_index_by_id(index_id)
            .unwrap()
            .lifecycle(),
        GlobalIndexLifecycle::Invalid
    );
}

#[test]
fn full_and_sampled_validation_are_bounded_machine_readable_and_cancellable() {
    let temp = tempfile::tempdir().unwrap();
    let (mut database, index_id) = setup(temp.path(), false, 5_000);
    let sampled = database
        .validate_global_index_with_cancellation(
            index_id,
            GlobalIndexValidationOptions::sampled(7)
                .unwrap()
                .with_max_reported_issues(3)
                .unwrap(),
            &CancellationToken::new(),
        )
        .unwrap();
    assert!(sampled.is_valid());
    assert_eq!(sampled.mode().code(), "sampled");
    assert!(sampled.source_rows_examined() <= u64::from(SHARDS) * 7);
    assert!(sampled.physical_entries_examined() <= u64::from(SHARDS) * 7);

    let full = database.validate_global_index(index_id).unwrap();
    assert!(full.is_valid());
    assert_eq!(full.source_rows_examined(), 5_000);
    assert_eq!(full.physical_entries_examined(), 5_000);
    assert_eq!(full.lifecycle_after(), GlobalIndexLifecycle::Ready);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let error = database
        .validate_global_index_with_cancellation(
            index_id,
            GlobalIndexValidationOptions::full(),
            &cancelled,
        )
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::Cancelled);
    assert_eq!(
        database
            .catalog()
            .global_index_by_id(index_id)
            .unwrap()
            .lifecycle(),
        GlobalIndexLifecycle::Rebuilding
    );
    assert!(database.rebuild_global_index(index_id).is_ok());
}

#[test]
fn non_unique_validation_classifies_seeded_metadata_and_entry_corruption() {
    type Tamper = fn(&Connection, i64);
    let cases: &[(GlobalIndexValidationIssueKind, Tamper)] = &[
        (
            GlobalIndexValidationIssueKind::MissingBuildRecord,
            |db, id| {
                db.execute(
                    "DELETE FROM briskdb_global_index_builds WHERE index_id = ?1",
                    [id],
                )
                .unwrap();
            },
        ),
        (GlobalIndexValidationIssueKind::IncompleteBuild, |db, id| {
            db.execute(
                "UPDATE briskdb_global_index_builds SET build_state = 1 WHERE index_id = ?1",
                [id],
            )
            .unwrap();
        }),
        (
            GlobalIndexValidationIssueKind::DefinitionMismatch,
            |db, id| {
                db.execute(
                    "UPDATE briskdb_global_index_builds SET definition_digest = zeroblob(32)
                 WHERE index_id = ?1",
                    [id],
                )
                .unwrap();
            },
        ),
        (
            GlobalIndexValidationIssueKind::MissingCheckpoint,
            |db, id| {
                db.execute(
                    "DELETE FROM briskdb_global_index_checkpoints
                 WHERE index_id = ?1 AND source_shard = (
                    SELECT min(source_shard) FROM briskdb_global_index_checkpoints
                    WHERE index_id = ?1
                 )",
                    [id],
                )
                .unwrap();
            },
        ),
        (
            GlobalIndexValidationIssueKind::UnexpectedCheckpoint,
            |db, id| {
                db.execute(
                    "INSERT INTO briskdb_global_index_checkpoints
                 SELECT index_id, 63, source_digest, indexed_rows, unique_rows
                 FROM briskdb_global_index_checkpoints
                 WHERE index_id = ?1 LIMIT 1",
                    [id],
                )
                .unwrap();
            },
        ),
        (
            GlobalIndexValidationIssueKind::CheckpointMismatch,
            |db, id| {
                db.execute(
                    "UPDATE briskdb_global_index_checkpoints SET source_digest = zeroblob(32)
                 WHERE index_id = ?1 AND source_shard = (
                    SELECT min(source_shard) FROM briskdb_global_index_checkpoints
                    WHERE index_id = ?1
                 )",
                    [id],
                )
                .unwrap();
            },
        ),
        (GlobalIndexValidationIssueKind::MissingEntry, |db, id| {
            db.execute(
                "DELETE FROM briskdb_global_index_entries
                 WHERE index_id = ?1 AND (source_shard, source_ordinal) = (
                    SELECT source_shard, source_ordinal FROM briskdb_global_index_entries
                    WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1
                 )",
                [id],
            )
            .unwrap();
        }),
        (GlobalIndexValidationIssueKind::DanglingEntry, |db, id| {
            db.execute(
                "INSERT INTO briskdb_global_index_entries
                    (index_id, encoded_key, source_shard, source_ordinal, source_locator)
                 SELECT index_id, encoded_key, source_shard, 999999,
                        x'4252494c00000001000101000000000000270f'
                 FROM briskdb_global_index_entries
                 WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1",
                [id],
            )
            .unwrap();
        }),
        (GlobalIndexValidationIssueKind::StaleEntry, |db, id| {
            db.execute(
                "UPDATE briskdb_global_index_entries AS target
                 SET encoded_key = (
                    SELECT encoded_key FROM briskdb_global_index_entries
                    WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1 OFFSET 1
                 )
                 WHERE target.index_id = ?1 AND (source_shard, source_ordinal) = (
                    SELECT source_shard, source_ordinal FROM briskdb_global_index_entries
                    WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1
                 )",
                [id],
            )
            .unwrap();
        }),
        (GlobalIndexValidationIssueKind::BadShardTarget, |db, id| {
            db.execute(
                "INSERT INTO briskdb_global_index_entries
                    (index_id, encoded_key, source_shard, source_ordinal, source_locator)
                 SELECT index_id, encoded_key, 63, 0, source_locator
                 FROM briskdb_global_index_entries
                 WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1",
                [id],
            )
            .unwrap();
        }),
        (
            GlobalIndexValidationIssueKind::IncompatibleKeyEncoding,
            |db, id| {
                db.execute(
                    "UPDATE briskdb_global_index_entries SET encoded_key = x'00'
                     WHERE index_id = ?1 AND (source_shard, source_ordinal) = (
                        SELECT source_shard, source_ordinal FROM briskdb_global_index_entries
                        WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1
                     )",
                    [id],
                )
                .unwrap();
            },
        ),
        (
            GlobalIndexValidationIssueKind::IncompatibleLocatorEncoding,
            |db, id| {
                db.execute(
                    "UPDATE briskdb_global_index_entries SET source_locator = x'00'
                     WHERE index_id = ?1 AND (source_shard, source_ordinal) = (
                        SELECT source_shard, source_ordinal FROM briskdb_global_index_entries
                        WHERE index_id = ?1 ORDER BY source_shard, source_ordinal LIMIT 1
                     )",
                    [id],
                )
                .unwrap();
            },
        ),
    ];

    for (expected, tamper) in cases {
        let temp = tempfile::tempdir().unwrap();
        let (mut database, index_id) = setup(temp.path(), false, 12);
        tamper(
            &physical(temp.path()),
            i64::try_from(index_id.get()).unwrap(),
        );
        assert_finding(&mut database, index_id, *expected);
    }
}

#[test]
fn non_unique_repair_replaces_affected_shards_and_never_leaves_false_negatives() {
    let temp = tempfile::tempdir().unwrap();
    let (mut database, index_id) = setup(temp.path(), false, 100);
    let connection = physical(temp.path());
    let damaged_shard: i64 = connection
        .query_row(
            "SELECT min(source_shard) FROM briskdb_global_index_entries WHERE index_id = ?1",
            [i64::try_from(index_id.get()).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "DELETE FROM briskdb_global_index_entries
             WHERE index_id = ?1 AND source_shard = ?2 AND source_ordinal = 0",
            params![i64::try_from(index_id.get()).unwrap(), damaged_shard],
        )
        .unwrap();
    drop(connection);

    assert_finding(
        &mut database,
        index_id,
        GlobalIndexValidationIssueKind::MissingEntry,
    );
    let repair = database.repair_global_index(index_id).unwrap();
    assert_eq!(repair.repaired_shards(), &[damaged_shard as u16]);
    assert!(repair.validation().is_valid());
    assert_eq!(repair.indexed_rows(), 100);
    assert_eq!(
        repair.validation().lifecycle_after(),
        GlobalIndexLifecycle::Ready
    );
    assert!(database.validate_global_index(index_id).unwrap().is_valid());
}

#[test]
fn unique_corruption_is_never_repaired_and_full_rebuild_restores_authority() {
    let cases = [
        GlobalIndexValidationIssueKind::DuplicateAuthoritativeKey,
        GlobalIndexValidationIssueKind::MissingUniqueReservation,
        GlobalIndexValidationIssueKind::DanglingUniqueReservation,
        GlobalIndexValidationIssueKind::MismatchedUniqueReservation,
    ];
    for expected in cases {
        let temp = tempfile::tempdir().unwrap();
        let (mut database, index_id) = setup(temp.path(), true, 12);
        let connection = physical(temp.path());
        let id = i64::try_from(index_id.get()).unwrap();
        match expected {
            GlobalIndexValidationIssueKind::DuplicateAuthoritativeKey => {
                connection
                    .execute(
                        "UPDATE briskdb_global_index_entries AS target
                         SET encoded_key = (
                            SELECT encoded_key FROM briskdb_global_index_entries
                            WHERE index_id = ?1 ORDER BY encoded_key LIMIT 1
                         )
                         WHERE target.index_id = ?1 AND (source_shard, source_ordinal) = (
                            SELECT source_shard, source_ordinal
                            FROM briskdb_global_index_entries WHERE index_id = ?1
                            ORDER BY encoded_key LIMIT 1 OFFSET 1
                         )",
                        [id],
                    )
                    .unwrap();
            }
            GlobalIndexValidationIssueKind::MissingUniqueReservation => {
                connection
                    .execute(
                        "DELETE FROM briskdb_global_index_unique_keys
                         WHERE index_id = ?1 AND encoded_key = (
                            SELECT min(encoded_key) FROM briskdb_global_index_unique_keys
                            WHERE index_id = ?1
                         )",
                        [id],
                    )
                    .unwrap();
            }
            GlobalIndexValidationIssueKind::DanglingUniqueReservation => {
                connection
                    .execute(
                        "INSERT INTO briskdb_global_index_unique_keys
                            (index_id, encoded_key, source_shard, source_locator)
                         SELECT index_id, CAST(encoded_key || x'00' AS BLOB),
                                source_shard, source_locator
                         FROM briskdb_global_index_unique_keys
                         WHERE index_id = ?1 ORDER BY encoded_key LIMIT 1",
                        [id],
                    )
                    .unwrap();
            }
            GlobalIndexValidationIssueKind::MismatchedUniqueReservation => {
                connection
                    .execute(
                        "UPDATE briskdb_global_index_unique_keys
                         SET source_locator = x'4252494c00000001000101000000000000270f'
                         WHERE index_id = ?1 AND encoded_key = (
                            SELECT min(encoded_key) FROM briskdb_global_index_unique_keys
                            WHERE index_id = ?1
                         )",
                        [id],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        drop(connection);
        assert_finding(&mut database, index_id, expected);
        assert_eq!(
            database.repair_global_index(index_id).unwrap_err().kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            database
                .rebuild_global_index(index_id)
                .unwrap()
                .indexed_rows(),
            12
        );
        assert!(database.validate_global_index(index_id).unwrap().is_valid());
    }
}

#[test]
fn missing_physical_storage_is_reported_and_rebuildable() {
    let temp = tempfile::tempdir().unwrap();
    let (mut database, index_id) = setup(temp.path(), false, 20);
    std::fs::rename(
        temp.path().join("global-indexes/global.sqlite"),
        temp.path().join("global-indexes/lost.sqlite"),
    )
    .unwrap();
    assert_finding(
        &mut database,
        index_id,
        GlobalIndexValidationIssueKind::MissingPhysicalStorage,
    );
    assert_eq!(
        database
            .rebuild_global_index(index_id)
            .unwrap()
            .indexed_rows(),
        20
    );
}
