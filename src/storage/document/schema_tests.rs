//! Exact physical-schema validation without repeated secondary-schema queries.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

use super::*;

fn snapshot(connection: &Connection) -> Vec<(String, String, Option<String>)> {
    connection
        .prepare("SELECT type, name, sql FROM sqlite_schema ORDER BY type, name")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn ready_document_schema_uses_exactly_two_inspections_per_call_without_cached_authority() {
    let mut connection = Connection::open_in_memory().unwrap();
    ensure_schema(&mut connection).unwrap();
    let selects = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&selects);
    connection
        .authorizer(Some(move |context: AuthContext<'_>| {
            if matches!(context.action, AuthAction::Select) {
                observed.fetch_add(1, Ordering::Relaxed);
            }
            Authorization::Allow
        }))
        .unwrap();
    for _ in 0..8 {
        for operation in 0..3 {
            selects.store(0, Ordering::Relaxed);
            match operation {
                0 => require_schema(&connection).unwrap(),
                1 => ensure_schema(&mut connection).unwrap(),
                _ => assert!(validate_optional_schema(&connection).unwrap()),
            }
            assert_eq!(selects.load(Ordering::Relaxed), 2);
        }
    }
    connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
    connection
        .execute_batch("DROP INDEX briskdb_document_index_entries_by_record_v1")
        .unwrap();
    assert_eq!(
        require_schema(&connection).unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
}

#[test]
fn absent_and_legacy_record_only_schemas_keep_their_distinct_upgrade_contracts() {
    for legacy in [false, true] {
        let mut connection = Connection::open_in_memory().unwrap();
        if legacy {
            connection.execute_batch(RECORDS_SCHEMA_SQL).unwrap();
        }
        let expected = if legacy {
            DocumentSchemaPresence::RecordsOnly
        } else {
            DocumentSchemaPresence::Absent
        };
        assert_eq!(inspect_schema(&connection).unwrap(), expected);
        assert_eq!(validate_optional_schema(&connection).unwrap(), legacy);
        let before = snapshot(&connection);
        assert_eq!(
            require_schema(&connection).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(
            snapshot(&connection),
            before,
            "ordinary operations must not provision storage"
        );
        ensure_schema(&mut connection).unwrap();
        assert_eq!(
            inspect_schema(&connection).unwrap(),
            DocumentSchemaPresence::Complete
        );
        require_schema(&connection).unwrap();
        let ready = snapshot(&connection);
        ensure_schema(&mut connection).unwrap();
        assert_eq!(snapshot(&connection), ready);
        if legacy {
            assert!(
                ready.contains(&before[0]),
                "legacy record schema is not rewritten"
            );
        }
    }
}

#[test]
fn malformed_or_orphaned_document_schemas_fail_all_entry_points_without_repair() {
    for damage in [
        "DROP INDEX briskdb_document_index_entries_by_record_v1",
        "DROP INDEX briskdb_document_index_entries_by_record_v1; CREATE INDEX briskdb_document_index_entries_by_record_v1 ON briskdb_document_index_entries_v1 (id_key)",
        "ALTER TABLE briskdb_documents_v1 ADD COLUMN unexpected INTEGER",
        "ALTER TABLE briskdb_document_index_entries_v1 ADD COLUMN unexpected INTEGER",
        "DROP TABLE briskdb_documents_v1",
        "DROP TABLE briskdb_documents_v1; CREATE VIEW briskdb_documents_v1 AS SELECT 1 AS collection_id",
    ] {
        let mut connection = Connection::open_in_memory().unwrap();
        ensure_schema(&mut connection).unwrap();
        connection.execute_batch(damage).unwrap();
        let before = snapshot(&connection);
        for error in [
            validate_optional_schema(&connection).unwrap_err(),
            require_schema(&connection).unwrap_err(),
            ensure_schema(&mut connection).unwrap_err(),
        ] {
            assert_eq!(error.kind(), EngineErrorKind::DataCorruption, "{damage}");
        }
        assert_eq!(snapshot(&connection), before, "{damage}");
    }
}

#[test]
fn failed_secondary_inspection_is_not_treated_as_valid_or_missing_schema() {
    let mut connection = Connection::open_in_memory().unwrap();
    ensure_schema(&mut connection).unwrap();
    let before = snapshot(&connection);
    for operation in 0..3 {
        let selects = Arc::new(AtomicUsize::new(0));
        connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if matches!(context.action, AuthAction::Select)
                    && selects.fetch_add(1, Ordering::Relaxed) == 1
                {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }))
            .unwrap();
        match operation {
            0 => assert!(require_schema(&connection).is_err()),
            1 => assert!(ensure_schema(&mut connection).is_err()),
            _ => assert!(validate_optional_schema(&connection).is_err()),
        }
        connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        assert_eq!(snapshot(&connection), before);
    }
}
