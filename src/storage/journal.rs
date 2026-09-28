//! Checked SQLite journal and connection-durability policy.
//!
//! This is not a storage-profile selector. Callers must validate ownership and
//! the persisted format before initializing a journal. In particular, a normal
//! reopen must validate the expected mode, not silently convert a database.

use rusqlite::Connection;

use crate::{
    core::{EngineError, EngineErrorKind, EngineResult},
    sqlite_error,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JournalMode {
    Wal,
    Delete,
    Persist,
}

impl JournalMode {
    const fn name(self) -> &'static str {
        match self {
            Self::Wal => "WAL",
            Self::Delete => "DELETE",
            Self::Persist => "PERSIST",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct JournalPolicy {
    mode: JournalMode,
    synchronous: i64,
}

impl JournalPolicy {
    pub(super) const fn is_wal(self) -> bool {
        matches!(self.mode, JournalMode::Wal)
    }

    pub(super) const LOCAL: Self = Self {
        mode: JournalMode::Wal,
        synchronous: 2, // FULL: preserve the existing local data-file contract.
    };

    #[cfg(any(feature = "auth-scram", test))]
    pub(super) const SECURITY: Self = Self {
        mode: JournalMode::Delete,
        synchronous: 2, // Existing security stores additionally use fullfsync.
    };

    // Validated persisted rollback policies. Not evidence that a mount is qualified.
    pub(super) const NFS_DELETE: Self = Self {
        mode: JournalMode::Delete,
        synchronous: 3,
    };

    pub(super) const NFS_PERSIST: Self = Self {
        mode: JournalMode::Persist,
        synchronous: 3,
    };

    /// Connection-local only: never changes the persistent journal mode,
    /// native locking, busy handler, transaction state, or application data.
    /// Read back the effective value because SQLite can ignore some pragmas.
    pub(super) fn configure_durability(self, connection: &Connection) -> EngineResult<()> {
        // Revalidation may share an existing read snapshot. SQLite forbids
        // changing synchronous inside a transaction: only verify the policy
        // that its opener established before beginning that snapshot.
        if connection.is_autocommit() {
            connection
                .pragma_update(Some("main"), "synchronous", self.synchronous)
                .map_err(sqlite_error::storage)?;
        }
        let effective: i64 = connection
            .pragma_query_value(Some("main"), "synchronous", |row| row.get(0))
            .map_err(sqlite_error::storage)?;
        if effective != self.synchronous {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                format!(
                    "SQLite retained synchronous={effective}, expected {}",
                    self.synchronous
                ),
            ));
        }
        Ok(())
    }

    /// Only initialization or an explicitly coordinated transition may call
    /// this. It does not authorize conversion of an already-owned database.
    pub(super) fn initialize_mode(
        self,
        connection: &Connection,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        let effective = connection
            .pragma_update_and_check(Some("main"), "journal_mode", self.mode.name(), |row| {
                row.get::<_, String>(0)
            })
            .map_err(sqlite_error::storage)?;
        self.check_mode(&effective, mismatch_kind, description)
    }

    /// Validation is observational; a wrong mode is never repaired here.
    #[cfg(test)]
    pub(super) fn require_mode(
        self,
        connection: &Connection,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        let effective: String = connection
            .pragma_query_value(Some("main"), "journal_mode", |row| row.get(0))
            .map_err(sqlite_error::storage)?;
        self.check_mode(&effective, mismatch_kind, description)
    }

    /// Reapply a validated rollback policy on this connection, without ever
    /// converting WAL storage. PERSIST is connection-local: a fresh handle
    /// normally reports DELETE even when the persisted BriskDB policy is
    /// PERSIST. Read-only handles only validate the rollback family and never
    /// change it. Native SQLite locks and hot-journal recovery remain enabled.
    pub(super) fn configure_existing(
        self,
        connection: &Connection,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        // A metadata read can trigger hot-journal recovery. Set durability
        // before that read, not after recovery has already written pages.
        self.configure_durability(connection)?;
        self.configure_existing_mode(connection, mismatch_kind, description)
    }

    /// Call after configuring durability, outside a transaction. Kept separate
    /// so identity validation can precede connection-local journal selection.
    pub(super) fn configure_existing_mode(
        self,
        connection: &Connection,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        let effective: String = connection
            .pragma_query_value(Some("main"), "journal_mode", |row| row.get(0))
            .map_err(sqlite_error::storage)?;
        self.check_reopened_mode(&effective, mismatch_kind, description)?;
        if !self.is_wal()
            && !connection
                .is_readonly(rusqlite::MAIN_DB)
                .map_err(sqlite_error::storage)?
            && !effective.eq_ignore_ascii_case(self.mode.name())
        {
            self.initialize_mode(connection, mismatch_kind, description)?;
        }
        Ok(())
    }

    pub(super) fn check_reopened_mode(
        self,
        effective: &str,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        if matches!(self.mode, JournalMode::Persist) && effective.eq_ignore_ascii_case("delete") {
            Ok(())
        } else {
            self.check_mode(effective, mismatch_kind, description)
        }
    }

    pub(super) fn require_reopened_mode(
        self,
        connection: &Connection,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        let effective: String = connection
            .pragma_query_value(Some("main"), "journal_mode", |row| row.get(0))
            .map_err(sqlite_error::storage)?;
        self.check_reopened_mode(&effective, mismatch_kind, description)
    }

    pub(super) fn check_mode(
        self,
        effective: &str,
        mismatch_kind: EngineErrorKind,
        description: &str,
    ) -> EngineResult<()> {
        if effective.eq_ignore_ascii_case(self.mode.name()) {
            Ok(())
        } else {
            Err(EngineError::new(
                mismatch_kind,
                format!(
                    "{description} uses journal mode {effective}, expected {}",
                    self.mode.name()
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    use std::{fs, process::Command, time::Duration};

    const POLICIES: [JournalPolicy; 4] = [
        JournalPolicy::LOCAL,
        JournalPolicy::SECURITY,
        JournalPolicy::NFS_DELETE,
        JournalPolicy::NFS_PERSIST,
    ];

    fn initialize(connection: &Connection, policy: JournalPolicy) {
        policy.configure_durability(connection).unwrap();
        policy
            .initialize_mode(connection, EngineErrorKind::FailedPrecondition, "test file")
            .unwrap();
    }

    fn text_pragma(connection: &Connection, name: &str) -> String {
        connection
            .pragma_query_value(Some("main"), name, |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn persisted_rollback_policy_is_reapplied_without_converting_wal() {
        for policy in [JournalPolicy::NFS_DELETE, JournalPolicy::NFS_PERSIST] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let original = Connection::open(file.path()).unwrap();
            initialize(&original, policy);
            original
                .execute_batch("CREATE TABLE item(value); INSERT INTO item VALUES (7)")
                .unwrap();
            drop(original);
            let reopened = Connection::open(file.path()).unwrap();
            assert_eq!(text_pragma(&reopened, "journal_mode"), "delete");
            policy
                .configure_existing(&reopened, EngineErrorKind::FailedPrecondition, "test")
                .unwrap();
            assert!(
                text_pragma(&reopened, "journal_mode").eq_ignore_ascii_case(policy.mode.name())
            );
            assert_eq!(
                reopened
                    .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                    .unwrap(),
                3
            );
            assert_eq!(text_pragma(&reopened, "locking_mode"), "normal");
            reopened
                .execute_batch("INSERT INTO item VALUES (8)")
                .unwrap();
            drop(reopened);
            let read_only = Connection::open_with_flags(
                file.path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            policy
                .configure_existing(&read_only, EngineErrorKind::FailedPrecondition, "test")
                .unwrap();
            assert_eq!(text_pragma(&read_only, "journal_mode"), "delete");
            assert_eq!(
                read_only
                    .query_row("SELECT COUNT(*) FROM item", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                2
            );
            drop(read_only);

            let wal = Connection::open(file.path()).unwrap();
            initialize(&wal, JournalPolicy::LOCAL);
            let before = fs::read(file.path()).unwrap();
            assert_eq!(
                policy
                    .configure_existing(&wal, EngineErrorKind::FailedPrecondition, "test")
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::FailedPrecondition
            );
            assert_eq!(text_pragma(&wal, "journal_mode"), "wal");
            assert_eq!(fs::read(file.path()).unwrap(), before);
        }
    }

    #[test]
    fn snapshot_revalidation_checks_preconfigured_durability_without_changing_it() {
        let mut connection = Connection::open_in_memory().unwrap();
        JournalPolicy::NFS_PERSIST
            .configure_durability(&connection)
            .unwrap();
        let transaction = connection.transaction().unwrap();
        JournalPolicy::NFS_PERSIST
            .configure_durability(&transaction)
            .unwrap();
        assert_eq!(
            JournalPolicy::LOCAL
                .configure_durability(&transaction)
                .unwrap_err()
                .kind(),
            EngineErrorKind::FailedPrecondition
        );
        assert_eq!(
            transaction
                .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                .unwrap(),
            3
        );
    }

    #[test]
    fn checked_policies_preserve_native_locks_and_other_connection_settings() {
        for policy in POLICIES {
            let file = tempfile::NamedTempFile::new().unwrap();
            let connection = Connection::open(file.path()).unwrap();
            connection.busy_timeout(Duration::from_millis(37)).unwrap();
            connection
                .pragma_update(None, "foreign_keys", true)
                .unwrap();
            initialize(&connection, policy);
            assert!(
                text_pragma(&connection, "journal_mode").eq_ignore_ascii_case(policy.mode.name())
            );
            assert_eq!(text_pragma(&connection, "locking_mode"), "normal");
            for (name, expected) in [
                ("synchronous", policy.synchronous),
                ("busy_timeout", 37),
                ("foreign_keys", 1),
            ] {
                assert_eq!(
                    connection
                        .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                        .unwrap(),
                    expected
                );
            }
            policy
                .require_mode(
                    &connection,
                    EngineErrorKind::FailedPrecondition,
                    "test file",
                )
                .unwrap();
        }
    }

    #[test]
    fn wrong_mode_validation_never_converts_or_writes_the_database() {
        for actual in POLICIES {
            let file = tempfile::NamedTempFile::new().unwrap();
            let connection = Connection::open(file.path()).unwrap();
            initialize(&connection, actual);
            connection
                .execute_batch("CREATE TABLE sample(value); INSERT INTO sample VALUES (7)")
                .unwrap();
            let before = fs::read(file.path()).unwrap();
            for expected in POLICIES {
                let result = expected.require_mode(
                    &connection,
                    EngineErrorKind::DataCorruption,
                    "test file",
                );
                if expected.mode == actual.mode {
                    result.unwrap();
                } else {
                    assert_eq!(result.unwrap_err().kind(), EngineErrorKind::DataCorruption);
                }
                assert!(
                    text_pragma(&connection, "journal_mode")
                        .eq_ignore_ascii_case(actual.mode.name())
                );
                assert_eq!(fs::read(file.path()).unwrap(), before);
            }
        }
    }

    #[test]
    fn connection_durability_configuration_does_not_select_a_journal() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        initialize(&connection, JournalPolicy::LOCAL);
        for policy in POLICIES {
            policy.configure_durability(&connection).unwrap();
            assert_eq!(text_pragma(&connection, "journal_mode"), "wal");
        }
    }

    #[test]
    fn initialization_checks_the_effective_mode_when_sqlite_ignores_it() {
        for policy in POLICIES {
            let connection = Connection::open_in_memory().unwrap();
            let error = policy
                .initialize_mode(
                    &connection,
                    EngineErrorKind::FailedPrecondition,
                    "test file",
                )
                .unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
            assert_eq!(text_pragma(&connection, "journal_mode"), "memory");
        }
    }

    #[test]
    fn durability_checks_the_effective_value_when_sqlite_ignores_it() {
        for policy in POLICIES {
            let connection = Connection::open_in_memory().unwrap();
            connection.pragma_update(None, "synchronous", 0).unwrap();
            connection
                .authorizer(Some(|context: AuthContext<'_>| match context.action {
                    AuthAction::Pragma {
                        pragma_name: "synchronous",
                        pragma_value: Some(_),
                    } => Authorization::Ignore,
                    _ => Authorization::Allow,
                }))
                .unwrap();
            assert_eq!(
                policy.configure_durability(&connection).unwrap_err().kind(),
                EngineErrorKind::FailedPrecondition
            );
        }
    }

    #[test]
    fn policies_only_configure_main_not_attached_or_temporary_databases() {
        let main = tempfile::NamedTempFile::new().unwrap();
        let attached = tempfile::NamedTempFile::new().unwrap();
        let connection = Connection::open(main.path()).unwrap();
        connection
            .execute(
                "ATTACH DATABASE ?1 AS secondary",
                [attached.path().to_str().unwrap()],
            )
            .unwrap();
        connection
            .execute_batch("CREATE TEMP TABLE temporary(value)")
            .unwrap();
        for policy in POLICIES {
            initialize(&connection, policy);
            assert_eq!(
                connection
                    .query_row("PRAGMA secondary.journal_mode", [], |row| row
                        .get::<_, String>(0))
                    .unwrap(),
                "delete"
            );
            assert_eq!(
                connection
                    .query_row("PRAGMA temp.synchronous", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    }

    #[test]
    fn rollback_candidates_keep_sqlite_peer_writer_exclusion() {
        for policy in [JournalPolicy::NFS_DELETE, JournalPolicy::NFS_PERSIST] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let first = Connection::open(file.path()).unwrap();
            initialize(&first, policy);
            first
                .execute_batch("CREATE TABLE sample(value); INSERT INTO sample VALUES (1)")
                .unwrap();
            let second = Connection::open(file.path()).unwrap();
            initialize(&second, policy);
            second.busy_timeout(Duration::ZERO).unwrap();
            first
                .execute_batch("BEGIN IMMEDIATE; UPDATE sample SET value=2")
                .unwrap();
            let error = second.execute("UPDATE sample SET value=3", []).unwrap_err();
            assert_eq!(sqlite_error::storage(error).kind(), EngineErrorKind::Busy);
            first.execute_batch("ROLLBACK").unwrap();
            second.execute("UPDATE sample SET value=3", []).unwrap();
            assert_eq!(
                first
                    .query_row("SELECT value FROM sample", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                3
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn persist_reuses_the_journal_inode_across_commits() {
        use std::os::unix::fs::MetadataExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sample.sqlite");
        let connection = Connection::open(&path).unwrap();
        initialize(&connection, JournalPolicy::NFS_PERSIST);
        connection
            .execute_batch("CREATE TABLE sample(value); INSERT INTO sample VALUES (1)")
            .unwrap();
        let journal = directory.path().join("sample.sqlite-journal");
        let identity = fs::metadata(&journal).unwrap().ino();
        for value in 2..6 {
            connection
                .execute("UPDATE sample SET value=?1", [value])
                .unwrap();
            assert_eq!(fs::metadata(&journal).unwrap().ino(), identity);
            assert_eq!(&fs::read(&journal).unwrap()[..28], &[0; 28]);
        }
        assert!(!directory.path().join("sample.sqlite-wal").exists());
        assert!(!directory.path().join("sample.sqlite-shm").exists());
        drop(connection);
        let reopened = Connection::open(&path).unwrap();
        // Unlike WAL, PERSIST is connection-local. Every NFS-profile opener
        // must explicitly configure it after validating the persisted profile.
        assert_eq!(text_pragma(&reopened, "journal_mode"), "delete");
        initialize(&reopened, JournalPolicy::NFS_PERSIST);
        reopened.execute("UPDATE sample SET value=6", []).unwrap();
        assert_eq!(fs::metadata(&journal).unwrap().ino(), identity);
    }

    #[test]
    fn rollback_candidates_recover_process_exit_before_and_after_commit() {
        const CHILD: &str = "BRISKDB_TEST_JOURNAL_CRASH_PATH";
        if let Ok(path) = std::env::var(CHILD) {
            let policy = if std::env::var("BRISKDB_TEST_JOURNAL_MODE").unwrap() == "PERSIST" {
                JournalPolicy::NFS_PERSIST
            } else {
                JournalPolicy::NFS_DELETE
            };
            let connection = Connection::open(&path).unwrap();
            initialize(&connection, policy);
            connection.execute_batch("PRAGMA cache_size=2; BEGIN IMMEDIATE; UPDATE sample SET value=2, payload=zeroblob(2048)").unwrap();
            if std::env::var("BRISKDB_TEST_JOURNAL_COMMIT").unwrap() == "yes" {
                connection.execute_batch("COMMIT").unwrap();
            } else {
                // The small pager cache forces dirty-page spill, producing a
                // real hot journal rather than only unflushed process memory.
                let journal = fs::read(format!("{path}-journal")).unwrap();
                assert!(journal.len() > 512);
                assert_ne!(&journal[..8], &[0; 8]);
            }
            std::process::exit(73);
        }
        for policy in [JournalPolicy::NFS_DELETE, JournalPolicy::NFS_PERSIST] {
            for commit in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("sample.sqlite");
                let connection = Connection::open(&path).unwrap();
                initialize(&connection, policy);
                connection.execute_batch("CREATE TABLE sample(value INTEGER, payload BLOB); WITH RECURSIVE sequence(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM sequence WHERE n<32) INSERT INTO sample SELECT 1, zeroblob(2048) FROM sequence").unwrap();
                drop(connection);
                let status = Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "storage::journal::tests::rollback_candidates_recover_process_exit_before_and_after_commit", "--nocapture"])
                    .env(CHILD, &path)
                    .env("BRISKDB_TEST_JOURNAL_MODE", policy.mode.name())
                    .env("BRISKDB_TEST_JOURNAL_COMMIT", if commit { "yes" } else { "no" })
                    .status().unwrap();
                assert_eq!(status.code(), Some(73));
                let reopened = Connection::open(&path).unwrap();
                initialize(&reopened, policy);
                assert_eq!(
                    reopened
                        .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                        .unwrap(),
                    "ok"
                );
                let (count, minimum, maximum): (i64, i64, i64) = reopened
                    .query_row(
                        "SELECT count(*), min(value), max(value) FROM sample",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
                let expected = if commit { 2 } else { 1 };
                assert_eq!((count, minimum, maximum), (32, expected, expected));
                assert!(!directory.path().join("sample.sqlite-wal").exists());
                assert!(!directory.path().join("sample.sqlite-shm").exists());
            }
        }
    }
}
