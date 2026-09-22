//! Per-input commit boundaries for inserts and exact-ID deletes.

use rusqlite::{Connection, Transaction, TransactionBehavior};

use super::ensure_document_cpu_active;
use crate::{
    core::{CancellationToken, EngineError, EngineErrorKind, EngineResult, OperationControl},
    sqlite_error,
};

/// Rollback evidence is local to one transaction, never to an enclosing batch.
/// It must not escape as a command-wide no-changes certificate: earlier inputs
/// or shards may already have committed.
#[derive(Debug)]
pub(super) struct WriteTransactionError {
    error: EngineError,
    rolled_back: bool,
}

impl WriteTransactionError {
    fn uncertain(error: EngineError) -> Self {
        Self {
            error,
            rolled_back: false,
        }
    }

    pub(super) fn is_rolled_back_duplicate(&self) -> bool {
        self.rolled_back && self.error.kind() == EngineErrorKind::UniqueViolation
    }

    pub(super) fn into_engine_error(self) -> EngineError {
        self.error
    }
}

pub(super) fn write_transaction<T>(
    connection: &Connection,
    cancellation: &CancellationToken,
    control: &OperationControl,
    write: impl FnOnce(&Transaction<'_>) -> EngineResult<T>,
) -> Result<T, WriteTransactionError> {
    ensure_document_cpu_active(cancellation, control).map_err(WriteTransactionError::uncertain)?;
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
        .map_err(sqlite_error::statement)
        .map_err(WriteTransactionError::uncertain)?;
    let result = write(&transaction).and_then(|value| {
        #[cfg(test)]
        crash_checkpoint("after-write");
        ensure_document_cpu_active(cancellation, control)?;
        Ok(value)
    });
    match result {
        Ok(value) => {
            #[cfg(test)]
            crash_checkpoint("before-commit");
            transaction
                .commit()
                .map_err(sqlite_error::statement)
                .map_err(WriteTransactionError::uncertain)?;
            #[cfg(test)]
            crash_checkpoint("after-commit");
            // A known successful commit wins a late cancellation race.
            Ok(value)
        }
        Err(error) => {
            // Drop's best-effort rollback is not evidence for continuing a
            // batch after an error. Rollback/commit failures are always fatal.
            transaction
                .rollback()
                .map_err(sqlite_error::statement)
                .map_err(WriteTransactionError::uncertain)?;
            Err(WriteTransactionError {
                error,
                rolled_back: true,
            })
        }
    }
}

#[cfg(test)]
fn crash_checkpoint(point: &str) {
    if std::env::var("BRISKDB_TEST_DOCUMENT_WRITE_CRASH")
        .ok()
        .as_deref()
        == Some(point)
    {
        // Only isolated subprocess tests configure this hook. Do not run
        // destructors: exercise SQLite/WAL recovery after process death.
        std::process::exit(73);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{CancellationReason, Engine, RequestContext},
        document::{
            BsonDocument, BsonValue, DocumentCollectionOptions, DocumentCommand,
            DocumentDeleteRequest, DocumentFilter, DocumentInsertRequest, DocumentMutationScope,
            DocumentNamespace, DocumentRequest, DocumentRequestId, DocumentWriteOptions,
            DocumentWriteRollback,
        },
        storage::Storage,
    };
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

    fn fixture() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        connection
    }

    fn insert(connection: &Connection, id: i64) -> EngineResult<()> {
        connection
            .execute("INSERT INTO effects VALUES (?1)", [id])
            .map_err(sqlite_error::statement)?;
        Ok(())
    }

    fn count(connection: &Connection) -> i64 {
        connection
            .query_row("SELECT count(*) FROM effects", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn duplicate_rolls_back_all_statement_effects_without_certifying_the_batch() {
        let connection = fixture();
        let cancellation = CancellationToken::new();
        let control = OperationControl::new(None);
        write_transaction(&connection, &cancellation, &control, |transaction| {
            insert(transaction, 1)
        })
        .unwrap();
        let error = write_transaction(&connection, &cancellation, &control, |transaction| {
            insert(transaction, 2)?;
            insert(transaction, 1)
        })
        .unwrap_err();
        assert!(error.is_rolled_back_duplicate());
        assert!(!DocumentWriteRollback::is_certified(
            &error.into_engine_error()
        ));
        assert!(connection.is_autocommit());
        assert_eq!(count(&connection), 1, "earlier committed input survives");
        write_transaction(&connection, &cancellation, &control, |transaction| {
            insert(transaction, 2)?;
            insert(transaction, 3)
        })
        .unwrap();
        assert_eq!(
            count(&connection),
            3,
            "lease can continue after proven rollback"
        );
    }

    #[test]
    fn precommit_cancellation_and_deadline_roll_back_all_effects() {
        for deadline in [false, true] {
            let connection = fixture();
            let cancellation = CancellationToken::new();
            let control = OperationControl::new(None);
            let error = write_transaction(&connection, &cancellation, &control, |transaction| {
                insert(transaction, 1)?;
                insert(transaction, 2)?;
                if deadline {
                    control.request_cancel(CancellationReason::DeadlineExceeded);
                } else {
                    cancellation.cancel();
                }
                Ok(())
            })
            .unwrap_err();
            assert!(error.rolled_back);
            assert!(!error.is_rolled_back_duplicate());
            assert_eq!(
                error.into_engine_error().kind(),
                if deadline {
                    EngineErrorKind::DeadlineExceeded
                } else {
                    EngineErrorKind::Cancelled
                }
            );
            assert!(connection.is_autocommit());
            assert_eq!(count(&connection), 0);
        }
    }

    #[test]
    fn successful_commit_wins_late_cancellation() {
        let connection = fixture();
        let control = OperationControl::new(None);
        let hook_control = control.clone();
        connection
            .commit_hook(Some(move || {
                hook_control.request_cancel(CancellationReason::Cancelled);
                false
            }))
            .unwrap();
        let result = write_transaction(
            &connection,
            &CancellationToken::new(),
            &control,
            |transaction| {
                insert(transaction, 1)?;
                Ok(42)
            },
        )
        .map_err(WriteTransactionError::into_engine_error);
        assert_eq!(control.complete(result).unwrap(), 42);
        assert_eq!(count(&connection), 1);
    }

    #[test]
    fn begin_and_commit_failures_never_allow_duplicate_continuation() {
        let connection = fixture();
        let cancellation = CancellationToken::new();
        let control = OperationControl::new(None);
        let outer =
            Transaction::new_unchecked(&connection, TransactionBehavior::Immediate).unwrap();
        let error = write_transaction(
            &connection,
            &cancellation,
            &control,
            |_| -> EngineResult<()> { panic!("write must not run when BEGIN fails") },
        )
        .unwrap_err();
        assert!(!error.rolled_back);
        assert!(!error.is_rolled_back_duplicate());
        outer.rollback().unwrap();
        connection.commit_hook(Some(|| true)).unwrap();
        let error = write_transaction(&connection, &cancellation, &control, |transaction| {
            insert(transaction, 1)
        })
        .unwrap_err();
        assert!(!error.rolled_back);
        assert!(!error.is_rolled_back_duplicate());
        assert!(connection.is_autocommit());
        assert_eq!(count(&connection), 0);
    }

    #[test]
    fn rollback_failure_is_fatal_even_if_the_write_error_was_a_duplicate() {
        let connection = fixture();
        connection
            .authorizer(Some(|context: AuthContext<'_>| {
                if matches!(
                    context.action,
                    AuthAction::Transaction {
                        operation: TransactionOperation::Rollback
                    }
                ) {
                    Authorization::Deny
                } else {
                    Authorization::Allow
                }
            }))
            .unwrap();
        let error = write_transaction(
            &connection,
            &CancellationToken::new(),
            &OperationControl::new(None),
            |transaction| {
                insert(transaction, 1)?;
                insert(transaction, 1)
            },
        )
        .unwrap_err();
        assert!(!error.rolled_back);
        assert!(!error.is_rolled_back_duplicate());
        assert!(!DocumentWriteRollback::is_certified(
            &error.into_engine_error()
        ));
        assert!(!connection.is_autocommit());
        connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        connection.execute_batch("ROLLBACK").unwrap();
        assert_eq!(count(&connection), 0);
    }

    fn document(id: i32) -> BsonDocument {
        BsonDocument::from_entries([("_id", BsonValue::Int32(id))]).unwrap()
    }

    #[tokio::test]
    async fn document_write_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_TEST_DOCUMENT_WRITE_ROOT") else {
            return;
        };
        let engine = Engine::open(root, 2).await.unwrap();
        let namespace = DocumentNamespace::new("app", "items").unwrap();
        let command = match std::env::var("BRISKDB_TEST_DOCUMENT_WRITE_MODE")
            .unwrap()
            .as_str()
        {
            "insert" => DocumentCommand::Insert(
                DocumentInsertRequest::new(
                    namespace,
                    vec![document(2), document(3)],
                    DocumentWriteOptions::new(),
                )
                .unwrap(),
            ),
            "delete" => DocumentCommand::Delete(DocumentDeleteRequest::new(
                namespace,
                DocumentFilter::new(document(1)).unwrap(),
                DocumentMutationScope::One,
                DocumentWriteOptions::new(),
            )),
            _ => panic!("unknown crash test mode"),
        };
        engine
            .execute_document(
                &engine.session(),
                DocumentRequest::new(
                    DocumentRequestId::new([1; 16]).unwrap(),
                    RequestContext::new(),
                    command,
                ),
            )
            .await
            .unwrap();
        panic!("configured crash checkpoint was not reached");
    }

    #[test]
    fn document_write_crashes_preserve_exact_committed_prefix() {
        for mode in ["insert", "delete"] {
            for checkpoint in ["after-write", "before-commit", "after-commit"] {
                let temp = tempfile::tempdir().unwrap();
                let storage = Storage::open(temp.path(), 2).unwrap();
                let collection = storage
                    .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
                    .unwrap();
                storage
                    .insert_document(collection.id(), &document(1))
                    .unwrap();
                drop(storage);
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "core::engine::document_engine::write_transaction::tests::document_write_crash_child", "--nocapture"])
                    .env("BRISKDB_TEST_DOCUMENT_WRITE_ROOT", temp.path())
                    .env("BRISKDB_TEST_DOCUMENT_WRITE_MODE", mode)
                    .env("BRISKDB_TEST_DOCUMENT_WRITE_CRASH", checkpoint)
                    .output().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(73),
                    "{mode}/{checkpoint}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let storage = Storage::open(temp.path(), 2).unwrap();
                let committed = checkpoint == "after-commit";
                assert_eq!(
                    storage
                        .get_document(collection.id(), &BsonValue::Int32(1))
                        .unwrap()
                        .is_some(),
                    !(mode == "delete" && committed)
                );
                assert_eq!(
                    storage
                        .get_document(collection.id(), &BsonValue::Int32(2))
                        .unwrap()
                        .is_some(),
                    mode == "insert" && committed
                );
                assert!(
                    storage
                        .get_document(collection.id(), &BsonValue::Int32(3))
                        .unwrap()
                        .is_none(),
                    "second input never started"
                );
                // A recovered root accepts another write without stale leases.
                storage
                    .insert_document(collection.id(), &document(4))
                    .unwrap();
            }
        }
    }
}
