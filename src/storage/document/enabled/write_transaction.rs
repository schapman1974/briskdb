//! A document transaction owns any cross-shard writer fence until SQLite ends.

use std::{ops::Deref, time::Instant};

use super::*;
use crate::storage::{CONNECTION_BUSY_TIMEOUT, process_lock::document_write::DocumentWriteFence};

pub(crate) struct DocumentWriteTransaction<'c> {
    transaction: Option<Transaction<'c>>,
    connection: &'c Connection,
    storage: Storage,
    collection: DocumentCollectionId,
    shard: u16,
    fence: Option<DocumentWriteFence>,
}

impl std::fmt::Debug for DocumentWriteTransaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentWriteTransaction")
            .field("collection", &self.collection)
            .field("shard", &self.shard)
            .field("fenced", &self.fence.is_some())
            .finish_non_exhaustive()
    }
}

impl Storage {
    /// Call only with schema admission held and before opening a shard write
    /// transaction. Pending declarations never authorize uniqueness or locks.
    pub(crate) fn begin_document_write<'c>(
        &self,
        connection: &'c Connection,
        collection: DocumentCollectionId,
        shard: u16,
        cancellation: &CancellationToken,
        control: Option<&OperationControl>,
    ) -> EngineResult<DocumentWriteTransaction<'c>> {
        let fenced = self
            .active_document_indexes(collection)?
            .is_some_and(|indexes| indexes.has_unique_secondary());
        DocumentWriteTransaction::begin(
            self,
            connection,
            collection,
            shard,
            cancellation,
            control,
            fenced,
        )
    }
}

impl<'c> DocumentWriteTransaction<'c> {
    #[allow(clippy::too_many_arguments)]
    fn begin(
        storage: &Storage,
        connection: &'c Connection,
        collection: DocumentCollectionId,
        shard: u16,
        cancellation: &CancellationToken,
        control: Option<&OperationControl>,
        fenced: bool,
    ) -> EngineResult<Self> {
        let check = || {
            if let Some(reason) = control.and_then(OperationControl::reason) {
                return Err(reason.error());
            }
            ensure_document_operation_not_cancelled(cancellation, "before document write admission")
        };
        check()?;
        if !connection.is_autocommit() || shard >= storage.shard_count() {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "document write admission requires an idle, valid shard connection",
            ));
        }
        let fence = if fenced {
            let started = Instant::now();
            loop {
                check()?;
                match DocumentWriteFence::try_acquire(
                    &storage.root,
                    collection,
                    Arc::clone(&storage.schema_coordination.document_write_stripes),
                ) {
                    Ok(fence) => break Some(fence),
                    Err(error)
                        if error.kind() == EngineErrorKind::Busy
                            && started.elapsed() < CONNECTION_BUSY_TIMEOUT =>
                    {
                        // Blocking worker only. No shard transaction or foreign
                        // pool lease is held while waiting for the collection.
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                    Err(error) => return Err(error),
                }
            }
        } else {
            None
        };
        check()?;
        let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Immediate)
            .map_err(sqlite_error::statement)?;
        Ok(Self {
            transaction: Some(transaction),
            connection,
            storage: storage.clone(),
            collection,
            shard,
            fence,
        })
    }

    pub(super) fn require_scope(
        &self,
        storage: &Storage,
        collection: DocumentCollectionId,
        shard: u16,
    ) -> EngineResult<()> {
        if self.collection != collection
            || self.shard != shard
            || !Arc::ptr_eq(
                &self.storage.schema_coordination,
                &storage.schema_coordination,
            )
        {
            return Err(EngineError::new(
                EngineErrorKind::Internal,
                "document mutation does not match its admitted root, collection, and shard",
            ));
        }
        require_write_transaction(self)
    }

    pub(crate) fn commit(mut self) -> rusqlite::Result<()> {
        self.transaction.take().expect("live transaction").commit()
    }

    pub(crate) fn rollback(mut self) -> rusqlite::Result<()> {
        self.transaction
            .take()
            .expect("live transaction")
            .rollback()
    }
}

impl<'c> Deref for DocumentWriteTransaction<'c> {
    type Target = Transaction<'c>;

    fn deref(&self) -> &Self::Target {
        self.transaction.as_ref().expect("live transaction")
    }
}

impl Drop for DocumentWriteTransaction<'_> {
    fn drop(&mut self) {
        // A blocking worker owns this value, not the parent async task. Finish
        // SQLite first even during unwinding, cancellation, or task abandonment.
        drop(self.transaction.take());
        if !self.connection.is_autocommit() {
            self.storage.record_schema_degraded();
            if let Some(fence) = self.fence.take() {
                // Rollback could not be proven. Fail closed until process exit
                // instead of releasing uniqueness protection around live SQL.
                // Keep the degraded coordinator and process root lease alive
                // too: closing the last Engine must not permit same-process
                // reopening or exclusive schema mutation around uncertain SQL.
                std::mem::forget((self.storage.clone(), fence));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::CancellationReason;
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

    fn setup() -> (tempfile::TempDir, Storage, DocumentCollectionId, Connection) {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::open(root.path(), 2).unwrap();
        let collection = storage
            .create_document_collection("app", "items", &DocumentCollectionOptions::empty())
            .unwrap()
            .id();
        let connection = storage.open_unconfigured_shard(0).unwrap();
        connection
            .execute_batch("CREATE TEMP TABLE effects (id INTEGER PRIMARY KEY)")
            .unwrap();
        (root, storage, collection, connection)
    }

    fn begin<'c>(
        storage: &Storage,
        collection: DocumentCollectionId,
        connection: &'c Connection,
    ) -> DocumentWriteTransaction<'c> {
        DocumentWriteTransaction::begin(
            storage,
            connection,
            collection,
            0,
            &CancellationToken::new(),
            None,
            true,
        )
        .unwrap()
    }

    fn claim(
        storage: &Storage,
        collection: DocumentCollectionId,
    ) -> EngineResult<DocumentWriteFence> {
        DocumentWriteFence::try_acquire(
            &storage.root,
            collection,
            Arc::clone(&storage.schema_coordination.document_write_stripes),
        )
    }

    #[test]
    fn fence_survives_until_commit_rollback_drop_and_unwind_finish() {
        for finish in [
            "commit",
            "rollback",
            "drop",
            "unwind",
            "failed-commit",
            "automatic-rollback",
        ] {
            let (_root, storage, collection, connection) = setup();
            // The observer must still see the fence at SQLite's terminal hook.
            let observer = storage.clone();
            connection
                .commit_hook(Some(move || {
                    assert_eq!(
                        claim(&observer, collection).unwrap_err().kind(),
                        EngineErrorKind::Busy
                    );
                    finish == "failed-commit"
                }))
                .unwrap();
            let observer = storage.clone();
            connection
                .rollback_hook(Some(move || {
                    assert_eq!(
                        claim(&observer, collection).unwrap_err().kind(),
                        EngineErrorKind::Busy
                    );
                }))
                .unwrap();
            let transaction = begin(&storage, collection, &connection);
            transaction
                .execute("INSERT INTO effects VALUES (1)", [])
                .unwrap();
            assert_eq!(
                claim(&storage, collection).unwrap_err().kind(),
                EngineErrorKind::Busy
            );
            match finish {
                "commit" => transaction.commit().unwrap(),
                "rollback" => transaction.rollback().unwrap(),
                "failed-commit" => {
                    transaction.commit().unwrap_err();
                }
                "automatic-rollback" => {
                    transaction
                        .execute("INSERT OR ROLLBACK INTO effects VALUES (1)", [])
                        .unwrap_err();
                    assert!(transaction.is_autocommit());
                    assert_eq!(
                        transaction
                            .require_scope(&storage, collection, 0)
                            .unwrap_err()
                            .kind(),
                        EngineErrorKind::Internal
                    );
                    drop(transaction);
                }
                "unwind" => {
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            let _transaction = transaction;
                            panic!("injected worker unwind");
                        }))
                        .is_err()
                    );
                }
                _ => drop(transaction),
            }
            assert!(connection.is_autocommit());
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM effects", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                i64::from(finish == "commit")
            );
            drop(claim(&storage, collection).unwrap());
        }
    }

    #[test]
    fn scope_is_root_collection_and_shard_bound_and_shared_by_reopened_handles() {
        let (root, storage, collection, connection) = setup();
        let peer = Storage::open(root.path(), 2).unwrap();
        let (_other_root, other, _, _) = setup();
        let transaction = begin(&storage, collection, &connection);
        transaction.require_scope(&peer, collection, 0).unwrap();
        for (scope, id, shard) in [
            (&other, collection, 0),
            (
                &storage,
                DocumentCollectionId::from_validated(collection.get() + 1),
                0,
            ),
            (&storage, collection, 1),
        ] {
            assert_eq!(
                transaction
                    .require_scope(scope, id, shard)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::Internal
            );
        }
        assert_eq!(
            claim(&peer, collection).unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        drop(transaction);
        drop(claim(&peer, collection).unwrap());
    }

    #[test]
    fn waiting_for_fence_is_cancellable_and_holds_no_shard_transaction() {
        for reason in [
            CancellationReason::Cancelled,
            CancellationReason::DeadlineExceeded,
        ] {
            let (_root, storage, collection, _connection) = setup();
            let fence = claim(&storage, collection).unwrap();
            let control = OperationControl::new(None);
            let worker_storage = storage.clone();
            let worker_control = Arc::clone(&control);
            let (started, ready) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let connection = worker_storage.open_unconfigured_shard(1).unwrap();
                started.send(()).unwrap();
                let error = DocumentWriteTransaction::begin(
                    &worker_storage,
                    &connection,
                    collection,
                    1,
                    &CancellationToken::new(),
                    Some(&worker_control),
                    true,
                )
                .unwrap_err();
                assert!(connection.is_autocommit());
                error.kind()
            });
            ready
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            // Same shard remains writable while its worker waits for the fence.
            let connection = storage.open_unconfigured_shard(1).unwrap();
            Transaction::new_unchecked(&connection, TransactionBehavior::Immediate)
                .unwrap()
                .rollback()
                .unwrap();
            let start = Instant::now();
            control.request_cancel(reason);
            assert_eq!(worker.join().unwrap(), reason.error().kind());
            assert!(start.elapsed() < std::time::Duration::from_secs(2));
            assert_eq!(
                claim(&storage, collection).unwrap_err().kind(),
                EngineErrorKind::Busy
            );
            drop(fence);
            drop(claim(&storage, collection).unwrap());
        }
    }

    #[test]
    fn unsuccessful_rollback_fails_closed_and_retains_the_fence() {
        let (_root, storage, collection, connection) = setup();
        let coordinator = Arc::downgrade(&storage.schema_coordination);
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
        let transaction = begin(&storage, collection, &connection);
        transaction
            .execute("INSERT INTO effects VALUES (1)", [])
            .unwrap();
        transaction.rollback().unwrap_err();
        assert!(!connection.is_autocommit());
        assert!(storage.enter_schema_operation().is_err());
        assert_eq!(
            claim(&storage, collection).unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        connection
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        connection.execute_batch("ROLLBACK").unwrap();
        // No late cleanup is allowed to guess at restoring root authority.
        assert_eq!(
            claim(&storage, collection).unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        drop(connection);
        drop(storage);
        assert!(
            coordinator.upgrade().is_some(),
            "uncertain cleanup must retain degraded root authority until process exit"
        );
    }

    #[test]
    fn failed_begin_releases_fence_and_pre_cancelled_admission_never_begins() {
        let (_root, storage, collection, connection) = setup();
        let blocker = storage.open_unconfigured_shard(0).unwrap();
        let held = Transaction::new_unchecked(&blocker, TransactionBehavior::Immediate).unwrap();
        connection.busy_timeout(std::time::Duration::ZERO).unwrap();
        let error = DocumentWriteTransaction::begin(
            &storage,
            &connection,
            collection,
            0,
            &CancellationToken::new(),
            None,
            true,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Busy);
        assert!(connection.is_autocommit());
        drop(claim(&storage, collection).unwrap());
        held.rollback().unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = DocumentWriteTransaction::begin(
            &storage,
            &connection,
            collection,
            0,
            &cancellation,
            None,
            true,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::Cancelled);
        assert!(connection.is_autocommit());
        drop(claim(&storage, collection).unwrap());
    }

    #[test]
    fn ordinary_and_pending_unique_indexes_do_not_acquire_a_fence() {
        let (_root, storage, collection, connection) = setup();
        let index = storage
            .declare_document_index(
                collection,
                "unique_value",
                &BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap(),
                true,
            )
            .unwrap();
        let cancellation = CancellationToken::new();
        let transaction = storage
            .begin_document_write(&connection, collection, 0, &cancellation, None)
            .unwrap();
        assert!(transaction.fence.is_none());
        drop(claim(&storage, collection).unwrap());
        transaction.rollback().unwrap();
        // Test-only cache injection exercises prospective Ready authority.
        // Persistent unique activation remains unsupported in this milestone.
        let catalog = storage.document_catalog().unwrap();
        storage
            .publish_document_indexes(
                compile_indexes_with_candidate(&catalog, Some(index.id()), None, &mut || Ok(()))
                    .unwrap(),
            )
            .unwrap();
        let transaction = storage
            .begin_document_write(&connection, collection, 0, &cancellation, None)
            .unwrap();
        assert!(transaction.fence.is_some());
        assert_eq!(
            claim(&storage, collection).unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        transaction.rollback().unwrap();
        storage
            .publish_document_indexes(compile_ready_indexes(&catalog, &mut || Ok(())).unwrap())
            .unwrap();
        drop(claim(&storage, collection).unwrap());
    }

    #[tokio::test]
    async fn aborting_the_parent_does_not_release_a_live_worker_transaction() {
        let (_root, storage, collection, _connection) = setup();
        let worker_storage = storage.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let parent = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let connection = worker_storage.open_unconfigured_shard(0).unwrap();
                let transaction = begin(&worker_storage, collection, &connection);
                ready_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                transaction.rollback().unwrap();
                finished_tx.send(()).unwrap();
            })
            .await
            .unwrap();
        });
        ready_rx.await.unwrap();
        parent.abort();
        assert!(parent.await.unwrap_err().is_cancelled());
        assert_eq!(
            claim(&storage, collection).unwrap_err().kind(),
            EngineErrorKind::Busy
        );
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        drop(claim(&storage, collection).unwrap());
    }

    #[test]
    fn fenced_write_crash_child() {
        let Ok(root) = std::env::var("BRISKDB_TEST_FENCED_WRITE_ROOT") else {
            return;
        };
        let storage = Storage::open(root, 2).unwrap();
        let collection = storage.document_catalog().unwrap().collections()[0].id();
        let cancellation = CancellationToken::new();
        let document = BsonDocument::from_entries([("_id", BsonValue::Int32(42))]).unwrap();
        let prepared = storage.prepare_document_write(&document).unwrap();
        let order = storage
            .reserve_document_natural_orders_for_engine(collection, 1, &cancellation)
            .unwrap();
        let connection = storage.open_unconfigured_shard(prepared.shard()).unwrap();
        let transaction = DocumentWriteTransaction::begin(
            &storage,
            &connection,
            collection,
            prepared.shard(),
            &cancellation,
            None,
            true,
        )
        .unwrap();
        storage
            .insert_prepared_document_on_connection(
                &transaction,
                collection,
                order,
                prepared.shard(),
                &prepared,
                &cancellation,
            )
            .unwrap();
        if std::env::var("BRISKDB_TEST_FENCED_WRITE_COMMIT").unwrap() == "yes" {
            transaction.commit().unwrap();
        }
        std::process::exit(73);
    }

    #[test]
    fn process_death_preserves_commit_boundaries_and_releases_fence() {
        for committed in [false, true] {
            let (root, storage, collection, connection) = setup();
            drop(connection);
            drop(storage);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("storage::document::enabled::write_transaction::tests::fenced_write_crash_child")
                .arg("--nocapture")
                .env("BRISKDB_TEST_FENCED_WRITE_ROOT", root.path())
                .env("BRISKDB_TEST_FENCED_WRITE_COMMIT", if committed { "yes" } else { "no" })
                .output().unwrap();
            assert_eq!(output.status.code(), Some(73), "{output:?}");
            let storage = Storage::open(root.path(), 2).unwrap();
            assert_eq!(
                storage
                    .get_document(collection, &BsonValue::Int32(42))
                    .unwrap()
                    .is_some(),
                committed
            );
            drop(claim(&storage, collection).unwrap());
        }
    }
}
