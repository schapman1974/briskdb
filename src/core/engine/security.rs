//! Explicit security-root startup and immutable authenticated sessions.

use super::*;
use crate::core::security_catalog::{
    DurableSecurityCatalog, ScramAttempt, SecurityCatalog, SecurityName,
};
use crate::storage::{security_catalog::SecurityStoreId, security_root};

impl Engine {
    #[cfg(feature = "mongo")]
    pub(super) async fn copy_session_identity(
        &self,
        source: &Session,
        target: &mut Session,
    ) -> EngineResult<()> {
        let principal = source.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        target.principal = Some(
            self.security_call(move |authority| {
                authority.validate_principal(&principal)?;
                Ok(principal)
            })
            .await?,
        );
        Ok(())
    }

    /// Offline, one-way activation of an existing ready root. Close **all**
    /// handles first. The root must be owner-only on a supported platform.
    /// Creates `security.sqlite` exclusively, then binds its ID in the manifest.
    /// Failures may leave an orphan; never delete/adopt it automatically.
    /// Ordinary openers (including older binaries) can no longer use this root.
    /// Back up the entire root, including its private credential store.
    pub async fn provision_security(
        root: impl AsRef<Path>,
        requested_shards: u16,
        catalog: SecurityCatalog,
    ) -> EngineResult<SecurityStoreId> {
        let root = root.as_ref().to_path_buf();
        super::flatten_join(
            tokio::task::spawn_blocking(move || {
                security_root::provision(&root, requested_shards, &catalog)
            })
            .await,
        )
    }

    /// Open an activated root with its matching durable authority. Unlike
    /// ordinary startup this never initializes an unbound root or missing store.
    /// Only explicitly authenticated document commands are currently supported;
    /// SQL and anonymous sessions fail closed. The standalone Mongo TLS adapter
    /// can authenticate document clients; other adapters still reject this mode.
    /// Rust host administration is trusted, not a wire user-management API.
    pub async fn open_authenticated(
        root: impl AsRef<Path>,
        requested_shards: u16,
        options: EngineOptions,
    ) -> EngineResult<Self> {
        crate::storage::validate_shard_count(requested_shards)?;
        let root = root.as_ref().to_path_buf();
        let workers = BlockingPool::new(options.worker_limit(requested_shards)?);
        let (database, authority) = workers
            .run(move || {
                let authority = security_root::open(&root, requested_shards)?;
                let database = Database {
                    storage: crate::storage::Storage::open_with_security_binding(
                        &root,
                        requested_shards,
                        Some(*authority.store_id().as_bytes()),
                    )?,
                    global_index_worker_id: crate::core::random_global_index_worker_id()?,
                };
                Ok((database, authority))
            })
            .await?;
        let mut engine = Self::from_parts(Arc::new(database), options, workers)?;
        Arc::get_mut(&mut engine.inner)
            .expect("unpublished engine")
            .security = Some(Arc::new(std::sync::Mutex::new(authority)));
        Ok(engine)
    }

    /// Begin verification for a trusted adapter. The adapter must implement a
    /// fresh server-owned SCRAM transcript, replay protection and transport policy.
    /// This low-level API is not a wire authentication conversation.
    pub async fn begin_authentication(&self, name: SecurityName) -> EngineResult<ScramAttempt> {
        self.security_call(move |authority| authority.begin_scram(&name))
            .await
    }

    /// Verify an adapter-validated SCRAM proof and create a **new** session.
    /// Sessions cannot be relabeled or reauthenticated in place; old cursors,
    /// prepared statements and transactions cannot cross into another identity.
    pub async fn complete_authentication(
        &self,
        attempt: ScramAttempt,
        auth_message: Vec<u8>,
        client_proof: Vec<u8>,
    ) -> EngineResult<(Session, [u8; 32])> {
        let (principal, signature) = self
            .security_call(move |authority| {
                let result = authority
                    .complete_scram(attempt, &auth_message, &client_proof)?
                    .into_parts();
                authority.validate_principal(&result.0)?;
                Ok(result)
            })
            .await?;
        let mut session = self.session();
        session.principal = Some(principal);
        Ok((session, signature))
    }

    /// Trusted Rust-host administration, never exposed as an unchecked wire command.
    /// The edit is not retried after a revision conflict or uncertain result.
    pub async fn update_security_catalog<T, F>(&self, edit: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut SecurityCatalog) -> EngineResult<T> + Send + 'static,
    {
        self.security_call(move |authority| authority.update(edit))
            .await
    }

    async fn security_call<T, F>(&self, work: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut DurableSecurityCatalog) -> EngineResult<T> + Send + 'static,
    {
        let authority = self.inner.security.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "engine security is not enabled",
            )
        })?;
        let mut operation = self.operation_lifecycle(RequestContext::new())?;
        let permit = operation.wait_pending(self.inner.workers.acquire()).await?;
        operation.check_before_start()?;
        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let join = permit.spawn(move || {
            let _lease = lease;
            let result = (|| {
                let mut authority = authority.lock().map_err(|_| poisoned())?;
                if let Some(reason) = control.reason() {
                    return Err(reason.error());
                }
                work(&mut authority)
            })();
            control.complete(result)
        });
        let result = operation.wait_started(join).await;
        operation.finish_started(result)
    }
}

fn poisoned() -> EngineError {
    EngineError::new(
        EngineErrorKind::FailedPrecondition,
        "security authority is unavailable; reopen the engine explicitly",
    )
}

#[cfg(feature = "documents")]
pub(super) mod document;

#[cfg(all(test, unix))]
mod tests;
