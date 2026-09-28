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
    /// Only explicitly authenticated document and typed user commands are supported;
    /// SQL and anonymous sessions fail closed. The standalone Mongo TLS adapter
    /// can authenticate document clients; other adapters still reject this mode.
    /// Arbitrary Rust-host catalog edits remain trusted; typed user commands
    /// separately require current realm privileges.
    pub async fn open_authenticated(
        root: impl AsRef<Path>,
        requested_shards: u16,
        options: EngineOptions,
    ) -> EngineResult<Self> {
        options.storage_profile().require_available()?;
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

    /// Execute a typed user command under the caller's current realm privileges.
    /// The permission snapshot and durable compare-and-swap share one revision;
    /// a competing catalog edit rejects the write rather than replaying it.
    /// Password derivation runs only after authorization on a bounded worker.
    /// Cancellation before admission skips the edit; once blocking work starts,
    /// a cancelled/timed-out request may still commit. Never retry automatically.
    pub async fn execute_user_management(
        &self,
        session: &Session,
        context: RequestContext,
        command: super::super::user_management::UserManagementCommand,
    ) -> EngineResult<()> {
        let mut operation = self.operation_lifecycle(context.clone())?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        let requirements = command.requirements()?;
        operation.check_before_start()?;
        let result = self
            .security_call_with_context(context, move |authority| {
                authority
                    .update_authorized(&principal, &requirements, |catalog| command.apply(catalog))
            })
            .await;
        operation.finish(result)
    }

    async fn security_call<T, F>(&self, work: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut DurableSecurityCatalog) -> EngineResult<T> + Send + 'static,
    {
        self.security_call_with_context(RequestContext::new(), work)
            .await
    }

    /// Inspect credential-free account metadata under current realm privileges.
    /// Exact self-inspection is allowed; listing a realm or another account
    /// requires ViewUsers. Lifecycle, session ownership and result limits apply.
    pub async fn user_info(
        &self,
        session: &Session,
        context: RequestContext,
        request: super::super::security_catalog::UserInfoRequest,
    ) -> EngineResult<Vec<super::super::security_catalog::UserInfo>> {
        let mut operation = self.operation_lifecycle(context.clone())?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        let limits = operation.result_limits;
        operation.check_before_start()?;
        let result = self
            .security_call_with_context(context, move |authority| {
                authority.user_info(&principal, &request, limits)
            })
            .await;
        operation.finish(result)
    }

    /// Inspect stored flat-role names under current membership/realm authority.
    /// Directly assigned roles are visible; other names and realm listings need
    /// ViewRoles. This does not export policies, inheritance or credentials.
    pub async fn role_info(
        &self,
        session: &Session,
        context: RequestContext,
        request: super::super::security_catalog::RoleInfoRequest,
    ) -> EngineResult<Vec<super::super::security_catalog::RoleInfo>> {
        let mut operation = self.operation_lifecycle(context.clone())?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        let limits = operation.result_limits;
        operation.check_before_start()?;
        let result = self
            .security_call_with_context(context, move |authority| {
                authority.role_info(&principal, &request, limits)
            })
            .await;
        operation.finish(result)
    }

    async fn security_call_with_context<T, F>(
        &self,
        context: RequestContext,
        work: F,
    ) -> EngineResult<T>
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
        let mut operation = self.operation_lifecycle(context)?;
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
