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
    /// Only authenticated document and typed user/role commands are supported;
    /// SQL and anonymous sessions fail closed. The standalone Mongo TLS adapter
    /// can authenticate document clients; other adapters still reject this mode.
    /// Arbitrary Rust-host catalog edits remain trusted; typed user/role commands
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
        let startup = StartupOperation::new(options.contention_policy());
        let control = startup.control.clone();
        let (database, authority) = workers
            .run(move || {
                let authority = security_root::open_controlled(
                    &root,
                    requested_shards,
                    control.clone(),
                    options.storage_profile(),
                )?;
                let database = Database {
                    storage: crate::storage::Storage::open_with_profile_control(
                        &root,
                        requested_shards,
                        Some(*authority.store_id().as_bytes()),
                        control.as_ref(),
                        options.storage_profile(),
                    )?,
                    global_index_worker_id: crate::core::random_global_index_worker_id()?,
                };
                Ok((database, authority))
            })
            .await?;
        let mut engine =
            Self::from_parts(Arc::new(database), options, workers, startup.complete())?;
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
        let mut operation = self.operation_lifecycle(context)?;
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
            .security_call_from_parent(&operation, move |authority| {
                authority
                    .update_authorized(&principal, &requirements, |catalog| command.apply(catalog))
            })
            .await;
        operation.finish(result)
    }

    /// Create a flat document-data role confined to its own security realm.
    /// Requires both CreateRole and GrantRole on that realm before checking for
    /// an existing name. Only exact document database/collection grants and
    /// non-system collection scopes in that database are accepted, even for
    /// admin. No SQL, global, security-administration or inherited grants.
    /// No user is assigned the new role automatically. Publication is one
    /// revision-checked edit, never retried; after blocking work starts a
    /// cancellation or timeout can have an uncertain result.
    pub async fn create_document_role(
        &self,
        session: &Session,
        context: RequestContext,
        name: SecurityName,
        policy: crate::core::authorization::Policy,
    ) -> EngineResult<()> {
        use crate::core::authorization::{Action, Resource};

        let mut operation = self.operation_lifecycle(context)?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        crate::core::security_catalog::validate_document_role_policy(&name, &policy)?;
        let realm = Resource::security_realm(name.realm())?;
        let requirements = [
            (Action::CreateRole, realm.clone()),
            (Action::GrantRole, realm),
        ];
        operation.check_before_start()?;
        let result = self
            .security_call_from_parent(&operation, move |authority| {
                authority.update_authorized(&principal, &requirements, |catalog| {
                    catalog.create_role(name, policy)
                })
            })
            .await;
        operation.finish(result)
    }

    /// Add database-local document privileges to an existing role without
    /// removing other grants or changing membership. Current GrantRole on the
    /// exact realm is required even for empty/repeated additions or missing roles.
    /// Both role and affected-user policy limits are checked before publication.
    /// Authorization and durable publication share one revision; no automatic
    /// retry follows a conflict or an uncertain cancellation/timeout.
    pub async fn grant_document_role_privileges(
        &self,
        session: &Session,
        context: RequestContext,
        name: SecurityName,
        additions: crate::core::authorization::Policy,
    ) -> EngineResult<()> {
        use crate::core::authorization::{Action, Resource};
        let mut operation = self.operation_lifecycle(context)?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        crate::core::security_catalog::validate_document_role_policy(&name, &additions)?;
        let requirements = [(Action::GrantRole, Resource::security_realm(name.realm())?)];
        operation.check_before_start()?;
        let result = self
            .security_call_from_parent(&operation, move |authority| {
                authority.update_authorized(&principal, &requirements, |catalog| {
                    catalog.grant_document_role_privileges(&name, additions)
                })
            })
            .await;
        operation.finish(result)
    }

    /// Remove exact database-local document action/scope pairs. Requires current
    /// RevokeRole on the exact realm before lookup, even for empty/no-op removal.
    /// Existing memberships and unrelated/internal database admission grants are
    /// retained. Authorization and publication use one catalog revision, without
    /// automatic retries. Already-started work can have an uncertain outcome on
    /// timeout/cancellation; queued cancelled work makes no catalog change.
    pub async fn revoke_document_role_privileges(
        &self,
        session: &Session,
        context: RequestContext,
        name: SecurityName,
        removals: crate::core::authorization::Policy,
    ) -> EngineResult<()> {
        use crate::core::authorization::{Action, Resource};
        let mut operation = self.operation_lifecycle(context)?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        crate::core::security_catalog::validate_document_role_revocations(&name, &removals)?;
        let requirements = [(Action::RevokeRole, Resource::security_realm(name.realm())?)];
        operation.check_before_start()?;
        let result = self
            .security_call_from_parent(&operation, move |authority| {
                authority.update_authorized(&principal, &requirements, |catalog| {
                    catalog.revoke_document_role_privileges(&name, removals)
                })
            })
            .await;
        operation.finish(result)
    }

    /// Delete a stored flat role and all its memberships in one catalog commit.
    /// Requires current DropRole authority on the target realm, including for a
    /// missing role. Existing sessions retain their login but observe revoked
    /// privileges on their next admission; recreating a name restores no grants.
    /// Uses the same revision-checked, non-retrying publication as user commands.
    /// Cancellation before admission skips the edit; after blocking work starts,
    /// cancellation/timeout can have an uncertain result and must not be replayed.
    pub async fn drop_role(
        &self,
        session: &Session,
        context: RequestContext,
        name: SecurityName,
    ) -> EngineResult<()> {
        use crate::core::authorization::{Action, Resource};

        let mut operation = self.operation_lifecycle(context)?;
        let _session = operation.wait_pending(self.ready_session(session)).await?;
        let principal = session.principal.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::PermissionDenied,
                "authentication is required",
            )
        })?;
        let requirements = [(Action::DropRole, Resource::security_realm(name.realm())?)];
        operation.check_before_start()?;
        let result = self
            .security_call_from_parent(&operation, move |authority| {
                authority.update_authorized(&principal, &requirements, |catalog| {
                    catalog.drop_role(&name)
                })
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
        let mut operation = self.operation_lifecycle(context)?;
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
            .security_call_from_parent(&operation, move |authority| {
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
        let mut operation = self.operation_lifecycle(context)?;
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
            .security_call_from_parent(&operation, move |authority| {
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
        let authority = self.security_authority()?;
        let operation = self.operation_lifecycle(context)?;
        self.run_security_call(authority, operation, work).await
    }

    /// A child keeps the parent's effective deadline, cancellation and budget,
    /// but has a separate execution phase/interrupt slot and worker lease.
    async fn security_call_from_parent<T, F>(&self, parent: &Operation, work: F) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut DurableSecurityCatalog) -> EngineResult<T> + Send + 'static,
    {
        let authority = self.security_authority()?;
        let lease = self.inner.lifecycle.try_acquire()?;
        let scope = RequestScope::new(parent.cancellation.clone(), &parent.control);
        let control = scope.child_control(parent.deadline);
        let operation = Operation {
            lease: Some(lease),
            cancel_on_drop: CancelOnDrop::new(Arc::clone(&control)),
            control,
            cancellation: parent.cancellation.clone(),
            shutdown_cancel: parent.shutdown_cancel.clone(),
            deadline: parent.deadline,
            result_limits: parent.result_limits,
        };
        operation.check_before_start()?;
        self.run_security_call(authority, operation, work).await
    }

    fn security_authority(&self) -> EngineResult<Arc<std::sync::Mutex<DurableSecurityCatalog>>> {
        self.inner.security.clone().ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "engine security is not enabled",
            )
        })
    }

    async fn run_security_call<T, F>(
        &self,
        authority: Arc<std::sync::Mutex<DurableSecurityCatalog>>,
        mut operation: Operation,
        work: F,
    ) -> EngineResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut DurableSecurityCatalog) -> EngineResult<T> + Send + 'static,
    {
        let permit = operation.wait_pending(self.inner.workers.acquire()).await?;
        operation.check_before_start()?;
        let lease = operation.take_lease();
        let control = Arc::clone(&operation.control);
        let join = permit.spawn(move || {
            let _lease = lease;
            let result = (|| {
                let mut authority = lock_authority(&authority, &control)?;
                if let Some(reason) = control.reason() {
                    return Err(reason.error());
                }
                crate::storage::security_catalog::with_operation_control(
                    Arc::clone(&control),
                    || work(&mut authority),
                )
            })();
            control.complete(result)
        });
        let result = operation.wait_started(join).await;
        operation.finish_started(result)
    }
}

fn lock_authority<'a>(
    authority: &'a std::sync::Mutex<DurableSecurityCatalog>,
    control: &OperationControl,
) -> EngineResult<std::sync::MutexGuard<'a, DurableSecurityCatalog>> {
    loop {
        if let Some(reason) = control.reason() {
            return Err(reason.error());
        }
        match authority.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err(poisoned()),
            Err(std::sync::TryLockError::WouldBlock) => match control.wait_for_contention(None) {
                // Unconfigured engines retain the previous blocking-mutex policy.
                None => return authority.lock().map_err(|_| poisoned()),
                Some(true) => continue,
                Some(false) => {
                    return Err(control.reason().map_or_else(
                        || EngineError::new(EngineErrorKind::Busy, "security authority is busy"),
                        |reason| reason.error(),
                    ));
                }
            },
        }
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
