//! Durable publication for a future engine-owned security authority.

use std::{fmt, sync::Arc};

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use super::{Principal, ScramAttempt, ScramAuthentication, SecurityCatalog, SecurityName};
use crate::{
    core::{
        EngineError, EngineErrorKind, EngineResult,
        authorization::{Action, Resource},
    },
    storage::security_catalog::{SecurityCatalogStore, SecurityStoreId},
};

/// Serial, revision-checked authority over one already-validated catalog store.
///
/// Every authentication phase and permission check refreshes durable state first.
/// Valid successors preserve this authority's incarnation, so existing principals
/// see current roles; rotation/drop still invalidate the affected identities.
/// Reopening creates a new incarnation and invalidates all old principals.
///
/// Calls perform blocking SQLite I/O. A host must serialize access (for example
/// behind a mutex on blocking workers), derive requirements from each operation,
/// and bind sessions/cursors and the database root to this authority. This type
/// alone does **not** enable engine security, authenticate a listener, validate a
/// wire SCRAM exchange, cancel already-admitted work or activate a manifest binding.
///
/// ```no_run
/// use briskdb::core::security_catalog::{DurableSecurityCatalog, SecurityName};
/// use briskdb::storage::security_catalog::{SecurityCatalogStore, SecurityStoreId};
/// let id = SecurityStoreId::from_bytes([7; 16])?; // Real hosts use a trusted stored ID.
/// let store = SecurityCatalogStore::open("/private/security.sqlite", id)?;
/// let mut authority = DurableSecurityCatalog::from_store(store)?;
/// authority.update(|catalog| {
///     catalog.set_user_roles(&SecurityName::new("app", "alice")?, [])
/// })?; // Trusted administration; not an authorized network command.
/// # Ok::<_, briskdb::EngineError>(())
/// ```
pub struct DurableSecurityCatalog {
    store: SecurityCatalogStore,
    catalog: SecurityCatalog,
    revision: u64,
    fingerprint: Zeroizing<[u8; 32]>,
    fenced: bool,
}

impl fmt::Debug for DurableSecurityCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableSecurityCatalog")
            .field("revision", &self.revision)
            .field("fenced", &self.fenced)
            .finish_non_exhaustive()
    }
}

impl DurableSecurityCatalog {
    /// Take ownership of a store and create a new process-local incarnation.
    /// Never resumes principals or partially completed proofs from another owner.
    pub fn from_store(mut store: SecurityCatalogStore) -> EngineResult<Self> {
        let (revision, catalog) = store.load()?.into_parts();
        let fingerprint = fingerprint(&catalog)?;
        Ok(Self {
            store,
            catalog,
            revision,
            fingerprint,
            fenced: false,
        })
    }

    pub const fn store_id(&self) -> SecurityStoreId {
        self.store.id()
    }

    /// Last observed revision, not an authorization decision or freshness check.
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub const fn is_fenced(&self) -> bool {
        self.fenced
    }

    pub(crate) fn validate_principal(&mut self, principal: &Principal) -> EngineResult<()> {
        self.refresh()?;
        self.catalog.current_user(principal).map(|_| ())
    }

    pub fn begin_scram(&mut self, name: &SecurityName) -> EngineResult<ScramAttempt> {
        self.refresh()?;
        self.catalog.begin_scram(name)
    }

    pub fn complete_scram(
        &mut self,
        attempt: ScramAttempt,
        auth_message: &[u8],
        client_proof: &[u8],
    ) -> EngineResult<ScramAuthentication> {
        self.refresh()?;
        self.catalog
            .complete_scram(attempt, auth_message, client_proof)
    }

    pub fn authorize(
        &mut self,
        principal: &Principal,
        action: Action,
        resource: &Resource,
    ) -> EngineResult<()> {
        self.refresh()?;
        self.catalog.authorize(principal, action, resource)
    }

    pub fn authorize_all<'a>(
        &mut self,
        principal: &Principal,
        requirements: impl IntoIterator<Item = (Action, &'a Resource)>,
    ) -> EngineResult<()> {
        self.refresh()?;
        self.catalog.authorize_all(principal, requirements)
    }

    /// Trusted administration: edit a detached copy and publish only after commit.
    /// Callback/validation errors never publish or persist the detached candidate.
    /// A competing writer may cause a revision conflict; no automatic replay of
    /// the callback occurs. A subsequent admission refreshes the winning revision.
    /// Any uncertain write fences this authority until explicit reconstruction.
    pub fn update<T>(
        &mut self,
        edit: impl FnOnce(&mut SecurityCatalog) -> EngineResult<T>,
    ) -> EngineResult<T> {
        self.refresh()?;
        self.update_current(edit)
    }

    /// Authorization and the write's expected revision must come from the same
    /// refresh. A second refresh between these steps could accept a revocation
    /// while retaining a stale permission decision. A peer racing after this
    /// check causes CAS failure, never automatic callback replay.
    pub(crate) fn update_authorized<T>(
        &mut self,
        principal: &Principal,
        requirements: &[(Action, Resource)],
        edit: impl FnOnce(&mut SecurityCatalog) -> EngineResult<T>,
    ) -> EngineResult<T> {
        self.refresh()?;
        self.catalog.authorize_all(
            principal,
            requirements
                .iter()
                .map(|(action, resource)| (*action, resource)),
        )?;
        self.update_current(edit)
    }

    fn update_current<T>(
        &mut self,
        edit: impl FnOnce(&mut SecurityCatalog) -> EngineResult<T>,
    ) -> EngineResult<T> {
        let mut candidate = SecurityCatalog::from_record(self.catalog.to_record()?.as_bytes())?;
        let result = edit(&mut candidate)?;
        self.catalog.validate_successor(&candidate)?;
        let fingerprint = fingerprint(&candidate)?;
        let revision = match self.store.replace(self.revision, &candidate) {
            Ok(revision) => revision,
            Err(error) => {
                // Only a confirmed pre-write revision conflict stays usable.
                // Read/identity errors must not look like an ordinary conflict,
                // even when they happened before the store's write fence.
                let conflict = error.kind() == EngineErrorKind::FailedPrecondition
                    && !self.store.is_fenced()
                    && self.store.observed_revision() > self.revision;
                self.fenced = !conflict;
                return Err(error);
            }
        };
        // All fallible preparation precedes commit; publication only moves values
        // and retains the original incarnation. Detached-copy principals stay foreign.
        self.publish(revision, candidate, fingerprint);
        Ok(result)
    }

    fn refresh(&mut self) -> EngineResult<()> {
        if self.fenced {
            return Err(EngineError::new(
                EngineErrorKind::FailedPrecondition,
                "security authority requires explicit reopen",
            ));
        }
        let result = self.refresh_checked();
        if result.is_err() {
            // Never fall back to a previously cached policy on read/lineage failure.
            self.fenced = true;
        }
        result
    }

    fn refresh_checked(&mut self) -> EngineResult<()> {
        let (revision, candidate) = self.store.load()?.into_parts();
        let fingerprint = fingerprint(&candidate)?;
        if revision == self.revision {
            if !bool::from(self.fingerprint[..].ct_eq(&fingerprint[..])) {
                return Err(corrupt());
            }
            return Ok(());
        }
        if revision < self.revision {
            return Err(corrupt());
        }
        self.catalog
            .validate_successor(&candidate)
            .map_err(|_| corrupt())?;
        self.publish(revision, candidate, fingerprint);
        Ok(())
    }

    fn publish(
        &mut self,
        revision: u64,
        mut candidate: SecurityCatalog,
        fingerprint: Zeroizing<[u8; 32]>,
    ) {
        candidate.identity = Arc::clone(&self.catalog.identity);
        self.catalog = candidate;
        self.fingerprint = fingerprint;
        self.revision = revision;
    }
}

fn fingerprint(catalog: &SecurityCatalog) -> EngineResult<Zeroizing<[u8; 32]>> {
    let record = catalog.to_record()?;
    Ok(Zeroizing::new(*blake3::hash(record.as_bytes()).as_bytes()))
}

fn corrupt() -> EngineError {
    EngineError::new(
        EngineErrorKind::DataCorruption,
        "security authority history is inconsistent",
    )
}

#[cfg(all(test, unix))]
mod tests;
