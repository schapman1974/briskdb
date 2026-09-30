//! Shared user/role catalogs and principal-admission foundations.
//!
//! [`SecurityCatalog`] is in-memory state, not the engine's active policy. It
//! does not persist data, enable listener authentication or wrap engine operations.
//! Mutation methods are trusted provisioning APIs, not authorized wire commands.
//! [`DurableSecurityCatalog`] supplies revision-checked publication and refresh;
//! a host still must integrate it with engine/session admission.
//!
//! SCRAM adapters must validate a fresh server-owned transcript, enforce TLS,
//! prevent replay and bound pending exchanges. Unknown-user concealment, rate
//! limiting and exchange deadlines belong at that boundary, before exposure.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};
use subtle::ConstantTimeEq;

use super::{
    EngineError, EngineErrorKind, EngineResult,
    authentication::ScramSha256Verifier,
    authorization::{Action, MAX_POLICY_ROLES, Policy, Resource},
};

mod durable;
mod mongo_data_roles;
mod record;
mod role_info;
mod user_info;
pub use durable::DurableSecurityCatalog;
pub use mongo_data_roles::MongoDataRole;
pub(crate) use mongo_data_roles::validate_document_role_policy;
pub use record::{MAX_SECURITY_CATALOG_RECORD_BYTES, SecurityCatalogRecord};
pub use role_info::{MAX_ROLE_INFO_SELECTORS, RoleInfo, RoleInfoRequest};
pub use user_info::{MAX_USER_INFO_SELECTORS, UserInfo, UserInfoRequest};

pub const MAX_SECURITY_USERS: usize = 1_024;
pub const MAX_SECURITY_ROLES: usize = 1_024;
pub const MAX_SECURITY_NAME_BYTES: usize = 128;

/// A requested stored role is absent. Carries no realm/name or credentials.
#[derive(Debug)]
pub struct RoleNotFound;

impl fmt::Display for RoleNotFound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("role does not exist")
    }
}

impl std::error::Error for RoleNotFound {}

/// Exact, case-sensitive realm/name pair. No globbing, folding or normalization.
/// User and role namespaces are separate even when their names are equal.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SecurityName {
    realm: String,
    name: String,
}

impl SecurityName {
    pub fn new(realm: &str, name: &str) -> EngineResult<Self> {
        if name.is_empty() || name.len() > MAX_SECURITY_NAME_BYTES || name.contains('\0') {
            return Err(error(
                EngineErrorKind::InvalidArgument,
                "security name is invalid",
            ));
        }
        let realm = Resource::security_realm(realm)?;
        Ok(Self {
            realm: realm.realm_name().expect("validated realm").to_owned(),
            name: name.to_owned(),
        })
    }

    pub fn realm(&self) -> &str {
        &self.realm
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Debug for SecurityName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecurityName { .. }")
    }
}

struct User {
    id: u64,
    credential_generation: u64,
    verifier: ScramSha256Verifier,
    roles: BTreeSet<SecurityName>,
}

/// An unforgeable, process-local admission identity, not a cached permission set.
/// Only this catalog incarnation may validate it. Rotation and drop invalidate
/// it on the next admission; already-running operations are not interrupted.
#[derive(Clone)]
pub struct Principal {
    catalog: Arc<()>,
    user: SecurityName,
    user_id: u64,
    credential_generation: u64,
}

impl Principal {
    /// For listener-scoped pseudonymous audit correlation, never authorization.
    /// The caller must namespace the ID to its one bound catalog/engine.
    #[cfg(feature = "mongo")]
    pub(crate) fn audit_identity(&self) -> (u64, u64) {
        (self.user_id, self.credential_generation)
    }

    #[cfg(feature = "mongo")]
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.catalog, &other.catalog)
            && self.user == other.user
            && self.user_id == other.user_id
            && self.credential_generation == other.credential_generation
    }

    /// Exact name for trusted audit context; Debug intentionally omits it.
    pub fn name(&self) -> &SecurityName {
        &self.user
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Principal { .. }")
    }
}

/// A single-use verifier snapshot; cannot itself authorize work. The host must
/// bound its lifetime and count. It retains only this credential, not the catalog.
pub struct ScramAttempt {
    principal: Principal,
    verifier: ScramSha256Verifier,
}

impl ScramAttempt {
    pub fn salt(&self) -> &[u8] {
        self.verifier.salt()
    }
    pub fn iterations(&self) -> u32 {
        self.verifier.iterations()
    }
}

impl fmt::Debug for ScramAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScramAttempt { .. }")
    }
}

/// Both outputs are produced only after proof and current-identity validation.
pub struct ScramAuthentication {
    principal: Principal,
    server_signature: [u8; 32],
}

impl ScramAuthentication {
    pub fn into_parts(self) -> (Principal, [u8; 32]) {
        (self.principal, self.server_signature)
    }
}

impl fmt::Debug for ScramAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScramAuthentication { .. }")
    }
}

/// Bounded named users/flat roles. Empty catalogs have no implicit administrator.
///
/// Provisioning requires exclusive access (`&mut self`). Hosts sharing a catalog
/// must hold their lock through proof completion or permission admission; never
/// treat a retained principal or a previous decision as cached authority.
///
/// ```
/// use briskdb::core::{authentication::ScramSha256Verifier, authorization::Policy,
///     security_catalog::{SecurityCatalog, SecurityName}};
/// let mut catalog = SecurityCatalog::new();
/// let role = SecurityName::new("app", "no_access")?;
/// catalog.create_role(role.clone(), Policy::default())?;
/// catalog.create_user(SecurityName::new("app", "alice")?,
///     ScramSha256Verifier::from_password("example provisioning password")?, [role])?;
/// assert_eq!(catalog.user_count(), 1);
/// let record = catalog.to_record()?; // Sensitive; storage must protect this.
/// let restored = SecurityCatalog::from_record(record.as_bytes())?;
/// assert_eq!(restored.user_count(), 1);
/// // Provisioning alone enables no listener authentication or engine policy.
/// # Ok::<_, briskdb::EngineError>(())
/// ```
pub struct SecurityCatalog {
    identity: Arc<()>,
    next_user_id: u64,
    users: BTreeMap<SecurityName, User>,
    roles: BTreeMap<SecurityName, Policy>,
}

impl Default for SecurityCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl SecurityCatalog {
    pub fn new() -> Self {
        Self {
            identity: Arc::new(()),
            next_user_id: 1,
            users: BTreeMap::new(),
            roles: BTreeMap::new(),
        }
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }
    pub fn role_count(&self) -> usize {
        self.roles.len()
    }

    /// Trusted provisioning: no implicit grants or inherited roles.
    pub fn create_role(&mut self, name: SecurityName, policy: Policy) -> EngineResult<()> {
        if self.roles.contains_key(&name) {
            return Err(already_exists());
        }
        if self.roles.len() == MAX_SECURITY_ROLES {
            return Err(limit());
        }
        self.roles.insert(name, policy);
        Ok(())
    }

    /// Validate every affected resolved union before atomically replacing a role.
    /// An oversized union changes neither this role nor any user's authority.
    pub fn replace_role(&mut self, name: &SecurityName, policy: Policy) -> EngineResult<()> {
        if !self.roles.contains_key(name) {
            return Err(not_found());
        }
        for user in self.users.values().filter(|user| user.roles.contains(name)) {
            Policy::combine(user.roles.iter().map(|role| {
                if role == name {
                    &policy
                } else {
                    self.roles.get(role).expect("catalog role invariant")
                }
            }))?;
        }
        self.roles.insert(name.clone(), policy);
        Ok(())
    }

    /// Remove memberships too: recreating this name never resurrects old grants.
    pub fn drop_role(&mut self, name: &SecurityName) -> EngineResult<()> {
        self.roles.remove(name).ok_or_else(|| {
            EngineError::from_source(
                EngineErrorKind::FailedPrecondition,
                "security role does not exist",
                RoleNotFound,
            )
        })?;
        for user in self.users.values_mut() {
            user.roles.remove(name);
        }
        Ok(())
    }

    pub fn create_user(
        &mut self,
        name: SecurityName,
        verifier: ScramSha256Verifier,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<()> {
        if self.users.contains_key(&name) {
            return Err(already_exists());
        }
        if self.users.len() == MAX_SECURITY_USERS {
            return Err(limit());
        }
        let roles = self.validate_roles(roles)?;
        let next = self.next_user_id.checked_add(1).ok_or_else(limit)?;
        self.users.insert(
            name,
            User {
                id: self.next_user_id,
                credential_generation: 1,
                verifier,
                roles,
            },
        );
        self.next_user_id = next;
        Ok(())
    }

    /// Replace memberships atomically; existing principals see them on next admission.
    /// Cross-realm assignments are explicit and require host-side grant authorization.
    pub fn set_user_roles(
        &mut self,
        name: &SecurityName,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<()> {
        if !self.users.contains_key(name) {
            return Err(not_found());
        }
        let roles = self.validate_roles(roles)?;
        self.users.get_mut(name).expect("validated user").roles = roles;
        Ok(())
    }

    /// Trusted, bounded union; validate the full resulting membership before editing.
    pub fn grant_user_roles(
        &mut self,
        name: &SecurityName,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<()> {
        let additions = self.validate_roles(roles)?;
        let mut combined = self.users.get(name).ok_or_else(not_found)?.roles.clone();
        combined.extend(additions);
        let combined = self.validate_roles(combined)?;
        self.users.get_mut(name).expect("validated user").roles = combined;
        Ok(())
    }

    /// Trusted, bounded removal. Unknown/unassigned names are harmless no-ops;
    /// never mutate membership until the complete input has been bounded.
    pub fn revoke_user_roles(
        &mut self,
        name: &SecurityName,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<()> {
        let mut retained = self.users.get(name).ok_or_else(not_found)?.roles.clone();
        for (index, role) in roles.into_iter().enumerate() {
            if index >= MAX_POLICY_ROLES {
                return Err(limit());
            }
            retained.remove(&role);
        }
        self.users.get_mut(name).expect("validated user").roles = retained;
        Ok(())
    }

    /// Invalidate pending proofs and all retained principals at their next admission,
    /// even if the replacement password/record happens to equal the previous one.
    pub fn rotate_credentials(
        &mut self,
        name: &SecurityName,
        verifier: ScramSha256Verifier,
    ) -> EngineResult<()> {
        let user = self.users.get_mut(name).ok_or_else(not_found)?;
        let next = user
            .credential_generation
            .checked_add(1)
            .ok_or_else(limit)?;
        user.verifier = verifier;
        user.credential_generation = next;
        Ok(())
    }

    pub fn drop_user(&mut self, name: &SecurityName) -> EngineResult<()> {
        self.users.remove(name).ok_or_else(not_found)?;
        Ok(())
    }

    /// Trusted exchange preparation, not a wire-level unknown-user concealment policy.
    pub fn begin_scram(&self, name: &SecurityName) -> EngineResult<ScramAttempt> {
        let user = self.users.get(name).ok_or_else(authentication_failed)?;
        Ok(ScramAttempt {
            principal: Principal {
                catalog: Arc::clone(&self.identity),
                user: name.clone(),
                user_id: user.id,
                credential_generation: user.credential_generation,
            },
            verifier: user.verifier.clone(),
        })
    }

    /// Consume the attempt and recheck identity/generation before releasing either
    /// output. `&self` prevents a concurrent mutation without host synchronization.
    /// This verifies a host-validated transcript; it is not a SCRAM state machine.
    pub fn complete_scram(
        &self,
        attempt: ScramAttempt,
        auth_message: &[u8],
        client_proof: &[u8],
    ) -> EngineResult<ScramAuthentication> {
        self.current_user(&attempt.principal)
            .map_err(|_| authentication_failed())?;
        let server_signature = attempt
            .verifier
            .verify_client_proof(auth_message, client_proof)
            .map_err(|_| authentication_failed())?;
        Ok(ScramAuthentication {
            principal: attempt.principal,
            server_signature,
        })
    }

    /// Resolve current roles for every admission, never the authentication-time set.
    pub fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        resource: &Resource,
    ) -> EngineResult<()> {
        self.current_policy(principal)?.authorize(action, resource)
    }

    /// Finish all requirements before performing work. Does not cancel in-flight
    /// operations or bind transaction/cursor ownership; those need engine integration.
    pub fn authorize_all<'a>(
        &self,
        principal: &Principal,
        requirements: impl IntoIterator<Item = (Action, &'a Resource)>,
    ) -> EngineResult<()> {
        self.current_policy(principal)?.authorize_all(requirements)
    }

    fn current_user(&self, principal: &Principal) -> EngineResult<&User> {
        if !Arc::ptr_eq(&self.identity, &principal.catalog) {
            return Err(denied());
        }
        let user = self.users.get(&principal.user).ok_or_else(denied)?;
        if user.id != principal.user_id
            || user.credential_generation != principal.credential_generation
        {
            return Err(denied());
        }
        Ok(user)
    }

    fn current_policy(&self, principal: &Principal) -> EngineResult<Policy> {
        let user = self.current_user(principal)?;
        Policy::combine(
            user.roles
                .iter()
                .map(|name| self.roles.get(name).expect("catalog role invariant")),
        )
    }

    fn validate_roles(
        &self,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<BTreeSet<SecurityName>> {
        let mut names = BTreeSet::new();
        for (index, name) in roles.into_iter().enumerate() {
            if index >= MAX_POLICY_ROLES {
                return Err(limit());
            }
            if !self.roles.contains_key(&name) {
                return Err(not_found());
            }
            names.insert(name);
        }
        Policy::combine(
            names
                .iter()
                .map(|name| self.roles.get(name).expect("validated role")),
        )?;
        Ok(names)
    }

    /// Storage replacement must preserve identity history before a runtime can
    /// safely couple durable revisions to retained principals. Restoration to an
    /// earlier history requires a separate store/root incarnation, not this API.
    pub(crate) fn validate_successor(&self, successor: &Self) -> EngineResult<()> {
        let invalid = || {
            error(
                EngineErrorKind::FailedPrecondition,
                "security catalog replacement rewinds identity or credential history",
            )
        };
        if successor.next_user_id < self.next_user_id {
            return Err(invalid());
        }
        for (name, next) in &successor.users {
            match self
                .users
                .get(name)
                .filter(|previous| previous.id == next.id)
            {
                Some(previous) => {
                    if next.credential_generation < previous.credential_generation {
                        return Err(invalid());
                    }
                    if next.credential_generation == previous.credential_generation {
                        let old_record = previous.verifier.to_record();
                        let next_record = next.verifier.to_record();
                        if !bool::from(old_record.as_bytes().ct_eq(next_record.as_bytes())) {
                            return Err(invalid());
                        }
                    }
                }
                None if next.id < self.next_user_id => return Err(invalid()),
                None => {}
            }
        }
        Ok(())
    }
}

impl fmt::Debug for SecurityCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityCatalog")
            .field("user_count", &self.user_count())
            .field("role_count", &self.role_count())
            .finish_non_exhaustive()
    }
}

fn error(kind: EngineErrorKind, message: &'static str) -> EngineError {
    EngineError::new(kind, message)
}
fn already_exists() -> EngineError {
    error(
        EngineErrorKind::UniqueViolation,
        "security entry already exists",
    )
}
fn not_found() -> EngineError {
    error(
        EngineErrorKind::FailedPrecondition,
        "security entry does not exist",
    )
}
fn limit() -> EngineError {
    error(
        EngineErrorKind::LimitExceeded,
        "security catalog limit exceeded",
    )
}
fn authentication_failed() -> EngineError {
    error(EngineErrorKind::PermissionDenied, "authentication failed")
}
fn denied() -> EngineError {
    error(
        EngineErrorKind::PermissionDenied,
        "operation is not authorized",
    )
}

#[cfg(test)]
pub(crate) mod tests;
