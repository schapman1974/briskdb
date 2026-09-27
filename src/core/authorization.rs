//! Bounded, deny-by-default permission decisions shared by future frontends.
//!
//! These values are not principals or an enabled engine policy. The caller must
//! resolve current, trusted catalog roles, derive every protected resource from
//! the operation, and check all requirements before performing any work. No
//! listener, session or persistent catalog is changed by constructing a policy.

use std::{collections::BTreeSet, fmt};

use super::{EngineError, EngineErrorKind, EngineResult, validate_catalog_identifier};

/// Distinct data models: an SQL table grant never grants document collection access.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataDomain {
    Relational,
    Document,
}

/// The exact kind of resource an action protects.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceKind {
    Server,
    SecurityRealm,
    DataDomain,
    Database,
    Object,
}

/// Individual privileges, with no implicit read/write/admin hierarchy.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    ConnectDatabase,
    ListDatabases,
    ListObjects,
    CreateDatabase,
    DropDatabase,
    ReadData,
    InsertData,
    UpdateData,
    DeleteData,
    CreateObject,
    DropObject,
    ListIndexes,
    CreateIndex,
    DropIndex,
    ViewUsers,
    CreateUser,
    DropUser,
    RotateCredentials,
    GrantRole,
    RevokeRole,
    ViewRoles,
    CreateRole,
    UpdateRole,
    DropRole,
    ReadDiagnostics,
    ManageServer,
}

impl Action {
    /// Known actions only; future additions do not silently expand existing grants.
    pub const ALL: &'static [Self] = &[
        Self::ConnectDatabase,
        Self::ListDatabases,
        Self::ListObjects,
        Self::CreateDatabase,
        Self::DropDatabase,
        Self::ReadData,
        Self::InsertData,
        Self::UpdateData,
        Self::DeleteData,
        Self::CreateObject,
        Self::DropObject,
        Self::ListIndexes,
        Self::CreateIndex,
        Self::DropIndex,
        Self::ViewUsers,
        Self::CreateUser,
        Self::DropUser,
        Self::RotateCredentials,
        Self::GrantRole,
        Self::RevokeRole,
        Self::ViewRoles,
        Self::CreateRole,
        Self::UpdateRole,
        Self::DropRole,
        Self::ReadDiagnostics,
        Self::ManageServer,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::ConnectDatabase => "connect_database",
            Self::ListDatabases => "list_databases",
            Self::ListObjects => "list_objects",
            Self::CreateDatabase => "create_database",
            Self::DropDatabase => "drop_database",
            Self::ReadData => "read_data",
            Self::InsertData => "insert_data",
            Self::UpdateData => "update_data",
            Self::DeleteData => "delete_data",
            Self::CreateObject => "create_object",
            Self::DropObject => "drop_object",
            Self::ListIndexes => "list_indexes",
            Self::CreateIndex => "create_index",
            Self::DropIndex => "drop_index",
            Self::ViewUsers => "view_users",
            Self::CreateUser => "create_user",
            Self::DropUser => "drop_user",
            Self::RotateCredentials => "rotate_credentials",
            Self::GrantRole => "grant_role",
            Self::RevokeRole => "revoke_role",
            Self::ViewRoles => "view_roles",
            Self::CreateRole => "create_role",
            Self::UpdateRole => "update_role",
            Self::DropRole => "drop_role",
            Self::ReadDiagnostics => "read_diagnostics",
            Self::ManageServer => "manage_server",
        }
    }

    /// Parse only an exact known action. Wildcards, aliases and unknowns fail closed.
    pub fn from_code(code: &str) -> EngineResult<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|action| action.code() == code)
            .ok_or_else(|| invalid("authorization action is not recognized"))
    }

    pub const fn resource_kind(self) -> ResourceKind {
        match self {
            Self::ConnectDatabase
            | Self::ListObjects
            | Self::CreateDatabase
            | Self::DropDatabase => ResourceKind::Database,
            Self::ListDatabases => ResourceKind::DataDomain,
            Self::ReadData
            | Self::InsertData
            | Self::UpdateData
            | Self::DeleteData
            | Self::CreateObject
            | Self::DropObject
            | Self::ListIndexes
            | Self::CreateIndex
            | Self::DropIndex => ResourceKind::Object,
            Self::ViewUsers
            | Self::CreateUser
            | Self::DropUser
            | Self::RotateCredentials
            | Self::GrantRole
            | Self::RevokeRole
            | Self::ViewRoles
            | Self::CreateRole
            | Self::UpdateRole
            | Self::DropRole => ResourceKind::SecurityRealm,
            Self::ReadDiagnostics | Self::ManageServer => ResourceKind::Server,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ResourceValue {
    Server,
    Realm(String),
    Domain(DataDomain),
    Database(DataDomain, String),
    Object(DataDomain, String, String),
}

/// An exact logical identity, never a path, regular expression or glob.
/// Names are bounded and compared byte-for-byte. SQL names must be canonical
/// catalog identifiers; document/realm names are case-sensitive UTF-8.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Resource(ResourceValue);

impl Resource {
    pub const fn server() -> Self {
        Self(ResourceValue::Server)
    }

    pub fn security_realm(name: &str) -> EngineResult<Self> {
        validate_database_name(name)?;
        Ok(Self(ResourceValue::Realm(name.to_owned())))
    }

    pub const fn data_domain(domain: DataDomain) -> Self {
        Self(ResourceValue::Domain(domain))
    }

    pub fn database(domain: DataDomain, name: &str) -> EngineResult<Self> {
        validate_data_database(domain, name)?;
        Ok(Self(ResourceValue::Database(domain, name.to_owned())))
    }

    /// Identify one SQL table or document collection, including a not-yet-created one.
    pub fn object(domain: DataDomain, database: &str, object: &str) -> EngineResult<Self> {
        validate_data_database(domain, database)?;
        if object.is_empty()
            || database
                .len()
                .saturating_add(1)
                .saturating_add(object.len())
                > 255
            || object.contains('\0')
            || (domain == DataDomain::Relational && !validate_catalog_identifier(object))
        {
            return Err(invalid("authorization object name is invalid"));
        }
        Ok(Self(ResourceValue::Object(
            domain,
            database.to_owned(),
            object.to_owned(),
        )))
    }

    pub const fn kind(&self) -> ResourceKind {
        match self.0 {
            ResourceValue::Server => ResourceKind::Server,
            ResourceValue::Realm(_) => ResourceKind::SecurityRealm,
            ResourceValue::Domain(_) => ResourceKind::DataDomain,
            ResourceValue::Database(..) => ResourceKind::Database,
            ResourceValue::Object(..) => ResourceKind::Object,
        }
    }

    pub const fn domain(&self) -> Option<DataDomain> {
        match self.0 {
            ResourceValue::Domain(domain)
            | ResourceValue::Database(domain, _)
            | ResourceValue::Object(domain, _, _) => Some(domain),
            _ => None,
        }
    }

    pub fn database_name(&self) -> Option<&str> {
        match &self.0 {
            ResourceValue::Database(_, name) | ResourceValue::Object(_, name, _) => Some(name),
            _ => None,
        }
    }

    pub fn object_name(&self) -> Option<&str> {
        match &self.0 {
            ResourceValue::Object(_, _, name) => Some(name),
            _ => None,
        }
    }

    pub fn realm_name(&self) -> Option<&str> {
        match &self.0 {
            ResourceValue::Realm(name) => Some(name),
            _ => None,
        }
    }
}

impl fmt::Debug for Resource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationResource")
            .field("kind", &self.kind())
            .field("domain", &self.domain())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ScopeValue {
    Exact(Resource),
    Database(DataDomain, String),
    AllDatabases(DataDomain),
    AllSecurityRealms,
}

/// Explicit coverage for a privilege. Broad data coverage never includes a
/// security realm or the server. An exact database does not cover its objects;
/// use Scope::database when descendant coverage is intended.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Scope(ScopeValue);

impl Scope {
    pub fn exact(resource: Resource) -> Self {
        Self(ScopeValue::Exact(resource))
    }

    pub fn database(domain: DataDomain, database: &str) -> EngineResult<Self> {
        validate_data_database(domain, database)?;
        Ok(Self(ScopeValue::Database(domain, database.to_owned())))
    }

    pub const fn all_databases(domain: DataDomain) -> Self {
        Self(ScopeValue::AllDatabases(domain))
    }

    pub const fn all_security_realms() -> Self {
        Self(ScopeValue::AllSecurityRealms)
    }

    fn accepts(&self, kind: ResourceKind) -> bool {
        match &self.0 {
            ScopeValue::Exact(resource) => resource.kind() == kind,
            ScopeValue::Database(..) => {
                matches!(kind, ResourceKind::Database | ResourceKind::Object)
            }
            ScopeValue::AllDatabases(_) => matches!(
                kind,
                ResourceKind::DataDomain | ResourceKind::Database | ResourceKind::Object
            ),
            ScopeValue::AllSecurityRealms => kind == ResourceKind::SecurityRealm,
        }
    }

    fn covers(&self, resource: &Resource) -> bool {
        match &self.0 {
            ScopeValue::Exact(exact) => exact == resource,
            ScopeValue::Database(domain, database) => {
                resource.domain() == Some(*domain)
                    && resource.database_name() == Some(database.as_str())
            }
            ScopeValue::AllDatabases(domain) => resource.domain() == Some(*domain),
            ScopeValue::AllSecurityRealms => resource.kind() == ResourceKind::SecurityRealm,
        }
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ScopeValue::Exact(resource) => {
                formatter.debug_tuple("ExactScope").field(resource).finish()
            }
            ScopeValue::Database(domain, _) => formatter
                .debug_struct("DatabaseScope")
                .field("domain", domain)
                .finish_non_exhaustive(),
            ScopeValue::AllDatabases(domain) => formatter
                .debug_tuple("AllDatabasesScope")
                .field(domain)
                .finish(),
            ScopeValue::AllSecurityRealms => formatter.write_str("AllSecurityRealmsScope"),
        }
    }
}

/// A validated action/scope pair. No action implies any other action.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Privilege {
    action: Action,
    scope: Scope,
}

impl Privilege {
    pub fn new(action: Action, scope: Scope) -> EngineResult<Self> {
        if !scope.accepts(action.resource_kind()) {
            return Err(invalid("authorization action does not apply to its scope"));
        }
        Ok(Self { action, scope })
    }

    pub const fn action(&self) -> Action {
        self.action
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }
}

/// Maximum distinct grants in one resolved policy, and input items to Policy::new.
pub const MAX_POLICY_PRIVILEGES: usize = 256;
/// Maximum already-bounded role policies to combine. No role inheritance/recursion.
pub const MAX_POLICY_ROLES: usize = 64;
/// Maximum resources checked for one operation before any work is performed.
pub const MAX_AUTHORIZATION_REQUIREMENTS: usize = 256;

/// Immutable, deny-by-default union of explicit positive privileges.
/// This performs decisions only; it does not bind a principal or enforce engine calls.
///
/// ```
/// use briskdb::core::authorization::{Action, DataDomain, Policy, Privilege, Resource, Scope};
///
/// let users = Resource::object(DataDomain::Document, "app", "users")?;
/// let role = Policy::new([
///     Privilege::new(Action::ReadData, Scope::exact(users.clone()))?,
/// ])?;
/// role.authorize(Action::ReadData, &users)?;
/// assert!(role.authorize(Action::UpdateData, &users).is_err());
/// # Ok::<_, briskdb::EngineError>(())
/// ```
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Policy {
    privileges: BTreeSet<Privilege>,
}

impl Policy {
    /// Bound work before deduplication, including an infinite duplicate iterator.
    pub fn new(privileges: impl IntoIterator<Item = Privilege>) -> EngineResult<Self> {
        let mut policy = Self::default();
        for (index, privilege) in privileges.into_iter().enumerate() {
            if index >= MAX_POLICY_PRIVILEGES {
                return Err(limit("authorization policy input exceeds its limit"));
            }
            policy.privileges.insert(privilege);
        }
        Ok(policy)
    }

    /// Resolve at most 64 flat role policies with at most 256 distinct grants.
    /// Overlapping grants deduplicate; neither ordering nor empty roles grant access.
    pub fn combine<'a>(roles: impl IntoIterator<Item = &'a Self>) -> EngineResult<Self> {
        let mut policy = Self::default();
        for (index, role) in roles.into_iter().enumerate() {
            if index >= MAX_POLICY_ROLES {
                return Err(limit("authorization role count exceeds its limit"));
            }
            for privilege in &role.privileges {
                if !policy.privileges.contains(privilege) {
                    if policy.privileges.len() == MAX_POLICY_PRIVILEGES {
                        return Err(limit("resolved authorization policy exceeds its limit"));
                    }
                    policy.privileges.insert(privilege.clone());
                }
            }
        }
        Ok(policy)
    }

    pub fn privilege_count(&self) -> usize {
        self.privileges.len()
    }

    /// Mis-scoped actions and ungranted requirements are always denied.
    pub fn allows(&self, action: Action, resource: &Resource) -> bool {
        action.resource_kind() == resource.kind()
            && self
                .privileges
                .iter()
                .any(|grant| grant.action == action && grant.scope.covers(resource))
    }

    pub fn authorize(&self, action: Action, resource: &Resource) -> EngineResult<()> {
        if self.allows(action, resource) {
            Ok(())
        } else {
            Err(denied())
        }
    }

    /// Check every requirement before execution. Empty and oversized lists fail
    /// closed; do not interleave these decisions with writes or returned results.
    pub fn authorize_all<'a>(
        &self,
        requirements: impl IntoIterator<Item = (Action, &'a Resource)>,
    ) -> EngineResult<()> {
        let mut requested = false;
        for (index, (action, resource)) in requirements.into_iter().enumerate() {
            if index >= MAX_AUTHORIZATION_REQUIREMENTS {
                return Err(limit("authorization requirement count exceeds its limit"));
            }
            self.authorize(action, resource)?;
            requested = true;
        }
        if !requested {
            return Err(denied());
        }
        Ok(())
    }
}

impl fmt::Debug for Policy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPolicy")
            .field("privilege_count", &self.privileges.len())
            .finish_non_exhaustive()
    }
}

fn validate_database_name(name: &str) -> EngineResult<()> {
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        return Err(invalid("authorization database or realm name is invalid"));
    }
    Ok(())
}

fn validate_data_database(domain: DataDomain, name: &str) -> EngineResult<()> {
    validate_database_name(name)?;
    if domain == DataDomain::Relational && !validate_catalog_identifier(name) {
        return Err(invalid(
            "relational authorization database name is not canonical",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, message)
}

fn limit(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::LimitExceeded, message)
}

fn denied() -> EngineError {
    EngineError::new(
        EngineErrorKind::PermissionDenied,
        "operation is not authorized",
    )
}

#[cfg(test)]
mod tests;
