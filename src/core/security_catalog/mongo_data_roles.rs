//! Explicit host-provisioned Mongo data-role profiles, not implicit roles.

use super::*;
use crate::core::authorization::{DataDomain, Privilege, Scope};

/// The supported data/schema subset of Mongo's database-local data roles.
/// Unsupported Mongo operations remain unsupported. These profiles grant no
/// user/role administration, SQL access, database deletion or global discovery.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoDataRole {
    Read,
    ReadWrite,
}

impl MongoDataRole {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ReadWrite => "readWrite",
        }
    }

    /// Construct a bounded policy without changing any catalog or session.
    /// Object grants cover non-system collections plus exactly `system.js`.
    /// BriskDB's create-database permission is included only for readWrite,
    /// because collection creation may create this exact logical database.
    pub fn policy(self, database: &str) -> EngineResult<Policy> {
        let db = Resource::database(DataDomain::Document, database)?;
        let collections = Scope::non_system_document_collections(database)?;
        let javascript = Scope::exact(Resource::object(
            DataDomain::Document,
            database,
            "system.js",
        )?);
        let mut grants = Vec::new();
        for action in [Action::ConnectDatabase, Action::ListObjects] {
            grants.push(Privilege::new(action, Scope::exact(db.clone()))?);
        }
        let mut object_actions = vec![Action::ReadData, Action::ListIndexes];
        if self == Self::ReadWrite {
            grants.push(Privilege::new(Action::CreateDatabase, Scope::exact(db))?);
            object_actions.extend([
                Action::InsertData,
                Action::UpdateData,
                Action::DeleteData,
                Action::CreateObject,
                Action::DropObject,
                Action::CreateIndex,
                Action::DropIndex,
            ]);
        }
        for action in object_actions {
            grants.push(Privilege::new(action, collections.clone())?);
            grants.push(Privilege::new(action, javascript.clone())?);
        }
        Policy::new(grants)
    }
}

impl SecurityCatalog {
    /// Trusted provisioning of the supported `read` and `readWrite` profiles
    /// in one exact database. Both are installed or neither is. A collision
    /// (even an identical existing policy) or capacity limit changes nothing.
    ///
    /// These are explicit, flat stored roles, not a virtual built-in namespace:
    /// they count toward catalog limits and trusted hosts may replace/drop them.
    /// No users, memberships, logical databases or implicit admin are created.
    /// Call through `Engine::update_security_catalog` for an active secured root.
    ///
    /// ```
    /// use briskdb::core::security_catalog::{SecurityCatalog, SecurityName};
    /// let mut catalog = SecurityCatalog::new();
    /// catalog.provision_mongo_data_roles("app")?;
    /// // Assign this pre-provisioned role through create_user or authorized
    /// // Mongo createUser/grantRolesToUser: {role: "readWrite", db: "app"}.
    /// let writer_role = SecurityName::new("app", "readWrite")?;
    /// assert_eq!(catalog.role_count(), 2);
    /// # Ok::<_, briskdb::EngineError>(())
    /// ```
    pub fn provision_mongo_data_roles(&mut self, database: &str) -> EngineResult<()> {
        let profiles = [MongoDataRole::Read, MongoDataRole::ReadWrite]
            .into_iter()
            .map(|role| {
                Ok((
                    SecurityName::new(database, role.name())?,
                    role.policy(database)?,
                ))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        if profiles
            .iter()
            .any(|(name, _)| self.roles.contains_key(name))
        {
            return Err(already_exists());
        }
        if self.roles.len() + profiles.len() > MAX_SECURITY_ROLES {
            return Err(limit());
        }
        self.roles.extend(profiles);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
