//! Bounded role-name inspection under current realm/membership authority.

use super::*;
use crate::core::ResultLimits;

pub const MAX_ROLE_INFO_SELECTORS: usize = 64;

/// Exact role references or one whole realm; no wildcard/all-realms selection.
/// An empty selection still validates the current principal.
#[derive(Clone)]
pub struct RoleInfoRequest(Selection);

#[derive(Clone)]
enum Selection {
    Names(BTreeSet<SecurityName>),
    Realm(Resource),
}

impl RoleInfoRequest {
    pub fn names(names: impl IntoIterator<Item = SecurityName>) -> EngineResult<Self> {
        let mut selected = BTreeSet::new();
        for (index, name) in names.into_iter().enumerate() {
            if index >= MAX_ROLE_INFO_SELECTORS {
                return Err(limit());
            }
            selected.insert(name);
        }
        Ok(Self(Selection::Names(selected)))
    }

    pub fn realm(realm: &str) -> EngineResult<Self> {
        Ok(Self(Selection::Realm(Resource::security_realm(realm)?)))
    }
}

impl fmt::Debug for RoleInfoRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RoleInfoRequest { .. }")
    }
}

/// Metadata for one stored flat role, not a policy or credential export.
/// No inheritance or implicit/protected built-in role namespace is supplied.
#[derive(Clone, PartialEq, Eq)]
pub struct RoleInfo {
    name: SecurityName,
}

impl RoleInfo {
    pub fn name(&self) -> &SecurityName {
        &self.name
    }
}

impl fmt::Debug for RoleInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RoleInfo { .. }")
    }
}

impl SecurityCatalog {
    /// Inspect directly assigned roles without ViewRoles. Other exact roles
    /// require ViewRoles on their realms, even if missing; whole-realm listing
    /// always requires it. All checks precede role lookup/result allocation.
    /// Results use realm/name order, omit authorized missing names, and enforce
    /// conservative metadata byte/row budgets rather than exact BSON/RSS sizes.
    pub fn role_info(
        &self,
        principal: &Principal,
        request: &RoleInfoRequest,
        limits: ResultLimits,
    ) -> EngineResult<Vec<RoleInfo>> {
        let policy = self.current_policy(principal)?;
        let user = self.current_user(principal)?;
        match &request.0 {
            Selection::Names(names) => {
                for name in names {
                    if !user.roles.contains(name) {
                        policy.authorize(
                            Action::ViewRoles,
                            &Resource::security_realm(name.realm())?,
                        )?;
                    }
                }
            }
            Selection::Realm(realm) => policy.authorize(Action::ViewRoles, realm)?,
        }
        let mut bytes = 64u64;
        if bytes > limits.max_bytes() {
            return Err(limit());
        }
        let mut result = Vec::new();
        for name in self.roles.keys() {
            let selected = match &request.0 {
                Selection::Names(names) => names.contains(name),
                Selection::Realm(realm) => Some(name.realm()) == realm.realm_name(),
            };
            if !selected {
                continue;
            }
            bytes += 256 + 2 * (name.realm().len() + name.name().len()) as u64;
            if result.len() as u64 >= limits.max_rows() || bytes > limits.max_bytes() {
                return Err(limit());
            }
            result.push(RoleInfo { name: name.clone() });
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
