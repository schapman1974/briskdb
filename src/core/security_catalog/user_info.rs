//! Credential-free, permission-checked account metadata, never catalog export.

use super::*;
use crate::core::ResultLimits;

pub const MAX_USER_INFO_SELECTORS: usize = 64;

/// A bounded exact selection or one complete realm. No wildcard/all-realms query.
/// Debug omits names. An empty exact selection reveals nothing but still checks
/// that the caller's principal is current.
#[derive(Clone)]
pub struct UserInfoRequest(Selection);

#[derive(Clone)]
enum Selection {
    Names(BTreeSet<SecurityName>),
    Realm(Resource),
}

impl UserInfoRequest {
    pub fn names(names: impl IntoIterator<Item = SecurityName>) -> EngineResult<Self> {
        let mut selected = BTreeSet::new();
        for (index, name) in names.into_iter().enumerate() {
            if index >= MAX_USER_INFO_SELECTORS {
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

impl fmt::Debug for UserInfoRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UserInfoRequest { .. }")
    }
}

/// Only public account metadata. No verifier, salt, password, proof, credential
/// generation or internal account ID is copied into this value.
#[derive(Clone, PartialEq, Eq)]
pub struct UserInfo {
    name: SecurityName,
    roles: Vec<SecurityName>,
}

impl UserInfo {
    pub fn name(&self) -> &SecurityName {
        &self.name
    }

    pub fn roles(&self) -> &[SecurityName] {
        &self.roles
    }
}

impl fmt::Debug for UserInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserInfo")
            .field("role_count", &self.roles.len())
            .finish_non_exhaustive()
    }
}

impl SecurityCatalog {
    /// Validate current identity and every requested realm before looking up any
    /// selected account. Exact self-inspection is permitted without ViewUsers;
    /// whole-realm listing always requires it. Missing authorized names are omitted.
    /// Results follow canonical realm/name order and use conservative metadata
    /// byte accounting, not exact BSON size or an allocator/RSS guarantee.
    pub fn user_info(
        &self,
        principal: &Principal,
        request: &UserInfoRequest,
        limits: ResultLimits,
    ) -> EngineResult<Vec<UserInfo>> {
        let policy = self.current_policy(principal)?;
        match &request.0 {
            Selection::Names(names) => {
                for name in names {
                    if name != principal.name() {
                        policy.authorize(
                            Action::ViewUsers,
                            &Resource::security_realm(name.realm())?,
                        )?;
                    }
                }
            }
            Selection::Realm(realm) => policy.authorize(Action::ViewUsers, realm)?,
        }
        // Charge a fixed empty envelope and each selected row before cloning it.
        let mut bytes = 64u64;
        if bytes > limits.max_bytes() {
            return Err(limit());
        }
        let mut result = Vec::new();
        for (name, user) in &self.users {
            let selected = match &request.0 {
                Selection::Names(names) => names.contains(name),
                Selection::Realm(realm) => Some(name.realm()) == realm.realm_name(),
            };
            if !selected {
                continue;
            }
            bytes += 256 + 2 * (name.realm().len() + name.name().len()) as u64;
            for role in &user.roles {
                bytes += 128 + (role.realm().len() + role.name().len()) as u64;
            }
            if result.len() as u64 >= limits.max_rows() || bytes > limits.max_bytes() {
                return Err(limit());
            }
            result.push(UserInfo {
                name: name.clone(),
                roles: user.roles.iter().cloned().collect(),
            });
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
