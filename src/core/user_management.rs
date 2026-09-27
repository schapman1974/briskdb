//! Bounded, owned user-management commands. Privileges are derived by the engine,
//! not supplied by a protocol caller. Password hashing happens only after current
//! authorization, on the engine's bounded worker, before revision-checked commit.

use std::fmt;
use zeroize::Zeroizing;

#[cfg(test)]
mod tests;

use super::{
    EngineError, EngineErrorKind, EngineResult,
    authentication::{MAX_SCRAM_PASSWORD_BYTES, ScramSha256Verifier},
    authorization::{Action, MAX_POLICY_ROLES, Resource},
    security_catalog::{SecurityCatalog, SecurityName},
};

/// User commands never create implicit roles or an implicit administrator.
/// Passwords and account/role names are omitted from Debug. Owned passwords
/// zeroize on drop; callers remain responsible for their own buffers and logs.
pub struct UserManagementCommand(Kind);

enum Kind {
    Create {
        name: SecurityName,
        password: Zeroizing<String>,
        roles: Vec<SecurityName>,
    },
    ChangePassword {
        name: SecurityName,
        password: Zeroizing<String>,
    },
    Drop(SecurityName),
    GrantRoles {
        name: SecurityName,
        roles: Vec<SecurityName>,
    },
    RevokeRoles {
        name: SecurityName,
        roles: Vec<SecurityName>,
    },
}

impl UserManagementCommand {
    pub fn create(
        name: SecurityName,
        password: &str,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<Self> {
        Ok(Self(Kind::Create {
            name,
            password: secret(password)?,
            roles: bounded_roles(roles, false)?,
        }))
    }

    /// Requires RotateCredentials even for one's own account; there is no
    /// implicit self-service privilege. Rotation invalidates existing sessions.
    pub fn change_password(name: SecurityName, password: &str) -> EngineResult<Self> {
        Ok(Self(Kind::ChangePassword {
            name,
            password: secret(password)?,
        }))
    }

    pub fn drop_user(name: SecurityName) -> Self {
        Self(Kind::Drop(name))
    }

    /// GrantRole is required on every assigned role's realm. Role names must
    /// already exist; granting a role never silently creates or broadens it.
    pub fn grant_roles(
        name: SecurityName,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<Self> {
        Ok(Self(Kind::GrantRoles {
            name,
            roles: bounded_roles(roles, true)?,
        }))
    }

    /// RevokeRole is required on every removed role's realm, including no-ops.
    pub fn revoke_roles(
        name: SecurityName,
        roles: impl IntoIterator<Item = SecurityName>,
    ) -> EngineResult<Self> {
        Ok(Self(Kind::RevokeRoles {
            name,
            roles: bounded_roles(roles, true)?,
        }))
    }

    pub(crate) fn requirements(&self) -> EngineResult<Vec<(Action, Resource)>> {
        let realm = |name: &SecurityName| Resource::security_realm(name.realm());
        let mut requirements = Vec::new();
        match &self.0 {
            Kind::Create { name, roles, .. } => {
                requirements.push((Action::CreateUser, realm(name)?));
                for role in roles {
                    requirements.push((Action::GrantRole, realm(role)?));
                }
            }
            Kind::ChangePassword { name, .. } => {
                requirements.push((Action::RotateCredentials, realm(name)?))
            }
            Kind::Drop(name) => requirements.push((Action::DropUser, realm(name)?)),
            Kind::GrantRoles { roles, .. } => {
                for role in roles {
                    requirements.push((Action::GrantRole, realm(role)?));
                }
            }
            Kind::RevokeRoles { roles, .. } => {
                for role in roles {
                    requirements.push((Action::RevokeRole, realm(role)?));
                }
            }
        }
        Ok(requirements)
    }

    pub(crate) fn apply(self, catalog: &mut SecurityCatalog) -> EngineResult<()> {
        match self.0 {
            Kind::Create {
                name,
                password,
                roles,
            } => catalog.create_user(name, ScramSha256Verifier::from_password(&password)?, roles),
            Kind::ChangePassword { name, password } => {
                catalog.rotate_credentials(&name, ScramSha256Verifier::from_password(&password)?)
            }
            Kind::Drop(name) => catalog.drop_user(&name),
            Kind::GrantRoles { name, roles } => catalog.grant_user_roles(&name, roles),
            Kind::RevokeRoles { name, roles } => catalog.revoke_user_roles(&name, roles),
        }
    }
}

impl fmt::Debug for UserManagementCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserManagementCommand")
            .field(
                "kind",
                &match self.0 {
                    Kind::Create { .. } => "create",
                    Kind::ChangePassword { .. } => "change_password",
                    Kind::Drop(_) => "drop",
                    Kind::GrantRoles { .. } => "grant_roles",
                    Kind::RevokeRoles { .. } => "revoke_roles",
                },
            )
            .finish_non_exhaustive()
    }
}

fn secret(password: &str) -> EngineResult<Zeroizing<String>> {
    if password.is_empty() || password.len() > MAX_SCRAM_PASSWORD_BYTES {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "password length is invalid",
        ));
    }
    Ok(Zeroizing::new(password.to_owned()))
}

fn bounded_roles(
    roles: impl IntoIterator<Item = SecurityName>,
    nonempty: bool,
) -> EngineResult<Vec<SecurityName>> {
    let mut result = Vec::new();
    for role in roles {
        if result.len() >= MAX_POLICY_ROLES {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "role assignment exceeds its limit",
            ));
        }
        result.push(role);
    }
    if nonempty && result.is_empty() {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "at least one role is required",
        ));
    }
    Ok(result)
}
