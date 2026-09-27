//! Strict, opt-in user command translation. No passwords or arbitrary diagnostics
//! enter replies/telemetry, and no plaintext/anonymous root gains administration.

use super::*;
use crate::core::{
    authorization::MAX_POLICY_ROLES, security_catalog::SecurityName,
    user_management::UserManagementCommand,
};

#[cfg(test)]
mod tests;

pub(super) fn is_command(name: &str) -> bool {
    matches!(
        name,
        "createUser" | "updateUser" | "dropUser" | "grantRolesToUser" | "revokeRolesFromUser"
    )
}

pub(super) fn prepare(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    if request.more_to_come || request.legacy_handshake || !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    let Some((command, BsonValue::String(username))) = request.body.iter().next() else {
        return Err(CommandError::invalid());
    };
    if request.database == "local" {
        return Err(CommandError::unsupported());
    }
    let name = SecurityName::new(&request.database, username)?;
    let allowed: &[&str] = match command {
        "createUser" => &[
            "createUser",
            "pwd",
            "roles",
            "mechanisms",
            "digestPassword",
            "writeConcern",
            "$db",
        ],
        "updateUser" => &[
            "updateUser",
            "pwd",
            "mechanisms",
            "digestPassword",
            "writeConcern",
            "$db",
        ],
        "dropUser" => &["dropUser", "writeConcern", "$db"],
        "grantRolesToUser" => &["grantRolesToUser", "roles", "writeConcern", "$db"],
        "revokeRolesFromUser" => &["revokeRolesFromUser", "roles", "writeConcern", "$db"],
        _ => return Err(CommandError::unsupported()),
    };
    let mut seen = 0u32;
    for (field, value) in request.body.iter() {
        let index = allowed
            .iter()
            .position(|allowed| *allowed == field)
            .ok_or_else(CommandError::options)?;
        if seen & (1 << index) != 0 {
            return Err(CommandError::invalid());
        }
        seen |= 1 << index;
        match field {
            "mechanisms" if !matches!(value, BsonValue::Array(values) if values.as_slice() == [BsonValue::from("SCRAM-SHA-256")]) =>
            {
                return Err(CommandError::options());
            }
            "digestPassword" if *value != BsonValue::Boolean(true) => {
                return Err(CommandError::options());
            }
            "writeConcern" => {
                let BsonValue::Document(concern) = value else {
                    return Err(CommandError::options());
                };
                let mut seen_w = false;
                for (key, value) in concern.iter() {
                    if key != "w"
                        || seen_w
                        || !matches!(value, BsonValue::Int32(1) | BsonValue::Int64(1))
                    {
                        return Err(CommandError::options());
                    }
                    seen_w = true;
                }
            }
            _ => {}
        }
    }
    let password = || match request.body.get_first("pwd") {
        Some(BsonValue::String(value)) => Ok(value.as_str()),
        _ => Err(CommandError::invalid()),
    };
    let command = match command {
        "createUser" => UserManagementCommand::create(name, password()?, roles(request)?)?,
        // Replacing the whole roles array has a different global revoke contract;
        // use the scoped grant/revoke commands, not a silently narrowed update.
        "updateUser" => UserManagementCommand::change_password(name, password()?)?,
        "dropUser" => UserManagementCommand::drop_user(name),
        "grantRolesToUser" => UserManagementCommand::grant_roles(name, roles(request)?)?,
        "revokeRolesFromUser" => UserManagementCommand::revoke_roles(name, roles(request)?)?,
        _ => unreachable!("validated command"),
    };
    let deadline = started + limits.command_timeout();
    if Instant::now() >= deadline {
        return Err(CommandError::new(
            50,
            "MaxTimeMSExpired",
            "command deadline exceeded",
        ));
    }
    Ok(Prepared {
        command: Command::UserManagement(command),
        deadline,
        advisory_hint: false,
    })
}

fn roles(request: &Request) -> Result<Vec<SecurityName>> {
    let Some(BsonValue::Array(values)) = request.body.get_first("roles") else {
        return Err(CommandError::invalid());
    };
    if values.len() > MAX_POLICY_ROLES {
        return Err(CommandError::invalid());
    }
    values
        .iter()
        .map(|value| match value {
            BsonValue::String(name) => Ok(SecurityName::new(&request.database, name)?),
            BsonValue::Document(role) => {
                let mut name = None;
                let mut realm = None;
                for (field, value) in role.iter() {
                    match (field, value) {
                        ("role", BsonValue::String(value)) if name.is_none() => {
                            name = Some(value.as_str())
                        }
                        ("db", BsonValue::String(value)) if realm.is_none() => {
                            realm = Some(value.as_str())
                        }
                        _ => return Err(CommandError::invalid()),
                    }
                }
                Ok(SecurityName::new(
                    realm.ok_or_else(CommandError::invalid)?,
                    name.ok_or_else(CommandError::invalid)?,
                )?)
            }
            _ => Err(CommandError::invalid()),
        })
        .collect()
}
