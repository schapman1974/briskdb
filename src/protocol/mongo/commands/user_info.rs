//! Credential-free account inspection. Selection permissions belong to the engine.

use super::*;
use crate::core::security_catalog::{
    MAX_USER_INFO_SELECTORS, SecurityName, UserInfo, UserInfoRequest,
};

pub(super) fn prepare(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    if request.more_to_come || request.legacy_handshake || !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    let allowed = [
        "usersInfo",
        "showCredentials",
        "showCustomData",
        "showPrivileges",
        "showAuthenticationRestrictions",
        "filter",
        "$db",
    ];
    let mut seen = 0u16;
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
            // Deliberately never expose credential hashes, even to administrators.
            "showCredentials" | "showPrivileges" | "showAuthenticationRestrictions"
                if *value != BsonValue::Boolean(false) =>
            {
                return Err(CommandError::options());
            }
            "showCustomData" if !matches!(value, BsonValue::Boolean(_)) => {
                return Err(CommandError::options());
            }
            "filter" if !matches!(value, BsonValue::Document(document) if document.is_empty()) => {
                return Err(CommandError::options());
            }
            _ => {}
        }
    }
    let selection = match request.body.get_first("usersInfo") {
        Some(BsonValue::Int32(1) | BsonValue::Int64(1)) => {
            UserInfoRequest::realm(&request.database)?
        }
        Some(BsonValue::Array(values)) => {
            if values.len() > MAX_USER_INFO_SELECTORS {
                return Err(CommandError::invalid());
            }
            UserInfoRequest::names(
                values
                    .iter()
                    .map(|value| name(value, &request.database))
                    .collect::<Result<Vec<_>>>()?,
            )?
        }
        Some(value) => UserInfoRequest::names([name(value, &request.database)?])?,
        None => return Err(CommandError::invalid()),
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
        command: Command::UserInfo(selection),
        deadline,
        advisory_hint: false,
    })
}

fn name(value: &BsonValue, current_realm: &str) -> Result<SecurityName> {
    match value {
        BsonValue::String(name) => Ok(SecurityName::new(current_realm, name)?),
        BsonValue::Document(document) => {
            let mut user = None;
            let mut realm = None;
            for (field, value) in document.iter() {
                match (field, value) {
                    ("user", BsonValue::String(name)) if user.is_none() => {
                        user = Some(name.as_str())
                    }
                    ("db", BsonValue::String(name)) if realm.is_none() => {
                        realm = Some(name.as_str())
                    }
                    _ => return Err(CommandError::invalid()),
                }
            }
            Ok(SecurityName::new(
                realm.ok_or_else(CommandError::invalid)?,
                user.ok_or_else(CommandError::invalid)?,
            )?)
        }
        _ => Err(CommandError::invalid()),
    }
}

pub(super) fn reply(users: Vec<UserInfo>) -> BsonDocument {
    let users = users
        .into_iter()
        .map(|user| {
            let name = user.name();
            let roles = user
                .roles()
                .iter()
                .map(|role| {
                    BsonValue::Document(fields([
                        ("role", BsonValue::from(role.name())),
                        ("db", BsonValue::from(role.realm())),
                    ]))
                })
                .collect();
            BsonValue::Document(fields([
                (
                    "_id",
                    BsonValue::from(format!("{}.{}", name.realm(), name.name())),
                ),
                ("user", BsonValue::from(name.name())),
                ("db", BsonValue::from(name.realm())),
                ("roles", BsonValue::Array(roles)),
                (
                    "mechanisms",
                    BsonValue::Array(vec![BsonValue::from("SCRAM-SHA-256")]),
                ),
            ]))
        })
        .collect();
    fields([
        ("users", BsonValue::Array(users)),
        ("ok", BsonValue::Double(1.0)),
    ])
}

#[cfg(test)]
mod tests;
