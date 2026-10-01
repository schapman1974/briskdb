//! Stored flat-role inspection; membership/realm checks belong to the engine.

use super::*;
use crate::core::security_catalog::{
    MAX_ROLE_INFO_SELECTORS, RoleInfo, RoleInfoRequest, SecurityName,
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
        "rolesInfo",
        "showPrivileges",
        "showAuthenticationRestrictions",
        "showBuiltinRoles",
        "$db",
    ];
    let mut seen = 0u8;
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
            // No partial/misleading export of Brisk-specific policies or built-ins.
            "showPrivileges" if !matches!(value, BsonValue::Boolean(_)) => {
                return Err(CommandError::options());
            }
            "showAuthenticationRestrictions" | "showBuiltinRoles"
                if *value != BsonValue::Boolean(false) =>
            {
                return Err(CommandError::options());
            }
            _ => {}
        }
    }
    let selection = match request.body.get_first("rolesInfo") {
        Some(BsonValue::Int32(1) | BsonValue::Int64(1)) => {
            RoleInfoRequest::realm(&request.database)?
        }
        Some(BsonValue::Array(values)) => {
            if values.len() > MAX_ROLE_INFO_SELECTORS {
                return Err(CommandError::invalid());
            }
            RoleInfoRequest::names(
                values
                    .iter()
                    .map(|value| name(value, &request.database))
                    .collect::<Result<Vec<_>>>()?,
            )?
        }
        Some(value) => RoleInfoRequest::names([name(value, &request.database)?])?,
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
        command: Command::RoleInfo(selection.with_document_privileges(
            request.body.get_first("showPrivileges") == Some(&BsonValue::Boolean(true)),
        )),
        deadline,
        advisory_hint: false,
    })
}

fn name(value: &BsonValue, realm: &str) -> Result<SecurityName> {
    match value {
        BsonValue::String(name) => Ok(SecurityName::new(realm, name)?),
        BsonValue::Document(document) => {
            let mut role = None;
            let mut realm = None;
            for (field, value) in document.iter() {
                match (field, value) {
                    ("role", BsonValue::String(name)) if role.is_none() => {
                        role = Some(name.as_str())
                    }
                    ("db", BsonValue::String(name)) if realm.is_none() => {
                        realm = Some(name.as_str())
                    }
                    _ => return Err(CommandError::invalid()),
                }
            }
            Ok(SecurityName::new(
                realm.ok_or_else(CommandError::invalid)?,
                role.ok_or_else(CommandError::invalid)?,
            )?)
        }
        _ => Err(CommandError::invalid()),
    }
}

pub(super) fn reply(roles: Vec<RoleInfo>) -> BsonDocument {
    let roles = roles
        .into_iter()
        .map(|role| {
            let name = role.name();
            let mut document = fields([
                (
                    "_id",
                    BsonValue::from(format!("{}.{}", name.realm(), name.name())),
                ),
                ("role", BsonValue::from(name.name())),
                ("db", BsonValue::from(name.realm())),
                ("isBuiltin", BsonValue::Boolean(false)),
                ("roles", BsonValue::Array(vec![])),
                ("inheritedRoles", BsonValue::Array(vec![])),
            ]);
            if let Some(privileges) = role.document_privileges() {
                let values: Vec<_> = privileges
                    .iter()
                    .map(|privilege| {
                        BsonValue::Document(fields([
                            (
                                "resource",
                                BsonValue::Document(fields([
                                    ("db", BsonValue::from(privilege.database())),
                                    ("collection", BsonValue::from(privilege.collection())),
                                ])),
                            ),
                            (
                                "actions",
                                BsonValue::Array(vec![BsonValue::from(privilege.action())]),
                            ),
                        ]))
                    })
                    .collect();
                document
                    .push("privileges", BsonValue::Array(values.clone()))
                    .expect("fixed field");
                document
                    .push("inheritedPrivileges", BsonValue::Array(values))
                    .expect("fixed field");
            }
            BsonValue::Document(document)
        })
        .collect();
    fields([
        ("roles", BsonValue::Array(roles)),
        ("ok", BsonValue::Double(1.0)),
    ])
}

#[cfg(test)]
mod tests;
