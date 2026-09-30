//! Strict flat role administration using the same durable authority as user edits.

use super::*;
use crate::core::security_catalog::SecurityName;

pub(super) fn prepare_drop(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    if request.more_to_come || request.legacy_handshake || !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    if request.database == "local" {
        return Err(CommandError::unsupported());
    }
    let allowed = ["dropRole", "writeConcern", "$db"];
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
        if field == "writeConcern" {
            users::write_concern(value)?;
        }
    }
    let Some(BsonValue::String(name)) = request.body.get_first("dropRole") else {
        return Err(CommandError::invalid());
    };
    let name = SecurityName::new(&request.database, name)?;
    let deadline = started + limits.command_timeout();
    if Instant::now() >= deadline {
        return Err(CommandError::new(
            50,
            "MaxTimeMSExpired",
            "command deadline exceeded",
        ));
    }
    Ok(Prepared {
        command: Command::DropRole(name),
        deadline,
        advisory_hint: false,
    })
}

#[cfg(test)]
mod tests;

pub(super) fn prepare_create(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    prepare_policy(request, started, limits, false)
}

pub(super) fn prepare_grant(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
) -> Result<Prepared> {
    prepare_policy(request, started, limits, true)
}

fn prepare_policy(
    request: &Request,
    started: Instant,
    limits: super::super::MongoResourceLimits,
    grant: bool,
) -> Result<Prepared> {
    if request.more_to_come || request.legacy_handshake || !request.sequences.is_empty() {
        return Err(CommandError::options());
    }
    if request.database == "local" {
        return Err(CommandError::unsupported());
    }
    let command_name = if grant {
        "grantPrivilegesToRole"
    } else {
        "createRole"
    };
    let allowed: &[&str] = if grant {
        &["grantPrivilegesToRole", "privileges", "writeConcern", "$db"]
    } else {
        &["createRole", "privileges", "roles", "writeConcern", "$db"]
    };
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
        if field == "writeConcern" {
            users::write_concern(value)?;
        }
    }
    let Some(BsonValue::String(name)) = request.body.get_first(command_name) else {
        return Err(CommandError::invalid());
    };
    let name = SecurityName::new(&request.database, name)?;
    if !grant {
        let Some(BsonValue::Array(roles)) = request.body.get_first("roles") else {
            return Err(CommandError::invalid());
        };
        if !roles.is_empty() {
            return Err(CommandError::options());
        }
    }
    let Some(BsonValue::Array(privileges)) = request.body.get_first("privileges") else {
        return Err(CommandError::invalid());
    };
    let policy = data_policy(&request.database, privileges)?;
    let deadline = started + limits.command_timeout();
    if Instant::now() >= deadline {
        return Err(CommandError::new(
            50,
            "MaxTimeMSExpired",
            "command deadline exceeded",
        ));
    }
    Ok(Prepared {
        command: if grant {
            Command::GrantRolePrivileges(name, policy)
        } else {
            Command::CreateRole(name, policy)
        },
        deadline,
        advisory_hint: false,
    })
}

fn data_policy(
    database: &str,
    privileges: &[BsonValue],
) -> Result<crate::core::authorization::Policy> {
    use crate::core::authorization::{
        Action, DataDomain, MAX_POLICY_PRIVILEGES, Policy, Privilege, Resource, Scope,
    };
    if privileges.len() > MAX_POLICY_PRIVILEGES {
        return Err(CommandError::invalid());
    }
    let db = Resource::database(DataDomain::Document, database)?;
    let mut grants = Vec::new();
    let mut actions_seen = 0usize;
    for privilege in privileges {
        let BsonValue::Document(privilege) = privilege else {
            return Err(CommandError::invalid());
        };
        exact_fields(privilege, ["resource", "actions"])?;
        let Some(BsonValue::Document(resource)) = privilege.get_first("resource") else {
            return Err(CommandError::invalid());
        };
        exact_fields(resource, ["db", "collection"])?;
        let (Some(BsonValue::String(target)), Some(BsonValue::String(collection))) =
            (resource.get_first("db"), resource.get_first("collection"))
        else {
            return Err(CommandError::invalid());
        };
        // Even admin roles are database-local in this deliberately bounded subset.
        if target != database {
            return Err(CommandError::options());
        }
        let scope = if collection.is_empty() {
            Scope::non_system_document_collections(database)?
        } else {
            Scope::exact(Resource::object(
                DataDomain::Document,
                database,
                collection,
            )?)
        };
        let Some(BsonValue::Array(actions)) = privilege.get_first("actions") else {
            return Err(CommandError::invalid());
        };
        if actions.is_empty() {
            return Err(CommandError::invalid());
        }
        actions_seen = actions_seen.saturating_add(actions.len());
        if actions_seen > MAX_POLICY_PRIVILEGES {
            return Err(CommandError::invalid());
        }
        for action in actions {
            let BsonValue::String(action) = action else {
                return Err(CommandError::invalid());
            };
            let action = match action.as_str() {
                "find" => Action::ReadData,
                "insert" => Action::InsertData,
                "update" => Action::UpdateData,
                "remove" => Action::DeleteData,
                "listIndexes" => Action::ListIndexes,
                "createIndex" => Action::CreateIndex,
                "dropIndex" => Action::DropIndex,
                "createCollection" => Action::CreateObject,
                "dropCollection" => Action::DropObject,
                "listCollections" if collection.is_empty() => Action::ListObjects,
                "dropDatabase" if collection.is_empty() => Action::DropDatabase,
                _ => return Err(CommandError::options()),
            };
            let scope = if matches!(action, Action::ListObjects | Action::DropDatabase) {
                Scope::exact(db.clone())
            } else {
                scope.clone()
            };
            if action == Action::CreateObject {
                grants.push(Privilege::new(
                    Action::CreateDatabase,
                    Scope::exact(db.clone()),
                )?);
            }
            grants.push(Privilege::new(action, scope)?);
            if grants.len() >= MAX_POLICY_PRIVILEGES {
                return Err(CommandError::invalid());
            }
        }
    }
    if !grants.is_empty() {
        // BriskDB requires database admission independently of every data action.
        grants.push(Privilege::new(Action::ConnectDatabase, Scope::exact(db))?);
    }
    Ok(Policy::new(grants)?)
}

fn exact_fields(document: &BsonDocument, allowed: [&str; 2]) -> Result<()> {
    let mut seen = 0u8;
    for (field, _) in document.iter() {
        let index = allowed
            .iter()
            .position(|allowed| *allowed == field)
            .ok_or_else(CommandError::options)?;
        if seen & (1 << index) != 0 {
            return Err(CommandError::invalid());
        }
        seen |= 1 << index;
    }
    if seen != 3 {
        return Err(CommandError::invalid());
    }
    Ok(())
}
