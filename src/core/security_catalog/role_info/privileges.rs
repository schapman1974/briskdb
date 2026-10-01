//! Lossless projection of the bounded Mongo policy subset.
use super::*;
use crate::core::authorization::{DataDomain, MAX_POLICY_PRIVILEGES, Privilege, Scope, ScopeValue};

pub(super) fn project(
    name: &SecurityName,
    policy: &Policy,
    bytes: &mut u64,
    max_bytes: u64,
) -> EngineResult<Vec<DocumentRolePrivilege>> {
    let unsupported = || {
        error(
            EngineErrorKind::Unsupported,
            "role policy cannot be represented as Mongo document privileges",
        )
    };
    let database = Resource::database(DataDomain::Document, name.realm())?;
    let mut reconstructed = Vec::new();
    let mut result = Vec::new();
    let mut wire_grants = 0usize;
    for grant in policy.privileges() {
        // These are valid omissions only if reconstruction below reproduces
        // them exactly as the implicit admission grants of the public subset.
        if matches!(
            grant.action(),
            Action::ConnectDatabase | Action::CreateDatabase
        ) {
            continue;
        }
        let collection = match grant.scope().stored_value() {
            ScopeValue::Exact(resource)
                if resource.domain() == Some(DataDomain::Document)
                    && resource.database_name() == Some(name.realm()) =>
            {
                match resource.object_name() {
                    Some(collection) => collection,
                    None if matches!(
                        grant.action(),
                        Action::ListObjects | Action::DropDatabase
                    ) =>
                    {
                        ""
                    }
                    _ => return Err(unsupported()),
                }
            }
            ScopeValue::NonSystemDocumentCollections(db) if db == name.realm() => "",
            _ => return Err(unsupported()),
        };
        let action = match grant.action() {
            Action::ReadData => "find",
            Action::InsertData => "insert",
            Action::UpdateData => "update",
            Action::DeleteData => "remove",
            Action::ListIndexes => "listIndexes",
            Action::CreateIndex => "createIndex",
            Action::DropIndex => "dropIndex",
            Action::CreateObject => "createCollection",
            Action::DropObject => "dropCollection",
            Action::ListObjects if collection.is_empty() => "listCollections",
            Action::DropDatabase if collection.is_empty() => "dropDatabase",
            _ => return Err(unsupported()),
        };
        // The public parser bounds pre-deduplication grants, including one
        // implicit create-database entry for each create-collection action.
        wire_grants += 1 + usize::from(grant.action() == Action::CreateObject);
        if wire_grants + 1 > MAX_POLICY_PRIVILEGES {
            return Err(unsupported());
        }
        // Both direct and inherited privilege arrays repeat the projection.
        // Reserve a conservative BSON/allocation bound before copying strings.
        *bytes = bytes
            .checked_add(2 * (256 + name.realm().len() as u64 + collection.len() as u64))
            .ok_or_else(limit)?;
        if *bytes > max_bytes {
            return Err(limit());
        }
        reconstructed.push(grant.clone());
        result.push(DocumentRolePrivilege {
            database: name.realm().to_owned(),
            collection: collection.to_owned(),
            action,
        });
    }
    let mut reconstructed = Policy::new(reconstructed)?;
    if !result.is_empty() {
        let mut admission = vec![Privilege::new(
            Action::ConnectDatabase,
            Scope::exact(database.clone()),
        )?];
        if result
            .iter()
            .any(|entry| entry.action == "createCollection")
        {
            admission.push(Privilege::new(
                Action::CreateDatabase,
                Scope::exact(database),
            )?);
        }
        let admission = Policy::new(admission)?;
        reconstructed = Policy::combine([&reconstructed, &admission])?;
    }
    if &reconstructed != policy {
        return Err(unsupported());
    }
    Ok(result)
}
