//! Offline provisioning and identity-bound access to a root's security authority.

use std::{fs::File, path::Path};

use super::{
    CONNECTION_BUSY_TIMEOUT, ROOT_SCHEMA_COORDINATIONS, configure_manifest_connection_with_policy,
    manifest, open_existing_manifest, process_lock,
    security_catalog::{SecurityCatalogStore, SecurityStoreId, private_path},
};
use crate::core::{
    EngineError, EngineErrorKind, EngineResult, StorageProfile,
    security_catalog::{DurableSecurityCatalog, SecurityCatalog},
};

pub(crate) const FILE_NAME: &str = "security.sqlite";

/// The startup lock excludes new local/peer openers. The registry check excludes
/// existing local owners (which share one process lease); the exclusive lease
/// separately excludes existing owners in other processes.
pub(crate) fn provision(
    root: &Path,
    shards: u16,
    catalog: &SecurityCatalog,
) -> EngineResult<SecurityStoreId> {
    provision_with_profile(root, shards, catalog, StorageProfile::Local)
}

pub(crate) fn provision_with_profile(
    root: &Path,
    shards: u16,
    catalog: &SecurityCatalog,
    profile: StorageProfile,
) -> EngineResult<SecurityStoreId> {
    super::validate_shard_count(shards)?;
    if catalog.user_count() == 0 {
        return Err(EngineError::new(
            EngineErrorKind::InvalidArgument,
            "security activation requires at least one provisioned user",
        ));
    }
    let path = private_path(&root.join(FILE_NAME))?;
    let root = path.parent().expect("canonical private parent");
    let _startup = process_lock::RootStartupGuard::acquire(root, CONNECTION_BUSY_TIMEOUT)?;
    if let Some(registry) = ROOT_SCHEMA_COORDINATIONS.get() {
        let registry = registry.lock().map_err(|_| {
            EngineError::new(
                EngineErrorKind::Internal,
                "root coordination registry is poisoned",
            )
        })?;
        if registry
            .get(root)
            .is_some_and(|owner| owner.strong_count() != 0)
        {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "security activation requires all local database handles to close",
            ));
        }
    }
    let lease = process_lock::RootProcessLease::acquire(root)?;
    let _exclusive = lease.try_acquire_exclusive()?;
    let mut manifest = open_existing_manifest(&root.join("manifest.sqlite"))?;
    let journal = prepare_manifest(&manifest, shards, profile)?;
    manifest::validate_security_activation(&manifest, shards)?;
    let id = SecurityStoreId::generate()?;
    // Creation is exclusive and durable before the manifest can refer to it.
    // Any orphan after failure is retained for explicit operator recovery;
    // retries never adopt, truncate, replace or delete that file.
    let mut store =
        SecurityCatalogStore::create_with_journal(&path, id, catalog, security_journal(journal))?;
    store.load()?;
    manifest::activate_security_binding(&mut manifest, shards, *id.as_bytes())?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            crate::sqlite_error::storage_io(error, "failed to sync security activation directory")
        })?;
    Ok(id)
}

pub(crate) fn open(root: &Path, shards: u16) -> EngineResult<DurableSecurityCatalog> {
    open_with_profile(root, shards, StorageProfile::Local)
}

fn security_journal(journal: super::journal::JournalPolicy) -> super::journal::JournalPolicy {
    if journal.is_wal() {
        super::journal::JournalPolicy::SECURITY
    } else {
        journal
    }
}

fn prepare_manifest(
    connection: &rusqlite::Connection,
    shards: u16,
    profile: StorageProfile,
) -> EngineResult<super::journal::JournalPolicy> {
    let initial = match profile {
        StorageProfile::Local => super::journal::JournalPolicy::LOCAL,
        StorageProfile::Nfs => super::journal::JournalPolicy::NFS_PERSIST,
    };
    configure_manifest_connection_with_policy(connection, initial)?;
    let id = manifest::security_binding(connection, shards)?;
    let (_, journal) = manifest::startup_profile(connection, shards, id, profile)?;
    journal.configure_existing_mode(
        connection,
        EngineErrorKind::FailedPrecondition,
        "security root manifest",
    )?;
    Ok(journal)
}

pub(super) fn open_with_profile(
    root: &Path,
    shards: u16,
    profile: StorageProfile,
) -> EngineResult<DurableSecurityCatalog> {
    super::validate_shard_count(shards)?;
    let path = private_path(&root.join(FILE_NAME))?;
    let root = path.parent().expect("canonical private parent");
    let mut manifest = open_existing_manifest(&root.join("manifest.sqlite"))?;
    let journal = prepare_manifest(&manifest, shards, profile)?;
    let transaction = manifest
        .transaction()
        .map_err(crate::sqlite_error::storage)?;
    let id = manifest::security_binding(&transaction, shards)?.ok_or_else(|| {
        EngineError::new(
            EngineErrorKind::FailedPrecondition,
            "authenticated startup requires an activated security root",
        )
    })?;
    let authority = DurableSecurityCatalog::from_store(SecurityCatalogStore::open_with_journal(
        path,
        SecurityStoreId::from_bytes(id)?,
        security_journal(journal),
    )?)?;
    transaction.commit().map_err(crate::sqlite_error::storage)?;
    Ok(authority)
}

pub(crate) fn open_controlled(
    root: &Path,
    shards: u16,
    control: Option<std::sync::Arc<crate::core::OperationControl>>,
    profile: StorageProfile,
) -> EngineResult<DurableSecurityCatalog> {
    let open = || match profile {
        StorageProfile::Local => open(root, shards),
        StorageProfile::Nfs => open_with_profile(root, shards, profile),
    };
    super::contention::with_control(control.clone(), || match control {
        Some(control) => super::security_catalog::with_operation_control(control, open),
        None => open(),
    })
}

#[cfg(all(test, unix))]
mod tests;
