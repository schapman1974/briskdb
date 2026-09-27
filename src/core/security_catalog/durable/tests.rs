use super::*;
use crate::core::{
    authentication::ScramSha256Verifier,
    authorization::{Policy, Privilege, Scope},
    security_catalog::tests as fixtures,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

struct Fixture {
    _temp: tempfile::TempDir,
    path: PathBuf,
    id: SecurityStoreId,
    user: SecurityName,
    role: SecurityName,
    authority: DurableSecurityCatalog,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = temp.path().join("security.sqlite");
        let id = SecurityStoreId::generate().unwrap();
        let (catalog, user, role) = fixtures::setup();
        let store = SecurityCatalogStore::create(&path, id, &catalog).unwrap();
        Self {
            _temp: temp,
            path,
            id,
            user,
            role,
            authority: DurableSecurityCatalog::from_store(store).unwrap(),
        }
    }

    fn peer(&self) -> SecurityCatalogStore {
        SecurityCatalogStore::open(&self.path, self.id).unwrap()
    }

    fn login(&mut self) -> Principal {
        login(&mut self.authority, &self.user, fixtures::PASSWORD)
    }
}

fn login(authority: &mut DurableSecurityCatalog, user: &SecurityName, password: &str) -> Principal {
    let attempt = authority.begin_scram(user).unwrap();
    let (mut client, transcript, proof) = fixtures::exchange(&attempt, password);
    let (principal, signature) = authority
        .complete_scram(attempt, transcript.as_bytes(), &proof)
        .unwrap()
        .into_parts();
    client
        .finish(format!("v={}", STANDARD.encode(signature)).as_bytes())
        .unwrap();
    principal
}

fn allowed(authority: &mut DurableSecurityCatalog, principal: &Principal, action: Action) {
    authority
        .authorize(principal, action, &fixtures::target())
        .unwrap();
}

fn denied(authority: &mut DurableSecurityCatalog, principal: &Principal, action: Action) {
    assert_eq!(
        authority
            .authorize(principal, action, &fixtures::target())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[test]
fn role_publication_follows_commit_and_retained_principals_use_current_permissions() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut peer = f.peer();
    let user = f.user.clone();
    let role = f.role.clone();
    let result = f
        .authority
        .update(|candidate| {
            candidate.replace_role(&role, fixtures::policy(Action::InsertData))?;
            let (revision, committed) = peer.load()?.into_parts();
            assert_eq!(revision, 1);
            let prior = fixtures::login(&committed, &user);
            committed.authorize(&prior, Action::ReadData, &fixtures::target())?;
            assert!(
                committed
                    .authorize(&prior, Action::InsertData, &fixtures::target())
                    .is_err()
            );
            Ok(42)
        })
        .unwrap();
    assert_eq!(result, 42);
    assert_eq!(f.authority.revision(), 2);
    assert_eq!(f.authority.store_id(), f.id);
    denied(&mut f.authority, &principal, Action::ReadData);
    allowed(&mut f.authority, &principal, Action::InsertData);
    assert_eq!(peer.load().unwrap().revision(), 2);
}

#[test]
fn rejected_edits_and_callback_panics_never_publish_partial_changes() {
    let mut f = Fixture::new();
    let principal = f.login();
    let before = fs::read(&f.path).unwrap();
    let user = f.user.clone();
    assert!(
        f.authority
            .update::<()>(|candidate| {
                candidate.drop_user(&user)?;
                Err(EngineError::new(
                    EngineErrorKind::Cancelled,
                    "cancelled edit",
                ))
            })
            .is_err()
    );
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        f.authority.update::<()>(|candidate| {
            candidate.drop_user(&user)?;
            panic!("aborted detached edit");
        })
    }));
    assert!(panic.is_err());
    assert!(
        f.authority
            .update(|candidate| {
                *candidate = SecurityCatalog::new();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(fs::read(&f.path).unwrap(), before);
    assert_eq!(f.authority.revision(), 1);
    assert!(!f.authority.is_fenced());
    allowed(&mut f.authority, &principal, Action::ReadData);
}

#[test]
fn rotation_revokes_retained_principals_and_already_started_proofs() {
    let mut f = Fixture::new();
    let principal = f.login();
    let attempt = f.authority.begin_scram(&f.user).unwrap();
    let (_, transcript, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    let user = f.user.clone();
    f.authority
        .update(|candidate| {
            candidate.rotate_credentials(
                &user,
                ScramSha256Verifier::from_password_with_iterations("new password", 4096)?,
            )
        })
        .unwrap();
    denied(&mut f.authority, &principal, Action::ReadData);
    assert_eq!(
        f.authority
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    let fresh = login(&mut f.authority, &f.user, "new password");
    allowed(&mut f.authority, &fresh, Action::ReadData);
}

#[test]
fn peer_role_changes_refresh_without_invalidating_unchanged_user_identity() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut peer = f.peer();
    let (revision, mut candidate) = peer.load().unwrap().into_parts();
    candidate
        .replace_role(&f.role, fixtures::policy(Action::InsertData))
        .unwrap();
    peer.replace(revision, &candidate).unwrap();
    allowed(&mut f.authority, &principal, Action::InsertData);
    denied(&mut f.authority, &principal, Action::ReadData);
    assert_eq!(f.authority.revision(), 2);
    let attempt = f.authority.begin_scram(&f.user).unwrap();
    let (_, transcript, proof) = fixtures::exchange(&attempt, fixtures::PASSWORD);
    candidate
        .rotate_credentials(&f.user, fixtures::credential())
        .unwrap();
    peer.replace(2, &candidate).unwrap();
    assert!(
        f.authority
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .is_err()
    );
    denied(&mut f.authority, &principal, Action::InsertData);
}

#[test]
fn peer_delete_and_recreation_cannot_resurrect_an_old_principal() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut peer = f.peer();
    let (_, mut candidate) = peer.load().unwrap().into_parts();
    candidate.drop_user(&f.user).unwrap();
    peer.replace(1, &candidate).unwrap();
    denied(&mut f.authority, &principal, Action::ReadData);
    candidate
        .create_user(f.user.clone(), fixtures::credential(), [f.role.clone()])
        .unwrap();
    peer.replace(2, &candidate).unwrap();
    denied(&mut f.authority, &principal, Action::ReadData);
    let fresh = f.login();
    allowed(&mut f.authority, &fresh, Action::ReadData);
}

#[test]
fn conflict_never_replays_edits_or_publishes_the_losing_candidate() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut peer = f.peer();
    let role = f.role.clone();
    let mut calls = 0;
    let error = f
        .authority
        .update(|candidate| {
            calls += 1;
            candidate.replace_role(&role, fixtures::policy(Action::DeleteData))?;
            let (revision, mut winner) = peer.load()?.into_parts();
            winner.replace_role(&role, fixtures::policy(Action::InsertData))?;
            peer.replace(revision, &winner)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::FailedPrecondition);
    assert_eq!(calls, 1);
    assert!(!f.authority.is_fenced());
    allowed(&mut f.authority, &principal, Action::InsertData);
    denied(&mut f.authority, &principal, Action::DeleteData);
    assert_eq!(f.authority.revision(), 2);
}

#[test]
fn uncertain_commit_fences_admission_until_a_fresh_incarnation_is_opened() {
    let mut f = Fixture::new();
    let principal = f.login();
    let reader = rusqlite::Connection::open(&f.path).unwrap();
    reader
        .execute_batch("BEGIN; SELECT revision FROM briskdb_security_state;")
        .unwrap();
    let user = f.user.clone();
    assert!(
        f.authority
            .update(|candidate| candidate.drop_user(&user))
            .is_err()
    );
    assert!(f.authority.is_fenced());
    assert_eq!(f.authority.revision(), 1);
    reader.execute_batch("COMMIT").unwrap();
    assert_eq!(
        f.authority
            .authorize(&principal, Action::ReadData, &fixtures::target())
            .unwrap_err()
            .kind(),
        EngineErrorKind::FailedPrecondition
    );
    let mut reopened = DurableSecurityCatalog::from_store(f.peer()).unwrap();
    assert_eq!(reopened.revision(), 1);
    denied(&mut reopened, &principal, Action::ReadData);
    let fresh = login(&mut reopened, &f.user, fixtures::PASSWORD);
    allowed(&mut reopened, &fresh, Action::ReadData);
}

#[test]
fn authorized_edits_bind_permission_and_compare_and_swap_to_one_revision() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut peer = f.peer();
    let user = f.user.clone();
    let role = f.role.clone();
    let requirements = [(Action::ReadData, fixtures::target())];
    let mut calls = 0;
    let result = f
        .authority
        .update_authorized(&principal, &requirements, |candidate| {
            calls += 1;
            candidate.drop_user(&user)?;
            let (revision, mut revoked) = peer.load()?.into_parts();
            revoked.replace_role(&role, Policy::default())?;
            peer.replace(revision, &revoked)?;
            Ok(())
        });
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::FailedPrecondition
    );
    assert_eq!(calls, 1);
    assert!(!f.authority.is_fenced());
    let result = f
        .authority
        .update_authorized(&principal, &requirements, |_| {
            calls += 1;
            Ok(())
        });
    assert_eq!(
        result.unwrap_err().kind(),
        EngineErrorKind::PermissionDenied
    );
    assert_eq!(
        calls, 1,
        "revoked authority must not execute or replay an edit"
    );
    assert_eq!(peer.load().unwrap().into_parts().1.user_count(), 1);
}

#[test]
fn denied_authorized_edit_never_evaluates_callback_or_changes_bytes() {
    let mut f = Fixture::new();
    let principal = f.login();
    let before = fs::read(&f.path).unwrap();
    for requirements in [vec![], vec![(Action::DeleteData, fixtures::target())]] {
        let result =
            f.authority
                .update_authorized(&principal, &requirements, |_| -> EngineResult<()> {
                    panic!("unauthorized callback executed")
                });
        assert_eq!(
            result.unwrap_err().kind(),
            EngineErrorKind::PermissionDenied
        );
        assert_eq!(f.authority.revision(), 1);
        assert_eq!(fs::read(&f.path).unwrap(), before);
    }
    let requirements = [(Action::ReadData, fixtures::target())];
    f.authority
        .update_authorized(&principal, &requirements, |_| Ok(()))
        .unwrap();
    assert_eq!(f.authority.revision(), 2);
}

#[test]
fn changed_contents_at_the_same_revision_fence_even_with_a_valid_record_checksum() {
    let mut f = Fixture::new();
    let principal = f.login();
    let (_, mut changed) = f.peer().load().unwrap().into_parts();
    let original = changed.to_record().unwrap();
    changed
        .replace_role(&f.role, fixtures::policy(Action::InsertData))
        .unwrap();
    let record = changed.to_record().unwrap();
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection
        .execute(
            "UPDATE briskdb_security_state SET record = ?1",
            [record.as_bytes()],
        )
        .unwrap();
    assert_eq!(
        f.authority
            .authorize(&principal, Action::InsertData, &fixtures::target())
            .unwrap_err()
            .kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(f.authority.is_fenced());
    connection
        .execute(
            "UPDATE briskdb_security_state SET record = ?1",
            [original.as_bytes()],
        )
        .unwrap();
    assert!(f.authority.begin_scram(&f.user).is_err());
}

#[test]
fn advanced_revision_cannot_rewind_live_identity_history() {
    let mut f = Fixture::new();
    let record = SecurityCatalog::new().to_record().unwrap();
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    connection
        .execute(
            "UPDATE briskdb_security_state SET revision = 2, record = ?1",
            [record.as_bytes()],
        )
        .unwrap();
    assert_eq!(
        f.authority.begin_scram(&f.user).unwrap_err().kind(),
        EngineErrorKind::DataCorruption
    );
    assert!(f.authority.is_fenced());
}

#[test]
fn corruption_encountered_during_publication_is_not_a_retryable_revision_conflict() {
    let mut f = Fixture::new();
    let connection = rusqlite::Connection::open(&f.path).unwrap();
    let role = f.role.clone();
    let error = f
        .authority
        .update(|candidate| {
            candidate.replace_role(&role, fixtures::policy(Action::InsertData))?;
            connection
                .execute(
                    "UPDATE briskdb_security_state SET record = zeroblob(length(record))",
                    [],
                )
                .unwrap();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::DataCorruption);
    assert!(f.authority.is_fenced());
}

#[test]
fn retained_descriptor_replacement_cannot_fall_back_to_cached_authority() {
    let mut f = Fixture::new();
    let principal = f.login();
    let original = f.path.with_extension("original");
    fs::rename(&f.path, &original).unwrap();
    fs::copy(&original, &f.path).unwrap();
    assert!(
        f.authority
            .authorize(&principal, Action::ReadData, &fixtures::target())
            .is_err()
    );
    assert!(f.authority.is_fenced());
    fs::rename(&f.path, f.path.with_extension("replacement")).unwrap();
    fs::rename(&original, &f.path).unwrap();
    assert!(f.authority.begin_scram(&f.user).is_err());
}

#[test]
fn detached_and_other_authority_principals_cannot_cross_incarnations() {
    let mut f = Fixture::new();
    let principal = f.login();
    let mut other = DurableSecurityCatalog::from_store(f.peer()).unwrap();
    denied(&mut other, &principal, Action::ReadData);
    let user = f.user.clone();
    let detached = f
        .authority
        .update(|candidate| Ok(fixtures::login(candidate, &user)))
        .unwrap();
    denied(&mut f.authority, &detached, Action::ReadData);
    allowed(&mut f.authority, &principal, Action::ReadData);
    let other_principal = login(&mut other, &user, fixtures::PASSWORD);
    denied(&mut f.authority, &other_principal, Action::ReadData);
}

#[test]
fn combined_admission_refreshes_all_requirements_and_never_uses_an_empty_grant() {
    let mut f = Fixture::new();
    let principal = f.login();
    let target = fixtures::target();
    assert!(f.authority.authorize_all(&principal, []).is_err());
    assert!(
        f.authority
            .authorize_all(
                &principal,
                [(Action::ReadData, &target), (Action::InsertData, &target)]
            )
            .is_err()
    );
    let role = f.role.clone();
    f.authority
        .update(|candidate| {
            candidate.replace_role(
                &role,
                Policy::new([
                    Privilege::new(Action::ReadData, Scope::exact(target.clone()))?,
                    Privilege::new(Action::InsertData, Scope::exact(target.clone()))?,
                ])?,
            )
        })
        .unwrap();
    f.authority
        .authorize_all(
            &principal,
            [(Action::ReadData, &target), (Action::InsertData, &target)],
        )
        .unwrap();
}

#[test]
fn serialized_cross_thread_revocation_is_visible_to_the_next_admission() {
    let mut f = Fixture::new();
    let principal = f.login();
    let shared = Arc::new(std::sync::Mutex::new(f.authority));
    let writer = Arc::clone(&shared);
    let user = f.user;
    std::thread::spawn(move || {
        writer
            .lock()
            .unwrap()
            .update(|catalog| catalog.set_user_roles(&user, []))
            .unwrap()
    })
    .join()
    .unwrap();
    denied(&mut shared.lock().unwrap(), &principal, Action::ReadData);
}

#[test]
fn debug_exposes_only_revision_and_fence_state() {
    let f = Fixture::new();
    let text = format!("{:?}", f.authority);
    assert_eq!(
        text,
        "DurableSecurityCatalog { revision: 1, fenced: false, .. }"
    );
    for secret in [
        "alice",
        "reader",
        fixtures::PASSWORD,
        f.path.to_str().unwrap(),
    ] {
        assert!(!text.contains(secret));
    }
}
