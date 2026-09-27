use super::*;
use crate::core::authorization::{DataDomain, MAX_POLICY_PRIVILEGES, Privilege, Scope};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};

pub(super) const PASSWORD: &str = "private password";

fn copy_catalog(catalog: &SecurityCatalog) -> SecurityCatalog {
    SecurityCatalog::from_record(catalog.to_record().unwrap().as_bytes()).unwrap()
}

#[test]
fn valid_successors_preserve_history_across_role_changes_rotation_and_recreation() {
    let (catalog, user, role) = setup();
    let mut next = copy_catalog(&catalog);
    catalog.validate_successor(&next).unwrap();
    next.replace_role(&role, policy(Action::InsertData))
        .unwrap();
    next.set_user_roles(&user, []).unwrap();
    catalog.validate_successor(&next).unwrap();
    next.rotate_credentials(&user, credential()).unwrap();
    catalog.validate_successor(&next).unwrap();
    next.create_user(name("other", "bob"), credential(), [])
        .unwrap();
    catalog.validate_successor(&next).unwrap();
    next.drop_user(&user).unwrap();
    catalog.validate_successor(&next).unwrap();
    next.create_user(user, credential(), [role]).unwrap();
    catalog.validate_successor(&next).unwrap();
}

#[test]
fn successor_rejects_reset_counters_reused_ids_and_identity_renames() {
    let (mut catalog, user, _) = setup();
    let prior = copy_catalog(&catalog);
    assert!(catalog.validate_successor(&SecurityCatalog::new()).is_err());
    let mut renamed = copy_catalog(&catalog);
    let entry = renamed.users.remove(&user).unwrap();
    renamed.users.insert(name("other", "alice"), entry);
    assert!(catalog.validate_successor(&renamed).is_err());
    catalog.drop_user(&user).unwrap();
    assert!(catalog.validate_successor(&prior).is_err());
    let mut valid = copy_catalog(&catalog);
    valid.create_user(user.clone(), credential(), []).unwrap();
    catalog.validate_successor(&valid).unwrap();
    valid.users.get_mut(&user).unwrap().id = 1;
    assert!(catalog.validate_successor(&valid).is_err());
}

#[test]
fn successor_rejects_credential_rewind_or_changed_verifier_without_a_new_generation() {
    let (mut catalog, user, _) = setup();
    let old = copy_catalog(&catalog);
    let mut changed = copy_catalog(&catalog);
    changed.users.get_mut(&user).unwrap().verifier = credential();
    assert!(catalog.validate_successor(&changed).is_err());
    changed.users.get_mut(&user).unwrap().credential_generation += 1;
    catalog.validate_successor(&changed).unwrap();
    catalog.rotate_credentials(&user, credential()).unwrap();
    assert!(catalog.validate_successor(&old).is_err());
    let mut same_record = copy_catalog(&catalog);
    let identical = same_record.users[&user].verifier.clone();
    same_record.rotate_credentials(&user, identical).unwrap();
    catalog.validate_successor(&same_record).unwrap();
}

pub(super) fn name(realm: &str, name: &str) -> SecurityName {
    SecurityName::new(realm, name).unwrap()
}
pub(super) fn credential() -> ScramSha256Verifier {
    ScramSha256Verifier::from_password_with_iterations(PASSWORD, 4096).unwrap()
}
pub(super) fn target() -> Resource {
    Resource::object(DataDomain::Document, "app", "items").unwrap()
}
pub(super) fn policy(action: Action) -> Policy {
    Policy::new([Privilege::new(action, Scope::exact(target())).unwrap()]).unwrap()
}
pub(super) fn setup() -> (SecurityCatalog, SecurityName, SecurityName) {
    let mut catalog = SecurityCatalog::new();
    let user = name("app", "alice");
    let role = name("app", "reader");
    catalog
        .create_role(role.clone(), policy(Action::ReadData))
        .unwrap();
    catalog
        .create_user(user.clone(), credential(), [role.clone()])
        .unwrap();
    (catalog, user, role)
}

// Independent client both generates the proof and validates the server signature.
pub(super) fn exchange(attempt: &ScramAttempt, password: &str) -> (ScramSha256, String, Vec<u8>) {
    let mut client = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
    let first = std::str::from_utf8(client.message())
        .unwrap()
        .strip_prefix("n,,")
        .unwrap()
        .to_owned();
    let nonce = first.rsplit_once(",r=").unwrap().1;
    let challenge = format!(
        "r={nonce}catalog-test-server,s={},i={}",
        STANDARD.encode(attempt.salt()),
        attempt.iterations()
    );
    client.update(challenge.as_bytes()).unwrap();
    let (without_proof, proof) = std::str::from_utf8(client.message())
        .unwrap()
        .rsplit_once(",p=")
        .unwrap();
    let transcript = format!("{first},{challenge},{without_proof}");
    let proof = STANDARD.decode(proof).unwrap();
    (client, transcript, proof)
}

pub(super) fn login(catalog: &SecurityCatalog, user: &SecurityName) -> Principal {
    let attempt = catalog.begin_scram(user).unwrap();
    let (mut client, transcript, proof) = exchange(&attempt, PASSWORD);
    let (principal, signature) = catalog
        .complete_scram(attempt, transcript.as_bytes(), &proof)
        .unwrap()
        .into_parts();
    client
        .finish(format!("v={}", STANDARD.encode(signature)).as_bytes())
        .unwrap();
    principal
}

fn denied_read(catalog: &SecurityCatalog, principal: &Principal) {
    assert_eq!(
        catalog
            .authorize(principal, Action::ReadData, &target())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[test]
fn authenticated_principals_have_only_explicit_current_permissions() {
    let (mut catalog, user, role) = setup();
    let principal = login(&catalog, &user);
    assert_eq!(principal.name(), &user);
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
    assert!(
        catalog
            .authorize(&principal, Action::InsertData, &target())
            .is_err()
    );
    assert!(
        catalog
            .authorize(
                &principal,
                Action::ReadData,
                &Resource::object(DataDomain::Relational, "app", "items").unwrap()
            )
            .is_err()
    );
    assert!(
        catalog
            .authorize(
                &principal,
                Action::ReadData,
                &Resource::object(DataDomain::Document, "other", "items").unwrap()
            )
            .is_err()
    );
    catalog
        .replace_role(&role, policy(Action::InsertData))
        .unwrap();
    denied_read(&catalog, &principal);
    catalog
        .authorize(&principal, Action::InsertData, &target())
        .unwrap();
    catalog.set_user_roles(&user, []).unwrap();
    assert!(
        catalog
            .authorize(&principal, Action::InsertData, &target())
            .is_err()
    );
    catalog.set_user_roles(&user, [role]).unwrap();
    catalog
        .authorize(&principal, Action::InsertData, &target())
        .unwrap();
}

#[test]
fn empty_catalog_and_roleless_users_have_no_implicit_administrator() {
    let mut catalog = SecurityCatalog::default();
    let user = name("admin", "root");
    assert!(catalog.begin_scram(&user).is_err());
    catalog.create_user(user.clone(), credential(), []).unwrap();
    let principal = login(&catalog, &user);
    for &action in Action::ALL {
        for resource in [
            target(),
            Resource::server(),
            Resource::security_realm("admin").unwrap(),
        ] {
            assert!(catalog.authorize(&principal, action, &resource).is_err());
        }
    }
}

#[test]
fn wrong_password_unknown_user_and_malformed_proofs_use_one_fixed_failure() {
    let (catalog, user, _) = setup();
    let unknown = catalog.begin_scram(&name("app", "missing")).unwrap_err();
    assert_eq!(unknown.to_string(), "authentication failed");
    for password in ["wrong password", PASSWORD] {
        let attempt = catalog.begin_scram(&user).unwrap();
        let (_, transcript, mut proof) = exchange(&attempt, password);
        if password == PASSWORD {
            proof.pop();
        }
        let error = catalog
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .unwrap_err();
        assert_eq!(error.to_string(), unknown.to_string());
        assert_eq!(error.kind(), EngineErrorKind::PermissionDenied);
    }
    let attempt = catalog.begin_scram(&user).unwrap();
    assert_eq!(
        catalog
            .complete_scram(attempt, &[], &[])
            .unwrap_err()
            .to_string(),
        unknown.to_string()
    );
    catalog
        .authorize(&login(&catalog, &user), Action::ReadData, &target())
        .unwrap();
}

#[test]
fn rotation_invalidates_inflight_proofs_and_retained_principals_even_for_same_record() {
    let (mut catalog, user, _) = setup();
    let principal = login(&catalog, &user);
    let retained = principal.clone();
    let attempt = catalog.begin_scram(&user).unwrap();
    let (_, transcript, proof) = exchange(&attempt, PASSWORD);
    let identical = catalog.users.get(&user).unwrap().verifier.clone();
    catalog.rotate_credentials(&user, identical).unwrap();
    denied_read(&catalog, &principal);
    denied_read(&catalog, &retained);
    assert_eq!(
        catalog
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .unwrap_err()
            .to_string(),
        "authentication failed"
    );
    catalog
        .authorize(&login(&catalog, &user), Action::ReadData, &target())
        .unwrap();
}

#[test]
fn new_password_is_required_after_rotation() {
    let (mut catalog, user, _) = setup();
    let principal = login(&catalog, &user);
    catalog
        .rotate_credentials(
            &user,
            ScramSha256Verifier::from_password_with_iterations("replacement", 4096).unwrap(),
        )
        .unwrap();
    denied_read(&catalog, &principal);
    for password in [PASSWORD, "replacement"] {
        let attempt = catalog.begin_scram(&user).unwrap();
        let (mut client, transcript, proof) = exchange(&attempt, password);
        let result = catalog.complete_scram(attempt, transcript.as_bytes(), &proof);
        if password == PASSWORD {
            assert!(result.is_err());
        } else {
            let (principal, signature) = result.unwrap().into_parts();
            client
                .finish(format!("v={}", STANDARD.encode(signature)).as_bytes())
                .unwrap();
            catalog
                .authorize(&principal, Action::ReadData, &target())
                .unwrap();
        }
    }
}

#[test]
fn drop_and_recreate_user_never_resurrects_a_principal_or_pending_proof() {
    let (mut catalog, user, role) = setup();
    let principal = login(&catalog, &user);
    let attempt = catalog.begin_scram(&user).unwrap();
    let (_, transcript, proof) = exchange(&attempt, PASSWORD);
    let identical = catalog.users.get(&user).unwrap().verifier.clone();
    catalog.drop_user(&user).unwrap();
    denied_read(&catalog, &principal);
    catalog
        .create_user(user.clone(), identical, [role])
        .unwrap();
    denied_read(&catalog, &principal);
    assert!(
        catalog
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .is_err()
    );
    catalog
        .authorize(&login(&catalog, &user), Action::ReadData, &target())
        .unwrap();
}

#[test]
fn drop_and_recreate_role_does_not_resurrect_memberships() {
    let (mut catalog, user, role) = setup();
    let principal = login(&catalog, &user);
    catalog.drop_role(&role).unwrap();
    denied_read(&catalog, &principal);
    catalog
        .create_role(role.clone(), policy(Action::ReadData))
        .unwrap();
    denied_read(&catalog, &principal);
    denied_read(&catalog, &login(&catalog, &user));
    catalog.set_user_roles(&user, [role]).unwrap();
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
}

#[test]
fn catalog_incarnations_cannot_accept_each_others_principals_or_attempts() {
    let (first, user, role) = setup();
    let principal = login(&first, &user);
    let attempt = first.begin_scram(&user).unwrap();
    let (_, transcript, proof) = exchange(&attempt, PASSWORD);
    let mut second = SecurityCatalog::new();
    second
        .create_role(role.clone(), policy(Action::ReadData))
        .unwrap();
    second
        .create_user(user.clone(), first.users[&user].verifier.clone(), [role])
        .unwrap();
    assert_eq!(first.users[&user].id, second.users[&user].id);
    denied_read(&second, &principal);
    assert!(
        second
            .complete_scram(attempt, transcript.as_bytes(), &proof)
            .is_err()
    );
    let other = login(&second, &user);
    denied_read(&first, &other);
    drop(first);
    denied_read(&second, &principal);
}

#[test]
fn realm_case_and_name_components_are_exact_and_separate() {
    let (mut catalog, user, role) = setup();
    let verifier = credential();
    for other in [
        name("App", "alice"),
        name("app", "Alice"),
        name("other", "alice"),
        name("app", "reader"),
    ] {
        assert!(catalog.begin_scram(&other).is_err());
        catalog
            .create_user(other.clone(), verifier.clone(), [])
            .unwrap();
        denied_read(&catalog, &login(&catalog, &other));
    }
    let principal = login(&catalog, &user);
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
    catalog
        .create_role(name("other", "reader"), Policy::default())
        .unwrap();
    catalog
        .set_user_roles(&user, [name("other", "reader")])
        .unwrap();
    denied_read(&catalog, &principal);
    catalog.set_user_roles(&user, [role]).unwrap();
    assert_ne!(name("a.b", "c"), name("a", "b.c"));
    assert_ne!(name("é", "x"), name("e\u{301}", "x"));
    assert_ne!(name("app", "*"), name("app", "alice"));
}

#[test]
fn names_are_bounded_before_allocation_and_diagnostics_are_redacted() {
    for invalid in [
        String::new(),
        "x".repeat(129),
        "private\0name".into(),
        "🦀".repeat(33),
    ] {
        let error = SecurityName::new("app", &invalid).unwrap_err();
        assert_eq!(error.to_string(), "security name is invalid");
    }
    for invalid in [String::new(), "x".repeat(64), "private\0realm".into()] {
        assert!(SecurityName::new(&invalid, "alice").is_err());
    }
    let valid = SecurityName::new(&"r".repeat(63), &"🦀".repeat(32)).unwrap();
    assert_eq!(valid.realm().len(), 63);
    assert_eq!(valid.name().len(), 128);
}

#[test]
fn failed_provisioning_is_atomic_and_does_not_replace_credentials_or_roles() {
    let (mut catalog, user, role) = setup();
    let principal = login(&catalog, &user);
    let next = catalog.next_user_id;
    let before = catalog.users[&user].verifier.to_record();
    assert_eq!(
        catalog
            .create_user(user.clone(), credential(), [])
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    assert_eq!(
        catalog
            .create_role(role.clone(), Policy::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::UniqueViolation
    );
    assert!(
        catalog
            .replace_role(&name("app", "missing"), Policy::default())
            .is_err()
    );
    assert!(catalog.drop_role(&name("app", "missing")).is_err());
    assert!(catalog.drop_user(&name("app", "missing")).is_err());
    assert!(
        catalog
            .rotate_credentials(&name("app", "missing"), credential())
            .is_err()
    );
    assert!(
        catalog
            .set_user_roles(&user, [role.clone(), name("app", "missing")])
            .is_err()
    );
    assert!(
        catalog
            .create_user(name("app", "new"), credential(), [name("app", "missing")])
            .is_err()
    );
    assert_eq!(catalog.next_user_id, next);
    assert_eq!(catalog.user_count(), 1);
    assert_eq!(catalog.role_count(), 1);
    assert_eq!(
        catalog.users[&user].verifier.to_record().as_bytes(),
        before.as_bytes()
    );
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
}

#[test]
fn duplicate_and_infinite_membership_inputs_are_bounded_before_deduplication() {
    let (mut catalog, user, role) = setup();
    catalog
        .set_user_roles(&user, std::iter::repeat_n(role.clone(), MAX_POLICY_ROLES))
        .unwrap();
    assert_eq!(catalog.users[&user].roles.len(), 1);
    let mut visited = 0;
    assert_eq!(
        catalog
            .set_user_roles(&user, std::iter::repeat(role).inspect(|_| visited += 1))
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(visited, MAX_POLICY_ROLES + 1);
    catalog
        .authorize(&login(&catalog, &user), Action::ReadData, &target())
        .unwrap();
}

#[test]
fn role_replacements_and_memberships_reject_oversized_unions_without_partial_changes() {
    let (mut catalog, user, role) = setup();
    let full = Policy::new((0..MAX_POLICY_PRIVILEGES).map(|n| {
        Privilege::new(
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Document, "app", &format!("c{n}")).unwrap()),
        )
        .unwrap()
    }))
    .unwrap();
    let full_role = name("app", "full");
    catalog
        .create_role(full_role.clone(), full.clone())
        .unwrap();
    let principal = login(&catalog, &user);
    assert!(
        catalog
            .set_user_roles(&user, [role.clone(), full_role.clone()])
            .is_err()
    );
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
    assert!(
        catalog
            .create_user(
                name("app", "new"),
                credential(),
                [role.clone(), full_role.clone()]
            )
            .is_err()
    );
    catalog.replace_role(&role, Policy::default()).unwrap();
    catalog
        .set_user_roles(&user, [role.clone(), full_role.clone()])
        .unwrap();
    assert!(
        catalog
            .replace_role(&role, policy(Action::ReadData))
            .is_err()
    );
    assert_eq!(catalog.roles[&role], Policy::default());
    assert_eq!(catalog.roles[&full_role], full);
    denied_read(&catalog, &principal);
    catalog
        .authorize(
            &principal,
            Action::ReadData,
            &Resource::object(DataDomain::Document, "app", "c0").unwrap(),
        )
        .unwrap();
}

#[test]
fn catalog_sizes_and_generation_counters_fail_closed_without_reuse() {
    let mut catalog = SecurityCatalog::new();
    let verifier = credential();
    for n in 0..MAX_SECURITY_ROLES {
        catalog
            .create_role(name("app", &format!("role{n}")), Policy::default())
            .unwrap();
    }
    assert_eq!(
        catalog
            .create_role(name("app", "overflow"), Policy::default())
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    for n in 0..MAX_SECURITY_USERS {
        catalog
            .create_user(name("app", &format!("user{n}")), verifier.clone(), [])
            .unwrap();
    }
    assert_eq!(
        catalog
            .create_user(name("app", "overflow"), verifier.clone(), [])
            .unwrap_err()
            .kind(),
        EngineErrorKind::LimitExceeded
    );
    assert_eq!(catalog.user_count(), MAX_SECURITY_USERS);
    assert_eq!(catalog.role_count(), MAX_SECURITY_ROLES);

    let (mut catalog, user, role) = setup();
    catalog.next_user_id = u64::MAX;
    assert!(
        catalog
            .create_user(name("app", "overflow"), verifier.clone(), [role])
            .is_err()
    );
    assert_eq!(catalog.user_count(), 1);
    catalog.users.get_mut(&user).unwrap().credential_generation = u64::MAX;
    let principal = login(&catalog, &user);
    let before = catalog.users[&user].verifier.to_record();
    assert!(catalog.rotate_credentials(&user, verifier).is_err());
    assert_eq!(
        catalog.users[&user].verifier.to_record().as_bytes(),
        before.as_bytes()
    );
    catalog
        .authorize(&principal, Action::ReadData, &target())
        .unwrap();
}

#[test]
fn every_resource_is_required_and_revocation_wins_at_next_thread_admission() {
    let (catalog, user, _) = setup();
    let principal = login(&catalog, &user);
    assert!(catalog.authorize_all(&principal, []).is_err());
    assert!(
        catalog
            .authorize_all(
                &principal,
                [
                    (Action::ReadData, &target()),
                    (Action::DeleteData, &target())
                ]
            )
            .is_err()
    );
    catalog
        .authorize_all(&principal, [(Action::ReadData, &target())])
        .unwrap();
    let catalog = Arc::new(std::sync::RwLock::new(catalog));
    let reader = Arc::clone(&catalog);
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        recv.recv().unwrap();
        denied_read(&reader.read().unwrap(), &principal);
    });
    catalog.write().unwrap().set_user_roles(&user, []).unwrap();
    send.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn role_changes_during_authentication_are_not_cached_at_exchange_start() {
    let (mut catalog, user, _) = setup();
    let attempt = catalog.begin_scram(&user).unwrap();
    let (_, transcript, proof) = exchange(&attempt, PASSWORD);
    catalog.set_user_roles(&user, []).unwrap();
    let (principal, _) = catalog
        .complete_scram(attempt, transcript.as_bytes(), &proof)
        .unwrap()
        .into_parts();
    denied_read(&catalog, &principal);
}

#[test]
fn debug_output_is_redacted_and_owned_handles_are_thread_safe() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<SecurityCatalog>();
    send_sync::<Principal>();
    send_sync::<ScramAttempt>();
    send_sync::<ScramAuthentication>();
    let (catalog, user, role) = setup();
    let attempt = catalog.begin_scram(&user).unwrap();
    let (_, transcript, proof) = exchange(&attempt, PASSWORD);
    let attempt_debug = format!("{attempt:?}");
    let result = catalog
        .complete_scram(attempt, transcript.as_bytes(), &proof)
        .unwrap();
    let result_debug = format!("{result:?}");
    let (principal, _) = result.into_parts();
    for debug in [
        format!("{catalog:?}"),
        format!("{user:?}"),
        format!("{role:?}"),
        attempt_debug,
        result_debug,
        format!("{principal:?}"),
    ] {
        for secret in [
            "alice",
            "reader",
            PASSWORD,
            "app",
            "credential_generation",
            "user_id",
        ] {
            assert!(!debug.contains(secret));
        }
    }
}
