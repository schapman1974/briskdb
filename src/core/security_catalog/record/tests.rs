use super::*;
use crate::core::security_catalog::tests::{credential, login, name, policy, setup, target};

fn seal(payload: &[u8]) -> Vec<u8> {
    let mut bytes = payload.to_vec();
    bytes.extend_from_slice(checksum(payload).as_bytes());
    bytes
}

fn modify(record: &SecurityCatalogRecord, mutate: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut payload = record.as_bytes()[..record.as_bytes().len() - CHECKSUM_BYTES].to_vec();
    mutate(&mut payload);
    seal(&payload)
}

fn rejects(bytes: &[u8]) {
    let error = SecurityCatalog::from_record(bytes).unwrap_err();
    assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
    assert_eq!(error.to_string(), "security catalog record is invalid");
}

#[test]
fn empty_record_has_fixed_version_layout_and_is_deterministic() {
    let catalog = SecurityCatalog::new();
    let record = catalog.to_record().unwrap();
    assert_eq!(record.as_bytes().len(), MIN_RECORD_BYTES);
    assert_eq!(&record.as_bytes()[..8], MAGIC);
    assert_eq!(&record.as_bytes()[8..16], &1_u64.to_be_bytes());
    assert_eq!(&record.as_bytes()[16..20], &[0; 4]);
    let restored = SecurityCatalog::from_record(record.as_bytes()).unwrap();
    assert_eq!(restored.user_count(), 0);
    assert_eq!(restored.role_count(), 0);
    assert_eq!(restored.to_record().unwrap().as_bytes(), record.as_bytes());
}

#[test]
fn restored_credentials_permissions_and_counters_match_but_old_principals_do_not() {
    let (mut catalog, user, role) = setup();
    catalog.rotate_credentials(&user, credential()).unwrap();
    let principal = login(&catalog, &user);
    let attempt = catalog.begin_scram(&user).unwrap();
    let record = catalog.to_record().unwrap();
    let mut restored = SecurityCatalog::from_record(record.as_bytes()).unwrap();
    assert_eq!(restored.to_record().unwrap().as_bytes(), record.as_bytes());
    assert_eq!(restored.users[&user].credential_generation, 2);
    assert_eq!(restored.next_user_id, catalog.next_user_id);
    assert!(
        restored
            .authorize(&principal, Action::ReadData, &target())
            .is_err()
    );
    // Identity validation fails before the transcript/proof can be considered.
    assert!(
        restored
            .complete_scram(attempt, b"old transcript", &[0; 32])
            .is_err()
    );
    let fresh = login(&restored, &user);
    restored
        .authorize(&fresh, Action::ReadData, &target())
        .unwrap();
    restored
        .replace_role(&role, policy(Action::InsertData))
        .unwrap();
    assert!(
        restored
            .authorize(&fresh, Action::ReadData, &target())
            .is_err()
    );
    restored
        .authorize(&fresh, Action::InsertData, &target())
        .unwrap();
    let old_id = restored.users[&user].id;
    restored.drop_user(&user).unwrap();
    restored
        .create_user(user.clone(), credential(), [role])
        .unwrap();
    assert!(restored.users[&user].id > old_id);
    assert!(
        restored
            .authorize(&fresh, Action::InsertData, &target())
            .is_err()
    );
}

#[test]
fn every_action_scope_and_resource_variant_round_trips_without_broadening() {
    let resources = vec![
        Resource::server(),
        Resource::security_realm("é").unwrap(),
        Resource::data_domain(DataDomain::Relational),
        Resource::data_domain(DataDomain::Document),
        Resource::database(DataDomain::Relational, "app").unwrap(),
        Resource::database(DataDomain::Document, "App").unwrap(),
        Resource::object(DataDomain::Relational, "app", "items").unwrap(),
        Resource::object(DataDomain::Document, "App", "é.*").unwrap(),
    ];
    let scopes = resources
        .iter()
        .cloned()
        .map(Scope::exact)
        .chain([
            Scope::database(DataDomain::Relational, "app").unwrap(),
            Scope::database(DataDomain::Document, "App").unwrap(),
            Scope::all_databases(DataDomain::Relational),
            Scope::all_databases(DataDomain::Document),
            Scope::all_security_realms(),
        ])
        .collect::<Vec<_>>();
    let mut catalog = SecurityCatalog::new();
    for (index, scope) in scopes.iter().enumerate() {
        let policy = Policy::new(
            Action::ALL
                .iter()
                .filter_map(|&action| Privilege::new(action, scope.clone()).ok()),
        )
        .unwrap();
        catalog
            .create_role(name("realm", &format!("r{index}")), policy)
            .unwrap();
    }
    let original = catalog.to_record().unwrap();
    let restored = SecurityCatalog::from_record(original.as_bytes()).unwrap();
    assert_eq!(restored.roles, catalog.roles);
    assert_eq!(
        restored.to_record().unwrap().as_bytes(),
        original.as_bytes()
    );
    for (name, policy) in &catalog.roles {
        for &action in Action::ALL {
            for resource in &resources {
                assert_eq!(
                    restored.roles[name].allows(action, resource),
                    policy.allows(action, resource)
                );
            }
        }
    }
}

#[test]
fn every_truncation_mutated_byte_and_trailing_byte_is_rejected_by_integrity_check() {
    let (catalog, _, _) = setup();
    let record = catalog.to_record().unwrap();
    let bytes = record.as_bytes();
    for end in 0..bytes.len() {
        rejects(&bytes[..end]);
    }
    for index in 0..bytes.len() {
        let mut mutated = bytes.to_vec();
        mutated[index] ^= 1;
        rejects(&mutated);
    }
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    rejects(&trailing);
    rejects(&modify(&record, |payload| payload.push(0)));
    rejects(&modify(&record, |payload| payload[7] = b'2'));
}

#[test]
fn decoder_rejects_oversized_record_counts_and_invalid_identity_generations() {
    let record = SecurityCatalog::new().to_record().unwrap();
    rejects(&modify(&record, |payload| {
        payload[16..18].copy_from_slice(&1025_u16.to_be_bytes())
    }));
    rejects(&modify(&record, |payload| {
        payload[18..20].copy_from_slice(&1025_u16.to_be_bytes())
    }));
    rejects(&modify(&record, |payload| payload[8..16].fill(0)));
    let oversized = vec![0_u8; MAX_SECURITY_CATALOG_RECORD_BYTES + 1];
    rejects(&oversized);
    drop(oversized);

    for mode in 0..5 {
        let (mut catalog, user, _) = setup();
        match mode {
            0 => catalog.users.get_mut(&user).unwrap().id = 0,
            1 => catalog.users.get_mut(&user).unwrap().id = catalog.next_user_id,
            2 => catalog.users.get_mut(&user).unwrap().credential_generation = 0,
            3 => catalog.next_user_id = 0,
            _ => {
                catalog
                    .create_user(name("app", "other"), credential(), [])
                    .unwrap();
                let id = catalog.users[&user].id;
                catalog.users.get_mut(&name("app", "other")).unwrap().id = id;
            }
        }
        rejects(catalog.to_record().unwrap().as_bytes());
    }
}

fn writer() -> Writer {
    Writer {
        bytes: Some(Zeroizing::new(Vec::with_capacity(4096))),
        len: 0,
    }
}
fn finish(writer: Writer) -> Vec<u8> {
    seal(&writer.bytes.unwrap())
}

fn raw_policy(grants: &[(&str, &[u8])]) -> Vec<u8> {
    let mut output = writer();
    output.put(MAGIC).unwrap();
    output.long(1).unwrap();
    output.short(1).unwrap();
    output.name(&name("app", "role")).unwrap();
    output.short(grants.len()).unwrap();
    for (action, scope) in grants {
        output.text(action).unwrap();
        output.put(scope).unwrap();
    }
    output.short(0).unwrap();
    finish(output)
}

#[test]
fn decoder_rejects_unknown_or_misscoped_actions_tags_and_duplicate_grants() {
    let scope = [3, 2]; // all document databases
    for invalid in [
        "*",
        "ReadData",
        "read_data ",
        "manage_everything",
        "create_user",
    ] {
        rejects(&raw_policy(&[(invalid, &scope)]));
    }
    for invalid in [
        &[0][..],
        &[5],
        &[3, 0],
        &[3, 3],
        &[1, 0],
        &[1, 6],
        &[1, 3, 9],
    ] {
        rejects(&raw_policy(&[("read_data", invalid)]));
    }
    rejects(&raw_policy(&[("read_data", &scope), ("read_data", &scope)]));
    let valid = raw_policy(&[("read_data", &scope)]);
    assert!(SecurityCatalog::from_record(&valid).is_ok());
}

#[test]
fn decoder_validates_scope_names_utf8_lengths_and_credential_encoding() {
    for database in ["", "x\0y", "x".repeat(64).as_str()] {
        let mut scope = writer();
        scope.byte(2).unwrap();
        scope.byte(2).unwrap();
        scope.text(database).unwrap();
        rejects(&raw_policy(&[("read_data", &scope.bytes.unwrap())]));
    }
    for database in ["Upper", "sqlite_master", "briskdb_users"] {
        let mut scope = writer();
        scope.byte(2).unwrap();
        scope.byte(1).unwrap();
        scope.text(database).unwrap();
        rejects(&raw_policy(&[("read_data", &scope.bytes.unwrap())]));
    }
    rejects(&raw_policy(&[("read_data", &[2, 2, 0, 1, 0xff])]));
    rejects(&raw_policy(&[("read_data", &[2, 2, 0xff, 0xff])]));
    let (catalog, _, _) = setup();
    let record = catalog.to_record().unwrap();
    let offset = record
        .as_bytes()
        .windows(8)
        .position(|bytes| bytes == b"BRKSCR01")
        .unwrap();
    rejects(&modify(&record, |payload| payload[offset + 7] = b'2'));
    rejects(&modify(&record, |payload| {
        payload[offset + 8..offset + 12].copy_from_slice(&1_000_001_u32.to_be_bytes())
    }));
}

#[test]
fn duplicate_names_and_memberships_missing_references_and_oversized_unions_fail() {
    let (catalog, _, _) = setup();
    let record = catalog.to_record().unwrap();
    // Locate the bounded role/user records without relying on name byte offsets.
    let payload = &record.as_bytes()[..record.as_bytes().len() - CHECKSUM_BYTES];
    let mut reader = Reader { remaining: payload };
    reader.take(18).unwrap();
    let role_start = payload.len() - reader.remaining.len();
    reader.name().unwrap();
    reader.policy().unwrap();
    let role_end = payload.len() - reader.remaining.len();
    reader.short().unwrap();
    let user_start = payload.len() - reader.remaining.len();
    reader.name().unwrap();
    reader.long().unwrap();
    reader.long().unwrap();
    reader.take(SCRAM_SHA256_RECORD_BYTES).unwrap();
    let membership_start = payload.len() - reader.remaining.len();
    rejects(&modify(&record, |bytes| {
        bytes[16..18].copy_from_slice(&2_u16.to_be_bytes());
        let second = bytes[role_start..role_end].to_vec();
        bytes.splice(role_end..role_end, second);
    }));
    rejects(&modify(&record, |bytes| {
        bytes[role_end..role_end + 2].copy_from_slice(&2_u16.to_be_bytes());
        let second = bytes[user_start..].to_vec();
        bytes.extend_from_slice(&second);
    }));
    rejects(&modify(&record, |bytes| {
        bytes[membership_start..membership_start + 2].copy_from_slice(&2_u16.to_be_bytes());
        bytes.extend_from_slice(&[0, 0]);
    }));
    rejects(&modify(&record, |bytes| {
        bytes[membership_start + 2..].copy_from_slice(&1_u16.to_be_bytes())
    }));
    rejects(&modify(&record, |bytes| {
        bytes[membership_start..membership_start + 2].copy_from_slice(&65_u16.to_be_bytes())
    }));

    let (mut catalog, user, role) = setup();
    let full = Policy::new((0..MAX_POLICY_PRIVILEGES).map(|n| {
        Privilege::new(
            Action::ReadData,
            Scope::exact(Resource::object(DataDomain::Document, "app", &format!("c{n}")).unwrap()),
        )
        .unwrap()
    }))
    .unwrap();
    let full_name = name("app", "full");
    catalog.create_role(full_name.clone(), full).unwrap();
    catalog.users.get_mut(&user).unwrap().roles = BTreeSet::from([role, full_name]);
    rejects(catalog.to_record().unwrap().as_bytes());
}

#[test]
fn random_checksum_valid_mutations_are_bounded_and_never_panic_or_reuse_identity() {
    let (catalog, user, _) = setup();
    let principal = login(&catalog, &user);
    let record = catalog.to_record().unwrap();
    let mut seed = 0x9876_1234_u64;
    for _ in 0..2_048 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let mutated = modify(&record, |bytes| {
            let index = seed as usize % bytes.len();
            bytes[index] ^= (seed >> 24) as u8 | 1;
        });
        if let Ok(restored) = SecurityCatalog::from_record(&mutated) {
            assert!(
                restored
                    .authorize(&principal, Action::ReadData, &target())
                    .is_err()
            );
            let exported = restored.to_record().unwrap();
            assert_eq!(
                SecurityCatalog::from_record(exported.as_bytes())
                    .unwrap()
                    .to_record()
                    .unwrap()
                    .as_bytes(),
                exported.as_bytes()
            );
        }
    }
}

#[test]
fn export_uses_one_fixed_capacity_and_debug_omits_all_sensitive_content() {
    fn zeroize_on_drop<T: ZeroizeOnDrop>() {}
    fn send_sync<T: Send + Sync>() {}
    zeroize_on_drop::<SecurityCatalogRecord>();
    send_sync::<SecurityCatalogRecord>();
    let (catalog, _, _) = setup();
    let record = catalog.to_record().unwrap();
    assert_eq!(record.0.len(), record.0.capacity());
    let debug = format!("{record:?}");
    for value in ["alice", "reader", "app", "BRKSCR01", "private password"] {
        assert!(!debug.contains(value));
    }
    assert!(
        !record
            .as_bytes()
            .windows(b"private password".len())
            .any(|bytes| bytes == b"private password")
    );
}
