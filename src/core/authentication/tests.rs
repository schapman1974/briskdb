use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};

const AUTH_MESSAGE: &[u8] = concat!(
    "n=user,r=rOprNGfwEbeRWgbNEkqO,",
    "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,",
    "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,",
    "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
)
.as_bytes();

fn hex<const N: usize>(value: &str) -> [u8; N] {
    assert_eq!(value.len(), N * 2);
    std::array::from_fn(|index| u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap())
}

fn salt() -> [u8; 16] {
    hex("5b6d99689d12358eeca04b141236fa81")
}

fn fixture(password: &str) -> ScramSha256Verifier {
    let normalized = normalize_password(password).unwrap();
    ScramSha256Verifier::derive(
        normalized.as_bytes(),
        salt(),
        validate_iterations(4096).unwrap(),
    )
}

fn proof() -> [u8; 32] {
    hex("747cdb65aa56224e2352137e52d7bdcad6a0f738df30782caa69a2cfb0277554")
}

fn signature() -> [u8; 32] {
    hex("eabae24d1062db75a9451ff0b6ea7e98c8546549ff741e672d3251b2397de46e")
}

#[test]
fn rfc7677_sha256_exchange_matches_independently_derived_keys_and_signature() {
    // RFC 7677 section 3; keys independently calculated with Python hashlib/hmac.
    let verifier = fixture("pencil");
    assert_eq!(
        &verifier.stored_key[..],
        &hex::<32>("586e5df283e6dceb5c3e791d8b8528ec191e664045ce971792e2e6b5bb13e2a6")
    );
    assert_eq!(
        &verifier.server_key[..],
        &hex::<32>("c1f3cbc1c13a9d35a14c0990eed97629ea225863e566a4314ab99f3f00e5d9d5")
    );
    assert_eq!(verifier.salt(), salt());
    assert_eq!(verifier.iterations(), 4096);
    assert_eq!(
        verifier
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap(),
        signature()
    );
}

#[test]
fn independent_scram_client_checks_both_directions_with_random_salts_and_unicode() {
    for password in ["pencil", "I\u{00ad}X", "user\u{00a0}password"] {
        let verifier = ScramSha256Verifier::from_password_with_iterations(password, 4096).unwrap();
        for correct_password in [true, false] {
            let client_password = if correct_password {
                password
            } else {
                "wrong password"
            };
            let mut client =
                ScramSha256::new(client_password.as_bytes(), ChannelBinding::unsupported());
            let first = std::str::from_utf8(client.message())
                .unwrap()
                .strip_prefix("n,,")
                .unwrap()
                .to_owned();
            let nonce = first.rsplit_once(",r=").unwrap().1;
            let challenge = format!(
                "r={nonce}briskdb-server-test,s={},i={}",
                STANDARD.encode(verifier.salt()),
                verifier.iterations()
            );
            client.update(challenge.as_bytes()).unwrap();
            let final_message = std::str::from_utf8(client.message()).unwrap();
            let (without_proof, proof) = final_message.rsplit_once(",p=").unwrap();
            let transcript = format!("{first},{challenge},{without_proof}");
            let proof = STANDARD.decode(proof).unwrap();
            let result = verifier.verify_client_proof(transcript.as_bytes(), &proof);
            if correct_password {
                let reply = format!("v={}", STANDARD.encode(result.unwrap()));
                client.finish(reply.as_bytes()).unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().kind(),
                    EngineErrorKind::PermissionDenied
                );
            }
        }
    }
}

#[test]
fn every_changed_proof_or_transcript_byte_fails_without_returning_a_signature() {
    let verifier = fixture("pencil");
    for index in 0..32 {
        let mut changed = proof();
        changed[index] ^= 1;
        let error = verifier
            .verify_client_proof(AUTH_MESSAGE, &changed)
            .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "SCRAM authentication failed");
    }
    for index in 0..AUTH_MESSAGE.len() {
        let mut changed = AUTH_MESSAGE.to_vec();
        changed[index] ^= 1;
        assert_eq!(
            verifier
                .verify_client_proof(&changed, &proof())
                .unwrap_err()
                .kind(),
            EngineErrorKind::PermissionDenied
        );
    }
    for invalid in [vec![], vec![0; 31], vec![0; 33], vec![0; 1_024]] {
        assert_eq!(
            verifier
                .verify_client_proof(AUTH_MESSAGE, &invalid)
                .unwrap_err()
                .to_string(),
            "SCRAM authentication failed"
        );
    }
    assert_eq!(
        verifier
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap(),
        signature()
    );
}

#[test]
fn transcript_work_is_bounded_before_authentication() {
    let verifier = fixture("pencil");
    for invalid in [vec![], vec![0; MAX_SCRAM_AUTH_MESSAGE_BYTES + 1]] {
        assert_eq!(
            verifier
                .verify_client_proof(&invalid, &proof())
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }
    assert_eq!(
        verifier
            .verify_client_proof(&[0; MAX_SCRAM_AUTH_MESSAGE_BYTES], &proof())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
}

#[test]
fn versioned_records_round_trip_exactly_without_password_or_salted_password() {
    let verifier = fixture("pencil");
    let record = verifier.to_record();
    assert_eq!(record.as_bytes().len(), SCRAM_SHA256_RECORD_BYTES);
    assert_eq!(&record.as_bytes()[..8], b"BRKSCR01");
    assert_eq!(&record.as_bytes()[8..12], &4096_u32.to_be_bytes());
    assert_eq!(&record.as_bytes()[12..28], verifier.salt());
    let restored = ScramSha256Verifier::from_record(record.as_bytes()).unwrap();
    assert_eq!(restored.to_record().as_bytes(), record.as_bytes());
    assert_eq!(
        restored
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap(),
        signature()
    );
    drop(verifier);
    drop(record);
    assert_eq!(
        restored
            .clone()
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap(),
        signature()
    );
}

#[test]
fn record_decoder_rejects_truncation_trailing_bytes_version_and_excessive_work() {
    let record = fixture("pencil").to_record();
    for length in 0..SCRAM_SHA256_RECORD_BYTES {
        assert_eq!(
            ScramSha256Verifier::from_record(&record.as_bytes()[..length])
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
    }
    let mut trailing = record.as_bytes().to_vec();
    trailing.push(0);
    assert!(ScramSha256Verifier::from_record(&trailing).is_err());
    for index in 0..8 {
        let mut changed = record.as_bytes().to_vec();
        changed[index] ^= 1;
        assert!(ScramSha256Verifier::from_record(&changed).is_err());
    }
    for iterations in [
        0,
        1,
        MIN_SCRAM_SHA256_ITERATIONS - 1,
        MAX_SCRAM_SHA256_ITERATIONS + 1,
        u32::MAX,
    ] {
        let mut changed = record.as_bytes().to_vec();
        changed[8..12].copy_from_slice(&iterations.to_be_bytes());
        assert!(ScramSha256Verifier::from_record(&changed).is_err());
        assert!(ScramSha256Verifier::from_password_with_iterations("pencil", iterations).is_err());
    }
    assert!(validate_iterations(MIN_SCRAM_SHA256_ITERATIONS).is_ok());
    assert!(validate_iterations(MAX_SCRAM_SHA256_ITERATIONS).is_ok());
}

#[test]
fn malformed_passwords_are_rejected_before_derivation_with_fixed_diagnostics() {
    for invalid_password in [
        String::new(),
        "x".repeat(MAX_SCRAM_PASSWORD_BYTES + 1),
        "private\0value".to_owned(),
        "private\nvalue".to_owned(),
        "\u{00ad}".to_owned(),
        "\u{fdfa}".repeat(80),
        "abc\u{05d0}".to_owned(),
    ] {
        let error = ScramSha256Verifier::from_password_with_iterations(
            &invalid_password,
            MIN_SCRAM_SHA256_ITERATIONS,
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert!(!error.to_string().contains("private"));
    }
    assert!(
        ScramSha256Verifier::from_password_with_iterations(
            &"x".repeat(MAX_SCRAM_PASSWORD_BYTES),
            MIN_SCRAM_SHA256_ITERATIONS
        )
        .is_ok()
    );
}

#[test]
fn strict_saslprep_equivalences_produce_the_same_verifier_without_case_folding() {
    for (raw, normalized) in [
        ("I\u{00ad}X", "IX"),
        ("user\u{00a0}password", "user password"),
        ("\u{2168}", "IX"),
    ] {
        assert_eq!(
            fixture(raw).to_record().as_bytes(),
            fixture(normalized).to_record().as_bytes()
        );
    }
    assert_ne!(
        fixture("Pencil").to_record().as_bytes(),
        fixture("pencil").to_record().as_bytes()
    );
}

#[test]
fn default_provisioning_uses_fresh_salts_and_the_explicit_default_cost() {
    let first = ScramSha256Verifier::from_password("same test password").unwrap();
    let second = ScramSha256Verifier::from_password("same test password").unwrap();
    assert_eq!(first.iterations(), DEFAULT_SCRAM_SHA256_ITERATIONS);
    assert_eq!(second.iterations(), DEFAULT_SCRAM_SHA256_ITERATIONS);
    assert_ne!(first.salt(), second.salt());
    assert_ne!(first.to_record().as_bytes(), second.to_record().as_bytes());
}

#[test]
fn credential_rotation_does_not_mutate_retained_generations_or_accept_old_proofs() {
    let old = fixture("pencil");
    let retained = old.clone();
    let replacement = fixture("replacement password");
    assert_eq!(
        replacement
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap_err()
            .kind(),
        EngineErrorKind::PermissionDenied
    );
    drop(old);
    assert_eq!(
        retained
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .unwrap(),
        signature()
    );
    let restored = ScramSha256Verifier::from_record(replacement.to_record().as_bytes()).unwrap();
    assert!(
        restored
            .verify_client_proof(AUTH_MESSAGE, &proof())
            .is_err()
    );
}

#[test]
fn credential_and_record_debug_are_redacted_thread_safe_and_zeroizing() {
    fn contract<T: Send + Sync + ZeroizeOnDrop>() {}
    contract::<ScramSha256Verifier>();
    contract::<ScramSha256Record>();
    let verifier = fixture("pencil");
    assert_eq!(
        format!("{verifier:?}"),
        "ScramSha256Verifier { iterations: 4096, .. }"
    );
    assert_eq!(
        format!("{:?}", verifier.to_record()),
        "ScramSha256Record { .. }"
    );
}
