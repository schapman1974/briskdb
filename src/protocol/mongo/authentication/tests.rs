use super::*;
use crate::{
    core::{
        EngineOptions,
        security_catalog::{SecurityCatalog, tests as fixtures},
    },
    storage::Database,
};
use ring::{digest, hmac, pbkdf2};
use std::{num::NonZeroU32, os::unix::fs::PermissionsExt};

async fn service() -> (tempfile::TempDir, Authentication) {
    let root = tempfile::tempdir().unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    drop(Database::open(root.path(), 2).unwrap());
    let mut catalog = SecurityCatalog::new();
    catalog
        .create_user(
            SecurityName::new("admin", "alice").unwrap(),
            fixtures::credential(),
            [],
        )
        .unwrap();
    Engine::provision_security(root.path(), 2, catalog)
        .await
        .unwrap();
    let engine = Engine::open_authenticated(root.path(), 2, EngineOptions::default())
        .await
        .unwrap();
    (root, Authentication::new(engine).unwrap())
}

fn request(entries: &[(&str, BsonValue)]) -> Request {
    Request {
        request_id: 1,
        database: "admin".into(),
        body: super::super::listener::fields(entries),
        sequences: vec![],
        more_to_come: false,
        legacy_handshake: false,
    }
}

fn binary(bytes: &[u8]) -> BsonValue {
    BsonValue::Binary(BsonBinary::new(0, bytes))
}
fn start(name: &str, skip: bool) -> Request {
    request(&[
        ("saslStart", BsonValue::Int32(1)),
        ("mechanism", BsonValue::from("SCRAM-SHA-256")),
        (
            "payload",
            binary(format!("n,,n={name},r=client-nonce").as_bytes()),
        ),
        (
            "options",
            BsonValue::Document(super::super::listener::fields(&[(
                "skipEmptyExchange",
                BsonValue::Boolean(skip),
            )])),
        ),
        ("$db", BsonValue::from("admin")),
    ])
}

fn bytes(reply: &BsonDocument) -> &[u8] {
    let Some(BsonValue::Binary(payload)) = reply.get_first("payload") else {
        panic!("missing payload: {reply:?}");
    };
    payload.bytes()
}

fn continuation(challenge: &BsonDocument, payload: &[u8]) -> Request {
    request(&[
        ("saslContinue", BsonValue::Int32(1)),
        (
            "conversationId",
            challenge.get_first("conversationId").unwrap().clone(),
        ),
        ("payload", binary(payload)),
        ("$db", BsonValue::from("admin")),
    ])
}

// Test-side transcript construction. External PyMongo verifies interoperability
// separately, including the server signature, without this helper.
fn proof(challenge: &BsonDocument, name: &str, password: &str) -> (Vec<u8>, Vec<u8>) {
    let challenge = std::str::from_utf8(bytes(challenge)).unwrap();
    let parts = attributes(challenge).unwrap();
    let salt = STANDARD.decode(parts[1].1).unwrap();
    let mut salted = [0; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(parts[2].1.parse().unwrap()).unwrap(),
        &salt,
        password.as_bytes(),
        &mut salted,
    );
    let mac = |key: &[u8], message: &[u8]| {
        hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), message)
            .as_ref()
            .to_vec()
    };
    let client = mac(&salted, b"Client Key");
    let stored = digest::digest(&digest::SHA256, &client);
    let final_bare = format!("c=biws,r={}", parts[0].1);
    let message = format!("n={name},r=client-nonce,{challenge},{final_bare}");
    let signature = mac(stored.as_ref(), message.as_bytes());
    let proof: Vec<_> = client
        .iter()
        .zip(signature)
        .map(|(key, signature)| key ^ signature)
        .collect();
    let server = mac(&mac(&salted, b"Server Key"), message.as_bytes());
    (
        format!("{final_bare},p={}", STANDARD.encode(proof)).into_bytes(),
        format!("v={}", STANDARD.encode(server)).into_bytes(),
    )
}

fn failed(reply: &BsonDocument) {
    assert_eq!(reply.get_first("code"), Some(&BsonValue::Int32(18)));
    assert_eq!(
        reply.get_first("errmsg"),
        Some(&BsonValue::from("authentication failed"))
    );
}

#[test]
fn strict_scram_grammar_nonce_binding_and_username_escaping() {
    let (_, user, nonce) = client_first(b"n,,n=a=2Cb=3Dc,r=abc").unwrap();
    assert_eq!((user.as_str(), nonce), ("a,b=c", "abc"));
    for first in [
        "n,,n=,r=abc",
        "y,,n=a,r=abc",
        "p=tls-server-end-point,,n=a,r=abc",
        "n,a=x,n=a,r=abc",
        "n,,r=abc,n=a",
        "n,,n=a,r=",
        "n,,n=a,r=a b",
        "n,,n=a=2c,r=abc",
        "n,,n=a,r=abc,r=def",
        "n,,m=x,n=a,r=abc",
        "n,,n=a,r=abc,m=x",
    ] {
        assert!(client_first(first.as_bytes()).is_err(), "{first}");
    }
    let proof = STANDARD.encode([1u8; 32]);
    let valid = format!("c=biws,r=abc,p={proof}");
    assert!(client_final(valid.as_bytes(), "abc").is_ok());
    assert!(client_final(valid.as_bytes(), "def").is_err());
    for value in [
        format!("c=eSws,r=abc,p={proof}"),
        format!("c=biws,r=abc,p={proof},x=v"),
        format!("c=biws,r=abc,p={proof},p={proof}"),
        format!("c=biws,r=abc,m=x,p={proof}"),
        "c=biws,r=abc,p=AAAA".into(),
        format!("r=abc,c=biws,p={proof}"),
    ] {
        assert!(client_final(value.as_bytes(), "abc").is_err());
    }
}

#[tokio::test]
async fn success_requires_the_correct_proof_and_optional_empty_ack_before_session_replacement() {
    let (_root, service) = service().await;
    for skip in [false, true] {
        let mut conversation = Conversation::default();
        let mut session = service.engine.session();
        let original = session.id();
        let challenge = conversation
            .handle(&service, &start("alice", skip), &mut session)
            .await
            .unwrap();
        assert!(!session.is_authenticated());
        let (proof, expected) = proof(&challenge, "alice", fixtures::PASSWORD);
        let reply = conversation
            .handle(&service, &continuation(&challenge, &proof), &mut session)
            .await
            .unwrap();
        assert_eq!(bytes(&reply), expected);
        assert_eq!(session.is_authenticated(), skip);
        assert_eq!(reply.get_first("done"), Some(&BsonValue::Boolean(skip)));
        if !skip {
            assert_eq!(session.id(), original);
            let reply = conversation
                .handle(&service, &continuation(&challenge, b""), &mut session)
                .await
                .unwrap();
            assert_eq!(reply.get_first("done"), Some(&BsonValue::Boolean(true)));
        }
        assert!(session.is_authenticated());
        assert_ne!(session.id(), original);
        assert!(conversation.deadline().is_none());
        // Same-connection reauthentication cannot relabel this session.
        let identity = session.id();
        failed(
            &conversation
                .handle(&service, &start("alice", true), &mut session)
                .await
                .unwrap(),
        );
        assert_eq!(session.id(), identity);
    }
}

#[tokio::test]
async fn absent_users_wrong_passwords_and_replays_fail_with_identical_redacted_errors() {
    let (_root, service) = service().await;
    let mut errors = Vec::new();
    for (name, password) in [("alice", "incorrect"), ("missing", fixtures::PASSWORD)] {
        let mut conversation = Conversation::default();
        let mut session = service.engine.session();
        let challenge = conversation
            .handle(&service, &start(name, true), &mut session)
            .await
            .unwrap();
        let (proof, _) = proof(&challenge, name, password);
        let reply = conversation
            .handle(&service, &continuation(&challenge, &proof), &mut session)
            .await
            .unwrap();
        failed(&reply);
        assert!(!session.is_authenticated());
        assert!(conversation.failed());
        errors.push(reply);
    }
    assert_eq!(errors[0], errors[1]);
    let mut challenges = Vec::new();
    let mut old_proof: Option<Vec<u8>> = None;
    for _ in 0..2 {
        let mut conversation = Conversation::default();
        let mut session = service.engine.session();
        let challenge = conversation
            .handle(&service, &start("alice", true), &mut session)
            .await
            .unwrap();
        if let Some(proof) = &old_proof {
            failed(
                &conversation
                    .handle(&service, &continuation(&challenge, proof), &mut session)
                    .await
                    .unwrap(),
            );
        } else {
            old_proof = Some(proof(&challenge, "alice", fixtures::PASSWORD).0);
        }
        challenges.push(bytes(&challenge).to_vec());
    }
    assert_ne!(challenges[0], challenges[1]);
    let a = ScramSha256Verifier::concealed(&service.seed, "admin", "missing");
    let b = ScramSha256Verifier::concealed(&service.seed, "admin", "missing");
    let c = ScramSha256Verifier::concealed(&service.seed, "admin", "other");
    assert_eq!(a.salt(), b.salt());
    assert_ne!(a.salt(), c.salt());
}

#[tokio::test]
async fn wrong_conversation_realm_expiry_rotation_and_out_of_order_messages_fail_closed() {
    let (_root, service) = service().await;
    for variant in 0..7 {
        let mut conversation = Conversation::default();
        let mut session = service.engine.session();
        let challenge = conversation
            .handle(&service, &start("alice", true), &mut session)
            .await
            .unwrap();
        let (proof, _) = proof(&challenge, "alice", fixtures::PASSWORD);
        let mut next = continuation(&challenge, &proof);
        match variant {
            0 => conversation.exchange.as_mut().unwrap().id = 0,
            1 => next.database = "other".into(),
            2 => {
                conversation.exchange.as_mut().unwrap().deadline =
                    Instant::now() - Duration::from_secs(1)
            }
            3 => {
                service
                    .engine
                    .update_security_catalog(|catalog| {
                        catalog.rotate_credentials(
                            &SecurityName::new("admin", "alice")?,
                            fixtures::credential(),
                        )
                    })
                    .await
                    .unwrap();
            }
            4 => next = start("alice", true),
            5 => next.more_to_come = true,
            6 => next.legacy_handshake = true,
            _ => unreachable!(),
        }
        failed(
            &conversation
                .handle(&service, &next, &mut session)
                .await
                .unwrap(),
        );
        assert!(!session.is_authenticated());
        assert!(conversation.exchange.is_none());
        assert!(conversation.failed());
    }
}

#[tokio::test]
async fn authentication_admission_and_payload_fields_have_fixed_bounds() {
    let (_root, service) = service().await;
    for _ in 0..64 {
        service.admit().unwrap();
    }
    assert!(service.admit().is_err());
    service.admission.lock().unwrap().0 = Instant::now() - Duration::from_secs(2);
    service.admit().unwrap();
    assert!(service.admission.lock().unwrap().1 <= BURST - 1.0);
    for variant in 0..5 {
        let mut conversation = Conversation::default();
        let mut session = service.engine.session();
        let mut first = start("alice", true);
        match variant {
            0 => first.body.push("payload", binary(b"duplicate")).unwrap(),
            1 => first.body.push("unknown", BsonValue::Int32(1)).unwrap(),
            2 => first.more_to_come = true,
            3 => first.legacy_handshake = true,
            4 => {
                first = request(&[
                    ("saslStart", BsonValue::Int32(1)),
                    ("mechanism", BsonValue::from("SCRAM-SHA-256")),
                    ("payload", binary(&vec![b'x'; MAX_PAYLOAD + 1])),
                ])
            }
            _ => unreachable!(),
        }
        failed(
            &conversation
                .handle(&service, &first, &mut session)
                .await
                .unwrap(),
        );
        assert!(!session.is_authenticated());
    }
}
