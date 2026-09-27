//! Bounded SCRAM-SHA-256 conversations over an already authenticated TLS transport.
//! No passwords, transcripts, names or verifier material are logged.

use std::{io, sync::Mutex, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::time::Instant;
use zeroize::Zeroizing;

use super::Request;
use crate::{
    EngineErrorKind,
    core::{
        Engine, Session,
        authentication::ScramSha256Verifier,
        security_catalog::{ScramAttempt, SecurityName},
    },
    document::{BsonBinary, BsonDocument, BsonValue},
};

const CONVERSATION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PAYLOAD: usize = 4096;
const BURST: f64 = 64.0;
const PER_SECOND: f64 = 32.0;

#[cfg(all(test, unix))]
mod tests;

/// Shared by all connections, with no attacker-controlled per-user map.
pub(super) struct Authentication {
    engine: Engine,
    seed: Zeroizing<[u8; 32]>,
    audit_key: Zeroizing<[u8; 32]>,
    admission: Mutex<(Instant, f64)>,
}

impl Authentication {
    pub(super) fn new(engine: Engine) -> io::Result<Self> {
        let mut seed = Zeroizing::new([0; 32]);
        SystemRandom::new()
            .fill(&mut seed[..])
            .map_err(|_| io::Error::other("Mongo authentication entropy unavailable"))?;
        let mut audit_key = Zeroizing::new([0; 32]);
        SystemRandom::new()
            .fill(&mut audit_key[..])
            .map_err(|_| io::Error::other("Mongo audit entropy unavailable"))?;
        Ok(Self {
            engine,
            seed,
            audit_key,
            admission: Mutex::new((Instant::now(), BURST)),
        })
    }

    /// The listener owns one engine/catalog. IDs are stable across its pooled
    /// sockets and password changes, but not stable across listener lifetimes.
    /// No account name, attempted username or per-user map enters telemetry.
    pub(super) fn audit_context(&self, session: &Session) -> super::events::AuditContext {
        match &session.principal {
            Some(principal) => {
                let (id, generation) = principal.audit_identity();
                let user = blake3::keyed_hash(&self.audit_key, &id.to_le_bytes());
                super::events::AuditContext::authenticated(user, generation)
            }
            None => super::events::AuditContext::unauthenticated(),
        }
    }

    fn admit(&self) -> Result<(), ()> {
        let mut budget = self.admission.lock().map_err(|_| ())?;
        let now = Instant::now();
        budget.1 = (budget.1 + now.duration_since(budget.0).as_secs_f64() * PER_SECOND).min(BURST);
        budget.0 = now;
        if budget.1 < 1.0 {
            return Err(());
        }
        budget.1 -= 1.0;
        Ok(())
    }
}

enum Credential {
    Known(ScramAttempt),
    Absent(ScramSha256Verifier),
}

struct Proof {
    credential: Credential,
    first: String,
    challenge: String,
    nonce: String,
    skip_empty: bool,
}

enum Pending {
    Proof(Proof),
    Empty(Session),
}

struct Exchange {
    pending: Pending,
    id: i32,
    realm: String,
    deadline: Instant,
}

#[derive(Default)]
pub(super) struct Conversation {
    exchange: Option<Exchange>,
    finished: bool,
    failed: bool,
}

impl Conversation {
    pub(super) fn deadline(&self) -> Option<Instant> {
        self.exchange.as_ref().map(|exchange| exchange.deadline)
    }

    pub(super) fn failed(&self) -> bool {
        self.failed
    }

    /// None means this is not an authentication command. Data admission is
    /// checked separately, including during the legacy final empty exchange.
    pub(super) async fn handle(
        &mut self,
        service: &Authentication,
        request: &Request,
        session: &mut Session,
    ) -> Option<BsonDocument> {
        let command = request.body.iter().next()?.0;
        if !matches!(
            command,
            "saslStart" | "saslContinue" | "authenticate" | "logout"
        ) {
            return None;
        }
        let deadline = self
            .deadline()
            .unwrap_or_else(|| Instant::now() + CONVERSATION_TIMEOUT);
        let result = tokio::time::timeout_at(deadline, async {
            if self.failed
                || self.finished
                || request.legacy_handshake
                || request.more_to_come
                || !request.sequences.is_empty()
            {
                return Err(());
            }
            match command {
                "saslStart" if self.exchange.is_none() => {
                    self.start(service, request, deadline).await
                }
                "saslContinue" => self.continue_exchange(service, request, session).await,
                _ => Err(()),
            }
        })
        .await;
        Some(match result {
            Ok(Ok(reply)) => reply,
            _ => {
                self.exchange = None;
                self.failed = true;
                super::listener::error(18, "AuthenticationFailed", "authentication failed")
            }
        })
    }

    async fn start(
        &mut self,
        service: &Authentication,
        request: &Request,
        deadline: Instant,
    ) -> Result<BsonDocument, ()> {
        service.admit()?;
        validate_fields(
            request,
            &[
                "saslStart",
                "mechanism",
                "payload",
                "options",
                "autoAuthorize",
                "$db",
            ],
        )?;
        if !one(request.body.get_first("saslStart"))
            || !matches!(request.body.get_first("mechanism"), Some(BsonValue::String(name)) if name == "SCRAM-SHA-256")
            || request
                .body
                .get_first("autoAuthorize")
                .is_some_and(|value| !one(Some(value)))
        {
            return Err(());
        }
        let skip_empty = match request.body.get_first("options") {
            None => false,
            Some(BsonValue::Document(options)) => {
                let mut skip = false;
                let mut seen = false;
                for (key, value) in options.iter() {
                    if key != "skipEmptyExchange" || seen {
                        return Err(());
                    }
                    let BsonValue::Boolean(value) = value else {
                        return Err(());
                    };
                    skip = *value;
                    seen = true;
                }
                skip
            }
            _ => return Err(()),
        };
        let (first, username, client_nonce) = client_first(payload(request)?)?;
        let name = SecurityName::new(&request.database, &username).map_err(|_| ())?;
        let credential = match service.engine.begin_authentication(name).await {
            Ok(attempt) => Credential::Known(attempt),
            Err(error) if error.kind() == EngineErrorKind::PermissionDenied => Credential::Absent(
                ScramSha256Verifier::concealed(&service.seed, &request.database, &username),
            ),
            Err(_) => return Err(()),
        };
        let mut entropy = [0; 36];
        SystemRandom::new().fill(&mut entropy).map_err(|_| ())?;
        let nonce = format!("{client_nonce}{}", STANDARD.encode(&entropy[..32]));
        let id = (u32::from_be_bytes(entropy[32..].try_into().expect("ID width")) & 0x7fff_ffff)
            .max(1) as i32;
        let (salt, iterations) = match &credential {
            Credential::Known(attempt) => (attempt.salt(), attempt.iterations()),
            Credential::Absent(verifier) => (verifier.salt(), verifier.iterations()),
        };
        let challenge = format!("r={nonce},s={},i={iterations}", STANDARD.encode(salt));
        let reply = reply(id, false, challenge.as_bytes());
        self.exchange = Some(Exchange {
            pending: Pending::Proof(Proof {
                credential,
                first: first.to_owned(),
                challenge,
                nonce,
                skip_empty,
            }),
            id,
            realm: request.database.clone(),
            deadline,
        });
        Ok(reply)
    }

    async fn continue_exchange(
        &mut self,
        service: &Authentication,
        request: &Request,
        session: &mut Session,
    ) -> Result<BsonDocument, ()> {
        validate_fields(
            request,
            &["saslContinue", "conversationId", "payload", "$db"],
        )?;
        if !one(request.body.get_first("saslContinue")) {
            return Err(());
        }
        let exchange = self.exchange.take().ok_or(())?;
        if exchange.deadline <= Instant::now()
            || exchange.realm != request.database
            || !matches!(request.body.get_first("conversationId"), Some(BsonValue::Int32(id)) if *id == exchange.id)
        {
            return Err(());
        }
        let bytes = payload(request)?;
        match exchange.pending {
            Pending::Proof(proof) => {
                let (final_bare, client_proof) = client_final(bytes, &proof.nonce)?;
                let message = format!("{},{},{final_bare}", proof.first, proof.challenge);
                let (authenticated, signature) = match proof.credential {
                    Credential::Known(attempt) => service
                        .engine
                        .complete_authentication(attempt, message.into_bytes(), client_proof)
                        .await
                        .map_err(|_| ())?,
                    Credential::Absent(verifier) => {
                        let _ = verifier.verify_client_proof(message.as_bytes(), &client_proof);
                        return Err(());
                    }
                };
                if Instant::now() >= exchange.deadline {
                    return Err(());
                }
                let reply = reply(
                    exchange.id,
                    proof.skip_empty,
                    format!("v={}", STANDARD.encode(signature)).as_bytes(),
                );
                if proof.skip_empty {
                    *session = authenticated;
                    self.finished = true;
                } else {
                    self.exchange = Some(Exchange {
                        pending: Pending::Empty(authenticated),
                        ..exchange
                    });
                }
                Ok(reply)
            }
            Pending::Empty(authenticated) => {
                if !bytes.is_empty() {
                    return Err(());
                }
                *session = authenticated;
                self.finished = true;
                Ok(reply(exchange.id, true, b""))
            }
        }
    }
}

fn one(value: Option<&BsonValue>) -> bool {
    matches!(value, Some(BsonValue::Int32(1) | BsonValue::Int64(1)))
        || matches!(value, Some(BsonValue::Double(value)) if *value == 1.0)
}

fn validate_fields(request: &Request, allowed: &[&str]) -> Result<(), ()> {
    // The allowed set is small and fixed; no attacker-controlled set allocation.
    let mut seen = 0u32;
    for (name, _) in request.body.iter() {
        let index = allowed
            .iter()
            .position(|allowed| *allowed == name)
            .ok_or(())?;
        if seen & (1 << index) != 0 {
            return Err(());
        }
        seen |= 1 << index;
    }
    Ok(())
}

fn payload(request: &Request) -> Result<&[u8], ()> {
    match request.body.get_first("payload") {
        Some(BsonValue::Binary(value))
            if value.subtype() == 0 && value.bytes().len() <= MAX_PAYLOAD =>
        {
            Ok(value.bytes())
        }
        _ => Err(()),
    }
}

fn attributes(message: &str) -> Result<Vec<(&str, &str)>, ()> {
    let mut seen = [false; 128];
    let mut fields = Vec::new();
    for field in message.split(',') {
        let (key, value) = field.split_once('=').ok_or(())?;
        if key.len() != 1 || !key.as_bytes()[0].is_ascii_alphabetic() || key == "m" {
            return Err(());
        }
        let slot = &mut seen[key.as_bytes()[0] as usize];
        if *slot || value.is_empty() {
            return Err(());
        }
        *slot = true;
        fields.push((key, value));
    }
    Ok(fields)
}

fn client_first(bytes: &[u8]) -> Result<(&str, String, &str), ()> {
    let first = std::str::from_utf8(bytes)
        .map_err(|_| ())?
        .strip_prefix("n,,")
        .ok_or(())?;
    let fields = attributes(first)?;
    let [("n", escaped), ("r", nonce), ..] = fields.as_slice() else {
        return Err(());
    };
    if nonce.len() > 1024
        || !nonce
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && byte != b',')
    {
        return Err(());
    }
    let mut username = String::with_capacity(escaped.len());
    let mut parts = escaped.split('=');
    username.push_str(parts.next().ok_or(())?);
    for part in parts {
        if let Some(rest) = part.strip_prefix("2C") {
            username.push(',');
            username.push_str(rest);
        } else if let Some(rest) = part.strip_prefix("3D") {
            username.push('=');
            username.push_str(rest);
        } else {
            return Err(());
        }
    }
    Ok((first, username, nonce))
}

fn client_final<'a>(bytes: &'a [u8], nonce: &str) -> Result<(&'a str, Vec<u8>), ()> {
    let message = std::str::from_utf8(bytes).map_err(|_| ())?;
    let fields = attributes(message)?;
    let [("c", "biws"), ("r", echoed), ..] = fields.as_slice() else {
        return Err(());
    };
    if *echoed != nonce {
        return Err(());
    }
    let Some(("p", encoded)) = fields.last() else {
        return Err(());
    };
    let proof = STANDARD.decode(encoded).map_err(|_| ())?;
    if proof.len() != 32 || STANDARD.encode(&proof) != *encoded {
        return Err(());
    }
    let (bare, _) = message.rsplit_once(",p=").ok_or(())?;
    Ok((bare, proof))
}

fn reply(id: i32, done: bool, payload: &[u8]) -> BsonDocument {
    super::listener::fields(&[
        ("ok", BsonValue::Double(1.0)),
        ("conversationId", BsonValue::Int32(id)),
        ("done", BsonValue::Boolean(done)),
        (
            "payload",
            BsonValue::Binary(BsonBinary::new(0, payload.to_vec())),
        ),
    ])
}
