//! Protocol-neutral SCRAM-SHA-256 credential material.
//!
//! This opt-in foundation does not enable listener authentication, associate a
//! user with a session, or authorize any operation. Adapters must build the
//! authentication transcript from a validated, fresh, server-owned exchange,
//! enforce TLS and mechanism/channel-binding policy, and bind successful proof
//! verification to the exact catalog identity before admitting a session.

use std::{fmt, num::NonZeroU32};

use ring::{digest, hmac, pbkdf2};
use subtle::ConstantTimeEq;
use zeroize::{ZeroizeOnDrop, Zeroizing};

use super::{EngineError, EngineErrorKind, EngineResult};

/// Default cost for newly derived credentials; existing listeners are unchanged.
pub const DEFAULT_SCRAM_SHA256_ITERATIONS: u32 = 600_000;
/// Compatibility floor for an explicitly selected or restored work factor.
pub const MIN_SCRAM_SHA256_ITERATIONS: u32 = 4_096;
/// Bound trusted provisioning work and reject corrupt/excessive stored costs.
pub const MAX_SCRAM_SHA256_ITERATIONS: u32 = 1_000_000;
/// Limit both input and normalized UTF-8 password bytes before derivation.
pub const MAX_SCRAM_PASSWORD_BYTES: usize = 1_024;
/// The host must additionally validate the exchange grammar, nonce and binding.
pub const MAX_SCRAM_AUTH_MESSAGE_BYTES: usize = 8_192;
/// Fixed versioned record size: magic, iterations, salt, StoredKey, ServerKey.
pub const SCRAM_SHA256_RECORD_BYTES: usize = 92;
const RECORD_MAGIC: &[u8; 8] = b"BRKSCR01";

/// Immutable salted verifier, not a retained plaintext or SaltedPassword.
///
/// PBKDF2-HMAC-SHA-256, HMAC and SHA-256 use ring. Provisioning is synchronous
/// CPU work: async hosts must use bounded blocking admission. Debug is redacted.
/// Owned normalized/derived buffers and keys are zeroized on drop; no claim is
/// made about caller-owned inputs or every copy inside normalization/crypto code.
///
/// ```
/// use briskdb::core::authentication::ScramSha256Verifier;
///
/// let verifier = ScramSha256Verifier::from_password("example provisioning password")?;
/// let record = verifier.to_record(); // Sensitive: do not log or send to clients.
/// let restored = ScramSha256Verifier::from_record(record.as_bytes())?;
/// assert_eq!(restored.salt(), verifier.salt());
/// assert_eq!(restored.iterations(), verifier.iterations());
/// # Ok::<_, briskdb::EngineError>(())
/// ```
#[derive(Clone)]
pub struct ScramSha256Verifier {
    iterations: NonZeroU32,
    salt: [u8; 16],
    stored_key: Zeroizing<[u8; 32]>,
    server_key: Zeroizing<[u8; 32]>,
}

impl ScramSha256Verifier {
    /// Provision with a fresh operating-system salt and the default work factor.
    /// Passwords use strict SASLprep, including rejection of prohibited output.
    pub fn from_password(password: &str) -> EngineResult<Self> {
        Self::from_password_with_iterations(password, DEFAULT_SCRAM_SHA256_ITERATIONS)
    }

    /// Explicit bounded work factor for interoperability or measured host policy.
    /// The compatibility floor is not a recommendation to lower the default.
    pub fn from_password_with_iterations(password: &str, iterations: u32) -> EngineResult<Self> {
        let iterations = validate_iterations(iterations)?;
        let normalized = normalize_password(password)?;
        let mut salt = [0; 16];
        getrandom::fill(&mut salt).map_err(|_| {
            EngineError::new(EngineErrorKind::Internal, "SCRAM salt generation failed")
        })?;
        Ok(Self::derive(normalized.as_bytes(), salt, iterations))
    }

    fn derive(password: &[u8], salt: [u8; 16], iterations: NonZeroU32) -> Self {
        let mut salted = Zeroizing::new([0; 32]);
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            &salt,
            password,
            &mut salted[..],
        );
        let client_key = Zeroizing::new(mac(&salted[..], b"Client Key"));
        let stored_key = digest::digest(&digest::SHA256, &client_key[..]);
        Self {
            iterations,
            salt,
            stored_key: Zeroizing::new(stored_key.as_ref().try_into().expect("SHA-256 size")),
            server_key: Zeroizing::new(mac(&salted[..], b"Server Key")),
        }
    }

    /// Public challenge metadata; no secret key material is returned.
    pub const fn iterations(&self) -> u32 {
        self.iterations.get()
    }

    /// Per-credential salt advertised in the SCRAM server-first challenge.
    pub fn salt(&self) -> &[u8] {
        &self.salt
    }

    /// Check a decoded 32-byte ClientProof against the host's AuthMessage.
    ///
    /// On success return the 32-byte ServerSignature for this exact transcript.
    /// This is not a SASL parser or a replay-prevention mechanism. The trusted
    /// adapter must validate nonce, username, mechanism, channel binding and
    /// exchange state before calling, and must not reuse completed exchanges.
    /// Wrong/incorrect-length proofs return one fixed PermissionDenied error.
    pub fn verify_client_proof(
        &self,
        auth_message: &[u8],
        client_proof: &[u8],
    ) -> EngineResult<[u8; 32]> {
        if auth_message.is_empty() || auth_message.len() > MAX_SCRAM_AUTH_MESSAGE_BYTES {
            return Err(invalid("SCRAM authentication transcript length is invalid"));
        }
        if client_proof.len() != 32 {
            return Err(authentication_failed());
        }
        let signature = Zeroizing::new(mac(&self.stored_key[..], auth_message));
        let mut client_key = Zeroizing::new([0; 32]);
        for (index, byte) in client_key.iter_mut().enumerate() {
            *byte = client_proof[index] ^ signature[index];
        }
        let candidate = digest::digest(&digest::SHA256, &client_key[..]);
        if !bool::from(candidate.as_ref().ct_eq(&self.stored_key[..])) {
            return Err(authentication_failed());
        }
        Ok(mac(&self.server_key[..], auth_message))
    }

    /// Export sensitive verifier material for a trusted catalog, not a client.
    ///
    /// This fixed, versioned record is neither encrypted nor authenticated.
    /// Its storage layer must protect access and supply its own integrity and
    /// atomic-update guarantees. Export does not write any database files.
    pub fn to_record(&self) -> ScramSha256Record {
        let mut bytes = Zeroizing::new([0; SCRAM_SHA256_RECORD_BYTES]);
        bytes[..8].copy_from_slice(RECORD_MAGIC);
        bytes[8..12].copy_from_slice(&self.iterations().to_be_bytes());
        bytes[12..28].copy_from_slice(&self.salt);
        bytes[28..60].copy_from_slice(&self.stored_key[..]);
        bytes[60..92].copy_from_slice(&self.server_key[..]);
        ScramSha256Record(bytes)
    }

    /// Restore only the exact supported record version, size and bounded cost.
    /// This validates encoding, not authenticity or the catalog's user binding.
    pub fn from_record(bytes: &[u8]) -> EngineResult<Self> {
        if bytes.len() != SCRAM_SHA256_RECORD_BYTES || &bytes[..8] != RECORD_MAGIC {
            return Err(invalid("SCRAM verifier record encoding is invalid"));
        }
        let iterations = validate_iterations(u32::from_be_bytes(
            bytes[8..12].try_into().expect("record size checked"),
        ))?;
        Ok(Self {
            iterations,
            salt: bytes[12..28].try_into().expect("record size checked"),
            stored_key: Zeroizing::new(bytes[28..60].try_into().expect("record size checked")),
            server_key: Zeroizing::new(bytes[60..92].try_into().expect("record size checked")),
        })
    }
}

impl fmt::Debug for ScramSha256Verifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramSha256Verifier")
            .field("iterations", &self.iterations())
            .finish_non_exhaustive()
    }
}

impl ZeroizeOnDrop for ScramSha256Verifier {}

/// Sensitive, bounded catalog record. Debug is redacted and owned bytes zeroize.
/// Explicit borrowing is required for storage; no automatic serialization trait.
#[derive(Clone)]
pub struct ScramSha256Record(Zeroizing<[u8; SCRAM_SHA256_RECORD_BYTES]>);

impl ScramSha256Record {
    /// Borrow sensitive bytes while the zeroizing record remains alive.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }
}

impl fmt::Debug for ScramSha256Record {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScramSha256Record")
            .finish_non_exhaustive()
    }
}

impl ZeroizeOnDrop for ScramSha256Record {}

fn validate_iterations(iterations: u32) -> EngineResult<NonZeroU32> {
    if !(MIN_SCRAM_SHA256_ITERATIONS..=MAX_SCRAM_SHA256_ITERATIONS).contains(&iterations) {
        return Err(invalid(
            "SCRAM iteration count is outside the supported bounds",
        ));
    }
    Ok(NonZeroU32::new(iterations).expect("positive bounded iteration count"))
}

fn normalize_password(password: &str) -> EngineResult<Zeroizing<String>> {
    if password.is_empty() || password.len() > MAX_SCRAM_PASSWORD_BYTES {
        return Err(invalid("SCRAM password length is invalid"));
    }
    let normalized = Zeroizing::new(
        stringprep::saslprep(password)
            .map_err(|_| invalid("SCRAM password normalization failed"))?
            .into_owned(),
    );
    if normalized.is_empty() || normalized.len() > MAX_SCRAM_PASSWORD_BYTES {
        return Err(invalid("SCRAM normalized password length is invalid"));
    }
    Ok(normalized)
}

fn mac(key: &[u8], bytes: &[u8]) -> [u8; 32] {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), bytes)
        .as_ref()
        .try_into()
        .expect("HMAC-SHA-256 size")
}

fn invalid(message: &'static str) -> EngineError {
    EngineError::new(EngineErrorKind::InvalidArgument, message)
}

fn authentication_failed() -> EngineError {
    EngineError::new(
        EngineErrorKind::PermissionDenied,
        "SCRAM authentication failed",
    )
}

#[cfg(test)]
mod tests;
