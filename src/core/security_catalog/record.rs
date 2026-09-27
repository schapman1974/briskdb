//! Explicit versioned encoding for a trusted storage layer; never a wire format.

use super::*;
use crate::core::{
    authentication::SCRAM_SHA256_RECORD_BYTES,
    authorization::{
        DataDomain, MAX_POLICY_PRIVILEGES, Privilege, ResourceKind, Scope, ScopeValue,
    },
};
use zeroize::{ZeroizeOnDrop, Zeroizing};

/// Whole-record admission limit, checked before hashing, parsing or allocation.
pub const MAX_SECURITY_CATALOG_RECORD_BYTES: usize = 128 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"BRKSEC01";
const CHECKSUM_DOMAIN: &[u8] = b"briskdb.security-catalog.record.v1\0";
const CHECKSUM_BYTES: usize = 32;
const MIN_RECORD_BYTES: usize = 8 + 8 + 2 + 2 + CHECKSUM_BYTES;

/// Sensitive verifier-bearing snapshot with redacted Debug and zeroizing storage.
/// The checksum detects accidental corruption, **not forgery or rollback**. This
/// record is not encrypted. The storage layer must protect access, root binding,
/// atomic publication, freshness and downgrade behavior before enabling security.
pub struct SecurityCatalogRecord(Zeroizing<Vec<u8>>);

impl SecurityCatalogRecord {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl ZeroizeOnDrop for SecurityCatalogRecord {}

impl fmt::Debug for SecurityCatalogRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecurityCatalogRecord")
            .field("byte_count", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl SecurityCatalog {
    /// Export a complete deterministic versioned snapshot; performs no file I/O.
    /// A count-only pass permits a single allocation before writing secret bytes.
    pub fn to_record(&self) -> EngineResult<SecurityCatalogRecord> {
        let mut count = Writer {
            bytes: None,
            len: 0,
        };
        encode(self, &mut count)?;
        let capacity = count
            .len
            .checked_add(CHECKSUM_BYTES)
            .filter(|size| *size <= MAX_SECURITY_CATALOG_RECORD_BYTES)
            .ok_or_else(limit)?;
        let mut writer = Writer {
            bytes: Some(Zeroizing::new(Vec::with_capacity(capacity))),
            len: 0,
        };
        encode(self, &mut writer)?;
        let mut bytes = writer.bytes.expect("record writer allocated");
        let digest = checksum(&bytes);
        bytes.extend_from_slice(digest.as_bytes());
        debug_assert_eq!(bytes.len(), capacity);
        Ok(SecurityCatalogRecord(bytes))
    }

    /// Fully validate a bounded record before returning a new catalog incarnation.
    /// Every previously issued principal/attempt remains invalid in the restored
    /// catalog. The input is sensitive and its caller-owned buffer is not erased.
    pub fn from_record(bytes: &[u8]) -> EngineResult<Self> {
        if !(MIN_RECORD_BYTES..=MAX_SECURITY_CATALOG_RECORD_BYTES).contains(&bytes.len()) {
            return Err(invalid_record());
        }
        let (payload, digest) = bytes.split_at(bytes.len() - CHECKSUM_BYTES);
        if !payload.starts_with(MAGIC) || checksum(payload).as_bytes() != digest {
            return Err(invalid_record());
        }
        decode(payload).map_err(|_| invalid_record())
    }
}

fn checksum(bytes: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHECKSUM_DOMAIN);
    hasher.update(bytes);
    hasher.finalize()
}

struct Writer {
    bytes: Option<Zeroizing<Vec<u8>>>,
    len: usize,
}

impl Writer {
    fn put(&mut self, bytes: &[u8]) -> EngineResult<()> {
        let next = self
            .len
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_SECURITY_CATALOG_RECORD_BYTES - CHECKSUM_BYTES)
            .ok_or_else(limit)?;
        if let Some(output) = &mut self.bytes {
            // Never reallocate a buffer after writing credentials into it.
            if next > output.capacity() {
                return Err(limit());
            }
            output.extend_from_slice(bytes);
        }
        self.len = next;
        Ok(())
    }
    fn byte(&mut self, value: u8) -> EngineResult<()> {
        self.put(&[value])
    }
    fn short(&mut self, value: usize) -> EngineResult<()> {
        self.put(&u16::try_from(value).map_err(|_| limit())?.to_be_bytes())
    }
    fn long(&mut self, value: u64) -> EngineResult<()> {
        self.put(&value.to_be_bytes())
    }
    fn text(&mut self, value: &str) -> EngineResult<()> {
        self.short(value.len())?;
        self.put(value.as_bytes())
    }
    fn name(&mut self, name: &SecurityName) -> EngineResult<()> {
        self.text(name.realm())?;
        self.text(name.name())
    }
}

fn encode(catalog: &SecurityCatalog, writer: &mut Writer) -> EngineResult<()> {
    writer.put(MAGIC)?;
    writer.long(catalog.next_user_id)?;
    writer.short(catalog.roles.len())?;
    let mut role_indices = BTreeMap::new();
    for (index, (name, policy)) in catalog.roles.iter().enumerate() {
        role_indices.insert(name, index);
        writer.name(name)?;
        writer.short(policy.privilege_count())?;
        for privilege in policy.privileges() {
            writer.text(privilege.action().code())?;
            encode_scope(privilege.scope(), writer)?;
        }
    }
    writer.short(catalog.users.len())?;
    for (name, user) in &catalog.users {
        writer.name(name)?;
        writer.long(user.id)?;
        writer.long(user.credential_generation)?;
        writer.put(user.verifier.to_record().as_bytes())?;
        writer.short(user.roles.len())?;
        for role in &user.roles {
            writer.short(*role_indices.get(role).ok_or_else(invalid_record)?)?;
        }
    }
    Ok(())
}

fn encode_domain(domain: DataDomain, writer: &mut Writer) -> EngineResult<()> {
    writer.byte(match domain {
        DataDomain::Relational => 1,
        DataDomain::Document => 2,
    })
}

fn encode_resource(resource: &Resource, writer: &mut Writer) -> EngineResult<()> {
    match resource.kind() {
        ResourceKind::Server => writer.byte(1),
        ResourceKind::SecurityRealm => {
            writer.byte(2)?;
            writer.text(resource.realm_name().expect("realm resource"))
        }
        ResourceKind::DataDomain => {
            writer.byte(3)?;
            encode_domain(resource.domain().expect("domain resource"), writer)
        }
        ResourceKind::Database => {
            writer.byte(4)?;
            encode_domain(resource.domain().expect("database domain"), writer)?;
            writer.text(resource.database_name().expect("database resource"))
        }
        ResourceKind::Object => {
            writer.byte(5)?;
            encode_domain(resource.domain().expect("object domain"), writer)?;
            writer.text(resource.database_name().expect("object database"))?;
            writer.text(resource.object_name().expect("object name"))
        }
    }
}

fn encode_scope(scope: &Scope, writer: &mut Writer) -> EngineResult<()> {
    match scope.stored_value() {
        ScopeValue::Exact(resource) => {
            writer.byte(1)?;
            encode_resource(resource, writer)
        }
        ScopeValue::Database(domain, database) => {
            writer.byte(2)?;
            encode_domain(*domain, writer)?;
            writer.text(database)
        }
        ScopeValue::AllDatabases(domain) => {
            writer.byte(3)?;
            encode_domain(*domain, writer)
        }
        ScopeValue::AllSecurityRealms => writer.byte(4),
    }
}

struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> EngineResult<&'a [u8]> {
        if len > self.remaining.len() {
            return Err(invalid_record());
        }
        let (value, rest) = self.remaining.split_at(len);
        self.remaining = rest;
        Ok(value)
    }
    fn byte(&mut self) -> EngineResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn short(&mut self) -> EngineResult<usize> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().expect("length checked")) as usize)
    }
    fn count(&mut self, maximum: usize) -> EngineResult<usize> {
        let count = self.short()?;
        if count > maximum {
            return Err(invalid_record());
        }
        Ok(count)
    }
    fn long(&mut self) -> EngineResult<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
    fn text(&mut self, maximum: usize) -> EngineResult<&'a str> {
        let len = self.count(maximum)?;
        std::str::from_utf8(self.take(len)?).map_err(|_| invalid_record())
    }
    fn name(&mut self) -> EngineResult<SecurityName> {
        SecurityName::new(self.text(63)?, self.text(MAX_SECURITY_NAME_BYTES)?)
    }
    fn domain(&mut self) -> EngineResult<DataDomain> {
        match self.byte()? {
            1 => Ok(DataDomain::Relational),
            2 => Ok(DataDomain::Document),
            _ => Err(invalid_record()),
        }
    }
    fn resource(&mut self) -> EngineResult<Resource> {
        match self.byte()? {
            1 => Ok(Resource::server()),
            2 => Resource::security_realm(self.text(63)?),
            3 => Ok(Resource::data_domain(self.domain()?)),
            4 => Resource::database(self.domain()?, self.text(63)?),
            5 => Resource::object(self.domain()?, self.text(63)?, self.text(253)?),
            _ => Err(invalid_record()),
        }
    }
    fn scope(&mut self) -> EngineResult<Scope> {
        match self.byte()? {
            1 => Ok(Scope::exact(self.resource()?)),
            2 => Scope::database(self.domain()?, self.text(63)?),
            3 => Ok(Scope::all_databases(self.domain()?)),
            4 => Ok(Scope::all_security_realms()),
            _ => Err(invalid_record()),
        }
    }
    fn policy(&mut self) -> EngineResult<Policy> {
        let count = self.count(MAX_POLICY_PRIVILEGES)?;
        let mut grants = BTreeSet::new();
        for _ in 0..count {
            let action = Action::from_code(self.text(64)?)?;
            let grant = Privilege::new(action, self.scope()?)?;
            if !grants.insert(grant) {
                return Err(invalid_record());
            }
        }
        Policy::new(grants)
    }
}

fn decode(payload: &[u8]) -> EngineResult<SecurityCatalog> {
    let mut reader = Reader { remaining: payload };
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(invalid_record());
    }
    let next_user_id = reader.long()?;
    if next_user_id == 0 {
        return Err(invalid_record());
    }
    let mut catalog = SecurityCatalog::new();
    catalog.next_user_id = next_user_id;
    let role_count = reader.count(MAX_SECURITY_ROLES)?;
    let mut role_names = Vec::with_capacity(role_count);
    for _ in 0..role_count {
        let name = reader.name()?;
        let policy = reader.policy()?;
        catalog.create_role(name.clone(), policy)?;
        role_names.push(name);
    }
    let user_count = reader.count(MAX_SECURITY_USERS)?;
    let mut user_ids = BTreeSet::new();
    for _ in 0..user_count {
        let name = reader.name()?;
        let id = reader.long()?;
        let credential_generation = reader.long()?;
        if id == 0
            || id >= next_user_id
            || credential_generation == 0
            || !user_ids.insert(id)
            || catalog.users.contains_key(&name)
        {
            return Err(invalid_record());
        }
        let verifier = ScramSha256Verifier::from_record(reader.take(SCRAM_SHA256_RECORD_BYTES)?)?;
        let count = reader.count(MAX_POLICY_ROLES)?;
        let mut roles = BTreeSet::new();
        for _ in 0..count {
            let role = role_names.get(reader.short()?).ok_or_else(invalid_record)?;
            if !roles.insert(role.clone()) {
                return Err(invalid_record());
            }
        }
        let roles = catalog.validate_roles(roles)?;
        catalog.users.insert(
            name,
            User {
                id,
                credential_generation,
                verifier,
                roles,
            },
        );
    }
    if !reader.remaining.is_empty() {
        return Err(invalid_record());
    }
    Ok(catalog)
}

fn invalid_record() -> EngineError {
    error(
        EngineErrorKind::InvalidArgument,
        "security catalog record is invalid",
    )
}

#[cfg(test)]
mod tests;
