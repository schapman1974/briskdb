//! Bounded, redacted active-connection metadata, separate from metric labels.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::document::{BsonDocument, BsonValue};

pub(super) const MAX_CONNECTIONS: usize = 8;

/// Untrusted driver identification, not authentication or a capability grant.
/// Unknown names are collapsed without retaining any client-supplied string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MongoDriverKind {
    PyMongo,
    PyMongoAsync,
    Other,
}

/// Redacted metadata from an active connection's first successful handshake.
/// No application, OS, platform, environment, peer address or arbitrary name is
/// retained. Versions are optional numeric triples, not free-form strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MongoClientMetadata {
    /// Listener-local engine session identifier; never an authentication ID.
    pub connection_id: u64,
    pub driver: MongoDriverKind,
    /// Present only for recognized driver names and exactly three u16 numbers.
    pub driver_version: Option<[u16; 3]>,
}

#[derive(Default)]
pub(super) struct Registry {
    records: Mutex<BTreeMap<u64, MongoClientMetadata>>,
}

impl Registry {
    pub(super) fn connection(self: &Arc<Self>, id: u64) -> Connection {
        Connection {
            registry: Arc::clone(self),
            id,
            observed: false,
        }
    }

    pub(super) fn snapshot(&self) -> Vec<MongoClientMetadata> {
        self.records
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .copied()
            .collect()
    }
}

pub(super) struct Connection {
    registry: Arc<Registry>,
    id: u64,
    observed: bool,
}

impl Connection {
    pub(super) fn observe(&mut self, request: &BsonDocument, reply: &BsonDocument) {
        if self.observed
            || !matches!(
                request.iter().next().map(|(key, _)| key),
                Some("hello" | "isMaster" | "ismaster")
            )
            || !matches!(reply.get_first("ok"), Some(BsonValue::Double(value)) if *value == 1.0)
        {
            return;
        }
        // Freeze even an absent client field: monitoring/repeated hellos cannot
        // replace or introduce metadata after the first successful handshake.
        self.observed = true;
        let Some(BsonValue::Document(client)) = request.get_first("client") else {
            return;
        };
        let mut record = MongoClientMetadata {
            connection_id: self.id,
            driver: MongoDriverKind::Other,
            driver_version: None,
        };
        if let Some(BsonValue::Document(driver)) = client.get_first("driver") {
            record.driver = match driver.get_first("name") {
                Some(BsonValue::String(name)) => match name.as_str() {
                    "PyMongo" | "PyMongo|c" => MongoDriverKind::PyMongo,
                    "PyMongo|async" | "PyMongo|c|async" => MongoDriverKind::PyMongoAsync,
                    _ => MongoDriverKind::Other,
                },
                _ => MongoDriverKind::Other,
            };
            if record.driver != MongoDriverKind::Other {
                if let Some(BsonValue::String(version)) = driver.get_first("version") {
                    record.driver_version = version_triple(version);
                }
            }
        }
        let mut records = self
            .registry
            .records
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Independent hard cap, in addition to listener admission. Metadata
        // collection never rejects an otherwise supported wire operation.
        if records.len() < MAX_CONNECTIONS {
            records.insert(self.id, record);
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.registry
            .records
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&self.id);
    }
}

fn version_triple(text: &str) -> Option<[u16; 3]> {
    if text.len() > 17 {
        return None;
    }
    let mut parts = text.split('.');
    let mut result = [0; 3];
    for value in &mut result {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        *value = part.parse().ok()?;
    }
    parts.next().is_none().then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(fields: impl IntoIterator<Item = (&'static str, BsonValue)>) -> BsonDocument {
        BsonDocument::from_entries(fields).unwrap()
    }

    fn hello(name: &str, version: &str) -> BsonDocument {
        doc([
            ("hello", BsonValue::Int32(1)),
            (
                "client",
                BsonValue::Document(doc([
                    (
                        "driver",
                        BsonValue::Document(doc([
                            ("name", BsonValue::from(name)),
                            ("version", BsonValue::from(version)),
                            ("private", BsonValue::from("secret-token")),
                        ])),
                    ),
                    ("application", BsonValue::from("secret-token")),
                    ("os", BsonValue::from("secret-token")),
                    ("platform", BsonValue::from("secret-token")),
                    ("env", BsonValue::from("secret-token")),
                ])),
            ),
        ])
    }

    #[test]
    fn metadata_is_allowlisted_bounded_and_removed_on_drop() {
        let registry = Arc::new(Registry::default());
        let success = doc([("ok", BsonValue::Double(1.0))]);
        let mut guards = Vec::new();
        for id in 1..=12 {
            let mut guard = registry.connection(id);
            guard.observe(&hello("PyMongo|c|async", "4.17.0"), &success);
            guards.push(guard);
        }
        let records = registry.snapshot();
        assert_eq!(records.len(), MAX_CONNECTIONS);
        assert!(
            records
                .iter()
                .all(|record| record.driver == MongoDriverKind::PyMongoAsync
                    && record.driver_version == Some([4, 17, 0]))
        );
        assert!(!format!("{records:?}").contains("secret-token"));
        drop(guards);
        assert!(registry.snapshot().is_empty());
    }

    #[test]
    fn only_first_successful_handshake_can_record_metadata() {
        let registry = Arc::new(Registry::default());
        let success = doc([("ok", BsonValue::Double(1.0))]);
        let mut guard = registry.connection(1);
        guard.observe(
            &hello("secret-token", "1.2.3"),
            &doc([("ok", BsonValue::Double(0.0))]),
        );
        assert!(registry.snapshot().is_empty());
        guard.observe(&hello("PyMongo", "4.17.0"), &success);
        guard.observe(&hello("secret-token", "1.2.3"), &success);
        assert_eq!(registry.snapshot()[0].driver, MongoDriverKind::PyMongo);
        let mut absent = registry.connection(2);
        absent.observe(&doc([("hello", BsonValue::Int32(1))]), &success);
        absent.observe(&hello("PyMongo", "4.17.0"), &success);
        assert_eq!(registry.snapshot().len(), 1);
        let mut unknown = registry.connection(3);
        unknown.observe(&hello("secret-token", "1.2.3"), &success);
        let record = registry.snapshot()[1];
        assert_eq!(record.driver, MongoDriverKind::Other);
        assert_eq!(record.driver_version, None);
        assert!(!format!("{record:?}").contains("secret-token"));
    }

    #[test]
    fn version_policy_cannot_retain_freeform_or_oversized_values() {
        for invalid in [
            "",
            "1.2",
            "1.2.3.4",
            "1.2.3-secret",
            "1.2.+3",
            "1.2.65536",
            "1.2.\n3",
            "1.2.٣",
            "123456789012345678",
        ] {
            assert_eq!(version_triple(invalid), None, "{invalid:?}");
        }
        assert_eq!(version_triple("65535.65535.65535"), Some([65535; 3]));
    }

    #[test]
    fn unwinding_releases_connection_metadata() {
        let registry = Arc::new(Registry::default());
        let inside = Arc::clone(&registry);
        assert!(
            std::panic::catch_unwind(move || {
                let mut guard = inside.connection(1);
                guard.observe(
                    &hello("PyMongo", "4.17.0"),
                    &doc([("ok", BsonValue::Double(1.0))]),
                );
                panic!("injected connection unwind");
            })
            .is_err()
        );
        assert!(registry.snapshot().is_empty());
    }
}
