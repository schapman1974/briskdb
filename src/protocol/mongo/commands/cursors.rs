//! Wire cursor IDs are capabilities scoped to this listener and namespace.
//! A dedicated engine session follows each cursor across pooled TCP sockets.
//! Disconnect cleanup follows the last socket to use it.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use super::super::metrics;
use super::{CommandError, Result};
use crate::{
    core::Session,
    document::{DocumentCursorId, DocumentNamespace},
};

const IDLE_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_CONNECTION_CURSORS: usize = 8;
const MAX_WIRE_CURSORS: usize = 32;

struct Entry {
    namespace: DocumentNamespace,
    session: Arc<Session>,
    connection: u64,
    touched: Instant,
    remaining: Option<Duration>,
    in_use: bool,
    metrics: metrics::CursorGuard,
}

#[derive(Default)]
pub(super) struct WireCursors(
    Mutex<BTreeMap<DocumentCursorId, Entry>>,
    Arc<metrics::Metrics>,
);

impl WireCursors {
    pub(super) fn new(metrics: Arc<metrics::Metrics>) -> Self {
        Self(Mutex::new(BTreeMap::new()), metrics)
    }

    pub fn register(
        &self,
        id: DocumentCursorId,
        namespace: DocumentNamespace,
        session: Arc<Session>,
        connection: u64,
        remaining: Option<Duration>,
    ) -> Result<()> {
        let mut entries = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut entries);
        if entries.len() >= MAX_WIRE_CURSORS
            || entries
                .values()
                .filter(|entry| entry.connection == connection)
                .count()
                >= MAX_CONNECTION_CURSORS
        {
            self.1.cursor_rejected();
            return Err(CommandError::new(
                10334,
                "BSONObjectTooLarge",
                "open cursor resource limit exceeded",
            ));
        }
        entries.insert(
            id,
            Entry {
                namespace,
                session,
                connection,
                touched: Instant::now(),
                remaining,
                in_use: false,
                metrics: self.1.register_cursor(),
            },
        );
        Ok(())
    }

    pub fn lookup(
        self: &Arc<Self>,
        id: DocumentCursorId,
        namespace: &DocumentNamespace,
        connection: u64,
    ) -> Result<WireCursorLease> {
        let mut entries = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut entries);
        let previous = entries
            .get(&id)
            .filter(|entry| &entry.namespace == namespace)
            .ok_or_else(|| CommandError::new(43, "CursorNotFound", "cursor not found"))?
            .connection;
        if previous != connection
            && entries
                .values()
                .filter(|entry| entry.connection == connection)
                .count()
                >= MAX_CONNECTION_CURSORS
        {
            self.1.cursor_rejected();
            return Err(CommandError::new(
                10334,
                "BSONObjectTooLarge",
                "open cursor resource limit exceeded",
            ));
        }
        let entry = entries
            .get_mut(&id)
            .filter(|entry| &entry.namespace == namespace)
            .ok_or_else(|| CommandError::new(43, "CursorNotFound", "cursor not found"))?;
        if entry.in_use {
            return Err(CommandError::new(
                237,
                "CursorInUse",
                "cursor is already in use",
            ));
        }
        if entry.remaining.is_some_and(|remaining| remaining.is_zero()) {
            entries.remove(&id);
            return Err(CommandError::new(
                50,
                "MaxTimeMSExpired",
                "cursor time budget exhausted",
            ));
        }
        entry.connection = connection;
        entry.touched = Instant::now();
        entry.in_use = true;
        Ok(WireCursorLease {
            registry: Arc::clone(self),
            id,
            session: Arc::clone(&entry.session),
            remaining: entry.remaining,
            completed: false,
        })
    }

    pub fn take(
        &self,
        id: DocumentCursorId,
        namespace: &DocumentNamespace,
    ) -> Option<Arc<Session>> {
        let mut entries = self.0.lock().unwrap_or_else(|error| error.into_inner());
        prune(&mut entries);
        if entries
            .get(&id)
            .is_some_and(|entry| &entry.namespace == namespace)
        {
            entries.remove(&id).map(|entry| entry.session)
        } else {
            None
        }
    }

    pub fn discard(&self, id: DocumentCursorId) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&id);
    }

    pub fn prune(&self) {
        prune(&mut self.0.lock().unwrap_or_else(|error| error.into_inner()));
    }

    pub fn connection(self: &Arc<Self>, owner: u64) -> ConnectionCursors {
        ConnectionCursors {
            registry: Arc::clone(self),
            owner,
        }
    }
}

fn prune(entries: &mut BTreeMap<DocumentCursorId, Entry>) {
    entries.retain(|_, entry| {
        let keep = entry.in_use || entry.touched.elapsed() < IDLE_TIMEOUT;
        if !keep {
            entry.metrics.expired();
        }
        keep
    });
}

pub(super) struct WireCursorLease {
    registry: Arc<WireCursors>,
    id: DocumentCursorId,
    pub session: Arc<Session>,
    pub remaining: Option<Duration>,
    completed: bool,
}

impl WireCursorLease {
    pub fn complete(mut self, elapsed: Duration, retained: bool) -> Result<()> {
        let mut entries = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.completed = true;
        if retained {
            let entry = entries.get_mut(&self.id).ok_or_else(|| {
                CommandError::new(43, "CursorNotFound", "cursor was closed during the request")
            })?;
            entry.remaining = entry
                .remaining
                .map(|remaining| remaining.saturating_sub(elapsed));
            entry.touched = Instant::now();
            entry.in_use = false;
        } else {
            entries.remove(&self.id);
        }
        Ok(())
    }
}

impl Drop for WireCursorLease {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.discard(self.id);
        }
    }
}

pub(crate) struct ConnectionCursors {
    registry: Arc<WireCursors>,
    owner: u64,
}

impl Drop for ConnectionCursors {
    fn drop(&mut self) {
        self.registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, entry| entry.connection != self.owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::PreparedStatementLimits;

    fn namespace() -> DocumentNamespace {
        DocumentNamespace::new("db", "items").unwrap()
    }

    fn register(
        registry: &WireCursors,
        value: u64,
        owner: u64,
        budget: Option<Duration>,
    ) -> DocumentCursorId {
        let id = DocumentCursorId::new(value).unwrap();
        registry
            .register(
                id,
                namespace(),
                Arc::new(Session::new(1, PreparedStatementLimits::default())),
                owner,
                budget,
            )
            .unwrap();
        id
    }

    fn error_code(result: Result<WireCursorLease>) -> i32 {
        match result {
            Err(error) => error.code,
            Ok(_) => panic!("expected rejected continuation"),
        }
    }

    fn assert_counts(metrics: &metrics::Metrics, expected: (u64, u64, u64, u64, u64, u64)) {
        let cursors = metrics.snapshot().cursors;
        assert_eq!(
            (
                cursors.registered,
                cursors.closed,
                cursors.active,
                cursors.peak,
                cursors.idle_expired,
                cursors.limit_rejections
            ),
            expected
        );
    }

    #[test]
    fn cumulative_budget_busy_and_abandoned_leases_are_bounded() {
        let registry = Arc::new(WireCursors::default());
        let id = register(&registry, 1, 10, Some(Duration::from_millis(30)));
        let lease = registry.lookup(id, &namespace(), 10).unwrap();
        assert_eq!(error_code(registry.lookup(id, &namespace(), 20)), 237);
        lease.complete(Duration::from_millis(10), true).unwrap();
        // Client idle time does not consume the execution-time budget.
        registry.0.lock().unwrap().get_mut(&id).unwrap().touched =
            Instant::now() - Duration::from_secs(30);
        let lease = registry.lookup(id, &namespace(), 20).unwrap();
        assert_eq!(lease.remaining, Some(Duration::from_millis(20)));
        lease.complete(Duration::from_millis(21), true).unwrap();
        assert_eq!(error_code(registry.lookup(id, &namespace(), 20)), 50);
        assert_eq!(error_code(registry.lookup(id, &namespace(), 20)), 43);

        let id = register(&registry, 2, 10, None);
        drop(registry.lookup(id, &namespace(), 10).unwrap());
        assert_eq!(error_code(registry.lookup(id, &namespace(), 10)), 43);
        let id = register(&registry, 3, 10, None);
        registry
            .lookup(id, &namespace(), 10)
            .unwrap()
            .complete(Duration::ZERO, false)
            .unwrap();
        assert!(registry.0.lock().unwrap().is_empty());
        assert_counts(&registry.1, (3, 3, 0, 1, 0, 0));
    }

    #[test]
    fn handoff_disconnect_expiry_and_namespace_preserve_ownership() {
        let registry = Arc::new(WireCursors::default());
        let first = registry.connection(10);
        let second = registry.connection(20);
        let id = register(&registry, 1, 10, None);
        let wrong = DocumentNamespace::new("db", "wrong").unwrap();
        assert_eq!(error_code(registry.lookup(id, &wrong, 20)), 43);
        assert!(registry.take(id, &wrong).is_none());
        registry
            .lookup(id, &namespace(), 20)
            .unwrap()
            .complete(Duration::ZERO, true)
            .unwrap();
        drop(first);
        assert!(registry.0.lock().unwrap().contains_key(&id));
        drop(second);
        assert_eq!(error_code(registry.lookup(id, &namespace(), 20)), 43);

        let id = register(&registry, 2, 10, None);
        registry.0.lock().unwrap().get_mut(&id).unwrap().touched = Instant::now() - IDLE_TIMEOUT;
        registry.prune();
        assert_eq!(error_code(registry.lookup(id, &namespace(), 10)), 43);
        let id = register(&registry, 3, 10, None);
        let lease = registry.lookup(id, &namespace(), 10).unwrap();
        registry.0.lock().unwrap().get_mut(&id).unwrap().touched = Instant::now() - IDLE_TIMEOUT;
        registry.prune();
        assert!(registry.0.lock().unwrap().contains_key(&id));
        registry.discard(id);
        assert_eq!(lease.complete(Duration::ZERO, true).unwrap_err().code, 43);
        assert_counts(&registry.1, (3, 3, 0, 1, 1, 0));
    }

    #[test]
    fn handoff_respects_receiving_connection_quota_without_losing_cursor() {
        let registry = Arc::new(WireCursors::default());
        for value in 1..=8 {
            register(&registry, value, 10, None);
        }
        let id = register(&registry, 9, 20, None);
        assert_eq!(error_code(registry.lookup(id, &namespace(), 10)), 10334);
        registry
            .lookup(id, &namespace(), 20)
            .unwrap()
            .complete(Duration::ZERO, true)
            .unwrap();
        registry.discard(DocumentCursorId::new(1).unwrap());
        registry
            .lookup(id, &namespace(), 10)
            .unwrap()
            .complete(Duration::ZERO, true)
            .unwrap();
        assert_eq!(registry.0.lock().unwrap().get(&id).unwrap().connection, 10);
        assert_counts(&registry.1, (9, 1, 8, 9, 0, 1));
        let metrics = Arc::clone(&registry.1);
        drop(registry);
        assert_counts(&metrics, (9, 9, 0, 9, 0, 1));
    }

    #[test]
    fn capacity_rejections_do_not_register_and_registry_drop_drains_all_cursors() {
        for owners in [1, 4] {
            let metrics = Arc::new(metrics::Metrics::default());
            let registry = WireCursors::new(Arc::clone(&metrics));
            let total = owners * 8;
            for value in 1..=total {
                register(&registry, value, (value - 1) / 8, None);
            }
            let rejected = registry.register(
                DocumentCursorId::new(total + 1).unwrap(),
                namespace(),
                Arc::new(Session::new(1, PreparedStatementLimits::default())),
                if owners == 1 { 0 } else { 4 },
                None,
            );
            assert_eq!(rejected.unwrap_err().code, 10334);
            assert_counts(&metrics, (total, 0, total, total, 0, 1));
            drop(registry);
            assert_counts(&metrics, (total, total, 0, total, 0, 1));
        }
    }

    #[test]
    fn explicit_take_closes_once_without_retaining_session_or_registry() {
        let metrics = Arc::new(metrics::Metrics::default());
        let registry = Arc::new(WireCursors::new(Arc::clone(&metrics)));
        let id = register(&registry, 1, 10, None);
        let session = registry.take(id, &namespace()).unwrap();
        let weak_session = Arc::downgrade(&session);
        assert_counts(&metrics, (1, 1, 0, 1, 0, 0));
        assert!(registry.take(id, &namespace()).is_none());
        registry.discard(id);
        drop(session);
        let weak_registry = Arc::downgrade(&registry);
        drop(registry);
        assert!(weak_session.upgrade().is_none());
        assert!(weak_registry.upgrade().is_none());
        assert_counts(&metrics, (1, 1, 0, 1, 0, 0));
    }
}
