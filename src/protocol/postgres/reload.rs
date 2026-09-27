//! Publish a complete validated identity, never independent TLS/auth fields.

use std::{fmt, sync::Arc};
use tokio::sync::watch;

use super::LoadedSecurity;

#[derive(Clone)]
pub(crate) struct ReloadableSecurity {
    current: watch::Sender<Arc<LoadedSecurity>>,
}

impl ReloadableSecurity {
    pub(crate) fn new(security: LoadedSecurity) -> Self {
        let (current, _) = watch::channel(Arc::new(security));
        Self { current }
    }

    pub(crate) fn snapshot(&self) -> Arc<LoadedSecurity> {
        // Clone under the short read lock; never hold a watch borrow over await.
        self.current.borrow().clone()
    }

    pub(crate) fn replace(&self, security: LoadedSecurity) {
        // send_replace retains the value even with no receivers. Old snapshots
        // stay valid for connections already admitted before this publication.
        drop(self.current.send_replace(Arc::new(security)));
    }
}

impl fmt::Debug for ReloadableSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReloadableSecurity")
            .finish_non_exhaustive()
    }
}
