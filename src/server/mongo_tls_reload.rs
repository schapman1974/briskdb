//! The attached handle observes its borrowed engine without retaining its pools.

use super::*;
use crate::{
    EngineState, RequestContext,
    protocol::mongo::{LoadedTls, MongoTlsConfig, ReloadableTls},
};

pub(super) struct Target {
    identity: ReloadableTls,
    engine: crate::core::EngineReadinessProbe,
}

impl Target {
    pub(super) fn new(identity: ReloadableTls, engine: crate::core::EngineReadinessProbe) -> Self {
        Self { identity, engine }
    }
}

impl std::fmt::Debug for Target {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MongoTlsReloadTarget")
            .finish_non_exhaustive()
    }
}

impl AttachedServer {
    /// Reload already-encrypted Mongo without rebinding or changing its loopback
    /// policy. Newly admitted sockets use the replacement identity and budget;
    /// existing sockets retain their original generation. Invalid/cancelled
    /// preparation never replaces the active identity. Not session revocation.
    pub async fn reload_mongo_tls(&self, config: MongoTlsConfig) -> anyhow::Result<()> {
        self.reload_mongo_tls_with_context(config, RequestContext::new())
            .await
    }

    /// Apply ordinary host cancellation/deadline controls before preparation,
    /// while waiting, and immediately before publication. Query result limits
    /// do not apply. Concurrent successful reloads publish in completion order;
    /// cancellation cannot undo an identity already published.
    pub async fn reload_mongo_tls_with_context(
        &self,
        config: MongoTlsConfig,
        context: RequestContext,
    ) -> anyhow::Result<()> {
        self.reload_mongo_tls_with(context, async move {
            tokio::task::spawn_blocking(move || config.load())
                .await
                .context("Mongo TLS reload worker failed")?
                .context("failed to reload Mongo TLS configuration")
        })
        .await
    }

    async fn reload_mongo_tls_with(
        &self,
        context: RequestContext,
        prepare: impl Future<Output = anyhow::Result<LoadedTls>>,
    ) -> anyhow::Result<()> {
        check_security_reload_context(&context, "Mongo TLS")?;
        let target = self
            .mongo_tls
            .as_ref()
            .context("Mongo TLS reload requires an already-encrypted attached Mongo listener")?;
        self.require_running_for_mongo_reload(target)?;
        let cancellation = context.cancellation_token();
        let deadline = async {
            if let Some(deadline) = context.deadline() {
                tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let loaded = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(security_reload_cancelled("Mongo TLS").into()),
            _ = deadline => return Err(security_reload_timed_out("Mongo TLS").into()),
            result = prepare => result?,
        };
        self.require_running_for_mongo_reload(target)?;
        check_security_reload_context(&context, "Mongo TLS")?;
        target.identity.replace(loaded);
        Ok(())
    }

    fn require_running_for_mongo_reload(&self, target: &Target) -> anyhow::Result<()> {
        if self.shutdown.is_none()
            || self.is_closed()
            || target
                .engine
                .snapshot()
                .is_none_or(|engine| engine.lifecycle_state() != EngineState::Running)
        {
            anyhow::bail!("Mongo TLS reload requires a running attached listener and engine");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
