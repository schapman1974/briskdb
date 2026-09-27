//! Blocking preparation owns no publication handle; the attached host publishes.

use super::*;
use crate::server::{
    AttachedServer, check_security_reload_context, security_reload_cancelled,
    security_reload_timed_out,
};
use crate::{EngineState, RequestContext};
use anyhow::Context;
use std::future::Future;

pub(in crate::server) struct Target {
    identities: Planes,
    engine: crate::core::EngineReadinessProbe,
}

impl Target {
    pub(in crate::server) fn new(
        identities: Planes,
        engine: crate::core::EngineReadinessProbe,
    ) -> Self {
        Self { identities, engine }
    }
}

impl std::fmt::Debug for Target {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpTlsReloadTarget")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum Plane {
    Data,
    Admin,
}

impl Plane {
    fn operation(self) -> &'static str {
        match self {
            Self::Data => "HTTP data TLS",
            Self::Admin => "HTTP admin TLS",
        }
    }

    fn identity(self, target: &Target) -> Option<&Reloadable> {
        match self {
            Self::Data => target.identities.data.as_ref(),
            Self::Admin => target.identities.admin.as_ref(),
        }
    }
}

impl AttachedServer {
    /// Replace the already-encrypted data plane's certificate/key and handshake
    /// budget, without rebinding or changing HTTP authorization or routes.
    /// New admissions use the replacement; pending handshakes and established
    /// sockets retain their original generation. This is not session revocation.
    pub async fn reload_http_tls(&self, config: HttpTlsConfig) -> anyhow::Result<()> {
        self.reload_http_tls_with_context(config, RequestContext::new())
            .await
    }

    /// As `reload_http_tls`, with cancellation/deadline controls during preparation
    /// and immediately before publication. Query result limits do not apply.
    /// Concurrent successful reloads publish in completion order; cancellation
    /// cannot undo an identity already published. Preparation cannot publish late.
    pub async fn reload_http_tls_with_context(
        &self,
        config: HttpTlsConfig,
        context: RequestContext,
    ) -> anyhow::Result<()> {
        self.reload_http_plane(Plane::Data, config, context).await
    }

    /// Replace only the already-encrypted administration plane's identity.
    /// Data, PostgreSQL and Mongo identities are unaffected. This cannot enable
    /// a disabled plane or upgrade a plaintext plane to TLS.
    pub async fn reload_admin_tls(&self, config: HttpTlsConfig) -> anyhow::Result<()> {
        self.reload_admin_tls_with_context(config, RequestContext::new())
            .await
    }

    /// Administration-plane counterpart of `reload_http_tls_with_context`.
    pub async fn reload_admin_tls_with_context(
        &self,
        config: HttpTlsConfig,
        context: RequestContext,
    ) -> anyhow::Result<()> {
        self.reload_http_plane(Plane::Admin, config, context).await
    }

    async fn reload_http_plane(
        &self,
        plane: Plane,
        config: HttpTlsConfig,
        context: RequestContext,
    ) -> anyhow::Result<()> {
        self.reload_http_plane_with(plane, context, async move {
            tokio::task::spawn_blocking(move || config.load())
                .await
                .context("HTTP TLS reload worker failed")?
                .context("failed to reload HTTP TLS configuration")
        })
        .await
    }

    async fn reload_http_plane_with(
        &self,
        plane: Plane,
        context: RequestContext,
        prepare: impl Future<Output = anyhow::Result<Arc<Loaded>>>,
    ) -> anyhow::Result<()> {
        let operation = plane.operation();
        check_security_reload_context(&context, operation)?;
        let identity = plane.identity(&self.http_tls).with_context(|| {
            format!("{operation} reload requires an already-encrypted attached listener")
        })?;
        self.require_running_for_http_reload()?;
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
            _ = cancellation.cancelled() => return Err(security_reload_cancelled(operation).into()),
            _ = deadline => return Err(security_reload_timed_out(operation).into()),
            result = prepare => result?,
        };
        self.require_running_for_http_reload()?;
        check_security_reload_context(&context, operation)?;
        identity.replace(loaded);
        Ok(())
    }

    fn require_running_for_http_reload(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.shutdown.is_some()
                && !self.is_closed()
                && self
                    .http_tls
                    .engine
                    .snapshot()
                    .is_some_and(|engine| engine.lifecycle_state() == EngineState::Running),
            "HTTP TLS reload requires a running attached listener and engine"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
