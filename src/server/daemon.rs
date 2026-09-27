//! Explicit process options and serialized signal-driven security replacement.

use super::*;
use crate::{CancellationToken, EngineError, EngineErrorKind, EngineState, RequestContext};

/// Optional process-owned listener behavior. Defaults preserve legacy startup;
/// no Mongo listener, TLS identity or SIGHUP handler is enabled implicitly.
#[derive(Default, Clone)]
pub struct DaemonOptions {
    pub(super) http_tls: Option<HttpTlsConfig>,
    pub(super) admin_tls: Option<HttpTlsConfig>,
    pub(super) address: Option<SocketAddr>,
    #[cfg(feature = "mongo-tls")]
    pub(super) tls: Option<crate::protocol::mongo::MongoTlsConfig>,
    pub(super) reload_on_sighup: bool,
}

impl DaemonOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Encrypt the data plane without changing its loopback-only policy.
    #[must_use]
    pub fn with_http_tls(mut self, config: HttpTlsConfig) -> Self {
        self.http_tls = Some(config);
        self
    }

    /// Encrypt an explicitly enabled administration plane independently.
    #[must_use]
    pub fn with_admin_tls(mut self, config: HttpTlsConfig) -> Self {
        self.admin_tls = Some(config);
        self
    }

    #[cfg(feature = "mongo")]
    #[must_use]
    pub fn with_mongo(mut self, address: SocketAddr) -> Self {
        self.address = Some(address);
        self
    }

    /// TLS still requires loopback; changing the address does not remove TLS.
    #[cfg(feature = "mongo-tls")]
    #[must_use]
    pub fn with_mongo_tls(
        mut self,
        address: SocketAddr,
        tls: crate::protocol::mongo::MongoTlsConfig,
    ) -> Self {
        self.address = Some(address);
        self.tls = Some(tls);
        self
    }

    /// On Unix, SIGHUP rereads the startup certificate/key/password paths of
    /// configured secure listeners. This is not session revocation or a watcher.
    /// Startup rejects this option without a secure identity or on non-Unix.
    #[must_use]
    pub fn with_sighup_reload(mut self) -> Self {
        self.reload_on_sighup = true;
        self
    }

    pub(super) fn validate_reload(&self, postgres_secure: bool) -> anyhow::Result<()> {
        if !self.reload_on_sighup {
            return Ok(());
        }
        if !cfg!(unix) {
            anyhow::bail!("SIGHUP security reload requires a Unix target");
        }
        #[cfg(feature = "mongo-tls")]
        let mongo_secure = self.tls.is_some();
        #[cfg(not(feature = "mongo-tls"))]
        let mongo_secure = false;
        anyhow::ensure!(
            postgres_secure || mongo_secure || self.http_tls.is_some() || self.admin_tls.is_some(),
            "SIGHUP security reload requires at least one already-secure listener"
        );
        Ok(())
    }
}

impl std::fmt::Debug for DaemonOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("DaemonOptions");
        debug
            .field("http_tls", &self.http_tls.is_some())
            .field("admin_tls", &self.admin_tls.is_some())
            .field("mongo_address", &self.address)
            .field("reload_on_sighup", &self.reload_on_sighup);
        #[cfg(feature = "mongo-tls")]
        debug.field("mongo_tls", &self.tls.is_some());
        debug.finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(super) struct Sources {
    pub http: Option<HttpTlsConfig>,
    pub admin: Option<HttpTlsConfig>,
    pub postgres: Option<postgres::SecurityConfig>,
    #[cfg(feature = "mongo-tls")]
    pub mongo: Option<crate::protocol::mongo::MongoTlsConfig>,
}

impl Sources {
    fn load(self) -> anyhow::Result<Loaded> {
        // Prepare every configured identity before publishing any replacement.
        // This worker owns only paths/configuration, never publication targets.
        Ok(Loaded {
            postgres: self.postgres.map(|config| config.load()).transpose()?,
            #[cfg(feature = "mongo-tls")]
            mongo: self.mongo.map(|config| config.load()).transpose()?,
            http: self.http.map(HttpTlsConfig::load).transpose()?,
            admin: self.admin.map(HttpTlsConfig::load).transpose()?,
        })
    }
}

pub(super) struct Targets {
    pub http: Option<http_tls::Reloadable>,
    pub admin: Option<http_tls::Reloadable>,
    pub postgres: Option<postgres::ReloadableSecurity>,
    #[cfg(feature = "mongo-tls")]
    pub mongo: Option<crate::protocol::mongo::ReloadableTls>,
}

struct Loaded {
    http: Option<Arc<http_tls::Loaded>>,
    admin: Option<Arc<http_tls::Loaded>>,
    postgres: Option<postgres::LoadedSecurity>,
    #[cfg(feature = "mongo-tls")]
    mongo: Option<crate::protocol::mongo::LoadedTls>,
}

pub(super) struct Reloader {
    #[cfg(unix)]
    signal: Option<tokio::signal::unix::Signal>,
    sources: Sources,
    targets: Targets,
}

impl Reloader {
    pub fn prepare(enabled: bool, sources: Sources, targets: Targets) -> anyhow::Result<Self> {
        #[cfg(unix)]
        let signal = enabled
            .then(|| tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()))
            .transpose()
            .context("failed to install SIGHUP security reload handler")?;
        #[cfg(not(unix))]
        anyhow::ensure!(!enabled, "SIGHUP security reload requires a Unix target");
        Ok(Self {
            #[cfg(unix)]
            signal,
            sources,
            targets,
        })
    }

    async fn next(&mut self) -> Option<()> {
        #[cfg(unix)]
        if let Some(signal) = &mut self.signal {
            return signal.recv().await;
        }
        std::future::pending().await
    }

    pub async fn run(mut self, engine: &Engine, shutdown: &CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                signal = self.next() => if signal.is_none() {
                    warn!(reason = "signal_stream_closed", "security reload disabled; active identities unchanged");
                    return;
                },
            }
            let context = RequestContext::new()
                .with_timeout(Duration::from_secs(15))
                .expect("fixed positive reload deadline");
            if self.targets.check(engine, shutdown, &context).is_err() {
                return;
            }
            let sources = self.sources.clone();
            let mut worker = tokio::task::spawn_blocking(move || sources.load());
            let _ = self
                .targets
                .finish_attempt(engine, shutdown, context, &mut worker)
                .await;
        }
    }
}

impl Targets {
    async fn finish_attempt(
        &self,
        engine: &Engine,
        shutdown: &CancellationToken,
        context: RequestContext,
        worker: &mut JoinHandle<anyhow::Result<Loaded>>,
    ) -> anyhow::Result<()> {
        let result = self
            .publish_after(engine, shutdown, context, async {
                (&mut *worker)
                    .await
                    .context("security reload worker failed")?
            })
            .await;
        match &result {
            Ok(()) => info!("listener security reloaded"),
            Err(error) => warn!(
                reason = failure_reason(error),
                "listener security reload rejected; active identities unchanged"
            ),
        }
        // A timed-out blocking file read cannot be forcibly interrupted.
        // Retain its handle, discard its result, and never queue a second
        // worker until it completes. Shutdown may stop waiting immediately.
        if !worker.is_finished() {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {},
                _ = &mut *worker => {},
            }
        }
        result
    }

    async fn publish_after(
        &self,
        engine: &Engine,
        shutdown: &CancellationToken,
        context: RequestContext,
        prepare: impl Future<Output = anyhow::Result<Loaded>>,
    ) -> anyhow::Result<()> {
        self.check(engine, shutdown, &context)?;
        let cancelled = context.cancellation_token();
        let deadline = async {
            match context.deadline() {
                Some(deadline) => {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                }
                None => std::future::pending().await,
            }
        };
        let loaded = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(security_reload_cancelled("Listener security").into()),
            _ = cancelled.cancelled() => return Err(security_reload_cancelled("Listener security").into()),
            _ = deadline => return Err(security_reload_timed_out("Listener security").into()),
            result = prepare => result?,
        };
        self.check(engine, shutdown, &context)?;
        anyhow::ensure!(
            loaded.postgres.is_some() == self.postgres.is_some(),
            "PostgreSQL reload target mismatch"
        );
        #[cfg(feature = "mongo-tls")]
        anyhow::ensure!(
            loaded.mongo.is_some() == self.mongo.is_some(),
            "Mongo reload target mismatch"
        );
        anyhow::ensure!(
            loaded.http.is_some() == self.http.is_some(),
            "HTTP data reload target mismatch"
        );
        anyhow::ensure!(
            loaded.admin.is_some() == self.admin.is_some(),
            "HTTP admin reload target mismatch"
        );
        // Each connector publishes one whole immutable identity. This is not a
        // cross-connector transaction or an atomic multi-file deployment.
        if let (Some(target), Some(identity)) = (&self.postgres, loaded.postgres) {
            target.replace(identity);
        }
        #[cfg(feature = "mongo-tls")]
        if let (Some(target), Some(identity)) = (&self.mongo, loaded.mongo) {
            target.replace(identity);
        }
        if let (Some(target), Some(identity)) = (&self.http, loaded.http) {
            target.replace(identity);
        }
        if let (Some(target), Some(identity)) = (&self.admin, loaded.admin) {
            target.replace(identity);
        }
        Ok(())
    }

    fn check(
        &self,
        engine: &Engine,
        shutdown: &CancellationToken,
        context: &RequestContext,
    ) -> anyhow::Result<()> {
        check_security_reload_context(context, "Listener security")?;
        if shutdown.is_cancelled() {
            return Err(security_reload_cancelled("Listener security").into());
        }
        anyhow::ensure!(
            engine.state() == EngineState::Running,
            "security reload requires a running engine"
        );
        Ok(())
    }
}

fn failure_reason(error: &anyhow::Error) -> &'static str {
    match error.downcast_ref::<EngineError>().map(EngineError::kind) {
        Some(EngineErrorKind::Cancelled) => "cancelled",
        Some(EngineErrorKind::DeadlineExceeded) => "deadline_exceeded",
        _ => "invalid_configuration_or_lifecycle",
    }
}

#[cfg(test)]
mod tests;
