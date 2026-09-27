//! Caller-controlled publication; blocking loaders never own the publication handle.

use super::*;
use crate::{RequestContext, protocol::mongo::tls::LoadedTls};

impl MongoServer {
    /// Replace an already-encrypted listener's certificate/key and handshake budget.
    /// Newly admitted sockets use the new immutable identity. Established sockets
    /// and pending handshakes retain their original identity and budget.
    /// Invalid input or a dropped reload future before publication changes nothing.
    /// Concurrent successful reloads publish in completion order. This is not
    /// client authentication, session revocation or permission to bind remotely.
    pub async fn reload_tls(&self, config: super::super::MongoTlsConfig) -> io::Result<()> {
        self.reload_tls_with_context(config, RequestContext::new())
            .await
    }

    /// Cancellation/deadline apply to preparation and are rechecked immediately
    /// before publication. Query result limits do not apply. A worker may finish
    /// loading after cancellation but cannot publish; published identities cannot
    /// be undone by a later cancellation.
    pub async fn reload_tls_with_context(
        &self,
        config: super::super::MongoTlsConfig,
        context: RequestContext,
    ) -> io::Result<()> {
        self.reload_tls_with(context, async move {
            tokio::task::spawn_blocking(move || config.load())
                .await
                .map_err(|_| io::Error::other("Mongo TLS reload worker failed"))?
        })
        .await
    }

    async fn reload_tls_with(
        &self,
        context: RequestContext,
        prepare: impl std::future::Future<Output = io::Result<LoadedTls>>,
    ) -> io::Result<()> {
        check_context(&context)?;
        let target = self
            .tls
            .as_ref()
            .ok_or_else(|| invalid("Mongo TLS reload requires an already-encrypted listener"))?;
        self.require_running_for_tls_reload()?;
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
            _ = self.shutdown.cancelled() => return Err(invalid("Mongo TLS listener is closing")),
            _ = cancellation.cancelled() => return Err(cancelled()),
            _ = deadline => return Err(timed_out()),
            result = prepare => result?,
        };
        self.require_running_for_tls_reload()?;
        check_context(&context)?;
        target.replace(loaded);
        Ok(())
    }

    fn require_running_for_tls_reload(&self) -> io::Result<()> {
        if self.health.state(self.shutdown.is_cancelled())
            != super::super::MongoListenerState::Running
            || self.task.as_ref().is_none_or(JoinHandle::is_finished)
            || self
                .engine_readiness
                .snapshot()
                .is_none_or(|engine| engine.lifecycle_state() != EngineState::Running)
        {
            return Err(invalid(
                "Mongo TLS reload requires a running listener and engine",
            ));
        }
        Ok(())
    }
}

fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "Mongo TLS reload was cancelled")
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "Mongo TLS reload deadline elapsed")
}

fn check_context(context: &RequestContext) -> io::Result<()> {
    if context.cancellation_token().is_cancelled() {
        return Err(cancelled());
    }
    if context
        .deadline()
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(timed_out());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
