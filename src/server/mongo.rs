//! Coordination of the optional Mongo listener with the established server.

use std::{future::Future, net::SocketAddr};

use anyhow::Context;
use tokio::sync::oneshot;

use crate::{
    CancellationToken, DocumentSupport, EngineState, core::Engine, protocol::mongo::MongoServer,
};

use super::ListenerConfig;

#[cfg(test)]
mod tests;

pub(super) fn validate_address(config: &ListenerConfig, address: SocketAddr) -> anyhow::Result<()> {
    if !address.ip().is_loopback() {
        anyhow::bail!(
            "unauthenticated Mongo startup requires a loopback listen address; received {address}"
        );
    }
    if address.port() != 0
        && [
            Some(config.http_listen),
            config.admin_listen,
            config.postgres_listen,
        ]
        .into_iter()
        .flatten()
        .any(|other| other == address)
    {
        anyhow::bail!(
            "Mongo and other listeners require distinct addresses; {address} is already configured"
        );
    }
    Ok(())
}

pub(super) fn validate_database(database: &crate::BriskDb) -> anyhow::Result<()> {
    if database.document_support() != DocumentSupport::Enabled {
        anyhow::bail!("Mongo server assembly requires explicitly enabled document support");
    }
    if database.state() != EngineState::Running {
        anyhow::bail!("Mongo server assembly requires a running database");
    }
    Ok(())
}

pub(super) async fn coordinate<F>(
    mut mongo: MongoServer,
    primary: F,
    stop_primary: oneshot::Sender<()>,
    requested: CancellationToken,
    engine: Engine,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>> + Send,
{
    tokio::pin!(primary);
    tokio::select! {
        result = &mut primary => {
            let mongo_result = mongo.close().await;
            result?;
            mongo_result.context("Mongo listener shutdown failed")
        }
        result = mongo.wait() => {
            let expected = requested.is_cancelled() || engine.state() != EngineState::Running;
            let _ = stop_primary.send(());
            // Preserve the primary listener/core error if it caused the Mongo
            // health loop to exit. Always finish both cleanup paths first.
            primary.await?;
            result.context("Mongo listener failed")?;
            if !expected {
                anyhow::bail!("Mongo listener stopped unexpectedly");
            }
            Ok(())
        }
    }
}
