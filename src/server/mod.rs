//! Server process assembly and listener lifecycle.

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use tracing::info;

use crate::{
    core::{Engine, EngineOptions},
    protocol::http,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub shards: u16,
}

pub async fn run(config: Config) -> anyhow::Result<()> {
    run_with_engine_options(config, EngineOptions::default()).await
}

/// Start the server with explicit bounded-worker and per-shard pool limits.
///
/// `options` is validated before any database files or listener are created.
/// [`run`] remains the compatibility entry point and delegates here with
/// [`EngineOptions::default`].
pub async fn run_with_engine_options(config: Config, options: EngineOptions) -> anyhow::Result<()> {
    let engine = Engine::open_with_options(&config.data_dir, config.shards, options).await?;
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind {}", config.listen))?;

    info!(
        listen = %config.listen,
        data_dir = %config.data_dir.display(),
        shards = engine.shard_count(),
        max_blocking_workers = engine.options().connections_per_shard()
            * usize::from(engine.shard_count()),
        connections_per_shard = engine.options().connections_per_shard(),
        queue_capacity_per_shard = engine.options().queue_capacity_per_shard(),
        "BriskDB is ready"
    );

    axum::serve(listener, http::router_with_engine(engine)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unavailable_address() -> (std::net::TcpListener, SocketAddr) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        (listener, address)
    }

    #[tokio::test]
    async fn default_entry_point_validates_before_database_or_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("database");
        let (_occupied_listener, listen) = unavailable_address();
        let error = run(Config {
            listen,
            data_dir: data_dir.clone(),
            shards: 1,
        })
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "shard count must be between 2 and 64");
        assert!(!data_dir.exists());
    }

    #[tokio::test]
    async fn aggregate_pool_limit_fails_before_database_or_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("database");
        let (_occupied_listener, listen) = unavailable_address();
        let options = EngineOptions::new(16, 1).unwrap();
        let error = run_with_engine_options(
            Config {
                listen,
                data_dir: data_dir.clone(),
                shards: 64,
            },
            options,
        )
        .await
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must contain between 1 and 512 total active connections")
        );
        assert!(!data_dir.exists());
    }
}
