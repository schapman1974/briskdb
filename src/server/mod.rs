//! Server process assembly and listener lifecycle.

use std::{net::SocketAddr, path::PathBuf};

use anyhow::Context;
use tracing::info;

use crate::{core::Engine, protocol::http};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub shards: u16,
}

pub async fn run(config: Config) -> anyhow::Result<()> {
    let engine = Engine::open(&config.data_dir, config.shards).await?;
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind {}", config.listen))?;

    info!(
        listen = %config.listen,
        data_dir = %config.data_dir.display(),
        shards = engine.shard_count(),
        "BriskDB is ready"
    );

    axum::serve(listener, http::router_with_engine(engine)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_shard_count_fails_before_listener_startup() {
        let temp = tempfile::tempdir().unwrap();
        let error = run(Config {
            listen: "127.0.0.1:0".parse().unwrap(),
            data_dir: temp.path().to_path_buf(),
            shards: 1,
        })
        .await
        .unwrap_err();

        assert_eq!(error.to_string(), "shard count must be between 2 and 64");
    }
}
