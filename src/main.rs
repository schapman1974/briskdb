use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use briskdb::{api, storage::Database};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Address on which the HTTP server listens.
    #[arg(long, env = "BRISKDB_LISTEN", default_value = "127.0.0.1:7654")]
    listen: SocketAddr,

    /// Directory containing the manifest and shard files.
    #[arg(long, env = "BRISKDB_DATA_DIR", default_value = "./briskdb-data")]
    data_dir: PathBuf,

    /// Fixed shard count for a new database (2-64).
    #[arg(long, env = "BRISKDB_SHARDS", default_value_t = 4)]
    shards: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("briskdb=info")),
        )
        .init();

    let args = Args::parse();
    let database = Arc::new(Database::open(&args.data_dir, args.shards)?);
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;

    info!(
        listen = %args.listen,
        data_dir = %args.data_dir.display(),
        shards = database.shard_count(),
        "BriskDB is ready"
    );

    axum::serve(listener, api::router(database)).await?;
    Ok(())
}
