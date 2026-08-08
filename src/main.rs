use std::{net::SocketAddr, path::PathBuf};

use briskdb::server::{self, Config};
use clap::Parser;
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
    server::run(Config {
        listen: args.listen,
        data_dir: args.data_dir,
        shards: args.shards,
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_are_preserved() {
        let args = Args::try_parse_from(["briskdb"]).unwrap();

        assert_eq!(args.listen, "127.0.0.1:7654".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("./briskdb-data"));
        assert_eq!(args.shards, 4);
    }

    #[test]
    fn cli_flags_are_preserved() {
        let args = Args::try_parse_from([
            "briskdb",
            "--listen",
            "127.0.0.1:9000",
            "--data-dir",
            "/tmp/briskdb-test-data",
            "--shards",
            "8",
        ])
        .unwrap();

        assert_eq!(args.listen, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("/tmp/briskdb-test-data"));
        assert_eq!(args.shards, 8);
    }
}
