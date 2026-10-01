//! Dedicated authenticated Mongo process: no anonymous companion listeners.
mod cli_contention;

use briskdb::{
    core::EngineOptions,
    protocol::mongo::MongoTlsConfig,
    server::{AuthenticatedMongoConfig, run_authenticated_mongo},
};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Serve an already provisioned BriskDB security root over Mongo TLS/SCRAM only"
)]
struct Args {
    #[command(flatten)]
    contention: cli_contention::ContentionArgs,
    /// Existing security-bound root; never creates credentials or enables anonymous access.
    #[arg(long, env = "BRISKDB_DATA_DIR")]
    data_dir: PathBuf,
    /// Existing root's exact shard count.
    #[arg(long, env = "BRISKDB_SHARDS")]
    shards: u16,
    /// TLS Mongo address. Explicit non-loopback binding is supported.
    #[arg(long, env = "BRISKDB_MONGO_LISTEN", default_value = "127.0.0.1:27017")]
    mongo_listen: SocketAddr,
    /// PEM server certificate chain.
    #[arg(long, env = "BRISKDB_MONGO_TLS_CERT")]
    mongo_tls_cert: PathBuf,
    /// Owner-private PEM server key.
    #[arg(long, env = "BRISKDB_MONGO_TLS_KEY")]
    mongo_tls_key: PathBuf,
}

impl Args {
    fn into_parts(self) -> anyhow::Result<(AuthenticatedMongoConfig, EngineOptions)> {
        let options = EngineOptions::default().with_contention_policy(self.contention.policy()?);
        options.validate_for_shards(self.shards)?;
        Ok((
            AuthenticatedMongoConfig {
                data_dir: self.data_dir,
                shards: self.shards,
                listen: self.mongo_listen,
                tls: MongoTlsConfig::new(self.mongo_tls_cert, self.mongo_tls_key),
            },
            options,
        ))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("briskdb=info")),
        )
        .init();
    let (config, options) = Args::parse().into_parts()?;
    run_authenticated_mongo(config, options).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_explicit_root_shards_and_tls_and_rejects_companion_listeners() {
        let required = [
            "briskdb-mongo",
            "--data-dir",
            "root",
            "--shards",
            "2",
            "--mongo-tls-cert",
            "cert",
            "--mongo-tls-key",
            "key",
        ];
        let (config, _) = Args::try_parse_from(required)
            .unwrap()
            .into_parts()
            .unwrap();
        assert_eq!(config.listen, "127.0.0.1:27017".parse().unwrap());
        for option in [
            "--listen",
            "--admin-listen",
            "--postgres-listen",
            "--reload-on-sighup",
        ] {
            assert!(
                Args::try_parse_from(required.into_iter().chain([option, "127.0.0.1:9999"]))
                    .is_err()
            );
        }
        for index in [1, 3, 5, 7] {
            let without: Vec<_> = required
                .iter()
                .enumerate()
                .filter_map(|(i, value)| (!(index..index + 2).contains(&i)).then_some(*value))
                .collect();
            assert!(Args::try_parse_from(without).is_err());
        }
    }
}
