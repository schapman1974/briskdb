use std::{net::SocketAddr, path::PathBuf};

use briskdb::{
    core::{
        DEFAULT_CONNECTIONS_PER_SHARD, DEFAULT_QUEUE_CAPACITY_PER_SHARD, EngineOptions,
        EngineResult,
    },
    server::{self, Config},
};
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

    /// Maximum active SQLite connections for each shard (1-16).
    #[arg(
        long,
        env = "BRISKDB_CONNECTIONS_PER_SHARD",
        default_value_t = DEFAULT_CONNECTIONS_PER_SHARD
    )]
    connections_per_shard: usize,

    /// Maximum queued operations waiting for each shard (1-1024).
    #[arg(
        long,
        env = "BRISKDB_QUEUE_CAPACITY_PER_SHARD",
        default_value_t = DEFAULT_QUEUE_CAPACITY_PER_SHARD
    )]
    queue_capacity_per_shard: usize,
}

impl Args {
    /// Convert command-line input into validated server startup configuration.
    ///
    /// Keeping this conversion ahead of `server::run_with_engine_options`
    /// ensures invalid limits cannot bind a listener or create database files.
    fn into_server_parts(self) -> EngineResult<(Config, EngineOptions)> {
        let options =
            EngineOptions::new(self.connections_per_shard, self.queue_capacity_per_shard)?;
        let config = Config {
            listen: self.listen,
            data_dir: self.data_dir,
            shards: self.shards,
        };
        Ok((config, options))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("briskdb=info")),
        )
        .init();

    let (config, options) = Args::parse().into_server_parts()?;
    server::run_with_engine_options(config, options).await
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_defaults_are_preserved() {
        let args = Args::try_parse_from(["briskdb"]).unwrap();

        assert_eq!(args.listen, "127.0.0.1:7654".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("./briskdb-data"));
        assert_eq!(args.shards, 4);
        assert_eq!(args.connections_per_shard, DEFAULT_CONNECTIONS_PER_SHARD);
        assert_eq!(
            args.queue_capacity_per_shard,
            DEFAULT_QUEUE_CAPACITY_PER_SHARD
        );
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
            "--connections-per-shard",
            "3",
            "--queue-capacity-per-shard",
            "17",
        ])
        .unwrap();

        assert_eq!(args.listen, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("/tmp/briskdb-test-data"));
        assert_eq!(args.shards, 8);
        assert_eq!(args.connections_per_shard, 3);
        assert_eq!(args.queue_capacity_per_shard, 17);
    }

    #[test]
    fn cli_values_convert_to_server_config_and_validated_options() {
        let args = Args::try_parse_from([
            "briskdb",
            "--listen",
            "127.0.0.1:9000",
            "--data-dir",
            "/tmp/briskdb-test-data",
            "--shards",
            "8",
            "--connections-per-shard",
            "3",
            "--queue-capacity-per-shard",
            "17",
        ])
        .unwrap();

        let (config, options) = args.into_server_parts().unwrap();
        assert_eq!(
            config,
            Config {
                listen: "127.0.0.1:9000".parse().unwrap(),
                data_dir: PathBuf::from("/tmp/briskdb-test-data"),
                shards: 8,
            }
        );
        assert_eq!(options.connections_per_shard(), 3);
        assert_eq!(options.queue_capacity_per_shard(), 17);
    }

    #[test]
    fn invalid_cli_limits_fail_during_startup_conversion() {
        for arguments in [
            [
                "briskdb",
                "--connections-per-shard",
                "0",
                "--queue-capacity-per-shard",
                "1",
            ],
            [
                "briskdb",
                "--connections-per-shard",
                "1",
                "--queue-capacity-per-shard",
                "1025",
            ],
        ] {
            let error = Args::try_parse_from(arguments)
                .unwrap()
                .into_server_parts()
                .unwrap_err();
            assert_eq!(
                error.kind(),
                briskdb::core::EngineErrorKind::InvalidArgument
            );
        }
    }

    #[test]
    fn pool_flags_are_bound_to_the_documented_environment_variables() {
        let command = Args::command();
        let connections = command
            .get_arguments()
            .find(|argument| argument.get_id() == "connections_per_shard")
            .unwrap();
        let queue = command
            .get_arguments()
            .find(|argument| argument.get_id() == "queue_capacity_per_shard")
            .unwrap();

        assert_eq!(
            connections.get_env(),
            Some(OsStr::new("BRISKDB_CONNECTIONS_PER_SHARD"))
        );
        assert_eq!(
            queue.get_env(),
            Some(OsStr::new("BRISKDB_QUEUE_CAPACITY_PER_SHARD"))
        );
    }

    #[test]
    fn pool_limits_parse_from_environment_in_an_isolated_process() {
        const CHILD_MARKER: &str = "BRISKDB_POOL_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_MARKER).is_some() {
            let args = Args::try_parse_from(["briskdb"]).unwrap();
            assert_eq!(args.connections_per_shard, 6);
            assert_eq!(args.queue_capacity_per_shard, 41);
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::pool_limits_parse_from_environment_in_an_isolated_process",
            ])
            .env(CHILD_MARKER, "1")
            .env("BRISKDB_LISTEN", "127.0.0.1:7654")
            .env("BRISKDB_DATA_DIR", "./briskdb-env-test-data")
            .env("BRISKDB_SHARDS", "4")
            .env("BRISKDB_CONNECTIONS_PER_SHARD", "6")
            .env("BRISKDB_QUEUE_CAPACITY_PER_SHARD", "41")
            .status()
            .unwrap();

        assert!(status.success());
    }
}
