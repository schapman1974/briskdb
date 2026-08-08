use std::{net::SocketAddr, path::PathBuf, time::Duration};

use briskdb::{
    core::{
        DEFAULT_CONNECTIONS_PER_SHARD, DEFAULT_MAX_PORTALS_PER_SESSION,
        DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION, DEFAULT_MAX_RESULT_BYTES,
        DEFAULT_MAX_RESULT_ROWS, DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES,
        DEFAULT_QUEUE_CAPACITY_PER_SHARD, DEFAULT_REQUEST_TIMEOUT_MS, DEFAULT_SHUTDOWN_GRACE_MS,
        EngineOptions, EngineResult, PreparedStatementLimits, ResultLimits,
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

    /// Maximum rows materialized by one query (1-1000000).
    #[arg(
        long,
        env = "BRISKDB_MAX_RESULT_ROWS",
        default_value_t = DEFAULT_MAX_RESULT_ROWS
    )]
    max_result_rows: u64,

    /// Maximum protocol-neutral logical bytes materialized by one query (1-1 GiB).
    #[arg(
        long,
        env = "BRISKDB_MAX_RESULT_BYTES",
        default_value_t = DEFAULT_MAX_RESULT_BYTES
    )]
    max_result_bytes: u64,

    /// Maximum prepared statements retained by one session (1-1024).
    #[arg(
        long,
        env = "BRISKDB_MAX_PREPARED_STATEMENTS_PER_SESSION",
        default_value_t = DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION
    )]
    max_prepared_statements_per_session: usize,

    /// Maximum bound portals retained by one session (1-1024).
    #[arg(
        long,
        env = "BRISKDB_MAX_PORTALS_PER_SESSION",
        default_value_t = DEFAULT_MAX_PORTALS_PER_SESSION
    )]
    max_portals_per_session: usize,

    /// Maximum retained portal and per-bind route/marker planning bytes (1-1 GiB).
    #[arg(
        long,
        env = "BRISKDB_MAX_RETAINED_BOUND_VALUE_BYTES",
        default_value_t = DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES
    )]
    max_retained_bound_value_bytes: u64,

    /// Engine request timeout in milliseconds; zero disables the default deadline.
    #[arg(
        long,
        env = "BRISKDB_REQUEST_TIMEOUT_MS",
        default_value_t = DEFAULT_REQUEST_TIMEOUT_MS
    )]
    request_timeout_ms: u64,

    /// Graceful-shutdown drain period in milliseconds.
    #[arg(
        long,
        env = "BRISKDB_SHUTDOWN_GRACE_MS",
        default_value_t = DEFAULT_SHUTDOWN_GRACE_MS
    )]
    shutdown_grace_ms: u64,
}

impl Args {
    /// Convert command-line input into validated server startup configuration.
    ///
    /// Keeping this conversion ahead of `server::run_with_engine_options`
    /// ensures invalid limits cannot bind a listener or create database files.
    fn into_server_parts(self) -> EngineResult<(Config, EngineOptions)> {
        let result_limits = ResultLimits::new(self.max_result_rows, self.max_result_bytes)?;
        let prepared_statement_limits = PreparedStatementLimits::new(
            self.max_prepared_statements_per_session,
            self.max_portals_per_session,
            self.max_retained_bound_value_bytes,
        )?;
        let request_timeout =
            (self.request_timeout_ms != 0).then(|| Duration::from_millis(self.request_timeout_ms));
        let options =
            EngineOptions::new(self.connections_per_shard, self.queue_capacity_per_shard)?
                .with_result_limits(result_limits)
                .with_prepared_statement_limits(prepared_statement_limits)
                .with_request_timeout(request_timeout)?
                .with_shutdown_grace(Duration::from_millis(self.shutdown_grace_ms))?;
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
        assert_eq!(args.max_result_rows, DEFAULT_MAX_RESULT_ROWS);
        assert_eq!(args.max_result_bytes, DEFAULT_MAX_RESULT_BYTES);
        assert_eq!(
            args.max_prepared_statements_per_session,
            DEFAULT_MAX_PREPARED_STATEMENTS_PER_SESSION
        );
        assert_eq!(
            args.max_portals_per_session,
            DEFAULT_MAX_PORTALS_PER_SESSION
        );
        assert_eq!(
            args.max_retained_bound_value_bytes,
            DEFAULT_MAX_RETAINED_BOUND_VALUE_BYTES
        );
        assert_eq!(args.request_timeout_ms, DEFAULT_REQUEST_TIMEOUT_MS);
        assert_eq!(args.shutdown_grace_ms, DEFAULT_SHUTDOWN_GRACE_MS);
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
            "--max-result-rows",
            "321",
            "--max-result-bytes",
            "654321",
            "--max-prepared-statements-per-session",
            "43",
            "--max-portals-per-session",
            "47",
            "--max-retained-bound-value-bytes",
            "7654321",
            "--request-timeout-ms",
            "2500",
            "--shutdown-grace-ms",
            "4000",
        ])
        .unwrap();

        assert_eq!(args.listen, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("/tmp/briskdb-test-data"));
        assert_eq!(args.shards, 8);
        assert_eq!(args.connections_per_shard, 3);
        assert_eq!(args.queue_capacity_per_shard, 17);
        assert_eq!(args.max_result_rows, 321);
        assert_eq!(args.max_result_bytes, 654_321);
        assert_eq!(args.max_prepared_statements_per_session, 43);
        assert_eq!(args.max_portals_per_session, 47);
        assert_eq!(args.max_retained_bound_value_bytes, 7_654_321);
        assert_eq!(args.request_timeout_ms, 2_500);
        assert_eq!(args.shutdown_grace_ms, 4_000);
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
            "--max-result-rows",
            "321",
            "--max-result-bytes",
            "654321",
            "--max-prepared-statements-per-session",
            "43",
            "--max-portals-per-session",
            "47",
            "--max-retained-bound-value-bytes",
            "7654321",
            "--request-timeout-ms",
            "2500",
            "--shutdown-grace-ms",
            "4000",
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
        assert_eq!(
            options.result_limits(),
            ResultLimits::new(321, 654_321).unwrap()
        );
        assert_eq!(
            options.prepared_statement_limits(),
            PreparedStatementLimits::new(43, 47, 7_654_321).unwrap()
        );
        assert_eq!(
            options.request_timeout(),
            Some(Duration::from_millis(2_500))
        );
        assert_eq!(options.shutdown_grace(), Duration::from_millis(4_000));
    }

    #[test]
    fn invalid_cli_limits_fail_during_startup_conversion() {
        for arguments in [
            vec![
                "briskdb",
                "--connections-per-shard",
                "0",
                "--queue-capacity-per-shard",
                "1",
            ],
            vec![
                "briskdb",
                "--connections-per-shard",
                "1",
                "--queue-capacity-per-shard",
                "1025",
            ],
            vec!["briskdb", "--max-result-rows", "0"],
            vec!["briskdb", "--max-result-rows", "1000001"],
            vec!["briskdb", "--max-result-bytes", "0"],
            vec!["briskdb", "--max-result-bytes", "1073741825"],
            vec!["briskdb", "--max-prepared-statements-per-session", "0"],
            vec!["briskdb", "--max-prepared-statements-per-session", "1025"],
            vec!["briskdb", "--max-portals-per-session", "0"],
            vec!["briskdb", "--max-portals-per-session", "1025"],
            vec!["briskdb", "--max-retained-bound-value-bytes", "0"],
            vec!["briskdb", "--max-retained-bound-value-bytes", "1073741825"],
            vec!["briskdb", "--request-timeout-ms", "86400001"],
            vec!["briskdb", "--shutdown-grace-ms", "0"],
            vec!["briskdb", "--shutdown-grace-ms", "86400001"],
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
    fn zero_request_timeout_explicitly_disables_only_the_engine_default() {
        let args = Args::try_parse_from(["briskdb", "--request-timeout-ms", "0"]).unwrap();
        let (_, options) = args.into_server_parts().unwrap();
        assert_eq!(options.request_timeout(), None);
    }

    #[test]
    fn resource_flags_are_bound_to_the_documented_environment_variables() {
        let command = Args::command();
        let connections = command
            .get_arguments()
            .find(|argument| argument.get_id() == "connections_per_shard")
            .unwrap();
        let queue = command
            .get_arguments()
            .find(|argument| argument.get_id() == "queue_capacity_per_shard")
            .unwrap();
        let rows = command
            .get_arguments()
            .find(|argument| argument.get_id() == "max_result_rows")
            .unwrap();
        let bytes = command
            .get_arguments()
            .find(|argument| argument.get_id() == "max_result_bytes")
            .unwrap();
        let prepared_statements = command
            .get_arguments()
            .find(|argument| argument.get_id() == "max_prepared_statements_per_session")
            .unwrap();
        let portals = command
            .get_arguments()
            .find(|argument| argument.get_id() == "max_portals_per_session")
            .unwrap();
        let retained_bound_value_bytes = command
            .get_arguments()
            .find(|argument| argument.get_id() == "max_retained_bound_value_bytes")
            .unwrap();
        let timeout = command
            .get_arguments()
            .find(|argument| argument.get_id() == "request_timeout_ms")
            .unwrap();
        let shutdown = command
            .get_arguments()
            .find(|argument| argument.get_id() == "shutdown_grace_ms")
            .unwrap();

        assert_eq!(
            connections.get_env(),
            Some(OsStr::new("BRISKDB_CONNECTIONS_PER_SHARD"))
        );
        assert_eq!(
            queue.get_env(),
            Some(OsStr::new("BRISKDB_QUEUE_CAPACITY_PER_SHARD"))
        );
        assert_eq!(rows.get_env(), Some(OsStr::new("BRISKDB_MAX_RESULT_ROWS")));
        assert_eq!(
            bytes.get_env(),
            Some(OsStr::new("BRISKDB_MAX_RESULT_BYTES"))
        );
        assert_eq!(
            prepared_statements.get_env(),
            Some(OsStr::new("BRISKDB_MAX_PREPARED_STATEMENTS_PER_SESSION"))
        );
        assert_eq!(
            portals.get_env(),
            Some(OsStr::new("BRISKDB_MAX_PORTALS_PER_SESSION"))
        );
        assert_eq!(
            retained_bound_value_bytes.get_env(),
            Some(OsStr::new("BRISKDB_MAX_RETAINED_BOUND_VALUE_BYTES"))
        );
        assert_eq!(
            timeout.get_env(),
            Some(OsStr::new("BRISKDB_REQUEST_TIMEOUT_MS"))
        );
        assert_eq!(
            shutdown.get_env(),
            Some(OsStr::new("BRISKDB_SHUTDOWN_GRACE_MS"))
        );
    }

    #[test]
    fn pool_limits_parse_from_environment_in_an_isolated_process() {
        const CHILD_MARKER: &str = "BRISKDB_POOL_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_MARKER).is_some() {
            let args = Args::try_parse_from(["briskdb"]).unwrap();
            assert_eq!(args.connections_per_shard, 6);
            assert_eq!(args.queue_capacity_per_shard, 41);
            assert_eq!(args.max_result_rows, 500);
            assert_eq!(args.max_result_bytes, 65_536);
            assert_eq!(args.max_prepared_statements_per_session, 29);
            assert_eq!(args.max_portals_per_session, 31);
            assert_eq!(args.max_retained_bound_value_bytes, 98_304);
            assert_eq!(args.request_timeout_ms, 1_250);
            assert_eq!(args.shutdown_grace_ms, 2_750);
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
            .env("BRISKDB_MAX_RESULT_ROWS", "500")
            .env("BRISKDB_MAX_RESULT_BYTES", "65536")
            .env("BRISKDB_MAX_PREPARED_STATEMENTS_PER_SESSION", "29")
            .env("BRISKDB_MAX_PORTALS_PER_SESSION", "31")
            .env("BRISKDB_MAX_RETAINED_BOUND_VALUE_BYTES", "98304")
            .env("BRISKDB_REQUEST_TIMEOUT_MS", "1250")
            .env("BRISKDB_SHUTDOWN_GRACE_MS", "2750")
            .status()
            .unwrap();

        assert!(status.success());
    }
}
