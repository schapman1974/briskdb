use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

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

const DEFAULT_POSTGRES_LISTEN: &str = "127.0.0.1:5433";

/// Command-line representation of an optional TCP listener.
///
/// Keeping the `disabled` sentinel at the process boundary means the public
/// server API can use `Option<SocketAddr>` without carrying CLI spelling into
/// Rust callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListenerSetting {
    Address(SocketAddr),
    Disabled,
}

impl ListenerSetting {
    const fn into_option(self) -> Option<SocketAddr> {
        match self {
            Self::Address(address) => Some(address),
            Self::Disabled => None,
        }
    }
}

impl FromStr for ListenerSetting {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "disabled" {
            return Ok(Self::Disabled);
        }

        value.parse::<SocketAddr>().map(Self::Address).map_err(|_| {
            format!("expected a socket address or the exact value 'disabled', got '{value}'")
        })
    }
}

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Address on which the HTTP server listens.
    #[arg(long, env = "BRISKDB_LISTEN", default_value = "127.0.0.1:7654")]
    listen: SocketAddr,

    /// Loopback PostgreSQL TCP listener address, or `disabled` to turn it off.
    #[arg(
        long,
        env = "BRISKDB_POSTGRES_LISTEN",
        default_value = DEFAULT_POSTGRES_LISTEN,
        value_name = "SOCKET_ADDR|disabled"
    )]
    postgres_listen: ListenerSetting,

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

    /// Route registered autocommit writes through the experimental virtual-table facade.
    #[cfg(feature = "experimental-vtab")]
    #[arg(
        long,
        env = "BRISKDB_EXPERIMENTAL_VTAB_WRITES",
        default_value_t = false
    )]
    experimental_vtab_writes: bool,
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
        #[cfg(feature = "experimental-vtab")]
        let options = options.with_experimental_vtab_writes(self.experimental_vtab_writes);
        let config = Config {
            listen: self.listen,
            postgres_listen: self.postgres_listen.into_option(),
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
        assert_eq!(
            args.postgres_listen,
            ListenerSetting::Address(DEFAULT_POSTGRES_LISTEN.parse().unwrap())
        );
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
        #[cfg(feature = "experimental-vtab")]
        assert!(!args.experimental_vtab_writes);
    }

    #[test]
    fn cli_flags_are_preserved() {
        let args = Args::try_parse_from([
            "briskdb",
            "--listen",
            "127.0.0.1:9000",
            "--postgres-listen",
            "127.0.0.1:9543",
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
        assert_eq!(
            args.postgres_listen,
            ListenerSetting::Address("127.0.0.1:9543".parse().unwrap())
        );
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
            "--postgres-listen",
            "127.0.0.1:9543",
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
                postgres_listen: Some("127.0.0.1:9543".parse().unwrap()),
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
        #[cfg(feature = "experimental-vtab")]
        assert!(!options.experimental_vtab_writes());
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn experimental_vtab_write_flag_is_forwarded_to_engine_options() {
        let args = Args::try_parse_from(["briskdb", "--experimental-vtab-writes"]).unwrap();
        assert!(args.experimental_vtab_writes);

        let (_, options) = args.into_server_parts().unwrap();
        assert!(options.experimental_vtab_writes());
    }

    #[cfg(not(feature = "experimental-vtab"))]
    #[test]
    fn experimental_vtab_write_flag_is_absent_without_the_cargo_feature() {
        assert!(Args::try_parse_from(["briskdb", "--experimental-vtab-writes"]).is_err());
        assert!(
            Args::command()
                .get_arguments()
                .all(|argument| argument.get_id() != "experimental_vtab_writes")
        );
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
    fn postgres_listener_can_be_disabled_explicitly() {
        let args = Args::try_parse_from(["briskdb", "--postgres-listen", "disabled"]).unwrap();
        assert_eq!(args.postgres_listen, ListenerSetting::Disabled);

        let (config, _) = args.into_server_parts().unwrap();
        assert_eq!(config.postgres_listen, None);
    }

    #[test]
    fn postgres_listener_accepts_ipv4_and_ipv6_socket_addresses() {
        for (input, expected) in [
            ("127.0.0.1:6543", "127.0.0.1:6543"),
            ("[::1]:6543", "[::1]:6543"),
        ] {
            let args = Args::try_parse_from(["briskdb", "--postgres-listen", input]).unwrap();
            assert_eq!(
                args.postgres_listen,
                ListenerSetting::Address(expected.parse().unwrap())
            );
        }
    }

    #[test]
    fn malformed_postgres_listener_values_fail_during_cli_parsing() {
        for value in [
            "",
            "off",
            "none",
            "DISABLED",
            "localhost:5433",
            "127.0.0.1",
            "127.0.0.1:not-a-port",
        ] {
            assert!(
                Args::try_parse_from(["briskdb", "--postgres-listen", value]).is_err(),
                "value should be rejected: {value:?}"
            );
        }
    }

    #[test]
    fn resource_flags_are_bound_to_the_documented_environment_variables() {
        let command = Args::command();
        let postgres_listener = command
            .get_arguments()
            .find(|argument| argument.get_id() == "postgres_listen")
            .unwrap();
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
        #[cfg(feature = "experimental-vtab")]
        let experimental_vtab_writes = command
            .get_arguments()
            .find(|argument| argument.get_id() == "experimental_vtab_writes")
            .unwrap();

        assert_eq!(
            postgres_listener.get_env(),
            Some(OsStr::new("BRISKDB_POSTGRES_LISTEN"))
        );
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
        #[cfg(feature = "experimental-vtab")]
        assert_eq!(
            experimental_vtab_writes.get_env(),
            Some(OsStr::new("BRISKDB_EXPERIMENTAL_VTAB_WRITES"))
        );
    }

    #[cfg(feature = "experimental-vtab")]
    #[test]
    fn experimental_vtab_writes_parse_from_environment_in_an_isolated_process() {
        const CHILD_MARKER: &str = "BRISKDB_VTAB_WRITE_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_MARKER).is_some() {
            let args = Args::try_parse_from(["briskdb"]).unwrap();
            assert!(args.experimental_vtab_writes);
            let (_, options) = args.into_server_parts().unwrap();
            assert!(options.experimental_vtab_writes());
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::experimental_vtab_writes_parse_from_environment_in_an_isolated_process",
            ])
            .env(CHILD_MARKER, "1")
            .env("BRISKDB_EXPERIMENTAL_VTAB_WRITES", "true")
            .status()
            .unwrap();

        assert!(status.success());
    }

    #[test]
    fn pool_limits_parse_from_environment_in_an_isolated_process() {
        const CHILD_MARKER: &str = "BRISKDB_POOL_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_MARKER).is_some() {
            let args = Args::try_parse_from(["briskdb"]).unwrap();
            assert_eq!(
                args.postgres_listen,
                ListenerSetting::Address("127.0.0.1:6543".parse().unwrap())
            );
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
            .env("BRISKDB_POSTGRES_LISTEN", "127.0.0.1:6543")
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

    #[test]
    fn postgres_listener_address_and_disabled_state_parse_from_environment() {
        const CHILD_MARKER: &str = "BRISKDB_POSTGRES_LISTENER_ENV_TEST_CHILD";

        if let Some(expected) = std::env::var_os(CHILD_MARKER) {
            let args = Args::try_parse_from(["briskdb"]).unwrap();
            match expected.to_str().unwrap() {
                "address" => assert_eq!(
                    args.postgres_listen,
                    ListenerSetting::Address("127.0.0.1:7543".parse().unwrap())
                ),
                "disabled" => assert_eq!(args.postgres_listen, ListenerSetting::Disabled),
                unexpected => panic!("unexpected child case {unexpected}"),
            }
            return;
        }

        for (value, expected) in [("127.0.0.1:7543", "address"), ("disabled", "disabled")] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::postgres_listener_address_and_disabled_state_parse_from_environment",
                ])
                .env(CHILD_MARKER, expected)
                .env("BRISKDB_POSTGRES_LISTEN", value)
                .status()
                .unwrap();
            assert!(status.success(), "environment case failed: {expected}");
        }
    }

    #[test]
    fn explicit_postgres_listener_cli_value_overrides_environment() {
        const CHILD_MARKER: &str = "BRISKDB_POSTGRES_LISTENER_PRECEDENCE_TEST_CHILD";

        if let Some(expected) = std::env::var_os(CHILD_MARKER) {
            match expected.to_str().unwrap() {
                "disabled" => {
                    let args =
                        Args::try_parse_from(["briskdb", "--postgres-listen", "disabled"]).unwrap();
                    assert_eq!(args.postgres_listen, ListenerSetting::Disabled);
                }
                "address" => {
                    let args =
                        Args::try_parse_from(["briskdb", "--postgres-listen", "127.0.0.1:9543"])
                            .unwrap();
                    assert_eq!(
                        args.postgres_listen,
                        ListenerSetting::Address("127.0.0.1:9543".parse().unwrap())
                    );
                }
                unexpected => panic!("unexpected child case {unexpected}"),
            }
            return;
        }

        for (environment, expected) in [("127.0.0.1:8543", "disabled"), ("disabled", "address")] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::explicit_postgres_listener_cli_value_overrides_environment",
                ])
                .env(CHILD_MARKER, expected)
                .env("BRISKDB_POSTGRES_LISTEN", environment)
                .status()
                .unwrap();
            assert!(status.success(), "precedence case failed: {expected}");
        }
    }

    #[test]
    fn malformed_postgres_listener_environment_value_is_rejected() {
        const CHILD_MARKER: &str = "BRISKDB_POSTGRES_LISTENER_BAD_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_MARKER).is_some() {
            assert!(Args::try_parse_from(["briskdb"]).is_err());
            return;
        }

        for value in ["", "localhost:5433"] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::malformed_postgres_listener_environment_value_is_rejected",
                ])
                .env(CHILD_MARKER, "1")
                .env("BRISKDB_POSTGRES_LISTEN", value)
                .status()
                .unwrap();
            assert!(status.success(), "environment value should fail: {value:?}");
        }
    }
}
