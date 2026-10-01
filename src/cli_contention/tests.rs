use briskdb::core::{ContentionJitter, ContentionPolicy, EngineErrorKind};
use clap::Parser;
use std::time::Duration;

fn backoff() -> Vec<&'static str> {
    vec![
        "briskdb",
        "--contention-mode",
        "backoff",
        "--contention-initial-delay-ms",
        "2",
        "--contention-max-delay-ms",
        "50",
        "--contention-multiplier",
        "2",
        "--contention-jitter",
        "full",
        "--contention-max-retries",
        "20",
        "--contention-max-elapsed-ms",
        "1000",
    ]
}

#[test]
fn daemon_legacy_fail_fast_and_backoff_map_to_the_engine_policy() {
    let (_, options) = crate::Args::try_parse_from(["briskdb"])
        .unwrap()
        .into_server_parts()
        .unwrap();
    assert_eq!(options.contention_policy(), None);
    let (_, options) = crate::Args::try_parse_from(["briskdb", "--contention-mode", "fail-fast"])
        .unwrap()
        .into_server_parts()
        .unwrap();
    assert_eq!(
        options.contention_policy(),
        Some(ContentionPolicy::fail_fast())
    );
    let (_, options) = crate::Args::try_parse_from(backoff())
        .unwrap()
        .into_server_parts()
        .unwrap();
    assert_eq!(
        options.contention_policy(),
        Some(
            ContentionPolicy::new(
                Duration::from_millis(2),
                Duration::from_millis(50),
                2,
                ContentionJitter::Full,
                20,
                Duration::from_millis(1000),
            )
            .unwrap()
        )
    );
}

#[test]
fn partial_ignored_invalid_and_overflowing_settings_are_rejected_before_storage() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("must-not-exist");
    let cases = [
        vec!["briskdb", "--contention-mode", "backoff"],
        vec!["briskdb", "--contention-max-retries", "2"],
        vec![
            "briskdb",
            "--contention-mode",
            "fail-fast",
            "--contention-max-retries",
            "2",
        ],
    ];
    for mut args in cases {
        args.extend(["--data-dir", path.to_str().unwrap()]);
        assert_eq!(
            crate::Args::try_parse_from(args)
                .unwrap()
                .into_server_parts()
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );
        assert!(!path.exists());
    }
    for (flag, invalid) in [
        ("--contention-initial-delay-ms", "0"),
        ("--contention-initial-delay-ms", "51"),
        ("--contention-max-delay-ms", "1001"),
        ("--contention-multiplier", "0"),
        ("--contention-multiplier", "1025"),
        ("--contention-max-retries", "0"),
        ("--contention-max-retries", "1000001"),
        ("--contention-max-elapsed-ms", "86400001"),
        ("--contention-max-elapsed-ms", "18446744073709551615"),
    ] {
        let mut args = backoff();
        let position = args.iter().position(|value| *value == flag).unwrap() + 1;
        args[position] = invalid;
        args.extend(["--data-dir", path.to_str().unwrap()]);
        assert_eq!(
            crate::Args::try_parse_from(args)
                .unwrap()
                .into_server_parts()
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument,
            "{flag}={invalid}"
        );
        assert!(!path.exists());
    }
    assert!(crate::Args::try_parse_from(["briskdb", "--contention-mode", "unknown"]).is_err());
    assert!(crate::Args::try_parse_from(["briskdb", "--contention-jitter", "unknown"]).is_err());
}

#[test]
fn daemon_environment_and_cli_precedence_are_isolated() {
    const MARKER: &str = "BRISKDB_CONTENTION_ENV_TEST_CHILD";
    if std::env::var_os(MARKER).is_some() {
        let (_, options) = crate::Args::try_parse_from(["briskdb"])
            .unwrap()
            .into_server_parts()
            .unwrap();
        let policy = options.contention_policy().unwrap();
        assert_eq!(policy.initial_delay(), Duration::from_millis(2));
        assert_eq!(policy.max_delay(), Duration::from_millis(50));
        assert_eq!(policy.multiplier(), 3);
        assert_eq!(policy.jitter(), ContentionJitter::Full);
        assert_eq!(policy.max_retries(), 20);
        assert_eq!(policy.max_elapsed(), Duration::from_millis(1000));
        let (_, options) = crate::Args::try_parse_from([
            "briskdb",
            "--contention-initial-delay-ms",
            "7",
            "--contention-jitter",
            "none",
        ])
        .unwrap()
        .into_server_parts()
        .unwrap();
        let policy = options.contention_policy().unwrap();
        assert_eq!(policy.initial_delay(), Duration::from_millis(7));
        assert_eq!(policy.jitter(), ContentionJitter::None);
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli_contention_tests::daemon_environment_and_cli_precedence_are_isolated",
        ])
        .env_clear()
        .env(MARKER, "1")
        .env("BRISKDB_CONTENTION_MODE", "backoff")
        .env("BRISKDB_CONTENTION_INITIAL_DELAY_MS", "2")
        .env("BRISKDB_CONTENTION_MAX_DELAY_MS", "50")
        .env("BRISKDB_CONTENTION_MULTIPLIER", "3")
        .env("BRISKDB_CONTENTION_JITTER", "full")
        .env("BRISKDB_CONTENTION_MAX_RETRIES", "20")
        .env("BRISKDB_CONTENTION_MAX_ELAPSED_MS", "1000")
        .status()
        .unwrap();
    assert!(status.success());
}
