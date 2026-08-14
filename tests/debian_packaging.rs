#[test]
fn debian_service_uses_fhs_paths_journald_and_a_restricted_account() {
    let unit = include_str!("../packaging/debian/briskdb.service");
    for required in [
        "User=briskdb",
        "Group=briskdb",
        "EnvironmentFile=-/etc/default/briskdb",
        "WorkingDirectory=/var/lib/briskdb",
        "StateDirectory=briskdb",
        "StateDirectoryMode=0750",
        "ExecStart=/usr/bin/briskdb",
        "ConditionFileIsExecutable=/usr/bin/briskdb",
        "StandardOutput=journal",
        "StandardError=journal",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "NoNewPrivileges=true",
        "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6",
    ] {
        assert!(
            unit.contains(required),
            "systemd unit is missing: {required}"
        );
    }

    let configuration = include_str!("../packaging/debian/briskdb.default");
    for required in [
        "BRISKDB_LISTEN=127.0.0.1:7654",
        "BRISKDB_POSTGRES_LISTEN=disabled",
        "BRISKDB_DATA_DIR=/var/lib/briskdb/data",
        "RUST_LOG=briskdb=info",
    ] {
        assert!(
            configuration.contains(required),
            "Debian configuration is missing: {required}"
        );
    }
}

#[test]
fn cargo_versions_convert_to_debian_versions_without_shell_tilde_expansion() {
    let builder = format!(
        "{}/packaging/debian/build-deb.sh",
        env!("CARGO_MANIFEST_DIR")
    );

    for (cargo_version, expected) in [("0.1.0-alpha.4", "0.1.0~alpha.4-1"), ("0.1.0", "0.1.0-1")] {
        let output = Command::new(&builder)
            .args(["--print-debian-version", cargo_version])
            .output()
            .expect("package builder must run");
        assert!(output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), expected);
    }

    let output = Command::new(&builder)
        .args(["--print-package-filename", "0.1.0-alpha.4"])
        .output()
        .expect("package builder must print its external filename");
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "briskdb_0.1.0.alpha.4-1_ARCH.deb"
    );
}

#[test]
fn debian_package_contract_preserves_configuration_and_database_state() {
    let builder = include_str!("../packaging/debian/build-deb.sh");
    for required in [
        "/usr/bin/briskdb",
        "/usr/bin/briskdb-import",
        "/lib/systemd/system/briskdb.service",
        "/etc/default/briskdb",
        "dpkg-deb --build --root-owner-group",
    ] {
        assert!(
            builder.contains(required),
            "package builder is missing: {required}"
        );
    }

    let postrm = include_str!("../packaging/debian/postrm");
    assert!(postrm.contains("deliberately retained"));
    assert!(!postrm.contains("rm -"));

    let smoke = include_str!("../packaging/debian/test-deb-service.sh");
    for required in [
        "systemd-analyze verify",
        "BriskDB is ready",
        "package smoke-test local configuration",
        "wait_for_service",
        "dpkg -r briskdb",
        "package-smoke-state",
    ] {
        assert!(
            smoke.contains(required),
            "package smoke test is missing: {required}"
        );
    }
}
use std::process::Command;
