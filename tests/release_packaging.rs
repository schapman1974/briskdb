#[test]
fn alpha_release_contract_covers_every_native_archive_and_safety_boundary() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0-alpha.2");

    let workflow = include_str!("../.github/workflows/release.yml");
    for required in [
        "ubuntu-24.04",
        "ubuntu-24.04-arm",
        "macos-15-intel",
        "macos-15",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "cargo build --release --locked --bins",
        "Smoke-test native server",
        "deb_architecture: amd64",
        "deb_architecture: arm64",
        "Install and smoke-test Debian service",
        "packaging/debian/build-deb.sh",
        "docs/OFFLINE_BACKUP.md",
        "SHA256SUMS",
        "--prerelease",
    ] {
        assert!(
            workflow.contains(required),
            "release workflow is missing: {required}"
        );
    }

    let notes = include_str!("../RELEASE_NOTES.md");
    for required in [
        "no authentication, authorization, or TLS",
        "PostgreSQL cannot execute SQL yet",
        "General cross-shard transactions are unsupported",
        "complete data-directory copy",
        "There is no stable pre-1.0 on-disk compatibility promise",
        "manifest version 12",
        "In-place downgrade is unsupported",
        "no on-disk format change",
    ] {
        assert!(
            notes.contains(required),
            "release notes are missing critical boundary: {required}"
        );
    }
}
