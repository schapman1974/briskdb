#[test]
fn alpha_release_contract_covers_every_native_archive_and_safety_boundary() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0-alpha.5");

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
        "PostgreSQL extended-query protocol is unsupported",
        "psycopg.ClientCursor",
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

#[test]
fn python_release_contract_covers_every_supported_wheel_and_publish_gate() {
    let python_manifest = include_str!("../python/Cargo.toml");
    assert!(python_manifest.contains(&format!("version = {:?}", env!("CARGO_PKG_VERSION"))));
    assert!(python_manifest.contains("features = [\"abi3-py39\"]"));

    let metadata = include_str!("../python/pyproject.toml");
    for required in [
        "requires-python = \">=3.9\"",
        "Typing :: Typed",
        "license-files = [\"BRISKDB_LICENSE.txt\"]",
        "maturin==1.14.1",
    ] {
        assert!(
            metadata.contains(required),
            "Python metadata is missing: {required}"
        );
    }

    let workflow = include_str!("../.github/workflows/python-wheels.yml");
    for required in [
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "macosx_11_0_x86_64",
        "macosx_11_0_arm64",
        "python-version: \"3.9\"",
        "python-version: \"3.14\"",
        "--only-binary=:all:",
        "auditwheel show",
        "delocate-listdeps --all",
        "maturin sdist",
        "check_dist.py",
        "mypy --strict --python-version 3.9",
    ] {
        assert!(
            workflow.contains(required),
            "Python distribution workflow is missing: {required}"
        );
    }

    let release = include_str!("../.github/workflows/release.yml");
    for required in [
        "needs: [build, python-wheels]",
        "actions/attest-build-provenance",
        "SHA256SUMS",
        "briskdb-python-wheel-*",
        "briskdb-python-sdist",
        "pypa/gh-action-pypi-publish",
        "password: ${{ secrets.PYPI_API_TOKEN }}",
        "attestations: false",
    ] {
        assert!(
            release.contains(required),
            "release workflow is missing Python gate: {required}"
        );
    }

    let compatibility = include_str!("../python/COMPATIBILITY.md");
    assert!(compatibility.contains("`manylinux_2_28`"));
    assert!(compatibility.contains("`musllinux`/Alpine"));
    assert!(compatibility.contains("no stable pre-1.0 compatibility promise"));

    assert!(include_bytes!("../python/python/briskdb/py.typed").is_empty());
    assert!(!include_str!("../python/python/briskdb/__init__.pyi").is_empty());
    assert!(!include_str!("../python/python/briskdb/_briskdb.pyi").is_empty());
    assert!(!include_str!("../python/python/briskdb/api.pyi").is_empty());
}
