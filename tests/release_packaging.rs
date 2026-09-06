#[test]
fn alpha_release_contract_covers_every_native_archive_and_safety_boundary() {
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0-alpha.6");

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
        "--admin-listen 127.0.0.1:17655",
        "http://127.0.0.1:17654/v1",
        "http://127.0.0.1:17654/v1/query/stream",
        "http://127.0.0.1:17655/v1/ready",
        "http://127.0.0.1:17655/admin",
        "briskdb-request-id",
        "application/x-ndjson; charset=utf-8",
        r#"{"kind": "complete", "rows": 1}"#,
        "deb_architecture: amd64",
        "deb_architecture: arm64",
        "Install and smoke-test Debian service",
        "packaging/debian/build-deb.sh",
        "docs/OFFLINE_BACKUP.md",
        "test -s docs/openapi-v1.json",
        "$archive/docs/openapi-v1.json",
        "SHA256SUMS",
        "--prerelease",
    ] {
        assert!(
            workflow.contains(required),
            "release workflow is missing: {required}"
        );
    }

    let notes = include_str!("../RELEASE_NOTES.md");
    let release_heading = format!("# BriskDB {}", env!("CARGO_PKG_VERSION"));
    let current_release = notes
        .split_once(&release_heading)
        .unwrap_or_else(|| panic!("release notes are missing heading: {release_heading}"))
        .1
        .split("\n# BriskDB ")
        .next()
        .expect("current release section must be present");
    let current_release = current_release
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    for required in [
        "TLS and SCRAM-SHA-256",
        "bounded simple and parameterized text/binary extended queries",
        "single-shard transactions",
        "native document API is limited",
        "collection/index/insert/find/count/delete engine slice",
        "`insert_one`",
        "`find` and `count_documents`",
        "`delete_one`",
        "complete data-directory copy",
        "There is no stable pre-1.0 on-disk compatibility promise",
        "manifest version 14",
        "manifest versions 2 through 13",
        "In-place downgrade is unsupported",
    ] {
        assert!(
            current_release.contains(required),
            "current release notes are missing critical boundary: {required}"
        );
    }
}

#[test]
fn openapi_artifact_and_generator_are_part_of_the_cargo_contract() {
    let manifest = include_str!("../Cargo.toml");
    for required in [
        "name = \"generate-openapi-v1\"",
        "path = \"examples/openapi.rs\"",
        "required-features = [\"http\"]",
        "\"dep:utoipa\"",
        "\"dep:utoipa-axum\"",
        "utoipa = { version = \"=5.5.0\", default-features = false, features = [\"macros\"], optional = true }",
        "utoipa-axum = { version = \"=0.2.0\", default-features = false, optional = true }",
        "jsonschema = { version = \"=0.54.0\", default-features = false, features = [\"arbitrary-precision\"] }",
        "oas3 = \"=0.21.0\"",
    ] {
        assert!(
            manifest.contains(required),
            "Cargo OpenAPI contract is missing: {required}"
        );
    }

    let continuous_integration = include_str!("../.github/workflows/ci.yml");
    assert!(continuous_integration.contains("cargo package --locked --allow-dirty"));
    assert!(continuous_integration.contains("utoipa|utoipa-axum|utoipa-gen"));
    assert!(!include_bytes!("../docs/openapi-v1.json").is_empty());
    assert!(!include_bytes!("../examples/openapi.rs").is_empty());
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
