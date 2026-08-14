# Publishing the Python package

The `python-wheels` workflow is the release gate. It builds one `cp39-abi3`
wheel on each native macOS/Linux ARM/x86 runner, audits native dependencies,
installs each wheel under CPython 3.9 and 3.14, runs the complete Python suite,
checks packaged typing, and separately builds/installs the sdist.

A `v<crate-version>` tag is publishable only when the root Rust crate,
`briskdb-python` crate, Python metadata, wheel filenames, and runtime
`briskdb.__version__` agree. The release workflow then:

1. waits for native archives, Debian packages, wheels, and sdist gates;
2. creates SHA-256 checksums and GitHub build-provenance attestations;
3. attaches distributions and platform audit reports to the GitHub prerelease;
4. publishes wheels and sdist to PyPI through its `pypi` environment and OIDC
   trusted publishing, including PyPI attestations.

Repository setup must register `schapman1974/briskdb`'s `release.yml` workflow
as a pending trusted publisher for the new `briskdb` PyPI project (or a normal
trusted publisher after its first upload) and protect the GitHub `pypi`
environment. No long-lived PyPI token is stored in the workflow.

Verify a downloaded artifact with:

```bash
sha256sum --check SHA256SUMS
gh attestation verify briskdb-0.1.0a4-*.whl --repo schapman1974/briskdb
```

Do not reuse or move a release tag. Update both Cargo package versions and the
release notes, commit, pass CI, and then create the matching tag.
