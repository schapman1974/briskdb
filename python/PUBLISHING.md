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
4. publishes wheels and sdist to PyPI through its protected `pypi` environment
   using the masked `PYPI_API_TOKEN` Actions secret.

The repository secret must contain a PyPI API token with permission to publish
`briskdb`. The workflow passes it directly to the pinned publisher action as
the `__token__` credential; it is never written into an artifact or source
file. Protect the GitHub `pypi` environment and rotate the token if exposed.
PEP 740 PyPI attestations are unavailable with token authentication, so the
workflow disables them while retaining GitHub build-provenance attestations.

Verify a downloaded artifact with:

```bash
sha256sum --check SHA256SUMS
gh attestation verify briskdb-0.1.0a5-*.whl --repo schapman1974/briskdb
```

Do not reuse or move a release tag. Update both Cargo package versions and the
release notes, commit, pass CI, and then create the matching `v<crate-version>`
tag. Pushing that tag is the single trigger for both the GitHub prerelease and
PyPI publication; an ordinary version-changing branch or main-branch push
cannot publish.
