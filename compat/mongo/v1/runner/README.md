# Executable Mongo compatibility corpus

This directory contains an executable, target-neutral adaptation of the
TinyMongo v1.3.0 contract suite. The same pytest corpus can run against the
frozen TinyMongo reference, an optional real MongoDB server, and a configured
BriskDB Mongo-compatible endpoint. BriskDB does not provide that endpoint yet,
so its checked-in CI report remains reference-only.

The contract source is frozen at TinyMongo commit
`53cbf44e98b8caa036163725d195fd29592e1cc0`. `provenance.json` records the
upstream Git blob and SHA-256 digest for every copied file, the digest of every
adapted file, and the exact contract and runtime tree IDs. `adaptation.patch`
is the complete reviewable diff from the upstream files to this runner. The
upstream license is preserved in `UPSTREAM_LICENSE.txt`.

The adaptations replace TinyMongo-owned fixtures, exception imports, BSON
identity helpers, and the unsupported-index warning type with neutral runner
equivalents. They also distinguish implementation identity from direct versus
PyMongo transport behavior. The assertions and parameter values stay in the
vendored test modules. Each pytest execution emits these JUnit properties:

- `tinymongo.contract_id`: the canonical upstream case ID
- `tinymongo.api`: `sync` or `async`
- `tinymongo.backend`: the selected storage or server target
- `tinymongo.suite`: the frozen contract suite

TinyMongo is imported lazily and only by `adapters/tinymongo.py`. The neutral
corpus and BriskDB code do not import it. PyMongo provides the public BSON and
error vocabulary used by the contracts; its clients are loaded lazily only
when a PyMongo-backed target is opened.

Create an isolated test environment with the pinned reference dependencies.
Python 3.9.6 is the recorded harvest interpreter; CI replays the corpus with
Python 3.9.25, the pinned 3.9 patch available for Ubuntu 24.04.

```bash
python3.9 -m venv .venv-mongo-contract
.venv-mongo-contract/bin/python -m pip install \
  -r compat/mongo/v1/runner/requirements.txt
git clone https://github.com/schapman1974/tinymongo \
  target/tinymongo-source
git -C target/tinymongo-source checkout --detach \
  53cbf44e98b8caa036163725d195fd29592e1cc0
.venv-mongo-contract/bin/python -m pip install \
  --no-build-isolation --no-deps \
  ./target/tinymongo-source
```

Run the complete in-memory reference corpus:

```bash
.venv-mongo-contract/bin/python -m pytest -o addopts='' -q \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=tinymongo \
  --mongo-contract-backend=memory \
  --mongo-contract-api=both \
  --junitxml=target/mongo-contract-tinymongo.xml
```

That command collects 228 canonical cases and executes each through the sync
and async APIs, for 456 executions. A pre-existing local TinyMongo checkout can
replace the clone above; verify that it resolves to the locked commit, then
install it with
`python -m pip install --no-build-isolation --no-deps /path/to/tinymongo`.

Run the same corpus against a real MongoDB server:

```bash
BRISKDB_MONGODB_URI='mongodb://127.0.0.1:27017/?directConnection=true' \
.venv-mongo-contract/bin/python -m pytest -o addopts='' -q \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=mongodb \
  --mongo-contract-api=both \
  --mongo-contract-require-target \
  --junitxml=target/mongo-contract-mongodb.xml
```

Without `--mongo-contract-require-target`, an unavailable MongoDB server is an
ordinary skip.

Run the same corpus against a BriskDB candidate endpoint:

```bash
.venv-mongo-contract/bin/python -m pytest -o addopts='' -q \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=briskdb \
  --mongo-contract-briskdb-uri='mongodb://127.0.0.1:27018/?directConnection=true' \
  --mongo-contract-api=both \
  --mongo-contract-require-target \
  --junitxml=target/mongo-contract-briskdb.xml
```

The BriskDB adapter uses PyMongo only inside this contract environment and
keeps the implementation identity `briskdb`. Separate `transport` metadata
selects unavoidable PyMongo encoding and decoding behavior without granting
the candidate real-Mongo semantic exemptions. With no configured candidate
URI, selecting `briskdb` fails clearly with `TargetUnavailable`.

Run the dependency-boundary and provenance checks without installing the
contract dependencies:

```bash
python3 -m unittest discover -s tests -p 'test_mongo_contract_runner.py'
```
