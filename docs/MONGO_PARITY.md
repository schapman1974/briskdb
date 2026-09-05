# Mongo compatibility parity contract

Status: TinyMongo v1 contract frozen for issue
[#161](https://github.com/schapman1974/briskdb/issues/161); BriskDB candidate
endpoint not yet implemented

BriskDB uses a versioned differential contract to define the document behavior
that its Rust and Python APIs, and later its MongoDB listener, must preserve.
The first contract is source-locked to
[TinyMongo v1.3.0](https://github.com/schapman1974/tinymongo/releases/tag/v1.3.0)
at commit `53cbf44e98b8caa036163725d195fd29592e1cc0`. It covers 228 logical cases
through both synchronous and asynchronous APIs. Across TinyMongo's seven
contract backends, that is a 3,192-execution discovery matrix.

This is a frozen behavioral input, not a claim that BriskDB already has MongoDB
parity. The checked-in report contains only the 456 sync/async executions from
the `tinymongo-memory` reference. BriskDB's owned runner reproduces all 456 in
CI and byte-compares the normalized result with the checked-in reference. The
report remains `reference-only` until a BriskDB endpoint supplies candidate
results through the checked-in adapter.

## Versioned files

[`compat/mongo/v1/manifest.json`](../compat/mongo/v1/manifest.json) is the
entry point. It records the source commit and contract-tree digests, suite and
backend dimensions, file hashes, reference target, counts, and the reviewed
capability inventory. That inventory includes client, database, collection,
and cursor APIs; options; BSON values and ordering; query and update operators;
projections; indexes; aggregation; result shapes; warnings; errors and codes;
and unsupported behavior.

The remaining files have distinct roles:

- `corpus.json` assigns stable IDs, suites, APIs, and source provenance to each
  logical case. The source-file hashes cover the complete TinyMongo contract
  tree at the locked commit, and the harvest metadata pins Python, pytest, and
  PyMongo.
- `reference-results.json` holds the normalized `tinymongo-memory` outcomes for
  all 228 cases through both APIs.
- `semantic-variants.json` records reviewed backend-specific branches and skips
  already present in the locked TinyMongo tests. These entries explain the
  intended behavior; they do not permit a candidate result mismatch.
- `intentional-differences.json` is the strict candidate-difference allow-list.
  Every entry is scoped to one target, case, and API and requires a BriskDB
  issue. Its current sync and async entries record the `mongodb-mongodb` skip
  for a Regex `_id`, which real MongoDB forbids. When that target is present,
  an entry that no longer matches the observed result is stale and makes the
  comparison fail.
- `runner/contracts/` contains BriskDB-owned, target-neutral copies of the 228
  executable contract bodies. `runner/adapters/` is the only target-specific
  boundary, with modules for TinyMongo, real MongoDB, and a configurable
  BriskDB candidate endpoint.

The manifest hashes every checked-in contract input. Changing the source
commit, corpus, reference results, capability inventory, semantic variants, or
difference policy therefore requires a normal reviewed BriskDB change. Do not
silently refresh a snapshot while implementing a candidate.

## Process boundary

The BriskDB Rust library, Python wheel, and server do not declare or load
TinyMongo. CI installs TinyMongo, PyMongo, and pytest separately inside the
Mongo contract test environment. None is pulled in by BriskDB's Cargo or Python
dependency metadata, and production code does not import the oracle.

Source distributions, including the Rust `.crate` and Python sdist, may retain
the checked-in manifest, fixtures, adapters, and harness for provenance and
offline audit. Those files are inert package source: a normal build or install
does not execute them or install TinyMongo. Built wheels and binaries do not
contain the TinyMongo package.

The normalization harness in
[`scripts/mongo_parity.py`](../scripts/mongo_parity.py) uses only the Python
standard library. The executable fixtures use pytest plus PyMongo's public BSON
and exception types as the compatibility vocabulary. They do not import
TinyMongo. The only TinyMongo import in the owned runner is lazy and isolated
in `runner/adapters/tinymongo.py`; selecting the BriskDB or MongoDB adapter does
not load it.

A producer identifies each execution with the exact JUnit properties
`tinymongo.api`, `tinymongo.backend`, and `tinymongo.suite`. Its test name maps
to a stable corpus case ID. Assertions inside the producer cover ordered BSON,
document mutation, result and cursor behavior, warning categories, exception
classes and stable codes, and explicit unsupported operations. The normalized
result preserves the target, API, backend, suite, outcome, and a normalized
observation. The observation combines a category with a SHA-256 fingerprint of
the outcome, category, and redacted reason, so a different failure cannot pass
by sharing only the expected outcome.

This adapter boundary lets the owned fixtures exercise each implementation and
publish the same result envelope. TinyMongo remains an oracle-only test
dependency. Its adapter source may remain visible in a source distribution, but
the oracle is neither installed nor imported by building, installing, or using
BriskDB. An optional real MongoDB target follows the same boundary.

## Validate and report

Validate the locked files and hashes from the repository root:

```bash
python3 scripts/mongo_parity.py validate
```

If a TinyMongo Git checkout is available, verify the locked source objects
against it. This reads blobs from the manifest's commit and does not trust the
checkout's working tree:

```bash
python3 scripts/mongo_parity.py verify-source \
  --source-root /path/to/tinymongo
```

Install the pinned test tools and the locked TinyMongo checkout into a test
environment, then reproduce the reference with BriskDB's owned fixtures:

```bash
python3 -m pip install -r compat/mongo/v1/runner/requirements.txt
python3 -m pip install --no-build-isolation --no-deps /path/to/tinymongo

mkdir -p target/mongo-parity
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=tinymongo \
  --mongo-contract-backend=memory \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/tinymongo-memory.xml

python3 scripts/mongo_parity.py ingest \
  --implementation tinymongo \
  --junit target/mongo-parity/tinymongo-memory.xml \
  --output target/mongo-parity/tinymongo-memory.json

cmp compat/mongo/v1/reference-results.json \
  target/mongo-parity/tinymongo-memory.json
```

CI checks out commit `53cbf44e98b8caa036163725d195fd29592e1cc0`
under `target/`, installs it only in this contract job, runs the same 456 owned
fixture executions, and requires the byte comparison to pass. The source
snapshot records its original Python 3.9.6 harvest environment. CI replays it
with Python 3.9.25, the pinned 3.9 patch available for Ubuntu 24.04.

Generate the currently available reference report:

```bash
mkdir -p target/mongo-parity
python3 scripts/mongo_parity.py report \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md
```

The report should say `reference-only`. CI runs both commands, publishes the
Markdown report in the workflow summary, and uploads the JSON and Markdown as
the `mongo-parity-report` artifact. This baseline comparison permits absent
optional targets, including real MongoDB.

To normalize JUnit produced by a candidate adapter:

```bash
python3 scripts/mongo_parity.py ingest \
  --implementation briskdb \
  --junit target/mongo-parity/briskdb.xml \
  --output target/mongo-parity/briskdb.json

python3 scripts/mongo_parity.py report \
  --require-target briskdb-briskdb \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md \
  target/mongo-parity/briskdb.json
```

`--require-target` is repeatable for publish gates. `report` fails when a
required target is absent, or for an uncovered case, an unexpected observation
fingerprint, or a stale intentional difference. An intentional difference must
name the exact reference and candidate fingerprints as well as their outcomes.
Once a BriskDB candidate endpoint exists, its normalized result belongs in the
required CI comparison; the checked-in TinyMongo reference remains the
comparison baseline.

A publish gate that includes reviewed optional targets should also require
every target named by the allow-list and supply its normalized result:

```bash
python3 scripts/mongo_parity.py report \
  --require-target briskdb-briskdb \
  --require-allowlist-targets \
  --json-output target/mongo-parity/report.json \
  --markdown-output target/mongo-parity/report.md \
  target/mongo-parity/briskdb.json \
  target/mongo-parity/mongodb.json
```

Without `--require-allowlist-targets`, absent optional targets do not fail a
reference-only or partial comparison. If an allow-listed target is present,
its stale entries always fail regardless of that flag.

## Runner targets and options

The owned pytest runner accepts these target controls:

| Option | Environment fallback | Behavior |
| --- | --- | --- |
| `--mongo-contract-target=tinymongo|mongodb|briskdb` | `BRISKDB_MONGO_CONTRACT_TARGET` | Selects one lazy-loaded adapter. |
| `--mongo-contract-api=sync|async|both` | `BRISKDB_MONGO_CONTRACT_API` | Runs one API or both; the default is both. |
| `--mongo-contract-backend=<id>` | `BRISKDB_MONGO_CONTRACT_BACKEND` | Selects a TinyMongo backend; the default is `memory`. |
| `--mongo-contract-mongodb-uri=<uri>` | `BRISKDB_MONGODB_URI` | Supplies the optional real MongoDB endpoint. |
| `--mongo-contract-briskdb-uri=<uri>` | `BRISKDB_MONGO_PARITY_BRISKDB_URI` | Supplies the BriskDB candidate endpoint. |
| `--mongo-contract-require-target` | none | Fails instead of skipping when an optional target is unavailable. |

For example, an available real MongoDB instance can produce its normalized
candidate this way:

```bash
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=mongodb \
  --mongo-contract-mongodb-uri="$BRISKDB_MONGODB_URI" \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/mongodb.xml

python3 scripts/mongo_parity.py ingest \
  --implementation mongodb \
  --junit target/mongo-parity/mongodb.xml \
  --output target/mongo-parity/mongodb.json
```

Run the same corpus against a BriskDB candidate endpoint when one is available:

```bash
python3 -m pytest -q -p no:cacheprovider -c /dev/null \
  compat/mongo/v1/runner/contracts \
  --mongo-contract-target=briskdb \
  --mongo-contract-briskdb-uri='mongodb://127.0.0.1:27018/?directConnection=true' \
  --mongo-contract-api=both \
  --mongo-contract-require-target \
  --junitxml=target/mongo-parity/briskdb.xml
```

The candidate adapter uses PyMongo's sync and async transports but retains the
implementation identity `briskdb`. The runner records transport separately so
UUID configuration, Regex decoding, bytearray encoding, client-side
validation, and warning behavior follow PyMongo without inheriting semantic
exemptions that belong only to real MongoDB. No candidate endpoint is built in
this issue, and the reference-only CI job does not attempt this command.

## Refreshing the source snapshot

Refreshing the corpus is a compatibility-policy change. Check out the reviewed
TinyMongo commit separately, run its entire contract matrix to JUnit, then use
`snapshot` with the full 40-character commit:

```bash
python3 scripts/mongo_parity.py snapshot \
  --junit /path/to/tinymongo-contract.xml \
  --source-root /path/to/tinymongo \
  --source-commit <reviewed-commit> \
  --python-version 3.9.6 \
  --pytest-version 8.4.2 \
  --pymongo-version 4.17.0 \
  --corpus-output compat/mongo/v1/corpus.json \
  --results-output compat/mongo/v1/reference-results.json
```

After generating a reviewed snapshot, repeat the same `snapshot` command with
`--check`; this regenerates the canonical bytes and fails on drift without
rewriting either output file.

Review the source-tree identity, capability inventory, case additions and
removals, target-specific semantics, pinned harvest toolchain, normalized
reference results, and every intentional difference. Then update the manifest
counts and hashes, run `snapshot --check`, `validate`, and `verify-source`
against the updated manifest before committing the refresh.
