# Unreleased

The administration HTTP listener now exposes versioned operational endpoints
for readiness, relational catalog inspection, migration progress and exact
generation lookup, validated shard state, bounded active-query inspection and
cancellation, stopped-server backup capability, and passive checkpoint
maintenance. The data listener continues to own query and execute; an opaque
active-query handle created there can be listed and cancelled from the admin
listener because every Engine clone shares the same bounded registry. Reports
omit SQL, parameters, query SQL digests, and the durable migration identity;
handles disappear when their HTTP handlers finish or drop, and cancelling one
handle cannot target later work. Engine leases remain held independently until
any interrupted SQLite cleanup finishes.

`GET /v1/ready` returns HTTP 200 only while lifecycle and schema admission are
ready; release archives and Debian service smoke tests now require that probe
instead of treating a bound socket as sufficient. `/ready` is its unversioned
probe alias. The backup endpoint reports the existing stopped-directory-copy
procedure and performs no copy. Passive checkpoint remains optional preparation
and is not an online snapshot or cross-file recovery point; coordinated online
backup remains issue #67. These endpoints add no manifest, shard, or other
on-disk format change.

HTTP data and administration traffic now use separate routers and listener
sockets over the same engine. `--listen` and `BRISKDB_LISTEN` remain the data
plane at `127.0.0.1:7654`, serving `/v1` discovery, query, and execute. The new
`--admin-listen` / `BRISKDB_ADMIN_LISTEN` defaults to
`127.0.0.1:7655`, accepts the exact value `disabled`, and exclusively serves
`/health`, `/metrics`, `/v1/health`, `/v1/admin/*`, and the `/admin` browser.
Both unauthenticated HTTP planes remain loopback-only.

Relative route paths and response representations are unchanged, but operator
and browser clients must use the administration base address. Cross-plane paths
return 404 rather than reaching the other router. Rust retains the established
combined HTTP router helpers for host-owned integration, while daemon and
attached-server assembly use the split routers. Python attached servers add an
ephemeral-by-default `admin` listener and optional `admin_address`; passing
`admin=None` disables it. All configured sockets bind before readiness and
share the existing drain lifecycle. Pre-1.0 Rust callers that construct
`server::Config` or `server::ListenerConfig` with a struct literal must add the
new optional `admin_listen` field. This change adds no dependency or on-disk
format change. See [the HTTP listener contract](docs/HTTP_LISTENERS.md).

HTTP v1 now offers the opt-in `lossless-json-v1` value encoding while retaining
`legacy-json-v1` as the byte-compatible default. Lossless query rows keep the
existing ordered columns and positional arrays, tag every signed and unsigned
integer with exact decimal text, preserve binary64 values as their 16-digit
lowercase hexadecimal bits, and carry blobs or invalid SQLite text as canonical
padded base64. The same tags are accepted as execute/query parameters, so a
returned blob can be reused without becoming JSON text. Discovery advertises
both encodings, and malformed or unknown selections and tags fail before engine
execution.

The codec preserves the current protocol-neutral values; it does not add a
SQLite storage class. Existing decimal, oversized unsigned-integer,
invalid-text, and NaN binding rejections remain. Relational timestamps still use
an application-chosen ordinary `Text` or `Int64` representation and have no
native BriskDB HTTP tag. The change adds no manifest, shard-file, listener,
routing, or other on-disk change.

# BriskDB 0.1.0-alpha.6 — 2026-09-06

Alpha 6 expands the supported PostgreSQL and embedded Python surfaces while
keeping the same-host, local-filesystem multi-process boundary from alpha 5.
It adds a bounded extended-query lifecycle for PostgreSQL, single-shard
transactions, TLS/SCRAM protection for remote PostgreSQL listeners, versioned
HTTP behavior, experimental global indexes, and an opt-in Rust and Python
document-command slice. This remains an evaluation and development release,
not a production claim.

Mongo compatibility now has a versioned, source-locked TinyMongo v1.3.0
contract. Its manifest inventories the supported document surface, 228 logical
cases cover both sync and async APIs across a 3,192-execution backend matrix,
and CI validates the checked-in corpus and publishes a readable parity report.
The current report is reference-only; it does not claim BriskDB parity before a
candidate endpoint is available and tested. See [the Mongo parity contract](docs/MONGO_PARITY.md).

The opt-in `documents` feature now provides the Rust BSON foundation for that
work. It preserves ordered documents and BSON wire types, applies an explicit
duplicate-field policy, implements TinyMongo-compatible equality, hashing, and
comparison across numeric and extended BSON values, and exposes versioned
`BBKY` semantic keys. Codec validation is bounded to MongoDB's 16 MiB document
and 100-level nesting limits and is covered by frozen comparison vectors,
official BSON corpus cases, property tests, and cargo-fuzz targets. This release
also adds manifest-v14 document namespaces, exact sharded BSON records,
canonical `_id` routing and uniqueness, restart-safe collection provisioning,
and an atomic TinyMongo v1.3 SQLite importer. A new protocol-neutral document
engine executes collection and index metadata commands, single explicit-ID
inserts, empty-filter or exact-`_id` find/count commands, and exact-`_id`
single-document deletion through the same session, pool, routing, cancellation,
deadline, shutdown, and result-limit boundaries as SQL. Point plans target one
shard; scatter reads merge in durable natural order while preserving exact
ordered BSON.

The embedded Rust surface now exposes that engine through thin
`BriskDb::execute_document` and `BriskSession::execute_document` methods. A
document-enabled handle forwards the caller's owned `DocumentRequest`
unchanged, preserving ordered BSON, typed results, request identity, point or
scatter plans, cancellation, deadlines, result limits, and classified engine
errors. Document support remains opt-in twice: compile with the `documents`
feature and open with `DocumentSupport::Enabled`. A disabled handle rejects a
facade call with `FailedPrecondition`; a build without `documents` rejects the
enabled builder setting with `Unsupported` before accessing storage.
Differential integration tests compare direct engine execution with both
facades for point and scatter commands, ordered extended BSON values, request
controls, session ownership, and shutdown.

The typed Python wheel now exposes the same command slice on `Session` and
`AsyncSession`, gated by `documents=True`. Ordered mappings and PyMongo BSON
classes convert without JSON, including exact int32/int64 identity, IEEE-754
bits, Decimal128 BID payloads, ObjectId, Code with scope, and all five UUID
representation modes. Datetimes use aware UTC millisecond results, validation
enforces the 16 MiB and 100-container BSON limits, and conversion failures use
the existing stable Python exception hierarchy. PyMongo 4.17.0 is the pinned
compatibility oracle, but remains an optional, lazily imported companion:
installed-wheel and sdist gates first prove SQL and the missing-BSON failure
path in an environment without PyMongo.

The facade exposes the exact engine slice above. General matchers, update
expressions, replacements, aggregation, retained cursors, the MongoDB listener,
a collection-oriented Rust convenience API, and broader Python document
operations remain follow-up work before MongoDB compatibility can be claimed.
See the
[embedded Rust guide](docs/EMBEDDED_RUST.md),
[the BSON contract](docs/BSON.md),
[the document storage contract](docs/DOCUMENT_STORAGE.md), and
[the document engine contract](docs/DOCUMENT_ENGINE.md). Python users should
also read the [document API map](python/API.md) and
[value-conversion contract](python/VALUE_CONVERSIONS.md).

HTTP now has an explicit version-1 contract, discovery at `/v1`, a versioned
`/v1/health` alias, and `BriskDB-API-Version: 1` on v1 responses. Existing valid
SQL requests and result encodings are preserved. Unknown or duplicate envelope
fields now fail before execution; JSON decoding, body-limit, media-type,
missing-route, and method errors use fixed, redacted problem details. Clients
that relied on ignored fields or framework error text must migrate as described
in [the HTTP API contract](docs/HTTP_API.md). This HTTP change does not alter
storage or engine behavior. Lossless JSON value encoding remains issue #51.

Global indexes now have a production/release gate: redaction-safe Rust health
reports, richer `/health` and `/v1/admin/global-indexes` responses, Prometheus
`/metrics`, complete shard/manifest/global-index checkpoint reporting, and a
restore test covering reservations, value sequences, outboxes, watermarks, and
summaries. Dedicated fault, clock, disk-full, crash-boundary, and mixed
multi-process soak suites run through a manual GitHub workflow.

Indexed writes continue to omit unchanged non-unique outbox events and now also
omit summary and unique-snapshot mutations when a captured update leaves every
canonical index key and row locator unchanged.
Unique snapshot publication validates the existing ordered prefix and appends
only a new suffix, while deletions and middle/key/locator changes retain the
full rebuild fallback. This removes repeated whole-shard snapshot rewrites from
append-heavy inserts. Unchanged unique indexes receive a compare-first coherence
check that starts no global write transaction when clean and repairs markerless
snapshot-only crash residue when stale, preserving recovery and rollback.

The same-host 2/4/10/64-shard before/after matrix confirms identical results
and constraints while indexed hits/misses execute on one shard. It also finds
that current freshness/summary inspection and write coordination are slower
than the direct hot-cache baseline. Global indexes therefore remain explicit,
experimental alpha functionality and are not yet recommended for
latency-sensitive production use. The first hosted Ubuntu calibration also
found shard-linear SQLite WAL shared-memory process output during indexed reads
and bounded it with explicit Linux-tested alpha guardrails;
[#293](https://github.com/schapman1974/briskdb/issues/293) tracks removing that
connection churn. See `docs/GLOBAL_INDEX_RELEASE_GATE.md`.

## Install the Python package

Compiler-free `cp39-abi3` wheels support CPython 3.9 through 3.14 on Linux
x86-64/ARM64 (`manylinux_2_28`) and macOS Intel/Apple Silicon (macOS 11+):

```bash
python -m pip install briskdb==0.1.0a6
```

Each native wheel is dependency-audited and installed for the minimum and
maximum supported Python versions before publication. The separately built
source distribution is also installed and tested. Release automation checks
the Rust, Python, and tag versions, distribution contents, type information,
checksums, and build provenance before publishing.

## Critical alpha boundaries

- HTTP remains unauthenticated and loopback-only. PostgreSQL may bind remotely
  only with its configured TLS and SCRAM-SHA-256 boundary; BriskDB does not yet
  provide general users, roles, or authorization policy.
- PostgreSQL supports bounded simple and parameterized text/binary extended
  queries plus single-shard transactions. DDL, `COPY`, broad type coverage,
  and general PostgreSQL session semantics remain unavailable.
- PostgreSQL operates only on an imported or registered catalog. It does not
  provide an online `CREATE TABLE` workflow or full PostgreSQL compatibility.
- General cross-shard transactions are unsupported. Global ordering,
  pagination, and aggregation pushdown are incomplete, and BriskDB does not
  claim full SQL compatibility.
- Backups require every server and embedder to stop first. The supported
  procedure is a complete data-directory copy. Online backup/restore,
  resharding, and online rebalance are unsupported.
- Global indexes remain experimental and their release-gate measurements do
  not establish production latency or durability claims for the database as a
  whole.
- The Python package does not claim DB-API 2.0 or broad MongoDB compatibility.
  Its native document API is limited to the
  collection/index/insert/find/count/delete engine slice: collection and index
  metadata, `insert_one`, empty-filter or exact-`_id` `find` and
  `count_documents`, and exact-`_id` `delete_one`. General matchers, updates,
  aggregation, and retained document cursors remain unavailable.

## Storage compatibility

There is no stable pre-1.0 on-disk compatibility promise. This release writes
manifest version 14 and accepts the exact documented legacy version-1 shape and
manifest versions 2 through 13 for automatic, ordered forward migration.
Unknown, malformed, partially migrated, or newer layouts fail closed.

Before opening existing data, stop every process and make a complete backup as
described in `docs/OFFLINE_BACKUP.md`. Startup may migrate the data. In-place
downgrade is unsupported; rollback requires restoring the complete pre-upgrade
backup. Alpha 6 changes the on-disk format from alpha 5.

# BriskDB 0.1.0-alpha.5

Alpha 5 lets independently started BriskDB server, Rust, and Python processes
safely share one ready data root on the same machine. It keeps the embedded
library, Python wheels, HTTP/PostgreSQL server, and Debian service from alpha 4.
This remains an evaluation and development release, not a production claim.

## Multiple processes, one data root

- Reads and autocommit writes may overlap through SQLite WAL, including traffic
  to the same shard. Normal writer contention can return retryable `Busy`.
- Native and manifest-leased hi/lo generated IDs remain unique across
  independently started processes.
- Passive checkpoints may overlap. A competing checkpoint can report `busy`
  with unavailable frame counts through the new `counts_available` field.
- Schema, catalog, generated-table DDL, initialization, upgrade, and recovery
  require sole-process ownership and return retryable `Busy` before mutation
  while another process has the root open.

The supported boundary is one Linux or macOS host and one local filesystem.
Every process must open its own handle after it starts. Inherited live handles
after `fork()`, NFS/SMB, cloud-synchronized folders, multi-host volumes, object
storage, and online backup remain unsupported. The exact contract is in
`docs/MULTIPROCESS.md`.

Rust subprocess tests cover same- and cross-shard traffic, checkpoints,
generated IDs, forced contention and retry, abrupt writer exit, final SQLite
integrity, and a service sharing its root with an embedder. Installed-wheel
tests repeat the public Python contract with spawned interpreters.

## Install from PyPI

Compiler-free `cp39-abi3` wheels support CPython 3.9 through 3.14 on Linux
x86-64/ARM64 (`manylinux_2_28`) and macOS Intel/Apple Silicon (macOS 11+):

```bash
python -m pip install briskdb==0.1.0a5
```

```python
import briskdb

with briskdb.connect("./data", shards=4) as db:
    with db.session(routing_key="account-1") as session:
        session.migrate(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"
        )
        session.execute("INSERT INTO notes VALUES (?1, ?2)", [1, "hello"])
        print(session.query("SELECT body FROM notes WHERE id = ?1", [1]))
```

The typed package includes synchronous context managers and an asyncio facade.
It runs the native Rust engine in the Python process without a listener,
subprocess, signal handler, or global logger. Async task cancellation reaches
the engine's native cancellation token.

## Embedded Rust library

The root crate separates its listener-free engine from optional HTTP,
PostgreSQL, importer, and CLI layers. Downstream Rust applications can select
only the embedded API:

```toml
[dependencies]
briskdb = { git = "https://github.com/schapman1974/briskdb", tag = "v0.1.0-alpha.5", default-features = false, features = ["embedded"] }
```

`BriskDb` and owned `BriskSession` handles expose initialization, migrations,
prepared statements, routed SQL execution, checkpoints, cancellation,
deadlines, bounded results, and graceful close without installing process-wide
runtime behavior.

## Standalone distributions

The release also provides `briskdb` and `briskdb-import` archives for Ubuntu
24.04 x86-64/ARM64 and macOS Intel/Apple Silicon. Linux assets include systemd
`.deb` packages for `amd64` and `arm64`. Each package installs an unprivileged
`briskdb` account, administrator configuration under `/etc/default/briskdb`,
persistent state under `/var/lib/briskdb`, and journald logging.

The disabled-by-default PostgreSQL listener supports one registered-table
simple-query `SELECT`, `INSERT`, `UPDATE`, or `DELETE` statement at a time.
Psycopg 3 clients must use `psycopg.ClientCursor`; the ordinary cursor uses the
unsupported extended-query protocol. See `docs/POSTGRES_QUICKSTART.md`.

Every native archive and wheel is built and smoke-tested on its matching native
GitHub runner. Wheels are installed and tested under CPython 3.9 and 3.14, and
native dependencies are audited. Verify downloads against `SHA256SUMS` and the
GitHub build-provenance attestation.

## Critical alpha boundaries

- There is no authentication, authorization, or TLS. HTTP and PostgreSQL are
  restricted to loopback. Do not expose either listener to a network.
- PostgreSQL extended-query protocol is unsupported. Parameters sent through
  Parse/Bind/Execute, server-side prepared statements, transactions, DDL,
  `COPY`, and binary results are unavailable. Psycopg must use
  `psycopg.ClientCursor`.
- PostgreSQL accepts exactly one simple-query statement per message and only
  operates on an offline imported/registered catalog. It does not provide an
  online `CREATE TABLE` workflow or full PostgreSQL compatibility.
- General cross-shard transactions are unsupported. Global ordering,
  pagination, and aggregation pushdown are incomplete, and BriskDB does not
  claim full SQL compatibility.
- Backups require every server and embedder to stop first. The supported
  procedure is a complete data-directory copy. Online backup/restore,
  resharding, and online rebalance are unsupported.
- There is no production metrics or observability suite.
- The Python package does not claim DB-API 2.0 compatibility, transaction
  methods, retained SQLite streaming cursors, or native document operations.

## Storage compatibility

There is no stable pre-1.0 on-disk compatibility promise. This release writes
manifest version 12 and accepts the exact documented legacy version-1 shape and
manifest versions 2 through 11 for automatic, ordered forward migration.
Unknown, malformed, partially migrated, or newer layouts fail closed.

Before opening existing data, stop every process and make a complete backup as
described in `docs/OFFLINE_BACKUP.md`. Startup may migrate the data.
In-place downgrade is unsupported; rollback requires restoring the complete
pre-upgrade backup. This release has no on-disk format change from alpha 1,
alpha 2, alpha 3, or alpha 4.
