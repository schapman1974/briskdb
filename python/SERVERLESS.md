# Serverless and S3/Parquet guide

BriskDB's optional S3 overlay is an experimental SQL mode for short-lived
processes. The beta.2 Linux/macOS wheels include its native engine; ordinary
SQLite storage remains the default.

| Mode | Metadata and data | Scope |
| --- | --- | --- |
| Ordinary SQLite | SQLite metadata and writable SQLite shards | Existing local SQL/document APIs |
| Hybrid ISAM metadata | Native ISAM manifest and writable SQLite shards | Experimental SQL-only metadata option; does not enable ordinary NFS opens |
| S3 overlay | ISAM catalog, immutable SQLite bases on shared storage, Parquet changes in S3 | Separate synchronous SQL API; the serverless path described here |

The [README's serverless example](../README.md#example-a-serverless-app) shows
how an application can use API Gateway, Lambda, FastAPI/Mangum, EFS and S3.
BriskDB does not deploy an API or provision AWS resources for you.

## Optional S3/Parquet overlay: fresh database per request

Install `briskdb==0.1.0b2` and provision a **new** root once using the
[creation example](../README.md#build-and-use-the-overlay). For Lambda, package
the wheel for the deployed Linux architecture, not your development Mac.
The Rust `s3-overlay` feature remains opt-in; Python packaging enables the
capability without changing the default storage mode. DuckDB is not bundled.

Configure these trusted deployment settings:

```text
BRISKDB_STORAGE_MODE=s3-overlay
BRISKDB_OVERLAY_ROOT=/mnt/shared/my-overlay
BRISKDB_OVERLAY_PARQUET_PRUNING=true
BRISKDB_OVERLAY_READ_ONLY=false
```

The last two flags are optional (defaults shown) and accept only exact
`true` / `false`. Only `Database.from_env()` reads these variables; ordinary
opens do not. Bucket, region, prefix and schema come from the persisted catalog.

```python
from briskdb.s3_overlay import Database

def handler(event, _context):
    with Database.from_env() as db:
        rows = db.query("SELECT * FROM events WHERE id = ?", [event["id"]]).rows
        return {"rows": rows}
```

Open and close inside each request. A committed write has completed durable
S3 publication before responding; do not depend on Lambda continuing work
after its response. SQL reads include published pending changes immediately,
without waiting for compaction. Keep storage paths, SQL structure and options
out of untrusted request parameters.

The [Lambda example](examples/s3_overlay_lambda.py) handles reads, inserts and
partition-scoped compaction. It is an IAM-invoked example, not a public HTTP
endpoint with application authentication.

### AWS deployment requirements

- Use an EFS access point with appropriate POSIX ownership and permissions.
  Restrict NFS access to the Lambda security group; enable encryption at rest
  and in transit. Follow [Lambda's EFS setup](https://docs.aws.amazon.com/lambda/latest/dg/configuration-filesystem-efs.html)
  and [EFS security guidance](https://docs.aws.amazon.com/efs/latest/ug/security-considerations.html).
- Keep S3 private, block public access, and use encryption and HTTPS.
  Use the Lambda execution role, not stored credentials. Ordinary queries,
  writes and compaction need scoped `s3:GetObject` / `s3:PutObject` access to
  the database prefix; read-only deployments can restrict write permissions.
  See [S3 security guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html).
- Provide S3 connectivity from the function's VPC, such as an S3 gateway
  endpoint. External provider APIs need their own internet-egress path.
- Set request deadlines, failure/queue-age alarms and storage metrics. Enable
  access logging and CloudTrail data events with encrypted log destinations;
  budget for their cost as well as compute, EFS storage/throughput and S3
  storage/requests. See [EFS pricing](https://aws.amazon.com/efs/pricing/) and
  [S3 pricing](https://aws.amazon.com/s3/pricing/).

## Selecting and configuring this mode

`briskdb.open()` / `briskdb.connect()` default to `storage_mode="sqlite"`.
Explicit `storage_mode="s3-overlay"` opens an existing overlay; it never creates
or converts a root implicitly. Its `query()` / `execute()` interface is not
the ordinary `session()`, Mongo, async or network-connector API. Ordinary
`Config`, `shards`, document and UUID options are rejected on overlay opens.
Missing native build support raises `UnsupportedError`, without fallback.

`OpenOptions(parquet_pruning=True, read_only=False)` controls each open.
Read-only mode rejects mutations and compaction in the native layer, but does
not replace AWS/filesystem access controls. `db.settings()` reports the stored
layout and effective flags without credentials.

Creation settings are persisted and cannot be overridden per request:

| Python creation argument | CLI creation flag | Default |
| --- | --- | --- |
| `bucket`, `region`, `prefix`, `tables` | `--bucket`, `--region`, `--prefix`, `--schema-file` | Required |
| `shards` | `--shards` | `4` |
| `partitions` | `--partitions` | `64` |
| `compact_after_files` | `--compact-after-files` | `32` |
| `max_pending_files` | `--max-pending-files` | `64` |
| `write_retry_ms` | `--write-retry-ms` | `60000` |

Shards must be 2–64; partitions 2–256 and divisible by the shard count;
pending files 2–128; compaction threshold 1 through that limit; publication
retry budget 1–120000 ms. Invalid values are rejected. The publication budget
covers head conflicts, not replaying arbitrary SQL or the entire request.

Build CLI tools with `cargo build --features s3-overlay-cli`:

```bash
# Provision once; schema.json contains the README's JSON table list.
briskdb overlay --root /mnt/shared/my-overlay create \
  --bucket my-private-bucket --region us-east-1 --prefix briskdb \
  --schema-file schema.json --shards 4 --partitions 64

briskdb overlay --root /mnt/shared/my-overlay --read-only query \
  --sql 'SELECT * FROM events WHERE id = ?' \
  --params-json '[{"Text":"event-1"}]'

briskdb overlay --root /mnt/shared/my-overlay compact --table events --partition 0
briskdb overlay --root /mnt/shared/my-overlay --parquet-pruning false settings
```

Creation flags except `--schema-file` also accept `BRISKDB_OVERLAY_` plus
their uppercase underscore name; explicit flags win. Python creation takes
explicit arguments. `briskdb-s3-overlay` accepts the same flags without the
`overlay` subcommand, or one JSON command on stdin when invoked without
arguments. These commands close and exit; daemon/listener flags are separate.
The older `experimental-s3-overlay` / `experimental-duckdb-reader` feature
names remain aliases. No overlay feature is enabled by default.

### Parquet pruning

The default SQLite reader uses per-partition ISAM min/max and Bloom summaries
to skip irrelevant pending files for BINARY equality on INTEGER/TEXT/BLOB
primary-key columns. Missing, stale or busy summaries fall back to reading the
payload; old unindexed deltas remain readable but are not retroactively indexed.

```python
with Database("/mnt/shared/my-overlay") as db:
    rows = db.query("SELECT * FROM events WHERE id = ?", ["event-42"]).rows
    print(db.read_stats())
    db.set_parquet_pruning(False)  # optional controlled comparison
```

Counters cover the last SQL scan's files read/skipped and Parquet bytes, not
all publication, retry or compaction I/O. Pruning does not cover arbitrary
non-key/range predicates or the DuckDB reader. Deploy matching binaries:
older experimental builds reject the optional `index_hash` head field.

### Open and immutable-base reuse

Opening reads one shared-lock ISAM catalog snapshot and reuses its validated
root page while loading the catalog. It does not preload SQLite shards or S3
payloads. `db.open_stats()` reports `total_ms`, `root_path_ms`, `catalog_ms`,
`store_client_ms`, `runtime_ms`, and `connection_ms`, plus logical catalog
file-open/root-read/page-read/lock-request counts (not physical NFS RPC counts).
It returns `None` on a handle returned by `Database.create()`.

Each open handle retains at most eight immutable SQLite base connections in
least-recently-used order, keyed by table, partition, and published base ID.
Repeated queries can reuse SQLite's page and prepared-statement caches. Every
new statement still fetches fresh S3 heads; updates and deletes remain visible,
and compaction's new base ID selects a new connection. Closing the handle
releases all retained connections. This works within one Lambda invocation;
it does not depend on keeping Lambda warm or copying EFS data to `/tmp`.

`db.read_stats()` adds `sqlite_base_opens` (successful opens),
`sqlite_base_cache_hits`, and `sqlite_base_cache_evictions` for the last SQL scan.
The CLI includes the same counters and an `open_stats` breakdown alongside
its existing end-to-end `open_ms`. These optimizations apply only to the
opt-in S3-overlay SQLite reader, not normal BriskDB or the DuckDB reader.

## Safe updates and optional durable queue handoff

For a bounded point edit, supply the complete primary key and preserve the
same request/operation ID across retries:

```python
from briskdb.s3_overlay import Database, RetryOptions, UpdateRequest

# Save this request and ID before submitting; do not regenerate on retry.
edit = UpdateRequest("events", {"id": "event-1"},
                     set={"message": "Updated"}, expected={"message": "Hello"})
with Database("/mnt/shared/my-overlay") as db:
    result = db.update(edit, retry=RetryOptions(timeout_ms=1000, max_retries=2))
    print(result["status"])  # committed OR condition_not_met
```

`UpdateRequest` supports field replacement, numeric increments and expected
old-value/version guards. Its default operation ID is 32 lowercase hexadecimal
characters. Reusing the ID with different contents is rejected. Retries keep
the original guards; unrelated-row changes may reuse prepared work, while a
target-row change requires a fresh attempt.

Default retries use jitter up to 20 then 40 ms within a 1-second update budget.
That budget includes storage calls and retry pauses, not database opening or
an uninterruptible filesystem call. It can still expire under contention.
A publication timeout may leave an unknown outcome: reuse the same request,
never a new ID for the same increment. `db.update_status(edit.operation_id)`
returns a confirmed result or `None`; `None` does **not** prove failure.
Ordinary `execute()` does not automatically replay SQL or weaken write concern.

Foreground safe updates do not compact by default. Schedule compaction, or
explicitly allow it in a worker with `RetryOptions(allow_compaction=True)`.

### Optional SQS handoff

Provision a FIFO SQS queue and FIFO dead-letter queue explicitly, install the
`s3-queue` extra (`pip install 'briskdb[s3-queue]==0.1.0b2'`), and opt in:

```python
from briskdb.s3_overlay_queue import QueuedUpdates, SqsUpdateQueue

queue = SqsUpdateQueue(
    "https://sqs.us-east-1.amazonaws.com/123456789012/briskdb-updates.fifo",
    region="us-east-1",
)
with Database("/mnt/shared/my-overlay") as db:
    result = QueuedUpdates(db, queue).submit(edit, mode="quick")
    # committed / condition_not_met / queued. "queued" is NOT "saved".
```

`quick` reserves 250 ms of the default budget for queue submission, without
hidden SDK retries; network/credential setup can exceed that target.
`mode="queued"` skips the foreground write; `mode="committed"` never queues.
Submission failure raises an error rather than reporting success.

Use the [worker example](examples/s3_overlay_writer_lambda.py) for event-source,
permissions, visibility-timeout and alarm requirements. Replacements require
expected old values for every replaced field, or a configured version column
maintained by every writer. The worker preserves those guards and uses native
operation IDs, not just SQS's deduplication window.

Queued work may fail its condition or reach the DLQ. Expose pending/conflict
states, check completion, and monitor queue age and DLQ depth. FIFO groups
serialize a table partition but do not order direct writes outside the queue.
No queue, schedule or background worker is created automatically.

### Format and permission requirements

The first safe update upgrades its partition head to format 2, rejected by
older binaries. Upgrade all readers/writers before using it. Operation claims
and receipts must remain available while retries or redelivery are possible;
receipts survive compaction/reopening and add storage/request cost.

Safe updates/status checks also need `s3:ListBucket` on the bucket so missing
receipts return a distinguishable error, although BriskDB does not list objects.
A 403 is never treated as "not committed." Keep object permissions scoped to
the database prefix; use a dedicated private bucket when necessary. See
[S3's missing-object permission behavior](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

## Compaction and current limits

Schedule one table/partition per invocation and cover all partitions:

```python
with Database("/mnt/shared/my-overlay") as db:
    db.compact("events", 0)  # partitions 0..63 by default
```

Compaction atomically publishes a new base and remaining pending set. Ordinary
writes can trigger it at the configured threshold; safe updates default to
foreground compaction disabled. A repeated compaction is safe; uncertain user
writes must not be blindly retried.

- A modifying statement is atomic within **one table/key partition**.
  Cross-partition mutations fail without partial row commits. Queries can
  join tables, but there is no global multi-partition transaction snapshot.
- IDs are caller-supplied; the routing key must be part of the primary key.
  Inserts route across partitions; updates cannot move between them.
- Online DDL, foreign keys, global unique indexes, SQL transactions,
  Mongo/PostgreSQL adapters and automatic migration are not supported here.
- Queries/partition compactions are bounded to 100,000 rows and 64 MiB of row
  data. A Parquet batch is limited to 10,000 changes and 16 MiB.
- Old SQLite bases, file-index records, merged Parquet objects and S3 versions
  are retained for reader safety. Automatic garbage collection is not
  implemented. Do not modify immutable bases or add age-only deletion rules.
- This is experimental shared-storage functionality, not general production
  EFS qualification. Successful benchmark runs do not establish failure,
  recovery or application-retention guarantees.

## Optional DuckDB reader (experimental)

Build with `duckdb-reader` and separately provide the official DuckDB **1.5.6**
shared library and matching signed `sqlite_scanner` extension for the target
OS/architecture. They are not bundled in the wheel. Only trusted absolute
paths are accepted; runtime extension downloads are disabled.

```python
with Database("/mnt/shared/my-overlay") as db:
    rows = db.query_partition_duckdb(
        "events", "event-1",
        "SELECT id, message FROM events WHERE id = ?", ("event-1",),
        library="/opt/duckdb/libduckdb.so",
        sqlite_extension="/opt/duckdb/sqlite_scanner.duckdb_extension",
        threads=4, memory_mb=256,
    ).rows
```

This API reads **one routed table partition**. Keep the routing predicate in
the SQL. DuckDB scans the immutable SQLite base directly; BriskDB fetches and
verifies pending changes for the merged view. It is not direct DuckDB S3
scanning or a replacement for arbitrary cross-table SQL. Writes are unchanged.

SQL must pass SQLite preflight and DuckDB execution; expressions use DuckDB
semantics. Results support null, signed integers, finite floats, text and blobs;
cast other types. Each call opens/closes DuckDB, with a 120-second interrupt
watchdog, no disk spill and no silent SQLite fallback.

The example handler opts in with `BRISKDB_OVERLAY_READER=duckdb`,
`BRISKDB_DUCKDB_LIBRARY`, `BRISKDB_DUCKDB_SQLITE_EXTENSION` and optional
`BRISKDB_DUCKDB_THREADS`. Its default is twice visible CPUs, capped at 16,
not twice Lambda's CPU quota. The `WITHOUT ROWID` base scan is single-threaded;
see the [measured comparison](../docs/BENCHMARKS.md#optional-duckdb-reader-lambdaefs-2026-10-02)
before assuming a larger thread count helps.

## Ordinary-engine warm-handler quickstart

The [ordinary handler example](examples/serverless_handler.py) retains one
database handle and creates a session per request. Use it for a writable
local path that is durable for the required lifetime; `/tmp` is disposable.
Processes on one host can share a local root under the
[multi-process contract](../docs/MULTIPROCESS.md). Independently autoscaled
instances with separate local paths remain separate databases.

This pattern does not enable ordinary SQLite databases on EFS or object
storage. Do not upload live SQLite/WAL files individually as a backup.

## Isolated EFS source-test runner (not public NFS support)

The [ignored Rust qualification runner](../src/storage/profile/efs_qualification.rs)
is a maintainer-only diagnostic, not a deployment recipe. It runs ordinary
SQL/document paths with internal `PERSIST` / `EXTRA` rollback storage and an
exclusive root lock around open/work/close; SQLite's own locks remain enabled.
Public ordinary-engine NFS opens remain disabled.

Running it requires approved disposable data on two independent Linux
NFSv4.1 clients, the same reviewed build and effective owner UID, and a new
empty owner-only `briskdb-efs-test-RUN` directory. The runner validates
`rw,hard,proto=tcp,local_lock=none` and a retained run marker. Record filesystem,
mount/access-point identity, different host boot IDs, build hashes and logs;
local containers are not independent NFS clients.

Stop at the first unexpected result and preserve evidence. Never delete locks,
reuse a failed root, blindly replay writes, or simulate lease/network loss with
this runner. Its contention budget cannot interrupt hard-mount kernel I/O.
Passing it does not establish stale-writer fencing, recovery under lock loss,
parallel-shard progress or production support. Existing compute/storage costs
apply; it provisions and cleans up no AWS resources.
