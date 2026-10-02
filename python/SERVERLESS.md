# Serverless patterns

## Optional S3/Parquet overlay: fresh database per request

An experimental Unix source build with `s3-overlay` provides a
separate mode with ISAM metadata, immutable SQLite bases on shared storage,
and pending Parquet changes in S3. It does not switch normal BriskDB databases
to object storage. Use the [main README example](../README.md#optional-s3parquet-write-overlay-experimental-source-builds)
and [Lambda handler](examples/s3_overlay_lambda.py). The handler closes its
database before responding, and writes are acknowledged only after durable
S3 publication. It never depends on Lambda continuing after its response.

Provision the overlay once, then configure these **trusted deployment settings**:

```text
BRISKDB_STORAGE_MODE=s3-overlay
BRISKDB_OVERLAY_ROOT=/mnt/shared/my-overlay
BRISKDB_OVERLAY_PARQUET_PRUNING=true
BRISKDB_OVERLAY_READ_ONLY=false
```

The last two flags are optional (defaults shown), accept only exact `true` /
`false`, and are read by `Database.from_env()`, not by ordinary database opens.
Use `true` for read-only deployments; that rejects mutation/compaction in the
native layer, but does not replace scoped AWS permissions. S3 location/schema
come from the persisted catalog; no bucket/prefix override is accepted on reopen.

```python
from briskdb.s3_overlay import Database

def handler(event, _context):
    with Database.from_env() as db:
        rows = db.query("SELECT * FROM events WHERE id = ?", [event["id"]]).rows
        return {"rows": rows}
```

Keep bucket, root, SQL structure and flags out of untrusted request parameters.
No database handle is retained after the response. Creation-only shard,
partition, compaction and publication-retry flags are listed in the
[configuration reference](../README.md#selecting-and-configuring-this-mode).

The SQLite reader uses advisory per-partition ISAM min/max/Bloom summaries to
skip pending files for compatible primary-key equality filters. `db.read_stats()`
reports actual files read/skipped and Parquet bytes read. Disable with
`db.set_parquet_pruning(False)` for a controlled comparison. Each request still
opens its own ISAM snapshot; no warm database handle is required. Missing or
busy summaries safely fall back to payload reads. This is separate from the
optional DuckDB reader and does not accelerate a partition with no pending files.

For scheduled compaction, invoke one table/partition per event and cover all
partitions. The same operation can be called from cron or an administrative
request. A repeated compaction is safe; uncertain user writes must not be
blindly retried. Configure caller/runtime deadlines, logging and alarms around
these jobs. No recurring AWS schedule is enabled automatically by installing
the wheel or creating an overlay database.

The mode retains old SQLite bases and S3 objects for active-reader safety;
automatic reclamation is not yet supported. Queries can span partitions but
writes cannot, and neither global transactions nor Mongo adapters are enabled.
See the README for the exact limits. The older ordinary-engine examples below
are distinct from this mode and do not acquire its storage behavior.

To experiment with DuckDB reads, build with `duckdb-reader` and
provision the pinned native library/SQLite extension described in the
[README](../README.md#optional-duckdb-reader-experimental). The example handler
opts in only with `BRISKDB_OVERLAY_READER=duckdb`, trusted absolute
`BRISKDB_DUCKDB_LIBRARY` and `BRISKDB_DUCKDB_SQLITE_EXTENSION` paths, and optional
`BRISKDB_DUCKDB_THREADS`. Its default is twice the visible CPU count (maximum
16), which is not a claim of twice Lambda's actual CPU capacity. DuckDB opens
and closes inside each read request. Writes/compaction are unchanged; this
reader is partition-scoped, not a replacement for arbitrary cross-table SQL.

## Ordinary-engine warm-handler quickstart

BriskDB can run inside one long-lived function/container process because the
Python package starts no listener or subprocess. Keep one database handle warm
and create one session per request:

```python
import briskdb

_database = briskdb.connect("/mnt/persistent/briskdb", shards=4)

def handler(event, _context):
    account_id = str(event["account_id"])
    with _database.session(routing_key=account_id) as session:
        result = session.query(
            "SELECT body FROM notes WHERE id = ?1",
            [int(event["id"])],
            timeout_ms=2_000,
        )
        return {"rows": result["rows"]}
```

The path must be writable and durable for the desired lifetime. `/tmp` is
suitable only for disposable data. Independently spawned processes on one host
may share one local path under the [multi-process contract](../docs/MULTIPROCESS.md).
Independent autoscaled instances pointed at independent local files remain
independent databases.

This is an embedded warm-handler pattern, not a production serverless storage
claim. A shared network mount or object store does not become safe because
local multi-process locking exists. Atomic snapshots, provider adapters, and
multi-host fencing remain tracked in issues #194–#196. Do not upload live
SQLite/WAL files individually as a backup.

## Isolated EFS source-test runner (not public NFS support)

The first fixed-routing EFS experiment is available as an explicitly ignored
Rust test. It uses the real BriskDB SQL/document storage paths, the internal
NFS rollback profile (`PERSIST`/`EXTRA`), and one retained, exclusive root lock
covering **open → work → close**. SQLite's native locks stay enabled. Database
files and rollback journals remain on EFS; there is no local database copy.
Ordinary Python, Rust and CLI NFS opens still fail closed. This is **not** a
wheel/Lambda adapter, a concurrent-shard implementation, or a production support
claim. Issues [#512](https://github.com/schapman1974/briskdb/issues/512),
[#513](https://github.com/schapman1974/briskdb/issues/513) and
[#516](https://github.com/schapman1974/briskdb/issues/516) remain open.

### Target and safety requirements

- Obtain approval for **two independent Linux hosts and a new disposable
  directory** on the same EFS filesystem. Different processes/containers on
  one host are not independent NFS clients. Never use an application directory.
- Both hosts must use Linux NFSv4.1 with `rw,hard,proto=tcp,local_lock=none`.
  The runner checks the actual directory descriptor's mount ID in Linux
  `fdinfo`/`mountinfo`, rejecting local/overlaid mounts and conflicting settings.
  This does not authenticate the EFS filesystem identity: separately record
  mount target/access point, EFS configuration, security groups and client
  identity. Use approved encrypted-at-rest storage and TLS mounts with
  least-privilege access. Do not change production mount settings for this test.
- Use the same effective file-owner UID on both hosts. The root must already
  exist, be owner-only (`0700`), and initially be empty. Its exact basename is
  `briskdb-efs-test-RUN`. Initialization will not adopt existing files. A durable
  marker binds the run/filesystem declaration and root/retained-lock inodes.
  Never remove or replace the marker, lock, or root while a test could be alive.
- EFS locking is **advisory**, not storage-enforced stale-writer fencing. This
  first test must not pause/freeze hosts, sever networking, remount, manipulate
  leases, or force-unlock. Those require the separate recovery qualification.
  The contention budget cannot interrupt a kernel I/O stall on a hard mount.

Sources: [AWS EFS mounting guidance](https://docs.aws.amazon.com/efs/latest/ug/mounting-fs-old.html),
[EFS locking limits](https://docs.aws.amazon.com/efs/latest/ug/limits.html),
[Linux flock and NFS semantics](https://man7.org/linux/man-pages/man2/flock.2.html).

### Build on each host's local disk

Use the **same reviewed commit**, Rust 1.85 or newer, a C build toolchain,
Clang/libclang (for SQLite bindings), pkg-config and Python 3 on both hosts.
Keep source, Cargo output and logs on local disk; only
the disposable database root belongs on EFS. The following commands use Bash.

```bash
export BRISKDB_EFS_ARTIFACTS="$(mktemp -d /tmp/briskdb-efs-artifacts.XXXXXX)"
git rev-parse HEAD > "$BRISKDB_EFS_ARTIFACTS/commit.txt"
git status --porcelain > "$BRISKDB_EFS_ARTIFACTS/worktree.txt"
test ! -s "$BRISKDB_EFS_ARTIFACTS/worktree.txt" # require a clean checkout
rustc -Vv > "$BRISKDB_EFS_ARTIFACTS/rustc.txt"
CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2 cargo test --locked --all-features \
  --lib --no-run --message-format=json > "$BRISKDB_EFS_ARTIFACTS/build.json"
export BRISKDB_EFS_TEST_BIN="$(python3 -c '
import json, sys
paths = [item["executable"] for line in sys.stdin
         if (item := json.loads(line)).get("reason") == "compiler-artifact"
         and item.get("executable") and item["target"]["name"] == "briskdb"
         and "lib" in item["target"]["kind"] and item["profile"]["test"]]
assert len(paths) == 1, paths
print(paths[0])
' < "$BRISKDB_EFS_ARTIFACTS/build.json")"
test -x "$BRISKDB_EFS_TEST_BIN"
sha256sum "$BRISKDB_EFS_TEST_BIN" > "$BRISKDB_EFS_ARTIFACTS/binary.sha256"
```

### Run the bounded experiment

Choose a fresh shared run name and an approved EFS mount path; the values below
are placeholders, not deployment targets. Create the root **once on host A**
with `mkdir -m 700` (without `-p`), only after confirming the existing parent is
the approved EFS mount. It should then be visible on B. Set these on both hosts,
with `BRISKDB_EFS_TEST_CLIENT=B` on host B:

```bash
export BRISKDB_EFS_TEST_ACK=disposable-no-production-data
export BRISKDB_EFS_TEST_RUN=YOUR-UNIQUE-RUN
export BRISKDB_EFS_TEST_FILESYSTEM=fs-YOUR-APPROVED-FILESYSTEM
export BRISKDB_EFS_TEST_ROOT="/mnt/efs/briskdb-efs-test-$BRISKDB_EFS_TEST_RUN"
export BRISKDB_EFS_TEST_CLIENT=A
export BRISKDB_EFS_TEST_ROWS=20
export BRISKDB_EFS_TEST_WAIT_MS=60000
export BRISKDB_EFS_TEST_HOLD_MS=30000

set -o pipefail
efs_run() {
  local log
  log="$(mktemp "$BRISKDB_EFS_ARTIFACTS/$BRISKDB_EFS_TEST_CLIENT-$1.XXXXXX")" || return
  BRISKDB_EFS_TEST_ACTION="$1" "$BRISKDB_EFS_TEST_BIN" \
    storage::profile::efs_qualification::efs_qualification \
    --ignored --exact --nocapture --test-threads=1 2>&1 | tee "$log"
}
```

1. On A, run `efs_run init`. Require success; do not reinitialize after failure.
2. On A, run `efs_run hold`. While its `held` line is visible, run
   `efs_run probe` on B. Require `busy-as-required`. Repeat with B holding and
   A probing. Compare the logged **boot IDs**: they must differ. A probe after
   the holder finishes must fail, not produce false exclusion evidence.
3. Start `efs_run write` once on A and once on B, overlapping them. The root
   lock serializes their scopes; the 60-second contention allowance includes
   waiting for a peer's entire scope, not just one statement. Both must
   complete successfully; save any
   failure rather than rerunning the write. Then run `efs_run verify` on each.
   This checks every expected SQL value, deleted transient rows, document
   content, exact SQL row counts, both shards' integrity, and absence of WAL/SHM.
4. On A, run the following controlled process exit. A status of **73** and the
   named boundary log are required; this intentionally skips SQLite cleanup
   after flushing an uncommitted transaction with a nonempty journal header:

   ```bash
   if efs_run crash-before-commit; then
     echo "ERROR: expected process exit 73"; false
   else
     test "$?" -eq 73
   fi
   ```

   After A exits, run `efs_run verify` on B. All earlier records must survive;
   the uncommitted sentinel must be absent after native hot-journal recovery.
5. On A, similarly invoke `efs_run crash-after-commit` and require exit 73.
   Then run `efs_run verify-committed` on B and A. The sentinel must now exist
   exactly once with value 99. Do not repeat the crash/write actions on this run.

**Stop on the first unexpected result.** A write scope contains multiple
commits, so a failure can leave partial work. There is no automatic write retry
or outcome reconciliation in this runner. Preserve the root and both hosts'
logs for investigation; do not delete locks or retry until a result looks good.
Passing local process tests is not evidence that the cloud sequence passed.

### Evidence, costs and remaining gates

Keep the commit, clean-worktree/build/binary records, both boot IDs, kernel,
architecture, actual mount options, declared/independently verified EFS identity,
access point/configuration and all action logs. `open_us` and `crud_us` are
elapsed microseconds, not NFS RPC counts. Capture per-mount client NFS counters
before/after on isolated clients to investigate metadata noise; unrelated I/O
can contaminate those counters. Set performance thresholds before evaluating
results. This small correctness run alone establishes no latency/throughput SLA.

The runner creates no AWS resources and does not alter mounts, IAM, security
groups or infrastructure. Existing compute and EFS I/O/storage can incur cost.
After retaining evidence and verifying both hosts have stopped, the operator
may remove **only the exact disposable root created for this run**; no automatic
cloud/data cleanup is performed.

Still unqualified: lock loss/reclaim/frozen writers, parallel shard progress,
document updates/deletes/indexes on EFS, global uniqueness/index workers,
security-state and backup/restore cloud behavior, generated/uncertain insert
identities, insert-only shard selection, concurrent startup stress, metadata
RPC/performance targets, and the public warm Lambda lifecycle. Keep the public
NFS gate closed until the relevant issue contracts are met.
