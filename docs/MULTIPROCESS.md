# Sharing one data directory between processes

BriskDB supports multiple independently started processes opening one ready
data directory on the same Linux or macOS host. The server, Rust library, and
Python wheel use the same locks and SQLite WAL files.

## Supported contract

| Operation | While peers are open |
| --- | --- |
| Reads and autocommit writes | Supported; normal SQLite writer contention can return retryable `Busy` |
| Generated IDs | Supported; native ranges and manifest-leased hi/lo blocks remain unique |
| Non-unique global-index outbox writes | Supported; row and event share one shard transaction, and each shard cursor follows WAL commit order |
| Passive checkpoints | Supported; a competing checkpoint can report `busy` with unavailable frame counts |
| Read-only global-index catalog inspection | Supported; readers observe one complete old or new checksummed snapshot |
| Opening a current `Ready`/`Degraded` root | Supported; startup inspection is serialized |
| Global-index create/lifecycle/remove | Requires sole-process ownership; each checksummed transition is one manifest transaction |
| Schema migration, catalog registration, generated-table DDL, initialization, upgrade, or recovery | Requires sole-process ownership; retryable `Busy` is returned before mutation when a peer is open |
| Offline import or backup/restore | Stop every server and embedder first |

This contract is limited to one host and one local filesystem with working
SQLite and advisory-file locks. NFS, SMB, cloud-synchronized folders, shared
volumes between hosts, and object storage are unsupported. Provider fencing for
serverless storage is separate work tracked by issue #196.

Each process must open its own `Database`, `Engine`, or `BriskDb` after that
process starts. A live handle, SQLite connection, Tokio runtime, or cached
generated-ID lease inherited across `fork()` must not be used. Python code
should use the `spawn` multiprocessing context or start a fresh interpreter.

## Coordination files

Two owner-only regular files live beside `manifest.sqlite`:

```text
.briskdb-process.lock   lifetime shared lease and sole-process mutation fence
.briskdb-startup.lock   startup and recovery serialization
```

They are created with mode `0600`, opened without following symbolic links,
and released by the kernel when a process exits, including an abrupt exit. The
files may remain after shutdown; lock ownership is not stored in their bytes.
Do not delete, replace, or edit them while any BriskDB process is running.

The lock order is startup lock, process mutation fence when needed, in-process
schema gate, manifest transaction, then shard work. Steady-state data requests
hold only the lifetime shared process lease plus ordinary SQLite locks.

## Python processes

Create the schema before workers start, then let each spawned process reopen
the same path:

```python
from multiprocessing import get_context
import briskdb

def worker(path, account_id):
    with briskdb.open(path, shards=4) as db:
        with db.session(routing_key=account_id) as session:
            session.execute(
                "INSERT INTO jobs (id, body) VALUES (?1, ?2)",
                [1, "ready"],
            )

if __name__ == "__main__":
    context = get_context("spawn")
    workers = [context.Process(target=worker, args=("./data", str(i)))
               for i in range(2)]
    for process in workers:
        process.start()
    for process in workers:
        process.join()
```

Every worker owns and closes its own handle. If a process receives `Busy`,
retry the same autocommit operation with bounded exponential backoff and
jitter. Do not automatically retry non-retryable errors.

## systemd service plus an embedded app

The Debian package runs the service as Unix user and group `briskdb`. Because
the coordination files are owner-only, an embedded app sharing
`/var/lib/briskdb/data` must run with the same effective UID:

```bash
sudo -u briskdb /opt/myapp/bin/worker
```

Configure both processes with the exact same data path and shard count. A
supplementary group alone is not enough for the `0600` lock files. Separate
service accounts are outside the current support contract.

Apply schema/catalog changes during a maintenance window after stopping every
peer. The same all-process stop is required before copying or restoring the
directory. See [offline backup](OFFLINE_BACKUP.md) and the
[Debian/systemd guide](DEBIAN_INSTALL.md).

## Automated evidence

`tests/multiprocess_shared_root.rs` covers overlapping same- and cross-shard
traffic, pooled handles, checkpoints, generated IDs, retryable contention,
global-index writer fencing, ordered outbox writers and atomic read-only
catalog snapshots, abrupt termination, reopen/integrity validation, and one
service plus one embedder.
`python/tests/test_multiprocess.py` repeats the public wheel contract with
independently spawned interpreters.
