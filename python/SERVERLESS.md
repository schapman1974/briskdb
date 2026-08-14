# Warm-handler quickstart

BriskDB can run inside one long-lived function/container process because the
Opening the Python package starts no listener or subprocess. Keep one database handle warm
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
