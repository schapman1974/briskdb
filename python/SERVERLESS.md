# Warm-handler quickstart

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

The path must be writable, durable for the desired lifetime, and owned by one
coordinated BriskDB instance. `/tmp` is suitable only for disposable data.
Independent autoscaled instances pointed at independent local files are
independent databases.

This is an embedded warm-handler pattern, not a production serverless storage
claim. Atomic object-store snapshots, restore fencing, provider adapters, and
multi-instance coordination remain tracked in issues #194–#196. Do not upload
live SQLite/WAL files individually as a backup.
