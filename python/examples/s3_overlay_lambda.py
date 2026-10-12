"""IAM-invoked Lambda example; requires an overlay-enabled BriskDB wheel.

Create the root once with Database.create before deploying. Mount shared storage
at BRISKDB_OVERLAY_ROOT, set BRISKDB_STORAGE_MODE=s3-overlay, and grant GetObject/PutObject on the
database prefix. No public HTTP endpoint/authentication is supplied here.

Schedule one {"action":"compact", "table":"events", "partition":N} event for
each partition (0..63 by default). Work completes BEFORE the response. A failed
compaction is safe to retry; do not blindly retry uncertain modifying SQL.
"""
import os
from briskdb.s3_overlay import Database


def handler(event, context):
    # Deployment-owned flags, never storage paths/options from event payloads.
    with Database.from_env() as db:
        action = event["action"]
        if action == "compact":
            return db.compact(event["table"], event["partition"])
        if action == "insert":
            return db.execute("INSERT INTO events (id,message) VALUES (?,?)",
                              (event["id"], event["message"]))
        if action == "get":
            sql, params = "SELECT id,message FROM events WHERE id=?", (event["id"],)
            reader = os.environ.get("BRISKDB_OVERLAY_READER", "sqlite")
            if reader == "duckdb":
                # Trusted deployment settings, never native paths from the event.
                # os.cpu_count reports visible CPUs, not Lambda's CPU quota.
                return db.query_partition_duckdb(
                    "events", event["id"], sql, params,
                    library=os.environ["BRISKDB_DUCKDB_LIBRARY"],
                    sqlite_extension=os.environ["BRISKDB_DUCKDB_SQLITE_EXTENSION"],
                    threads=int(os.environ.get("BRISKDB_DUCKDB_THREADS",
                                               str(min(16, 2 * (os.cpu_count() or 1))))),
                ).rows
            if reader != "sqlite":
                raise ValueError("unknown configured overlay reader")
            return db.query(sql, params).rows
        raise ValueError("unknown action")
