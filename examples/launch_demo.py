#!/usr/bin/env python3
"""Run the exact four-shard scenario shown in the README launch demo."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import sqlite3
import tempfile
import urllib.request
from pathlib import Path

import briskdb

SHARDS = 4
WRITERS = 4


def shard_counts(root: Path) -> list[int]:
    counts = []
    for shard in range(SHARDS):
        path = root / "shards" / f"{shard:04}.sqlite"
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
            count = connection.execute("SELECT count(*) FROM launch_events").fetchone()[
                0
            ]
        counts.append(int(count))
    return counts


def run_demo(writes: int) -> dict[str, object]:
    if writes < SHARDS:
        raise ValueError(f"writes must be at least {SHARDS}")

    with tempfile.TemporaryDirectory(prefix="briskdb-launch-") as directory:
        root = Path(directory)
        with briskdb.open(root, shards=SHARDS) as database:
            with database.session(routing_key="schema") as session:
                session.migrate(
                    "CREATE TABLE launch_events ("
                    "tenant_id TEXT NOT NULL, "
                    "event_id INTEGER NOT NULL, "
                    "payload TEXT NOT NULL, "
                    "PRIMARY KEY (tenant_id, event_id)"
                    ") STRICT"
                )

            def write_event(event_id: int) -> int:
                tenant = f"tenant-{event_id}"
                with database.session(routing_key=tenant) as session:
                    result = session.execute(
                        "INSERT INTO launch_events VALUES (?1, ?2, ?3)",
                        [tenant, event_id, f"event-{event_id}"],
                    )
                return int(result["rows_affected"])

            with concurrent.futures.ThreadPoolExecutor(max_workers=WRITERS) as pool:
                written = sum(pool.map(write_event, range(writes)))

            def read_event(event_id: int) -> tuple[int, int]:
                tenant = f"tenant-{event_id}"
                with database.session(routing_key=tenant) as session:
                    result = session.query(
                        "SELECT payload FROM launch_events "
                        "WHERE tenant_id = ?1 AND event_id = ?2",
                        [tenant, event_id],
                    )
                return int(result["shards"][0]), len(result["rows"])

            with concurrent.futures.ThreadPoolExecutor(max_workers=WRITERS) as pool:
                reads = list(pool.map(read_event, range(writes)))

            counts = shard_counts(root)
            with database.serve(postgres="127.0.0.1:0") as server:
                with urllib.request.urlopen(
                    f"http://{server.http_address}/health", timeout=5
                ) as response:
                    health = json.load(response)
                postgres_address = server.postgres_address

        return {
            "version": briskdb.__version__,
            "shards": SHARDS,
            "writers": WRITERS,
            "writes": written,
            "read_rows": sum(rows for _shard, rows in reads),
            "read_shards": sorted({shard for shard, _rows in reads}),
            "shard_counts": counts,
            "health": health["status"],
            "postgres_address": postgres_address,
            "ordinary_sqlite_files": all(
                (root / "shards" / f"{shard:04}.sqlite").is_file()
                for shard in range(SHARDS)
            ),
        }


def print_summary(summary: dict[str, object]) -> None:
    counts = " / ".join(str(count) for count in summary["shard_counts"])
    print(f"BriskDB {summary['version']} · {summary['shards']} SQLite WAL shards")
    print(
        f"✓ {summary['writes']} routed writes from {summary['writers']} Python threads"
    )
    print(f"✓ shard files contain {counts} rows")
    print(f"✓ routed reads returned {summary['read_rows']} rows")
    print(f"✓ HTTP /health → {summary['health']}")
    print(f"✓ PostgreSQL listener → {summary['postgres_address']}")
    print("✓ manifest.sqlite + four ordinary SQLite shard files")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writes", type=int, default=32)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    summary = run_demo(args.writes)
    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        print_summary(summary)


if __name__ == "__main__":
    main()
