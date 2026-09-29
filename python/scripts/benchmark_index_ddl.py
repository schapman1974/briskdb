"""Synthetic #547 reproduction; timings are evidence, not CI pass/fail thresholds.

Run against an installed wheel: python python/scripts/benchmark_index_ddl.py
Every trial uses a fresh temporary store and cleans it up after the client closes.
"""
import argparse
import asyncio
import json
from pathlib import Path
import tempfile
import time

from briskdb import mongo
from pymongo import ASCENDING, IndexModel


async def trial(documents, document_bytes, other_indexes, creating):
    with tempfile.TemporaryDirectory(prefix="briskdb-index-ddl-") as folder:
        async with mongo.AsyncMongoClient(folder=folder) as client:
            big = client["other_db"]["big"]
            if other_indexes:
                await big.create_indexes([
                    IndexModel([(f"k{j}", ASCENDING)], name=f"k{j}_1")
                    for j in range(other_indexes)
                ])
            for start in range(0, documents, 50):
                await big.insert_many([
                    {**{f"k{j}": i for j in range(4)}, "body": "x" * document_bytes}
                    for i in range(start, min(start + 50, documents))
                ])
            empty = client["probe_db"]["empty"]
            models = [IndexModel([(f"f{j}", ASCENDING)], name=f"f{j}_1")
                      for j in range(creating)]
            started = time.perf_counter()
            first = await empty.create_indexes(models)
            first_seconds = time.perf_counter() - started
            started = time.perf_counter()
            repeated = await empty.create_indexes(models)
            repeat_seconds = time.perf_counter() - started
            assert first == repeated == [f"f{j}_1" for j in range(creating)]
            result = dict(documents=documents, document_bytes=document_bytes,
                          other_indexes=other_indexes, creating=creating,
                          first_seconds=first_seconds, repeat_seconds=repeat_seconds)
            print(json.dumps(result), flush=True)
            return result


async def run(args):
    results = [await trial(0, args.document_bytes, 0, 5)]
    for other_indexes in (0, 1, 4):
        for creating in (1, 5):
            results.append(await trial(args.documents, args.document_bytes, other_indexes, creating))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--documents", type=int, default=1000)
    parser.add_argument("--document-bytes", type=int, default=100_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.documents < 0 or args.document_bytes < 0:
        parser.error("sizes must be nonnegative")
    if args.output and args.output.exists():
        parser.error("use a new output path to preserve previous measurements")
    results = asyncio.run(run(args))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
