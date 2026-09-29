"""Synthetic store-reopen benchmark for #550; no timing assertions.

Uses temporary local stores and real async PyMongo clients. Each sample opens a
fresh engine and runs an empty-namespace query; client close and fixture growth
are outside the timer. OS caches are not flushed, so this measures warm-cache
reopens, not cold-disk or process-start latency.
"""
import argparse
import asyncio
import importlib.metadata
import json
from pathlib import Path
import statistics
import tempfile
import time

from briskdb import mongo
from pymongo import IndexModel


async def measure(folder, shards):
    started = time.perf_counter()
    client = mongo.AsyncMongoClient(folder=folder, shards=shards)
    try:
        assert await client.probe.empty.find_one({}) is None
        return time.perf_counter() - started
    finally:
        await client.close()


async def run(args):
    results = []
    for indexes in args.indexes:
        with tempfile.TemporaryDirectory(prefix="briskdb-store-open-") as folder:
            count = 0
            async with mongo.AsyncMongoClient(folder=folder, shards=args.shards) as client:
                if indexes:
                    await client.data.big.create_indexes([IndexModel(f"k{j}") for j in range(indexes)])
            for payload_mb in args.payload_mb:
                target = payload_mb * 1_000_000 // args.document_bytes
                async with mongo.AsyncMongoClient(folder=folder, shards=args.shards) as client:
                    while count < target:
                        end = min(count + 50, target)
                        await client.data.big.insert_many([
                            {"_id": i, **{f"k{j}": i for j in range(max(indexes, 4))},
                             "body": "x" * args.document_bytes}
                            for i in range(count, end)
                        ])
                        count = end
                    if count:
                        assert (await client.data.big.find_one({"_id": count - 1}))["k0"] == count - 1
                samples = [await measure(folder, args.shards) for _ in range(args.trials)]
                result = dict(secondary_indexes=indexes, documents=count,
                              payload_mb=count * args.document_bytes / 1_000_000,
                              samples_seconds=samples, median_seconds=statistics.median(samples))
                results.append(result)
                print(json.dumps(result), flush=True)
    return dict(label=args.label, briskdb_version=importlib.metadata.version("briskdb"),
                pymongo_version=importlib.metadata.version("pymongo"), shards=args.shards,
                document_bytes=args.document_bytes, trials=args.trials, results=results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="local")
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--document-bytes", type=int, default=100_000)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--payload-mb", type=int, nargs="+", default=[0, 50, 100, 200])
    parser.add_argument("--indexes", type=int, nargs="+", default=[0, 4])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if min(args.shards, args.document_bytes, args.trials) <= 0:
        parser.error("shards, document-bytes and trials must be positive")
    if min(args.payload_mb + args.indexes) < 0 or args.payload_mb != sorted(args.payload_mb):
        parser.error("sizes/index counts must be nonnegative and sizes must be ascending")
    if args.output and args.output.exists():
        parser.error("use a new output path to preserve previous measurements")
    results = asyncio.run(run(args))
    if args.output:
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(results, stream, indent=2)
            stream.write("\n")


if __name__ == "__main__":
    main()
