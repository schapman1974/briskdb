"""Isolated public-API Mongo benchmark worker; never a production dependency."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import importlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import sys
import threading
import time


SOURCE_MODULES = {
    "tinymongo": "d372699407b46a7abefb5bb132d99d4645f3e7bfddc73213c1fe3fccbac476e0",
    "table_backends": "b16dbc8c435a639d85c29d857f8487b2c88d2eef10969a9e412d8afce02898a1",
    "sharded_sqlite": "c89aeeecb69ee2116c50d144f3023d5929e8b5778b1f54c2b9fe2cc3605e4445",
    "indexes": "91001ac8d89a89eed65555ebe8196de8345735b15aeefeaa18851619e3519ca6",
    "bson_codec": "4830400569176fb7f7144844487cabec52be87820b65aa0a7c1b3b5d7fa55617",
}
SOURCE_PYTHON_SHA256 = "f05599c84725b33b79371110e9dcdf6baf81192fbab586939d4625275640dd4c"


def checksum(rows):
    return sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def document(number):
    return {"_id": number, "bucket": number % 7, "value": number, "payload": "x" * 128}


def connect(backend, location, shards, workers):
    if backend == "tinymongo":
        import tinymongo

        package_root = Path(tinymongo.__file__).parent
        files = {path.relative_to(package_root).as_posix(): sha256(path.read_bytes()).hexdigest()
                 for path in sorted(package_root.rglob("*.py"))}
        if len(files) != 19 or checksum(files) != SOURCE_PYTHON_SHA256:
            raise ValueError("TinyMongo benchmark package differs from the locked Python runtime")
        for name, expected in SOURCE_MODULES.items():
            module = importlib.import_module("tinymongo." + name)
            if sha256(Path(module.__file__).read_bytes()).hexdigest() != expected:
                raise ValueError("TinyMongo benchmark source is not the locked oracle: " + name)
        return tinymongo.TinyMongoClient(location, backend="sqlite-sharded", sqlite_shards=shards)
    if backend == "briskdb":
        import briskdb

        if any(name == "tinymongo" or name.startswith("tinymongo.") for name in sys.modules):
            raise ValueError("candidate process must not import TinyMongo")
        return briskdb.MongoClient(location, shards=shards, maxPoolSize=workers,
                                  serverMonitoringMode="poll")
    if backend == "mongodb":
        from pymongo import MongoClient

        return MongoClient(location, maxPoolSize=workers, retryWrites=False,
                           serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                           socketTimeoutMS=15000, serverMonitoringMode="poll")
    raise ValueError("unknown benchmark backend")


def cleanup_owned_database(client, config):
    database = client[config["database"]]
    if not database.list_collection_names():
        return
    claim = database["_briskdb_benchmark_owner"].find_one({"_id": "owner"})
    if claim != {"_id": "owner", "token": config["owner"]}:
        raise ValueError("refusing to drop a namespace without the exact benchmark ownership claim")
    client.drop_database(config["database"])


def run(config):
    backend, location = config["backend"], config["location"]
    count, operations, workers = config["documents"], config["operations"], config["workers"]
    if (not re.fullmatch(r"briskdb_bench_[0-9a-f]{32}", config["database"])
            or not re.fullmatch(r"[0-9a-f]{32}", config["owner"])):
        raise ValueError("benchmark database must use its owned namespace")
    if config.get("cleanup_only"):
        if backend != "mongodb":
            raise ValueError("explicit cleanup only applies to the owned MongoDB namespace")
        client = connect(backend, location, config["shards"], workers)
        try:
            cleanup_owned_database(client, config)
        finally:
            client.close()
        return {"cleaned": config["database"]}
    if backend != "mongodb" and Path(location).exists():
        raise ValueError("benchmark storage must be a new owned directory")
    samples = {}

    def measure(name, units, action, check):
        start = time.perf_counter_ns()
        result = action()
        elapsed = time.perf_counter_ns() - start
        check(result)  # Verify every result, outside the timed operation.
        samples.setdefault(name, []).append({"elapsed_ns": elapsed, "units": units})
        return result

    def equal(expected):
        def check(actual):
            if actual != expected:
                raise AssertionError("benchmark result mismatch")
        return check

    started = time.perf_counter_ns()
    client = connect(backend, location, config["shards"], workers)
    startup_ns = time.perf_counter_ns() - started
    cleanup_database = False
    try:
        database = client[config["database"]]
        if database.list_collection_names():
            raise ValueError("refusing to modify an existing benchmark namespace")
        if backend == "mongodb":
            database["_briskdb_benchmark_owner"].insert_one({"_id": "owner", "token": config["owner"]})
        cleanup_database = True
        items = database.records
        seed = [document(number) for number in range(count)]
        measure("seed_insert_many", count, lambda: items.insert_many(seed),
                lambda result: equal(list(range(count)))(result.inserted_ids))
        equal(count)(items.count_documents({}))
        measure("build_index", 1, lambda: items.create_index("bucket"), equal("bucket_1"))
        for index in range(min(16, operations)):
            equal(document(index % count))(items.find_one({"_id": index % count}))
        for index in range(operations):
            identifier = (index * 7919 + 17) % count
            measure("point_read", 1, lambda: items.find_one({"_id": identifier}), equal(document(identifier)))
            bucket = index % 7
            expected = [{"_id": i} for i in range(count) if i % 7 == bucket]
            measure("indexed_equality", 1,
                    lambda: list(items.find({"bucket": bucket}, {"_id": 1}).sort("_id", 1)), equal(expected))

        def stream():
            cursor = items.find({})
            # TinyMongo has no transport batch_size API. Its iteration is timed
            # as provided, not mislabeled as a server getMore implementation.
            if backend != "tinymongo":
                cursor = cursor.batch_size(64)
            try:
                seen = total = 0
                for row in cursor:
                    seen += 1
                    total += row["_id"]
                return seen, total
            finally:
                cursor.close()

        pipeline = [{"$group": {"_id": "$bucket", "n": {"$sum": 1}, "total": {"$sum": "$value"}}},
                    {"$sort": {"_id": 1}}]
        groups = [{"_id": bucket, "n": len(range(bucket, count, 7)),
                   "total": sum(range(bucket, count, 7))} for bucket in range(7)]
        window = [{"_id": i} for i in range(count - 1, -1, -1) if i % 7 in (1, 3, 5)][3:8]
        for _ in range(max(1, operations // 8)):
            measure("scan", count,
                    lambda: list(items.find({"value": {"$gte": count // 2}}).sort("_id", 1)),
                    equal(seed[count // 2:]))
            measure("group_aggregation", count, lambda: list(items.aggregate(pipeline)), equal(groups))
            measure("cursor_iteration", count, stream, equal((count, sum(range(count)))))
            measure("scatter_sorted_window", 1,
                    lambda: list(items.find({"bucket": {"$in": [1, 3, 5]}}, {"_id": 1})
                                 .sort("value", -1).skip(3).limit(5)), equal(window))

        expected_rows = [dict(row) for row in seed]
        for index in range(operations):
            row = document(count + index)
            measure("point_insert", 1, lambda: items.insert_one(row),
                    lambda result: equal(row["_id"])(result.inserted_id))
            measure("point_delete", 1, lambda: items.delete_one({"_id": row["_id"]}),
                    lambda result: equal(1)(result.deleted_count))
            identifier = index % count
            measure("point_update", 1, lambda: items.update_one({"_id": identifier}, {"$inc": {"value": 1}}),
                    lambda result: equal((1, 1))((result.matched_count, result.modified_count)))
            expected_rows[identifier]["value"] += 1

        start_gate = threading.Barrier(workers)

        def write(worker):
            start_gate.wait(timeout=30)
            for index in range(operations):
                row = document(count + operations + worker * operations + index)
                equal(row["_id"])(items.insert_one(row).inserted_id)

        def concurrent_writes():
            # Thread startup/barrier/join are included, the same for every backend.
            with ThreadPoolExecutor(max_workers=workers) as pool:
                return list(pool.map(write, range(workers)))

        measure("threaded_insert_wave", workers * operations, concurrent_writes, equal([None] * workers))
        expected_rows.extend(document(count + operations + offset) for offset in range(workers * operations))
        actual = list(items.find({}).sort("_id", 1))
        equal(expected_rows)(actual)
        digest = checksum(actual)
        metadata = {"python": platform.python_version(), "platform": platform.system(),
                    "release": platform.release(), "architecture": platform.machine(),
                    "startup_ns": startup_ns, "transport": "in-process" if backend == "tinymongo" else "loopback-wire"}
        if backend == "tinymongo":
            metadata.update(version=importlib.metadata.version("tinymongo"), source_modules=SOURCE_MODULES,
                            package_python_sha256=SOURCE_PYTHON_SHA256,
                            pymongo=importlib.metadata.version("pymongo"),
                            source_commit="53cbf44e98b8caa036163725d195fd29592e1cc0")
        elif backend == "briskdb":
            import briskdb._briskdb as native
            import briskdb.mongo as wrapper

            package_root = Path(wrapper.__file__).parent
            files = {path.relative_to(package_root).as_posix(): sha256(path.read_bytes()).hexdigest()
                     for path in sorted(package_root.rglob("*.py"))}
            metadata.update(version=importlib.metadata.version("briskdb"),
                            pymongo=importlib.metadata.version("pymongo"),
                            native_sha256=sha256(Path(native.__file__).read_bytes()).hexdigest(),
                            client_sha256=sha256(Path(wrapper.__file__).read_bytes()).hexdigest(),
                            package_python_sha256=checksum(files))
        else:
            info = client.admin.command("buildInfo")
            metadata.update(version=info["version"], git_version=info["gitVersion"],
                            pymongo=importlib.metadata.version("pymongo"),
                            storage=client.admin.command("serverStatus")["storageEngine"]["name"])
        return {"backend": backend, "metadata": metadata, "samples": samples,
                "final_count": len(actual), "final_sha256": digest}
    finally:
        try:
            if backend == "mongodb" and cleanup_database:
                cleanup_owned_database(client, config)
        finally:
            client.close()


if __name__ == "__main__":
    print(json.dumps(run(json.load(sys.stdin)), sort_keys=True))
