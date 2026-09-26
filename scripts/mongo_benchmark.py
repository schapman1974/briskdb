#!/usr/bin/env python3
"""Reproducible isolated Mongo workload comparison and optional regression gate."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests/mongo_benchmark_client.py"
WORKLOADS = ("seed_insert_many", "build_index", "point_read", "indexed_equality", "scan",
             "group_aggregation", "cursor_iteration", "scatter_sorted_window", "point_insert",
             "point_delete", "point_update", "threaded_insert_wave")


def validate_config(config):
    for name, low, high in (("documents", 16, 100000), ("operations", 1, 10000),
                            ("trials", 1, 20), ("shards", 2, 64), ("workers", 1, 4)):
        if type(config[name]) is not int or not low <= config[name] <= high:
            raise ValueError(f"{name} must be an integer between {low} and {high}")


def validate_mongodb_uri(uri):
    parsed = urlsplit(uri)
    if (parsed.scheme != "mongodb" or parsed.hostname not in ("127.0.0.1", "::1")
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment
            or parsed.port is None or parsed.port == 0):
        raise ValueError("MongoDB reference must be a credential-free literal-loopback URI with an explicit port and no database/options")


def validate_run(run, backend, config):
    if run.get("backend") != backend or set(run.get("samples", {})) != set(WORKLOADS):
        raise ValueError("worker returned an incomplete or wrong benchmark")
    if run.get("final_count") != config["documents"] + config["workers"] * config["operations"]:
        raise ValueError("worker returned the wrong final record count")
    if not isinstance(run.get("final_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", run["final_sha256"]):
        raise ValueError("worker omitted its verified final result digest")
    for name, samples in run["samples"].items():
        expected_count = (1 if name in ("seed_insert_many", "build_index", "threaded_insert_wave")
                          else max(1, config["operations"] // 8) if name in
                          ("scan", "group_aggregation", "cursor_iteration", "scatter_sorted_window")
                          else config["operations"])
        expected_units = (config["documents"] if name in
                          ("seed_insert_many", "scan", "group_aggregation", "cursor_iteration")
                          else config["workers"] * config["operations"] if name == "threaded_insert_wave"
                          else 1)
        if len(samples) != expected_count:
            raise ValueError("workload sample count differs")
        for sample in samples:
            if any(type(sample.get(key)) is not int or sample[key] <= 0 for key in ("elapsed_ns", "units")):
                raise ValueError("invalid measurement")
            if sample["units"] != expected_units:
                raise ValueError("workload unit count differs")


def summarize(runs):
    summaries = {}
    for name in WORKLOADS:
        samples = [sample for run in runs for sample in run["samples"][name]]
        ns_per_unit = sorted(sample["elapsed_ns"] / sample["units"] for sample in samples)
        summaries[name] = {"sample_count": len(samples),
                           "median_ns_per_unit": statistics.median(ns_per_unit),
                           "p95_ns_per_unit": ns_per_unit[math.ceil(0.95 * len(ns_per_unit)) - 1],
                           "total_units": sum(sample["units"] for sample in samples),
                           "total_elapsed_ns": sum(sample["elapsed_ns"] for sample in samples)}
    return summaries


def compare_baseline(report, baseline, maximum):
    if not math.isfinite(maximum) or maximum < 1:
        raise ValueError("maximum regression ratio must be finite and at least 1")
    for key in ("schema_version", "configuration", "host", "worker_sha256", "mongodb_environment"):
        if report[key] != baseline.get(key):
            raise ValueError("baseline does not match " + key)
    if report["configuration"]["trials"] < 3:
        raise ValueError("regression gates require at least three trials")
    if set(report["backends"]) != set(baseline.get("backends", {})):
        raise ValueError("baseline backend set differs")
    checks = []
    expected_digest = None
    for backend, current in report["backends"].items():
        old = baseline["backends"][backend]
        for result in (current, old):
            if len(result.get("trials", [])) != report["configuration"]["trials"]:
                raise ValueError("baseline/current trial count differs")
            for run in result["trials"]:
                validate_run(run, backend, report["configuration"])
                if expected_digest is not None and run["final_sha256"] != expected_digest:
                    raise ValueError("baseline/current final data differs")
                expected_digest = run["final_sha256"]
                identity = {key: value for key, value in run["metadata"].items() if key != "startup_ns"}
                first_identity = {key: value for key, value in result["trials"][0]["metadata"].items() if key != "startup_ns"}
                if identity != first_identity:
                    raise ValueError("runtime changed between trials")
            if result.get("summary") != summarize(result["trials"]):
                raise ValueError("summary does not match raw measurements")
        for key in ("python", "platform", "release", "architecture", "transport", "pymongo"):
            if current["trials"][0]["metadata"].get(key) != old["trials"][0]["metadata"].get(key):
                raise ValueError("baseline runtime differs: " + key)
        if backend != "briskdb" and current["trials"][0]["metadata"]["version"] != old["trials"][0]["metadata"]["version"]:
            raise ValueError("baseline reference version differs")
        for name in WORKLOADS:
            before = old["summary"][name]["median_ns_per_unit"]
            after = current["summary"][name]["median_ns_per_unit"]
            if not all(type(value) in (int, float) and math.isfinite(value) and value > 0
                       for value in (before, after)):
                raise ValueError("baseline/current timing must be positive and finite")
            ratio = after / before
            checks.append({"backend": backend, "workload": name,
                           "ratio": ratio, "passed": ratio <= maximum})
    return {"maximum_ratio": maximum, "passed": all(check["passed"] for check in checks), "checks": checks}


def benchmark(config, interpreters, mongodb_uri=None, work_root=None, timeout=300, mongodb_environment=None):
    validate_config(config)
    if mongodb_uri:
        validate_mongodb_uri(mongodb_uri)
        if not mongodb_environment or len(mongodb_environment) > 1000:
            raise ValueError("describe the MongoDB deployment/storage environment (up to 1,000 characters)")
    backends = list(interpreters)
    if set(backends) != ({"briskdb", "tinymongo", "mongodb"} if mongodb_uri else {"briskdb", "tinymongo"}):
        raise ValueError("both candidate and locked reference interpreters are required")
    report = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
              "configuration": config, "worker_sha256": sha256(WORKER.read_bytes()).hexdigest(),
              "host": {"system": platform.system(), "release": platform.release(),
                       "architecture": platform.machine(), "logical_cpus": os.cpu_count()},
              "backends": {name: {"trials": []} for name in backends}, "order": [],
              "mongodb_environment": mongodb_environment,
              "limits": ["end-to-end public client calls; not equivalent storage or transport implementations",
                         "TinyMongo iteration has no wire batching; wire clients request batches of 64",
                         "threaded writes include thread startup/barrier/join; no automatic retries",
                         "temporary local roots and warm reads; no cold-cache, power-loss or sustained-load claim",
                         "MongoDB deployment/storage environment must be documented with any published result"]}
    digest = None
    # Only exact fresh directories owned by this context are removed afterwards.
    with tempfile.TemporaryDirectory(prefix="briskdb-mongo-benchmark-", dir=work_root) as root:
        for trial in range(config["trials"]):
            order = backends[trial % len(backends):] + backends[:trial % len(backends)]
            report["order"].append(order)
            for backend in order:
                request = dict(config, backend=backend,
                               location=mongodb_uri if backend == "mongodb" else str(Path(root) / f"{backend}-{trial}"),
                               database="briskdb_bench_" + uuid4().hex, owner=uuid4().hex)
                command = [interpreters[backend], "-I", str(WORKER)]
                try:
                    child = subprocess.run(command, input=json.dumps(request), text=True,
                                           capture_output=True, timeout=timeout, cwd=root)
                finally:
                    if backend == "mongodb":
                        # A timed-out worker cannot run its own finally block.
                        # Drop only this parent-generated random namespace.
                        cleanup = subprocess.run(command, input=json.dumps(dict(request, cleanup_only=True)),
                                                 text=True, capture_output=True, timeout=30, cwd=root)
                        if cleanup.returncode:
                            raise RuntimeError("MongoDB benchmark cleanup failed for " + request["database"])
                if child.returncode:
                    raise RuntimeError(f"{backend} benchmark failed: {child.stderr[-4000:]}")
                run = json.loads(child.stdout)
                validate_run(run, backend, config)
                if digest is not None and digest != run["final_sha256"]:
                    raise ValueError("backends/trials produced different final records")
                digest = run["final_sha256"]
                trials = report["backends"][backend]["trials"]
                if trials:
                    identity = {key: value for key, value in run["metadata"].items() if key != "startup_ns"}
                    first_identity = {key: value for key, value in trials[0]["metadata"].items() if key != "startup_ns"}
                    if identity != first_identity:
                        raise ValueError("runtime changed between trials")
                trials.append(run)
    for result in report["backends"].values():
        result["summary"] = summarize(result["trials"])
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--briskdb-python", required=True)
    parser.add_argument("--tinymongo-python", required=True)
    parser.add_argument("--mongodb-uri", help="disposable literal-loopback MongoDB reference")
    parser.add_argument("--mongodb-environment", help="deployment, storage, container digest and resource limits")
    parser.add_argument("--documents", type=int, default=1000)
    parser.add_argument("--operations", type=int, default=64)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--work-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--maximum-regression-ratio", type=float, default=1.5)
    args = parser.parse_args(argv)
    config = {name: getattr(args, name) for name in ("documents", "operations", "trials", "shards", "workers")}
    interpreters = {"briskdb": args.briskdb_python, "tinymongo": args.tinymongo_python}
    if args.mongodb_uri:
        interpreters["mongodb"] = args.briskdb_python
    report = benchmark(config, interpreters, args.mongodb_uri, args.work_root,
                       mongodb_environment=args.mongodb_environment)
    if args.baseline:
        report["regression"] = compare_baseline(report, json.loads(args.baseline.read_text()), args.maximum_regression_ratio)
    # Refuse to overwrite an earlier raw report; choose a new output per run.
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"output": str(args.output), "backends": list(report["backends"]),
                      "verified_trials": sum(len(value["trials"]) for value in report["backends"].values()),
                      "regression_passed": report.get("regression", {}).get("passed")}, sort_keys=True))
    return 0 if report.get("regression", {}).get("passed", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
