"""Dependency-free gates for the public Mongo benchmark and regression report."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("mongo_benchmark", Path(__file__).resolve().parents[1] / "scripts/mongo_benchmark.py")
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)
WORKER_SPEC = importlib.util.spec_from_file_location("benchmark_worker", benchmark.WORKER)
worker = importlib.util.module_from_spec(WORKER_SPEC)
WORKER_SPEC.loader.exec_module(worker)
CONFIG = {"documents": 32, "operations": 8, "trials": 3, "shards": 2, "workers": 2}


def fixture(backend, config=CONFIG):
    samples = {}
    for name in benchmark.WORKLOADS:
        count = (1 if name in ("seed_insert_many", "build_index", "threaded_insert_wave",
                              "scan", "group_aggregation", "cursor_iteration", "scatter_sorted_window")
                 else config["operations"])
        units = (config["documents"] if name in ("seed_insert_many", "scan", "group_aggregation", "cursor_iteration")
                 else config["workers"] * config["operations"] if name == "threaded_insert_wave" else 1)
        samples[name] = [{"elapsed_ns": 1000, "units": units} for _ in range(count)]
    return {"backend": backend, "metadata": {"python": "3.13", "version": "fixture", "transport": "fixture"},
            "samples": samples, "final_count": config["documents"] + config["workers"] * config["operations"],
            "final_sha256": "a" * 64}


def report():
    result = {"schema_version": 1, "configuration": CONFIG, "host": {"architecture": "fixture"},
              "worker_sha256": "b" * 64, "mongodb_environment": None, "backends": {}}
    for backend in ("briskdb", "tinymongo"):
        runs = [fixture(backend) for _ in range(CONFIG["trials"])]
        result["backends"][backend] = {"trials": runs, "summary": benchmark.summarize(runs)}
    return result


class MongoBenchmarkTests(unittest.TestCase):
    def test_cleanup_requires_exact_ownership_and_never_drops_a_preexisting_namespace(self):
        config = {"database": "briskdb_bench_" + "a" * 32, "owner": "b" * 32}
        client = mock.MagicMock()
        database = client.__getitem__.return_value
        database.list_collection_names.return_value = ["existing_data"]
        for claim in (None, {"_id": "owner", "token": "someone-else"}):
            database.__getitem__.return_value.find_one.return_value = claim
            with self.assertRaises(ValueError):
                worker.cleanup_owned_database(client, config)
            client.drop_database.assert_not_called()
        database.__getitem__.return_value.find_one.return_value = {"_id": "owner", "token": config["owner"]}
        worker.cleanup_owned_database(client, config)
        client.drop_database.assert_called_once_with(config["database"])
        client.reset_mock()
        database.list_collection_names.return_value = []
        worker.cleanup_owned_database(client, config)
        client.drop_database.assert_not_called()

    def test_configuration_is_bounded_and_boolean_values_are_not_counts(self):
        benchmark.validate_config(CONFIG)
        for key, bad in (("documents", 0), ("operations", True), ("trials", 21),
                         ("shards", 1), ("workers", 5)):
            with self.assertRaises(ValueError):
                benchmark.validate_config(dict(CONFIG, **{key: bad}))

    def test_reference_uri_cannot_target_remote_or_named_application_databases(self):
        for uri in ("mongodb://127.0.0.1:27017", "mongodb://[::1]:27017/"):
            benchmark.validate_mongodb_uri(uri)
        for uri in ("mongodb://localhost:27017", "mongodb://example.test:27017",
                    "mongodb://127.0.0.1:27017/app", "mongodb://user:secret@127.0.0.1:27017",
                    "mongodb+srv://example.test", "mongodb://127.0.0.1",
                    "mongodb://127.0.0.1:0",
                    "mongodb://127.0.0.1:27017/?tls=true"):
            with self.assertRaises(ValueError):
                benchmark.validate_mongodb_uri(uri)

    def test_measurements_require_every_workload_sample_unit_and_result_digest(self):
        benchmark.validate_run(fixture("briskdb"), "briskdb", CONFIG)
        for mutate in (
            lambda value: value.update(backend="wrong"),
            lambda value: value.update(final_count=0),
            lambda value: value.update(final_sha256="z" * 64),
            lambda value: value["samples"].pop("point_read"),
            lambda value: value["samples"]["point_read"].pop(),
            lambda value: value["samples"]["point_read"][0].update(elapsed_ns=0),
            lambda value: value["samples"]["point_read"][0].update(units=True),
            lambda value: value["samples"]["point_read"][0].update(units=2),
        ):
            value = fixture("briskdb")
            mutate(value)
            with self.assertRaises(ValueError):
                benchmark.validate_run(value, "briskdb", CONFIG)

    def test_summaries_and_regression_ratios_use_verified_raw_measurements(self):
        baseline = report()
        current = deepcopy(baseline)
        self.assertTrue(benchmark.compare_baseline(current, baseline, 1.5)["passed"])
        for run in current["backends"]["briskdb"]["trials"]:
            for sample in run["samples"]["point_read"]:
                sample["elapsed_ns"] *= 2
        current["backends"]["briskdb"]["summary"] = benchmark.summarize(current["backends"]["briskdb"]["trials"])
        result = benchmark.compare_baseline(current, baseline, 1.5)
        self.assertFalse(result["passed"])
        failed = [check for check in result["checks"] if not check["passed"]]
        self.assertEqual(failed, [{"backend": "briskdb", "workload": "point_read", "ratio": 2.0, "passed": False}])

    def test_baseline_mismatches_and_unverified_summaries_fail_closed(self):
        for mutate in (
            lambda value: value.update(worker_sha256="changed"),
            lambda value: value.update(host={}),
            lambda value: value.update(mongodb_environment="changed"),
            lambda value: value["backends"]["briskdb"]["trials"].pop(),
            lambda value: value["backends"]["briskdb"]["summary"]["point_read"].update(median_ns_per_unit=1),
            lambda value: value["backends"]["tinymongo"]["trials"][0]["metadata"].update(version="changed"),
            lambda value: value["backends"]["briskdb"]["trials"][0]["metadata"].update(python="changed"),
            lambda value: value["backends"]["briskdb"]["trials"][0].update(final_sha256="c" * 64),
        ):
            baseline = report()
            mutate(baseline)
            with self.assertRaises(ValueError):
                benchmark.compare_baseline(report(), baseline, 1.5)
        for ratio in (0, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                benchmark.compare_baseline(report(), report(), ratio)

    def test_orchestrator_rotates_isolated_backends_and_checks_equal_final_state(self):
        calls = []

        def child(command, **kwargs):
            request = json.loads(kwargs["input"])
            self.assertIn("-I", command)
            self.assertFalse(Path(request["location"]).exists())
            calls.append(request["backend"])
            return subprocess.CompletedProcess(command, 0, json.dumps(fixture(request["backend"])), "")

        with mock.patch.object(benchmark.subprocess, "run", side_effect=child):
            result = benchmark.benchmark(CONFIG, {"briskdb": "candidate", "tinymongo": "oracle"})
        self.assertEqual(calls, ["briskdb", "tinymongo", "tinymongo", "briskdb", "briskdb", "tinymongo"])
        self.assertEqual(len(result["backends"]["briskdb"]["trials"]), 3)

    def test_timeout_still_requests_cleanup_of_only_the_generated_mongodb_namespace(self):
        cleaned = []

        def child(command, **kwargs):
            request = json.loads(kwargs["input"])
            if request["backend"] == "mongodb":
                if request.get("cleanup_only"):
                    cleaned.append(request["database"])
                    return subprocess.CompletedProcess(command, 0, "{}", "")
                raise subprocess.TimeoutExpired(command, 1)
            return subprocess.CompletedProcess(command, 0, json.dumps(fixture(request["backend"])), "")

        with mock.patch.object(benchmark.subprocess, "run", side_effect=child):
            with self.assertRaises(subprocess.TimeoutExpired):
                benchmark.benchmark(CONFIG, {"briskdb": "candidate", "tinymongo": "oracle", "mongodb": "driver"},
                                    mongodb_uri="mongodb://127.0.0.1:27017", mongodb_environment="owned test server")
        self.assertEqual(len(cleaned), 1)
        self.assertRegex(cleaned[0], r"^briskdb_bench_[0-9a-f]{32}$")


if __name__ == "__main__":
    unittest.main()
