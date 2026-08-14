import multiprocessing
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Optional

import briskdb

SHARDS = 4
WRITES_PER_PROCESS = 24
WAIT_SECONDS = 20.0
SUPPORTED_HOST = sys.platform == "darwin" or sys.platform.startswith("linux")


def _wait_for(path: Path) -> None:
    deadline = time.monotonic() + WAIT_SECONDS
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.005)
    if not path.exists():
        raise TimeoutError(f"timed out waiting for {path.name}")


def _process_worker(
    root: str,
    mode: str,
    route: str,
    worker: int,
    ready: str,
    go: str,
    output: str,
) -> None:
    database = briskdb.open(root, shards=SHARDS)
    session = database.session(routing_key=route)
    Path(ready).touch()
    _wait_for(Path(go))

    if mode in ("writer", "crash"):
        for ordinal in range(WRITES_PER_PROCESS):
            session.execute(
                "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                [
                    route,
                    worker * 10_000 + ordinal,
                    f"worker-{worker}-{ordinal}",
                ],
            )
            if ordinal == WRITES_PER_PROCESS // 2:
                database.checkpoint()
            time.sleep(0.001)
        Path(output).write_text("committed", encoding="utf-8")
        if mode == "crash":
            while True:
                time.sleep(1)

    session.close()
    database.close()


def _setup_root(root: str) -> None:
    database = briskdb.open(root, shards=SHARDS)
    session = database.session(routing_key="setup")
    session.migrate(
        "CREATE TABLE events ("
        "tenant_id TEXT NOT NULL, "
        "id INTEGER NOT NULL, "
        "payload TEXT NOT NULL, "
        "PRIMARY KEY (tenant_id, id)"
        ")"
    )
    session.close()
    database.close()


def _route_shard(database: briskdb.Database, route: str) -> int:
    session = database.session(routing_key=route)
    try:
        return int(session.query("SELECT 1")["shards"][0])
    finally:
        session.close()


def _different_routes(root: str) -> tuple[str, str]:
    database = briskdb.open(root, shards=SHARDS)
    try:
        first = "same-shard"
        first_shard = _route_shard(database, first)
        for ordinal in range(10_000):
            candidate = f"different-{ordinal}"
            if _route_shard(database, candidate) != first_shard:
                return first, candidate
    finally:
        database.close()
    raise AssertionError("failed to find routes on different shards")


def _join_process(process: multiprocessing.Process) -> Optional[int]:
    process.join(WAIT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join(WAIT_SECONDS)
        raise TimeoutError(f"child process {process.pid} did not exit")
    return process.exitcode


@unittest.skipUnless(SUPPORTED_HOST, "shared-root process locks require Linux or macOS")
class MultiprocessBriskDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = multiprocessing.get_context("spawn")

    def _spawn(
        self,
        root: str,
        mode: str,
        route: str,
        worker: int,
        ready: Path,
        go: Path,
        output: Path,
    ) -> multiprocessing.Process:
        process = self.context.Process(
            target=_process_worker,
            args=(
                root,
                mode,
                route,
                worker,
                str(ready),
                str(go),
                str(output),
            ),
        )
        process.start()
        self.addCleanup(self._stop_process, process)
        return process

    @staticmethod
    def _stop_process(process: multiprocessing.Process) -> None:
        if process.is_alive():
            process.kill()
            process.join(WAIT_SECONDS)

    def test_spawned_processes_overlap_same_and_cross_shard_writes(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            _setup_root(root)
            same_route, different_route = _different_routes(root)
            go = Path(root) / "writers-go"
            processes = []
            ready_paths = []
            for worker, route in enumerate((same_route, same_route, different_route)):
                ready = Path(root) / f"writer-ready-{worker}"
                output = Path(root) / f"writer-output-{worker}"
                processes.append(
                    self._spawn(root, "writer", route, worker, ready, go, output)
                )
                ready_paths.append(ready)

            for ready in ready_paths:
                _wait_for(ready)
            go.touch()
            for process in processes:
                self.assertEqual(_join_process(process), 0)

            database = briskdb.open(root, shards=SHARDS)
            same_session = database.session(routing_key=same_route)
            different_session = database.session(routing_key=different_route)
            self.assertEqual(
                same_session.query("SELECT COUNT(*) FROM events")["rows"],
                [(WRITES_PER_PROCESS * 2,)],
            )
            self.assertEqual(
                different_session.query("SELECT COUNT(*) FROM events")["rows"],
                [(WRITES_PER_PROCESS,)],
            )
            same_session.close()
            different_session.close()
            database.close()

            reopened = briskdb.open(root, shards=SHARDS)
            self.assertEqual(len(reopened.checkpoint()["shards"]), SHARDS)
            reopened.close()

    def test_killed_process_releases_the_root_and_survivor_continues(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            _setup_root(root)
            ready = Path(root) / "crash-ready"
            go = Path(root) / "crash-go"
            committed = Path(root) / "crash-committed"
            route = "crash-route"
            process = self._spawn(root, "crash", route, 7, ready, go, committed)
            _wait_for(ready)

            survivor = briskdb.open(root, shards=SHARDS)
            session = survivor.session(routing_key=route)
            go.touch()
            _wait_for(committed)
            process.kill()
            process.join(WAIT_SECONDS)
            self.assertFalse(process.is_alive())
            self.assertNotEqual(process.exitcode, 0)

            session.execute(
                "INSERT INTO events (tenant_id, id, payload) VALUES (?1, ?2, ?3)",
                [route, 999_999, "survivor"],
            )
            self.assertEqual(
                session.query("SELECT COUNT(*) FROM events")["rows"],
                [(WRITES_PER_PROCESS + 1,)],
            )
            session.close()
            survivor.close()

            reopened = briskdb.open(root, shards=SHARDS)
            reopened.close()

    def test_schema_change_is_retryable_busy_until_peer_closes(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            _setup_root(root)
            ready = Path(root) / "holder-ready"
            release = Path(root) / "holder-release"
            output = Path(root) / "holder-output"
            process = self._spawn(root, "hold", "holder", 0, ready, release, output)
            _wait_for(ready)

            database = briskdb.open(root, shards=SHARDS)
            session = database.session(routing_key="schema")
            migration = "CREATE INDEX events_payload_idx ON events(payload)"
            with self.assertRaises(briskdb.BusyError) as raised:
                session.migrate(migration)
            self.assertEqual(raised.exception.code, "busy")
            self.assertTrue(raised.exception.retryable)

            release.touch()
            self.assertEqual(_join_process(process), 0)
            self.assertEqual(session.migrate(migration), list(range(SHARDS)))
            session.close()
            database.close()


if __name__ == "__main__":
    unittest.main()
