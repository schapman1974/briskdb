"""Owned local engines and cancellation-drained cleanup for Mongo clients."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import shutil
import tempfile
import threading
from typing import Any, Optional

from . import _briskdb

_lock = threading.RLock()
_stores: dict[Path, _Store] = {}
_pid = os.getpid()


def _validate_contention_policy(policy: Optional[_briskdb.ContentionPolicy]) -> None:
    if policy is not None and not isinstance(policy, _briskdb.ContentionPolicy):
        raise TypeError("contention_policy must be a briskdb.ContentionPolicy or None")


def _policy_key(policy: _briskdb.ContentionPolicy) -> tuple[Any, ...]:
    # Native Config readback clones this immutable value; object identity is
    # not policy equality. Compare every validated setting, not its repr.
    return (policy.initial_delay_ms, policy.max_delay_ms, policy.multiplier,
            policy.jitter, policy.max_retries, policy.max_elapsed_ms)


def _check_process() -> None:
    # Never acquire an inherited lock to decide whether a fork was safe: its
    # owning thread may no longer exist, even when the store registry is empty.
    # A fresh spawned interpreter owns its own marker, locks and native runtime.
    if os.getpid() != _pid:
        raise RuntimeError("BriskDB Mongo state cannot be inherited after fork; use multiprocessing spawn")


async def _drained(awaitable: Any) -> Any:
    """Finish owned cleanup/startup even if its caller is cancelled repeatedly."""
    task = asyncio.ensure_future(awaitable)
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


class _Store:
    def __init__(self, path: Path, temporary: bool, shards: Optional[int],
                 contention_policy: Optional[_briskdb.ContentionPolicy]) -> None:
        self.path = path
        self.temporary = temporary
        self.pid = os.getpid()
        self.references = 1
        self.closing = False
        # Creation defaults to four shards. Reopening retains the stored layout.
        if shards is None and not (path / "manifest.sqlite").exists():
            shards = 4
        self.database = _briskdb.open(path, config=_briskdb.Config(
            shards=shards, documents=True, contention_policy=contention_policy))
        self.contention_policy = contention_policy
        try:
            self.listener = self.database._serve_mongo()
        except BaseException:
            self.database.close()
            raise

    def check_process(self) -> None:
        if os.getpid() != self.pid:
            raise RuntimeError("BriskDB Mongo clients cannot be inherited after fork; use multiprocessing spawn")

    def check_contention_policy(self, policy: Optional[_briskdb.ContentionPolicy]) -> None:
        self.check_process()
        _validate_contention_policy(policy)
        # None selects legacy behavior for a new engine, but inherits an
        # already-owned engine's policy. Never silently reconfigure that engine.
        if policy is not None:
            current = self.contention_policy
            if current is None or _policy_key(policy) != _policy_key(current):
                raise ValueError("contention_policy does not match the open BriskDB database")

    def release(self) -> None:
        self.check_process()
        with _lock:
            self.references -= 1
            if self.references:
                return
            self.closing = True
            try:
                self.listener.close()
            finally:
                self.database.close()
            _stores.pop(self.path, None)
            # Closed clients may stay in application variables without retaining
            # native runtime/listener handles after their last owner closes.
            self.listener = None
            self.database = None
            if self.temporary:
                # Only the exact directory created by acquire(None) is removed.
                # User-supplied persistent folders never enter this branch.
                shutil.rmtree(self.path)


def acquire(folder: Any, shards: Optional[int],
            contention_policy: Optional[_briskdb.ContentionPolicy] = None) -> _Store:
    _check_process()
    _validate_contention_policy(contention_policy)
    with _lock:
        if shards is not None and (type(shards) is not int or not 2 <= shards <= 64):
            raise ValueError("shards must be an integer between 2 and 64")
        temporary = folder is None
        if folder is not None and os.fspath(folder) == "":
            raise ValueError("folder must not be empty")
        path = Path(tempfile.mkdtemp(prefix="briskdb-mongo-") if temporary else folder).resolve()
        existing = _stores.get(path)
        if existing is not None:
            existing.check_process()
            if existing.closing:
                raise RuntimeError("the BriskDB Mongo engine is closing")
            if shards is not None and shards != existing.database.shard_count:
                raise ValueError("shards does not match the open BriskDB database")
            existing.check_contention_policy(contention_policy)
            existing.references += 1
            return existing
        try:
            store = _Store(path, temporary, shards, contention_policy)
        except BaseException:
            if temporary:
                shutil.rmtree(path)
            raise
        _stores[path] = store
        return store
