"""Opt-in, process-global PyMongo constructor patching for local test scopes."""

from __future__ import annotations

import asyncio
from contextlib import ContextDecorator
import importlib
import inspect
import threading
from typing import Any, Optional
import warnings

from ._mongo_runtime import _drained, acquire

_lock = threading.RLock()
_owner: Any = None
_entries: list[_Entry] = []


def _context_owner() -> Any:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return threading.get_ident(), task


class _Entry:
    def __init__(self, asynchronous: bool) -> None:
        self.asynchronous = asynchronous
        self.active = False
        self.starting = True
        self.store: Any = None
        self.clients: list[Any] = []
        self.async_clients: list[Any] = []
        self.replacement: Any = None
        self.pymongo: Any = None
        self.original: Any = None
        self.original_async: Any = None

    def replacements(self, mongo: Any) -> tuple[Any, Any]:
        entry = self

        class ConfiguredMongoClient(mongo.MongoClient):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                with _lock:
                    if not entry.active:
                        raise mongo.errors.InvalidOperation("BriskDB patch scope is closed")
                    kwargs["_briskdb_store"] = entry.store
                    super().__init__(*args, **kwargs)
                    entry.clients.append(self)

        class ConfiguredAsyncMongoClient(mongo.AsyncMongoClient):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                with _lock:
                    if not entry.active:
                        raise mongo.errors.InvalidOperation("BriskDB patch scope is closed")
                    if not entry.asynchronous:
                        raise RuntimeError("AsyncMongoClient requires async with briskdb.patch() for awaited cleanup")
                    kwargs["_briskdb_store"] = entry.store
                    super().__init__(*args, **kwargs)
                    entry.async_clients.append(self)

        return ConfiguredMongoClient, ConfiguredAsyncMongoClient

    def cleanup(self) -> None:
        error = None
        for client in self.clients:
            try:
                client.close()
            except BaseException as failure:
                error = error or failure
        self.clients.clear()
        if self.store is not None:
            try:
                self.store.release()
            except BaseException as failure:
                error = error or failure
            self.store = None
        if error is not None:
            raise error

    async def cleanup_async(self) -> None:
        error = None
        for client in self.async_clients:
            try:
                await client.close()
            except BaseException as failure:
                error = error or failure
        self.async_clients.clear()
        try:
            await asyncio.to_thread(self.cleanup)
        except BaseException as failure:
            error = error or failure
        if error is not None:
            raise error


class MongoPatch(ContextDecorator):
    """Reusable sync/decorator or async scope returned by :func:`patch`.

    The scope changes only PyMongo's two top-level client constructors. Existing
    clients and names imported before entry are not redirected. Concurrent
    scopes on different threads/tasks are rejected; nested scopes restore LIFO.
    """

    def __init__(self, folder: Any = None, backend: str = "sqlite", *, shards: Optional[int] = None) -> None:
        if backend not in ("sqlite", "sqlite-sharded"):
            raise ValueError("BriskDB patch uses SQLite storage; omit folder for isolated temporary data")
        self.folder = folder
        self.shards = shards
        self._stack: list[_Entry] = []

    def _enter(self, owner: Any, entry: _Entry) -> Any:
        global _owner
        with _lock:
            if _owner is not None and _owner != owner:
                raise RuntimeError("briskdb.patch cannot overlap across threads or async tasks")
            if _entries and _entries[-1].starting:
                raise RuntimeError("a BriskDB patch scope is already starting")
            _owner = owner
            _entries.append(entry)
        # Reserve ownership before blocking startup, but never hold the patch
        # lock over native I/O: other tasks must fail, not block their event loop.
        try:
            mongo = importlib.import_module(".mongo", __package__)
            entry.pymongo = importlib.import_module("pymongo")
            entry.store = acquire(self.folder, self.shards)
            replacement, replacement_async = entry.replacements(mongo)
            with _lock:
                entry.original = entry.pymongo.MongoClient
                entry.original_async = entry.pymongo.AsyncMongoClient
                entry.pymongo.MongoClient = replacement
                entry.pymongo.AsyncMongoClient = replacement_async
                entry.replacement = replacement
                entry.starting = False
                entry.active = True
                self._stack.append(entry)
            return replacement
        except BaseException:
            with _lock:
                _entries.remove(entry)
                if not _entries:
                    _owner = None
            entry.cleanup()
            raise

    def _restore(self, owner: Any, expected: Optional[_Entry] = None) -> _Entry:
        global _owner
        with _lock:
            if not self._stack:
                raise RuntimeError("briskdb.patch scope exited without being entered")
            entry = self._stack[-1]
            if _owner != owner:
                raise RuntimeError("briskdb.patch must exit on its owning thread/task")
            if not _entries or _entries[-1] is not entry or (expected is not None and expected is not entry):
                raise RuntimeError("briskdb.patch scopes must exit in nested order")
            entry.active = False
            entry.pymongo.MongoClient = entry.original
            entry.pymongo.AsyncMongoClient = entry.original_async
            self._stack.pop()
            _entries.pop()
            if not _entries:
                _owner = None
            return entry

    def __enter__(self) -> Any:
        return self._enter(_context_owner(), _Entry(False))

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        entry = self._restore(_context_owner())
        try:
            entry.cleanup()
        except BaseException:
            if exc_type is None:
                raise
            warnings.warn("BriskDB patch cleanup failed while handling an application exception", ResourceWarning)
        return False

    async def __aenter__(self) -> Any:
        owner = _context_owner()
        entry = _Entry(True)
        try:
            return await _drained(asyncio.to_thread(self._enter, owner, entry))
        except BaseException:
            if entry.active:
                self._restore(owner, entry)
                await _drained(entry.cleanup_async())
            raise

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        entry = self._restore(_context_owner())
        try:
            await _drained(entry.cleanup_async())
        except BaseException:
            if exc_type is None:
                raise
            warnings.warn("BriskDB patch cleanup failed while handling an application exception", ResourceWarning)
        return False

    def __call__(self, function: Any) -> Any:
        if inspect.iscoroutinefunction(function):
            raise TypeError("briskdb.patch does not decorate async functions; use async with briskdb.patch()")
        return super().__call__(function)


def patch(folder: Any = None, backend: str = "sqlite", *, shards: Optional[int] = None) -> MongoPatch:
    """Route newly constructed PyMongo clients to a scoped local BriskDB engine.

    No folder means isolated temporary SQLite storage, deleted after close (not
    a RAM-only backend). An explicit folder is persistent and never deleted.
    Use ``async with`` whenever the scope creates ``AsyncMongoClient`` objects.
    PyMongo is imported only on entry, not on importing BriskDB or calling patch.
    """
    return MongoPatch(folder, backend, shards=shards)
