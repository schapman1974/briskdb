"""Real PyMongo clients backed by an owned, local BriskDB engine.

Import this module only when Mongo compatibility is requested. No TinyMongo
runtime or separate database process is involved.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlsplit
import weakref

try:
    import pymongo
    from pymongo.asynchronous.mongo_client import AsyncMongoClient as _AsyncClient
    from pymongo.synchronous.mongo_client import MongoClient as _Client
except ImportError as error:
    raise ImportError('BriskDB Mongo clients require PyMongo; install "briskdb[pymongo]"') from error

if pymongo.version_tuple[:3] != (4, 17, 0):
    raise ImportError("BriskDB Mongo clients currently require pymongo==4.17.0")

from ._mongo_runtime import _Store, _drained, acquire
from ._mongo_collections import Database, AsyncDatabase, IndexCompatibilityWarning

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING
ReturnDocument = pymongo.ReturnDocument
IndexModel = pymongo.IndexModel
errors = pymongo.errors


def _local_options(host: Any, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Retain database/driver options, never a remote address or credential.

    urlsplit does no DNS lookup (including mongodb+srv). PyMongo sees only a
    newly constructed literal-loopback URI. Network/auth/TLS/proxy options are
    deliberately ignored for this explicit local substitution.
    """
    database = ""
    uri_options: list[tuple[str, str]] = []
    if isinstance(host, str) and host.startswith(("mongodb://", "mongodb+srv://")):
        parsed = urlsplit(host)
        database = parsed.path.lstrip("/")
        if "&" in parsed.query and ";" in parsed.query:
            raise errors.InvalidURI("Can not mix '&' and ';' for option separators")
        if parsed.query:
            for option in parsed.query.split(";" if ";" in parsed.query else "&"):
                key, separator, value = option.partition("=")
                if not separator:
                    raise errors.InvalidURI("MongoDB URI options are key=value pairs")
                uri_options.append((key, value))

    def remote_option(key: str) -> bool:
        key = key.lower()
        return (key in {"username", "password", "ssl", "replicaset", "loadbalanced", "directconnection"}
                or key.startswith(("tls", "auth", "proxy", "srv")))

    keyword_options = {key.lower(): value for key, value in kwargs.items()
                       if not remote_option(key)}
    if keyword_options.get("auto_encryption_opts") is not None:
        raise ValueError("automatic encryption is not supported by local BriskDB Mongo clients")
    keyword_options.pop("auto_encryption_opts", None)
    # Preserve URI option parsing by PyMongo: string-valued URI booleans must
    # not become keyword booleans. Keep original escaping (PyMongo treats tag
    # delimiters specially), and retain repeated readPreferenceTags in order.
    # Keyword values keep their original types and override URI options.
    uri_options = [(key, value) for key, value in uri_options
                   if not remote_option(key) and key.lower() not in keyword_options]
    names = {key.lower() for key, _ in uri_options} | keyword_options.keys()
    uri_options.append(("directConnection", "true"))
    for key, value in {"retryWrites": False, "retryReads": False,
                       "serverSelectionTimeoutMS": 5000, "maxPoolSize": 2}.items():
        if key.lower() not in names:
            keyword_options[key] = value
    suffix = "/" + database + "?" + "&".join(key + "=" + value for key, value in uri_options)
    return suffix, keyword_options


def _configuration(host: Any, port: Any, folder: Any, shards: Optional[int],
                   options: dict[str, Any], shared: Optional[_Store]) -> tuple[Any, Optional[int]]:
    aliases = [options.pop(name, None) for name in
               ("briskdb_folder", "briskdb_path", "tinymongo_folder", "tinymongo_path", "foldername")]
    backend = options.pop("backend", "sqlite")
    old_shards = options.pop("sqlite_shards", None)
    if shared is not None:
        return None, None  # A patch scope, not a caller, owns its storage selection.
    supplied = [value for value in [folder, *aliases] if value is not None]
    if len(supplied) > 1:
        raise TypeError("specify the BriskDB folder only once")
    if str(backend).lower() not in ("sqlite", "sqlite-sharded"):
        raise ValueError("BriskDB uses SQLite storage; TinyMongo backend selection is not supported")
    if shards is not None and old_shards is not None:
        raise TypeError("specify shards or sqlite_shards, not both")
    shards = shards if shards is not None else old_shards
    if supplied:
        folder = supplied[0]
    elif isinstance(host, os.PathLike):
        folder = host
    elif (isinstance(host, str) and port is None and ":" not in host
          and host != "localhost" and (host.startswith((".", "/")) or "." not in host)):
        folder = host
    else:
        folder = os.environ.get("BRISKDB_HOME", "briskdb-data")
    return folder, shards


class _LocalStoreBinding:
    """Bind storage after the pinned driver's option checks, before topology.

    PyMongo 4.17 calls this synchronous hook in both client constructors after
    building ClientOptions and before creating any topology or background work.
    The placeholder URI is only parsed, never used for a network connection.
    Keeping validation inside that constructor avoids duplicated validators,
    warning emissions, temporary clients, monitors and option-normalization drift.
    """

    def _init_based_on_options(self, seeds: Any, srv_max_hosts: Any,
                               srv_service_name: Any) -> None:
        pending = self._briskdb_pending
        if pending is not None:
            folder, shards, shared, suffix = pending
            store = shared if shared is not None else acquire(folder, shards)
            self._briskdb_store = store
            self._briskdb_release = weakref.finalize(self, store.release) if shared is None else None
            self._briskdb_pending = None
            host, port = store.listener.address.rsplit(":", 1)
            # The driver's seed set is also referenced by _resolve_srv_info.
            # Mutate it in place before TopologySettings captures the endpoint.
            seeds.clear()
            seeds.add((host, int(port)))
            uri = "mongodb://" + store.listener.address + suffix
            self._host = [uri]
            self._init_kwargs["host"] = uri
        super()._init_based_on_options(seeds, srv_max_hosts, srv_service_name)


class MongoClient(_LocalStoreBinding, _Client):
    """PyMongo-compatible synchronous client owning local BriskDB storage.

    Use ``folder=`` (or a positional filesystem path) for persistent data.
    Hosts/credentials are never contacted. Close the client or use ``with``.
    """

    def __init__(self, host: Any = None, port: Any = None, document_class: Any = None,
                 tz_aware: Any = None, connect: Any = None, type_registry: Any = None,
                 *, folder: Any = None, shards: Optional[int] = None, **kwargs: Any) -> None:
        shared = kwargs.pop("_briskdb_store", None)
        folder, shards = _configuration(host, port, folder, shards, kwargs, shared)
        suffix, options = _local_options(host, kwargs)
        self._briskdb_pending = (folder, shards, shared, suffix)
        self._briskdb_release = None
        try:
            super().__init__("mongodb://127.0.0.1:1" + suffix,
                             document_class=document_class, tz_aware=tz_aware,
                             connect=connect, type_registry=type_registry, **options)
        except BaseException:
            if self._briskdb_release is not None:
                self._briskdb_release()
            raise

    @property
    def briskdb_path(self) -> Path:
        return self._briskdb_store.path

    def __getitem__(self, name: str) -> Database:
        return Database._wrap(super().__getitem__(name))

    def get_database(self, *args: Any, **kwargs: Any) -> Database:
        return Database._wrap(super().get_database(*args, **kwargs))

    def get_default_database(self, *args: Any, **kwargs: Any) -> Database:
        return Database._wrap(super().get_default_database(*args, **kwargs))

    def close(self) -> None:
        self._briskdb_store.check_process()
        try:
            super().close()
        finally:
            if self._briskdb_release is not None:
                self._briskdb_release()


class AsyncMongoClient(_LocalStoreBinding, _AsyncClient):
    """Async PyMongo client; use ``async with`` or ``await client.close()``.

    Construction opens local storage synchronously. ``async with
    briskdb.patch()`` instead performs engine startup/cleanup off the event loop.
    """

    def __init__(self, host: Any = None, port: Any = None, document_class: Any = None,
                 tz_aware: Any = None, connect: Any = None, type_registry: Any = None,
                 *, folder: Any = None, shards: Optional[int] = None, **kwargs: Any) -> None:
        shared = kwargs.pop("_briskdb_store", None)
        folder, shards = _configuration(host, port, folder, shards, kwargs, shared)
        suffix, options = _local_options(host, kwargs)
        self._briskdb_pending = (folder, shards, shared, suffix)
        self._briskdb_release = None
        try:
            super().__init__("mongodb://127.0.0.1:1" + suffix,
                             document_class=document_class, tz_aware=tz_aware,
                             connect=connect, type_registry=type_registry, **options)
        except BaseException:
            if self._briskdb_release is not None:
                self._briskdb_release()
            raise

    @property
    def briskdb_path(self) -> Path:
        return self._briskdb_store.path

    def __getitem__(self, name: str) -> AsyncDatabase:
        return AsyncDatabase._wrap(super().__getitem__(name))

    def get_database(self, *args: Any, **kwargs: Any) -> AsyncDatabase:
        return AsyncDatabase._wrap(super().get_database(*args, **kwargs))

    def get_default_database(self, *args: Any, **kwargs: Any) -> AsyncDatabase:
        return AsyncDatabase._wrap(super().get_default_database(*args, **kwargs))

    async def close(self) -> None:
        self._briskdb_store.check_process()

        async def cleanup() -> None:
            try:
                await super(AsyncMongoClient, self).close()
            finally:
                if self._briskdb_release is not None:
                    await asyncio.to_thread(self._briskdb_release)

        await _drained(cleanup())
