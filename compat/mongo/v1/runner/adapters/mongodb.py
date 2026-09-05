"""Real MongoDB adapter for the neutral Mongo compatibility runner."""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Optional
from uuid import uuid4

from . import (
    AsyncCollectionAdapter,
    AsyncRunner,
    PathValue,
    TargetHandles,
    TargetUnavailable,
)


TARGET = "mongodb"


def _client_classes() -> tuple[Any, Any]:
    # PyMongo stays lazy so importing the adapter registry or an unavailable
    # target does not load a contract-only dependency.
    try:
        from pymongo import AsyncMongoClient, MongoClient
    except ImportError as error:
        raise TargetUnavailable(
            "the PyMongo transport requires PyMongo with AsyncMongoClient support"
        ) from error
    return MongoClient, AsyncMongoClient


def _server_selection_options(client_options: Mapping[str, Any]) -> dict[str, Any]:
    options = dict(client_options)
    if not any(key.lower() == "serverselectiontimeoutms" for key in options):
        options["serverSelectionTimeoutMS"] = 1_000
    return options


def _configured_uri(uri: Optional[str]) -> str:
    configured = (
        uri
        or os.environ.get("BRISKDB_MONGO_PARITY_MONGODB_URI")
        or os.environ.get("TINYMONGO_MONGODB_URI")
    )
    if not configured:
        raise TargetUnavailable(
            "set BRISKDB_MONGO_PARITY_MONGODB_URI to run real MongoDB contracts"
        )
    return configured


def _wait_until_ready(
    client: Any, runner: Optional[AsyncRunner], connect_timeout: float
) -> None:
    if connect_timeout <= 0:
        raise ValueError("connect_timeout must be greater than zero")
    deadline = time.monotonic() + connect_timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            command = client.admin.command("ping")
            if runner is not None:
                runner.run(command)
            return
        except Exception as error:  # noqa: BLE001 - retry server startup
            last_error = error
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    raise TargetUnavailable(
        "the Mongo-compatible target did not become ready: {0}".format(last_error)
    )


@contextmanager
def open_pymongo_target(
    target_name: str,
    api: str,
    tmp_path: PathValue,
    client_options: Optional[Mapping[str, Any]] = None,
    *,
    backend: str = "memory",
    uri: str,
    database_name: Optional[str] = None,
    collection_name: str = "items",
    connect_timeout: float = 15.0,
) -> Iterator[TargetHandles]:
    """Open an isolated target through PyMongo's sync or async transport."""

    del tmp_path, backend
    if api not in ("sync", "async"):
        raise ValueError("api must be 'sync' or 'async', got {0!r}".format(api))
    if not target_name:
        raise ValueError("target_name must not be empty")
    if not uri:
        raise ValueError("uri must not be empty")
    database_name = database_name or "briskdb_contract_{0}".format(uuid4().hex)
    if not database_name:
        raise ValueError("database_name must not be empty")
    if not collection_name:
        raise ValueError("collection_name must not be empty")

    sync_client, async_client = _client_classes()
    options = _server_selection_options(client_options or {})
    runner = None
    client = None
    ready = False
    try:
        if api == "async":
            runner = AsyncRunner()
            client = async_client(uri, **options)
        else:
            client = sync_client(uri, **options)
        _wait_until_ready(client, runner, connect_timeout)
        ready = True

        database = client[database_name]
        collection = database[collection_name]
        exposed_collection = (
            AsyncCollectionAdapter(collection, runner)
            if runner is not None
            else collection
        )
        yield TargetHandles(
            name=target_name,
            transport="pymongo",
            api=api,
            client=client,
            database=database,
            collection=exposed_collection,
            unsupported_warning=UserWarning,
        )
    finally:
        if client is not None:
            try:
                if ready:
                    drop_result = client.drop_database(database_name)
                    if runner is not None:
                        runner.run(drop_result)
            finally:
                try:
                    close_result = client.close()
                    if runner is not None:
                        runner.run(close_result)
                finally:
                    if runner is not None:
                        runner.close()
        elif runner is not None:
            runner.close()


@contextmanager
def open_target(
    api: str,
    tmp_path: PathValue,
    client_options: Optional[Mapping[str, Any]] = None,
    *,
    backend: str = "memory",
    uri: Optional[str] = None,
    database_name: Optional[str] = None,
    collection_name: str = "items",
    connect_timeout: float = 15.0,
) -> Iterator[TargetHandles]:
    """Open one isolated database on a real MongoDB server."""
    configured_uri = _configured_uri(uri)
    with open_pymongo_target(
        TARGET,
        api,
        tmp_path,
        client_options,
        backend=backend,
        uri=configured_uri,
        database_name=database_name,
        collection_name=collection_name,
        connect_timeout=connect_timeout,
    ) as handles:
        yield handles
