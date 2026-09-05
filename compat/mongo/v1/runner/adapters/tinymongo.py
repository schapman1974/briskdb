"""TinyMongo adapter for the neutral Mongo compatibility runner."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional
from uuid import uuid4

from . import (
    AsyncCollectionAdapter,
    AsyncRunner,
    PathValue,
    TargetHandles,
    TargetUnavailable,
)


TARGET = "tinymongo"


def _client_classes() -> tuple[Any, Any, Any, Any, type[Warning]]:
    # TinyMongo is intentionally imported nowhere else in the BriskDB runner.
    try:
        import tinymongo
        from tinymongo.asyncio import AsyncMongoClient, AsyncTinyMongoClient
    except ImportError as error:
        raise TargetUnavailable(
            "the TinyMongo target requires the frozen TinyMongo package"
        ) from error
    return (
        tinymongo.TinyMongoClient,
        tinymongo.MongoClient,
        AsyncTinyMongoClient,
        AsyncMongoClient,
        tinymongo.TinyMongoUnsupportedWarning,
    )


def _validate_names(database_name: str, collection_name: str) -> None:
    if not database_name:
        raise ValueError("database_name must not be empty")
    if not collection_name:
        raise ValueError("collection_name must not be empty")


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
    """Open an isolated TinyMongo backend for one sync or async contract."""

    del uri, connect_timeout  # These transport settings apply only to MongoDB.
    if api not in ("sync", "async"):
        raise ValueError("api must be 'sync' or 'async', got {0!r}".format(api))

    target_name = str(backend).strip().lower()
    storage_backend = "tinydb" if target_name == "json" else target_name
    if not target_name:
        raise ValueError("backend must not be empty")
    database_name = database_name or "tinymongo_contract_{0}".format(uuid4().hex)
    _validate_names(database_name, collection_name)

    root = Path(tmp_path) / "tinymongo-{0}-{1}".format(target_name, uuid4().hex)
    root.mkdir(parents=True, exist_ok=False)
    options = dict(client_options or {})
    reserved = {"backend", "foldername", "tinymongo_folder", "tinymongo_path"}
    conflicts = sorted(reserved.intersection(options))
    if conflicts:
        raise ValueError(
            "client_options cannot override adapter-owned option(s): {0}".format(
                ", ".join(conflicts)
            )
        )

    tiny_sync, mongo_sync, tiny_async, mongo_async, unsupported_warning = (
        _client_classes()
    )
    runner = None
    client = None
    try:
        if api == "async":
            runner = AsyncRunner()
            if options:
                client = mongo_async(
                    tinymongo_folder=str(root), backend=storage_backend, **options
                )
            else:
                client = tiny_async(str(root), backend=storage_backend)
        elif options:
            client = mongo_sync(
                tinymongo_folder=str(root), backend=storage_backend, **options
            )
        else:
            client = tiny_sync(str(root), backend=storage_backend)

        database = client[database_name]
        collection = database[collection_name]
        exposed_collection = (
            AsyncCollectionAdapter(collection, runner)
            if runner is not None
            else collection
        )
        yield TargetHandles(
            name=target_name,
            transport="direct",
            api=api,
            client=client,
            database=database,
            collection=exposed_collection,
            unsupported_warning=unsupported_warning,
        )
    finally:
        if client is not None:
            try:
                close_result = client.close()
                if runner is not None:
                    runner.run(close_result)
            finally:
                if runner is not None:
                    runner.close()
