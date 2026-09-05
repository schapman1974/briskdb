"""PyMongo transport adapter for a BriskDB Mongo-compatible endpoint."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator, Mapping, Optional

from . import PathValue, TargetHandles, TargetUnavailable
from .mongodb import open_pymongo_target


TARGET = "briskdb"


def _configured_uri(uri: Optional[str]) -> str:
    configured = uri or os.environ.get("BRISKDB_MONGO_PARITY_BRISKDB_URI")
    if not configured:
        raise TargetUnavailable(
            "set BRISKDB_MONGO_PARITY_BRISKDB_URI to run the BriskDB Mongo "
            "compatibility adapter"
        )
    return configured


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
    """Open one isolated database on a configured BriskDB Mongo endpoint."""

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
