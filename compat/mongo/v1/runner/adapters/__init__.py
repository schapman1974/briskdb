"""Target-neutral adapters for the vendored Mongo compatibility contracts.

The contract bodies are synchronous so a single assertion corpus can exercise
both client APIs.  Async targets therefore expose their native client and
database handles alongside a synchronous collection facade.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from os import PathLike
from types import ModuleType
from typing import Any, ContextManager, Mapping, Optional, Protocol, Union, cast


PathValue = Union[str, PathLike[str]]


class AdapterError(RuntimeError):
    """Base error raised while configuring a contract target."""


class TargetUnavailable(AdapterError):
    """Raised when a requested target cannot run in this environment."""


@dataclass
class TargetHandles:
    """Objects and metadata exposed to each neutral contract body."""

    name: str
    transport: str
    api: str
    client: Any
    database: Any
    collection: Any
    unsupported_warning: type[Warning]


class TargetAdapter(Protocol):
    """Structural interface implemented by each target adapter module."""

    TARGET: str

    def open_target(
        self,
        api: str,
        tmp_path: PathValue,
        client_options: Optional[Mapping[str, Any]] = None,
        *,
        backend: str = "memory",
        uri: Optional[str] = None,
        database_name: Optional[str] = None,
        collection_name: str = "items",
        connect_timeout: float = 15.0,
    ) -> ContextManager[TargetHandles]:
        """Open one isolated target and close it when the context exits."""


class AsyncRunner:
    """Run one async target on a stable loop from synchronous contracts."""

    def __init__(self) -> None:
        import asyncio

        self.loop = asyncio.new_event_loop()

    def run(self, value: Any) -> Any:
        """Resolve an awaitable, or return an already-immediate value."""

        if inspect.isawaitable(value):
            return self.loop.run_until_complete(value)
        return value

    def close(self) -> None:
        self.loop.close()


class AsyncCursorAdapter:
    """Present an async cursor through the synchronous contract interface."""

    def __init__(self, cursor: Any, runner: AsyncRunner) -> None:
        self._cursor = cursor
        self._runner = runner

    def sort(self, *args: Any, **kwargs: Any) -> "AsyncCursorAdapter":
        self._runner.run(self._cursor.sort(*args, **kwargs))
        return self

    def skip(self, *args: Any, **kwargs: Any) -> "AsyncCursorAdapter":
        self._runner.run(self._cursor.skip(*args, **kwargs))
        return self

    def limit(self, *args: Any, **kwargs: Any) -> "AsyncCursorAdapter":
        self._runner.run(self._cursor.limit(*args, **kwargs))
        return self

    def to_list(self, length: Optional[int] = None) -> Any:
        return self._runner.run(self._cursor.to_list(length=length))

    def close(self) -> Any:
        return self._runner.run(self._cursor.close())

    def __iter__(self) -> Any:
        return iter(self.to_list())


def _is_async_cursor(value: Any) -> bool:
    return hasattr(value, "__aiter__") and callable(getattr(value, "to_list", None))


class AsyncCollectionAdapter:
    """Await collection calls while preserving immediate cursor construction."""

    def __init__(self, collection: Any, runner: AsyncRunner) -> None:
        # These names deliberately match the original contract fixture.  Two
        # UUID cases rebuild this facade after applying CodecOptions.
        self._collection = collection
        self._runner = runner

    def _adapt_result(self, value: Any) -> Any:
        value = self._runner.run(value)
        if _is_async_cursor(value):
            return AsyncCursorAdapter(value, self._runner)
        return value

    def find(self, *args: Any, **kwargs: Any) -> AsyncCursorAdapter:
        cursor = self._collection.find(*args, **kwargs)
        return AsyncCursorAdapter(cursor, self._runner)

    def aggregate(self, *args: Any, **kwargs: Any) -> AsyncCursorAdapter:
        cursor = self._runner.run(self._collection.aggregate(*args, **kwargs))
        return AsyncCursorAdapter(cursor, self._runner)

    def with_options(self, *args: Any, **kwargs: Any) -> "AsyncCollectionAdapter":
        collection = self._runner.run(self._collection.with_options(*args, **kwargs))
        return type(self)(collection, self._runner)

    def __getitem__(self, name: str) -> "AsyncCollectionAdapter":
        return type(self)(self._collection[name], self._runner)

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._collection, name)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            return self._adapt_result(attribute(*args, **kwargs))

        return call


_ADAPTER_MODULES = {
    "briskdb": ".briskdb",
    "mongodb": ".mongodb",
    "tinymongo": ".tinymongo",
}


def available_targets() -> tuple[str, ...]:
    """Return the stable target names accepted by :func:`open_target`."""

    return tuple(sorted(_ADAPTER_MODULES))


def _normalize_api(api: str) -> str:
    normalized = str(api).strip().lower()
    if normalized not in ("sync", "async"):
        raise ValueError("api must be 'sync' or 'async', got {0!r}".format(api))
    return normalized


def load_adapter(target: str) -> TargetAdapter:
    """Load only the selected adapter and its target-specific dependencies."""

    normalized = str(target).strip().lower()
    module_name = _ADAPTER_MODULES.get(normalized)
    if module_name is None:
        raise ValueError(
            "unknown Mongo compatibility target {0!r}; choose one of: {1}".format(
                target, ", ".join(available_targets())
            )
        )
    module = importlib.import_module(module_name, package=__name__)
    if getattr(module, "TARGET", None) != normalized or not callable(
        getattr(module, "open_target", None)
    ):
        raise AdapterError(
            "adapter {0} does not implement the target protocol".format(module.__name__)
        )
    return cast(TargetAdapter, cast(ModuleType, module))


def open_target(
    target: str,
    api: str,
    tmp_path: PathValue,
    client_options: Optional[Mapping[str, Any]] = None,
    *,
    backend: str = "memory",
    uri: Optional[str] = None,
    database_name: Optional[str] = None,
    collection_name: str = "items",
    connect_timeout: float = 15.0,
) -> ContextManager[TargetHandles]:
    """Open a named target through a uniform context-manager interface."""

    normalized_api = _normalize_api(api)
    return load_adapter(target).open_target(
        normalized_api,
        tmp_path,
        dict(client_options or {}),
        backend=backend,
        uri=uri,
        database_name=database_name,
        collection_name=collection_name,
        connect_timeout=connect_timeout,
    )


__all__ = [
    "AdapterError",
    "AsyncCollectionAdapter",
    "AsyncCursorAdapter",
    "AsyncRunner",
    "TargetAdapter",
    "TargetHandles",
    "TargetUnavailable",
    "available_targets",
    "load_adapter",
    "open_target",
]
