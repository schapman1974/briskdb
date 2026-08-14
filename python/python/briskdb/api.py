"""Ergonomic synchronous and asyncio wrappers around the native extension."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import AsyncIterator
from os import PathLike
from typing import Any, Optional, Union

from ._briskdb import (
    CancellationToken,
    Config,
    Cursor,
    Database,
    Server,
    Session,
    open as _native_open,
)


def connect(
    path: Union[str, PathLike[str]],
    *,
    shards: Optional[int] = None,
    config: Optional[Config] = None,
) -> Database:
    """Open an in-process database; no listener starts unless ``serve()`` is called."""

    return _native_open(path, shards=shards, config=config)


async def _cancelable_call(
    method: Any,
    /,
    *args: Any,
    timeout_ms: Optional[int] = None,
    cancellation: Optional[CancellationToken] = None,
    **kwargs: Any,
) -> Any:
    token = cancellation if cancellation is not None else CancellationToken()
    call = functools.partial(
        method,
        *args,
        timeout_ms=timeout_ms,
        cancellation=token,
        **kwargs,
    )
    try:
        return await asyncio.to_thread(call)
    except asyncio.CancelledError:
        token.cancel()
        raise


class AsyncCursor(AsyncIterator[tuple[Any, ...]]):
    """Async iteration over one already bounded native result."""

    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    @property
    def columns(self) -> list[dict[str, str]]:
        return self._cursor.columns

    @property
    def shards(self) -> list[int]:
        return self._cursor.shards

    @property
    def closed(self) -> bool:
        return self._cursor.closed

    @property
    def remaining(self) -> int:
        return self._cursor.remaining

    async def fetchone(self) -> Optional[tuple[Any, ...]]:
        return await asyncio.to_thread(self._cursor.fetchone)

    async def fetchmany(self, size: Optional[int] = None) -> list[tuple[Any, ...]]:
        return await asyncio.to_thread(self._cursor.fetchmany, size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return await asyncio.to_thread(self._cursor.fetchall)

    async def close(self) -> None:
        await asyncio.to_thread(self._cursor.close)

    def __aiter__(self) -> AsyncCursor:
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> AsyncCursor:
        return self

    async def __aexit__(self, *_exception: object) -> bool:
        await self.close()
        return False


class AsyncSession:
    """Asyncio facade for an owned native BriskDB session."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def native(self) -> Session:
        return self._session

    @property
    def closed(self) -> bool:
        return self._session.closed

    @property
    def database_state(self) -> str:
        return self._session.database_state

    async def get_state(self) -> str:
        return await asyncio.to_thread(lambda: self._session.state)

    async def get_routing_key(self) -> Optional[str]:
        return await asyncio.to_thread(lambda: self._session.routing_key)

    async def set_routing_key(self, routing_key: str) -> None:
        await asyncio.to_thread(self._session.set_routing_key, routing_key)

    async def clear_routing_key(self) -> None:
        await asyncio.to_thread(self._session.clear_routing_key)

    async def migrate(
        self,
        sql: str,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> list[int]:
        return await _cancelable_call(
            self._session.migrate,
            sql,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )

    async def execute(
        self,
        sql: str,
        params: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> dict[str, Any]:
        return await _cancelable_call(
            self._session.execute,
            sql,
            params,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )

    async def query(
        self,
        sql: str,
        params: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> dict[str, Any]:
        return await _cancelable_call(
            self._session.query,
            sql,
            params,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )

    async def query_logical(
        self,
        sql: str,
        params: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> dict[str, Any]:
        return await _cancelable_call(
            self._session.query_logical,
            sql,
            params,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )

    async def cursor(
        self,
        sql: str,
        params: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        *,
        batch_size: int = 1_000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> AsyncCursor:
        cursor = await _cancelable_call(
            self._session.cursor,
            sql,
            params,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )
        return AsyncCursor(cursor)

    async def logical_cursor(
        self,
        sql: str,
        params: Optional[Union[list[Any], tuple[Any, ...]]] = None,
        *,
        batch_size: int = 1_000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> AsyncCursor:
        cursor = await _cancelable_call(
            self._session.logical_cursor,
            sql,
            params,
            batch_size=batch_size,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )
        return AsyncCursor(cursor)

    async def status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._session.status)

    async def close(self) -> None:
        await asyncio.to_thread(self._session.close)

    async def __aenter__(self) -> AsyncSession:
        return self

    async def __aexit__(self, *_exception: object) -> bool:
        await self.close()
        return False


class AsyncDatabase:
    """Asyncio facade for one in-process native BriskDB database."""

    def __init__(self, database: Database) -> None:
        self._database = database

    @property
    def native(self) -> Database:
        return self._database

    @property
    def path(self) -> PathLike[str]:
        return self._database.path

    @property
    def shard_count(self) -> int:
        return self._database.shard_count

    @property
    def closed(self) -> bool:
        return self._database.closed

    @property
    def state(self) -> str:
        return self._database.state

    async def session(self, *, routing_key: Optional[str] = None) -> AsyncSession:
        session = await asyncio.to_thread(
            self._database.session, routing_key=routing_key
        )
        return AsyncSession(session)

    async def checkpoint(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> dict[str, Any]:
        return await _cancelable_call(
            self._database.checkpoint,
            timeout_ms=timeout_ms,
            cancellation=cancellation,
        )

    async def serve(
        self,
        *,
        http: str = "127.0.0.1:0",
        postgres: Optional[str] = None,
        postgres_tls_cert: Optional[Union[str, PathLike[str]]] = None,
        postgres_tls_key: Optional[Union[str, PathLike[str]]] = None,
        postgres_user: str = "briskdb",
        postgres_password_file: Optional[Union[str, PathLike[str]]] = None,
    ) -> AsyncServer:
        """Start listeners against this database's existing engine."""

        server = await asyncio.to_thread(
            self._database.serve,
            http=http,
            postgres=postgres,
            postgres_tls_cert=postgres_tls_cert,
            postgres_tls_key=postgres_tls_key,
            postgres_user=postgres_user,
            postgres_password_file=postgres_password_file,
        )
        return AsyncServer(server)

    async def close(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._database.close)

    async def __aenter__(self) -> AsyncDatabase:
        return self

    async def __aexit__(self, *_exception: object) -> bool:
        await self.close()
        return False


class AsyncServer:
    """Asyncio lifecycle wrapper for an attached native listener server."""

    def __init__(self, server: Server) -> None:
        self._server = server

    @property
    def native(self) -> Server:
        return self._server

    @property
    def http_address(self) -> str:
        return self._server.http_address

    @property
    def postgres_address(self) -> Optional[str]:
        return self._server.postgres_address

    @property
    def closed(self) -> bool:
        return self._server.closed

    async def close(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._server.close)

    async def __aenter__(self) -> AsyncServer:
        return self

    async def __aexit__(self, *_exception: object) -> bool:
        await self.close()
        return False


async def connect_async(
    path: Union[str, PathLike[str]],
    *,
    shards: Optional[int] = None,
    config: Optional[Config] = None,
) -> AsyncDatabase:
    """Open an in-process database without blocking the event loop."""

    database = await asyncio.to_thread(connect, path, shards=shards, config=config)
    return AsyncDatabase(database)


open_async = connect_async
