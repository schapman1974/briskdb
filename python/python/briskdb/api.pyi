from os import PathLike
from typing import AsyncIterator, List, Optional, Sequence, Union
from uuid import UUID

from ._briskdb import (
    BsonDocument,
    CancellationToken,
    CloseReport,
    CheckpointReport,
    ColumnInfo,
    Config,
    CountDocumentsResult,
    CreateCollectionResult,
    CreateIndexResult,
    Database,
    DeleteOneResult,
    FindResult,
    InsertOneResult,
    ListCollectionsResult,
    ListIndexesResult,
    QueryResult,
    Server,
    ServerCloseReport,
    Session,
    SqlParameter,
    SqlRow,
    Status,
    Transaction,
    UuidRepresentation,
    WriteResult,
)

def connect(
    path: Union[str, PathLike[str]],
    *,
    shards: Optional[int] = None,
    documents: bool = False,
    uuid_representation: Optional[UuidRepresentation] = None,
    config: Optional[Config] = None,
) -> Database: ...

class AsyncCursor(AsyncIterator[SqlRow]):
    @property
    def columns(self) -> List[ColumnInfo]: ...
    @property
    def shards(self) -> List[int]: ...
    @property
    def closed(self) -> bool: ...
    async def fetchone(self) -> Optional[SqlRow]: ...
    async def fetchmany(self, size: Optional[int] = None) -> List[SqlRow]: ...
    async def fetchall(self) -> List[SqlRow]: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncCursor: ...
    async def __anext__(self) -> SqlRow: ...
    async def __aenter__(self) -> AsyncCursor: ...
    async def __aexit__(self, *exception: object) -> bool: ...

class AsyncSession:
    @property
    def native(self) -> Session: ...
    @property
    def closed(self) -> bool: ...
    @property
    def database_state(self) -> str: ...
    async def get_state(self) -> str: ...
    async def get_routing_key(self) -> Optional[str]: ...
    async def set_routing_key(self, routing_key: str) -> None: ...
    async def clear_routing_key(self) -> None: ...
    async def migrate(
        self,
        sql: str,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> List[int]: ...
    async def execute(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> WriteResult: ...
    async def query(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    async def query_logical(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    async def cursor(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        batch_size: int = 1000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> AsyncCursor: ...
    async def logical_cursor(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        batch_size: int = 1000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> AsyncCursor: ...
    async def create_collection(
        self,
        database: str,
        collection: str,
        *,
        options: Optional[BsonDocument] = None,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> CreateCollectionResult: ...
    async def list_collections(
        self,
        database: str,
        *,
        skip: int = 0,
        limit: Optional[int] = None,
        batch_size: int = 101,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> ListCollectionsResult: ...
    async def create_index(
        self,
        database: str,
        collection: str,
        keys: BsonDocument,
        *,
        name: str,
        unique: bool = False,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> CreateIndexResult: ...
    async def list_indexes(
        self,
        database: str,
        collection: str,
        *,
        skip: int = 0,
        limit: Optional[int] = None,
        batch_size: int = 101,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> ListIndexesResult: ...
    async def insert_one(
        self,
        database: str,
        collection: str,
        document: BsonDocument,
        *,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> InsertOneResult: ...
    async def find(
        self,
        database: str,
        collection: str,
        filter: Optional[BsonDocument] = None,
        *,
        skip: int = 0,
        limit: Optional[int] = None,
        batch_size: int = 101,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> FindResult: ...
    async def count_documents(
        self,
        database: str,
        collection: str,
        filter: Optional[BsonDocument] = None,
        *,
        skip: int = 0,
        limit: Optional[int] = None,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> CountDocumentsResult: ...
    async def delete_one(
        self,
        database: str,
        collection: str,
        filter: BsonDocument,
        *,
        request_id: Optional[UUID] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> DeleteOneResult: ...
    async def status(self) -> Status: ...
    async def close(self) -> None: ...
    async def __aenter__(self) -> AsyncSession: ...
    async def __aexit__(self, *exception: object) -> bool: ...

class AsyncTransaction:
    @property
    def native(self) -> Transaction: ...
    @property
    def closed(self) -> bool: ...
    async def get_state(self) -> str: ...
    async def set_routing_key(self, routing_key: str) -> None: ...
    async def execute(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> WriteResult: ...
    async def query(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    async def commit(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> str: ...
    async def rollback(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> str: ...
    async def __aenter__(self) -> AsyncTransaction: ...
    async def __aexit__(self, *exception: object) -> bool: ...

class AsyncDatabase:
    @property
    def native(self) -> Database: ...
    @property
    def path(self) -> PathLike[str]: ...
    @property
    def shard_count(self) -> int: ...
    @property
    def closed(self) -> bool: ...
    @property
    def state(self) -> str: ...
    async def session(self, *, routing_key: Optional[str] = None) -> AsyncSession: ...
    async def transaction(
        self,
        *,
        routing_key: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> AsyncTransaction: ...
    async def checkpoint(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> CheckpointReport: ...
    async def serve(
        self,
        *,
        http: str = "127.0.0.1:0",
        admin: Optional[str] = "127.0.0.1:0",
        postgres: Optional[str] = None,
        postgres_tls_cert: Optional[Union[str, PathLike[str]]] = None,
        postgres_tls_key: Optional[Union[str, PathLike[str]]] = None,
        postgres_user: str = "briskdb",
        postgres_password_file: Optional[Union[str, PathLike[str]]] = None,
    ) -> AsyncServer: ...
    async def close(self) -> CloseReport: ...
    async def __aenter__(self) -> AsyncDatabase: ...
    async def __aexit__(self, *exception: object) -> bool: ...

class AsyncServer:
    @property
    def native(self) -> Server: ...
    @property
    def http_address(self) -> str: ...
    @property
    def data_address(self) -> str: ...
    @property
    def admin_address(self) -> Optional[str]: ...
    @property
    def postgres_address(self) -> Optional[str]: ...
    @property
    def closed(self) -> bool: ...
    async def close(self) -> ServerCloseReport: ...
    async def __aenter__(self) -> AsyncServer: ...
    async def __aexit__(self, *exception: object) -> bool: ...

async def connect_async(
    path: Union[str, PathLike[str]],
    *,
    shards: Optional[int] = None,
    documents: bool = False,
    uuid_representation: Optional[UuidRepresentation] = None,
    config: Optional[Config] = None,
) -> AsyncDatabase: ...

open_async = connect_async
