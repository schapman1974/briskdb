from decimal import Decimal
from os import PathLike
from typing import Any, Dict, Iterator, List, Literal, Mapping, Optional, Sequence, Tuple, TypedDict, Union
from uuid import UUID

SqlParameter = Union[None, bool, int, float, Decimal, str, bytes, bytearray, memoryview]
SqlRow = Tuple[object, ...]
BsonDocument = Mapping[str, Any]
BsonResultDocument = Dict[str, Any]
UuidRepresentation = Literal[
    "unspecified", "standard", "python_legacy", "java_legacy", "csharp_legacy"
]
DocumentPlanKind = Literal["point", "scatter"]
DocumentIndexLifecycle = Literal["ready", "pending_build"]

class ColumnInfo(TypedDict):
    name: str
    type: str

class QueryResult(TypedDict):
    shards: List[int]
    columns: List[ColumnInfo]
    rows: List[SqlRow]

class GeneratedKey(TypedDict):
    column: str
    value: object

class WriteResult(TypedDict):
    shard: int
    rows_affected: int
    generated_key: Optional[GeneratedKey]

class DocumentPlan(TypedDict):
    kind: DocumentPlanKind
    collection_id: int
    shards: List[int]

class DocumentNamespaceInfo(TypedDict):
    database: str
    collection: str

class DocumentPlacementInfo(TypedDict):
    code: int
    version: int

class DocumentIndexInfo(TypedDict):
    name: str
    keys: BsonResultDocument
    unique: bool
    built_in: bool
    lifecycle: DocumentIndexLifecycle

class DocumentCollectionInfo(TypedDict):
    id: int
    database_id: int
    database: str
    name: str
    namespace: str
    options: BsonResultDocument
    placement: DocumentPlacementInfo
    indexes: List[DocumentIndexInfo]

class DocumentExecution(TypedDict):
    request_id: UUID
    plan: Optional[DocumentPlan]

class CreateCollectionResult(DocumentExecution):
    kind: Literal["collection"]
    collection: DocumentCollectionInfo

class ListCollectionsResult(DocumentExecution):
    kind: Literal["collections"]
    collections: List[DocumentCollectionInfo]

class CreateIndexResult(DocumentExecution):
    kind: Literal["index_name"]
    index_name: str
    lifecycle: Literal["pending_build"]

class ListIndexesResult(DocumentExecution):
    kind: Literal["indexes"]
    indexes: List[DocumentIndexInfo]

class InsertOneResult(DocumentExecution):
    kind: Literal["insert"]
    acknowledged: bool
    inserted_count: int
    inserted_ids: List[Any]

class FindResult(DocumentExecution):
    kind: Literal["cursor"]
    namespace: DocumentNamespaceInfo
    cursor_id: Optional[int]
    exhausted: bool
    documents: List[BsonResultDocument]

class CountDocumentsResult(DocumentExecution):
    kind: Literal["count"]
    count: int

class DeleteOneResult(DocumentExecution):
    kind: Literal["delete"]
    acknowledged: bool
    deleted_count: int

class CloseReport(TypedDict):
    already_closed: bool
    forced: bool

class ServerCloseReport(TypedDict):
    already_closed: bool

class CheckpointShard(TypedDict):
    shard: int
    busy: bool
    counts_available: bool
    wal_frames: int
    checkpointed_frames: int
    complete: bool

class CheckpointReport(TypedDict):
    busy: bool
    complete: bool
    shards: List[CheckpointShard]

class Status(TypedDict):
    shards: int
    max_blocking_workers: int
    connections_per_shard: int
    queue_capacity_per_shard: int
    max_result_rows: int
    max_result_bytes: int
    request_timeout_ms: Optional[int]
    shutdown_grace_ms: int

class BriskDBError(Exception):
    code: str
    retryable: bool

class DataError(BriskDBError): ...
class OperationalError(BriskDBError): ...
class IntegrityError(BriskDBError): ...
class ProgrammingError(BriskDBError): ...
class InvalidArgumentError(ProgrammingError): ...
class NumericOutOfRangeError(DataError): ...
class InvalidTextEncodingError(DataError): ...
class InvalidQueryError(ProgrammingError): ...
class UnsupportedError(ProgrammingError): ...
class FailedPreconditionError(OperationalError): ...
class IdempotencyConflictError(IntegrityError): ...
class TypeMismatchError(DataError): ...
class ConstraintViolationError(IntegrityError): ...
class UniqueViolationError(ConstraintViolationError): ...
class NotNullViolationError(ConstraintViolationError): ...
class ForeignKeyViolationError(ConstraintViolationError): ...
class CheckViolationError(ConstraintViolationError): ...
class PermissionDeniedError(OperationalError): ...
class ReadOnlyError(OperationalError): ...
class BusyError(OperationalError): ...
class CancelledError(OperationalError): ...
class DeadlineExceededError(OperationalError): ...
class LimitExceededError(OperationalError): ...
class ShuttingDownError(OperationalError): ...
class StorageFullError(OperationalError): ...
class OutOfMemoryError(OperationalError): ...
class StorageUnavailableError(OperationalError): ...
class DataCorruptionError(OperationalError): ...
class InternalError(OperationalError): ...

class Config:
    shards: Optional[int]
    documents: bool
    uuid_representation: UuidRepresentation
    connections_per_shard: int
    queue_capacity_per_shard: int
    max_result_rows: int
    max_result_bytes: int
    max_prepared_statements_per_session: int
    max_portals_per_session: int
    max_retained_bound_value_bytes: int
    request_timeout_ms: int
    shutdown_grace_ms: int
    def __init__(
        self,
        *,
        shards: Optional[int] = ...,
        documents: bool = ...,
        uuid_representation: UuidRepresentation = ...,
        connections_per_shard: int = ...,
        queue_capacity_per_shard: int = ...,
        max_result_rows: int = ...,
        max_result_bytes: int = ...,
        max_prepared_statements_per_session: int = ...,
        max_portals_per_session: int = ...,
        max_retained_bound_value_bytes: int = ...,
        request_timeout_ms: int = ...,
        shutdown_grace_ms: int = ...,
    ) -> None: ...

class CancellationToken:
    def __init__(self) -> None: ...
    @property
    def cancelled(self) -> bool: ...
    def cancel(self) -> bool: ...

class Cursor(Iterator[SqlRow]):
    @property
    def shards(self) -> List[int]: ...
    @property
    def columns(self) -> List[ColumnInfo]: ...
    @property
    def closed(self) -> bool: ...
    def fetchone(self) -> Optional[SqlRow]: ...
    def fetchmany(self, size: Optional[int] = None) -> List[SqlRow]: ...
    def fetchall(self) -> List[SqlRow]: ...
    def close(self) -> None: ...
    def __iter__(self) -> Cursor: ...
    def __next__(self) -> SqlRow: ...
    def __enter__(self) -> Cursor: ...
    def __exit__(self, *exception: object) -> bool: ...

class Database:
    def __init__(
        self,
        path: Union[str, PathLike[str]],
        *,
        shards: Optional[int] = None,
        documents: bool = False,
        uuid_representation: Optional[UuidRepresentation] = None,
        config: Optional[Config] = None,
    ) -> None: ...
    @property
    def path(self) -> PathLike[str]: ...
    @property
    def shard_count(self) -> int: ...
    @property
    def config(self) -> Config: ...
    @property
    def closed(self) -> bool: ...
    @property
    def state(self) -> str: ...
    def session(self, *, routing_key: Optional[str] = None) -> Session: ...
    def transaction(
        self,
        *,
        routing_key: Optional[str] = None,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> Transaction: ...
    def checkpoint(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> CheckpointReport: ...
    def serve(
        self,
        *,
        http: str = "127.0.0.1:0",
        admin: Optional[str] = "127.0.0.1:0",
        postgres: Optional[str] = None,
        postgres_tls_cert: Optional[Union[str, PathLike[str]]] = None,
        postgres_tls_key: Optional[Union[str, PathLike[str]]] = None,
        postgres_user: str = "briskdb",
        postgres_password_file: Optional[Union[str, PathLike[str]]] = None,
    ) -> Server: ...
    def close(self) -> CloseReport: ...
    def __enter__(self) -> Database: ...
    def __exit__(self, *exception: object) -> bool: ...

class Server:
    @property
    def data_address(self) -> str: ...
    @property
    def http_address(self) -> str: ...
    @property
    def admin_address(self) -> Optional[str]: ...
    @property
    def postgres_address(self) -> Optional[str]: ...
    @property
    def closed(self) -> bool: ...
    def close(self) -> ServerCloseReport: ...
    def __enter__(self) -> Server: ...
    def __exit__(self, *exception: object) -> bool: ...

class Session:
    @property
    def closed(self) -> bool: ...
    @property
    def state(self) -> str: ...
    @property
    def database_state(self) -> str: ...
    @property
    def routing_key(self) -> Optional[str]: ...
    def set_routing_key(self, routing_key: str) -> None: ...
    def clear_routing_key(self) -> None: ...
    def migrate(
        self,
        sql: str,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> List[int]: ...
    def execute(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> WriteResult: ...
    def query(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    def query_logical(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    def cursor(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        batch_size: int = 1000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> Cursor: ...
    def logical_cursor(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        batch_size: int = 1000,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> Cursor: ...
    def create_collection(
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
    def list_collections(
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
    def create_index(
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
    def list_indexes(
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
    def insert_one(
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
    def find(
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
    def count_documents(
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
    def delete_one(
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
    def status(self) -> Status: ...
    def close(self) -> None: ...
    def __enter__(self) -> Session: ...
    def __exit__(self, *exception: object) -> bool: ...

class Transaction:
    @property
    def closed(self) -> bool: ...
    @property
    def state(self) -> str: ...
    def set_routing_key(self, routing_key: str) -> None: ...
    def execute(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> WriteResult: ...
    def query(
        self,
        sql: str,
        params: Optional[Sequence[SqlParameter]] = None,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> QueryResult: ...
    def commit(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> str: ...
    def rollback(
        self,
        *,
        timeout_ms: Optional[int] = None,
        cancellation: Optional[CancellationToken] = None,
    ) -> str: ...
    def __enter__(self) -> Transaction: ...
    def __exit__(self, *exception: object) -> bool: ...

def open(
    path: Union[str, PathLike[str]],
    *,
    shards: Optional[int] = None,
    documents: bool = False,
    uuid_representation: Optional[UuidRepresentation] = None,
    config: Optional[Config] = None,
) -> Database: ...

__version__: str
