from pathlib import Path
from typing import Any, Optional
from pymongo import (
    MongoClient as _MongoClient,
    AsyncMongoClient as _AsyncMongoClient,
    ASCENDING as ASCENDING,
    DESCENDING as DESCENDING,
    ReturnDocument as ReturnDocument,
    IndexModel as IndexModel,
    errors as errors,
)

class MongoClient(_MongoClient[dict[str, Any]]):
    def __init__(self, host: Any = ..., port: Any = ..., document_class: Any = ...,
                 tz_aware: Any = ..., connect: Any = ..., type_registry: Any = ...,
                 *, folder: Any = ..., shards: Optional[int] = ..., **kwargs: Any) -> None: ...
    @property
    def briskdb_path(self) -> Path: ...

class AsyncMongoClient(_AsyncMongoClient[dict[str, Any]]):
    def __init__(self, host: Any = ..., port: Any = ..., document_class: Any = ...,
                 tz_aware: Any = ..., connect: Any = ..., type_registry: Any = ...,
                 *, folder: Any = ..., shards: Optional[int] = ..., **kwargs: Any) -> None: ...
    @property
    def briskdb_path(self) -> Path: ...
