"""Opt-in local-client index models; ordinary PyMongo classes are not patched."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import warnings

from bson import BSON
from pymongo.errors import InvalidOperation, OperationFailure
from pymongo.synchronous.collection import Collection as _Collection
from pymongo.synchronous.database import Database as _Database
from pymongo.asynchronous.collection import AsyncCollection as _AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase as _AsyncDatabase


class IndexCompatibilityWarning(UserWarning):
    """An index model was accepted with explicitly reduced behavior."""


def _unsupported(message: str) -> OperationFailure:
    return OperationFailure(message, code=115)


def _distinct_index_fields(keys: Any) -> None:
    """Reject duplicates before PyMongo folds a key sequence into a mapping.

    Leave other input validation and all ordinary index options to the driver
    and server. In particular, do not route create_index through the model
    compatibility path, which would change valid descending declarations.
    """
    if not isinstance(keys, (list, tuple)):
        return
    seen = set()
    for item in keys:
        if isinstance(item, str):
            field = item
        elif isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[0], str):
            field = item[0]
        else:
            # Preserve the driver's error for malformed key-pair shapes.
            return
        if field in seen:
            raise _unsupported("index fields must be distinct")
        seen.add(field)


def _models(indexes: Any, codec_options: Any) -> list[dict[str, Any]]:
    """Bound and copy the whole iterable before sending any mutation request."""
    models = []
    size = 0
    metadata_codec = codec_options.with_options(document_class=dict)
    for model in indexes:
        if len(models) == 1000:
            raise OperationFailure("index model batch exceeds capacity", code=10334)
        if isinstance(model, Mapping):
            document = model
        else:
            document = getattr(model, "document", None)
            if document is None:
                # TinyMongo IndexSpec's public metadata protocol, without a
                # TinyMongo dependency, import, or runtime class replacement.
                metadata = getattr(model, "to_metadata", None)
                document = metadata() if callable(metadata) else None
                if (not isinstance(document, Mapping)
                        or type(document.get("v")) is not int
                        or document["v"] not in (1, 2)
                        or set(document) - {"v", "key", "name", "unique", "sparse", "partialFilterExpression"}):
                    raise TypeError("index models need a mapping document or supported index metadata")
                document = {key: value for key, value in document.items() if key != "v"}
        if not isinstance(document, Mapping):
            raise TypeError("index model document must be a mapping")
        document = dict(document)
        keys = document.get("key")
        if isinstance(keys, Mapping):
            keys = keys.items()
        elif not isinstance(keys, (list, tuple)):
            raise _unsupported("index model keys must be a mapping or sequence of pairs")
        normalized = {}
        for pair in keys:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise _unsupported("index model keys must contain field/direction pairs")
            field, direction = pair
            if not isinstance(field, str) or not field or field in normalized:
                raise _unsupported("index model fields must be distinct nonempty strings")
            if isinstance(direction, bool) or direction not in (1, -1, "hashed", "text"):
                raise _unsupported("index model direction must be 1, -1, hashed, or text")
            normalized[field] = direction
        if not normalized:
            raise _unsupported("index model must contain at least one key")
        document["key"] = normalized
        # TinyMongo treats these explicit None options as absent. All actual
        # index semantics remain validated eagerly by the shared Rust engine.
        for field in ("name", "partialFilterExpression"):
            if document.get(field) is None:
                document.pop(field, None)
        encoded = BSON.encode(document, codec_options=metadata_codec)
        size += len(encoded)
        if size > 16 * 1024 * 1024:
            raise OperationFailure("index model batch exceeds capacity", code=10334)
        models.append(BSON(encoded).decode(codec_options=metadata_codec))
    return models


def _command(collection: Any, indexes: Any, session: Any, comment: Any,
             options: dict[str, Any]) -> tuple[dict[str, Any], int]:
    client = collection.database.client
    client._briskdb_store.check_process()
    # The pinned driver's closed flag also guards local-only empty batches;
    # skipping the wire must not make a closed client usable again.
    if client._closed:
        raise InvalidOperation("Cannot use MongoClient after close")
    if session is not None:
        raise OperationFailure("local BriskDB index models do not support sessions", code=72)
    if set(options) & {"createIndexes", "indexes", "briskdbIndexModelCompatibility", "writeConcern", "$db"}:
        raise TypeError("index command routing and compatibility options cannot be overridden")
    models = _models(indexes, collection.codec_options)
    # Mongo identifies the command by its first field, even with extra options.
    command = dict(createIndexes=collection.name, indexes=models, briskdbIndexModelCompatibility=True)
    command.update(options)
    if collection.write_concern.document:
        command["writeConcern"] = collection.write_concern.document
    if comment is not None:
        command["comment"] = comment
    return command, len(models)


def _result(reply: Any, count: int) -> list[str]:
    names = reply.get("briskdbIndexNames")
    if (not isinstance(names, list) or len(names) != count
            or not all(isinstance(name, str) for name in names)):
        raise InvalidOperation("BriskDB returned an invalid resolved index-name list")
    descriptions = {
        "descending": "descending direction is treated as ascending equality indexing",
        "hashed": "hashed indexing uses ascending equality keys",
        "ttl": "TTL expiration is not performed",
        "background": "background creation runs synchronously",
        "text": "text indexing is skipped; text search is not supported",
    }
    for warning in reply.get("briskdbIndexWarnings", []):
        details = "; ".join(descriptions.get(item.split(":", 1)[0], item)
                            for item in warning.get("reducedBehavior", []))
        message = f"Index {warning['name']!r} accepted with reduced behavior: {details}."
        if "reusedIndex" in warning:
            message += f" Reused existing index {warning['reusedIndex']!r}."
        warnings.warn(message, IndexCompatibilityWarning, stacklevel=3)
    return names


def _options(value: Any) -> dict[str, Any]:
    return dict(codec_options=value.codec_options, read_preference=value.read_preference,
                write_concern=value.write_concern, read_concern=value.read_concern)


class Collection(_Collection):
    """A real PyMongo collection with TinyMongo-style index-model input."""

    @classmethod
    def _wrap(cls, value: Any) -> Collection:
        return cls(value.database, value.name, **_options(value))

    def __getitem__(self, name: str) -> Collection:
        return self._wrap(super().__getitem__(name))

    def with_options(self, *args: Any, **kwargs: Any) -> Collection:
        return self._wrap(super().with_options(*args, **kwargs))

    def create_index(self, keys: Any, session: Any = None, comment: Any = None,
                     **kwargs: Any) -> str:
        _distinct_index_fields(keys)
        return super().create_index(keys, session=session, comment=comment, **kwargs)

    def create_indexes(self, indexes: Any, session: Any = None, comment: Any = None,
                       **kwargs: Any) -> list[str]:
        command, count = _command(self, indexes, session, comment, kwargs)
        if not count:
            return []
        reply = self.database.command(command, codec_options=self.codec_options.with_options(document_class=dict))
        return _result(reply, count)


class AsyncCollection(_AsyncCollection):
    """Async PyMongo collection using the same bounded model protocol."""

    @classmethod
    def _wrap(cls, value: Any) -> AsyncCollection:
        return cls(value.database, value.name, **_options(value))

    def __getitem__(self, name: str) -> AsyncCollection:
        return self._wrap(super().__getitem__(name))

    def with_options(self, *args: Any, **kwargs: Any) -> AsyncCollection:
        return self._wrap(super().with_options(*args, **kwargs))

    async def create_index(self, keys: Any, session: Any = None, comment: Any = None,
                           **kwargs: Any) -> str:
        _distinct_index_fields(keys)
        return await super().create_index(keys, session=session, comment=comment, **kwargs)

    async def create_indexes(self, indexes: Any, session: Any = None, comment: Any = None,
                             **kwargs: Any) -> list[str]:
        command, count = _command(self, indexes, session, comment, kwargs)
        if not count:
            return []
        reply = await self.database.command(command, codec_options=self.codec_options.with_options(document_class=dict))
        return _result(reply, count)


class Database(_Database):
    @classmethod
    def _wrap(cls, value: Any) -> Database:
        return cls(value.client, value.name, **_options(value))

    def __getitem__(self, name: str) -> Collection:
        return Collection._wrap(super().__getitem__(name))

    def get_collection(self, *args: Any, **kwargs: Any) -> Collection:
        return Collection._wrap(super().get_collection(*args, **kwargs))

    def with_options(self, *args: Any, **kwargs: Any) -> Database:
        return self._wrap(super().with_options(*args, **kwargs))

    def create_collection(self, *args: Any, **kwargs: Any) -> Collection:
        return Collection._wrap(super().create_collection(*args, **kwargs))


class AsyncDatabase(_AsyncDatabase):
    @classmethod
    def _wrap(cls, value: Any) -> AsyncDatabase:
        return cls(value.client, value.name, **_options(value))

    def __getitem__(self, name: str) -> AsyncCollection:
        return AsyncCollection._wrap(super().__getitem__(name))

    def get_collection(self, *args: Any, **kwargs: Any) -> AsyncCollection:
        return AsyncCollection._wrap(super().get_collection(*args, **kwargs))

    def with_options(self, *args: Any, **kwargs: Any) -> AsyncDatabase:
        return self._wrap(super().with_options(*args, **kwargs))

    async def create_collection(self, *args: Any, **kwargs: Any) -> AsyncCollection:
        return AsyncCollection._wrap(await super().create_collection(*args, **kwargs))
