"""Local-client serialization preflight; server write semantics stay in PyMongo."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

from bson import BSON, ObjectId
from bson.raw_bson import RawBSONDocument
from pymongo.errors import BulkWriteError, DocumentTooLarge, InvalidOperation


# Match the local listener's advertised maxBsonObjectSize. An installed-wheel
# regression checks this boundary against hello so a server limit change cannot
# silently make this preflight stale. This is not a new wire or engine limit.
_MAX_DOCUMENT_BYTES = 512 * 1024


def prepare(collection: Any, documents: Any, ordered: Any,
            bypass_document_validation: Any) -> tuple[list[Any], list[RawBSONDocument], list[Any]]:
    """Consume once and encode once, before sending the first mutation.

    Keep immutable BSON snapshots so custom type encoders and mutable source
    documents cannot introduce a second serialization failure after an earlier
    wire batch commits. As with PyMongo, the entire iterable is materialized;
    this additionally retains its encoded bytes until the operation completes.
    """
    client = collection.database.client
    client._briskdb_store.check_process()
    if client._closed:
        raise InvalidOperation("Cannot use MongoClient after close")
    if type(ordered) is not bool:
        raise TypeError("ordered must be True or False")
    if bypass_document_validation is not None and type(bypass_document_validation) is not bool:
        raise TypeError("bypass_document_validation must be True or False")
    if not isinstance(documents, Iterable) or isinstance(documents, Mapping):
        raise TypeError("documents must be a non-empty iterable of documents")
    originals = list(documents)
    if not originals:
        raise TypeError("documents must be a non-empty iterable of documents")
    inserted_ids = []
    # Assign generated IDs to every mapping before serialization, preserving
    # the driver's caller-visible ID behavior even on an invalid later record.
    for document in originals:
        if isinstance(document, RawBSONDocument):
            continue
        if not isinstance(document, MutableMapping):
            raise TypeError("each document must be a mutable mapping or RawBSONDocument")
        if "_id" not in document:
            document["_id"] = ObjectId()
        inserted_ids.append(document["_id"])
    encoded = []
    for document in originals:
        raw = BSON.encode(document, codec_options=collection.codec_options)
        if len(raw) > _MAX_DOCUMENT_BYTES:
            raise DocumentTooLarge("document exceeds the local BriskDB BSON size limit")
        encoded.append(RawBSONDocument(raw))
    return originals, encoded, inserted_ids


def restore_error_operations(error: BulkWriteError, originals: list[Any]) -> None:
    """Keep PyMongo's public error indices and caller-owned operation objects."""
    for item in error.details.get("writeErrors", []):
        index = item.get("index")
        if type(index) is int and 0 <= index < len(originals):
            item["op"] = originals[index]
