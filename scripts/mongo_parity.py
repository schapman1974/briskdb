#!/usr/bin/env python3
"""Validate, run, ingest, and compare BriskDB's Mongo compatibility contract.

The harness intentionally uses only the Python standard library. Contract
implementations run as child processes and communicate through JUnit XML, so
BriskDB never imports TinyMongo or PyMongo into its own runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


SCHEMA_VERSION = 1
OUTCOMES = frozenset(("passed", "failed", "error", "skipped", "xfailed", "xpassed"))
EXPECTED_APIS = ("sync", "async")
EXPECTED_BACKENDS = (
    "memory",
    "json",
    "sqlite",
    "sqlite-sharded",
    "duckdb",
    "parquet",
    "mongodb",
)
EXPECTED_CASE_COUNT = 228
EXPECTED_MODULE_COUNT = 16

EXPECTED_CAPABILITY_CATEGORIES = frozenset(
    (
        "aggregation",
        "api",
        "commands",
        "errors",
        "indexes",
        "projections",
        "queries",
        "results",
        "unsupported",
        "updates",
        "values",
        "warnings",
    )
)

EXPECTED_API_OWNER_CLASSES = {
    "async_client": ("tinymongo.AsyncTinyMongoClient", "tinymongo.AsyncMongoClient"),
    "async_collection": (
        "tinymongo.AsyncTinyMongoCollection",
        "tinymongo.AsyncCollection",
    ),
    "async_cursor": ("tinymongo.AsyncTinyMongoCursor", "tinymongo.AsyncCursor"),
    "async_database": ("tinymongo.AsyncTinyMongoDatabase", "tinymongo.AsyncDatabase"),
    "gridfs_placeholder": ("tinymongo.TinyGridFS",),
    "sync_client": ("tinymongo.TinyMongoClient", "tinymongo.MongoClient"),
    "sync_collection": ("tinymongo.TinyMongoCollection",),
    "sync_cursor": ("tinymongo.TinyMongoCursor",),
    "sync_database": ("tinymongo.TinyMongoDatabase",),
}

EXPECTED_API_CONSTRUCTOR_SIGNATURES = {
    "tinymongo.AsyncMongoClient": "AsyncMongoClient(host=None, port=None, "
    "document_class=None, tz_aware=None, connect=None, "
    "type_registry=None, **kwargs)",
    "tinymongo.AsyncTinyMongoClient": "AsyncTinyMongoClient(foldername='tinydb', "
    "backend='tinydb', *, tinymongo_folder=None, "
    "threads=None, sqlite_shards=None, "
    "storage_uri=None, duckdb_config=None, dsn=None)",
    "tinymongo.AsyncTinyMongoCollection": "AsyncTinyMongoCollection(database, name)",
    "tinymongo.AsyncTinyMongoCursor": "AsyncTinyMongoCursor(collection=None, filter=None, "
    "projection=None, find_args=(), find_kwargs=None, "
    "documents=None)",
    "tinymongo.AsyncTinyMongoDatabase": "AsyncTinyMongoDatabase(client, name)",
    "tinymongo.MongoClient": "MongoClient(host=None, port=None, document_class=None, "
    "tz_aware=None, connect=None, type_registry=None, **kwargs)",
    "tinymongo.TinyGridFS": "TinyGridFS(*args, **kwargs)",
    "tinymongo.TinyMongoClient": "TinyMongoClient(foldername='tinydb', backend='tinydb', "
    "*, tinymongo_folder=None, threads=None, "
    "sqlite_shards=None, storage_uri=None, "
    "duckdb_config=None, dsn=None)",
    "tinymongo.TinyMongoCollection": "TinyMongoCollection(table, parent=None)",
    "tinymongo.TinyMongoCursor": "TinyMongoCursor(cursordat, sort=None, skip=None, "
    "limit=None, collection=None, projection=None, "
    "query=None, source_projected=False, "
    "deferred_loader=None, materializer=None)",
    "tinymongo.TinyMongoDatabase": "TinyMongoDatabase(database, path, storage, "
    "engine=None, client=None)",
}

EXPECTED_API_CONSTRUCTOR_OPTIONS = {
    "tinymongo.AsyncMongoClient": (
        "host",
        "port",
        "document_class",
        "tz_aware",
        "connect",
        "type_registry",
        "**kwargs",
    ),
    "tinymongo.AsyncTinyMongoClient": (
        "foldername",
        "backend",
        "tinymongo_folder",
        "threads",
        "sqlite_shards",
        "storage_uri",
        "duckdb_config",
        "dsn",
    ),
    "tinymongo.AsyncTinyMongoCollection": (),
    "tinymongo.AsyncTinyMongoCursor": (
        "collection",
        "filter",
        "projection",
        "find_args",
        "find_kwargs",
        "documents",
    ),
    "tinymongo.AsyncTinyMongoDatabase": (),
    "tinymongo.MongoClient": (
        "host",
        "port",
        "document_class",
        "tz_aware",
        "connect",
        "type_registry",
        "**kwargs",
    ),
    "tinymongo.TinyGridFS": ("*args", "**kwargs"),
    "tinymongo.TinyMongoClient": (
        "foldername",
        "backend",
        "tinymongo_folder",
        "threads",
        "sqlite_shards",
        "storage_uri",
        "duckdb_config",
        "dsn",
    ),
    "tinymongo.TinyMongoCollection": ("parent",),
    "tinymongo.TinyMongoCursor": (
        "sort",
        "skip",
        "limit",
        "collection",
        "projection",
        "query",
        "source_projected",
        "deferred_loader",
        "materializer",
    ),
    "tinymongo.TinyMongoDatabase": ("engine", "client"),
}

EXPECTED_OPERATION_RESULT_SHAPES = {
    "async_client": {
        "capabilities": "capabilities_document",
        "close": "none",
        "database_names": "string_list",
        "drop_database": "none",
        "get_database": "database_handle_async",
        "list_database_names": "string_list",
        "list_databases": "database_metadata_cursor_async",
        "server_info": "server_info_document",
        "start_session": "unsupported_error",
        "supports": "boolean",
        "watch": "unsupported_error",
    },
    "async_collection": {
        "aggregate": "cursor_async",
        "bulk_write": "unsupported_error",
        "count": "integer",
        "count_documents": "integer",
        "create_index": "string",
        "create_indexes": "string_list",
        "delete_many": "delete_result",
        "delete_one": "delete_result",
        "distinct": "value_list",
        "drop": "boolean",
        "drop_index": "none",
        "estimated_document_count": "integer",
        "find": "cursor_async",
        "find_one": "document_or_none",
        "find_one_and_delete": "document_or_none",
        "find_one_and_replace": "document_or_none",
        "find_one_and_update": "document_or_none",
        "index_information": "index_information_document",
        "insert": "legacy_insert_result",
        "insert_many": "insert_many_result",
        "insert_one": "insert_one_result",
        "list_indexes": "index_metadata_cursor_async",
        "remove": "delete_result",
        "replace_one": "update_result",
        "update": "legacy_update_result",
        "update_many": "update_result",
        "update_one": "update_result",
        "watch": "unsupported_error",
        "with_options": "self_async_collection",
    },
    "async_cursor": {
        "clone": "cursor_handle_async",
        "close": "none",
        "count": "integer",
        "hasNext": "boolean",
        "has_next": "boolean",
        "limit": "self_async_cursor",
        "next": "document",
        "paginate": "self_async_cursor",
        "rewind": "self_async_cursor",
        "skip": "self_async_cursor",
        "sort": "self_async_cursor",
        "to_list": "document_list",
        "try_next": "document_or_none",
    },
    "async_database": {
        "close": "none",
        "collection_names": "string_list",
        "command": "command_document",
        "drop_collection": "boolean",
        "get_collection": "collection_handle_async",
        "list_collection_names": "string_list",
        "watch": "unsupported_error",
    },
    "gridfs_placeholder": {
        "GridFS": "self_gridfs_placeholder",
        "grid_fs": "self_gridfs_placeholder",
    },
    "sync_client": {
        "capabilities": "capabilities_document",
        "close": "none",
        "database_names": "string_list",
        "drop_database": "none",
        "get_database": "database_handle_sync",
        "list_database_names": "string_list",
        "list_databases": "database_metadata_cursor_sync",
        "server_info": "server_info_document",
        "start_session": "unsupported_error",
        "supports": "boolean",
        "watch": "unsupported_error",
    },
    "sync_collection": {
        "aggregate": "cursor_sync",
        "build_table": "none",
        "bulk_write": "unsupported_error",
        "count": "integer",
        "count_documents": "integer",
        "create_index": "string",
        "create_indexes": "string_list",
        "delete_many": "delete_result",
        "delete_one": "delete_result",
        "distinct": "value_list",
        "drop": "boolean",
        "drop_index": "none",
        "estimated_document_count": "integer",
        "find": "cursor_sync",
        "find_one": "document_or_none",
        "find_one_and_delete": "document_or_none",
        "find_one_and_replace": "document_or_none",
        "find_one_and_update": "document_or_none",
        "index_information": "index_information_document",
        "insert": "legacy_insert_result",
        "insert_many": "insert_many_result",
        "insert_one": "insert_one_result",
        "list_indexes": "index_metadata_list",
        "parse_condition": "query_condition_iterator",
        "parse_query": "query_object",
        "remove": "delete_result",
        "replace_one": "update_result",
        "update": "legacy_update_result",
        "update_many": "update_result",
        "update_one": "update_result",
        "watch": "unsupported_error",
        "with_options": "self_sync_collection",
    },
    "sync_cursor": {
        "clone": "cursor_handle_sync",
        "close": "none",
        "count": "integer",
        "hasNext": "boolean",
        "has_next": "boolean",
        "limit": "self_sync_cursor",
        "next": "document",
        "paginate": "self_sync_cursor",
        "rewind": "self_sync_cursor",
        "skip": "self_sync_cursor",
        "sort": "self_sync_cursor",
        "to_list": "document_list",
    },
    "sync_database": {
        "close": "none",
        "collection_names": "string_list",
        "command": "command_document",
        "drop_collection": "boolean",
        "get_collection": "collection_handle_sync",
        "list_collection_names": "string_list",
        "watch": "unsupported_error",
    },
}

EXPECTED_WRITE_RESULT_FIELDS = {
    "tinymongo.results.DeleteResult": ("acknowledged", "deleted_count", "raw_result"),
    "tinymongo.results.InsertManyResult": ("acknowledged", "eids", "inserted_ids"),
    "tinymongo.results.InsertOneResult": ("acknowledged", "eid", "inserted_id"),
    "tinymongo.results.UpdateResult": (
        "acknowledged",
        "did_upsert",
        "matched_count",
        "modified_count",
        "raw_result",
        "upserted_id",
    ),
}

EXPECTED_WRITE_RESULT_CONSTRUCTORS = {
    "tinymongo.results.DeleteResult": "DeleteResult(raw_result, acknowledged=True)",
    "tinymongo.results.InsertManyResult": (
        "InsertManyResult(eids, inserted_ids, acknowledged=True)"
    ),
    "tinymongo.results.InsertOneResult": (
        "InsertOneResult(eid, inserted_id, acknowledged=True)"
    ),
    "tinymongo.results.UpdateResult": (
        "UpdateResult(raw_result, acknowledged=True, matched_count=None, "
        "modified_count=None, upserted_id=None)"
    ),
}

EXPECTED_RESULT_SHAPE_IDS = frozenset(
    (
        "boolean",
        "boolean_true",
        "bson_value",
        "capabilities_document",
        "collection_handle_async",
        "collection_handle_sync",
        "command_document",
        "cursor_async",
        "cursor_handle_async",
        "cursor_handle_sync",
        "cursor_sync",
        "database_handle_async",
        "database_handle_sync",
        "database_metadata_cursor_async",
        "database_metadata_cursor_sync",
        "delete_result",
        "document",
        "document_list",
        "document_or_none",
        "index_information_document",
        "index_metadata_cursor_async",
        "index_metadata_document",
        "index_metadata_list",
        "insert_many_result",
        "insert_one_result",
        "integer",
        "key_direction_pair",
        "key_direction_pair_array",
        "legacy_insert_result",
        "legacy_update_result",
        "none",
        "original_document",
        "path_or_uri",
        "query_condition_iterator",
        "query_object",
        "self_async_collection",
        "self_async_cursor",
        "self_gridfs_placeholder",
        "self_sync_collection",
        "self_sync_cursor",
        "server_info_document",
        "string",
        "string_list",
        "string_or_none",
        "unsupported_error",
        "update_result",
        "update_result_list",
        "value_list",
    )
)
EXPECTED_RESULT_SHAPES_SHA256 = (
    "e3f5096f041eb120bc260ac0b05058fddad3f63ee8a10d278cdda098386ac204"
)
EXPECTED_PYMONGO_CONNECTION_OPTIONS = (
    "appname",
    "authmechanism",
    "authmechanismproperties",
    "authoidcallowedhosts",
    "authsource",
    "auto_encryption_opts",
    "compressors",
    "connect",
    "connecttimeoutms",
    "datetime_conversion",
    "directconnection",
    "document_class",
    "driver",
    "enable_overload_retargeting",
    "enableoverloadretargeting",
    "event_listeners",
    "fsync",
    "heartbeatfrequencyms",
    "journal",
    "loadbalanced",
    "localthresholdms",
    "max_adaptive_retries",
    "maxadaptiveretries",
    "maxconnecting",
    "maxidletimems",
    "maxpoolsize",
    "maxstalenessseconds",
    "minpoolsize",
    "password",
    "read_preference",
    "readconcernlevel",
    "readpreference",
    "readpreferencetags",
    "replicaset",
    "retryreads",
    "retrywrites",
    "server_api",
    "server_selector",
    "servermonitoringmode",
    "serverselectiontimeoutms",
    "sockettimeoutms",
    "srvmaxhosts",
    "srvservicename",
    "ssl",
    "timeoutms",
    "tls",
    "tlsallowinvalidcertificates",
    "tlsallowinvalidhostnames",
    "tlscafile",
    "tlscertificatekeyfile",
    "tlscertificatekeyfilepassword",
    "tlscrlfile",
    "tlsdisableocspendpointcheck",
    "tlsinsecure",
    "type_registry",
    "tz_aware",
    "tzinfo",
    "unicode_decode_error_handler",
    "username",
    "uuidrepresentation",
    "w",
    "waitqueuemultiple",
    "waitqueuetimeoutms",
    "wtimeoutms",
    "zlibcompressionlevel",
)

EXPECTED_API_ALIASES = {
    "tinymongo.AsyncCollection": "tinymongo.AsyncTinyMongoCollection",
    "tinymongo.AsyncCursor": "tinymongo.AsyncTinyMongoCursor",
    "tinymongo.AsyncDatabase": "tinymongo.AsyncTinyMongoDatabase",
}
EXPECTED_API_UNSUPPORTED_METHODS = {
    "async_client": frozenset(("start_session", "watch")),
    "async_collection": frozenset(("bulk_write", "watch")),
    "async_cursor": frozenset(),
    "async_database": frozenset(("watch",)),
    "gridfs_placeholder": frozenset(("GridFS", "grid_fs")),
    "sync_client": frozenset(("start_session", "watch")),
    "sync_collection": frozenset(("bulk_write", "watch")),
    "sync_cursor": frozenset(),
    "sync_database": frozenset(("watch",)),
}
EXPECTED_API_EXTENSION_METHODS = {
    "async_client": frozenset(("capabilities", "database_names", "supports")),
    "async_collection": frozenset(("count", "insert", "remove", "update")),
    "async_cursor": frozenset(("count", "hasNext", "has_next", "paginate")),
    "async_database": frozenset(("collection_names",)),
    "gridfs_placeholder": frozenset(),
    "sync_client": frozenset(("capabilities", "database_names", "supports")),
    "sync_collection": frozenset(
        (
            "build_table",
            "count",
            "insert",
            "parse_condition",
            "parse_query",
            "remove",
            "update",
        )
    ),
    "sync_cursor": frozenset(("count", "hasNext", "has_next", "paginate")),
    "sync_database": frozenset(("collection_names",)),
}
EXPECTED_AWAITABLE_METHODS = {
    "async_client": frozenset(
        (
            "capabilities",
            "close",
            "database_names",
            "drop_database",
            "list_database_names",
            "list_databases",
            "server_info",
            "supports",
            "watch",
        )
    ),
    "async_collection": frozenset(
        (
            "aggregate",
            "bulk_write",
            "count",
            "count_documents",
            "create_index",
            "create_indexes",
            "delete_many",
            "delete_one",
            "distinct",
            "drop",
            "drop_index",
            "estimated_document_count",
            "find_one",
            "find_one_and_delete",
            "find_one_and_replace",
            "find_one_and_update",
            "index_information",
            "insert",
            "insert_many",
            "insert_one",
            "list_indexes",
            "remove",
            "replace_one",
            "update",
            "update_many",
            "update_one",
            "watch",
        )
    ),
    "async_cursor": frozenset(
        (
            "close",
            "count",
            "hasNext",
            "has_next",
            "next",
            "rewind",
            "to_list",
            "try_next",
        )
    ),
    "async_database": frozenset(
        (
            "close",
            "collection_names",
            "command",
            "drop_collection",
            "list_collection_names",
            "watch",
        )
    ),
    "gridfs_placeholder": frozenset(),
    "sync_client": frozenset(),
    "sync_collection": frozenset(),
    "sync_cursor": frozenset(),
    "sync_database": frozenset(),
}
EXPECTED_API_BEHAVIORS = frozenset(
    (
        "accepted_no_effect",
        "applied",
        "applied_conditionally",
        "default_or_empty_only",
        "dispatched",
        "null_only",
        "rejected",
        "rejected_unsupported",
    )
)
EXPECTED_TINYMONGO_STORAGE_OPTIONS = frozenset(
    (
        "backend",
        "dsn",
        "duckdb_config",
        "foldername",
        "sqlite_shards",
        "storage_uri",
        "threads",
        "tinymongo_folder",
        "tinymongo_path",
    )
)
EXPECTED_NATIVE_BSON_TYPES = (
    "array",
    "binary",
    "boolean",
    "datetime",
    "double",
    "int",
    "long",
    "null",
    "object",
    "regex",
    "string",
    "uuid",
)
EXPECTED_BSON_FAMILY_ALIASES = {"int": "int32", "long": "int64"}
EXPECTED_OPTIONAL_PYMONGO_BSON_TYPES = (
    "Binary",
    "Code",
    "Decimal128",
    "MaxKey",
    "MinKey",
    "ObjectId",
    "Regex",
    "Timestamp",
)
EXPECTED_ERROR_CLASSES = frozenset(
    (
        "BulkWriteError",
        "ConfigurationError",
        "ConnectionFailure",
        "CursorNotFound",
        "DuplicateKeyError",
        "InvalidDocument",
        "InvalidOperation",
        "LockError",
        "OperationFailure",
        "StorageCorruptionError",
        "StorageError",
        "TinyMongoError",
        "TinyMongoNotSupportedError",
        "WriteError",
    )
)
EXPECTED_ERROR_FIELDS = {
    "BulkWriteError": frozenset(("code", "details", "timeout")),
    "ConfigurationError": frozenset(("timeout",)),
    "ConnectionFailure": frozenset(("timeout",)),
    "CursorNotFound": frozenset(("code", "details", "timeout")),
    "DuplicateKeyError": frozenset(("code", "details", "timeout")),
    "InvalidDocument": frozenset(("document", "timeout")),
    "InvalidOperation": frozenset(("timeout",)),
    "LockError": frozenset(("timeout",)),
    "OperationFailure": frozenset(("code", "details", "timeout")),
    "StorageCorruptionError": frozenset(("timeout",)),
    "StorageError": frozenset(("timeout",)),
    "TinyMongoError": frozenset(("timeout",)),
    "TinyMongoNotSupportedError": frozenset(("timeout",)),
    "WriteError": frozenset(("code", "details", "timeout")),
}
EXPECTED_QUERY_CAPABILITIES = {
    "field_operators": [
        "$all",
        "$elemMatch",
        "$eq",
        "$exists",
        "$gt",
        "$gte",
        "$in",
        "$lt",
        "$lte",
        "$mod",
        "$ne",
        "$nin",
        "$not",
        "$options",
        "$regex",
        "$size",
        "$type",
    ],
    "ignored_metadata_operators": ["$comment"],
    "ignored_metadata_operator_scope": (
        "query-document keys only; field-operator use is invalid"
    ),
    "logical_operators": ["$and", "$nor", "$or"],
    "regex_options": ["i", "m", "s", "u", "x"],
    "unsupported_operators": [
        "$bitsAllClear",
        "$bitsAllSet",
        "$bitsAnyClear",
        "$bitsAnySet",
        "$expr",
        "$geoIntersects",
        "$geoWithin",
        "$jsonSchema",
        "$near",
        "$nearSphere",
        "$text",
        "$where",
    ],
    "type_aliases": [
        "minKey",
        "double",
        "string",
        "object",
        "array",
        "binData",
        "undefined",
        "objectId",
        "bool",
        "date",
        "null",
        "regex",
        "dbPointer",
        "javascript",
        "symbol",
        "javascriptWithScope",
        "int",
        "timestamp",
        "long",
        "decimal",
        "maxKey",
        "number",
    ],
    "type_codes": {
        "-1": "minKey",
        "1": "double",
        "2": "string",
        "3": "object",
        "4": "array",
        "5": "binData",
        "6": "undefined",
        "7": "objectId",
        "8": "bool",
        "9": "date",
        "10": "null",
        "11": "regex",
        "12": "dbPointer",
        "13": "javascript",
        "14": "symbol",
        "15": "javascriptWithScope",
        "16": "int",
        "17": "timestamp",
        "18": "long",
        "19": "decimal",
        "127": "maxKey",
    },
    "rules": [
        "array and dotted-path traversal",
        "BSON type brackets",
        "missing/null distinction",
        "numeric cross-type comparison",
        "recursive BSON equality",
        "regex option normalization",
    ],
}
EXPECTED_UPDATE_CAPABILITIES = {
    "modifiers": {
        "$addToSet": ["$each"],
        "$push": ["$each", "$position", "$slice", "$sort"],
    },
    "operators": [
        "$addToSet",
        "$inc",
        "$max",
        "$min",
        "$pop",
        "$pull",
        "$pullAll",
        "$push",
        "$rename",
        "$set",
        "$unset",
    ],
    "path_policies": {
        "mapping_only_dotted_paths": ["$addToSet", "$inc", "$set", "$unset"],
        "numeric_array_index_paths": [
            "$max",
            "$min",
            "$pop",
            "$pull",
            "$pullAll",
            "$push",
        ],
        "rejects_array_element_and_positional_paths": ["$rename"],
    },
    "pull_operators": {
        "document_field_extra": ["$not"],
        "logical": ["$and", "$or", "$nor"],
        "scalar_and_document_field": [
            "$all",
            "$elemMatch",
            "$eq",
            "$exists",
            "$gt",
            "$gte",
            "$in",
            "$lt",
            "$lte",
            "$mod",
            "$ne",
            "$nin",
            "$options",
            "$regex",
            "$size",
            "$type",
        ],
    },
    "rules": [
        "atomic post-image validation",
        (
            "root _id remains unchanged after successful updates; error behavior "
            "is operator-specific"
        ),
        "replacement writes",
        "single and multi writes",
        "upsert equality seeding",
    ],
}
EXPECTED_PROJECTION_CAPABILITIES = {
    "rules": [
        "boolean and BSON-number flags only",
        "empty mapping or field sequence means no projection",
        "nested mappings are dotted-path shorthand",
        "path collisions are rejected",
    ],
    "supported": [
        "array traversal",
        "dotted paths",
        "exclusion",
        "explicit _id inclusion/exclusion",
        "inclusion",
        "list-of-fields shorthand",
    ],
    "unsupported": [
        "$elemMatch projection",
        "$slice projection",
        "expression projection in find",
        "mixed inclusion/exclusion except _id",
        "numeric array-index output paths",
        "positional projection",
    ],
}
EXPECTED_INDEX_CAPABILITIES = {
    "constraints": [
        "compound indexes reject parallel array fields",
        "dotted index paths cannot traverse arrays",
        (
            "indexed objects, nested arrays, non-finite numbers, and unsupported "
            "BSON values are rejected"
        ),
        "sparse and partialFilterExpression are mutually exclusive",
        "top-level arrays create multikey entries",
    ],
    "create_index": {
        "directions": [1],
        "options": ["name", "partialFilterExpression", "sparse", "unique"],
    },
    "create_indexes_index_model": {
        "directions": [1, -1, "hashed", "text"],
        "options": [
            "background",
            "expireAfterSeconds",
            "name",
            "partialFilterExpression",
            "sparse",
            "unique",
        ],
    },
    "degraded_with_warning": [
        "background",
        "descending",
        "hashed",
        "text",
        "ttl",
    ],
    "degradation_outcomes": {
        "ascending_fallback": ["descending", "hashed"],
        "ignored_semantics": ["background", "ttl"],
        "skipped_without_effective_index": ["text"],
    },
    "operations": [
        "create_index",
        "create_indexes",
        "drop_index",
        "index_information",
        "list_indexes",
    ],
    "partial_filter_operators": [
        "$and",
        "$eq",
        "$exists:true",
        "$gt",
        "$gte",
        "$in",
        "$lt",
        "$lte",
        "$or",
        "$type",
    ],
    "supported": [
        "ascending",
        "compound",
        "dotted paths",
        "multikey",
        "named",
        "partial",
        "sparse",
        "unique",
    ],
    "unsafe_unique_degradations_rejected": ["hashed", "text", "ttl"],
}
EXPECTED_AGGREGATION_CAPABILITIES = {
    "accumulators": [
        "$addToSet",
        "$avg",
        "$first",
        "$last",
        "$max",
        "$min",
        "$push",
        "$sum",
    ],
    "expressions": ["$ifNull", "$literal", "$size"],
    "expression_stages": ["$addFields", "$group", "$project", "$set"],
    "remove_variable_stages": ["$addFields", "$project", "$set"],
    "operation_options": {
        "other_keyword_arguments": "unsupported",
        "session": "null-only",
    },
    "stage_rules": [
        "$set is an alias of $addFields",
        ("$unset accepts a field string or a non-empty list/tuple of field strings"),
        "$group _id accepts only a field path or null",
    ],
    "stages": [
        "$match",
        "$sort",
        "$skip",
        "$limit",
        "$count",
        "$project",
        "$set",
        "$addFields",
        "$unset",
        "$group",
    ],
    "unsupported_special_forms": [
        "$sort $meta",
        "aggregation variables other than $$REMOVE in its supported stages",
    ],
}
EXPECTED_COMMAND_CAPABILITIES = {
    "rules": [
        "command names are case-insensitive",
        "session is null-only",
        (
            "string or non-empty command-document input; the document's first key "
            "selects the command"
        ),
        "value, positional arguments, and other keyword arguments are ignored",
    ],
    "supported": ["buildInfo", "ping"],
    "unsupported": ["arbitrary database commands"],
}
EXPECTED_WARNING_CAPABILITIES = {
    "class": "TinyMongoUnsupportedWarning",
    "contexts": [
        "create_indexes degraded model creation or skip",
        "create_indexes degraded model reuse",
        "cursor sorting of unsupported values",
        "aggregation $sort of unsupported values",
    ],
    "degraded_index_feature_messages": {
        "background": "background creation is ignored",
        "descending": "descending direction is treated as ascending",
        "hashed": "hashed indexing is replaced by ascending equality indexing",
        "text": "text indexing is ignored because $text queries are not supported",
        "ttl": "TTL expiration is not performed",
    },
    "message_templates": {
        "index_creation": "Index {0!r} was created with reduced behavior: {1}.",
        "index_reuse": (
            "Index {0!r} was accepted with reduced behavior: {1}. Its effective "
            "specification matches existing index {2!r}, so TinyMongo reused that "
            "index instead of creating a duplicate."
        ),
        "unsupported_sort_value": (
            "Sorting field '{0}' encountered unsupported value type '{1}'; values "
            "of this type compare as null."
        ),
    },
    "policy": (
        "warnings name the reduced behavior and are attributed to the caller; sort "
        "warnings say the value compares as null and are de-duplicated per field "
        "and Python type within the cursor or aggregation engine"
    ),
}
EXPECTED_UNSUPPORTED_CAPABILITIES = [
    "aggregate keyword options other than session=None",
    "arbitrary server commands",
    (
        "array filters and positional path tokens are not implemented; update "
        "kwargs are ignored and mapping-only operators can persist tokens as "
        "literal keys"
    ),
    "bulk_write",
    "change streams",
    "filtered collection listing",
    "geospatial operators",
    "non-null sessions",
    "non-default read and write concerns",
    "server-side JavaScript",
    "TinyGridFS storage operations",
    "transactions",
    "unsupported aggregation stages/expressions/accumulators",
    "unsupported index specifications",
    "watch",
]
EXPECTED_SEMANTIC_CAPABILITIES = {
    "aggregation": EXPECTED_AGGREGATION_CAPABILITIES,
    "commands": EXPECTED_COMMAND_CAPABILITIES,
    "indexes": EXPECTED_INDEX_CAPABILITIES,
    "projections": EXPECTED_PROJECTION_CAPABILITIES,
    "queries": EXPECTED_QUERY_CAPABILITIES,
    "unsupported": EXPECTED_UNSUPPORTED_CAPABILITIES,
    "updates": EXPECTED_UPDATE_CAPABILITIES,
    "warnings": EXPECTED_WARNING_CAPABILITIES,
}
EXPECTED_CAPABILITIES_SHA256 = (
    "786b11215470f2dbc51a84e9718dc84928209ab0cfdaf828fbcb9a574c96dc0e"
)

TARGET_PATTERN = re.compile(r"^[a-z][a-z0-9-]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ISSUE_PATTERN = re.compile(
    r"^https://github\.com/schapman1974/briskdb/issues/[1-9][0-9]*$"
)
ABSOLUTE_PATH_TOKEN = "<ABSOLUTE_PATH>"
QUOTED_ABSOLUTE_PATH_PATTERN = re.compile(
    r"""(?P<quote>["'])(?:[A-Za-z]:[\\/]|\\\\|/(?!/))[^"']*(?P=quote)"""
)
WINDOWS_ABSOLUTE_PATH_PATTERN = re.compile(
    r"""(?<![A-Za-z0-9_\\/])(?:[A-Za-z]:[\\/]|\\\\)[^\s"'<>()[\]{},;]+"""
)
UNIX_ABSOLUTE_PATH_PATTERN = re.compile(
    r"""(?<![A-Za-z0-9_:/\\])/(?!/)[^\s"'<>()[\]{},;]+"""
)
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "compat" / "mongo" / "v1" / "manifest.json"


class ContractError(Exception):
    """A deterministic contract, result, or comparison failure."""


def _load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError("cannot read valid JSON from {0}: {1}".format(path, error))


def _canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _pretty_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_pretty_bytes(value))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ContractError("cannot hash {0}: {1}".format(path, error))
    return digest.hexdigest()


def _git(source_root: Path, arguments: Sequence[str]) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "-C", str(source_root)] + list(arguments),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", b"").decode("utf-8", "replace").strip()
        raise ContractError(
            "cannot read source Git snapshot: {0}".format(detail or error)
        )
    return completed.stdout


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ContractError("{0} must be a JSON object".format(label))
    return value


def _list(value: Any, label: str) -> List[Any]:
    if not isinstance(value, list):
        raise ContractError("{0} must be a JSON array".format(label))
    return value


def _string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError("{0} must be a nonempty string".format(label))
    return value


def _unique_strings(value: Any, label: str) -> List[str]:
    items = _list(value, label)
    strings = [_string(item, "{0} item".format(label)) for item in items]
    if len(strings) != len(set(strings)):
        raise ContractError("{0} contains duplicates".format(label))
    return strings


def _sha256_string(value: Any, label: str) -> str:
    digest = _string(value, label)
    if not SHA256_PATTERN.fullmatch(digest):
        raise ContractError("{0} must be a lowercase SHA-256".format(label))
    return digest


def _runner_local_path(contract_base: Path, value: Any, label: str) -> Tuple[str, Path]:
    relative = _string(value, label)
    parts = Path(relative).parts
    prefix = ("compat", "mongo", "v1")
    if (
        Path(relative).is_absolute()
        or ".." in parts
        or tuple(parts[: len(prefix)]) != prefix
        or len(parts) <= len(prefix)
        or parts[len(prefix)] != "runner"
    ):
        raise ContractError(
            "{0} must be a repository-relative path under compat/mongo/v1/runner".format(
                label
            )
        )
    return relative, contract_base.joinpath(*parts[len(prefix) :])


def _exact_keys(value: Any, expected: Sequence[str], label: str) -> Mapping[str, Any]:
    mapping = _object(value, label)
    actual = set(mapping)
    expected_set = set(expected)
    if actual != expected_set:
        raise ContractError(
            "{0} keys differ; missing={1}, unexpected={2}".format(
                label,
                sorted(expected_set - actual),
                sorted(actual - expected_set),
            )
        )
    return mapping


def _validate_option_records(
    value: Any, behaviors: Sequence[str], label: str
) -> Tuple[str, ...]:
    records = _list(value, label)
    names = []
    for index, raw_record in enumerate(records):
        record_label = "{0}[{1}]".format(label, index)
        record = _object(raw_record, record_label)
        if not {"name", "behavior"}.issubset(record) or not set(record).issubset(
            {"name", "behavior", "condition"}
        ):
            raise ContractError(
                "{0} must contain name, behavior, and optional condition".format(
                    record_label
                )
            )
        name = _string(record.get("name"), "{0}.name".format(record_label))
        behavior = _string(record.get("behavior"), "{0}.behavior".format(record_label))
        if behavior not in behaviors:
            raise ContractError(
                "{0} references unknown option behavior {1}".format(
                    record_label, behavior
                )
            )
        condition = record.get("condition")
        if condition is not None:
            _string(condition, "{0}.condition".format(record_label))
        if behavior == "applied_conditionally" and condition is None:
            raise ContractError(
                "{0} conditionally applied option needs a condition".format(
                    record_label
                )
            )
        names.append(name)
    if len(names) != len(set(names)):
        raise ContractError("{0} contains duplicate option names".format(label))
    return tuple(names)


def _validate_api_capabilities(value: Any) -> None:
    api = _exact_keys(
        value,
        (
            "aliases",
            "behavior_enum",
            "connection_options",
            "constructors",
            "inventory_version",
            "method_options",
            "owners",
        ),
        "capabilities.api",
    )
    if api.get("inventory_version") != 1:
        raise ContractError("capabilities.api inventory_version must be 1")

    behavior_enum = _object(api.get("behavior_enum"), "API option behavior enum")
    if set(behavior_enum) != set(EXPECTED_API_BEHAVIORS):
        raise ContractError("API option behavior enum does not match TinyMongo v1.3.0")
    for behavior, description in behavior_enum.items():
        _string(description, "API option behavior {0}".format(behavior))

    aliases = _object(api.get("aliases"), "API aliases")
    if aliases != EXPECTED_API_ALIASES:
        raise ContractError("API aliases do not match TinyMongo v1.3.0")

    owners = _exact_keys(api.get("owners"), EXPECTED_API_OWNER_CLASSES, "API owners")
    method_options = _exact_keys(
        api.get("method_options"), EXPECTED_API_OWNER_CLASSES, "API method options"
    )
    for owner_name, expected_classes in EXPECTED_API_OWNER_CLASSES.items():
        owner_label = "API owner {0}".format(owner_name)
        owner = _exact_keys(
            owners[owner_name],
            (
                "awaitability",
                "classes",
                "extensions",
                "present_but_unsupported",
                "properties",
                "protocols",
                "supported",
            ),
            owner_label,
        )
        classes = tuple(_unique_strings(owner.get("classes"), owner_label + " classes"))
        if classes != expected_classes:
            raise ContractError(
                "{0} classes do not match TinyMongo v1.3.0".format(owner_label)
            )

        supported = set(
            _unique_strings(owner.get("supported"), owner_label + " supported methods")
        )
        unsupported = set(
            _unique_strings(
                owner.get("present_but_unsupported"),
                owner_label + " unsupported methods",
            )
        )
        extensions = set(
            _unique_strings(owner.get("extensions"), owner_label + " extensions")
        )
        expected_methods = set(EXPECTED_OPERATION_RESULT_SHAPES[owner_name])
        if unsupported != set(EXPECTED_API_UNSUPPORTED_METHODS[owner_name]):
            raise ContractError(
                "{0} unsupported methods do not match source".format(owner_label)
            )
        if extensions != set(EXPECTED_API_EXTENSION_METHODS[owner_name]):
            raise ContractError(
                "{0} extensions do not match source".format(owner_label)
            )
        expected_supported = expected_methods - unsupported - extensions
        if supported != expected_supported:
            raise ContractError(
                "{0} supported methods do not match source".format(owner_label)
            )
        if (
            (supported & unsupported)
            or (supported & extensions)
            or (unsupported & extensions)
        ):
            raise ContractError("{0} method categories overlap".format(owner_label))

        awaitability = _exact_keys(
            owner.get("awaitability"),
            ("awaitable", "immediate"),
            owner_label + " awaitability",
        )
        awaitable = set(
            _unique_strings(
                awaitability.get("awaitable"), owner_label + " awaitable methods"
            )
        )
        immediate = set(
            _unique_strings(
                awaitability.get("immediate"), owner_label + " immediate methods"
            )
        )
        if awaitable != set(EXPECTED_AWAITABLE_METHODS[owner_name]):
            raise ContractError(
                "{0} awaitable methods do not match source".format(owner_label)
            )
        if immediate != expected_methods - awaitable or awaitable & immediate:
            raise ContractError(
                "{0} awaitability does not cover each method once".format(owner_label)
            )

        properties = _list(owner.get("properties"), owner_label + " properties")
        property_names = []
        for index, raw_property in enumerate(properties):
            property_label = "{0} property {1}".format(owner_label, index)
            property_record = _object(raw_property, property_label)
            if not {"kind", "name"}.issubset(property_record) or not set(
                property_record
            ).issubset({"kind", "meaning", "name"}):
                raise ContractError("{0} has invalid fields".format(property_label))
            _string(property_record.get("kind"), property_label + " kind")
            property_names.append(
                _string(property_record.get("name"), property_label + " name")
            )
            if "meaning" in property_record:
                _string(property_record["meaning"], property_label + " meaning")
        if len(property_names) != len(set(property_names)):
            raise ContractError("{0} contains duplicate properties".format(owner_label))

        protocol_methods = set()
        for index, raw_protocol in enumerate(
            _list(owner.get("protocols"), owner_label + " protocols")
        ):
            protocol_label = "{0} protocol {1}".format(owner_label, index)
            protocol = _exact_keys(
                raw_protocol,
                ("awaitability", "methods", "name"),
                protocol_label,
            )
            _string(protocol.get("name"), protocol_label + " name")
            if protocol.get("awaitability") not in ("awaitable", "immediate", "mixed"):
                raise ContractError(
                    "{0} has invalid awaitability".format(protocol_label)
                )
            methods = set(
                _unique_strings(protocol.get("methods"), protocol_label + " methods")
            )
            if not methods or methods & protocol_methods or methods & expected_methods:
                raise ContractError(
                    "{0} has duplicate or ordinary methods".format(protocol_label)
                )
            protocol_methods.update(methods)

        owner_options = _exact_keys(
            method_options[owner_name],
            expected_methods,
            owner_label + " method options",
        )
        for method in sorted(expected_methods):
            _validate_option_records(
                owner_options[method],
                EXPECTED_API_BEHAVIORS,
                "{0}.{1} options".format(owner_label, method),
            )

    constructors = _exact_keys(
        api.get("constructors"),
        EXPECTED_API_CONSTRUCTOR_SIGNATURES,
        "API constructors",
    )
    for class_name, expected_signature in EXPECTED_API_CONSTRUCTOR_SIGNATURES.items():
        label = "constructor {0}".format(class_name)
        constructor = _object(constructors[class_name], label)
        if not {"options", "signature"}.issubset(constructor) or not set(
            constructor
        ).issubset({"construction_scope", "options", "signature"}):
            raise ContractError("{0} has invalid fields".format(label))
        if constructor.get("signature") != expected_signature:
            raise ContractError("{0} signature does not match source".format(label))
        option_names = _validate_option_records(
            constructor.get("options"), EXPECTED_API_BEHAVIORS, label + " options"
        )
        if option_names != EXPECTED_API_CONSTRUCTOR_OPTIONS[class_name]:
            raise ContractError("{0} options do not match source".format(label))
        if "construction_scope" in constructor:
            _string(constructor["construction_scope"], label + " construction scope")

    connection = _exact_keys(
        api.get("connection_options"),
        (
            "lookup",
            "pymongo_fallback_catalog",
            "pymongo_fallback_count",
            "tinymongo_storage_options",
            "unknown_option_behavior",
        ),
        "API connection options",
    )
    _string(connection.get("lookup"), "connection option lookup")
    _string(
        connection.get("unknown_option_behavior"),
        "unknown connection option behavior",
    )
    pymongo_names = _validate_option_records(
        connection.get("pymongo_fallback_catalog"),
        EXPECTED_API_BEHAVIORS,
        "PyMongo fallback connection options",
    )
    if (
        connection.get("pymongo_fallback_count") != 65
        or len(pymongo_names) != 65
        or pymongo_names != EXPECTED_PYMONGO_CONNECTION_OPTIONS
    ):
        raise ContractError(
            "PyMongo fallback connection options must be the exact 65-name catalog"
        )
    storage_names = _validate_option_records(
        connection.get("tinymongo_storage_options"),
        EXPECTED_API_BEHAVIORS,
        "TinyMongo storage options",
    )
    if set(storage_names) != set(EXPECTED_TINYMONGO_STORAGE_OPTIONS):
        raise ContractError("TinyMongo storage options do not match source")


def _validate_result_shape_references(
    value: Any, known_shapes: Sequence[str], label: str
) -> None:
    known = set(known_shapes)

    def walk(node: Any, node_label: str) -> None:
        if not isinstance(node, dict):
            return
        for key in ("additional_properties", "item"):
            reference = node.get(key)
            if isinstance(reference, str) and reference not in known:
                raise ContractError(
                    "{0}.{1} references unknown result shape {2}".format(
                        node_label, key, reference
                    )
                )
            if isinstance(reference, dict):
                walk(reference, "{0}.{1}".format(node_label, key))
        for key in ("members", "items"):
            references = node.get(key)
            if references is not None:
                for index, reference in enumerate(
                    _list(references, node_label + "." + key)
                ):
                    if not isinstance(reference, str) or reference not in known:
                        raise ContractError(
                            "{0}.{1}[{2}] references unknown result shape".format(
                                node_label, key, index
                            )
                        )
        for key in ("optional_fields", "required_fields"):
            fields = node.get(key)
            if fields is None:
                continue
            for field, field_shape in _object(fields, node_label + "." + key).items():
                _string(field, node_label + " field name")
                if isinstance(field_shape, str):
                    if field_shape not in known:
                        raise ContractError(
                            "{0}.{1}.{2} references unknown result shape {3}".format(
                                node_label, key, field, field_shape
                            )
                        )
                elif isinstance(field_shape, dict):
                    walk(field_shape, "{0}.{1}.{2}".format(node_label, key, field))
                else:
                    raise ContractError(
                        "{0}.{1}.{2} must be a shape reference or inline shape".format(
                            node_label, key, field
                        )
                    )

    walk(value, label)


def _validate_result_capabilities(value: Any) -> None:
    results = _exact_keys(
        value,
        (
            "cursor_results",
            "document_results",
            "inventory_version",
            "operation_result_shapes",
            "result_shapes",
            "write_result_classes",
        ),
        "capabilities.results",
    )
    if results.get("inventory_version") != 1:
        raise ContractError("capabilities.results inventory_version must be 1")

    shapes = _exact_keys(
        results.get("result_shapes"), EXPECTED_RESULT_SHAPE_IDS, "result shapes"
    )
    for shape_name, raw_shape in shapes.items():
        shape = _object(raw_shape, "result shape {0}".format(shape_name))
        _string(shape.get("kind"), "result shape {0} kind".format(shape_name))
        _validate_result_shape_references(
            shape, EXPECTED_RESULT_SHAPE_IDS, "result shape {0}".format(shape_name)
        )
    if _sha256_bytes(_canonical_bytes(shapes)) != EXPECTED_RESULT_SHAPES_SHA256:
        raise ContractError("result shape definitions do not match TinyMongo v1.3.0")

    operation_shapes = _exact_keys(
        results.get("operation_result_shapes"),
        EXPECTED_OPERATION_RESULT_SHAPES,
        "operation result shapes",
    )
    for owner, expected in EXPECTED_OPERATION_RESULT_SHAPES.items():
        observed = _object(operation_shapes[owner], "operation results for " + owner)
        if observed != expected:
            raise ContractError(
                "operation result shapes for {0} do not match source".format(owner)
            )
        for method, shape_name in observed.items():
            if shape_name not in shapes:
                raise ContractError(
                    "operation {0}.{1} references unknown shape {2}".format(
                        owner, method, shape_name
                    )
                )

    cursor_results = _exact_keys(
        results.get("cursor_results"),
        (
            "async_class",
            "async_cursor_returning_operations",
            "behavior",
            "sync_class",
            "sync_cursor_returning_operations",
        ),
        "cursor results",
    )
    if (
        cursor_results.get("sync_class") != "tinymongo.TinyMongoCursor"
        or cursor_results.get("async_class") != "tinymongo.AsyncTinyMongoCursor"
    ):
        raise ContractError("cursor result classes do not match source")
    if _unique_strings(
        cursor_results.get("sync_cursor_returning_operations"),
        "sync cursor-returning operations",
    ) != ["aggregate", "find", "list_databases"]:
        raise ContractError("sync cursor-returning operations do not match source")
    if _unique_strings(
        cursor_results.get("async_cursor_returning_operations"),
        "async cursor-returning operations",
    ) != ["aggregate", "find", "list_databases", "list_indexes"]:
        raise ContractError("async cursor-returning operations do not match source")
    if not _unique_strings(cursor_results.get("behavior"), "cursor result behavior"):
        raise ContractError("cursor result behavior must be inventoried")

    document_results = _exact_keys(
        results.get("document_results"),
        ("datetime_conversion", "field_order", "mapping_class", "value_isolation"),
        "document results",
    )
    for field, description in document_results.items():
        _string(description, "document result {0}".format(field))

    write_results = _exact_keys(
        results.get("write_result_classes"),
        EXPECTED_WRITE_RESULT_FIELDS,
        "write result classes",
    )
    for class_name, expected_fields in EXPECTED_WRITE_RESULT_FIELDS.items():
        class_shape = _exact_keys(
            write_results[class_name],
            ("constructor", "fields"),
            "write result " + class_name,
        )
        if (
            class_shape.get("constructor")
            != EXPECTED_WRITE_RESULT_CONSTRUCTORS[class_name]
        ):
            raise ContractError(
                "{0} constructor does not match source".format(class_name)
            )
        fields = _list(class_shape.get("fields"), class_name + " fields")
        field_names = []
        for index, raw_field in enumerate(fields):
            field = _object(raw_field, "{0} field {1}".format(class_name, index))
            if not {"kind", "meaning", "name"}.issubset(field) or set(field) != {
                "kind",
                "meaning",
                "name",
            }:
                raise ContractError(
                    "{0} has an invalid field record".format(class_name)
                )
            _string(field.get("kind"), class_name + " field kind")
            _string(field.get("meaning"), class_name + " field meaning")
            field_names.append(_string(field.get("name"), class_name + " field name"))
        if tuple(field_names) != expected_fields:
            raise ContractError("{0} fields do not match source".format(class_name))


def _validate_error_shapes(errors: Mapping[str, Any]) -> None:
    classes = set(_unique_strings(errors.get("classes"), "error classes"))
    if classes != set(EXPECTED_ERROR_CLASSES):
        raise ContractError("error classes do not match TinyMongo v1.3.0")
    _string(errors.get("field_scope"), "error field scope")
    class_shapes = _exact_keys(
        errors.get("class_shapes"), EXPECTED_ERROR_CLASSES, "error class shapes"
    )
    for class_name, expected_fields in EXPECTED_ERROR_FIELDS.items():
        shape = _object(class_shapes[class_name], "error shape " + class_name)
        if not {"fields"}.issubset(shape) or not set(shape).issubset(
            {"constructor", "detail_shape", "fields"}
        ):
            raise ContractError("error shape {0} has invalid fields".format(class_name))
        if "constructor" in shape:
            _string(shape["constructor"], class_name + " constructor")
        fields = _list(shape.get("fields"), class_name + " public fields")
        names = []
        for index, raw_field in enumerate(fields):
            field = _object(raw_field, "{0} field {1}".format(class_name, index))
            names.append(_string(field.get("name"), class_name + " field name"))
            _string(field.get("kind"), class_name + " field kind")
        if set(names) != set(expected_fields) or len(names) != len(expected_fields):
            raise ContractError(
                "error shape {0} fields do not match source".format(class_name)
            )
    if class_shapes["InvalidDocument"].get("constructor") != (
        "InvalidDocument(message, document=None)"
    ):
        raise ContractError("InvalidDocument constructor does not match source")
    invalid_document_fields = {
        field["name"]: field for field in class_shapes["InvalidDocument"]["fields"]
    }
    if invalid_document_fields["document"].get("identity") != (
        "original root document rejected by the BSON codec"
    ):
        raise ContractError("InvalidDocument.document identity is not exact")

    bulk_error = class_shapes["BulkWriteError"]
    bulk_error_fields = {field["name"]: field for field in bulk_error["fields"]}
    if (
        bulk_error.get("constructor") != "BulkWriteError(results)"
        or bulk_error.get("detail_shape") != "bulk_write_details"
        or bulk_error_fields["code"].get("constant") != 65
        or bulk_error_fields["details"].get("shape") != "bulk_write_details"
    ):
        raise ContractError("BulkWriteError public shape does not match source")

    detail_shapes = _exact_keys(
        errors.get("detail_shapes"), ("bulk_write_details",), "error detail shapes"
    )
    bulk = _object(detail_shapes["bulk_write_details"], "bulk write details")
    if bulk.get("kind") != "document":
        raise ContractError("bulk write details must be a document")
    fields = _exact_keys(
        bulk.get("required_fields"),
        (
            "nInserted",
            "nMatched",
            "nModified",
            "nRemoved",
            "nUpserted",
            "upserted",
            "writeConcernErrors",
            "writeErrors",
        ),
        "bulk write detail fields",
    )
    if fields["nInserted"] != "integer":
        raise ContractError("bulk write nInserted must be an integer")
    for field_name in ("nMatched", "nModified", "nRemoved", "nUpserted"):
        if fields[field_name] != {"constant": 0}:
            raise ContractError(
                "bulk write {0} must be the constant zero".format(field_name)
            )
    for field_name in ("upserted", "writeConcernErrors"):
        if fields[field_name] != {"constant": []}:
            raise ContractError(
                "bulk write {0} must be the constant empty array".format(field_name)
            )
    write_errors = _object(fields["writeErrors"], "bulk write errors")
    if write_errors.get("kind") != "array":
        raise ContractError("bulk writeErrors must be an array")
    item = _object(write_errors.get("item"), "bulk write error item")
    if item.get("kind") != "document":
        raise ContractError("bulk write error item must be a document")
    item_fields = _exact_keys(
        item.get("required_fields"),
        ("code", "errmsg", "index", "keyPattern", "keyValue", "op"),
        "bulk write error item fields",
    )
    if (
        item_fields["code"] != {"constant": 11000}
        or item_fields["errmsg"] != "string"
        or item_fields["index"] != "integer"
        or item_fields["keyPattern"] != "document"
        or item_fields["keyValue"] != "document"
        or item_fields["op"] != "original_document"
    ):
        raise ContractError("bulk write error item shape does not match source")


def _validate_capabilities(value: Any) -> Mapping[str, Any]:
    capabilities = _exact_keys(
        value, EXPECTED_CAPABILITY_CATEGORIES, "manifest.capabilities"
    )
    for category, category_value in capabilities.items():
        if not isinstance(category_value, (dict, list)) or not category_value:
            raise ContractError("capabilities.{0} must be nonempty".format(category))
    for category, expected in EXPECTED_SEMANTIC_CAPABILITIES.items():
        if capabilities[category] != expected:
            raise ContractError(
                "capabilities.{0} do not match TinyMongo v1.3.0".format(category)
            )
    _validate_api_capabilities(capabilities["api"])
    _validate_result_capabilities(capabilities["results"])
    errors = _object(capabilities["errors"], "capabilities.errors")
    _validate_error_shapes(errors)

    values = _exact_keys(
        capabilities["values"],
        ("bson_family_aliases", "native", "optional_pymongo_bson", "rules"),
        "capabilities.values",
    )
    if tuple(_unique_strings(values.get("native"), "native BSON types")) != (
        EXPECTED_NATIVE_BSON_TYPES
    ):
        raise ContractError("native BSON types do not match supported_bson_types()")
    aliases = _object(values.get("bson_family_aliases"), "BSON family aliases")
    if aliases != EXPECTED_BSON_FAMILY_ALIASES:
        raise ContractError("BSON family aliases must map int/long to int32/int64")
    if (
        tuple(
            _unique_strings(
                values.get("optional_pymongo_bson"), "optional PyMongo BSON types"
            )
        )
        != EXPECTED_OPTIONAL_PYMONGO_BSON_TYPES
    ):
        raise ContractError(
            "optional PyMongo BSON type inventory does not match source"
        )
    if not _unique_strings(values.get("rules"), "BSON value rules"):
        raise ContractError("BSON value rules must be inventoried")
    if _sha256_bytes(_canonical_bytes(capabilities)) != EXPECTED_CAPABILITIES_SHA256:
        raise ContractError(
            "capability definitions do not match the frozen TinyMongo v1.3.0 inventory"
        )
    return capabilities


def _validate_runner_provenance(
    provenance_path: Path,
    contract_base: Path,
    manifest: Mapping[str, Any],
    source_hashes: Mapping[str, str],
) -> None:
    provenance = _object(_load_json(provenance_path), "runner provenance")
    if provenance.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("runner provenance schema_version must be 1")
    if provenance.get("corpus") != manifest.get("contract"):
        raise ContractError("runner provenance contract does not match manifest")

    provenance_source = _object(provenance.get("source"), "runner provenance source")
    manifest_source = _object(manifest.get("source"), "manifest.source")
    for key in ("repository", "commit", "contract_git_tree", "runtime_git_tree"):
        if provenance_source.get(key) != manifest_source.get(key):
            raise ContractError(
                "runner provenance source {0} does not match manifest".format(key)
            )

    listed_paths = set()
    recorded_upstream = {}

    def record_local(path_value: Any, digest_value: Any, label: str) -> str:
        relative, local_path = _runner_local_path(contract_base, path_value, label)
        if relative in listed_paths:
            raise ContractError("duplicate runner provenance path {0}".format(relative))
        expected = _sha256_string(digest_value, "{0} sha256".format(label))
        actual = _sha256_file(local_path)
        if actual != expected:
            raise ContractError(
                "{0} hash mismatch: expected {1}, found {2}".format(
                    local_path, expected, actual
                )
            )
        listed_paths.add(relative)
        return relative

    def record_upstream(entry: Mapping[str, Any], label: str) -> Tuple[str, str]:
        relative = _string(entry.get("upstream_path"), "{0} path".format(label))
        parts = Path(relative).parts
        if (
            Path(relative).is_absolute()
            or ".." in parts
            or tuple(parts[:2]) != ("tests", "contracts")
        ):
            raise ContractError(
                "{0} must identify a frozen tests/contracts source".format(label)
            )
        if relative in recorded_upstream:
            raise ContractError(
                "duplicate upstream provenance path {0}".format(relative)
            )
        blob = _string(entry.get("upstream_git_blob"), "{0} Git blob".format(label))
        if not re.fullmatch(r"[0-9a-f]{40}", blob):
            raise ContractError("{0} Git blob must be a full object ID".format(label))
        digest = _sha256_string(
            entry.get("upstream_sha256"), "{0} upstream sha256".format(label)
        )
        recorded_upstream[relative] = digest
        return relative, digest

    adaptation = _object(provenance.get("adaptation"), "runner adaptation")
    adapted_paths = []
    for index, value in enumerate(_list(adaptation.get("files"), "adaptation files")):
        label = "adaptation file {0}".format(index)
        entry = _object(value, label)
        upstream_path, upstream_digest = record_upstream(entry, label)
        runner_path = record_local(
            entry.get("runner_path"), entry.get("runner_sha256"), label
        )
        expected_runner = "compat/mongo/v1/runner/contracts/" + Path(upstream_path).name
        if runner_path != expected_runner:
            raise ContractError(
                "{0} does not map to the matching runner contract path".format(label)
            )
        status = _string(entry.get("status"), "{0} status".format(label))
        runner_digest = _sha256_string(
            entry.get("runner_sha256"), "{0} runner sha256".format(label)
        )
        if status == "exact":
            if runner_digest != upstream_digest:
                raise ContractError(
                    "exact {0} has different source and runner hashes".format(label)
                )
        elif status == "adapted":
            if runner_digest == upstream_digest:
                raise ContractError(
                    "adapted {0} has identical source and runner hashes".format(label)
                )
            adapted_paths.append((upstream_path, runner_path))
        else:
            raise ContractError("{0} status must be exact or adapted".format(label))

    for index, value in enumerate(
        _list(
            adaptation.get("excluded_upstream_files"),
            "excluded upstream files",
        )
    ):
        label = "excluded upstream file {0}".format(index)
        entry = _object(value, label)
        record_upstream(entry, label)
        _string(entry.get("reason"), "{0} reason".format(label))

    if recorded_upstream != dict(source_hashes):
        raise ContractError(
            "runner provenance does not account for every frozen contract source"
        )

    patch_path = record_local(
        adaptation.get("patch"), adaptation.get("patch_sha256"), "adaptation patch"
    )
    try:
        patch_text = (
            contract_base / Path(patch_path).relative_to("compat/mongo/v1")
        ).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ContractError("cannot read adaptation patch: {0}".format(error))
    for upstream_path, runner_path in adapted_paths:
        source_header = "--- a/{0}".format(upstream_path)
        runner_header = "+++ b/{0}".format(runner_path)
        if source_header not in patch_text or runner_header not in patch_text:
            raise ContractError(
                "adaptation patch does not contain {0}".format(upstream_path)
            )

    license_entry = _object(provenance_source.get("license"), "runner source license")
    if (
        license_entry.get("spdx") != "MIT"
        or license_entry.get("upstream_path") != "LICENSE.txt"
    ):
        raise ContractError("runner provenance must preserve the upstream MIT license")
    license_blob = _string(
        license_entry.get("upstream_git_blob"), "runner source license Git blob"
    )
    if not re.fullmatch(r"[0-9a-f]{40}", license_blob):
        raise ContractError("runner source license Git blob must be a full object ID")
    upstream_license_hash = _sha256_string(
        license_entry.get("upstream_sha256"), "upstream license sha256"
    )
    runner_license_hash = _sha256_string(
        license_entry.get("runner_sha256"), "runner license sha256"
    )
    if runner_license_hash != upstream_license_hash:
        raise ContractError("runner license must be byte-for-byte upstream")
    record_local(
        license_entry.get("runner_path"), runner_license_hash, "runner source license"
    )

    for index, value in enumerate(
        _list(provenance.get("runner_files"), "runner files")
    ):
        entry = _object(value, "runner file {0}".format(index))
        record_local(
            entry.get("path"), entry.get("sha256"), "runner file {0}".format(index)
        )

    expected_local = {
        Path(relative).relative_to("compat/mongo/v1").as_posix()
        for relative in listed_paths
    }
    actual_local = set()
    cache_directories = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
    for local_path in provenance_path.parent.rglob("*"):
        if not local_path.is_file() or local_path == provenance_path:
            continue
        runner_relative = local_path.relative_to(contract_base)
        if cache_directories.intersection(runner_relative.parts):
            continue
        if local_path.suffix in (".pyc", ".pyo"):
            continue
        actual_local.add(runner_relative.as_posix())
    if actual_local != expected_local:
        missing = sorted(expected_local - actual_local)
        unlisted = sorted(actual_local - expected_local)
        raise ContractError(
            "runner file inventory mismatch; missing={0}, unlisted={1}".format(
                missing, unlisted
            )
        )


def _manifest_paths(
    manifest_path: Path,
) -> Tuple[Mapping[str, Any], Path, Path, Path, Path]:
    manifest = _object(_load_json(manifest_path), "manifest")
    base = manifest_path.parent
    files = _object(manifest.get("files"), "manifest.files")
    corpus = base / _string(files.get("corpus"), "manifest.files.corpus")
    reference = base / _string(
        files.get("reference_results"), "manifest.files.reference_results"
    )
    differences = base / _string(
        files.get("intentional_differences"),
        "manifest.files.intentional_differences",
    )
    variants = base / _string(
        files.get("semantic_variants"), "manifest.files.semantic_variants"
    )
    return manifest, corpus, reference, differences, variants


def validate_contract(manifest_path: Path) -> Mapping[str, Any]:
    manifest, corpus_path, reference_path, differences_path, variants_path = (
        _manifest_paths(manifest_path)
    )
    files = _object(manifest.get("files"), "manifest.files")
    provenance_path = manifest_path.parent / _string(
        files.get("runner_provenance"), "manifest.files.runner_provenance"
    )
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("manifest schema_version must be 1")
    contract = _string(manifest.get("contract"), "manifest.contract")
    if contract != "tinymongo-v1":
        raise ContractError("manifest.contract must be tinymongo-v1")

    source = _object(manifest.get("source"), "manifest.source")
    _string(source.get("repository"), "manifest.source.repository")
    commit = _string(source.get("commit"), "manifest.source.commit")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ContractError("manifest.source.commit must be a full Git commit")
    tree_digest = _string(source.get("contract_tree_sha256"), "source digest")
    if not SHA256_PATTERN.fullmatch(tree_digest):
        raise ContractError("source contract_tree_sha256 must be lowercase SHA-256")
    git_tree = _string(source.get("contract_git_tree"), "source Git tree")
    if not re.fullmatch(r"[0-9a-f]{40}", git_tree):
        raise ContractError(
            "manifest.source.contract_git_tree must be a Git tree object"
        )
    runtime_git_tree = _string(
        source.get("runtime_git_tree"), "source runtime Git tree"
    )
    if not re.fullmatch(r"[0-9a-f]{40}", runtime_git_tree):
        raise ContractError(
            "manifest.source.runtime_git_tree must be a Git tree object"
        )

    hashes = _object(manifest.get("sha256"), "manifest.sha256")
    for label, path in (
        ("corpus", corpus_path),
        ("reference_results", reference_path),
        ("intentional_differences", differences_path),
        ("semantic_variants", variants_path),
        ("runner_provenance", provenance_path),
    ):
        expected = _string(hashes.get(label), "manifest.sha256.{0}".format(label))
        if not SHA256_PATTERN.fullmatch(expected):
            raise ContractError("manifest.sha256.{0} is invalid".format(label))
        actual = _sha256_file(path)
        if actual != expected:
            raise ContractError(
                "{0} hash mismatch: expected {1}, found {2}".format(
                    path, expected, actual
                )
            )

    dimensions = _object(manifest.get("dimensions"), "manifest.dimensions")
    apis = _unique_strings(dimensions.get("apis"), "manifest.dimensions.apis")
    if apis != list(EXPECTED_APIS):
        raise ContractError("manifest APIs must be ordered as sync, async")
    backends = _unique_strings(
        dimensions.get("tinymongo_backends"), "manifest.dimensions.tinymongo_backends"
    )
    if backends != list(EXPECTED_BACKENDS):
        raise ContractError(
            "manifest TinyMongo backends do not match the frozen matrix"
        )

    toolchain = _object(manifest.get("harvest_toolchain"), "manifest.harvest_toolchain")
    for component in ("python", "pytest", "pymongo"):
        _string(
            toolchain.get(component), "manifest.harvest_toolchain.{0}".format(component)
        )

    suites = _unique_strings(
        manifest.get("contract_suites"), "manifest.contract_suites"
    )
    if not suites:
        raise ContractError("at least one contract suite is required")
    capabilities = _validate_capabilities(manifest.get("capabilities"))
    errors = _object(capabilities["errors"], "capabilities.errors")
    code_domains = _object(
        errors.get("stable_codes_by_domain"),
        "capabilities.errors.stable_codes_by_domain",
    )
    emitted_codes = set()
    for domain, mapping_value in code_domains.items():
        mapping = _object(mapping_value, "error-code domain {0}".format(domain))
        for code, description in mapping.items():
            if (
                not str(code).isdigit()
                or not isinstance(description, str)
                or not description
            ):
                raise ContractError("invalid stable error-code inventory entry")
            if int(code) in emitted_codes:
                raise ContractError("duplicate stable error code {0}".format(code))
            emitted_codes.add(int(code))
    if (
        errors.get("runtime_emitted_code_count") != len(emitted_codes)
        or len(emitted_codes) != 60
    ):
        raise ContractError(
            "stable error-code inventory must contain all 60 runtime codes"
        )
    asserted_values = _list(
        errors.get("contract_asserted_codes"), "contract asserted error codes"
    )
    if any(
        isinstance(code, bool) or not isinstance(code, int) for code in asserted_values
    ):
        raise ContractError("contract-asserted error codes must be integers")
    asserted_codes = set(asserted_values)
    if not asserted_codes or not asserted_codes.issubset(emitted_codes):
        raise ContractError(
            "contract-asserted error codes must be emitted runtime codes"
        )

    corpus = _object(_load_json(corpus_path), "corpus")
    if (
        corpus.get("schema_version") != SCHEMA_VERSION
        or corpus.get("contract") != contract
    ):
        raise ContractError("corpus version or contract does not match manifest")
    if corpus.get("source_commit") != commit:
        raise ContractError("corpus source commit does not match manifest")
    if corpus.get("source_git_tree") != git_tree:
        raise ContractError("corpus source Git tree does not match manifest")
    if corpus.get("harvest_toolchain") != toolchain:
        raise ContractError("corpus harvest toolchain does not match manifest")
    source_files = _list(corpus.get("source_files"), "corpus.source_files")
    if len(source_files) != 21:
        raise ContractError("corpus must inventory all 21 frozen contract files")
    tree_hasher = hashlib.sha256()
    seen_sources = set()
    source_hashes = {}
    for entry_value in source_files:
        entry = _object(entry_value, "corpus source file")
        relative = _string(entry.get("path"), "source file path")
        digest = _string(entry.get("sha256"), "source file sha256")
        if (
            relative in seen_sources
            or relative.startswith("/")
            or ".." in Path(relative).parts
        ):
            raise ContractError(
                "invalid or duplicate source file path {0}".format(relative)
            )
        if not SHA256_PATTERN.fullmatch(digest):
            raise ContractError("invalid source hash for {0}".format(relative))
        seen_sources.add(relative)
        source_hashes[relative] = digest
        tree_hasher.update(relative.encode("utf-8"))
        tree_hasher.update(b"\0")
        tree_hasher.update(bytes.fromhex(digest))
        tree_hasher.update(b"\0")
    if tree_hasher.hexdigest() != tree_digest:
        raise ContractError(
            "corpus source file inventory does not match source tree digest"
        )
    _validate_runner_provenance(
        provenance_path,
        manifest_path.parent,
        manifest,
        source_hashes,
    )

    cases = _list(corpus.get("cases"), "corpus.cases")
    expected_count = manifest.get("case_count")
    if not isinstance(expected_count, int) or expected_count != len(cases):
        raise ContractError("manifest case_count does not match corpus")
    seen_cases = set()
    suite_counts = Counter()
    expected_executions = 0
    for value in cases:
        case = _object(value, "corpus case")
        case_id = _string(case.get("id"), "case id")
        if case_id in seen_cases:
            raise ContractError("duplicate case id {0}".format(case_id))
        seen_cases.add(case_id)
        suite = _string(case.get("suite"), "case suite")
        if suite not in suites:
            raise ContractError("case {0} has unknown suite {1}".format(case_id, suite))
        relative = _string(case.get("source"), "case source")
        if relative not in seen_sources:
            raise ContractError(
                "case {0} has untracked source {1}".format(case_id, relative)
            )
        case_apis = _unique_strings(case.get("apis"), "case APIs")
        if case_apis != apis:
            raise ContractError(
                "case {0} must cover sync and async APIs".format(case_id)
            )
        requirements = case.get("requirements", {})
        if not isinstance(requirements, dict):
            raise ContractError(
                "case {0} requirements must be an object".format(case_id)
            )
        suite_counts[suite] += 1
        expected_executions += len(case_apis)
    if dict(sorted(suite_counts.items())) != manifest.get("suite_case_counts"):
        raise ContractError("manifest suite_case_counts does not match corpus")
    if manifest.get("reference_execution_count") != expected_executions:
        raise ContractError("manifest reference_execution_count does not match corpus")
    full_matrix_count = expected_executions * len(backends)
    if manifest.get("full_matrix_execution_count") != full_matrix_count:
        raise ContractError(
            "manifest full_matrix_execution_count does not match dimensions"
        )
    if len(cases) != EXPECTED_CASE_COUNT:
        raise ContractError("frozen corpus must contain exactly 228 logical cases")
    test_modules = {case["source"] for case in cases}
    if len(test_modules) != EXPECTED_MODULE_COUNT:
        raise ContractError("frozen corpus must contain exactly 16 test modules")

    differences = _object(_load_json(differences_path), "intentional differences")
    if (
        differences.get("schema_version") != SCHEMA_VERSION
        or differences.get("contract") != contract
    ):
        raise ContractError("intentional differences version or contract is invalid")
    difference_keys = set()
    for value in _list(differences.get("differences"), "differences"):
        difference = _object(value, "intentional difference")
        target = _string(difference.get("target"), "difference target")
        case_id = _string(difference.get("case_id"), "difference case_id")
        api = _string(difference.get("api"), "difference api")
        issue = _string(difference.get("issue"), "difference issue")
        _string(difference.get("reason"), "difference reason")
        reference_outcome = _string(
            difference.get("reference_outcome"), "difference reference outcome"
        )
        candidate_outcome = _string(
            difference.get("candidate_outcome"), "difference candidate outcome"
        )
        reference_fingerprint = _string(
            difference.get("reference_fingerprint"),
            "difference reference fingerprint",
        )
        candidate_fingerprint = _string(
            difference.get("candidate_fingerprint"),
            "difference candidate fingerprint",
        )
        if not TARGET_PATTERN.fullmatch(target):
            raise ContractError("invalid difference target {0}".format(target))
        if case_id not in seen_cases or api not in apis:
            raise ContractError("difference references an unknown case or API")
        if not ISSUE_PATTERN.fullmatch(issue):
            raise ContractError("difference must link one BriskDB issue")
        if reference_outcome not in OUTCOMES or candidate_outcome not in OUTCOMES:
            raise ContractError("difference outcomes are invalid")
        if not SHA256_PATTERN.fullmatch(
            reference_fingerprint
        ) or not SHA256_PATTERN.fullmatch(candidate_fingerprint):
            raise ContractError("difference fingerprints must be lowercase SHA-256")
        key = (target, case_id, api)
        if key in difference_keys:
            raise ContractError("duplicate intentional difference {0}".format(key))
        difference_keys.add(key)

    variants = _object(_load_json(variants_path), "semantic variants")
    if (
        variants.get("schema_version") != SCHEMA_VERSION
        or variants.get("contract") != contract
    ):
        raise ContractError("semantic variants version or contract is invalid")
    variant_keys = set()
    for value in _list(variants.get("variants"), "semantic variants"):
        variant = _object(value, "semantic variant")
        case_id = _string(variant.get("case_id"), "variant case_id")
        backend = _string(variant.get("backend"), "variant backend")
        dimension = _string(variant.get("dimension"), "variant dimension")
        issue = _string(variant.get("issue"), "variant issue")
        _string(variant.get("reference_behavior"), "variant reference behavior")
        _string(variant.get("backend_behavior"), "variant backend behavior")
        variant_apis = _unique_strings(variant.get("apis"), "variant APIs")
        if case_id not in seen_cases or backend not in backends:
            raise ContractError(
                "semantic variant references an unknown case or backend"
            )
        if any(api not in apis for api in variant_apis):
            raise ContractError("semantic variant references an unknown API")
        if not ISSUE_PATTERN.fullmatch(issue):
            raise ContractError("semantic variant must link one BriskDB issue")
        key = (case_id, backend, dimension)
        if key in variant_keys:
            raise ContractError("duplicate semantic variant {0}".format(key))
        variant_keys.add(key)

    reference = validate_results(
        _load_json(reference_path), corpus, "reference results"
    )
    target_ids = {execution["target"] for execution in reference["executions"]}
    if target_ids != {manifest.get("reference_target")}:
        raise ContractError("reference results contain an unexpected target")
    expected_keys = {(case["id"], api) for case in cases for api in case["apis"]}
    actual_keys = {
        (execution["case_id"], execution["api"])
        for execution in reference["executions"]
    }
    if actual_keys != expected_keys:
        raise ContractError("reference results do not cover the entire corpus")
    return manifest


def validate_results(
    value: Any, corpus: Mapping[str, Any], label: str = "results"
) -> Mapping[str, Any]:
    results = _object(value, label)
    if results.get("schema_version") != SCHEMA_VERSION:
        raise ContractError("{0} schema_version must be 1".format(label))
    if results.get("contract") != corpus.get("contract"):
        raise ContractError("{0} contract does not match corpus".format(label))
    known_cases = {
        case["id"]: {
            "apis": set(case["apis"]),
            "suite": case["suite"],
        }
        for case in _list(corpus.get("cases"), "corpus.cases")
    }
    seen = set()
    for value in _list(results.get("executions"), "{0}.executions".format(label)):
        execution = _object(value, "result execution")
        target = _string(execution.get("target"), "execution target")
        if not TARGET_PATTERN.fullmatch(target):
            raise ContractError("invalid execution target {0}".format(target))
        case_id = _string(execution.get("case_id"), "execution case_id")
        api = _string(execution.get("api"), "execution api")
        outcome = _string(execution.get("outcome"), "execution outcome")
        backend = _string(execution.get("backend"), "execution backend")
        suite = _string(execution.get("suite"), "execution suite")
        if case_id not in known_cases or api not in known_cases[case_id]["apis"]:
            raise ContractError(
                "result references unknown case/API {0}/{1}".format(case_id, api)
            )
        if suite != known_cases[case_id]["suite"]:
            raise ContractError(
                "execution suite does not match corpus case {0}".format(case_id)
            )
        if outcome not in OUTCOMES:
            raise ContractError("invalid outcome {0}".format(outcome))
        key = (target, case_id, api)
        if key in seen:
            raise ContractError("duplicate execution {0}".format(key))
        seen.add(key)
        if not TARGET_PATTERN.fullmatch(backend):
            raise ContractError("invalid backend {0}".format(backend))
        if not target.endswith("-" + backend):
            raise ContractError(
                "execution backend {0} does not match target {1}".format(
                    backend, target
                )
            )
        reason = execution.get("reason")
        if reason is not None and not isinstance(reason, str):
            raise ContractError("execution reason must be a string or null")
        observation = _object(execution.get("observation"), "execution observation")
        category = _string(observation.get("category"), "observation category")
        if outcome == "passed" and (category != "passed" or reason is not None):
            raise ContractError(
                "passed execution requires category passed and a null reason"
            )
        fingerprint = _string(observation.get("fingerprint"), "observation fingerprint")
        if not SHA256_PATTERN.fullmatch(fingerprint):
            raise ContractError("observation fingerprint must be lowercase SHA-256")
        expected_fingerprint = _observation(outcome, category, reason)["fingerprint"]
        if fingerprint != expected_fingerprint:
            raise ContractError(
                "observation fingerprint does not match normalized result"
            )
    return results


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _properties(testcase: Any) -> Dict[str, List[str]]:
    values: Dict[str, List[str]] = defaultdict(list)
    for element in testcase.iter():
        if _local_name(element.tag) != "property":
            continue
        name = element.get("name")
        if name:
            values[name].append((element.get("value") or element.text or "").strip())
    return values


def _one_property(properties: Mapping[str, List[str]], name: str) -> Optional[str]:
    values = properties.get(name, [])
    return values[0] if len(values) == 1 and values[0] else None


def _normalize_reason(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    message = " ".join(value.split())
    message = QUOTED_ABSOLUTE_PATH_PATTERN.sub(
        lambda match: "{0}{1}{0}".format(match.group("quote"), ABSOLUTE_PATH_TOKEN),
        message,
    )
    message = WINDOWS_ABSOLUTE_PATH_PATTERN.sub(ABSOLUTE_PATH_TOKEN, message)
    message = UNIX_ABSOLUTE_PATH_PATTERN.sub(ABSOLUTE_PATH_TOKEN, message)
    return message


def _display_reason(value: Optional[str]) -> Optional[str]:
    return value[:500] if value is not None else None


def _observation(
    outcome: str, category: str, reason: Optional[str]
) -> Mapping[str, str]:
    payload = "{0}\0{1}\0{2}".format(outcome, category, reason or "").encode("utf-8")
    return {"category": category, "fingerprint": _sha256_bytes(payload)}


def _result(testcase: Any) -> Tuple[str, Optional[str], Mapping[str, str]]:
    child = next(
        (
            item
            for item in testcase
            if _local_name(item.tag) in ("failure", "error", "skipped")
        ),
        None,
    )
    if child is None:
        outcome, reason, category = "passed", None, "passed"
        return outcome, reason, _observation(outcome, category, reason)
    tag = _local_name(child.tag)
    raw_type = child.get("type") or tag
    result_type = raw_type.lower()
    reason = _normalize_reason(child.get("message") or child.text)
    if tag == "error":
        outcome = "error"
    elif tag == "skipped":
        outcome = "xfailed" if "xfail" in result_type else "skipped"
    elif tag == "failure" and (
        result_type in ("xpass", "pytest.xpass")
        or (reason or "").startswith("[XPASS(strict)]")
    ):
        outcome = "xpassed"
    else:
        outcome = "failed"
    return outcome, reason, _observation(outcome, raw_type, reason)


def _case_name(name: str, api: str, backend: str) -> str:
    match = re.search(r"\[([^][]*)\]$", name)
    if match is None:
        raise ContractError(
            "test name {0} lacks API/backend parameters {1}/{2}".format(
                name, api, backend
            )
        )
    base = name[: match.start()]
    parameters = match.group(1)
    target_prefix = "{0}-{1}".format(api, backend)
    if parameters != target_prefix and not parameters.startswith(target_prefix + "-"):
        raise ContractError(
            "test name {0} does not begin with API/backend {1}/{2}".format(
                name, api, backend
            )
        )
    remaining = parameters[len(target_prefix) :].lstrip("-")
    return "{0}[{1}]".format(base, remaining) if remaining else base


def ingest_junit(
    junit_path: Path,
    implementation: str,
    corpus: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    if not TARGET_PATTERN.fullmatch(implementation):
        raise ContractError(
            "implementation must use lowercase letters, digits, and hyphens"
        )
    try:
        import xml.etree.ElementTree as element_tree

        root = element_tree.parse(str(junit_path)).getroot()
    except (OSError, element_tree.ParseError) as error:
        raise ContractError("cannot read JUnit XML {0}: {1}".format(junit_path, error))

    executions = []
    for testcase in root.iter():
        if _local_name(testcase.tag) != "testcase":
            continue
        properties = _properties(testcase)
        api = _one_property(properties, "tinymongo.api")
        backend = _one_property(properties, "tinymongo.backend")
        suite = _one_property(properties, "tinymongo.suite")
        if not api or not backend or not suite:
            raise ContractError(
                "JUnit testcase {0} lacks unique tinymongo.api, tinymongo.backend, "
                "or tinymongo.suite properties".format(
                    testcase.get("name") or "<unnamed>"
                )
            )
        classname = _string(testcase.get("classname"), "JUnit classname")
        name = _string(testcase.get("name"), "JUnit test name")
        normalized_name = _case_name(name, api, backend)
        explicit_values = properties.get("tinymongo.contract_id", [])
        explicit_case_id = _one_property(properties, "tinymongo.contract_id")
        if explicit_values and explicit_case_id is None:
            raise ContractError(
                "JUnit testcase {0} lacks a unique tinymongo.contract_id property".format(
                    name
                )
            )
        case_id = explicit_case_id or "{0}::{1}".format(classname, normalized_name)
        outcome, reason, observation = _result(testcase)
        executions.append(
            {
                "api": api,
                "backend": backend,
                "case_id": case_id,
                "outcome": outcome,
                "observation": observation,
                "reason": reason,
                "suite": suite,
                "target": "{0}-{1}".format(implementation, backend),
            }
        )
    executions.sort(key=lambda item: (item["target"], item["case_id"], item["api"]))
    result = {
        "schema_version": SCHEMA_VERSION,
        "contract": "tinymongo-v1",
        "executions": executions,
    }
    if corpus is not None:
        validate_results(result, corpus, "ingested results")
    return result


def _case_requirements(case_id: str) -> Mapping[str, Any]:
    ordered_reads = {
        "tests.contracts.test_client_read_fidelity_contract::"
        "test_configured_document_and_datetime_results_match_mongodb",
        "tests.contracts.test_client_read_fidelity_contract::"
        "test_same_millisecond_write_identity_matches_mongodb",
        "tests.contracts.test_bson_value_types_contract::"
        "test_tm035_scoped_code_honors_recursive_client_read_options",
    }
    if case_id in ordered_reads:
        return {
            "client_options": {
                "document_class": "collections.OrderedDict",
                "tz_aware": True,
            }
        }
    standard_uuid = {
        "tests.contracts.test_uuid_regex_contract::"
        "test_uuid_round_trip_uses_standard_binary_identity",
        "tests.contracts.test_uuid_regex_contract::"
        "test_uuid_and_subtype_four_binary_share_unique_identity",
    }
    if case_id in standard_uuid:
        return {
            "mongodb_collection_options": {
                "uuid_representation": "STANDARD",
            }
        }
    return {}


def snapshot_corpus(
    junit_path: Path,
    source_root: Path,
    source_commit: str,
    expected_apis: Sequence[str] = EXPECTED_APIS,
    expected_backends: Sequence[str] = EXPECTED_BACKENDS,
    expected_case_count: int = EXPECTED_CASE_COUNT,
    harvest_toolchain: Optional[Mapping[str, str]] = None,
) -> Tuple[Mapping[str, Any], Mapping[str, Any], str]:
    matrix_results = ingest_junit(junit_path, "tinymongo")
    expected_cells = {
        (api, backend) for api in expected_apis for backend in expected_backends
    }
    seen_cells = set()
    grouped: Dict[str, Dict[str, Any]] = {}
    for execution in matrix_results["executions"]:
        case_id = execution["case_id"]
        cell = (case_id, execution["api"], execution["backend"])
        if cell in seen_cells:
            raise ContractError("duplicate matrix cell {0}".format(cell))
        seen_cells.add(cell)
        if (execution["api"], execution["backend"]) not in expected_cells:
            raise ContractError(
                "matrix contains an unexpected API/backend cell {0}".format(cell)
            )
        source_module = case_id.split("::", 1)[0]
        relative = source_module.replace(".", "/") + ".py"
        entry = grouped.setdefault(
            case_id,
            {
                "apis": [],
                "id": case_id,
                "source": relative,
                "suite": execution["suite"],
                "matrix_cells": set(),
            },
        )
        if entry["suite"] != execution["suite"] or entry["source"] != relative:
            raise ContractError(
                "case metadata disagrees across executions: {0}".format(case_id)
            )
        entry["apis"].append(execution["api"])
        entry["matrix_cells"].add((execution["api"], execution["backend"]))
    if len(grouped) != expected_case_count:
        raise ContractError(
            "complete matrix must contain {0} logical cases; found {1}".format(
                expected_case_count, len(grouped)
            )
        )
    cases = []
    for case_id in sorted(grouped):
        entry = grouped[case_id]
        if entry.pop("matrix_cells") != expected_cells:
            raise ContractError(
                "complete matrix has missing cells for {0}".format(case_id)
            )
        entry["apis"] = list(expected_apis)
        entry["requirements"] = _case_requirements(case_id)
        cases.append(entry)

    reference_executions = [
        execution
        for execution in matrix_results["executions"]
        if execution["backend"] == "memory"
    ]
    results = {
        "schema_version": SCHEMA_VERSION,
        "contract": "tinymongo-v1",
        "executions": reference_executions,
    }

    resolved_commit = (
        _git(source_root, ["rev-parse", "{0}^{{commit}}".format(source_commit)])
        .decode("ascii")
        .strip()
    )
    if resolved_commit != source_commit:
        raise ContractError(
            "--source-commit does not resolve to the exact supplied commit"
        )
    source_git_tree = (
        _git(source_root, ["rev-parse", "{0}:tests/contracts".format(source_commit)])
        .decode("ascii")
        .strip()
    )
    source_paths = set(
        _git(
            source_root,
            ["ls-tree", "-r", "--name-only", source_commit, "--", "tests/contracts"],
        )
        .decode("utf-8")
        .splitlines()
    )
    if not source_paths or any(case["source"] not in source_paths for case in cases):
        raise ContractError(
            "source commit does not contain every collected contract module"
        )
    source_files = []
    tree_hasher = hashlib.sha256()
    for relative in sorted(source_paths):
        blob = _git(source_root, ["show", "{0}:{1}".format(source_commit, relative)])
        digest = _sha256_bytes(blob)
        source_files.append({"path": relative, "sha256": digest})
        tree_hasher.update(relative.encode("utf-8"))
        tree_hasher.update(b"\0")
        tree_hasher.update(bytes.fromhex(digest))
        tree_hasher.update(b"\0")
    corpus = {
        "schema_version": SCHEMA_VERSION,
        "contract": "tinymongo-v1",
        "source_commit": source_commit,
        "source_git_tree": source_git_tree,
        "source_files": source_files,
        "cases": cases,
    }
    if harvest_toolchain is not None:
        corpus["harvest_toolchain"] = dict(harvest_toolchain)
    return corpus, results, tree_hasher.hexdigest()


def _load_result_files(
    paths: Sequence[Path], corpus: Mapping[str, Any]
) -> List[Mapping[str, Any]]:
    loaded = []
    for path in paths:
        loaded.append(validate_results(_load_json(path), corpus, str(path)))
    return loaded


def compare_results(
    manifest: Mapping[str, Any],
    corpus: Mapping[str, Any],
    differences: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
    reference_target: str,
    required_targets: Sequence[str] = (),
    require_allowlist_targets: bool = False,
) -> Tuple[Mapping[str, Any], str, bool]:
    executions = []
    for result in results:
        executions.extend(result["executions"])
    by_target: Dict[str, Dict[Tuple[str, str], Mapping[str, Any]]] = defaultdict(dict)
    for execution in executions:
        key = (execution["case_id"], execution["api"])
        if key in by_target[execution["target"]]:
            raise ContractError(
                "duplicate execution across result files for {0}/{1}/{2}".format(
                    execution["target"], key[0], key[1]
                )
            )
        by_target[execution["target"]][key] = execution
    if reference_target not in by_target:
        raise ContractError(
            "reference target {0} has no results".format(reference_target)
        )

    expected = {(case["id"], api) for case in corpus["cases"] for api in case["apis"]}
    reference = by_target[reference_target]
    if set(reference) != expected:
        raise ContractError("reference target does not cover the complete corpus")
    allowlist = {
        (item["target"], item["case_id"], item["api"]): item
        for item in differences["differences"]
    }
    observed_allowlist = set()
    target_reports = []
    missing_required_targets = sorted(set(required_targets) - set(by_target))
    failed = bool(missing_required_targets)

    def fingerprint(execution: Mapping[str, Any]) -> str:
        observation = execution.get("observation")
        if isinstance(observation, dict) and isinstance(
            observation.get("fingerprint"), str
        ):
            return observation["fingerprint"]
        category = execution.get("outcome", "missing")
        return _observation(execution["outcome"], category, execution.get("reason"))[
            "fingerprint"
        ]

    for target in sorted(by_target):
        observed = by_target[target]
        counts = Counter(item["outcome"] for item in observed.values())
        mismatches = []
        if target != reference_target:
            for key in sorted(expected):
                expected_execution = reference[key]
                actual = observed.get(key)
                actual_outcome = actual["outcome"] if actual else "missing"
                expected_fingerprint = fingerprint(expected_execution)
                actual_fingerprint = fingerprint(actual) if actual else None
                if (
                    actual_outcome == expected_execution["outcome"]
                    and actual_fingerprint == expected_fingerprint
                ):
                    continue
                difference = allowlist.get((target, key[0], key[1]))
                allowed = bool(
                    actual
                    and difference
                    and difference["reference_outcome"] == expected_execution["outcome"]
                    and difference["candidate_outcome"] == actual_outcome
                    and difference.get("reference_fingerprint", expected_fingerprint)
                    == expected_fingerprint
                    and difference["candidate_fingerprint"] == actual_fingerprint
                )
                if allowed:
                    observed_allowlist.add((target, key[0], key[1]))
                else:
                    failed = True
                mismatches.append(
                    {
                        "allowed": allowed,
                        "api": key[1],
                        "case_id": key[0],
                        "expected": expected_execution["outcome"],
                        "expected_fingerprint": expected_fingerprint,
                        "expected_reason": _display_reason(
                            expected_execution.get("reason")
                        ),
                        "observed": actual_outcome,
                        "observed_fingerprint": actual_fingerprint,
                        "observed_reason": _display_reason(
                            actual.get("reason") if actual else None
                        ),
                        "policy": (
                            {
                                "issue": difference["issue"],
                                "reason": difference["reason"],
                            }
                            if difference is not None
                            else None
                        ),
                    }
                )
        target_reports.append(
            {
                "counts": dict(sorted(counts.items())),
                "executions": len(observed),
                "mismatches": mismatches,
                "target": target,
            }
        )
    stale = []
    targets = set(by_target)
    for key, item in sorted(allowlist.items()):
        if (
            key[0] in targets or require_allowlist_targets
        ) and key not in observed_allowlist:
            stale.append(item)
            failed = True
    report = {
        "schema_version": SCHEMA_VERSION,
        "contract": manifest["contract"],
        "reference_target": reference_target,
        "status": "failed"
        if failed
        else ("reference-only" if len(by_target) == 1 else "passed"),
        "targets": target_reports,
        "missing_required_targets": missing_required_targets,
        "stale_intentional_differences": stale,
    }
    lines = [
        "# Mongo compatibility parity",
        "",
        "Contract: `{0}`  ".format(manifest["contract"]),
        "TinyMongo source: `{0}`  ".format(manifest["source"]["commit"]),
        "Reference: `{0}`  ".format(reference_target),
        "Status: **{0}**".format(report["status"]),
        "",
        "| Target | Executions | Passed | Failed/error | Skipped/xfail | Mismatches |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    if missing_required_targets:
        lines.extend(
            [
                "",
                "Missing required targets: {0}".format(
                    ", ".join(
                        "`{0}`".format(target) for target in missing_required_targets
                    )
                ),
            ]
        )
    for target in target_reports:
        counts = target["counts"]
        lines.append(
            "| `{0}` | {1} | {2} | {3} | {4} | {5} |".format(
                target["target"],
                target["executions"],
                counts.get("passed", 0) + counts.get("xpassed", 0),
                counts.get("failed", 0) + counts.get("error", 0),
                counts.get("skipped", 0) + counts.get("xfailed", 0),
                len(target["mismatches"]),
            )
        )
    mismatch_rows = [
        (target["target"], mismatch)
        for target in target_reports
        for mismatch in target["mismatches"]
    ]
    if mismatch_rows:
        lines.extend(
            [
                "",
                "## Outcome differences",
                "",
                "| Target | API | Contract | Reference | Observed | Policy |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for target, mismatch in mismatch_rows:
            lines.append(
                "| `{0}` | `{1}` | `{2}` | `{3}` | `{4}` | {5} |".format(
                    target,
                    mismatch["api"],
                    mismatch["case_id"].replace("|", "\\|"),
                    mismatch["expected"],
                    mismatch["observed"],
                    (
                        "[allowed]({0})".format(mismatch["policy"]["issue"])
                        if mismatch["allowed"]
                        else "uncovered"
                    ),
                )
            )
    if stale:
        lines.extend(
            [
                "",
                "## Stale intentional differences",
                "",
                "Strict allow-list entries must be removed when the recorded difference is not observed.",
                "",
            ]
        )
        lines.extend(
            "- `{0}` ({1})".format(item["case_id"], item["issue"]) for item in stale
        )
    lines.append("")
    return report, "\n".join(lines), failed


def _command_validate(arguments: argparse.Namespace) -> int:
    manifest = validate_contract(arguments.manifest)
    print(
        "validated {0}: {1} cases, {2} reference executions".format(
            manifest["contract"],
            manifest["case_count"],
            manifest["reference_execution_count"],
        )
    )
    return 0


def _command_snapshot(arguments: argparse.Namespace) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", arguments.source_commit):
        raise ContractError("--source-commit must be a full Git commit")
    corpus, results, tree_digest = snapshot_corpus(
        arguments.junit,
        arguments.source_root,
        arguments.source_commit,
        harvest_toolchain={
            "pymongo": arguments.pymongo_version,
            "pytest": arguments.pytest_version,
            "python": arguments.python_version,
        },
    )
    if any(item["outcome"] != "passed" for item in results["executions"]):
        raise ContractError("memory reference must pass every collected contract")
    artifacts = (
        (arguments.corpus_output, _pretty_bytes(corpus)),
        (arguments.results_output, _pretty_bytes(results)),
    )
    if arguments.check:
        for path, expected in artifacts:
            try:
                actual = path.read_bytes()
            except OSError as error:
                raise ContractError(
                    "cannot check snapshot {0}: {1}".format(path, error)
                )
            if actual != expected:
                raise ContractError("snapshot drift detected in {0}".format(path))
    else:
        for path, value in (
            (arguments.corpus_output, corpus),
            (arguments.results_output, results),
        ):
            _write_json(path, value)
    print(
        "{0} {1} cases, {2} matrix cells, and {3} reference executions; "
        "source tree {4}".format(
            "checked" if arguments.check else "snapshotted",
            len(corpus["cases"]),
            len(corpus["cases"]) * len(EXPECTED_APIS) * len(EXPECTED_BACKENDS),
            len(results["executions"]),
            tree_digest,
        )
    )
    return 0


def _command_ingest(arguments: argparse.Namespace) -> int:
    manifest = validate_contract(arguments.manifest)
    _, corpus_path, _, _, _ = _manifest_paths(arguments.manifest)
    corpus = _object(_load_json(corpus_path), "corpus")
    results = ingest_junit(arguments.junit, arguments.implementation, corpus)
    _write_json(arguments.output, results)
    print(
        "ingested {0} executions for {1} under {2}".format(
            len(results["executions"]), arguments.implementation, manifest["contract"]
        )
    )
    return 0


def _command_run(arguments: argparse.Namespace) -> int:
    if not arguments.command:
        raise ContractError("run requires a command after --")
    command = list(arguments.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ContractError("run requires a command after --")
    environment = os.environ.copy()
    environment["BRISKDB_MONGO_PARITY_JUNIT"] = str(arguments.junit.resolve())
    try:
        arguments.junit.unlink()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise ContractError("cannot remove stale JUnit output: {0}".format(error))
    completed = subprocess.run(
        command, cwd=str(arguments.working_directory), env=environment
    )
    if not arguments.junit.is_file():
        raise ContractError(
            "runner did not create {0}; use BRISKDB_MONGO_PARITY_JUNIT".format(
                arguments.junit
            )
        )
    ingest_arguments = argparse.Namespace(
        implementation=arguments.implementation,
        junit=arguments.junit,
        manifest=arguments.manifest,
        output=arguments.output,
    )
    _command_ingest(ingest_arguments)
    return completed.returncode


def _command_report(arguments: argparse.Namespace) -> int:
    manifest = validate_contract(arguments.manifest)
    for target in arguments.require_target:
        if not TARGET_PATTERN.fullmatch(target):
            raise ContractError("invalid required target {0}".format(target))
    _, corpus_path, reference_path, differences_path, _ = _manifest_paths(
        arguments.manifest
    )
    corpus = _object(_load_json(corpus_path), "corpus")
    differences = _object(_load_json(differences_path), "intentional differences")
    paths = [reference_path] + list(arguments.results)
    results = _load_result_files(paths, corpus)
    report, markdown, failed = compare_results(
        manifest,
        corpus,
        differences,
        results,
        arguments.reference_target or manifest["reference_target"],
        arguments.require_target,
        arguments.require_allowlist_targets,
    )
    _write_json(arguments.json_output, report)
    arguments.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    arguments.markdown_output.write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 1 if failed else 0


def _verify_provenance_git_blob(
    source_root: Path,
    commit: str,
    entry: Mapping[str, Any],
    label: str,
) -> Tuple[str, bytes]:
    relative = _string(entry.get("upstream_path"), "{0} path".format(label))
    expected_object = _string(
        entry.get("upstream_git_blob"), "{0} Git blob".format(label)
    )
    if not re.fullmatch(r"[0-9a-f]{40}", expected_object):
        raise ContractError("{0} Git blob must be a full object ID".format(label))

    actual_object = (
        _git(
            source_root,
            ["rev-parse", "--verify", "{0}:{1}".format(commit, relative)],
        )
        .decode("ascii")
        .strip()
    )
    if actual_object != expected_object:
        raise ContractError(
            "{0} Git blob mismatch for {1}: expected {2}, found {3}".format(
                label, relative, expected_object, actual_object
            )
        )
    object_type = (
        _git(source_root, ["cat-file", "-t", actual_object]).decode("ascii").strip()
    )
    if object_type != "blob":
        raise ContractError("{0} is not a Git blob: {1}".format(label, relative))

    contents = _git(source_root, ["cat-file", "blob", actual_object])
    expected_digest = _sha256_string(
        entry.get("upstream_sha256"), "{0} upstream sha256".format(label)
    )
    actual_digest = _sha256_bytes(contents)
    if actual_digest != expected_digest:
        raise ContractError(
            "{0} source hash mismatch for {1}: expected {2}, found {3}".format(
                label, relative, expected_digest, actual_digest
            )
        )
    return relative, contents


def _run_git_apply(
    staging_root: Path, patch_path: Path, arguments: Sequence[str]
) -> bytes:
    try:
        completed = subprocess.run(
            ["git", "apply"] + list(arguments) + ["--", str(patch_path.resolve())],
            cwd=str(staging_root),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", b"").decode("utf-8", "replace").strip()
        raise ContractError(
            "cannot replay adaptation patch against pinned source blobs: {0}".format(
                detail or error
            )
        )
    return completed.stdout


def _verify_source_provenance(
    source_root: Path,
    commit: str,
    provenance_path: Path,
    contract_base: Path,
) -> None:
    provenance = _object(_load_json(provenance_path), "runner provenance")
    provenance_source = _object(provenance.get("source"), "runner provenance source")
    if provenance_source.get("commit") != commit:
        raise ContractError("runner provenance source commit does not match manifest")

    adaptation = _object(provenance.get("adaptation"), "runner adaptation")
    adaptation_files = [
        _object(value, "adaptation file {0}".format(index))
        for index, value in enumerate(
            _list(adaptation.get("files"), "adaptation files")
        )
    ]
    excluded_files = [
        _object(value, "excluded upstream file {0}".format(index))
        for index, value in enumerate(
            _list(
                adaptation.get("excluded_upstream_files"),
                "excluded upstream files",
            )
        )
    ]

    upstream_blobs = {}
    for index, entry in enumerate(adaptation_files):
        label = "adaptation file {0}".format(index)
        relative, contents = _verify_provenance_git_blob(
            source_root, commit, entry, label
        )
        upstream_blobs[relative] = contents
    for index, entry in enumerate(excluded_files):
        label = "excluded upstream file {0}".format(index)
        relative, contents = _verify_provenance_git_blob(
            source_root, commit, entry, label
        )
        upstream_blobs[relative] = contents

    license_entry = _object(provenance_source.get("license"), "runner source license")
    license_path, license_contents = _verify_provenance_git_blob(
        source_root, commit, license_entry, "runner source license"
    )
    if license_path != "LICENSE.txt":
        raise ContractError("runner source license must come from LICENSE.txt")
    _, local_license_path = _runner_local_path(
        contract_base,
        license_entry.get("runner_path"),
        "runner source license",
    )
    try:
        local_license = local_license_path.read_bytes()
    except OSError as error:
        raise ContractError("cannot read runner source license: {0}".format(error))
    if local_license != license_contents:
        raise ContractError("runner source license is not byte-for-byte upstream")

    _, patch_path = _runner_local_path(
        contract_base, adaptation.get("patch"), "adaptation patch"
    )
    expected_patch_digest = _sha256_string(
        adaptation.get("patch_sha256"), "adaptation patch sha256"
    )
    if _sha256_file(patch_path) != expected_patch_digest:
        raise ContractError("adaptation patch hash does not match runner provenance")

    with tempfile.TemporaryDirectory(prefix="briskdb-mongo-provenance-") as temporary:
        staging_root = Path(temporary)
        expected_paths = set()
        expected_adapted_paths = set()
        local_outputs = {}
        for index, entry in enumerate(adaptation_files):
            label = "adaptation file {0}".format(index)
            upstream_path = _string(
                entry.get("upstream_path"), "{0} path".format(label)
            )
            runner_relative, local_output = _runner_local_path(
                contract_base, entry.get("runner_path"), label
            )
            staged_output = staging_root.joinpath(*Path(runner_relative).parts)
            staged_output.parent.mkdir(parents=True, exist_ok=True)
            staged_output.write_bytes(upstream_blobs[upstream_path])
            expected_paths.add(runner_relative)
            local_outputs[runner_relative] = local_output
            status = _string(entry.get("status"), "{0} status".format(label))
            if status == "adapted":
                expected_adapted_paths.add(runner_relative)

        # The unified diff names the upstream file in its `a/` header and the
        # vendored destination in its `b/` header. Seed each destination with
        # the pinned upstream blob, then require Git to apply every hunk.
        numstat = _run_git_apply(staging_root, patch_path, ["--numstat", "-z"])
        touched_paths = set()
        for record in numstat.split(b"\0"):
            if not record:
                continue
            fields = record.split(b"\t", 2)
            if len(fields) != 3:
                raise ContractError("adaptation patch has invalid numstat output")
            try:
                touched_paths.add(fields[2].decode("utf-8"))
            except UnicodeDecodeError as error:
                raise ContractError(
                    "adaptation patch contains a non-UTF-8 path: {0}".format(error)
                )
        if touched_paths != expected_adapted_paths:
            raise ContractError(
                "adaptation patch path inventory mismatch; expected={0}, found={1}".format(
                    sorted(expected_adapted_paths), sorted(touched_paths)
                )
            )

        _run_git_apply(staging_root, patch_path, ["--check", "--whitespace=error-all"])
        _run_git_apply(staging_root, patch_path, ["--whitespace=error-all"])

        reconstructed_paths = {
            path.relative_to(staging_root).as_posix()
            for path in staging_root.rglob("*")
            if path.is_file()
        }
        if reconstructed_paths != expected_paths:
            raise ContractError(
                "adaptation patch output inventory mismatch; expected={0}, found={1}".format(
                    sorted(expected_paths), sorted(reconstructed_paths)
                )
            )

        for index, entry in enumerate(adaptation_files):
            label = "adaptation file {0}".format(index)
            runner_relative = _string(
                entry.get("runner_path"), "{0} runner path".format(label)
            )
            reconstructed = staging_root.joinpath(*Path(runner_relative).parts)
            expected_digest = _sha256_string(
                entry.get("runner_sha256"), "{0} runner sha256".format(label)
            )
            reconstructed_digest = _sha256_file(reconstructed)
            if reconstructed_digest != expected_digest:
                raise ContractError(
                    "adaptation patch does not reconstruct {0}: expected {1}, found {2}".format(
                        runner_relative, expected_digest, reconstructed_digest
                    )
                )
            try:
                local_contents = local_outputs[runner_relative].read_bytes()
            except OSError as error:
                raise ContractError(
                    "cannot read adapted runner file {0}: {1}".format(
                        runner_relative, error
                    )
                )
            if reconstructed.read_bytes() != local_contents:
                raise ContractError(
                    "adaptation patch output differs from checked-in runner file {0}".format(
                        runner_relative
                    )
                )


def _command_verify_source(arguments: argparse.Namespace) -> int:
    manifest = validate_contract(arguments.manifest)
    _, corpus_path, _, _, _ = _manifest_paths(arguments.manifest)
    corpus = _object(_load_json(corpus_path), "corpus")
    commit = manifest["source"]["commit"]
    resolved = (
        _git(arguments.source_root, ["rev-parse", "{0}^{{commit}}".format(commit)])
        .decode("ascii")
        .strip()
    )
    if resolved != commit:
        raise ContractError("source repository does not contain the locked commit")
    git_tree = (
        _git(arguments.source_root, ["rev-parse", "{0}:tests/contracts".format(commit)])
        .decode("ascii")
        .strip()
    )
    if git_tree != manifest["source"]["contract_git_tree"]:
        raise ContractError("locked source Git tree does not match the manifest")
    runtime_git_tree = (
        _git(arguments.source_root, ["rev-parse", "{0}:tinymongo".format(commit)])
        .decode("ascii")
        .strip()
    )
    if runtime_git_tree != manifest["source"]["runtime_git_tree"]:
        raise ContractError("locked runtime Git tree does not match the manifest")
    paths = (
        _git(
            arguments.source_root,
            ["ls-tree", "-r", "--name-only", commit, "--", "tests/contracts"],
        )
        .decode("utf-8")
        .splitlines()
    )
    recorded = {entry["path"]: entry["sha256"] for entry in corpus["source_files"]}
    if set(paths) != set(recorded):
        raise ContractError("locked source path inventory does not match the corpus")
    for path in paths:
        blob = _git(arguments.source_root, ["show", "{0}:{1}".format(commit, path)])
        if _sha256_bytes(blob) != recorded[path]:
            raise ContractError(
                "locked source blob does not match corpus: {0}".format(path)
            )
    manifest_files = _object(manifest.get("files"), "manifest.files")
    provenance_path = arguments.manifest.parent / _string(
        manifest_files.get("runner_provenance"), "manifest.files.runner_provenance"
    )
    _verify_source_provenance(
        arguments.source_root,
        commit,
        provenance_path,
        arguments.manifest.parent,
    )
    print(
        "verified TinyMongo source {0} ({1}) and runner provenance".format(
            commit, git_tree
        )
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.set_defaults(function=None)
    subparsers = parser.add_subparsers(dest="subcommand")

    validate = subparsers.add_parser(
        "validate", help="validate the checked-in contract"
    )
    validate.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    validate.set_defaults(function=_command_validate)

    snapshot = subparsers.add_parser(
        "snapshot", help="snapshot a TinyMongo JUnit corpus"
    )
    snapshot.add_argument("--junit", type=Path, required=True)
    snapshot.add_argument("--source-root", type=Path, required=True)
    snapshot.add_argument("--source-commit", required=True)
    snapshot.add_argument("--corpus-output", type=Path, required=True)
    snapshot.add_argument("--results-output", type=Path, required=True)
    snapshot.add_argument("--python-version", required=True)
    snapshot.add_argument("--pytest-version", required=True)
    snapshot.add_argument("--pymongo-version", required=True)
    snapshot.add_argument(
        "--check",
        action="store_true",
        help="compare a regenerated snapshot with the output files",
    )
    snapshot.set_defaults(function=_command_snapshot)

    verify_source = subparsers.add_parser(
        "verify-source", help="verify the locked TinyMongo Git blobs"
    )
    verify_source.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    verify_source.add_argument("--source-root", type=Path, required=True)
    verify_source.set_defaults(function=_command_verify_source)

    ingest = subparsers.add_parser(
        "ingest", help="normalize one implementation's JUnit"
    )
    ingest.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ingest.add_argument("--junit", type=Path, required=True)
    ingest.add_argument("--implementation", required=True)
    ingest.add_argument("--output", type=Path, required=True)
    ingest.set_defaults(function=_command_ingest)

    run = subparsers.add_parser(
        "run", help="run an external contract producer and ingest JUnit"
    )
    run.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    run.add_argument("--implementation", required=True)
    run.add_argument("--working-directory", type=Path, required=True)
    run.add_argument("--junit", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("command", nargs=argparse.REMAINDER)
    run.set_defaults(function=_command_run)

    report = subparsers.add_parser("report", help="compare normalized results")
    report.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    report.add_argument("--reference-target")
    report.add_argument("--json-output", type=Path, required=True)
    report.add_argument("--markdown-output", type=Path, required=True)
    report.add_argument(
        "--require-target",
        action="append",
        default=[],
        help="fail unless the named target is present; repeatable",
    )
    report.add_argument(
        "--require-allowlist-targets",
        action="store_true",
        help="fail when a target named by the allow-list has no result",
    )
    report.add_argument("results", nargs="*", type=Path)
    report.set_defaults(function=_command_report)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.function is None:
        parser.error("a subcommand is required")
    try:
        return int(arguments.function(arguments))
    except ContractError as error:
        print("mongo parity contract error: {0}".format(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
