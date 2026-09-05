"""Runtime-neutral helpers used by the frozen Mongo compatibility contracts.

This module deliberately depends on PyMongo's public BSON and exception types,
which form the compatibility surface, but never imports TinyMongo.  A target
adapter is responsible for translating its failures to these public shapes.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re
from collections.abc import Mapping
from typing import Any, Callable, Optional
from uuid import UUID

from bson import Binary, Code, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp
from pymongo.errors import DuplicateKeyError, OperationFailure, WriteError


DUPLICATE_KEY_ERRORS = (DuplicateKeyError,)
OPERATION_ERRORS = (OperationFailure,)
WRITE_ERRORS = (WriteError,)
UNSUPPORTED_ERRORS = (NotImplementedError,)
QUERY_REJECTION_ERRORS = OPERATION_ERRORS + UNSUPPORTED_ERRORS


@dataclass(frozen=True)
class Outcome:
    """Normalized result of one operation against a contract target."""

    value: Any = None
    error: Optional[str] = None


@dataclass
class ContractTarget:
    """Objects and metadata exposed to each shared contract."""

    name: str
    transport: str
    api: str
    client: Any
    database: Any
    collection: Any
    unsupported_warning: type


def error_category(error: Exception) -> str:
    """Map adapter-specific failures to the contract vocabulary."""

    if isinstance(error, DuplicateKeyError):
        return "duplicate_key"
    if isinstance(error, OperationFailure):
        return "operation_failure"
    return "{0}.{1}".format(type(error).__module__, type(error).__name__)


def observe(operation: Callable[[], Any]) -> Outcome:
    """Run an operation and retain either its value or normalized error."""

    try:
        return Outcome(value=operation())
    except Exception as error:  # noqa: BLE001 - exceptions are contract output
        return Outcome(error=error_category(error))


def regex_type():
    """Return the public BSON regular-expression class."""

    return Regex


def _number_identity(value: Any):
    if isinstance(value, Decimal128):
        decimal_value = value.to_decimal()
    elif isinstance(value, int) and not isinstance(value, bool):
        decimal_value = Decimal(value)
    elif isinstance(value, float):
        decimal_value = Decimal.from_float(value)
    else:
        raise TypeError("value is not a BSON number")
    if decimal_value.is_nan():
        return "nan"
    if decimal_value.is_infinite():
        return "-infinity" if decimal_value.is_signed() else "infinity"
    return decimal_value.as_integer_ratio()


def _regex_options(flags: Any) -> str:
    if isinstance(flags, str):
        return "".join(letter for letter in "ilmsux" if letter in flags)
    return "".join(
        letter
        for letter, flag in (
            ("i", re.IGNORECASE),
            ("l", re.LOCALE),
            ("m", re.MULTILINE),
            ("s", re.DOTALL),
            ("u", re.UNICODE),
            ("x", re.VERBOSE),
        )
        if int(flags) & int(flag)
    )


def _datetime_milliseconds(value: datetime) -> int:
    normalized = (
        value.replace(tzinfo=timezone.utc)
        if value.utcoffset() is None
        else value.astimezone(timezone.utc)
    )
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = normalized - epoch
    return (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def bson_identity_key(value: Any):
    """Return the scalar BSON equality identity used by the frozen assertions."""

    if value is None:
        return "null", None
    if type(value) is bool:
        return "boolean", value
    if isinstance(value, (int, float, Decimal128)) and not isinstance(value, bool):
        return "number", _number_identity(value)
    if isinstance(value, UUID):
        return "binary", (4, value.bytes)
    if isinstance(value, Binary):
        return "binary", (int(value.subtype), bytes(value))
    if type(value) in (bytes, bytearray):
        return "binary", (0, bytes(value))
    if isinstance(value, Code):
        scope = None if value.scope is None else bson_value_identity_key(value.scope)
        return "code", (str(value), scope)
    if isinstance(value, str):
        return "string", value
    if isinstance(value, ObjectId):
        return "objectId", value.binary
    if isinstance(value, datetime):
        return "date", _datetime_milliseconds(value)
    if isinstance(value, Timestamp):
        return "timestamp", (value.time, value.inc)
    if isinstance(value, (Regex, type(re.compile("")))):
        pattern = value.pattern
        if isinstance(pattern, bytes):
            pattern = pattern.decode("utf-8")
        return "regex", (pattern, _regex_options(value.flags))
    if isinstance(value, MinKey):
        return "minKey", None
    if isinstance(value, MaxKey):
        return "maxKey", None
    return None


def bson_value_identity_key(value: Any):
    """Return a recursive, ordered and hashable BSON equality identity."""

    scalar = bson_identity_key(value)
    if scalar is not None:
        return scalar
    if isinstance(value, Mapping):
        items = []
        for key, item in value.items():
            item_key = bson_value_identity_key(item)
            if item_key is None:
                return None
            items.append((key, item_key))
        return "object", tuple(items)
    if isinstance(value, (list, tuple)):
        items = []
        for item in value:
            item_key = bson_value_identity_key(item)
            if item_key is None:
                return None
            items.append(item_key)
        return "array", tuple(items)
    return None
