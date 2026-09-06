# Python value and error contract

BriskDB converts Python values directly to its protocol-neutral Rust values.
It never passes them through JSON.

## SQL parameters and results

| Python parameter | BriskDB value | Result behavior |
| --- | --- | --- |
| `None` | null | `None` |
| `bool` | Boolean | SQLite materializes it as integer `0` or `1` |
| `int`, `-2^63` through `2^63-1` | signed 64-bit | exact Python `int` |
| `int`, `2^63` through `2^64-1` | unsigned 64-bit | accepted by conversion, then rejected by SQL because SQLite cannot bind it losslessly |
| other `int` | none | `NumericOutOfRangeError` |
| finite `float` or infinity | 64-bit float | exact Python `float` |
| `float('nan')` | 64-bit float | `UnsupportedError`; SQLite would silently turn it into null |
| `decimal.Decimal` | exact decimal text | `UnsupportedError` at SQL execution until SQLite has a lossless decimal binding; a native decimal result becomes `decimal.Decimal` |
| `str` | UTF-8 text | exact Python `str` |
| `bytes`, `bytearray`, `memoryview` | binary | immutable Python `bytes` |
| datetime, UUID, containers, and other objects | none | `TypeMismatchError` |

Rows are ordered tuples. Column metadata and rows remain separate so duplicate
column names are not lost. Generated keys retain their column name and exact
converted value.

The Python test suite executes this table at the signed/unsigned boundaries,
with randomized values, and with unsupported and self-referential objects.

## Errors

Every engine error becomes a `BriskDBError` subclass. The diagnostic is
available through `str(error)`, while `error.code` is a stable machine-readable
value and `error.retryable` states whether automatic retry is recommended.

The hierarchy groups errors as `DataError`, `ProgrammingError`,
`IntegrityError`, or `OperationalError`, with one concrete exception for each
Rust `EngineErrorKind`. Constraint-specific errors inherit from both
`ConstraintViolationError` and `IntegrityError`. Only `BusyError` is currently
marked retryable.

## BSON documents

Document methods use the `bson` package distributed with PyMongo. Install it
separately when an application needs document commands:

```bash
python -m pip install briskdb pymongo
```

PyMongo is deliberately absent from BriskDB's runtime dependencies. Importing
BriskDB, opening a database, and using every SQL method neither imports nor
requires `bson`. A document method checks for the optional package before it
executes and otherwise raises `UnsupportedError` with code `unsupported`. The
unrelated `bson` distribution on PyPI is not supported; install PyMongo, which
owns the public `bson` package tested by BriskDB. The release gate currently
pins PyMongo 4.17.0.

Python mappings become BriskDB's ordered BSON documents directly, without a
JSON intermediate. Returned documents are ordinary insertion-ordered `dict`
objects. A mapping cannot express repeated field names, and stored document
fields must be unique; raw duplicate-preserving BSON remains a Rust codec
facility.

| Python document value | BSON representation | Returned Python value |
| --- | --- | --- |
| `None` | null | `None` |
| `bool` | Boolean | `bool` |
| plain `int` in `-2^31..=2^31-1` | int32 | plain `int` |
| plain `int` in the remaining signed 64-bit range | int64 | `bson.Int64` |
| `bson.Int64`, including a small value | int64 | `bson.Int64` |
| other `int` | none | `NumericOutOfRangeError` |
| `float` | double | exact IEEE-754 bits, including signed zero, infinities, and NaN payloads |
| `str` | UTF-8 string | exact `str`, including embedded NUL characters |
| `bytes`, `bytearray`, `memoryview` | binary subtype 0 | immutable `bytes` |
| `bson.Binary` | binary with its unsigned subtype | subtype 0 becomes `bytes`, a configured matching UUID encoding becomes `uuid.UUID`, and every other subtype remains `bson.Binary` |
| `bson.Decimal128` | exact 16-byte BID payload | `bson.Decimal128`, including finite values, infinities, quiet NaN, and signaling NaN |
| `bson.ObjectId` | exact 12 bytes | `bson.ObjectId` |
| `datetime.datetime` | signed UTC milliseconds | aware UTC `datetime`, or `bson.datetime_ms.DatetimeMS` outside Python's datetime range |
| `bson.datetime_ms.DatetimeMS` | signed UTC milliseconds | the same date rule as above |
| `uuid.UUID` | UUID binary selected by the database UUID mode | `uuid.UUID` unless the mode is `unspecified`, which rejects it |
| `bson.Regex` | pattern plus canonical BSON flags | `bson.Regex` |
| `bson.Timestamp` | unsigned seconds and increment | `bson.Timestamp` |
| `bson.Code` | JavaScript source and optional ordered scope | `bson.Code` |
| `bson.MinKey` / `bson.MaxKey` | BSON sentinels | the matching BSON sentinel |
| `list`/`tuple` and nested mappings | array and document | `list` and insertion-ordered `dict` recursively |

`decimal.Decimal` is the SQL decimal input type. BSON documents require
`bson.Decimal128`, which retains its exact representation rather than
converting through a language decimal.

### Datetimes

A naive `datetime` is interpreted as UTC. An aware value is normalized to UTC.
BSON stores milliseconds, so conversion floors discarded microseconds toward
the earlier millisecond; for example, one microsecond before the Unix epoch
becomes millisecond `-1`. In-range results are timezone-aware and carry
`datetime.timezone.utc`. BSON millisecond values outside years 1 through 9999
return `DatetimeMS` instead of clamping or failing.

### UUID representations

`uuid_representation` is fixed on a database handle so inputs, filters, index
keys, and results use one rule. The default is `standard`; the other modes are
`unspecified`, `python_legacy`, `java_legacy`, and `csharp_legacy`.

- `standard` stores RFC-4122 bytes in subtype 4.
- Each legacy mode stores subtype 3 with that driver's historical byte order.
- `unspecified` rejects native `uuid.UUID` values and leaves subtype 3 and 4
  values as `bson.Binary`.
- A configured mode materializes a matching 16-byte subtype as `uuid.UUID`.
  A nonmatching subtype remains `bson.Binary` with exact bytes and subtype.

BSON wire data does not say whether matching UUID bytes originally came from a
native UUID or an explicit `bson.Binary`. Consequently, a matching explicit
Binary also returns as `uuid.UUID`. This is the same representation boundary
used by PyMongo's `CodecOptions`. The same convergence applies to subtype 0:
an explicit `bson.Binary(payload, 0)` returns as immutable `bytes`.

### Invalid containers and values

Document conversion rejects non-string keys, keys containing NUL, lone Unicode
surrogates in keys or values, cyclic containers (including a `Code` scope
cycle), invalid regular-expression patterns or flags, and structures deeper
than 100 BSON containers. A structure containing exactly 100 containers is
valid. One BSON document may be at most 16 MiB. These failures use stable
BriskDB exception subclasses and fixed diagnostics that do not include
application values. Unsupported BSON families such as DBRef and deprecated
BSON wire types are rejected instead of being converted through JSON. Reusing
one acyclic child mapping in more than one location is valid; cycle detection
follows the active recursion path.
