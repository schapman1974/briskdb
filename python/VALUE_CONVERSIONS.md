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

## BSON status

The Python package has no BSON dependency for SQL use. BSON conversion cannot
be implemented honestly until BriskDB has the ordered, duplicate-safe BSON
value model tracked by [#164](https://github.com/schapman1974/briskdb/issues/164).
Once that lands, the Python side will map its native values to the installed
`bson` package's `ObjectId`, `Binary`, `Decimal128`, `Regex`, `Timestamp`,
`MinKey`, and `MaxKey`, plus timezone-aware UTC `datetime`, `UUID`, lists, and
ordered mappings. Document entrypoints remain unavailable until then rather
than silently coercing those values through JSON.

| BSON-facing Python value | Current behavior |
| --- | --- |
| aware or naive `datetime`, at any precision | no document entrypoint; rejected by SQL as `TypeMismatchError` |
| `UUID`, with any representation | no document entrypoint; rejected by SQL as `TypeMismatchError` |
| `Decimal128`, including finite values, NaN, and infinity | no document entrypoint; never imported by SQL-only use |
| `ObjectId`, `Binary`, `Regex`, `Timestamp`, `MinKey`, `MaxKey` | no document entrypoint; never imported by SQL-only use |
| lists and ordered mappings | no document entrypoint; rejected by SQL as `TypeMismatchError` |
| cyclic or excessively deep containers | rejected at the SQL boundary without traversal; BSON depth/cycle limits belong to #164 |
