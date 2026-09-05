# BSON value and codec contract

BriskDB's `documents` feature provides the BSON value model used by the planned
MongoDB listener, document storage, query engine, and Rust and Python document
APIs. The foundation is protocol-neutral and does not start a listener or add
document commands by itself.

```toml
[dependencies]
briskdb = { version = "0.1.0-alpha.5", default-features = false, features = ["documents"] }
```

The contract follows the source-locked TinyMongo v1.3.0 behavior recorded in
[`compat/mongo/v1`](../compat/mongo/v1/), with raw encoding rules checked
against the [BSON 1.1 specification](https://bsonspec.org/spec.html) and the
MongoDB BSON corpus. The comparison rules in this document are part of the
persistent identity contract. Changing them requires a new canonical-key
version.

## Values

`BsonValue` retains the wire representation needed for a lossless round-trip
of every TinyMongo-supported BSON family:

| BSON family | Rust representation | Preserved information |
| --- | --- | --- |
| null | `Null` | BSON null |
| Boolean | `Boolean(bool)` | `false` or `true` |
| 32-bit integer | `Int32(i32)` | Exact integer wire width |
| 64-bit integer | `Int64(i64)` | Exact integer wire width |
| double | `Double(f64)` | IEEE-754 bits, including signed zero and NaN payloads |
| Decimal128 | `Decimal128(BsonDecimal128)` | Exact 16-byte BID payload |
| string | `String(String)` | UTF-8 text, including embedded NUL characters |
| object | `Document(BsonDocument)` | Field order and repeated field names |
| array | `Array(Vec<BsonValue>)` | Element order |
| binary | `Binary(BsonBinary)` | All bytes and the unsigned subtype |
| UUID | `Uuid(BsonUuid)` | Logical RFC-4122 bytes and one concrete wire representation |
| ObjectId | `ObjectId(BsonObjectId)` | Exact 12 bytes |
| UTC datetime | `DateTime(BsonDateTime)` | Signed milliseconds from the Unix epoch |
| timestamp | `Timestamp(BsonTimestamp)` | Unsigned seconds and increment components |
| regular expression | `RegularExpression(BsonRegex)` | UTF-8 pattern and canonical BSON options |
| JavaScript | `JavaScript(BsonJavaScript)` | Source text and absence of scope |
| JavaScript with scope | `JavaScript(BsonJavaScript)` | Source text and ordered scope document |
| minimum/maximum | `MinKey` / `MaxKey` | Sentinel type |

The deprecated Undefined, DBPointer, and Symbol wire types are outside
TinyMongo's value inventory and have no `BsonValue` variant. The BSON codec
rejects them with `UnsupportedType` (MongoDB code 22) instead of silently
converting them.

`BsonDocument` stores an ordered sequence of entries. Raw BSON permits repeated
field names but assigns no query semantics to them, while MongoDB requires
stored document field names to be unique. BriskDB therefore never collapses a
decoded document into a map. The normal decoder rejects duplicates. An explicit
preserve policy retains every occurrence for inspection, proxying, and exact
re-encoding. `get_first`, `get_last`, and `get_all` state which occurrence they
return; `get_unique` and `validate_unique` report ambiguity.

Arrays use physical element order when decoded. A liberal decoder accepts the
BSON corpus's noncanonical array field names, including empty, nonnumeric, and
duplicate indexes. Re-encoding writes canonical `"0"`, `"1"`, and subsequent
indexes.

## Encoding and decoding

`encode_document` emits one complete BSON document. `decode_document` consumes
one exact byte slice and rejects trailing data. The option-taking forms control
duplicate fields, UUID conversion, and the conservative decoded-allocation
budget without changing structural validation. `BSON_MAX_DECODED_BYTES` sets
the default and hard maximum for that retained-heap budget at 64 MiB.

The codec preserves field order, integer width, double bits, binary subtype,
ObjectId bytes, signed datetime milliseconds, Timestamp components, the raw
Decimal128 BID payload, and the distinction between JavaScript and JavaScript
with scope. A canonical BSON document therefore satisfies
`encode_document(decode_document(bytes)) == bytes`. The two BSON-corpus
degenerate forms deliberately normalize on re-encode: array indexes become
sequential and regular-expression options become ordered.

All wire integers and lengths use BSON's little-endian representation. A
Timestamp stores the increment first and seconds second on the wire, but its
public value and comparison key are `(seconds, increment)`. Legacy binary
subtype 2 contains an inner length; the decoder verifies it equals the outer
payload length minus four and exposes only the user bytes.

Strings and JavaScript source use an explicit length and may contain NUL.
Document keys and regular-expression pattern/options are C strings and cannot.
Every text value must be valid UTF-8. JavaScript with scope has a minimum total
length of 14 bytes, and its total, source-string, and scope-document lengths
must agree exactly with their enclosing frame.

### UUIDs

The frozen TinyMongo contract uses standard UUIDs: RFC-4122 byte order in
binary subtype 4. A standard UUID and subtype-4 binary with the same 16 bytes
have the same equality, hash, and comparison identity.

Raw BSON does not distinguish an application UUID from binary. With unspecified
UUID representation the decoder therefore returns `Binary`. With a configured
representation, a matching 16-byte binary value is materialized as `Uuid` and
re-encoding produces the same bytes. `UuidRepresentation` supports Standard
(subtype 4, RFC bytes), Python legacy (subtype 3, RFC bytes), Java legacy
(subtype 3, each 8-byte half reversed), and C# legacy (subtype 3, UUID fields in
little-endian order). Equality, hashing, and ordering use the exact encoded
binary pair `(subtype, bytes)`. The same logical UUID in two representations is
therefore equal only when those representations produce the same pair. The
legacy conversions are codec utilities checked against the BSON UUID rules;
only Standard has a frozen TinyMongo parity claim.

### Datetimes

BSON datetimes are signed `i64` UTC milliseconds. Naive Python datetimes in the
reference behavior are interpreted as UTC; aware values are converted to UTC.
Sub-millisecond precision is floored, including before the epoch:

```text
1969-12-31T23:59:59.999999Z -> -1 ms
2026-01-02T08:04:05.123456Z -> ...123 ms
```

The BSON layer stores the integer and does not restrict it to the host date
library's year range. Client adapters decide whether to return a naive UTC or
timezone-aware object.

### Regular expressions

Regex identity is `(UTF-8 pattern, options)`. Accepted BSON value flags are
de-duplicated and emitted in `ilmsux` order; an unknown flag is rejected. A
native Python `IGNORECASE` pattern carries Python's implicit Unicode flag and
therefore has the same identity as BSON options `iu`, not `i`. Regex comparison
never compiles the pattern, so malformed executable patterns still have a
stable BSON value identity. The later query `$options` surface is narrower
(`imsxu`); locale `l` belongs only to BSON value identity. Query validation and
regex execution are later document-engine concerns.

## Equality, representation equality, hashing, and order

`BsonValue` equality is MongoDB value equality. `representation_eq` is the
stronger storage comparison used when a write must determine whether bytes or
wire types changed.

The numeric variants form one equality family. A finite value is converted to
a unique exact factorization: a sign, a core coefficient with every factor of
two and five removed, and independent signed exponents for two and five.
Integers, doubles, and Decimal128 values compare equal when those compact
tuples match. This makes integer `1`, double `1.0`, and Decimal128 `1.00`
equal, while double `0.1`
(`3602879701896397 / 36028797018963968`) differs from Decimal128 `0.1`
(`1 / 10`). Boolean values are not numeric. Signed numeric zeros compare equal;
all double and Decimal128 NaNs, including quiet/signaling, signed, and payload
forms, share one value identity. Negative and positive infinity remain
distinct.

Decimal128 identity is decoded directly from the 16-byte BID payload. As the
BSON Decimal128 specification requires, a finite BID coefficient above the
34-digit maximum is a noncanonical representation of zero. This follows the
BSON corpus and the pinned Rust BSON codec even where another driver chooses a
different recovery behavior for malformed BID payloads.

Hashing uses the same semantic identity as equality. In particular it must not
hash a numeric variant discriminant, raw NaN bits, Decimal128 scale, or the UUID
wrapper. Representation equality retains those differences: integer `1`,
double `1.0`, Decimal128 `1.0`, and Decimal128 `1.00` are four representations.

Ascending value order is:

```text
MinKey
Null
Numbers (NaN, -Infinity, finite values, +Infinity)
String
Document
Array
Binary and UUID
ObjectId
Boolean
DateTime
Timestamp
Regex
JavaScript
JavaScript with scope
MaxKey
```

Numbers use exact mathematical order after the special values. Binary values
compare by encoded length, subtype, then bytes; subtype 2 adds four to its user
byte length. ObjectIds compare by their 12 bytes. Timestamps compare seconds,
then increment. Regex values compare pattern, then canonical options.
JavaScript with scope compares source, then the recursive scope document.

Arrays compare recursive element keys lexicographically. Documents compare
entries in field order. For each entry, BSON type rank is compared first, then
field name, then the value within that type; a common prefix sorts before a
longer container. Field order and every duplicate occurrence are significant
for equality, hashing, and ordering.

Choosing a cursor sort key from an array is a separate query-engine rule. In
that context an empty array sorts between MinKey and Null, and a nonempty array
contributes its minimum member for ascending sort or maximum member for
descending sort. The value comparator itself always places an array in the
Array family.

## Canonical semantic keys

`CanonicalBsonKey` is the stable equality key for grouping, exact indexes, and
hashed routing. It starts with the ASCII magic `BBKY` and a big-endian `u32`
encoding version. Version 1 is exposed as `BSON_KEY_ENCODING_VERSION`.

The body recursively encodes semantic families. Equal numeric wire variants
use one compact factorized body `(sign, core coefficient, exponent-two,
exponent-five)`, every NaN uses one body, UUID uses its equivalent binary
subtype and bytes, and documents retain field order and duplicate occurrences.
The finite numeric body is bounded to 26 bytes, including its numeric tags, so
extreme Decimal128 exponents never expand into stored powers of ten. Variable
lengths and integers have one minimal representation.
`from_bytes` rejects unknown versions, unknown tags, truncated payloads,
nonminimal or noncanonical values, excess nesting, and trailing bytes.
Encoding and validation also enforce the 16 MiB
`BSON_MAX_CANONICAL_KEY_BYTES` total-key limit before growing or copying the
key buffer.

Canonical key bytes are an equality and hash encoding. Their bytewise lexical
order is not the BSON value order; callers use `BsonValue::cmp` for ranges and
sorting. This key format is separate from SQL `CanonicalIndexKey`, whose SQL
numeric, date, timestamp, and binary rules are intentionally different.

## Validation and limits

The default maximum document size is 16 MiB, the conservative retained-heap
budget for one decoded document is 64 MiB, and the maximum object/array depth
is 100. The decoded budget accounts for container slots, duplicate-detection
indexes, owned field names, strings, binary payloads, regex data, and JavaScript
source before growing those allocations. Lengths are checked against the
enclosing frame and configured limit before allocation. Encoding uses checked
arithmetic and rejects any value that cannot fit BSON's signed 32-bit length
fields.

The 100-level limit applies when the BSON codec or canonical-key validator
walks a value. The in-memory `BsonValue` constructors remain protocol-neutral
and can build deeper trees; a bounded codec or key operation rejects them
before recursively processing beyond its limit.

Malformed input returns `BsonError`; arbitrary bytes must never panic. Its
stable kinds distinguish invalid values, invalid UTF-8, unsupported types,
duplicate fields, invalid canonical keys, truncation, oversized documents,
decoded representations or canonical keys, and excess nesting. Diagnostics
identify explicit top-level length and terminator failures while malformed raw
values use fixed diagnostic detail that does not echo payloads. The separate
diagnostic path uses escaped bracket notation, may include a field-name prefix,
and is capped at 1 KiB.
Client-input `InvalidUtf8` errors map to the engine's `InvalidTextEncoding`;
stored-data errors map to data
corruption. The later Mongo wire adapter maps every BSON error except
`Oversized` to MongoDB `InvalidBSON` (code 22); document-size, decoded-budget,
canonical-key-size, and allocation failures classified as `Oversized` map to
`BSONObjectTooLarge` (code 10334). Message-framing errors may require closing
the connection instead of writing a reply.

The integration and fuzz suites cover canonical and degenerate round-trips,
the frozen comparison vectors, duplicate policy, malformed/truncated input,
size and depth limits, equality/hash/order laws, and panic-free decoding. Run
them with:

```bash
cargo test --features documents --test bson_document
cargo install cargo-fuzz
cargo fuzz run --fuzz-dir fuzz bson_codec
cargo fuzz run --fuzz-dir fuzz bson_comparison
```
