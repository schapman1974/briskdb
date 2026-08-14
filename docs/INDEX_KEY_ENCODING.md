# Canonical global-index keys

Global indexes need one answer to a deceptively hard question: when two
frontends mean the same indexed value, what exact bytes do they write?

BriskDB answers that in the protocol-neutral Rust core. HTTP, PostgreSQL,
Python, the embedded library, and future protocols convert inputs to core
values and call the same encoder. No adapter owns a second key format.

This codec is separate from the older shard-routing key encoding. Routing
version 1 must preserve its already-persisted raw-byte behavior; global-index
key encoding version 1 is tagged, compound-safe, order-preserving, and exposed
as `CanonicalIndexKey`.

## Status

The codec is implemented and public. Global-index catalog and storage work
starts in issue #228, so version 1 keys are not persisted by the current
release and this change does not alter `manifest.sqlite` or shard files.

`CanonicalIndexKey::from_bytes` rejects unknown versions, malformed framing,
unknown tags, invalid UTF-8, and noncanonical float representations. Its debug
output redacts all key bytes.

## Supported values

| Logical value | Version 1 behavior |
| --- | --- |
| NULL | Encoded with explicit NULL placement metadata |
| Boolean | `false` before `true` |
| Signed/unsigned 64-bit integer | Separate typed domains; big-endian order-preserving payload |
| 64-bit float | Numeric total order described below |
| Text | Exact UTF-8, SQLite `BINARY` collation only |
| Binary | Exact bytes |
| Date | Signed days from `1970-01-01` |
| Timestamp | Signed microseconds from the Unix epoch after time-zone normalization |

Decimal keys are explicitly unsupported in version 1. `InvalidText` fails as
`InvalidTextEncoding`; a non-`BINARY` collation fails as `Unsupported`. This is
intentional: silently rounding a decimal or approximating a collation would
make uniqueness unsafe.

## Ordering and NULLs

Every component records ascending/descending direction and NULLS FIRST/LAST.
The convenience constructors match SQLite defaults:

- `IndexKeyPart::ascending`: ASC NULLS FIRST;
- `IndexKeyPart::descending`: DESC NULLS LAST.

Descending components complement their type and value bytes, reversing value
order without changing the requested NULL placement. Range-order guarantees
apply to keys with the same component count, declared types, directions,
NULL placement, and collation—the normal shape of one index. Type tags provide
collision separation and a deterministic mixed-type order, but do not promise
SQLite's dynamic cross-type comparison rules.

General secondary indexes encode NULL normally. Unique reservations must also
choose `UniqueNullSemantics`:

- `Distinct` returns no reservation key if any component is NULL, matching
  ordinary SQLite UNIQUE behavior;
- `NotDistinct` encodes NULL, allowing a future `NULLS NOT DISTINCT` policy to
  reserve it like any other value.

All components are validated even when `Distinct` ultimately returns no key.

## Frozen version 1 format

All integers in the format are big-endian.

```text
key := "BIDX" | u32(version = 1) | component+

component := options | collation | null-rank | [type-tag | payload]
```

The component header is:

| Byte | Meaning |
| --- | --- |
| `0x10 + flags` | bit 0 = descending, bit 1 = NULLS LAST; other bits are invalid |
| `0x01` | `BINARY` collation |
| `0x00` or `0x01` | low/high NULL rank selected from the recorded NULL policy |

NULL ends after the rank. A non-NULL component adds one tag and its payload:

| Tag | Payload |
| --- | --- |
| `0x10` | Boolean byte `0` or `1` |
| `0x20` | `i64` bits with the sign bit flipped |
| `0x21` | Plain `u64` |
| `0x30` | Ordered float bits |
| `0x40` | `i32` date bits with the sign bit flipped |
| `0x41` | `i64` timestamp bits with the sign bit flipped |
| `0x50` | Escaped UTF-8 followed by a terminator |
| `0x51` | Escaped binary bytes followed by a terminator |

Variable payloads escape byte `00` as `00 ff` and end with `00 00`. That makes
embedded zero bytes and compound boundaries unambiguous while preserving
bytewise order. For descending components, every tag/payload byte—including
escape and terminator bytes—is complemented.

Float encoding canonicalizes both signed zeros to positive zero and all NaN
sign/payload variants to one quiet NaN. The order is:

```text
-infinity < finite negatives < zero < finite positives < +infinity < NaN
```

## Public API

```rust
use briskdb::{
    CanonicalIndexKey, IndexKeyPart, IndexKeyValueRef, UniqueNullSemantics,
};

let parts = [
    IndexKeyPart::ascending(IndexKeyValueRef::Text("tenant-a")),
    IndexKeyPart::descending(IndexKeyValueRef::Timestamp(1_700_000_000_000_000)),
];

let key = CanonicalIndexKey::encode(&parts)?;
let decoded = CanonicalIndexKey::from_bytes(key.as_bytes())?;
assert_eq!(decoded, key);

let reservation = CanonicalIndexKey::encode_unique(
    &parts,
    UniqueNullSemantics::Distinct,
)?;
# Ok::<(), briskdb::EngineError>(())
```

`CanonicalIndexKey::encode_values` is the zero-copy convenience path for the
current core `Value` variants. Date and timestamp callers use explicit
`IndexKeyValueRef` variants until those logical types are added to the shared
query-value system.

## Verification

Golden vectors freeze the bytes across architectures. Property tests cover
signed/unsigned order, binary/text escaping, compound tuple order, round trips,
type separation, NULL policies, and float edge cases. Random adversarial byte
streams exercise the decoder and prove it never panics. Frontend tests feed
HTTP JSON, PostgreSQL text/binary parameters, and Python objects through their
real conversion paths and compare the result with direct Rust core values.
