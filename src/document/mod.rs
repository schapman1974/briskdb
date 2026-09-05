//! BSON values, wire encoding, comparison, and canonical identity keys.
//!
//! This module is available with the non-default `documents` feature. It is
//! protocol-neutral: the MongoDB listener, embedded document API, and storage
//! layer all consume the same value and codec rules.

mod codec;
mod error;
mod key;
mod number;
mod value;

pub use codec::{
    BSON_MAX_DECODED_BYTES, BSON_MAX_DOCUMENT_BYTES, BSON_MAX_NESTING_DEPTH, BsonCodecOptions,
    DuplicateFieldPolicy, decode_document, decode_document_with_options, encode_document,
    encode_document_with_options,
};
pub use error::{BsonError, BsonErrorContext, BsonErrorKind, BsonResult};
pub use key::{BSON_KEY_ENCODING_VERSION, BSON_MAX_CANONICAL_KEY_BYTES, CanonicalBsonKey};
pub use value::{
    BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonJavaScript, BsonObjectId,
    BsonRegex, BsonTimestamp, BsonUuid, BsonValue, UuidRepresentation,
};
