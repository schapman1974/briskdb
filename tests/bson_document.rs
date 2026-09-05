#![cfg(feature = "documents")]

use std::{
    cmp::Ordering,
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use briskdb::EngineErrorKind;
use briskdb::document::{
    BSON_KEY_ENCODING_VERSION, BSON_MAX_DOCUMENT_BYTES, BsonBinary, BsonCodecOptions, BsonDateTime,
    BsonDecimal128, BsonDocument, BsonError, BsonErrorContext, BsonErrorKind, BsonJavaScript,
    BsonObjectId, BsonRegex, BsonTimestamp, BsonUuid, BsonValue, CanonicalBsonKey,
    DuplicateFieldPolicy, UuidRepresentation, decode_document, decode_document_with_options,
    encode_document, encode_document_with_options,
};
use proptest::prelude::*;

fn hex_bytes(encoded: &str) -> Vec<u8> {
    assert_eq!(encoded.len() % 2, 0);
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

fn one_value(encoded: &str) -> BsonValue {
    let bytes = hex_bytes(encoded);
    let document = decode_document(&bytes).unwrap();
    assert_eq!(document.len(), 1);
    document.iter().next().unwrap().1.clone()
}

fn semantic_hash(value: &BsonValue) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn official_canonical_bson_vectors_round_trip_exactly() {
    // Selected from the accepted MongoDB BSON corpus. Together these exercise
    // every TinyMongo-supported framing shape and representation-sensitive
    // value without introducing Extended JSON as an intermediate form.
    let vectors = [
        "0500000000",                                                         // empty document
        "10000000016400120000000000F87F00", // double NaN with payload
        "190000000261000D0000006162006261620062616261620000", // string with NULs
        "0D000000037800050000000000",       // nested empty document
        "140000000461000C0000001030000A0000000000", // one-element array
        "13000000057800060000000202000000FFFF00", // old binary subtype 2
        "1400000007610056E1FC72E0C917E9C471416100", // ObjectId
        "090000000862000100",               // Boolean true
        "10000000096100C33CE7B9BDFFFFFF00", // negative UTC datetime
        "1000000011610000286BEE00286BEE00", // unsigned timestamp components
        "100000000B610061626300696D780000", // canonical regex options
        "190000000D61000D0000006162006261620062616261620000", // code with NULs
        "210000000F6100190000000500000061626364000C000000107800010000000000", // scoped code
        "0C0000001069000000008000",         // int32 minimum
        "10000000126100FFFFFFFFFFFFFF7F00", // int64 maximum
        "180000001364001200000000000000000000000000007E00", // Decimal128 NaN payload
        "08000000FF610000",                 // MinKey
        "080000007F610000",                 // MaxKey
    ];

    for vector in vectors {
        let bytes = hex_bytes(vector);
        let decoded = decode_document(&bytes).unwrap();
        let encoded = encode_document(&decoded).unwrap();
        assert_eq!(encoded, bytes, "failed BSON corpus vector {vector}");

        let decoded_again = decode_document(&encoded).unwrap();
        assert!(decoded.representation_eq(&decoded_again));
    }
}

#[test]
fn degenerate_arrays_and_regex_options_reencode_canonically() {
    let cases = [
        (
            // The two physical array fields both say "0". Array position is
            // determined by element order and is re-emitted as "0", "1".
            "1B000000046100130000001030000A000000103000140000000000",
            "1B000000046100130000001030000A000000103100140000000000",
        ),
        (
            // BSON corpus degenerate input has options "mix"; canonical is
            // alphabetic "imx".
            "100000000B6100616263006D69780000",
            "100000000B610061626300696D780000",
        ),
    ];

    for (degenerate, canonical) in cases {
        let decoded = decode_document(&hex_bytes(degenerate)).unwrap();
        assert_eq!(encode_document(&decoded).unwrap(), hex_bytes(canonical));
    }
}

#[test]
fn duplicate_fields_are_rejected_by_default_and_preserved_explicitly() {
    // {"a": int32(1), "a": int32(2)}. BSON itself can carry both entries,
    // even though MongoDB query/storage semantics require unique field names.
    let bytes = hex_bytes("13000000106100010000001061000200000000");
    assert_eq!(
        decode_document(&bytes).unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );

    let options =
        BsonCodecOptions::default().with_duplicate_field_policy(DuplicateFieldPolicy::Preserve);
    let document = decode_document_with_options(&bytes, &options).unwrap();
    assert_eq!(document.len(), 2);
    assert_eq!(document.get_first("a"), Some(&BsonValue::Int32(1)));
    assert_eq!(document.get_last("a"), Some(&BsonValue::Int32(2)));
    assert_eq!(document.get_all("a").count(), 2);
    assert_eq!(
        document.get_unique("a").unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );
    assert_eq!(
        document.validate_unique().unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );

    assert_eq!(
        encode_document(&document).unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );
    assert_eq!(
        encode_document_with_options(&document, &options).unwrap(),
        bytes
    );
}

#[test]
fn duplicate_policy_applies_recursively_inside_arrays() {
    // {"a": [{"x": int32(1), "x": int32(2)}]}
    let bytes = hex_bytes("230000000461001B000000033000130000001078000100000010780002000000000000");
    assert_eq!(
        decode_document(&bytes).unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );

    let options =
        BsonCodecOptions::default().with_duplicate_field_policy(DuplicateFieldPolicy::Preserve);
    let document = decode_document_with_options(&bytes, &options).unwrap();
    let Some(BsonValue::Array(values)) = document.get_unique("a").unwrap() else {
        panic!("expected nested array")
    };
    let BsonValue::Document(nested) = &values[0] else {
        panic!("expected nested document")
    };
    assert_eq!(nested.get_all("x").count(), 2);
    assert_eq!(
        encode_document(&document).unwrap_err().kind(),
        BsonErrorKind::DuplicateField
    );
    assert_eq!(
        encode_document_with_options(&document, &options).unwrap(),
        bytes
    );
}

#[test]
fn numeric_equality_hashing_order_and_representation_follow_frozen_vectors() {
    let integer = one_value("0C0000001069000100000000");
    let long = one_value("10000000126100010000000000000000");
    let double = one_value("10000000016400000000000000F03F00");
    let decimal = one_value("1800000013640064000000000000000000000000003C3000");

    for value in [&long, &double, &decimal] {
        assert_eq!(&integer, value);
        assert_eq!(integer.cmp(value), Ordering::Equal);
        assert_eq!(semantic_hash(&integer), semantic_hash(value));
        assert_eq!(
            CanonicalBsonKey::encode(&integer).unwrap(),
            CanonicalBsonKey::encode(value).unwrap()
        );
        assert!(!integer.representation_eq(value));
    }

    let double_tenth = one_value("100000000164009A9999999999B93F00");
    let decimal_tenth = one_value("1800000013640001000000000000000000000000003E3000");
    assert_ne!(double_tenth, decimal_tenth);
    assert_ne!(
        CanonicalBsonKey::encode(&double_tenth).unwrap(),
        CanonicalBsonKey::encode(&decimal_tenth).unwrap()
    );

    let mut identities = HashSet::new();
    identities.extend([integer, long, double, decimal]);
    assert_eq!(identities.len(), 1);
}

#[test]
fn all_nan_forms_share_one_semantic_identity_but_keep_wire_bits() {
    let double_nan = one_value("10000000016400000000000000F87F00");
    let double_payload = one_value("10000000016400120000000000F87F00");
    let decimal_nan = one_value("180000001364000000000000000000000000000000007C00");
    let decimal_snan_payload = one_value("180000001364001200000000000000000000000000007E00");

    let values = [
        double_nan.clone(),
        double_payload.clone(),
        decimal_nan.clone(),
        decimal_snan_payload.clone(),
    ];
    for left in &values {
        for right in &values {
            assert_eq!(left, right);
            assert_eq!(left.cmp(right), Ordering::Equal);
            assert_eq!(semantic_hash(left), semantic_hash(right));
            assert_eq!(
                CanonicalBsonKey::encode(left).unwrap(),
                CanonicalBsonKey::encode(right).unwrap()
            );
        }
    }
    assert!(!double_nan.representation_eq(&double_payload));
    assert!(!decimal_nan.representation_eq(&decimal_snan_payload));
    assert!(double_nan < BsonValue::Double(f64::NEG_INFINITY));
}

#[test]
fn complete_tinymongo_supported_value_order_is_stable() {
    let expected = vec![
        ("minimum", one_value("08000000FF610000")),
        ("null", one_value("080000000A610000")),
        ("number", one_value("0C0000001069000100000000")),
        ("string", one_value("0E00000002610002000000620000")),
        ("object", one_value("0D000000037800050000000000")),
        ("array", one_value("0D000000046100050000000000")),
        ("binary", one_value("0F0000000578000200000000FFFF00")),
        (
            "object-id",
            one_value("1400000007610000000000000000000000000000"),
        ),
        ("boolean", one_value("090000000862000000")),
        ("date", one_value("10000000096100000000000000000000")),
        ("timestamp", one_value("100000001161002A00000015CD5B0700")),
        ("regex", one_value("0B0000000B610078000000")),
        ("code", one_value("0E0000000D610002000000620000")),
        (
            "scoped-code",
            one_value("1A0000000F610012000000050000006162636400050000000000"),
        ),
        ("maximum", one_value("080000007F610000")),
    ];
    let mut reversed = expected.clone();
    reversed.reverse();
    reversed.sort_by(|left, right| left.1.cmp(&right.1));
    assert_eq!(
        reversed.iter().map(|(label, _)| *label).collect::<Vec<_>>(),
        expected.iter().map(|(label, _)| *label).collect::<Vec<_>>()
    );
}

#[test]
fn binary_subtype_two_uses_encoded_length() {
    let values = [
        BsonBinary::new(0, b"a".to_vec()),
        BsonBinary::new(4, b"a".to_vec()),
        BsonBinary::new(0, b"aa".to_vec()),
        BsonBinary::new(2, b"a".to_vec()),
    ];
    assert!(values.windows(2).all(|window| window[0] < window[1]));
}

#[test]
fn documents_compare_value_type_before_field_name_and_preserve_order() {
    let numeric_z = BsonDocument::from_entries([("z", BsonValue::Int32(0))]).unwrap();
    let string_a = BsonDocument::from_entries([("a", BsonValue::String("text".into()))]).unwrap();
    assert!(numeric_z < string_a);

    let first =
        BsonDocument::from_entries([("a", BsonValue::Int32(1)), ("b", BsonValue::Int32(2))])
            .unwrap();
    let reordered =
        BsonDocument::from_entries([("b", BsonValue::Int32(2)), ("a", BsonValue::Int32(1))])
            .unwrap();
    assert_ne!(first, reordered);
}

#[test]
fn datetime_precision_and_timestamp_order_are_signed_and_exact() {
    assert_eq!(BsonDateTime::from_micros(-1).timestamp_millis(), -1);
    assert_eq!(
        BsonDateTime::from_micros(123_001),
        BsonDateTime::from_micros(123_999)
    );
    assert!(BsonDateTime::from_micros(123_999) < BsonDateTime::from_micros(124_000));

    let decoded = one_value("100000001161002A00000015CD5B0700");
    let BsonValue::Timestamp(timestamp) = decoded else {
        panic!("expected BSON Timestamp")
    };
    assert_eq!(timestamp.time(), 123_456_789);
    assert_eq!(timestamp.increment(), 42);
}

#[test]
fn uuid_representations_map_to_their_exact_binary_identity() {
    let bytes: [u8; 16] = hex_bytes("00112233445566778899AABBCCDDEEFF")
        .try_into()
        .unwrap();
    let cases = [
        (
            UuidRepresentation::Standard,
            4,
            "00112233445566778899AABBCCDDEEFF",
        ),
        (
            UuidRepresentation::PythonLegacy,
            3,
            "00112233445566778899AABBCCDDEEFF",
        ),
        (
            UuidRepresentation::JavaLegacy,
            3,
            "7766554433221100FFEEDDCCBBAA9988",
        ),
        (
            UuidRepresentation::CSharpLegacy,
            3,
            "33221100554477668899AABBCCDDEEFF",
        ),
    ];

    for (representation, subtype, encoded) in cases {
        let uuid = BsonUuid::new(bytes, representation);
        let binary = uuid.to_binary();
        assert_eq!(binary.subtype(), subtype);
        let expected_bytes = hex_bytes(encoded);
        assert_eq!(binary.bytes(), expected_bytes.as_slice());

        let uuid_value = BsonValue::Uuid(uuid);
        let binary_value = BsonValue::Binary(binary.clone());
        assert_eq!(uuid_value, binary_value);
        assert_eq!(uuid_value.cmp(&binary_value), Ordering::Equal);
        assert_eq!(semantic_hash(&uuid_value), semantic_hash(&binary_value));
        assert!(!uuid_value.representation_eq(&binary_value));
        assert_eq!(
            BsonUuid::from_binary(&binary, representation)
                .unwrap()
                .bytes(),
            bytes
        );
    }

    let raw = hex_bytes("1D000000057800100000000400112233445566778899AABBCCDDEEFF00");
    assert!(matches!(
        decode_document(&raw).unwrap().get_unique("x").unwrap(),
        Some(BsonValue::Binary(_))
    ));
    let options =
        BsonCodecOptions::default().with_uuid_representation(Some(UuidRepresentation::Standard));
    let decoded = decode_document_with_options(&raw, &options).unwrap();
    assert!(matches!(
        decoded.get_unique("x").unwrap(),
        Some(BsonValue::Uuid(_))
    ));
    assert_eq!(encode_document(&decoded).unwrap(), raw);
}

#[test]
fn configured_uuid_decoding_checks_subtype_and_width() {
    let options =
        BsonCodecOptions::default().with_uuid_representation(Some(UuidRepresentation::Standard));

    let short =
        BsonDocument::from_entries([("x", BsonValue::Binary(BsonBinary::new(4, vec![0; 15])))])
            .unwrap();
    let short = encode_document(&short).unwrap();
    assert_eq!(
        decode_document_with_options(&short, &options)
            .unwrap_err()
            .kind(),
        BsonErrorKind::InvalidValue
    );

    let legacy =
        BsonDocument::from_entries([("x", BsonValue::Binary(BsonBinary::new(3, vec![0; 16])))])
            .unwrap();
    let legacy = encode_document(&legacy).unwrap();
    assert!(matches!(
        decode_document_with_options(&legacy, &options)
            .unwrap()
            .get_unique("x")
            .unwrap(),
        Some(BsonValue::Binary(_))
    ));
}

#[test]
fn regex_and_code_keep_their_bson_families() {
    let regex = BsonRegex::new("same", "miim").unwrap();
    assert_eq!(regex.pattern(), "same");
    assert_eq!(regex.options(), "im");
    assert_eq!(
        BsonRegex::new("same", "iz").unwrap_err().kind(),
        BsonErrorKind::InvalidValue
    );
    assert_eq!(
        BsonRegex::new("nul\0pattern", "").unwrap_err().kind(),
        BsonErrorKind::InvalidValue
    );

    let code = BsonValue::JavaScript(BsonJavaScript::new("same"));
    let text = BsonValue::String("same".into());
    let scoped = BsonValue::JavaScript(BsonJavaScript::with_scope(
        "same",
        BsonDocument::from_entries([("answer", BsonValue::Int32(1))]).unwrap(),
    ));
    assert_ne!(code, text);
    assert_ne!(code, scoped);
    assert!(code < scoped);
}

#[test]
fn canonical_key_has_a_frozen_header_and_validates_versions() {
    let key = CanonicalBsonKey::encode(&BsonValue::Null).unwrap();
    assert_eq!(key.as_bytes(), hex_bytes("42424B590000000101").as_slice());
    assert_eq!(key.encoding_version(), BSON_KEY_ENCODING_VERSION);
    assert_eq!(&key.as_bytes()[..4], b"BBKY");
    assert_eq!(
        &key.as_bytes()[4..8],
        &BSON_KEY_ENCODING_VERSION.to_be_bytes()
    );
    assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);

    let zero = CanonicalBsonKey::encode(&BsonValue::Int32(0)).unwrap();
    assert_eq!(
        zero.as_bytes(),
        hex_bytes("42424B59000000010202000000000000000000").as_slice()
    );

    for length in 0..key.as_bytes().len() {
        assert!(CanonicalBsonKey::from_bytes(&key.as_bytes()[..length]).is_err());
    }
    let mut future = key.as_bytes().to_vec();
    future[4..8].copy_from_slice(&(BSON_KEY_ENCODING_VERSION + 1).to_be_bytes());
    assert_eq!(
        CanonicalBsonKey::from_bytes(&future).unwrap_err().kind(),
        BsonErrorKind::InvalidCanonicalKey
    );

    let mut bad_magic = key.as_bytes().to_vec();
    bad_magic[0] ^= 0xff;
    let mut unknown_tag = key.as_bytes().to_vec();
    unknown_tag[8] = 0xff;
    let mut trailing = key.as_bytes().to_vec();
    trailing.push(0);
    let noncanonical_zero = hex_bytes("42424B59000000010202000000000000000001");
    for invalid in [bad_magic, unknown_tag, trailing, noncanonical_zero] {
        assert_eq!(
            CanonicalBsonKey::from_bytes(&invalid).unwrap_err().kind(),
            BsonErrorKind::InvalidCanonicalKey
        );
    }
}

#[test]
fn canonical_key_v1_comprehensive_golden_is_stable() {
    let nested = BsonDocument::from_entries([("v", BsonValue::Int32(1))]).unwrap();
    let scope = BsonDocument::from_entries([("q", BsonValue::Int64(1))]).unwrap();
    let document = BsonDocument::from_entries([
        ("min", BsonValue::MinKey),
        ("null", BsonValue::Null),
        ("zero", BsonValue::Int32(0)),
        ("pos", BsonValue::Int64(1)),
        ("neg", BsonValue::Int32(-2)),
        ("frac", BsonValue::Double(0.5)),
        (
            "dec",
            BsonValue::Decimal128(BsonDecimal128::parse("0.3").unwrap()),
        ),
        ("nan", BsonValue::Double(f64::NAN)),
        ("ninf", BsonValue::Double(f64::NEG_INFINITY)),
        ("pinf", BsonValue::Double(f64::INFINITY)),
        ("str", BsonValue::String("x".into())),
        ("doc", BsonValue::Document(nested)),
        (
            "arr",
            BsonValue::Array(vec![BsonValue::Boolean(false), BsonValue::Null]),
        ),
        ("bin", BsonValue::Binary(BsonBinary::new(2, [0x00, 0xff]))),
        (
            "uuid",
            BsonValue::Uuid(BsonUuid::new([0x11; 16], UuidRepresentation::Standard)),
        ),
        (
            "oid",
            BsonValue::ObjectId(BsonObjectId::from_bytes([0x22; 12])),
        ),
        ("bool", BsonValue::Boolean(true)),
        ("date", BsonValue::DateTime(BsonDateTime::from_millis(-1))),
        ("ts", BsonValue::Timestamp(BsonTimestamp::new(2, 3))),
        (
            "re",
            BsonValue::RegularExpression(BsonRegex::new("a", "mi").unwrap()),
        ),
        ("js", BsonValue::JavaScript(BsonJavaScript::new("x"))),
        (
            "scope",
            BsonValue::JavaScript(BsonJavaScript::with_scope("y", scope)),
        ),
        ("max", BsonValue::MaxKey),
    ])
    .unwrap();

    let key = CanonicalBsonKey::encode(&BsonValue::Document(document)).unwrap();
    let expected = hex_bytes(concat!(
        "42424B59000000010400000017000000036D696E00000000046E756C6C01000000047A65726F020200000000000000000000",
        "000003706F73020201000000010100000000000000036E656702020200000001010001000000000004667261630202010000",
        "000101FFFF0000000000036465630202010000000103FFFFFFFF000000036E616E0200000000046E696E6602010000000470",
        "696E6602030000000373747203000000017800000003646F6304000000010000000176020201000000010100000000000000",
        "0361727205000000020800010000000362696E06020000000200FF0000000475756964060400000010111111111111111111",
        "11111111111111000000036F69640722222222222222222222222200000004626F6F6C0801000000046461746509FFFFFFFF",
        "FFFFFFFF0000000274730A00000002000000030000000272650B000000016100000002696D000000026A730C000000017800",
        "00000573636F70650D0000000179000000010000000171020201000000010100000000000000036D61780E",
    ));
    assert_eq!(expected.len(), 393);
    assert_eq!(key.as_bytes(), expected.as_slice());
    assert_eq!(CanonicalBsonKey::from_bytes(&expected).unwrap(), key);
}

#[test]
fn invalid_truncated_unsupported_and_oversized_bson_fail_safely() {
    let invalid_vectors = [
        "0100000000",                   // declared length below BSON's five-byte minimum
        "05000000",                     // missing final EOO
        "0500000001",                   // nonzero EOO
        "07000000800000",               // invalid type tag
        "0E00000002610002000000E90000", // invalid string UTF-8
        "090000000862000200",           // invalid Boolean byte
        "13000000057800060000000203000000FFFF00", // subtype-2 inner length mismatch
        "160000000F61000D0000000100000000050000000000", // short code-with-scope
    ];
    for vector in invalid_vectors {
        assert!(
            decode_document(&hex_bytes(vector)).is_err(),
            "accepted invalid BSON corpus vector {vector}"
        );
    }

    assert_eq!(
        decode_document(&hex_bytes("1200000002666F6F0004000000626172"))
            .unwrap_err()
            .kind(),
        BsonErrorKind::Truncated
    );
    assert_eq!(
        decode_document(&hex_bytes("0800000006610000"))
            .unwrap_err()
            .kind(),
        BsonErrorKind::UnsupportedType
    );
    assert_eq!(
        decode_document(&hex_bytes("0E00000002610002000000E90000"))
            .unwrap_err()
            .kind(),
        BsonErrorKind::InvalidUtf8
    );
    assert_eq!(
        decode_document(&hex_bytes("0C00000010E9000100000000"))
            .unwrap_err()
            .kind(),
        BsonErrorKind::InvalidUtf8
    );

    for unsupported in [
        "0800000006610000",                                     // Undefined
        "0E0000000E610002000000780000",                         // Symbol("x")
        "1A0000000C610002000000780000000000000000000000000000", // DBPointer("x", nil)
    ] {
        assert_eq!(
            decode_document(&hex_bytes(unsupported)).unwrap_err().kind(),
            BsonErrorKind::UnsupportedType
        );
    }

    let declared = BSON_MAX_DOCUMENT_BYTES + 1;
    let oversized_header = i32::try_from(declared).unwrap().to_le_bytes();
    let error = decode_document(&oversized_header).unwrap_err();
    assert_eq!(error.kind(), BsonErrorKind::Oversized);
    assert_eq!(error.mongo_code(), 10_334);
}

#[test]
fn bson_errors_have_stable_codes_and_contextual_engine_mappings() {
    let kinds = [
        (BsonErrorKind::InvalidValue, "invalid_value", 22),
        (BsonErrorKind::InvalidUtf8, "invalid_utf8", 22),
        (BsonErrorKind::UnsupportedType, "unsupported_type", 22),
        (BsonErrorKind::DuplicateField, "duplicate_field", 22),
        (
            BsonErrorKind::InvalidCanonicalKey,
            "invalid_canonical_key",
            22,
        ),
        (BsonErrorKind::Truncated, "truncated", 22),
        (BsonErrorKind::Oversized, "oversized", 10_334),
        (BsonErrorKind::NestingLimit, "nesting_limit", 22),
    ];
    for (kind, code, mongo_code) in kinds {
        let error = BsonError::new(kind, "fixture");
        assert_eq!(error.code(), code);
        assert_eq!(error.mongo_code(), mongo_code);
        assert_eq!(kind.code(), code);
        assert_eq!(kind.mongo_code(), mongo_code);
    }

    let client_cases = [
        (
            BsonErrorKind::InvalidUtf8,
            EngineErrorKind::InvalidTextEncoding,
        ),
        (BsonErrorKind::Oversized, EngineErrorKind::LimitExceeded),
        (
            BsonErrorKind::InvalidValue,
            EngineErrorKind::InvalidArgument,
        ),
        (BsonErrorKind::UnsupportedType, EngineErrorKind::Unsupported),
    ];
    for (kind, expected) in client_cases {
        let error =
            BsonError::new(kind, "fixture").into_engine_error(BsonErrorContext::ClientInput);
        assert_eq!(error.kind(), expected);
    }

    let stored = BsonError::new(BsonErrorKind::InvalidUtf8, "fixture")
        .into_engine_error(BsonErrorContext::StoredData);
    assert_eq!(stored.kind(), EngineErrorKind::DataCorruption);
}

fn nested_document(depth: usize) -> BsonDocument {
    assert!(depth >= 1);
    let mut document = BsonDocument::from_entries([("value", BsonValue::Int32(1))]).unwrap();
    for _ in 1..depth {
        document = BsonDocument::from_entries([("nested", BsonValue::Document(document))]).unwrap();
    }
    document
}

#[test]
fn nesting_limit_counts_the_root_and_each_container() {
    let accepted = nested_document(100);
    let bytes = encode_document(&accepted).unwrap();
    assert!(decode_document(&bytes).is_ok());

    // Wrap the accepted 100-level document without calling the bounded
    // encoder, producing valid raw BSON whose new root is level 1 and whose
    // deepest document is level 101.
    let wrapped_len = 4usize
        .checked_add(1)
        .and_then(|length| length.checked_add(b"nested\0".len()))
        .and_then(|length| length.checked_add(bytes.len()))
        .and_then(|length| length.checked_add(1))
        .unwrap();
    let mut wrapped = Vec::with_capacity(wrapped_len);
    wrapped.extend_from_slice(&i32::try_from(wrapped_len).unwrap().to_le_bytes());
    wrapped.push(0x03);
    wrapped.extend_from_slice(b"nested\0");
    wrapped.extend_from_slice(&bytes);
    wrapped.push(0);
    assert_eq!(wrapped.len(), wrapped_len);
    assert_eq!(
        decode_document(&wrapped).unwrap_err().kind(),
        BsonErrorKind::NestingLimit
    );

    let rejected = nested_document(101);
    assert_eq!(
        encode_document(&rejected).unwrap_err().kind(),
        BsonErrorKind::NestingLimit
    );
}

fn arb_bson_value() -> impl Strategy<Value = BsonValue> {
    let leaf = prop_oneof![
        Just(BsonValue::Null),
        any::<bool>().prop_map(BsonValue::Boolean),
        any::<i32>().prop_map(BsonValue::Int32),
        any::<i64>().prop_map(BsonValue::Int64),
        any::<u64>().prop_map(|bits| BsonValue::Double(f64::from_bits(bits))),
        "[a-zA-Z0-9]{0,20}".prop_map(BsonValue::String),
        prop::collection::vec(any::<u8>(), 0..24)
            .prop_map(|bytes| BsonValue::Binary(BsonBinary::new(0, bytes))),
    ];

    leaf.prop_recursive(4, 64, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(BsonValue::Array),
            prop::collection::vec(inner, 0..4).prop_map(|values| {
                BsonValue::Document(
                    BsonDocument::from_entries(
                        values
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| (format!("field-{index}"), value)),
                    )
                    .unwrap(),
                )
            }),
        ]
    })
}

proptest! {
    #[test]
    fn arbitrary_small_inputs_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_document(&bytes);
    }

    #[test]
    fn generated_values_round_trip_without_representation_loss(value in arb_bson_value()) {
        let document = BsonDocument::from_entries([("value", value.clone())]).unwrap();
        let encoded = encode_document(&document).unwrap();
        let decoded = decode_document(&encoded).unwrap();
        let restored = decoded.get_unique("value").unwrap().unwrap();
        prop_assert!(value.representation_eq(restored));
    }

    #[test]
    fn equality_hash_and_order_obey_their_laws(
        left in arb_bson_value(),
        middle in arb_bson_value(),
        right in arb_bson_value(),
    ) {
        prop_assert_eq!(left == middle, middle == left);
        prop_assert_eq!(left.cmp(&middle), middle.cmp(&left).reverse());
        prop_assert_eq!(left.cmp(&middle) == Ordering::Equal, left == middle);
        if left == middle {
            prop_assert_eq!(semantic_hash(&left), semantic_hash(&middle));
        }
        if left <= middle && middle <= right {
            prop_assert!(left <= right);
        }
        prop_assert!(left.representation_eq(&left));
    }

    #[test]
    fn canonical_keys_parse_idempotently(value in arb_bson_value()) {
        let key = CanonicalBsonKey::encode(&value).unwrap();
        let parsed = CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap();
        prop_assert_eq!(parsed, key);
    }
}
