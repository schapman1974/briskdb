use super::*;

pub(super) fn values() -> Vec<(&'static str, BsonValue)> {
    let document = |entries| BsonDocument::from_entries(entries).unwrap();
    let uuid = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    vec![
        ("min", BsonValue::MinKey),
        ("null", BsonValue::Null),
        ("zero", BsonValue::Int32(0)),
        ("one", BsonValue::Int32(1)),
        ("negative", BsonValue::Int32(-2)),
        ("large_integer", BsonValue::Int64(9_007_199_254_740_993)),
        ("fraction", BsonValue::Double(0.5)),
        (
            "decimal",
            BsonValue::Decimal128(BsonDecimal128::parse("0.3").unwrap()),
        ),
        (
            "nan",
            BsonValue::Double(f64::from_bits(0x7ff8_0000_0000_0012)),
        ),
        ("negative_infinity", BsonValue::Double(f64::NEG_INFINITY)),
        ("positive_infinity", BsonValue::Double(f64::INFINITY)),
        ("string", BsonValue::from("x")),
        ("nul_unicode", BsonValue::from("a\0é")),
        (
            "object",
            BsonValue::Document(document([
                ("a", BsonValue::Int32(1)),
                ("b", BsonValue::Int32(2)),
            ])),
        ),
        (
            "reordered_object",
            BsonValue::Document(document([
                ("b", BsonValue::Int32(2)),
                ("a", BsonValue::Int32(1)),
            ])),
        ),
        (
            "array",
            BsonValue::Array(vec![BsonValue::Boolean(false), BsonValue::Null]),
        ),
        ("empty_array", BsonValue::Array(vec![])),
        ("binary", BsonValue::Binary(BsonBinary::new(0, [0, 255]))),
        (
            "old_binary",
            BsonValue::Binary(BsonBinary::new(2, [0, 255])),
        ),
        (
            "uuid_standard",
            BsonValue::Uuid(BsonUuid::new(uuid, UuidRepresentation::Standard)),
        ),
        (
            "uuid_python",
            BsonValue::Uuid(BsonUuid::new(uuid, UuidRepresentation::PythonLegacy)),
        ),
        (
            "uuid_java",
            BsonValue::Uuid(BsonUuid::new(uuid, UuidRepresentation::JavaLegacy)),
        ),
        (
            "uuid_csharp",
            BsonValue::Uuid(BsonUuid::new(uuid, UuidRepresentation::CSharpLegacy)),
        ),
        (
            "object_id",
            BsonValue::ObjectId(BsonObjectId::from_bytes([0x22; 12])),
        ),
        ("false", BsonValue::Boolean(false)),
        ("true", BsonValue::Boolean(true)),
        ("date", BsonValue::DateTime(BsonDateTime::from_millis(-1))),
        ("timestamp", BsonValue::Timestamp(BsonTimestamp::new(2, 3))),
        (
            "regex",
            BsonValue::RegularExpression(BsonRegex::new("a", "mi").unwrap()),
        ),
        ("code", BsonValue::JavaScript(BsonJavaScript::new("x"))),
        (
            "scope",
            BsonValue::JavaScript(BsonJavaScript::with_scope(
                "y",
                BsonDocument::from_entries([("q", BsonValue::Int64(1))]).unwrap(),
            )),
        ),
        ("max", BsonValue::MaxKey),
        ("one_int64_alias", BsonValue::Int64(1)),
        ("one_double_alias", BsonValue::Double(1.0)),
        (
            "one_decimal_alias",
            BsonValue::Decimal128(BsonDecimal128::parse("1.00").unwrap()),
        ),
        ("negative_zero_alias", BsonValue::Double(-0.0)),
        (
            "nan_decimal_alias",
            BsonValue::Decimal128(BsonDecimal128::parse("NaN").unwrap()),
        ),
        (
            "uuid_binary_alias",
            BsonValue::Binary(BsonBinary::new(4, uuid)),
        ),
    ]
}
