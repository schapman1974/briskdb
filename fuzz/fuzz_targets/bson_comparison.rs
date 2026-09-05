#![no_main]

use std::{
    cmp::Ordering,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use briskdb::document::{
    BsonBinary, BsonDateTime, BsonDecimal128, BsonDocument, BsonJavaScript, BsonObjectId,
    BsonRegex, BsonTimestamp, BsonUuid, BsonValue, CanonicalBsonKey, UuidRepresentation,
};
use libfuzzer_sys::fuzz_target;

struct Input<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    fn byte(&mut self) -> u8 {
        let value = self.bytes.get(self.offset).copied().unwrap_or(0);
        self.offset = self.offset.saturating_add(1);
        value
    }

    fn fixed<const N: usize>(&mut self) -> [u8; N] {
        let mut value = [0; N];
        for byte in &mut value {
            *byte = self.byte();
        }
        value
    }

    fn short_bytes(&mut self) -> Vec<u8> {
        let length = usize::from(self.byte() % 16);
        (0..length).map(|_| self.byte()).collect()
    }

    fn text(&mut self) -> String {
        self.short_bytes()
            .into_iter()
            .map(|byte| char::from(b'a' + byte % 26))
            .collect()
    }

    fn value(&mut self, depth: usize) -> BsonValue {
        let variants = if depth == 0 { 18 } else { 22 };
        match usize::from(self.byte()) % variants {
            0 => BsonValue::MinKey,
            1 => BsonValue::Null,
            2 => BsonValue::Int32(i32::from_le_bytes(self.fixed())),
            3 => BsonValue::Int64(i64::from_le_bytes(self.fixed())),
            4 => BsonValue::Double(f64::from_bits(u64::from_le_bytes(self.fixed()))),
            5 => BsonValue::Decimal128(BsonDecimal128::from_bid(self.fixed())),
            6 => BsonValue::String(self.text()),
            7 => BsonValue::Binary(BsonBinary::new(self.byte(), self.short_bytes())),
            8 => BsonValue::ObjectId(BsonObjectId::from_bytes(self.fixed())),
            9 => BsonValue::Boolean(self.byte() & 1 == 1),
            10 => BsonValue::DateTime(BsonDateTime::from_millis(i64::from_le_bytes(self.fixed()))),
            11 => BsonValue::Timestamp(BsonTimestamp::new(
                u32::from_le_bytes(self.fixed()),
                u32::from_le_bytes(self.fixed()),
            )),
            12 => {
                const OPTIONS: [&str; 8] = ["", "i", "m", "s", "u", "x", "il", "ilmsux"];
                BsonValue::RegularExpression(
                    BsonRegex::new(
                        self.text(),
                        OPTIONS[usize::from(self.byte()) % OPTIONS.len()],
                    )
                    .unwrap(),
                )
            }
            13 => BsonValue::JavaScript(BsonJavaScript::new(self.text())),
            14 => BsonValue::MaxKey,
            15 => BsonValue::Binary(BsonBinary::new(2, self.short_bytes())),
            16 => BsonValue::Double(if self.byte() & 1 == 0 { f64::NAN } else { -0.0 }),
            17 => {
                let bytes = self.fixed();
                let representation = match self.byte() % 4 {
                    0 => UuidRepresentation::Standard,
                    1 => UuidRepresentation::PythonLegacy,
                    2 => UuidRepresentation::JavaLegacy,
                    _ => UuidRepresentation::CSharpLegacy,
                };
                BsonValue::Uuid(BsonUuid::new(bytes, representation))
            }
            18 => {
                let length = usize::from(self.byte() % 4);
                BsonValue::Array((0..length).map(|_| self.value(depth - 1)).collect())
            }
            19 => {
                let length = usize::from(self.byte() % 4);
                let entries = (0..length).map(|index| {
                    let name = format!("{}-{index}", self.text());
                    (name, self.value(depth - 1))
                });
                BsonValue::Document(BsonDocument::from_entries(entries).unwrap())
            }
            20 => {
                let scope = BsonDocument::from_entries([("x", self.value(depth - 1))]).unwrap();
                BsonValue::JavaScript(BsonJavaScript::with_scope(self.text(), scope))
            }
            _ => BsonValue::Array(vec![self.value(depth - 1)]),
        }
    }
}

fn hash(value: &BsonValue) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn check_pair(left: &BsonValue, right: &BsonValue) {
    let left_equals_right = left == right;
    let right_equals_left = right == left;
    assert_eq!(left_equals_right, right_equals_left);
    assert_eq!(left.cmp(right), right.cmp(left).reverse());
    assert_eq!(left.cmp(right) == Ordering::Equal, left_equals_right);
    if left_equals_right {
        assert_eq!(hash(left), hash(right));
        assert_eq!(
            CanonicalBsonKey::encode(left).unwrap(),
            CanonicalBsonKey::encode(right).unwrap()
        );
    }
}

fn check_uuid_binary_identity(value: &BsonValue) {
    match value {
        BsonValue::Uuid(uuid) => {
            let binary = BsonValue::Binary(uuid.to_binary());
            check_pair(value, &binary);
            assert_eq!(value, &binary);
        }
        BsonValue::Binary(binary) => {
            for representation in [
                UuidRepresentation::Standard,
                UuidRepresentation::PythonLegacy,
                UuidRepresentation::JavaLegacy,
                UuidRepresentation::CSharpLegacy,
            ] {
                if let Ok(uuid) = BsonUuid::from_binary(binary, representation) {
                    let uuid = BsonValue::Uuid(uuid);
                    check_pair(value, &uuid);
                    assert_eq!(value, &uuid);
                }
            }
        }
        _ => {}
    }
}

fuzz_target!(|data: &[u8]| {
    let mut input = Input {
        bytes: data,
        offset: 0,
    };
    let left = input.value(3);
    let middle = input.value(3);
    let right = input.value(3);

    check_pair(&left, &middle);
    check_pair(&middle, &right);
    check_pair(&left, &right);
    assert_eq!(left.cmp(&left), Ordering::Equal);
    assert!(left.representation_eq(&left));
    if left <= middle && middle <= right {
        assert!(left <= right);
    }

    for value in [&left, &middle, &right] {
        check_uuid_binary_identity(value);
        let key = CanonicalBsonKey::encode(value).unwrap();
        let parsed = CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap();
        assert_eq!(parsed, key);
    }
});
