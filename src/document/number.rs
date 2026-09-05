use std::cmp::Ordering;

use num_bigint::BigUint;

/// Decimal128's smallest stored quantum.
pub(crate) const MIN_CANONICAL_EXPONENT_TWO: i16 = -6176;
/// Decimal128's largest stored quantum plus the largest possible power of two
/// removed from its 34-digit coefficient.
pub(crate) const MAX_CANONICAL_EXPONENT_TWO: i16 = 6223;
/// Decimal128's smallest stored quantum.
pub(crate) const MIN_CANONICAL_EXPONENT_FIVE: i16 = -6176;
/// Decimal128's largest stored quantum plus the largest possible power of five
/// removed from its 34-digit coefficient.
pub(crate) const MAX_CANONICAL_EXPONENT_FIVE: i16 = 6159;
/// Every emitted core coefficient is below 10^34 and therefore at most 15 bytes.
pub(crate) const MAX_CANONICAL_COEFFICIENT_BYTES: usize = 15;
/// IEEE Decimal128's largest valid 34-digit coefficient.
pub(crate) const MAX_DECIMAL128_COEFFICIENT: u128 = 9_999_999_999_999_999_999_999_999_999_999_999;

const DECIMAL128_EXPONENT_BIAS: i32 = 6176;
const DECIMAL128_NORMAL_COEFFICIENT_MASK: u128 = (1_u128 << 113) - 1;
const DECIMAL128_STEERING_COEFFICIENT_MASK: u128 = (1_u128 << 111) - 1;

/// A unique exact identity for a finite BSON number.
///
/// Its value is `(-1)^negative * coefficient * 2^exponent_two *
/// 5^exponent_five`. A nonzero coefficient has no factor of two or five;
/// zero has no sign and both exponents are zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalFinite {
    negative: bool,
    coefficient: u128,
    exponent_two: i16,
    exponent_five: i16,
}

impl CanonicalFinite {
    pub(crate) const fn is_negative(self) -> bool {
        self.negative
    }

    pub(crate) const fn coefficient(self) -> u128 {
        self.coefficient
    }

    pub(crate) const fn exponent_two(self) -> i16 {
        self.exponent_two
    }

    pub(crate) const fn exponent_five(self) -> i16 {
        self.exponent_five
    }

    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        debug_assert_ne!(self.coefficient, 0);
        debug_assert_ne!(other.coefficient, 0);

        let common_two = self.exponent_two.min(other.exponent_two);
        let common_five = self.exponent_five.min(other.exponent_five);
        aligned_magnitude(self, common_two, common_five).cmp(&aligned_magnitude(
            other,
            common_two,
            common_five,
        ))
    }
}

impl PartialOrd for CanonicalFinite {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CanonicalFinite {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
            return Ordering::Equal;
        }
        match (self.coefficient == 0, other.coefficient == 0) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if other.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, true) => {
                return if self.negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, false) => {}
        }
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.magnitude_cmp(other),
            (true, true) => self.magnitude_cmp(other).reverse(),
        }
    }
}

fn aligned_magnitude(value: &CanonicalFinite, common_two: i16, common_five: i16) -> BigUint {
    let delta_two = u32::try_from(i32::from(value.exponent_two) - i32::from(common_two))
        .expect("the common exponent is the minimum");
    let delta_five = u32::try_from(i32::from(value.exponent_five) - i32::from(common_five))
        .expect("the common exponent is the minimum");

    let mut magnitude = BigUint::from(value.coefficient);
    magnitude <<= usize::try_from(delta_two).expect("u32 fits usize on supported targets");
    if delta_five != 0 {
        magnitude *= BigUint::from(5_u8).pow(delta_five);
    }
    magnitude
}

/// Exact, representation-independent identity for a BSON number.
///
/// TinyMongo and MongoDB compare the integer, double, and Decimal128 families
/// by numeric value. Finite values use a compact factorization with a core
/// coefficient coprime to ten and independent powers of two and five. This is
/// exact for integers, binary doubles, and Decimal128 without expanding large
/// decimal powers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalNumber {
    NaN,
    NegativeInfinity,
    Finite(CanonicalFinite),
    PositiveInfinity,
}

impl CanonicalNumber {
    pub(crate) fn from_i32(value: i32) -> Self {
        finite(value.is_negative(), u128::from(value.unsigned_abs()), 0, 0)
    }

    pub(crate) fn from_i64(value: i64) -> Self {
        finite(value.is_negative(), u128::from(value.unsigned_abs()), 0, 0)
    }

    pub(crate) fn from_f64(value: f64) -> Self {
        if value.is_nan() {
            return Self::NaN;
        }
        if value == f64::NEG_INFINITY {
            return Self::NegativeInfinity;
        }
        if value == f64::INFINITY {
            return Self::PositiveInfinity;
        }

        let bits = value.to_bits();
        let negative = bits >> 63 != 0;
        let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        if exponent_bits == 0 && fraction == 0 {
            return finite(false, 0, 0, 0);
        }

        let (significand, exponent_two) = if exponent_bits == 0 {
            (fraction, -1022 - 52)
        } else {
            ((1_u64 << 52) | fraction, exponent_bits - 1023 - 52)
        };
        finite(negative, u128::from(significand), exponent_two, 0)
    }

    pub(crate) fn from_decimal_bid(bid: [u8; 16]) -> Self {
        let raw = u128::from_le_bytes(bid);
        let negative = raw >> 127 != 0;

        if (raw >> 123) & 0x0f == 0x0f {
            return if raw & (1_u128 << 122) != 0 {
                Self::NaN
            } else if negative {
                Self::NegativeInfinity
            } else {
                Self::PositiveInfinity
            };
        }

        let (biased_exponent, coefficient) = if (raw >> 125) & 0b11 == 0b11 {
            (
                (raw >> 111) & 0x3fff,
                (1_u128 << 113) | (raw & DECIMAL128_STEERING_COEFFICIENT_MASK),
            )
        } else {
            (
                (raw >> 113) & 0x3fff,
                raw & DECIMAL128_NORMAL_COEFFICIENT_MASK,
            )
        };
        let coefficient = if coefficient > MAX_DECIMAL128_COEFFICIENT {
            0
        } else {
            coefficient
        };
        let exponent = i32::try_from(biased_exponent).expect("the Decimal128 exponent is 14 bits")
            - DECIMAL128_EXPONENT_BIAS;
        finite(negative, coefficient, exponent, exponent)
    }
}

impl PartialOrd for CanonicalNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CanonicalNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        use CanonicalNumber::{Finite, NaN, NegativeInfinity, PositiveInfinity};

        let rank = |value: &Self| match value {
            NaN => 0_u8,
            NegativeInfinity => 1,
            Finite(_) => 2,
            PositiveInfinity => 3,
        };
        rank(self)
            .cmp(&rank(other))
            .then_with(|| match (self, other) {
                (Finite(left), Finite(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

fn finite(
    negative: bool,
    mut coefficient: u128,
    mut exponent_two: i32,
    mut exponent_five: i32,
) -> CanonicalNumber {
    if coefficient == 0 {
        return CanonicalNumber::Finite(CanonicalFinite {
            negative: false,
            coefficient: 0,
            exponent_two: 0,
            exponent_five: 0,
        });
    }

    while coefficient % 2 == 0 {
        coefficient /= 2;
        exponent_two += 1;
    }
    while coefficient % 5 == 0 {
        coefficient /= 5;
        exponent_five += 1;
    }

    assert!(
        (i32::from(MIN_CANONICAL_EXPONENT_TWO)..=i32::from(MAX_CANONICAL_EXPONENT_TWO))
            .contains(&exponent_two)
    );
    assert!(
        (i32::from(MIN_CANONICAL_EXPONENT_FIVE)..=i32::from(MAX_CANONICAL_EXPONENT_FIVE))
            .contains(&exponent_five)
    );
    CanonicalNumber::Finite(CanonicalFinite {
        negative,
        coefficient,
        exponent_two: i16::try_from(exponent_two)
            .expect("the canonical power-of-two exponent is in range"),
        exponent_five: i16::try_from(exponent_five)
            .expect("the canonical power-of-five exponent is in range"),
    })
}

#[cfg(test)]
fn parse_rendered_decimal(value: &str) -> Option<CanonicalNumber> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    match unsigned {
        "NaN" | "sNaN" => return Some(CanonicalNumber::NaN),
        "Infinity" => {
            return Some(if negative {
                CanonicalNumber::NegativeInfinity
            } else {
                CanonicalNumber::PositiveInfinity
            });
        }
        _ => {}
    }

    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned.get(index + 1..)?.parse::<i32>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }

    let mut coefficient = 0_u128;
    for digit in whole.bytes().chain(fraction.bytes()) {
        if !digit.is_ascii_digit() {
            return None;
        }
        coefficient = coefficient
            .checked_mul(10)?
            .checked_add(u128::from(digit - b'0'))?;
    }
    let scale = exponent.checked_sub(i32::try_from(fraction.len()).ok()?)?;
    if !(-6176..=6111).contains(&scale) && coefficient != 0 {
        return None;
    }
    Some(finite(negative, coefficient, scale, scale))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        hint::black_box,
        str::FromStr,
        time::Instant,
    };

    use proptest::prelude::*;

    use super::*;
    use crate::document::{BsonDecimal128, BsonValue, CanonicalBsonKey};

    fn finite_parts(value: CanonicalNumber) -> CanonicalFinite {
        match value {
            CanonicalNumber::Finite(value) => value,
            _ => panic!("expected a finite number"),
        }
    }

    fn bid_bytes(hex: &str) -> [u8; 16] {
        assert_eq!(hex.len(), 32);
        let mut bytes = [0_u8; 16];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
        }
        bytes
    }

    #[test]
    fn binary_double_identity_is_exact() {
        assert_ne!(
            CanonicalNumber::from_f64(0.1),
            CanonicalNumber::from_decimal_bid(bson::Decimal128::from_str("0.1").unwrap().bytes())
        );
        assert_eq!(CanonicalNumber::from_f64(1.0), CanonicalNumber::from_i32(1));
        assert_eq!(
            CanonicalNumber::from_f64(-0.0),
            CanonicalNumber::from_i32(0)
        );
        assert_eq!(
            CanonicalNumber::from_i64(i64::MIN),
            CanonicalNumber::from_f64(i64::MIN as f64)
        );
    }

    #[test]
    fn finite_identity_strips_two_and_five_factors_separately() {
        assert_eq!(
            finite_parts(CanonicalNumber::from_i32(100)),
            CanonicalFinite {
                negative: false,
                coefficient: 1,
                exponent_two: 2,
                exponent_five: 2,
            }
        );
        assert_eq!(
            finite_parts(CanonicalNumber::from_f64(0.5)),
            CanonicalFinite {
                negative: false,
                coefficient: 1,
                exponent_two: -1,
                exponent_five: 0,
            }
        );
        assert_eq!(
            finite_parts(CanonicalNumber::from_decimal_bid(
                bson::Decimal128::from_str("0.1").unwrap().bytes()
            )),
            CanonicalFinite {
                negative: false,
                coefficient: 1,
                exponent_two: -1,
                exponent_five: -1,
            }
        );
    }

    #[test]
    fn decimal_extremes_and_specials_are_total() {
        let low = bson::Decimal128::from_str("1E-6176").unwrap().bytes();
        let high = bson::Decimal128::from_str("9.999999999999999999999999999999999E+6144")
            .unwrap()
            .bytes();
        let low = CanonicalNumber::from_decimal_bid(low);
        let high = CanonicalNumber::from_decimal_bid(high);
        assert!(low < high);
        let low = finite_parts(low);
        let high = finite_parts(high);
        assert_eq!(low.exponent_two, MIN_CANONICAL_EXPONENT_TWO);
        assert_eq!(low.exponent_five, MIN_CANONICAL_EXPONENT_FIVE);
        assert!(high.exponent_two <= MAX_CANONICAL_EXPONENT_TWO);
        assert!(high.exponent_five <= MAX_CANONICAL_EXPONENT_FIVE);

        let nan = bson::Decimal128::from_str("NaN").unwrap().bytes();
        let signalling = bson::Decimal128::from_str("sNaN").unwrap().bytes();
        assert_eq!(
            CanonicalNumber::from_decimal_bid(nan),
            CanonicalNumber::from_decimal_bid(signalling)
        );
        assert!(CanonicalNumber::from_f64(f64::NAN) < CanonicalNumber::from_i32(-1));
    }

    #[test]
    fn raw_bid_edges_follow_the_bson_decimal128_canonicalization() {
        let zero = CanonicalNumber::from_i32(0);
        for encoded in [
            "00000000000000000000000000000000",
            "00000000000000000000000000000080",
            "0000000000000000000000000000FE5F",
            "0000000000000000000000000000FEDF",
            // Coefficients above 10^34 - 1 and steering noncanonical values
            // are the BSON specification's noncanonical encodings of zero.
            "00000000648E8D37C087ADBE09ED4130",
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFF4130",
            "00000000000000000000000000000060",
        ] {
            assert_eq!(CanonicalNumber::from_decimal_bid(bid_bytes(encoded)), zero);
        }

        for (encoded, decimal) in [
            ("01000000000000000000000000000000", "1E-6176"),
            ("01000000000000000000000000004200", "1E-6143"),
            (
                "FFFFFFFF638E8D37C087ADBE09EDFF5F",
                "9.999999999999999999999999999999999E+6144",
            ),
        ] {
            assert_eq!(
                CanonicalNumber::from_decimal_bid(bid_bytes(encoded)),
                CanonicalNumber::from_decimal_bid(
                    bson::Decimal128::from_str(decimal).unwrap().bytes()
                )
            );
        }

        for encoded in [
            "0000000000000000000000000000007C",
            "0000000000000000000000000000007E",
            "341200000000000000000000000000FC",
            "000000000000000000000000000000FE",
        ] {
            assert_eq!(
                CanonicalNumber::from_decimal_bid(bid_bytes(encoded)),
                CanonicalNumber::NaN
            );
        }
        assert_eq!(
            CanonicalNumber::from_decimal_bid(bid_bytes("00000000000000000000000000000078")),
            CanonicalNumber::PositiveInfinity
        );
        assert_eq!(
            CanonicalNumber::from_decimal_bid(bid_bytes("000000000000000000000000000000F8")),
            CanonicalNumber::NegativeInfinity
        );
    }

    #[test]
    fn finite_order_handles_sign_zero_and_independent_exponents() {
        let values = [
            CanonicalNumber::from_i32(-10),
            CanonicalNumber::from_decimal_bid(bson::Decimal128::from_str("-0.1").unwrap().bytes()),
            CanonicalNumber::from_f64(-0.0),
            CanonicalNumber::from_decimal_bid(bson::Decimal128::from_str("0.1").unwrap().bytes()),
            CanonicalNumber::from_f64(0.5),
            CanonicalNumber::from_i32(5),
        ];
        for pair in values.windows(2) {
            assert!(pair[0] < pair[1]);
        }
        for left in values {
            for right in values {
                assert_eq!(left.cmp(&right), right.cmp(&left).reverse());
                assert_eq!(left.cmp(&right) == Ordering::Equal, left == right);
            }
        }
    }

    proptest! {
        #[test]
        fn direct_decimal_bid_decode_matches_display_oracle(bid in any::<[u8; 16]>()) {
            let direct = CanonicalNumber::from_decimal_bid(bid);
            let rendered = bson::Decimal128::from_bytes(bid).to_string();
            prop_assert_eq!(direct, parse_rendered_decimal(&rendered).unwrap());
        }

        #[test]
        fn arbitrary_double_order_is_lawful(left in any::<f64>(), right in any::<f64>()) {
            let left = CanonicalNumber::from_f64(left);
            let right = CanonicalNumber::from_f64(right);
            prop_assert_eq!(left.cmp(&right), right.cmp(&left).reverse());
            prop_assert_eq!(left.cmp(&right) == Ordering::Equal, left == right);
        }

        #[test]
        fn arbitrary_decimal_bid_order_is_lawful(
            left in any::<[u8; 16]>(),
            right in any::<[u8; 16]>(),
        ) {
            let left = CanonicalNumber::from_decimal_bid(left);
            let right = CanonicalNumber::from_decimal_bid(right);
            prop_assert_eq!(left.cmp(&right), right.cmp(&left).reverse());
            prop_assert_eq!(left.cmp(&right) == Ordering::Equal, left == right);
        }
    }

    #[test]
    #[ignore = "manual release-mode BSON numeric hot-path benchmark"]
    fn benchmark_extreme_decimal_array_identity_paths() {
        const COUNT: usize = 6_511;

        let decimal = BsonValue::Decimal128(
            BsonDecimal128::parse("1E-6176").expect("valid Decimal128 benchmark fixture"),
        );
        let left = BsonValue::Array(vec![decimal; COUNT]);
        let right = left.clone();

        let started = Instant::now();
        assert!(black_box(&left).eq(black_box(&right)));
        let equality = started.elapsed();

        let started = Instant::now();
        assert_eq!(black_box(&left).cmp(black_box(&right)), Ordering::Equal);
        let comparison = started.elapsed();

        let started = Instant::now();
        let mut hasher = DefaultHasher::new();
        black_box(&left).hash(&mut hasher);
        black_box(hasher.finish());
        let hashing = started.elapsed();

        let started = Instant::now();
        let key = CanonicalBsonKey::encode(black_box(&left)).unwrap();
        black_box(key.as_bytes());
        let key_encoding = started.elapsed();

        eprintln!(
            "{COUNT} Decimal128(1E-6176): eq={equality:?} cmp={comparison:?} \
             hash={hashing:?} key={key_encoding:?} key_bytes={}",
            key.as_bytes().len()
        );
        assert!(key.as_bytes().len() < 100_000);
        assert_eq!(CanonicalBsonKey::from_bytes(key.as_bytes()).unwrap(), key);
    }
}
