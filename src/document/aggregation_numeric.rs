//! Ordered numeric accumulators. Do not merge rounded shard totals: floating
//! addition and Decimal128 context rounding are not associative.

use dec::{Context, Decimal, Rounding};
use num_bigint::BigUint;

use super::{BsonDecimal128, BsonValue, number::CanonicalNumber};

type Total = Decimal<12>;
// Every exact finite binary64 coefficient fits in 768 decimal digits, including
// the smallest subnormal (5^1074 * 10^-1074). Keep the operand exact until add;
// rounding it to Decimal128 first would introduce double-rounding differences.
type Operand = Decimal<256>;

fn context() -> Context<Total> {
    let mut context = Context::default();
    context.set_precision(34).expect("Decimal128 precision");
    context.set_min_exponent(-6143).expect("Decimal128 Emin");
    context.set_max_exponent(6144).expect("Decimal128 Emax");
    context.set_clamp(true);
    context.set_rounding(Rounding::HalfEven);
    context
}

fn exact_double(value: f64) -> String {
    match CanonicalNumber::from_f64(value) {
        CanonicalNumber::NaN => "NaN".into(), // Decimal.from_float drops NaN sign.
        CanonicalNumber::NegativeInfinity => "-Infinity".into(),
        CanonicalNumber::PositiveInfinity => "Infinity".into(),
        CanonicalNumber::Finite(number) => {
            // Decimal.from_float gives exact integer-valued doubles quantum 0,
            // not a reduced positive exponent (which changes sum result BID).
            let exponent = number.exponent_two().min(number.exponent_five()).min(0);
            let mut coefficient = BigUint::from(number.coefficient());
            coefficient <<= (number.exponent_two() - exponent) as usize;
            coefficient *= BigUint::from(5_u8).pow((number.exponent_five() - exponent) as u32);
            let sign = if value.is_sign_negative() { "-" } else { "" };
            format!("{sign}{coefficient}E{exponent}")
        }
    }
}

fn decimal_text(value: BsonDecimal128) -> String {
    let raw = u128::from_le_bytes(value.bid());
    let sign = if raw >> 127 != 0 { "-" } else { "" };
    // BSON uses BID, dec uses decNumber/DPD. Never reinterpret their bytes.
    // Preserve zero quantum and signed/signaling NaNs, unlike numeric identity.
    if (raw >> 123) & 15 == 15 {
        let special = if raw & (1 << 122) == 0 {
            "Infinity"
        } else if raw & (1 << 121) != 0 {
            "sNaN"
        } else {
            "NaN"
        };
        return format!("{sign}{special}");
    }
    let (exponent, coefficient) = if (raw >> 125) & 3 == 3 {
        ((raw >> 111) & 0x3fff, (1 << 113) | (raw & ((1 << 111) - 1)))
    } else {
        ((raw >> 113) & 0x3fff, raw & ((1 << 113) - 1))
    };
    let coefficient = if coefficient > super::number::MAX_DECIMAL128_COEFFICIENT {
        0
    } else {
        coefficient
    };
    format!("{sign}{coefficient}E{}", exponent as i32 - 6176)
}

fn operand(value: &BsonValue) -> Operand {
    let text = match value {
        BsonValue::Int32(value) => value.to_string(),
        BsonValue::Int64(value) => value.to_string(),
        BsonValue::Double(value) => exact_double(*value),
        BsonValue::Decimal128(value) => decimal_text(*value),
        _ => unreachable!("numeric operand"),
    };
    Context::<Operand>::default()
        .parse(text)
        .expect("exact bounded number")
}

fn output(value: Total) -> BsonValue {
    BsonValue::Decimal128(BsonDecimal128::parse(&value.to_string()).expect("Decimal128 result"))
}

fn double_output(value: f64) -> BsonValue {
    // Arithmetic NaN sign/payload is not specified by IEEE arithmetic and can
    // change with compiler operand scheduling (including in the Python oracle).
    // Give newly computed NaNs a deterministic representation. Pass-through
    // accumulators and group keys still retain their original BSON bits.
    BsonValue::Double(if value.is_nan() { f64::NAN } else { value })
}

/// Update arithmetic deliberately differs from aggregation: a Double promotes
/// with 15 significant digits, not its exact binary expansion. Both operands
/// already fit Decimal128 precision, so parsing cannot introduce double rounding.
pub(super) fn increment_decimal(left: &BsonValue, right: &BsonValue) -> BsonValue {
    let parse = |value: &BsonValue| {
        let text = match value {
            BsonValue::Int32(value) => value.to_string(),
            BsonValue::Int64(value) => value.to_string(),
            BsonValue::Double(value) if value.is_finite() => format!("{value:.14e}"),
            BsonValue::Double(value) => exact_double(*value),
            BsonValue::Decimal128(value) => decimal_text(*value),
            _ => unreachable!("validated numeric update"),
        };
        context().parse(text).expect("bounded Decimal128 operand")
    };
    let mut result = parse(left);
    context().add(&mut result, &parse(right));
    let result = output(result);
    if matches!(left, BsonValue::Decimal128(_))
        && result.canonical_number() != Some(CanonicalNumber::NaN)
        && result == *left
    {
        // Retain quantum, signed zero and noncanonical zero BID on a rounded
        // no-op. NaNs are executed arithmetic, never this equality shortcut.
        left.clone()
    } else {
        result
    }
}

#[derive(Default)]
pub(super) struct Sum {
    integer: i128,
    decimal: Option<Total>,
    double: f64,
    has_decimal: bool,
}

impl Sum {
    pub fn add(&mut self, value: &BsonValue) {
        if value.canonical_number().is_none() {
            return;
        }
        if self.decimal.is_none() {
            let integer = match value {
                BsonValue::Int32(value) => Some(i128::from(*value)),
                BsonValue::Int64(value) => Some(i128::from(*value)),
                _ => None,
            };
            if let Some(integer) = integer {
                // At most 65,536 signed 64-bit inputs: fewer than 80 bits.
                self.integer += integer;
                return;
            }
            self.decimal = Some(context().from_i128(self.integer));
            self.double = self.integer as f64;
        }
        context().add(self.decimal.as_mut().expect("initialized"), &operand(value));
        let is_decimal = matches!(value, BsonValue::Decimal128(_));
        if !self.has_decimal && !is_decimal {
            self.double += match value {
                BsonValue::Int32(value) => f64::from(*value),
                BsonValue::Int64(value) => *value as f64,
                BsonValue::Double(value) => *value,
                _ => unreachable!("non-decimal number"),
            };
        }
        self.has_decimal |= is_decimal;
    }

    pub fn finish(self) -> BsonValue {
        if self.has_decimal {
            output(self.decimal.expect("decimal total"))
        } else if self.decimal.is_some() {
            double_output(self.double)
        } else if let Ok(value) = i32::try_from(self.integer) {
            BsonValue::Int32(value)
        } else if let Ok(value) = i64::try_from(self.integer) {
            BsonValue::Int64(value)
        } else {
            // Frozen Python returns an unencodable arbitrary-width integer here.
            // BSON's supported overflow result is Double, never truncated Int64.
            BsonValue::Double(self.integer as f64)
        }
    }
}

#[derive(Default)]
pub(super) struct Average {
    count: u64,
    decimal: Total,
    non_decimal: DoubleDouble,
    has_decimal: bool,
    has_double: bool,
}

impl Average {
    pub fn add(&mut self, value: &BsonValue) {
        match value {
            BsonValue::Decimal128(_) => {
                context().add(&mut self.decimal, &operand(value));
                self.has_decimal = true;
            }
            BsonValue::Int32(value) => self.non_decimal.add_int(i64::from(*value)),
            BsonValue::Int64(value) => self.non_decimal.add_int(*value),
            BsonValue::Double(value) => {
                self.non_decimal.add(*value);
                self.has_double = true;
            }
            _ => return,
        }
        self.count += 1;
    }

    pub fn finish(mut self) -> BsonValue {
        if self.count == 0 {
            return BsonValue::Null;
        }
        if self.has_decimal {
            let mut context = context();
            context.add(&mut self.decimal, &self.non_decimal.decimal());
            context.div(&mut self.decimal, &Total::from(self.count));
            output(self.decimal)
        } else {
            double_output(self.non_decimal.value(!self.has_double) / self.count as f64)
        }
    }
}

#[derive(Default)]
struct DoubleDouble {
    sum: f64,
    addend: f64,
    special: f64,
}

impl DoubleDouble {
    fn add(&mut self, value: f64) {
        self.special += value;
        let total = value + self.addend;
        self.addend -= total - value;
        let sum = self.sum + total;
        let left = sum - total;
        let right = sum - left;
        self.addend += (self.sum - left) + (total - right);
        self.sum = sum;
    }

    fn add_int(&mut self, value: i64) {
        if i32::try_from(value).is_ok() {
            self.add(value as f64);
        } else {
            let high = (value / (1_i64 << 32)) * (1_i64 << 32);
            self.add((value - high) as f64);
            self.add(high as f64);
        }
    }

    fn value(&self, include_addend: bool) -> f64 {
        if self.sum.is_nan() {
            self.special
        } else if include_addend && self.sum.is_finite() {
            self.sum + self.addend
        } else {
            self.sum
        }
    }

    fn decimal(&self) -> Total {
        let mut context = context();
        if !self.sum.is_finite() {
            return context.parse(exact_double(self.special)).expect("double");
        }
        let mut sum = context.parse(exact_double(self.sum)).expect("double");
        let addend = context.parse(exact_double(self.addend)).expect("double");
        context.add(&mut sum, &addend);
        sum
    }
}
