//! Versioned order-preserving bytes, separate from the equality/routing codec.
//! Each component is self-delimiting before descending inversion; raw BSON and
//! canonical identity bytes must never be substituted for this ordering.

use num_bigint::BigUint;

use super::{Atom, BsonDocument, BsonValue, DocumentSortKey, MAX_DEPTH, MAX_STEPS, limit};
use crate::{core::EngineResult, document::number::CanonicalNumber};

const MAX_BYTES: usize = 16 * 1024 * 1024;
const CHECK_CHUNK: usize = 4096;

impl DocumentSortKey {
    /// Version of the order-preserving encoding (not the identity-key format).
    pub const ORDERED_ENCODING_VERSION: u32 = 1;

    /// Encode an owned sort key for lexicographic byte ordering. Equal BSON
    /// numbers across representations produce identical bytes. Direction and
    /// array-selection semantics come from the original compiled sorter.
    /// Natural-order tie-breakers are deliberately not included.
    pub fn ordered_bytes(&self) -> EngineResult<Vec<u8>> {
        self.ordered_bytes_with_check(&mut || Ok(()))
    }

    /// As `ordered_bytes`, with cooperative checks and a 16-MiB output bound.
    /// The format is versioned; comparing bytes of different versions is not a
    /// supported ordering. No persistence or index selection is performed here.
    pub fn ordered_bytes_with_check(
        &self,
        check: &mut dyn FnMut() -> EngineResult<()>,
    ) -> EngineResult<Vec<u8>> {
        let mut encoder = Encoder {
            bytes: Vec::new(),
            steps: 0,
            check,
        };
        encoder.extend(b"BBSO")?;
        encoder.extend(&Self::ORDERED_ENCODING_VERSION.to_be_bytes())?;
        for (descending, atom) in &self.components {
            encoder.step()?;
            encoder.extend(&[1, u8::from(*descending)])?;
            let start = encoder.bytes.len();
            match atom {
                Atom::EmptyArray => encoder.extend(&[2])?,
                Atom::Value(value) => encoder.value(value, 0)?,
            }
            if *descending {
                for chunk in encoder.bytes[start..].chunks_mut(CHECK_CHUNK) {
                    (encoder.check)()?;
                    for byte in chunk {
                        *byte = !*byte;
                    }
                }
            }
        }
        encoder.extend(&[0])?;
        (encoder.check)()?;
        Ok(encoder.bytes)
    }
}

struct Encoder<'a> {
    bytes: Vec<u8>,
    steps: usize,
    check: &'a mut dyn FnMut() -> EngineResult<()>,
}

impl Encoder<'_> {
    fn step(&mut self) -> EngineResult<()> {
        (self.check)()?;
        self.steps += 1;
        if self.steps > MAX_STEPS {
            return Err(limit());
        }
        Ok(())
    }

    fn extend(&mut self, bytes: &[u8]) -> EngineResult<()> {
        let needed = self.bytes.len().saturating_add(bytes.len());
        if needed > MAX_BYTES {
            return Err(limit());
        }
        if needed > self.bytes.capacity() {
            // Grow geometrically, but never request a backing allocation larger
            // than the output budget just because Vec would double its capacity.
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .min(MAX_BYTES);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| limit())?;
        }
        for chunk in bytes.chunks(CHECK_CHUNK) {
            (self.check)()?;
            self.bytes.extend_from_slice(chunk);
        }
        Ok(())
    }

    // NUL escaping preserves lexical order and makes every string prefix-free.
    // Descending byte inversion therefore also reverses proper-prefix strings.
    fn string(&mut self, bytes: &[u8]) -> EngineResult<()> {
        for chunk in bytes.chunks(CHECK_CHUNK) {
            (self.check)()?;
            for run in chunk.split_inclusive(|byte| *byte == 0) {
                self.extend(run)?;
                if run.last() == Some(&0) {
                    self.extend(&[255])?;
                }
            }
        }
        self.extend(&[0, 0])
    }

    fn value(&mut self, value: &BsonValue, depth: usize) -> EngineResult<()> {
        self.step()?;
        if depth > MAX_DEPTH {
            return Err(limit());
        }
        let rank = value.type_rank();
        self.extend(&[if rank == 0 { 1 } else { rank + 2 }])?;
        if let Some(number) = value.canonical_number() {
            return self.number(number);
        }
        if let Some(result) = value.with_binary_parts(|subtype, bytes| {
            self.extend(&(bytes.len() as u64 + u64::from(subtype == 2) * 4).to_be_bytes())?;
            self.extend(&[subtype])?;
            self.extend(bytes)
        }) {
            return result;
        }
        match value {
            BsonValue::MinKey | BsonValue::Null | BsonValue::MaxKey => Ok(()),
            BsonValue::String(value) => self.string(value.as_bytes()),
            BsonValue::Document(value) => self.document(value, depth + 1),
            BsonValue::Array(values) => {
                for value in values {
                    self.value(value, depth + 1)?;
                }
                self.extend(&[0])
            }
            BsonValue::ObjectId(value) => self.extend(&value.bytes()),
            BsonValue::Boolean(value) => self.extend(&[u8::from(*value)]),
            BsonValue::DateTime(value) => {
                self.extend(&((value.timestamp_millis() as u64) ^ (1_u64 << 63)).to_be_bytes())
            }
            BsonValue::Timestamp(value) => {
                self.extend(&value.time().to_be_bytes())?;
                self.extend(&value.increment().to_be_bytes())
            }
            BsonValue::RegularExpression(value) => {
                self.string(value.pattern().as_bytes())?;
                self.string(value.options().as_bytes())
            }
            BsonValue::JavaScript(value) => {
                self.string(value.code().as_bytes())?;
                if let Some(scope) = value.scope() {
                    self.document(scope, depth + 1)?;
                }
                Ok(())
            }
            BsonValue::Double(_)
            | BsonValue::Int32(_)
            | BsonValue::Int64(_)
            | BsonValue::Decimal128(_)
            | BsonValue::Binary(_)
            | BsonValue::Uuid(_) => unreachable!("numeric and binary families encoded above"),
        }
    }

    fn document(&mut self, document: &BsonDocument, depth: usize) -> EngineResult<()> {
        if depth > MAX_DEPTH {
            return Err(limit());
        }
        for (name, value) in document.iter() {
            // BSON objects compare each value's family, then its field name,
            // then its value. Field order and duplicates remain significant.
            self.extend(&[value.type_rank() + 1])?;
            self.string(name.as_bytes())?;
            self.value(value, depth)?;
        }
        self.extend(&[0])
    }

    fn number(&mut self, value: CanonicalNumber) -> EngineResult<()> {
        let finite = match value {
            CanonicalNumber::NaN => return self.extend(&[0]),
            CanonicalNumber::NegativeInfinity => return self.extend(&[1]),
            CanonicalNumber::PositiveInfinity => return self.extend(&[5]),
            CanonicalNumber::Finite(finite) if finite.coefficient() == 0 => {
                return self.extend(&[3]);
            }
            CanonicalNumber::Finite(finite) => finite,
        };
        self.extend(&[if finite.is_negative() { 2 } else { 4 }])?;
        let start = self.bytes.len();
        let exponent = finite.exponent_two().min(finite.exponent_five());
        let mut integer = BigUint::from(finite.coefficient());
        integer <<= usize::try_from(i32::from(finite.exponent_two()) - i32::from(exponent))
            .expect("common exponent is the minimum");
        let fives = u32::try_from(i32::from(finite.exponent_five()) - i32::from(exponent))
            .expect("common exponent is the minimum");
        if fives != 0 {
            integer *= BigUint::from(5_u8).pow(fives);
        }
        let digits = integer.to_str_radix(10);
        let scientific_exponent = i32::from(exponent) + digits.len() as i32 - 1;
        self.extend(&((scientific_exponent as u32) ^ (1_u32 << 31)).to_be_bytes())?;
        self.string(digits.as_bytes())?;
        // The canonical coefficient has no factor of two or five; scaling by
        // only the unmatched prime cannot leave redundant decimal trailing zeroes.
        if finite.is_negative() {
            for byte in &mut self.bytes[start..] {
                *byte = !*byte;
            }
        }
        (self.check)()
    }
}

#[cfg(test)]
mod tests;
