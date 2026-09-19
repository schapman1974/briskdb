//! Conservative owned-heap charges for BSON that has passed the bounded codec.
//! Preflight before cloning; these are retention quotas, not RSS measurements.

use super::{BsonDocument, BsonValue};
use crate::core::{EngineError, EngineErrorKind, EngineResult};

pub(super) fn document_bytes(
    document: &BsonDocument,
    maximum: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<usize> {
    let mut bytes = 0;
    charge_document(document, &mut bytes, maximum, check)?;
    Ok(bytes)
}

pub(super) fn value_bytes(
    value: &BsonValue,
    maximum: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<usize> {
    let mut bytes = 0;
    charge_value(value, &mut bytes, maximum, check)?;
    Ok(bytes)
}

fn add(bytes: &mut usize, amount: usize, maximum: usize) -> EngineResult<()> {
    *bytes = bytes
        .checked_add(amount)
        .filter(|total| *total <= maximum)
        .ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::LimitExceeded,
                "document retained value limit exceeded",
            )
        })?;
    Ok(())
}

fn charge_document(
    document: &BsonDocument,
    bytes: &mut usize,
    maximum: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    add(bytes, 128, maximum)?;
    for (name, value) in document.iter() {
        add(bytes, name.len(), maximum)?;
        charge_value(value, bytes, maximum, check)?;
    }
    Ok(())
}

fn charge_value(
    value: &BsonValue,
    bytes: &mut usize,
    maximum: usize,
    check: &mut dyn FnMut() -> EngineResult<()>,
) -> EngineResult<()> {
    check()?;
    add(bytes, 128, maximum)?;
    match value {
        BsonValue::String(value) => add(bytes, value.len(), maximum),
        BsonValue::Document(value) => charge_document(value, bytes, maximum, check),
        BsonValue::Array(values) => {
            for value in values {
                charge_value(value, bytes, maximum, check)?;
            }
            Ok(())
        }
        BsonValue::Binary(value) => add(bytes, value.bytes().len(), maximum),
        BsonValue::RegularExpression(value) => {
            add(bytes, value.pattern().len(), maximum)?;
            add(bytes, value.options().len(), maximum)
        }
        BsonValue::JavaScript(value) => {
            add(bytes, value.code().len(), maximum)?;
            if let Some(scope) = value.scope() {
                charge_document(scope, bytes, maximum, check)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
