//! Deterministic protocol-neutral merging for scatter query results.

use super::{EngineError, EngineErrorKind, EngineResult, ResultLimits, ResultSet, Routed, Value};

const RESULT_ENVELOPE_BYTES: u64 = 16;
const TYPE_TAG_BYTES: u64 = 1;
const LENGTH_BYTES: u64 = 8;
const ROW_FRAME_BYTES: u64 = 8;
const FIXED_VALUE_PAYLOAD_BYTES: u64 = 8;

/// Merge independently materialized shard results with `UNION ALL` semantics.
///
/// Rows are concatenated in ascending physical-shard order, regardless of the
/// order in which shard work completed. Duplicate rows are deliberately
/// retained. Every shard must return identical column names and types, and the
/// final result is charged against one shared row and logical-byte budget.
/// Validation and accounting finish before any row is moved into the result,
/// so an error never exposes a partial merge.
pub(crate) fn merge_scatter_results(
    mut shard_results: Vec<Routed<ResultSet>>,
    limits: ResultLimits,
) -> EngineResult<ResultSet> {
    shard_results.sort_by_key(|result| result.shard);

    reject_duplicate_shards(&shard_results)?;
    validate_column_metadata(&shard_results)?;
    let row_count = account_merged_result(&shard_results, limits)?;

    let capacity = usize::try_from(row_count).map_err(|_| row_limit_exceeded())?;
    let mut columns = None;
    let mut rows = Vec::with_capacity(capacity);
    for routed in shard_results {
        let (shard_columns, mut shard_rows) = routed.value.into_parts();
        if columns.is_none() {
            columns = Some(shard_columns);
        }
        rows.append(&mut shard_rows);
    }

    ResultSet::new(columns.unwrap_or_default(), rows).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "scatter merge produced rows that do not match their column metadata",
            error,
        )
    })
}

fn reject_duplicate_shards(shard_results: &[Routed<ResultSet>]) -> EngineResult<()> {
    if let Some(duplicate) = shard_results
        .windows(2)
        .find(|pair| pair[0].shard == pair[1].shard)
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "scatter query returned more than one result for shard {}",
                duplicate[0].shard
            ),
        ));
    }
    Ok(())
}

fn validate_column_metadata(shard_results: &[Routed<ResultSet>]) -> EngineResult<()> {
    let Some(reference) = shard_results.first() else {
        return Ok(());
    };

    if let Some(mismatch) = shard_results
        .iter()
        .skip(1)
        .find(|result| result.value.columns() != reference.value.columns())
    {
        return Err(EngineError::new(
            EngineErrorKind::Internal,
            format!(
                "scatter query returned column metadata on shard {} that differs from shard {}",
                mismatch.shard, reference.shard
            ),
        ));
    }
    Ok(())
}

fn account_merged_result(
    shard_results: &[Routed<ResultSet>],
    limits: ResultLimits,
) -> EngineResult<u64> {
    let mut logical_bytes = account_bytes(0, RESULT_ENVELOPE_BYTES, limits.max_bytes())?;
    if let Some(reference) = shard_results.first() {
        for column in reference.value.columns() {
            logical_bytes = account_bytes(logical_bytes, TYPE_TAG_BYTES, limits.max_bytes())?;
            logical_bytes = account_bytes(logical_bytes, LENGTH_BYTES, limits.max_bytes())?;
            logical_bytes = account_bytes(
                logical_bytes,
                usize_to_u64(column.name.len())?,
                limits.max_bytes(),
            )?;
        }
    }

    let mut row_count = 0;
    for routed in shard_results {
        for row in routed.value.rows() {
            row_count = account_row(row_count, limits.max_rows())?;
            logical_bytes = account_bytes(logical_bytes, ROW_FRAME_BYTES, limits.max_bytes())?;
            for value in row.values() {
                logical_bytes = account_bytes(
                    logical_bytes,
                    logical_value_bytes(value)?,
                    limits.max_bytes(),
                )?;
            }
        }
    }
    Ok(row_count)
}

fn logical_value_bytes(value: &Value) -> EngineResult<u64> {
    let payload = match value {
        Value::Null => 0,
        Value::Boolean(_) | Value::Int64(_) | Value::UInt64(_) | Value::Float64(_) => {
            FIXED_VALUE_PAYLOAD_BYTES
        }
        Value::Decimal(value) => usize_to_u64(value.as_str().len())?,
        Value::Text(value) => usize_to_u64(value.len())?,
        Value::InvalidText(value) | Value::Binary(value) => usize_to_u64(value.len())?,
    };
    TYPE_TAG_BYTES
        .checked_add(LENGTH_BYTES)
        .and_then(|framing| framing.checked_add(payload))
        .ok_or_else(byte_limit_exceeded)
}

fn account_row(current: u64, maximum: u64) -> EngineResult<u64> {
    let next = current.checked_add(1).ok_or_else(row_limit_exceeded)?;
    if next > maximum {
        return Err(row_limit_exceeded());
    }
    Ok(next)
}

fn account_bytes(current: u64, additional: u64, maximum: u64) -> EngineResult<u64> {
    let next = current
        .checked_add(additional)
        .ok_or_else(byte_limit_exceeded)?;
    if next > maximum {
        return Err(byte_limit_exceeded());
    }
    Ok(next)
}

fn usize_to_u64(value: usize) -> EngineResult<u64> {
    u64::try_from(value).map_err(|_| byte_limit_exceeded())
}

fn row_limit_exceeded() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "query result exceeds the configured row limit",
    )
}

fn byte_limit_exceeded() -> EngineError {
    EngineError::new(
        EngineErrorKind::LimitExceeded,
        "query result exceeds the configured logical byte limit",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Column, DataType, Row};

    fn columns() -> Vec<Column> {
        vec![Column::new("v", DataType::Int64)]
    }

    fn result(shard: u16, values: &[i64]) -> Routed<ResultSet> {
        Routed {
            shard,
            value: ResultSet::new(
                columns(),
                values
                    .iter()
                    .copied()
                    .map(|value| Row::new(vec![Value::from(value)]))
                    .collect(),
            )
            .unwrap(),
        }
    }

    fn merged_ints(result: &ResultSet) -> Vec<i64> {
        result
            .rows()
            .iter()
            .map(|row| row.get(0).and_then(Value::as_i64).unwrap())
            .collect()
    }

    #[test]
    fn concatenates_in_ascending_shard_order_and_preserves_duplicates() {
        let merged = merge_scatter_results(
            vec![result(2, &[20, 21]), result(0, &[7]), result(1, &[7, 10])],
            ResultLimits::default(),
        )
        .unwrap();

        assert_eq!(merged.columns(), columns());
        assert_eq!(merged_ints(&merged), [7, 7, 10, 20, 21]);
    }

    #[test]
    fn accepts_exact_shared_row_and_byte_boundaries() {
        // Envelope and one-column metadata use 26 bytes. Each integer row uses
        // an eight-byte row frame plus 17 bytes for its framed value.
        let merged = merge_scatter_results(
            vec![result(1, &[2]), result(0, &[1])],
            ResultLimits::new(2, 76).unwrap(),
        )
        .unwrap();

        assert_eq!(merged_ints(&merged), [1, 2]);
    }

    #[test]
    fn one_over_either_shared_limit_returns_no_partial_result() {
        let row_error = merge_scatter_results(
            vec![result(0, &[1]), result(1, &[2])],
            ResultLimits::new(1, 76).unwrap(),
        )
        .unwrap_err();
        assert_eq!(row_error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            row_error.diagnostic(),
            "query result exceeds the configured row limit"
        );

        // Each one-row shard result is 51 logical bytes in isolation. Their
        // 76-byte union must still share a single 75-byte ceiling.
        let byte_error = merge_scatter_results(
            vec![result(0, &[1]), result(1, &[2])],
            ResultLimits::new(2, 75).unwrap(),
        )
        .unwrap_err();
        assert_eq!(byte_error.kind(), EngineErrorKind::LimitExceeded);
        assert_eq!(
            byte_error.diagnostic(),
            "query result exceeds the configured logical byte limit"
        );
    }

    #[test]
    fn rejects_column_name_or_type_mismatches_before_merging_rows() {
        let mismatched = Routed {
            shard: 1,
            value: ResultSet::new(
                vec![Column::new("other", DataType::Text)],
                vec![Row::new(vec![Value::from("not merged")])],
            )
            .unwrap(),
        };
        let error =
            merge_scatter_results(vec![mismatched, result(0, &[1])], ResultLimits::default())
                .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            error.diagnostic(),
            "scatter query returned column metadata on shard 1 that differs from shard 0"
        );
    }

    #[test]
    fn empty_shard_results_preserve_metadata_and_consume_one_metadata_budget() {
        let merged = merge_scatter_results(
            vec![result(2, &[]), result(0, &[]), result(1, &[])],
            ResultLimits::new(1, 26).unwrap(),
        )
        .unwrap();

        assert_eq!(merged.columns(), columns());
        assert!(merged.is_empty());

        let error = merge_scatter_results(
            vec![result(0, &[]), result(1, &[])],
            ResultLimits::new(1, 25).unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    #[test]
    fn no_shards_produces_an_empty_zero_column_result_with_one_envelope() {
        let merged = merge_scatter_results(Vec::new(), ResultLimits::new(1, 16).unwrap()).unwrap();
        assert!(merged.columns().is_empty());
        assert!(merged.is_empty());

        let error =
            merge_scatter_results(Vec::new(), ResultLimits::new(1, 15).unwrap()).unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::LimitExceeded);
    }

    #[test]
    fn duplicate_shard_results_are_rejected_as_an_internal_failure() {
        let error = merge_scatter_results(
            vec![result(0, &[1]), result(0, &[2])],
            ResultLimits::default(),
        )
        .unwrap_err();

        assert_eq!(error.kind(), EngineErrorKind::Internal);
        assert_eq!(
            error.diagnostic(),
            "scatter query returned more than one result for shard 0"
        );
    }

    #[test]
    fn checked_accounting_classifies_integer_overflow_as_a_limit_failure() {
        assert_eq!(
            account_row(u64::MAX, u64::MAX).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
        assert_eq!(
            account_bytes(u64::MAX, 1, u64::MAX).unwrap_err().kind(),
            EngineErrorKind::LimitExceeded
        );
    }

    #[test]
    fn accounts_every_protocol_neutral_value_variant() {
        assert_eq!(logical_value_bytes(&Value::Null).unwrap(), 9);
        assert_eq!(logical_value_bytes(&Value::from(true)).unwrap(), 17);
        assert_eq!(logical_value_bytes(&Value::from(1_i64)).unwrap(), 17);
        assert_eq!(logical_value_bytes(&Value::from(1_u64)).unwrap(), 17);
        assert_eq!(logical_value_bytes(&Value::from(1.0_f64)).unwrap(), 17);
        assert_eq!(
            logical_value_bytes(&Value::decimal("1.25").unwrap()).unwrap(),
            13
        );
        assert_eq!(logical_value_bytes(&Value::from("é")).unwrap(), 11);
        assert_eq!(
            logical_value_bytes(&Value::InvalidText(vec![0x80])).unwrap(),
            10
        );
        assert_eq!(logical_value_bytes(&Value::from(vec![0, 255])).unwrap(), 11);
    }
}
