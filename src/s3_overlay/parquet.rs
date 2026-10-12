//! Real, uncompressed Parquet files with typed columns, PKs, and tombstones.
use super::{Cell, ColumnType, Result, Row, Table, corrupt, storage_error};
use bytes::Bytes;
use parquet::{
    data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type},
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    record::{Field, RowAccessor},
    schema::parser::parse_message_type,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) type Changes = BTreeMap<Vec<u8>, Option<Row>>;

fn schema(table: &Table) -> String {
    let columns = table
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let kind = match c.kind {
                ColumnType::Integer => "INT64",
                ColumnType::Real => "DOUBLE",
                _ => "BINARY",
            };
            let logical = if c.kind == ColumnType::Text {
                " (UTF8)"
            } else {
                ""
            };
            format!("OPTIONAL {kind} c{i}{logical};")
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "message briskdb_delta_v1 {{ REQUIRED BINARY briskdb_key; REQUIRED BOOLEAN briskdb_deleted; {columns} }}"
    )
}

pub(crate) fn encode(table: &Table, changes: &Changes) -> Result<Bytes> {
    if changes.is_empty() || changes.len() > 10_000 {
        return Err(super::limit("Parquet batch must have 1..10000 rows"));
    }
    let schema = Arc::new(parse_message_type(&schema(table)).map_err(storage_error)?);
    let mut writer = SerializedFileWriter::new(
        Vec::new(),
        schema,
        Arc::new(WriterProperties::builder().build()),
    )
    .map_err(storage_error)?;
    let mut group = writer.next_row_group().map_err(storage_error)?;
    let mut column = group
        .next_column()
        .map_err(storage_error)?
        .ok_or_else(|| corrupt("missing key writer"))?;
    let keys = changes
        .keys()
        .map(|k| ByteArray::from(k.as_slice()))
        .collect::<Vec<_>>();
    column
        .typed::<ByteArrayType>()
        .write_batch(&keys, None, None)
        .map_err(storage_error)?;
    column.close().map_err(storage_error)?;
    let mut column = group
        .next_column()
        .map_err(storage_error)?
        .ok_or_else(|| corrupt("missing tombstone writer"))?;
    column
        .typed::<BoolType>()
        .write_batch(
            &changes.values().map(Option::is_none).collect::<Vec<_>>(),
            None,
            None,
        )
        .map_err(storage_error)?;
    column.close().map_err(storage_error)?;
    for (i, definition) in table.columns.iter().enumerate() {
        let cells = changes
            .values()
            .map(|r| r.as_ref().and_then(|r| r.get(i)))
            .collect::<Vec<_>>();
        let levels = cells
            .iter()
            .map(|v| i16::from(v.is_some_and(|v| !matches!(v, Cell::Null))))
            .collect::<Vec<_>>();
        let mut column = group
            .next_column()
            .map_err(storage_error)?
            .ok_or_else(|| corrupt("missing column writer"))?;
        match definition.kind {
            ColumnType::Integer => {
                let values = cells
                    .iter()
                    .filter_map(|v| {
                        if let Some(Cell::Integer(n)) = v {
                            Some(*n)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                column
                    .typed::<Int64Type>()
                    .write_batch(&values, Some(&levels), None)
                    .map_err(storage_error)?;
            }
            ColumnType::Real => {
                let values = cells
                    .iter()
                    .filter_map(|v| {
                        if let Some(Cell::Real(n)) = v {
                            Some(*n)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                column
                    .typed::<DoubleType>()
                    .write_batch(&values, Some(&levels), None)
                    .map_err(storage_error)?;
            }
            ColumnType::Text | ColumnType::Blob => {
                let values = cells
                    .iter()
                    .filter_map(|v| match v {
                        Some(Cell::Text(n)) => Some(ByteArray::from(n.as_bytes())),
                        Some(Cell::Blob(n)) => Some(ByteArray::from(n.as_slice())),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                column
                    .typed::<ByteArrayType>()
                    .write_batch(&values, Some(&levels), None)
                    .map_err(storage_error)?;
            }
        }
        column.close().map_err(storage_error)?;
    }
    group.close().map_err(storage_error)?;
    let bytes = writer.into_inner().map_err(storage_error)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(super::limit("Parquet batch exceeds 16 MiB"));
    }
    Ok(Bytes::from(bytes))
}

pub(crate) fn decode(table: &Table, bytes: Bytes) -> Result<Changes> {
    let reader = SerializedFileReader::new(bytes).map_err(storage_error)?;
    let meta = reader.metadata().file_metadata();
    if !(1..=10_000).contains(&meta.num_rows())
        || meta.schema() != &parse_message_type(&schema(table)).map_err(storage_error)?
    {
        return Err(corrupt("Parquet schema/row bounds differ from catalog"));
    }
    let mut changes = Changes::new();
    let mut decoded_bytes = 0usize;
    for record in reader.get_row_iter(None).map_err(storage_error)? {
        let record = record.map_err(storage_error)?;
        let key = record.get_bytes(0).map_err(storage_error)?.data().to_vec();
        let deleted = record.get_bool(1).map_err(storage_error)?;
        let mut row = Vec::new();
        for (_, field) in record.get_column_iter().skip(2) {
            row.push(match field {
                Field::Null => Cell::Null,
                Field::Long(v) => Cell::Integer(*v),
                Field::Double(v) => Cell::Real(*v),
                Field::Str(v) => Cell::Text(v.clone()),
                Field::Bytes(v) => Cell::Blob(v.data().to_vec()),
                _ => return Err(corrupt("unexpected Parquet cell type")),
            });
        }
        if deleted {
            if row.iter().any(|v| *v != Cell::Null) {
                return Err(corrupt("tombstone has a payload"));
            }
        } else {
            table.check_row(&row)?;
            if key != table.key(&row) {
                return Err(corrupt("Parquet primary key differs from payload"));
            }
        }
        decoded_bytes += key.len() + row.iter().map(Cell::size).sum::<usize>();
        if decoded_bytes > super::MAX_BYTES {
            return Err(super::limit("decoded Parquet batch exceeds memory limit"));
        }
        if changes
            .insert(key, if deleted { None } else { Some(row) })
            .is_some()
        {
            return Err(corrupt("duplicate primary key within a Parquet batch"));
        }
    }
    Ok(changes)
}
