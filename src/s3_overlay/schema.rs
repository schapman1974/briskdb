use super::{Result, invalid};
use rusqlite::types::{ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Lossless SQLite scalar values in the optional overlay SQL API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Cell {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Cell {
    pub(crate) fn from_sql(value: ValueRef<'_>) -> rusqlite::Result<Self> {
        Ok(match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(v) => Self::Integer(v),
            ValueRef::Real(v) if v.is_finite() => Self::Real(v),
            ValueRef::Real(_) => {
                return Err(super::vtab::error("non-finite numbers are unsupported"));
            }
            ValueRef::Text(v) => Self::Text(
                std::str::from_utf8(v)
                    .map_err(|_| super::vtab::error("text must be valid UTF-8"))?
                    .into(),
            ),
            ValueRef::Blob(v) => Self::Blob(v.to_vec()),
        })
    }

    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Text(v) => v.len(),
            Self::Blob(v) => v.len(),
            _ => 8,
        }
    }
}

impl ToSql for Cell {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(v) => ValueRef::Integer(*v),
            Self::Real(v) => ValueRef::Real(*v),
            Self::Text(v) => ValueRef::Text(v.as_bytes()),
            Self::Blob(v) => ValueRef::Blob(v),
        }))
    }
}

/// A table row, in declared column order.
pub type Row = Vec<Cell>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Integer,
    Real,
    Text,
    Blob,
}

impl ColumnType {
    pub(crate) fn sql(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub kind: ColumnType,
    pub nullable: bool,
}

/// Immutable SQL schema. A primary key must contain the routing column.
/// Additional indexes are non-unique; global unique indexes, generated IDs,
/// triggers, foreign keys, and online DDL are rejected/not exposed in this mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub shard_key: String,
    pub indexes: Vec<Vec<String>>,
}

pub(crate) fn identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.as_bytes()[0].is_ascii_alphabetic()
        && name.bytes().all(|v| v.is_ascii_alphanumeric() || v == b'_')
        && !name.to_ascii_lowercase().starts_with("sqlite_")
        && !["rowid", "oid", "_rowid_"].contains(&name.to_ascii_lowercase().as_str())
}

pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl Table {
    pub(crate) fn validate(&self) -> Result<()> {
        if !identifier(&self.name)
            || !(1..=64).contains(&self.columns.len())
            || !(1..=8).contains(&self.primary_key.len())
            || self.indexes.len() > 32
        {
            return Err(invalid("invalid overlay table bounds/name"));
        }
        let mut names = HashSet::new();
        for column in &self.columns {
            if !identifier(&column.name) || !names.insert(column.name.to_ascii_lowercase()) {
                return Err(invalid("invalid or duplicate column name"));
            }
        }
        let mut primary = HashSet::new();
        for name in &self.primary_key {
            let column = self
                .columns
                .iter()
                .find(|c| c.name == *name)
                .ok_or_else(|| invalid("primary key column is missing"))?;
            if column.nullable
                || !matches!(
                    column.kind,
                    ColumnType::Integer | ColumnType::Text | ColumnType::Blob
                )
                || !primary.insert(name)
            {
                return Err(invalid(
                    "primary key columns must be distinct, non-null INTEGER/TEXT/BLOB",
                ));
            }
        }
        if !self.primary_key.contains(&self.shard_key) {
            return Err(invalid(
                "primary key must include shard key for partition-local uniqueness",
            ));
        }
        for index in &self.indexes {
            let mut seen = HashSet::new();
            if index.is_empty()
                || index.len() > 8
                || index
                    .iter()
                    .any(|v| !self.columns.iter().any(|c| c.name == *v) || !seen.insert(v))
            {
                return Err(invalid("invalid overlay index columns"));
            }
        }
        Ok(())
    }

    pub(crate) fn routing_column(&self) -> usize {
        self.columns
            .iter()
            .position(|v| v.name == self.shard_key)
            .expect("validated schema")
    }

    pub(crate) fn check_row(&self, row: &[Cell]) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(invalid("row length differs from schema"));
        }
        if row.iter().map(Cell::size).sum::<usize>() > 1024 * 1024 {
            return Err(super::limit("row exceeds 1 MiB"));
        }
        for (column, value) in self.columns.iter().zip(row) {
            let valid = match (column.kind, value) {
                (_, Cell::Null) => column.nullable,
                (ColumnType::Integer, Cell::Integer(_))
                | (ColumnType::Text, Cell::Text(_))
                | (ColumnType::Blob, Cell::Blob(_)) => true,
                (ColumnType::Real, Cell::Real(v)) => v.is_finite(),
                _ => false,
            };
            if !valid {
                return Err(invalid(format!(
                    "invalid type/null for column {}",
                    column.name
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn key(&self, row: &[Cell]) -> Vec<u8> {
        let values = self
            .primary_key
            .iter()
            .map(|name| {
                &row[self
                    .columns
                    .iter()
                    .position(|v| v.name == *name)
                    .expect("validated primary key")]
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&values).expect("finite validated scalar values")
    }

    pub(crate) fn columns_sql(&self) -> String {
        self.columns
            .iter()
            .map(|c| {
                format!(
                    "{} {}{}",
                    quote(&c.name),
                    c.kind.sql(),
                    if c.nullable { "" } else { " NOT NULL" }
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn create_sql(&self) -> String {
        format!(
            "CREATE TABLE {} ({}, PRIMARY KEY ({})) WITHOUT ROWID, STRICT",
            quote(&self.name),
            self.columns_sql(),
            self.primary_key
                .iter()
                .map(|s| quote(s))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    pub(crate) fn select_columns(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(",")
    }
}
