//! Protocol-neutral values and tabular query results.

use std::{error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Unknown,
    Null,
    Boolean,
    Int64,
    UInt64,
    Float64,
    Decimal,
    Text,
    Binary,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Decimal(Decimal),
    Text(String),
    InvalidText(Vec<u8>),
    Binary(Vec<u8>),
}

impl Value {
    pub const fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float64(_) => DataType::Float64,
            Self::Decimal(_) => DataType::Decimal,
            Self::Text(_) | Self::InvalidText(_) => DataType::Text,
            Self::Binary(_) => DataType::Binary,
        }
    }

    pub fn decimal(value: impl Into<String>) -> Result<Self, ParseDecimalError> {
        Decimal::parse(value).map(Self::Decimal)
    }

    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(value) => Some(*value),
            _ => None,
        }
    }

    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::UInt64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<&str> {
        match self {
            Self::Decimal(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_text_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Text(value) => Some(value.as_bytes()),
            Self::InvalidText(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(value) => Some(value),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::UInt64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
}

impl From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

/// One generated column value captured by the write that produced it.
///
/// Adapters must render the typed [`Value`] directly from this result instead
/// of consulting connection-local SQLite state after a pooled handle has been
/// released. Native-range IDs are represented as [`Value::Int64`]. Values
/// produced by the Engine always use the canonical logical catalog column.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GeneratedKey {
    /// Canonical logical column name from the table catalog.
    pub column: String,
    /// Generated value captured on the same physical SQLite connection.
    pub value: Value,
}

impl GeneratedKey {
    /// Construct a generated-key value.
    ///
    /// This protocol-neutral container does not validate `column`; Engine
    /// results provide the stronger canonical-catalog-name guarantee described
    /// on [`GeneratedKey`].
    pub fn new(column: impl Into<String>, value: Value) -> Self {
        Self {
            column: column.into(),
            value,
        }
    }
}

/// Protocol-neutral outcome of one logical write.
///
/// The native-range allocation contract accepts at most one automatically
/// generated row per statement, so `generated_key` is singular. Statements
/// with explicit keys, updates, and deletes return `None`. The current public
/// Engine SQL planner also returns `None` because omitted generated-key
/// planning remains roadmap issue #130.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WriteResult {
    /// Rows durably affected by the reconciled physical operation.
    pub rows_affected: usize,
    /// Generated key captured before the owning SQLite handle was released.
    pub generated_key: Option<GeneratedKey>,
}

impl WriteResult {
    /// Construct a write result that did not allocate a key.
    pub const fn without_generated_key(rows_affected: usize) -> Self {
        Self {
            rows_affected,
            generated_key: None,
        }
    }

    /// Construct a write result containing one generated key.
    pub fn with_generated_key(rows_affected: usize, generated_key: GeneratedKey) -> Self {
        Self {
            rows_affected,
            generated_key: Some(generated_key),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Decimal {
    representation: String,
}

impl Decimal {
    pub fn parse(value: impl Into<String>) -> Result<Self, ParseDecimalError> {
        let representation = value.into();
        if is_decimal_literal(&representation) {
            Ok(Self { representation })
        } else {
            Err(ParseDecimalError)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.representation
    }

    pub fn into_string(self) -> String {
        self.representation
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Decimal {
    type Err = ParseDecimalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDecimalError;

impl fmt::Display for ParseDecimalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid decimal literal")
    }
}

impl Error for ParseDecimalError {}

fn is_decimal_literal(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;

    if matches!(bytes.first(), Some(b'+' | b'-')) {
        index += 1;
    }

    let mut mantissa_digits = 0;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
        mantissa_digits += 1;
    }

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
            mantissa_digits += 1;
        }
    }

    if mantissa_digits == 0 {
        return false;
    }

    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }

        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return false;
        }
    }

    index == bytes.len()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSetShapeError {
    row_index: usize,
    expected: usize,
    actual: usize,
}

impl ResultSetShapeError {
    pub const fn row_index(&self) -> usize {
        self.row_index
    }

    pub const fn expected(&self) -> usize {
        self.expected
    }

    pub const fn actual(&self) -> usize {
        self.actual
    }
}

impl fmt::Display for ResultSetShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "row {} has {} values but the result set has {} columns",
            self.row_index, self.actual, self.expected
        )
    }
}

impl Error for ResultSetShapeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultSet {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

impl ResultSet {
    pub fn new(columns: Vec<Column>, rows: Vec<Row>) -> Result<Self, ResultSetShapeError> {
        if let Some((row_index, row)) = rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.len() != columns.len())
        {
            return Err(ResultSetShapeError {
                row_index,
                expected: columns.len(),
                actual: row.len(),
            });
        }
        Ok(Self { columns, rows })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn into_parts(self) -> (Vec<Column>, Vec<Row>) {
        (self.columns, self.rows)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_report_their_exact_protocol_neutral_types() {
        let cases = [
            (Value::Null, DataType::Null),
            (Value::from(true), DataType::Boolean),
            (Value::from(42_i64), DataType::Int64),
            (Value::from(42_u64), DataType::UInt64),
            (Value::from(1.5_f64), DataType::Float64),
            (Value::decimal("12.3400").unwrap(), DataType::Decimal),
            (Value::from("text"), DataType::Text),
            (Value::InvalidText(vec![0x80]), DataType::Text),
            (Value::from(vec![0_u8, 255]), DataType::Binary),
        ];

        for (value, expected) in cases {
            assert_eq!(value.data_type(), expected);
        }
    }

    #[test]
    fn typed_accessors_do_not_coerce_values() {
        assert_eq!(Value::from(42_i64).as_i64(), Some(42));
        assert_eq!(Value::from(42_u64).as_u64(), Some(42));
        assert_eq!(Value::from(42.0_f64).as_i64(), None);
        assert_eq!(
            Value::decimal("12.3400").unwrap().as_decimal(),
            Some("12.3400")
        );
        assert_eq!(Value::from("text").as_str(), Some("text"));
        assert_eq!(Value::InvalidText(vec![0x80]).as_str(), None);
        assert_eq!(
            Value::InvalidText(vec![b'f', 0x80]).as_text_bytes(),
            Some(&[b'f', 0x80][..])
        );
        assert_eq!(Value::from(true).as_str(), None);
        assert_eq!(Value::from(vec![1_u8, 2]).as_bytes(), Some(&[1, 2][..]));
        assert_eq!(Value::Null.as_bytes(), None);
    }

    #[test]
    fn write_results_carry_generated_keys_without_connection_state() {
        let ordinary = WriteResult::without_generated_key(2);
        assert_eq!(ordinary.rows_affected, 2);
        assert_eq!(ordinary.generated_key, None);

        let generated = WriteResult::with_generated_key(
            1,
            GeneratedKey::new("id", Value::Int64(0x4000_0000_0000_0001)),
        );
        assert_eq!(generated.rows_affected, 1);
        assert_eq!(generated.generated_key.unwrap().column, "id");
    }

    #[test]
    fn decimal_values_validate_and_preserve_exact_text() {
        for valid in [
            "0", "-0", "+12", "12.3400", "1.", ".5", "-.5", "1e3", "1.20E-4",
        ] {
            let decimal = Decimal::parse(valid).unwrap();
            assert_eq!(decimal.as_str(), valid);
            assert_eq!(decimal.to_string(), valid);
        }

        for invalid in [
            "", "+", "-", ".", "e1", "1e", "1e+", "1.2.3", " 1", "1 ", "NaN", "Infinity", "1_000",
        ] {
            assert_eq!(
                Decimal::parse(invalid).unwrap_err().to_string(),
                "invalid decimal literal"
            );
        }

        assert_eq!(
            "12.3400".parse::<Decimal>().unwrap().into_string(),
            "12.3400"
        );
    }

    #[test]
    fn result_sets_keep_ordered_columns_and_positional_rows() {
        let result = ResultSet::new(
            vec![
                Column::new("first", DataType::Unknown),
                Column::new("second", DataType::Unknown),
            ],
            vec![Row::new(vec![Value::from(1_i64), Value::from("two")])],
        )
        .unwrap();

        assert_eq!(result.columns()[0].name, "first");
        assert_eq!(result.columns()[1].name, "second");
        assert_eq!(result.rows()[0].get(0), Some(&Value::from(1_i64)));
        assert_eq!(result.rows()[0].get(1), Some(&Value::from("two")));
        assert_eq!(result.len(), 1);
        assert!(!result.is_empty());
        assert!(!result.rows()[0].is_empty());
    }

    #[test]
    fn result_sets_keep_duplicate_names_in_their_original_positions() {
        let result = ResultSet::new(
            vec![
                Column::new("duplicate", DataType::Int64),
                Column::new("middle", DataType::Text),
                Column::new("duplicate", DataType::Boolean),
            ],
            vec![Row::new(vec![
                Value::from(1_i64),
                Value::from("two"),
                Value::from(true),
            ])],
        )
        .unwrap();

        assert_eq!(
            result
                .columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["duplicate", "middle", "duplicate"]
        );
        assert_eq!(
            result.rows()[0].values(),
            [Value::from(1_i64), Value::from("two"), Value::from(true)]
        );
    }

    #[test]
    fn result_sets_accept_all_valid_empty_shapes() {
        let completely_empty = ResultSet::new(Vec::new(), Vec::new()).unwrap();
        assert!(completely_empty.columns().is_empty());
        assert!(completely_empty.rows().is_empty());

        let empty_row = ResultSet::new(Vec::new(), vec![Row::new(Vec::new())]).unwrap();
        assert!(empty_row.columns().is_empty());
        assert_eq!(empty_row.rows(), [Row::new(Vec::new())]);

        let metadata_only =
            ResultSet::new(vec![Column::new("value", DataType::Unknown)], Vec::new()).unwrap();
        assert_eq!(
            metadata_only.columns(),
            [Column::new("value", DataType::Unknown)]
        );
        assert!(metadata_only.rows().is_empty());
    }

    #[test]
    fn result_sets_reject_short_and_long_rows() {
        let columns = vec![
            Column::new("first", DataType::Unknown),
            Column::new("second", DataType::Unknown),
        ];

        let short = ResultSet::new(columns.clone(), vec![Row::new(vec![Value::Null])]).unwrap_err();
        assert_eq!(short.row_index(), 0);
        assert_eq!(short.expected(), 2);
        assert_eq!(short.actual(), 1);
        assert_eq!(
            short.to_string(),
            "row 0 has 1 values but the result set has 2 columns"
        );

        let long = ResultSet::new(
            columns,
            vec![Row::new(vec![Value::Null, Value::Null, Value::Null])],
        )
        .unwrap_err();
        assert_eq!(long.expected(), 2);
        assert_eq!(long.actual(), 3);
    }
}
