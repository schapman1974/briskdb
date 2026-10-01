use super::{Error, Layout, Mutation, Result, Store, format::FORMAT_VERSION};
use std::{collections::HashSet, path::Path};

const CATALOG_KEY_BYTES: u16 = 128;
const CATALOG_VALUE_BYTES: u16 = 1024;
const ROOT_KEY_PREFIX: &[u8; 2] = b"\0R";
const TABLE_KEY_PREFIX: &[u8; 2] = b"\0T";
const TABLE_KEY_END: &[u8; 2] = b"\0U";
const ROOT_MAGIC: &[u8; 8] = b"BRICAT01";
const TABLE_MAGIC: &[u8; 8] = b"BRITBL01";
const CATALOG_VERSION: u16 = 1;
const MAX_NAME_BYTES: usize = 63;
const MAX_COLUMNS: usize = 64;
const MAX_INDEXES: usize = 32;
const MAX_INDEX_COLUMNS: usize = 8;

/// Stable identity stored in the native catalog root record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogIdentity([u8; 16]);

impl CatalogIdentity {
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Types the native schema catalog can describe. This does not imply that the
/// record or query layer can yet encode or index every type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColumnType {
    Boolean = 1,
    Int64 = 2,
    UInt64 = 3,
    Float64 = 4,
    Text = 5,
    Binary = 6,
}

impl TryFrom<u8> for ColumnType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Boolean),
            2 => Ok(Self::Int64),
            3 => Ok(Self::UInt64),
            4 => Ok(Self::Float64),
            5 => Ok(Self::Text),
            6 => Ok(Self::Binary),
            _ => Err(Error::Corrupt("unknown native catalog column type")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDefinition {
    pub name: String,
    pub schema_version: u32,
    pub columns: Vec<ColumnDefinition>,
    pub indexes: Vec<IndexDefinition>,
}

impl TableDefinition {
    fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;
        if self.schema_version == 0
            || self.columns.is_empty()
            || self.columns.len() > MAX_COLUMNS
            || self.indexes.len() > MAX_INDEXES
        {
            return Err(Error::Invalid("invalid native table schema bounds"));
        }

        let mut column_names = HashSet::with_capacity(self.columns.len());
        for column in &self.columns {
            validate_name(&column.name)?;
            if !column_names.insert(column.name.as_str()) {
                return Err(Error::Invalid("duplicate native column name"));
            }
        }

        let mut index_names = HashSet::with_capacity(self.indexes.len());
        for index in &self.indexes {
            validate_name(&index.name)?;
            if !index_names.insert(index.name.as_str())
                || index.columns.is_empty()
                || index.columns.len() > MAX_INDEX_COLUMNS
            {
                return Err(Error::Invalid("invalid native index declaration"));
            }
            let mut indexed_columns = HashSet::with_capacity(index.columns.len());
            for name in &index.columns {
                if !column_names.contains(name.as_str()) || !indexed_columns.insert(name.as_str()) {
                    return Err(Error::Invalid("invalid native index column reference"));
                }
            }
        }
        Ok(())
    }
}

/// A small native-only schema catalog stored as records in the ISAM format.
///
/// Catalog index declarations are metadata only; they do not create physical
/// indexes or enforce uniqueness. SQLite is not opened by this type.
#[derive(Debug)]
pub struct NativeCatalog {
    store: Store,
    identity: CatalogIdentity,
}

impl NativeCatalog {
    /// Create a new catalog file. Existing files are never adopted or replaced.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let mut identity = [0; 16];
        getrandom::fill(&mut identity).map_err(std::io::Error::other)?;
        let mut store = Store::create(path, Layout::new(CATALOG_KEY_BYTES, CATALOG_VALUE_BYTES)?)?;
        let root = encode_root(CatalogIdentity(identity));
        store.write_batch(&[Mutation::insert(root_key().to_vec(), root)])?;
        Ok(Self {
            store,
            identity: CatalogIdentity(identity),
        })
    }

    /// Open and validate an existing native catalog without creating metadata.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut store = Store::open(path)?;
        if store.layout() != Layout::new(CATALOG_KEY_BYTES, CATALOG_VALUE_BYTES)? {
            return Err(Error::Corrupt("native catalog record layout mismatch"));
        }
        let read = store.read_batch()?;
        let root = read
            .get(&root_key())?
            .ok_or(Error::Corrupt("missing native catalog identity record"))?;
        let identity = decode_root(&root)?;
        Ok(Self { store, identity })
    }

    pub const fn identity(&self) -> CatalogIdentity {
        self.identity
    }

    /// Current ISAM root generation. Each catalog mutation advances it once.
    pub fn generation(&mut self) -> Result<u64> {
        Ok(self.store.read_batch()?.generation())
    }

    pub fn table(&mut self, name: &str) -> Result<Option<TableDefinition>> {
        validate_name(name)?;
        let key = table_key(name)?;
        let Some(value) = self.store.read_batch()?.get(&key)? else {
            return Ok(None);
        };
        let definition = decode_table(&value)?;
        if definition.name != name {
            return Err(Error::Corrupt("native catalog table key/name mismatch"));
        }
        Ok(Some(definition))
    }

    /// Create a table declaration atomically. Physical record/index support is
    /// deliberately outside this metadata-only catalog API.
    pub fn create_table(&mut self, definition: &TableDefinition) -> Result<()> {
        definition.validate()?;
        let key = table_key(&definition.name)?;
        let value = encode_table(definition)?;
        let root = encode_root(self.identity);
        self.store.write_batch(&[
            Mutation::insert(key.to_vec(), value),
            Mutation::put(root_key().to_vec(), root),
        ])
    }

    /// Remove a table declaration. Existing record/index data is not managed
    /// by this initial catalog slice.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        validate_name(name)?;
        let key = table_key(name)?;
        if self.store.read_batch()?.get(&key)?.is_none() {
            return Ok(());
        }
        let root = encode_root(self.identity);
        self.store.write_batch(&[
            Mutation::delete(key.to_vec()),
            Mutation::put(root_key().to_vec(), root),
        ])
    }

    pub fn tables(&mut self) -> Result<Vec<TableDefinition>> {
        let read = self.store.read_batch()?;
        let records = read.range(&table_key_start(), Some(&table_key_end()), 4096)?;
        if records.len() == 4096 {
            let mut next_start = records.last().unwrap().key.clone();
            *next_start.last_mut().unwrap() += 1;
            if !read
                .range(&next_start, Some(&table_key_end()), 1)?
                .is_empty()
            {
                return Err(Error::Invalid("native catalog table limit exceeded"));
            }
        }
        records
            .into_iter()
            .map(|record| {
                let definition = decode_table(&record.value)?;
                if table_key(&definition.name)?.to_vec() != record.key {
                    return Err(Error::Corrupt("native catalog table key/name mismatch"));
                }
                Ok(definition)
            })
            .collect()
    }
}

fn validate_name(name: &str) -> Result<()> {
    let mut bytes = name.bytes();
    if name.len() > MAX_NAME_BYTES
        || !bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(Error::Invalid("invalid native catalog identifier"));
    }
    Ok(())
}

fn root_key() -> [u8; CATALOG_KEY_BYTES as usize] {
    let mut key = [0; CATALOG_KEY_BYTES as usize];
    key[..ROOT_KEY_PREFIX.len()].copy_from_slice(ROOT_KEY_PREFIX);
    key
}

fn table_key(name: &str) -> Result<[u8; CATALOG_KEY_BYTES as usize]> {
    validate_name(name)?;
    let mut key = [0; CATALOG_KEY_BYTES as usize];
    key[..TABLE_KEY_PREFIX.len()].copy_from_slice(TABLE_KEY_PREFIX);
    key[TABLE_KEY_PREFIX.len()..TABLE_KEY_PREFIX.len() + name.len()]
        .copy_from_slice(name.as_bytes());
    Ok(key)
}

fn table_key_start() -> [u8; CATALOG_KEY_BYTES as usize] {
    let mut key = [0; CATALOG_KEY_BYTES as usize];
    key[..TABLE_KEY_PREFIX.len()].copy_from_slice(TABLE_KEY_PREFIX);
    key
}

fn table_key_end() -> [u8; CATALOG_KEY_BYTES as usize] {
    let mut key = [0; CATALOG_KEY_BYTES as usize];
    key[..TABLE_KEY_END.len()].copy_from_slice(TABLE_KEY_END);
    key
}

fn encode_root(identity: CatalogIdentity) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(28);
    bytes.extend_from_slice(ROOT_MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(identity.as_bytes());
    bytes
}

fn decode_root(bytes: &[u8]) -> Result<CatalogIdentity> {
    if bytes.len() != 28
        || &bytes[..8] != ROOT_MAGIC
        || u16::from_le_bytes([bytes[8], bytes[9]]) != FORMAT_VERSION
        || u16::from_le_bytes([bytes[10], bytes[11]]) != CATALOG_VERSION
    {
        return Err(Error::Corrupt("unknown or invalid native catalog root"));
    }
    Ok(CatalogIdentity(bytes[12..28].try_into().unwrap()))
}

fn encode_table(definition: &TableDefinition) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(TABLE_MAGIC);
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(&definition.schema_version.to_le_bytes());
    push_name(&mut bytes, &definition.name)?;
    bytes.push(definition.columns.len() as u8);
    for column in &definition.columns {
        push_name(&mut bytes, &column.name)?;
        bytes.push(column.column_type as u8);
        bytes.push(u8::from(column.nullable));
    }
    bytes.push(definition.indexes.len() as u8);
    for index in &definition.indexes {
        push_name(&mut bytes, &index.name)?;
        bytes.push(u8::from(index.unique));
        bytes.push(index.columns.len() as u8);
        for column in &index.columns {
            push_name(&mut bytes, column)?;
        }
    }
    if bytes.len() > usize::from(CATALOG_VALUE_BYTES) {
        return Err(Error::Invalid(
            "native catalog table schema exceeds record limit",
        ));
    }
    Ok(bytes)
}

fn decode_table(bytes: &[u8]) -> Result<TableDefinition> {
    let mut decoder = Decoder { bytes, offset: 0 };
    if decoder.take(8)? != TABLE_MAGIC || decoder.u16()? != CATALOG_VERSION {
        return Err(Error::Corrupt("unknown native catalog table format"));
    }
    let schema_version = decoder.u32()?;
    let name = decoder.name()?;
    let column_count = usize::from(decoder.u8()?);
    if column_count == 0 || column_count > MAX_COLUMNS {
        return Err(Error::Corrupt("invalid native catalog column count"));
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column_name = decoder.name()?;
        let column_type = ColumnType::try_from(decoder.u8()?)?;
        let nullable = match decoder.u8()? {
            0 => false,
            1 => true,
            _ => return Err(Error::Corrupt("invalid native catalog nullability flag")),
        };
        columns.push(ColumnDefinition {
            name: column_name,
            column_type,
            nullable,
        });
    }
    let index_count = usize::from(decoder.u8()?);
    if index_count > MAX_INDEXES {
        return Err(Error::Corrupt("invalid native catalog index count"));
    }
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        let index_name = decoder.name()?;
        let unique = match decoder.u8()? {
            0 => false,
            1 => true,
            _ => return Err(Error::Corrupt("invalid native catalog uniqueness flag")),
        };
        let column_count = usize::from(decoder.u8()?);
        if column_count == 0 || column_count > MAX_INDEX_COLUMNS {
            return Err(Error::Corrupt("invalid native catalog index column count"));
        }
        let mut index_columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            index_columns.push(decoder.name()?);
        }
        indexes.push(IndexDefinition {
            name: index_name,
            columns: index_columns,
            unique,
        });
    }
    if decoder.offset != bytes.len() {
        return Err(Error::Corrupt("trailing native catalog table bytes"));
    }
    let definition = TableDefinition {
        name,
        schema_version,
        columns,
        indexes,
    };
    definition
        .validate()
        .map_err(|_| Error::Corrupt("invalid native catalog table definition"))?;
    Ok(definition)
}

fn push_name(bytes: &mut Vec<u8>, name: &str) -> Result<()> {
    validate_name(name)?;
    bytes.push(name.len() as u8);
    bytes.extend_from_slice(name.as_bytes());
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Decoder<'_> {
    fn take(&mut self, length: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Error::Corrupt("truncated native catalog record"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn name(&mut self) -> Result<String> {
        let length = usize::from(self.u8()?);
        let bytes = self.take(length)?;
        let name = std::str::from_utf8(bytes)
            .map_err(|_| Error::Corrupt("invalid native catalog identifier encoding"))?;
        validate_name(name).map_err(|_| Error::Corrupt("invalid native catalog identifier"))?;
        Ok(name.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> TableDefinition {
        TableDefinition {
            name: "items".to_owned(),
            schema_version: 3,
            columns: vec![
                ColumnDefinition {
                    name: "id".to_owned(),
                    column_type: ColumnType::UInt64,
                    nullable: false,
                },
                ColumnDefinition {
                    name: "label".to_owned(),
                    column_type: ColumnType::Text,
                    nullable: true,
                },
            ],
            indexes: vec![IndexDefinition {
                name: "by_label".to_owned(),
                columns: vec!["label".to_owned()],
                unique: false,
            }],
        }
    }

    #[test]
    fn table_record_codec_round_trips_and_rejects_truncation() {
        let encoded = encode_table(&definition()).unwrap();
        assert_eq!(decode_table(&encoded).unwrap(), definition());
        for end in 0..encoded.len() {
            assert!(matches!(
                decode_table(&encoded[..end]),
                Err(Error::Corrupt(_))
            ));
        }
    }

    #[test]
    fn root_record_rejects_unknown_versions_and_lengths() {
        let identity = CatalogIdentity([7; 16]);
        let mut encoded = encode_root(identity);
        assert_eq!(decode_root(&encoded).unwrap(), identity);
        encoded[10] = CATALOG_VERSION.wrapping_add(1) as u8;
        assert!(matches!(decode_root(&encoded), Err(Error::Corrupt(_))));
        assert!(matches!(
            decode_root(&encoded[..encoded.len() - 1]),
            Err(Error::Corrupt(_))
        ));
    }
}
