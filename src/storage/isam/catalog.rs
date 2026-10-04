use super::{Error, Layout, Mutation, Result, Store};
use crate::core::{CanonicalIndexKey, IndexKeyPart, IndexKeyValueRef, UniqueNullSemantics};
use std::{collections::HashSet, path::Path};

const CATALOG_KEY_BYTES: u16 = 128;
const CATALOG_VALUE_BYTES: u16 = 1024;
const ROOT_KEY_PREFIX: &[u8; 2] = b"\0R";
const TABLE_KEY_PREFIX: &[u8; 2] = b"\0T";
const TABLE_KEY_END: &[u8; 2] = b"\0U";
const ROOT_MAGIC: &[u8; 8] = b"BRICAT01";
const TABLE_MAGIC_V1: &[u8; 8] = b"BRITBL01";
const TABLE_MAGIC_V2: &[u8; 8] = b"BRITBL02";
const ROW_MAGIC: &[u8; 8] = b"BRIROW01";
const CATALOG_VERSION: u16 = 1;
const TABLE_VERSION_V1: u16 = 1;
const TABLE_VERSION_V2: u16 = 2;
const ROW_VERSION: u16 = 1;
const MAX_NAME_BYTES: usize = 63;
const MAX_COLUMNS: usize = 64;
const MAX_INDEXES: usize = 32;
const MAX_INDEX_COLUMNS: usize = 8;
const DATA_KEY_PREFIX: &[u8; 2] = b"\0D";
const INDEX_KEY_PREFIX: &[u8; 2] = b"\0I";
const MAX_PRIMARY_KEY_COLUMNS: usize = 8;
const MAX_ROW_BYTES: usize = 1024;

/// Stable identity stored in the native catalog root record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogIdentity([u8; 16]);

impl CatalogIdentity {
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// Scalar types supported by the native row codec and secondary indexes.
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
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NativeValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Text(String),
    Binary(Vec<u8>),
}

impl TableDefinition {
    fn validate(&self) -> Result<()> {
        validate_name(&self.name)?;
        if self.schema_version == 0
            || self.columns.is_empty()
            || self.columns.len() > MAX_COLUMNS
            || self.primary_key.len() > MAX_PRIMARY_KEY_COLUMNS
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

        let mut primary_columns = HashSet::with_capacity(self.primary_key.len());
        for name in &self.primary_key {
            if !column_names.contains(name.as_str()) || !primary_columns.insert(name.as_str()) {
                return Err(Error::Invalid("invalid native primary key column"));
            }
        }

        let mut index_names = HashSet::with_capacity(self.indexes.len());
        for index in &self.indexes {
            validate_name(&index.name)?;
            if !index_names.insert(index.name.as_str())
                || index.columns.is_empty()
                || index.columns.len() > MAX_INDEX_COLUMNS
                || self.name.len() + index.name.len() > 108
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

/// A native-only schema catalog and typed row/index layer stored in the ISAM
/// format. It does not open SQLite or integrate with SQL/document execution.
#[derive(Debug)]
pub struct NativeCatalog {
    store: Store,
    identity: CatalogIdentity,
}

impl NativeCatalog {
    /// Create a new catalog file. Existing files are never adopted or replaced.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_inner(path.as_ref(), super::format::FORMAT_VERSION)
    }

    /// Create a new native catalog using opt-in, uncompressed v3 packed pages.
    pub fn create_packed(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_inner(path.as_ref(), super::format::PACKED_FORMAT_VERSION)
    }

    /// New opt-in v4 catalog: packed pages and pipelined durable commits.
    /// This does not convert existing files or qualify NFS/EFS operation.
    pub fn create_pipelined(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_inner(path.as_ref(), super::format::PIPELINED_FORMAT_VERSION)
    }

    fn create_inner(path: &Path, version: u16) -> Result<Self> {
        let mut identity = [0; 16];
        getrandom::fill(&mut identity).map_err(std::io::Error::other)?;
        let layout = Layout::new(CATALOG_KEY_BYTES, CATALOG_VALUE_BYTES)?;
        let mut store = Store::create_inner(path, layout, version)?;
        let root = encode_root(CatalogIdentity(identity), store.format_version());
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
        let identity = decode_root(&root, read.snapshot.format_version)?;
        Ok(Self { store, identity })
    }

    pub const fn identity(&self) -> CatalogIdentity {
        self.identity
    }

    /// Return logical operation counts and timings for the underlying ISAM file.
    pub fn operation_stats(&self) -> super::OperationStats {
        self.store.operation_stats()
    }

    /// Reset logical operation counts without changing filesystem cache state.
    pub fn reset_operation_stats(&mut self) {
        self.store.reset_operation_stats();
    }

    /// Configure bounded lock admission for subsequent catalog operations.
    /// This never retries a mutation with an uncertain commit outcome.
    pub fn set_lock_policy(&mut self, policy: super::LockPolicy) {
        self.store.set_lock_policy(policy);
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

    /// Create a table declaration atomically.
    pub fn create_table(&mut self, definition: &TableDefinition) -> Result<()> {
        definition.validate()?;
        let key = table_key(&definition.name)?;
        let value = encode_table(definition)?;
        let root = encode_root(self.identity, self.store.format_version());
        self.store.write_batch(&[
            Mutation::insert(key.to_vec(), value),
            Mutation::put(root_key().to_vec(), root),
        ])
    }

    /// Remove an empty table declaration while excluding concurrent row writes.
    /// Nonempty tables are refused.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        validate_name(name)?;
        loop {
            let Some(definition) = self.table(name)? else {
                return Ok(());
            };
            let key = table_key(name)?.to_vec();
            let expected_metadata = encode_table(&definition)?;
            let root = encode_root(self.identity, self.store.format_version());
            let mutations = [
                Mutation::delete(key.clone()),
                Mutation::put(root_key().to_vec(), root),
            ];
            let dropped = self
                .store
                .write_batch_checked_exclusive_legacy(&mutations, |read| {
                    if read.get(&key)?.as_deref() != Some(expected_metadata.as_slice()) {
                        return Ok(false);
                    }
                    let data_prefix = table_data_prefix(name)?;
                    let data_end = prefix_ceiling(&data_prefix.key, data_prefix.used_bytes);
                    let index_prefix = table_index_prefix(name)?;
                    let index_end = prefix_ceiling(&index_prefix.key, index_prefix.used_bytes);
                    if !read.range(&data_prefix.key, Some(&data_end), 1)?.is_empty()
                        || !read
                            .range(&index_prefix.key, Some(&index_end), 1)?
                            .is_empty()
                    {
                        return Err(Error::Invalid("cannot drop a nonempty native table"));
                    }
                    Ok(true)
                })?;
            if dropped {
                return Ok(());
            }
        }
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

    /// Insert one typed row and all declared secondary-index entries atomically.
    pub fn insert_row(&mut self, table_name: &str, values: &[NativeValue]) -> Result<()> {
        self.insert_rows(table_name, &[values.to_vec()])
    }

    /// Insert multiple typed rows and their secondary-index entries in one
    /// atomic durable publication. Empty input is a no-op.
    pub fn insert_rows(&mut self, table_name: &str, rows: &[Vec<NativeValue>]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        if rows.len() > super::MAX_BATCH_RECORDS {
            return Err(Error::Invalid("native row batch exceeds record limit"));
        }
        loop {
            let definition = self
                .table(table_name)?
                .ok_or(Error::Invalid("native table does not exist"))?;
            let mut mutations = Vec::with_capacity(rows.len());
            for values in rows {
                let value = encode_row(&definition, values)?;
                let primary = encode_primary_key(&definition, values)?;
                let row_key = data_key(&definition.name, &primary)?;
                mutations.push(Mutation::insert(row_key.to_vec(), value));
                if mutations.len() > super::MAX_BATCH_RECORDS {
                    return Err(Error::Invalid("native row batch exceeds record limit"));
                }
                for index in &definition.indexes {
                    if let Some(entry) =
                        index_entry(&definition, index, values, &primary, &row_key)?
                    {
                        mutations.push(Mutation::insert(entry.key.to_vec(), entry.value));
                        if mutations.len() > super::MAX_BATCH_RECORDS {
                            return Err(Error::Invalid("native row batch exceeds record limit"));
                        }
                    }
                }
            }

            let metadata_key = table_key(&definition.name)?.to_vec();
            let expected_metadata = encode_table(&definition)?;
            let committed = self.store.write_batch_checked(&mutations, &[], |read| {
                Ok(read.get(&metadata_key)?.as_deref() == Some(expected_metadata.as_slice()))
            })?;
            if committed {
                return Ok(());
            }
        }
    }

    /// Read one row by its non-NULL primary-key values.
    pub fn get_row(
        &mut self,
        table_name: &str,
        primary_values: &[NativeValue],
    ) -> Result<Option<Vec<NativeValue>>> {
        let definition = self
            .table(table_name)?
            .ok_or(Error::Invalid("native table does not exist"))?;
        let primary = encode_primary_values(&definition, primary_values)?;
        let key = data_key(&definition.name, &primary)?;
        let read = self.store.read_batch()?;
        read.get(&key)?
            .map(|bytes| decode_row(&definition, &bytes))
            .transpose()
    }

    /// Atomically replace a row and its index entries, optionally moving its
    /// primary key. Returns false when the old primary key is absent.
    pub fn update_row(
        &mut self,
        table_name: &str,
        old_primary_values: &[NativeValue],
        new_values: &[NativeValue],
    ) -> Result<bool> {
        self.update_rows(
            table_name,
            &[(old_primary_values.to_vec(), new_values.to_vec())],
        )
    }

    /// Atomically update a set of rows and all affected secondary indexes.
    /// Every old key must exist; otherwise no rows are changed and false is
    /// returned. Conflicting target keys reject the whole batch.
    pub fn update_rows(
        &mut self,
        table_name: &str,
        updates: &[(Vec<NativeValue>, Vec<NativeValue>)],
    ) -> Result<bool> {
        if updates.is_empty() {
            return Ok(true);
        }
        if updates.len() > super::MAX_BATCH_RECORDS / 2 {
            return Err(Error::Invalid("native row batch exceeds record limit"));
        }
        loop {
            let definition = self
                .table(table_name)?
                .ok_or(Error::Invalid("native table does not exist"))?;
            let read = self.store.read_batch()?;
            let mut original_rows = Vec::with_capacity(updates.len());
            let mut new_rows = Vec::with_capacity(updates.len());
            let mut seen_old_keys = HashSet::with_capacity(updates.len());
            for (old_primary_values, new_values) in updates {
                let new_row = encode_row(&definition, new_values)?;
                let old_primary = encode_primary_values(&definition, old_primary_values)?;
                let new_primary = encode_primary_key(&definition, new_values)?;
                if !seen_old_keys.insert(old_primary.clone()) {
                    return Err(Error::Invalid("duplicate native row update key"));
                }
                let old_key = data_key(&definition.name, &old_primary)?;
                let Some(old_bytes) = read.get(&old_key)? else {
                    return Ok(false);
                };
                let old_row = decode_row(&definition, &old_bytes)?;
                let old_entries = index_entries(&definition, &old_row, &old_primary, &old_key)?;
                let new_key = data_key(&definition.name, &new_primary)?;
                let new_entries = index_entries(&definition, new_values, &new_primary, &new_key)?;
                original_rows.push((old_primary, old_key, old_bytes, old_entries));
                new_rows.push((new_primary, new_key, new_row, new_entries));
            }
            drop(read);
            let metadata_key = table_key(&definition.name)?.to_vec();
            let expected_metadata = encode_table(&definition)?;

            let mut mutations = Vec::with_capacity(updates.len().min(super::MAX_BATCH_RECORDS));
            // Keep unchanged index entries in place. Row stripes still guard
            // each owner and the validator checks every old index entry. An
            // entry whose owner moves is not unchanged, even if its key is.
            for ((_, old_key, _, old_entries), (_, _, _, new_entries)) in
                original_rows.iter().zip(&new_rows)
            {
                mutations.push(Mutation::delete(old_key.to_vec()));
                mutations.extend(
                    old_entries
                        .iter()
                        .zip(new_entries)
                        .filter(|(old, new)| old != new)
                        .filter_map(|(old, _)| old.as_ref())
                        .map(|entry| Mutation::delete(entry.key.to_vec())),
                );
                if mutations.len() > super::MAX_BATCH_RECORDS {
                    return Err(Error::Invalid("native row batch exceeds record limit"));
                }
            }
            for ((_, _, _, old_entries), (_, new_key, new_row, new_entries)) in
                original_rows.iter().zip(&new_rows)
            {
                mutations.push(Mutation::insert(new_key.to_vec(), new_row.clone()));
                mutations.extend(
                    new_entries
                        .iter()
                        .zip(old_entries)
                        .filter(|(new, old)| new != old)
                        .filter_map(|(new, _)| new.as_ref())
                        .map(|entry| Mutation::insert(entry.key.to_vec(), entry.value.clone())),
                );
                if mutations.len() > super::MAX_BATCH_RECORDS {
                    return Err(Error::Invalid("native row batch exceeds record limit"));
                }
            }

            let unchanged = self.store.write_batch_checked(&mutations, &[], |current| {
                if current.get(&metadata_key)?.as_deref() != Some(expected_metadata.as_slice()) {
                    return Ok(false);
                }
                for (_, old_key, old_bytes, old_entries) in &original_rows {
                    if current.get(old_key)?.as_deref() != Some(old_bytes.as_slice()) {
                        return Ok(false);
                    }
                    validate_index_entries(current, old_entries)?;
                }
                Ok(true)
            })?;
            if unchanged {
                return Ok(true);
            }
        }
    }

    /// Delete one row and its index entries. Returns false when absent.
    pub fn delete_row(&mut self, table_name: &str, primary_values: &[NativeValue]) -> Result<bool> {
        self.delete_rows(table_name, &[primary_values.to_vec()])
    }

    /// Atomically delete a set of rows and all their secondary-index entries.
    /// Every primary key must exist; otherwise no rows are changed and false is
    /// returned. Duplicate input keys are rejected.
    pub fn delete_rows(
        &mut self,
        table_name: &str,
        primary_values: &[Vec<NativeValue>],
    ) -> Result<bool> {
        if primary_values.is_empty() {
            return Ok(true);
        }
        if primary_values.len() > super::MAX_BATCH_RECORDS {
            return Err(Error::Invalid("native row batch exceeds record limit"));
        }
        loop {
            let definition = self
                .table(table_name)?
                .ok_or(Error::Invalid("native table does not exist"))?;
            let read = self.store.read_batch()?;
            let mut original_rows = Vec::with_capacity(primary_values.len());
            let mut seen_keys = HashSet::with_capacity(primary_values.len());
            for key_values in primary_values {
                let primary = encode_primary_values(&definition, key_values)?;
                if !seen_keys.insert(primary.clone()) {
                    return Err(Error::Invalid("duplicate native row delete key"));
                }
                let key = data_key(&definition.name, &primary)?;
                let Some(old_bytes) = read.get(&key)? else {
                    return Ok(false);
                };
                let row = decode_row(&definition, &old_bytes)?;
                let entries = index_entries(&definition, &row, &primary, &key)?;
                original_rows.push((key, old_bytes, entries));
            }
            drop(read);
            let metadata_key = table_key(&definition.name)?.to_vec();
            let expected_metadata = encode_table(&definition)?;
            let mut mutations =
                Vec::with_capacity(primary_values.len().min(super::MAX_BATCH_RECORDS));
            for (key, _, entries) in &original_rows {
                mutations.push(Mutation::delete(key.to_vec()));
                mutations.extend(
                    entries
                        .iter()
                        .flatten()
                        .map(|entry| Mutation::delete(entry.key.to_vec())),
                );
                if mutations.len() > super::MAX_BATCH_RECORDS {
                    return Err(Error::Invalid("native row batch exceeds record limit"));
                }
            }
            let deleted = self.store.write_batch_checked(&mutations, &[], |current| {
                if current.get(&metadata_key)?.as_deref() != Some(expected_metadata.as_slice()) {
                    return Ok(false);
                }
                for (key, old_bytes, entries) in &original_rows {
                    if current.get(key)?.as_deref() != Some(old_bytes.as_slice()) {
                        return Ok(false);
                    }
                    validate_index_entries(current, entries)?;
                }
                Ok(true)
            })?;
            if deleted {
                return Ok(true);
            }
        }
    }

    /// Exact secondary-index lookup. Unique indexes omit NULL keys using
    /// SQL-style distinct-NULL semantics; non-unique indexes retain NULL.
    pub fn lookup_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        values: &[NativeValue],
        limit: usize,
    ) -> Result<Vec<Vec<NativeValue>>> {
        if limit > super::MAX_BATCH_RECORDS {
            return Err(Error::Invalid("native index result limit exceeded"));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let definition = self
            .table(table_name)?
            .ok_or(Error::Invalid("native table does not exist"))?;
        let index = definition
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .ok_or(Error::Invalid("native index does not exist"))?;
        let canonical = canonical_index_values(&definition, index, values, true)?;
        let Some(canonical) = canonical else {
            return Ok(Vec::new());
        };
        let prefix = index_key_prefix(&definition.name, &index.name, &canonical)?;
        let read = self.store.read_batch()?;
        if index.unique {
            let key = unique_index_key(&prefix)?;
            let Some(data_key) = read.get(&key)? else {
                return Ok(Vec::new());
            };
            let Some(row) = read.get(&data_key)? else {
                return Err(Error::Corrupt("native unique index points to missing row"));
            };
            return Ok(vec![decode_row(&definition, &row)?]);
        }

        let start = prefix.key;
        let end = prefix_ceiling(&start, prefix.used_bytes);
        read.range(&start, Some(&end), limit)?
            .into_iter()
            .map(|entry| {
                let row = read
                    .get(&entry.value)?
                    .ok_or(Error::Corrupt("native index points to missing row"))?;
                decode_row(&definition, &row)
            })
            .collect()
    }

    /// Ordered composite-index scan with an inclusive lower key and exclusive
    /// upper key, bounded by the caller's row limit.
    pub fn range_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        start_values: &[NativeValue],
        end_values: Option<&[NativeValue]>,
        limit: usize,
    ) -> Result<Vec<Vec<NativeValue>>> {
        if limit > super::MAX_BATCH_RECORDS {
            return Err(Error::Invalid("native index result limit exceeded"));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let definition = self
            .table(table_name)?
            .ok_or(Error::Invalid("native table does not exist"))?;
        let index = definition
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .ok_or(Error::Invalid("native index does not exist"))?;
        let start_canonical = canonical_index_values(&definition, index, start_values, false)?
            .ok_or(Error::Corrupt("missing native index range start"))?;
        let start_prefix = index_key_prefix(&definition.name, &index.name, &start_canonical)?;
        let end = if let Some(end_values) = end_values {
            let end_canonical = canonical_index_values(&definition, index, end_values, false)?
                .ok_or(Error::Corrupt("missing native index range end"))?;
            if start_canonical > end_canonical {
                return Err(Error::Invalid("reversed native index range"));
            }
            let end_prefix = index_key_prefix(&definition.name, &index.name, &end_canonical)?;
            end_prefix.key
        } else {
            let index_prefix = named_index_prefix(&definition.name, &index.name)?;
            prefix_ceiling(&index_prefix.key, index_prefix.used_bytes)
        };
        let read = self.store.read_batch()?;
        read.range(&start_prefix.key, Some(&end), limit)?
            .into_iter()
            .map(|entry| {
                let row = read
                    .get(&entry.value)?
                    .ok_or(Error::Corrupt("native index points to missing row"))?;
                decode_row(&definition, &row)
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

struct EncodedPrefix {
    key: [u8; CATALOG_KEY_BYTES as usize],
    used_bytes: usize,
}

#[derive(PartialEq, Eq)]
struct IndexEntry {
    key: [u8; CATALOG_KEY_BYTES as usize],
    value: Vec<u8>,
}

fn data_key(table: &str, primary: &[u8]) -> Result<[u8; CATALOG_KEY_BYTES as usize]> {
    let mut bytes = Vec::with_capacity(3 + table.len() + primary.len());
    bytes.extend_from_slice(DATA_KEY_PREFIX);
    push_name(&mut bytes, table)?;
    push_length_prefixed(&mut bytes, primary)?;
    fixed_key(bytes)
}

fn table_data_prefix(table: &str) -> Result<EncodedPrefix> {
    let mut bytes = Vec::with_capacity(3 + table.len());
    bytes.extend_from_slice(DATA_KEY_PREFIX);
    push_name(&mut bytes, table)?;
    let used_bytes = bytes.len();
    Ok(EncodedPrefix {
        key: fixed_key(bytes)?,
        used_bytes,
    })
}

fn table_index_prefix(table: &str) -> Result<EncodedPrefix> {
    let mut bytes = Vec::with_capacity(3 + table.len());
    bytes.extend_from_slice(INDEX_KEY_PREFIX);
    push_name(&mut bytes, table)?;
    let used_bytes = bytes.len();
    Ok(EncodedPrefix {
        key: fixed_key(bytes)?,
        used_bytes,
    })
}

fn named_index_prefix(table: &str, index: &str) -> Result<EncodedPrefix> {
    let mut bytes = Vec::with_capacity(4 + table.len() + index.len());
    bytes.extend_from_slice(INDEX_KEY_PREFIX);
    push_name(&mut bytes, table)?;
    push_name(&mut bytes, index)?;
    let used_bytes = bytes.len();
    Ok(EncodedPrefix {
        key: fixed_key(bytes)?,
        used_bytes,
    })
}

fn index_key_prefix(table: &str, index: &str, canonical: &[u8]) -> Result<EncodedPrefix> {
    let mut prefix = named_index_prefix(table, index)?;
    let mut bytes = prefix.key[..prefix.used_bytes].to_vec();
    bytes.extend_from_slice(&ordered_frame(canonical));
    prefix.used_bytes = bytes.len();
    prefix.key = fixed_key(bytes)?;
    Ok(prefix)
}

fn unique_index_key(prefix: &EncodedPrefix) -> Result<[u8; CATALOG_KEY_BYTES as usize]> {
    Ok(prefix.key)
}

fn nonunique_index_key(
    prefix: &EncodedPrefix,
    primary: &[u8],
) -> Result<[u8; CATALOG_KEY_BYTES as usize]> {
    let mut bytes = prefix.key[..prefix.used_bytes].to_vec();
    bytes.extend_from_slice(&ordered_frame(primary));
    fixed_key(bytes)
}

fn ordered_frame(bytes: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(bytes.len() + 2);
    for byte in bytes {
        if *byte == 0 {
            frame.extend_from_slice(&[0, u8::MAX]);
        } else {
            frame.push(*byte);
        }
    }
    frame.extend_from_slice(&[0, 0]);
    frame
}

fn fixed_key(bytes: Vec<u8>) -> Result<[u8; CATALOG_KEY_BYTES as usize]> {
    if bytes.len() > CATALOG_KEY_BYTES as usize {
        return Err(Error::Invalid("native key exceeds fixed key-width limit"));
    }
    let mut key = [0; CATALOG_KEY_BYTES as usize];
    key[..bytes.len()].copy_from_slice(&bytes);
    Ok(key)
}

fn push_length_prefixed(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u16::try_from(value.len())
        .map_err(|_| Error::Invalid("native key component exceeds encoding limit"))?;
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(value);
    Ok(())
}

fn prefix_ceiling(
    prefix: &[u8; CATALOG_KEY_BYTES as usize],
    used_bytes: usize,
) -> [u8; CATALOG_KEY_BYTES as usize] {
    let mut end = *prefix;
    end[used_bytes..].fill(u8::MAX);
    end
}

fn column<'a>(definition: &'a TableDefinition, name: &str) -> Result<&'a ColumnDefinition> {
    definition
        .columns
        .iter()
        .find(|column| column.name == name)
        .ok_or(Error::Corrupt("native schema references missing column"))
}

fn encode_primary_key(definition: &TableDefinition, values: &[NativeValue]) -> Result<Vec<u8>> {
    if definition.primary_key.is_empty() {
        return Err(Error::Invalid("native table has no declared primary key"));
    }
    let primary_values = definition
        .primary_key
        .iter()
        .map(|name| {
            let position = definition
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or(Error::Corrupt(
                    "native primary key references missing column",
                ))?;
            values
                .get(position)
                .cloned()
                .ok_or(Error::Invalid("native row has wrong column count"))
        })
        .collect::<Result<Vec<_>>>()?;
    encode_primary_values(definition, &primary_values)
}

fn encode_primary_values(definition: &TableDefinition, values: &[NativeValue]) -> Result<Vec<u8>> {
    if definition.primary_key.is_empty() || values.len() != definition.primary_key.len() {
        return Err(Error::Invalid(
            "native primary key has wrong component count",
        ));
    }
    let mut parts = Vec::with_capacity(values.len());
    for (name, value) in definition.primary_key.iter().zip(values) {
        let column = column(definition, name)?;
        check_column_value(column, value)?;
        if matches!(value, NativeValue::Null) {
            return Err(Error::Invalid("native primary key cannot contain NULL"));
        }
        parts.push(IndexKeyPart::ascending(index_value(value)));
    }
    CanonicalIndexKey::encode(&parts)
        .map(CanonicalIndexKey::into_bytes)
        .map_err(|_| Error::Invalid("unsupported native primary key value"))
}

fn canonical_index_values(
    definition: &TableDefinition,
    index: &IndexDefinition,
    values: &[NativeValue],
    omit_unique_nulls: bool,
) -> Result<Option<Vec<u8>>> {
    if values.len() != index.columns.len() {
        return Err(Error::Invalid("native index has wrong component count"));
    }
    let mut parts = Vec::with_capacity(values.len());
    for (name, value) in index.columns.iter().zip(values) {
        let column = column(definition, name)?;
        if !matches!(value, NativeValue::Null) {
            check_column_value(column, value)?;
        }
        parts.push(IndexKeyPart::ascending(index_value(value)));
    }
    let key = if index.unique && omit_unique_nulls {
        CanonicalIndexKey::encode_unique(&parts, UniqueNullSemantics::Distinct)
    } else {
        CanonicalIndexKey::encode(&parts).map(Some)
    }
    .map_err(|_| Error::Invalid("unsupported native secondary-index value"))?;
    Ok(key.map(CanonicalIndexKey::into_bytes))
}

fn index_entry(
    definition: &TableDefinition,
    index: &IndexDefinition,
    row: &[NativeValue],
    primary: &[u8],
    row_key: &[u8; CATALOG_KEY_BYTES as usize],
) -> Result<Option<IndexEntry>> {
    let values = index
        .columns
        .iter()
        .map(|name| {
            let position = definition
                .columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or(Error::Corrupt("native index references missing column"))?;
            row.get(position)
                .ok_or(Error::Invalid("native row has wrong column count"))
        })
        .collect::<Result<Vec<_>>>()?;
    let values: Vec<_> = values.into_iter().cloned().collect();
    let Some(canonical) = canonical_index_values(definition, index, &values, true)? else {
        return Ok(None);
    };
    let prefix = index_key_prefix(&definition.name, &index.name, &canonical)?;
    let key = if index.unique {
        unique_index_key(&prefix)?
    } else {
        nonunique_index_key(&prefix, primary)?
    };
    Ok(Some(IndexEntry {
        key,
        value: row_key.to_vec(),
    }))
}

fn index_entries(
    definition: &TableDefinition,
    row: &[NativeValue],
    primary: &[u8],
    row_key: &[u8; CATALOG_KEY_BYTES as usize],
) -> Result<Vec<Option<IndexEntry>>> {
    definition
        .indexes
        .iter()
        .map(|index| index_entry(definition, index, row, primary, row_key))
        .collect()
}

fn validate_index_entries(
    read: &super::ReadBatch<'_>,
    entries: &[Option<IndexEntry>],
) -> Result<()> {
    for entry in entries.iter().flatten() {
        if read.get(&entry.key)?.as_deref() != Some(entry.value.as_slice()) {
            return Err(Error::Corrupt(
                "native row is missing a secondary index entry",
            ));
        }
    }
    Ok(())
}

fn index_value(value: &NativeValue) -> IndexKeyValueRef<'_> {
    match value {
        NativeValue::Null => IndexKeyValueRef::Null,
        NativeValue::Boolean(value) => IndexKeyValueRef::Boolean(*value),
        NativeValue::Int64(value) => IndexKeyValueRef::Int64(*value),
        NativeValue::UInt64(value) => IndexKeyValueRef::UInt64(*value),
        NativeValue::Float64(value) => IndexKeyValueRef::Float64(*value),
        NativeValue::Text(value) => IndexKeyValueRef::Text(value),
        NativeValue::Binary(value) => IndexKeyValueRef::Binary(value),
    }
}

fn check_column_value(column: &ColumnDefinition, value: &NativeValue) -> Result<()> {
    let matches = match value {
        NativeValue::Null => column.nullable,
        NativeValue::Boolean(_) => column.column_type == ColumnType::Boolean,
        NativeValue::Int64(_) => column.column_type == ColumnType::Int64,
        NativeValue::UInt64(_) => column.column_type == ColumnType::UInt64,
        NativeValue::Float64(_) => column.column_type == ColumnType::Float64,
        NativeValue::Text(_) => column.column_type == ColumnType::Text,
        NativeValue::Binary(_) => column.column_type == ColumnType::Binary,
    };
    if matches {
        Ok(())
    } else {
        Err(Error::Invalid(
            "native row value does not match column type",
        ))
    }
}

fn encode_row(definition: &TableDefinition, values: &[NativeValue]) -> Result<Vec<u8>> {
    if values.len() != definition.columns.len() {
        return Err(Error::Invalid("native row has wrong column count"));
    }
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(ROW_MAGIC);
    bytes.extend_from_slice(&ROW_VERSION.to_le_bytes());
    bytes.extend_from_slice(&definition.schema_version.to_le_bytes());
    bytes.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for (column, value) in definition.columns.iter().zip(values) {
        check_column_value(column, value)?;
        let (tag, payload): (u8, Vec<u8>) = match value {
            NativeValue::Null => (0, Vec::new()),
            NativeValue::Boolean(value) => (1, vec![u8::from(*value)]),
            NativeValue::Int64(value) => (2, value.to_le_bytes().to_vec()),
            NativeValue::UInt64(value) => (3, value.to_le_bytes().to_vec()),
            NativeValue::Float64(value) => (4, value.to_bits().to_le_bytes().to_vec()),
            NativeValue::Text(value) => (5, value.as_bytes().to_vec()),
            NativeValue::Binary(value) => (6, value.clone()),
        };
        bytes.push(tag);
        push_length_prefixed(&mut bytes, &payload)?;
    }
    if bytes.len() > MAX_ROW_BYTES {
        return Err(Error::Invalid("native row exceeds encoded-value limit"));
    }
    Ok(bytes)
}

fn decode_row(definition: &TableDefinition, bytes: &[u8]) -> Result<Vec<NativeValue>> {
    let mut decoder = Decoder { bytes, offset: 0 };
    if decoder.take(8)? != ROW_MAGIC
        || decoder.u16()? != ROW_VERSION
        || decoder.u32()? != definition.schema_version
    {
        return Err(Error::Corrupt("unknown or mismatched native row format"));
    }
    let count = usize::from(decoder.u16()?);
    if count != definition.columns.len() {
        return Err(Error::Corrupt("native row column count mismatch"));
    }
    let mut values = Vec::with_capacity(count);
    for column in &definition.columns {
        let tag = decoder.u8()?;
        let length = usize::from(decoder.u16()?);
        let payload = decoder.take(length)?;
        let value = match (tag, column.column_type, payload) {
            (0, _, []) if column.nullable => NativeValue::Null,
            (1, ColumnType::Boolean, [0]) => NativeValue::Boolean(false),
            (1, ColumnType::Boolean, [1]) => NativeValue::Boolean(true),
            (2, ColumnType::Int64, bytes) if bytes.len() == 8 => {
                NativeValue::Int64(i64::from_le_bytes(bytes.try_into().unwrap()))
            }
            (3, ColumnType::UInt64, bytes) if bytes.len() == 8 => {
                NativeValue::UInt64(u64::from_le_bytes(bytes.try_into().unwrap()))
            }
            (4, ColumnType::Float64, bytes) if bytes.len() == 8 => NativeValue::Float64(
                f64::from_bits(u64::from_le_bytes(bytes.try_into().unwrap())),
            ),
            (5, ColumnType::Text, bytes) => NativeValue::Text(
                std::str::from_utf8(bytes)
                    .map_err(|_| Error::Corrupt("invalid native text value"))?
                    .to_owned(),
            ),
            (6, ColumnType::Binary, bytes) => NativeValue::Binary(bytes.to_vec()),
            _ => return Err(Error::Corrupt("native row value encoding mismatch")),
        };
        values.push(value);
    }
    if decoder.offset != bytes.len() {
        return Err(Error::Corrupt("trailing native row bytes"));
    }
    Ok(values)
}

fn encode_root(identity: CatalogIdentity, format_version: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(28);
    bytes.extend_from_slice(ROOT_MAGIC);
    bytes.extend_from_slice(&format_version.to_le_bytes());
    bytes.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    bytes.extend_from_slice(identity.as_bytes());
    bytes
}

fn decode_root(bytes: &[u8], format_version: u16) -> Result<CatalogIdentity> {
    if bytes.len() != 28
        || &bytes[..8] != ROOT_MAGIC
        || u16::from_le_bytes([bytes[8], bytes[9]]) != format_version
        || u16::from_le_bytes([bytes[10], bytes[11]]) != CATALOG_VERSION
    {
        return Err(Error::Corrupt("unknown or invalid native catalog root"));
    }
    Ok(CatalogIdentity(bytes[12..28].try_into().unwrap()))
}

fn encode_table(definition: &TableDefinition) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(128);
    bytes.extend_from_slice(TABLE_MAGIC_V2);
    bytes.extend_from_slice(&TABLE_VERSION_V2.to_le_bytes());
    bytes.extend_from_slice(&definition.schema_version.to_le_bytes());
    push_name(&mut bytes, &definition.name)?;
    bytes.push(definition.primary_key.len() as u8);
    for column in &definition.primary_key {
        push_name(&mut bytes, column)?;
    }
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
    let magic: [u8; 8] = decoder.take(8)?.try_into().unwrap();
    let version = decoder.u16()?;
    let has_primary_key = if &magic == TABLE_MAGIC_V1 && version == TABLE_VERSION_V1 {
        false
    } else if &magic == TABLE_MAGIC_V2 && version == TABLE_VERSION_V2 {
        true
    } else {
        return Err(Error::Corrupt("unknown native catalog table format"));
    };
    let schema_version = decoder.u32()?;
    let name = decoder.name()?;
    let primary_key = if has_primary_key {
        let count = usize::from(decoder.u8()?);
        if count > MAX_PRIMARY_KEY_COLUMNS {
            return Err(Error::Corrupt("invalid native primary key column count"));
        }
        (0..count)
            .map(|_| decoder.name())
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
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
        primary_key,
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
            primary_key: vec!["id".to_owned()],
            indexes: vec![IndexDefinition {
                name: "by_label".to_owned(),
                columns: vec!["label".to_owned()],
                unique: false,
            }],
        }
    }

    #[test]
    fn unchanged_index_entries_are_still_validated_before_updates() {
        let directory = tempfile::tempdir().unwrap();
        let mut catalog = NativeCatalog::create(directory.path().join("catalog.isam")).unwrap();
        let definition = definition();
        catalog.create_table(&definition).unwrap();
        let values = vec![NativeValue::UInt64(1), NativeValue::Text("label".into())];
        catalog.insert_row("items", &values).unwrap();
        let primary = encode_primary_key(&definition, &values).unwrap();
        let row_key = data_key("items", &primary).unwrap();
        let index = index_entry(
            &definition,
            &definition.indexes[0],
            &values,
            &primary,
            &row_key,
        )
        .unwrap()
        .unwrap();
        // Simulate a logically missing index entry, not a corrupt page checksum.
        catalog
            .store
            .write_batch(&[Mutation::delete(index.key.to_vec())])
            .unwrap();
        let generation = catalog.generation().unwrap();
        catalog.reset_operation_stats();
        assert!(matches!(
            catalog.update_row("items", &[NativeValue::UInt64(1)], &values),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(
            (
                catalog.operation_stats().root_writes,
                catalog.operation_stats().syncs
            ),
            (0, 0)
        );
        assert_eq!(catalog.generation().unwrap(), generation);
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
        let mut encoded = encode_root(identity, super::super::format::FORMAT_VERSION);
        let packed = encode_root(identity, super::super::format::PACKED_FORMAT_VERSION);
        assert_eq!(
            decode_root(&packed, super::super::format::PACKED_FORMAT_VERSION).unwrap(),
            identity
        );
        assert!(decode_root(&packed, super::super::format::FORMAT_VERSION).is_err());
        assert_eq!(
            decode_root(&encoded, super::super::format::FORMAT_VERSION).unwrap(),
            identity
        );
        encoded[10] = CATALOG_VERSION.wrapping_add(1) as u8;
        assert!(matches!(
            decode_root(&encoded, super::super::format::FORMAT_VERSION),
            Err(Error::Corrupt(_))
        ));
        assert!(matches!(
            decode_root(
                &encoded[..encoded.len() - 1],
                super::super::format::FORMAT_VERSION
            ),
            Err(Error::Corrupt(_))
        ));
    }
}
