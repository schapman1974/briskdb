//! Native metadata authority for the hybrid ISAM-metadata/SQLite-shard mode.
//! Records, not SQLite pages or a serialized SQLite database, live in this tree.

use super::isam::{Layout, Mutation, Store};
use super::shard::{SHARD_APPLICATION_ID, SHARD_METADATA_VERSION};
use super::*;
use crate::core::{
    BUCKET_ALGORITHM_VERSION, HASH_VERSION, IDENTIFIER_ENCODING_VERSION, INITIAL_MAP_GENERATION,
    KEY_ENCODING_VERSION, LogicalDatabaseMetadata, RoutingCatalog, TableMetadata,
    VIRTUAL_BUCKET_COUNT, initial_physical_shard,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(super) const FILE_NAME: &str = "manifest.isam";
const KEY_BYTES: u16 = 17;
const VALUE_BYTES: usize = 1024;
const FORMAT: &str = "briskdb.hybrid-metadata.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    format: String,
    pub layout_id: [u8; 16],
    pub shards: u16,
    // Only the internal disposable-data qualification harness creates this
    // profile. Public opens still reject NFS before touching storage.
    #[serde(default)]
    pub rollback_journal: bool,
    hash_version: u32,
    key_encoding: u32,
    bucket_algorithm: u32,
    pub generation: u64,
    pub ready: bool,
    pub degraded: bool,
    pub digest: Option<[u8; 32]>,
    pub tables: usize,
    pub active: Option<Migration>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Migration {
    pub source: u64,
    pub target: u64,
    pub sql_bytes: usize,
    pub sql_hash: [u8; 32],
    pub source_digest: [u8; 32],
    pub target_digest: [u8; 32],
    pub next_shard: u16,
}

impl Migration {
    fn validate(&self) -> EngineResult<()> {
        if self.source.checked_add(1) != Some(self.target)
            || self.target > manifest::MAX_SCHEMA_GENERATION
            || self.sql_bytes == 0
            || self.sql_bytes > manifest::MAX_SCHEMA_MIGRATION_SQL_BYTES
            || self.next_shard > 64
        {
            return Err(corrupt("invalid native migration record bounds"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableRecord {
    name: String,
    placement: u8,
    column: Option<String>,
    key_type: Option<u8>,
}

#[derive(Debug)]
pub(super) struct NativeManifest {
    store: Store,
    identity: [u8; 16],
}

fn key(kind: u8, id: u64, part: u64) -> Vec<u8> {
    let mut key = vec![kind];
    key.extend_from_slice(&id.to_be_bytes());
    key.extend_from_slice(&part.to_be_bytes());
    key
}

fn sql_key(hash: &[u8; 32]) -> Vec<u8> {
    let mut key = vec![4];
    key.extend_from_slice(&hash[..16]);
    key
}

fn encode(value: &impl Serialize) -> EngineResult<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::Internal,
            "cannot encode native metadata",
            error,
        )
    })?;
    if bytes.len() > VALUE_BYTES {
        return Err(EngineError::new(
            EngineErrorKind::LimitExceeded,
            "native metadata record exceeds its format bound",
        ));
    }
    Ok(bytes)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> EngineResult<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        EngineError::from_source(
            EngineErrorKind::DataCorruption,
            "invalid native metadata record",
            error,
        )
    })
}

fn corrupt(message: &str) -> EngineError {
    EngineError::new(EngineErrorKind::DataCorruption, message)
}

pub(super) fn unsupported() -> EngineError {
    EngineError::new(
        EngineErrorKind::Unsupported,
        "this operation is not yet supported with ISAM metadata; no SQLite metadata fallback is allowed",
    )
}

pub(super) fn map_error(error: isam::Error) -> EngineError {
    let kind = match &error {
        isam::Error::Busy => EngineErrorKind::Busy,
        isam::Error::Corrupt(_) => EngineErrorKind::DataCorruption,
        isam::Error::Duplicate => EngineErrorKind::UniqueViolation,
        isam::Error::ReadOnly => EngineErrorKind::ReadOnly,
        isam::Error::Invalid(_) => EngineErrorKind::InvalidArgument,
        // In particular, CommitUnknown is NOT Busy and must never be retried
        // as an uncommitted write. Startup reconciles the durable authority.
        _ => EngineErrorKind::StorageUnavailable,
    };
    EngineError::from_source(kind, format!("ISAM metadata: {error}"), error)
}

impl Root {
    pub fn journal(&self) -> journal::JournalPolicy {
        if self.rollback_journal {
            journal::JournalPolicy::NFS_PERSIST
        } else {
            journal::JournalPolicy::LOCAL
        }
    }

    fn validate(&self) -> EngineResult<()> {
        if self.format != FORMAT
            || self.hash_version != HASH_VERSION
            || self.key_encoding != KEY_ENCODING_VERSION
            || self.bucket_algorithm != BUCKET_ALGORITHM_VERSION
        {
            return Err(corrupt(
                "unsupported native metadata format/routing version",
            ));
        }
        if !(2..=64).contains(&self.shards)
            || self.layout_id == [0; 16]
            || self.generation > manifest::MAX_SCHEMA_GENERATION
            || self.tables >= isam::MAX_BATCH_RECORDS
            || (self.ready && self.digest.is_none())
            || (!self.ready && (self.generation != 0 || self.tables != 0 || self.digest.is_some()))
        {
            return Err(corrupt("invalid native metadata root bounds"));
        }
        if let Some(active) = &self.active {
            active.validate()?;
            if !self.ready
                || active.source != self.generation
                || active.target != self.generation + 1
                || active.target > manifest::MAX_SCHEMA_GENERATION
                || active.next_shard > self.shards
                || self.digest != Some(active.source_digest)
                || active.sql_bytes == 0
                || active.sql_bytes > manifest::MAX_SCHEMA_MIGRATION_SQL_BYTES
            {
                return Err(corrupt("invalid native migration journal"));
            }
        }
        Ok(())
    }

    pub fn layout(&self) -> shard::ShardLayout {
        shard::ShardLayout::from_validated_parts(
            self.layout_id,
            SHARD_APPLICATION_ID,
            SHARD_METADATA_VERSION,
            if self.ready {
                shard::ShardLayoutState::Ready
            } else {
                shard::ShardLayoutState::Creating
            },
        )
        .with_journal(self.journal())
    }
}

impl NativeManifest {
    pub fn create(path: &Path, shards: u16) -> EngineResult<Self> {
        Self::create_with_profile(path, shards, false)
    }

    pub(super) fn create_with_profile(
        path: &Path,
        shards: u16,
        rollback_journal: bool,
    ) -> EngineResult<Self> {
        validate_shard_count(shards)?;
        let mut layout_id = [0; 16];
        getrandom::fill(&mut layout_id).map_err(|error| {
            EngineError::new(EngineErrorKind::StorageUnavailable, error.to_string())
        })?;
        let root = Root {
            format: FORMAT.into(),
            layout_id,
            shards,
            rollback_journal,
            hash_version: HASH_VERSION,
            key_encoding: KEY_ENCODING_VERSION,
            bucket_algorithm: BUCKET_ALGORITHM_VERSION,
            generation: 0,
            ready: false,
            degraded: false,
            digest: None,
            tables: 0,
            active: None,
        };
        let mut store = Store::create_pipelined(
            path,
            Layout::new(KEY_BYTES, VALUE_BYTES as u16).map_err(map_error)?,
        )
        .map_err(map_error)?;
        store
            .write_batch(&[Mutation::insert(key(0, 0, 0), encode(&root)?)])
            .map_err(map_error)?;
        Ok(Self {
            store,
            identity: layout_id,
        })
    }

    pub fn open(path: &Path, read_only: bool) -> EngineResult<Self> {
        validate_existing_manifest_file(path)?;
        let mut store = if read_only {
            Store::open_read_only(path)
        } else {
            Store::open(path)
        }
        .map_err(map_error)?;
        if store.layout() != Layout::new(KEY_BYTES, VALUE_BYTES as u16).map_err(map_error)?
            || !matches!(store.format_version(), 3 | 4)
        {
            return Err(corrupt(
                "ISAM file is not a supported hybrid metadata manifest",
            ));
        }
        let read = store.read_batch().map_err(map_error)?;
        let root: Root = decode(
            &read
                .get(&key(0, 0, 0))
                .map_err(map_error)?
                .ok_or_else(|| corrupt("native metadata initialization is incomplete"))?,
        )?;
        root.validate()?;
        Ok(Self {
            store,
            identity: root.layout_id,
        })
    }

    pub fn root(&mut self) -> EngineResult<(u64, Root)> {
        let read = self.store.read_batch().map_err(map_error)?;
        let root: Root = decode(
            &read
                .get(&key(0, 0, 0))
                .map_err(map_error)?
                .ok_or_else(|| corrupt("native metadata identity is missing"))?,
        )?;
        root.validate()?;
        if root.layout_id != self.identity {
            return Err(corrupt("native metadata identity changed"));
        }
        Ok((read.generation(), root))
    }

    pub fn publish(
        &mut self,
        expected: u64,
        root: &Root,
        mut changes: Vec<Mutation>,
    ) -> EngineResult<()> {
        root.validate()?;
        changes.push(Mutation::put(key(0, 0, 0), encode(root)?));
        if !self
            .store
            .write_batch_at_generation(&changes, expected)
            .map_err(map_error)?
        {
            return Err(EngineError::new(
                EngineErrorKind::Busy,
                "native metadata changed; reopen and revalidate before retrying",
            ));
        }
        Ok(())
    }

    pub fn catalog(&mut self) -> EngineResult<CatalogSnapshot> {
        let read = self.store.read_batch().map_err(map_error)?;
        let root: Root = decode(
            &read
                .get(&key(0, 0, 0))
                .map_err(map_error)?
                .ok_or_else(|| corrupt("native metadata identity is missing"))?,
        )?;
        root.validate()?;
        if root.layout_id != self.identity {
            return Err(corrupt("native metadata identity changed"));
        }
        let rows = read
            .range(&key(1, 0, 0), Some(&key(2, 0, 0)), isam::MAX_BATCH_RECORDS)
            .map_err(map_error)?;
        if rows.len() != root.tables {
            return Err(corrupt("native table catalog count mismatch"));
        }
        let mut tables = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            if row.key != key(1, index as u64 + 1, 0) {
                return Err(corrupt("invalid native table identity"));
            }
            let record: TableRecord = decode(&row.value)?;
            let placement = record.placement()?;
            tables.push(TableMetadata::from_validated(
                index as u64 + 1,
                1,
                record.name,
                placement,
            ));
        }
        if !tables.windows(2).all(|w| w[0].name() < w[1].name()) {
            return Err(corrupt("invalid native table ordering"));
        }
        let routing = RoutingCatalog::from_validated_parts(
            root.shards,
            HASH_VERSION,
            KEY_ENCODING_VERSION,
            BUCKET_ALGORITHM_VERSION,
            INITIAL_MAP_GENERATION,
            (0..VIRTUAL_BUCKET_COUNT)
                .map(|bucket| initial_physical_shard(bucket, root.shards))
                .collect(),
        );
        let logical = Catalog::from_validated_parts(
            IDENTIFIER_ENCODING_VERSION,
            root.generation,
            1,
            vec![LogicalDatabaseMetadata::from_validated(1, "default".into())].into_boxed_slice(),
            tables.into_boxed_slice(),
        );
        Ok(CatalogSnapshot::from_validated_parts(routing, logical))
    }

    pub fn register(&mut self, declarations: &[TableDeclaration]) -> EngineResult<CatalogSnapshot> {
        let (version, mut root) = self.root()?;
        if root.tables != 0 || root.active.is_some() || root.degraded {
            return Err(corrupt("native catalog is not empty and ready"));
        }
        if declarations.len() >= isam::MAX_BATCH_RECORDS {
            return Err(EngineError::new(
                EngineErrorKind::LimitExceeded,
                "ISAM metadata supports at most 4095 tables",
            ));
        }
        let mut sorted = declarations.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|d| d.name());
        let mut changes = Vec::with_capacity(sorted.len());
        for (index, declaration) in sorted.into_iter().enumerate() {
            if declaration.database_id().get() != 1
                || declaration.generated_id_policy() != &GeneratedIdPolicy::None
            {
                return Err(unsupported());
            }
            let (placement, column, key_type) = match declaration.placement() {
                TablePlacement::Sharded(key) => (
                    1,
                    Some(key.column().to_owned()),
                    Some(match key.key_type() {
                        ShardKeyType::Int64 => 1,
                        ShardKeyType::Text => 2,
                        ShardKeyType::Binary => 3,
                    }),
                ),
                TablePlacement::Global => (2, None, None),
                TablePlacement::Catalog => (3, None, None),
            };
            changes.push(Mutation::insert(
                key(1, index as u64 + 1, 0),
                encode(&TableRecord {
                    name: declaration.name().into(),
                    placement,
                    column,
                    key_type,
                })?,
            ));
        }
        root.tables = declarations.len();
        self.publish(version, &root, changes)?;
        self.catalog()
    }

    pub fn begin_migration(
        &mut self,
        sql: &str,
        source_digest: [u8; 32],
        target_digest: [u8; 32],
    ) -> EngineResult<Root> {
        let _ = manifest::schema_migration_id(sql)?;
        let (version, mut root) = self.root()?;
        if root.active.is_some() || root.degraded {
            return Err(corrupt("native metadata cannot start another migration"));
        }
        let migration = Migration {
            source: root.generation,
            target: root.generation + 1,
            sql_bytes: sql.len(),
            sql_hash: *blake3::hash(sql.as_bytes()).as_bytes(),
            source_digest,
            target_digest,
            next_shard: 0,
        };
        let mut changes = vec![Mutation::insert(
            key(2, migration.target, 0),
            encode(&migration)?,
        )];
        for (part, bytes) in sql.as_bytes().chunks(VALUE_BYTES).enumerate() {
            changes.push(Mutation::insert(
                key(3, migration.target, part as u64),
                bytes,
            ));
        }
        root.active = Some(migration);
        self.publish(version, &root, changes)?;
        Ok(root)
    }

    pub fn migration_sql(&mut self, migration: &Migration) -> EngineResult<String> {
        // Validate persisted lengths before allocating or iterating chunks.
        migration.validate()?;
        let read = self.store.read_batch().map_err(map_error)?;
        let mut bytes = Vec::with_capacity(migration.sql_bytes);
        for part in 0..migration.sql_bytes.div_ceil(VALUE_BYTES) {
            bytes.extend(
                read.get(&key(3, migration.target, part as u64))
                    .map_err(map_error)?
                    .ok_or_else(|| corrupt("missing native migration SQL chunk"))?,
            );
        }
        if bytes.len() != migration.sql_bytes
            || *blake3::hash(&bytes).as_bytes() != migration.sql_hash
        {
            return Err(corrupt("native migration SQL checksum mismatch"));
        }
        String::from_utf8(bytes).map_err(|_| corrupt("native migration SQL is not UTF-8"))
    }

    pub fn advance(&mut self, next: u16) -> EngineResult<()> {
        let (version, mut root) = self.root()?;
        let active = root
            .active
            .as_mut()
            .ok_or_else(|| corrupt("native migration journal disappeared"))?;
        if next != active.next_shard + 1 || next > root.shards {
            return Err(corrupt("native migration progress skipped a shard"));
        }
        active.next_shard = next;
        let changes = vec![Mutation::put(key(2, active.target, 0), encode(active)?)];
        self.publish(version, &root, changes)
    }

    pub fn finish(&mut self) -> EngineResult<Root> {
        let (version, mut root) = self.root()?;
        let active = root
            .active
            .take()
            .ok_or_else(|| corrupt("native migration journal disappeared"))?;
        if active.next_shard != root.shards {
            return Err(corrupt("native migration has unfinished shards"));
        }
        root.generation = active.target;
        root.digest = Some(active.target_digest);
        self.publish(
            version,
            &root,
            vec![Mutation::insert(
                sql_key(&active.sql_hash),
                encode(&active)?,
            )],
        )?;
        Ok(root)
    }

    pub fn completed_sql(&mut self, sql: &str) -> EngineResult<bool> {
        let (_, root) = self.root()?;
        let hash = *blake3::hash(sql.as_bytes()).as_bytes();
        let read = self.store.read_batch().map_err(map_error)?;
        let Some(bytes) = read.get(&sql_key(&hash)).map_err(map_error)? else {
            return Ok(false);
        };
        let migration: Migration = decode(&bytes)?;
        migration.validate()?;
        if migration.target > root.generation || migration.next_shard != root.shards {
            return Err(corrupt("native SQL receipt is not a completed migration"));
        }
        if migration.sql_hash != hash {
            return Err(corrupt("native migration receipt hash collision"));
        }
        Ok(self.migration_sql(&migration)? == sql)
    }

    pub fn status(&mut self, generation: u64) -> EngineResult<Option<SchemaMigrationStatus>> {
        let read = self.store.read_batch().map_err(map_error)?;
        let root = root_in_snapshot(&read, self.identity)?;
        status_in_snapshot(&read, &root, generation)
    }

    pub fn summary(&mut self) -> EngineResult<SchemaMigrationSummary> {
        let read = self.store.read_batch().map_err(map_error)?;
        let root = root_in_snapshot(&read, self.identity)?;
        Ok(SchemaMigrationSummary {
            schema_generation: root.generation,
            active: root
                .active
                .as_ref()
                .map(|m| status_in_snapshot(&read, &root, m.target))
                .transpose()?
                .flatten(),
            latest_complete: if root.generation == 0 {
                None
            } else {
                status_in_snapshot(&read, &root, root.generation)?
            },
        })
    }
}

fn root_in_snapshot(read: &isam::ReadBatch<'_>, identity: [u8; 16]) -> EngineResult<Root> {
    let root: Root = decode(
        &read
            .get(&key(0, 0, 0))
            .map_err(map_error)?
            .ok_or_else(|| corrupt("native metadata identity is missing"))?,
    )?;
    root.validate()?;
    if root.layout_id != identity {
        return Err(corrupt("native metadata identity changed"));
    }
    Ok(root)
}

fn status_in_snapshot(
    read: &isam::ReadBatch<'_>,
    root: &Root,
    generation: u64,
) -> EngineResult<Option<SchemaMigrationStatus>> {
    let Some(bytes) = read.get(&key(2, generation, 0)).map_err(map_error)? else {
        if generation != 0 && generation <= root.generation + u64::from(root.active.is_some()) {
            return Err(corrupt("native migration history is incomplete"));
        }
        return Ok(None);
    };
    let migration: Migration = decode(&bytes)?;
    migration.validate()?;
    if migration.target != generation
        || migration.source.checked_add(1) != Some(generation)
        || migration.next_shard > root.shards
        || generation > root.generation + u64::from(root.active.is_some())
        || (generation <= root.generation && migration.next_shard != root.shards)
        || root
            .active
            .as_ref()
            .is_some_and(|active| active.target == generation && active != &migration)
    {
        return Err(corrupt("invalid native migration history"));
    }
    Ok(Some(SchemaMigrationStatus {
        generation,
        source_generation: migration.source,
        target_generation: generation,
        state: if generation <= root.generation {
            SchemaMigrationState::Complete
        } else {
            SchemaMigrationState::Applying
        },
        shard_count: root.shards,
        next_shard: migration.next_shard,
        sql_bytes: migration.sql_bytes,
    }))
}

impl TableRecord {
    fn placement(&self) -> EngineResult<TablePlacement> {
        if !crate::core::validate_catalog_identifier(&self.name) {
            return Err(corrupt("invalid native table name"));
        }
        match (self.placement, self.column.as_ref(), self.key_type) {
            (1, Some(column), Some(kind)) => {
                let kind = match kind {
                    1 => ShardKeyType::Int64,
                    2 => ShardKeyType::Text,
                    3 => ShardKeyType::Binary,
                    _ => return Err(corrupt("invalid native shard-key type")),
                };
                Ok(TablePlacement::Sharded(
                    crate::core::ShardKeyMetadata::new(column.clone(), kind)
                        .map_err(|_| corrupt("invalid native shard-key column"))?,
                ))
            }
            (2, None, None) => Ok(TablePlacement::Global),
            (3, None, None) => Ok(TablePlacement::Catalog),
            _ => Err(corrupt("invalid native table placement")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_metadata_defaults_to_v4_and_reopens_read_only_and_writable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(FILE_NAME);
        let mut native = NativeManifest::create(&path, 2).unwrap();
        assert_eq!(native.store.format_version(), 4);
        let identity = native.root().unwrap().1.layout_id;
        drop(native);
        let before = fs::read(&path).unwrap();
        let mut reader = NativeManifest::open(&path, true).unwrap();
        assert_eq!(reader.store.format_version(), 4);
        assert_eq!(reader.root().unwrap().1.layout_id, identity);
        drop(reader);
        assert_eq!(before, fs::read(&path).unwrap());
        let mut writer = NativeManifest::open(&path, false).unwrap();
        let (version, mut root) = writer.root().unwrap();
        root.degraded = true;
        writer.publish(version, &root, vec![]).unwrap();
        drop(writer);
        let mut reopened = NativeManifest::open(&path, true).unwrap();
        assert_eq!(reopened.store.format_version(), 4);
        assert!(reopened.root().unwrap().1.degraded);
    }

    #[test]
    fn legacy_v3_metadata_remains_readable_and_writable_without_conversion() {
        let directory = tempfile::tempdir().unwrap();
        let mut source = NativeManifest::create(&directory.path().join("source.isam"), 2).unwrap();
        let root = source.root().unwrap().1;
        let path = directory.path().join(FILE_NAME);
        let mut legacy =
            Store::create_packed(&path, Layout::new(KEY_BYTES, VALUE_BYTES as u16).unwrap())
                .unwrap();
        legacy
            .write_batch(&[Mutation::insert(key(0, 0, 0), encode(&root).unwrap())])
            .unwrap();
        drop(legacy);
        let before = fs::read(&path).unwrap();
        let mut reader = NativeManifest::open(&path, true).unwrap();
        assert_eq!(reader.root().unwrap().1.layout_id, root.layout_id);
        assert_eq!(reader.store.format_version(), 3);
        drop(reader);
        assert_eq!(before, fs::read(&path).unwrap());
        let mut writer = NativeManifest::open(&path, false).unwrap();
        let (version, mut updated) = writer.root().unwrap();
        updated.degraded = true;
        writer.publish(version, &updated, vec![]).unwrap();
        drop(writer);
        let mut reopened = NativeManifest::open(&path, true).unwrap();
        assert_eq!(reopened.store.format_version(), 3);
        assert!(reopened.root().unwrap().1.degraded);
    }

    #[test]
    fn v2_tree_with_valid_metadata_records_is_still_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let mut source = NativeManifest::create(&directory.path().join("source.isam"), 2).unwrap();
        let root = source.root().unwrap().1;
        let path = directory.path().join(FILE_NAME);
        let mut unsupported =
            Store::create(&path, Layout::new(KEY_BYTES, VALUE_BYTES as u16).unwrap()).unwrap();
        unsupported
            .write_batch(&[Mutation::insert(key(0, 0, 0), encode(&root).unwrap())])
            .unwrap();
        drop(unsupported);
        let before = fs::read(&path).unwrap();
        for read_only in [true, false] {
            assert_eq!(
                NativeManifest::open(&path, read_only).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
            assert_eq!(before, fs::read(&path).unwrap());
        }
    }

    #[test]
    fn worst_case_root_and_migration_fit_the_persisted_record_bound() {
        let root = Root {
            format: FORMAT.into(),
            layout_id: [255; 16],
            shards: 64,
            rollback_journal: false,
            hash_version: HASH_VERSION,
            key_encoding: KEY_ENCODING_VERSION,
            bucket_algorithm: BUCKET_ALGORITHM_VERSION,
            generation: manifest::MAX_SCHEMA_GENERATION - 1,
            ready: true,
            degraded: true,
            digest: Some([255; 32]),
            tables: 4095,
            active: Some(Migration {
                source: manifest::MAX_SCHEMA_GENERATION - 1,
                target: manifest::MAX_SCHEMA_GENERATION,
                sql_bytes: manifest::MAX_SCHEMA_MIGRATION_SQL_BYTES,
                sql_hash: [255; 32],
                source_digest: [255; 32],
                target_digest: [255; 32],
                next_shard: 64,
            }),
        };
        root.validate().unwrap();
        assert!(encode(&root).unwrap().len() <= VALUE_BYTES);
    }

    #[test]
    fn stale_metadata_publication_cannot_erase_a_newer_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let mut first = NativeManifest::create(&path, 2).unwrap();
        let mut second = NativeManifest::open(&path, false).unwrap();
        let (stale_version, stale_root) = second.root().unwrap();
        let (version, mut root) = first.root().unwrap();
        root.degraded = true;
        first.publish(version, &root, vec![]).unwrap();
        assert_eq!(
            second
                .publish(stale_version, &stale_root, vec![])
                .unwrap_err()
                .kind(),
            EngineErrorKind::Busy
        );
        assert!(second.root().unwrap().1.degraded);
    }

    #[test]
    fn corrupt_or_non_metadata_isam_is_not_adopted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let store =
            Store::create_packed(&path, Layout::new(KEY_BYTES, VALUE_BYTES as u16).unwrap())
                .unwrap();
        drop(store);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(
            NativeManifest::open(&path, false).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(bytes, fs::read(path).unwrap());
    }

    #[test]
    fn oversized_migration_lengths_fail_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let mut native = NativeManifest::create(&dir.path().join(FILE_NAME), 2).unwrap();
        let migration = Migration {
            source: 0,
            target: 1,
            sql_bytes: usize::MAX,
            sql_hash: [0; 32],
            source_digest: [0; 32],
            target_digest: [0; 32],
            next_shard: 0,
        };
        assert_eq!(
            native.migration_sql(&migration).unwrap_err().kind(),
            EngineErrorKind::DataCorruption
        );
    }

    #[test]
    fn summary_uses_one_snapshot_while_an_independent_writer_advances() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let mut native = NativeManifest::create(&path, 2).unwrap();
        let (version, mut root) = native.root().unwrap();
        root.ready = true;
        root.digest = Some([1; 32]);
        native.publish(version, &root, vec![]).unwrap();
        let mut reader = NativeManifest::open(&path, true).unwrap();
        let policy = isam::LockPolicy::new(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(1),
        )
        .unwrap();
        native.store.set_lock_policy(policy);
        reader.store.set_lock_policy(policy);
        let writer = std::thread::spawn(move || {
            for index in 0..20 {
                native
                    .begin_migration(
                        &format!("CREATE TABLE t{index}(id INTEGER)"),
                        [1; 32],
                        [1; 32],
                    )
                    .unwrap();
                native.advance(1).unwrap();
                native.advance(2).unwrap();
                native.finish().unwrap();
            }
        });
        for _ in 0..200 {
            match reader.summary() {
                Ok(summary) => {
                    if let Some(active) = summary.active {
                        assert_eq!(active.source_generation, summary.schema_generation);
                    }
                    if let Some(latest) = summary.latest_complete {
                        assert_eq!(latest.target_generation, summary.schema_generation);
                    }
                }
                Err(error) if error.kind() == EngineErrorKind::Busy => {}
                Err(error) => panic!("snapshot mixed generations: {error}"),
            }
        }
        writer.join().unwrap();
        assert_eq!(reader.summary().unwrap().schema_generation, 20);
    }
}
