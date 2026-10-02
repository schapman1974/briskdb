//! Advisory per-partition ISAM summaries. Only a hash committed in the S3 head
//! authorizes skipping a payload. Missing, stale, busy or damaged indexes are
//! cache misses, never evidence that data is absent.
use super::{Cell, ColumnType, Config, Table, base_directory, parquet::Changes, registry::Delta};
use crate::isam::{Layout, Mutation, Store};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
};

const MAX_RECORD: usize = 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Summary {
    format: u32,
    payload_hash: String,
    columns: Vec<ColumnSummary>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnSummary {
    column: usize,
    min: Option<Cell>,
    max: Option<Cell>,
    // Hex encoding keeps a 256-bit Bloom filter compact in the ISAM record.
    bloom: String,
}

fn typed(kind: ColumnType, cell: &Cell) -> bool {
    matches!(
        (kind, cell),
        (ColumnType::Integer, Cell::Integer(_))
            | (ColumnType::Text, Cell::Text(_))
            | (ColumnType::Blob, Cell::Blob(_))
    )
}

fn compare(a: &Cell, b: &Cell) -> Option<Ordering> {
    match (a, b) {
        (Cell::Integer(a), Cell::Integer(b)) => Some(a.cmp(b)),
        (Cell::Text(a), Cell::Text(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (Cell::Blob(a), Cell::Blob(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn bits(cell: &Cell) -> [usize; 3] {
    let hash = blake3::hash(&serde_json::to_vec(cell).expect("validated key"));
    [
        hash.as_bytes()[0] as usize,
        hash.as_bytes()[1] as usize,
        hash.as_bytes()[2] as usize,
    ]
}

pub(super) fn applicable(table: &Table, predicates: &[(usize, Cell)]) -> bool {
    predicates.iter().any(|(i, v)| {
        table
            .columns
            .get(*i)
            .is_some_and(|c| table.primary_key.contains(&c.name) && typed(c.kind, v))
    })
}

impl Summary {
    fn build(table: &Table, changes: &Changes, hash: &str) -> Option<Vec<u8>> {
        // Decode the primary keys, NOT the row values: tombstones and the old
        // key of a key-changing UPDATE must participate in every summary.
        let keys = changes
            .keys()
            .map(|k| serde_json::from_slice::<Vec<Cell>>(k))
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;
        if keys.is_empty() || keys.iter().any(|k| k.len() != table.primary_key.len()) {
            return None;
        }
        let mut columns = table
            .primary_key
            .iter()
            .enumerate()
            .map(|(key, name)| {
                (
                    key,
                    table.columns.iter().position(|c| c.name == *name).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        columns.sort_by_key(|(_, i)| *i != table.routing_column());
        let mut summary = Self {
            format: 1,
            payload_hash: hash.into(),
            columns: Vec::new(),
        };
        for (key, column) in columns {
            if keys
                .iter()
                .any(|k| !typed(table.columns[column].kind, &k[key]))
            {
                return None;
            }
            let mut bloom = [0u8; 32];
            for row in &keys {
                for bit in bits(&row[key]) {
                    bloom[bit / 8] |= 1 << (bit % 8);
                }
            }
            // Omit wide bounds rather than truncate them (unsafe for pruning).
            let bounded = keys.iter().all(|k| k[key].size() <= 64);
            let min = bounded.then(|| {
                keys.iter()
                    .map(|k| &k[key])
                    .min_by(|a, b| compare(a, b).unwrap())
                    .unwrap()
                    .clone()
            });
            let max = bounded.then(|| {
                keys.iter()
                    .map(|k| &k[key])
                    .max_by(|a, b| compare(a, b).unwrap())
                    .unwrap()
                    .clone()
            });
            summary.columns.push(ColumnSummary {
                column,
                min,
                max,
                bloom: bloom.iter().map(|b| format!("{b:02x}")).collect(),
            });
            if serde_json::to_vec(&summary).ok()?.len() > MAX_RECORD {
                summary.columns.pop();
                break;
            }
        }
        if summary.columns.is_empty() {
            return None;
        }
        serde_json::to_vec(&summary).ok()
    }

    pub(super) fn excludes(&self, table: &Table, predicates: &[(usize, Cell)]) -> bool {
        predicates.iter().any(|(column, value)| {
            let Some(schema) = table.columns.get(*column) else {
                return false;
            };
            // SQLite coercion and non-BINARY collation cannot use these bounds.
            if !table.primary_key.contains(&schema.name) || !typed(schema.kind, value) {
                return false;
            }
            let Some(index) = self.columns.iter().find(|c| c.column == *column) else {
                return false;
            };
            if index.min.as_ref().and_then(|m| compare(value, m)) == Some(Ordering::Less)
                || index.max.as_ref().and_then(|m| compare(value, m)) == Some(Ordering::Greater)
            {
                return true;
            }
            let bloom = index.bloom.as_bytes();
            bits(value).into_iter().any(|bit| {
                let offset = (bit / 8) * 2;
                let byte = ((bloom[offset] as char).to_digit(16).unwrap() << 4)
                    | (bloom[offset + 1] as char).to_digit(16).unwrap();
                byte & (1 << (bit % 8)) == 0
            })
        })
    }

    fn valid(&self, table: &Table, delta: &Delta) -> bool {
        self.format == 1
            && self.payload_hash == delta.hash
            && !self.columns.is_empty()
            && self.columns.len() <= table.primary_key.len()
            && self.columns.iter().all(|c| {
                table.columns.get(c.column).is_some_and(|schema| {
                    table.primary_key.contains(&schema.name)
                        && c.min.as_ref().is_none_or(|v| typed(schema.kind, v))
                        && c.max.as_ref().is_none_or(|v| typed(schema.kind, v))
                        && c.bloom.len() == 64
                        && c.bloom.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
    }
}

pub(super) fn path(root: &Path, config: &Config, table: usize, partition: u16) -> PathBuf {
    base_directory(root, config, table, partition).join("parquet-index.isam")
}

/// Failure affects performance only. The caller publishes index_hash=None.
pub(super) fn publish(
    root: &Path,
    config: &Config,
    table: usize,
    partition: u16,
    id: &str,
    hash: &str,
    changes: &Changes,
) -> Option<String> {
    let data = Summary::build(&config.tables[table], changes, hash)?;
    let path = path(root, config, table, partition);
    let mut store = match Store::open(&path) {
        Ok(s) => s,
        Err(crate::isam::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(path.parent()?).ok()?;
            match Store::create_packed(&path, Layout::new(32, MAX_RECORD as u16).ok()?) {
                Ok(s) => s,
                // Another writer may have created it. Never overwrite/repair.
                Err(_) => Store::open(&path).ok()?,
            }
        }
        Err(_) => return None,
    };
    if store.layout() != Layout::new(32, MAX_RECORD as u16).ok()? {
        return None;
    }
    let hash = blake3::hash(&data).to_hex().to_string();
    store
        .write_batch(&[Mutation::insert(id.as_bytes(), data)])
        .ok()?;
    Some(hash)
}

/// One shared read snapshot for all files, not one ISAM open per Parquet file.
pub(super) fn load(
    root: &Path,
    config: &Config,
    table: usize,
    partition: u16,
    deltas: &[Delta],
) -> Option<Vec<Option<Summary>>> {
    let mut store = Store::open_read_only(path(root, config, table, partition)).ok()?;
    if store.layout() != Layout::new(32, MAX_RECORD as u16).ok()? {
        return None;
    }
    let read = store.read_batch().ok()?;
    Some(
        deltas
            .iter()
            .map(|delta| {
                let expected = delta.index_hash.as_ref()?;
                let data = read.get(delta.id.as_bytes()).ok()??;
                if blake3::hash(&data).to_hex().as_str() != expected {
                    return None;
                }
                let summary: Summary = serde_json::from_slice(&data).ok()?;
                summary
                    .valid(&config.tables[table], delta)
                    .then_some(summary)
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_overlay::Column;

    #[test]
    fn every_typed_key_and_tombstone_is_a_bloom_hit_including_long_and_nul_keys() {
        for (kind, values, missing) in [
            (
                ColumnType::Integer,
                vec![
                    Cell::Integer(i64::MIN),
                    Cell::Integer(-1),
                    Cell::Integer(0),
                    Cell::Integer(i64::MAX),
                ],
                Cell::Integer(42),
            ),
            (
                ColumnType::Text,
                vec![
                    Cell::Text("".into()),
                    Cell::Text("a\0z".into()),
                    Cell::Text("α".into()),
                    Cell::Text("z".repeat(1000)),
                ],
                Cell::Text("middle".into()),
            ),
            (
                ColumnType::Blob,
                vec![
                    Cell::Blob(vec![]),
                    Cell::Blob(vec![0, 255]),
                    Cell::Blob(vec![7; 1000]),
                ],
                Cell::Blob(vec![3]),
            ),
        ] {
            let table = Table {
                name: "test".into(),
                columns: vec![Column {
                    name: "id".into(),
                    kind,
                    nullable: false,
                }],
                primary_key: vec!["id".into()],
                shard_key: "id".into(),
                indexes: vec![],
            };
            let changes = values
                .iter()
                .map(|v| (table.key(&[v.clone()]), None))
                .collect();
            let bytes = Summary::build(&table, &changes, &"a".repeat(64)).unwrap();
            assert!(bytes.len() <= MAX_RECORD);
            let summary: Summary = serde_json::from_slice(&bytes).unwrap();
            for value in values {
                assert!(!summary.excludes(&table, &[(0, value)]));
            }
            assert!(summary.excludes(&table, &[(0, missing)]));
            assert!(!summary.excludes(&table, &[(0, Cell::Real(0.0))]));
            assert!(!summary.excludes(&table, &[(0, Cell::Null)]));
        }
    }

    #[test]
    fn composite_key_bounds_include_old_and_new_keys_and_fit_record_limit() {
        let table = Table {
            name: "test".into(),
            columns: (0..8)
                .map(|i| Column {
                    name: format!("c{i}"),
                    kind: ColumnType::Text,
                    nullable: false,
                })
                .collect(),
            primary_key: (0..8).map(|i| format!("c{i}")).collect(),
            shard_key: "c7".into(),
            indexes: vec![],
        };
        let old = vec![Cell::Text("a".repeat(64)); 8];
        let new = vec![Cell::Text("b".repeat(64)); 8];
        let changes = Changes::from([
            (table.key(&old), None),
            (table.key(&new), Some(new.clone())),
        ]);
        let bytes = Summary::build(&table, &changes, &"a".repeat(64)).unwrap();
        assert!(bytes.len() <= MAX_RECORD);
        let summary: Summary = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(summary.columns[0].column, 7);
        for row in [old, new] {
            for (i, value) in row.into_iter().enumerate() {
                assert!(!summary.excludes(&table, &[(i, value)]));
            }
        }
        assert!(summary.excludes(&table, &[(7, Cell::Text("c".into()))]));
        // A column omitted to honor the ISAM record limit cannot be pruned.
        assert!(summary.columns.len() < 8);
        assert!(!summary.excludes(&table, &[(6, Cell::Text("c".into()))]));
    }
}
