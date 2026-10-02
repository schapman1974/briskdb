use super::{Config, Result, corrupt, storage_error};
use crate::isam::{Layout, Mutation, Store};
use serde::{Deserialize, Serialize};
use std::{fs::File, path::Path};

pub(crate) const FILE: &str = "overlay.isam";
const CHUNK: usize = 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    bytes: usize,
    hash: String,
}

/// Publish the catalog only after every immutable base and S3 head is durable.
pub(crate) fn create(root: &Path, config: &Config) -> Result<()> {
    let data = serde_json::to_vec(config).map_err(storage_error)?;
    if data.len() > 1024 * 1024 {
        return Err(super::limit("catalog exceeds 1 MiB"));
    }
    let mut store = Store::create_packed(
        root.join(FILE),
        Layout::new(8, CHUNK as u16).map_err(storage_error)?,
    )
    .map_err(storage_error)?;
    let header = serde_json::to_vec(&Header {
        bytes: data.len(),
        hash: blake3::hash(&data).to_hex().to_string(),
    })
    .map_err(storage_error)?;
    let mut records = vec![Mutation::insert(0u64.to_be_bytes(), header)];
    records.extend(
        data.chunks(CHUNK)
            .enumerate()
            .map(|(i, chunk)| Mutation::insert((i as u64 + 1).to_be_bytes(), chunk)),
    );
    store.write_batch(&records).map_err(storage_error)?;
    File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(storage_error)
}

/// Shared read locks only. No SQLite manifest, recovery, or root startup fence.
pub(crate) fn open(root: &Path) -> Result<Config> {
    let mut store = Store::open_read_only(root.join(FILE)).map_err(storage_error)?;
    if store.layout() != Layout::new(8, CHUNK as u16).map_err(storage_error)? {
        return Err(corrupt("overlay ISAM catalog layout mismatch"));
    }
    let read = store.read_batch().map_err(storage_error)?;
    let header: Header = serde_json::from_slice(
        &read
            .get(&0u64.to_be_bytes())
            .map_err(storage_error)?
            .ok_or_else(|| corrupt("incomplete overlay catalog; creation did not commit"))?,
    )
    .map_err(storage_error)?;
    if header.bytes == 0 || header.bytes > 1024 * 1024 {
        return Err(corrupt("invalid catalog size"));
    }
    let mut data = Vec::with_capacity(header.bytes);
    for i in 1..=header.bytes.div_ceil(CHUNK) {
        data.extend(
            read.get(&(i as u64).to_be_bytes())
                .map_err(storage_error)?
                .ok_or_else(|| corrupt("missing catalog chunk"))?,
        );
    }
    if data.len() != header.bytes || blake3::hash(&data).to_hex().as_str() != header.hash {
        return Err(corrupt("overlay catalog checksum mismatch"));
    }
    let config: Config = serde_json::from_slice(&data).map_err(storage_error)?;
    config.validate()?;
    Ok(config)
}
