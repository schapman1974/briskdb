//! Protocol-neutral, versioned key routing.

pub(crate) const HASH_VERSION: u32 = 1;
pub(crate) const KEY_ENCODING_VERSION: u32 = 1;
pub(crate) const BUCKET_ALGORITHM_VERSION: u32 = 1;
pub(crate) const VIRTUAL_BUCKET_COUNT: u16 = 4_096;
pub(crate) const INITIAL_MAP_GENERATION: u64 = 1;

/// Validated, immutable routing state loaded from one manifest snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutingCatalog {
    initial_shard_count: u16,
    hash_version: u32,
    key_encoding_version: u32,
    bucket_algorithm_version: u32,
    map_generation: u64,
    buckets: Box<[u16]>,
}

impl RoutingCatalog {
    /// Construct a catalog from data already validated by the storage boundary.
    pub(crate) fn from_validated_parts(
        initial_shard_count: u16,
        hash_version: u32,
        key_encoding_version: u32,
        bucket_algorithm_version: u32,
        map_generation: u64,
        buckets: Box<[u16]>,
    ) -> Self {
        debug_assert!((2..=64).contains(&initial_shard_count));
        debug_assert_eq!(hash_version, HASH_VERSION);
        debug_assert_eq!(key_encoding_version, KEY_ENCODING_VERSION);
        debug_assert_eq!(bucket_algorithm_version, BUCKET_ALGORITHM_VERSION);
        debug_assert_eq!(map_generation, INITIAL_MAP_GENERATION);
        debug_assert_eq!(buckets.len(), usize::from(VIRTUAL_BUCKET_COUNT));
        debug_assert!(buckets.iter().all(|shard| *shard < initial_shard_count));
        Self {
            initial_shard_count,
            hash_version,
            key_encoding_version,
            bucket_algorithm_version,
            map_generation,
            buckets,
        }
    }

    pub(crate) const fn shard_count(&self) -> u16 {
        self.initial_shard_count
    }

    pub(crate) const fn hash_version(&self) -> u32 {
        self.hash_version
    }

    pub(crate) const fn key_encoding_version(&self) -> u32 {
        self.key_encoding_version
    }

    pub(crate) const fn bucket_algorithm_version(&self) -> u32 {
        self.bucket_algorithm_version
    }

    pub(crate) const fn map_generation(&self) -> u64 {
        self.map_generation
    }

    pub(crate) fn shard_for_key(&self, key: &[u8]) -> u16 {
        let bucket = usize::from(self.bucket_for_key(key));
        self.buckets[bucket]
    }

    fn bucket_for_key(&self, key: &[u8]) -> u16 {
        self.bucket_for_hash(self.hash_for_key(key))
    }

    fn hash_for_key(&self, key: &[u8]) -> u64 {
        let canonical_key = match self.key_encoding_version {
            KEY_ENCODING_VERSION => key,
            _ => unreachable!("only validated key encodings enter the routing catalog"),
        };
        match self.hash_version {
            HASH_VERSION => {
                let digest = blake3::hash(canonical_key);
                let prefix: [u8; 8] = digest.as_bytes()[..8]
                    .try_into()
                    .expect("BLAKE3 digest always contains eight bytes");
                u64::from_le_bytes(prefix)
            }
            _ => unreachable!("only validated hashes enter the routing catalog"),
        }
    }

    fn bucket_for_hash(&self, hash: u64) -> u16 {
        match self.bucket_algorithm_version {
            BUCKET_ALGORITHM_VERSION => self.bucket_for_hash_v1(hash),
            _ => unreachable!("only validated bucket algorithms enter the routing catalog"),
        }
    }

    fn bucket_for_hash_v1(&self, hash: u64) -> u16 {
        debug_assert_eq!(self.map_generation, INITIAL_MAP_GENERATION);
        let shard_count = u64::from(self.initial_shard_count);
        let bucket_count = u64::from(VIRTUAL_BUCKET_COUNT);
        let legacy_shard = hash % shard_count;
        let base_size = bucket_count / shard_count;
        let wider_shards = bucket_count % shard_count;
        let group_size = base_size + u64::from(legacy_shard < wider_shards);
        let offset = legacy_shard * base_size + legacy_shard.min(wider_shards);
        let bucket = offset + (hash / shard_count) % group_size;
        u16::try_from(bucket).expect("a validated hash maps into the virtual bucket range")
    }
}

/// Return the generation-1 owner of a virtual bucket.
pub(crate) fn initial_physical_shard(bucket_id: u16, shard_count: u16) -> u16 {
    debug_assert!((2..=64).contains(&shard_count));
    debug_assert!(bucket_id < VIRTUAL_BUCKET_COUNT);

    let bucket_count = u32::from(VIRTUAL_BUCKET_COUNT);
    let shard_count = u32::from(shard_count);
    let bucket_id = u32::from(bucket_id);
    let base_size = bucket_count / shard_count;
    let wider_shards = bucket_count % shard_count;
    let wider_span = (base_size + 1) * wider_shards;
    let shard = if bucket_id < wider_span {
        bucket_id / (base_size + 1)
    } else {
        wider_shards + (bucket_id - wider_span) / base_size
    };
    u16::try_from(shard).expect("a virtual bucket maps to a supported shard")
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

    fn generation_one_catalog(shard_count: u16) -> RoutingCatalog {
        RoutingCatalog::from_validated_parts(
            shard_count,
            HASH_VERSION,
            KEY_ENCODING_VERSION,
            BUCKET_ALGORITHM_VERSION,
            INITIAL_MAP_GENERATION,
            (0..VIRTUAL_BUCKET_COUNT)
                .map(|bucket| initial_physical_shard(bucket, shard_count))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    #[test]
    fn golden_vectors_freeze_hash_bucket_and_shard() {
        struct GoldenVector {
            key: &'static [u8],
            shard_count: u16,
            digest_prefix: [u8; 8],
            hash: u64,
            bucket: u16,
            shard: u16,
        }

        let vectors = [
            GoldenVector {
                key: b"",
                shard_count: 3,
                digest_prefix: [0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6],
                hash: 0xa6a1_f9f5_b949_13af,
                bucket: 1_730,
                shard: 1,
            },
            GoldenVector {
                key: b"\x00\x01\x02",
                shard_count: 6,
                digest_prefix: [0xe1, 0xbe, 0x4d, 0x7a, 0x8a, 0xb5, 0x56, 0x0a],
                hash: 0x0a56_b58a_7a4d_bee1,
                bucket: 2_460,
                shard: 3,
            },
            GoldenVector {
                key: b"\x00\x01\x02",
                shard_count: 10,
                digest_prefix: [0xe1, 0xbe, 0x4d, 0x7a, 0x8a, 0xb5, 0x56, 0x0a],
                hash: 0x0a56_b58a_7a4d_bee1,
                bucket: 3_754,
                shard: 9,
            },
            GoldenVector {
                key: b"abc",
                shard_count: 4,
                digest_prefix: [0x64, 0x37, 0xb3, 0xac, 0x38, 0x46, 0x51, 0x33],
                hash: 0x3351_4638_acb3_3764,
                bucket: 473,
                shard: 0,
            },
            GoldenVector {
                key: b"customer-42",
                shard_count: 6,
                digest_prefix: [0x57, 0x9d, 0x61, 0xfc, 0xa7, 0x21, 0x36, 0xd2],
                hash: 0xd236_21a7_fc61_9d57,
                bucket: 768,
                shard: 1,
            },
            GoldenVector {
                key: b"tenant/alpha",
                shard_count: 10,
                digest_prefix: [0x21, 0x1a, 0xb7, 0x84, 0x48, 0x11, 0x1e, 0x6e],
                hash: 0x6e1e_1148_84b7_1a21,
                bucket: 1_283,
                shard: 3,
            },
            GoldenVector {
                key: b"\x00\x01\x02\xff",
                shard_count: 63,
                digest_prefix: [0x10, 0xf8, 0x47, 0x93, 0x6e, 0xb4, 0xf5, 0x65],
                hash: 0x65f5_b46e_9347_f810,
                bucket: 1_546,
                shard: 23,
            },
            GoldenVector {
                key: "snowman-☃".as_bytes(),
                shard_count: 64,
                digest_prefix: [0x81, 0x36, 0xa2, 0x00, 0xe9, 0x3c, 0x61, 0xf3],
                hash: 0xf361_3ce9_00a2_3681,
                bucket: 90,
                shard: 1,
            },
            GoldenVector {
                key: b"a\0b",
                shard_count: 6,
                digest_prefix: [0xfd, 0xeb, 0x88, 0xa4, 0xc6, 0xf0, 0x22, 0x46],
                hash: 0x4622_f0c6_a488_ebfd,
                bucket: 1_310,
                shard: 1,
            },
        ];

        for vector in vectors {
            let catalog = generation_one_catalog(vector.shard_count);
            let digest = blake3::hash(vector.key);
            assert_eq!(&digest.as_bytes()[..8], vector.digest_prefix);
            assert_eq!(catalog.hash_for_key(vector.key), vector.hash);
            assert_eq!(catalog.bucket_for_key(vector.key), vector.bucket);
            assert_eq!(catalog.buckets[usize::from(vector.bucket)], vector.shard);
            assert_eq!(catalog.shard_for_key(vector.key), vector.shard);
            assert_eq!(catalog.hash_version, 1);
            assert_eq!(catalog.key_encoding_version, 1);
            assert_eq!(catalog.bucket_algorithm_version, 1);
            assert_eq!(catalog.buckets.len(), 4_096);
            assert_eq!(catalog.map_generation, 1);
        }
    }

    #[test]
    fn bucket_boundaries_are_frozen_for_wide_and_narrow_ranges() {
        let vectors = [
            (6, 0, 0, 0),
            (6, 3, 2_049, 3),
            (6, 4, 2_732, 4),
            (6, 5, 3_414, 5),
            (6, 6, 1, 0),
            (6, u64::MAX, 2_646, 3),
            (64, 63, 4_032, 63),
            (64, 64, 1, 0),
            (64, u64::MAX, 4_095, 63),
        ];

        for (shard_count, hash, expected_bucket, expected_shard) in vectors {
            let catalog = generation_one_catalog(shard_count);
            let bucket = catalog.bucket_for_hash(hash);
            assert_eq!(bucket, expected_bucket);
            assert_eq!(catalog.buckets[usize::from(bucket)], expected_shard);
        }
    }

    #[test]
    fn every_bucket_is_reachable_and_generation_one_preserves_modulo_placement() {
        for shard_count in 2..=64_u16 {
            let catalog = generation_one_catalog(shard_count);
            let bucket_count = u64::from(VIRTUAL_BUCKET_COUNT);
            let shard_count_u64 = u64::from(shard_count);
            let base_size = bucket_count / shard_count_u64;
            let wider_shards = bucket_count % shard_count_u64;
            let mut reached = vec![false; usize::from(VIRTUAL_BUCKET_COUNT)];

            for shard in 0..shard_count_u64 {
                let size = base_size + u64::from(shard < wider_shards);
                let offset = shard * base_size + shard.min(wider_shards);
                for ordinal in 0..size {
                    let hash = shard_count_u64 * ordinal + shard;
                    let bucket = catalog.bucket_for_hash(hash);
                    assert_eq!(u64::from(bucket), offset + ordinal);
                    assert_eq!(catalog.buckets[usize::from(bucket)], shard as u16);
                    assert_eq!(
                        catalog.buckets[usize::from(bucket)],
                        (hash % shard_count_u64) as u16
                    );
                    reached[usize::from(bucket)] = true;
                }
            }

            let max_bucket = catalog.bucket_for_hash(u64::MAX);
            assert_eq!(
                catalog.buckets[usize::from(max_bucket)],
                (u64::MAX % shard_count_u64) as u16
            );
            assert!(reached.into_iter().all(|was_reached| was_reached));
        }
    }

    #[test]
    fn routing_reads_the_bucket_map_instead_of_recomputing_modulo() {
        let key = b"catalog-lookup-proof";
        let mut catalog = generation_one_catalog(6);
        let hash = catalog.hash_for_key(key);
        let legacy_shard = (hash % 6) as u16;
        let bucket = catalog.bucket_for_key(key);
        let remapped_shard = (legacy_shard + 1) % 6;

        catalog.buckets[usize::from(bucket)] = remapped_shard;

        assert_ne!(remapped_shard, legacy_shard);
        assert_eq!(catalog.shard_for_key(key), remapped_shard);
    }

    #[test]
    fn immutable_catalog_is_send_sync_and_routes_deterministically_in_parallel() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RoutingCatalog>();

        let catalog = Arc::new(generation_one_catalog(10));
        let expected = catalog.shard_for_key(b"parallel-routing");
        let mut threads = Vec::new();
        for _ in 0..8 {
            let catalog = Arc::clone(&catalog);
            threads.push(thread::spawn(move || {
                for _ in 0..10_000 {
                    assert_eq!(catalog.shard_for_key(b"parallel-routing"), expected);
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
