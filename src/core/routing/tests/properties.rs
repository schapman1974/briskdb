use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn arbitrary_hashes_preserve_initial_owners_and_bucket_bounds(hash in any::<u64>(), shards in 2_u16..=64) {
        let catalog = generation_one_catalog(shards);
        let bucket = catalog.bucket_for_hash(hash);
        prop_assert!(bucket < VIRTUAL_BUCKET_COUNT);
        prop_assert_eq!(u64::from(catalog.buckets[usize::from(bucket)]), hash % u64::from(shards));
        prop_assert_eq!(catalog.buckets[usize::from(bucket)], initial_physical_shard(bucket, shards));
    }

    #[test]
    fn generated_keys_consult_the_owner_map_and_leave_snapshots_immutable(
        key in prop::collection::vec(any::<u8>(), 0..1024),
        shards in 2_u16..=64,
        offset in 1_u16..64,
    ) {
        let original = generation_one_catalog(shards);
        let hash = u64::from_le_bytes(blake3::hash(&key).as_bytes()[..8].try_into().unwrap());
        let expected = (hash % u64::from(shards)) as u16;
        prop_assert_eq!(original.shard_for_key(&key), expected);
        let mut remapped = original.clone();
        let bucket = original.bucket_for_key(&key);
        let changed = (expected + 1 + (offset - 1) % (shards - 1)) % shards;
        remapped.buckets[usize::from(bucket)] = changed;
        prop_assert_ne!(changed, expected);
        prop_assert_eq!(remapped.shard_for_key(&key), changed);
        prop_assert_eq!(remapped.clone().shard_for_key(&key), changed);
        prop_assert_eq!(original.shard_for_key(&key), expected);
    }

    #[cfg(feature = "documents")]
    #[test]
    fn numeric_bson_aliases_share_canonical_routes_at_every_shape(number in any::<i32>(), shards in 2_u16..=64) {
        use crate::document::{BsonDecimal128, BsonDocument, BsonValue, CanonicalBsonKey};
        let catalog = generation_one_catalog(shards);
        let aliases = [BsonValue::Int32(number), BsonValue::Int64(i64::from(number)),
            BsonValue::Double(f64::from(number)),
            BsonValue::Decimal128(BsonDecimal128::parse(&number.to_string()).unwrap())];
        for shape in 0..3 {
            let wrap = |value: BsonValue| match shape {
                0 => value,
                1 => BsonValue::Array(vec![value]),
                _ => BsonValue::Document(BsonDocument::from_entries([("part", value)]).unwrap()),
            };
            let expected = CanonicalBsonKey::encode(&wrap(aliases[0].clone())).unwrap();
            for alias in &aliases {
                let actual = CanonicalBsonKey::encode(&wrap(alias.clone())).unwrap();
                prop_assert_eq!(actual.as_bytes(), expected.as_bytes());
                prop_assert_eq!(catalog.shard_for_key(actual.as_bytes()), catalog.shard_for_key(expected.as_bytes()));
            }
        }
    }
}
