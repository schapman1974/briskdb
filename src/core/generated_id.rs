//! Stable generated-ID encodings shared by planning and storage.
//!
//! Issue #125 freezes the codec and its failure boundaries before issue #128
//! lets SQLite allocate from these ranges. Production execution does not use
//! every helper yet.

#![allow(dead_code)]

use super::{EngineError, EngineErrorKind, EngineResult, GeneratedIdPolicy};

pub(crate) const NATIVE_RANGE_V1_FORMAT_MARKER: u64 = 0x4000_0000_0000_0000;
pub(crate) const NATIVE_RANGE_V1_OWNER_BITS: u32 = 10;
pub(crate) const NATIVE_RANGE_V1_LOCAL_SEQUENCE_BITS: u32 = 52;
pub(crate) const NATIVE_RANGE_V1_OWNER_SHIFT: u32 = NATIVE_RANGE_V1_LOCAL_SEQUENCE_BITS;
pub(crate) const MAX_ALLOCATION_OWNER_SLOT: u16 = (1_u16 << NATIVE_RANGE_V1_OWNER_BITS) - 1;
pub(crate) const MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE: u64 =
    (1_u64 << NATIVE_RANGE_V1_LOCAL_SEQUENCE_BITS) - 1;

/// `hilo_v1` owns the positive signed interval whose two highest usable bits
/// are `01`. Values at or above this marker are never legacy IDs under a
/// `hilo_v1` policy; the native interval is therefore an incompatible stored
/// namespace rather than something a hi/lo table may hash-route.
pub(crate) const HILO_V1_FORMAT_MARKER: u64 = 0x2000_0000_0000_0000;
pub(crate) const HILO_V1_SEQUENCE_BITS: u32 = 61;
pub(crate) const MAX_HILO_V1_SEQUENCE: u64 = (1_u64 << HILO_V1_SEQUENCE_BITS) - 1;

const NATIVE_RANGE_V1_OWNER_MASK: u64 =
    ((1_u64 << NATIVE_RANGE_V1_OWNER_BITS) - 1) << NATIVE_RANGE_V1_OWNER_SHIFT;

/// Stable allocation namespace encoded into a native generated ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct AllocationOwnerSlot(u16);

impl AllocationOwnerSlot {
    pub(crate) fn new(value: u16) -> EngineResult<Self> {
        if value > MAX_ALLOCATION_OWNER_SLOT {
            return Err(EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                format!(
                    "allocation-owner slot {value} exceeds the native-range limit {MAX_ALLOCATION_OWNER_SLOT}"
                ),
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_validated(value: u16) -> Self {
        debug_assert!(value <= MAX_ALLOCATION_OWNER_SLOT);
        Self(value)
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

/// Durable lifecycle state for one native-range allocation namespace.
///
/// Retired owners remain routable so committed historical IDs can still be
/// read, but they are never selected for a new SQLite allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AllocationOwnerState {
    Active,
    Retired,
}

/// Immutable, bidirectional assignment of generated-ID owner slots to
/// physical SQLite shards.
///
/// Owner slots are part of the durable ID format and therefore cannot be
/// inferred from a mutable shard position. The manifest validates and loads
/// this map once; native ID routing and shard-local allocation then consult
/// the same immutable runtime snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AllocationOwnerMap {
    by_owner: Box<[(AllocationOwnerSlot, u16, AllocationOwnerState)]>,
    by_physical_shard: Box<[AllocationOwnerSlot]>,
}

impl AllocationOwnerMap {
    /// Validate a complete one-to-one mapping for the current physical shard
    /// set and normalize entries into owner-slot order.
    pub(crate) fn try_from_pairs(
        physical_shard_count: u16,
        pairs: Box<[(u16, u16)]>,
    ) -> EngineResult<Self> {
        Self::try_from_assignments(
            physical_shard_count,
            pairs
                .into_vec()
                .into_iter()
                .map(|(owner, physical_shard)| {
                    (owner, physical_shard, AllocationOwnerState::Active)
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )
    }

    /// Validate active and historical owner assignments for the current
    /// physical shard set and normalize them into owner-slot order.
    ///
    /// Every physical shard has exactly one active allocator. Any number of
    /// retired owners may retain a route to that shard, and every owner slot
    /// appears at most once across both lifecycle states. The active owner for
    /// a shard must be greater than all of its retired owners so SQLite's
    /// non-decreasing `AUTOINCREMENT` high-water mark remains in the active
    /// owner's encoded range.
    pub(crate) fn try_from_assignments(
        physical_shard_count: u16,
        assignments: Box<[(u16, u16, AllocationOwnerState)]>,
    ) -> EngineResult<Self> {
        if physical_shard_count == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "an allocation-owner map requires at least one physical shard",
            ));
        }
        if assignments.len() < usize::from(physical_shard_count) {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "an allocation-owner map must contain an active entry for every physical shard",
            ));
        }

        let mut by_owner = assignments
            .into_vec()
            .into_iter()
            .map(|(owner, physical_shard, state)| {
                AllocationOwnerSlot::new(owner).map(|owner| (owner, physical_shard, state))
            })
            .collect::<EngineResult<Vec<_>>>()?;
        by_owner.sort_unstable_by_key(|(owner, _, _)| *owner);
        if by_owner
            .windows(2)
            .any(|entries| entries[0].0 == entries[1].0)
        {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "an allocation-owner slot cannot be assigned more than once",
            ));
        }

        let mut by_physical_shard = vec![None; usize::from(physical_shard_count)];
        for &(owner, physical_shard, state) in &by_owner {
            let Some(active_owner) = by_physical_shard.get_mut(usize::from(physical_shard)) else {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "allocation owner {} references physical shard {physical_shard}, outside the current shard set",
                        owner.get()
                    ),
                ));
            };
            if state == AllocationOwnerState::Active && active_owner.replace(owner).is_some() {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "physical shard {physical_shard} cannot have more than one active allocation owner"
                    ),
                ));
            }
        }
        let by_physical_shard = by_physical_shard
            .into_iter()
            .enumerate()
            .map(|(physical_shard, owner)| {
                owner.ok_or_else(|| {
                    EngineError::new(
                        EngineErrorKind::InvalidArgument,
                        format!("physical shard {physical_shard} is missing its allocation owner"),
                    )
                })
            })
            .collect::<EngineResult<Vec<_>>>()?;
        for &(retired_owner, physical_shard, state) in &by_owner {
            if state != AllocationOwnerState::Retired {
                continue;
            }
            let active_owner = by_physical_shard[usize::from(physical_shard)];
            if active_owner <= retired_owner {
                return Err(EngineError::new(
                    EngineErrorKind::InvalidArgument,
                    format!(
                        "active allocation owner {} for physical shard {physical_shard} must be greater than retired owner {}",
                        active_owner.get(),
                        retired_owner.get()
                    ),
                ));
            }
        }

        Ok(Self {
            by_owner: by_owner.into_boxed_slice(),
            by_physical_shard: by_physical_shard.into_boxed_slice(),
        })
    }

    pub(crate) fn physical_shard(&self, owner: AllocationOwnerSlot) -> Option<u16> {
        self.by_owner
            .binary_search_by_key(&owner, |(candidate, _, _)| *candidate)
            .ok()
            .map(|index| self.by_owner[index].1)
    }

    pub(crate) fn owner_state(&self, owner: AllocationOwnerSlot) -> Option<AllocationOwnerState> {
        self.by_owner
            .binary_search_by_key(&owner, |(candidate, _, _)| *candidate)
            .ok()
            .map(|index| self.by_owner[index].2)
    }

    pub(crate) fn owner_is_active(&self, owner: AllocationOwnerSlot) -> bool {
        self.owner_state(owner) == Some(AllocationOwnerState::Active)
    }

    pub(crate) fn owner_for_physical_shard(
        &self,
        physical_shard: u16,
    ) -> Option<AllocationOwnerSlot> {
        self.by_physical_shard
            .get(usize::from(physical_shard))
            .copied()
    }

    pub(crate) fn physical_shard_count(&self) -> u16 {
        u16::try_from(self.by_physical_shard.len())
            .expect("a validated allocation-owner map fits the physical shard ID range")
    }

    pub(crate) fn pairs(&self) -> impl ExactSizeIterator<Item = (u16, u16)> + '_ {
        self.by_owner
            .iter()
            .map(|(owner, physical_shard, _)| (owner.get(), *physical_shard))
    }

    pub(crate) fn assignments(
        &self,
    ) -> impl ExactSizeIterator<Item = (u16, u16, AllocationOwnerState)> + '_ {
        self.by_owner
            .iter()
            .map(|(owner, physical_shard, state)| (owner.get(), *physical_shard, *state))
    }
}

/// Decoded fields of one valid `native_range_v1` ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct NativeRangeV1Id {
    owner: AllocationOwnerSlot,
    local_sequence: u64,
}

/// Decoded sequence of one manifest-leased `hilo_v1` ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct HiloV1Id {
    sequence: u64,
}

impl HiloV1Id {
    pub(crate) fn new(sequence: u64) -> EngineResult<Self> {
        if sequence == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "hilo_v1 sequence zero is reserved",
            ));
        }
        if sequence > MAX_HILO_V1_SEQUENCE {
            return Err(EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                format!("hilo_v1 sequence {sequence} exceeds {MAX_HILO_V1_SEQUENCE}"),
            ));
        }
        Ok(Self { sequence })
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }

    pub(crate) fn encode(self) -> i64 {
        i64::try_from(HILO_V1_FORMAT_MARKER | self.sequence)
            .expect("hilo_v1 reserves the signed high bit")
    }

    pub(crate) fn decode(encoded: i64) -> EngineResult<Self> {
        decode_hilo_v1(encoded)?.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "value does not contain the hilo_v1 format marker",
            )
        })
    }
}

impl NativeRangeV1Id {
    pub(crate) fn new(owner: AllocationOwnerSlot, local_sequence: u64) -> EngineResult<Self> {
        if local_sequence == 0 {
            return Err(EngineError::new(
                EngineErrorKind::InvalidArgument,
                "native_range_v1 local sequence zero is reserved for the SQLite sequence floor",
            ));
        }
        if local_sequence > MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE {
            return Err(EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                format!(
                    "native_range_v1 local sequence {local_sequence} exceeds {MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE}"
                ),
            ));
        }
        Ok(Self {
            owner,
            local_sequence,
        })
    }

    pub(crate) const fn owner(self) -> AllocationOwnerSlot {
        self.owner
    }

    pub(crate) const fn local_sequence(self) -> u64 {
        self.local_sequence
    }

    pub(crate) fn encode(self) -> i64 {
        let encoded = sequence_floor_bits(self.owner) | self.local_sequence;
        i64::try_from(encoded).expect("native_range_v1 reserves the signed high bit")
    }

    /// Decode a generated value strictly as `native_range_v1`.
    ///
    /// Legacy values return `NumericOutOfRange`; callers that accept both
    /// formats should use [`classify_generated_id`] instead.
    pub(crate) fn decode(encoded: i64) -> EngineResult<Self> {
        decode_native_range_v1(encoded)?.ok_or_else(|| {
            EngineError::new(
                EngineErrorKind::NumericOutOfRange,
                "value does not contain the native_range_v1 format marker",
            )
        })
    }
}

/// Policy-aware interpretation of a stored logical key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GeneratedIdClassification {
    /// A caller-supplied or imported value routed by the legacy key codec.
    Legacy(i64),
    /// A valid value allocated from one native owner range.
    NativeRangeV1(NativeRangeV1Id),
    /// A valid value allocated from the manifest-backed hi/lo sequence.
    HiloV1(HiloV1Id),
}

/// Classify one value without inferring generation policy from its bits.
///
/// A table whose policy is `None` treats every signed 64-bit value as legacy,
/// including values that resemble a generated marker. A native-range table
/// keeps negative and pre-native-marker values on the legacy routing path,
/// allowing an explicitly validated transition without changing existing
/// keys. A hi/lo table keeps only negative and pre-hi/lo-marker values on that
/// path; its empty-only registration contract reserves every larger value for
/// generated formats.
pub(crate) fn classify_generated_id(
    policy: &GeneratedIdPolicy,
    encoded: i64,
) -> EngineResult<GeneratedIdClassification> {
    match policy {
        GeneratedIdPolicy::None => Ok(GeneratedIdClassification::Legacy(encoded)),
        GeneratedIdPolicy::NativeRangeV1 { .. } => decode_native_range_v1(encoded).map(|decoded| {
            decoded.map_or(
                GeneratedIdClassification::Legacy(encoded),
                GeneratedIdClassification::NativeRangeV1,
            )
        }),
        GeneratedIdPolicy::HiloV1 { .. } => decode_hilo_v1(encoded).map(|decoded| {
            decoded.map_or(
                GeneratedIdClassification::Legacy(encoded),
                GeneratedIdClassification::HiloV1,
            )
        }),
    }
}

/// Classify a caller-supplied value without treating malformed input as
/// corruption of already-committed storage.
///
/// In particular, the owner-local sequence-zero sentinel is valid only in
/// `sqlite_sequence`; callers cannot insert it as a row ID.
pub(crate) fn classify_caller_generated_id(
    policy: &GeneratedIdPolicy,
    encoded: i64,
) -> EngineResult<GeneratedIdClassification> {
    match policy {
        GeneratedIdPolicy::None => Ok(GeneratedIdClassification::Legacy(encoded)),
        GeneratedIdPolicy::NativeRangeV1 { .. } => {
            decode_native_range_v1_with_reserved_error(encoded, EngineErrorKind::InvalidArgument)
                .map(|decoded| {
                    decoded.map_or(
                        GeneratedIdClassification::Legacy(encoded),
                        GeneratedIdClassification::NativeRangeV1,
                    )
                })
        }
        GeneratedIdPolicy::HiloV1 { .. } => {
            decode_hilo_v1_with_reserved_error(encoded, EngineErrorKind::InvalidArgument).map(
                |decoded| {
                    decoded.map_or(
                        GeneratedIdClassification::Legacy(encoded),
                        GeneratedIdClassification::HiloV1,
                    )
                },
            )
        }
    }
}

/// SQLite `sqlite_sequence` floor for an empty owner-local table.
///
/// This sentinel contains the v1 marker and owner but local sequence zero, so
/// it is deliberately not a valid row ID. SQLite's first automatic allocation
/// advances it to local sequence one.
pub(crate) fn native_range_v1_sequence_floor(owner: AllocationOwnerSlot) -> i64 {
    i64::try_from(sequence_floor_bits(owner)).expect("native_range_v1 reserves the signed high bit")
}

/// First row ID SQLite allocates after an owner-local sequence floor.
pub(crate) fn native_range_v1_first_id(owner: AllocationOwnerSlot) -> i64 {
    native_range_v1_sequence_floor(owner)
        .checked_add(1)
        .expect("every native-range owner has a non-empty signed ID interval")
}

/// Last row ID that belongs to an owner-local allocation range.
pub(crate) fn native_range_v1_sequence_ceiling(owner: AllocationOwnerSlot) -> i64 {
    NativeRangeV1Id::new(owner, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE)
        .expect("the codec's maximum local sequence is valid")
        .encode()
}

/// Reserved sequence-zero boundary immediately below the first hi/lo ID.
pub(crate) fn hilo_v1_sequence_floor() -> i64 {
    i64::try_from(HILO_V1_FORMAT_MARKER).expect("hilo_v1 reserves the signed high bit")
}

/// First value produced by the global per-table hi/lo sequence.
pub(crate) fn hilo_v1_first_id() -> i64 {
    HiloV1Id::new(1)
        .expect("hilo_v1 sequence one is valid")
        .encode()
}

/// Last value available to the global per-table hi/lo sequence.
pub(crate) fn hilo_v1_sequence_ceiling() -> i64 {
    HiloV1Id::new(MAX_HILO_V1_SEQUENCE)
        .expect("the codec's maximum hi/lo sequence is valid")
        .encode()
}

fn sequence_floor_bits(owner: AllocationOwnerSlot) -> u64 {
    NATIVE_RANGE_V1_FORMAT_MARKER | (u64::from(owner.get()) << NATIVE_RANGE_V1_OWNER_SHIFT)
}

fn decode_native_range_v1(encoded: i64) -> EngineResult<Option<NativeRangeV1Id>> {
    decode_native_range_v1_with_reserved_error(encoded, EngineErrorKind::DataCorruption)
}

fn decode_native_range_v1_with_reserved_error(
    encoded: i64,
    reserved_error_kind: EngineErrorKind,
) -> EngineResult<Option<NativeRangeV1Id>> {
    if encoded < 0 {
        return Ok(None);
    }
    let bits = u64::try_from(encoded).expect("non-negative i64 fits u64");
    if bits & NATIVE_RANGE_V1_FORMAT_MARKER == 0 {
        return Ok(None);
    }

    let local_sequence = bits & MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE;
    if local_sequence == 0 {
        return Err(EngineError::new(
            reserved_error_kind,
            "native_range_v1 row ID contains the reserved local sequence zero",
        ));
    }
    let owner = u16::try_from((bits & NATIVE_RANGE_V1_OWNER_MASK) >> NATIVE_RANGE_V1_OWNER_SHIFT)
        .expect("the native owner field contains ten bits");
    Ok(Some(NativeRangeV1Id {
        owner: AllocationOwnerSlot::from_validated(owner),
        local_sequence,
    }))
}

fn decode_hilo_v1(encoded: i64) -> EngineResult<Option<HiloV1Id>> {
    decode_hilo_v1_with_reserved_error(encoded, EngineErrorKind::DataCorruption)
}

fn decode_hilo_v1_with_reserved_error(
    encoded: i64,
    reserved_error_kind: EngineErrorKind,
) -> EngineResult<Option<HiloV1Id>> {
    if encoded < 0 {
        return Ok(None);
    }
    let bits = u64::try_from(encoded).expect("non-negative i64 fits u64");
    if bits < HILO_V1_FORMAT_MARKER {
        return Ok(None);
    }
    if bits > (HILO_V1_FORMAT_MARKER | MAX_HILO_V1_SEQUENCE) {
        return Err(EngineError::new(
            reserved_error_kind,
            "hilo_v1 row ID uses an incompatible generated-ID namespace",
        ));
    }
    let sequence = bits & MAX_HILO_V1_SEQUENCE;
    if sequence == 0 {
        return Err(EngineError::new(
            reserved_error_kind,
            "hilo_v1 row ID contains the reserved sequence zero",
        ));
    }
    Ok(Some(HiloV1Id { sequence }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_policy() -> GeneratedIdPolicy {
        GeneratedIdPolicy::native_range_v1("id").unwrap()
    }

    fn hilo_policy() -> GeneratedIdPolicy {
        GeneratedIdPolicy::hilo_v1("id").unwrap()
    }

    #[test]
    fn every_owner_round_trips_at_both_local_boundaries() {
        for owner in 0..=MAX_ALLOCATION_OWNER_SLOT {
            let owner = AllocationOwnerSlot::new(owner).unwrap();
            for local_sequence in [1, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE] {
                let id = NativeRangeV1Id::new(owner, local_sequence).unwrap();
                let encoded = id.encode();
                assert!(encoded > 0);
                assert_eq!(NativeRangeV1Id::decode(encoded).unwrap(), id);
                assert_eq!(
                    classify_generated_id(&native_policy(), encoded).unwrap(),
                    GeneratedIdClassification::NativeRangeV1(id)
                );
            }
        }
    }

    #[test]
    fn golden_vectors_freeze_marker_owner_and_sequence_bits() {
        let vectors = [
            (0, 1, 0x4000_0000_0000_0001_i64),
            (0, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE, 0x400f_ffff_ffff_ffff),
            (1, 1, 0x4010_0000_0000_0001),
            (63, 1, 0x43f0_0000_0000_0001),
            (1_023, 1, 0x7ff0_0000_0000_0001),
            (1_023, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE, i64::MAX),
        ];

        for (owner, local_sequence, expected) in vectors {
            let owner = AllocationOwnerSlot::new(owner).unwrap();
            let id = NativeRangeV1Id::new(owner, local_sequence).unwrap();
            assert_eq!(id.encode(), expected);
            assert_eq!(id.owner(), owner);
            assert_eq!(id.local_sequence(), local_sequence);
        }
    }

    #[test]
    fn hilo_global_sequence_round_trips_at_both_boundaries() {
        for (sequence, expected) in [
            (1, 0x2000_0000_0000_0001_i64),
            (MAX_HILO_V1_SEQUENCE, 0x3fff_ffff_ffff_ffff),
        ] {
            let id = HiloV1Id::new(sequence).unwrap();
            assert_eq!(id.sequence(), sequence);
            assert_eq!(id.encode(), expected);
            assert_eq!(HiloV1Id::decode(expected).unwrap(), id);
            assert_eq!(
                classify_generated_id(&hilo_policy(), expected).unwrap(),
                GeneratedIdClassification::HiloV1(id)
            );
        }
        assert_eq!(hilo_v1_sequence_floor(), 0x2000_0000_0000_0000);
        assert_eq!(hilo_v1_first_id(), 0x2000_0000_0000_0001);
        assert_eq!(hilo_v1_sequence_ceiling(), 0x3fff_ffff_ffff_ffff);
        assert!(hilo_v1_sequence_ceiling() < NATIVE_RANGE_V1_FORMAT_MARKER as i64);
    }

    #[test]
    fn hilo_zero_overflow_and_later_namespaces_fail_closed() {
        assert_eq!(
            HiloV1Id::new(0).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            HiloV1Id::new(MAX_HILO_V1_SEQUENCE + 1).unwrap_err().kind(),
            EngineErrorKind::NumericOutOfRange
        );

        let floor = hilo_v1_sequence_floor();
        assert_eq!(
            classify_generated_id(&hilo_policy(), floor)
                .unwrap_err()
                .kind(),
            EngineErrorKind::DataCorruption
        );
        assert_eq!(
            classify_caller_generated_id(&hilo_policy(), floor)
                .unwrap_err()
                .kind(),
            EngineErrorKind::InvalidArgument
        );

        for value in [NATIVE_RANGE_V1_FORMAT_MARKER as i64, i64::MAX] {
            let stored = classify_generated_id(&hilo_policy(), value).unwrap_err();
            assert_eq!(stored.kind(), EngineErrorKind::DataCorruption);
            assert!(stored.diagnostic().contains("incompatible"));

            let caller = classify_caller_generated_id(&hilo_policy(), value).unwrap_err();
            assert_eq!(caller.kind(), EngineErrorKind::InvalidArgument);
            assert!(caller.diagnostic().contains("incompatible"));
            assert_eq!(
                HiloV1Id::decode(value).unwrap_err().kind(),
                EngineErrorKind::DataCorruption
            );
        }
    }

    #[test]
    fn hilo_policy_preserves_only_negative_and_pre_marker_legacy_ids() {
        for value in [i64::MIN, -1, 0, 1, (HILO_V1_FORMAT_MARKER - 1) as i64] {
            assert_eq!(
                classify_generated_id(&hilo_policy(), value).unwrap(),
                GeneratedIdClassification::Legacy(value)
            );
            assert_eq!(
                classify_caller_generated_id(&hilo_policy(), value).unwrap(),
                GeneratedIdClassification::Legacy(value)
            );
            assert_eq!(
                HiloV1Id::decode(value).unwrap_err().kind(),
                EngineErrorKind::NumericOutOfRange
            );
        }
    }

    #[test]
    fn sqlite_sequence_floors_are_reserved_and_advance_to_the_first_id() {
        for owner in [0, 1, 63, MAX_ALLOCATION_OWNER_SLOT] {
            let owner = AllocationOwnerSlot::new(owner).unwrap();
            let floor = native_range_v1_sequence_floor(owner);
            assert_eq!(
                classify_generated_id(&native_policy(), floor)
                    .unwrap_err()
                    .kind(),
                EngineErrorKind::DataCorruption
            );
            assert_eq!(
                native_range_v1_first_id(owner),
                NativeRangeV1Id::new(owner, 1).unwrap().encode()
            );
            assert_eq!(
                native_range_v1_sequence_ceiling(owner),
                NativeRangeV1Id::new(owner, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE)
                    .unwrap()
                    .encode()
            );
        }
    }

    #[test]
    fn caller_classification_rejects_reserved_floors_as_input_not_corruption() {
        for owner in [0, 1, 63, MAX_ALLOCATION_OWNER_SLOT] {
            let floor = native_range_v1_sequence_floor(AllocationOwnerSlot::new(owner).unwrap());
            let error = classify_caller_generated_id(&native_policy(), floor).unwrap_err();
            assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
            assert!(error.diagnostic().contains("sequence zero"));
        }
    }

    #[test]
    fn owner_and_local_overflow_fail_before_encoding() {
        assert_eq!(
            AllocationOwnerSlot::new(MAX_ALLOCATION_OWNER_SLOT + 1)
                .unwrap_err()
                .kind(),
            EngineErrorKind::NumericOutOfRange
        );
        let owner = AllocationOwnerSlot::new(0).unwrap();
        assert_eq!(
            NativeRangeV1Id::new(owner, 0).unwrap_err().kind(),
            EngineErrorKind::InvalidArgument
        );
        assert_eq!(
            NativeRangeV1Id::new(owner, MAX_NATIVE_RANGE_V1_LOCAL_SEQUENCE + 1)
                .unwrap_err()
                .kind(),
            EngineErrorKind::NumericOutOfRange
        );
    }

    #[test]
    fn policy_context_keeps_imported_marker_values_legacy() {
        for value in [
            i64::MIN,
            -1,
            0,
            1,
            HILO_V1_FORMAT_MARKER as i64,
            0x2000_0000_0000_0001,
            0x3fff_ffff_ffff_ffff,
            NATIVE_RANGE_V1_FORMAT_MARKER as i64,
            0x4000_0000_0000_0001,
            i64::MAX,
        ] {
            assert_eq!(
                classify_generated_id(&GeneratedIdPolicy::None, value).unwrap(),
                GeneratedIdClassification::Legacy(value)
            );
        }
    }

    #[test]
    fn native_policy_classifies_pre_marker_and_negative_values_as_legacy() {
        for value in [
            i64::MIN,
            -1,
            0,
            1,
            (NATIVE_RANGE_V1_FORMAT_MARKER - 1) as i64,
        ] {
            assert_eq!(
                classify_generated_id(&native_policy(), value).unwrap(),
                GeneratedIdClassification::Legacy(value)
            );
            assert_eq!(
                NativeRangeV1Id::decode(value).unwrap_err().kind(),
                EngineErrorKind::NumericOutOfRange
            );
        }
    }

    #[test]
    fn codec_types_are_owned_send_and_sync() {
        fn assert_owned<T: Clone + Send + Sync + 'static>() {}
        assert_owned::<AllocationOwnerSlot>();
        assert_owned::<AllocationOwnerState>();
        assert_owned::<AllocationOwnerMap>();
        assert_owned::<NativeRangeV1Id>();
        assert_owned::<HiloV1Id>();
        assert_owned::<GeneratedIdClassification>();
    }

    #[test]
    fn allocation_owner_map_normalizes_and_routes_in_both_directions() {
        let map = AllocationOwnerMap::try_from_pairs(
            4,
            vec![(19, 2), (7, 0), (MAX_ALLOCATION_OWNER_SLOT, 3), (11, 1)].into_boxed_slice(),
        )
        .unwrap();

        assert_eq!(map.physical_shard_count(), 4);
        assert_eq!(
            map.pairs().collect::<Vec<_>>(),
            [(7, 0), (11, 1), (19, 2), (MAX_ALLOCATION_OWNER_SLOT, 3)]
        );
        for (owner, shard) in [(7, 0), (11, 1), (19, 2), (MAX_ALLOCATION_OWNER_SLOT, 3)] {
            let owner = AllocationOwnerSlot::new(owner).unwrap();
            assert_eq!(map.physical_shard(owner), Some(shard));
            assert_eq!(map.owner_for_physical_shard(shard), Some(owner));
        }
        assert_eq!(
            map.physical_shard(AllocationOwnerSlot::new(1).unwrap()),
            None
        );
        assert_eq!(map.owner_for_physical_shard(4), None);
    }

    #[test]
    fn retired_owners_keep_historical_routes_but_cannot_allocate() {
        let map = AllocationOwnerMap::try_from_assignments(
            2,
            vec![
                (7, 0, AllocationOwnerState::Retired),
                (8, 0, AllocationOwnerState::Active),
                (11, 1, AllocationOwnerState::Active),
            ]
            .into_boxed_slice(),
        )
        .unwrap();

        let retired = AllocationOwnerSlot::new(7).unwrap();
        let replacement = AllocationOwnerSlot::new(8).unwrap();
        assert_eq!(map.physical_shard(retired), Some(0));
        assert_eq!(
            map.owner_state(retired),
            Some(AllocationOwnerState::Retired)
        );
        assert!(!map.owner_is_active(retired));
        assert_eq!(map.owner_for_physical_shard(0), Some(replacement));
        assert!(map.owner_is_active(replacement));
        assert_eq!(
            map.assignments().collect::<Vec<_>>(),
            [
                (7, 0, AllocationOwnerState::Retired),
                (8, 0, AllocationOwnerState::Active),
                (11, 1, AllocationOwnerState::Active),
            ]
        );
    }

    #[test]
    fn replacement_owner_must_advance_past_every_retired_owner_on_its_shard() {
        let error = AllocationOwnerMap::try_from_assignments(
            2,
            vec![
                (9, 0, AllocationOwnerState::Retired),
                (8, 0, AllocationOwnerState::Active),
                (0, 1, AllocationOwnerState::Active),
            ]
            .into_boxed_slice(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), EngineErrorKind::InvalidArgument);
        assert_eq!(
            error.diagnostic(),
            "active allocation owner 8 for physical shard 0 must be greater than retired owner 9"
        );

        // Succession is monotonic per physical shard, not a global ordering
        // between otherwise independent shard allocators.
        let valid = AllocationOwnerMap::try_from_assignments(
            2,
            vec![
                (7, 0, AllocationOwnerState::Retired),
                (9, 0, AllocationOwnerState::Retired),
                (10, 0, AllocationOwnerState::Active),
                (1, 1, AllocationOwnerState::Active),
            ]
            .into_boxed_slice(),
        )
        .unwrap();
        assert_eq!(
            valid.owner_for_physical_shard(0),
            Some(AllocationOwnerSlot::new(10).unwrap())
        );
        assert_eq!(
            valid.owner_for_physical_shard(1),
            Some(AllocationOwnerSlot::new(1).unwrap())
        );
    }

    #[test]
    fn owner_lifecycle_requires_one_active_allocator_per_shard() {
        for assignments in [
            vec![
                (0, 0, AllocationOwnerState::Retired),
                (1, 1, AllocationOwnerState::Active),
            ],
            vec![
                (0, 0, AllocationOwnerState::Active),
                (1, 0, AllocationOwnerState::Active),
                (2, 1, AllocationOwnerState::Active),
            ],
            vec![
                (0, 0, AllocationOwnerState::Active),
                (0, 1, AllocationOwnerState::Retired),
                (2, 1, AllocationOwnerState::Active),
            ],
        ] {
            assert!(
                AllocationOwnerMap::try_from_assignments(2, assignments.into_boxed_slice())
                    .is_err()
            );
        }
    }

    #[test]
    fn allocation_owner_map_rejects_incomplete_duplicate_and_out_of_range_rows() {
        let cases = [
            (0, vec![]),
            (2, vec![(0, 0)]),
            (2, vec![(0, 0), (0, 1)]),
            (2, vec![(0, 0), (1, 0)]),
            (2, vec![(0, 0), (1, 2)]),
            (2, vec![(0, 0), (MAX_ALLOCATION_OWNER_SLOT + 1, 1)]),
        ];
        for (shard_count, pairs) in cases {
            assert!(
                AllocationOwnerMap::try_from_pairs(shard_count, pairs.into_boxed_slice()).is_err()
            );
        }
    }
}
