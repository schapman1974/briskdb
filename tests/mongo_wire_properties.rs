#![cfg(feature = "mongo")]

#[path = "support/mongo_wire_fuzz.rs"]
mod harness;

use proptest::prelude::*;

#[test]
fn deterministic_seeds_reach_valid_requests_crc_sequences_and_legacy_handshake() {
    for seed in [
        vec![],
        vec![0; 512],
        vec![255; 512],
        (0..=255).collect(),
        vec![1; 16 * 1024],
    ] {
        harness::check(&seed);
    }
    for length in [i32::MIN, -1, 0, 15, 16, 1_048_576, 1_048_577, i32::MAX] {
        harness::check(&length.to_le_bytes());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn fragmented_and_coalesced_wire_inputs_keep_boundaries_and_request_outcomes(
        data in proptest::collection::vec(any::<u8>(), 0..4096)
    ) {
        harness::check(&data);
    }
}
