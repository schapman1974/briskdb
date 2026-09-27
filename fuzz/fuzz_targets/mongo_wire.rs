#![no_main]

#[path = "../../tests/support/mongo_wire_fuzz.rs"]
mod harness;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| harness::check(data));
