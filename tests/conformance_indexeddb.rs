//! The shared spec, run against the IndexedDB backend.
//!
//! Four lines. That is the entire cost of adding a backend.

#![cfg(target_arch = "wasm32")]

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

crossbank_conformance::conformance_suite!(crossbank_conformance::harness::IndexedDbHarness::new);
