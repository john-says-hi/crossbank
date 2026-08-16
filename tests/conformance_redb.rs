//! The shared spec, run against the `redb` backend.
//!
//! Four lines. That is the entire cost of adding a backend, and it is the
//! reason the suite lives in its own crate: the behaviour a backend must
//! satisfy is written once, not once per backend.

#![cfg(not(target_arch = "wasm32"))]

crossbank_conformance::conformance_suite!(crossbank_conformance::harness::RedbHarness::new);
