//! The shared spec, run against the in-memory backend.
//!
//! Short by design: a backend costs one file like this, and the behaviour it
//! must satisfy lives in exactly one place.
//!
//! This runs natively *and* in a browser. The memory backend is identical on
//! both, so a wasm failure here means the harness or the emitter is broken
//! rather than the backend — which is precisely what makes it a useful canary
//! ahead of the IndexedDB backend landing.

// Without this the runner defaults to Node, and a browser-only lane would exit
// 0 having run nothing. ci/assert-tests-ran.sh is the backstop; this is the fix.
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

crossbank_conformance::conformance_suite!(crossbank_conformance::harness::MemoryHarness::new);
