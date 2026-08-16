//! M0 spike: does the headless browser test lane actually work, and is the
//! page cross-origin isolated?
//!
//! This is the highest-risk unknown in the whole plan. `SharedArrayBuffer` —
//! and therefore any threaded/shared-memory wasm build — is only exposed when
//! the page is cross-origin isolated, which needs
//! `Cross-Origin-Opener-Policy: same-origin` and
//! `Cross-Origin-Embedder-Policy: require-corp` on the document response.
//! Whether `wasm-bindgen-test-runner`'s built-in server sends those headers is
//! what this spike answers.
//!
//! Run with:
//!   wasm-pack test --headless --chrome
//!   wasm-pack test --headless --firefox
//!
//! These tests are diagnostic: they report the environment rather than assert
//! a particular answer, so the lane itself can be proven before we depend on
//! any specific capability.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Baseline: proves the browser runner executes anything at all.
#[wasm_bindgen_test]
fn browser_runner_executes() {
    assert_eq!(2 + 2, 4);
}

/// Proves we are in a real browser context with a DOM, not Node.
#[wasm_bindgen_test]
fn window_is_available() {
    assert!(
        web_sys::window().is_some(),
        "no Window — this is not a browser context"
    );
}

/// The question that matters: is `SharedArrayBuffer` reachable here?
///
/// Reports rather than asserts. A `false` result does not fail the spike — it
/// tells us the runner's server does not set COOP/COEP, and that we need one
/// of the documented fallbacks before the atomics lane can exist.
#[wasm_bindgen_test]
fn report_cross_origin_isolation() {
    let global = js_sys::global();

    let has_sab = js_sys::Reflect::get(&global, &"SharedArrayBuffer".into())
        .map(|v| !v.is_undefined())
        .unwrap_or(false);

    let cross_origin_isolated = js_sys::Reflect::get(&global, &"crossOriginIsolated".into())
        .ok()
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    web_sys::console::log_1(
        &format!(
            "CROSSBANK-SPIKE SharedArrayBuffer={has_sab} crossOriginIsolated={cross_origin_isolated}"
        )
        .into(),
    );

    // Asserted on purpose. M0's whole question is whether the runner's server
    // sets COOP/COEP, and wasm-pack does not forward --nocapture, so the
    // assertion message is how the answer gets reported either way.
    assert!(
        has_sab && cross_origin_isolated,
        "NOT cross-origin isolated: SharedArrayBuffer={has_sab} crossOriginIsolated={cross_origin_isolated} \
         — the atomics lane needs a webdriver.json capability override or a custom header-setting server"
    );
}

/// IndexedDB must at minimum be reachable from this context, or the whole
/// web backend is a non-starter.
#[wasm_bindgen_test]
fn indexeddb_factory_is_reachable() {
    let window = web_sys::window().expect("no Window");
    let factory = js_sys::Reflect::get(&window, &"indexedDB".into())
        .expect("indexedDB lookup threw");

    assert!(
        !factory.is_undefined() && !factory.is_null(),
        "indexedDB is not available in this context"
    );
}
