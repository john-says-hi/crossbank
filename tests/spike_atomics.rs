//! M0 spike: does the threaded / shared-memory build actually work, and does
//! IndexedDB still accept our bytes when wasm memory is a `SharedArrayBuffer`?
//!
//! This is the lane that matters most, because a threaded build is what a real
//! consumer ships. The specific hazard: under `--shared-memory`,
//! `wasm_bindgen::memory().buffer` is a `SharedArrayBuffer`, and IndexedDB's
//! structured-clone-for-storage step *throws* `DataCloneError` on one. So the
//! obvious zero-copy write path (`Uint8Array::view`, which aliases wasm memory)
//! fails only on the atomics build — the one that ships.
//!
//! `Uint8Array::from` copies into a fresh, non-shared buffer, which is why
//! crossbank must always use it. This spike proves both halves.
//!
//! Plain lane:
//!   wasm-pack test --headless --firefox
//! Atomics lane:
//!   ci/wasm-test.sh --atomics --firefox --browser

#![cfg(target_arch = "wasm32")]

use std::convert::Infallible;

use indexed_db::Factory;
use js_sys::Uint8Array;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const STORE: &str = "atomics_store";

/// Reports which lane we are in, so a misconfigured atomics job cannot
/// masquerade as a passing plain job.
#[wasm_bindgen_test]
fn lane_is_what_it_claims() {
    let atomics = cfg!(target_feature = "atomics");

    let buffer = wasm_bindgen::memory()
        .dyn_into::<js_sys::WebAssembly::Memory>()
        .map(|m| m.buffer())
        .unwrap_or(JsValue::UNDEFINED);

    let ctor = js_sys::Reflect::get(&buffer, &"constructor".into())
        .ok()
        .and_then(|c| js_sys::Reflect::get(&c, &"name".into()).ok())
        .and_then(|n| n.as_string())
        .unwrap_or_else(|| "unknown".to_string());

    web_sys::console::log_1(
        &format!("CROSSBANK-SPIKE atomics={atomics} memory_buffer={ctor}").into(),
    );

    // When built with +atomics, wasm memory must be shared. If it is not, the
    // rustflags did not reach the compiler and the lane is proving nothing.
    if atomics {
        assert_eq!(
            ctor, "SharedArrayBuffer",
            "built with +atomics but wasm memory is not shared — rustflags did not apply"
        );
    }

    // The guard that makes this test worth having.
    //
    // Without it, this test passes in BOTH lanes and therefore discriminates
    // nothing — a silently-plain "atomics" job would look green. The atomics
    // lane sets CROSSBANK_EXPECT_ATOMICS=1, read at compile time, and we
    // assert the build matches what the lane claims.
    let expected = option_env!("CROSSBANK_EXPECT_ATOMICS") == Some("1");
    assert_eq!(
        atomics, expected,
        "lane mismatch: CROSSBANK_EXPECT_ATOMICS={expected} but target_feature=atomics is {atomics}"
    );
}

/// A 1 MiB value through IndexedDB using `Uint8Array::from`.
///
/// Must pass in *both* lanes. If it fails only under atomics, the copy is not
/// happening and something is handing IndexedDB a shared buffer.
#[wasm_bindgen_test]
async fn one_mib_round_trips_via_copy() {
    let db_name = format!("crossbank_atomics_{}", js_sys::Date::now() as u64);
    let payload: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();

    let factory = Factory::<Infallible>::get().expect("no IndexedDB factory");
    let db = factory
        .open(&db_name, 1, async move |evt| {
            evt.database().build_object_store(STORE).create()?;
            Ok(())
        })
        .await
        .expect("open failed");

    // `from` copies into a fresh non-shared buffer. `view` would alias wasm
    // memory and throw DataCloneError under --shared-memory.
    let value = Uint8Array::from(payload.as_slice());

    db.transaction(&[STORE])
        .rw()
        .run(async move |t| {
            let store = t.object_store(STORE)?;
            store.put_kv(&JsValue::from_str("big"), &value).await?;
            Ok(())
        })
        .await
        .expect("1 MiB write failed");

    let got = db
        .transaction(&[STORE])
        .run(async move |t| {
            let store = t.object_store(STORE)?;
            store.get(&JsValue::from_str("big")).await
        })
        .await
        .expect("read failed")
        .expect("1 MiB value missing");

    assert_eq!(
        Uint8Array::new(&got).to_vec(),
        payload,
        "1 MiB value changed"
    );

    db.close();
}
