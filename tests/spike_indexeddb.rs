//! M0 spike: does `indexed-db` actually persist bytes, and does the data
//! survive closing and reopening the database?
//!
//! This is the second milestone-zero question. The first (proven in
//! `spike_browser.rs`) was whether the headless lane runs cross-origin
//! isolated. This one asks whether the candidate web backend works at all.
//!
//! Run with:
//!   wasm-pack test --headless --firefox

#![cfg(target_arch = "wasm32")]

use std::convert::Infallible;

use indexed_db::Factory;
use js_sys::Uint8Array;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const STORE: &str = "spike_store";

/// Bytes written, then read back through a *separate* database connection.
///
/// Reopening is the part that matters. An in-memory round trip would prove
/// nothing about persistence.
#[wasm_bindgen_test]
async fn bytes_survive_close_and_reopen() {
    // A fresh database name per run, so a stale one cannot fake a pass.
    let db_name = format!("crossbank_spike_{}", js_sys::Date::now() as u64);

    let payload: Vec<u8> = (0u8..=255).cycle().take(4096).collect();

    // --- connection 1: write ---
    {
        let factory = Factory::<Infallible>::get().expect("no IndexedDB factory");
        let db = factory
            .open(&db_name, 1, async move |evt| {
                evt.database().build_object_store(STORE).create()?;
                Ok(())
            })
            .await
            .expect("open for write failed");

        let value = Uint8Array::from(payload.as_slice());
        db.transaction(&[STORE])
            .rw()
            .run(async move |t| {
                let store = t.object_store(STORE)?;
                store.put_kv(&JsValue::from_str("key"), &value).await?;
                Ok(())
            })
            .await
            .expect("write transaction failed");

        db.close();
    }

    // --- connection 2: read back ---
    {
        let factory = Factory::<Infallible>::get().expect("no IndexedDB factory");
        let db = factory
            .open(&db_name, 1, async move |_evt| Ok(()))
            .await
            .expect("reopen failed");

        let got = db
            .transaction(&[STORE])
            .run(async move |t| {
                let store = t.object_store(STORE)?;
                store.get(&JsValue::from_str("key")).await
            })
            .await
            .expect("read transaction failed")
            .expect("key missing after reopen");

        let round_tripped = Uint8Array::new(&got).to_vec();

        assert_eq!(
            round_tripped.len(),
            payload.len(),
            "length changed across reopen"
        );
        assert_eq!(round_tripped, payload, "bytes changed across reopen");

        db.close();
    }
}

/// Negative control for the test above, and a real behaviour in its own right:
/// a key that was never written must read back as `None`, not as empty bytes.
///
/// Without this, `bytes_survive_close_and_reopen` could pass vacuously if the
/// read path returned something truthy for everything.
#[wasm_bindgen_test]
async fn missing_key_reads_as_none() {
    let db_name = format!("crossbank_spike_absent_{}", js_sys::Date::now() as u64);

    let factory = Factory::<Infallible>::get().expect("no IndexedDB factory");
    let db = factory
        .open(&db_name, 1, async move |evt| {
            evt.database().build_object_store(STORE).create()?;
            Ok(())
        })
        .await
        .expect("open failed");

    let got = db
        .transaction(&[STORE])
        .run(async move |t| {
            let store = t.object_store(STORE)?;
            store.get(&JsValue::from_str("never-written")).await
        })
        .await
        .expect("read transaction failed");

    assert!(got.is_none(), "absent key returned a value: {got:?}");

    db.close();
}
