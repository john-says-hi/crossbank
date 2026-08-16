//! M0 spike: key ordering must be identical on every backend, or `range()`
//! silently returns different results on web than on native.
//!
//! IndexedDB compares **string** keys by UTF-16 code units. `redb` and
//! `BTreeMap` compare by UTF-8 bytes. These disagree for anything above the
//! Basic Multilingual Plane, because a character like U+1F34E encodes as the
//! surrogate pair D83C DF4E in UTF-16 — which sorts *below* U+E000 — while in
//! UTF-8 it is F0 9F 8D 8E, which sorts *above* U+E000's EE 80 80.
//!
//! So a single emoji in a key reverses the order of a range scan, on one
//! platform only. This spike proves both the hazard and the fix: store keys as
//! **binary** IndexedDB keys holding the UTF-8 bytes, which IndexedDB orders
//! bytewise — exactly matching `BTreeMap<Vec<u8>>` and `redb`.

#![cfg(target_arch = "wasm32")]

use std::collections::BTreeSet;
use std::convert::Infallible;

use indexed_db::Factory;
use js_sys::Uint8Array;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const BINARY_STORE: &str = "binary_keys";
const STRING_STORE: &str = "string_keys";

/// Keys chosen so UTF-8 and UTF-16 orderings genuinely disagree.
fn sample_keys() -> Vec<&'static str> {
    vec![
        "a",
        "z",
        "candles::BTCUSDT::0000001700",
        "\u{E000}",  // private use: UTF-8 EE 80 80, UTF-16 E000
        "\u{1F34E}", // 🍎 astral: UTF-8 F0 9F 8D 8E, UTF-16 D83C DF4E
        "\u{FFFD}",  // replacement char: UTF-8 EF BF BD, UTF-16 FFFD
    ]
}

/// The native reference ordering: UTF-8 bytewise, exactly what `BTreeMap` and
/// `redb` produce.
fn native_order() -> Vec<Vec<u8>> {
    sample_keys()
        .into_iter()
        .map(|k| k.as_bytes().to_vec())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn open(db_name: &str) -> indexed_db::Database<Infallible> {
    Factory::<Infallible>::get()
        .expect("no IndexedDB factory")
        .open(db_name, 1, async move |evt| {
            evt.database().build_object_store(BINARY_STORE).create()?;
            evt.database().build_object_store(STRING_STORE).create()?;
            Ok(())
        })
        .await
        .expect("open failed")
}

/// THE FIX: binary keys order bytewise in IndexedDB, matching native exactly.
#[wasm_bindgen_test]
async fn binary_keys_match_native_ordering() {
    let db = open(&format!(
        "crossbank_keyorder_bin_{}",
        js_sys::Date::now() as u64
    ))
    .await;

    db.transaction(&[BINARY_STORE])
        .rw()
        .run(async move |t| {
            let store = t.object_store(BINARY_STORE)?;
            for key in sample_keys() {
                let k = Uint8Array::from(key.as_bytes());
                store.put_kv(&k, &JsValue::from_str("v")).await?;
            }
            Ok(())
        })
        .await
        .expect("write failed");

    let keys = db
        .transaction(&[BINARY_STORE])
        .run(async move |t| t.object_store(BINARY_STORE)?.get_all_keys(None).await)
        .await
        .expect("read failed");

    let from_idb: Vec<Vec<u8>> = keys.iter().map(|k| Uint8Array::new(k).to_vec()).collect();

    assert_eq!(
        from_idb,
        native_order(),
        "binary IndexedDB key order diverged from BTreeMap/redb UTF-8 byte order"
    );

    db.close();
}

/// THE HAZARD, pinned as a test so nobody "simplifies" back to string keys.
///
/// Asserts that string keys DO diverge. If this ever starts failing, IndexedDB
/// changed its collation and the comment above needs revisiting — but until
/// then, this is why crossbank stores binary keys.
#[wasm_bindgen_test]
async fn string_keys_diverge_from_native_ordering() {
    let db = open(&format!(
        "crossbank_keyorder_str_{}",
        js_sys::Date::now() as u64
    ))
    .await;

    db.transaction(&[STRING_STORE])
        .rw()
        .run(async move |t| {
            let store = t.object_store(STRING_STORE)?;
            for key in sample_keys() {
                store
                    .put_kv(&JsValue::from_str(key), &JsValue::from_str("v"))
                    .await?;
            }
            Ok(())
        })
        .await
        .expect("write failed");

    let keys = db
        .transaction(&[STRING_STORE])
        .run(async move |t| t.object_store(STRING_STORE)?.get_all_keys(None).await)
        .await
        .expect("read failed");

    let from_idb: Vec<Vec<u8>> = keys
        .iter()
        .map(|k| k.as_string().expect("key was not a string").into_bytes())
        .collect();

    assert_ne!(
        from_idb,
        native_order(),
        "string keys unexpectedly matched UTF-8 order — the astral-plane sample may be wrong"
    );

    db.close();
}
