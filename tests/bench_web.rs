//! Web timings for the same named workloads as `benches/kv.rs`.
//!
//! Criterion does not run in wasm. This prints one JSON document to the
//! browser console. Ignored by default so it is not a CI gate.
//!
//!   ci/wasm-test.sh --plain --firefox -- --test bench_web --include-ignored

#![cfg(target_arch = "wasm32")]

use crossbank::{Bank, BankConfig};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn now_ms() -> f64 {
    js_sys::Date::now()
}

fn payload(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add(i as u8)).collect()
}

#[wasm_bindgen_test]
#[ignore]
async fn web_workload_timings() {
    let db_name = format!("crossbank-bench-{}", js_sys::Date::now() as u64);
    let bank = Bank::open(BankConfig::web(&db_name)).await.unwrap();

    let settings = bank.locker::<Vec<u8>>("settings").await.unwrap();
    for i in 0..50 {
        settings
            .put(&format!("k{i:04}"), payload(1024, i as u8))
            .await
            .unwrap();
    }
    let start = now_ms();
    for _ in 0..200 {
        let _ = settings.get("k0001");
    }
    let settings_get = (now_ms() - start) / 200.0;

    let lazy = bank.lazy_locker::<Vec<u8>>("bulk").await.unwrap();
    let start = now_ms();
    for i in 0..200 {
        lazy.put(&format!("k{i:04}"), &payload(256, i as u8))
            .await
            .unwrap();
    }
    let put_ms = now_ms() - start;

    let start = now_ms();
    for _ in 0..200 {
        let _ = lazy.get("k0001").await.unwrap();
    }
    let get_ms = (now_ms() - start) / 200.0;

    let doc = format!(
        "{{\"backend\":\"indexeddb\",\"settings_get_ms\":{settings_get:.4},\"bulk_put_200_ms\":{put_ms:.4},\"bulk_get_ms\":{get_ms:.4}}}"
    );
    web_sys::console::log_1(&doc.into());

    crossbank::IndexedDbBackend::delete_database(&db_name)
        .await
        .unwrap();
}
