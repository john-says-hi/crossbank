//! Web timings for the same named workloads as `benches/kv.rs`.
//!
//! Criterion does not run in wasm, so this lane rolls its own sampling: one
//! un-timed warm-up plus [`ITERATIONS`] timed iterations per workload, timed
//! with `performance.now()` (microsecond-ish resolution), reported as a median
//! and a p99 — the same maths `bench/hive_ce/lib/workloads.dart` uses, so the
//! Hive row and the crossbank row of a table are computed identically.
//!
//! Shapes come from `benches/common/mod.rs`, shared with `benches/kv.rs`.
//!
//! It prints ONE compact JSON document to the console prefixed with
//! `BENCH_JSON `, in the same schema `bench/hive_ce/web` emits, which
//! `ci/web-bench/merge-crossbank.mjs` merges into `bench/results/<date>-web.json`.
//!
//! Ignored by default so it is not a CI gate:
//!
//!   ci/bench.sh --web --chrome       (or --firefox)

#![cfg(target_arch = "wasm32")]
// The `bench!` macro assigns into caller-declared slots from its `setup` block
// and clears them in `teardown`; the last clear of each is genuinely dead.
#![allow(unused_assignments)]

use crossbank::{Bank, BankConfig, LazyLocker, Locker, LockerConfig};
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[path = "../benches/common/mod.rs"]
mod common;

use common::{
    key, payload, small_key, BIG_BYTES, BULK_BYTES, BULK_GET_OPS, BULK_N, GET_STRIDE, ITERATIONS,
    SETTINGS_BYTES, SETTINGS_N, SETTINGS_OPS, SMALL_BULK_N, SMALL_SETTINGS_N, SMALL_SETTINGS_OPS,
    TXN_N,
};

const BACKEND: &str = "crossbank_indexeddb_web";

// `performance.now()` rather than `Date.now()`: the latter is quantised to
// whole milliseconds on this platform, which reported sub-millisecond loops as
// zero. Bound directly instead of through `web_sys::Performance` so this bench
// needs no new `web-sys` feature (Cargo.toml belongs to another lane).
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn perf_now() -> f64;
}

struct Sample {
    workload: &'static str,
    n: usize,
    bytes: usize,
    p50_ms: f64,
    p99_ms: f64,
    ops_per_s: f64,
}

impl Sample {
    fn new(workload: &'static str, n: usize, bytes: usize, mut ms: Vec<f64>) -> Self {
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pct = |p: f64| -> f64 {
            let idx = (((ms.len() - 1) as f64) * p).round() as usize;
            ms[idx]
        };
        let p50 = pct(0.5);
        Self {
            workload,
            n,
            bytes,
            p50_ms: p50,
            p99_ms: pct(0.99),
            ops_per_s: if p50 == 0.0 {
                0.0
            } else {
                n as f64 / (p50 / 1000.0)
            },
        }
    }

    fn to_json(&self) -> String {
        format!(
            "{{\"workload\":\"{}\",\"backend\":\"{}\",\"n\":{},\"bytes\":{},\"p50_ms\":{:.4},\"p99_ms\":{:.4},\"ops_per_s\":{:.4}}}",
            self.workload, BACKEND, self.n, self.bytes, self.p50_ms, self.p99_ms, self.ops_per_s
        )
    }
}

/// One warm-up plus [`ITERATIONS`] timed runs of `body`, with `setup` and
/// `teardown` outside the clock. The blocks are pasted into this async fn's
/// scope, so they may `.await` and may assign to variables declared by the
/// caller — which is how `setup` hands a freshly created locker to `body`.
macro_rules! bench {
    ($samples:expr, $workload:literal, $n:expr, $bytes:expr,
     setup $setup:block body $body:block teardown $teardown:block) => {{
        web_sys::console::log_1(&format!("running {}", $workload).into());
        $setup
        $body
        $teardown
        let mut ms = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            $setup
            let start = perf_now();
            $body
            ms.push(perf_now() - start);
            $teardown
        }
        $samples.push(Sample::new($workload, $n, $bytes, ms));
    }};
}

#[wasm_bindgen_test]
#[ignore]
async fn web_workload_timings() {
    let stamp = js_sys::Date::now() as u64;
    let db_name = format!("crossbank-bench-{stamp}");
    let bank = Bank::open(BankConfig::web(&db_name)).await.unwrap();
    let mut samples: Vec<Sample> = Vec::new();
    // A fresh locker per iteration, as the Hive tool uses a fresh box: a
    // locker that already holds the previous iteration's keys is a different
    // workload from the one this claims to measure.
    let mut lockers = 0usize;
    let mut fresh = || {
        lockers += 1;
        format!("l{stamp}_{lockers}")
    };

    // ---- Large shapes: identical to benches/kv.rs and bench/hive_ce. -------

    {
        let mut name = String::new();
        let mut eager: Option<Locker<Vec<u8>>> = None;
        let mut i = 0usize;
        bench!(samples, "settings_eager", SETTINGS_OPS, SETTINGS_BYTES,
            setup {
                name = fresh();
                let l = bank.locker::<Vec<u8>>(&name).await.unwrap();
                for k in 0..SETTINGS_N {
                    l.put(&key(k), payload(SETTINGS_BYTES, k as u8)).await.unwrap();
                }
                eager = Some(l);
            }
            body {
                let l = eager.as_ref().unwrap();
                for _ in 0..SETTINGS_OPS {
                    if i % 10 == 0 {
                        l.put(&key(i % SETTINGS_N), payload(SETTINGS_BYTES, i as u8))
                            .await
                            .unwrap();
                    } else {
                        let _ = l.get(&key(i % SETTINGS_N));
                    }
                    i += 1;
                }
            }
            teardown {
                eager = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        bench!(samples, "bulk_lazy_put", BULK_N, BULK_N * BULK_BYTES,
            setup {
                name = fresh();
                lazy = Some(bank.lazy_locker::<Vec<u8>>(&name).await.unwrap());
            }
            body {
                let l = lazy.as_ref().unwrap();
                for k in 0..BULK_N {
                    l.put(&key(k), &payload(BULK_BYTES, k as u8)).await.unwrap();
                }
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        bench!(samples, "bulk_lazy_get", BULK_GET_OPS, BULK_BYTES,
            setup {
                name = fresh();
                let l = bank.lazy_locker::<Vec<u8>>(&name).await.unwrap();
                for k in 0..BULK_N {
                    l.put(&key(k), &payload(BULK_BYTES, k as u8)).await.unwrap();
                }
                lazy = Some(l);
            }
            body {
                let l = lazy.as_ref().unwrap();
                for op in 0..BULK_GET_OPS {
                    let _ = l.get(&key((op * GET_STRIDE) % BULK_N)).await.unwrap();
                }
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        let mut gen = 0u64;
        bench!(samples, "txn_batch", TXN_N, TXN_N * 64,
            setup {
                name = fresh();
                lazy = Some(bank.lazy_locker::<Vec<u8>>(&name).await.unwrap());
            }
            body {
                gen += 1;
                let g = gen;
                lazy
                    .as_ref()
                    .unwrap()
                    .transact(move |tx| async move {
                        for k in 0..TXN_N {
                            tx.put(&format!("{g}:{k}"), payload(64, k as u8))?;
                        }
                        Ok(())
                    })
                    .await
                    .unwrap();
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        // The one workload that needs a whole database of its own per
        // iteration: it is measuring `Bank::open`.
        let mut name = String::new();
        let mut reopened: Option<Bank> = None;
        let mut round = 0usize;
        bench!(samples, "reopen", 1, 1024,
            setup {
                round += 1;
                name = format!("{db_name}-reopen-{round}");
                let b = Bank::open(BankConfig::web(&name)).await.unwrap();
                let l = b.lazy_locker::<Vec<u8>>("l").await.unwrap();
                l.put("k", &payload(1024, 1)).await.unwrap();
                b.close().await.unwrap();
            }
            body {
                let b = Bank::open(BankConfig::web(&name)).await.unwrap();
                let l = b.lazy_locker::<Vec<u8>>("l").await.unwrap();
                let _ = l.get("k").await.unwrap();
                reopened = Some(b);
            }
            teardown {
                if let Some(b) = reopened.take() {
                    b.close().await.unwrap();
                }
                crossbank::IndexedDbBackend::delete_database(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        let big = payload(BIG_BYTES, 3);
        bench!(samples, "big_value_put_get", 1, BIG_BYTES,
            setup {
                name = fresh();
                lazy = Some(
                    bank.lazy_locker_with::<Vec<u8>>(&name, LockerConfig::default())
                        .await
                        .unwrap(),
                );
            }
            body {
                let l = lazy.as_ref().unwrap();
                l.put("k", &big).await.unwrap();
                let got = l.get("k").await.unwrap().unwrap();
                assert_eq!(got.len(), big.len(), "the chunked value must round-trip");
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    // ---- Small shapes: the pre-Phase-5 bench_web.rs shapes, kept so the
    // 2026-08-21 pre-Phase-3 rows still have a like-for-like successor. ------

    {
        let mut name = String::new();
        let mut eager: Option<Locker<Vec<u8>>> = None;
        bench!(samples, "settings_eager_web_small", SMALL_SETTINGS_OPS, SETTINGS_BYTES,
            setup {
                name = fresh();
                let l = bank.locker::<Vec<u8>>(&name).await.unwrap();
                for k in 0..SMALL_SETTINGS_N {
                    l.put(&small_key(k), payload(SETTINGS_BYTES, k as u8)).await.unwrap();
                }
                eager = Some(l);
            }
            body {
                let l = eager.as_ref().unwrap();
                for _ in 0..SMALL_SETTINGS_OPS {
                    let _ = l.get(&small_key(1));
                }
            }
            teardown {
                eager = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        bench!(samples, "bulk_lazy_put_web_small", SMALL_BULK_N, SMALL_BULK_N * BULK_BYTES,
            setup {
                name = fresh();
                lazy = Some(bank.lazy_locker::<Vec<u8>>(&name).await.unwrap());
            }
            body {
                let l = lazy.as_ref().unwrap();
                for k in 0..SMALL_BULK_N {
                    l.put(&small_key(k), &payload(BULK_BYTES, k as u8)).await.unwrap();
                }
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    {
        let mut name = String::new();
        let mut lazy: Option<LazyLocker<Vec<u8>>> = None;
        bench!(samples, "bulk_lazy_get_web_small", SMALL_BULK_N, BULK_BYTES,
            setup {
                name = fresh();
                let l = bank.lazy_locker::<Vec<u8>>(&name).await.unwrap();
                for k in 0..SMALL_BULK_N {
                    l.put(&small_key(k), &payload(BULK_BYTES, k as u8)).await.unwrap();
                }
                lazy = Some(l);
            }
            body {
                let l = lazy.as_ref().unwrap();
                for _ in 0..SMALL_BULK_N {
                    let _ = l.get(&small_key(1)).await.unwrap();
                }
            }
            teardown {
                lazy = None;
                bank.delete_locker(&name).await.unwrap();
            }
        );
    }

    // Tear the database down BEFORE printing. The runner used to lose the
    // browser during a post-print teardown and exit non-zero on a run that had
    // already produced its numbers; with the cleanup first, the JSON is the
    // last thing that happens and the exit code means what it says.
    bank.close().await.unwrap();
    crossbank::IndexedDbBackend::delete_database(&db_name)
        .await
        .unwrap();

    let rows: Vec<String> = samples.iter().map(Sample::to_json).collect();
    let date = js_sys::Date::new_0().to_iso_string();
    let doc = format!(
        "{{\"tool\":\"tests/bench_web.rs\",\"date_utc\":\"{}\",\"iterations\":{},\"samples\":[{}]}}",
        String::from(date),
        ITERATIONS,
        rows.join(",")
    );
    web_sys::console::log_1(&format!("BENCH_JSON {doc}").into());
}
