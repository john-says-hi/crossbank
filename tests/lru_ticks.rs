//! The LRU's logical clock must be durable-monotonic across a reopen.
//!
//! A read allocates a tick and deliberately writes nothing, so the persisted
//! `next_tick` lags the cursor that issued them; and two banks over one store
//! keep separate cursors, so the last one to commit can leave a *lower*
//! high-water mark in `meta` than ticks another cursor already recorded
//! against keys. A reopen that trusted only that number would hand out ticks
//! below what the `lru::` rows already hold, and the LRU would then shed a key
//! that had just been read instead of the one nobody has touched in weeks.
//!
//! Native only: this needs two banks over one in-RAM store and a raw scan of
//! `meta`, none of which a browser adds anything to. Gated rather than left to
//! run zero tests in a wasm lane, which is how a suite sits green having never
//! run.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use crossbank::backend::{Backend, KeyRange, MemoryBackend, ScanRequest, Table};
use crossbank::{Bank, LockerConfig, Policy};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

const LOCKER: &str = "cache";

fn cache() -> LockerConfig {
    // Far larger than anything written here: this test is about the clock,
    // and an eviction would only muddy what it reads back.
    LockerConfig::default().with_policy(Policy::Evictable { max_bytes: 1 << 20 })
}

/// Every `lru::` record in the store, as `(meta key, tick)`.
async fn recorded_ticks(backend: &dyn Backend) -> Vec<(Vec<u8>, u64)> {
    let page = backend
        .scan(ScanRequest {
            table: Table::Meta,
            range: KeyRange::prefix(b"lru::"),
            reverse: false,
            limit: 4096,
            want_values: true,
        })
        .await
        .expect("scan meta");
    page.items
        .into_iter()
        .filter_map(|(key, value)| {
            let raw = value?;
            let tick = u64::from_be_bytes(raw.get(0..8)?.try_into().ok()?);
            Some((key, tick))
        })
        .collect()
}

#[test]
fn a_reopened_bank_issues_ticks_above_every_recorded_one() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());

        // ---- session one -------------------------------------------------
        let first = Bank::with_backend(backend.clone()).await.expect("bank");
        let a = first
            .lazy_locker_with::<Vec<u8>>(LOCKER, cache())
            .await
            .expect("open");
        for key in ["alpha", "bravo", "charlie"] {
            a.put(key, &vec![1u8; 8]).await.expect("put");
        }

        // A second bank over the same store — the shape two browser tabs
        // have — seeds its own cursor from what is stored right now.
        let second = Bank::with_backend(backend.clone()).await.expect("bank");
        let b = second
            .lazy_locker_with::<Vec<u8>>(LOCKER, cache())
            .await
            .expect("open");
        b.put("delta", &vec![2u8; 8]).await.expect("put");

        // A long read-only burst. Every read allocates a tick and writes
        // nothing, so this moves the first bank's cursor a long way past
        // anything `meta` records.
        for _ in 0..50 {
            for key in ["alpha", "bravo", "charlie"] {
                assert!(a.get(key).await.expect("read").is_some(), "{key} vanished");
            }
        }
        // One write carries those deferred bumps into the records, so the
        // rows now hold ticks far above where the burst started.
        a.put("echo", &vec![3u8; 8]).await.expect("put");

        // The other bank's cursor knows nothing of that burst, so its next
        // write puts a *lower* high-water mark back into `meta`.
        b.put("foxtrot", &vec![4u8; 8]).await.expect("put");

        // Not `Bank::close`: closing a memory backend takes the data with it.
        drop(a);
        drop(b);
        drop(first);
        drop(second);

        // ---- session two -------------------------------------------------
        let reopened = Bank::with_backend(backend.clone()).await.expect("reopen");
        let c = reopened
            .lazy_locker_with::<Vec<u8>>(LOCKER, cache())
            .await
            .expect("open");
        c.put("golf", &vec![5u8; 8]).await.expect("put");

        let rows = recorded_ticks(backend.as_ref()).await;
        let golf = rows
            .iter()
            .find(|(key, _)| key.ends_with(b"golf"))
            .map(|(_, tick)| *tick)
            .expect("the key just written has an LRU record");
        for (key, tick) in &rows {
            if key.ends_with(b"golf") {
                continue;
            }
            assert!(
                golf > *tick,
                "the freshest write was issued tick {golf}, which is not above \
                 the {tick} already recorded for {:?} — a reopened clock has \
                 re-issued a tick and the LRU will shed the wrong key",
                String::from_utf8_lossy(&key[key.len().saturating_sub(8)..]),
            );
        }
    });
}
