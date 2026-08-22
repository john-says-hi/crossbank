//! One bank, two filter chains.
//!
//! A settings locker that stores bytes exactly as given, next to a candle
//! locker that compresses them — and the guard that stops either one being
//! reopened under the other's chain, which would hand stored bytes to the
//! wrong inverse transform and decode them into plausible garbage.
//!
//! Native only. Nothing here is backend-specific: the chain is chosen and
//! enforced entirely above the `Backend` trait, so a browser lane would run
//! the same code over the same memory backend and learn nothing.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use crossbank::backend::{Backend, MemoryBackend, Op, Table};
use crossbank::codec::{FilterChain, Lz4};
use crossbank::{Bank, Error, LockerConfig};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

/// Bytes exactly as given — the right chain for a small settings blob where a
/// compression pass is pure cost.
fn raw_chain() -> Arc<FilterChain> {
    Arc::new(FilterChain::raw())
}

/// LZ4 only, under an id of its own. Deliberately *not* the default chain's
/// id: an id names how bytes were transformed, so two chains that transform
/// differently must never share one.
fn lz4_chain() -> Arc<FilterChain> {
    Arc::new(FilterChain::new(7, vec![Box::new(Lz4)]))
}

/// The headline case: two lockers in one bank, each sealed with its own
/// chain, both round-tripping.
#[test]
fn two_lockers_in_one_bank_can_use_different_chains() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");

        let settings = bank
            .locker_with::<String>("settings", LockerConfig::default().with_chain(raw_chain()))
            .await
            .expect("settings locker");
        let candles = bank
            .lazy_locker_with::<Vec<u8>>(
                "candles",
                LockerConfig::default()
                    .with_chain(lz4_chain())
                    .with_chunk_size(4096),
            )
            .await
            .expect("candle locker");

        settings
            .put("theme", "dark".to_string())
            .await
            .expect("put setting");
        // Large enough to be chunked, so every piece is sealed with the
        // locker's own chain rather than the bank's.
        let series = vec![3u8; 40_000];
        candles.put("BTCUSDT", &series).await.expect("put candles");

        assert_eq!(settings.get("theme").as_deref(), Some(&"dark".to_string()));
        assert_eq!(
            candles.get("BTCUSDT").await.expect("read candles"),
            Some(series.clone())
        );

        // The compressed locker really did compress: a chunked run of one
        // repeated byte cannot survive LZ4 at anything near its own size.
        let stored: u64 = total_chunk_bytes(backend.as_ref()).await;
        assert!(
            stored < series.len() as u64 / 2,
            "the LZ4 locker stored {stored} bytes for a 40 kB run of one byte; \
             its chain cannot have been applied"
        );

        // And both survive a reopen under the same chains.
        settings.close().await.expect("close settings");
        candles.close().await.expect("close candles");
        let settings = bank
            .locker_with::<String>("settings", LockerConfig::default().with_chain(raw_chain()))
            .await
            .expect("reopen settings");
        let candles = bank
            .lazy_locker_with::<Vec<u8>>("candles", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .expect("reopen candles");
        assert_eq!(settings.get("theme").as_deref(), Some(&"dark".to_string()));
        assert_eq!(
            candles.get("BTCUSDT").await.expect("read candles"),
            Some(series)
        );
    });
}

/// Reopening under a different chain is refused, loudly.
#[test]
fn a_locker_reopened_under_another_chain_is_refused() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");

        let locker = bank
            .lazy_locker_with::<String>("notes", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .expect("open");
        locker.put("k", &"v".to_string()).await.expect("put");
        locker.close().await.expect("close");

        let err = bank
            .lazy_locker_with::<String>("notes", LockerConfig::default().with_chain(raw_chain()))
            .await
            .expect_err("a different chain must be refused");
        match err {
            Error::SchemaMismatch { stored, requested } => {
                assert!(
                    stored.contains("filter chain 7"),
                    "the message must name the chain the data was written with, got {stored:?}"
                );
                assert!(
                    requested.contains("chain 0"),
                    "the message must name the chain that was asked for, got {requested:?}"
                );
            }
            other => panic!("expected a schema mismatch, got {other:?}"),
        }

        // The bank chain is just as much a different chain as any other.
        bank.lazy_locker::<String>("notes")
            .await
            .expect_err("the bank chain is not this locker's chain either");
    });
}

/// A bank written before the `chain::` record existed has none. That is not a
/// mismatch — those values were sealed with the bank chain, which is what an
/// open with no per-locker chain still uses — so the id is written on this
/// open and enforced from the next one.
///
/// The fixture strips the record rather than pinning an old build, which is
/// the only way to spell "written by yesterday's crossbank" in a test.
#[test]
fn a_store_written_before_chains_were_recorded_still_opens() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        {
            let bank = Bank::with_backend(backend.clone()).await.expect("bank");
            let locker = bank.lazy_locker::<String>("legacy").await.expect("open");
            locker.put("k", &"v".to_string()).await.expect("put");
            locker.close().await.expect("close");
            // Dropped, not closed: `Bank::close` closes the backend too, and
            // this fixture goes on to edit the same store underneath.
        }

        // Every `chain::` record goes, leaving exactly what an older build
        // wrote: records, a locker id, a schema tag, and nothing else.
        let stripped = strip_chain_records(backend.as_ref()).await;
        assert!(stripped > 0, "the fixture must actually remove something");

        let bank = Bank::with_backend(backend.clone()).await.expect("reopen");
        let locker = bank
            .lazy_locker::<String>("legacy")
            .await
            .expect("a store with no chain record must still open");
        assert_eq!(
            locker.get("k").await.expect("read"),
            Some("v".to_string()),
            "and its values must still decode"
        );
        locker.close().await.expect("close");

        // The record is written on that open, so the guard is live from now
        // on: a different chain is refused where a moment ago it would have
        // been accepted.
        bank.lazy_locker_with::<String>("legacy", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .expect_err("the id recorded on the legacy open must now be enforced");
    });
}

// ---- fixture helpers ---------------------------------------------------

async fn scan_meta(backend: &dyn Backend) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let page = backend
        .scan(crossbank::backend::ScanRequest {
            table: Table::Meta,
            range: crossbank::backend::KeyRange::all(),
            reverse: false,
            limit: 1024,
            want_values: true,
        })
        .await
        .expect("scan meta");
    page.items
}

async fn strip_chain_records(backend: &dyn Backend) -> usize {
    let ops: Vec<Op> = scan_meta(backend)
        .await
        .into_iter()
        .filter(|(key, _)| key.starts_with(b"chain::"))
        .map(|(key, _)| Op::Delete {
            table: Table::Meta,
            key,
        })
        .collect();
    let n = ops.len();
    backend.commit(ops).await.expect("strip");
    n
}

async fn total_chunk_bytes(backend: &dyn Backend) -> u64 {
    let page = backend
        .scan(crossbank::backend::ScanRequest {
            table: Table::Chunks,
            range: crossbank::backend::KeyRange::all(),
            reverse: false,
            limit: 1024,
            want_values: true,
        })
        .await
        .expect("scan chunks");
    page.items
        .iter()
        .filter_map(|(_, v)| v.as_ref())
        .map(|v| v.len() as u64)
        .sum()
}
