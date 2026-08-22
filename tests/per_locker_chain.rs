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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbank::backend::{Backend, MemoryBackend, Op, Table};
use crossbank::codec::{FilterChain, Lz4};
use crossbank::{Bank, Error, LockerConfig};

/// Records the largest single allocation the process has made since the last
/// reset, so a test can assert that nothing reserved a value's *claimed* size
/// before reading it. A peak counter would be perturbed by anything else
/// running; the largest single request is not, because nothing else in this
/// suite asks for anywhere near a megabyte at once.
struct Watched;

static LARGEST: AtomicUsize = AtomicUsize::new(0);

unsafe impl std::alloc::GlobalAlloc for Watched {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new: usize) -> *mut u8 {
        LARGEST.fetch_max(new, Ordering::Relaxed);
        unsafe { std::alloc::System.realloc(ptr, layout, new) }
    }
}

#[global_allocator]
static ALLOCATOR: Watched = Watched;

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

// ---- bank maintenance under a per-locker chain -------------------------

/// Bank maintenance reads records with no locker handle, so it has no config
/// to take a chain from. If it reaches for the *bank* chain, every record of
/// an LZ4 locker fails to open and `verify` reports the whole locker as
/// corrupt — and `verify`'s own doc example feeds that list straight to
/// `quarantine`, which deletes every key in it. A survey that erases the
/// thing it surveys is the worst failure this crate has.
#[test]
fn verify_reads_a_per_locker_chain_through_that_chain() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");

        let candles = bank
            .lazy_locker_with::<Vec<u8>>(
                "candles",
                LockerConfig::default()
                    .with_chain(lz4_chain())
                    .with_chunk_size(4096),
            )
            .await
            .expect("open");
        for n in 0..50u8 {
            candles
                .put(&format!("k{n}"), &vec![n; 64])
                .await
                .expect("put");
        }
        // One value big enough to be chunked, so the chunk read path is
        // covered too and not just the inline one.
        candles
            .put("chunked", &vec![9u8; 40_000])
            .await
            .expect("put chunked");
        candles.close().await.expect("close");

        assert!(
            bank.verify("candles").await.expect("verify").is_empty(),
            "a healthy LZ4 locker must verify clean; a non-empty list here is \
             the bank chain being used to open records it never sealed"
        );
        assert_eq!(
            bank.quarantine("candles", &[]).await.expect("quarantine"),
            0,
            "quarantining nothing must remove nothing"
        );

        // And the data really is all still there.
        let reopened = bank
            .lazy_locker_with::<Vec<u8>>("candles", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .expect("reopen");
        assert_eq!(reopened.len(), 51, "verify must not have cost any keys");
        assert_eq!(
            reopened.get("chunked").await.expect("read"),
            Some(vec![9u8; 40_000])
        );
        reopened.close().await.expect("close");

        // Now break exactly one record. `verify` must name it and nothing
        // else — the negative control for the assertion above, which would
        // pass just as happily if `verify` always returned an empty list.
        backend
            .commit(vec![Op::Put {
                table: Table::Records,
                key: crossbank::key::encode(locker_id(&bank, "candles").await, "k7"),
                value: b"not an envelope".to_vec(),
            }])
            .await
            .expect("corrupt one record");

        let bad = bank.verify("candles").await.expect("verify");
        assert_eq!(
            bad,
            vec![b"k7".to_vec()],
            "verify must report exactly the record that was broken"
        );
    });
}

/// A locker whose recorded chain id names nothing this process knows must not
/// be surveyed at all. Answering "every key is bad" would be a lie that
/// `quarantine` acts on; the honest answer is that the locker cannot be read
/// from here. Deleting it is still allowed — that needs no chain.
#[test]
fn verify_refuses_a_locker_whose_chain_is_unknown() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        {
            let bank = Bank::with_backend(backend.clone()).await.expect("bank");
            let locker = bank
                .lazy_locker_with::<String>(
                    "notes",
                    LockerConfig::default().with_chain(lz4_chain()),
                )
                .await
                .expect("open");
            locker.put("k", &"v".to_string()).await.expect("put");
            locker.close().await.expect("close");
        }

        // A fresh bank that has never been told about chain 7.
        let bank = Bank::with_backend(backend.clone()).await.expect("reopen");
        let err = bank
            .verify("notes")
            .await
            .expect_err("an unknown chain cannot be verified");
        match err {
            Error::SchemaMismatch { stored, requested } => {
                assert!(
                    stored.contains("filter chain 7"),
                    "the message must name the chain the locker was written with, \
                     got {stored:?}"
                );
                assert!(
                    requested.contains("register_chain"),
                    "and must say how to fix it, got {requested:?}"
                );
            }
            other => panic!("expected a schema mismatch, got {other:?}"),
        }

        // Told about the chain, the same bank verifies it clean.
        bank.register_chain(lz4_chain());
        assert!(bank.verify("notes").await.expect("verify").is_empty());

        // And an unreadable locker is still deletable.
        assert!(bank.delete_locker("notes").await.expect("delete"));
    });
}

/// A locker written by a build that had no per-locker chains has no `chain::`
/// record, and its bytes were sealed with the bank chain. Adopting whatever
/// chain the caller asked for would stamp that lie into `meta` and brick the
/// locker permanently.
#[test]
fn a_pre_chain_locker_cannot_be_adopted_by_a_new_chain() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        {
            let bank = Bank::with_backend(backend.clone()).await.expect("bank");
            let locker = bank.lazy_locker::<String>("legacy").await.expect("open");
            locker.put("k", &"v".to_string()).await.expect("put");
            locker.close().await.expect("close");
        }
        assert!(strip_chain_records(backend.as_ref()).await > 0);

        let bank = Bank::with_backend(backend.clone()).await.expect("reopen");
        let err = bank
            .lazy_locker_with::<String>("legacy", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .expect_err("a pre-chain locker must not adopt a new chain");
        match err {
            Error::SchemaMismatch { stored, requested } => {
                assert!(
                    stored.contains("bank chain"),
                    "the message must explain what the bytes were sealed with, \
                     got {stored:?}"
                );
                assert!(
                    requested.contains("chain 7"),
                    "and must name what was asked for, got {requested:?}"
                );
            }
            other => panic!("expected a schema mismatch, got {other:?}"),
        }

        // Nothing was written, so the refusal is repeatable rather than a
        // one-shot that corrupts on the second try.
        assert!(bank
            .lazy_locker_with::<String>("legacy", LockerConfig::default().with_chain(lz4_chain()))
            .await
            .is_err());

        // The bank chain still opens it, and *that* open records the id.
        let locker = bank
            .lazy_locker::<String>("legacy")
            .await
            .expect("the bank chain is what these bytes were sealed with");
        assert_eq!(locker.get("k").await.expect("read"), Some("v".to_string()));
        locker.close().await.expect("close");
        assert!(
            scan_meta(backend.as_ref())
                .await
                .iter()
                .any(|(key, _)| key.starts_with(b"chain::")),
            "the bank chain id must now be recorded"
        );
    });
}

/// A chunk pointer is stored data, so its `total_len` is an attacker's — or a
/// corrupted disk's — number. Reserving from it up front asks for up to 256
/// MiB, and a wasm release build is `panic=abort`, so a failed allocation is
/// an unrecoverable app kill rather than an error anyone can catch.
#[test]
fn a_pointer_claiming_far_more_than_it_stores_fails_without_reserving_it() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");
        let locker = bank.lazy_locker::<Vec<u8>>("blobs").await.expect("open");
        locker.put("k", &vec![1u8; 8]).await.expect("put");
        locker.close().await.expect("close");
        let id = locker_id(&bank, "blobs").await;

        // 200 MiB claimed, one chunk actually present.
        let value_id = 99u64;
        let pointer = crossbank::locker::chunk::ChunkPointer {
            value_id,
            n_chunks: 1,
            total_len: 200 * 1024 * 1024,
            flags: crossbank::locker::chunk::FLAG_POSTCARD,
        };
        backend
            .commit(vec![
                Op::Put {
                    table: Table::Chunks,
                    key: crossbank::locker::chunk::chunk_key(value_id, 0),
                    value: bank.chain().seal(vec![1u8; 8]).expect("seal"),
                },
                Op::Put {
                    table: Table::Records,
                    key: crossbank::key::encode(id, "k"),
                    value: pointer.encode(),
                },
            ])
            .await
            .expect("plant the pointer");

        let locker = bank.lazy_locker::<Vec<u8>>("blobs").await.expect("reopen");
        LARGEST.store(0, Ordering::Relaxed);
        let err = locker
            .get("k")
            .await
            .expect_err("a pointer that over-claims must be corrupt, not fatal");
        assert!(
            matches!(err, Error::Corrupt(_)),
            "expected a corruption error, got {err:?}"
        );
        // The negative control for the fix: an up-front
        // `Vec::with_capacity(total_len)` makes this 200 MiB in one request.
        let largest = LARGEST.load(Ordering::Relaxed);
        assert!(
            largest < 1024 * 1024,
            "reading an over-claiming pointer asked for {largest} bytes in one \
             allocation; nothing may be reserved from a number read off storage"
        );
    });
}

async fn locker_id(bank: &Bank, name: &str) -> u32 {
    bank.locker_id(name).await.expect("locker id")
}
