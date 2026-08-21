//! `Commit::Deferred` under stress: the batch versus the byte budget, the
//! batch versus a failing backend, and the batch versus a second handle.
//!
//! Native only. Every behaviour here is backend-independent — one of them
//! needs a backend decorated to fail on demand, which no browser adds
//! anything to. Gated rather than left to run zero tests in a wasm lane,
//! which is how a suite sits green having never run.

#![cfg(not(target_arch = "wasm32"))]

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbank::backend::{
    BFut, Backend, KeyRange, MemoryBackend, Op, ScanPage, ScanRequest, Table, Usage,
};
use crossbank::{Bank, BankConfig, Commit, Error, LockerConfig, Policy};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

// ---- a backend that can be told to refuse a commit ---------------------

/// A memory backend whose commits fail while `broken` is set.
///
/// The same decorator pattern as `tests/value_ids.rs`, for the other kind of
/// question: not "what happens when two futures interleave" but "what happens
/// when the write genuinely does not land".
#[derive(Debug)]
struct Brittle {
    inner: Arc<MemoryBackend>,
    broken: AtomicBool,
}

impl Brittle {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryBackend::new()),
            broken: AtomicBool::new(false),
        })
    }

    fn break_commits(&self, broken: bool) {
        self.broken.store(broken, Ordering::Release);
    }
}

impl Backend for Brittle {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        self.inner.get(table, key)
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        self.inner.get_many(table, keys)
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        self.inner.scan(request)
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        Box::pin(async move {
            if self.broken.load(Ordering::Acquire) {
                return Err(Error::backend("the disk said no"));
            }
            self.inner.commit(ops).await
        })
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        self.inner.usage()
    }

    fn flush(&self) -> BFut<'_, ()> {
        self.inner.flush()
    }

    fn close(&self) -> BFut<'_, ()> {
        self.inner.close()
    }
}

// ---- F2b: no commit may orphan the chunks it just wrote ----------------

/// Every chunk in storage must be reachable from a live record.
///
/// Walks `records`, collects the value ids the chunk pointers name, then walks
/// `chunks` and asserts nothing else is there.
async fn assert_no_orphan_chunks(backend: &dyn Backend) {
    let mut live: Vec<u64> = Vec::new();
    let mut range = KeyRange::all();
    loop {
        let page = backend
            .scan(ScanRequest {
                table: Table::Records,
                range: range.clone(),
                reverse: false,
                limit: 256,
                want_values: true,
            })
            .await
            .expect("scan records");
        for (_, value) in &page.items {
            let Some(raw) = value else { continue };
            // A chunk pointer is `CCHK`, a version, flags, then the value id
            // big-endian. See `locker::chunk::ChunkPointer`.
            if raw.len() >= 14 && &raw[..4] == b"CCHK" {
                let id = u64::from_be_bytes(raw[6..14].try_into().expect("8 bytes"));
                live.push(id);
            }
        }
        match page.resume {
            Some(last) => range.start = Bound::Excluded(last),
            None => break,
        }
    }

    let mut range = KeyRange::all();
    let mut orphans: Vec<u64> = Vec::new();
    loop {
        let page = backend
            .scan(ScanRequest {
                table: Table::Chunks,
                range: range.clone(),
                reverse: false,
                limit: 256,
                want_values: false,
            })
            .await
            .expect("scan chunks");
        for (key, _) in &page.items {
            if key.len() < 8 {
                continue;
            }
            let id = u64::from_be_bytes(key[..8].try_into().expect("8 bytes"));
            if !live.contains(&id) && !orphans.contains(&id) {
                orphans.push(id);
            }
        }
        match page.resume {
            Some(last) => range.start = Bound::Excluded(last),
            None => break,
        }
    }

    assert!(
        orphans.is_empty(),
        "chunks with no live record pointing at them: {orphans:?}"
    );
}

/// A commit that both writes a key and evicts to stay in budget must not
/// leave the chunks of what it wrote behind.
///
/// This is the second half of the self-eviction bug. When the victim was also
/// an update in the same commit, the eviction ran `delete_value_ops` against
/// the *old* pointer — GC-ing the old chunks — while the update wrote fresh
/// ones. The record then went away and the new chunks were unreachable
/// forever, growing the database with nothing to reclaim them.
#[test]
fn a_flush_that_evicts_leaves_no_orphan_chunks() {
    block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");
        let config = LockerConfig::default()
            .with_policy(Policy::Evictable { max_bytes: 8_000 })
            .with_commit(Commit::Deferred { after: 64 })
            // Small enough that every value written here is chunked.
            .with_chunk_size(256);

        let locker = bank
            .lazy_locker_with::<Vec<u8>>("chunky", config)
            .await
            .expect("open");

        for round in 0..4u8 {
            for key in ["a", "b", "c", "d", "e"] {
                locker
                    .put(key, &vec![round; 2_000])
                    .await
                    .expect("staged put");
            }
            locker.flush().await.expect("flush");
            assert_no_orphan_chunks(backend.as_ref()).await;
        }

        // Whatever survived must still read back intact — an orphan check
        // passes trivially if the data is simply gone.
        for key in locker.keys() {
            let value = locker.get(&key).await.expect("read").expect("present");
            assert_eq!(value.len(), 2_000, "{key} came back the wrong size");
        }
        // Five 2 KiB values is 10 KiB against an 8 KiB budget, and every one
        // of them is a key the same commit wrote. The documented answer is to
        // shed everything else and keep the batch, rather than refuse to
        // store what we were just asked to store — so the budget is over, and
        // all five keys are here.
        assert_eq!(locker.len(), 5);
        assert_eq!(
            locker.budget_used(),
            10_010,
            "five postcard-framed 2 KiB values"
        );
    });
}

// ---- F7: a failed flush must not destroy the batch ---------------------

/// `close` must keep a batch its own flush could not land.
///
/// It called `discard_staged()` unconditionally, so a flush that failed — a
/// full disk, a quota refusal — was followed immediately by throwing away the
/// only copy of the data. The caller got an error it could do nothing about.
#[test]
fn a_close_whose_flush_fails_keeps_the_batch_for_a_retry() {
    block_on(async {
        let backend = Brittle::new();
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");
        let locker = bank
            .lazy_locker_with::<String>(
                "brittle",
                LockerConfig::default().with_commit(Commit::Deferred { after: 16 }),
            )
            .await
            .expect("open");

        locker.put("a", &"alpha".to_string()).await.expect("stage");
        locker.put("b", &"beta".to_string()).await.expect("stage");
        assert_eq!(locker.pending(), 2);

        backend.break_commits(true);
        let closed = locker.close().await;
        assert!(
            matches!(closed, Err(Error::Backend(_))),
            "the flush failure must be reported: {closed:?}"
        );
        assert!(locker.is_closed(), "close still closes");
        assert_eq!(
            locker.pending(),
            2,
            "the batch is the only copy of the data and must survive"
        );

        // Writes are refused, but the retry is not.
        assert!(matches!(
            locker.put("c", &"gamma".to_string()).await,
            Err(Error::Closed)
        ));

        backend.break_commits(false);
        locker.flush().await.expect("the retry must land");
        assert_eq!(locker.pending(), 0);

        // And the data really is stored, read through a fresh handle. The
        // closed handle no longer counts as live, so the name is free.
        let reader = bank.lazy_locker::<String>("brittle").await.expect("reopen");
        assert_eq!(
            reader.get("a").await.expect("read"),
            Some("alpha".to_string())
        );
        assert_eq!(
            reader.get("b").await.expect("read"),
            Some("beta".to_string())
        );
    });
}

/// The same contract on an eager locker.
#[test]
fn an_eager_close_whose_flush_fails_keeps_the_batch_too() {
    block_on(async {
        let backend = Brittle::new();
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");
        let locker = bank
            .locker_with::<String>(
                "brittle_eager",
                LockerConfig::default().with_commit(Commit::Deferred { after: 16 }),
            )
            .await
            .expect("open");

        locker.put("k", "value".to_string()).await.expect("stage");
        backend.break_commits(true);
        assert!(locker.close().await.is_err());
        assert_eq!(locker.pending(), 1);

        backend.break_commits(false);
        locker.flush().await.expect("retry");
        assert_eq!(locker.pending(), 0);
    });
}

/// A failed transaction must put the deferred batch it absorbed back.
///
/// The transaction drains the staged batch so both can ride in one commit; if
/// that commit does not land, the batch has to be exactly where it was.
#[test]
fn a_failed_transaction_restages_the_batch_it_absorbed() {
    block_on(async {
        let backend = Brittle::new();
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");
        let locker = bank
            .lazy_locker_with::<String>(
                "restage",
                LockerConfig::default().with_commit(Commit::Deferred { after: 16 }),
            )
            .await
            .expect("open");

        locker.put("a", &"alpha".to_string()).await.expect("stage");
        assert_eq!(locker.pending(), 1);

        backend.break_commits(true);
        let result = locker
            .transact(|tx| async move {
                tx.put("b", "beta".to_string())?;
                Ok(())
            })
            .await;
        assert!(result.is_err(), "the commit failed, so the transaction did");
        assert_eq!(
            locker.pending(),
            1,
            "the absorbed batch must be back where it was"
        );
        assert_eq!(
            locker.get("a").await.expect("read"),
            Some("alpha".to_string()),
            "and still readable through its own handle"
        );

        backend.break_commits(false);
        locker.flush().await.expect("flush");
        locker.close().await.expect("close");
        let reader = bank.lazy_locker::<String>("restage").await.expect("reopen");
        assert_eq!(
            reader.get("a").await.expect("read"),
            Some("alpha".to_string())
        );
    });
}

// ---- F13: one deferred handle per name ---------------------------------

/// Two handles on one name under `Commit::Deferred` each keep their own
/// staging buffer, so whichever flushed last silently overwrote the other.
/// The second open is refused instead.
#[test]
fn a_second_deferred_handle_on_one_name_is_refused() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let deferred = LockerConfig::default().with_commit(Commit::Deferred { after: 8 });

        let first = bank
            .lazy_locker_with::<String>("only_one", deferred)
            .await
            .expect("first open");

        let second = bank.lazy_locker_with::<String>("only_one", deferred).await;
        assert!(
            matches!(second, Err(Error::InvalidConfig(_))),
            "a second deferred handle must be refused: {second:?}"
        );

        // Even an immediate handle is refused while a deferred one is live:
        // it would not see the staged batch either.
        let immediate = bank.lazy_locker::<String>("only_one").await;
        assert!(matches!(immediate, Err(Error::InvalidConfig(_))));

        // Closing the first frees the name.
        first.close().await.expect("close");
        bank.lazy_locker::<String>("only_one")
            .await
            .expect("the name is free once the deferred handle is closed");
    });
}

/// Two immediate handles stay allowed — they hold nothing back, so the
/// backend already serialises every write.
#[test]
fn two_immediate_handles_on_one_name_are_still_allowed() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let one = bank.lazy_locker::<String>("shared").await.expect("first");
        let two = bank.lazy_locker::<String>("shared").await.expect("second");
        one.put("k", &"v".to_string()).await.expect("put");
        assert_eq!(two.get("k").await.expect("read"), Some("v".to_string()));
    });
}

/// A degenerate `Deferred` is `Immediate` with extra steps, and must not
/// inherit the one-handle restriction.
#[test]
fn a_batch_of_one_does_not_count_as_deferral_for_the_handle_rule() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let config = LockerConfig::default().with_commit(Commit::Deferred { after: 1 });
        let _one = bank
            .lazy_locker_with::<String>("degenerate", config)
            .await
            .expect("first");
        bank.lazy_locker_with::<String>("degenerate", config)
            .await
            .expect("a batch of one stages nothing, so a second handle is fine");
    });
}
