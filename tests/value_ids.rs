//! Chunk value ids must be unique bank-wide, even when two lockers allocate
//! at the same time.
//!
//! The conformance suite covers the sequential case on every backend. This
//! file covers the interleaved one, which needs a backend that actually
//! suspends: the memory and `redb` backends complete every future on the first
//! poll, so `join`ing two puts over them never interleaves and never
//! reproduces the collision.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crossbank::backend::{
    BFut, Backend, MemoryBackend, Op, ScanPage, ScanRequest, Table, Usage,
};
use crossbank::{Bank, LockerConfig, Result};

/// Suspends exactly once, so two futures joined over it take turns.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// A memory backend that suspends on the way into a commit.
///
/// That is the exact window the old allocator left open: it read the stored
/// counter and bumped it in a *later* commit, so a second writer suspending
/// there read the same number and wrote its chunks under the same id.
#[derive(Debug)]
struct SlowCommit(Arc<MemoryBackend>);

impl Backend for SlowCommit {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        self.0.get(table, key)
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        self.0.get_many(table, keys)
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        self.0.scan(request)
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        Box::pin(async move {
            YieldOnce(false).await;
            self.0.commit(ops).await
        })
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        self.0.usage()
    }

    fn flush(&self) -> BFut<'_, ()> {
        self.0.flush()
    }

    fn close(&self) -> BFut<'_, ()> {
        self.0.close()
    }
}

fn tiny() -> LockerConfig {
    LockerConfig::default().with_chunk_size(32)
}

async fn chunk_rows(bank: &Bank) -> Result<usize> {
    let page = bank
        .backend()
        .scan(ScanRequest {
            table: Table::Chunks,
            range: crossbank::backend::KeyRange::all(),
            reverse: false,
            limit: 4096,
            want_values: false,
        })
        .await?;
    Ok(page.items.len())
}

#[test]
fn two_lockers_allocating_at_once_get_distinct_value_ids() {
    let backend: Arc<dyn Backend> = Arc::new(SlowCommit(Arc::new(MemoryBackend::new())));

    futures::executor::block_on(async move {
        let bank = Bank::with_backend(backend).await.unwrap();
        let first = bank
            .lazy_locker_with::<String>("first", tiny())
            .await
            .unwrap();
        let second = bank
            .lazy_locker_with::<String>("second", tiny())
            .await
            .unwrap();

        let a = "a".repeat(200);
        let b = "b".repeat(200);
        let (l, r) = futures::future::join(first.put("k", &a), second.put("k", &b)).await;
        l.unwrap();
        r.unwrap();

        assert_eq!(first.get("k").await.unwrap(), Some(a.clone()));
        assert_eq!(second.get("k").await.unwrap(), Some(b.clone()));

        let per_value = chunk_rows(&bank).await.unwrap() / 2;
        assert!(per_value > 1, "the setup must have chunked");

        // Two handles on ONE name share stored data but are separate objects,
        // which is the same hazard.
        let one = bank
            .lazy_locker_with::<String>("shared", tiny())
            .await
            .unwrap();
        let two = bank
            .lazy_locker_with::<String>("shared", tiny())
            .await
            .unwrap();
        let (l, r) = futures::future::join(one.put("x", &a), two.put("y", &b)).await;
        l.unwrap();
        r.unwrap();
        assert_eq!(one.get("x").await.unwrap(), Some(a));
        assert_eq!(one.get("y").await.unwrap(), Some(b));

        assert_eq!(
            chunk_rows(&bank).await.unwrap(),
            per_value * 4,
            "four chunked values must own four disjoint sets of chunks"
        );
    });
}
