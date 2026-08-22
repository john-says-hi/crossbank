//! Every handle on one locker name is a view of the one open locker.
//!
//! The conformance suite pins the behaviour that matters to a caller — a write
//! through one handle is visible through every other, on every backend. This
//! file pins the edges around it: what happens when the second open does not
//! agree with the first, and what `close` means when more than one handle
//! holds the name.
//!
//! Native only. Nothing here is backend-dependent, and the memory backend
//! answers all of it.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use crossbank::backend::{
    BFut, Backend, CommitOptions, KeyRange, Op, ScanPage, ScanRequest, Table, Usage,
};
use crossbank::{Bank, BankConfig, Commit, Error, Locker, LockerConfig, MemoryBackend};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

/// A second open under a different value type is refused.
///
/// The two handles share one resident map, and the downcast back out of it is
/// only sound because the type tag says it is. A mismatch is the same answer
/// the stored schema guard gives, for the same reason.
#[test]
fn a_second_handle_under_a_different_type_is_refused() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _first: Locker<String> = bank.locker("settings").await.expect("first");

        let second = bank.locker::<u64>("settings").await;
        assert!(
            matches!(second, Err(Error::SchemaMismatch { .. })),
            "a second handle under another type must be refused: {second:?}"
        );
    });
}

/// So is opening an open eager name lazily, or the other way round.
#[test]
fn a_second_handle_of_the_other_kind_is_refused() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _eager: Locker<String> = bank.locker("settings").await.expect("eager");
        assert!(matches!(
            bank.lazy_locker::<String>("settings").await,
            Err(Error::SchemaMismatch { .. })
        ));

        let _lazy = bank
            .lazy_locker::<String>("series")
            .await
            .expect("lazy open");
        assert!(matches!(
            bank.locker::<String>("series").await,
            Err(Error::SchemaMismatch { .. })
        ));
    });
}

/// A second open under a different config is refused, and says which field.
///
/// Sharing means one set of rules governs both handles' writes. Letting the
/// second open name a chunk size, a commit mode or a durability the first
/// handle never asked for would apply it to the first handle's writes too,
/// silently.
#[test]
fn a_second_handle_under_a_different_config_names_the_field() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _first = bank
            .lazy_locker_with::<String>("series", LockerConfig::default())
            .await
            .expect("first");

        let second = bank
            .lazy_locker_with::<String>("series", LockerConfig::default().with_chunk_size(4096))
            .await;
        match second {
            Err(Error::InvalidConfig(message)) => assert!(
                message.contains("chunk_size"),
                "the error must name the differing field: {message}"
            ),
            other => panic!("a differing config must be refused: {other:?}"),
        }

        let commit = bank
            .lazy_locker_with::<String>(
                "series",
                LockerConfig::default().with_commit(Commit::Deferred { after: 8 }),
            )
            .await;
        match commit {
            Err(Error::InvalidConfig(message)) => assert!(message.contains("commit")),
            other => panic!("a differing commit mode must be refused: {other:?}"),
        }

        // The same config shares, which is the whole point.
        bank.lazy_locker_with::<String>("series", LockerConfig::default())
            .await
            .expect("an identical config shares the open locker");
    });
}

/// `close()` on one handle closes the locker for every handle on the name.
///
/// Hive's semantics: `box.close()` closes the box, not the caller's reference
/// to it. The alternative — a handle that keeps working after the locker it
/// shares was closed — would mean `close` did not mean what it says.
#[test]
fn closing_one_handle_closes_the_name_for_all_of_them() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.lazy_locker::<String>("l").await.expect("first");
        let b = bank.lazy_locker::<String>("l").await.expect("second");
        a.put("k", &"v".to_string()).await.expect("put");

        b.close().await.expect("close");

        assert!(a.is_closed(), "the other handle must report closed too");
        assert!(matches!(
            a.put("k2", &"v".to_string()).await,
            Err(Error::Closed)
        ));
        assert!(!bank.is_locker_open("l"));

        // Idempotent, through either handle.
        a.close().await.expect("closing twice is fine");
        b.close().await.expect("and through the other handle too");
    });
}

/// A closed name reopens, and the data is still there.
#[test]
fn a_name_reopens_after_it_was_closed_through_a_handle() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.locker::<String>("settings").await.expect("first");
        let b = bank.locker::<String>("settings").await.expect("second");
        a.put("theme", "dark".to_string()).await.expect("put");
        b.close().await.expect("close");

        let fresh = bank.locker::<String>("settings").await.expect("reopen");
        assert!(!fresh.is_closed());
        assert_eq!(fresh.get("theme").as_deref(), Some(&"dark".to_string()));

        // ...and it is a fresh locker, not the closed one handed back.
        assert!(a.is_closed());
    });
}

/// A dropped handle does not close the locker the others are still using.
#[test]
fn dropping_one_handle_leaves_the_others_working() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.locker::<String>("settings").await.expect("first");
        let b = bank.locker::<String>("settings").await.expect("second");
        b.put("theme", "dark".to_string()).await.expect("put");
        drop(b);

        assert!(bank.is_locker_open("settings"));
        assert_eq!(a.get("theme").as_deref(), Some(&"dark".to_string()));
        a.put("accent", "blue".to_string()).await.expect("put");
    });
}

/// Once every handle is gone the name is free, and a later open reads storage
/// afresh rather than finding a stale resident map.
#[test]
fn the_last_handle_going_frees_the_name() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        {
            let a = bank.locker::<String>("settings").await.expect("first");
            let b = bank.locker::<String>("settings").await.expect("second");
            a.put("theme", "dark".to_string()).await.expect("put");
            drop(a);
            drop(b);
        }
        assert!(!bank.is_locker_open("settings"));

        let fresh = bank.locker::<String>("settings").await.expect("reopen");
        assert_eq!(fresh.get("theme").as_deref(), Some(&"dark".to_string()));
    });
}

// ---- two opens of one name, racing each other across an await ----------

/// A backend decorator whose `scan` suspends exactly once per call.
///
/// The memory backend never suspends — every future it returns is ready on
/// its first poll — so two opens driven by one `join!` would run strictly one
/// after the other and the race could not be reached. One yield inside the
/// scan that builds a lazy locker's key index is enough to park both opens
/// past the registry check and before either registers, which is the window
/// the bug lived in. Nothing here locks, so nothing is held across an await.
struct YieldingScan {
    inner: Arc<dyn Backend>,
}

struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

impl Backend for YieldingScan {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        self.inner.get(table, key)
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        self.inner.get_many(table, keys)
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        Box::pin(async move {
            YieldOnce(false).await;
            self.inner.scan(request).await
        })
    }

    fn scan_page_size(&self) -> usize {
        self.inner.scan_page_size()
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        self.inner.commit(ops)
    }

    fn commit_with(&self, ops: Vec<Op>, options: CommitOptions) -> BFut<'_, ()> {
        self.inner.commit_with(ops, options)
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

fn yielding() -> Arc<dyn Backend> {
    Arc::new(YieldingScan {
        inner: Arc::new(MemoryBackend::new()),
    })
}

/// Every chunk row currently stored, whichever value they belong to.
async fn chunk_rows(backend: &Arc<dyn Backend>) -> usize {
    backend
        .scan(ScanRequest {
            table: Table::Chunks,
            range: KeyRange::all(),
            reverse: false,
            limit: 4096,
            want_values: false,
        })
        .await
        .expect("scan chunks")
        .items
        .len()
}

/// A small chunked config, so a few hundred bytes really do become chunks.
fn chunky() -> LockerConfig {
    LockerConfig::default()
        .with_max_inline(64)
        .with_chunk_size(32)
}

/// Two opens of one name that overlap in time still end up as one locker.
///
/// The registry check happens before `prepare` and `open` are awaited, so two
/// callers could both pass it, build an `Inner`, a `Resident` and an index
/// each, and have the second registration overwrite the first. The first
/// locker then existed and was invisible: not `is_locker_open`, not
/// `open_locker_names`, not the next `locker(name)`.
#[test]
fn two_simultaneous_opens_of_one_name_share_one_locker() {
    block_on(async {
        let backend = yielding();
        let bank = Bank::with_backend(backend).await.expect("bank");

        let (first, second) = futures::join!(
            bank.lazy_locker_with::<Vec<u8>>("series", chunky()),
            bank.lazy_locker_with::<Vec<u8>>("series", chunky()),
        );
        let first = first.expect("first open");
        let second = second.expect("second open");

        assert_eq!(
            bank.open_locker_names(),
            vec!["series".to_string()],
            "one name is one open locker, however the opens overlapped"
        );

        first.put("k", &vec![1u8, 2, 3]).await.expect("put");
        assert!(
            second.contains_key("k"),
            "a write through one handle must be visible through the other"
        );
        assert!(
            first.contains_key("k"),
            "and through the handle that wrote it"
        );
    });
}

/// And the chunks one of them wrote are not orphaned by the other's write.
///
/// This is trap 27's exact failure. Two independent indexes on one name each
/// believed a key absent that the other had chunk-written, so the overwrite
/// skipped the read that finds the old chunks to collect, and the pieces of
/// the replaced value stayed in `chunks` with nothing pointing at them —
/// forever, since only a record's own overwrite or delete ever collects them.
#[test]
fn racing_opens_do_not_orphan_each_others_chunks() {
    block_on(async {
        let backend = yielding();
        let bank = Bank::with_backend(backend.clone()).await.expect("bank");

        let (first, second) = futures::join!(
            bank.lazy_locker_with::<Vec<u8>>("series", chunky()),
            bank.lazy_locker_with::<Vec<u8>>("series", chunky()),
        );
        let first = first.expect("first open");
        let second = second.expect("second open");

        // Chunked through one handle...
        first
            .put("candles", &vec![7u8; 512])
            .await
            .expect("chunked put");
        assert!(
            chunk_rows(&backend).await > 0,
            "the fixture must really chunk, or it proves nothing"
        );

        // ...replaced by a small inline value through the other.
        second
            .put("candles", &vec![1u8; 4])
            .await
            .expect("small put");

        assert_eq!(
            chunk_rows(&backend).await,
            0,
            "the replaced value's chunks must be collected: the live value is \
             inline, so nothing points at a chunk any more"
        );
        assert_eq!(
            second.get("candles").await.expect("get"),
            Some(vec![1u8; 4])
        );
    });
}
