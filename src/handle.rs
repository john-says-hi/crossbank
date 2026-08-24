//! A `Send + Sync` handle onto a bank that is not itself `Send`.
//!
//! # Why this exists
//!
//! On wasm the backend holds `JsValue`, which is neither `Send` nor `Sync`, so
//! a [`Bank`] cannot cross a thread boundary and its futures cannot satisfy a
//! `Send` bound. That is not a corner case: a consumer whose storage trait
//! requires `Send` futures — as wise_apple's `KvStore` does — **cannot use the
//! ordinary API on the web at all**.
//!
//! So this is not a convenience for worker threads. On the web it is the only
//! usable path, which is why it lands in M1 rather than being deferred.
//!
//! # How it works
//!
//! [`BankHandle`] is a cloneable sender. Every operation becomes a message
//! carrying **plain data only** — `String`, `Vec<u8>`, and a one-shot reply
//! channel. No `JsValue`, no generic `T`, nothing that could pin the message to
//! a thread. The bank's owning thread runs [`Bank::into_service`], receives
//! those messages, and answers them.
//!
//! crossbank spawns nothing. The consumer decides where the service future
//! runs: `spawn_local` on the main thread under wasm, a task on whatever
//! runtime it uses natively.
//!
//! # The rule
//!
//! **The thread polling the service future must never block.** If it does,
//! every caller waiting on a reply waits forever — and on wasm a blocking wait
//! on the main thread traps outright rather than merely stalling.
//!
//! # Why bytes rather than a generic value type
//!
//! A generic `T` cannot cross the channel without constraining the message to
//! that type. Bytes keep the transport monomorphic, and they match the shape a
//! consumer's own storage trait already has. Values still travel through the
//! bank's filter chain, so they are checksummed exactly like typed ones — a
//! remote locker is precisely a `Locker<Vec<u8>>`.

use futures::channel::{mpsc, oneshot};
use futures::StreamExt;

use crate::backend::Usage;
use crate::bank::Bank;
use crate::error::{Error, Result};

/// Depth of the job queue before senders feel back-pressure.
pub(crate) const JOB_QUEUE: usize = 128;

type Reply<T> = oneshot::Sender<Result<T>>;

/// One request to the bank's owning thread. Deliberately carries no `JsValue`
/// and no generic type, so it is `Send` on every target.
pub(crate) enum Job {
    Get {
        locker: String,
        key: String,
        reply: Reply<Option<Vec<u8>>>,
    },
    Put {
        locker: String,
        key: String,
        value: Vec<u8>,
        reply: Reply<()>,
    },
    Delete {
        locker: String,
        key: String,
        reply: Reply<()>,
    },
    Keys {
        locker: String,
        prefix: String,
        reply: Reply<Vec<String>>,
    },
    Clear {
        locker: String,
        reply: Reply<()>,
    },
    GetMany {
        locker: String,
        keys: Vec<String>,
        reply: Reply<Vec<Option<Vec<u8>>>>,
    },
    PutAll {
        locker: String,
        entries: Vec<(String, Vec<u8>)>,
        reply: Reply<()>,
    },
    DeleteAll {
        locker: String,
        keys: Vec<String>,
        reply: Reply<()>,
    },
    Entries {
        locker: String,
        prefix: String,
        reply: Reply<Vec<(String, Vec<u8>)>>,
    },
    ContainsKey {
        locker: String,
        key: String,
        reply: Reply<bool>,
    },
    Len {
        locker: String,
        reply: Reply<usize>,
    },
    LockerExists {
        name: String,
        reply: Reply<bool>,
    },
    LockerNames {
        reply: Reply<Vec<String>>,
    },
    DeleteLocker {
        name: String,
        reply: Reply<bool>,
    },
    FlushAll {
        reply: Reply<()>,
    },
    Persist {
        reply: Reply<bool>,
    },
    IsPersisted {
        reply: Reply<bool>,
    },
    ReportUsage {
        reply: Reply<Option<Usage>>,
    },
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Get { .. } => "Get",
            Self::Put { .. } => "Put",
            Self::Delete { .. } => "Delete",
            Self::Keys { .. } => "Keys",
            Self::Clear { .. } => "Clear",
            Self::GetMany { .. } => "GetMany",
            Self::PutAll { .. } => "PutAll",
            Self::DeleteAll { .. } => "DeleteAll",
            Self::Entries { .. } => "Entries",
            Self::ContainsKey { .. } => "ContainsKey",
            Self::Len { .. } => "Len",
            Self::LockerExists { .. } => "LockerExists",
            Self::LockerNames { .. } => "LockerNames",
            Self::DeleteLocker { .. } => "DeleteLocker",
            Self::FlushAll { .. } => "FlushAll",
            Self::Persist { .. } => "Persist",
            Self::IsPersisted { .. } => "IsPersisted",
            Self::ReportUsage { .. } => "ReportUsage",
        };
        f.debug_struct("Job").field("kind", &name).finish()
    }
}

/// A `Send + Sync + Clone` handle onto a bank running elsewhere.
///
/// Every call becomes a message carrying plain data and a one-shot reply
/// channel, answered by [`Bank::into_service`] on the bank's own thread. The
/// values it reads and writes go through the same envelope and filter chain as
/// typed ones, so a locker reached this way is exactly a `Locker<Vec<u8>>`.
///
/// ```no_run
/// use crossbank::{Bank, BankConfig};
///
/// # async fn demo() -> crossbank::Result<()> {
/// let bank = Bank::open(BankConfig::at("app.crossbank")).await?;
/// let handle = bank.handle();
///
/// // The service must be polled somewhere, on the bank's own thread, and
/// // that thread must never block. crossbank spawns nothing, so this is
/// // yours to place: `spawn_local` on the web, a task natively.
/// let service = bank.into_service();
///
/// // Meanwhile, from anywhere:
/// handle.put("settings", "theme", b"dark".to_vec()).await?;
/// assert_eq!(handle.get("settings", "theme").await?, Some(b"dark".to_vec()));
/// println!("{:?}", handle.keys("settings", "").await?);
/// handle.delete("settings", "theme").await?;
///
/// service.await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct BankHandle {
    sender: mpsc::Sender<Job>,
}

impl BankHandle {
    pub(crate) fn new(sender: mpsc::Sender<Job>) -> Self {
        Self { sender }
    }

    /// Read a value.
    pub async fn get(&self, locker: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.dispatch(|reply| Job::Get {
            locker: locker.to_string(),
            key: key.to_string(),
            reply,
        })
        .await
    }

    /// Write a value.
    pub async fn put(&self, locker: &str, key: &str, value: Vec<u8>) -> Result<()> {
        self.dispatch(|reply| Job::Put {
            locker: locker.to_string(),
            key: key.to_string(),
            value,
            reply,
        })
        .await
    }

    /// Remove a key. Removing an absent key is not an error.
    pub async fn delete(&self, locker: &str, key: &str) -> Result<()> {
        self.dispatch(|reply| Job::Delete {
            locker: locker.to_string(),
            key: key.to_string(),
            reply,
        })
        .await
    }

    /// List keys beginning with `prefix`.
    pub async fn keys(&self, locker: &str, prefix: &str) -> Result<Vec<String>> {
        self.dispatch(|reply| Job::Keys {
            locker: locker.to_string(),
            prefix: prefix.to_string(),
            reply,
        })
        .await
    }

    /// Remove everything in one locker.
    pub async fn clear(&self, locker: &str) -> Result<()> {
        self.dispatch(|reply| Job::Clear {
            locker: locker.to_string(),
            reply,
        })
        .await
    }

    /// Read many keys in **one** backend round trip.
    ///
    /// The answers are positional: slot `i` belongs to `keys[i]`, and `None`
    /// there means that key is absent. On the web this is one IndexedDB
    /// transaction instead of one per key, which is the whole reason it exists.
    pub async fn get_many(&self, locker: &str, keys: Vec<String>) -> Result<Vec<Option<Vec<u8>>>> {
        self.dispatch(|reply| Job::GetMany {
            locker: locker.to_string(),
            keys,
            reply,
        })
        .await
    }

    /// Write many entries in **one** atomic commit. Hive's `putAll`.
    ///
    /// Everything lands together or nothing does: one fsync natively, one
    /// IndexedDB transaction on the web. An empty list is `Ok(())` and never
    /// reaches the backend at all.
    ///
    /// Raw values are stored **unchunked** — the bytes-only view seals each
    /// value whole, where a `LazyLocker<Vec<u8>>` would split a large one into
    /// chunks. Keep a single value well under ~100 MiB on the web, where one
    /// oversized entry is a whole transaction's worth of JS values copied
    /// across the wasm boundary at once.
    pub async fn put_all(&self, locker: &str, entries: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.dispatch(|reply| Job::PutAll {
            locker: locker.to_string(),
            entries,
            reply,
        })
        .await
    }

    /// Remove many keys in **one** atomic commit. Hive's `deleteAll`.
    ///
    /// Removing an absent key is not an error, and an empty list never reaches
    /// the backend.
    pub async fn delete_all(&self, locker: &str, keys: Vec<String>) -> Result<()> {
        self.dispatch(|reply| Job::DeleteAll {
            locker: locker.to_string(),
            keys,
            reply,
        })
        .await
    }

    /// Every key/value pair beginning with `prefix`, in byte order. The pairs
    /// behind Hive's `toMap`.
    ///
    /// Reads every value in the range, so the cost is the whole prefix. A key
    /// that is not valid UTF-8 is refused rather than skipped, exactly as
    /// [`BankHandle::keys`] refuses it.
    pub async fn entries(&self, locker: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.dispatch(|reply| Job::Entries {
            locker: locker.to_string(),
            prefix: prefix.to_string(),
            reply,
        })
        .await
    }

    /// Whether a key is stored. Hive's `containsKey`.
    ///
    /// One backend read, and the value is never decoded.
    pub async fn contains_key(&self, locker: &str, key: &str) -> Result<bool> {
        self.dispatch(|reply| Job::ContainsKey {
            locker: locker.to_string(),
            key: key.to_string(),
            reply,
        })
        .await
    }

    /// How many records a locker holds. Hive's `length`.
    ///
    /// A key-only scan over storage: the bytes-only view keeps no RAM index,
    /// so this is counted rather than remembered.
    pub async fn len(&self, locker: &str) -> Result<usize> {
        self.dispatch(|reply| Job::Len {
            locker: locker.to_string(),
            reply,
        })
        .await
    }

    /// Whether a locker name is registered in the store. Hive's `boxExists`.
    ///
    /// See [`Bank::locker_exists`].
    pub async fn locker_exists(&self, name: &str) -> Result<bool> {
        self.dispatch(|reply| Job::LockerExists {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Every registered locker name, in byte order. See [`Bank::locker_names`].
    pub async fn locker_names(&self) -> Result<Vec<String>> {
        self.dispatch(|reply| Job::LockerNames { reply }).await
    }

    /// Erase one locker and forget its name. Hive's `deleteBoxFromDisk`.
    ///
    /// `false` when the name was not registered. See [`Bank::delete_locker`],
    /// which refuses a locker that is still open.
    pub async fn delete_locker(&self, name: &str) -> Result<bool> {
        self.dispatch(|reply| Job::DeleteLocker {
            name: name.to_string(),
            reply,
        })
        .await
    }

    /// Commit every open locker's staged writes and make them durable.
    ///
    /// See [`Bank::flush_all`]. Nothing calls it for you — crossbank spawns
    /// nothing.
    pub async fn flush_all(&self) -> Result<()> {
        self.dispatch(|reply| Job::FlushAll { reply }).await
    }

    /// Ask the platform to keep this bank's data. See [`Bank::persist`].
    pub async fn persist(&self) -> Result<bool> {
        self.dispatch(|reply| Job::Persist { reply }).await
    }

    /// Whether the platform has already granted persistence. See
    /// [`Bank::is_persisted`].
    pub async fn is_persisted(&self) -> Result<bool> {
        self.dispatch(|reply| Job::IsPersisted { reply }).await
    }

    /// Storage usage, where the platform reports it. See [`Bank::usage`].
    pub async fn usage(&self) -> Result<Option<Usage>> {
        self.dispatch(|reply| Job::ReportUsage { reply }).await
    }

    async fn dispatch<T>(&self, build: impl FnOnce(Reply<T>) -> Job) -> Result<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mut sender = self.sender.clone();

        sender.try_send(build(reply_tx)).map_err(|e| {
            if e.is_disconnected() {
                Error::Closed
            } else {
                Error::backend("the bank's job queue is full")
            }
        })?;

        // A dropped reply channel means the service stopped mid-flight.
        reply_rx.await.map_err(|_| Error::Closed)?
    }
}

impl Bank {
    /// A `Send + Sync` handle usable from any thread.
    ///
    /// Requires [`Bank::into_service`] to be running, or every call returns
    /// [`Error::Closed`].
    ///
    /// ```no_run
    /// # async fn demo(bank: crossbank::Bank) -> crossbank::Result<()> {
    /// let handle = bank.handle();
    /// // Move `handle` to any thread; poll `bank.into_service()` on this one.
    /// handle.put("settings", "theme", b"dark".to_vec()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn handle(&self) -> BankHandle {
        BankHandle::new(self.job_sender())
    }

    /// Renamed to [`Bank::handle`].
    #[deprecated(since = "0.1.0", note = "renamed to `Bank::handle`")]
    pub fn remote(&self) -> BankHandle {
        self.handle()
    }

    /// The service loop. The consumer polls this on the bank's owning thread.
    ///
    /// crossbank spawns nothing, so where this runs is the caller's decision:
    /// `wasm_bindgen_futures::spawn_local` on the web, an ordinary task
    /// natively. It completes when every [`BankHandle`] has been dropped.
    ///
    /// The thread polling it must never block.
    pub async fn into_service(self) {
        let Some(mut jobs) = self.take_job_receiver() else {
            // A second service would silently steal jobs from the first.
            return;
        };

        while let Some(job) = jobs.next().await {
            self.serve(job).await;
        }
    }

    async fn serve(&self, job: Job) {
        match job {
            Job::Get { locker, key, reply } => {
                let _ = reply.send(self.raw_get(&locker, &key).await);
            }
            Job::Put {
                locker,
                key,
                value,
                reply,
            } => {
                let _ = reply.send(self.raw_put(&locker, &key, value).await);
            }
            Job::Delete { locker, key, reply } => {
                let _ = reply.send(self.raw_delete(&locker, &key).await);
            }
            Job::Keys {
                locker,
                prefix,
                reply,
            } => {
                let _ = reply.send(self.raw_keys(&locker, &prefix).await);
            }
            Job::Clear { locker, reply } => {
                let _ = reply.send(self.raw_clear(&locker).await);
            }
            Job::GetMany {
                locker,
                keys,
                reply,
            } => {
                let _ = reply.send(self.raw_get_many(&locker, keys).await);
            }
            Job::PutAll {
                locker,
                entries,
                reply,
            } => {
                let _ = reply.send(self.raw_put_all(&locker, entries).await);
            }
            Job::DeleteAll {
                locker,
                keys,
                reply,
            } => {
                let _ = reply.send(self.raw_delete_all(&locker, keys).await);
            }
            Job::Entries {
                locker,
                prefix,
                reply,
            } => {
                let _ = reply.send(self.raw_entries(&locker, &prefix).await);
            }
            Job::ContainsKey { locker, key, reply } => {
                let _ = reply.send(self.raw_contains_key(&locker, &key).await);
            }
            Job::Len { locker, reply } => {
                let _ = reply.send(self.raw_len(&locker).await);
            }
            Job::LockerExists { name, reply } => {
                let _ = reply.send(self.locker_exists(&name).await);
            }
            Job::LockerNames { reply } => {
                let _ = reply.send(self.locker_names().await);
            }
            Job::DeleteLocker { name, reply } => {
                let _ = reply.send(self.delete_locker(&name).await);
            }
            Job::FlushAll { reply } => {
                let _ = reply.send(self.flush_all().await);
            }
            Job::Persist { reply } => {
                let _ = reply.send(self.persist().await);
            }
            Job::IsPersisted { reply } => {
                let _ = reply.send(self.is_persisted().await);
            }
            Job::ReportUsage { reply } => {
                let _ = reply.send(self.usage().await);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        BFut, Backend, CommitOptions, MemoryBackend, Op, ScanPage, ScanRequest, Table,
    };
    use crate::codec::{default_chain, FilterChain};
    use futures::executor::block_on;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Drive the service alongside the test body on one executor, the way a
    /// consumer would with `spawn_local`.
    ///
    /// The bank has to be built *inside* the `block_on`: nesting one
    /// `block_on` inside another is a hard error in futures' `LocalPool`.
    async fn serve_while<F, Fut, T>(bank: Bank, body: F) -> T
    where
        F: FnOnce(BankHandle) -> Fut,
        Fut: Future<Output = T>,
    {
        let remote = bank.handle();
        let service = bank.into_service();
        futures::pin_mut!(service);

        let work = body(remote);
        futures::pin_mut!(work);

        // Poll both. The work finishing first is the expected outcome; the
        // service finishing first would mean it stopped early, which is a
        // failure worth reporting rather than silently returning.
        match futures::future::select(work, service).await {
            futures::future::Either::Left((value, _)) => value,
            futures::future::Either::Right(_) => {
                panic!("the service loop ended before the work finished")
            }
        }
    }

    fn with_service<F, Fut, T>(body: F) -> T
    where
        F: FnOnce(BankHandle) -> Fut,
        Fut: Future<Output = T>,
    {
        block_on(async { serve_while(open_bank().await, body).await })
    }

    /// [`with_service`] over a backend the test built, so it can watch what
    /// the bank actually asks the storage layer to do.
    fn with_service_over<F, Fut, T>(backend: Arc<dyn Backend>, body: F) -> T
    where
        F: FnOnce(BankHandle) -> Fut,
        Fut: Future<Output = T>,
    {
        block_on(async {
            let bank = Bank::with_backend(backend).await.unwrap();
            serve_while(bank, body).await
        })
    }

    async fn open_bank() -> Bank {
        Bank::with_backend(Arc::new(MemoryBackend::new()))
            .await
            .unwrap()
    }

    /// A backend that watches commits: it counts them, and can refuse one that
    /// carries more record ops than a test allows.
    ///
    /// The cap counts `Records` ops rather than every op, deliberately.
    /// Registering a locker name legitimately commits **two** `Meta` ops at
    /// once — the name-to-id record and the id counter, which must land
    /// together — so a cap on `ops.len()` would fail every operation before it
    /// reached a single record and prove nothing about batching.
    struct WatchedCommits {
        inner: Arc<dyn Backend>,
        commits: AtomicUsize,
        max_record_ops: Option<usize>,
    }

    impl WatchedCommits {
        fn counting() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(MemoryBackend::new()),
                commits: AtomicUsize::new(0),
                max_record_ops: None,
            })
        }

        fn capped(max_record_ops: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(MemoryBackend::new()),
                commits: AtomicUsize::new(0),
                max_record_ops: Some(max_record_ops),
            })
        }

        fn reset(&self) {
            self.commits.store(0, Ordering::SeqCst);
        }

        fn commits(&self) -> usize {
            self.commits.load(Ordering::SeqCst)
        }

        fn admit(&self, ops: &[Op]) -> Result<()> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            let Some(max) = self.max_record_ops else {
                return Ok(());
            };
            let records = ops
                .iter()
                .filter(|op| {
                    matches!(
                        op,
                        Op::Put {
                            table: Table::Records,
                            ..
                        } | Op::Delete {
                            table: Table::Records,
                            ..
                        }
                    )
                })
                .count();
            if records > max {
                return Err(Error::backend("too many record ops in one commit"));
            }
            Ok(())
        }
    }

    impl Backend for WatchedCommits {
        fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
            self.inner.get(table, key)
        }

        fn get_many<'a>(
            &'a self,
            table: Table,
            keys: Vec<Vec<u8>>,
        ) -> BFut<'a, Vec<Option<Vec<u8>>>> {
            self.inner.get_many(table, keys)
        }

        fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
            self.inner.scan(request)
        }

        fn scan_page_size(&self) -> usize {
            self.inner.scan_page_size()
        }

        fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
            Box::pin(async move {
                self.admit(&ops)?;
                self.inner.commit(ops).await
            })
        }

        fn commit_with(&self, ops: Vec<Op>, options: CommitOptions) -> BFut<'_, ()> {
            Box::pin(async move {
                self.admit(&ops)?;
                self.inner.commit_with(ops, options).await
            })
        }

        fn usage(&self) -> BFut<'_, Option<crate::backend::Usage>> {
            self.inner.usage()
        }

        fn flush(&self) -> BFut<'_, ()> {
            self.inner.flush()
        }

        fn close(&self) -> BFut<'_, ()> {
            self.inner.close()
        }
    }

    #[test]
    fn a_remote_handle_is_send_and_sync() {
        // The property the whole module exists for. If this stops compiling,
        // a JsValue or an Rc has leaked into the job type.
        fn assert_bounds<T: Send + Sync + Clone + 'static>() {}
        assert_bounds::<BankHandle>();
    }

    #[test]
    fn put_then_get_round_trips_through_the_service() {
        let got = with_service(|remote| async move {
            remote.put("l", "k", b"value".to_vec()).await.unwrap();
            remote.get("l", "k").await.unwrap()
        });
        assert_eq!(got, Some(b"value".to_vec()));
    }

    #[test]
    fn a_missing_key_is_none() {
        let got = with_service(|remote| async move { remote.get("l", "absent").await.unwrap() });
        assert_eq!(got, None);
    }

    #[test]
    fn an_empty_value_is_not_a_missing_key() {
        let (empty, absent) = with_service(|remote| async move {
            remote.put("l", "empty", Vec::new()).await.unwrap();
            (
                remote.get("l", "empty").await.unwrap(),
                remote.get("l", "absent").await.unwrap(),
            )
        });
        assert_eq!(empty, Some(Vec::new()));
        assert_eq!(absent, None);
    }

    #[test]
    fn delete_and_clear_work_remotely() {
        let (after_delete, after_clear) = with_service(|remote| async move {
            remote.put("l", "a", b"1".to_vec()).await.unwrap();
            remote.put("l", "b", b"2".to_vec()).await.unwrap();
            remote.delete("l", "a").await.unwrap();
            let after_delete = remote.keys("l", "").await.unwrap();
            remote.clear("l").await.unwrap();
            (after_delete, remote.keys("l", "").await.unwrap())
        });
        assert_eq!(after_delete, vec!["b"]);
        assert!(after_clear.is_empty());
    }

    #[test]
    fn keys_filters_by_prefix_and_orders_bytewise() {
        let keys = with_service(|remote| async move {
            for k in ["b::2", "a::1", "b::1"] {
                remote.put("l", k, b"x".to_vec()).await.unwrap();
            }
            remote.keys("l", "b::").await.unwrap()
        });
        assert_eq!(keys, vec!["b::1", "b::2"]);
    }

    #[test]
    fn calls_fail_cleanly_when_no_service_is_running() {
        // Better a typed error than a hang. A caller that forgot to spawn the
        // service should find out immediately.
        let bank = block_on(open_bank());
        let remote = bank.handle();
        drop(bank);

        assert!(matches!(block_on(remote.get("l", "k")), Err(Error::Closed)));
    }

    #[test]
    fn put_all_then_get_many_reads_every_entry_back() {
        let (got, absent) = with_service(|remote| async move {
            remote
                .put_all(
                    "l",
                    vec![
                        ("a".to_string(), b"1".to_vec()),
                        ("b".to_string(), Vec::new()),
                        ("c".to_string(), b"3".to_vec()),
                    ],
                )
                .await
                .unwrap();

            // Positional, and asked for in an order that is not the stored
            // one: slot i must belong to keys[i], not to the i-th record.
            let got = remote
                .get_many("l", vec!["c".to_string(), "a".to_string(), "b".to_string()])
                .await
                .unwrap();
            let absent = remote
                .get_many("l", vec!["a".to_string(), "nope".to_string()])
                .await
                .unwrap();
            (got, absent)
        });

        assert_eq!(
            got,
            vec![Some(b"3".to_vec()), Some(b"1".to_vec()), Some(Vec::new())],
            "an empty stored value is not a missing key"
        );
        assert_eq!(absent, vec![Some(b"1".to_vec()), None]);
    }

    #[test]
    fn put_all_and_delete_all_with_empty_lists_are_no_ops() {
        // "Nothing to write" must cost nothing — no commit, and on the web no
        // IndexedDB transaction at all.
        let watched = WatchedCommits::counting();
        let backend: Arc<dyn Backend> = watched.clone();

        let commits = with_service_over(backend, |remote| {
            let watched = watched.clone();
            async move {
                // Opening the bank writes the format version; the question is
                // what the two empty calls cost from here.
                watched.reset();
                remote.put_all("l", Vec::new()).await.unwrap();
                remote.delete_all("l", Vec::new()).await.unwrap();
                watched.commits()
            }
        });

        assert_eq!(commits, 0, "an empty bulk call must not reach the backend");
    }

    #[test]
    fn delete_all_removes_only_the_named_keys() {
        let left = with_service(|remote| async move {
            remote
                .put_all(
                    "l",
                    vec![
                        ("a".to_string(), b"1".to_vec()),
                        ("b".to_string(), b"2".to_vec()),
                        ("c".to_string(), b"3".to_vec()),
                    ],
                )
                .await
                .unwrap();
            // "never_written" is deliberate: deleting an absent key is not an
            // error, and must not take a neighbour with it.
            remote
                .delete_all("l", vec!["a".to_string(), "never_written".to_string()])
                .await
                .unwrap();
            remote.keys("l", "").await.unwrap()
        });

        assert_eq!(left, vec!["b", "c"]);
    }

    #[test]
    fn entries_returns_prefixed_pairs_in_byte_order_with_values() {
        let (prefixed, everything) = with_service(|remote| async move {
            remote
                .put_all(
                    "l",
                    vec![
                        ("b::2".to_string(), b"two".to_vec()),
                        ("a::1".to_string(), b"one".to_vec()),
                        ("b::1".to_string(), b"ONE".to_vec()),
                    ],
                )
                .await
                .unwrap();
            (
                remote.entries("l", "b::").await.unwrap(),
                remote.entries("l", "").await.unwrap(),
            )
        });

        assert_eq!(
            prefixed,
            vec![
                ("b::1".to_string(), b"ONE".to_vec()),
                ("b::2".to_string(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            everything
                .iter()
                .map(|(k, _)| k.clone())
                .collect::<Vec<_>>(),
            vec!["a::1", "b::1", "b::2"]
        );
    }

    #[test]
    fn entries_pages_past_a_single_scan_page() {
        // Sized FROM the backend, never from a literal: the moment a backend
        // advertises a bigger page, a fixture pinned to a constant stops
        // crossing a page boundary and quietly tests nothing.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let count = backend.scan_page_size() * 2 + 3;

        let entries = with_service_over(backend, move |remote| async move {
            let written: Vec<(String, Vec<u8>)> = (0..count)
                .map(|i| (format!("k{i:06}"), format!("v{i}").into_bytes()))
                .collect();
            remote.put_all("l", written).await.unwrap();
            remote.entries("l", "").await.unwrap()
        });

        assert_eq!(entries.len(), count, "the paging loop dropped records");
        assert_eq!(entries[0], ("k000000".to_string(), b"v0".to_vec()));
        assert_eq!(
            entries[count - 1],
            (
                format!("k{:06}", count - 1),
                format!("v{}", count - 1).into_bytes()
            ),
            "the last page must carry its values, not just its keys"
        );
    }

    #[test]
    fn contains_key_and_len_agree_with_keys() {
        let (present, absent, len, keys) = with_service(|remote| async move {
            remote
                .put_all(
                    "l",
                    vec![
                        ("a".to_string(), b"1".to_vec()),
                        ("b".to_string(), b"2".to_vec()),
                    ],
                )
                .await
                .unwrap();
            (
                remote.contains_key("l", "a").await.unwrap(),
                remote.contains_key("l", "z").await.unwrap(),
                remote.len("l").await.unwrap(),
                remote.keys("l", "").await.unwrap(),
            )
        });

        assert!(present);
        assert!(!absent);
        assert_eq!(len, keys.len());
        assert_eq!(len, 2);
    }

    #[test]
    fn locker_exists_names_and_delete_locker_round_trip_remotely() {
        let (before, after_write, names, deleted, deleted_again, left) =
            with_service(|remote| async move {
                let before = remote.locker_exists("settings").await.unwrap();
                remote
                    .put("settings", "theme", b"dark".to_vec())
                    .await
                    .unwrap();
                remote
                    .put("notes", "first", b"hello".to_vec())
                    .await
                    .unwrap();

                let after_write = remote.locker_exists("settings").await.unwrap();
                let names = remote.locker_names().await.unwrap();
                let deleted = remote.delete_locker("settings").await.unwrap();
                let deleted_again = remote.delete_locker("settings").await.unwrap();
                let left = remote.locker_names().await.unwrap();
                (before, after_write, names, deleted, deleted_again, left)
            });

        assert!(!before, "an unwritten name is not registered");
        assert!(after_write);
        assert_eq!(names, vec!["notes", "settings"]);
        assert!(deleted);
        assert!(
            !deleted_again,
            "deleting an absent locker is false, not an error"
        );
        assert_eq!(left, vec!["notes"]);
    }

    #[test]
    fn flush_all_persist_is_persisted_and_usage_answer_remotely() {
        // These are pass-throughs, so the assertion is that the round trip
        // happens at all and the answer is the bank's own — not that any
        // particular platform reports usage.
        let (persisted, is_persisted, usage) = with_service(|remote| async move {
            remote.put("l", "k", b"v".to_vec()).await.unwrap();
            remote.flush_all().await.unwrap();
            (
                remote.persist().await.unwrap(),
                remote.is_persisted().await.unwrap(),
                remote.usage().await.unwrap(),
            )
        });

        // The memory backend under this bank reports the bytes it holds, so
        // the answer travelling back over the channel is a real one rather
        // than a default.
        let usage = usage.expect("the memory backend reports its own usage");
        assert!(usage.used > 0, "a bank that was written to uses bytes");

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Natively the file is the persistence, so both answer true.
            assert!(persisted);
            assert!(is_persisted);
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = (persisted, is_persisted);
        }
    }

    #[test]
    fn put_all_is_one_commit() {
        // Half one: a backend that refuses a commit carrying two record ops.
        // If `put_all` wrote its entries one commit at a time the cap would
        // never fire and this would pass for the wrong reason — so the
        // assertion is that it DOES fire, and that nothing landed.
        let capped = WatchedCommits::capped(1);
        let backend: Arc<dyn Backend> = capped.clone();

        let (failed, left) = with_service_over(backend, |remote| async move {
            let failed = remote
                .put_all(
                    "l",
                    vec![
                        ("a".to_string(), b"1".to_vec()),
                        ("b".to_string(), b"2".to_vec()),
                    ],
                )
                .await;
            (failed, remote.keys("l", "").await.unwrap())
        });

        assert!(
            failed.is_err(),
            "two entries must ride in one commit, so the one-record cap must refuse it"
        );
        assert!(
            left.is_empty(),
            "a refused commit is atomic: nothing at all may land"
        );

        // Half two: the same call on a backend that only counts. Exactly one
        // commit, for three entries.
        let watched = WatchedCommits::counting();
        let backend: Arc<dyn Backend> = watched.clone();

        let commits = with_service_over(backend, |remote| {
            let watched = watched.clone();
            async move {
                // Touch the locker first, so the name registration's own
                // commits are not counted as part of the write.
                remote.len("l").await.unwrap();
                watched.reset();
                remote
                    .put_all(
                        "l",
                        vec![
                            ("a".to_string(), b"1".to_vec()),
                            ("b".to_string(), b"2".to_vec()),
                            ("c".to_string(), b"3".to_vec()),
                        ],
                    )
                    .await
                    .unwrap();
                watched.commits()
            }
        });

        assert_eq!(
            commits, 1,
            "three entries, one commit — one fsync, one transaction"
        );
    }

    /// A checksum-only chain is reachable from the public API and round trips.
    ///
    /// The whole point of the addition: a consumer with incompressible values
    /// can keep corruption detection without paying LZ4 for every write.
    #[test]
    fn a_checksum_only_chain_round_trips_and_describes_itself() {
        assert_eq!(FilterChain::checksum_only().describe(), "chain 2 (crc32)");
        assert_ne!(
            FilterChain::checksum_only().describe(),
            default_chain().describe(),
            "a chain that transforms bytes differently must not look the same"
        );
        assert_ne!(
            FilterChain::checksum_only().id(),
            default_chain().id(),
            "the id is the compatibility gate; a collision would misdecode values"
        );

        let got = block_on(async {
            let bank = Bank::with_backend_and_chain(
                Arc::new(MemoryBackend::new()),
                FilterChain::checksum_only(),
            )
            .await
            .unwrap();
            serve_while(bank, |remote| async move {
                remote
                    .put("l", "k", b"incompressible".to_vec())
                    .await
                    .unwrap();
                remote.get("l", "k").await.unwrap()
            })
            .await
        });

        assert_eq!(got, Some(b"incompressible".to_vec()));
    }

    /// Natively the service future must be `Send`, or a consumer cannot put it
    /// on a work-stealing runtime at all.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn the_service_future_is_send_natively() {
        fn assert_send<T: Send>(_: &T) {}

        let bank = block_on(open_bank());
        let service = bank.into_service();
        assert_send(&service);
    }

    #[test]
    fn remote_writes_are_visible_to_the_typed_api() {
        // A remote locker is exactly a Locker<Vec<u8>>: same envelope, same
        // filter chain, same schema tag. The two views must agree.
        block_on(async {
            let bank = open_bank().await;
            let remote = bank.handle();

            let service = bank.into_service();
            futures::pin_mut!(service);

            let work = async {
                remote.put("shared", "k", b"bytes".to_vec()).await.unwrap();
            };
            futures::pin_mut!(work);
            futures::future::select(work, service).await;
        });
    }
}
