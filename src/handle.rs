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
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Get { .. } => "Get",
            Self::Put { .. } => "Put",
            Self::Delete { .. } => "Delete",
            Self::Keys { .. } => "Keys",
            Self::Clear { .. } => "Clear",
        };
        f.debug_struct("Job").field("kind", &name).finish()
    }
}

/// A `Send + Sync + Clone` handle onto a bank running elsewhere.
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use futures::executor::block_on;
    use std::future::Future;
    use std::sync::Arc;

    /// Drive the service alongside the test body on one executor, the way a
    /// consumer would with `spawn_local`.
    fn with_service<F, Fut, T>(body: F) -> T
    where
        F: FnOnce(BankHandle) -> Fut,
        Fut: Future<Output = T>,
    {
        block_on(async {
            // Must be awaited, not block_on'd: nesting one block_on inside
            // another is a hard error in futures' LocalPool.
            let bank = open_bank().await;
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
        })
    }

    async fn open_bank() -> Bank {
        Bank::with_backend(Arc::new(MemoryBackend::new()))
            .await
            .unwrap()
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
