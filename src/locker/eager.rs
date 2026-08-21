//! The eager locker: everything resident, reads synchronous.
//!
//! Hive's `Box`, and the reason Hive feels the way it does — `get()` returns
//! immediately because the value is already here. Ideal for settings, theme
//! state, feature flags: small, hot, and read from render paths that cannot
//! await.
//!
//! Values are held as `Arc<T>`, decoded once at open. That keeps `get()` O(1),
//! avoids requiring `T: Clone`, and means a decode failure surfaces at open —
//! where it can be reported — rather than from an infallible getter.
//!
//! # Why this type refuses things
//!
//! A synchronous, infallible `get()` cannot await a chunk fetch, cannot await a
//! refetch after another tab invalidates a key, and cannot report an error. So
//! the type refuses, up front, anything that would put it in that position:
//! oversized values, oversized contents at open, and eviction policies.
//! Refusing loudly beats a getter that lies.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::Backend;
use crate::codec::FilterChain;
use crate::error::{Error, Result};
use crate::key::LockerId;

use super::inner::Inner;
use super::policy::{LockerConfig, OnCorrupt, Policy};
use super::resident::{Pending, Resident};
use super::transaction::{Staged, Transaction, TxMode};
use crate::watch::Event;

/// A locker whose values live in memory.
pub struct Locker<T> {
    inner: Arc<Inner>,
    /// Shared with the coherence sink, which replaces resident values from
    /// another tab's news without going through the locker handle.
    values: Arc<Mutex<BTreeMap<Vec<u8>, Arc<T>>>>,
    /// Keys whose stored bytes would not decode at open, under
    /// [`OnCorrupt::Skip`]. Fixed once open returns.
    corrupt: Vec<Vec<u8>>,
    /// Staged writes, shared with [`crate::Bank::flush_all`], which cannot
    /// know `T`. An eager locker keeps no key index and takes no byte budget,
    /// so staging is all this holds.
    res: Arc<Resident>,
    /// Keeps this locker's coherence registration alive; the channel holds
    /// only a `Weak`, so dropping the locker unregisters it.
    #[cfg(target_arch = "wasm32")]
    sink: Mutex<Option<crate::coherence::SinkHandle>>,
}

impl<T> std::fmt::Debug for Locker<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locker")
            .field("name", &self.inner.name)
            .field("entries", &self.len())
            .finish()
    }
}

impl<T> Locker<T>
where
    T: Serialize + DeserializeOwned,
{
    pub(crate) async fn open(
        backend: Arc<dyn Backend>,
        chain: Arc<FilterChain>,
        id: LockerId,
        name: String,
        config: LockerConfig,
        shared: Arc<super::inner::Shared>,
    ) -> Result<Self> {
        // Eviction and residency contradict each other: shedding a value from
        // storage while RAM still holds it leaves a replica that outlives the
        // eviction. Refuse the combination rather than paper over it.
        if let Policy::Evictable { .. } = config.policy {
            return Err(Error::InvalidConfig(format!(
                "locker {name:?} is eager, which cannot use an evictable policy; \
                 use a lazy locker instead"
            )));
        }

        let inner = Arc::new(Inner {
            write_lock: futures::lock::Mutex::new(()),
            backend,
            chain,
            id,
            name,
            config,
            shared,
            watchers: Default::default(),
            closed: AtomicBool::new(false),
        });

        let mut values = BTreeMap::new();
        let mut corrupt: Vec<Vec<u8>> = Vec::new();
        let mut loaded: u64 = 0;
        let budget = config.eager_budget;
        let mut overflow: Option<u64> = None;

        inner
            .walk(
                Bound::Unbounded,
                Bound::Unbounded,
                false,
                true,
                |key, raw| {
                    let bytes = raw.ok_or_else(|| {
                        Error::Corrupt(format!("backend omitted a value for key {key:?}"))
                    })?;

                    loaded = loaded.saturating_add(bytes.len() as u64);
                    if loaded > budget {
                        // Record and keep walking rather than decoding further —
                        // the point is to fail before the memory is committed.
                        overflow.get_or_insert(loaded);
                        return Ok(());
                    }

                    match inner.open::<T>(&bytes) {
                        Ok(value) => {
                            values.insert(key, Arc::new(value));
                        }
                        // Skip records the decoder chokes on, when asked to.
                        // The stored bytes are left untouched, so a later
                        // build with a working decoder can still read them.
                        Err(e) if config.on_corrupt == OnCorrupt::Skip && e.is_corruption() => {
                            corrupt.push(key);
                        }
                        Err(e) => return Err(e),
                    }
                    Ok(())
                },
            )
            .await?;

        if overflow.is_some() {
            return Err(Error::LockerTooLarge {
                bytes: loaded,
                budget,
            });
        }

        let res = Arc::new(Resident::new(inner.clone(), TxMode::Eager, None, None));
        Ok(Self {
            inner,
            values: Arc::new(Mutex::new(values)),
            corrupt,
            res,
            #[cfg(target_arch = "wasm32")]
            sink: Mutex::new(None),
        })
    }

    /// Store a value. Writes are asynchronous even though reads are not.
    pub async fn put(&self, key: &str, value: T) -> Result<Arc<T>> {
        self.put_by(key.as_bytes(), value).await
    }

    /// As [`Locker::put`], under a binary key.
    pub async fn put_by(&self, key: &[u8], value: T) -> Result<Arc<T>> {
        self.inner.ensure_open()?;
        let sealed = self.inner.seal(&value)?;
        if sealed.len() > self.inner.config.max_inline {
            return Err(Error::ValueTooLarge {
                bytes: sealed.len(),
                max_inline: self.inner.config.max_inline,
            });
        }

        if self.res.is_deferred() {
            // The resident copy is updated at once: this handle's own writes
            // are visible to it immediately, committed or not. The payload is
            // re-encoded rather than reusing `sealed`, because the flush path
            // seals every batch through one code path.
            let payload = postcard::to_allocvec(&value)
                .map_err(|e| Error::Filter(format!("postcard serialisation failed: {e}")))?;
            let full = self.res.stage(Pending::Put {
                key: key.to_vec(),
                payload,
            })?;
            let shared = Arc::new(value);
            if let Ok(mut guard) = self.values.lock() {
                guard.insert(key.to_vec(), shared.clone());
            }
            self.inner.announce(Event::Put { key: key.to_vec() });
            if full {
                let _guard = self.inner.write_lock.lock().await;
                self.res.flush_locked().await?;
            }
            return Ok(shared);
        }

        let op = crate::backend::api::Op::Put {
            table: crate::backend::api::Table::Records,
            key: self.inner.encode_key(key),
            value: sealed,
        };
        self.inner.commit(vec![op]).await?;

        // Only mirror into RAM after the write lands, so a failed commit does
        // not leave the resident copy claiming something that is not stored.
        let shared = Arc::new(value);
        if let Ok(mut guard) = self.values.lock() {
            guard.insert(key.to_vec(), shared.clone());
        }
        self.inner.announce(Event::Put { key: key.to_vec() });
        Ok(shared)
    }

    /// Commit everything this locker has staged. See [`crate::Commit`].
    ///
    /// Always safe to call: an immediate-commit locker has nothing staged and
    /// returns at once.
    pub async fn flush(&self) -> Result<()> {
        self.inner.ensure_open()?;
        self.res.flush().await
    }

    /// How many writes are staged and not yet committed.
    pub fn pending(&self) -> usize {
        self.res.pending_len()
    }

    /// Payload bytes staged and not yet committed.
    pub fn pending_bytes(&self) -> u64 {
        self.res.pending_bytes()
    }

    /// Run a transaction: every staged write lands together, or none does.
    ///
    /// Returning `Err` rolls back — nothing is written and the resident copy
    /// is untouched.
    pub async fn transact<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce(Transaction<T>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.inner.ensure_open()?;
        let _guard = self.inner.write_lock.lock().await;

        let tx = Transaction::new(self.inner.clone());
        let staged = tx.staged_handle();
        f(tx).await?;

        let entries = {
            let mut guard = staged
                .lock()
                .map_err(|_| Error::backend("transaction lock was poisoned"))?;
            std::mem::take(&mut *guard)
        };
        if entries.is_empty() {
            return Ok(());
        }

        // An eager locker holds its values, so the size limit applies to
        // transactional writes exactly as it does to a plain put. `ops_for`
        // enforces it while sealing, so the bytes are only produced once.
        let ops = Transaction::<T>::ops_for(&self.inner, &entries, TxMode::Eager).await?;
        self.inner.commit(ops).await?;

        if let Ok(mut guard) = self.values.lock() {
            for entry in &entries {
                match entry {
                    Staged::Put { key, value, .. } => {
                        guard.insert(key.clone(), value.clone());
                    }
                    Staged::Delete { key } => {
                        guard.remove(key);
                    }
                    Staged::Clear => guard.clear(),
                }
            }
        }

        for entry in &entries {
            match entry {
                Staged::Put { key, .. } => self.inner.announce(Event::Put { key: key.clone() }),
                Staged::Delete { key } => self.inner.announce(Event::Deleted { key: key.clone() }),
                Staged::Clear => self.inner.announce(Event::Cleared),
            }
        }
        Ok(())
    }

    /// Remove a key. Removing an absent key is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.delete_by(key.as_bytes()).await
    }

    /// As [`Locker::delete`], under a binary key.
    pub async fn delete_by(&self, key: &[u8]) -> Result<()> {
        self.inner.ensure_open()?;
        if self.res.is_deferred() {
            let full = self.res.stage(Pending::Delete { key: key.to_vec() })?;
            if let Ok(mut guard) = self.values.lock() {
                guard.remove(key);
            }
            self.inner.announce(Event::Deleted { key: key.to_vec() });
            if full {
                let _guard = self.inner.write_lock.lock().await;
                self.res.flush_locked().await?;
            }
            return Ok(());
        }
        self.inner.commit(vec![self.inner.delete_op(key)]).await?;
        if let Ok(mut guard) = self.values.lock() {
            guard.remove(key);
        }
        self.inner.announce(Event::Deleted { key: key.to_vec() });
        Ok(())
    }

    /// Store many entries in **one** atomic commit.
    ///
    /// Hive's `putAll`. Everything lands together or nothing does, which is
    /// both faster than N separate puts and the only way to write a set of
    /// keys that must stay consistent with each other.
    pub async fn put_all(&self, entries: impl IntoIterator<Item = (String, T)>) -> Result<()> {
        let entries: Vec<(String, T)> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(());
        }
        self.transact(move |tx| async move {
            for (key, value) in entries {
                tx.put(&key, value)?;
            }
            Ok(())
        })
        .await
    }

    /// Remove many keys in **one** atomic commit. Hive's `deleteAll`.
    ///
    /// Removing an absent key is not an error, exactly as for
    /// [`Locker::delete`].
    pub async fn delete_all(&self, keys: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        let keys: Vec<String> = keys.into_iter().map(|k| k.as_ref().to_string()).collect();
        if keys.is_empty() {
            return Ok(());
        }
        self.transact(move |tx| async move {
            for key in keys {
                tx.delete(&key)?;
            }
            Ok(())
        })
        .await
    }

    /// Remove everything in this locker, and nothing outside it.
    pub async fn clear(&self) -> Result<()> {
        self.inner.ensure_open()?;
        if self.res.is_deferred() {
            let full = self.res.stage(Pending::Clear)?;
            if let Ok(mut guard) = self.values.lock() {
                guard.clear();
            }
            self.inner.announce(Event::Cleared);
            if full {
                let _guard = self.inner.write_lock.lock().await;
                self.res.flush_locked().await?;
            }
            return Ok(());
        }
        self.inner.commit(vec![self.inner.clear_op()]).await?;
        if let Ok(mut guard) = self.values.lock() {
            guard.clear();
        }
        self.inner.announce(Event::Cleared);
        Ok(())
    }
}

/// Applies another tab's news to one open eager locker.
///
/// Generic over `T` because an eager locker holds decoded values: replacing
/// one means decoding the bytes the message carried.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) struct EagerSink<T> {
    inner: Arc<Inner>,
    values: Arc<Mutex<BTreeMap<Vec<u8>, Arc<T>>>>,
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
impl<T: DeserializeOwned> crate::coherence::Sink for EagerSink<T> {
    fn locker_id(&self) -> LockerId {
        self.inner.id
    }

    fn apply(&self, announcement: &crate::coherence::Announcement) {
        if self.inner.is_closed() {
            return;
        }
        if announcement.cleared {
            if let Ok(mut values) = self.values.lock() {
                values.clear();
            }
            self.inner.announce(Event::Cleared);
        }
        for change in &announcement.changes {
            // Decoded here, in a plain callback, so `get()` stays synchronous
            // and infallible. What cannot be decoded here cannot be held.
            let decoded = match (change.deleted, &change.value) {
                (true, _) => None,
                (false, Some(sealed)) => self.inner.open::<T>(sealed).ok().map(Arc::new),
                (false, None) => None,
            };

            let event = match (&decoded, change.deleted) {
                (Some(_), _) => Event::Put {
                    key: change.key.clone(),
                },
                (None, true) => Event::Deleted {
                    key: change.key.clone(),
                },
                // The key was written, but with a value this tab cannot hold:
                // too large to carry, or bytes it cannot decode. Dropping the
                // resident copy is the honest answer — a stale value would be
                // a lie an infallible getter could not take back.
                (None, false) => Event::Stale {
                    key: change.key.clone(),
                },
            };

            if let Ok(mut values) = self.values.lock() {
                match decoded {
                    Some(value) => {
                        values.insert(change.key.clone(), value);
                    }
                    None => {
                        values.remove(change.key.as_slice());
                    }
                }
            }
            self.inner.announce(event);
        }
    }
}

/// Bounds-free accessors. Split out so `Debug` — and any caller holding a
/// `Locker<T>` for a `T` that is not itself serialisable — can still read.
impl<T> Locker<T> {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn sink(&self) -> EagerSink<T> {
        EagerSink {
            inner: self.inner.clone(),
            values: self.values.clone(),
        }
    }
}

impl<T: DeserializeOwned + 'static> Locker<T> {
    /// Start receiving other tabs' news. See [`LazyLocker::join_coherence`].
    pub(crate) fn join_coherence(&self) {
        #[cfg(target_arch = "wasm32")]
        {
            if !self.inner.shared.coherence.is_enabled() {
                return;
            }
            let handle = crate::coherence::handle(self.sink());
            self.inner.shared.coherence.register(&handle);
            if let Ok(mut slot) = self.sink.lock() {
                *slot = Some(handle);
            }
        }
    }
}

impl<T> Locker<T> {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    pub(crate) fn resident(&self) -> &Arc<Resident> {
        &self.res
    }

    /// Keys whose stored bytes would not decode when this locker was opened.
    ///
    /// Always empty under [`OnCorrupt::Fail`], which refuses to open at all
    /// rather than reporting a partial view. Under [`OnCorrupt::Skip`] these
    /// are exactly the keys missing from [`Locker::keys`] and [`Locker::len`];
    /// their bytes are still on disk, untouched. [`crate::Bank::quarantine`]
    /// is the only thing that removes them.
    pub fn corrupt_keys(&self) -> Vec<Vec<u8>> {
        self.corrupt.clone()
    }

    /// Close this locker: drop its resident values and refuse further writes.
    ///
    /// The **bank's** backend is left open — closing one locker must not take
    /// the store down with it. Use [`crate::Bank::close`] for that.
    ///
    /// After this, [`Locker::get`] returns `None` and [`Locker::len`] is 0 for
    /// every key, exactly as if the locker were empty. That is not a lie the
    /// caller has to live with: [`Locker::is_closed`] is what distinguishes a
    /// closed locker from an empty one. Writes report [`Error::Closed`]
    /// instead of silently succeeding.
    ///
    /// Idempotent, and it does not delete anything — reopening the same name
    /// from the bank finds the data untouched.
    pub async fn close(&self) -> Result<()> {
        // Flush before closing, and close even if the flush fails: a caller
        // who is shutting down must not be left holding an open locker just
        // because its last batch would not land.
        let flushed = self.res.flush().await;
        self.res.discard_staged();
        self.inner.mark_closed();
        if let Ok(mut guard) = self.values.lock() {
            guard.clear();
        }
        flushed
    }

    /// Whether [`Locker::close`] has been called.
    ///
    /// The only way to tell a closed locker from an empty one, since a closed
    /// locker reads as empty by design.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Read a value. Synchronous and infallible — the whole point of the type.
    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        self.get_by(key.as_bytes())
    }

    /// As [`Locker::get`], under a binary key.
    pub fn get_by(&self, key: &[u8]) -> Option<Arc<T>> {
        self.values.lock().ok()?.get(key).cloned()
    }

    /// Read a value by cloning it out of the resident copy.
    ///
    /// [`Locker::get`] hands back an `Arc` and asks nothing of `T`; this is
    /// for callers who would rather own a `T` than reach through a pointer.
    pub fn get_cloned(&self, key: &str) -> Option<T>
    where
        T: Clone,
    {
        self.get(key).map(|v| (*v).clone())
    }

    /// Read a value, falling back to `default` when the key is absent.
    ///
    /// Hive's `get(key, defaultValue:)`. The default is *not* written — a read
    /// stays a read.
    pub fn get_or(&self, key: &str, default: T) -> Arc<T> {
        self.get(key).unwrap_or_else(|| Arc::new(default))
    }

    /// Run `f` over the stored value without cloning it or handing out an
    /// `Arc`. `None` when the key is absent.
    ///
    /// The lock is **not** held while `f` runs, so `f` may call back into this
    /// locker without deadlocking.
    pub fn with<R>(&self, key: &str, f: impl FnOnce(&T) -> R) -> Option<R> {
        let value = self.get(key)?;
        Some(f(&value))
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.contains_key_by(key.as_bytes())
    }

    /// As [`Locker::contains_key`], under a binary key.
    pub fn contains_key_by(&self, key: &[u8]) -> bool {
        self.values
            .lock()
            .map(|v| v.contains_key(key))
            .unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.values.lock().map(|v| v.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every UTF-8 key, in byte order.
    ///
    /// Keys that are not valid UTF-8 are **skipped**, not reported as an
    /// error — a listing must never fail because of a key it cannot spell.
    /// [`Locker::has_non_utf8_keys`] says whether any were skipped, and
    /// [`Locker::keys_bytes`] returns every key regardless.
    pub fn keys(&self) -> Vec<String> {
        self.values
            .lock()
            .map(|v| v.keys().filter_map(|k| super::lazy::utf8(k)).collect())
            .unwrap_or_default()
    }

    /// Every key as raw bytes, in byte order. Includes non-UTF-8 keys.
    pub fn keys_bytes(&self) -> Vec<Vec<u8>> {
        self.values
            .lock()
            .map(|v| v.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether this locker holds any key that [`Locker::keys`] cannot return.
    /// Never panics and never errors.
    pub fn has_non_utf8_keys(&self) -> bool {
        self.values
            .lock()
            .map(|v| v.keys().any(|k| std::str::from_utf8(k).is_err()))
            .unwrap_or(false)
    }

    /// Keys beginning with `prefix`, in byte order. UTF-8 keys only.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.keys_with_prefix_by(prefix.as_bytes())
            .iter()
            .filter_map(|k| super::lazy::utf8(k))
            .collect()
    }

    /// As [`Locker::keys_with_prefix`], over a binary prefix.
    pub fn keys_with_prefix_by(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.values
            .lock()
            .map(|v| {
                v.range(prefix.to_vec()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Subscribe to every change in this locker.
    pub fn watch(&self) -> crate::watch::EventStream {
        self.inner
            .watchers
            .subscribe(None, crate::watch::DEFAULT_CAPACITY)
    }

    /// Subscribe to changes affecting any of `keys`.
    ///
    /// Hive's `listenable(keys:)`. `Cleared` and `Lagged` always arrive: a
    /// clear affects every key, and a gap notice must never be dropped. An
    /// empty list therefore yields a stream of nothing but those two.
    pub fn watch_keys(&self, keys: &[&str]) -> crate::watch::EventStream {
        self.inner.watchers.subscribe(
            Some(keys.iter().map(|k| k.as_bytes().to_vec()).collect()),
            crate::watch::DEFAULT_CAPACITY,
        )
    }

    /// Subscribe to changes affecting one key.
    ///
    /// `Cleared` still arrives, because a clear affects every key.
    pub fn watch_key(&self, key: &str) -> crate::watch::EventStream {
        self.inner.watchers.subscribe(
            Some(vec![key.as_bytes().to_vec()]),
            crate::watch::DEFAULT_CAPACITY,
        )
    }

    /// Entries over a key range, in byte order. UTF-8 keys only.
    ///
    /// Synchronous, like every other eager read: the values are already here.
    pub fn range<'a, R: std::ops::RangeBounds<&'a str>>(&self, range: R) -> Vec<(String, Arc<T>)> {
        let start = crate::key::as_bytes(str_bound(range.start_bound()));
        let end = crate::key::as_bytes(str_bound(range.end_bound()));
        // `BTreeMap::range` *panics* on an inverted or empty-by-exclusion
        // range, and a wasm release build turns that into an abort.
        if crate::key::is_degenerate(start, end) {
            return Vec::new();
        }
        self.values
            .lock()
            .map(|v| {
                v.range::<[u8], _>((start, end))
                    .filter_map(|(k, val)| super::lazy::utf8(k).map(|k| (k, val.clone())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every entry as a map, in byte order. UTF-8 keys only.
    ///
    /// Hive's `toMap`. Free of I/O — an eager locker already holds everything.
    pub fn to_map(&self) -> BTreeMap<String, Arc<T>> {
        self.entries().into_iter().collect()
    }

    /// A snapshot of every entry, in byte order. UTF-8 keys only.
    pub fn entries(&self) -> Vec<(String, Arc<T>)> {
        self.values
            .lock()
            .map(|v| {
                v.iter()
                    .filter_map(|(k, val)| super::lazy::utf8(k).map(|k| (k, val.clone())))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A snapshot of every entry as raw key bytes, in byte order.
    pub fn entries_bytes(&self) -> Vec<(Vec<u8>, Arc<T>)> {
        self.values
            .lock()
            .map(|v| v.iter().map(|(k, val)| (k.clone(), val.clone())).collect())
            .unwrap_or_default()
    }
}

/// `Range<&str>` bounds arrive as `Bound<&&str>`; unwrap the extra reference.
fn str_bound<'a>(bound: Bound<&&'a str>) -> Bound<&'a str> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included(s),
        Bound::Excluded(s) => Bound::Excluded(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::codec::default_chain;
    use crate::coherence::{api::Change, Announcement, Sink};
    use futures::executor::block_on;
    use futures::StreamExt;

    fn open_with(config: LockerConfig) -> Result<Locker<String>> {
        block_on(Locker::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "test".into(),
            config,
            Default::default(),
        ))
    }

    fn locker() -> Locker<String> {
        open_with(LockerConfig::default()).unwrap()
    }

    /// An eager locker takes another tab's small write straight into RAM.
    #[test]
    fn a_sink_replaces_a_resident_value_from_an_inline_write() {
        let l = locker();
        block_on(l.put("k", "mine".into())).unwrap();
        let mut events = l.watch();

        let sealed = l.inner.seal(&"theirs".to_string()).unwrap();
        l.sink().apply(&Announcement {
            instance: 2,
            locker_id: l.inner.id,
            epoch: 1,
            cleared: false,
            changes: vec![Change {
                key: b"k".to_vec(),
                value: Some(sealed),
                deleted: false,
            }],
        });

        assert_eq!(l.get("k").as_deref(), Some(&"theirs".to_string()));
        assert_eq!(events.try_recv(), Some(Event::Put { key: b"k".to_vec() }));
    }

    /// A write too large to carry drops the resident copy and says so.
    ///
    /// The alternative — keeping the old value — would make an infallible
    /// getter hand back something the store no longer holds, with nothing to
    /// tell the caller. `None` plus [`Event::Stale`] is the honest answer.
    #[test]
    fn a_write_it_cannot_hold_goes_stale_rather_than_lying() {
        let l = locker();
        block_on(l.put("k", "mine".into())).unwrap();
        let mut events = l.watch();

        l.sink().apply(&Announcement {
            instance: 2,
            locker_id: l.inner.id,
            epoch: 1,
            cleared: false,
            changes: vec![Change {
                key: b"k".to_vec(),
                value: None,
                deleted: false,
            }],
        });

        assert_eq!(l.get("k"), None, "a value we cannot decode is not held");
        assert_eq!(events.try_recv(), Some(Event::Stale { key: b"k".to_vec() }));
    }

    #[test]
    fn get_is_synchronous_and_returns_what_was_put() {
        let l = locker();
        block_on(l.put("theme", "dark".into())).unwrap();
        // No await here. That is the feature.
        assert_eq!(l.get("theme").as_deref(), Some(&"dark".to_string()));
    }

    #[test]
    fn a_missing_key_is_none() {
        let l = locker();
        assert!(l.get("nope").is_none());
    }

    #[test]
    fn overwriting_replaces() {
        let l = locker();
        block_on(l.put("k", "first".into())).unwrap();
        block_on(l.put("k", "second".into())).unwrap();
        assert_eq!(l.get("k").as_deref(), Some(&"second".to_string()));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn delete_removes_from_ram_and_storage() {
        let l = locker();
        block_on(l.put("k", "v".into())).unwrap();
        block_on(l.delete("k")).unwrap();
        assert!(l.get("k").is_none());
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn a_transaction_updates_ram_only_after_it_commits() {
        let l = locker();
        block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            tx.put("b", "2".to_string())?;
            Ok(())
        }))
        .unwrap();

        assert_eq!(l.len(), 2);
        assert_eq!(l.get("a").as_deref(), Some(&"1".to_string()));
    }

    #[test]
    fn a_failed_transaction_leaves_the_resident_copy_alone() {
        let l = locker();
        block_on(l.put("existing", "old".into())).unwrap();

        let outcome: Result<()> = block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            Err(Error::backend("nope"))
        }));
        assert!(outcome.is_err());

        assert!(l.get("a").is_none());
        assert_eq!(l.get("existing").as_deref(), Some(&"old".to_string()));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn a_transaction_respects_the_inline_size_limit() {
        // The limit must not be bypassable by routing a write through a
        // transaction instead of put().
        let l = open_with(LockerConfig::default().with_max_inline(64)).unwrap();

        let outcome: Result<()> = block_on(l.transact(|tx| async move {
            tx.put("big", "x".repeat(10_000))?;
            Ok(())
        }));

        assert!(matches!(outcome, Err(Error::ValueTooLarge { .. })));
        assert!(l.get("big").is_none());
    }

    #[test]
    fn an_eager_locker_announces_its_writes() {
        let l = locker();
        let mut events = l.watch();

        block_on(l.put("theme", "dark".into())).unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(crate::watch::Event::Put {
                key: "theme".into()
            })
        );
    }

    #[test]
    fn keys_are_ordered_and_prefix_filtering_stops_at_the_boundary() {
        let l = locker();
        for k in ["ui::b", "ui::a", "uj::a"] {
            block_on(l.put(k, "v".into())).unwrap();
        }
        assert_eq!(l.keys(), vec!["ui::a", "ui::b", "uj::a"]);
        assert_eq!(l.keys_with_prefix("ui::"), vec!["ui::a", "ui::b"]);
    }

    #[test]
    fn an_oversized_value_is_refused_and_names_the_alternative() {
        let l = open_with(LockerConfig::default().with_max_inline(64)).unwrap();
        let big = "x".repeat(10_000);

        match block_on(l.put("big", big)) {
            Err(Error::ValueTooLarge { max_inline, .. }) => {
                assert_eq!(max_inline, 64);
            }
            other => panic!("expected ValueTooLarge, got {other:?}"),
        }
        // And it must not have half-landed.
        assert!(l.get("big").is_none());
    }

    #[test]
    fn an_evictable_policy_is_refused_at_open() {
        // Eviction plus residency would leave a RAM replica surviving the
        // eviction, so the combination is rejected rather than reconciled.
        let config = LockerConfig::default().with_policy(Policy::Evictable { max_bytes: 1024 });
        match open_with(config) {
            Err(Error::InvalidConfig(msg)) => {
                assert!(msg.contains("lazy locker"), "should name the fix: {msg}")
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn reopening_restores_every_value() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let chain = Arc::new(default_chain());

        let first: Locker<String> = block_on(Locker::open(
            backend.clone(),
            chain.clone(),
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        block_on(first.put("a", "alpha".into())).unwrap();
        block_on(first.put("b", "beta".into())).unwrap();
        drop(first);

        let second: Locker<String> = block_on(Locker::open(
            backend,
            chain,
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(second.get("a").as_deref(), Some(&"alpha".to_string()));
    }

    #[test]
    fn opening_over_too_much_data_fails_loudly() {
        // The guardrail against typing locker() where lazy_locker() was meant.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let chain = Arc::new(default_chain());

        let seed: Locker<String> = block_on(Locker::open(
            backend.clone(),
            chain.clone(),
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        for i in 0..50 {
            block_on(seed.put(&format!("k{i:03}"), "x".repeat(200))).unwrap();
        }
        drop(seed);

        let reopened: Result<Locker<String>> = block_on(Locker::open(
            backend,
            chain,
            1,
            "l".into(),
            LockerConfig::default().with_eager_budget(1024),
            Default::default(),
        ));

        match reopened {
            Err(Error::LockerTooLarge { budget, .. }) => assert_eq!(budget, 1024),
            other => panic!("expected LockerTooLarge, got {:?}", other.map(|_| "ok")),
        }
    }

    #[test]
    fn clear_empties_ram_and_storage() {
        let l = locker();
        block_on(l.put("a", "1".into())).unwrap();
        block_on(l.clear()).unwrap();
        assert_eq!(l.len(), 0);
        assert!(l.get("a").is_none());
    }

    #[test]
    fn binary_keys_live_alongside_text_ones() {
        let l = locker();
        block_on(l.put_by(&[0xFF], "high".into())).unwrap();
        block_on(l.put_by(&[0x00], "low".into())).unwrap();
        block_on(l.put("a", "text".into())).unwrap();

        assert_eq!(l.get_by(&[0xFF]).as_deref(), Some(&"high".to_string()));
        assert!(l.contains_key_by(&[0x00]));
        assert_eq!(
            l.keys_bytes(),
            vec![vec![0x00], b"a".to_vec(), vec![0xFF]],
            "keys must be ordered bytewise"
        );
        // The str listing skips what it cannot spell, and admits to it. Note
        // 0x00 IS valid UTF-8 (a NUL string), so it survives the filter.
        assert_eq!(l.keys(), vec!["\u{0}".to_string(), "a".to_string()]);
        assert!(l.has_non_utf8_keys());

        block_on(l.delete_by(&[0xFF])).unwrap();
        assert!(l.get_by(&[0xFF]).is_none());
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn a_locker_of_text_keys_reports_no_binary_ones() {
        let l = locker();
        block_on(l.put("a", "1".into())).unwrap();
        assert!(!l.has_non_utf8_keys());
        assert_eq!(l.keys_bytes(), vec![b"a".to_vec()]);
    }

    #[test]
    fn get_ergonomics_cover_clone_default_and_borrow() {
        let l = locker();
        block_on(l.put("theme", "dark".into())).unwrap();

        assert_eq!(l.get_cloned("theme"), Some("dark".to_string()));
        assert_eq!(l.get_cloned("absent"), None);

        assert_eq!(*l.get_or("theme", "light".into()), "dark".to_string());
        assert_eq!(*l.get_or("absent", "light".into()), "light".to_string());
        // The default is a read-time fallback, never a write.
        assert!(!l.contains_key("absent"));

        assert_eq!(l.with("theme", |v| v.len()), Some(4));
        assert_eq!(l.with("absent", |v| v.len()), None);
    }

    #[test]
    fn put_all_lands_as_one_commit() {
        let l = locker();
        let entries: Vec<(String, String)> = (0..50)
            .map(|i| (format!("k{i:03}"), format!("v{i}")))
            .collect();
        block_on(l.put_all(entries)).unwrap();

        assert_eq!(l.len(), 50);
        assert_eq!(l.get("k049").as_deref(), Some(&"v49".to_string()));

        block_on(l.delete_all(["k000", "k001"])).unwrap();
        assert_eq!(l.len(), 48);
        assert!(l.get("k000").is_none());
    }

    #[test]
    fn a_put_all_carrying_an_oversized_value_writes_nothing() {
        // The atomicity claim, stated as a negative: one bad entry must not
        // leave the good ones behind.
        let l = open_with(LockerConfig::default().with_max_inline(64)).unwrap();
        let entries = vec![
            ("ok".to_string(), "small".to_string()),
            ("bad".to_string(), "x".repeat(10_000)),
        ];

        assert!(matches!(
            block_on(l.put_all(entries)),
            Err(Error::ValueTooLarge { .. })
        ));
        assert_eq!(l.len(), 0, "a refused put_all must write nothing at all");
    }

    #[test]
    fn an_empty_bulk_operation_is_a_no_op() {
        let l = locker();
        block_on(l.put("k", "v".into())).unwrap();
        block_on(l.put_all(Vec::<(String, String)>::new())).unwrap();
        block_on(l.delete_all(Vec::<String>::new())).unwrap();
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn range_and_to_map_read_straight_from_ram() {
        let l = locker();
        for k in ["a", "b", "c", "d"] {
            block_on(l.put(k, k.to_uppercase())).unwrap();
        }

        let keys: Vec<String> = l.range("b".."d").into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["b", "c"]);
        assert_eq!(l.range(..).len(), 4);

        let map = l.to_map();
        assert_eq!(map.len(), 4);
        assert_eq!(map.get("c").map(|v| v.as_str()), Some("C"));
    }

    #[test]
    fn watch_keys_passes_any_named_key_and_every_clear() {
        let l = locker();
        let mut events = l.watch_keys(&["a", "b"]);

        block_on(l.put("ignored", "x".into())).unwrap();
        block_on(l.put("b", "y".into())).unwrap();
        block_on(l.clear()).unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(crate::watch::Event::Put { key: b"b".to_vec() })
        );
        assert_eq!(block_on(events.next()), Some(crate::watch::Event::Cleared));
    }

    #[test]
    fn values_are_shared_not_cloned_per_read() {
        let l = locker();
        let stored = block_on(l.put("k", "v".into())).unwrap();
        let read = l.get("k").unwrap();
        assert!(
            Arc::ptr_eq(&stored, &read),
            "reads should hand back the same Arc, not a fresh clone"
        );
    }
}
