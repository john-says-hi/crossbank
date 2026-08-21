//! The lazy locker: keys resident, values on demand.
//!
//! The Bitcask shape Hive's `LazyBox` uses. Opening reads only the key list, so
//! open cost is proportional to the number of keys rather than the size of the
//! data — which is what makes a multi-gigabyte candle cache openable at all.
//!
//! Because the index is in memory, `contains_key`, `len` and the key listings
//! are synchronous and free. Only the values require a round trip.

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::Backend;
use crate::codec::FilterChain;
use crate::error::{Error, Result};
use crate::key::LockerId;

use super::inner::Inner;
use super::policy::{LockerConfig, OnCorrupt};
use super::resident::{self, Pending, Resident};
use super::transaction::{Staged, Transaction, TxMode};
use crate::watch::Event;

/// A locker that keeps only its key index in memory.
pub struct LazyLocker<T> {
    pub(crate) inner: Arc<Inner>,
    /// The key index, the byte budget and the staged writes, shared with the
    /// coherence sink and with [`crate::Bank::flush_all`] — neither of which
    /// can know `T`.
    res: Arc<Resident>,
    /// Keys whose stored bytes failed to decode on a `get`. See
    /// [`LazyLocker::corrupt_keys`].
    corrupt: Mutex<BTreeSet<Vec<u8>>>,
    /// Keeps this locker's coherence registration alive; the channel holds
    /// only a `Weak`, so dropping the locker unregisters it.
    #[cfg(target_arch = "wasm32")]
    sink: Mutex<Option<crate::coherence::SinkHandle>>,
    _value: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for LazyLocker<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyLocker")
            .field("name", &self.inner.name)
            .field("keys", &self.len())
            .finish()
    }
}

impl<T> LazyLocker<T>
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
            epochs: Default::default(),
        });

        // Keys only. Reading values here would defeat the entire point.
        let mut index = BTreeSet::new();
        inner
            .walk(
                Bound::Unbounded,
                Bound::Unbounded,
                false,
                false,
                |key, _| {
                    index.insert(key);
                    Ok(())
                },
            )
            .await?;

        // Eviction accounting is one more prefix scan, of `meta` this time,
        // and only for a locker that asked for a budget.
        let lru = resident::open_lru(&inner, &index).await?;
        let res = Arc::new(Resident::new(inner.clone(), TxMode::Lazy, Some(index), lru));

        Ok(Self {
            inner,
            res,
            corrupt: Mutex::new(BTreeSet::new()),
            #[cfg(target_arch = "wasm32")]
            sink: Mutex::new(None),
            _value: PhantomData,
        })
    }

    /// Fetch and decode one value.
    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        self.get_by(key.as_bytes()).await
    }

    /// As [`LazyLocker::get`], under a binary key.
    ///
    /// A record that will not decode is **always** an error here, even under
    /// [`OnCorrupt::Skip`]. `Skip` governs *opening*: a lazy locker reads no
    /// values at open, so there is nothing for it to skip, and answering a
    /// direct `get` with `Ok(None)` would be indistinguishable from "that key
    /// was never written" — a lie about data that is still on disk. What
    /// `Skip` does add is bookkeeping: the failing key is recorded in
    /// [`LazyLocker::corrupt_keys`] on the way out.
    pub async fn get_by(&self, key: &[u8]) -> Result<Option<T>> {
        self.inner.ensure_open()?;

        // A staged write is this handle's own work and is visible to it
        // immediately, flushed or not.
        if let Some(view) = self.res.staged_view(key)? {
            return view
                .map(|payload| {
                    postcard::from_bytes(&payload).map_err(|e| {
                        Error::Corrupt(format!("postcard deserialisation failed: {e}"))
                    })
                })
                .transpose();
        }

        let found = self.load_and_note(key).await;
        if matches!(found, Ok(Some(_))) {
            self.res.note_read(key).await?;
        }
        found
    }

    async fn load_and_note(&self, key: &[u8]) -> Result<Option<T>> {
        match self.inner.load_value(key).await {
            Err(e) if self.inner.config.on_corrupt == OnCorrupt::Skip && e.is_corruption() => {
                if let Ok(mut guard) = self.corrupt.lock() {
                    guard.insert(key.to_vec());
                }
                Err(e)
            }
            other => other,
        }
    }

    /// Store one value. Large payloads are split across the `chunks` table.
    pub async fn put(&self, key: &str, value: &T) -> Result<()> {
        self.put_by(key.as_bytes(), value).await
    }

    /// As [`LazyLocker::put`], under a binary key.
    pub async fn put_by(&self, key: &[u8], value: &T) -> Result<()> {
        self.inner.ensure_open()?;
        let _guard = self.inner.write_lock.lock().await;
        let payload = postcard::to_allocvec(value)
            .map_err(|e| Error::Filter(format!("postcard serialisation failed: {e}")))?;
        let bytes = payload.len() as u64;

        if self.res.is_deferred() {
            let full = self.res.stage(Pending::Put {
                key: key.to_vec(),
                payload,
            })?;
            self.res.touch_index(|i| {
                i.insert(key.to_vec());
            });
            self.inner.announce(Event::Put { key: key.to_vec() });
            if full {
                self.res.flush_locked().await?;
            }
            return Ok(());
        }

        let mut ops = self
            .inner
            .put_payload_ops(key, payload, super::chunk::FLAG_POSTCARD)
            .await?;

        let budget = self
            .res
            .budget_ops(
                &mut ops,
                vec![(key.to_vec(), bytes)],
                Vec::new(),
                false,
                std::slice::from_ref(&key.to_vec()),
            )
            .await?;

        self.inner.commit(ops).await?;
        self.res.touch_index(|i| {
            i.insert(key.to_vec());
        });
        self.res.apply_budget(&budget);
        self.inner.announce(Event::Put { key: key.to_vec() });
        Ok(())
    }

    /// Commit everything this locker has staged. See [`crate::Commit`].
    ///
    /// Always safe to call: an immediate-commit locker has nothing staged and
    /// returns at once.
    ///
    /// **A closed locker may still be flushed.** [`LazyLocker::close`] keeps
    /// the batch when its own flush fails, so `pending()` stays honest and the
    /// caller can fix the cause — a full disk, a quota — and retry here. That
    /// is the one operation a closed lazy locker still accepts; every write
    /// reports [`Error::Closed`].
    ///
    /// Under [`crate::Durability::Eventual`] this also forces the backend
    /// fsync, so one `flush` is the whole durability contract regardless of
    /// which of the two knobs the locker turned.
    pub async fn flush(&self) -> Result<()> {
        let staged = self.res.flush().await;
        let forced = self.inner.flush_backend().await;
        staged.and(forced)
    }

    /// How many writes are staged and not yet committed.
    ///
    /// Reads 0 if the staging lock has been poisoned by a panic in another
    /// thread; [`LazyLocker::flush`] reports that as [`Error::Backend`]
    /// rather than silently doing nothing.
    pub fn pending(&self) -> usize {
        self.res.pending_len().unwrap_or(0)
    }

    /// Payload bytes staged and not yet committed. See [`LazyLocker::pending`]
    /// for the poisoned-lock caveat.
    pub fn pending_bytes(&self) -> u64 {
        self.res.pending_bytes().unwrap_or(0)
    }

    /// Remove one key. Removing an absent key is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.delete_by(key.as_bytes()).await
    }

    /// As [`LazyLocker::delete`], under a binary key.
    pub async fn delete_by(&self, key: &[u8]) -> Result<()> {
        self.inner.ensure_open()?;
        let _guard = self.inner.write_lock.lock().await;
        if self.res.is_deferred() {
            let full = self.res.stage(Pending::Delete { key: key.to_vec() })?;
            self.res.touch_index(|i| {
                i.remove(key);
            });
            self.inner.announce(Event::Deleted { key: key.to_vec() });
            if full {
                self.res.flush_locked().await?;
            }
            return Ok(());
        }

        let mut ops = self.inner.delete_value_ops(key).await?;
        let budget = self
            .res
            .budget_ops(&mut ops, Vec::new(), vec![key.to_vec()], false, &[])
            .await?;
        self.inner.commit(ops).await?;
        self.res.touch_index(|i| {
            i.remove(key);
        });
        self.res.apply_budget(&budget);
        self.inner.announce(Event::Deleted { key: key.to_vec() });
        Ok(())
    }

    /// Store many entries in **one** atomic commit. Hive's `putAll`.
    ///
    /// Everything lands together or nothing does. This is also the answer to
    /// bulk writes being slow: one commit rather than N.
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
    /// Removing an absent key is not an error.
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

    /// Every entry, in byte order. UTF-8 keys only.
    ///
    /// Reads every value, so the cost is the whole locker — the opposite of
    /// what a lazy locker is for. Reach for it on small lockers, or on the way
    /// out of one (a migration, an export), not on a hot path.
    pub async fn entries(&self) -> Result<Vec<(String, T)>> {
        self.range(..).await
    }

    /// Every entry as a map, in byte order. Hive's `toMap`.
    ///
    /// UTF-8 keys only: a key that is not valid UTF-8 is **skipped**, since a
    /// `BTreeMap<String, T>` has no way to spell it. Use
    /// [`LazyLocker::range_by`] over an unbounded range to see every key.
    /// Carries the same whole-locker cost as [`LazyLocker::entries`].
    pub async fn to_map(&self) -> Result<std::collections::BTreeMap<String, T>> {
        Ok(self.entries().await?.into_iter().collect())
    }

    /// Remove everything in this locker, and nothing outside it.
    pub async fn clear(&self) -> Result<()> {
        self.inner.ensure_open()?;
        let _guard = self.inner.write_lock.lock().await;
        if self.res.is_deferred() {
            let full = self.res.stage(Pending::Clear)?;
            self.res.touch_index(|i| i.clear());
            self.inner.announce(Event::Cleared);
            if full {
                self.res.flush_locked().await?;
            }
            return Ok(());
        }

        let mut ops = self.inner.clear_value_ops().await?;
        let budget = self
            .res
            .budget_ops(&mut ops, Vec::new(), Vec::new(), true, &[])
            .await?;
        self.inner.commit(ops).await?;
        self.res.touch_index(|i| i.clear());
        self.res.apply_budget(&budget);
        self.inner.announce(Event::Cleared);
        Ok(())
    }

    /// Run a transaction: every staged write lands together, or none does.
    ///
    /// The closure form is deliberate. Staging in memory means no backend
    /// transaction is open while your code runs, which is what makes this safe
    /// on IndexedDB — see the module docs on [`Transaction`].
    ///
    /// Returning `Err` rolls back: nothing is written and the index is
    /// untouched.
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

        // Absorb anything this handle has staged under `Commit::Deferred`.
        // Both batches have to ride in one commit, staged first, or a later
        // flush would write the older staged value over this transaction's
        // newer one. `ops_for_pending` collapses the merged list, so the
        // transaction is the last writer for any key both touch.
        let staged_before = self.res.take_staged()?;
        let merged = merge_batches(&staged_before, &entries);

        let mut ops = match super::transaction::ops_for_pending(&self.inner, &merged, TxMode::Lazy).await {
            Ok(ops) => ops,
            Err(e) => {
                self.res.restage(staged_before);
                return Err(e);
            }
        };

        // One transaction is one moment: every key it wrote is equally recent,
        // so they share a tick rather than being ordered by staging order.
        let (updates, removals, cleared) = resident::accounting(&merged);
        // Never evict a key this same commit is writing.
        let keep: Vec<Vec<u8>> = updates.iter().map(|(k, _)| k.clone()).collect();
        let budget = match self
            .res
            .budget_ops(&mut ops, updates, removals, cleared, &keep)
            .await
        {
            Ok(budget) => budget,
            Err(e) => {
                self.res.restage(staged_before);
                return Err(e);
            }
        };

        if let Err(e) = self.inner.commit(ops).await {
            self.res.restage(staged_before);
            return Err(e);
        }

        // Index updates only after the commit lands, so a failed write cannot
        // leave the index claiming keys that were never stored.
        self.res.touch_index(|i| {
            for entry in &entries {
                match entry {
                    Staged::Put { key, .. } => {
                        i.insert(key.clone());
                    }
                    Staged::Delete { key } => {
                        i.remove(key);
                    }
                    Staged::Clear => i.clear(),
                }
            }
        });
        self.res.apply_budget(&budget);

        for entry in &entries {
            match entry {
                Staged::Put { key, .. } => self.inner.announce(Event::Put { key: key.clone() }),
                Staged::Delete { key } => self.inner.announce(Event::Deleted { key: key.clone() }),
                Staged::Clear => self.inner.announce(Event::Cleared),
            }
        }
        Ok(())
    }

    /// Values over a key range, in byte order.
    ///
    /// UTF-8 keys only — see [`LazyLocker::range_by`] for the binary form.
    pub async fn range<'a, R: RangeBounds<&'a str>>(&self, range: R) -> Result<Vec<(String, T)>> {
        Ok(utf8_entries(
            self.collect(
                crate::key::as_bytes(deref_bound(range.start_bound())),
                crate::key::as_bytes(deref_bound(range.end_bound())),
                false,
                None,
            )
            .await?,
        ))
    }

    /// As [`LazyLocker::range`], over binary bounds, yielding raw key bytes.
    pub async fn range_by<'a, R: RangeBounds<&'a [u8]>>(
        &self,
        range: R,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        self.collect(
            deref_bound_bytes(range.start_bound()),
            deref_bound_bytes(range.end_bound()),
            false,
            None,
        )
        .await
    }

    /// As [`LazyLocker::range`], descending.
    pub async fn range_rev<'a, R: RangeBounds<&'a str>>(
        &self,
        range: R,
    ) -> Result<Vec<(String, T)>> {
        Ok(utf8_entries(
            self.collect(
                crate::key::as_bytes(deref_bound(range.start_bound())),
                crate::key::as_bytes(deref_bound(range.end_bound())),
                true,
                None,
            )
            .await?,
        ))
    }

    /// As [`LazyLocker::range_by`], descending.
    pub async fn range_rev_by<'a, R: RangeBounds<&'a [u8]>>(
        &self,
        range: R,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        self.collect(
            deref_bound_bytes(range.start_bound()),
            deref_bound_bytes(range.end_bound()),
            true,
            None,
        )
        .await
    }

    /// The first `limit` entries of a descending scan — "the latest N".
    pub async fn latest(&self, limit: usize) -> Result<Vec<(String, T)>> {
        Ok(utf8_entries(
            self.collect(Bound::Unbounded, Bound::Unbounded, true, Some(limit))
                .await?,
        ))
    }

    /// The shared body of every listing: walk storage, fold in whatever this
    /// handle has staged but not committed, and decode.
    ///
    /// The overlay is not optional. `get`, `len`, `keys` and `contains_key`
    /// all show a staged write to its own handle, so a listing that walked
    /// storage alone would have one locker telling two different stories about
    /// its own uncommitted writes.
    async fn collect(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        let staged = self.res.staged_snapshot()?;

        // Only a locker with nothing staged can apply the limit during the
        // walk: otherwise a staged key could belong inside a truncation the
        // walk has already made.
        let cap = match staged.is_empty() {
            true => limit.unwrap_or(usize::MAX),
            false => usize::MAX,
        };

        // Decode outside the visitor so a decode failure surfaces as an error
        // rather than being swallowed mid-walk.
        let mut stored: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.inner
            .walk(start, end, reverse, true, |key, value| {
                if stored.len() >= cap {
                    return Ok(());
                }
                let bytes = value.ok_or_else(|| {
                    Error::Corrupt(format!("backend omitted a value for key {key:?}"))
                })?;
                stored.push((key, bytes));
                Ok(())
            })
            .await?;

        if staged.is_empty() {
            let mut out = Vec::with_capacity(stored.len());
            for (key, bytes) in stored {
                out.push((key, self.inner.decode_record(&bytes).await?));
            }
            return Ok(out);
        }

        // A staged clear wipes everything before it: nothing in storage
        // survives it, only the writes staged after it.
        let after_clear = match staged.iter().rposition(|e| matches!(e, Pending::Clear)) {
            Some(i) => {
                stored.clear();
                &staged[i + 1..]
            }
            None => &staged[..],
        };

        // Later staged entries win, exactly as `staged_view` resolves them.
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Source>> = stored
            .into_iter()
            .map(|(key, bytes)| (key, Some(Source::Stored(bytes))))
            .collect();
        for entry in after_clear {
            match entry {
                Pending::Put { key, payload } => {
                    if in_bounds(key, start, end) {
                        merged.insert(key.clone(), Some(Source::Staged(payload.clone())));
                    }
                }
                Pending::Delete { key } => {
                    merged.remove(key.as_slice());
                }
                // None can remain: `after_clear` starts past the last one.
                Pending::Clear => {}
            }
        }

        let mut items: Vec<(Vec<u8>, Source)> = merged
            .into_iter()
            .filter_map(|(key, source)| source.map(|s| (key, s)))
            .collect();
        // `BTreeMap` already gives byte order; a reverse listing is that
        // reversed, and only then is the limit applied.
        if reverse {
            items.reverse();
        }
        items.truncate(limit.unwrap_or(usize::MAX));

        let mut out = Vec::with_capacity(items.len());
        for (key, source) in items {
            let value = match source {
                Source::Stored(bytes) => self.inner.decode_record(&bytes).await?,
                Source::Staged(payload) => postcard::from_bytes(&payload)
                    .map_err(|e| Error::Corrupt(format!("postcard deserialisation failed: {e}")))?,
            };
            out.push((key, value));
        }
        Ok(out)
    }
}

/// Where one entry of a listing came from. A stored record is a sealed
/// envelope; a staged one is the bare postcard payload, so they decode
/// differently.
enum Source {
    Stored(Vec<u8>),
    Staged(Vec<u8>),
}

/// Whether a staged key belongs in the range being listed.
///
/// The walk applies the bounds itself; a staged write has never been near the
/// backend, so it has to be filtered here or a `range("b".."d")` would start
/// reporting keys outside it.
fn in_bounds(key: &[u8], start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    let above = match start {
        Bound::Unbounded => true,
        Bound::Included(s) => key >= s,
        Bound::Excluded(s) => key > s,
    };
    let below = match end {
        Bound::Unbounded => true,
        Bound::Included(e) => key <= e,
        Bound::Excluded(e) => key < e,
    };
    above && below
}

/// Bounds-free accessors. Split out so `Debug` — and any caller holding a
/// `LazyLocker<T>` for a `T` that is not itself serialisable — can still ask
/// about the index.
/// Applies another tab's news to one open lazy locker.
///
/// Holds the index directly rather than the locker, so a `LazyLocker<T>` for
/// any `T` at all is served by one non-generic sink — the index is keys, and
/// keys have no type.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub(crate) struct LazySink {
    inner: Arc<Inner>,
    res: Arc<Resident>,
}

#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
impl crate::coherence::Sink for LazySink {
    fn locker_id(&self) -> LockerId {
        self.inner.id
    }

    fn apply(&self, announcement: &crate::coherence::Announcement) {
        if self.inner.is_closed() {
            return;
        }
        if announcement.cleared {
            self.res.touch_index(|index| index.clear());
            // The byte budget has to follow the index, or this tab's
            // accounting drifts from what storage holds and it starts evicting
            // against a total that describes a locker that no longer exists.
            self.res.remote_budget_clear();
            self.inner.epochs.forget_local();
            self.inner.announce(Event::Cleared);
        }
        for change in &announcement.changes {
            self.res.touch_index(|index| {
                if change.deleted {
                    index.remove(change.key.as_slice());
                } else {
                    index.insert(change.key.clone());
                }
            });
            // The size, where it can be had without awaiting: an inlined value
            // is opened through this tab's own filter chain, and a chunked one
            // announced its payload length. Anything else marks the accounting
            // dirty, and the next commit reloads it.
            let bytes = match (&change.value, change.bytes) {
                (Some(sealed), _) => self.inner.chain.open(sealed).ok().map(|p| p.len() as u64),
                (None, announced) => announced,
            };
            self.res.remote_budget(&change.key, bytes, change.deleted);

            // A lazy locker never holds values, so a write it could not carry
            // inline costs it nothing: the next `get` reads the new bytes.
            if change.deleted {
                self.inner.announce(Event::Deleted {
                    key: change.key.clone(),
                });
            } else {
                self.inner.announce(Event::Put {
                    key: change.key.clone(),
                });
            }
        }
    }
}

impl<T> LazyLocker<T> {
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn sink(&self) -> LazySink {
        LazySink {
            inner: self.inner.clone(),
            res: self.res.clone(),
        }
    }

    /// Start receiving other tabs' news, where there are other tabs.
    ///
    /// Native has no second process to hear from — `redb`'s exclusive file
    /// lock sees to that — so this is a no-op there rather than a `cfg` in the
    /// caller.
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

    /// Payload bytes this locker is accounted as holding, or 0 when it has no
    /// budget to hold them against.
    ///
    /// Counts the values as the caller handed them over, not their on-disk
    /// footprint — an on-disk figure would be both backend-dependent (the
    /// filter chain compresses) and unstable (chunk framing adds to it).
    pub fn budget_used(&self) -> u64 {
        self.res.budget_used()
    }

    /// Shed least-recently-used keys until at most `bytes` remain accounted.
    ///
    /// Returns how many keys went. A `Precious` locker sheds nothing and
    /// returns 0: this never turns a locker that refuses eviction into one
    /// that performs it.
    ///
    /// Each shed key raises [`crate::Event::Evicted`], and the data is gone —
    /// this is the same deletion the budget performs on its own, asked for
    /// early.
    pub async fn evict_to(&self, bytes: u64) -> Result<usize> {
        self.inner.ensure_open()?;
        self.res.evict_to(bytes).await
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    pub(crate) fn resident(&self) -> &Arc<Resident> {
        &self.res
    }

    /// Close this locker: drop its resident key index and refuse further use.
    ///
    /// The **bank's** backend is left open — closing one locker must not take
    /// the store down with it. Use [`crate::Bank::close`] for that.
    ///
    /// After this the locker reads as empty: [`LazyLocker::len`] is 0 and
    /// [`LazyLocker::contains_key`] is false for every key.
    /// [`LazyLocker::is_closed`] is what distinguishes a closed locker from an
    /// empty one. `get`, `put`, `delete`, `clear` and `transact` report
    /// [`Error::Closed`].
    ///
    /// Idempotent, and it does not delete anything — reopening the same name
    /// from the bank finds the data untouched.
    pub async fn close(&self) -> Result<()> {
        // Flush before closing, and close even if the flush fails: a caller
        // who is shutting down must not be left holding an open locker just
        // because its last batch would not land.
        let flushed = self.res.flush().await;
        // Only discard once the batch is actually stored. A failed flush has
        // written nothing, and this staging buffer is the only copy of it.
        if flushed.is_ok() {
            self.res.discard_staged();
        }
        // Separate from the staged flush on purpose: the discard above must
        // key off whether the *batch* landed, not off whether a later fsync
        // succeeded, or a retry would write it twice.
        let forced = self.inner.flush_backend().await;
        self.inner.mark_closed();
        self.res.touch_index(|index| index.clear());
        flushed.and(forced)
    }

    /// Whether [`LazyLocker::close`] has been called.
    ///
    /// The only way to tell a closed locker from an empty one, since a closed
    /// locker reads as empty by design.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Keys this locker is known to hold unreadable bytes for.
    ///
    /// A lazy locker reads no values at open, so — unlike
    /// [`crate::Locker::corrupt_keys`] — this starts empty and fills in as
    /// reads discover damage, and only when the locker was configured with
    /// [`OnCorrupt::Skip`]. It is therefore a record of what *has been hit*,
    /// not a survey. [`crate::Bank::verify`] is the survey.
    pub fn corrupt_keys(&self) -> Vec<Vec<u8>> {
        self.corrupt
            .lock()
            .map(|c| c.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Number of keys. Synchronous — the index is already here.
    pub fn len(&self) -> usize {
        self.res.read_index(|i| i.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Synchronous: answered from the index without touching storage.
    pub fn contains_key(&self, key: &str) -> bool {
        self.contains_key_by(key.as_bytes())
    }

    /// As [`LazyLocker::contains_key`], under a binary key.
    pub fn contains_key_by(&self, key: &[u8]) -> bool {
        self.res.read_index(|i| i.contains(key)).unwrap_or(false)
    }

    /// Every UTF-8 key, in byte order.
    ///
    /// Keys that are not valid UTF-8 are **skipped**, not reported as an
    /// error — a listing must never fail because of a key it cannot spell.
    /// [`LazyLocker::has_non_utf8_keys`] says whether any were skipped, and
    /// [`LazyLocker::keys_bytes`] returns every key regardless.
    pub fn keys(&self) -> Vec<String> {
        self.res
            .read_index(|i| i.iter().filter_map(|k| utf8(k)).collect())
            .unwrap_or_default()
    }

    /// Every key as raw bytes, in byte order. Includes non-UTF-8 keys.
    pub fn keys_bytes(&self) -> Vec<Vec<u8>> {
        self.res
            .read_index(|i| i.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether this locker holds any key that [`LazyLocker::keys`] cannot
    /// return. Never panics and never errors.
    pub fn has_non_utf8_keys(&self) -> bool {
        self.res
            .read_index(|i| i.iter().any(|k| std::str::from_utf8(k).is_err()))
            .unwrap_or(false)
    }

    /// Keys beginning with `prefix`, in byte order. UTF-8 keys only.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.keys_with_prefix_by(prefix.as_bytes())
            .iter()
            .filter_map(|k| utf8(k))
            .collect()
    }

    /// As [`LazyLocker::keys_with_prefix`], over a binary prefix.
    pub fn keys_with_prefix_by(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        self.res
            .read_index(|i| {
                i.range(prefix.to_vec()..)
                    .take_while(|k| k.starts_with(prefix))
                    .cloned()
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
}

/// `Range<&str>` cannot implement `RangeBounds<str>` because `str` is unsized,
/// so the bound is over `&str` and this unwraps the extra reference.
fn deref_bound<'a>(bound: Bound<&&'a str>) -> Bound<&'a str> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included(s),
        Bound::Excluded(s) => Bound::Excluded(s),
    }
}

/// The `&[u8]` twin of [`deref_bound`].
fn deref_bound_bytes<'a>(bound: Bound<&&'a [u8]>) -> Bound<&'a [u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(s) => Bound::Included(s),
        Bound::Excluded(s) => Bound::Excluded(s),
    }
}

/// A key as a `String`, or `None` when it is not UTF-8.
pub(crate) fn utf8(key: &[u8]) -> Option<String> {
    std::str::from_utf8(key).ok().map(str::to_string)
}

/// One deferred batch followed by one transaction's write-set, as a single
/// ordered list of mutations.
///
/// Order is what makes this correct: the staged writes are older, so they go
/// first and the transaction's writes overwrite them wherever both name the
/// same key.
pub(crate) fn merge_batches<T>(staged: &[Pending], tx: &[Staged<T>]) -> Vec<Pending> {
    let mut merged: Vec<Pending> = staged.to_vec();
    merged.extend(tx.iter().map(|entry| match entry {
        Staged::Put { key, payload, .. } => Pending::Put {
            key: key.clone(),
            payload: payload.clone(),
        },
        Staged::Delete { key } => Pending::Delete { key: key.clone() },
        Staged::Clear => Pending::Clear,
    }));
    merged
}

/// Drop the entries whose keys are not UTF-8. Used by the `&str` listing
/// methods, which cannot spell a binary key.
fn utf8_entries<T>(entries: Vec<(Vec<u8>, T)>) -> Vec<(String, T)> {
    entries
        .into_iter()
        .filter_map(|(k, v)| utf8(&k).map(|k| (k, v)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coherence::{Announcement, Sink};

    fn news(id: LockerId, cleared: bool, changes: Vec<(&[u8], bool)>) -> Announcement {
        Announcement {
            instance: 1,
            locker_id: id,
            epoch: 1,
            cleared,
            changes: changes
                .into_iter()
                .map(|(key, deleted)| crate::coherence::api::Change {
                    key: key.to_vec(),
                    value: None,
                    bytes: None,
                    deleted,
                })
                .collect(),
        }
    }
    use crate::backend::MemoryBackend;
    use crate::codec::default_chain;
    use crate::watch::Event;
    use futures::executor::block_on;
    use futures::StreamExt;

    fn locker() -> LazyLocker<String> {
        block_on(LazyLocker::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "test".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap()
    }

    fn seeded(entries: &[(&str, &str)]) -> LazyLocker<String> {
        let l = locker();
        for (k, v) in entries {
            block_on(l.put(k, &v.to_string())).unwrap();
        }
        l
    }

    /// Another tab's write appears in this tab's index, and is announced.
    ///
    /// A lazy locker holds no values, so it does not care whether the message
    /// carried the bytes: the key is there and the next `get` reads it.
    #[test]
    fn a_sink_folds_another_tabs_news_into_the_index() {
        let l = seeded(&[("a", "alpha")]);
        let mut events = l.watch();
        let sink = l.sink();

        sink.apply(&news(l.inner.id, false, vec![(b"b", false), (b"a", true)]));

        assert!(l.contains_key("b"), "another tab's write must be visible");
        assert!(!l.contains_key("a"), "another tab's delete must be visible");
        assert_eq!(events.try_recv(), Some(Event::Put { key: b"b".to_vec() }));
        assert_eq!(
            events.try_recv(),
            Some(Event::Deleted { key: b"a".to_vec() })
        );

        sink.apply(&news(l.inner.id, true, Vec::new()));
        assert_eq!(l.len(), 0);
        assert_eq!(events.try_recv(), Some(Event::Cleared));
    }

    /// Another tab's news must move the byte budget, not just the index.
    ///
    /// It did not: a lazy locker folded a remote write into its key index and
    /// left its LRU accounting untouched, so two tabs' budgets drifted apart
    /// and each evicted against a total that described neither of them.
    #[test]
    fn a_sink_keeps_the_byte_budget_in_step_with_another_tab() {
        let l = block_on(LazyLocker::<Vec<u8>>::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "budget".into(),
            LockerConfig::default().with_policy(crate::Policy::Evictable { max_bytes: 10_000 }),
            Default::default(),
        ))
        .expect("open");
        block_on(l.put("mine", &vec![1u8; 100])).expect("put");
        let before = l.budget_used();
        assert!(before > 0);

        let sink = l.sink();
        let id = l.inner.id;

        // An inlined value: the receiver has the bytes and can size it exactly
        // by opening them through its own filter chain.
        let payload = postcard::to_allocvec(&vec![9u8; 500]).expect("encode");
        let sealed = l.inner.chain.seal(&payload).expect("seal");
        sink.apply(&Announcement {
            instance: 2,
            locker_id: id,
            epoch: 1,
            cleared: false,
            changes: vec![crate::coherence::api::Change {
                key: b"theirs".to_vec(),
                value: Some(sealed),
                bytes: None,
                deleted: false,
            }],
        });
        assert_eq!(
            l.budget_used(),
            before + payload.len() as u64,
            "a remote write must be accounted for"
        );

        // A remote delete gives the bytes back.
        sink.apply(&Announcement {
            instance: 2,
            locker_id: id,
            epoch: 2,
            cleared: false,
            changes: vec![crate::coherence::api::Change {
                key: b"theirs".to_vec(),
                value: None,
                bytes: None,
                deleted: true,
            }],
        });
        assert_eq!(l.budget_used(), before, "a remote delete must give it back");

        // A write whose size cannot be worked out marks the accounting dirty
        // rather than inventing a number.
        sink.apply(&Announcement {
            instance: 2,
            locker_id: id,
            epoch: 3,
            cleared: false,
            changes: vec![crate::coherence::api::Change {
                key: b"unknown".to_vec(),
                value: None,
                bytes: None,
                deleted: false,
            }],
        });
        assert!(l.contains_key("unknown"));

        // And a remote clear zeroes it.
        sink.apply(&news(id, true, Vec::new()));
        assert_eq!(l.budget_used(), 0, "a remote clear must zero the budget");
    }

    /// A chunked remote write states its payload length, so it is accounted
    /// for exactly even though its bytes could not ride along.
    #[test]
    fn a_sink_accounts_for_a_chunked_remote_write_from_its_pointer() {
        let l = block_on(LazyLocker::<Vec<u8>>::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "budget".into(),
            LockerConfig::default().with_policy(crate::Policy::Evictable { max_bytes: 10_000 }),
            Default::default(),
        ))
        .expect("open");
        l.sink().apply(&Announcement {
            instance: 2,
            locker_id: l.inner.id,
            epoch: 1,
            cleared: false,
            changes: vec![crate::coherence::api::Change {
                key: b"big".to_vec(),
                value: None,
                bytes: Some(4_242),
                deleted: false,
            }],
        });
        assert_eq!(l.budget_used(), 4_242);
    }

    /// A closed locker absorbs nothing. Its index is deliberately empty, and
    /// repopulating it from a message would make `is_closed` a lie.
    #[test]
    fn a_closed_locker_ignores_the_channel() {
        let l = seeded(&[("a", "alpha")]);
        let sink = l.sink();
        block_on(l.close()).unwrap();
        sink.apply(&news(l.inner.id, false, vec![(b"b", false)]));
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn put_then_get_round_trips() {
        let l = seeded(&[("a", "alpha")]);
        assert_eq!(block_on(l.get("a")).unwrap(), Some("alpha".into()));
    }

    #[test]
    fn a_missing_key_is_none_not_an_error() {
        let l = locker();
        assert_eq!(block_on(l.get("nope")).unwrap(), None);
    }

    #[test]
    fn an_empty_value_is_distinguishable_from_a_missing_key() {
        let l = locker();
        block_on(l.put("empty", &String::new())).unwrap();
        assert_eq!(block_on(l.get("empty")).unwrap(), Some(String::new()));
        assert_eq!(block_on(l.get("absent")).unwrap(), None);
    }

    #[test]
    fn overwriting_replaces_rather_than_appends() {
        let l = seeded(&[("k", "first")]);
        block_on(l.put("k", &"second".to_string())).unwrap();
        assert_eq!(block_on(l.get("k")).unwrap(), Some("second".into()));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn the_index_tracks_writes_and_deletes() {
        let l = seeded(&[("a", "1"), ("b", "2")]);
        assert_eq!(l.len(), 2);
        assert!(l.contains_key("a"));

        block_on(l.delete("a")).unwrap();
        assert!(!l.contains_key("a"));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn deleting_an_absent_key_is_not_an_error() {
        let l = locker();
        block_on(l.delete("ghost")).unwrap();
    }

    #[test]
    fn a_transaction_commits_every_write_together() {
        let l = locker();
        block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            tx.put("b", "2".to_string())?;
            tx.put("manifest", "done".to_string())?;
            Ok(())
        }))
        .unwrap();

        assert_eq!(l.len(), 3);
        assert_eq!(block_on(l.get("manifest")).unwrap(), Some("done".into()));
    }

    #[test]
    fn a_failed_transaction_writes_nothing() {
        // The property that makes chunked writes safe: a crash or error partway
        // through must leave the previous state intact, not a half-written one.
        let l = seeded(&[("existing", "old")]);

        let outcome: Result<()> = block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            tx.put("b", "2".to_string())?;
            Err(Error::backend("something went wrong"))
        }));
        assert!(outcome.is_err());

        assert_eq!(block_on(l.get("a")).unwrap(), None);
        assert_eq!(block_on(l.get("b")).unwrap(), None);
        assert_eq!(block_on(l.get("existing")).unwrap(), Some("old".into()));
        assert_eq!(l.len(), 1, "the index must not have been touched either");
    }

    #[test]
    fn a_transaction_reads_its_own_writes() {
        let l = seeded(&[("k", "stored")]);
        block_on(l.transact(|tx| async move {
            assert_eq!(tx.get("k").await?, Some("stored".to_string()));
            tx.put("k", "staged".to_string())?;
            assert_eq!(tx.get("k").await?, Some("staged".to_string()));
            tx.delete("k")?;
            assert_eq!(tx.get("k").await?, None);
            Ok(())
        }))
        .unwrap();

        assert_eq!(block_on(l.get("k")).unwrap(), None);
    }

    #[test]
    fn later_staged_writes_win_over_earlier_ones() {
        let l = locker();
        block_on(l.transact(|tx| async move {
            tx.put("k", "first".to_string())?;
            tx.put("k", "second".to_string())?;
            Ok(())
        }))
        .unwrap();
        assert_eq!(block_on(l.get("k")).unwrap(), Some("second".into()));
    }

    #[test]
    fn an_empty_transaction_is_a_no_op() {
        let l = seeded(&[("k", "v")]);
        block_on(l.transact(|_tx| async move { Ok(()) })).unwrap();
        assert_eq!(block_on(l.get("k")).unwrap(), Some("v".into()));
    }

    #[test]
    fn a_staged_clear_hides_earlier_keys_from_reads() {
        let l = seeded(&[("a", "1")]);
        block_on(l.transact(|tx| async move {
            tx.clear()?;
            assert_eq!(tx.get("a").await?, None);
            tx.put("fresh", "new".to_string())?;
            Ok(())
        }))
        .unwrap();

        assert_eq!(block_on(l.get("a")).unwrap(), None);
        assert_eq!(block_on(l.get("fresh")).unwrap(), Some("new".into()));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn writes_and_deletes_are_announced_to_watchers() {
        let l = locker();
        let mut events = l.watch();

        block_on(l.put("k", &"v".to_string())).unwrap();
        block_on(l.delete("k")).unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(Event::Put { key: "k".into() })
        );
        assert_eq!(
            block_on(events.next()),
            Some(Event::Deleted { key: "k".into() })
        );
    }

    #[test]
    fn clearing_announces_one_event_not_one_per_key() {
        // A clear over a hundred thousand keys must not flood subscribers.
        let l = seeded(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let mut events = l.watch();

        block_on(l.clear()).unwrap();

        assert_eq!(block_on(events.next()), Some(Event::Cleared));
        assert!(
            events.try_recv().is_none(),
            "clear must produce exactly one event"
        );
    }

    #[test]
    fn a_key_watcher_ignores_other_keys() {
        let l = locker();
        let mut events = l.watch_key("wanted");

        block_on(l.put("ignored", &"x".to_string())).unwrap();
        block_on(l.put("wanted", &"y".to_string())).unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(Event::Put {
                key: "wanted".into()
            })
        );
    }

    #[test]
    fn a_rolled_back_transaction_announces_nothing() {
        // Subscribers must never be told about a write that did not happen.
        let l = locker();
        let mut events = l.watch();

        let outcome: Result<()> = block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            Err(Error::backend("nope"))
        }));
        assert!(outcome.is_err());

        assert!(events.try_recv().is_none(), "rollback must be silent");
    }

    #[test]
    fn a_committed_transaction_announces_every_change() {
        let l = locker();
        let mut events = l.watch();

        block_on(l.transact(|tx| async move {
            tx.put("a", "1".to_string())?;
            tx.put("b", "2".to_string())?;
            Ok(())
        }))
        .unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(Event::Put { key: "a".into() })
        );
        assert_eq!(
            block_on(events.next()),
            Some(Event::Put { key: "b".into() })
        );
    }

    #[test]
    fn keys_come_back_in_byte_order() {
        let l = seeded(&[("c", "3"), ("a", "1"), ("b", "2")]);
        assert_eq!(l.keys(), vec!["a", "b", "c"]);
    }

    #[test]
    fn prefix_listing_stops_at_the_prefix_boundary() {
        let l = seeded(&[
            ("BTCUSDT::1", "x"),
            ("BTCUSDT::2", "y"),
            ("BTCUSDU::1", "z"),
            ("ETHUSDT::1", "w"),
        ]);
        assert_eq!(
            l.keys_with_prefix("BTCUSDT::"),
            vec!["BTCUSDT::1", "BTCUSDT::2"]
        );
    }

    #[test]
    fn range_is_inclusive_start_exclusive_end() {
        let l = seeded(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        let got = block_on(l.range("b".."d")).unwrap();
        assert_eq!(
            got,
            vec![
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string())
            ]
        );
    }

    #[test]
    fn an_unbounded_range_returns_everything() {
        let l = seeded(&[("a", "1"), ("b", "2")]);
        assert_eq!(block_on(l.range(..)).unwrap().len(), 2);
    }

    #[test]
    fn reverse_range_descends() {
        let l = seeded(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let keys: Vec<_> = block_on(l.range_rev(..))
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["c", "b", "a"]);
    }

    #[test]
    fn latest_takes_from_the_top() {
        let l = seeded(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        let keys: Vec<_> = block_on(l.latest(2))
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["d", "c"]);
    }

    #[test]
    fn clear_empties_the_locker_and_its_index() {
        let l = seeded(&[("a", "1"), ("b", "2")]);
        block_on(l.clear()).unwrap();
        assert_eq!(l.len(), 0);
        assert_eq!(block_on(l.get("a")).unwrap(), None);
    }

    #[test]
    fn clear_does_not_touch_a_neighbouring_locker() {
        // The failure this guards: a DeleteRange whose upper bound leaks past
        // the locker prefix and wipes the next locker's data.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let chain = Arc::new(default_chain());

        let one: LazyLocker<String> = block_on(LazyLocker::open(
            backend.clone(),
            chain.clone(),
            1,
            "one".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        let two: LazyLocker<String> = block_on(LazyLocker::open(
            backend.clone(),
            chain,
            2,
            "two".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();

        block_on(one.put("k", &"one".to_string())).unwrap();
        block_on(two.put("k", &"two".to_string())).unwrap();

        block_on(one.clear()).unwrap();

        assert_eq!(block_on(one.get("k")).unwrap(), None);
        assert_eq!(
            block_on(two.get("k")).unwrap(),
            Some("two".into()),
            "clearing one locker must not disturb another"
        );
    }

    #[test]
    fn reopening_rebuilds_the_index_from_storage() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let chain = Arc::new(default_chain());

        let first: LazyLocker<String> = block_on(LazyLocker::open(
            backend.clone(),
            chain.clone(),
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        for i in 0..5 {
            block_on(first.put(&format!("k{i}"), &format!("v{i}"))).unwrap();
        }
        drop(first);

        let second: LazyLocker<String> = block_on(LazyLocker::open(
            backend,
            chain,
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(second.len(), 5);
        assert_eq!(block_on(second.get("k3")).unwrap(), Some("v3".into()));
    }

    #[test]
    fn opening_pages_past_a_single_scan_page() {
        // SCAN_PAGE is 256; prove the paging loop in walk() actually continues
        // rather than silently truncating the index.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let chain = Arc::new(default_chain());

        let first: LazyLocker<String> = block_on(LazyLocker::open(
            backend.clone(),
            chain.clone(),
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        for i in 0..600 {
            block_on(first.put(&format!("k{i:04}"), &"v".to_string())).unwrap();
        }
        drop(first);

        let second: LazyLocker<String> = block_on(LazyLocker::open(
            backend,
            chain,
            1,
            "l".into(),
            LockerConfig::default(),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(second.len(), 600);
    }

    #[test]
    fn a_range_spanning_many_pages_returns_every_entry() {
        let l = locker();
        for i in 0..600 {
            block_on(l.put(&format!("k{i:04}"), &"v".to_string())).unwrap();
        }
        assert_eq!(block_on(l.range(..)).unwrap().len(), 600);
    }

    #[test]
    fn binary_keys_round_trip_and_are_listed_separately() {
        let l = locker();
        block_on(l.put_by(&[0xFF], &"high".to_string())).unwrap();
        block_on(l.put_by(&[0x80, 0x01], &"mid".to_string())).unwrap();
        block_on(l.put("a", &"text".to_string())).unwrap();

        assert_eq!(block_on(l.get_by(&[0xFF])).unwrap(), Some("high".into()));
        assert!(l.contains_key_by(&[0x80, 0x01]));
        assert_eq!(
            l.keys_bytes(),
            vec![b"a".to_vec(), vec![0x80, 0x01], vec![0xFF]]
        );
        assert_eq!(l.keys(), vec!["a".to_string()]);
        assert!(l.has_non_utf8_keys());

        block_on(l.delete_by(&[0xFF])).unwrap();
        assert!(!l.contains_key_by(&[0xFF]));
    }

    #[test]
    fn a_binary_range_yields_raw_key_bytes() {
        let l = locker();
        for k in [&[0x00u8][..], &[0x80][..], &[0xFF][..]] {
            block_on(l.put_by(k, &"v".to_string())).unwrap();
        }
        let low: &[u8] = &[0x00];
        let high: &[u8] = &[0xFF];
        let got = block_on(l.range_by(low..high)).unwrap();
        let keys: Vec<Vec<u8>> = got.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![vec![0x00], vec![0x80]]);

        // The str range refuses to invent names for binary keys. 0x00 is a
        // legal UTF-8 NUL string, so it is the only one that survives.
        let named: Vec<String> = block_on(l.range(..))
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(named, vec!["\u{0}".to_string()]);
    }

    #[test]
    fn a_binary_key_watcher_sees_bytes_not_a_string() {
        let l = locker();
        let mut events = l.watch();
        block_on(l.put_by(&[0xFF], &"v".to_string())).unwrap();

        let event = block_on(events.next()).unwrap();
        assert_eq!(event.key_bytes(), &[0xFF]);
        assert_eq!(event.key(), None, "a binary key has no UTF-8 view");
    }

    #[test]
    fn put_all_and_delete_all_are_single_commits() {
        let l = locker();
        let entries: Vec<(String, String)> = (0..50)
            .map(|i| (format!("k{i:03}"), format!("v{i}")))
            .collect();
        block_on(l.put_all(entries)).unwrap();

        assert_eq!(l.len(), 50);
        assert_eq!(block_on(l.get("k049")).unwrap(), Some("v49".into()));

        block_on(l.delete_all(["k000", "k001", "never_written"])).unwrap();
        assert_eq!(l.len(), 48);
        assert_eq!(block_on(l.get("k000")).unwrap(), None);
    }

    #[test]
    fn entries_and_to_map_match_key_by_key_reads() {
        let l = seeded(&[("a", "1"), ("b", "2"), ("c", "3")]);

        let entries = block_on(l.entries()).unwrap();
        assert_eq!(entries.len(), 3);

        let map = block_on(l.to_map()).unwrap();
        for key in l.keys() {
            assert_eq!(map.get(&key), block_on(l.get(&key)).unwrap().as_ref());
        }
    }

    #[test]
    fn to_map_skips_a_key_it_cannot_spell() {
        let l = locker();
        block_on(l.put("a", &"1".to_string())).unwrap();
        block_on(l.put_by(&[0xFF], &"2".to_string())).unwrap();

        let map = block_on(l.to_map()).unwrap();
        assert_eq!(map.len(), 1, "a binary key has no place in a String map");
        assert!(map.contains_key("a"));
        // But it is still there, and still readable by bytes.
        assert_eq!(l.len(), 2);
        assert_eq!(block_on(l.get_by(&[0xFF])).unwrap(), Some("2".into()));
    }

    #[test]
    fn watch_keys_covers_several_keys_at_once() {
        let l = locker();
        let mut events = l.watch_keys(&["a", "b"]);

        block_on(l.put("ignored", &"x".to_string())).unwrap();
        block_on(l.put("a", &"1".to_string())).unwrap();
        block_on(l.delete("b")).unwrap();

        assert_eq!(
            block_on(events.next()),
            Some(Event::Put { key: b"a".to_vec() })
        );
        assert_eq!(
            block_on(events.next()),
            Some(Event::Deleted { key: b"b".to_vec() })
        );
    }

    #[test]
    fn keys_may_contain_astral_plane_characters() {
        let l = seeded(&[("\u{1F34E}", "apple"), ("\u{E000}", "private")]);
        // UTF-8 byte order puts U+E000 (EE 80 80) below U+1F34E (F0 9F 8D 8E).
        assert_eq!(l.keys(), vec!["\u{E000}", "\u{1F34E}"]);
    }
}
