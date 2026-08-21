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
use super::transaction::{Staged, Transaction, TxMode};
use crate::watch::Event;

/// A locker that keeps only its key index in memory.
pub struct LazyLocker<T> {
    pub(crate) inner: Arc<Inner>,
    index: Mutex<BTreeSet<Vec<u8>>>,
    /// Keys whose stored bytes failed to decode on a `get`. See
    /// [`LazyLocker::corrupt_keys`].
    corrupt: Mutex<BTreeSet<Vec<u8>>>,
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
        value_ids: Arc<super::chunk::ValueIds>,
    ) -> Result<Self> {
        let inner = Arc::new(Inner {
            write_lock: futures::lock::Mutex::new(()),
            backend,
            chain,
            id,
            name,
            config,
            value_ids,
            watchers: Default::default(),
            closed: AtomicBool::new(false),
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

        Ok(Self {
            inner,
            index: Mutex::new(index),
            corrupt: Mutex::new(BTreeSet::new()),
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
        let ops = self
            .inner
            .put_payload_ops(key, payload, super::chunk::FLAG_POSTCARD)
            .await?;
        self.inner.commit(ops).await?;
        self.touch_index(|i| {
            i.insert(key.to_vec());
        });
        self.inner.announce(Event::Put { key: key.to_vec() });
        Ok(())
    }

    /// Remove one key. Removing an absent key is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.delete_by(key.as_bytes()).await
    }

    /// As [`LazyLocker::delete`], under a binary key.
    pub async fn delete_by(&self, key: &[u8]) -> Result<()> {
        self.inner.ensure_open()?;
        let _guard = self.inner.write_lock.lock().await;
        let ops = self.inner.delete_value_ops(key).await?;
        self.inner.commit(ops).await?;
        self.touch_index(|i| {
            i.remove(key);
        });
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
        let ops = self.inner.clear_value_ops().await?;
        self.inner.commit(ops).await?;
        self.touch_index(|i| i.clear());
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

        let ops = Transaction::<T>::ops_for(&self.inner, &entries, TxMode::Lazy).await?;
        self.inner.commit(ops).await?;

        // Index updates only after the commit lands, so a failed write cannot
        // leave the index claiming keys that were never stored.
        self.touch_index(|i| {
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

    async fn collect(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, T)>> {
        // Decode outside the visitor so a decode failure surfaces as an error
        // rather than being swallowed mid-walk.
        let mut raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let cap = limit.unwrap_or(usize::MAX);

        self.inner
            .walk(start, end, reverse, true, |key, value| {
                if raw.len() >= cap {
                    return Ok(());
                }
                let bytes = value.ok_or_else(|| {
                    Error::Corrupt(format!("backend omitted a value for key {key:?}"))
                })?;
                raw.push((key, bytes));
                Ok(())
            })
            .await?;

        let mut out = Vec::with_capacity(raw.len());
        for (k, bytes) in raw {
            out.push((k, self.inner.decode_record(&bytes).await?));
        }
        Ok(out)
    }
}

/// Bounds-free accessors. Split out so `Debug` — and any caller holding a
/// `LazyLocker<T>` for a `T` that is not itself serialisable — can still ask
/// about the index.
impl<T> LazyLocker<T> {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.inner
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
    pub fn close(&self) {
        self.inner.mark_closed();
        if let Ok(mut guard) = self.index.lock() {
            guard.clear();
        }
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
        self.index.lock().map(|i| i.len()).unwrap_or(0)
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
        self.index.lock().map(|i| i.contains(key)).unwrap_or(false)
    }

    /// Every UTF-8 key, in byte order.
    ///
    /// Keys that are not valid UTF-8 are **skipped**, not reported as an
    /// error — a listing must never fail because of a key it cannot spell.
    /// [`LazyLocker::has_non_utf8_keys`] says whether any were skipped, and
    /// [`LazyLocker::keys_bytes`] returns every key regardless.
    pub fn keys(&self) -> Vec<String> {
        self.index
            .lock()
            .map(|i| i.iter().filter_map(|k| utf8(k)).collect())
            .unwrap_or_default()
    }

    /// Every key as raw bytes, in byte order. Includes non-UTF-8 keys.
    pub fn keys_bytes(&self) -> Vec<Vec<u8>> {
        self.index
            .lock()
            .map(|i| i.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether this locker holds any key that [`LazyLocker::keys`] cannot
    /// return. Never panics and never errors.
    pub fn has_non_utf8_keys(&self) -> bool {
        self.index
            .lock()
            .map(|i| i.iter().any(|k| std::str::from_utf8(k).is_err()))
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
        self.index
            .lock()
            .map(|i| {
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

    fn touch_index(&self, f: impl FnOnce(&mut BTreeSet<Vec<u8>>)) {
        if let Ok(mut guard) = self.index.lock() {
            f(&mut guard);
        }
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
