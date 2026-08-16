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
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::Backend;
use crate::codec::FilterChain;
use crate::error::{Error, Result};
use crate::key::LockerId;

use super::inner::Inner;
use super::policy::LockerConfig;

/// A locker that keeps only its key index in memory.
pub struct LazyLocker<T> {
    inner: Arc<Inner>,
    index: Mutex<BTreeSet<String>>,
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
    ) -> Result<Self> {
        let inner = Arc::new(Inner {
            backend,
            chain,
            id,
            name,
            config,
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
            _value: PhantomData,
        })
    }

    /// Fetch and decode one value.
    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        match self.inner.fetch(key).await? {
            Some(raw) => Ok(Some(self.inner.open(&raw)?)),
            None => Ok(None),
        }
    }

    /// Store one value.
    pub async fn put(&self, key: &str, value: &T) -> Result<()> {
        let op = self.inner.put_op(key, value)?;
        self.inner.commit(vec![op]).await?;
        self.touch_index(|i| {
            i.insert(key.to_string());
        });
        Ok(())
    }

    /// Remove one key. Removing an absent key is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.inner.commit(vec![self.inner.delete_op(key)]).await?;
        self.touch_index(|i| {
            i.remove(key);
        });
        Ok(())
    }

    /// Remove everything in this locker, and nothing outside it.
    pub async fn clear(&self) -> Result<()> {
        self.inner.commit(vec![self.inner.clear_op()]).await?;
        self.touch_index(|i| i.clear());
        Ok(())
    }

    /// Values over a key range, in byte order.
    pub async fn range<'a, R: RangeBounds<&'a str>>(&self, range: R) -> Result<Vec<(String, T)>> {
        self.collect(
            deref_bound(range.start_bound()),
            deref_bound(range.end_bound()),
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
        self.collect(
            deref_bound(range.start_bound()),
            deref_bound(range.end_bound()),
            true,
            None,
        )
        .await
    }

    /// The first `limit` entries of a descending scan — "the latest N".
    pub async fn latest(&self, limit: usize) -> Result<Vec<(String, T)>> {
        self.collect(Bound::Unbounded, Bound::Unbounded, true, Some(limit))
            .await
    }

    async fn collect(
        &self,
        start: Bound<&str>,
        end: Bound<&str>,
        reverse: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(String, T)>> {
        // Decode outside the visitor so a decode failure surfaces as an error
        // rather than being swallowed mid-walk.
        let mut raw: Vec<(String, Vec<u8>)> = Vec::new();
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

        raw.into_iter()
            .map(|(k, bytes)| self.inner.open(&bytes).map(|v| (k, v)))
            .collect()
    }
}

/// Bounds-free accessors. Split out so `Debug` — and any caller holding a
/// `LazyLocker<T>` for a `T` that is not itself serialisable — can still ask
/// about the index.
impl<T> LazyLocker<T> {
    pub fn name(&self) -> &str {
        &self.inner.name
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
        self.index.lock().map(|i| i.contains(key)).unwrap_or(false)
    }

    /// Every key, in byte order.
    pub fn keys(&self) -> Vec<String> {
        self.index
            .lock()
            .map(|i| i.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Keys beginning with `prefix`, in byte order.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.index
            .lock()
            .map(|i| {
                i.range(prefix.to_string()..)
                    .take_while(|k| k.starts_with(prefix))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn touch_index(&self, f: impl FnOnce(&mut BTreeSet<String>)) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::codec::default_chain;
    use futures::executor::block_on;

    fn locker() -> LazyLocker<String> {
        block_on(LazyLocker::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "test".into(),
            LockerConfig::default(),
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
        ))
        .unwrap();
        let two: LazyLocker<String> = block_on(LazyLocker::open(
            backend.clone(),
            chain,
            2,
            "two".into(),
            LockerConfig::default(),
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
    fn keys_may_contain_astral_plane_characters() {
        let l = seeded(&[("\u{1F34E}", "apple"), ("\u{E000}", "private")]);
        // UTF-8 byte order puts U+E000 (EE 80 80) below U+1F34E (F0 9F 8D 8E).
        assert_eq!(l.keys(), vec!["\u{E000}", "\u{1F34E}"]);
    }
}
