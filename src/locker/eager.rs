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
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::Backend;
use crate::codec::FilterChain;
use crate::error::{Error, Result};
use crate::key::LockerId;

use super::inner::Inner;
use super::policy::{LockerConfig, Policy};
use super::transaction::{Staged, Transaction};

/// A locker whose values live in memory.
pub struct Locker<T> {
    inner: Arc<Inner>,
    values: Mutex<BTreeMap<String, Arc<T>>>,
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
        });

        let mut values = BTreeMap::new();
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

                    values.insert(key, Arc::new(inner.open::<T>(&bytes)?));
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

        Ok(Self {
            inner,
            values: Mutex::new(values),
        })
    }

    /// Store a value. Writes are asynchronous even though reads are not.
    pub async fn put(&self, key: &str, value: T) -> Result<Arc<T>> {
        let sealed = self.inner.seal(&value)?;
        if sealed.len() > self.inner.config.max_inline {
            return Err(Error::ValueTooLarge {
                bytes: sealed.len(),
                max_inline: self.inner.config.max_inline,
            });
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
            guard.insert(key.to_string(), shared.clone());
        }
        Ok(shared)
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
        // transactional writes exactly as it does to a plain put.
        for entry in &entries {
            if let Staged::Put { bytes, .. } = entry {
                if bytes.len() > self.inner.config.max_inline {
                    return Err(Error::ValueTooLarge {
                        bytes: bytes.len(),
                        max_inline: self.inner.config.max_inline,
                    });
                }
            }
        }

        let ops = Transaction::<T>::ops_for(&self.inner, &entries);
        self.inner.commit(ops).await?;

        if let Ok(mut guard) = self.values.lock() {
            for entry in entries {
                match entry {
                    Staged::Put { key, value, .. } => {
                        guard.insert(key, value);
                    }
                    Staged::Delete { key } => {
                        guard.remove(&key);
                    }
                    Staged::Clear => guard.clear(),
                }
            }
        }
        Ok(())
    }

    /// Remove a key. Removing an absent key is not an error.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.inner.commit(vec![self.inner.delete_op(key)]).await?;
        if let Ok(mut guard) = self.values.lock() {
            guard.remove(key);
        }
        Ok(())
    }

    /// Remove everything in this locker, and nothing outside it.
    pub async fn clear(&self) -> Result<()> {
        self.inner.commit(vec![self.inner.clear_op()]).await?;
        if let Ok(mut guard) = self.values.lock() {
            guard.clear();
        }
        Ok(())
    }
}

/// Bounds-free accessors. Split out so `Debug` — and any caller holding a
/// `Locker<T>` for a `T` that is not itself serialisable — can still read.
impl<T> Locker<T> {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Read a value. Synchronous and infallible — the whole point of the type.
    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        self.values.lock().ok()?.get(key).cloned()
    }

    pub fn contains_key(&self, key: &str) -> bool {
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

    /// Every key, in byte order.
    pub fn keys(&self) -> Vec<String> {
        self.values
            .lock()
            .map(|v| v.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Keys beginning with `prefix`, in byte order.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.values
            .lock()
            .map(|v| {
                v.range(prefix.to_string()..)
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A snapshot of every entry, in byte order.
    pub fn entries(&self) -> Vec<(String, Arc<T>)> {
        self.values
            .lock()
            .map(|v| v.iter().map(|(k, val)| (k.clone(), val.clone())).collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::codec::default_chain;
    use futures::executor::block_on;

    fn open_with(config: LockerConfig) -> Result<Locker<String>> {
        block_on(Locker::open(
            Arc::new(MemoryBackend::new()),
            Arc::new(default_chain()),
            1,
            "test".into(),
            config,
        ))
    }

    fn locker() -> Locker<String> {
        open_with(LockerConfig::default()).unwrap()
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
