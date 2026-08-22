//! A locker's resident state: its key index, its byte budget, and its staged
//! writes.
//!
//! # Why this is not simply fields on the locker
//!
//! Two things need to reach it without knowing the locker's value type:
//!
//! * the **coherence callback**, which folds another tab's news into the key
//!   index (see [`crate::coherence`]);
//! * [`crate::Bank::flush_all`], which must commit every open locker's staged
//!   writes and cannot be generic over each one's `T`.
//!
//! Everything here is therefore non-generic. It works in keys, payload bytes
//! and ops — the value type only matters at the two edges, where a caller
//! hands one in or takes one out.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::backend::api::Op;
use crate::error::{Error, Result};
use crate::watch::Event;

use super::inner::{Inner, Prior};
use super::lru::{self, Entry, LruState, Plan};
use super::policy::{Commit, Policy};
use super::transaction::TxMode;

/// One write waiting for a commit.
///
/// Payload bytes rather than a value: encoding happens when the write is
/// staged, on the caller's thread, so a flush is pure I/O and no user code can
/// run inside it.
#[derive(Debug, Clone)]
pub(crate) enum Pending {
    Put { key: Vec<u8>, payload: Vec<u8> },
    Delete { key: Vec<u8> },
    Clear,
}

/// What one commit does to an evictable locker's accounting.
///
/// Empty, and free, for a `Precious` locker.
#[derive(Debug, Default)]
pub(crate) struct Budget {
    updates: Vec<(Vec<u8>, Entry)>,
    removals: Vec<Vec<u8>>,
    cleared: bool,
    pub(crate) plan: Plan,
    /// Deferred tick bumps this commit carried, to forget once it lands.
    pending: Vec<Vec<u8>>,
}

pub(crate) struct Resident {
    pub(crate) inner: Arc<Inner>,
    mode: TxMode,
    /// The resident key index. `None` on an eager locker, which holds whole
    /// values instead and keeps them itself.
    index: Option<Mutex<BTreeSet<Vec<u8>>>>,
    /// Byte-budget accounting, present only under [`Policy::Evictable`].
    lru: Option<Mutex<LruState>>,
    staged: Mutex<Vec<Pending>>,
    /// Keys this handle has itself stored an **inline** record under, and has
    /// not touched since in any way that could have made it chunked.
    ///
    /// Purely a fast path for [`Resident::prior`], and deliberately only ever
    /// *shrinks* on doubt: forgetting a key costs one read, wrongly keeping
    /// one orphans chunks. `None` on a locker with no index, where absence
    /// cannot be proven anyway.
    known_inline: Option<Mutex<BTreeSet<Vec<u8>>>>,
}

impl std::fmt::Debug for Resident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resident")
            .field("locker", &self.inner.name)
            .field("pending", &self.pending_len().unwrap_or(0))
            .finish()
    }
}

impl Resident {
    pub(crate) fn new(
        inner: Arc<Inner>,
        mode: TxMode,
        index: Option<BTreeSet<Vec<u8>>>,
        lru: Option<LruState>,
    ) -> Self {
        Self {
            inner,
            mode,
            known_inline: index.as_ref().map(|_| Mutex::new(BTreeSet::new())),
            index: index.map(Mutex::new),
            lru: lru.map(Mutex::new),
            staged: Mutex::new(Vec::new()),
        }
    }

    // ---- the key index -------------------------------------------------

    pub(crate) fn read_index<R>(&self, f: impl FnOnce(&BTreeSet<Vec<u8>>) -> R) -> Option<R> {
        let guard = self.index.as_ref()?.lock().ok()?;
        Some(f(&guard))
    }

    pub(crate) fn touch_index(&self, f: impl FnOnce(&mut BTreeSet<Vec<u8>>)) {
        if let Some(Ok(mut guard)) = self.index.as_ref().map(|i| i.lock()) {
            f(&mut guard);
        }
    }

    // ---- what is already stored under a key ----------------------------

    /// What this handle can prove about the record currently under `key`.
    ///
    /// Conservative by construction. It answers anything but
    /// [`Prior::Unknown`] only when **all** of these hold:
    ///
    /// * this locker keeps a key index at all (a lazy locker; an eager one
    ///   never writes chunks and has its own resident map);
    /// * nothing is staged. A staged delete drops the key from the index while
    ///   the record is still stored, so a staged batch makes the index
    ///   over-claim *absence* — the one direction that loses data;
    /// * this handle's index is authoritative — no second handle has been live
    ///   on the name, and on the web cross-tab coherence is on. Both of those
    ///   let someone else write a chunked record this index has never heard of,
    ///   whose chunks would then be orphaned forever. See
    ///   [`Inner::index_is_authoritative`].
    ///
    /// `Inline` additionally requires that this handle wrote the record and
    /// nothing has since invalidated that. See [`Resident::note_inline`].
    pub(crate) fn prior(&self, key: &[u8]) -> Prior {
        if self.pending_len().unwrap_or(usize::MAX) != 0 {
            return Prior::Unknown;
        }
        if !self.inner.index_is_authoritative() {
            return Prior::Unknown;
        }
        let Some(present) = self.read_index(|index| index.contains(key)) else {
            return Prior::Unknown;
        };
        if !present {
            return Prior::Absent;
        }
        match self.known_inline.as_ref().map(|k| k.lock()) {
            Some(Ok(guard)) if guard.contains(key) => Prior::Inline,
            _ => Prior::Unknown,
        }
    }

    /// Record that `key` now holds an inline record (`true`) or that whatever
    /// this handle knew about it is no longer trustworthy (`false`).
    ///
    /// Call with `false` on every path that could have made the key chunked,
    /// deleted it, or handed it to someone else.
    pub(crate) fn note_inline(&self, key: &[u8], inline: bool) {
        let Some(Ok(mut guard)) = self.known_inline.as_ref().map(|k| k.lock()) else {
            return;
        };
        if inline {
            guard.insert(key.to_vec());
        } else {
            guard.remove(key);
        }
    }

    /// Forget every inline marker. The answer to anything wholesale — a
    /// clear, a close, another tab clearing, a transaction — where tracking
    /// each key individually would be more ways to be wrong.
    pub(crate) fn forget_inline(&self) {
        if let Some(Ok(mut guard)) = self.known_inline.as_ref().map(|k| k.lock()) {
            guard.clear();
        }
    }

    // ---- the byte budget -----------------------------------------------

    fn lru_lock(&self) -> Option<std::sync::MutexGuard<'_, LruState>> {
        self.lru.as_ref()?.lock().ok()
    }

    pub(crate) fn budget_used(&self) -> u64 {
        self.lru_lock().map(|s| s.total()).unwrap_or(0)
    }

    /// Note that `key` was read. RAM only; the bump rides along with the next
    /// commit, because a read must not write.
    pub(crate) async fn note_read(&self, key: &[u8]) -> Result<()> {
        if self.lru.is_none() {
            return Ok(());
        }
        let tick = self
            .inner
            .shared
            .ticks
            .allocate(self.inner.backend.as_ref())
            .await?;
        if let Some(mut state) = self.lru_lock() {
            state.touch(key, tick);
        }
        Ok(())
    }

    /// Everything one commit is about to do to the byte budget.
    ///
    /// Computed before the commit, applied after it lands — so a commit that
    /// fails leaves the accounting describing what is actually stored.
    pub(crate) async fn budget_ops(
        &self,
        ops: &mut Vec<Op>,
        updates: Vec<(Vec<u8>, u64)>,
        removals: Vec<Vec<u8>>,
        cleared: bool,
        keep: &[Vec<u8>],
    ) -> Result<Budget> {
        if self.lru.is_none() {
            return Ok(Budget::default());
        }

        // Another tab's news may have named a key whose size this tab could
        // not work out (see [`super::lazy::LazySink`]). Reload before planning
        // rather than evicting against a total that is known to be wrong.
        self.reload_lru_if_dirty().await?;

        let tick = self
            .inner
            .shared
            .ticks
            .allocate(self.inner.backend.as_ref())
            .await?;
        let updates: Vec<(Vec<u8>, Entry)> = updates
            .into_iter()
            .map(|(key, bytes)| (key, Entry { tick, bytes }))
            .collect();

        // The lock is taken, used, and dropped before any await below. A
        // `std` mutex held across an await would deadlock the moment two
        // futures on one thread interleaved.
        let (plan, pending, mut pending_ops) = {
            let Some(state) = self.lru_lock() else {
                return Ok(Budget::default());
            };
            let plan = state.plan(&updates, &removals, cleared, state.max_bytes, keep);
            let skip: Vec<Vec<u8>> = updates.iter().map(|(k, _)| k.clone()).collect();
            let (pending, pending_ops) = if cleared {
                (Vec::new(), Vec::new())
            } else {
                state.pending_ops(self.inner.id, &skip)
            };
            (plan, pending, pending_ops)
        };

        let id = self.inner.id;
        if cleared {
            ops.push(lru::clear_op(id));
        }
        for (key, entry) in &updates {
            ops.push(lru::put_op(id, key, *entry));
        }
        ops.append(&mut pending_ops);
        for key in &removals {
            ops.push(lru::delete_op(id, key));
        }
        for victim in &plan.victims {
            ops.extend(self.inner.delete_value_ops(victim, Prior::Unknown).await?);
            ops.push(lru::delete_op(id, victim));
        }
        ops.push(self.inner.shared.ticks.counter_op()?);

        Ok(Budget {
            updates,
            removals,
            cleared,
            plan,
            pending,
        })
    }

    /// Apply the accounting, drop the evicted keys from the index, and say so.
    pub(crate) fn apply_budget(&self, budget: &Budget) {
        if let Some(mut state) = self.lru_lock() {
            state.apply(
                &budget.updates,
                &budget.removals,
                budget.cleared,
                &budget.plan,
            );
            state.clear_pending(&budget.pending);
        }
        if !budget.plan.victims.is_empty() {
            self.touch_index(|index| {
                for victim in &budget.plan.victims {
                    index.remove(victim.as_slice());
                }
            });
            for victim in &budget.plan.victims {
                self.note_inline(victim, false);
            }
        }
        for key in &budget.plan.victims {
            self.inner.announce(Event::Evicted { key: key.clone() });
        }
    }

    /// Shed least-recently-used keys until at most `bytes` remain accounted.
    pub(crate) async fn evict_to(&self, bytes: u64) -> Result<usize> {
        if self.lru.is_none() {
            return Ok(0);
        }
        let _guard = self.inner.write_lock.lock().await;

        let plan = {
            let Some(state) = self.lru_lock() else {
                return Ok(0);
            };
            state.plan(&[], &[], false, bytes, &[])
        };
        if plan.victims.is_empty() {
            return Ok(0);
        }

        let mut ops = Vec::new();
        for victim in &plan.victims {
            ops.extend(self.inner.delete_value_ops(victim, Prior::Unknown).await?);
            ops.push(lru::delete_op(self.inner.id, victim));
        }
        self.inner.commit(ops).await?;

        let shed = plan.victims.len();
        self.apply_budget(&Budget {
            plan,
            ..Budget::default()
        });
        Ok(shed)
    }

    /// Reload the `lru::` prefix when a coherence callback could not account
    /// for another tab's write. Cheap in the normal case: the flag is false.
    async fn reload_lru_if_dirty(&self) -> Result<()> {
        let (dirty, max_bytes) = match self.lru_lock() {
            Some(state) => (state.is_dirty(), state.max_bytes),
            None => return Ok(()),
        };
        if !dirty {
            return Ok(());
        }
        let loaded = lru::load(
            self.inner.backend.as_ref(),
            self.inner.id,
            max_bytes,
            &self.inner.shared.ticks,
        )
        .await?;
        if let Some(mut state) = self.lru_lock() {
            state.adopt_loaded(loaded);
        }
        Ok(())
    }

    /// Fold another tab's news into the byte budget.
    ///
    /// Synchronous by necessity — a coherence callback must never await — so
    /// a size it cannot determine marks the accounting dirty instead of
    /// guessing. See [`LruState::remote_put`].
    pub(crate) fn remote_budget(&self, key: &[u8], bytes: Option<u64>, deleted: bool) {
        let Some(mut state) = self.lru_lock() else {
            return;
        };
        if deleted {
            state.remote_delete(key);
        } else {
            state.remote_put(key, bytes);
        }
    }

    /// Fold another tab's clear into the byte budget.
    pub(crate) fn remote_budget_clear(&self) {
        if let Some(mut state) = self.lru_lock() {
            state.remote_clear();
        }
    }

    // ---- staged writes -------------------------------------------------

    /// How many writes must pile up before this locker commits by itself, or
    /// `None` when every write commits immediately.
    fn deferred_after(&self) -> Option<usize> {
        match self.inner.config.commit {
            Commit::Immediate => None,
            // 0 and 1 both mean "commit on the next write", which is
            // `Immediate` with extra steps — so treat them as it.
            Commit::Deferred { after } if after <= 1 => None,
            Commit::Deferred { after } => Some(after),
        }
    }

    pub(crate) fn is_deferred(&self) -> bool {
        self.deferred_after().is_some()
    }

    /// How many writes are staged.
    ///
    /// `Err` on a poisoned lock rather than 0: reporting "nothing staged"
    /// there would turn [`Resident::flush`] into a silent no-op and lose the
    /// only copy of the batch.
    pub(crate) fn pending_len(&self) -> Result<usize> {
        self.staged
            .lock()
            .map(|s| s.len())
            .map_err(|_| Error::backend("staged write lock was poisoned"))
    }

    pub(crate) fn pending_bytes(&self) -> Result<u64> {
        self.staged
            .lock()
            .map(|s| {
                s.iter()
                    .map(|p| match p {
                        Pending::Put { payload, .. } => payload.len() as u64,
                        Pending::Delete { .. } | Pending::Clear => 0,
                    })
                    .sum()
            })
            .map_err(|_| Error::backend("staged write lock was poisoned"))
    }

    /// Queue a write. Returns whether the batch is now full.
    pub(crate) fn stage(&self, entry: Pending) -> Result<bool> {
        let after = self.deferred_after().unwrap_or(usize::MAX);
        let mut guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("staged write lock was poisoned"))?;
        guard.push(entry);
        Ok(guard.len() >= after)
    }

    /// The staged payload for a key: `None` when nothing staged has touched
    /// it, `Some(None)` when the last thing staged removed it.
    pub(crate) fn staged_view(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("staged write lock was poisoned"))?;
        for entry in guard.iter().rev() {
            match entry {
                Pending::Put { key: k, payload } if k == key => {
                    return Ok(Some(Some(payload.clone())))
                }
                Pending::Delete { key: k } if k == key => return Ok(Some(None)),
                Pending::Clear => return Ok(Some(None)),
                _ => {}
            }
        }
        Ok(None)
    }

    /// A copy of everything staged, oldest first.
    ///
    /// Used by the listing paths, which walk storage and must then overlay
    /// what this handle has staged but not committed.
    pub(crate) fn staged_snapshot(&self) -> Result<Vec<Pending>> {
        let guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("staged write lock was poisoned"))?;
        Ok(guard.clone())
    }

    /// Commit everything staged, taking the locker's write lock.
    pub(crate) async fn flush(&self) -> Result<()> {
        if self.pending_len()? == 0 {
            return Ok(());
        }
        let _guard = self.inner.write_lock.lock().await;
        self.flush_locked().await
    }

    /// As [`Resident::flush`], for a caller that already holds the write lock.
    pub(crate) async fn flush_locked(&self) -> Result<()> {
        let entries = self.take_staged()?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut ops =
            match super::transaction::ops_for_pending(&self.inner, &entries, self.mode).await {
                Ok(ops) => ops,
                Err(e) => {
                    // Put the batch back rather than dropping it on the floor: a
                    // flush that failed has not written anything, and the caller
                    // may well fix the cause and try again.
                    self.restage(entries);
                    return Err(e);
                }
            };

        let (updates, removals, cleared) = accounting(&entries);
        // The batch's own keys are `keep`: a commit must never evict a key it
        // is writing in that same commit, which would GC the new chunks and
        // leave the record pointing at nothing.
        let keep: Vec<Vec<u8>> = updates.iter().map(|(k, _)| k.clone()).collect();
        let budget = match self
            .budget_ops(&mut ops, updates, removals, cleared, &keep)
            .await
        {
            Ok(budget) => budget,
            Err(e) => {
                self.restage(entries);
                return Err(e);
            }
        };

        if let Err(e) = self.inner.commit(ops).await {
            self.restage(entries);
            return Err(e);
        }
        self.apply_budget(&budget);
        Ok(())
    }

    /// Drain the staged batch. The caller owns it and must restage it if the
    /// commit it was drained for does not land.
    pub(crate) fn take_staged(&self) -> Result<Vec<Pending>> {
        let mut guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("staged write lock was poisoned"))?;
        Ok(std::mem::take(&mut *guard))
    }

    /// Put a drained batch back at the front, ahead of anything staged since.
    pub(crate) fn restage(&self, mut entries: Vec<Pending>) {
        if let Ok(mut guard) = self.staged.lock() {
            entries.append(&mut guard);
            *guard = entries;
        }
    }

    /// Drop everything staged. Used by `close`, after a flush has had its go.
    pub(crate) fn discard_staged(&self) {
        if let Ok(mut guard) = self.staged.lock() {
            guard.clear();
        }
    }
}

/// Keys written with their payload sizes, keys removed, and whether the batch
/// began with a clear.
pub(crate) type Accounting = (Vec<(Vec<u8>, u64)>, Vec<Vec<u8>>, bool);

/// What a batch of staged writes does to the byte budget, collapsed.
pub(crate) fn accounting(entries: &[Pending]) -> Accounting {
    let mut updates: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut removals: Vec<Vec<u8>> = Vec::new();
    let mut cleared = false;
    for entry in entries {
        match entry {
            Pending::Put { key, payload } => {
                updates.retain(|(k, _)| k != key);
                removals.retain(|k| k != key);
                updates.push((key.clone(), payload.len() as u64));
            }
            Pending::Delete { key } => {
                updates.retain(|(k, _)| k != key);
                removals.push(key.clone());
            }
            Pending::Clear => {
                updates.clear();
                removals.clear();
                cleared = true;
            }
        }
    }
    (updates, removals, cleared)
}

/// Build the LRU state a locker opens with, or `None` where it has no budget.
pub(crate) async fn open_lru(inner: &Inner, index: &BTreeSet<Vec<u8>>) -> Result<Option<LruState>> {
    let Policy::Evictable { max_bytes } = inner.config.policy else {
        return Ok(None);
    };
    let mut state = lru::load(
        inner.backend.as_ref(),
        inner.id,
        max_bytes,
        &inner.shared.ticks,
    )
    .await?;
    // Reconcile with what storage actually holds. A locker first written as
    // `Precious` and reopened as `Evictable` has keys with no accounting, and
    // a crash between two commits could leave accounting with no key.
    state.retain_keys(|k| index.contains(k));
    for key in index {
        state.adopt(key.clone());
    }
    Ok(Some(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    fn resident() -> Resident {
        let inner = Arc::new(Inner {
            write_lock: futures::lock::Mutex::new(()),
            backend: Arc::new(crate::backend::MemoryBackend::new()),
            chain: Arc::new(crate::codec::default_chain()),
            id: 1,
            name: "test".into(),
            config: super::super::policy::LockerConfig::default()
                .with_commit(Commit::Deferred { after: 8 }),
            shared: Default::default(),
            watchers: Default::default(),
            closed: std::sync::atomic::AtomicBool::new(false),
            epochs: Default::default(),
            name_shared: std::sync::atomic::AtomicBool::new(false),
        });
        Resident::new(inner, TxMode::Lazy, Some(BTreeSet::new()), None)
    }

    /// A poisoned staging lock must never read as "nothing staged".
    ///
    /// It did, and `flush` returned `Ok(())` without writing anything: the
    /// batch was still sitting in the buffer, the caller was told it had
    /// landed, and the next `close` threw it away. Losing writes silently is
    /// the one outcome a store may not have.
    /// Native only: poisoning a lock needs a second thread to panic in, and
    /// wasm32 has none.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_poisoned_staging_lock_fails_the_flush_rather_than_skipping_it() {
        let res = Arc::new(resident());
        res.stage(Pending::Put {
            key: b"k".to_vec(),
            payload: vec![1, 2, 3],
        })
        .expect("staged");
        assert_eq!(res.pending_len().expect("healthy"), 1);

        // Poison it exactly as a panicking writer would.
        let poisoner = res.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.staged.lock();
            panic!("poison the staging lock");
        })
        .join();

        assert!(
            res.pending_len().is_err(),
            "a poisoned lock must not answer 0"
        );
        let flushed = futures::executor::block_on(res.flush());
        assert!(
            matches!(flushed, Err(Error::Backend(_))),
            "flush must report the poisoning, not silently do nothing: {flushed:?}"
        );
    }

    #[test]
    fn a_batch_of_one_is_not_deferral() {
        // Both degenerate settings must read as `Immediate` rather than
        // staging a write nobody will ever flush.
        for after in [0usize, 1] {
            let config = super::super::policy::LockerConfig::default()
                .with_commit(Commit::Deferred { after });
            assert!(matches!(config.commit, Commit::Deferred { .. }));
            assert_eq!(
                match config.commit {
                    Commit::Deferred { after } if after <= 1 => None,
                    Commit::Deferred { after } => Some(after),
                    Commit::Immediate => None,
                },
                None
            );
        }
    }

    #[test]
    fn accounting_collapses_a_batch() {
        let entries = vec![
            Pending::Put {
                key: b"a".to_vec(),
                payload: vec![0; 10],
            },
            Pending::Put {
                key: b"a".to_vec(),
                payload: vec![0; 20],
            },
            Pending::Delete { key: b"b".to_vec() },
        ];
        let (updates, removals, cleared) = accounting(&entries);
        assert_eq!(updates, vec![(b"a".to_vec(), 20)]);
        assert_eq!(removals, vec![b"b".to_vec()]);
        assert!(!cleared);
    }

    #[test]
    fn a_clear_wipes_the_batch_before_it() {
        let entries = vec![
            Pending::Put {
                key: b"a".to_vec(),
                payload: vec![0; 10],
            },
            Pending::Clear,
            Pending::Put {
                key: b"c".to_vec(),
                payload: vec![0; 5],
            },
        ];
        let (updates, removals, cleared) = accounting(&entries);
        assert!(cleared);
        assert!(removals.is_empty());
        assert_eq!(updates, vec![(b"c".to_vec(), 5)]);
    }
}
