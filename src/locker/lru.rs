//! Byte-budget LRU bookkeeping for [`crate::Policy::Evictable`] lockers.
//!
//! # What is stored, and why in the same commit
//!
//! One `meta` record per live key:
//!
//! ```text
//!   lru::{locker_id u32 BE}::{key bytes}  ->  [tick u64 BE][bytes u32 BE]
//! ```
//!
//! written in **the same commit as the put it describes**. Anything else opens
//! a window where the record exists and its accounting does not (or the other
//! way round), and a budget that disagrees with the data it is supposed to
//! bound is worse than no budget at all.
//!
//! # Why a logical tick and not a clock
//!
//! `std::time::Instant` compiles on wasm32 and panics at runtime, and
//! `Date.now()` moves backwards when the user changes the system clock. The
//! ordering an LRU needs is *relative*, so a bank-wide `u64` counter answers it
//! exactly, cheaply, and identically on every platform.
//!
//! The counter is the same shape as [`super::chunk::ValueIds`]: seeded from
//! `meta` on first use, then a RAM cursor, with its high-water mark persisted
//! by **every** commit that allocates from it. A reopen therefore never
//! re-issues a tick that is already recorded against a key.
//!
//! # What `bytes` counts
//!
//! The value's own payload — what `postcard` produced — not its on-disk
//! footprint. The stored form goes through the filter chain (compression
//! changes the size) and may be split into chunk records (framing adds to it),
//! so the on-disk number is both backend-dependent and unstable across a
//! filter-chain change. A budget expressed in payload bytes is the one a
//! caller can reason about: it is the size of the data they handed us.

use std::collections::BTreeMap;

use crate::backend::api::{Backend, KeyRange, Op, ScanRequest, Table};
use crate::error::{Error, Result};
use crate::key::LockerId;

/// Meta key prefix for the LRU records of one locker.
const LRU_PREFIX: &[u8] = b"lru::";
const META_NEXT_TICK: &[u8] = b"next_tick";

/// One LRU record's value: `[tick u64 BE][bytes u32 BE]`.
const RECORD_LEN: usize = 12;

/// `lru::{locker_id BE}::`
pub(crate) fn locker_prefix(id: LockerId) -> Vec<u8> {
    let mut out = Vec::with_capacity(LRU_PREFIX.len() + 6);
    out.extend_from_slice(LRU_PREFIX);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(b"::");
    out
}

/// `lru::{locker_id BE}::{key}`
pub(crate) fn entry_key(id: LockerId, key: &[u8]) -> Vec<u8> {
    let mut out = locker_prefix(id);
    out.extend_from_slice(key);
    out
}

/// The user key an LRU meta key describes, or `None` if it is not one of ours.
fn user_key<'a>(prefix: &[u8], meta_key: &'a [u8]) -> Option<&'a [u8]> {
    meta_key.strip_prefix(prefix)
}

/// One key's accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry {
    pub tick: u64,
    pub bytes: u64,
}

fn encode_entry(entry: Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_LEN);
    out.extend_from_slice(&entry.tick.to_be_bytes());
    // Saturating rather than wrapping: a value past 4 GiB is accounted as
    // 4 GiB, which over-states nothing and keeps the record fixed-width.
    out.extend_from_slice(&(entry.bytes.min(u32::MAX as u64) as u32).to_be_bytes());
    out
}

fn parse_entry(raw: &[u8]) -> Option<Entry> {
    if raw.len() != RECORD_LEN {
        return None;
    }
    let tick = u64::from_be_bytes(raw[0..8].try_into().ok()?);
    let bytes = u32::from_be_bytes(raw[8..12].try_into().ok()?) as u64;
    Some(Entry { tick, bytes })
}

/// The op that records `key`'s accounting.
pub(crate) fn put_op(id: LockerId, key: &[u8], entry: Entry) -> Op {
    Op::Put {
        table: Table::Meta,
        key: entry_key(id, key),
        value: encode_entry(entry),
    }
}

/// The op that forgets one key's accounting.
pub(crate) fn delete_op(id: LockerId, key: &[u8]) -> Op {
    Op::Delete {
        table: Table::Meta,
        key: entry_key(id, key),
    }
}

/// The op that forgets a whole locker's accounting.
pub(crate) fn clear_op(id: LockerId) -> Op {
    Op::DeleteRange {
        table: Table::Meta,
        range: KeyRange::prefix(&locker_prefix(id)),
    }
}

/// The bank-wide logical clock the LRU orders by.
///
/// The same shape as [`super::chunk::ValueIds`], and for the same reason: one
/// cursor per bank, shared by every locker it opened, so two handles on one
/// name cannot hand out the same tick. The `std` mutex is **never** held
/// across an await — allocation is arithmetic.
#[derive(Debug, Default)]
pub struct Ticks {
    next: std::sync::Mutex<Option<u64>>,
}

impl Ticks {
    fn take(&self) -> Result<Option<u64>> {
        let mut guard = self
            .next
            .lock()
            .map_err(|_| Error::backend("tick cursor was poisoned"))?;
        match *guard {
            None => Ok(None),
            Some(tick) => {
                *guard = Some(advance(tick)?);
                Ok(Some(tick))
            }
        }
    }

    /// `max` rather than assignment: a concurrent allocation may have seeded
    /// the cursor while this one awaited the read, and a clock must only ever
    /// move forward.
    fn seed_and_take(&self, stored: u64) -> Result<u64> {
        let mut guard = self
            .next
            .lock()
            .map_err(|_| Error::backend("tick cursor was poisoned"))?;
        let tick = match *guard {
            Some(current) => current.max(stored),
            None => stored,
        };
        *guard = Some(advance(tick)?);
        Ok(tick)
    }

    /// Allocate the next tick.
    ///
    /// Every commit carrying a tick allocated here must also carry
    /// [`Ticks::counter_op`], or a reopen would re-issue ticks already
    /// recorded against keys.
    pub(crate) async fn allocate(&self, backend: &dyn Backend) -> Result<u64> {
        if let Some(tick) = self.take()? {
            return Ok(tick);
        }
        let raw = backend.get(Table::Meta, META_NEXT_TICK).await?;
        let stored = match raw {
            None => 0,
            Some(bytes) => <[u8; 8]>::try_from(bytes.as_slice())
                .map(u64::from_be_bytes)
                .map_err(|_| Error::Corrupt("next_tick is not an 8-byte integer".into()))?,
        };
        self.seed_and_take(stored)
    }

    /// Persist the current high-water mark. Belongs in every allocating commit.
    pub(crate) fn counter_op(&self) -> Result<Op> {
        let guard = self
            .next
            .lock()
            .map_err(|_| Error::backend("tick cursor was poisoned"))?;
        let next = guard.unwrap_or(0);
        Ok(Op::Put {
            table: Table::Meta,
            key: META_NEXT_TICK.to_vec(),
            value: next.to_be_bytes().to_vec(),
        })
    }
}

fn advance(tick: u64) -> Result<u64> {
    tick.checked_add(1)
        .ok_or_else(|| Error::backend("tick space is exhausted"))
}

/// How many deferred tick bumps ride along on one write commit.
///
/// Bounded so a read-heavy burst cannot turn the next small write into a
/// commit of thousands of meta records.
pub(crate) const PENDING_PER_COMMIT: usize = 64;

/// What a commit will do to the budget, worked out before anything is written.
#[derive(Debug, Default)]
pub(crate) struct Plan {
    /// Keys to shed, least recently used first.
    pub victims: Vec<Vec<u8>>,
    /// The running total once the updates and the evictions have landed.
    pub total: u64,
}

/// One evictable locker's resident accounting.
#[derive(Debug)]
pub(crate) struct LruState {
    pub max_bytes: u64,
    entries: BTreeMap<Vec<u8>, Entry>,
    total: u64,
    /// Ticks bumped by `get` that no commit has carried yet. Reads must not
    /// write, so these ride along with the next write — or a `flush`.
    pending: BTreeMap<Vec<u8>, u64>,
    /// Set when another tab's news named a key whose size this tab could not
    /// work out. The accounting is then known to be incomplete, and the next
    /// commit reloads the `lru::` prefix before planning. See
    /// [`LruState::mark_dirty`].
    dirty: bool,
}

impl LruState {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            entries: BTreeMap::new(),
            total: 0,
            pending: BTreeMap::new(),
            dirty: false,
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    /// Whether the accounting is known to be incomplete and wants a reload.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Say that a remote change could not be accounted for.
    ///
    /// Nothing is guessed: the total simply stops being trustworthy until
    /// [`LruState::adopt_loaded`] replaces it with what storage records.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Replace the accounting wholesale with a freshly loaded one, keeping the
    /// deferred read-ticks this tab has not committed yet.
    pub fn adopt_loaded(&mut self, loaded: LruState) {
        self.entries = loaded.entries;
        self.total = loaded.total;
        self.pending.retain(|k, _| self.entries.contains_key(k));
        self.dirty = false;
    }

    /// A tick that sorts after everything this state currently holds.
    ///
    /// Used for another tab's write, which this tab cannot allocate a real
    /// bank tick for without awaiting — a coherence callback must never await.
    /// It only has to order the remote write as "more recent than what is
    /// here", which this does.
    pub fn next_local_tick(&self) -> u64 {
        self.entries
            .values()
            .map(|e| e.tick)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
    }

    /// Fold another tab's put into the accounting.
    ///
    /// `bytes` is `None` when the size could not be determined, which marks
    /// the state dirty rather than inventing a number.
    pub fn remote_put(&mut self, key: &[u8], bytes: Option<u64>) {
        let tick = self.next_local_tick();
        match bytes {
            Some(bytes) => {
                if let Some(old) = self.entries.insert(key.to_vec(), Entry { tick, bytes }) {
                    self.total = self.total.saturating_sub(old.bytes);
                }
                self.total = self.total.saturating_add(bytes);
            }
            None => {
                // The key exists, so it must be in the index and in the
                // ordering; only its size is unknown. Account it as zero and
                // let the reload correct the total.
                self.entries.entry(key.to_vec()).or_insert(Entry { tick, bytes: 0 });
                self.mark_dirty();
            }
        }
    }

    /// Fold another tab's delete into the accounting.
    pub fn remote_delete(&mut self, key: &[u8]) {
        if let Some(old) = self.entries.remove(key) {
            self.total = self.total.saturating_sub(old.bytes);
        }
        self.pending.remove(key);
    }

    /// Fold another tab's clear into the accounting.
    pub fn remote_clear(&mut self) {
        self.entries.clear();
        self.pending.clear();
        self.total = 0;
        self.dirty = false;
    }

    /// Note that `key` was read. RAM only — see [`LruState::pending`].
    pub fn touch(&mut self, key: &[u8], tick: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.tick = tick;
            self.pending.insert(key.to_vec(), tick);
        }
    }

    /// The deferred tick bumps to carry on the next commit, and the ops.
    /// `skip` names keys the caller is already writing an LRU record for in
    /// this commit; a stale deferred tick must not overwrite a fresh one.
    pub fn pending_ops(&self, id: LockerId, skip: &[Vec<u8>]) -> (Vec<Vec<u8>>, Vec<Op>) {
        let mut keys = Vec::new();
        let mut ops = Vec::new();
        for (key, tick) in self.pending.iter().take(PENDING_PER_COMMIT) {
            if skip.iter().any(|s| s == key) {
                keys.push(key.clone());
                continue;
            }
            let Some(entry) = self.entries.get(key) else {
                continue;
            };
            keys.push(key.clone());
            ops.push(put_op(
                id,
                key,
                Entry {
                    tick: *tick,
                    bytes: entry.bytes,
                },
            ));
        }
        (keys, ops)
    }

    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn clear_pending(&mut self, keys: &[Vec<u8>]) {
        for key in keys {
            self.pending.remove(key);
        }
    }

    /// Work out what a commit will cost and who has to go, without touching
    /// anything. `keep` names the keys this same commit is writing, which must
    /// never be evicted to make room for themselves — a batch of them just as
    /// much as a single put. If the batch alone exceeds the budget, everything
    /// else is shed and the batch is left in place: refusing to store what we
    /// were just asked to store would be the wrong failure.
    ///
    /// `budget` is normally [`LruState::max_bytes`];
    /// [`crate::LazyLocker::evict_to`] passes a smaller one.
    pub fn plan(
        &self,
        updates: &[(Vec<u8>, Entry)],
        removals: &[Vec<u8>],
        cleared: bool,
        budget: u64,
        keep: &[Vec<u8>],
    ) -> Plan {
        let mut total = if cleared { 0 } else { self.total };

        if !cleared {
            for key in removals {
                if let Some(entry) = self.entries.get(key.as_slice()) {
                    total = total.saturating_sub(entry.bytes);
                }
            }
        }
        for (key, entry) in updates {
            if !cleared {
                if let Some(old) = self.entries.get(key.as_slice()) {
                    total = total.saturating_sub(old.bytes);
                }
            }
            total = total.saturating_add(entry.bytes);
        }

        if total <= budget {
            return Plan {
                victims: Vec::new(),
                total,
            };
        }

        // Only now is it worth materialising a candidate list. Eviction is the
        // rare path; a put that stays inside the budget must not pay for it.
        let mut candidates: Vec<(u64, &[u8], u64)> = Vec::new();
        if !cleared {
            for (key, entry) in &self.entries {
                if removals.iter().any(|r| r == key)
                    || updates.iter().any(|(k, _)| k == key)
                    || keep.iter().any(|k| k.as_slice() == key.as_slice())
                {
                    continue;
                }
                candidates.push((entry.tick, key.as_slice(), entry.bytes));
            }
        }
        for (key, entry) in updates {
            if keep.iter().any(|k| k == key) {
                continue;
            }
            candidates.push((entry.tick, key.as_slice(), entry.bytes));
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));

        let mut victims = Vec::new();
        for (_, key, bytes) in candidates {
            if total <= budget {
                break;
            }
            total = total.saturating_sub(bytes);
            victims.push(key.to_vec());
        }
        Plan { victims, total }
    }

    /// Apply what [`LruState::plan`] described, once the commit has landed.
    pub fn apply(
        &mut self,
        updates: &[(Vec<u8>, Entry)],
        removals: &[Vec<u8>],
        cleared: bool,
        plan: &Plan,
    ) {
        if cleared {
            self.entries.clear();
            self.pending.clear();
        }
        for key in removals {
            self.entries.remove(key.as_slice());
            self.pending.remove(key.as_slice());
        }
        for (key, entry) in updates {
            self.entries.insert(key.clone(), *entry);
        }
        for key in &plan.victims {
            self.entries.remove(key.as_slice());
            self.pending.remove(key.as_slice());
        }
        self.total = plan.total;
    }

    /// Seed one key that storage holds but the LRU records nothing for.
    ///
    /// Happens when a locker written under `Precious` is reopened as
    /// `Evictable`. Size unknown, so it is accounted as zero and ordered
    /// first: it is shed before anything whose cost we actually know.
    pub fn adopt(&mut self, key: Vec<u8>) {
        self.entries
            .entry(key)
            .or_insert(Entry { tick: 0, bytes: 0 });
    }

    pub fn retain_keys(&mut self, live: impl Fn(&[u8]) -> bool) {
        self.entries.retain(|k, _| live(k));
        self.pending.retain(|k, _| live(k));
        self.total = self.entries.values().map(|e| e.bytes).sum();
    }
}

/// Read one locker's LRU records back at open.
///
/// One prefix scan of `meta`, paged like every other scan because an IndexedDB
/// cursor cannot outlive its transaction.
pub(crate) async fn load(backend: &dyn Backend, id: LockerId, max_bytes: u64) -> Result<LruState> {
    let mut state = LruState::new(max_bytes);
    let prefix = locker_prefix(id);
    let mut range = KeyRange::prefix(&prefix);

    loop {
        let page = backend
            .scan(ScanRequest {
                table: Table::Meta,
                range: range.clone(),
                reverse: false,
                limit: super::inner::SCAN_PAGE,
                want_values: true,
            })
            .await?;

        for (meta_key, value) in &page.items {
            let Some(key) = user_key(&prefix, meta_key) else {
                continue;
            };
            // A record we cannot parse is treated as absent rather than fatal:
            // losing one key's read-ordering is not worth refusing to open a
            // locker whose data is perfectly intact.
            if let Some(entry) = value.as_deref().and_then(parse_entry) {
                state.total = state.total.saturating_add(entry.bytes);
                state.entries.insert(key.to_vec(), entry);
            }
        }

        match page.resume {
            Some(last) => range.start = std::ops::Bound::Excluded(last),
            None => break,
        }
    }

    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> LruState {
        let mut s = LruState::new(100);
        for (key, tick, bytes) in [(b"a", 1u64, 40u64), (b"b", 2, 40), (b"c", 3, 20)] {
            s.entries.insert(key.to_vec(), Entry { tick, bytes });
            s.total += bytes;
        }
        s
    }

    #[test]
    fn entry_records_round_trip() {
        let e = Entry {
            tick: 9_000_000_000,
            bytes: 4096,
        };
        assert_eq!(parse_entry(&encode_entry(e)), Some(e));
        assert_eq!(parse_entry(b"short"), None);
    }

    #[test]
    fn an_oversized_value_saturates_rather_than_wrapping() {
        let e = Entry {
            tick: 1,
            bytes: u64::from(u32::MAX) + 10_000,
        };
        let back = parse_entry(&encode_entry(e)).expect("fixed width");
        assert_eq!(back.bytes, u64::from(u32::MAX));
    }

    #[test]
    fn a_put_inside_the_budget_evicts_nothing() {
        let s = state();
        let plan = s.plan(
            &[(b"c".to_vec(), Entry { tick: 4, bytes: 20 })],
            &[],
            false,
            s.max_bytes,
            &[b"c".to_vec()],
        );
        assert!(plan.victims.is_empty());
        assert_eq!(plan.total, 100);
    }

    #[test]
    fn going_over_sheds_the_lowest_tick_first() {
        let s = state();
        // 40 + 40 + 20 + 30 = 130 > 100, so the oldest (a, tick 1) goes.
        let plan = s.plan(
            &[(b"d".to_vec(), Entry { tick: 4, bytes: 30 })],
            &[],
            false,
            s.max_bytes,
            &[b"d".to_vec()],
        );
        assert_eq!(plan.victims, vec![b"a".to_vec()]);
        assert_eq!(plan.total, 90);
    }

    #[test]
    fn the_key_being_written_is_never_its_own_victim() {
        let s = state();
        // One value larger than the whole budget: everything else goes, and
        // the key that caused it stays. Refusing to store what we were just
        // asked to store would be the wrong failure.
        let plan = s.plan(
            &[(
                b"big".to_vec(),
                Entry {
                    tick: 9,
                    bytes: 500,
                },
            )],
            &[],
            false,
            s.max_bytes,
            &[b"big".to_vec()],
        );
        assert_eq!(plan.victims.len(), 3);
        assert_eq!(plan.total, 500);
    }

    #[test]
    fn a_read_bumps_the_tick_and_defers_the_write() {
        let mut s = state();
        s.touch(b"a", 99);
        assert_eq!(s.pending_len(), 1);
        let (keys, ops) = s.pending_ops(7, &[]);
        assert_eq!(keys, vec![b"a".to_vec()]);
        assert_eq!(ops.len(), 1);
        // A key the same commit already records is not written twice.
        assert!(s.pending_ops(7, &[b"a".to_vec()]).1.is_empty());
        let plan = s.plan(
            &[(
                b"d".to_vec(),
                Entry {
                    tick: 100,
                    bytes: 30,
                },
            )],
            &[],
            false,
            s.max_bytes,
            &[b"d".to_vec()],
        );
        // `a` is no longer the oldest, so `b` is shed instead.
        assert_eq!(plan.victims, vec![b"b".to_vec()]);
    }

    #[test]
    fn touching_a_key_we_do_not_hold_records_nothing() {
        let mut s = state();
        s.touch(b"absent", 42);
        assert_eq!(s.pending_len(), 0);
    }
}
