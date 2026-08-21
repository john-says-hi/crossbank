//! Machinery shared by the eager and lazy lockers.
//!
//! Everything here is key/value plumbing over a [`Backend`]: encode a user key
//! into locker-prefixed bytes, seal a value through the filter chain, page a
//! scan to completion. Neither locker type owns any of it.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::{Backend, CommitOptions, Op, ScanRequest, Table};
use crate::codec::{self, FilterChain};
use crate::error::{Error, Result};
use crate::key::{self, LockerId};

use super::chunk::{
    chunk_key, gc_ops, is_pointer, ChunkPointer, ValueIds, FLAG_POSTCARD, FLAG_RAW,
};
use super::lru::Ticks;
use super::policy::LockerConfig;
use crate::coherence::Coherence;
use crate::watch::{Event, Watchers};

/// How many records a single scan page pulls. Bounded because an IndexedDB
/// cursor cannot outlive its transaction, so every scan pages regardless.
pub(crate) const SCAN_PAGE: usize = 256;

/// What every locker a bank opens shares with it.
///
/// Bundled into one handle rather than passed separately so that adding a
/// bank-wide facility does not ripple through every `open` signature in the
/// crate.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    /// Allocates chunk value ids. See [`ValueIds`].
    pub(crate) values: ValueIds,
    /// The logical clock the LRU orders by. See [`Ticks`].
    pub(crate) ticks: Ticks,
    /// The cross-tab channel, or an inert stand-in. See [`crate::coherence`].
    pub(crate) coherence: Coherence,
}

/// What a caller already knows about whatever is stored under a key.
///
/// `put` and `delete` have to GC the chunks of a value they are replacing,
/// and the only way to find those chunks is to read the old record first.
/// That read is a whole extra backend round trip — a second IndexedDB
/// transaction per write — and it is pure waste whenever the caller's own RAM
/// index already settles the question.
///
/// The asymmetry is the point: a wrong `Unknown` costs a read, a wrong
/// `Absent` or `Inline` **orphans chunks forever**. So every producer of this
/// type errs towards `Unknown`, and anything that could make the index
/// over-claim absence — a staged deferred batch, another tab's write — forces
/// it back to `Unknown`. See [`super::resident::Resident::prior`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Prior {
    /// Nothing is known. Read before writing.
    #[default]
    Unknown,
    /// There is provably no record under this key, so there is nothing to GC.
    Absent,
    /// There is a record and it is provably inline, so it names no chunks.
    Inline,
}

impl Prior {
    /// Whether the old record still has to be read to find chunks to GC.
    fn needs_read(self) -> bool {
        matches!(self, Prior::Unknown)
    }
}

pub(crate) struct Inner {
    /// Held for the duration of a transaction, so two overlapping transactions
    /// on one locker cannot lose each other's updates. A futures mutex rather
    /// than a std one because it is held across await points.
    pub(crate) write_lock: futures::lock::Mutex<()>,
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) chain: Arc<FilterChain>,
    pub(crate) id: LockerId,
    pub(crate) name: String,
    pub(crate) config: LockerConfig,
    /// Shared with the bank and with every other locker it opened, so chunk
    /// value ids and LRU ticks are unique bank-wide rather than per handle.
    pub(crate) shared: Arc<Shared>,
    pub(crate) watchers: Watchers,
    /// Set by `Locker::close` / `LazyLocker::close`.
    ///
    /// Lives here rather than on the two locker types because the bank's
    /// open-locker registry holds `Weak<Inner>` and needs to tell a closed
    /// locker from a live one without a back-pointer from the locker to the
    /// bank.
    pub(crate) closed: AtomicBool,
    /// The epoch of this tab's own last commit that touched each key, and the
    /// highest epoch absorbed from another tab. Together they let a coherence
    /// sink refuse news that is not actually newer. See
    /// [`crate::BankConfig::coherence`] for exactly what that orders.
    ///
    /// Only ever written when coherence is on, because it is filled from the
    /// announcement a commit produced and there is none otherwise.
    pub(crate) epochs: Epochs,
}

/// Per-locker epoch bookkeeping for cross-tab coherence.
#[derive(Debug, Default)]
pub(crate) struct Epochs {
    /// Keys this tab has written, and the epoch it wrote them at.
    ///
    /// Bounded: past [`EPOCH_MEMORY`] keys the whole map is dropped rather
    /// than grown without limit. Forgetting fails *open* — this tab simply
    /// stops refusing another tab's news for those keys, which is the same
    /// behaviour as before any of it was recorded.
    local: Mutex<BTreeMap<Vec<u8>, u64>>,
    /// The highest epoch this tab has absorbed from any other tab.
    applied: AtomicU64,
}

/// How many locally-written keys one locker remembers an epoch for.
pub(crate) const EPOCH_MEMORY: usize = 4096;

impl Epochs {
    /// Record that this tab committed `keys` at `epoch`.
    pub(crate) fn note_local(&self, keys: impl Iterator<Item = Vec<u8>>, epoch: u64) {
        let Ok(mut guard) = self.local.lock() else {
            return;
        };
        for key in keys {
            guard.insert(key, epoch);
        }
        if guard.len() > EPOCH_MEMORY {
            guard.clear();
        }
    }

    /// The epoch of this tab's own last commit touching `key`, if remembered.
    pub(crate) fn local(&self, key: &[u8]) -> Option<u64> {
        self.local.lock().ok()?.get(key).copied()
    }

    /// The highest epoch absorbed from another tab.
    pub(crate) fn applied(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// Raise the absorbed watermark. Never lowers it.
    pub(crate) fn note_applied(&self, epoch: u64) {
        self.applied.fetch_max(epoch, Ordering::AcqRel);
    }

    /// Forget every local marker. Used when another tab clears the locker:
    /// nothing this tab wrote survives, so nothing it wrote outranks anything.
    pub(crate) fn forget_local(&self) {
        if let Ok(mut guard) = self.local.lock() {
            guard.clear();
        }
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locker")
            .field("name", &self.name)
            .field("id", &self.id)
            .finish()
    }
}

impl Inner {
    /// Whether this locker has been closed.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Mark closed. Returns true the first time, so callers can make close
    /// idempotent without a second flag.
    pub(crate) fn mark_closed(&self) -> bool {
        !self.closed.swap(true, Ordering::AcqRel)
    }

    /// Refuse an operation on a closed locker.
    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        Ok(())
    }

    pub(crate) fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        key::encode_bytes(self.id, key)
    }

    pub(crate) fn seal<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        codec::encode(value, &self.chain)
    }

    pub(crate) fn open<T: DeserializeOwned>(&self, stored: &[u8]) -> Result<T> {
        codec::decode(stored, &self.chain)
    }

    pub(crate) fn delete_op(&self, key: &[u8]) -> Op {
        Op::Delete {
            table: Table::Records,
            key: self.encode_key(key),
        }
    }

    pub(crate) fn clear_op(&self) -> Op {
        Op::DeleteRange {
            table: Table::Records,
            range: key::locker_range(self.id),
        }
    }

    pub(crate) async fn fetch(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let encoded = self.encode_key(key);
        self.backend.get(Table::Records, &encoded).await
    }

    /// Ask the backend to make this locker's commits durable, when it is the
    /// locker's own setting that left them otherwise.
    ///
    /// A no-op on an `Immediate` locker, where every commit already landed.
    pub(crate) async fn flush_backend(&self) -> Result<()> {
        if !self.config.is_eventual() {
            return Ok(());
        }
        self.backend.flush().await
    }

    /// Commit, then tell the other tabs.
    ///
    /// The news is worked out **before** the commit (the op list is moved into
    /// it) and posted **after** it lands, so no tab is ever told about a write
    /// that failed, and no op list is cloned to make that possible.
    pub(crate) async fn commit(&self, ops: Vec<Op>) -> Result<()> {
        let news = self.shared.coherence.prepare(self.id, &ops);
        self.backend
            .commit_with(
                ops,
                CommitOptions {
                    durability: self.config.durability,
                },
            )
            .await?;
        if let Some(news) = news {
            // Remember what this tab just wrote, and when, so another tab's
            // older news cannot undo it. Recorded before the post so it is in
            // place before any reply can arrive.
            self.epochs
                .note_local(news.changes.iter().map(|c| c.key.clone()), news.epoch);
            if news.cleared {
                self.epochs.forget_local();
            }
            self.shared.coherence.post(news);
        }
        Ok(())
    }

    /// Decode a stored record into postcard (or raw) payload bytes.
    pub(crate) async fn load_payload(&self, stored: &[u8]) -> Result<Vec<u8>> {
        if is_pointer(stored) {
            let pointer = ChunkPointer::parse(stored)?;
            self.read_chunks(&pointer).await
        } else {
            self.chain.open(stored)
        }
    }

    pub(crate) async fn decode_record<T: serde::de::DeserializeOwned>(
        &self,
        stored: &[u8],
    ) -> Result<T> {
        let payload = self.load_payload(stored).await?;
        if is_pointer(stored) && ChunkPointer::parse(stored)?.flags == FLAG_RAW {
            let wrapped = postcard::to_allocvec(&payload)
                .map_err(|e| Error::Filter(format!("postcard wrap of raw chunks failed: {e}")))?;
            postcard::from_bytes(&wrapped)
                .map_err(|e| Error::Corrupt(format!("raw chunk reconstruct failed: {e}")))
        } else {
            postcard::from_bytes(&payload)
                .map_err(|e| Error::Corrupt(format!("postcard deserialisation failed: {e}")))
        }
    }

    pub(crate) async fn load_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>> {
        match self.fetch(key).await? {
            None => Ok(None),
            Some(raw) => Ok(Some(self.decode_record(&raw).await?)),
        }
    }

    pub(crate) async fn read_chunks(&self, pointer: &ChunkPointer) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(pointer.total_len as usize);
        for seq in 0..pointer.n_chunks {
            let key = chunk_key(pointer.value_id, seq);
            let sealed = self
                .backend
                .get(Table::Chunks, &key)
                .await?
                .ok_or_else(|| {
                    Error::Corrupt(format!("missing chunk {seq} of {}", pointer.value_id))
                })?;
            let piece = self.chain.open(&sealed)?;
            out.extend_from_slice(&piece);
        }
        if out.len() as u64 != pointer.total_len {
            return Err(Error::Corrupt(format!(
                "chunked value declared {} bytes, reassembled {}",
                pointer.total_len,
                out.len()
            )));
        }
        Ok(out)
    }

    pub(crate) async fn next_value_id(&self) -> Result<(u64, Op)> {
        self.shared.values.allocate(self.backend.as_ref()).await
    }

    /// Ops to store `postcard` bytes at `key`, chunking when needed, and to
    /// drop any previous chunked value under that key.
    pub(crate) async fn put_payload_ops(
        &self,
        key: &[u8],
        payload: Vec<u8>,
        flags: u8,
        prior: Prior,
    ) -> Result<Vec<Op>> {
        let mut ops = Vec::new();
        if prior.needs_read() {
            if let Some(existing) = self.fetch(key).await? {
                if is_pointer(&existing) {
                    ops.push(gc_ops(&ChunkPointer::parse(&existing)?));
                }
            }
        }

        if payload.len() <= self.config.chunk_size && flags == FLAG_POSTCARD {
            ops.push(Op::Put {
                table: Table::Records,
                key: self.encode_key(key),
                value: self.chain.seal(&payload)?,
            });
            return Ok(ops);
        }

        let (value_id, bump) = self.next_value_id().await?;
        ops.push(bump);

        let chunk_size = self.config.chunk_size;
        let mut seq = 0u32;
        for piece in payload.chunks(chunk_size) {
            ops.push(Op::Put {
                table: Table::Chunks,
                key: chunk_key(value_id, seq),
                value: self.chain.seal(piece)?,
            });
            seq = seq
                .checked_add(1)
                .ok_or_else(|| Error::backend("chunk sequence space is exhausted"))?;
        }

        let pointer = ChunkPointer {
            value_id,
            n_chunks: seq,
            total_len: payload.len() as u64,
            flags,
        };
        ops.push(Op::Put {
            table: Table::Records,
            key: self.encode_key(key),
            value: pointer.encode(),
        });
        Ok(ops)
    }

    pub(crate) async fn delete_value_ops(&self, key: &[u8], prior: Prior) -> Result<Vec<Op>> {
        let mut ops = Vec::new();
        if prior.needs_read() {
            if let Some(existing) = self.fetch(key).await? {
                if is_pointer(&existing) {
                    ops.push(gc_ops(&ChunkPointer::parse(&existing)?));
                }
            }
        }
        ops.push(self.delete_op(key));
        Ok(ops)
    }

    /// Whether a payload of this size and shape will be stored inline.
    ///
    /// The single source of truth for the branch `put_payload_ops` takes, so
    /// the caller's "is this now inline?" bookkeeping cannot drift from what
    /// was actually written.
    pub(crate) fn stores_inline(&self, payload_len: usize, flags: u8) -> bool {
        payload_len <= self.config.chunk_size && flags == FLAG_POSTCARD
    }

    /// Clear this locker's records and every chunk those records pointed at.
    pub(crate) async fn clear_value_ops(&self) -> Result<Vec<Op>> {
        let mut ops = Vec::new();
        self.walk(
            Bound::Unbounded,
            Bound::Unbounded,
            false,
            true,
            |_, value| {
                if let Some(raw) = value {
                    // A pointer we cannot parse is treated as an inline
                    // record: the range delete below still removes it, and
                    // there is nothing to GC because the chunks it named
                    // cannot be located. One damaged record must not make
                    // `clear`, `delete_locker` and `locker_bytes` fail
                    // forever.
                    if is_pointer(&raw) {
                        if let Ok(pointer) = ChunkPointer::parse(&raw) {
                            ops.push(gc_ops(&pointer));
                        }
                    }
                }
                Ok(())
            },
        )
        .await?;
        ops.push(self.clear_op());
        Ok(ops)
    }

    /// Announce a change. Called only AFTER a commit lands, so a subscriber is
    /// never told about a write that did not happen.
    pub(crate) fn announce(&self, event: Event) {
        self.watchers.broadcast(&event);
    }

    /// Walk every record in a range, paging until exhausted.
    ///
    /// The visitor receives the **raw user key bytes**, not a `String`: a key
    /// need not be UTF-8, and a walk must never fail because one is not.
    ///
    /// `want_values` is passed through so a keys-only walk does not make the
    /// backend read payloads it would immediately discard — which matters a
    /// great deal for a lazy locker opening over a large candle cache.
    pub(crate) async fn walk(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        reverse: bool,
        want_values: bool,
        mut visit: impl FnMut(Vec<u8>, Option<Vec<u8>>) -> Result<()>,
    ) -> Result<()> {
        if key::is_degenerate(start, end) {
            return Ok(());
        }
        let mut range = key::encode_range_bytes(self.id, start, end);

        loop {
            let page = self
                .backend
                .scan(ScanRequest {
                    table: Table::Records,
                    range: range.clone(),
                    reverse,
                    limit: SCAN_PAGE,
                    want_values,
                })
                .await?;

            for (encoded, value) in &page.items {
                let user_key = key::decode_bytes(self.id, encoded)?;
                visit(user_key.to_vec(), value.clone())?;
            }

            match page.resume {
                // Excluding the last key returned works identically in both
                // directions, which is what keeps paging free of an
                // off-by-one that would differ per backend.
                Some(last) => {
                    if reverse {
                        range.end = Bound::Excluded(last);
                    } else {
                        range.start = Bound::Excluded(last);
                    }
                }
                None => break,
            }
        }

        Ok(())
    }
}
