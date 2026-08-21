//! Machinery shared by the eager and lazy lockers.
//!
//! Everything here is key/value plumbing over a [`Backend`]: encode a user key
//! into locker-prefixed bytes, seal a value through the filter chain, page a
//! scan to completion. Neither locker type owns any of it.

use std::ops::Bound;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::{Backend, Op, ScanRequest, Table};
use crate::codec::{self, FilterChain};
use crate::error::{Error, Result};
use crate::key::{self, LockerId};

use super::chunk::{
    bump_counter_ops, chunk_key, gc_ops, is_pointer, parse_counter, ChunkPointer, FLAG_POSTCARD,
    FLAG_RAW,
};
use super::policy::LockerConfig;
use crate::watch::{Event, Watchers};

/// How many records a single scan page pulls. Bounded because an IndexedDB
/// cursor cannot outlive its transaction, so every scan pages regardless.
pub(crate) const SCAN_PAGE: usize = 256;

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
    pub(crate) watchers: Watchers,
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
    pub(crate) fn encode_key(&self, key: &str) -> Vec<u8> {
        key::encode(self.id, key)
    }

    pub(crate) fn seal<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        codec::encode(value, &self.chain)
    }

    pub(crate) fn open<T: DeserializeOwned>(&self, stored: &[u8]) -> Result<T> {
        codec::decode(stored, &self.chain)
    }

    pub(crate) fn delete_op(&self, key: &str) -> Op {
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

    pub(crate) async fn fetch(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let encoded = self.encode_key(key);
        self.backend.get(Table::Records, &encoded).await
    }

    pub(crate) async fn commit(&self, ops: Vec<Op>) -> Result<()> {
        self.backend.commit(ops).await
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
        key: &str,
    ) -> Result<Option<T>> {
        match self.fetch(key).await? {
            None => Ok(None),
            Some(raw) => Ok(Some(self.decode_record(&raw).await?)),
        }
    }

    async fn read_chunks(&self, pointer: &ChunkPointer) -> Result<Vec<u8>> {
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
        let raw = self
            .backend
            .get(Table::Meta, super::chunk::next_value_id_key())
            .await?;
        let current = parse_counter(raw.as_deref())?;
        bump_counter_ops(current)
    }

    /// Ops to store `postcard` bytes at `key`, chunking when needed, and to
    /// drop any previous chunked value under that key.
    pub(crate) async fn put_payload_ops(
        &self,
        key: &str,
        payload: Vec<u8>,
        flags: u8,
    ) -> Result<Vec<Op>> {
        let mut ops = Vec::new();
        if let Some(existing) = self.fetch(key).await? {
            if is_pointer(&existing) {
                ops.push(gc_ops(&ChunkPointer::parse(&existing)?));
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

    pub(crate) async fn delete_value_ops(&self, key: &str) -> Result<Vec<Op>> {
        let mut ops = Vec::new();
        if let Some(existing) = self.fetch(key).await? {
            if is_pointer(&existing) {
                ops.push(gc_ops(&ChunkPointer::parse(&existing)?));
            }
        }
        ops.push(self.delete_op(key));
        Ok(ops)
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
                    if is_pointer(&raw) {
                        ops.push(gc_ops(&ChunkPointer::parse(&raw)?));
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
    /// `want_values` is passed through so a keys-only walk does not make the
    /// backend read payloads it would immediately discard — which matters a
    /// great deal for a lazy locker opening over a large candle cache.
    pub(crate) async fn walk(
        &self,
        start: Bound<&str>,
        end: Bound<&str>,
        reverse: bool,
        want_values: bool,
        mut visit: impl FnMut(String, Option<Vec<u8>>) -> Result<()>,
    ) -> Result<()> {
        let mut range = key::encode_range(self.id, start, end);

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
                let user_key = key::decode(self.id, encoded)?;
                visit(user_key.to_string(), value.clone())?;
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
