//! Streaming writer and reader for large `Vec<u8>` values.
//!
//! A `Writer` is not part of `transact()`. It spans many commits. Dropping it
//! without [`Writer::finish`] leaves the previous complete value intact;
//! orphan chunks from the unfinished write are dropped on [`Writer::abort`].
//! Drop without abort leaks those orphans until a later overwrite.

use std::sync::Arc;

use crate::error::Result;

use super::chunk::{chunk_key, gc_ops, ChunkPointer, FLAG_RAW};
use super::inner::Inner;
use super::lazy::LazyLocker;
use crate::backend::api::{Op, Table};
use crate::watch::Event;

/// Appends raw bytes to a key. Obtain from [`LazyLocker<Vec<u8>>::writer`].
pub struct Writer {
    inner: Arc<Inner>,
    key: String,
    value_id: u64,
    seq: u32,
    total_len: u64,
    buf: Vec<u8>,
    finished: bool,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("key", &self.key)
            .field("bytes", &self.total_len)
            .finish()
    }
}

impl Writer {
    pub(crate) async fn start(inner: Arc<Inner>, key: String) -> Result<Self> {
        let (value_id, bump) = inner.next_value_id().await?;
        inner.commit(vec![bump]).await?;
        Ok(Self {
            inner,
            key,
            value_id,
            seq: 0,
            total_len: 0,
            buf: Vec::new(),
            finished: false,
        })
    }

    /// Append `bytes`. May flush a full chunk to storage.
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.buf.extend_from_slice(bytes);
        self.total_len += bytes.len() as u64;
        let chunk_size = self.inner.config.chunk_size;
        while self.buf.len() >= chunk_size {
            let piece: Vec<u8> = self.buf.drain(..chunk_size).collect();
            self.flush_piece(&piece).await?;
        }
        Ok(())
    }

    async fn flush_piece(&mut self, piece: &[u8]) -> Result<()> {
        let op = Op::Put {
            table: Table::Chunks,
            key: chunk_key(self.value_id, self.seq),
            value: self.inner.chain.seal(piece)?,
        };
        self.inner.commit(vec![op]).await?;
        self.seq += 1;
        Ok(())
    }

    /// Publish the value. The previous complete value is replaced atomically
    /// from the caller's point of view: the records pointer swaps in one commit
    /// together with leftover chunk bytes and GC of the old pointer.
    pub async fn finish(mut self) -> Result<()> {
        if !self.buf.is_empty() {
            let leftover = std::mem::take(&mut self.buf);
            self.flush_piece(&leftover).await?;
        }
        let mut ops = Vec::new();
        if let Some(existing) = self.inner.fetch(&self.key).await? {
            if super::chunk::is_pointer(&existing) {
                ops.push(gc_ops(&ChunkPointer::parse(&existing)?));
            }
        }
        let pointer = ChunkPointer {
            value_id: self.value_id,
            n_chunks: self.seq,
            total_len: self.total_len,
            flags: FLAG_RAW,
        };
        ops.push(Op::Put {
            table: Table::Records,
            key: self.inner.encode_key(&self.key),
            value: pointer.encode(),
        });
        self.inner.commit(ops).await?;
        self.inner.announce(Event::Put {
            key: self.key.clone(),
        });
        self.finished = true;
        Ok(())
    }

    /// Drop this write and delete the chunks already flushed. The previous
    /// complete value is unchanged.
    pub async fn abort(mut self) -> Result<()> {
        let pointer = ChunkPointer {
            value_id: self.value_id,
            n_chunks: self.seq,
            total_len: 0,
            flags: FLAG_RAW,
        };
        self.inner.commit(vec![gc_ops(&pointer)]).await?;
        self.finished = true;
        Ok(())
    }
}

/// Yields chunk payloads without holding the whole value.
pub struct Reader {
    inner: Arc<Inner>,
    pointer: Option<ChunkPointer>,
    seq: u32,
    /// Inline values are yielded as a single piece, then exhausted.
    inline: Option<Vec<u8>>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader").field("len", &self.len()).finish()
    }
}

impl Reader {
    pub(crate) fn from_pointer(inner: Arc<Inner>, pointer: ChunkPointer) -> Self {
        Self {
            inner,
            pointer: Some(pointer),
            seq: 0,
            inline: None,
        }
    }

    pub(crate) fn from_inline(inner: Arc<Inner>, payload: Vec<u8>) -> Self {
        let total = payload.len() as u64;
        Self {
            inner,
            pointer: Some(ChunkPointer {
                value_id: 0,
                n_chunks: 0,
                total_len: total,
                flags: FLAG_RAW,
            }),
            seq: 0,
            inline: Some(payload),
        }
    }

    /// Total bytes of the stored value.
    pub fn len(&self) -> u64 {
        self.pointer.map(|p| p.total_len).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Next chunk, or `None` at the end. Never larger than `chunk_size`
    /// for a chunked value; an inline value is a single piece.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(payload) = self.inline.take() {
            return Ok(Some(payload));
        }
        let Some(pointer) = self.pointer else {
            return Ok(None);
        };
        if self.seq >= pointer.n_chunks {
            return Ok(None);
        }
        let key = chunk_key(pointer.value_id, self.seq);
        let sealed = self
            .inner
            .backend
            .get(Table::Chunks, &key)
            .await?
            .ok_or_else(|| {
                crate::error::Error::Corrupt(format!(
                    "missing chunk {} of {}",
                    self.seq, pointer.value_id
                ))
            })?;
        self.seq += 1;
        Ok(Some(self.inner.chain.open(&sealed)?))
    }
}

impl LazyLocker<Vec<u8>> {
    /// Stream a large value into `key`. Not part of a `transact()` closure.
    pub async fn writer(&self, key: &str) -> Result<Writer> {
        let _guard = self.inner.write_lock.lock().await;
        Writer::start(self.inner.clone(), key.to_string()).await
    }

    /// Stream a stored value out. `None` if the key is missing.
    pub async fn reader(&self, key: &str) -> Result<Option<Reader>> {
        let Some(raw) = self.inner.fetch(key).await? else {
            return Ok(None);
        };
        if super::chunk::is_pointer(&raw) {
            let pointer = super::chunk::ChunkPointer::parse(&raw)?;
            Ok(Some(Reader::from_pointer(self.inner.clone(), pointer)))
        } else {
            let payload = self.inner.chain.open(&raw)?;
            Ok(Some(Reader::from_inline(self.inner.clone(), payload)))
        }
    }
}
