//! Chunked values: a pointer in `records` and payload pieces in `chunks`.
//!
//! Small values stay inline (the existing CBNK envelope in `records`). A value
//! whose postcard payload exceeds `LockerConfig::chunk_size` is split; each
//! piece is sealed through the filter chain independently so peak memory is
//! O(chunk_size) on the read path.

use crate::backend::api::{Backend, KeyRange, Op, Table};
use crate::codec::MAX_DECODED_BYTES;
use crate::error::{Error, Result};

/// Magic for a chunk pointer stored in `records`. Distinct from the inline
/// envelope magic `CBNK`, so the load path can branch on four bytes.
pub const POINTER_MAGIC: [u8; 4] = *b"CCHK";
pub const POINTER_VERSION: u8 = 1;
pub const POINTER_LEN: usize = 26;

/// The stored payload is postcard-encoded `T`.
pub const FLAG_POSTCARD: u8 = 0;
/// The stored payload is raw bytes (a `Writer` over `Vec<u8>`).
pub const FLAG_RAW: u8 = 1;

const META_NEXT_VALUE_ID: &[u8] = b"next_value_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPointer {
    pub value_id: u64,
    pub n_chunks: u32,
    pub total_len: u64,
    pub flags: u8,
}

impl ChunkPointer {
    pub fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(POINTER_LEN);
        out.extend_from_slice(&POINTER_MAGIC);
        out.push(POINTER_VERSION);
        out.push(self.flags);
        out.extend_from_slice(&self.value_id.to_be_bytes());
        out.extend_from_slice(&self.n_chunks.to_be_bytes());
        out.extend_from_slice(&self.total_len.to_be_bytes());
        out
    }

    pub fn parse(raw: &[u8]) -> Result<Self> {
        if raw.len() != POINTER_LEN || raw[0..4] != POINTER_MAGIC {
            return Err(Error::Corrupt("not a chunk pointer".into()));
        }
        if raw[4] != POINTER_VERSION {
            return Err(Error::UnsupportedVersion {
                found: raw[4],
                supported: POINTER_VERSION,
            });
        }
        let n_chunks = u32::from_be_bytes(raw[14..18].try_into().expect("length checked above"));
        let total_len = u64::from_be_bytes(raw[18..26].try_into().expect("length checked above"));
        // Both numbers are read straight off storage and both size an
        // allocation on the read path — the reassembly buffer from
        // `total_len`, the key list from `n_chunks`. A corrupt or hostile
        // pointer claiming `u32::MAX` chunks would ask for gigabytes, and a
        // wasm release build is `panic=abort`, so the allocation failure is an
        // unrecoverable app kill rather than an error a caller can see.
        //
        // The chunk size is not in the pointer, so it cannot be checked
        // exactly. Two bounds hold regardless of it: the value cannot decode
        // past the envelope's own ceiling, and every chunk holds at least one
        // byte. `n_chunks == 0` stays legal because a `Writer` closed without
        // a single `write` stores exactly that.
        if total_len > MAX_DECODED_BYTES as u64 {
            return Err(Error::Corrupt(format!(
                "chunk pointer declares {total_len} bytes, over the \
                 {MAX_DECODED_BYTES} byte limit"
            )));
        }
        if u64::from(n_chunks) > total_len {
            return Err(Error::Corrupt(format!(
                "chunk pointer declares {n_chunks} chunks for {total_len} bytes; \
                 a chunk holds at least one byte"
            )));
        }
        Ok(Self {
            flags: raw[5],
            value_id: u64::from_be_bytes(raw[6..14].try_into().expect("length checked above")),
            n_chunks,
            total_len,
        })
    }
}

pub fn is_pointer(raw: &[u8]) -> bool {
    raw.len() >= 4 && raw[0..4] == POINTER_MAGIC
}

/// `value_id: u64 BE || seq: u32 BE`
pub fn chunk_key(value_id: u64, seq: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(12);
    k.extend_from_slice(&value_id.to_be_bytes());
    k.extend_from_slice(&seq.to_be_bytes());
    k
}

pub fn chunk_range(value_id: u64) -> KeyRange {
    KeyRange::prefix(&value_id.to_be_bytes())
}

pub fn next_value_id_key() -> &'static [u8] {
    META_NEXT_VALUE_ID
}

pub fn bump_counter_ops(current: u64) -> Result<(u64, Op)> {
    let id = current;
    let next = current
        .checked_add(1)
        .ok_or_else(|| Error::backend("value id space is exhausted"))?;
    let op = Op::Put {
        table: Table::Meta,
        key: META_NEXT_VALUE_ID.to_vec(),
        value: next.to_be_bytes().to_vec(),
    };
    Ok((id, op))
}

pub fn parse_counter(raw: Option<&[u8]>) -> Result<u64> {
    match raw {
        None => Ok(0),
        Some(bytes) => <[u8; 8]>::try_from(bytes)
            .map(u64::from_be_bytes)
            .map_err(|_| Error::Corrupt("next_value_id is not an 8-byte integer".into())),
    }
}

/// The bank-wide allocator for chunk value ids.
///
/// One of these is owned by a [`crate::Bank`] and cloned into every locker it
/// opens, so two lockers — or two handles on the same name — can never hand
/// out the same id and collide in the `chunks` table. Reading the stored
/// counter and bumping it used to be two separate awaits per locker, which is
/// exactly the interleaving that produced duplicate ids.
///
/// The cursor is seeded from `meta` on first use and then lives in RAM. The
/// lock is a `std` mutex and is **never** held across an await: allocation is
/// pure arithmetic, and the returned `Op` persists the new high-water mark
/// with whatever commit the caller is already building.
#[derive(Debug, Default)]
pub struct ValueIds {
    /// The next id to hand out. `None` until seeded from `meta`.
    next: std::sync::Mutex<Option<u64>>,
}

impl ValueIds {
    /// Take the next id if the cursor is already seeded.
    fn take(&self) -> Result<Option<u64>> {
        let mut guard = self
            .next
            .lock()
            .map_err(|_| Error::backend("value id cursor was poisoned"))?;
        match *guard {
            None => Ok(None),
            Some(id) => {
                *guard = Some(advance(id)?);
                Ok(Some(id))
            }
        }
    }

    /// Seed from the stored counter and take one.
    ///
    /// `max` rather than a plain assignment because a concurrent allocation
    /// may have seeded the cursor while this one was awaiting the read; the
    /// cursor must only ever move forward.
    fn seed_and_take(&self, stored: u64) -> Result<u64> {
        let mut guard = self
            .next
            .lock()
            .map_err(|_| Error::backend("value id cursor was poisoned"))?;
        let id = match *guard {
            Some(current) => current.max(stored),
            None => stored,
        };
        *guard = Some(advance(id)?);
        Ok(id)
    }

    /// Allocate one id, plus the op that persists the new counter.
    pub async fn allocate(&self, backend: &dyn Backend) -> Result<(u64, Op)> {
        if let Some(id) = self.take()? {
            return bump_counter_ops(id);
        }
        let raw = backend.get(Table::Meta, META_NEXT_VALUE_ID).await?;
        let stored = parse_counter(raw.as_deref())?;
        let id = self.seed_and_take(stored)?;
        bump_counter_ops(id)
    }
}

fn advance(id: u64) -> Result<u64> {
    id.checked_add(1)
        .ok_or_else(|| Error::backend("value id space is exhausted"))
}

pub fn gc_ops(pointer: &ChunkPointer) -> Op {
    Op::DeleteRange {
        table: Table::Chunks,
        range: chunk_range(pointer.value_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_round_trips() {
        let p = ChunkPointer {
            value_id: 42,
            n_chunks: 3,
            total_len: 1000,
            flags: FLAG_POSTCARD,
        };
        let encoded = p.encode();
        assert_eq!(encoded.len(), POINTER_LEN);
        assert!(is_pointer(&encoded));
        assert_eq!(ChunkPointer::parse(&encoded).unwrap(), p);
    }

    /// A pointer is read straight off storage, and both of its numbers size
    /// an allocation. A corrupt one claiming `u32::MAX` chunks must be
    /// refused before anything is reserved — on wasm the allocation failure
    /// would be a `panic=abort` app kill, not an error anyone can catch.
    #[test]
    fn an_absurd_chunk_count_is_refused() {
        let raw = ChunkPointer {
            value_id: 42,
            n_chunks: u32::MAX,
            total_len: 1000,
            flags: FLAG_POSTCARD,
        }
        .encode();
        assert!(matches!(ChunkPointer::parse(&raw), Err(Error::Corrupt(_))));
    }

    #[test]
    fn a_length_past_the_envelope_ceiling_is_refused() {
        let raw = ChunkPointer {
            value_id: 42,
            n_chunks: 1,
            total_len: MAX_DECODED_BYTES as u64 + 1,
            flags: FLAG_POSTCARD,
        }
        .encode();
        assert!(matches!(ChunkPointer::parse(&raw), Err(Error::Corrupt(_))));
    }

    /// A `Writer` closed without a single `write` stores exactly this, so it
    /// must stay legal.
    #[test]
    fn an_empty_streamed_value_is_still_a_legal_pointer() {
        let p = ChunkPointer {
            value_id: 7,
            n_chunks: 0,
            total_len: 0,
            flags: FLAG_RAW,
        };
        assert_eq!(ChunkPointer::parse(&p.encode()).unwrap(), p);
    }

    #[test]
    fn inline_envelopes_are_not_pointers() {
        assert!(!is_pointer(b"CBNK...."));
        assert!(!is_pointer(b"short"));
    }
}
