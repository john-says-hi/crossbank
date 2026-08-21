//! Chunked values: a pointer in `records` and payload pieces in `chunks`.
//!
//! Small values stay inline (the existing CBNK envelope in `records`). A value
//! whose postcard payload exceeds `LockerConfig::chunk_size` is split; each
//! piece is sealed through the filter chain independently so peak memory is
//! O(chunk_size) on the read path.

use crate::backend::api::{KeyRange, Op, Table};
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
        Ok(Self {
            flags: raw[5],
            value_id: u64::from_be_bytes(raw[6..14].try_into().expect("length checked above")),
            n_chunks: u32::from_be_bytes(raw[14..18].try_into().expect("length checked above")),
            total_len: u64::from_be_bytes(raw[18..26].try_into().expect("length checked above")),
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

    #[test]
    fn inline_envelopes_are_not_pointers() {
        assert!(!is_pointer(b"CBNK...."));
        assert!(!is_pointer(b"short"));
    }
}
