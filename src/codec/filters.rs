//! The filters crossbank ships.
//!
//! Compression and checksumming only. No cipher — crossbank deliberately
//! ships none, so that key handling stays with the application that owns the
//! keys. Implement [`Filter`](super::api::Filter) to add one.

use super::api::{Filter, MAX_DECODED_BYTES};
use crate::error::{Error, Result};

/// LZ4 compression, length-prefixed.
///
/// Whether this earns its CPU is workload-dependent — densely packed `f64`
/// series compress close to 1.0x — which is why the chain is configurable per
/// locker rather than mandatory.
#[derive(Debug, Default, Clone, Copy)]
pub struct Lz4;

impl Filter for Lz4 {
    fn name(&self) -> &'static str {
        "lz4"
    }

    fn forward(&self, input: &[u8]) -> Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(input))
    }

    fn reverse(&self, input: &[u8]) -> Result<Vec<u8>> {
        // Check the declared size before allocating. A corrupt length field is
        // otherwise a straight path to an out-of-memory abort, which on wasm
        // takes the whole tab with it.
        let declared = input
            .get(..4)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| Error::Corrupt("lz4 payload has no length prefix".into()))?
            as usize;

        if declared > MAX_DECODED_BYTES {
            return Err(Error::Corrupt(format!(
                "lz4 payload declares {declared} bytes, over the {MAX_DECODED_BYTES} byte limit"
            )));
        }

        lz4_flex::decompress_size_prepended(input)
            .map_err(|e| Error::Corrupt(format!("lz4 decompression failed: {e}")))
    }
}

/// Appends a CRC32 of the input and verifies it on the way back.
///
/// Placed *after* compression in the default chain so the checksum covers the
/// stored bytes. Corruption is then caught before decompression rather than
/// after, which matters because feeding a damaged stream to a decompressor is
/// how you get wild allocations.
#[derive(Debug, Default, Clone, Copy)]
pub struct Crc32;

impl Filter for Crc32 {
    fn name(&self) -> &'static str {
        "crc32"
    }

    fn forward(&self, input: &[u8]) -> Result<Vec<u8>> {
        let sum = crc32fast::hash(input);
        let mut out = Vec::with_capacity(input.len() + 4);
        out.extend_from_slice(input);
        out.extend_from_slice(&sum.to_le_bytes());
        Ok(out)
    }

    fn reverse(&self, input: &[u8]) -> Result<Vec<u8>> {
        if input.len() < 4 {
            return Err(Error::Corrupt("value is too short to carry a CRC32".into()));
        }
        let split = input.len() - 4;
        let (body, tail) = input.split_at(split);
        let expected = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
        let actual = crc32fast::hash(body);
        if expected != actual {
            return Err(Error::Corrupt(format!(
                "CRC32 mismatch: stored {expected:#010x}, computed {actual:#010x}"
            )));
        }
        Ok(body.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_round_trips_compressible_data() {
        let payload = vec![7u8; 100_000];
        let packed = Lz4.forward(&payload).unwrap();
        assert!(
            packed.len() < payload.len(),
            "repetitive data should shrink"
        );
        assert_eq!(Lz4.reverse(&packed).unwrap(), payload);
    }

    #[test]
    fn lz4_round_trips_incompressible_data() {
        // Compression can legitimately grow a payload; the round trip must
        // still hold.
        let payload: Vec<u8> = (0..2048u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let packed = Lz4.forward(&payload).unwrap();
        assert_eq!(Lz4.reverse(&packed).unwrap(), payload);
    }

    #[test]
    fn lz4_round_trips_empty() {
        let packed = Lz4.forward(b"").unwrap();
        assert_eq!(Lz4.reverse(&packed).unwrap(), b"");
    }

    #[test]
    fn lz4_rejects_an_absurd_declared_size_without_allocating() {
        // A corrupt length field must be refused, not turned into a 4 GiB
        // allocation that aborts the process.
        let mut bogus = u32::MAX.to_le_bytes().to_vec();
        bogus.extend_from_slice(b"garbage");
        match Lz4.reverse(&bogus) {
            Err(Error::Corrupt(msg)) => assert!(msg.contains("limit"), "unexpected: {msg}"),
            other => panic!("expected a corrupt-size error, got {other:?}"),
        }
    }

    #[test]
    fn crc32_round_trips_and_adds_four_bytes() {
        let out = Crc32.forward(b"hello").unwrap();
        assert_eq!(out.len(), 9);
        assert_eq!(Crc32.reverse(&out).unwrap(), b"hello");
    }

    #[test]
    fn crc32_catches_a_single_flipped_bit() {
        // The whole point of carrying a checksum.
        let mut out = Crc32.forward(b"important payload").unwrap();
        out[3] ^= 0b0000_0001;
        assert!(matches!(Crc32.reverse(&out), Err(Error::Corrupt(_))));
    }

    #[test]
    fn crc32_catches_a_corrupted_checksum() {
        let mut out = Crc32.forward(b"payload").unwrap();
        let last = out.len() - 1;
        out[last] ^= 0xFF;
        assert!(matches!(Crc32.reverse(&out), Err(Error::Corrupt(_))));
    }

    #[test]
    fn crc32_rejects_a_truncated_value() {
        assert!(matches!(Crc32.reverse(&[1, 2]), Err(Error::Corrupt(_))));
    }

    #[test]
    fn crc32_round_trips_empty() {
        let out = Crc32.forward(b"").unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(Crc32.reverse(&out).unwrap(), b"");
    }
}
