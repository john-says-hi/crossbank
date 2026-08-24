//! Value encoding: the envelope, its filter chain, and the serde layer.
//!
//! Gating and re-exports only.

pub mod api;
pub mod filters;
pub mod value;

pub use api::{Filter, FilterChain, HEADER_LEN, MAGIC, MAX_DECODED_BYTES, VERSION};
pub use filters::{Crc32, Lz4};
pub use value::{decode, encode, type_tag};

/// Id of the chain returned by [`default_chain`].
pub const DEFAULT_CHAIN_ID: u8 = 1;

/// Id of the chain returned by [`FilterChain::checksum_only`].
///
/// Distinct from [`DEFAULT_CHAIN_ID`] and from [`FilterChain::raw`]'s `0`,
/// because the id is what gates format compatibility: two chains that
/// transform bytes differently must never share one.
pub const CHECKSUM_ONLY_CHAIN_ID: u8 = 2;

/// The chain used unless a locker asks for another: LZ4, then CRC32.
///
/// Compression first so the checksum covers what is actually stored — bit rot
/// is then caught before decompression, rather than by feeding a damaged
/// stream to a decompressor.
pub fn default_chain() -> FilterChain {
    FilterChain::new(DEFAULT_CHAIN_ID, vec![Box::new(Lz4), Box::new(Crc32)])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_chain_is_lz4_then_crc32() {
        // Order is load-bearing, so pin it.
        assert_eq!(default_chain().describe(), "chain 1 (lz4 -> crc32)");
    }

    #[test]
    fn the_default_chain_does_not_collide_with_raw() {
        assert_ne!(FilterChain::raw().id(), default_chain().id());
    }
}
