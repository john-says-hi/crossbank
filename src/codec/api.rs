//! The value envelope and its filter chain.
//!
//! A stored value is serialised once, then pushed through an ordered chain of
//! byte-to-byte transforms, then wrapped in a small fixed header.
//!
//! # Why one `Filter` trait instead of separate codec and cipher traits
//!
//! Compression, checksumming and encryption are all `&[u8] -> Vec<u8>` with an
//! inverse. Giving them one trait means one thing to test, one thing to
//! document, and — crucially — one **chain id** in the header that gates format
//! compatibility. A `Codec` trait with a generic `encode<T: Serialize>` method
//! would not be object-safe anyway, so the serde step has to sit outside the
//! chain regardless.
//!
//! # Header
//!
//! ```text
//! [0..4] magic "CBNK"
//! [4]    envelope version
//! [5]    filter chain id
//! [6..]  payload
//! ```
//!
//! Hand-rolled rather than serialised. Nesting one serialisation inside another
//! copies the payload an extra time, and on a 32-bit target whose linear memory
//! never shrinks, a transient extra copy of a large value permanently raises
//! the process's memory ceiling.
//!
//! The chain id is what makes a format change survivable: data written under a
//! different chain fails loudly on open instead of being fed to the wrong
//! inverse transform.

use crate::backend::api::{MaybeSend, MaybeSync};
use crate::error::{Error, Result};

/// Envelope magic. Present so a foreign or truncated blob is rejected before
/// anything tries to interpret it.
pub const MAGIC: [u8; 4] = *b"CBNK";

/// Current envelope version.
pub const VERSION: u8 = 1;

/// Bytes of fixed header preceding the payload.
pub const HEADER_LEN: usize = 6;

/// The fixed header for `chain_id`, as bytes.
const fn header(chain_id: u8) -> [u8; HEADER_LEN] {
    [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], VERSION, chain_id]
}

/// Refuse to allocate more than this when decoding, so a corrupt length field
/// cannot be turned into an out-of-memory abort.
pub const MAX_DECODED_BYTES: usize = 256 * 1024 * 1024;

/// A reversible byte transform.
///
/// Implement this to plug in encryption: crossbank ships no cipher of its own,
/// deliberately, so that key handling and its audit burden stay with the
/// application that owns the keys.
pub trait Filter: MaybeSend + MaybeSync + 'static {
    /// Short name, for diagnostics only. Not part of the format.
    fn name(&self) -> &'static str;

    /// Applied on the way to storage.
    fn forward(&self, input: &[u8]) -> Result<Vec<u8>>;

    /// Applied on the way back. Must invert [`Filter::forward`] exactly.
    fn reverse(&self, input: &[u8]) -> Result<Vec<u8>>;

    /// [`Filter::forward`], handed a buffer it may consume.
    ///
    /// The chain owns its intermediate buffers, so it can pass them in rather
    /// than lend them. A filter whose output is its input plus or minus a few
    /// bytes — a checksum, a length prefix, padding — can then work in place
    /// instead of allocating and copying a whole second buffer per value.
    ///
    /// The default forwards to the borrowing version, so implementing this is
    /// optional and never changes behaviour, only allocation.
    fn forward_owned(&self, input: Vec<u8>) -> Result<Vec<u8>> {
        self.forward(&input)
    }

    /// [`Filter::reverse`], handed a buffer it may consume. See
    /// [`Filter::forward_owned`].
    fn reverse_owned(&self, input: Vec<u8>) -> Result<Vec<u8>> {
        self.reverse(&input)
    }
}

impl std::fmt::Debug for dyn Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Filter")
            .field("name", &self.name())
            .finish()
    }
}

/// An ordered chain of filters, plus the id recorded in every value it writes.
///
/// The id is the compatibility gate. Two chains that transform bytes
/// differently must never share an id, or data written by one will be handed
/// to the other's inverse and decode into plausible garbage.
pub struct FilterChain {
    id: u8,
    filters: Vec<Box<dyn Filter>>,
}

impl std::fmt::Debug for FilterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterChain")
            .field("id", &self.id)
            .field(
                "filters",
                &self.filters.iter().map(|x| x.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl FilterChain {
    /// Build a chain. `id` must be unique across every chain an application
    /// ever writes with.
    pub fn new(id: u8, filters: Vec<Box<dyn Filter>>) -> Self {
        Self { id, filters }
    }

    /// A chain that stores bytes exactly as given. Useful for already-compressed
    /// or already-encrypted payloads, where a second pass is wasted work.
    pub fn raw() -> Self {
        Self::new(0, Vec::new())
    }

    /// A chain that checksums and nothing else: CRC32, no compression.
    ///
    /// The middle option between [`FilterChain::raw`], which detects no
    /// corruption at all, and [`crate::codec::default_chain`], which pays LZ4
    /// for every value. Reach for it where the payload will not compress —
    /// densely packed floats, already-compressed media, encrypted blobs — but
    /// bit rot should still be caught rather than decoded.
    ///
    /// ```
    /// use crossbank::FilterChain;
    ///
    /// let chain = FilterChain::checksum_only();
    /// assert_eq!(chain.describe(), "chain 2 (crc32)");
    ///
    /// let sealed = chain.seal_slice(b"incompressible").unwrap();
    /// assert_eq!(chain.open(&sealed).unwrap(), b"incompressible");
    /// ```
    pub fn checksum_only() -> Self {
        Self::new(
            crate::codec::CHECKSUM_ONLY_CHAIN_ID,
            vec![Box::new(super::filters::Crc32)],
        )
    }

    pub fn id(&self) -> u8 {
        self.id
    }

    /// Human-readable description, for error messages.
    pub fn describe(&self) -> String {
        if self.filters.is_empty() {
            return format!("chain {} (raw)", self.id);
        }
        format!(
            "chain {} ({})",
            self.id,
            self.filters
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
                .join(" -> ")
        )
    }

    /// Wrap `payload` for storage.
    ///
    /// Takes the payload **by value** so it can be handed down the chain
    /// rather than copied into it. An empty chain — [`FilterChain::raw`], the
    /// right choice for already-compressed or already-encrypted bytes — then
    /// costs no payload copy at all, where it used to cost two: one to clone
    /// the borrowed slice and one to append it to the output buffer.
    pub fn seal(&self, payload: Vec<u8>) -> Result<Vec<u8>> {
        let mut body = payload;
        for filter in &self.filters {
            body = filter.forward_owned(body)?;
        }

        // Prepended in place. Building a second buffer and copying the body
        // into it would cost an allocation the size of the whole value on top
        // of the same byte movement.
        body.splice(0..0, header(self.id));
        Ok(body)
    }

    /// [`FilterChain::seal`] for a caller that only has a borrow.
    ///
    /// Exactly one copy, made once at the boundary, which is the least a
    /// borrowed payload can cost.
    pub fn seal_slice(&self, payload: &[u8]) -> Result<Vec<u8>> {
        self.seal(payload.to_vec())
    }

    /// Unwrap a stored value.
    pub fn open(&self, stored: &[u8]) -> Result<Vec<u8>> {
        if stored.len() < HEADER_LEN {
            return Err(Error::Corrupt(format!(
                "value is {} bytes, shorter than the {HEADER_LEN} byte header",
                stored.len()
            )));
        }
        if stored[0..4] != MAGIC {
            return Err(Error::Corrupt(
                "value does not carry the crossbank magic".into(),
            ));
        }

        let version = stored[4];
        if version != VERSION {
            return Err(Error::UnsupportedVersion {
                found: version,
                supported: VERSION,
            });
        }

        let chain = stored[5];
        if chain != self.id {
            // Loud, not silent. Decoding under the wrong chain would run the
            // wrong inverse transform and could yield a valid-looking value.
            return Err(Error::SchemaMismatch {
                stored: format!("chain {chain}"),
                requested: self.describe(),
            });
        }

        // One copy out of the stored slice, then every filter after that
        // works on a buffer it owns and may consume.
        let mut body = stored[HEADER_LEN..].to_vec();
        for filter in self.filters.iter().rev() {
            body = filter.reverse_owned(body)?;
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A filter that is its own inverse, for exercising chain mechanics.
    struct Xor(u8);

    impl Filter for Xor {
        fn name(&self) -> &'static str {
            "xor"
        }
        fn forward(&self, input: &[u8]) -> Result<Vec<u8>> {
            Ok(input.iter().map(|b| b ^ self.0).collect())
        }
        fn reverse(&self, input: &[u8]) -> Result<Vec<u8>> {
            self.forward(input)
        }
    }

    /// Appends a byte on the way out and strips it on the way back, so filter
    /// ORDER is observable.
    struct Tag(u8);

    impl Filter for Tag {
        fn name(&self) -> &'static str {
            "tag"
        }
        fn forward(&self, input: &[u8]) -> Result<Vec<u8>> {
            let mut v = input.to_vec();
            v.push(self.0);
            Ok(v)
        }
        fn reverse(&self, input: &[u8]) -> Result<Vec<u8>> {
            let mut v = input.to_vec();
            match v.pop() {
                Some(b) if b == self.0 => Ok(v),
                other => Err(Error::Corrupt(format!("tag mismatch: {other:?}"))),
            }
        }
    }

    /// Sealing an owned payload and sealing a borrowed one are the same
    /// operation. `seal` prepends the header in place while `seal_slice`
    /// copies first, so the two could drift byte for byte.
    #[test]
    fn seal_owned_and_seal_slice_produce_identical_bytes() {
        for chain in [FilterChain::raw(), crate::codec::default_chain()] {
            for len in [0usize, 1, 6, 7, 1000] {
                let payload: Vec<u8> = (0..len).map(|i| (i * 17 % 253) as u8).collect();
                assert_eq!(
                    chain.seal(payload.clone()).unwrap(),
                    chain.seal_slice(&payload).unwrap(),
                    "{} disagreed at {len} bytes",
                    chain.describe()
                );
                let sealed = chain.seal(payload.clone()).unwrap();
                assert_eq!(&sealed[0..4], &MAGIC, "the header must survive splicing");
                assert_eq!(sealed[4], VERSION);
                assert_eq!(sealed[5], chain.id());
                assert_eq!(chain.open(&sealed).unwrap(), payload);
            }
        }
    }

    #[test]
    fn raw_chain_round_trips() {
        let c = FilterChain::raw();
        let sealed = c.seal_slice(b"hello").unwrap();
        assert_eq!(&sealed[0..4], &MAGIC);
        assert_eq!(c.open(&sealed).unwrap(), b"hello");
    }

    #[test]
    fn an_empty_payload_round_trips() {
        // Must stay distinguishable from a missing value everywhere it travels.
        let c = FilterChain::raw();
        let sealed = c.seal_slice(b"").unwrap();
        assert_eq!(sealed.len(), HEADER_LEN);
        assert_eq!(c.open(&sealed).unwrap(), b"");
    }

    #[test]
    fn filters_reverse_in_the_opposite_order() {
        // Tag(1) then Tag(2) writes ...0102; reversing must strip 2 first.
        let c = FilterChain::new(1, vec![Box::new(Tag(1)), Box::new(Tag(2))]);
        let sealed = c.seal_slice(b"x").unwrap();
        assert_eq!(&sealed[HEADER_LEN..], &[b'x', 1, 2]);
        assert_eq!(c.open(&sealed).unwrap(), b"x");
    }

    #[test]
    fn a_foreign_blob_is_rejected() {
        let c = FilterChain::raw();
        assert!(matches!(
            c.open(b"not a crossbank value"),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn a_truncated_value_is_an_error_not_a_panic() {
        let c = FilterChain::raw();
        assert!(matches!(c.open(b"CB"), Err(Error::Corrupt(_))));
    }

    #[test]
    fn a_future_version_is_refused_by_version_not_misread() {
        let c = FilterChain::raw();
        let mut sealed = c.seal_slice(b"payload").unwrap();
        sealed[4] = VERSION + 1;
        assert!(matches!(
            c.open(&sealed),
            Err(Error::UnsupportedVersion { .. })
        ));
    }

    /// The failure this design exists to prevent: bytes written under one chain
    /// must never be run through a different chain's inverse.
    #[test]
    fn a_different_chain_is_refused_rather_than_misdecoded() {
        let writer = FilterChain::new(1, vec![Box::new(Xor(0xAA))]);
        let reader = FilterChain::new(2, vec![Box::new(Xor(0x55))]);

        let sealed = writer.seal_slice(b"secret").unwrap();
        match reader.open(&sealed) {
            Err(Error::SchemaMismatch { .. }) => {}
            other => panic!("expected a schema mismatch, got {other:?}"),
        }
    }

    #[test]
    fn describe_names_the_filters_in_order() {
        let c = FilterChain::new(7, vec![Box::new(Tag(1)), Box::new(Xor(2))]);
        assert_eq!(c.describe(), "chain 7 (tag -> xor)");
        assert_eq!(FilterChain::raw().describe(), "chain 0 (raw)");
    }
}
