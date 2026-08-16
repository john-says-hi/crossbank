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
    pub fn seal(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let mut body = payload.to_vec();
        for filter in &self.filters {
            body = filter.forward(&body)?;
        }

        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.push(self.id);
        out.extend_from_slice(&body);
        Ok(out)
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

        let mut body = stored[HEADER_LEN..].to_vec();
        for filter in self.filters.iter().rev() {
            body = filter.reverse(&body)?;
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

    #[test]
    fn raw_chain_round_trips() {
        let c = FilterChain::raw();
        let sealed = c.seal(b"hello").unwrap();
        assert_eq!(&sealed[0..4], &MAGIC);
        assert_eq!(c.open(&sealed).unwrap(), b"hello");
    }

    #[test]
    fn an_empty_payload_round_trips() {
        // Must stay distinguishable from a missing value everywhere it travels.
        let c = FilterChain::raw();
        let sealed = c.seal(b"").unwrap();
        assert_eq!(sealed.len(), HEADER_LEN);
        assert_eq!(c.open(&sealed).unwrap(), b"");
    }

    #[test]
    fn filters_reverse_in_the_opposite_order() {
        // Tag(1) then Tag(2) writes ...0102; reversing must strip 2 first.
        let c = FilterChain::new(1, vec![Box::new(Tag(1)), Box::new(Tag(2))]);
        let sealed = c.seal(b"x").unwrap();
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
        let mut sealed = c.seal(b"payload").unwrap();
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

        let sealed = writer.seal(b"secret").unwrap();
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
