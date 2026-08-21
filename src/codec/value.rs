//! Serialisation of typed values.
//!
//! `postcard` for the wire format: compact, `no_std`-friendly, and stable.
//! It is deliberately **not** self-describing, which buys size but means
//! nothing in the bytes says what type produced them. That is why a locker
//! records a schema tag — without one, opening a locker as the wrong type
//! decodes garbage into a perfectly valid-looking value rather than failing.

use serde::{de::DeserializeOwned, Serialize};

use super::api::FilterChain;
use crate::error::{Error, Result};

/// Serialise `value` and wrap it for storage.
pub fn encode<T: Serialize>(value: &T, chain: &FilterChain) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value)
        .map_err(|e| Error::Filter(format!("postcard serialisation failed: {e}")))?;
    chain.seal(payload)
}

/// Unwrap and deserialise a stored value.
pub fn decode<T: DeserializeOwned>(stored: &[u8], chain: &FilterChain) -> Result<T> {
    let payload = chain.open(stored)?;
    postcard::from_bytes(&payload)
        .map_err(|e| Error::Corrupt(format!("postcard deserialisation failed: {e}")))
}

/// A stable identity for `T`, recorded per locker so a later open under a
/// different type is refused rather than mis-decoded.
///
/// `type_name` is explicitly not guaranteed stable across compiler versions,
/// so this is a *best-effort* guard against an obvious mistake, not a
/// cryptographic binding. Applications that need a hard guarantee should set
/// their own tag.
pub fn type_tag<T: ?Sized>() -> String {
    std::any::type_name::<T>().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::default_chain;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Settings {
        theme: String,
        scale: f64,
        enabled: bool,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Other {
        a: u64,
        b: u64,
    }

    #[test]
    fn typed_values_round_trip() {
        let chain = default_chain();
        let value = Settings {
            theme: "dark".into(),
            scale: 1.25,
            enabled: true,
        };

        let stored = encode(&value, &chain).unwrap();
        let back: Settings = decode(&stored, &chain).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn raw_bytes_work_as_a_value_type() {
        // Vec<u8> must be usable as T, so a caller who already has bytes is not
        // forced to invent a wrapper type.
        let chain = default_chain();
        let payload: Vec<u8> = (0u8..=255).collect();

        let stored = encode(&payload, &chain).unwrap();
        let back: Vec<u8> = decode(&stored, &chain).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn an_empty_vec_round_trips_and_is_not_nothing() {
        let chain = default_chain();
        let stored = encode(&Vec::<u8>::new(), &chain).unwrap();
        assert!(
            !stored.is_empty(),
            "an empty value must still produce a non-empty record, \
             or storage cannot tell it from a missing key"
        );
        let back: Vec<u8> = decode(&stored, &chain).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn corruption_is_caught_by_the_checksum_not_by_the_deserialiser() {
        let chain = default_chain();
        let mut stored = encode(
            &Settings {
                theme: "dark".into(),
                scale: 1.0,
                enabled: false,
            },
            &chain,
        )
        .unwrap();

        // Flip a bit in the body, past the header.
        let mid = stored.len() / 2;
        stored[mid] ^= 0b0001_0000;

        match decode::<Settings>(&stored, &chain) {
            Err(Error::Corrupt(msg)) => assert!(
                msg.contains("CRC32"),
                "corruption should be caught by the checksum first, got: {msg}"
            ),
            other => panic!("expected a corrupt error, got {other:?}"),
        }
    }

    /// Demonstrates precisely why lockers carry a schema tag.
    #[test]
    fn decoding_as_the_wrong_type_is_not_reliably_detected() {
        let chain = default_chain();
        let stored = encode(&Other { a: 1, b: 2 }, &chain).unwrap();

        // postcard is not self-describing, so this may well succeed and hand
        // back a plausible value. The codec layer cannot prevent it; only a
        // recorded schema tag can. This test documents the hazard rather than
        // asserting an outcome either way.
        let reinterpreted = decode::<(u64, u64)>(&stored, &chain);
        assert!(
            reinterpreted.is_ok(),
            "a struct and a matching tuple share a postcard encoding, which is \
             exactly the confusion the schema tag exists to catch"
        );
    }

    #[test]
    fn type_tag_distinguishes_types() {
        assert_ne!(type_tag::<Settings>(), type_tag::<Other>());
        assert_eq!(type_tag::<Settings>(), type_tag::<Settings>());
    }
}
