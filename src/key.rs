//! Key encoding.
//!
//! Every user key is stored as bytes, never as a platform string. That is the
//! single decision that keeps ordering identical on all three backends.
//!
//! IndexedDB compares **string** keys by UTF-16 code unit, while `redb` and
//! `BTreeMap` compare by UTF-8 byte. The two disagree above the Basic
//! Multilingual Plane: U+1F34E encodes as the surrogate pair `D83C DF4E` in
//! UTF-16, which sorts *below* U+E000, but as `F0 9F 8D 8E` in UTF-8, which
//! sorts *above* it. A single emoji in a key would therefore reverse a range
//! scan on the web and nowhere else.
//!
//! IndexedDB orders **binary** keys bytewise, exactly as `redb` and `BTreeMap`
//! do, so encoding to UTF-8 bytes makes all three agree by construction. The
//! browser half of this is proven in `tests/spike_key_ordering.rs`; the
//! ordering itself is proven below.
//!
//! # Layout
//!
//! ```text
//! [locker_id: u32 big-endian][user key bytes]
//! ```
//!
//! Big-endian so numeric locker order matches byte order. No separator byte:
//! the prefix is fixed width, so locker `n` already owns exactly the range
//! `[n, n+1)` and a delimiter would cost a byte per key while distinguishing
//! nothing. That matters at candle-cache scale.

use std::ops::Bound;

use crate::backend::api::KeyRange;
use crate::error::{Error, Result};

/// Identifies a locker within a bank. Assigned by the locker registry in
/// `meta` and stable for the life of the data.
pub type LockerId = u32;

/// Width of the locker prefix, in bytes.
pub const PREFIX_LEN: usize = 4;

/// Encode a binary user key for storage.
///
/// This is the primitive; [`encode`] is the `&str` view of it. A `&str` key is
/// stored as its UTF-8 bytes and nothing more, so the two are interchangeable
/// on disk and the on-disk format is identical either way.
pub fn encode_bytes(locker: LockerId, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX_LEN + key.len());
    out.extend_from_slice(&locker.to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Encode a user key for storage.
pub fn encode(locker: LockerId, key: &str) -> Vec<u8> {
    encode_bytes(locker, key.as_bytes())
}

/// Recover the user key from a stored key.
///
/// Fails if the key is too short to carry a prefix, or belongs to a different
/// locker, or is not valid UTF-8 — the last of which would mean the store was
/// written by something other than crossbank.
pub fn decode(locker: LockerId, encoded: &[u8]) -> Result<&str> {
    let rest = decode_bytes(locker, encoded)?;
    std::str::from_utf8(rest).map_err(|e| Error::Corrupt(format!("stored key is not UTF-8: {e}")))
}

/// Recover the raw user key bytes from a stored key.
///
/// Unlike [`decode`] this never fails on a non-UTF-8 key: binary keys are a
/// first-class thing crossbank stores, so only a missing or foreign locker
/// prefix is an error here.
pub fn decode_bytes(locker: LockerId, encoded: &[u8]) -> Result<&[u8]> {
    if encoded.len() < PREFIX_LEN {
        return Err(Error::Corrupt(format!(
            "stored key is {} bytes, shorter than the {PREFIX_LEN} byte locker prefix",
            encoded.len()
        )));
    }
    let (prefix, rest) = encoded.split_at(PREFIX_LEN);
    let found = LockerId::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    if found != locker {
        return Err(Error::Corrupt(format!(
            "stored key belongs to locker {found}, not {locker}"
        )));
    }
    Ok(rest)
}

/// The range covering every key in a locker.
pub fn locker_range(locker: LockerId) -> KeyRange {
    KeyRange::prefix(&locker.to_be_bytes())
}

/// The range covering every key in a locker beginning with `prefix`.
pub fn prefix_range(locker: LockerId, prefix: &str) -> KeyRange {
    prefix_range_bytes(locker, prefix.as_bytes())
}

/// As [`prefix_range`], over a binary prefix.
pub fn prefix_range_bytes(locker: LockerId, prefix: &[u8]) -> KeyRange {
    KeyRange::prefix(&encode_bytes(locker, prefix))
}

/// Translate a user-space bound into stored-key space.
///
/// An unbounded user bound does **not** become an unbounded stored bound — it
/// becomes the locker's own edge, or the scan would wander into a neighbouring
/// locker's keys.
pub fn encode_range(locker: LockerId, start: Bound<&str>, end: Bound<&str>) -> KeyRange {
    encode_range_bytes(locker, as_bytes(start), as_bytes(end))
}

/// As [`encode_range`], over binary bounds.
pub fn encode_range_bytes(locker: LockerId, start: Bound<&[u8]>, end: Bound<&[u8]>) -> KeyRange {
    let locker_bounds = locker_range(locker);

    let start = match start {
        Bound::Unbounded => locker_bounds.start,
        Bound::Included(k) => Bound::Included(encode_bytes(locker, k)),
        Bound::Excluded(k) => Bound::Excluded(encode_bytes(locker, k)),
    };
    let end = match end {
        Bound::Unbounded => locker_bounds.end,
        Bound::Included(k) => Bound::Included(encode_bytes(locker, k)),
        Bound::Excluded(k) => Bound::Excluded(encode_bytes(locker, k)),
    };

    KeyRange { start, end }
}

/// Reinterpret a `&str` bound as a byte bound. Free — a `&str` is its bytes.
pub fn as_bytes(bound: Bound<&str>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(k) => Bound::Included(k.as_bytes()),
        Bound::Excluded(k) => Bound::Excluded(k.as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn round_trips() {
        let e = encode(7, "candles::BTCUSDT");
        assert_eq!(decode(7, &e).unwrap(), "candles::BTCUSDT");
    }

    #[test]
    fn an_empty_key_is_legal() {
        let e = encode(1, "");
        assert_eq!(e.len(), PREFIX_LEN);
        assert_eq!(decode(1, &e).unwrap(), "");
    }

    #[test]
    fn decoding_under_the_wrong_locker_is_an_error() {
        let e = encode(1, "k");
        assert!(matches!(decode(2, &e), Err(Error::Corrupt(_))));
    }

    #[test]
    fn a_truncated_key_is_an_error_not_a_panic() {
        assert!(matches!(decode(1, &[0, 0]), Err(Error::Corrupt(_))));
    }

    /// The reason this module exists.
    ///
    /// Encoded keys must sort exactly as `BTreeSet<Vec<u8>>` sorts them, which
    /// is what `redb` does and what IndexedDB does for binary keys. The sample
    /// deliberately includes characters where UTF-8 and UTF-16 collation
    /// disagree, so a regression to string keys would fail here.
    #[test]
    fn ordering_is_utf8_bytewise_including_above_the_bmp() {
        let keys = [
            "a",
            "z",
            "candles::BTCUSDT::0000001700",
            "\u{E000}",  // UTF-8 EE 80 80 · UTF-16 E000
            "\u{1F34E}", // UTF-8 F0 9F 8D 8E · UTF-16 D83C DF4E
            "\u{FFFD}",  // UTF-8 EF BF BD · UTF-16 FFFD
        ];

        let encoded_order: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| encode(3, k))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let expected: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| k.as_bytes().to_vec())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|k| {
                let mut v = 3u32.to_be_bytes().to_vec();
                v.extend_from_slice(&k);
                v
            })
            .collect();

        assert_eq!(
            encoded_order, expected,
            "encoding must preserve UTF-8 byte ordering"
        );

        // And specifically: the astral-plane key sorts ABOVE U+E000, which is
        // the pair UTF-16 would have inverted.
        let apple = encode(3, "\u{1F34E}");
        let private = encode(3, "\u{E000}");
        assert!(
            apple > private,
            "U+1F34E must sort above U+E000 in UTF-8 byte order"
        );
    }

    /// Binary keys are the primitive; a `&str` key is just its UTF-8 bytes.
    #[test]
    fn a_str_key_and_its_bytes_encode_identically() {
        // The on-disk format must not change when a caller switches to the
        // byte API for the same key.
        assert_eq!(encode(9, "hello"), encode_bytes(9, b"hello"));
    }

    #[test]
    fn non_utf8_keys_round_trip_as_bytes() {
        for key in [&[0xFFu8][..], &[0x00][..], &[0x80, 0x01][..], &[][..]] {
            let e = encode_bytes(4, key);
            assert_eq!(decode_bytes(4, &e).unwrap(), key);
        }
        // And the `&str` view refuses rather than mangling them.
        let e = encode_bytes(4, &[0xFF]);
        assert!(matches!(decode(4, &e), Err(Error::Corrupt(_))));
    }

    #[test]
    fn binary_keys_order_bytewise() {
        let keys: [&[u8]; 4] = [&[0xFF], &[0x00], &[0x80, 0x01], b"a"];
        let mut encoded: Vec<Vec<u8>> = keys.iter().map(|k| encode_bytes(2, k)).collect();
        encoded.sort();
        let decoded: Vec<&[u8]> = encoded
            .iter()
            .map(|e| decode_bytes(2, e).unwrap())
            .collect();
        assert_eq!(
            decoded,
            vec![&[0x00][..], b"a", &[0x80, 0x01][..], &[0xFF][..]]
        );
    }

    #[test]
    fn lockers_do_not_overlap() {
        // Locker 1's range must exclude every key of lockers 0 and 2, however
        // extreme the user key.
        let range = locker_range(1);
        assert!(range.contains(&encode(1, "")));
        assert!(range.contains(&encode(1, "\u{10FFFF}")));
        assert!(!range.contains(&encode(0, "zzzz")));
        assert!(!range.contains(&encode(2, "")));
    }

    #[test]
    fn locker_ids_order_numerically() {
        // Big-endian is what makes this true; little-endian would interleave.
        assert!(encode(1, "z") < encode(2, "a"));
        assert!(encode(255, "z") < encode(256, "a"));
    }

    #[test]
    fn an_unbounded_user_range_stays_inside_its_locker() {
        // The bug this prevents: an unbounded scan in locker 1 reading locker
        // 2's keys.
        let r = encode_range(1, Bound::Unbounded, Bound::Unbounded);
        assert!(r.contains(&encode(1, "")));
        assert!(r.contains(&encode(1, "~~~~")));
        assert!(!r.contains(&encode(2, "")));
        assert!(!r.contains(&encode(0, "~~~~")));
    }

    #[test]
    fn bounded_ranges_respect_inclusivity() {
        let r = encode_range(1, Bound::Included("b"), Bound::Excluded("d"));
        assert!(!r.contains(&encode(1, "a")));
        assert!(r.contains(&encode(1, "b")));
        assert!(r.contains(&encode(1, "c")));
        assert!(!r.contains(&encode(1, "d")));
    }

    #[test]
    fn prefix_range_stays_within_the_locker() {
        let r = prefix_range(5, "BTCUSDT::");
        assert!(r.contains(&encode(5, "BTCUSDT::0")));
        assert!(!r.contains(&encode(5, "BTCUSDU")));
        assert!(!r.contains(&encode(6, "BTCUSDT::0")));
    }
}
