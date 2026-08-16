//! crossbank's error type.
//!
//! `Error` is unconditionally `Send + Sync + 'static`, on every target. That is
//! a deliberate constraint, not an accident: on wasm the backend deals in
//! `JsValue`, which is neither `Send` nor `Sync`, and a consumer whose own
//! error type is `Send` (`anyhow::Error`, for instance) could not carry ours if
//! we let one leak through. JS failures are therefore stringified at the
//! backend boundary, with the structured part kept as a typed variant.

use std::fmt;

/// The result type used throughout the public API.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The underlying store failed for a reason we do not model specifically.
    Backend(String),

    /// The store is full. `available` is the backend's own estimate and may be
    /// absent on platforms that do not report one.
    QuotaExceeded { needed: u64, available: Option<u64> },

    /// A value was handed to an eager `Locker` that is too
    /// large to hold resident.
    ///
    /// Eager lockers answer `get()` synchronously and infallibly, so they can
    /// never await a chunk fetch. Oversized values belong in a lazy locker.
    ValueTooLarge { bytes: usize, max_inline: usize },

    /// An eager locker's contents exceed its resident budget, discovered while
    /// opening it. Refusing here is the guardrail against typing `locker()`
    /// where `lazy_locker()` was meant and quietly loading hundreds of
    /// megabytes into memory.
    LockerTooLarge { bytes: u64, budget: u64 },

    /// The stored data was written under a different type or filter chain than
    /// the one being opened.
    ///
    /// postcard is not self-describing, so without this check opening
    /// `Locker<A>` over data written as `Locker<B>` would decode garbage into a
    /// perfectly valid-looking `A`.
    SchemaMismatch { stored: String, requested: String },

    /// A stored value failed its checksum or could not be decoded.
    Corrupt(String),

    /// The value's envelope carries a format version this build cannot read.
    UnsupportedVersion { found: u8, supported: u8 },

    /// A user-supplied codec or cipher failed.
    Filter(String),

    /// The `Bank` this handle belongs to has shut down.
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(msg) => write!(f, "storage backend failed: {msg}"),
            Self::QuotaExceeded { needed, available } => match available {
                Some(a) => write!(f, "quota exceeded: needed {needed} bytes, {a} available"),
                None => write!(f, "quota exceeded: needed {needed} bytes"),
            },
            Self::ValueTooLarge { bytes, max_inline } => write!(
                f,
                "value of {bytes} bytes exceeds the {max_inline} byte inline limit \
                 for an eager locker; use a lazy locker instead"
            ),
            Self::LockerTooLarge { bytes, budget } => write!(
                f,
                "eager locker holds {bytes} bytes, over its {budget} byte budget; \
                 use a lazy locker instead"
            ),
            Self::SchemaMismatch { stored, requested } => write!(
                f,
                "schema mismatch: stored as {stored:?}, opened as {requested:?}"
            ),
            Self::Corrupt(msg) => write!(f, "stored data is corrupt: {msg}"),
            Self::UnsupportedVersion { found, supported } => write!(
                f,
                "envelope version {found} is not readable by this build (supports {supported})"
            ),
            Self::Filter(msg) => write!(f, "codec or cipher failed: {msg}"),
            Self::Closed => write!(f, "the bank has been closed"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Convenience for backends reporting an opaque platform failure.
    pub fn backend(msg: impl fmt::Display) -> Self {
        Self::Backend(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The constraint this module exists to enforce. If a `JsValue` ever ends
    /// up inside `Error`, this stops compiling.
    #[test]
    fn error_is_send_sync_static() {
        fn assert_bounds<T: Send + Sync + 'static>() {}
        assert_bounds::<Error>();
        assert_bounds::<Result<Vec<u8>>>();
    }

    #[test]
    fn quota_message_reads_sensibly_without_an_estimate() {
        let with = Error::QuotaExceeded {
            needed: 10,
            available: Some(4),
        };
        assert_eq!(
            with.to_string(),
            "quota exceeded: needed 10 bytes, 4 available"
        );

        let without = Error::QuotaExceeded {
            needed: 10,
            available: None,
        };
        assert_eq!(without.to_string(), "quota exceeded: needed 10 bytes");
    }

    #[test]
    fn value_too_large_names_the_alternative() {
        let e = Error::ValueTooLarge {
            bytes: 41_000_000,
            max_inline: 262_144,
        };
        assert!(
            e.to_string().contains("lazy locker"),
            "the error must tell the caller what to do instead: {e}"
        );
    }
}
