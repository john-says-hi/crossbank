//! crossbank — cross-platform persistent key/value storage.
//!
//! One API on native and in the browser: `redb` underneath on desktop and
//! mobile, real IndexedDB on the web. Modelled on Hive, the Flutter key/value
//! store — its architecture and ergonomics, not its file format.
//!
//! See `PLAN.md` in the repository for the design and its rationale.
//!
//! # Status
//!
//! Pre-alpha. M0 (proving the test lanes) is complete; M1 (this layer) is in
//! progress. The public API is not yet stable and the crate is not published.
//!
//! # Shape
//!
//! ```text
//!   Bank / Locker / LazyLocker / Transaction / Writer / Reader
//! ─────────────────────────────────────────────────────────────  public API
//!   codec · cipher · chunking · RAM index · watch · eviction
//! ─────────────────────────────────────────────────────────────  engine (portable)
//!                        trait Backend
//! ─────────────────────────────────────────────────────────────
//!   memory (all)      redb (native)      IndexedDB (wasm32)
//! ```
//!
//! Everything above the `Backend` trait is portable Rust with no `cfg` in it.
//! Backends stay deliberately dumb — no chunking, no codecs, no eviction — so
//! that a single conformance suite can grade all of them against one spec.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod backend;
pub mod bank;
pub mod codec;
pub mod error;
pub mod key;
pub mod locker;

pub use backend::{Backend, MemoryBackend};
pub use bank::{Bank, BankConfig, Location};
pub use codec::{Filter, FilterChain};
pub use error::{Error, Result};
pub use locker::{LazyLocker, Locker, LockerConfig, Policy};

/// The crate name, useful in diagnostics.
pub const NAME: &str = "crossbank";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_stable() {
        assert_eq!(NAME, "crossbank");
    }
}
