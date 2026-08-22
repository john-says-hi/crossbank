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
//! Pre-alpha. M0–M5 are complete: data persists natively (`redb`) and in
//! real browsers (IndexedDB), large lazy values are chunked with streaming
//! [`Writer`]/[`Reader`] access, and storage pressure is answered by
//! [`Bank::persist`] / [`Bank::is_persisted`] / [`Bank::usage`] plus a
//! byte-budget LRU on [`Policy::Evictable`] lockers. Two facilities are
//! opt-in and never chosen for you: cross-tab coherence
//! ([`BankConfig::with_coherence`], web only) and write coalescing
//! ([`Commit::Deferred`], whose flush you own — crossbank spawns nothing).
//! The public API is not yet stable and the crate is not published.
//!
//! Web storage has rules a filesystem does not; the README's "Web caveats"
//! section covers persistence, Safari's seven-day eviction, and what
//! coherence changes about an eager `get()`.
//!
//! Keys are **bytes**. Every `&str` method has a `_by` twin taking `&[u8]`
//! ([`Locker::get_by`], [`Locker::put_by`], [`LazyLocker::range_by`], …), a
//! `&str` key is stored as exactly its UTF-8 bytes, and the `&str` listings
//! ([`Locker::keys`], [`LazyLocker::to_map`]) skip what they cannot spell
//! rather than failing — [`Locker::has_non_utf8_keys`] and
//! [`Locker::keys_bytes`] cover the rest.
//!
//! Bulk work goes through [`Locker::put_all`] / [`Locker::delete_all`], each
//! one atomic commit, and change notification through [`Locker::watch`],
//! [`Locker::watch_key`] and [`Locker::watch_keys`].
//!
//! Damaged data is survivable rather than fatal: [`OnCorrupt::Skip`] opens a
//! locker without its unreadable records and lists them via
//! [`Locker::corrupt_keys`], [`Bank::verify`] surveys a locker without
//! changing it, and [`Bank::quarantine`] is the only thing that deletes a
//! record for being corrupt.
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
pub(crate) mod coherence;
pub mod error;
pub mod handle;
pub mod key;
pub mod locker;
pub mod watch;

#[cfg(target_arch = "wasm32")]
pub use backend::IndexedDbBackend;
#[cfg(not(target_arch = "wasm32"))]
pub use backend::RedbBackend;
pub use backend::{Backend, MemoryBackend, Usage};
pub use bank::{delete_bank, Bank, BankConfig, Location};
pub use codec::{Filter, FilterChain};
pub use error::{Error, Result};
pub use handle::BankHandle;
pub use locker::{
    Commit, Durability, LazyLocker, Locker, LockerConfig, OnCorrupt, Policy, Reader, Transaction,
    Writer,
};
pub use watch::{Event, EventStream};

/// Renamed to [`BankHandle`].
#[deprecated(since = "0.1.0", note = "renamed to `BankHandle`")]
pub use handle::BankHandle as RemoteBank;

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
