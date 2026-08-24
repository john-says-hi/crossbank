//! crossbank — **local, on-device** key/value storage for Rust, and a direct
//! replacement for Flutter's [Hive](https://github.com/IO-Design-Team/hive_ce)
//! (`hive_ce`).
//!
//! It saves an application's data on the machine it is running on: a `redb`
//! file on Linux, macOS, Windows, Android and iOS, and real IndexedDB in the
//! browser, behind one API. There is **no network code, no server, no sync and
//! no cloud** anywhere in it — crossbank never talks to anything. Modelled on
//! Hive's architecture and ergonomics, deliberately not its file format, and
//! Hive's own data is not migrated.
//!
//! See `README.md` for the Hive-to-crossbank mapping and `PLAN.md` for the
//! design and its rationale.
//!
//! # Status
//!
//! **v0.1.1 tagged; crates.io publish pending. The API may still change
//! before 1.0.** M0–M6 are
//! complete: data persists natively (`redb`) and in real browsers
//! (IndexedDB), large lazy values are chunked with streaming
//! [`Writer`]/[`Reader`] access, and storage pressure is answered by
//! [`Bank::persist`] / [`Bank::is_persisted`] / [`Bank::usage`] plus a
//! byte-budget LRU on [`Policy::Evictable`] lockers. Two facilities are
//! opt-in and never chosen for you: cross-tab coherence
//! ([`BankConfig::with_coherence`], web only) and write coalescing
//! ([`Commit::Deferred`], whose flush you own — crossbank spawns nothing).
//!
//! Values are sealed through a [`FilterChain`], set per bank or per locker
//! ([`LockerConfig::with_chain`]). crossbank ships LZ4 and CRC32 and **no
//! cipher at all**: implement [`Filter`] to add one, so key handling and its
//! audit burden stay with the application that owns the keys.
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
//! [`Locker::watch_key`] and [`Locker::watch_keys`]. [`BankHandle`] carries the
//! same bulk shapes over its channel ([`BankHandle::put_all`],
//! [`BankHandle::delete_all`], [`BankHandle::get_many`],
//! [`BankHandle::entries`]) plus the bank-level questions, which is what a
//! Hive-shaped shim in front of it needs.
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
//!   Bank / Locker / LazyLocker / Transaction / Writer / Reader / BankHandle
//! ─────────────────────────────────────────────────────────────  public API
//!   codec · filters · chunking · RAM index · watch · eviction
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
