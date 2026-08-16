//! The storage backend contract.
//!
//! # Why this trait looks the way it does
//!
//! One constraint dominates the design: **no backend method may span a foreign
//! await.**
//!
//! IndexedDB transactions auto-commit as soon as the microtask queue drains
//! with no request in flight, and the `indexed-db` crate enforces this with a
//! waker that panics — `panic!("Transaction blocked without any request under
//! way")`. Because wasm release builds use `panic = "abort"`, awaiting anything
//! that is not an IDB request inside a transaction is an unrecoverable,
//! message-less process kill.
//!
//! Two API shapes are therefore ruled out, and their absence here is deliberate:
//!
//! * **No `begin_write()` / `commit()` handle pair.** Anything the engine
//!   awaited between the two — a lock, a channel, a user callback, another
//!   crossbank operation — would land inside a live IDB transaction. Instead,
//!   writes are staged in memory by the layer above and applied through a
//!   single [`Backend::commit`] call taking a complete op list.
//!
//! * **No `Stream` of scan results.** An IDB cursor lives inside a transaction,
//!   so a stream that yields to the caller between items would kill it.
//!   [`Backend::scan`] returns a bounded page plus a resume key instead.
//!
//! Both restrictions are satisfiable by every backend rather than tolerable by
//! one, which is what makes a single conformance suite meaningful.

use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;

use crate::error::Result;

// `Send`-ness is target-dependent. On native we want futures a consumer can
// move across threads; on wasm the backend holds `JsValue`, which is `!Send`,
// so requiring it would make the trait unimplementable. Callers who need a
// `Send` handle on wasm go through `RemoteBank`, which proxies over a channel.
//
// These marker traits have blanket impls, so users never name them — they only
// ever appear in a compiler error, which is why they carry a diagnostic note.

#[cfg(not(target_arch = "wasm32"))]
mod bounds {
    #[diagnostic::on_unimplemented(
        note = "on native targets crossbank requires `Send`; this type is not `Send`"
    )]
    pub trait MaybeSend: Send {}
    impl<T: Send> MaybeSend for T {}

    #[diagnostic::on_unimplemented(
        note = "on native targets crossbank requires `Sync`; this type is not `Sync`"
    )]
    pub trait MaybeSync: Sync {}
    impl<T: Sync> MaybeSync for T {}
}

#[cfg(target_arch = "wasm32")]
mod bounds {
    pub trait MaybeSend {}
    impl<T> MaybeSend for T {}

    pub trait MaybeSync {}
    impl<T> MaybeSync for T {}
}

pub use bounds::{MaybeSend, MaybeSync};

/// A boxed backend future.
///
/// Boxing is not a stylistic choice. `MaybeSend` cannot bound an `async fn`
/// return type — auto traits on an opaque type are inferred, not bounded — so
/// the only way to state "Send on native, unbounded on wasm" is a boxed trait
/// object behind a cfg.
#[cfg(not(target_arch = "wasm32"))]
pub type BFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// A boxed backend future. See the native variant for why this is boxed.
#[cfg(target_arch = "wasm32")]
pub type BFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

/// The three fixed tables every backend provides.
///
/// Fixed, and created exactly once, because on IndexedDB an object store can
/// only be created inside a `versionchange` transaction — which fires on every
/// other open tab and force-closes their connections. A store per locker would
/// make `bank.locker("new_name")` a cross-tab disruption. Lockers are a key
/// prefix instead, and the database version is never bumped again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// Bank and locker bookkeeping: format version, locker registry, schema
    /// tags, counters.
    Meta,
    /// User records, keyed by the encoded locker-prefixed key.
    Records,
    /// Chunk payloads for values too large to store inline.
    Chunks,
}

impl Table {
    /// Stable on-disk name. Changing one of these is a format break.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Records => "records",
            Self::Chunks => "chunks",
        }
    }

    /// Every table, for backends that must create them all up front.
    pub const ALL: [Table; 3] = [Table::Meta, Table::Records, Table::Chunks];
}

/// A half-open or closed key range, in encoded key space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start: Bound<Vec<u8>>,
    pub end: Bound<Vec<u8>>,
}

impl KeyRange {
    /// Every key in the table.
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// Every key beginning with `prefix`.
    ///
    /// The upper bound is the prefix with its last non-`0xFF` byte incremented,
    /// which is the successor of every string sharing the prefix. A prefix of
    /// all `0xFF` bytes has no successor, so the range stays unbounded above.
    pub fn prefix(prefix: &[u8]) -> Self {
        let start = Bound::Included(prefix.to_vec());
        let mut end = prefix.to_vec();
        while let Some(last) = end.pop() {
            if last != 0xFF {
                end.push(last + 1);
                return Self {
                    start,
                    end: Bound::Excluded(end),
                };
            }
        }
        Self {
            start,
            end: Bound::Unbounded,
        }
    }

    /// Whether `key` falls inside this range.
    pub fn contains(&self, key: &[u8]) -> bool {
        let above_start = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(s) => key >= s.as_slice(),
            Bound::Excluded(s) => key > s.as_slice(),
        };
        let below_end = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(e) => key <= e.as_slice(),
            Bound::Excluded(e) => key < e.as_slice(),
        };
        above_start && below_end
    }
}

/// A single mutation. A [`Backend::commit`] applies a list of these atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put {
        table: Table,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: Table,
        key: Vec<u8>,
    },
    DeleteRange {
        table: Table,
        range: KeyRange,
    },
}

/// A bounded scan. Bounded because an IndexedDB cursor cannot outlive its
/// transaction, so the caller resumes rather than holding one open.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub table: Table,
    pub range: KeyRange,
    pub reverse: bool,
    pub limit: usize,
    /// When false the backend may skip loading values, which lets a key-only
    /// scan avoid reading payloads it would immediately discard.
    pub want_values: bool,
}

/// One page of scan results.
#[derive(Debug, Clone, Default)]
pub struct ScanPage {
    pub items: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Where to resume, or `None` when the range is exhausted.
    pub resume: Option<Vec<u8>>,
}

/// Storage usage, where the platform reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    pub used: u64,
    pub available: Option<u64>,
    /// Whether the origin has been granted persistent storage. Always `true`
    /// on native, where nothing evicts us.
    pub persisted: bool,
}

/// A place bytes can be stored.
///
/// Implementations stay deliberately dumb: no chunking, no codecs, no indexes,
/// no eviction. All of that lives in the portable engine above, which is what
/// lets one conformance suite grade every backend against the same spec.
pub trait Backend: MaybeSend + MaybeSync + 'static {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>>;

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>>;

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage>;

    /// Apply every op, or none of them.
    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()>;

    /// `None` where the platform does not report usage, which is the normal
    /// case natively. Reporting a fabricated number would be worse.
    fn usage(&self) -> BFut<'_, Option<Usage>>;

    /// Ensure prior commits have reached durable storage.
    fn flush(&self) -> BFut<'_, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_range_covers_exactly_the_prefixed_keys() {
        let r = KeyRange::prefix(b"ab");
        assert!(r.contains(b"ab"));
        assert!(r.contains(b"abz"));
        assert!(r.contains(b"ab\xFF\xFF"));
        assert!(!r.contains(b"aa"));
        assert!(!r.contains(b"ac"));
        assert!(!r.contains(b"b"));
    }

    #[test]
    fn prefix_of_all_ff_bytes_has_no_upper_bound() {
        // 0xFF has no successor byte, so the range must stay open above rather
        // than wrapping to something that excludes real keys.
        let r = KeyRange::prefix(&[0xFF, 0xFF]);
        assert_eq!(r.end, Bound::Unbounded);
        assert!(r.contains(&[0xFF, 0xFF, 0x00]));
        assert!(!r.contains(&[0xFE]));
    }

    #[test]
    fn empty_prefix_is_everything() {
        let r = KeyRange::prefix(b"");
        assert_eq!(r.end, Bound::Unbounded);
        assert!(r.contains(b""));
        assert!(r.contains(b"anything"));
    }

    #[test]
    fn table_names_are_stable() {
        // These strings are on-disk identifiers. Changing one is a format break,
        // so pin them.
        assert_eq!(Table::Meta.name(), "meta");
        assert_eq!(Table::Records.name(), "records");
        assert_eq!(Table::Chunks.name(), "chunks");
        assert_eq!(Table::ALL.len(), 3);
    }
}
