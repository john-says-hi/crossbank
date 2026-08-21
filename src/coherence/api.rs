//! Cross-tab coherence: the portable half.
//!
//! # The problem
//!
//! Two tabs of the same web application share one IndexedDB database. Tab A
//! opens a lazy locker, tab B writes a key, and tab A's resident index — the
//! whole point of a lazy locker — is now a lie. Natively the question does not
//! arise: `redb` takes an exclusive file lock, so a second process cannot open
//! the database at all.
//!
//! # The shape of the answer
//!
//! One `BroadcastChannel` per bank, named `crossbank::{database name}`. After
//! a commit lands, the bank posts what changed. Every other tab's callback
//! updates its resident view and raises the same [`crate::Event`]s a local
//! write would.
//!
//! Three properties this design is built around:
//!
//! * **The callback is a plain closure, never inside an IndexedDB
//!   transaction.** A `BroadcastChannel` message arrives as an ordinary DOM
//!   event; if handling it awaited a fetch inside a live IDB transaction, the
//!   `indexed-db` crate would panic — and wasm release builds abort.
//! * **Small values ride along.** A change carries its sealed bytes when they
//!   are at most [`INLINE_LIMIT`], so the receiving tab can update an eager
//!   locker's resident value without a read. Past that the message says only
//!   *that* the key changed.
//! * **It is opt-in.** [`crate::BankConfig::with_coherence`] defaults to
//!   `false`, because a bank whose data is only ever touched by one tab should
//!   not pay for a channel, and because coherence changes what an eager
//!   `get()` can return (see [`crate::Event::Stale`]).
//!
//! Native accepts the flag and does nothing, so consumer code is identical on
//! both targets.

// Every item below is consumed by the web half and by none of the native
// half, and the dead-code lint cannot see the wasm build from a native one.
// The logic stays portable rather than cfg'd so it is unit-tested on every
// lane, so the unused warning is suppressed only where it is a false alarm.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

use crate::key::LockerId;

/// Largest sealed value carried inside a coherence message.
///
/// Past this the message names the key and says nothing about its contents.
/// `postMessage` structured-cloning megabytes to every tab on every write
/// would cost more than the read it saves.
pub(crate) const INLINE_LIMIT: usize = 4096;

/// One key's worth of news.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Change {
    pub key: Vec<u8>,
    /// The sealed record, when it was small enough to carry.
    ///
    /// `None` on a delete **and** on a write too large to inline, which is
    /// exactly why [`Change::deleted`] exists: a receiver has to tell "this
    /// key is gone" from "this key changed and you will have to read it".
    pub value: Option<Vec<u8>>,
    /// The value's **payload** length, when the sender could state it without
    /// the receiver having to decode anything.
    ///
    /// Only a chunk pointer carries it, because a pointer records the payload
    /// length it was built from. An inlined value needs no announcement — the
    /// receiver has the bytes and its own filter chain. A large non-chunked
    /// write states nothing, and a receiver that keeps a byte budget marks its
    /// accounting dirty rather than guessing (see [`crate::Policy::Evictable`]).
    pub bytes: Option<u64>,
    pub deleted: bool,
}

/// What one commit did, as broadcast to the other tabs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Announcement {
    /// Which bank instance posted it, so a tab can ignore its own news.
    pub instance: u32,
    pub locker_id: LockerId,
    /// A per-bank counter, bumped once per commit that has news.
    ///
    /// **It is not a vector clock.** It orders one sender's own messages, and
    /// a receiver uses it for exactly two things: dropping a message it has
    /// already seen (an epoch at or below the last it applied from any tab),
    /// and refusing to let another tab's news undo a write this tab committed
    /// at an equal or later epoch of its own. See
    /// [`crate::BankConfig::coherence`] for what that does and does not order.
    pub epoch: u64,
    pub cleared: bool,
    pub changes: Vec<Change>,
}

/// Something that can absorb another tab's news: one open locker.
pub(crate) trait Sink {
    fn locker_id(&self) -> LockerId;
    fn apply(&self, announcement: &Announcement);
}

/// Turn a commit's op list into news worth posting.
///
/// Deriving the message from the ops rather than from each write path means
/// every path is covered by construction — plain puts, transactions,
/// `put_all`, quarantine, and the deletes an eviction performs.
pub(crate) fn announcement_from_ops(
    instance: u32,
    locker_id: LockerId,
    epoch: u64,
    ops: &[crate::backend::api::Op],
) -> Option<Announcement> {
    use crate::backend::api::{Op, Table};

    let mut changes: Vec<Change> = Vec::new();
    let mut cleared = false;
    let locker_range = crate::key::locker_range(locker_id);

    for op in ops {
        match op {
            Op::Put {
                table: Table::Records,
                key,
                value,
            } => {
                let Ok(user_key) = crate::key::decode_bytes(locker_id, key) else {
                    continue;
                };
                // A chunk pointer is never inlined: the bytes it names are in
                // another table and the receiver has to go and read them.
                let pointer = crate::locker::chunk::is_pointer(value);
                let inline = if value.len() <= INLINE_LIMIT && !pointer {
                    Some(value.clone())
                } else {
                    None
                };
                // A pointer already records the payload length it was built
                // from, so a chunked write can be accounted for without a read.
                let bytes = if pointer {
                    crate::locker::chunk::ChunkPointer::parse(value)
                        .ok()
                        .map(|p| p.total_len)
                } else {
                    None
                };
                changes.push(Change {
                    key: user_key.to_vec(),
                    value: inline,
                    bytes,
                    deleted: false,
                });
            }
            Op::Delete {
                table: Table::Records,
                key,
            } => {
                let Ok(user_key) = crate::key::decode_bytes(locker_id, key) else {
                    continue;
                };
                changes.push(Change {
                    key: user_key.to_vec(),
                    value: None,
                    bytes: None,
                    deleted: true,
                });
            }
            // Only a range covering this whole locker is a clear. A narrower
            // one cannot be spelled by the public API today, and guessing at
            // one would be worse than saying nothing.
            Op::DeleteRange {
                table: Table::Records,
                range,
            } if *range == locker_range => {
                cleared = true;
                changes.clear();
            }
            // Meta and chunk writes are bookkeeping for the records above;
            // announcing them would tell another tab nothing it can act on.
            _ => {}
        }
    }

    if !cleared && changes.is_empty() {
        return None;
    }
    Some(Announcement {
        instance,
        locker_id,
        epoch,
        cleared,
        changes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::api::{Op, Table};

    fn put(id: LockerId, key: &str, value: Vec<u8>) -> Op {
        Op::Put {
            table: Table::Records,
            key: crate::key::encode(id, key),
            value,
        }
    }

    #[test]
    fn a_small_write_carries_its_bytes() {
        let a = announcement_from_ops(1, 7, 3, &[put(7, "k", vec![1, 2, 3])]).expect("news");
        assert_eq!(a.locker_id, 7);
        assert_eq!(a.epoch, 3);
        assert!(!a.cleared);
        assert_eq!(a.changes.len(), 1);
        assert_eq!(a.changes[0].key, b"k".to_vec());
        assert_eq!(a.changes[0].value, Some(vec![1, 2, 3]));
        assert!(!a.changes[0].deleted);
    }

    #[test]
    fn a_large_write_names_the_key_and_nothing_else() {
        let big = vec![0u8; INLINE_LIMIT + 1];
        let a = announcement_from_ops(1, 7, 0, &[put(7, "k", big)]).expect("news");
        assert_eq!(a.changes[0].value, None);
        assert!(
            !a.changes[0].deleted,
            "a large write is not a delete; a receiver must not drop the key"
        );
    }

    #[test]
    fn a_chunk_pointer_is_never_inlined() {
        let pointer = crate::locker::chunk::ChunkPointer {
            value_id: 1,
            n_chunks: 2,
            total_len: 9,
            flags: crate::locker::chunk::FLAG_POSTCARD,
        }
        .encode();
        let a = announcement_from_ops(1, 7, 0, &[put(7, "k", pointer)]).expect("news");
        assert_eq!(a.changes[0].value, None);
        assert!(!a.changes[0].deleted);
        assert_eq!(
            a.changes[0].bytes,
            Some(9),
            "a pointer states its payload length so a receiver can account for it"
        );
    }

    #[test]
    fn a_clear_supersedes_the_writes_before_it() {
        let ops = vec![
            put(7, "a", vec![1]),
            Op::DeleteRange {
                table: Table::Records,
                range: crate::key::locker_range(7),
            },
            put(7, "b", vec![2]),
        ];
        let a = announcement_from_ops(1, 7, 0, &ops).expect("news");
        assert!(a.cleared);
        assert_eq!(a.changes.len(), 1, "only the writes after the clear");
        assert_eq!(a.changes[0].key, b"b".to_vec());
    }

    #[test]
    fn bookkeeping_alone_is_not_news() {
        let ops = vec![Op::Put {
            table: Table::Meta,
            key: b"next_tick".to_vec(),
            value: vec![0; 8],
        }];
        assert!(announcement_from_ops(1, 7, 0, &ops).is_none());
    }
}
