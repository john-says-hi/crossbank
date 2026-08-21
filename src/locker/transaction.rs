//! Transactions: a write-set staged in memory, applied as one atomic commit.
//!
//! # Why the closure form is the only form
//!
//! A transaction is only reachable through `transact(|tx| async { … })`. There
//! is no handle you can hold, and that is a safety property rather than a
//! stylistic preference.
//!
//! IndexedDB transactions auto-commit the moment the microtask queue drains
//! with no request in flight, and the `indexed-db` crate enforces it with a
//! waker that panics —
//! `panic!("Transaction blocked without any request under way")`. Since wasm
//! release builds use `panic = "abort"`, awaiting anything that is not an IDB
//! request inside a live transaction is an unrecoverable process kill with no
//! message.
//!
//! A free-floating `tx` handle invites precisely that: nothing stops a caller
//! awaiting an HTTP request between two `put`s. The closure form scopes the
//! transaction, and staging the writes in memory means **no backend
//! transaction is open while user code runs at all**. The backend sees one
//! `commit(ops)` call containing pure bytes.
//!
//! # Semantics
//!
//! * Writes are encoded immediately, on the caller's thread, and buffered.
//! * `get` reads your own writes first, then falls through to storage.
//! * Nothing is visible outside the transaction until it commits.
//! * Returning `Err` from the closure rolls back: nothing is written, and the
//!   in-memory index is untouched.
//! * The locker's write lock is held for the transaction's duration, so two
//!   overlapping transactions cannot lose each other's updates.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::{Op, Table};
use crate::error::{Error, Result};

use super::chunk::{gc_ops, is_pointer, ChunkPointer, FLAG_POSTCARD};
use super::inner::{Inner, Prior};

/// Which locker is committing a write-set.
///
/// An eager locker holds every value in RAM, so its writes stay inline and
/// are size-checked; a lazy locker chunks exactly as `LazyLocker::put` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxMode {
    Eager,
    Lazy,
}

/// One staged mutation.
pub(crate) enum Staged<T> {
    Put {
        key: Vec<u8>,
        /// The postcard payload, **not** the sealed envelope. The lazy path
        /// needs the raw payload to chunk it, and the eager path seals it
        /// once while building ops.
        payload: Vec<u8>,
        value: Arc<T>,
    },
    Delete {
        key: Vec<u8>,
    },
    Clear,
}

/// A staged write-set. Obtained only inside `transact`.
///
/// Owned rather than borrowed, so `transact(|tx| async move { … })` works
/// without lifetime gymnastics in the caller's closure.
pub struct Transaction<T> {
    inner: Arc<Inner>,
    staged: Arc<Mutex<Vec<Staged<T>>>>,
    _value: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for Transaction<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("staged", &self.staged.lock().map(|s| s.len()).unwrap_or(0))
            .finish()
    }
}

impl<T> Transaction<T>
where
    T: Serialize + DeserializeOwned,
{
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            staged: Arc::new(Mutex::new(Vec::new())),
            _value: PhantomData,
        }
    }

    pub(crate) fn staged_handle(&self) -> Arc<Mutex<Vec<Staged<T>>>> {
        self.staged.clone()
    }

    /// Stage a write. Encoding happens now, on this thread, so no user code
    /// ever runs while a backend transaction is open.
    pub fn put(&self, key: &str, value: T) -> Result<()> {
        self.put_by(key.as_bytes(), value)
    }

    /// As [`Transaction::put`], under a binary key.
    pub fn put_by(&self, key: &[u8], value: T) -> Result<()> {
        let payload = postcard::to_allocvec(&value)
            .map_err(|e| Error::Filter(format!("postcard serialisation failed: {e}")))?;
        self.push(Staged::Put {
            key: key.to_vec(),
            payload,
            value: Arc::new(value),
        })
    }

    /// Stage a delete. Deleting an absent key is not an error.
    pub fn delete(&self, key: &str) -> Result<()> {
        self.delete_by(key.as_bytes())
    }

    /// As [`Transaction::delete`], under a binary key.
    pub fn delete_by(&self, key: &[u8]) -> Result<()> {
        self.push(Staged::Delete { key: key.to_vec() })
    }

    /// Stage a clear of the whole locker.
    pub fn clear(&self) -> Result<()> {
        self.push(Staged::Clear)
    }

    /// Read, seeing this transaction's own staged writes first.
    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        self.get_by(key.as_bytes()).await
    }

    /// As [`Transaction::get`], under a binary key.
    pub async fn get_by(&self, key: &[u8]) -> Result<Option<T>> {
        if let Some(staged) = self.staged_view(key)? {
            return staged
                .map(|payload| {
                    postcard::from_bytes(&payload).map_err(|e| {
                        Error::Corrupt(format!("postcard deserialisation failed: {e}"))
                    })
                })
                .transpose();
        }
        self.inner.load_value(key).await
    }

    /// How many mutations are queued.
    pub fn len(&self) -> usize {
        self.staged.lock().map(|s| s.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The staged **payload** for a key: `None` when this transaction has not
    /// touched it at all; `Some(None)` when it has deleted or cleared it.
    fn staged_view(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("transaction lock was poisoned"))?;

        // Later entries win, so scan backwards and stop at the first hit.
        for entry in guard.iter().rev() {
            match entry {
                Staged::Put {
                    key: k, payload, ..
                } if k == key => return Ok(Some(Some(payload.clone()))),
                Staged::Delete { key: k } if k == key => return Ok(Some(None)),
                Staged::Clear => return Ok(Some(None)),
                _ => {}
            }
        }
        Ok(None)
    }

    fn push(&self, entry: Staged<T>) -> Result<()> {
        self.staged
            .lock()
            .map_err(|_| Error::backend("transaction lock was poisoned"))?
            .push(entry);
        Ok(())
    }
}

/// One mutation, borrowed. The common shape of a staged transaction entry and
/// a deferred write, so both build their ops through the same code.
pub(crate) enum Item<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
    Clear,
}

/// Turn a write-set into backend ops.
///
/// Async because it is GC-aware: overwriting or deleting a key that holds a
/// chunk pointer has to look the old pointer up so its chunks are dropped in
/// the same commit. Staging bare `Op::Put`s instead is what orphaned chunks
/// forever on the `put_all` / `transact` path.
///
/// The write-set is collapsed first — everything before the last `clear` is
/// dropped, and only the last action per key survives — so one key written
/// twice allocates one value id, not two (the first of which nothing would
/// ever point at).
pub(crate) async fn ops_for_items(
    inner: &Inner,
    staged: &[Item<'_>],
    mode: TxMode,
) -> Result<Vec<Op>> {
    let mut ops = Vec::new();

    let tail = match staged.iter().rposition(|e| matches!(e, Item::Clear)) {
        Some(i) => {
            ops.extend(inner.clear_value_ops().await?);
            &staged[i + 1..]
        }
        None => staged,
    };

    enum Action<'a> {
        Put(&'a [u8]),
        Delete,
    }

    let mut actions: BTreeMap<&[u8], Action<'_>> = BTreeMap::new();
    for entry in tail {
        match entry {
            Item::Put(key, payload) => {
                actions.insert(key, Action::Put(payload));
            }
            Item::Delete(key) => {
                actions.insert(key, Action::Delete);
            }
            // None can remain: `tail` starts after the last clear.
            Item::Clear => {}
        }
    }

    for (key, action) in actions {
        match action {
            Action::Delete => ops.extend(inner.delete_value_ops(key, Prior::Unknown).await?),
            Action::Put(payload) => match mode {
                TxMode::Lazy => {
                    ops.extend(
                        inner
                            .put_payload_ops(key, payload.to_vec(), FLAG_POSTCARD, Prior::Unknown)
                            .await?,
                    );
                }
                TxMode::Eager => {
                    let sealed = inner.chain.seal_slice(payload)?;
                    if sealed.len() > inner.config.max_inline {
                        return Err(Error::ValueTooLarge {
                            bytes: sealed.len(),
                            max_inline: inner.config.max_inline,
                        });
                    }
                    // An eager locker never writes a pointer itself, but the
                    // same name may have been written through a lazy handle,
                    // so still GC what is actually there.
                    if let Some(existing) = inner.fetch(key).await? {
                        if is_pointer(&existing) {
                            if let Ok(pointer) = ChunkPointer::parse(&existing) {
                                ops.push(gc_ops(&pointer));
                            }
                        }
                    }
                    ops.push(Op::Put {
                        table: Table::Records,
                        key: inner.encode_key(key),
                        value: sealed,
                    });
                }
            },
        }
    }

    Ok(ops)
}

/// The deferred-write twin of [`ops_for_items`], over a staged batch.
pub(crate) async fn ops_for_pending(
    inner: &Inner,
    staged: &[super::resident::Pending],
    mode: TxMode,
) -> Result<Vec<Op>> {
    use super::resident::Pending;
    let items: Vec<Item<'_>> = staged
        .iter()
        .map(|entry| match entry {
            Pending::Put { key, payload } => Item::Put(key, payload),
            Pending::Delete { key } => Item::Delete(key),
            Pending::Clear => Item::Clear,
        })
        .collect();
    ops_for_items(inner, &items, mode).await
}
