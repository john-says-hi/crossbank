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

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::Op;
use crate::error::{Error, Result};

use super::inner::Inner;

/// One staged mutation.
pub(crate) enum Staged<T> {
    Put {
        key: Vec<u8>,
        bytes: Vec<u8>,
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

    /// Turn a drained write-set into backend ops.
    pub(crate) fn ops_for(inner: &Inner, staged: &[Staged<T>]) -> Vec<Op> {
        staged
            .iter()
            .map(|entry| match entry {
                Staged::Put { key, bytes, .. } => Op::Put {
                    table: crate::backend::api::Table::Records,
                    key: inner.encode_key(key),
                    value: bytes.clone(),
                },
                Staged::Delete { key } => inner.delete_op(key),
                Staged::Clear => inner.clear_op(),
            })
            .collect()
    }

    /// Stage a write. Encoding happens now, on this thread, so no user code
    /// ever runs while a backend transaction is open.
    pub fn put(&self, key: &str, value: T) -> Result<()> {
        self.put_by(key.as_bytes(), value)
    }

    /// As [`Transaction::put`], under a binary key.
    pub fn put_by(&self, key: &[u8], value: T) -> Result<()> {
        let bytes = self.inner.seal(&value)?;
        self.push(Staged::Put {
            key: key.to_vec(),
            bytes,
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
            return staged.map(|bytes| self.inner.open(&bytes)).transpose();
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

    /// `None` when this transaction has not touched the key at all;
    /// `Some(None)` when it has deleted or cleared it.
    fn staged_view(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        let guard = self
            .staged
            .lock()
            .map_err(|_| Error::backend("transaction lock was poisoned"))?;

        // Later entries win, so scan backwards and stop at the first hit.
        for entry in guard.iter().rev() {
            match entry {
                Staged::Put { key: k, bytes, .. } if k == key => {
                    return Ok(Some(Some(bytes.clone())))
                }
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
