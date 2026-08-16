//! Machinery shared by the eager and lazy lockers.
//!
//! Everything here is key/value plumbing over a [`Backend`]: encode a user key
//! into locker-prefixed bytes, seal a value through the filter chain, page a
//! scan to completion. Neither locker type owns any of it.

use std::ops::Bound;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::api::{Backend, Op, ScanRequest, Table};
use crate::codec::{self, FilterChain};
use crate::error::Result;
use crate::key::{self, LockerId};

use super::policy::LockerConfig;
use crate::watch::{Event, Watchers};

/// How many records a single scan page pulls. Bounded because an IndexedDB
/// cursor cannot outlive its transaction, so every scan pages regardless.
pub(crate) const SCAN_PAGE: usize = 256;

pub(crate) struct Inner {
    /// Held for the duration of a transaction, so two overlapping transactions
    /// on one locker cannot lose each other's updates. A futures mutex rather
    /// than a std one because it is held across await points.
    pub(crate) write_lock: futures::lock::Mutex<()>,
    pub(crate) backend: Arc<dyn Backend>,
    pub(crate) chain: Arc<FilterChain>,
    pub(crate) id: LockerId,
    pub(crate) name: String,
    pub(crate) config: LockerConfig,
    pub(crate) watchers: Watchers,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locker")
            .field("name", &self.name)
            .field("id", &self.id)
            .finish()
    }
}

impl Inner {
    pub(crate) fn encode_key(&self, key: &str) -> Vec<u8> {
        key::encode(self.id, key)
    }

    pub(crate) fn seal<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        codec::encode(value, &self.chain)
    }

    pub(crate) fn open<T: DeserializeOwned>(&self, stored: &[u8]) -> Result<T> {
        codec::decode(stored, &self.chain)
    }

    pub(crate) fn put_op<T: Serialize>(&self, key: &str, value: &T) -> Result<Op> {
        Ok(Op::Put {
            table: Table::Records,
            key: self.encode_key(key),
            value: self.seal(value)?,
        })
    }

    pub(crate) fn delete_op(&self, key: &str) -> Op {
        Op::Delete {
            table: Table::Records,
            key: self.encode_key(key),
        }
    }

    pub(crate) fn clear_op(&self) -> Op {
        Op::DeleteRange {
            table: Table::Records,
            range: key::locker_range(self.id),
        }
    }

    pub(crate) async fn fetch(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let encoded = self.encode_key(key);
        self.backend.get(Table::Records, &encoded).await
    }

    pub(crate) async fn commit(&self, ops: Vec<Op>) -> Result<()> {
        self.backend.commit(ops).await
    }

    /// Announce a change. Called only AFTER a commit lands, so a subscriber is
    /// never told about a write that did not happen.
    pub(crate) fn announce(&self, event: Event) {
        self.watchers.broadcast(&event);
    }

    /// Walk every record in a range, paging until exhausted.
    ///
    /// `want_values` is passed through so a keys-only walk does not make the
    /// backend read payloads it would immediately discard — which matters a
    /// great deal for a lazy locker opening over a large candle cache.
    pub(crate) async fn walk(
        &self,
        start: Bound<&str>,
        end: Bound<&str>,
        reverse: bool,
        want_values: bool,
        mut visit: impl FnMut(String, Option<Vec<u8>>) -> Result<()>,
    ) -> Result<()> {
        let mut range = key::encode_range(self.id, start, end);

        loop {
            let page = self
                .backend
                .scan(ScanRequest {
                    table: Table::Records,
                    range: range.clone(),
                    reverse,
                    limit: SCAN_PAGE,
                    want_values,
                })
                .await?;

            for (encoded, value) in &page.items {
                let user_key = key::decode(self.id, encoded)?;
                visit(user_key.to_string(), value.clone())?;
            }

            match page.resume {
                // Excluding the last key returned works identically in both
                // directions, which is what keeps paging free of an
                // off-by-one that would differ per backend.
                Some(last) => {
                    if reverse {
                        range.end = Bound::Excluded(last);
                    } else {
                        range.start = Bound::Excluded(last);
                    }
                }
                None => break,
            }
        }

        Ok(())
    }
}
