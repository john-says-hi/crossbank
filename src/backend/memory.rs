//! An in-memory backend.
//!
//! Not a toy. It is the oracle the property tests compare real backends
//! against, and it is the backend the whole public API is built against during
//! M1, so its ordering and range semantics *are* the specification.
//!
//! `BTreeMap<Vec<u8>, _>` orders keys bytewise, which is exactly what `redb`
//! does and exactly what IndexedDB does for **binary** keys. That three-way
//! agreement is the reason crossbank encodes keys as bytes rather than handing
//! IndexedDB strings, which it would compare by UTF-16 code unit instead.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use super::api::{BFut, Backend, Op, ScanPage, ScanRequest, Table, Usage};
use crate::error::{Error, Result};

type Tree = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Debug, Default)]
struct Tables {
    meta: Tree,
    records: Tree,
    chunks: Tree,
}

impl Tables {
    fn get(&self, table: Table) -> &Tree {
        match table {
            Table::Meta => &self.meta,
            Table::Records => &self.records,
            Table::Chunks => &self.chunks,
        }
    }

    fn get_mut(&mut self, table: Table) -> &mut Tree {
        match table {
            Table::Meta => &mut self.meta,
            Table::Records => &mut self.records,
            Table::Chunks => &mut self.chunks,
        }
    }
}

/// A backend that keeps everything in memory and loses it on drop.
///
/// Non-persistence is a *specified* behaviour, not an omission: the
/// conformance suite asserts that reopening a memory bank finds nothing, so
/// the persistence cases genuinely discriminate between backends.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    tables: Mutex<Tables>,
    /// Set by [`Backend::close`]. Memory has no handle to release, so the flag
    /// *is* the closure: it makes a closed memory backend behave like a closed
    /// file, which is what lets one conformance case grade every backend.
    closed: AtomicBool,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn with_tables<T>(&self, f: impl FnOnce(&mut Tables) -> T) -> Result<T> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut guard = self
            .tables
            .lock()
            .map_err(|_| Error::backend("memory backend lock was poisoned"))?;
        Ok(f(&mut guard))
    }

    fn apply(tables: &mut Tables, op: Op) {
        match op {
            Op::Put { table, key, value } => {
                tables.get_mut(table).insert(key, value);
            }
            Op::Delete { table, key } => {
                tables.get_mut(table).remove(&key);
            }
            Op::DeleteRange { table, range } => {
                let doomed: Vec<Vec<u8>> = tables
                    .get(table)
                    .keys()
                    .filter(|k| range.contains(k))
                    .cloned()
                    .collect();
                let tree = tables.get_mut(table);
                for key in doomed {
                    tree.remove(&key);
                }
            }
        }
    }
}

/// Collect one page, honouring direction and limit.
///
/// `resume` is the **last key returned**. A caller continues by excluding it:
/// `start = Excluded(resume)` going forward, `end = Excluded(resume)` going
/// backward. Keeping it uniform in both directions avoids an off-by-one that
/// would otherwise differ per backend.
fn scan_tree(tree: &Tree, request: &ScanRequest) -> ScanPage {
    let bounds = (
        as_ref_bound(&request.range.start),
        as_ref_bound(&request.range.end),
    );

    let mut items = Vec::new();
    let mut resume = None;

    // `range` on a BTreeMap yields ascending; reverse it for descending scans.
    let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<u8>)>> = if request.reverse {
        Box::new(tree.range::<[u8], _>(bounds).rev())
    } else {
        Box::new(tree.range::<[u8], _>(bounds))
    };

    for (key, value) in iter {
        if items.len() >= request.limit {
            // There is at least one more key, so the caller must come back.
            break;
        }
        items.push((key.clone(), request.want_values.then(|| value.clone())));
        resume = Some(key.clone());
    }

    // Only advertise a resume point when the page actually filled up. An
    // exhausted range must report `None`, or callers loop forever.
    let exhausted = items.len() < request.limit;
    ScanPage {
        items,
        resume: if exhausted { None } else { resume },
    }
}

fn as_ref_bound(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
    }
}

impl Backend for MemoryBackend {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.with_tables(|t| t.get(table).get(key).cloned()) })
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.with_tables(|t| {
                let tree = t.get(table);
                keys.iter().map(|k| tree.get(k).cloned()).collect()
            })
        })
    }

    /// Larger than the default. There is no transaction to outlive and no
    /// JS boundary to cross here — a range over a BTreeMap already in memory —
    /// so a page is nearly free and paging is nearly pure overhead.
    fn scan_page_size(&self) -> usize {
        1024
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        Box::pin(async move { self.with_tables(|t| scan_tree(t.get(request.table), &request)) })
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        Box::pin(async move {
            // Atomic by virtue of holding the lock for the whole batch: no
            // other caller can observe a partially applied list.
            self.with_tables(|t| {
                for op in ops {
                    Self::apply(t, op);
                }
            })
        })
    }

    fn close(&self) -> BFut<'_, ()> {
        Box::pin(async move {
            // Idempotent, and the data goes with it: a closed memory backend
            // that kept its tables would silently resurrect on reopen.
            if !self.closed.swap(true, Ordering::AcqRel) {
                if let Ok(mut guard) = self.tables.lock() {
                    *guard = Tables::default();
                }
            }
            Ok(())
        })
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        Box::pin(async move {
            let used = self.with_tables(|t| {
                let sum = |tree: &Tree| -> u64 {
                    tree.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
                };
                sum(&t.meta) + sum(&t.records) + sum(&t.chunks)
            })?;
            Ok(Some(Usage {
                used,
                available: None,
                persisted: false,
            }))
        })
    }

    fn flush(&self) -> BFut<'_, ()> {
        // Nothing to flush; there is no durable layer beneath.
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::api::KeyRange;
    use futures::executor::block_on;

    fn scan_all(b: &MemoryBackend, reverse: bool, limit: usize) -> ScanPage {
        block_on(b.scan(ScanRequest {
            table: Table::Records,
            range: KeyRange::all(),
            reverse,
            limit,
            want_values: true,
        }))
        .unwrap()
    }

    fn put(key: &[u8], value: &[u8]) -> Op {
        Op::Put {
            table: Table::Records,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn commit_then_get_round_trips() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"k", b"v")])).unwrap();
        assert_eq!(
            block_on(b.get(Table::Records, b"k")).unwrap(),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn an_empty_value_is_not_a_missing_key() {
        // A real distinction crossbank must preserve. wise_apple's existing
        // Dart bridge collapses these two, treating an empty payload as absent.
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"empty", b"")])).unwrap();

        assert_eq!(
            block_on(b.get(Table::Records, b"empty")).unwrap(),
            Some(Vec::new())
        );
        assert_eq!(block_on(b.get(Table::Records, b"absent")).unwrap(), None);
    }

    #[test]
    fn keys_come_back_in_bytewise_order() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"b", b"1"), put(b"a", b"2"), put(b"c", b"3")])).unwrap();

        let keys: Vec<_> = scan_all(&b, false, 100)
            .items
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn reverse_scan_descends() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"a", b"1"), put(b"b", b"2"), put(b"c", b"3")])).unwrap();

        let keys: Vec<_> = scan_all(&b, true, 100)
            .items
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
    }

    #[test]
    fn an_exhausted_scan_reports_no_resume_point() {
        // The bug this guards against is an infinite paging loop: a range that
        // is finished must say so, not hand back a cursor forever.
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"a", b"1"), put(b"b", b"2")])).unwrap();

        let page = scan_all(&b, false, 100);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.resume, None);
    }

    #[test]
    fn a_full_page_resumes_without_skipping_or_repeating() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![
            put(b"a", b"1"),
            put(b"b", b"2"),
            put(b"c", b"3"),
            put(b"d", b"4"),
        ]))
        .unwrap();

        let mut seen = Vec::new();
        let mut start = Bound::Unbounded;
        loop {
            let page = block_on(b.scan(ScanRequest {
                table: Table::Records,
                range: KeyRange {
                    start: start.clone(),
                    end: Bound::Unbounded,
                },
                reverse: false,
                limit: 2,
                want_values: false,
            }))
            .unwrap();

            seen.extend(page.items.iter().map(|(k, _)| k.clone()));
            match page.resume {
                Some(last) => start = Bound::Excluded(last),
                None => break,
            }
        }

        assert_eq!(
            seen,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
            "paging must cover every key exactly once"
        );
    }

    #[test]
    fn want_values_false_omits_payloads() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"a", b"payload")])).unwrap();

        let page = block_on(b.scan(ScanRequest {
            table: Table::Records,
            range: KeyRange::all(),
            reverse: false,
            limit: 10,
            want_values: false,
        }))
        .unwrap();

        assert_eq!(page.items[0].1, None);
    }

    #[test]
    fn delete_range_removes_only_the_prefix() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![
            put(b"p::1", b"x"),
            put(b"p::2", b"y"),
            put(b"q::1", b"z"),
        ]))
        .unwrap();

        block_on(b.commit(vec![Op::DeleteRange {
            table: Table::Records,
            range: KeyRange::prefix(b"p::"),
        }]))
        .unwrap();

        let keys: Vec<_> = scan_all(&b, false, 100)
            .items
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec![b"q::1".to_vec()]);
    }

    #[test]
    fn tables_are_independent() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![
            Op::Put {
                table: Table::Meta,
                key: b"k".to_vec(),
                value: b"meta".to_vec(),
            },
            Op::Put {
                table: Table::Records,
                key: b"k".to_vec(),
                value: b"record".to_vec(),
            },
        ]))
        .unwrap();

        assert_eq!(
            block_on(b.get(Table::Meta, b"k")).unwrap(),
            Some(b"meta".to_vec())
        );
        assert_eq!(
            block_on(b.get(Table::Records, b"k")).unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(block_on(b.get(Table::Chunks, b"k")).unwrap(), None);
    }

    #[test]
    fn get_many_preserves_request_order_and_gaps() {
        let b = MemoryBackend::new();
        block_on(b.commit(vec![put(b"a", b"1"), put(b"c", b"3")])).unwrap();

        let got = block_on(b.get_many(
            Table::Records,
            vec![b"c".to_vec(), b"missing".to_vec(), b"a".to_vec()],
        ))
        .unwrap();

        assert_eq!(
            got,
            vec![Some(b"3".to_vec()), None, Some(b"1".to_vec())],
            "results must line up positionally with the requested keys"
        );
    }
}
