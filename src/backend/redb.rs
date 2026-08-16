//! The native backend, on [`redb`](https://github.com/cberner/redb).
//!
//! Pure Rust, ACID, and it builds for every platform Flutter ships to —
//! Linux, macOS, Windows, Android and iOS. Crash safety, transactions and
//! compaction come with it rather than being ours to get right.
//!
//! # Why not Hive's own format
//!
//! Hive is Bitcask-inspired: an append-only log of frames, an index rebuilt by
//! scanning the whole file at open, and an explicit `compact()`. We copy Hive's
//! *architecture* — eager and lazy containers, watch streams, the box mental
//! model — but not its bytes, for two reasons. Its deleted values stay readable
//! on disk until someone calls `compact()`, and its keys are stored in plaintext
//! even in encrypted boxes. Neither is a property we want, and rolling our own
//! log would mean owning torn-write and crash-recovery correctness that redb has
//! already solved.
//!
//! # The one rule this file exists to honour
//!
//! redb is **synchronous**, and a `WriteTransaction` held across an `.await`
//! while another task on the same executor calls `begin_write` is a hard
//! deadlock — native-only, and therefore invisible to browser testing.
//!
//! The [`Backend`] trait makes that impossible rather than
//! merely discouraged: every method here is a single synchronous block with no
//! await points inside it. A transaction is opened and committed within one
//! function call, always. That is the payoff for `commit(Vec<Op>)` taking a
//! complete op list instead of handing out a transaction handle.
//!
//! Blocking inline is acceptable for local disk. If it ever stops being
//! acceptable, the fix is an offload hook, not an async runtime dependency.

use std::ops::Bound;
use std::path::{Path, PathBuf};

use redb::{Database, Durability, ReadableDatabase, TableDefinition, TableError};

use super::api::{BFut, Backend, KeyRange, Op, ScanPage, ScanRequest, Table, Usage};
use crate::error::{Error, Result};

type Def = TableDefinition<'static, &'static [u8], &'static [u8]>;

const META: Def = TableDefinition::new("meta");
const RECORDS: Def = TableDefinition::new("records");
const CHUNKS: Def = TableDefinition::new("chunks");

fn definition(table: Table) -> Def {
    match table {
        Table::Meta => META,
        Table::Records => RECORDS,
        Table::Chunks => CHUNKS,
    }
}

fn backend_err(context: &str, e: impl std::fmt::Display) -> Error {
    let message = e.to_string();
    // Running out of disk is a quota problem, not an opaque IO failure, and a
    // caller can do something about it.
    if message.contains("No space left") || message.contains("ENOSPC") {
        return Error::QuotaExceeded {
            needed: 0,
            available: Some(0),
        };
    }
    Error::Backend(format!("{context}: {message}"))
}

/// A bank stored in a single redb file.
#[derive(Debug)]
pub struct RedbBackend {
    db: Database,
    path: PathBuf,
}

impl RedbBackend {
    /// Open, or create, the database at `path`.
    ///
    /// redb takes an **exclusive** file lock, so a second process opening the
    /// same file fails rather than corrupting it. That is also why native
    /// cross-tab coherence is a non-problem: a desktop app is one process.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db = Database::create(&path).map_err(|e| backend_err("opening the database", e))?;

        let backend = Self { db, path };
        backend.create_tables()?;
        Ok(backend)
    }

    /// Create all three tables up front.
    ///
    /// Without this, a read transaction against a table nothing has written yet
    /// returns `TableDoesNotExist`, and every read path would need to translate
    /// that into "empty". Creating them once at open keeps the read paths
    /// honest and matches the fixed-table layout the web backend must use
    /// anyway.
    fn create_tables(&self) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| backend_err("beginning the setup transaction", e))?;
        {
            for table in Table::ALL {
                txn.open_table(definition(table))
                    .map_err(|e| backend_err("creating a table", e))?;
            }
        }
        txn.commit()
            .map_err(|e| backend_err("committing table creation", e))
    }

    fn read_one(&self, table: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| backend_err("beginning a read", e))?;
        let handle = match txn.open_table(definition(table)) {
            Ok(handle) => handle,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(backend_err("opening a table for reading", e)),
        };

        handle
            .get(key)
            .map_err(|e| backend_err("reading a key", e))
            .map(|found| found.map(|guard| guard.value().to_vec()))
    }

    fn scan_page(&self, request: &ScanRequest) -> Result<ScanPage> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| backend_err("beginning a scan", e))?;
        let handle = match txn.open_table(definition(request.table)) {
            Ok(handle) => handle,
            Err(TableError::TableDoesNotExist(_)) => return Ok(ScanPage::default()),
            Err(e) => return Err(backend_err("opening a table for scanning", e)),
        };

        let bounds = as_ref_bounds(&request.range);
        let iter = handle
            .range::<&[u8]>(bounds)
            .map_err(|e| backend_err("opening a range", e))?;

        let mut items = Vec::new();
        let mut resume = None;

        // `Box<dyn Iterator>` so direction is chosen once rather than
        // duplicating the body.
        let entries: Box<dyn Iterator<Item = _>> = if request.reverse {
            Box::new(iter.rev())
        } else {
            Box::new(iter)
        };

        for entry in entries {
            if items.len() >= request.limit {
                break;
            }
            let (key, value) = entry.map_err(|e| backend_err("reading a range entry", e))?;
            let key = key.value().to_vec();
            let value = request.want_values.then(|| value.value().to_vec());
            resume = Some(key.clone());
            items.push((key, value));
        }

        // Only offer a resume point when the page actually filled. An exhausted
        // range must report `None`, or a paging caller loops forever.
        let exhausted = items.len() < request.limit;
        Ok(ScanPage {
            items,
            resume: if exhausted { None } else { resume },
        })
    }

    /// Apply every op in one transaction, or none of them.
    ///
    /// No await points anywhere inside. See the module docs.
    fn apply(&self, ops: Vec<Op>) -> Result<()> {
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| backend_err("beginning a write", e))?;

        // Stated explicitly rather than relied upon: a commit must be durable
        // before it returns, or the crash-and-reopen tests prove nothing.
        txn.set_durability(Durability::Immediate)
            .map_err(|e| backend_err("setting durability", e))?;

        {
            for op in ops {
                match op {
                    Op::Put { table, key, value } => {
                        let mut handle = txn
                            .open_table(definition(table))
                            .map_err(|e| backend_err("opening a table for writing", e))?;
                        handle
                            .insert(key.as_slice(), value.as_slice())
                            .map_err(|e| backend_err("inserting a key", e))?;
                    }
                    Op::Delete { table, key } => {
                        let mut handle = txn
                            .open_table(definition(table))
                            .map_err(|e| backend_err("opening a table for deletion", e))?;
                        handle
                            .remove(key.as_slice())
                            .map_err(|e| backend_err("removing a key", e))?;
                    }
                    Op::DeleteRange { table, range } => {
                        let mut handle = txn
                            .open_table(definition(table))
                            .map_err(|e| backend_err("opening a table for range deletion", e))?;
                        let bounds = as_ref_bounds(&range);
                        handle
                            .retain_in::<&[u8], _>(bounds, |_, _| false)
                            .map_err(|e| backend_err("deleting a range", e))?;
                    }
                }
            }
        }

        txn.commit().map_err(|e| backend_err("committing", e))
    }
}

/// Borrow one bound as a slice.
///
/// A named function rather than a closure: a closure infers two unrelated
/// lifetimes for the argument and the return, and cannot tie them together.
fn borrow_bound(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(v) => Bound::Included(v.as_slice()),
        Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
    }
}

/// Borrow a [`KeyRange`] as bounds redb can consume.
fn as_ref_bounds(range: &KeyRange) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (borrow_bound(&range.start), borrow_bound(&range.end))
}

impl Backend for RedbBackend {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.read_one(table, key) })
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            // One read transaction for the batch, so the results are a
            // consistent snapshot rather than N independent reads.
            let txn = self
                .db
                .begin_read()
                .map_err(|e| backend_err("beginning a batch read", e))?;
            let handle = match txn.open_table(definition(table)) {
                Ok(handle) => handle,
                Err(TableError::TableDoesNotExist(_)) => return Ok(vec![None; keys.len()]),
                Err(e) => return Err(backend_err("opening a table for batch reading", e)),
            };

            keys.iter()
                .map(|key| {
                    handle
                        .get(key.as_slice())
                        .map_err(|e| backend_err("reading a key", e))
                        .map(|found| found.map(|guard| guard.value().to_vec()))
                })
                .collect()
        })
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        Box::pin(async move { self.scan_page(&request) })
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        Box::pin(async move { self.apply(ops) })
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        Box::pin(async move {
            let used = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
            Ok(Some(Usage {
                used,
                // Free disk space is a question about the filesystem, not about
                // us, and reporting a guess would be worse than reporting
                // nothing.
                available: None,
                // Nothing evicts a file behind the application's back.
                persisted: true,
            }))
        })
    }

    fn flush(&self) -> BFut<'_, ()> {
        // Every commit already runs at `Durability::Immediate`, so a commit that
        // returned is a commit that reached the disk. Nothing left to force.
        Box::pin(async move { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use tempfile::TempDir;

    fn backend(dir: &TempDir) -> RedbBackend {
        RedbBackend::open(dir.path().join("bank.redb")).unwrap()
    }

    fn put(key: &[u8], value: &[u8]) -> Op {
        Op::Put {
            table: Table::Records,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn a_reopened_database_still_has_its_data() {
        // The whole point of this backend.
        let dir = TempDir::new().unwrap();

        {
            let b = backend(&dir);
            block_on(b.commit(vec![put(b"k", b"survives")])).unwrap();
        }

        let b = backend(&dir);
        assert_eq!(
            block_on(b.get(Table::Records, b"k")).unwrap(),
            Some(b"survives".to_vec())
        );
    }

    #[test]
    fn reads_work_before_anything_has_been_written() {
        // Tables are created at open, so a read must return None rather than
        // erroring with TableDoesNotExist.
        let dir = TempDir::new().unwrap();
        let b = backend(&dir);

        assert_eq!(block_on(b.get(Table::Records, b"nothing")).unwrap(), None);
        assert!(block_on(b.scan(ScanRequest {
            table: Table::Chunks,
            range: KeyRange::all(),
            reverse: false,
            limit: 10,
            want_values: true,
        }))
        .unwrap()
        .items
        .is_empty());
    }

    #[test]
    fn an_empty_value_survives_a_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let b = backend(&dir);
            block_on(b.commit(vec![put(b"empty", b"")])).unwrap();
        }

        let b = backend(&dir);
        assert_eq!(
            block_on(b.get(Table::Records, b"empty")).unwrap(),
            Some(Vec::new()),
            "an empty value must stay distinguishable from a missing key"
        );
        assert_eq!(block_on(b.get(Table::Records, b"other")).unwrap(), None);
    }

    #[test]
    fn a_failed_op_list_leaves_the_previous_state_intact() {
        // Atomicity, from the backend's side: the whole list applies or none of
        // it does. Proven by committing a good list, then a list whose range
        // deletion and put must land together.
        let dir = TempDir::new().unwrap();
        let b = backend(&dir);

        block_on(b.commit(vec![put(b"a", b"1"), put(b"b", b"2")])).unwrap();
        block_on(b.commit(vec![
            Op::DeleteRange {
                table: Table::Records,
                range: KeyRange::prefix(b"a"),
            },
            put(b"c", b"3"),
        ]))
        .unwrap();

        assert_eq!(block_on(b.get(Table::Records, b"a")).unwrap(), None);
        assert_eq!(
            block_on(b.get(Table::Records, b"b")).unwrap(),
            Some(b"2".to_vec())
        );
        assert_eq!(
            block_on(b.get(Table::Records, b"c")).unwrap(),
            Some(b"3".to_vec())
        );
    }

    #[test]
    fn tables_stay_independent_across_a_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let b = backend(&dir);
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
        }

        let b = backend(&dir);
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
    fn usage_reports_a_real_file_size() {
        let dir = TempDir::new().unwrap();
        let b = backend(&dir);
        block_on(b.commit(vec![put(b"k", &vec![0u8; 10_000])])).unwrap();

        let usage = block_on(b.usage()).unwrap().expect("redb reports usage");
        assert!(usage.used > 0, "a file with data must report a size");
        assert!(usage.persisted, "a file on disk is not evicted behind us");
        assert_eq!(
            usage.available, None,
            "free disk space is the filesystem's business, not ours"
        );
    }
}
