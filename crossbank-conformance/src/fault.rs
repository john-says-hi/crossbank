//! A [`Backend`] decorator that injects a single, precisely-placed failure.
//!
//! # Why this lives in the spec crate
//!
//! "What does crossbank do when the write does not land?" is a *behavioural*
//! question, so its answers belong in the conformance suite where every
//! backend has to give the same one — not in a native-only test file where
//! IndexedDB, the backend most likely to fail in the field, would never be
//! asked.
//!
//! # How a plan is aimed
//!
//! [`Fault`] counts the [`Op`]s that pass through `commit` / `commit_with`,
//! starting from zero the moment [`Fault::arm`] is called. `at_op` is an index
//! into that stream. Arming *after* the bank and locker are open is what makes
//! a case deterministic: locker registration commits are simply not counted.
//!
//! A plan is **one-shot**. It fires once and disarms itself, so the case can
//! then read the store back through the same handle to see what survived.
//!
//! * [`Injection::Abort`], [`Injection::Io`] and [`Injection::Quota`] fail the
//!   commit that covers `at_op`, **before** any of it reaches the inner
//!   backend. Nothing is written, which is exactly the torn-commit question.
//! * [`Injection::Truncate`] and [`Injection::Corrupt`] mutate the value of the
//!   first `Op::Put` at or after `at_op` and then let the commit through, so
//!   the damage is real stored bytes rather than a read-time trick.
//!
//! # Why `Brittle` in `tests/deferred_batches.rs` was left alone
//!
//! It answers a different question — a backend that stays broken across many
//! commits and is then repaired — which a one-shot plan aimed at an op index
//! cannot express. Folding one into the other would make both harder to read.
//! `Fault` is the public one; a native-only file may still keep a decorator of
//! its own for a native-only question.
//!
//! # Rules this file follows
//!
//! * The `std::sync::Mutex` is **never** held across an await. Every decision
//!   is made and the guard dropped before the inner backend is touched.
//! * No `std::time`, no threads: it compiles and runs on `wasm32` unchanged.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crossbank::backend::{BFut, Backend, CommitOptions, Op, ScanPage, ScanRequest, Table, Usage};
use crossbank::{Error, Result};

/// What to do to the commit that `at_op` lands in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Injection {
    /// Refuse the commit before the backend sees it, as a process killed
    /// between staging and durability would.
    Abort,
    /// Refuse it with a backend I/O error.
    Io,
    /// Refuse it with [`Error::QuotaExceeded`], the browser's failure mode.
    Quota,
    /// Store only the first `keep` bytes of a put's value.
    Truncate { keep: usize },
    /// Flip one byte of a put's value, at `flip` modulo its length.
    Corrupt { flip: usize },
}

/// One armed injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultPlan {
    /// Index into the op stream since [`Fault::arm`].
    pub at_op: usize,
    pub injection: Injection,
}

impl FaultPlan {
    pub const fn new(at_op: usize, injection: Injection) -> Self {
        Self { at_op, injection }
    }
}

/// Wraps a backend and injects at most one planned failure.
pub struct Fault<B: Backend> {
    inner: B,
    plan: Mutex<Option<FaultPlan>>,
    ops: AtomicUsize,
}

impl<B: Backend> Fault<B> {
    pub fn new(inner: B) -> Self {
        Self {
            inner,
            plan: Mutex::new(None),
            ops: AtomicUsize::new(0),
        }
    }

    /// Arm `plan` and restart the op counter, so `at_op` is relative to now.
    ///
    /// Returns an error only if a previous panic poisoned the lock, which
    /// under `panic = "abort"` cannot happen at all.
    pub fn arm(&self, plan: FaultPlan) -> Result<()> {
        let mut guard = self.lock()?;
        *guard = Some(plan);
        self.ops.store(0, Ordering::Release);
        Ok(())
    }

    /// Whether the armed plan has already fired.
    pub fn fired(&self) -> Result<bool> {
        Ok(self.lock()?.is_none())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Option<FaultPlan>>> {
        self.plan
            .lock()
            .map_err(|_| Error::backend("the fault plan lock was poisoned"))
    }

    /// Decide what happens to this commit. Never awaits, never holds the lock
    /// past its own return.
    fn intercept(&self, mut ops: Vec<Op>) -> Result<Vec<Op>> {
        let count = ops.len();
        let start = self.ops.fetch_add(count, Ordering::AcqRel);
        let end = start + count;

        let mut guard = self.lock()?;
        let Some(plan) = *guard else {
            return Ok(ops);
        };

        match plan.injection {
            Injection::Abort | Injection::Io | Injection::Quota => {
                // Fire on the commit that covers `at_op` — or on the first one
                // past it, so a plan aimed at an op that never arrived still
                // fails something rather than sleeping forever.
                if plan.at_op >= end {
                    return Ok(ops);
                }
                *guard = None;
                drop(guard);
                Err(match plan.injection {
                    Injection::Abort => {
                        Error::backend("fault: the commit was aborted before it reached storage")
                    }
                    Injection::Quota => Error::QuotaExceeded {
                        needed: ops_bytes(&ops) as u64,
                        available: None,
                    },
                    _ => Error::backend("fault: the device reported an I/O error"),
                })
            }
            Injection::Truncate { .. } | Injection::Corrupt { .. } => {
                let from = plan.at_op.max(start);
                let Some(index) =
                    (from..end).find(|i| matches!(ops.get(i - start), Some(Op::Put { .. })))
                else {
                    // Nothing to damage in this commit; stay armed.
                    return Ok(ops);
                };
                if let Some(Op::Put { value, .. }) = ops.get_mut(index - start) {
                    match plan.injection {
                        Injection::Truncate { keep } => value.truncate(keep),
                        Injection::Corrupt { flip } if !value.is_empty() => {
                            let at = flip % value.len();
                            value[at] ^= 0xFF;
                        }
                        // Every other variant returned from the arm above.
                        _ => {}
                    }
                }
                *guard = None;
                Ok(ops)
            }
        }
    }
}

fn ops_bytes(ops: &[Op]) -> usize {
    ops.iter()
        .map(|op| match op {
            Op::Put { key, value, .. } => key.len() + value.len(),
            Op::Delete { key, .. } => key.len(),
            Op::DeleteRange { .. } => 0,
        })
        .sum()
}

impl<B: Backend> Backend for Fault<B> {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        self.inner.get(table, key)
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        self.inner.get_many(table, keys)
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        self.inner.scan(request)
    }

    fn scan_page_size(&self) -> usize {
        self.inner.scan_page_size()
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        match self.intercept(ops) {
            Ok(ops) => self.inner.commit(ops),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn commit_with(&self, ops: Vec<Op>, options: CommitOptions) -> BFut<'_, ()> {
        match self.intercept(ops) {
            Ok(ops) => self.inner.commit_with(ops, options),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        self.inner.usage()
    }

    fn flush(&self) -> BFut<'_, ()> {
        self.inner.flush()
    }

    fn close(&self) -> BFut<'_, ()> {
        self.inner.close()
    }
}

/// A `Backend` an `Arc<dyn Backend>` can be wrapped in.
///
/// [`Fault`] is generic over a concrete `B`, but a [`crate::Harness`] hands
/// out `Arc<dyn Backend>` — and neither `Arc` nor `Backend` is local to this
/// crate, so the obvious `impl Backend for Arc<dyn Backend>` is not allowed.
/// This one-line newtype is.
#[derive(Clone)]
pub struct Shared(pub Arc<dyn Backend>);

impl Backend for Shared {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        self.0.get(table, key)
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        self.0.get_many(table, keys)
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        self.0.scan(request)
    }

    fn scan_page_size(&self) -> usize {
        self.0.scan_page_size()
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        self.0.commit(ops)
    }

    fn commit_with(&self, ops: Vec<Op>, options: CommitOptions) -> BFut<'_, ()> {
        self.0.commit_with(ops, options)
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        self.0.usage()
    }

    fn flush(&self) -> BFut<'_, ()> {
        self.0.flush()
    }

    fn close(&self) -> BFut<'_, ()> {
        self.0.close()
    }
}

/// The shape a harness hands to a case: usable as a backend, and armable.
pub type SharedFault = Fault<Shared>;
