//! Cross-tab coherence on native: accepted, and deliberately inert.
//!
//! `redb` takes an **exclusive** file lock, so a second process cannot open
//! the same bank at all — there is no second reader to keep coherent, and
//! in-process handles already share one backend. The flag is accepted so that
//! consumer code compiles and reads identically on both targets rather than
//! sprouting a `cfg`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::api::Announcement;
use crate::backend::api::Op;
use crate::key::LockerId;

#[derive(Debug, Default)]
pub(crate) struct Coherence {
    /// Nothing native reads this in production. It exists so the *contract*
    /// the web half depends on — that dropping a `Coherence` unregisters it,
    /// whether or not anybody called `close` — is testable on a lane that
    /// runs on every machine.
    ///
    /// On the web the same `Drop` clears `BroadcastChannel.onmessage` and
    /// closes the channel. Skipping it there leaves the browser holding a raw
    /// pointer into a freed `Closure`: a use-after-free that a wasm test
    /// cannot observe, because nothing goes wrong until something else does.
    closed: Arc<AtomicBool>,
}

impl Coherence {
    pub(crate) fn disabled() -> Self {
        Self::default()
    }

    /// Named for symmetry with the web implementation. Opens nothing.
    pub(crate) fn open(_name: &str) -> Self {
        Self::default()
    }

    /// A handle onto the closed flag that outlives the `Coherence` itself, so
    /// a test can look at it after the drop.
    #[cfg(test)]
    pub(crate) fn close_witness(&self) -> Arc<AtomicBool> {
        self.closed.clone()
    }

    #[allow(dead_code)] // the web half asks; nothing native does.
    pub(crate) fn is_enabled(&self) -> bool {
        false
    }

    pub(crate) fn prepare(&self, _locker_id: LockerId, _ops: &[Op]) -> Option<Announcement> {
        None
    }

    pub(crate) fn post(&self, _announcement: Announcement) {}

    /// Idempotent, exactly as on the web, so running both this and `Drop` is
    /// the normal case rather than a mistake.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

/// The backstop for a bank dropped without [`crate::Bank::close`].
///
/// See the note on [`Coherence::closed`] for why this is worth having on a
/// target where coherence does nothing.
impl Drop for Coherence {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dropping a `Coherence` must unregister it, even when nobody called
    /// `close`. On the web that is what stops the browser calling into a
    /// freed `Closure`; here it is the same contract, observable.
    #[test]
    fn dropping_unregisters_without_a_close_call() {
        let witness = {
            let coherence = Coherence::open("never-closed");
            let witness = coherence.close_witness();
            assert!(!witness.load(Ordering::Acquire), "not closed yet");
            witness
            // dropped here, with no `close()` call
        };
        assert!(
            witness.load(Ordering::Acquire),
            "Drop must do what close does; on wasm skipping it is a use-after-free"
        );
    }

    /// And calling both is fine, which is what makes the backstop safe to add.
    #[test]
    fn close_then_drop_is_idempotent() {
        let coherence = Coherence::open("closed-twice");
        let witness = coherence.close_witness();
        coherence.close();
        assert!(witness.load(Ordering::Acquire));
        drop(coherence);
        assert!(witness.load(Ordering::Acquire));
    }
}
