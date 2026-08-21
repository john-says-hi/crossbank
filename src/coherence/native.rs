//! Cross-tab coherence on native: accepted, and deliberately inert.
//!
//! `redb` takes an **exclusive** file lock, so a second process cannot open
//! the same bank at all — there is no second reader to keep coherent, and
//! in-process handles already share one backend. The flag is accepted so that
//! consumer code compiles and reads identically on both targets rather than
//! sprouting a `cfg`.

use super::api::Announcement;
use crate::backend::api::Op;
use crate::key::LockerId;

#[derive(Debug, Default)]
pub(crate) struct Coherence;

impl Coherence {
    pub(crate) fn disabled() -> Self {
        Self
    }

    /// Named for symmetry with the web implementation. Opens nothing.
    pub(crate) fn open(_name: &str) -> Self {
        Self
    }

    #[allow(dead_code)] // the web half asks; nothing native does.
    pub(crate) fn is_enabled(&self) -> bool {
        false
    }

    pub(crate) fn prepare(&self, _locker_id: LockerId, _ops: &[Op]) -> Option<Announcement> {
        None
    }

    pub(crate) fn post(&self, _announcement: Announcement) {}

    pub(crate) fn close(&self) {}
}
