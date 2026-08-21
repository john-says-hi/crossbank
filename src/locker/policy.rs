//! Per-locker limits and retention policy.

/// What happens to a locker's contents when storage runs short.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// Never shed anything. The right choice for data that cannot be
    /// regenerated — saved reports, user documents, vault contents.
    ///
    /// The default: silently losing data for anyone who did not think about
    /// retention would be the wrong way round. Opt in to loss, never out.
    #[default]
    Precious,

    /// Shed least-recently-used entries to stay under `max_bytes`.
    ///
    /// A byte budget crossbank actually owns and enforces, deliberately not a
    /// fraction of `navigator.storage.estimate()`. That figure is origin-wide,
    /// moves when other tabs write, and cannot be attributed to a single
    /// locker — so a policy expressed against it would be untestable natively
    /// and unenforceable anywhere.
    Evictable { max_bytes: u64 },
}

/// What opening a locker does when a stored record will not decode.
///
/// Corruption is rare but not impossible: a truncated file, a filter chain
/// swapped without changing its id, a bug in a consumer's own `Filter`. The
/// question this settles is whether one bad record is allowed to make the
/// whole locker unopenable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnCorrupt {
    /// Refuse to open. The default, because silently hiding data loss is the
    /// wrong way round: a caller has to *ask* to carry on without it.
    #[default]
    Fail,

    /// Open anyway, skipping the bad records and listing their keys.
    ///
    /// Nothing is written and nothing is deleted — the stored bytes are left
    /// exactly as they are, so a later build with a fixed decoder can still
    /// read them. Use [`crate::Bank::quarantine`] to remove them deliberately.
    Skip,
}

/// When a write reaches storage.
///
/// The default is [`Commit::Immediate`] and it is never chosen for you.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Commit {
    /// Every write is its own commit, and `put` returns once it has landed.
    ///
    /// The safe answer, and the default: when `put` returns, the data is
    /// stored.
    #[default]
    Immediate,

    /// Stage writes in memory and commit them in batches of `after`.
    ///
    /// **Only one handle per name may use it.** Two handles on one locker each
    /// keep their own staging buffer, so whichever flushed last would win and
    /// the other's writes would be silently overwritten. [`crate::Bank`]
    /// refuses the second open with [`crate::Error::InvalidConfig`] rather
    /// than let that happen — see [`crate::Bank::locker_with`].
    ///
    /// # You must call `flush` yourself
    ///
    /// **crossbank spawns nothing.** There is no timer, no background task and
    /// no destructor that will flush for you — a `Drop` implementation cannot
    /// await, and on the web a closing tab does not run one anyway. Staged
    /// writes that are never flushed are lost, and that is the whole cost of
    /// this mode.
    ///
    /// So a consumer choosing `Deferred` **must** call
    /// [`crate::LazyLocker::flush`], [`crate::Locker::flush`] or
    /// [`crate::Bank::flush_all`]:
    ///
    /// * on the web, from `pagehide` and from `visibilitychange` when the
    ///   document becomes hidden — **not** `beforeunload`, which mobile
    ///   browsers frequently never fire;
    /// * natively, from whatever the application uses as its stop hook.
    ///
    /// `examples/flush_on_pagehide.rs` shows both.
    ///
    /// # What is visible before a flush
    ///
    /// Everything, to this handle: an eager locker updates its resident value
    /// immediately, and a lazy locker sees staged writes through `get`,
    /// `contains_key`, `len` and the key listings. Change events are raised
    /// when the write is staged, because that is when it becomes visible —
    /// which does mean an event can precede a commit that later fails.
    ///
    /// Nothing is visible to another handle, another tab or another process
    /// until it is committed.
    Deferred {
        /// Commit once this many writes are staged. Zero and one both mean
        /// "commit on the next write", which is [`Commit::Immediate`] with
        /// extra steps.
        after: usize,
    },
}

/// Limits applied to one locker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockerConfig {
    /// Largest value an eager locker will accept.
    ///
    /// Eager lockers answer `get()` synchronously and infallibly, so they can
    /// never await a chunk fetch. Anything past this belongs in a lazy locker.
    pub max_inline: usize,

    /// Largest total an eager locker will load at open.
    ///
    /// The guardrail against typing `locker()` where `lazy_locker()` was meant
    /// and quietly pulling hundreds of megabytes into memory. Failing loudly at
    /// open beats discovering it as an out-of-memory abort later.
    pub eager_budget: u64,

    pub policy: Policy,

    /// When writes reach storage. See [`Commit`].
    pub commit: Commit,

    /// What to do about a record that will not decode. See [`OnCorrupt`].
    pub on_corrupt: OnCorrupt,

    /// Size of one chunk for a lazy value that does not fit inline.
    ///
    /// Peak memory on the streaming path is a small multiple of this, not of
    /// the value. 256 KiB is the starting default; benches may move it.
    pub chunk_size: usize,
}

impl Default for LockerConfig {
    fn default() -> Self {
        Self {
            max_inline: 256 * 1024,
            eager_budget: 32 * 1024 * 1024,
            policy: Policy::Precious,
            commit: Commit::Immediate,
            on_corrupt: OnCorrupt::Fail,
            chunk_size: 256 * 1024,
        }
    }
}

impl LockerConfig {
    pub fn with_max_inline(mut self, bytes: usize) -> Self {
        self.max_inline = bytes;
        self
    }

    pub fn with_eager_budget(mut self, bytes: u64) -> Self {
        self.eager_budget = bytes;
        self
    }

    pub fn with_policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }

    /// See [`Commit`]. Deferred writes are never the default, and the caller
    /// takes on the duty of flushing them.
    /// Whether this config actually stages writes.
    ///
    /// `Deferred { after: 0 }` and `after: 1` both mean "commit on the next
    /// write", which is [`Commit::Immediate`] with extra steps, so they are
    /// not deferral and carry none of its restrictions.
    pub(crate) fn defers_writes(&self) -> bool {
        matches!(self.commit, Commit::Deferred { after } if after > 1)
    }

    pub fn with_commit(mut self, commit: Commit) -> Self {
        self.commit = commit;
        self
    }

    pub fn with_on_corrupt(mut self, on_corrupt: OnCorrupt) -> Self {
        self.on_corrupt = on_corrupt;
        self
    }

    pub fn with_chunk_size(mut self, bytes: usize) -> Self {
        self.chunk_size = bytes.max(1);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_conservative() {
        let c = LockerConfig::default();
        assert_eq!(c.max_inline, 256 * 1024);
        assert_eq!(c.eager_budget, 32 * 1024 * 1024);
        assert_eq!(c.policy, Policy::Precious);
        assert_eq!(c.chunk_size, 256 * 1024);
        assert_eq!(c.commit, Commit::Immediate);
    }

    #[test]
    fn writes_are_immediate_unless_asked_otherwise() {
        // Deferring by default would mean data silently lost for anyone who
        // never learned they had to flush. Opt in to the risk, never out.
        assert_eq!(Commit::default(), Commit::Immediate);
        let c = LockerConfig::default().with_commit(Commit::Deferred { after: 8 });
        assert_eq!(c.commit, Commit::Deferred { after: 8 });
    }

    #[test]
    fn failing_on_corruption_is_the_default() {
        // Opening over data we cannot read must be loud. Skipping silently
        // would turn data loss into a shrug.
        assert_eq!(OnCorrupt::default(), OnCorrupt::Fail);
        assert_eq!(LockerConfig::default().on_corrupt, OnCorrupt::Fail);
    }

    #[test]
    fn precious_is_the_default_policy() {
        // Defaulting to evictable would mean data silently disappearing for
        // anyone who did not think about it. Safe by default, opt in to loss.
        assert_eq!(Policy::default(), Policy::Precious);
    }

    #[test]
    fn builders_compose() {
        let c = LockerConfig::default()
            .with_max_inline(1024)
            .with_eager_budget(2048)
            .with_policy(Policy::Evictable { max_bytes: 4096 })
            .with_on_corrupt(OnCorrupt::Skip)
            .with_chunk_size(64);
        assert_eq!(c.on_corrupt, OnCorrupt::Skip);
        assert_eq!(c.max_inline, 1024);
        assert_eq!(c.eager_budget, 2048);
        assert_eq!(c.policy, Policy::Evictable { max_bytes: 4096 });
        assert_eq!(c.chunk_size, 64);
    }
}
