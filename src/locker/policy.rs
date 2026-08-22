//! Per-locker limits and retention policy.
//!
//! # Commit × Durability
//!
//! Two independent knobs decide when a write is safe, and they answer
//! different questions. [`Commit`] decides **when a commit happens**;
//! [`Durability`] decides **how hard that commit works to reach the disk**.
//! All four combinations are legal and each is useful:
//!
//! | | `Durability::Immediate` (default) | `Durability::Eventual` |
//! |---|---|---|
//! | **`Commit::Immediate`** (default) | Safest, slowest. One fsync per `put`; when `put` returns, the data survives a power cut. | One commit per `put`, no fsync. Survives the process dying; a power cut may lose recent writes until `flush`. |
//! | **`Commit::Deferred { after }`** | One fsync per batch of `after` writes. Nothing is stored — at all — until the batch commits or you `flush`. | Cheapest. Neither the batch nor the fsync happens until it fills or you `flush`. |
//!
//! **`flush` covers both.** [`crate::Locker::flush`],
//! [`crate::LazyLocker::flush`] and [`crate::Bank::flush_all`] commit whatever
//! is staged *and*, on an `Eventual` locker, force the backend fsync. One call
//! from `pagehide` or a native stop hook is the whole contract.
//!
//! The web backend does not honour `Durability` at all: IndexedDB has no
//! fsync knob to turn, and its own durability is the browser's business. An
//! `Eventual` locker on IndexedDB behaves exactly like an `Immediate` one, so
//! the setting is portable rather than platform-specific — it just costs
//! nothing there.

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

pub use crate::backend::api::Durability;

/// Limits applied to one locker.
///
/// Cloneable rather than `Copy`: a locker may carry its own filter chain, and
/// a chain is a `dyn Filter` list behind an `Arc`.
#[derive(Debug, Clone)]
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

    /// How hard each commit works to reach the disk. See [`Durability`] and
    /// the table in the module docs.
    pub durability: Durability,

    /// What to do about a record that will not decode. See [`OnCorrupt`].
    pub on_corrupt: OnCorrupt,

    /// Size of one chunk for a lazy value that does not fit inline.
    ///
    /// Peak memory on the streaming path is a small multiple of this, not of
    /// the value. 256 KiB is the starting default; benches may move it.
    pub chunk_size: usize,

    /// The filter chain this locker's values are sealed with.
    ///
    /// `None` — the default — means the bank's chain. Set it to give one
    /// locker a different one: LZ4 on a candle series next to a raw chain on
    /// settings, in the same bank.
    ///
    /// The choice is **persistent**, not a runtime option. The chain id is
    /// recorded in `meta` the first time the locker is opened, and every later
    /// open under a different id is refused with [`crate::Error::SchemaMismatch`]
    /// rather than handing stored bytes to the wrong inverse transform. Chunks
    /// are sealed piece by piece with the same chain, and so is anything a
    /// coherence message carries inline.
    pub chain: Option<std::sync::Arc<crate::codec::FilterChain>>,
}

/// Two configs are equal when they would behave identically. A filter chain
/// compares by its id, which is exactly the thing that gates format
/// compatibility — see [`crate::codec::FilterChain`].
impl PartialEq for LockerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.max_inline == other.max_inline
            && self.eager_budget == other.eager_budget
            && self.policy == other.policy
            && self.commit == other.commit
            && self.durability == other.durability
            && self.on_corrupt == other.on_corrupt
            && self.chunk_size == other.chunk_size
            && self.chain_id() == other.chain_id()
    }
}

impl Eq for LockerConfig {}

impl Default for LockerConfig {
    fn default() -> Self {
        Self {
            max_inline: 256 * 1024,
            eager_budget: 32 * 1024 * 1024,
            policy: Policy::Precious,
            commit: Commit::Immediate,
            durability: Durability::Immediate,
            on_corrupt: OnCorrupt::Fail,
            chunk_size: 256 * 1024,
            chain: None,
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

    /// See [`Durability`]. `Eventual` trades power-cut safety for speed and
    /// hands the caller the duty of calling `flush`.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Whether this locker's commits skip the per-commit fsync.
    pub(crate) fn is_eventual(&self) -> bool {
        matches!(self.durability, Durability::Eventual)
    }

    pub fn with_on_corrupt(mut self, on_corrupt: OnCorrupt) -> Self {
        self.on_corrupt = on_corrupt;
        self
    }

    pub fn with_chunk_size(mut self, bytes: usize) -> Self {
        self.chunk_size = bytes.max(1);
        self
    }

    /// Give this locker its own filter chain instead of the bank's.
    ///
    /// Recorded in `meta` at first open and enforced on every later one. See
    /// [`LockerConfig::chain`].
    pub fn with_chain(mut self, chain: std::sync::Arc<crate::codec::FilterChain>) -> Self {
        self.chain = Some(chain);
        self
    }

    /// This locker's own chain id, if it has one.
    pub(crate) fn chain_id(&self) -> Option<u8> {
        self.chain.as_ref().map(|c| c.id())
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
        assert_eq!(c.durability, Durability::Immediate);
    }

    #[test]
    fn durability_is_immediate_unless_asked_otherwise() {
        // The same rule as `commit`: nobody loses a write they did not ask to
        // risk. `Eventual` must be typed out.
        assert_eq!(Durability::default(), Durability::Immediate);
        assert!(!LockerConfig::default().is_eventual());
        let c = LockerConfig::default().with_durability(Durability::Eventual);
        assert_eq!(c.durability, Durability::Eventual);
        assert!(c.is_eventual());
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
            .with_chunk_size(64)
            .with_durability(Durability::Eventual)
            .with_chain(std::sync::Arc::new(crate::codec::FilterChain::raw()));
        assert_eq!(c.on_corrupt, OnCorrupt::Skip);
        assert_eq!(c.durability, Durability::Eventual);
        assert_eq!(c.max_inline, 1024);
        assert_eq!(c.eager_budget, 2048);
        assert_eq!(c.policy, Policy::Evictable { max_bytes: 4096 });
        assert_eq!(c.chunk_size, 64);
        assert_eq!(c.chain_id(), Some(0));
    }

    /// A chain compares by the id that gates format compatibility, not by
    /// pointer identity — two `Arc`s over the same chain must not read as two
    /// different configs.
    #[test]
    fn configs_compare_a_chain_by_its_id() {
        use std::sync::Arc;
        let a = LockerConfig::default().with_chain(Arc::new(crate::codec::FilterChain::raw()));
        let b = LockerConfig::default().with_chain(Arc::new(crate::codec::FilterChain::raw()));
        assert_eq!(a, b);
        assert_ne!(a, LockerConfig::default());
    }
}
