//! Per-locker limits and retention policy.

/// What happens to a locker's contents when storage runs short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Never shed anything. The right choice for data that cannot be
    /// regenerated — saved reports, user documents, vault contents.
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

impl Default for Policy {
    fn default() -> Self {
        Self::Precious
    }
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
}

impl Default for LockerConfig {
    fn default() -> Self {
        Self {
            max_inline: 256 * 1024,
            eager_budget: 32 * 1024 * 1024,
            policy: Policy::Precious,
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
            .with_policy(Policy::Evictable { max_bytes: 4096 });
        assert_eq!(c.max_inline, 1024);
        assert_eq!(c.eager_budget, 2048);
        assert_eq!(c.policy, Policy::Evictable { max_bytes: 4096 });
    }
}
