//! crossbank — cross-platform persistent key/value storage.
//!
//! One API on native and in the browser. See `PLAN.md` for the design.
//!
//! M0 scaffold: this crate is deliberately empty. Nothing is built until the
//! milestone-zero spikes prove the test lanes actually run.

/// Placeholder so the crate has something to compile and test against
/// while the M0 CI lanes are being proven.
pub const NAME: &str = "crossbank";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_compiles_and_tests_run() {
        assert_eq!(NAME, "crossbank");
    }
}
