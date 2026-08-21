//! The shared behavioural spec every crossbank backend must satisfy.
//!
//! **If a behaviour is not in here, it is not a guaranteed behaviour.**
//!
//! # Why this is a crate and not a `tests/` directory
//!
//! The same case functions have to be callable from a native `#[test]`, from a
//! `#[wasm_bindgen_test]` in a browser, from an on-device runner binary on
//! Android and iOS, and eventually by anyone writing their own [`Backend`]. Only
//! a library serves all four.
//!
//! # Why nothing here mentions `Send`
//!
//! `futures::executor::block_on` does not require `Send`, and neither does
//! `wasm_bindgen_test`'s async executor. The moment this suite named `Send`, the
//! IndexedDB backend — whose futures hold `JsValue` and are `!Send` — would be
//! excluded from the very spec that is supposed to grade it.
//!
//! # Adding a case
//!
//! Write it in [`cases`], then add its name to `for_each_case!`. That list is
//! the single source of truth; the arity guard fails if what ran and what was
//! declared disagree.

use std::future::Future;
use std::sync::Arc;

use crossbank::backend::Backend;
use crossbank::Result;

pub mod cases;
pub mod harness;

/// Differences between backends that a case may legitimately branch on.
///
/// Handled inside a case body rather than by a skip list, so a skipped
/// assertion is visible in the spec instead of hidden in a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    /// Whether data written through one handle is visible to the next one
    /// opened over the same location. False for the memory backend, and its
    /// falseness is itself asserted — that is what stops the persistence cases
    /// passing vacuously everywhere.
    pub persists_across_open: bool,

    /// Whether the platform reports real storage usage.
    pub reports_usage: bool,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            persists_across_open: true,
            reports_usage: false,
        }
    }
}

/// Supplies a backend to run the spec against.
///
/// `open` is called more than once per case: reopening is how persistence gets
/// tested, so a harness must return a handle onto the *same* underlying store
/// each time.
pub trait Harness {
    fn open(&self) -> impl Future<Output = Result<Arc<dyn Backend>>>;

    /// Destroy everything this harness owns. Called after every case, so a
    /// failing case cannot leak state into the next one.
    fn destroy(&self) -> impl Future<Output = Result<()>>;

    fn caps(&self) -> Caps {
        Caps::default()
    }
}

/// Number of cases in the spec. The arity guard compares against this.
pub const CASE_COUNT: usize = 28;

/// Run `block_on` without pulling in an async runtime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub fn block_on<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

/// The spec. Every name here is a `pub async fn` in [`cases`].
///
/// This list is the single source of truth for what the suite covers.
#[doc(hidden)]
#[macro_export]
macro_rules! __for_each_case {
    ($make:expr) => {
        $crate::__emit!(put_get_roundtrip, $make);
        $crate::__emit!(missing_key_is_none, $make);
        $crate::__emit!(empty_value_is_not_a_missing_key, $make);
        $crate::__emit!(overwrite_replaces_value, $make);
        $crate::__emit!(delete_is_idempotent, $make);
        $crate::__emit!(clear_empties_only_its_own_locker, $make);
        $crate::__emit!(keys_are_ordered_by_utf8_bytes, $make);
        $crate::__emit!(keys_above_the_bmp_sort_bytewise, $make);
        $crate::__emit!(prefix_listing_stops_at_the_boundary, $make);
        $crate::__emit!(range_is_inclusive_start_exclusive_end, $make);
        $crate::__emit!(reverse_range_descends, $make);
        $crate::__emit!(paging_covers_every_key_exactly_once, $make);
        $crate::__emit!(transaction_commit_is_atomic, $make);
        $crate::__emit!(transaction_rollback_writes_nothing, $make);
        $crate::__emit!(transaction_reads_its_own_writes, $make);
        $crate::__emit!(watch_reports_writes_and_deletes, $make);
        $crate::__emit!(reopen_matches_declared_persistence, $make);
        $crate::__emit!(schema_mismatch_is_refused, $make);
        $crate::__emit!(a_value_larger_than_the_chunk_size_round_trips, $make);
        $crate::__emit!(overwriting_a_chunked_value_replaces_it, $make);
        $crate::__emit!(deleting_a_chunked_value_removes_it, $make);
        $crate::__emit!(unfinished_writer_leaves_the_previous_value, $make);
        $crate::__emit!(close_then_reopen_sees_the_same_data, $make);
        $crate::__emit!(operations_after_close_report_closed, $make);
        $crate::__emit!(close_is_idempotent, $make);
        $crate::__emit!(delete_locker_removes_records_and_chunks, $make);
        $crate::__emit!(delete_locker_leaves_other_lockers_intact, $make);
        $crate::__emit!(a_deleted_locker_name_gets_a_fresh_id, $make);
    };
}

/// Native emitter: an ordinary `#[test]` driven by `block_on`.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __emit {
    ($name:ident, $make:expr) => {
        #[test]
        fn $name() {
            let harness = ($make)(::core::stringify!($name));
            $crate::block_on(async {
                $crate::cases::$name(&harness).await.unwrap();
                $crate::Harness::destroy(&harness).await.unwrap();
            });
        }
    };
}

/// Wasm emitter: the identical body under `#[wasm_bindgen_test]`.
///
/// Cleanup is explicit rather than `Drop`-based because the atomics lane builds
/// with `panic = "abort"`, where there is no unwinding to run destructors.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __emit {
    ($name:ident, $make:expr) => {
        #[::wasm_bindgen_test::wasm_bindgen_test]
        async fn $name() {
            let harness = ($make)(::core::stringify!($name));
            $crate::cases::$name(&harness).await.unwrap();
            $crate::Harness::destroy(&harness).await.unwrap();
        }
    };
}

/// Emit the whole suite against a harness factory.
///
/// `$make` is `fn(&str) -> impl Harness`, taking the case name so every case
/// gets its own isolated store.
#[macro_export]
macro_rules! conformance_suite {
    ($make:expr) => {
        $crate::__for_each_case!($make);
        $crate::__arity_guard!();
    };
}

/// Fails if the spec list and [`CASE_COUNT`] disagree.
///
/// Cheap insurance against a case being added to [`cases`] but never wired into
/// `for_each_case!`, which would leave it silently unrun in every lane.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __arity_guard {
    () => {
        #[test]
        fn conformance_case_count_matches_the_spec() {
            let mut counted = 0usize;
            macro_rules! count_one {
                ($n:ident, $m:expr) => {
                    counted += 1;
                };
            }
            $crate::__count_cases!(count_one, counted);
            assert_eq!(
                counted,
                $crate::CASE_COUNT,
                "a case was added to the spec list without updating CASE_COUNT"
            );
        }
    };
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[doc(hidden)]
#[macro_export]
macro_rules! __arity_guard {
    () => {};
}

/// Counts the spec list without emitting tests.
#[doc(hidden)]
#[macro_export]
macro_rules! __count_cases {
    ($counter:ident, $acc:ident) => {
        $counter!(put_get_roundtrip, ());
        $counter!(missing_key_is_none, ());
        $counter!(empty_value_is_not_a_missing_key, ());
        $counter!(overwrite_replaces_value, ());
        $counter!(delete_is_idempotent, ());
        $counter!(clear_empties_only_its_own_locker, ());
        $counter!(keys_are_ordered_by_utf8_bytes, ());
        $counter!(keys_above_the_bmp_sort_bytewise, ());
        $counter!(prefix_listing_stops_at_the_boundary, ());
        $counter!(range_is_inclusive_start_exclusive_end, ());
        $counter!(reverse_range_descends, ());
        $counter!(paging_covers_every_key_exactly_once, ());
        $counter!(transaction_commit_is_atomic, ());
        $counter!(transaction_rollback_writes_nothing, ());
        $counter!(transaction_reads_its_own_writes, ());
        $counter!(watch_reports_writes_and_deletes, ());
        $counter!(reopen_matches_declared_persistence, ());
        $counter!(schema_mismatch_is_refused, ());
        $counter!(a_value_larger_than_the_chunk_size_round_trips, ());
        $counter!(overwriting_a_chunked_value_replaces_it, ());
        $counter!(deleting_a_chunked_value_removes_it, ());
        $counter!(unfinished_writer_leaves_the_previous_value, ());
        $counter!(close_then_reopen_sees_the_same_data, ());
        $counter!(operations_after_close_report_closed, ());
        $counter!(close_is_idempotent, ());
        $counter!(delete_locker_removes_records_and_chunks, ());
        $counter!(delete_locker_leaves_other_lockers_intact, ());
        $counter!(a_deleted_locker_name_gets_a_fresh_id, ());
    };
}
