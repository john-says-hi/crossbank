//! Crash-and-reopen tests for the `redb` backend.
//!
//! These spawn a real child process and kill it. An in-process test cannot
//! prove any of this: dropping a handle runs destructors, and even a panic
//! unwinds. Only a process that actually dies tells you what reached the disk.
//!
//! The invariant under test, stated once:
//!
//! > After a process dies at any point, reopening yields either the complete
//! > pre-state or the complete post-state — never a blend — and the reopen
//! > itself succeeds rather than erroring.
//!
//! Scope, honestly: `abort()` kills the process, so the operating system's page
//! cache survives. This tests **process loss**, not power loss. Testing the
//! latter would additionally need `fsync` suppressed, which is a Linux-only
//! nightly job rather than a per-PR one.

#![cfg(not(target_arch = "wasm32"))]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use crossbank::backend::RedbBackend;
use crossbank::Bank;
use futures::executor::block_on;
use tempfile::TempDir;

/// Run the child in a given mode. Returns whether it exited cleanly.
fn run_child(db: &Path, mode: &str) -> bool {
    let status = Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .env("CROSSBANK_CRASH_DB", db)
        .env("CROSSBANK_CRASH_MODE", mode)
        // Abort prints to stderr; keep the test output readable.
        .stderr(std::process::Stdio::null())
        .status()
        .expect("could not spawn the crash child");
    status.success()
}

/// Read every key in the `crash` locker, from a fresh handle.
fn read_back(db: &Path) -> Vec<(String, String)> {
    block_on(async {
        let backend = Arc::new(RedbBackend::open(db).expect("reopen failed"));
        let bank = Bank::with_backend(backend)
            .await
            .expect("bank reopen failed");
        let locker = bank
            .lazy_locker::<String>("crash")
            .await
            .expect("locker reopen failed");
        locker.range(..).await.expect("read failed")
    })
}

fn fixture() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("bank.redb");
    assert!(
        run_child(&db, "baseline"),
        "the baseline child should exit cleanly"
    );
    (dir, db)
}

#[test]
fn a_clean_child_writes_a_readable_baseline() {
    // A control. If this fails, the other two prove nothing, because they would
    // be measuring a broken fixture rather than crash behaviour.
    let (_dir, db) = fixture();

    let entries = read_back(&db);
    assert_eq!(
        entries,
        vec![("baseline".to_string(), "original".to_string())]
    );
}

#[test]
fn a_commit_that_returned_survives_the_process_dying() {
    // The durability claim. redb commits at Durability::Immediate, so a commit
    // that returned has reached the disk — killing the process one instruction
    // later must not lose it.
    let (_dir, db) = fixture();

    assert!(
        !run_child(&db, "commit-then-die"),
        "the child was supposed to abort, not exit cleanly"
    );

    let entries = read_back(&db);
    let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();

    assert!(
        keys.contains(&"committed"),
        "a committed write was lost when the process died: {keys:?}"
    );
    assert!(
        keys.contains(&"baseline"),
        "the pre-existing state was damaged: {keys:?}"
    );
}

#[test]
fn a_transaction_killed_before_commit_leaves_nothing_behind() {
    // The atomicity claim, across process death rather than across an error
    // return. crossbank stages a write-set in memory and only hands the backend
    // one commit, so a process killed mid-transaction should have written
    // nothing at all.
    let (_dir, db) = fixture();

    assert!(
        !run_child(&db, "die-mid-transaction"),
        "the child was supposed to abort, not exit cleanly"
    );

    let entries = read_back(&db);
    let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();

    assert!(
        !keys.contains(&"staged_a") && !keys.contains(&"staged_b"),
        "a transaction that never committed left data behind: {keys:?}"
    );
    assert_eq!(
        entries,
        vec![("baseline".to_string(), "original".to_string())],
        "the store must be exactly the pre-state, with no partial write"
    );
}

#[test]
fn a_database_reopens_cleanly_after_a_crash() {
    // Not just "the data is right" but "opening it works at all". A corrupt
    // header or a stranded lock file would fail here rather than showing up
    // later as a mysterious error in an unrelated test.
    let (_dir, db) = fixture();
    run_child(&db, "commit-then-die");

    for attempt in 0..3 {
        let backend = RedbBackend::open(&db);
        assert!(
            backend.is_ok(),
            "reopen attempt {attempt} failed after a crash: {:?}",
            backend.err()
        );
    }
}
