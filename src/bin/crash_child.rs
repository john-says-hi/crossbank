//! A deliberately suicidal helper process, used by `tests/crash_recovery.rs`.
//!
//! In-process tests cannot prove crash safety. Dropping a handle runs
//! destructors; even a panic unwinds. The only way to find out what survives a
//! process dying is to kill one, so this binary does exactly that at a chosen
//! point and lets the parent inspect the wreckage.
//!
//! It aborts via [`std::process::abort`], which raises `SIGABRT` without
//! unwinding, without running destructors, and without atexit handlers. That is
//! a genuine hard kill and it needs no extra dependency.
//!
//! What this does and does not prove: it kills the *process*, so the operating
//! system's page cache survives. It therefore tests process loss, not power
//! loss. Real power-loss testing would need `fsync` suppressed as well.
//!
//! Driven by two environment variables:
//!
//! * `CROSSBANK_CRASH_DB`   — path to the database file
//! * `CROSSBANK_CRASH_MODE` — `baseline`, `commit-then-die`,
//!   `eventual-flush-then-die`, or `die-mid-transaction`

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    use crossbank::backend::RedbBackend;
    use crossbank::{Bank, Durability, LockerConfig};
    use futures::executor::block_on;
    use std::sync::Arc;

    let path = std::env::var("CROSSBANK_CRASH_DB").expect("CROSSBANK_CRASH_DB is required");
    let mode = std::env::var("CROSSBANK_CRASH_MODE").expect("CROSSBANK_CRASH_MODE is required");

    block_on(async move {
        let backend = Arc::new(RedbBackend::open(&path).expect("could not open the database"));
        let bank = Bank::with_backend(backend)
            .await
            .expect("could not open the bank");
        let locker = bank
            .lazy_locker::<String>("crash")
            .await
            .expect("could not open the locker");

        match mode.as_str() {
            // Write a known starting state and exit cleanly.
            "baseline" => {
                locker
                    .put("baseline", &"original".to_string())
                    .await
                    .expect("baseline write failed");
            }

            // A commit that RETURNED must survive the process dying one
            // instruction later. This is the durability claim.
            "commit-then-die" => {
                locker
                    .put("committed", &"survives".to_string())
                    .await
                    .expect("write failed");
                std::process::abort();
            }

            // An `Eventual` commit skips the per-commit fsync, so on its own it
            // proves nothing about durability. An explicit `flush` is the
            // caller's half of that bargain, and this asserts the bargain is
            // real: flush, then die, then find the data.
            "eventual-flush-then-die" => {
                let eventual = bank
                    .lazy_locker_with::<String>(
                        "eventual",
                        LockerConfig::default().with_durability(Durability::Eventual),
                    )
                    .await
                    .expect("could not open the eventual locker");
                eventual
                    .put("flushed", &"survives".to_string())
                    .await
                    .expect("write failed");
                eventual.flush().await.expect("flush failed");
                std::process::abort();
            }

            // A transaction killed before it commits must leave nothing behind.
            // crossbank stages writes in memory, so this should be true by
            // construction — which is exactly the sort of claim worth proving
            // rather than asserting.
            "die-mid-transaction" => {
                let _ = locker
                    .transact(|tx| async move {
                        tx.put("staged_a", "never".to_string())?;
                        tx.put("staged_b", "never".to_string())?;
                        // Dies with the write-set still in memory.
                        std::process::abort();
                    })
                    .await;
            }

            other => panic!("unknown crash mode: {other}"),
        }
    });
}

#[cfg(target_arch = "wasm32")]
fn main() {
    // There is no process to kill in a browser, and no redb backend either.
}
