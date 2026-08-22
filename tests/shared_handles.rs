//! Every handle on one locker name is a view of the one open locker.
//!
//! The conformance suite pins the behaviour that matters to a caller — a write
//! through one handle is visible through every other, on every backend. This
//! file pins the edges around it: what happens when the second open does not
//! agree with the first, and what `close` means when more than one handle
//! holds the name.
//!
//! Native only. Nothing here is backend-dependent, and the memory backend
//! answers all of it.

#![cfg(not(target_arch = "wasm32"))]

use crossbank::{Bank, BankConfig, Commit, Error, Locker, LockerConfig};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}

/// A second open under a different value type is refused.
///
/// The two handles share one resident map, and the downcast back out of it is
/// only sound because the type tag says it is. A mismatch is the same answer
/// the stored schema guard gives, for the same reason.
#[test]
fn a_second_handle_under_a_different_type_is_refused() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _first: Locker<String> = bank.locker("settings").await.expect("first");

        let second = bank.locker::<u64>("settings").await;
        assert!(
            matches!(second, Err(Error::SchemaMismatch { .. })),
            "a second handle under another type must be refused: {second:?}"
        );
    });
}

/// So is opening an open eager name lazily, or the other way round.
#[test]
fn a_second_handle_of_the_other_kind_is_refused() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _eager: Locker<String> = bank.locker("settings").await.expect("eager");
        assert!(matches!(
            bank.lazy_locker::<String>("settings").await,
            Err(Error::SchemaMismatch { .. })
        ));

        let _lazy = bank
            .lazy_locker::<String>("series")
            .await
            .expect("lazy open");
        assert!(matches!(
            bank.locker::<String>("series").await,
            Err(Error::SchemaMismatch { .. })
        ));
    });
}

/// A second open under a different config is refused, and says which field.
///
/// Sharing means one set of rules governs both handles' writes. Letting the
/// second open name a chunk size, a commit mode or a durability the first
/// handle never asked for would apply it to the first handle's writes too,
/// silently.
#[test]
fn a_second_handle_under_a_different_config_names_the_field() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let _first = bank
            .lazy_locker_with::<String>("series", LockerConfig::default())
            .await
            .expect("first");

        let second = bank
            .lazy_locker_with::<String>("series", LockerConfig::default().with_chunk_size(4096))
            .await;
        match second {
            Err(Error::InvalidConfig(message)) => assert!(
                message.contains("chunk_size"),
                "the error must name the differing field: {message}"
            ),
            other => panic!("a differing config must be refused: {other:?}"),
        }

        let commit = bank
            .lazy_locker_with::<String>(
                "series",
                LockerConfig::default().with_commit(Commit::Deferred { after: 8 }),
            )
            .await;
        match commit {
            Err(Error::InvalidConfig(message)) => assert!(message.contains("commit")),
            other => panic!("a differing commit mode must be refused: {other:?}"),
        }

        // The same config shares, which is the whole point.
        bank.lazy_locker_with::<String>("series", LockerConfig::default())
            .await
            .expect("an identical config shares the open locker");
    });
}

/// `close()` on one handle closes the locker for every handle on the name.
///
/// Hive's semantics: `box.close()` closes the box, not the caller's reference
/// to it. The alternative — a handle that keeps working after the locker it
/// shares was closed — would mean `close` did not mean what it says.
#[test]
fn closing_one_handle_closes_the_name_for_all_of_them() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.lazy_locker::<String>("l").await.expect("first");
        let b = bank.lazy_locker::<String>("l").await.expect("second");
        a.put("k", &"v".to_string()).await.expect("put");

        b.close().await.expect("close");

        assert!(a.is_closed(), "the other handle must report closed too");
        assert!(matches!(
            a.put("k2", &"v".to_string()).await,
            Err(Error::Closed)
        ));
        assert!(!bank.is_locker_open("l"));

        // Idempotent, through either handle.
        a.close().await.expect("closing twice is fine");
        b.close().await.expect("and through the other handle too");
    });
}

/// A closed name reopens, and the data is still there.
#[test]
fn a_name_reopens_after_it_was_closed_through_a_handle() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.locker::<String>("settings").await.expect("first");
        let b = bank.locker::<String>("settings").await.expect("second");
        a.put("theme", "dark".to_string()).await.expect("put");
        b.close().await.expect("close");

        let fresh = bank.locker::<String>("settings").await.expect("reopen");
        assert!(!fresh.is_closed());
        assert_eq!(fresh.get("theme").as_deref(), Some(&"dark".to_string()));

        // ...and it is a fresh locker, not the closed one handed back.
        assert!(a.is_closed());
    });
}

/// A dropped handle does not close the locker the others are still using.
#[test]
fn dropping_one_handle_leaves_the_others_working() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        let a = bank.locker::<String>("settings").await.expect("first");
        let b = bank.locker::<String>("settings").await.expect("second");
        b.put("theme", "dark".to_string()).await.expect("put");
        drop(b);

        assert!(bank.is_locker_open("settings"));
        assert_eq!(a.get("theme").as_deref(), Some(&"dark".to_string()));
        a.put("accent", "blue".to_string()).await.expect("put");
    });
}

/// Once every handle is gone the name is free, and a later open reads storage
/// afresh rather than finding a stale resident map.
#[test]
fn the_last_handle_going_frees_the_name() {
    block_on(async {
        let bank = Bank::open(BankConfig::memory()).await.expect("bank");
        {
            let a = bank.locker::<String>("settings").await.expect("first");
            let b = bank.locker::<String>("settings").await.expect("second");
            a.put("theme", "dark".to_string()).await.expect("put");
            drop(a);
            drop(b);
        }
        assert!(!bank.is_locker_open("settings"));

        let fresh = bank.locker::<String>("settings").await.expect("reopen");
        assert_eq!(fresh.get("theme").as_deref(), Some(&"dark".to_string()));
    });
}
