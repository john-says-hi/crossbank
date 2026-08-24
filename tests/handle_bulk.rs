//! The bytes-only bulk operations, against a real file.
//!
//! `src/handle.rs` proves the shapes over the memory backend, where "durable"
//! means nothing at all. This asks the only question that file cannot: does a
//! `put_all` that returned `Ok(())` still exist after the bank is closed and
//! the same path reopened?
//!
//! Native only. `redb` is the native backend, and a temp path is the whole
//! point.

#![cfg(not(target_arch = "wasm32"))]

use std::future::Future;

use crossbank::{Bank, BankConfig, BankHandle};
use futures::executor::block_on;

/// Open a bank at `config`, run `body` against a handle onto it, and release
/// the file before returning.
///
/// The service has to be driven on the same executor as the work — crossbank
/// spawns nothing — and the backend has to be closed explicitly: `redb` holds
/// an exclusive file lock for as long as its `Database` is alive, so a reopen
/// of the same path would otherwise fail.
fn with_bank<F, Fut, T>(config: BankConfig, body: F) -> T
where
    F: FnOnce(BankHandle) -> Fut,
    Fut: Future<Output = T>,
{
    block_on(async {
        let bank = Bank::open(config).await.expect("open the bank");
        let backend = bank.backend().clone();
        let remote = bank.handle();

        let service = bank.into_service();
        futures::pin_mut!(service);
        let work = body(remote);
        futures::pin_mut!(work);

        let out = match futures::future::select(work, service).await {
            futures::future::Either::Left((value, _)) => value,
            futures::future::Either::Right(_) => {
                panic!("the service loop ended before the work finished")
            }
        };

        backend.close().await.expect("close the backend");
        out
    })
}

fn entry(key: &str, value: &str) -> (String, Vec<u8>) {
    (key.to_string(), value.as_bytes().to_vec())
}

/// A `put_all` that returned is on the disk, not merely in a buffer.
///
/// The reopen is what makes this worth running: the whole batch rides in one
/// commit, and a commit that was atomic but never durable would still pass
/// every in-memory assertion.
#[test]
fn put_all_entries_survive_a_close_and_reopen() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("bulk.redb");
    let config = BankConfig::at(&path);

    let written = vec![
        entry("theme", "dark"),
        entry("locale", "en-GB"),
        entry("scale", "1.25"),
    ];

    with_bank(config.clone(), {
        let written = written.clone();
        |remote| async move {
            remote.put_all("settings", written).await.expect("put_all");
            remote
                .delete_all("settings", vec!["never_written".to_string()])
                .await
                .expect("delete_all");
            assert_eq!(remote.len("settings").await.expect("len"), 3);
        }
    });

    assert!(path.exists(), "the bank file must be on disk");

    with_bank(config, |remote| async move {
        // The registry survived too, or the values would be under a different
        // locker id and unreachable by name.
        assert_eq!(
            remote.locker_names().await.expect("locker_names"),
            vec!["settings"]
        );

        assert_eq!(remote.len("settings").await.expect("len"), 3);
        assert!(remote
            .contains_key("settings", "theme")
            .await
            .expect("contains_key"));
        assert!(!remote
            .contains_key("settings", "never_written")
            .await
            .expect("contains_key"));

        let got = remote
            .get_many(
                "settings",
                vec![
                    "scale".to_string(),
                    "theme".to_string(),
                    "never_written".to_string(),
                ],
            )
            .await
            .expect("get_many");
        assert_eq!(
            got,
            vec![Some(b"1.25".to_vec()), Some(b"dark".to_vec()), None,]
        );

        // Values as well as keys, in byte order.
        let entries = remote.entries("settings", "").await.expect("entries");
        assert_eq!(
            entries,
            vec![
                entry("locale", "en-GB"),
                entry("scale", "1.25"),
                entry("theme", "dark"),
            ]
        );
    });
}
