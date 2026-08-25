//! Opening a bank whose IndexedDB database already exists but holds no object
//! stores, in a real browser.
//!
//! This is a defect that reached production. A bare `indexedDB.open(name)`
//! from anywhere else on the page — a devtools probe, a hand-typed console
//! line, another library sniffing for the name — creates the database at
//! version 1 with zero stores. crossbank asks for version 1 too, so its
//! upgrade callback never fires, `open` reports success, and the first real
//! operation dies with `NotFoundError` ("Cannot change something that does
//! not exists"). Reopening never cleared it, because reopening was what
//! created it.
//!
//! It cannot be a conformance case: the shell has to be built through the raw
//! IndexedDB factory, which the memory and redb backends have no notion of.
//!
//! Rules this file follows, both learned the hard way: never rely on `Drop`
//! (the atomics lane is `panic = "abort"`, so nothing unwinds), and never
//! touch `std::time` (it compiles on wasm32 and panics at runtime).

#![cfg(target_arch = "wasm32")]

use std::convert::Infallible;

use crossbank::{delete_bank, Bank, BankConfig};
use indexed_db::Factory;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const BANK: &str = "crossbank-shell-recovery";

#[wasm_bindgen_test]
async fn a_store_less_database_is_repaired_on_open() {
    let config = BankConfig::web(BANK);

    // Whatever an earlier run left behind must not be what decides this test.
    delete_bank(&config).await.unwrap();

    // Reproduce the defect exactly: the database at crossbank's own version
    // with nothing inside it, which is what a bare `indexedDB.open(name)`
    // leaves on the origin.
    {
        let factory = Factory::<Infallible>::get().expect("no IndexedDB factory");
        let shell = factory
            .open(BANK, 1, async move |_evt| Ok(()))
            .await
            .expect("creating the empty shell must succeed");
        assert!(
            shell.object_store_names().is_empty(),
            "the shell must start with no object stores, or this test proves nothing"
        );
        shell.close();
    }

    // Before the repair, this opened fine and then failed on the first write.
    let bank = Bank::open(config.clone()).await.unwrap();
    let settings = bank.locker::<String>("settings").await.unwrap();
    settings.put("theme", "dark".into()).await.unwrap();
    assert_eq!(
        settings.get("theme").as_deref().map(String::as_str),
        Some("dark")
    );

    // The repair has to outlive the session that performed it, or it is just
    // a papering-over of one open.
    bank.close().await.unwrap();
    let reopened = Bank::open(config.clone()).await.unwrap();
    let settings = reopened.locker::<String>("settings").await.unwrap();
    assert_eq!(
        settings.get("theme").as_deref().map(String::as_str),
        Some("dark"),
        "the repaired database must persist its data across a reopen"
    );

    reopened.close().await.unwrap();
    delete_bank(&config).await.unwrap();
}
