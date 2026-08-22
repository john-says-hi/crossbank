//! Erasing a whole bank, in a real browser.
//!
//! The web half of `tests/delete_bank.rs`; see that file for why this is not
//! a conformance case.
//!
//! **Only the "reopens empty" half is here.** The open-bank refusal is
//! native-only by construction: the registry `delete_bank` consults is
//! `#[cfg(not(target_arch = "wasm32"))]`, and on the web an open connection
//! does not fail the delete, it *blocks* it — so asserting a refusal here
//! would not go red, it would hang the lane until its 180 s timeout. Closing
//! first is the contract on this platform.
//!
//! Rules this file follows, both learned the hard way: never rely on `Drop`
//! (the atomics lane is `panic = "abort"`, so nothing unwinds), and never
//! touch `std::time` (it compiles on wasm32 and panics at runtime).

#![cfg(target_arch = "wasm32")]

use crossbank::{delete_bank, Bank, BankConfig};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn a_deleted_bank_reopens_empty() {
    let config = BankConfig::web("crossbank-delete-bank");

    let bank = Bank::open(config.clone()).await.unwrap();
    let settings = bank.locker::<String>("settings").await.unwrap();
    let notes = bank.lazy_locker::<String>("notes").await.unwrap();
    settings.put("theme", "dark".into()).await.unwrap();
    notes.put("first", &"hello".to_string()).await.unwrap();
    assert!(bank.locker_exists("settings").await.unwrap());
    assert!(bank.locker_exists("notes").await.unwrap());

    // The connection has to go before the database can: an open one blocks
    // `deleteDatabase` rather than failing it.
    bank.close().await.unwrap();
    delete_bank(&config).await.unwrap();

    let reborn = Bank::open(config.clone()).await.unwrap();
    assert!(
        reborn.locker_names().await.unwrap().is_empty(),
        "a deleted bank must reopen with no lockers registered"
    );
    assert!(!reborn.locker_exists("settings").await.unwrap());
    assert!(!reborn.locker_exists("notes").await.unwrap());

    let settings = reborn.locker::<String>("settings").await.unwrap();
    let notes = reborn.lazy_locker::<String>("notes").await.unwrap();
    assert_eq!(settings.len(), 0);
    assert_eq!(settings.get("theme"), None);
    assert_eq!(notes.len(), 0);
    assert_eq!(notes.get("first").await.unwrap(), None);

    reborn.close().await.unwrap();
    delete_bank(&config).await.unwrap();
}
