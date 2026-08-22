//! Two opens of one locker name that overlap in time, in a real browser.
//!
//! The native half lives in `tests/shared_handles.rs`, where a backend
//! decorator has to be built to make the opens suspend at all: the memory
//! backend returns futures that are ready on their first poll. IndexedDB
//! needs no such help — every read really does suspend — so this is the
//! honest version of the race, on the platform crossbank exists for.
//!
//! What it pins: `Bank::locker` checks its registry, then awaits the locker
//! open, and the check is not a claim. Two opens joined together both passed
//! it, each built an `Inner` and an index of its own, and the second
//! registration overwrote the first — leaving one locker live and invisible.
//!
//! Rules this file follows, both learned the hard way: never rely on `Drop`
//! (the atomics lane is `panic = "abort"`, so nothing unwinds), and never
//! touch `std::time` (it compiles on wasm32 and panics at runtime).

#![cfg(target_arch = "wasm32")]

use crossbank::{delete_bank, Bank, BankConfig};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn two_simultaneous_opens_of_one_name_share_one_locker() {
    let config = BankConfig::web("crossbank-shared-handles");
    let bank = Bank::open(config.clone()).await.unwrap();

    let (first, second) = futures::join!(
        bank.lazy_locker::<Vec<u8>>("series"),
        bank.lazy_locker::<Vec<u8>>("series"),
    );
    let first = first.unwrap();
    let second = second.unwrap();

    assert_eq!(
        bank.open_locker_names(),
        vec!["series".to_string()],
        "one name is one open locker, however the opens overlapped"
    );

    first.put("k", &vec![1u8, 2, 3]).await.unwrap();
    assert!(
        second.contains_key("k"),
        "a write through one handle must be visible through the other"
    );
    assert_eq!(second.get("k").await.unwrap(), Some(vec![1u8, 2, 3]));
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);

    bank.close().await.unwrap();
    delete_bank(&config).await.unwrap();
}
