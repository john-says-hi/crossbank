//! `Bank::persist()` is callable on every target and never implicit.
//!
//! Natively the file is the persistence, so it answers `true`. In a browser it
//! asks `navigator.storage.persist()`; headless browsers usually refuse, so
//! the web test asserts only that the call completes with a boolean.

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use crossbank::{Bank, BankConfig};
    use futures::executor::block_on;

    #[test]
    fn native_bank_is_already_persistent() {
        let bank = block_on(Bank::open(BankConfig::memory())).unwrap();
        assert!(block_on(bank.persist()).unwrap());
    }
}

#[cfg(target_arch = "wasm32")]
mod web {
    use crossbank::{Bank, BankConfig};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn persist_is_callable_in_a_browser() {
        // Firefox gates persist() behind a user prompt; headless has no user,
        // so the promise never settles and the lane would time out. Chromium
        // answers without a prompt and exercises the real path.
        let ua = web_sys::window().unwrap().navigator().user_agent().unwrap();
        if ua.contains("Firefox") {
            return;
        }
        let bank = Bank::open(BankConfig::memory()).await.unwrap();
        let granted = bank.persist().await.unwrap();
        // Headless runners may refuse; the contract is "answers, never panics".
        let _ = granted;
    }
}
