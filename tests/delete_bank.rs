//! Erasing a whole bank, natively.
//!
//! Not a conformance case, and it cannot be one: the suite hands a case an
//! already-open [`crossbank::backend::Backend`], while `delete_bank` works
//! from a `BankConfig` — a *location* — and there is no backend-generic way to
//! spell one. So the native half lives here and the web half in
//! `tests/web_delete_bank.rs`, and the two assert the same shape.

#![cfg(not(target_arch = "wasm32"))]

use crossbank::{delete_bank, Bank, BankConfig, Error};
use futures::executor::block_on;

/// Erasing a closed bank leaves nothing behind, and reopening starts over.
///
/// Reopening the *same* location is the assertion that matters: an erase that
/// removed the records but left the locker registry would hand back a bank
/// that names lockers whose data is gone.
#[test]
fn a_deleted_bank_reopens_empty() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("erased.redb");
    let config = BankConfig::at(&path);

    {
        let bank = block_on(Bank::open(config.clone())).unwrap();
        let settings = block_on(bank.locker::<String>("settings")).unwrap();
        let notes = block_on(bank.lazy_locker::<String>("notes")).unwrap();
        block_on(settings.put("theme", "dark".into())).unwrap();
        block_on(notes.put("first", &"hello".to_string())).unwrap();

        assert!(block_on(bank.locker_exists("settings")).unwrap());
        assert!(block_on(bank.locker_exists("notes")).unwrap());
        block_on(bank.close()).unwrap();
    }

    block_on(delete_bank(&config)).unwrap();
    assert!(!path.exists(), "the file itself must be gone");

    let reborn = block_on(Bank::open(config)).unwrap();
    assert!(
        block_on(reborn.locker_names()).unwrap().is_empty(),
        "a deleted bank must reopen with no lockers registered"
    );
    assert!(!block_on(reborn.locker_exists("settings")).unwrap());
    assert!(!block_on(reborn.locker_exists("notes")).unwrap());

    let settings = block_on(reborn.locker::<String>("settings")).unwrap();
    let notes = block_on(reborn.lazy_locker::<String>("notes")).unwrap();
    assert_eq!(settings.len(), 0);
    assert_eq!(settings.get("theme"), None);
    assert_eq!(notes.len(), 0);
    assert_eq!(block_on(notes.get("first")).unwrap(), None);
    block_on(reborn.close()).unwrap();
}

/// Erasing a bank that is still open is refused, and refused *harmlessly*.
///
/// Unix will unlink a file `redb` still holds open, and the live `Bank` then
/// keeps committing into something with no name — every write after the
/// delete lost, with no error anywhere. The refusal has to leave the store
/// completely usable, which is the second half of this test.
#[test]
fn deleting_an_open_bank_is_refused_and_changes_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("live.redb");
    let config = BankConfig::at(&path);

    let bank = block_on(Bank::open(config.clone())).unwrap();
    let settings = block_on(bank.locker::<String>("settings")).unwrap();
    block_on(settings.put("theme", "dark".into())).unwrap();

    match block_on(delete_bank(&config)) {
        Err(Error::InvalidConfig(msg)) => assert!(
            msg.contains("close the bank first"),
            "the refusal must say what to do instead: {msg}"
        ),
        other => panic!("deleting an open bank must be refused, got {other:?}"),
    }

    // Intact, and still writable.
    assert!(path.exists());
    assert_eq!(settings.get("theme").as_deref(), Some(&"dark".to_string()));
    assert!(block_on(bank.locker_exists("settings")).unwrap());
    block_on(settings.put("scale", "1.25".into())).unwrap();

    // And the write made after the refusal really is in the file, not in a
    // nameless one: closing and reopening finds it.
    block_on(bank.close()).unwrap();
    let reopened = block_on(Bank::open(config.clone())).unwrap();
    let settings = block_on(reopened.locker::<String>("settings")).unwrap();
    assert_eq!(settings.get("scale").as_deref(), Some(&"1.25".to_string()));
    block_on(reopened.close()).unwrap();

    block_on(delete_bank(&config)).unwrap();
    assert!(!path.exists());
}
