//! Cross-tab coherence, in a real browser.
//!
//! Two `Bank`s on one database name in one page stand in for two tabs: they
//! are two independent IndexedDB connections with two independent resident
//! views, which is exactly the situation coherence exists to fix. A second
//! real tab would test nothing more and could not be driven from here.
//!
//! Rules this file follows, both learned the hard way:
//!
//! * Never rely on `Drop`. The atomics lane builds with `panic = "abort"`,
//!   where there is no unwinding to run destructors, so every bank is closed
//!   and every database deleted explicitly.
//! * Never sleep on `std::time`. It compiles on wasm32 and panics at runtime.
//!   Yielding goes through `setTimeout`.

#![cfg(target_arch = "wasm32")]

use crossbank::{Bank, BankConfig, LockerConfig};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

/// Yield to the event loop for `ms`, so a posted message can be delivered.
///
/// A `BroadcastChannel` message arrives as a DOM event, so it cannot be
/// observed without giving the event loop a turn.
async fn tick(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

/// Poll `check` for up to a second. Returns whether it ever came true.
async fn settles(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..100 {
        if check() {
            return true;
        }
        tick(10).await;
    }
    check()
}

fn config(name: &str) -> BankConfig {
    BankConfig::web(name).with_coherence(true)
}

/// Bytes LZ4 cannot shrink, so "larger than the inline limit" stays larger
/// than the inline limit once the value is sealed.
fn incompressible(len: usize) -> Vec<u8> {
    // A plain LCG: full 2^32 period, so there is no short repeat for LZ4 to
    // fold away. A cheaper pattern compressed under the inline limit and made
    // the "too large to carry" half of the test pass for the wrong reason.
    let mut state: u32 = 0x1234_5678;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 16) as u8
        })
        .collect()
}

/// A write in one bank shows up in the other bank's lazy key index.
#[wasm_bindgen_test]
async fn a_write_in_one_tab_reaches_the_other_tabs_lazy_index() {
    let name = "crossbank-coherence-lazy";
    let _ = crossbank::delete_bank(&BankConfig::web(name)).await;

    let a = Bank::open(config(name)).await.unwrap();
    let b = Bank::open(config(name)).await.unwrap();
    let la = a.lazy_locker::<String>("l").await.unwrap();
    let lb = b.lazy_locker::<String>("l").await.unwrap();

    assert!(!la.contains_key("k"), "nothing written yet");

    lb.put("k", &"written by b".to_string()).await.unwrap();
    assert!(
        settles(|| la.contains_key("k")).await,
        "bank a's index never learned about bank b's write"
    );

    // And the value is really there to be read, not just the key.
    assert_eq!(
        la.get("k").await.unwrap(),
        Some("written by b".to_string()),
        "a coherent index must point at real data"
    );

    lb.delete("k").await.unwrap();
    assert!(
        settles(|| !la.contains_key("k")).await,
        "bank a's index never learned about bank b's delete"
    );

    lb.put("x", &"one".to_string()).await.unwrap();
    assert!(settles(|| la.contains_key("x")).await);
    lb.clear().await.unwrap();
    assert!(
        settles(|| la.len() == 0).await,
        "a clear must reach the other tab too"
    );

    a.close().await.unwrap();
    b.close().await.unwrap();
    crossbank::delete_bank(&BankConfig::web(name))
        .await
        .unwrap();
}

/// An eager locker takes a small write straight into RAM, and refuses to hold
/// a large one rather than lying about it.
#[wasm_bindgen_test]
async fn an_eager_locker_absorbs_small_writes_and_goes_stale_on_large_ones() {
    let name = "crossbank-coherence-eager";
    let _ = crossbank::delete_bank(&BankConfig::web(name)).await;

    let a = Bank::open(config(name)).await.unwrap();
    let b = Bank::open(config(name)).await.unwrap();
    let config = LockerConfig::default().with_max_inline(1024 * 1024);
    let ea = a.locker_with::<Vec<u8>>("e", config).await.unwrap();
    let eb = b.locker_with::<Vec<u8>>("e", config).await.unwrap();

    // Small enough to ride along inside the message.
    eb.put("small", incompressible(64)).await.unwrap();
    assert!(
        settles(|| ea.get("small").is_some()).await,
        "a small write must arrive with its bytes"
    );
    assert_eq!(ea.get("small").as_deref(), Some(&incompressible(64)));

    // Now overwrite the same key with something too large to carry. The
    // resident copy must go rather than stay behind as a stale answer from an
    // infallible getter.
    eb.put("small", incompressible(200_000)).await.unwrap();
    assert!(
        settles(|| ea.get("small").is_none()).await,
        "a value too large to carry must not be answered from a stale copy"
    );

    // Reopening is what recovers it, exactly as documented.
    let reopened = a.locker_with::<Vec<u8>>("e", config).await.unwrap();
    assert_eq!(
        reopened.get("small").as_deref(),
        Some(&incompressible(200_000)),
        "the data itself was never in doubt"
    );

    a.close().await.unwrap();
    b.close().await.unwrap();
    crossbank::delete_bank(&BankConfig::web(name))
        .await
        .unwrap();
}

/// Coherence is opt-in: without it, nothing crosses.
///
/// The negative half of the same experiment, so that the two cases above
/// cannot pass for some reason other than the channel.
#[wasm_bindgen_test]
async fn without_the_flag_nothing_crosses() {
    let name = "crossbank-coherence-off";
    let _ = crossbank::delete_bank(&BankConfig::web(name)).await;

    let a = Bank::open(BankConfig::web(name)).await.unwrap();
    let b = Bank::open(BankConfig::web(name)).await.unwrap();
    let la = a.lazy_locker::<String>("l").await.unwrap();
    let lb = b.lazy_locker::<String>("l").await.unwrap();

    lb.put("k", &"written by b".to_string()).await.unwrap();
    tick(100).await;
    assert!(
        !la.contains_key("k"),
        "coherence is opt-in; an unsubscribed bank must not hear anything"
    );

    a.close().await.unwrap();
    b.close().await.unwrap();
    crossbank::delete_bank(&BankConfig::web(name))
        .await
        .unwrap();
}
