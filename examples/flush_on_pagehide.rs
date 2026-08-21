//! Deferred writes, and whose job it is to flush them.
//!
//! Run with `cargo run --example flush_on_pagehide`.
//!
//! [`Commit::Deferred`] trades durability for throughput: writes are staged in
//! memory and committed in batches. **crossbank spawns nothing** — no timer,
//! no background task, and no destructor, because a `Drop` cannot await and a
//! closing browser tab would not run one anyway. So the application owns the
//! flush, and this example shows both halves of that ownership.
//!
//! The native half below runs. The web half is the commented snippet at the
//! bottom, because this example compiles for the host, not for wasm.

use crossbank::{Bank, BankConfig, Commit, LockerConfig};

fn main() -> crossbank::Result<()> {
    // `block_on` only because an example needs *some* executor. crossbank
    // itself never depends on one.
    futures::executor::block_on(run())
}

async fn run() -> crossbank::Result<()> {
    let bank = Bank::open(BankConfig::memory()).await?;

    // Commit once eight writes have piled up.
    let config = LockerConfig::default().with_commit(Commit::Deferred { after: 8 });
    let cache = bank.lazy_locker_with::<Vec<u8>>("candles", config).await?;

    for i in 0..5u32 {
        cache.put(&format!("bar-{i}"), &vec![i as u8; 32]).await?;
    }

    // Staged, and visible to this handle — but not yet stored.
    println!(
        "staged: {} writes, {} bytes",
        cache.pending(),
        cache.pending_bytes()
    );
    println!(
        "readable already: {:?}",
        cache.get("bar-0").await?.is_some()
    );

    // === the stop hook ===
    //
    // Natively this belongs wherever the application shuts down: a Ctrl-C
    // handler, a Tauri/winit exit event, an Android `onStop`, an iOS
    // `applicationDidEnterBackground`. One call covers every open locker.
    bank.flush_all().await?;
    println!("after flush_all: {} staged", cache.pending());

    // `close` flushes too, so a clean shutdown cannot lose a batch by
    // forgetting — but do not rely on that as the *only* flush: an app that is
    // killed never gets to call it.
    bank.close().await?;
    Ok(())
}

// === the web half ===
//
// On wasm, register the flush from `pagehide` **and** from
// `visibilitychange` when the document becomes hidden. Do *not* use
// `beforeunload`: mobile browsers frequently never fire it, and a tab
// backgrounded and later discarded never unloads at all.
//
// The bank is `!Send` on wasm, so keep it in a thread-local `Rc` and spawn the
// flush with `wasm_bindgen_futures::spawn_local` — the callback itself cannot
// await.
//
// ```ignore
// use wasm_bindgen::prelude::*;
//
// fn install_flush_hooks(bank: std::rc::Rc<crossbank::Bank>) {
//     let window = web_sys::window().expect("a window");
//     let document = window.document().expect("a document");
//
//     let on_hide = {
//         let bank = bank.clone();
//         Closure::<dyn FnMut()>::new(move || {
//             let bank = bank.clone();
//             wasm_bindgen_futures::spawn_local(async move {
//                 // A failed flush here has nowhere to go but a log: the page
//                 // is on its way out.
//                 if let Err(e) = bank.flush_all().await {
//                     web_sys::console::warn_1(&format!("crossbank flush: {e}").into());
//                 }
//             });
//         })
//     };
//     window
//         .add_event_listener_with_callback("pagehide", on_hide.as_ref().unchecked_ref())
//         .expect("pagehide listener");
//
//     let on_visibility = {
//         let bank = bank.clone();
//         let document = document.clone();
//         Closure::<dyn FnMut()>::new(move || {
//             if document.visibility_state() == web_sys::VisibilityState::Hidden {
//                 let bank = bank.clone();
//                 wasm_bindgen_futures::spawn_local(async move {
//                     let _ = bank.flush_all().await;
//                 });
//             }
//         })
//     };
//     document
//         .add_event_listener_with_callback(
//             "visibilitychange",
//             on_visibility.as_ref().unchecked_ref(),
//         )
//         .expect("visibilitychange listener");
//
//     // Both closures must outlive this function or the listeners call into
//     // freed memory.
//     on_hide.forget();
//     on_visibility.forget();
// }
// ```
