//! The page `ci/web-e2e.sh` drives: a real tab, a real reload, real tabs.
//!
//! # Why this exists when the wasm suite already runs in browsers
//!
//! `wasm_bindgen_test` gives us a browser, but it gives us **one page that
//! never navigates**. Two things an application depends on are therefore
//! invisible to it:
//!
//! * a genuine reload — a fresh wasm instance, a fresh heap, an IndexedDB
//!   connection opened from nothing, reading bytes a *previous* instance
//!   wrote. `tests/web_coherence.rs` reopens a `Bank` inside one page, which
//!   is not the same claim;
//! * two real tabs. Coherence rides a `BroadcastChannel`, and the in-suite
//!   version puts both `Bank`s in one page. That is honest about the two
//!   connections and the two resident views, but not about the browser
//!   actually delivering a message between documents.
//!
//! So this is a normal `--target web` module, driven by Playwright, and it
//! makes no attempt to be a test framework: every function reports what it
//! saw and the assertions live in the driver script.
//!
//! Build:
//!
//! ```sh
//! ci/web-e2e.sh                 # chromium (default)
//! ci/web-e2e.sh --browser firefox
//! ```

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    eprintln!(
        "web_e2e_page is a wasm32 page, not a native binary. Build and drive it with \
         ci/web-e2e.sh."
    );
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(target_arch = "wasm32")]
mod page {
    use std::cell::RefCell;
    use std::rc::Rc;

    use crossbank::{Bank, BankConfig, LazyLocker};
    use wasm_bindgen::prelude::*;

    const DB: &str = "crossbank-web-e2e";
    const LOCKER: &str = "e2e";
    const BATCH: usize = 500;

    thread_local! {
        /// The open handles. `Rc`, and always cloned out of the `RefCell`
        /// before anything is awaited: holding a `RefCell` borrow across an
        /// await is how a second call panics on a page that is doing two
        /// things at once.
        static OPEN: RefCell<Option<(Rc<Bank>, Rc<LazyLocker<String>>)>> =
            const { RefCell::new(None) };
    }

    fn err(e: crossbank::Error) -> JsValue {
        JsValue::from_str(&e.to_string())
    }

    fn handles() -> Result<(Rc<Bank>, Rc<LazyLocker<String>>), JsValue> {
        OPEN.with(|slot| slot.borrow().clone())
            .ok_or_else(|| JsValue::from_str("call open() first"))
    }

    fn key(i: usize) -> String {
        format!("k{i:06}")
    }

    /// The value for `i`. Deterministic, so "byte-identical after a reload"
    /// is a claim the driver can check without carrying 10k strings around.
    fn value(i: usize) -> String {
        format!("v{i:06}-{}", i.wrapping_mul(2_654_435_761) % 1_000_000_007)
    }

    /// Open the bank on IndexedDB with cross-tab coherence on.
    #[wasm_bindgen(js_name = open)]
    pub async fn open() -> Result<(), JsValue> {
        if OPEN.with(|slot| slot.borrow().is_some()) {
            return Ok(());
        }
        let bank = Bank::open(BankConfig::web(DB).with_coherence(true))
            .await
            .map_err(err)?;
        let locker = bank.lazy_locker::<String>(LOCKER).await.map_err(err)?;
        OPEN.with(|slot| {
            *slot.borrow_mut() = Some((Rc::new(bank), Rc::new(locker)));
        });
        Ok(())
    }

    /// Write `n` deterministic keys, in batches.
    #[wasm_bindgen(js_name = writeKeys)]
    pub async fn write_keys(n: usize) -> Result<(), JsValue> {
        let (_bank, locker) = handles()?;
        let mut batch = Vec::with_capacity(BATCH);
        for i in 0..n {
            batch.push((key(i), value(i)));
            if batch.len() == BATCH {
                locker
                    .put_all(std::mem::take(&mut batch))
                    .await
                    .map_err(err)?;
                batch.reserve(BATCH);
            }
        }
        if !batch.is_empty() {
            locker.put_all(batch).await.map_err(err)?;
        }
        Ok(())
    }

    /// Write one key by name, which is how the second tab pokes the first.
    #[wasm_bindgen(js_name = writeOne)]
    pub async fn write_one(k: String, v: String) -> Result<(), JsValue> {
        let (_bank, locker) = handles()?;
        locker.put(&k, &v).await.map_err(err)
    }

    /// Keys in the resident index. This is what a coherence message updates.
    #[wasm_bindgen(js_name = count)]
    pub fn count() -> Result<usize, JsValue> {
        Ok(handles()?.1.len())
    }

    /// Whether the resident index knows `k` — no storage read, on purpose.
    #[wasm_bindgen(js_name = indexHas)]
    pub fn index_has(k: String) -> Result<bool, JsValue> {
        Ok(handles()?.1.contains_key(&k))
    }

    /// One value, read through to storage.
    #[wasm_bindgen(js_name = readKey)]
    pub async fn read_key(k: String) -> Result<Option<String>, JsValue> {
        let (_bank, locker) = handles()?;
        locker.get(&k).await.map_err(err)
    }

    /// Read every stored entry and fold it into one 64-bit hash.
    ///
    /// A digest rather than the data: 10k round-tripped strings would say the
    /// same thing far more slowly, and an FNV over key and value in key order
    /// catches a dropped, reordered or altered byte just as well.
    #[wasm_bindgen(js_name = readAll)]
    pub async fn read_all() -> Result<String, JsValue> {
        let (_bank, locker) = handles()?;
        let entries = locker.entries().await.map_err(err)?;
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for (k, v) in &entries {
            for byte in k.as_bytes().iter().chain(b"=").chain(v.as_bytes()) {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        Ok(format!("{}:{hash:016x}", entries.len()))
    }

    /// Every value must be exactly what this build would write for its key.
    ///
    /// The reload check that a digest alone cannot make: it compares against
    /// values computed *now*, in a wasm instance that did not write them.
    #[wasm_bindgen(js_name = verify)]
    pub async fn verify(n: usize) -> Result<String, JsValue> {
        let (_bank, locker) = handles()?;
        for i in 0..n {
            match locker.get(&key(i)).await.map_err(err)? {
                Some(got) if got == value(i) => {}
                Some(got) => return Ok(format!("MISMATCH at {i}: {got}")),
                None => return Ok(format!("MISSING at {i}")),
            }
        }
        Ok("ok".to_string())
    }

    /// Close the bank, unregistering the coherence channel's callback.
    #[wasm_bindgen(js_name = close)]
    pub async fn close() -> Result<(), JsValue> {
        let taken = OPEN.with(|slot| slot.borrow_mut().take());
        if let Some((bank, _locker)) = taken {
            bank.close().await.map_err(err)?;
        }
        Ok(())
    }

    /// Delete the database, so a run starts from nothing.
    #[wasm_bindgen(js_name = destroy)]
    pub async fn destroy() -> Result<(), JsValue> {
        close().await?;
        crossbank::delete_bank(&BankConfig::web(DB))
            .await
            .map_err(err)
    }
}
