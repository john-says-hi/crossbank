//! Scale: does a locker still behave at a hundred thousand keys?
//!
//! Not a benchmark. Nothing here asserts a duration — timings are *printed*,
//! because a number that fails on a loaded machine teaches nobody anything and
//! a number in the log tells the next person whether an open got slower. What
//! is asserted is behaviour that only breaks at size: the key index counts
//! every key, a range still returns exactly its slice, and a reopen finds the
//! lot.
//!
//! # Why it is gated, twice, in two different ways
//!
//! **Natively** by the `CROSSBANK_SCALE=1` environment variable, checked at
//! run time. The test is always compiled — so it can never rot unnoticed —
//! and prints why it skipped, rather than vanishing from the list.
//!
//! **On wasm** by the `scale` cargo feature, checked at compile time. It has
//! to be a compile-time gate: a `#[wasm_bindgen_test]` that merely returns
//! early still *runs*, and `ci/expected-tests.txt` holds each lane's EXACT
//! count (trap 20), so a test that quietly no-ops would both inflate that
//! number and be the "green having tested nothing" shape trap 2 is about. A
//! cargo feature is the only compile-time switch available: `cfg` cannot read
//! an environment variable without a build script, and setting one through
//! `RUSTFLAGS --cfg` is forbidden outright (trap 1). The wasm lanes do not
//! pass `--all-features`, so this stays out of them; the nightly job turns it
//! on explicitly.
//!
//! Twenty thousand rather than a hundred on wasm, because the browser holds
//! the whole key index in a wasm heap that a headless tab will not grow
//! forever.

#![cfg(any(not(target_arch = "wasm32"), feature = "scale"))]

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::time::Instant;

    use crossbank::{Bank, BankConfig, Durability, LockerConfig, Result};

    const KEYS: usize = 100_000;
    const BATCH: usize = 1_000;

    fn key(i: usize) -> String {
        format!("k{i:06}")
    }

    fn value(i: usize) -> String {
        format!("value-{i}-{}", "x".repeat(48))
    }

    /// Fill a fresh bank, then reopen it and check what came back.
    ///
    /// `durability` is the whole reason this is a function rather than a test
    /// body: the run is done twice, so the log carries both the relaxed and
    /// the default fsync cost side by side on the same machine.
    async fn fill_and_reopen(dir: &std::path::Path, durability: Durability) -> Result<()> {
        let file = dir.join("scale.redb");
        let _ = std::fs::remove_file(&file);
        let config = BankConfig::at(&file);
        let locker_config = LockerConfig::default().with_durability(durability);

        let write = {
            let bank = Bank::open(config.clone()).await?;
            let locker = bank
                .lazy_locker_with::<String>("scale", locker_config)
                .await?;

            let started = Instant::now();
            let mut batch = Vec::with_capacity(BATCH);
            for i in 0..KEYS {
                batch.push((key(i), value(i)));
                if batch.len() == BATCH {
                    locker.put_all(std::mem::take(&mut batch)).await?;
                    batch.reserve(BATCH);
                }
            }
            if !batch.is_empty() {
                locker.put_all(batch).await?;
            }
            // The one call an Eventual locker is owed. On the default
            // durability it costs nothing extra, which is why it is
            // unconditional.
            locker.flush().await?;
            let elapsed = started.elapsed();

            assert_eq!(locker.len(), KEYS, "every key must be in the index");
            bank.close().await?;
            elapsed
        };

        // A cold open: this is the number that matters to an application's
        // start-up, because a lazy locker builds its whole key index here.
        let bank = Bank::open(config).await?;
        let open_started = Instant::now();
        let locker = bank
            .lazy_locker_with::<String>("scale", locker_config)
            .await?;
        let open = open_started.elapsed();

        println!(
            "  {durability:?}: wrote {KEYS} keys in {write:.2?} \
             ({:.0} keys/s), reopened the index in {open:.2?}",
            KEYS as f64 / write.as_secs_f64().max(f64::MIN_POSITIVE),
        );

        assert_eq!(
            locker.len(),
            KEYS,
            "a reopen must find every key that was written"
        );

        // A range over storage — not the RAM index — must still be exactly
        // its slice, at both ends.
        let from = key(50_000);
        let to = key(50_100);
        let slice = locker.range(from.as_str()..to.as_str()).await?;
        assert_eq!(slice.len(), 100, "a range must not overrun at scale");
        assert_eq!(slice[0].0, key(50_000), "inclusive start");
        assert_eq!(slice[99].0, key(50_099), "exclusive end");
        assert_eq!(slice[0].1, value(50_000), "values must ride along");

        // And the index agrees with it.
        let prefixed = locker.keys_with_prefix("k05000");
        assert_eq!(
            prefixed.len(),
            10,
            "k05000* is exactly ten keys of a six-digit space"
        );

        // Spot-check the far ends, which a paged scan is likeliest to drop.
        assert_eq!(locker.get(&key(0)).await?, Some(value(0)));
        assert_eq!(locker.get(&key(KEYS - 1)).await?, Some(value(KEYS - 1)));
        assert_eq!(locker.get("k999999").await?, None);

        bank.close().await?;
        let _ = std::fs::remove_file(&file);
        Ok(())
    }

    #[test]
    fn hundred_thousand_keys_open_and_scan() {
        if std::env::var("CROSSBANK_SCALE").ok().as_deref() != Some("1") {
            println!(
                "SKIP hundred_thousand_keys_open_and_scan: set CROSSBANK_SCALE=1 to run it \
                 (it writes {KEYS} keys and takes minutes on a cold cache)."
            );
            return;
        }

        let dir = tempfile::Builder::new()
            .prefix("crossbank-scale-")
            .tempdir()
            .expect("could not create a temporary directory");

        println!("scale: {KEYS} keys, batches of {BATCH}");
        futures::executor::block_on(async {
            // Relaxed first, because that is the configuration an application
            // doing a bulk import would actually use.
            fill_and_reopen(dir.path(), Durability::Eventual).await?;
            fill_and_reopen(dir.path(), Durability::Immediate).await
        })
        .expect("the scale run must complete");
    }
}

#[cfg(all(target_arch = "wasm32", feature = "scale"))]
mod web {
    use crossbank::{Bank, BankConfig, Result};
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    const KEYS: usize = 20_000;
    const BATCH: usize = 1_000;

    fn key(i: usize) -> String {
        format!("k{i:06}")
    }

    fn value(i: usize) -> String {
        format!("value-{i}")
    }

    async fn run() -> Result<()> {
        let name = "crossbank-scale";
        let config = BankConfig::web(name);
        crossbank::delete_bank(&config).await?;

        {
            let bank = Bank::open(config.clone()).await?;
            let locker = bank.lazy_locker::<String>("scale").await?;
            let mut batch = Vec::with_capacity(BATCH);
            for i in 0..KEYS {
                batch.push((key(i), value(i)));
                if batch.len() == BATCH {
                    locker.put_all(std::mem::take(&mut batch)).await?;
                    batch.reserve(BATCH);
                }
            }
            if !batch.is_empty() {
                locker.put_all(batch).await?;
            }
            assert_eq!(locker.len(), KEYS);
            // Explicit, never `Drop`: the atomics lane aborts on panic and
            // would never run a destructor.
            bank.close().await?;
        }

        let bank = Bank::open(config.clone()).await?;
        let locker = bank.lazy_locker::<String>("scale").await?;
        assert_eq!(locker.len(), KEYS, "a reopen must find every key");

        let from = key(10_000);
        let to = key(10_100);
        let slice = locker.range(from.as_str()..to.as_str()).await?;
        assert_eq!(slice.len(), 100);
        assert_eq!(slice[0].0, key(10_000));
        assert_eq!(slice[99].0, key(10_099));
        assert_eq!(locker.get(&key(KEYS - 1)).await?, Some(value(KEYS - 1)));

        bank.close().await?;
        crossbank::delete_bank(&config).await?;
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn twenty_thousand_keys_open_and_scan() {
        run().await.expect("the wasm scale run must complete");
    }
}
