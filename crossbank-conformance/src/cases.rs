//! The cases themselves.
//!
//! Plain `pub async fn`s with no test attributes — the emitter macros in the
//! crate root turn them into `#[test]` or `#[wasm_bindgen_test]` as
//! appropriate.
//!
//! Two rules these follow, both forced by the atomics lane building with
//! `panic = "abort"`:
//!
//! * Never `#[should_panic]`. Assert on an `Err` variant instead — which is
//!   better spec-writing regardless: "returns `SchemaMismatch`" is a contract,
//!   "panics" is not.
//! * Never rely on `Drop` for cleanup. With abort there is no unwinding.

use crossbank::{Bank, Error, Event, LockerConfig, Result};
use futures::StreamExt;

use crate::Harness;

/// The value type the suite stores. Deliberately boring — this suite grades
/// storage behaviour, not serialisation.
type V = String;

async fn bank<H: Harness>(h: &H) -> Result<Bank> {
    Bank::with_backend(h.open().await?).await
}

fn v(s: &str) -> V {
    s.to_string()
}

pub async fn put_get_roundtrip<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("k", &v("value")).await?;
    assert_eq!(locker.get("k").await?, Some(v("value")));
    Ok(())
}

pub async fn missing_key_is_none<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    assert_eq!(locker.get("absent").await?, None);
    assert!(!locker.contains_key("absent"));
    Ok(())
}

/// A stored empty value and a missing key are different things.
///
/// Worth pinning explicitly: the Dart bridge crossbank is meant to replace
/// conflates them, treating an empty payload as absent, which silently drops
/// legitimately-empty values.
pub async fn empty_value_is_not_a_missing_key<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("empty", &v("")).await?;

    assert_eq!(locker.get("empty").await?, Some(v("")));
    assert_eq!(locker.get("never_written").await?, None);
    assert!(locker.contains_key("empty"));
    Ok(())
}

pub async fn overwrite_replaces_value<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("k", &v("first")).await?;
    locker.put("k", &v("second")).await?;

    assert_eq!(locker.get("k").await?, Some(v("second")));
    assert_eq!(locker.len(), 1, "overwrite must not add a second key");
    Ok(())
}

pub async fn delete_is_idempotent<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("k", &v("value")).await?;
    locker.delete("k").await?;
    locker.delete("k").await?;
    locker.delete("never_existed").await?;

    assert_eq!(locker.get("k").await?, None);
    Ok(())
}

/// A clear must be bounded by its own locker.
///
/// The failure this catches is a range whose upper bound leaks past the locker
/// prefix and wipes a neighbour's data.
pub async fn clear_empties_only_its_own_locker<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let one = bank.lazy_locker::<V>("one").await?;
    let two = bank.lazy_locker::<V>("two").await?;

    one.put("k", &v("from one")).await?;
    two.put("k", &v("from two")).await?;

    one.clear().await?;

    assert_eq!(one.get("k").await?, None);
    assert_eq!(
        two.get("k").await?,
        Some(v("from two")),
        "clearing one locker must not disturb another"
    );
    Ok(())
}

pub async fn keys_are_ordered_by_utf8_bytes<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    for k in ["c", "a", "B", "b", "A"] {
        locker.put(k, &v("x")).await?;
    }

    // Uppercase sorts below lowercase in ASCII, which is byte order.
    assert_eq!(locker.keys(), vec!["A", "B", "a", "b", "c"]);
    Ok(())
}

/// The case that makes the web backend honest.
///
/// IndexedDB compares *string* keys by UTF-16 code unit, where U+1F34E is the
/// surrogate pair D83C DF4E and sorts BELOW U+E000. In UTF-8 it is F0 9F 8D 8E
/// and sorts above. Any backend storing string keys fails here.
pub async fn keys_above_the_bmp_sort_bytewise<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    for k in ["\u{1F34E}", "\u{E000}", "\u{FFFD}", "a"] {
        locker.put(k, &v("x")).await?;
    }

    assert_eq!(
        locker.keys(),
        vec!["a", "\u{E000}", "\u{FFFD}", "\u{1F34E}"],
        "keys must sort by UTF-8 bytes, not UTF-16 code units"
    );
    Ok(())
}

pub async fn prefix_listing_stops_at_the_boundary<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    for k in [
        "BTCUSDT::1",
        "BTCUSDT::2",
        "BTCUSDU::1",
        "BTC",
        "ETHUSDT::1",
    ] {
        locker.put(k, &v("x")).await?;
    }

    assert_eq!(
        locker.keys_with_prefix("BTCUSDT::"),
        vec!["BTCUSDT::1", "BTCUSDT::2"]
    );
    Ok(())
}

pub async fn range_is_inclusive_start_exclusive_end<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    for k in ["a", "b", "c", "d"] {
        locker.put(k, &v(k)).await?;
    }

    let keys: Vec<String> = locker
        .range("b".."d")
        .await?
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(keys, vec!["b", "c"]);
    Ok(())
}

pub async fn reverse_range_descends<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    for k in ["a", "b", "c"] {
        locker.put(k, &v(k)).await?;
    }

    let keys: Vec<String> = locker
        .range_rev(..)
        .await?
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(keys, vec!["c", "b", "a"]);
    Ok(())
}

/// Every scan pages, because an IndexedDB cursor cannot outlive its
/// transaction. This proves paging neither skips nor repeats across the
/// boundary.
pub async fn paging_covers_every_key_exactly_once<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    const N: usize = 600; // comfortably past the internal page size
    for i in 0..N {
        locker.put(&format!("k{i:04}"), &v("x")).await?;
    }

    let all = locker.range(..).await?;
    assert_eq!(all.len(), N);

    let mut keys: Vec<String> = all.into_iter().map(|(k, _)| k).collect();
    keys.dedup();
    assert_eq!(keys.len(), N, "paging must not repeat a key");
    assert_eq!(locker.len(), N, "the index must agree with storage");
    Ok(())
}

pub async fn transaction_commit_is_atomic<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker
        .transact(|tx| async move {
            tx.put("chunk::0", v("a"))?;
            tx.put("chunk::1", v("b"))?;
            tx.put("manifest", v("done"))?;
            Ok(())
        })
        .await?;

    assert_eq!(locker.get("chunk::0").await?, Some(v("a")));
    assert_eq!(locker.get("chunk::1").await?, Some(v("b")));
    assert_eq!(locker.get("manifest").await?, Some(v("done")));
    Ok(())
}

/// The property that will make chunked writes safe: a failure partway through
/// leaves the previous state intact, never a blend.
pub async fn transaction_rollback_writes_nothing<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("existing", &v("old")).await?;

    let outcome: Result<()> = locker
        .transact(|tx| async move {
            tx.put("a", v("1"))?;
            tx.put("b", v("2"))?;
            Err(Error::backend("deliberate failure"))
        })
        .await;
    assert!(outcome.is_err(), "the transaction should have failed");

    assert_eq!(locker.get("a").await?, None);
    assert_eq!(locker.get("b").await?, None);
    assert_eq!(locker.get("existing").await?, Some(v("old")));
    assert_eq!(locker.len(), 1, "the index must not have been touched");
    Ok(())
}

pub async fn transaction_reads_its_own_writes<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    locker.put("k", &v("stored")).await?;

    locker
        .transact(|tx| async move {
            assert_eq!(tx.get("k").await?, Some(v("stored")));
            tx.put("k", v("staged"))?;
            assert_eq!(tx.get("k").await?, Some(v("staged")));
            tx.delete("k")?;
            assert_eq!(tx.get("k").await?, None);
            Ok(())
        })
        .await?;

    assert_eq!(locker.get("k").await?, None);
    Ok(())
}

pub async fn watch_reports_writes_and_deletes<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    let mut events = locker.watch();

    locker.put("k", &v("value")).await?;
    locker.delete("k").await?;

    assert_eq!(events.next().await, Some(Event::Put { key: "k".into() }));
    assert_eq!(
        events.next().await,
        Some(Event::Deleted { key: "k".into() })
    );
    Ok(())
}

/// Persistence, asserted in both directions.
///
/// A persistent backend must find its data after reopening; the memory backend
/// must not. Asserting the negative is what stops this case passing vacuously
/// on a backend that never stored anything.
pub async fn reopen_matches_declared_persistence<H: Harness>(h: &H) -> Result<()> {
    {
        let bank = bank(h).await?;
        let locker = bank.lazy_locker::<V>("l").await?;
        locker.put("k", &v("survives")).await?;
    }

    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    if h.caps().persists_across_open {
        assert_eq!(
            locker.get("k").await?,
            Some(v("survives")),
            "a persistent backend must find its data after reopening"
        );
    } else {
        assert_eq!(
            locker.get("k").await?,
            None,
            "a non-persistent backend must NOT find data after reopening"
        );
    }
    Ok(())
}

/// Reopening a locker under a different value type is refused.
///
/// postcard is not self-describing, so without the schema tag this would decode
/// old bytes into the new shape and hand back plausible garbage.
pub async fn schema_mismatch_is_refused<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    bank.lazy_locker::<V>("typed").await?;

    match bank.lazy_locker::<u64>("typed").await {
        Err(Error::SchemaMismatch { .. }) => Ok(()),
        Err(other) => panic!("expected SchemaMismatch, got {other:?}"),
        Ok(_) => panic!("opening a locker as a different type should have been refused"),
    }
}

fn tiny_chunks() -> LockerConfig {
    LockerConfig::default().with_chunk_size(32)
}

/// A value larger than the chunk size round-trips through `put`/`get`.
pub async fn a_value_larger_than_the_chunk_size_round_trips<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker_with::<V>("l", tiny_chunks()).await?;
    let big = "x".repeat(200);
    locker.put("k", &big).await?;
    assert_eq!(locker.get("k").await?, Some(big));
    Ok(())
}

/// Overwriting a chunked value replaces it and does not blend old chunks.
pub async fn overwriting_a_chunked_value_replaces_it<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker_with::<V>("l", tiny_chunks()).await?;
    locker.put("k", &"a".repeat(200)).await?;
    locker.put("k", &"b".repeat(180)).await?;
    assert_eq!(locker.get("k").await?, Some("b".repeat(180)));
    Ok(())
}

/// Deleting a chunked value makes it gone.
pub async fn deleting_a_chunked_value_removes_it<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker_with::<V>("l", tiny_chunks()).await?;
    locker.put("k", &"z".repeat(200)).await?;
    locker.delete("k").await?;
    assert_eq!(locker.get("k").await?, None);
    assert!(!locker.contains_key("k"));
    Ok(())
}

/// An unfinished writer leaves the previous complete value intact.
pub async fn unfinished_writer_leaves_the_previous_value<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank
        .lazy_locker_with::<Vec<u8>>("bytes", tiny_chunks())
        .await?;
    locker.put("k", &vec![1u8; 80]).await?;

    let mut writer = locker.writer("k").await?;
    writer.write_chunk(&[9u8; 40]).await?;
    writer.abort().await?;

    assert_eq!(locker.get("k").await?, Some(vec![1u8; 80]));
    Ok(())
}
