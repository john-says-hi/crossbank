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

/// Closing a bank releases the store, and reopening finds the same data.
///
/// The case the whole close machinery exists for. `redb` holds an exclusive
/// file lock for as long as its `Database` lives, so before `close()` this was
/// simply impossible in one process — and a consuming test suite that closes
/// and reopens between tests would deadlock on its own data.
pub async fn close_then_reopen_sees_the_same_data<H: Harness>(h: &H) -> Result<()> {
    let first = bank(h).await?;
    let locker = first.lazy_locker::<V>("l").await?;
    locker.put("k", &v("survives")).await?;

    first.close().await?;
    assert!(first.is_closed(), "close() must be observable");

    let second = bank(h).await?;
    let locker = second.lazy_locker::<V>("l").await?;

    if h.caps().persists_across_open {
        assert_eq!(
            locker.get("k").await?,
            Some(v("survives")),
            "a persistent backend must find its data after close and reopen"
        );
    } else {
        assert_eq!(
            locker.get("k").await?,
            None,
            "a non-persistent backend must NOT find data after close and reopen"
        );
    }
    Ok(())
}

/// Everything refuses politely after a close. Nothing panics, nothing lies.
pub async fn operations_after_close_report_closed<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let lazy = bank.lazy_locker::<V>("lazy").await?;
    let eager = bank.locker::<V>("eager").await?;
    let survivor = bank.lazy_locker::<V>("survivor").await?;

    lazy.put("k", &v("x")).await?;
    eager.put("k", v("x")).await?;

    assert!(bank.is_locker_open("lazy"));
    assert!(bank.is_locker_open("eager"));

    lazy.close();
    eager.close();

    // A closed locker reads as empty; `is_closed` is what tells the two apart.
    assert!(lazy.is_closed());
    assert_eq!(lazy.len(), 0);
    assert!(!lazy.contains_key("k"));
    assert!(eager.is_closed());
    assert_eq!(eager.get("k"), None, "an eager get must not await or fail");
    assert_eq!(eager.len(), 0);

    assert!(!bank.is_locker_open("lazy"), "a closed locker is not open");
    assert!(!bank.is_locker_open("eager"));
    assert_eq!(bank.open_locker_names(), vec!["survivor".to_string()]);

    assert!(matches!(lazy.get("k").await, Err(Error::Closed)));
    assert!(matches!(lazy.put("k", &v("y")).await, Err(Error::Closed)));
    assert!(matches!(lazy.delete("k").await, Err(Error::Closed)));
    assert!(matches!(lazy.clear().await, Err(Error::Closed)));
    assert!(matches!(
        lazy.transact(|tx| async move {
            tx.put("k", v("y"))?;
            Ok(())
        })
        .await,
        Err(Error::Closed)
    ));
    assert!(matches!(eager.put("k", v("y")).await, Err(Error::Closed)));
    assert!(matches!(eager.delete("k").await, Err(Error::Closed)));
    assert!(matches!(eager.clear().await, Err(Error::Closed)));

    // Now the bank itself. A locker that was never individually closed still
    // has to stop working, because the store underneath it is gone.
    bank.close().await?;
    assert!(bank.is_closed());

    assert!(matches!(survivor.get("k").await, Err(Error::Closed)));
    assert!(matches!(
        survivor.put("k", &v("y")).await,
        Err(Error::Closed)
    ));
    assert!(matches!(survivor.delete("k").await, Err(Error::Closed)));
    assert!(matches!(
        bank.lazy_locker::<V>("opened_too_late").await,
        Err(Error::Closed)
    ));
    Ok(())
}

/// Closing twice is not an error, at either level.
pub async fn close_is_idempotent<H: Harness>(h: &H) -> Result<()> {
    let first = bank(h).await?;
    let locker = first.lazy_locker::<V>("l").await?;
    locker.put("k", &v("x")).await?;

    locker.close();
    locker.close();
    assert!(locker.is_closed());

    first.close().await?;
    first.close().await?;
    assert!(first.is_closed());

    // And the store is genuinely released, not merely flagged: a second close
    // must not have left anything behind that blocks the next open.
    let second = bank(h).await?;
    second.lazy_locker::<V>("l").await?;
    second.close().await?;
    Ok(())
}

/// Count every row in one of the fixed tables, paging to the end.
///
/// Reaches past the locker API on purpose: "the value is gone" is a weaker
/// claim than "the chunk rows that held it are gone", and only the second one
/// proves the delete does not leak storage.
async fn count_rows(bank: &Bank, table: crossbank::backend::Table) -> Result<usize> {
    use crossbank::backend::{KeyRange, ScanRequest};

    let mut range = KeyRange::all();
    let mut seen = 0usize;
    loop {
        let page = bank
            .backend()
            .scan(ScanRequest {
                table,
                range: range.clone(),
                reverse: false,
                limit: 64,
                want_values: false,
            })
            .await?;
        seen += page.items.len();
        match page.resume {
            Some(last) => range.start = std::ops::Bound::Excluded(last),
            None => break,
        }
    }
    Ok(seen)
}

/// Deleting a locker takes its records and its chunk payloads with it.
pub async fn delete_locker_removes_records_and_chunks<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker_with::<V>("doomed", tiny_chunks()).await?;

    locker.put("small", &v("inline")).await?;
    locker.put("big", &"x".repeat(200)).await?;

    assert!(
        count_rows(&bank, crossbank::backend::Table::Chunks).await? > 0,
        "the setup must actually have chunked something"
    );
    assert!(bank.locker_exists("doomed").await?);
    assert!(bank.locker_bytes("doomed").await? > 200);

    // An open handle would be left serving data that no longer exists.
    assert!(matches!(
        bank.delete_locker("doomed").await,
        Err(Error::InvalidConfig(_))
    ));

    locker.close();
    assert!(bank.delete_locker("doomed").await?);

    assert_eq!(
        count_rows(&bank, crossbank::backend::Table::Records).await?,
        0
    );
    assert_eq!(
        count_rows(&bank, crossbank::backend::Table::Chunks).await?,
        0,
        "every chunk the deleted locker pointed at must be gone too"
    );
    assert!(!bank.locker_exists("doomed").await?);
    assert_eq!(bank.locker_bytes("doomed").await?, 0);
    assert!(
        !bank.delete_locker("doomed").await?,
        "deleting a name that is not there is not an error"
    );
    Ok(())
}

/// A delete stops at the locker boundary, exactly as `clear` does.
pub async fn delete_locker_leaves_other_lockers_intact<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let doomed = bank.lazy_locker_with::<V>("doomed", tiny_chunks()).await?;
    let keeper = bank.lazy_locker_with::<V>("keeper", tiny_chunks()).await?;

    doomed.put("k", &"x".repeat(200)).await?;
    keeper.put("k", &"y".repeat(200)).await?;
    keeper.put("inline", &v("kept")).await?;

    doomed.close();
    assert!(bank.delete_locker("doomed").await?);

    assert_eq!(keeper.get("k").await?, Some("y".repeat(200)));
    assert_eq!(keeper.get("inline").await?, Some(v("kept")));
    assert!(bank.locker_exists("keeper").await?);
    assert!(
        bank.locker_names().await?.contains(&"keeper".to_string()),
        "the surviving locker must still be registered"
    );
    assert!(!bank.locker_names().await?.contains(&"doomed".to_string()));
    Ok(())
}

/// Ids are never recycled, so a stale record can never be read as a new
/// locker's. Recreating a deleted name gets a fresh id.
pub async fn a_deleted_locker_name_gets_a_fresh_id<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("recycled").await?;
    let first = bank.locker_id("recycled").await?;
    locker.put("k", &v("old")).await?;
    locker.close();

    assert!(bank.delete_locker("recycled").await?);

    let reborn = bank.lazy_locker::<V>("recycled").await?;
    let second = bank.locker_id("recycled").await?;

    assert_ne!(
        first, second,
        "a deleted locker's id must never be handed out again"
    );
    assert_eq!(
        reborn.get("k").await?,
        None,
        "the recreated locker must start empty"
    );
    Ok(())
}

/// Keys are bytes, not strings — and they sort bytewise on every backend.
///
/// The property a Hive-shaped Dart shim needs: Hive allows integer keys, and
/// the only deterministic way to carry one is to encode it to bytes. IndexedDB
/// orders binary keys bytewise exactly as `redb` and `BTreeMap` do, so this
/// must come out identical everywhere.
pub async fn binary_keys_round_trip_and_sort_bytewise<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let locker = bank.lazy_locker::<V>("l").await?;

    let binary: [&[u8]; 3] = [&[0xFF], &[0x00], &[0x80, 0x01]];
    for (i, key) in binary.iter().enumerate() {
        locker.put_by(key, &v(&format!("b{i}"))).await?;
    }
    locker.put("a", &v("text")).await?;

    for (i, key) in binary.iter().enumerate() {
        assert_eq!(
            locker.get_by(key).await?,
            Some(v(&format!("b{i}"))),
            "a binary key must read back under the bytes it was written with"
        );
        assert!(locker.contains_key_by(key));
    }
    assert_eq!(locker.get("a").await?, Some(v("text")));
    assert_eq!(locker.len(), 4);

    assert_eq!(
        locker.keys_bytes(),
        vec![vec![0x00], b"a".to_vec(), vec![0x80, 0x01], vec![0xFF],],
        "keys must come back in bytewise order"
    );

    // The `&str` listing skips what it cannot spell, and says so rather than
    // failing or pretending the locker holds only what it can name. Note that
    // 0x00 IS valid UTF-8 — a one-character NUL string — so it survives the
    // filter while 0xFF and 0x80 0x01 do not.
    assert_eq!(locker.keys(), vec!["\u{0}".to_string(), "a".to_string()]);
    assert!(locker.has_non_utf8_keys());

    locker.delete_by(&[0xFF]).await?;
    assert_eq!(locker.get_by(&[0xFF]).await?, None);
    assert_eq!(locker.len(), 3);
    Ok(())
}

/// A bulk write is one commit, and a refused one writes nothing.
///
/// Hive's `putAll`. The negative half is the part that matters: crossbank
/// validates the whole write-set before committing any of it, so a single
/// oversized entry must leave the locker exactly as it was rather than
/// half-filled.
pub async fn put_all_is_atomic<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let lazy = bank.lazy_locker::<V>("bulk").await?;

    let entries: Vec<(String, V)> = (0..50)
        .map(|i| (format!("k{i:03}"), v(&format!("v{i}"))))
        .collect();
    lazy.put_all(entries).await?;

    assert_eq!(lazy.len(), 50);
    for i in 0..50 {
        assert_eq!(
            lazy.get(&format!("k{i:03}")).await?,
            Some(v(&format!("v{i}"))),
            "every entry of a put_all must be present"
        );
    }

    lazy.delete_all(["k000", "k001", "never_written"]).await?;
    assert_eq!(lazy.len(), 48);
    assert_eq!(lazy.get("k000").await?, None);

    // Now the negative half, on an eager locker whose inline limit the second
    // entry breaks. No Fault backend needed: the refusal is crossbank's own.
    let eager = bank
        .locker_with::<V>("strict", LockerConfig::default().with_max_inline(64))
        .await?;
    let mixed = vec![
        ("ok".to_string(), v("small")),
        ("bad".to_string(), "x".repeat(10_000)),
    ];
    assert!(
        matches!(eager.put_all(mixed).await, Err(Error::ValueTooLarge { .. })),
        "an oversized entry must refuse the whole put_all"
    );
    assert_eq!(eager.len(), 0, "a refused put_all must write NOTHING");
    assert!(eager.get("ok").is_none());

    // And storage agrees with RAM: reopening finds nothing either.
    eager.close();
    let reopened = bank
        .locker_with::<V>("strict", LockerConfig::default().with_max_inline(64))
        .await?;
    assert_eq!(reopened.len(), 0);
    Ok(())
}

/// `to_map` is a bulk view of exactly what key-by-key reads return.
///
/// Hive's `toMap`, and the shape a Dart shim needs. Worth its own case
/// because the map path and the `get` path take different routes through the
/// backend — a scan versus point lookups — and a backend that ordered or
/// paged its scan wrongly would disagree with itself here.
pub async fn to_map_matches_key_by_key_reads<H: Harness>(h: &H) -> Result<()> {
    let bank = bank(h).await?;
    let lazy = bank.lazy_locker::<V>("l").await?;

    let entries: Vec<(String, V)> = (0..40)
        .map(|i| (format!("k{i:03}"), v(&format!("v{i}"))))
        .collect();
    lazy.put_all(entries).await?;
    lazy.put("empty", &v("")).await?;

    let map = lazy.to_map().await?;
    assert_eq!(map.len(), lazy.len());
    for key in lazy.keys() {
        assert_eq!(
            map.get(&key),
            lazy.get(&key).await?.as_ref(),
            "to_map and get must agree about {key:?}"
        );
    }
    assert_eq!(map.get("empty"), Some(&v("")), "an empty value is a value");

    // The eager locker answers the same question from RAM.
    let eager = bank.locker::<V>("e").await?;
    eager
        .put_all(vec![("a".to_string(), v("1")), ("b".to_string(), v("2"))])
        .await?;
    let eager_map = eager.to_map();
    assert_eq!(eager_map.len(), 2);
    assert_eq!(eager_map.get("a").map(|x| x.as_str()), Some("1"));
    assert_eq!(
        eager
            .range("a".."b")
            .into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<_>>(),
        vec!["a".to_string()]
    );
    Ok(())
}
