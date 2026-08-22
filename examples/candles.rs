//! A lazy locker holding bulk market data: batched writes, streaming I/O, and
//! a byte budget that actually evicts.
//!
//! Run with `cargo run --example candles`.
//!
//! This is the other half of the Hive split. A [`LazyLocker`] keeps only the
//! key index resident, so opening it costs the number of keys rather than the
//! size of the data, and every read is an await. That is what lets one locker
//! hold hundreds of megabytes on a platform where an eager one could not.
//!
//! [`LazyLocker`]: crossbank::LazyLocker

use serde::{Deserialize, Serialize};

use crossbank::{Bank, BankConfig, LazyLocker, LockerConfig, Policy};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Candle {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

fn main() -> crossbank::Result<()> {
    futures::executor::block_on(run())
}

async fn run() -> crossbank::Result<()> {
    let bank = Bank::open(BankConfig::memory()).await?;

    // === one transaction, not one commit per bar ===
    //
    // The honest answer to "many small puts". Every staged write lands
    // together or none does, and the closure sees its own writes.
    let series: LazyLocker<Candle> = bank.lazy_locker("BTCUSDT-1m").await?;
    series
        .transact(|tx| async move {
            for minute in 0..500u32 {
                let price = 60_000.0 + f64::from(minute);
                tx.put(
                    &format!("{minute:06}"),
                    Candle {
                        open: price,
                        high: price + 12.0,
                        low: price - 9.0,
                        close: price + 3.0,
                    },
                )?;
            }
            // Reads inside the transaction see the staged writes.
            let staged = tx.get("000499").await?;
            println!("inside the transaction, the last bar is {staged:?}");
            Ok(())
        })
        .await?;
    println!("{} bars stored in one commit", series.len());

    // Keys are ordered, so a time window is a range scan.
    let window = series.range("000100".."000103").await?;
    println!(
        "range 100..103 returned {:?}",
        window.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>()
    );

    // === streaming a value too big to hold ===
    //
    // A `Writer` over a `LazyLocker<Vec<u8>>` never holds the whole value:
    // each piece is sealed and committed on its own, so peak memory is a small
    // multiple of `chunk_size` rather than of the value. The same in reverse
    // for `Reader`.
    let blobs: LazyLocker<Vec<u8>> = bank
        .lazy_locker_with(
            "raw-feed",
            LockerConfig::default().with_chunk_size(64 * 1024),
        )
        .await?;

    let mut writer = blobs.writer("2026-08-21").await?;
    for _ in 0..16 {
        writer.write_chunk(&vec![7u8; 64 * 1024]).await?;
    }
    // Nothing under that key changes until `finish`. A `Writer` dropped or
    // aborted leaves the previous value exactly as it was.
    writer.finish().await?;

    let mut reader = blobs.reader("2026-08-21").await?.expect("just written");
    let mut total = 0usize;
    let mut pieces = 0usize;
    while let Some(piece) = reader.next_chunk().await? {
        total += piece.len();
        pieces += 1;
    }
    println!("streamed {total} bytes back in {pieces} pieces");

    // === a byte budget that evicts ===
    //
    // `Policy::Evictable` is opt-in and never chosen for you: the default is
    // `Precious`, because silently losing data for anyone who did not think
    // about retention would be the wrong way round. Here a cache is capped,
    // and the least recently used entries are shed to stay under the cap.
    let cache: LazyLocker<Vec<u8>> = bank
        .lazy_locker_with(
            "thumbnail-cache",
            LockerConfig::default().with_policy(Policy::Evictable { max_bytes: 8 * 1024 }),
        )
        .await?;

    for i in 0..20u32 {
        cache.put(&format!("thumb-{i:02}"), &vec![i as u8; 1024]).await?;
    }
    println!(
        "cache holds {} of 20 entries, {} bytes against an 8 KiB budget",
        cache.len(),
        cache.budget_used()
    );
    println!(
        "the oldest is gone: {:?}, the newest is not: {:?}",
        cache.get("thumb-00").await?.is_none(),
        cache.get("thumb-19").await?.is_some()
    );

    series.close().await?;
    blobs.close().await?;
    cache.close().await?;
    bank.close().await?;
    Ok(())
}
