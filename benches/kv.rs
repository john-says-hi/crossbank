//! Native comparison benches: crossbank memory, crossbank redb, raw redb.
//!
//! Values are `Vec<u8>` so serialisation is not the story. Run with
//! `cargo bench`. Not a CI gate — numbers come from a noisy machine.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbank::backend::{Backend, MemoryBackend, Op, Table};
use crossbank::codec::FilterChain;
use crossbank::{Bank, BankConfig, Durability, LazyLocker, Locker, LockerConfig};
use futures::executor::block_on;
use redb::{Database, TableDefinition};
use tempfile::TempDir;

/// Workload shapes shared with `tests/bench_web.rs` (and mirrored in
/// `bench/hive_ce/lib/workloads.dart`), so the native and web lanes measure
/// the same operation counts over the same bytes.
#[path = "common/mod.rs"]
mod common;

use common::{key, payload, BULK_BYTES, BULK_N, SETTINGS_BYTES, SETTINGS_N, TXN_N};

fn wait<T>(fut: impl std::future::Future<Output = T>) -> T {
    block_on(fut)
}

struct NativeBank {
    _dir: Option<TempDir>,
    bank: Bank,
}

fn memory_bank() -> NativeBank {
    NativeBank {
        _dir: None,
        bank: wait(Bank::open(BankConfig::memory())).unwrap(),
    }
}

fn redb_bank() -> NativeBank {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bank.redb");
    NativeBank {
        bank: wait(Bank::open(BankConfig::at(path))).unwrap(),
        _dir: Some(dir),
    }
}

fn redb_raw_chain() -> NativeBank {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bank.redb");
    let backend = Arc::new(crossbank::RedbBackend::open(path).unwrap());
    NativeBank {
        bank: wait(Bank::with_backend_and_chain(backend, FilterChain::raw())).unwrap(),
        _dir: Some(dir),
    }
}

/// A locker config at the given durability. `Eventual` is what the
/// `*_eventual` bench arms measure: same code path, one fsync per commit
/// removed.
fn cfg(durability: Durability) -> LockerConfig {
    LockerConfig::default().with_durability(durability)
}

fn fill_eager(locker: &Locker<Vec<u8>>, n: usize, bytes: usize) {
    for i in 0..n {
        wait(locker.put(&key(i), payload(bytes, i as u8))).unwrap();
    }
}

fn fill_lazy(locker: &LazyLocker<Vec<u8>>, n: usize, bytes: usize) {
    for i in 0..n {
        wait(locker.put(&key(i), &payload(bytes, i as u8))).unwrap();
    }
}

fn settings_eager(c: &mut Criterion) {
    let mut group = c.benchmark_group("settings_eager");
    group.throughput(Throughput::Elements(SETTINGS_N as u64));
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(2));

    for (label, factory, durability) in [
        (
            "memory",
            memory_bank as fn() -> NativeBank,
            Durability::Immediate,
        ),
        (
            "redb",
            redb_bank as fn() -> NativeBank,
            Durability::Immediate,
        ),
        (
            "redb_eventual",
            redb_bank as fn() -> NativeBank,
            Durability::Eventual,
        ),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let native = factory();
            let locker = wait(
                native
                    .bank
                    .locker_with::<Vec<u8>>("settings", cfg(durability)),
            )
            .unwrap();
            fill_eager(&locker, SETTINGS_N, SETTINGS_BYTES);
            let mut i = 0usize;
            b.iter(|| {
                // 90/10 get/put, Hive Box shaped.
                if i.is_multiple_of(10) {
                    wait(locker.put(&key(i % SETTINGS_N), payload(SETTINGS_BYTES, i as u8)))
                        .unwrap();
                } else {
                    criterion::black_box(locker.get(&key(i % SETTINGS_N)));
                }
                i = i.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn bulk_lazy_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_lazy_put");
    group.throughput(Throughput::Bytes((BULK_N * BULK_BYTES) as u64));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(3));

    for (label, factory, durability) in [
        (
            "memory",
            memory_bank as fn() -> NativeBank,
            Durability::Immediate,
        ),
        (
            "redb",
            redb_bank as fn() -> NativeBank,
            Durability::Immediate,
        ),
        (
            "redb_eventual",
            redb_bank as fn() -> NativeBank,
            Durability::Eventual,
        ),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_with_setup(factory, |native| {
                let locker = wait(
                    native
                        .bank
                        .lazy_locker_with::<Vec<u8>>("bulk", cfg(durability)),
                )
                .unwrap();
                fill_lazy(&locker, BULK_N, BULK_BYTES);
                // An eventual run must pay for the fsync it deferred, or the
                // number is a lie rather than a trade.
                wait(locker.flush()).unwrap();
                native
            });
        });
    }
    group.finish();
}

fn bulk_lazy_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_lazy_get");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(2));

    for (label, factory) in [
        ("memory", memory_bank as fn() -> NativeBank),
        ("redb", redb_bank as fn() -> NativeBank),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let native = factory();
            let locker = wait(native.bank.lazy_locker::<Vec<u8>>("bulk")).unwrap();
            fill_lazy(&locker, BULK_N, BULK_BYTES);
            let mut i = 0usize;
            b.iter(|| {
                let got = wait(locker.get(&key(i % BULK_N))).unwrap();
                criterion::black_box(got);
                i = i.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_scan");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(2));

    for (label, factory) in [
        ("memory", memory_bank as fn() -> NativeBank),
        ("redb", redb_bank as fn() -> NativeBank),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let native = factory();
            let locker = wait(native.bank.lazy_locker::<Vec<u8>>("scan")).unwrap();
            for i in 0..BULK_N {
                let k = if i % 2 == 0 {
                    format!("keep::{i:06}")
                } else {
                    format!("skip::{i:06}")
                };
                wait(locker.put(&k, &payload(16, i as u8))).unwrap();
            }
            b.iter(|| {
                let keys = locker.keys_with_prefix("keep::");
                criterion::black_box(keys.len());
            });
        });
    }
    group.finish();
}

fn txn_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("txn_batch");
    group.throughput(Throughput::Elements(TXN_N as u64));
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(2));

    for (label, factory) in [
        ("memory", memory_bank as fn() -> NativeBank),
        ("redb", redb_bank as fn() -> NativeBank),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let native = factory();
            let locker = wait(native.bank.lazy_locker::<Vec<u8>>("txn")).unwrap();
            let mut gen = 0u64;
            b.iter(|| {
                gen += 1;
                wait(locker.transact(|tx| {
                    let g = gen;
                    async move {
                        for i in 0..TXN_N {
                            tx.put(&format!("{g}:{i}"), payload(64, i as u8))?;
                        }
                        Ok(())
                    }
                }))
                .unwrap();
            });
        });
    }
    group.finish();
}

fn reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("reopen");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    // Cold: a file this process has never opened before, one per iteration.
    // Creating it and writing the key is SETUP, not measurement — Hive's
    // 1.3 ms `reopen` times only the open, so timing redb's file creation here
    // too would compare a create against an open. Only
    // `Bank::open` + `lazy_locker` + one `get` is inside the timed closure.
    group.bench_function("redb", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("bank.redb");
                let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
                let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
                wait(locker.put("k", &payload(1024, 1))).unwrap();
                wait(bank.close()).unwrap();
                (dir, path)
            },
            |(dir, path)| {
                let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
                let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
                criterion::black_box(wait(locker.get("k")).unwrap());
                // Returned so the TempDir is dropped outside the timed region.
                dir
            },
            criterion::BatchSize::PerIteration,
        );
    });
    // The same open against a file that has already been opened in this
    // process, so the OS page cache is warm. This arm is the one that
    // describes a *second* application start on the same bank.
    group.bench_function("redb_warm", |b| {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bank.redb");
        {
            let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
            let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
            wait(locker.put("k", &payload(1024, 1))).unwrap();
        }
        b.iter(|| {
            let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
            let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
            criterion::black_box(wait(locker.get("k")).unwrap());
        });
    });
    group.finish();
}

/// Opening a lazy locker over a locker that already holds a lot of keys.
///
/// The scan-page path: a keys-only walk of the whole locker, paged. This is
/// what an application start actually pays for a big candle cache, and it is
/// the only workload that can tell one page size from another.
fn index_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_open");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(BULK_N as u64));

    group.bench_function("redb", |b| {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bank.redb");
        {
            let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
            let locker =
                wait(bank.lazy_locker_with::<Vec<u8>>("bulk", cfg(Durability::Eventual))).unwrap();
            fill_lazy(&locker, BULK_N, BULK_BYTES);
            wait(locker.flush()).unwrap();
        }
        b.iter(|| {
            let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
            let locker = wait(bank.lazy_locker::<Vec<u8>>("bulk")).unwrap();
            criterion::black_box(locker.len());
        });
    });
    group.finish();
}

fn envelope_tax(c: &mut Criterion) {
    let mut group = c.benchmark_group("envelope_tax");
    group.throughput(Throughput::Bytes((SETTINGS_N * SETTINGS_BYTES) as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("crossbank_default_chain", |b| {
        b.iter_with_setup(redb_bank, |native| {
            let locker = wait(native.bank.lazy_locker::<Vec<u8>>("l")).unwrap();
            fill_lazy(&locker, SETTINGS_N, SETTINGS_BYTES);
            native
        });
    });
    group.bench_function("crossbank_raw_chain", |b| {
        b.iter_with_setup(redb_raw_chain, |native| {
            let locker = wait(native.bank.lazy_locker::<Vec<u8>>("l")).unwrap();
            fill_lazy(&locker, SETTINGS_N, SETTINGS_BYTES);
            native
        });
    });
    group.bench_function("raw_redb", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let path = dir.path().join("raw.redb");
                let db = Database::create(&path).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                const T: TableDefinition<&[u8], &[u8]> = TableDefinition::new("records");
                let txn = db.begin_write().unwrap();
                {
                    let mut table = txn.open_table(T).unwrap();
                    for i in 0..SETTINGS_N {
                        let k = key(i);
                        let v = payload(SETTINGS_BYTES, i as u8);
                        table.insert(k.as_bytes(), v.as_slice()).unwrap();
                    }
                }
                txn.commit().unwrap();
            },
        );
    });
    group.finish();
}

/// Tiny sanity that the memory backend's `commit` is in the same ballpark as
/// a `BTreeMap` insert, so a later regression in the engine is visible.
fn backend_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("backend_put");
    group.sample_size(20);
    let backend = MemoryBackend::new();
    let mut i = 0u64;
    group.bench_function("memory", |b| {
        b.iter(|| {
            i += 1;
            wait(backend.commit(vec![Op::Put {
                table: Table::Records,
                key: i.to_be_bytes().to_vec(),
                value: payload(64, i as u8),
            }]))
            .unwrap();
        });
    });
    group.finish();
}

const BIG_BYTES: usize = 8 * 1024 * 1024;

/// Chunk-size sweep over one 8 MiB value: write + read back, redb, default
/// chain. Answers PLAN.md's "what should the default chunk size be".
fn chunk_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_sweep");
    group.throughput(Throughput::Bytes(BIG_BYTES as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    let value = payload(BIG_BYTES, 3);
    for chunk_kib in [256usize, 1024, 4096, 8192] {
        group.bench_function(
            BenchmarkId::new("redb_put_get", format!("{chunk_kib}KiB")),
            |b| {
                b.iter_with_setup(redb_bank, |native| {
                    let locker = wait(native.bank.lazy_locker_with::<Vec<u8>>(
                        "big",
                        LockerConfig::default().with_chunk_size(chunk_kib * 1024),
                    ))
                    .unwrap();
                    wait(locker.put("k", &value)).unwrap();
                    let got = wait(locker.get("k")).unwrap().unwrap();
                    criterion::black_box(got.len());
                    native
                });
            },
        );
    }
    group.finish();
}

/// Candle-shaped payload: dense IEEE-754 f64 OHLCV rows as little-endian
/// bytes, prices random-walking so the mantissas are high-entropy.
fn f64_candles(rows: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows * 6 * 8);
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut price = 42_000.0f64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    for i in 0..rows {
        let ts = 1_700_000_000.0 + i as f64 * 60.0;
        let o = price;
        let h = o * (1.0 + next() * 0.002);
        let l = o * (1.0 - next() * 0.002);
        price = l + (h - l) * next();
        let v = next() * 1000.0;
        for f in [ts, o, h, l, price, v] {
            out.extend_from_slice(&f.to_le_bytes());
        }
    }
    out
}

/// Does LZ4 earn its CPU on candle data? Same 1 MiB payload, f64 candles vs
/// a compressible ramp, default chain vs raw, redb, one put + one get.
fn lz4_f64(c: &mut Criterion) {
    let mut group = c.benchmark_group("lz4_f64");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(4));

    let candles = f64_candles(1024 * 1024 / 48);
    let ramp = payload(candles.len(), 5);
    group.throughput(Throughput::Bytes(candles.len() as u64));

    for (payload_label, data) in [("f64_candles", &candles), ("ramp", &ramp)] {
        for (chain_label, factory) in [
            ("default_lz4", redb_bank as fn() -> NativeBank),
            ("raw", redb_raw_chain as fn() -> NativeBank),
        ] {
            group.bench_function(BenchmarkId::new(chain_label, payload_label), |b| {
                b.iter_with_setup(factory, |native| {
                    let locker = wait(native.bank.lazy_locker::<Vec<u8>>("c")).unwrap();
                    wait(locker.put("k", data)).unwrap();
                    let got = wait(locker.get("k")).unwrap().unwrap();
                    criterion::black_box(got.len());
                    native
                });
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    settings_eager,
    bulk_lazy_put,
    bulk_lazy_get,
    prefix_scan,
    txn_batch,
    reopen,
    index_open,
    envelope_tax,
    backend_put,
    chunk_sweep,
    lz4_f64
);
criterion_main!(benches);
