//! Native comparison benches: crossbank memory, crossbank redb, raw redb.
//!
//! Values are `Vec<u8>` so serialisation is not the story. Run with
//! `cargo bench`. Not a CI gate — numbers come from a noisy machine.

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbank::backend::{Backend, MemoryBackend, Op, Table};
use crossbank::codec::FilterChain;
use crossbank::{Bank, BankConfig, Locker, LazyLocker};
use futures::executor::block_on;
use redb::{Database, TableDefinition};
use tempfile::TempDir;

const SETTINGS_N: usize = 200;
const SETTINGS_BYTES: usize = 1024;
const BULK_N: usize = 2_000;
const BULK_BYTES: usize = 256;
const TXN_N: usize = 100;

fn payload(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add(i as u8)).collect()
}

fn key(i: usize) -> String {
    format!("k{i:06}")
}

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

    for (label, factory) in [
        ("memory", memory_bank as fn() -> NativeBank),
        ("redb", redb_bank as fn() -> NativeBank),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            let native = factory();
            let locker = wait(native.bank.locker::<Vec<u8>>("settings")).unwrap();
            fill_eager(&locker, SETTINGS_N, SETTINGS_BYTES);
            let mut i = 0usize;
            b.iter(|| {
                // 90/10 get/put, Hive Box shaped.
                if i % 10 == 0 {
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

    for (label, factory) in [
        ("memory", memory_bank as fn() -> NativeBank),
        ("redb", redb_bank as fn() -> NativeBank),
    ] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_with_setup(factory, |native| {
                let locker = wait(native.bank.lazy_locker::<Vec<u8>>("bulk")).unwrap();
                fill_lazy(&locker, BULK_N, BULK_BYTES);
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

    group.bench_function("redb", |b| {
        b.iter(|| {
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("bank.redb");
            {
                let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
                let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
                wait(locker.put("k", &payload(1024, 1))).unwrap();
            }
            let bank = wait(Bank::open(BankConfig::at(&path))).unwrap();
            let locker = wait(bank.lazy_locker::<Vec<u8>>("l")).unwrap();
            criterion::black_box(wait(locker.get("k")).unwrap());
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

criterion_group!(
    benches,
    settings_eager,
    bulk_lazy_put,
    bulk_lazy_get,
    prefix_scan,
    txn_batch,
    reopen,
    envelope_tax,
    backend_put
);
criterion_main!(benches);
