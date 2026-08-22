//! Shared workload shapes for the crossbank benches.
//!
//! Included by BOTH `benches/kv.rs` (native, Criterion) and
//! `tests/bench_web.rs` (wasm, IndexedDB) via `#[path]`, so the two lanes
//! measure literally the same numbers of operations over the same bytes.
//! It is the Rust twin of `bench/hive_ce/lib/workloads.dart` — every constant
//! here has a same-named constant there, and they must move together or the
//! Hive-vs-crossbank tables stop being a comparison.
//!
//! It is not a `mod` of the crate and must stay dependency-free (no `crossbank`
//! imports), so both lanes can include it without pulling anything in.
#![allow(dead_code)]

/// Large shapes — `benches/kv.rs`, `bench/hive_ce/bin`, `bench/hive_ce/web`.
pub const SETTINGS_N: usize = 200;
pub const SETTINGS_BYTES: usize = 1024;
/// Operations in ONE timed `settings_eager` iteration (90/10 get/put).
pub const SETTINGS_OPS: usize = 1_000;
pub const BULK_N: usize = 2_000;
pub const BULK_BYTES: usize = 256;
/// Random gets in ONE timed `bulk_lazy_get` iteration.
pub const BULK_GET_OPS: usize = 1_000;
pub const TXN_N: usize = 100;
pub const BIG_BYTES: usize = 8 * 1024 * 1024;

/// Small shapes — the pre-Phase-5 `tests/bench_web.rs` shapes, kept so the
/// `*_web_small` rows stay comparable with the 2026-08-21 pre-Phase-3 run.
pub const SMALL_SETTINGS_N: usize = 50;
pub const SMALL_SETTINGS_OPS: usize = 200;
pub const SMALL_BULK_N: usize = 200;

/// Timed iterations per workload in the sampling lanes (`bench/hive_ce` and
/// `tests/bench_web.rs`). Criterion picks its own; this is for the lanes that
/// compute their own median/p99.
pub const ITERATIONS: usize = 20;

pub fn payload(n: usize, seed: u8) -> Vec<u8> {
    (0..n).map(|i| seed.wrapping_add(i as u8)).collect()
}

/// 6-digit keys — `key()` in `workloads.dart`.
pub fn key(i: usize) -> String {
    format!("k{i:06}")
}

/// 4-digit keys, for the `_web_small` rows — `smallKey()` in `workloads.dart`.
pub fn small_key(i: usize) -> String {
    format!("k{i:04}")
}

/// The stride the `bulk_lazy_get` workloads walk the key space with, so the
/// reads are scattered rather than sequential. Mirrors `web/main.dart`.
pub const GET_STRIDE: usize = 7919;
