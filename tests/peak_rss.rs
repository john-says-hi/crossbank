//! Peak RSS while streaming a large value stays O(chunk_size), not O(value).
//!
//! Linux-only. Other hosts compile the test away.

#![cfg(all(not(target_arch = "wasm32"), target_os = "linux"))]

use crossbank::{Bank, BankConfig, LockerConfig};
use futures::executor::block_on;

fn vm_hwm() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[test]
fn streaming_a_large_value_does_not_hold_it_all() {
    let Some(before) = vm_hwm() else {
        return;
    };

    let dir = tempfile::TempDir::new().unwrap();
    let bank = block_on(Bank::open(BankConfig::at(dir.path().join("bank.redb")))).unwrap();
    let chunk = 64 * 1024;
    let locker = block_on(
        bank.lazy_locker_with::<Vec<u8>>("big", LockerConfig::default().with_chunk_size(chunk)),
    )
    .unwrap();

    let total = 8 * 1024 * 1024;
    let mut writer = block_on(locker.writer("k")).unwrap();
    let piece = vec![7u8; chunk];
    let mut written = 0usize;
    while written < total {
        block_on(writer.write_chunk(&piece)).unwrap();
        written += chunk;
    }
    block_on(writer.finish()).unwrap();

    let mut reader = block_on(locker.reader("k")).unwrap().unwrap();
    let mut n = 0u64;
    while let Some(part) = block_on(reader.next_chunk()).unwrap() {
        assert!(part.len() <= chunk, "reader yielded {} bytes", part.len());
        n += part.len() as u64;
    }
    assert_eq!(n, total as u64);

    let after = vm_hwm().unwrap_or(before);
    let growth = after.saturating_sub(before);
    // Allow headroom for redb, the filter chain, and the test process itself.
    // The failure we care about is "grew by ~the whole 8 MiB value many times".
    assert!(
        growth < 32 * 1024 * 1024,
        "peak RSS grew by {growth} bytes streaming {total} bytes in {chunk}-byte chunks"
    );
}
