//! Storage backends.
//!
//! Gating and re-exports only — no working code lives here.

pub mod api;
pub mod memory;
// `indexeddb` lands in M3, behind this same trait.
#[cfg(not(target_arch = "wasm32"))]
pub mod redb;

pub use api::{
    BFut, Backend, KeyRange, MaybeSend, MaybeSync, Op, ScanPage, ScanRequest, Table, Usage,
};
pub use memory::MemoryBackend;
#[cfg(not(target_arch = "wasm32"))]
pub use redb::RedbBackend;
