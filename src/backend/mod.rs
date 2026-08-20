//! Storage backends.
//!
//! Gating and re-exports only — no working code lives here.

pub mod api;
#[cfg(target_arch = "wasm32")]
pub mod indexeddb;
pub mod memory;
#[cfg(not(target_arch = "wasm32"))]
pub mod redb;

pub use api::{
    BFut, Backend, KeyRange, MaybeSend, MaybeSync, Op, ScanPage, ScanRequest, Table, Usage,
};
#[cfg(target_arch = "wasm32")]
pub use indexeddb::IndexedDbBackend;
pub use memory::MemoryBackend;
#[cfg(not(target_arch = "wasm32"))]
pub use redb::RedbBackend;
