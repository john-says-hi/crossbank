//! Storage backends.
//!
//! Gating and re-exports only — no working code lives here.

pub mod api;
pub mod memory;
// `redb` lands in M2, `indexeddb` in M3. Both behind the same trait.

pub use api::{
    BFut, Backend, KeyRange, MaybeSend, MaybeSync, Op, ScanPage, ScanRequest, Table, Usage,
};
pub use memory::MemoryBackend;
