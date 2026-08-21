//! Cross-tab coherence.
//!
//! Gating and re-exports only — the portable half is in [`api`], the two
//! platform halves in `web` and `native`.

pub(crate) mod api;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod web;

pub(crate) use api::{Announcement, Sink};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use native::Coherence;
#[cfg(target_arch = "wasm32")]
pub(crate) use web::{handle, Coherence, SinkHandle};
