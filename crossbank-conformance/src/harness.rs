//! Harnesses the suite can run against.
//!
//! Backends that need platform setup (a temp directory, a named IndexedDB
//! database) bring their own harness alongside them in M2 and M3.

use std::sync::Arc;

use crossbank::backend::{Backend, MemoryBackend};
use crossbank::Result;

use crate::{Caps, Harness};

/// Runs the suite against the in-memory backend.
///
/// Each `open` returns a *fresh* store, which is the honest model of a backend
/// that persists nothing — and it is what lets the persistence case assert the
/// negative rather than skipping.
#[derive(Debug)]
pub struct MemoryHarness {
    #[allow(dead_code)]
    case: String,
}

impl MemoryHarness {
    pub fn new(case: &str) -> Self {
        Self {
            case: case.to_string(),
        }
    }
}

impl Harness for MemoryHarness {
    async fn open(&self) -> Result<Arc<dyn Backend>> {
        Ok(Arc::new(MemoryBackend::new()))
    }

    async fn destroy(&self) -> Result<()> {
        // Nothing outlives the handles this harness handed out.
        Ok(())
    }

    fn caps(&self) -> Caps {
        Caps {
            persists_across_open: false,
            reports_usage: true,
        }
    }
}
