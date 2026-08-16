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

/// Runs the suite against the `redb` backend, in a temporary directory.
///
/// Each `open` returns a fresh handle onto the **same file**, which is what
/// makes the persistence case genuinely test persistence rather than assert its
/// absence. redb takes an exclusive file lock, so a case must drop one handle
/// before opening the next — the suite is written that way.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct RedbHarness {
    dir: tempfile::TempDir,
}

#[cfg(not(target_arch = "wasm32"))]
impl RedbHarness {
    pub fn new(case: &str) -> Self {
        let dir = tempfile::Builder::new()
            .prefix(&format!("crossbank-{case}-"))
            .tempdir()
            .expect("could not create a temporary directory");
        Self { dir }
    }

    fn file(&self) -> std::path::PathBuf {
        self.dir.path().join("bank.redb")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Harness for RedbHarness {
    async fn open(&self) -> Result<Arc<dyn Backend>> {
        Ok(Arc::new(crossbank::backend::RedbBackend::open(
            self.file(),
        )?))
    }

    async fn destroy(&self) -> Result<()> {
        // Removing the file is enough; the TempDir cleans itself up on drop,
        // but doing it explicitly means a failing case cannot leak state into
        // the next one even under panic=abort.
        let _ = std::fs::remove_file(self.file());
        Ok(())
    }

    fn caps(&self) -> Caps {
        Caps {
            persists_across_open: true,
            reports_usage: true,
        }
    }
}
