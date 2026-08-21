//! The bank: the root handle, and the registry that maps locker names to ids.
//!
//! # Why a registry
//!
//! Lockers are a key prefix, not a table (see [`crate::backend::api::Table`]),
//! so each one needs a small numeric id. The mapping from name to id lives in
//! the `meta` table and is stable for the life of the data — renaming is not
//! supported, and an id is never reused, so a stale key can never be read as
//! belonging to a different locker.
//!
//! Ids are handed out by a persisted counter rather than a hash of the name.
//! A hash would collide eventually and, worse, silently: two lockers sharing a
//! prefix would interleave their keys.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::channel::mpsc;

use crate::backend::api::{Backend, Op, ScanRequest, Table};
use crate::backend::KeyRange;
use crate::codec::{default_chain, type_tag, FilterChain};
use crate::error::{Error, Result};

use crate::key::LockerId;
use crate::locker::inner::Inner;
use crate::locker::{LazyLocker, Locker, LockerConfig};
use crate::remote::{Job, JOB_QUEUE};
use serde::{de::DeserializeOwned, Serialize};

/// On-disk format version for the bank as a whole.
///
/// Distinct from the value envelope's version. This one gates the *layout*
/// (which tables exist, how meta keys are spelled); that one gates how a single
/// value is packed.
pub const FORMAT_VERSION: u32 = 1;

const META_FORMAT_VERSION: &[u8] = b"format_version";
const META_NEXT_LOCKER_ID: &[u8] = b"next_locker_id";
const META_LOCKER_PREFIX: &[u8] = b"locker::";
const META_SCHEMA_PREFIX: &[u8] = b"schema::";

fn locker_key(name: &str) -> Vec<u8> {
    let mut k = META_LOCKER_PREFIX.to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

fn schema_key(id: LockerId) -> Vec<u8> {
    let mut k = META_SCHEMA_PREFIX.to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn u32_from(bytes: &[u8], what: &str) -> Result<u32> {
    <[u8; 4]>::try_from(bytes)
        .map(u32::from_be_bytes)
        .map_err(|_| Error::Corrupt(format!("{what} is not a 4-byte integer")))
}

/// Where a bank stores its data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Location {
    /// Nothing is persisted; the data dies with the handle.
    Memory,
    /// A file on disk. Native only.
    Path(std::path::PathBuf),
    /// A named IndexedDB database. Web only.
    Web(String),
}

/// How to open a bank.
///
/// The location is always explicit. crossbank does not guess a platform
/// default, because a library with no knowledge of the calling application has
/// no business choosing where that application's data lives.
#[derive(Debug, Clone)]
pub struct BankConfig {
    pub location: Location,
}

impl BankConfig {
    pub fn memory() -> Self {
        Self {
            location: Location::Memory,
        }
    }

    pub fn at(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            location: Location::Path(path.into()),
        }
    }

    pub fn web(name: impl Into<String>) -> Self {
        Self {
            location: Location::Web(name.into()),
        }
    }
}

/// The root handle. Lockers are opened from it.
pub struct Bank {
    backend: Arc<dyn Backend>,
    chain: Arc<FilterChain>,
    /// Name to id, cached so opening a locker twice does not re-read `meta`.
    registry: Mutex<HashMap<String, LockerId>>,
    /// Cloned into every `RemoteBank`.
    job_sender: mpsc::Sender<Job>,
    /// Taken exactly once, by `into_service`. A second service would silently
    /// steal jobs from the first.
    job_receiver: Mutex<Option<mpsc::Receiver<Job>>>,
    /// Name to the locker handles currently open under it.
    ///
    /// `Weak`, so a locker the application dropped stops counting as open
    /// without needing a destructor to reach back into the bank. Entries are
    /// pruned lazily on every read.
    open_lockers: Mutex<HashMap<String, Weak<Inner>>>,
    closed: AtomicBool,
}

impl std::fmt::Debug for Bank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bank").field("chain", &self.chain).finish()
    }
}

impl Bank {
    /// Ask the platform to keep this bank's data.
    ///
    /// On wasm this is `navigator.storage.persist()`. On native the file is
    /// already durable, so this returns `Ok(true)`. Never called implicitly
    /// on open — precious data on the web must opt in.
    ///
    /// Browsers differ: Chromium decides silently from site-engagement
    /// heuristics, while Firefox shows the user a permission prompt and the
    /// returned future does not resolve until they answer it. Call this off
    /// the startup path and never block a UI on it.
    pub async fn persist(&self) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = self;
            Ok(true)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = self;
            let Some(window) = web_sys::window() else {
                return Ok(false);
            };
            match window.navigator().storage().persist() {
                Ok(promise) => {
                    let value = wasm_bindgen_futures::JsFuture::from(promise)
                        .await
                        .map_err(|e| Error::backend(format!("{e:?}")))?;
                    Ok(value.as_bool().unwrap_or(false))
                }
                Err(_) => Ok(false),
            }
        }
    }

    /// Open a bank at the location named in `config`.
    ///
    /// The location is always explicit. `Location::Path` is native-only;
    /// `Location::Web` is wasm-only. The other combination is
    /// [`Error::InvalidConfig`].
    pub async fn open(config: BankConfig) -> Result<Self> {
        match config.location {
            Location::Memory => {
                Self::with_backend(Arc::new(crate::backend::MemoryBackend::new())).await
            }
            Location::Path(path) => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    Self::with_backend(Arc::new(crate::backend::RedbBackend::open(path)?)).await
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = path;
                    Err(Error::InvalidConfig(
                        "Location::Path is native-only; use Location::Web on wasm".into(),
                    ))
                }
            }
            Location::Web(name) => {
                #[cfg(target_arch = "wasm32")]
                {
                    let backend = crate::backend::IndexedDbBackend::open(name).await?;
                    Self::with_backend(Arc::new(backend)).await
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let _ = name;
                    Err(Error::InvalidConfig(
                        "Location::Web is wasm-only; use Location::Path on native".into(),
                    ))
                }
            }
        }
    }

    /// Open a bank over an already-constructed backend.
    pub async fn with_backend(backend: Arc<dyn Backend>) -> Result<Self> {
        Self::with_backend_and_chain(backend, default_chain()).await
    }

    /// As [`Bank::with_backend`], with a specific filter chain.
    pub async fn with_backend_and_chain(
        backend: Arc<dyn Backend>,
        chain: FilterChain,
    ) -> Result<Self> {
        let (job_sender, job_receiver) = mpsc::channel(JOB_QUEUE);
        let bank = Self {
            backend,
            chain: Arc::new(chain),
            registry: Mutex::new(HashMap::new()),
            job_sender,
            job_receiver: Mutex::new(Some(job_receiver)),
            open_lockers: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        };
        bank.check_or_write_format_version().await?;
        Ok(bank)
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    pub fn chain(&self) -> &Arc<FilterChain> {
        &self.chain
    }

    /// Refuse to touch a store written by a future, incompatible layout.
    async fn check_or_write_format_version(&self) -> Result<()> {
        match self.backend.get(Table::Meta, META_FORMAT_VERSION).await? {
            Some(raw) => {
                let found = u32_from(&raw, "format version")?;
                if found != FORMAT_VERSION {
                    return Err(Error::UnsupportedVersion {
                        // Saturating because the layout version is u32 while the
                        // error reports a u8; a mismatch is fatal either way and
                        // the exact number only has to be legible.
                        found: found.min(u8::MAX as u32) as u8,
                        supported: FORMAT_VERSION as u8,
                    });
                }
                Ok(())
            }
            None => {
                self.backend
                    .commit(vec![Op::Put {
                        table: Table::Meta,
                        key: META_FORMAT_VERSION.to_vec(),
                        value: FORMAT_VERSION.to_be_bytes().to_vec(),
                    }])
                    .await
            }
        }
    }

    /// The id for `name`, allocating one on first use.
    pub async fn locker_id(&self, name: &str) -> Result<LockerId> {
        if let Some(id) = self.cached(name) {
            return Ok(id);
        }

        if let Some(raw) = self.backend.get(Table::Meta, &locker_key(name)).await? {
            let id = u32_from(&raw, "locker id")?;
            self.cache(name, id);
            return Ok(id);
        }

        let next = match self.backend.get(Table::Meta, META_NEXT_LOCKER_ID).await? {
            Some(raw) => u32_from(&raw, "next locker id")?,
            None => 0,
        };
        let id = next;
        let bumped = next
            .checked_add(1)
            .ok_or_else(|| Error::backend("locker id space is exhausted"))?;

        // Registration and the counter bump land together, so a failure cannot
        // leave a name pointing at an id the counter will hand out again.
        self.backend
            .commit(vec![
                Op::Put {
                    table: Table::Meta,
                    key: locker_key(name),
                    value: id.to_be_bytes().to_vec(),
                },
                Op::Put {
                    table: Table::Meta,
                    key: META_NEXT_LOCKER_ID.to_vec(),
                    value: bumped.to_be_bytes().to_vec(),
                },
            ])
            .await?;

        self.cache(name, id);
        Ok(id)
    }

    /// Open an eager locker: values resident, reads synchronous.
    ///
    /// For settings-shaped data — small, hot, read from paths that cannot
    /// await. Fails if the stored contents exceed the configured budget, which
    /// is the guardrail against reaching for this where a lazy locker was
    /// meant.
    pub async fn locker<T>(&self, name: &str) -> Result<Locker<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        self.locker_with(name, LockerConfig::default()).await
    }

    /// As [`Bank::locker`], with explicit limits.
    pub async fn locker_with<T>(&self, name: &str, config: LockerConfig) -> Result<Locker<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        let id = self.prepare::<T>(name).await?;
        let locker = Locker::open(
            self.backend.clone(),
            self.chain.clone(),
            id,
            name.to_string(),
            config,
        )
        .await?;
        self.register_open(name, locker.inner());
        Ok(locker)
    }

    /// Open a lazy locker: key index resident, values fetched on demand.
    ///
    /// For bulk data. Open cost scales with the number of keys, not the size
    /// of the data.
    pub async fn lazy_locker<T>(&self, name: &str) -> Result<LazyLocker<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        self.lazy_locker_with(name, LockerConfig::default()).await
    }

    /// As [`Bank::lazy_locker`], with explicit limits.
    pub async fn lazy_locker_with<T>(
        &self,
        name: &str,
        config: LockerConfig,
    ) -> Result<LazyLocker<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        let id = self.prepare::<T>(name).await?;
        let locker = LazyLocker::open(
            self.backend.clone(),
            self.chain.clone(),
            id,
            name.to_string(),
            config,
        )
        .await?;
        self.register_open(name, locker.inner());
        Ok(locker)
    }

    /// Resolve the id and bind the value type, so reopening a locker under a
    /// different `T` is caught rather than silently mis-decoded.
    async fn prepare<T>(&self, name: &str) -> Result<LockerId> {
        let id = self.locker_id(name).await?;
        self.bind_schema(id, &type_tag::<T>()).await?;
        Ok(id)
    }

    pub(crate) fn job_sender(&self) -> mpsc::Sender<Job> {
        self.job_sender.clone()
    }

    pub(crate) fn take_job_receiver(&self) -> Option<mpsc::Receiver<Job>> {
        self.job_receiver.lock().ok()?.take()
    }

    /// Bytes-level access, used by the remote handle.
    ///
    /// Values go through the same envelope and filter chain as typed ones, and
    /// bind the same schema tag, so a locker reached this way is precisely a
    /// `Locker<Vec<u8>>` — the two views cannot disagree.
    async fn raw_locker(&self, name: &str) -> Result<LockerId> {
        self.prepare::<Vec<u8>>(name).await
    }

    pub(crate) async fn raw_get(&self, locker: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let id = self.raw_locker(locker).await?;
        let encoded = crate::key::encode(id, key);
        match self.backend.get(Table::Records, &encoded).await? {
            Some(stored) => Ok(Some(crate::codec::decode(&stored, &self.chain)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn raw_put(&self, locker: &str, key: &str, value: Vec<u8>) -> Result<()> {
        let id = self.raw_locker(locker).await?;
        let sealed = crate::codec::encode(&value, &self.chain)?;
        self.backend
            .commit(vec![Op::Put {
                table: Table::Records,
                key: crate::key::encode(id, key),
                value: sealed,
            }])
            .await
    }

    pub(crate) async fn raw_delete(&self, locker: &str, key: &str) -> Result<()> {
        let id = self.raw_locker(locker).await?;
        self.backend
            .commit(vec![Op::Delete {
                table: Table::Records,
                key: crate::key::encode(id, key),
            }])
            .await
    }

    pub(crate) async fn raw_clear(&self, locker: &str) -> Result<()> {
        let id = self.raw_locker(locker).await?;
        self.backend
            .commit(vec![Op::DeleteRange {
                table: Table::Records,
                range: crate::key::locker_range(id),
            }])
            .await
    }

    pub(crate) async fn raw_keys(&self, locker: &str, prefix: &str) -> Result<Vec<String>> {
        let id = self.raw_locker(locker).await?;
        let mut range = crate::key::prefix_range(id, prefix);
        let mut keys = Vec::new();

        loop {
            let page = self
                .backend
                .scan(ScanRequest {
                    table: Table::Records,
                    range: range.clone(),
                    reverse: false,
                    limit: 256,
                    want_values: false,
                })
                .await?;

            for (encoded, _) in &page.items {
                keys.push(crate::key::decode(id, encoded)?.to_string());
            }

            match page.resume {
                Some(last) => range.start = std::ops::Bound::Excluded(last),
                None => break,
            }
        }

        Ok(keys)
    }

    /// Record, or verify, the schema tag a locker was written with.
    ///
    /// postcard is not self-describing, so without this an application that
    /// changes a locker's value type silently decodes old bytes into the new
    /// shape. First write wins; every later open must match.
    pub async fn bind_schema(&self, id: LockerId, tag: &str) -> Result<()> {
        let key = schema_key(id);
        match self.backend.get(Table::Meta, &key).await? {
            Some(raw) => {
                let stored = String::from_utf8(raw)
                    .map_err(|e| Error::Corrupt(format!("schema tag is not UTF-8: {e}")))?;
                if stored != tag {
                    return Err(Error::SchemaMismatch {
                        stored,
                        requested: tag.to_string(),
                    });
                }
                Ok(())
            }
            None => {
                self.backend
                    .commit(vec![Op::Put {
                        table: Table::Meta,
                        key,
                        value: tag.as_bytes().to_vec(),
                    }])
                    .await
            }
        }
    }

    /// Every registered locker name, in registration-independent byte order.
    pub async fn locker_names(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        let mut range = KeyRange::prefix(META_LOCKER_PREFIX);

        loop {
            let page = self
                .backend
                .scan(ScanRequest {
                    table: Table::Meta,
                    range: range.clone(),
                    reverse: false,
                    limit: 256,
                    want_values: false,
                })
                .await?;

            for (key, _) in &page.items {
                let raw = &key[META_LOCKER_PREFIX.len()..];
                names.push(
                    String::from_utf8(raw.to_vec())
                        .map_err(|e| Error::Corrupt(format!("locker name is not UTF-8: {e}")))?,
                );
            }

            match page.resume {
                Some(last) => range.start = std::ops::Bound::Excluded(last),
                None => break,
            }
        }

        Ok(names)
    }

    /// Build a throwaway `Inner` over an existing locker id.
    ///
    /// Only for bank-level maintenance that needs the record/chunk walk the
    /// lockers already own. It has its own watchers, so nothing it does is
    /// announced to a live locker's subscribers — deleting a locker out from
    /// under an open handle is refused instead.
    fn maintenance_inner(&self, id: LockerId, name: &str) -> Arc<Inner> {
        Arc::new(Inner {
            write_lock: futures::lock::Mutex::new(()),
            backend: self.backend.clone(),
            chain: self.chain.clone(),
            id,
            name: name.to_string(),
            config: LockerConfig::default(),
            watchers: Default::default(),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Whether a locker has ever been registered under `name`.
    ///
    /// Registration, not content: a locker that was opened and never written
    /// to still exists. Hive's `boxExists`.
    pub async fn locker_exists(&self, name: &str) -> Result<bool> {
        if self.cached(name).is_some() {
            return Ok(true);
        }
        Ok(self
            .backend
            .get(Table::Meta, &locker_key(name))
            .await?
            .is_some())
    }

    /// Bytes stored for one locker: its records plus the chunks they point at.
    ///
    /// Measured by scanning, so the cost is proportional to the locker. It is
    /// the size of the *stored* — sealed, and compressed if the chain
    /// compresses — payloads, not of the values a caller put in, and it
    /// excludes key bytes and whatever the backend spends on its own
    /// bookkeeping. An unknown name is 0 rather than an error.
    pub async fn locker_bytes(&self, name: &str) -> Result<u64> {
        if !self.locker_exists(name).await? {
            return Ok(0);
        }
        let id = self.locker_id(name).await?;
        let inner = self.maintenance_inner(id, name);

        let mut total: u64 = 0;
        let mut chunked: Vec<u64> = Vec::new();
        inner
            .walk(
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                false,
                true,
                |_, value| {
                    if let Some(raw) = value {
                        total = total.saturating_add(raw.len() as u64);
                        if crate::locker::chunk::is_pointer(&raw) {
                            chunked.push(crate::locker::chunk::ChunkPointer::parse(&raw)?.value_id);
                        }
                    }
                    Ok(())
                },
            )
            .await?;

        for value_id in chunked {
            total = total.saturating_add(self.chunk_bytes(value_id).await?);
        }
        Ok(total)
    }

    /// Sum the stored chunk payloads for one chunked value.
    async fn chunk_bytes(&self, value_id: u64) -> Result<u64> {
        let mut range = crate::locker::chunk::chunk_range(value_id);
        let mut total: u64 = 0;
        loop {
            let page = self
                .backend
                .scan(ScanRequest {
                    table: Table::Chunks,
                    range: range.clone(),
                    reverse: false,
                    limit: 256,
                    want_values: true,
                })
                .await?;
            for (_, value) in &page.items {
                if let Some(bytes) = value {
                    total = total.saturating_add(bytes.len() as u64);
                }
            }
            match page.resume {
                Some(last) => range.start = std::ops::Bound::Excluded(last),
                None => break,
            }
        }
        Ok(total)
    }

    /// Delete a locker and everything it stores, permanently.
    ///
    /// Returns `false` if no locker was ever registered under `name`, so
    /// deleting a name twice is not an error. Hive's `deleteBoxFromDisk`.
    ///
    /// Refuses with [`Error::InvalidConfig`] while a handle to the locker is
    /// open. An open eager locker holds its values in RAM and an open lazy
    /// locker holds its key index, so deleting underneath either would leave a
    /// handle confidently serving data that no longer exists. Close it first.
    ///
    /// Records, the chunks they point at, the name registration and the schema
    /// tag all go in **one** commit, so a failure leaves the locker whole
    /// rather than half-erased.
    ///
    /// The locker's id is *not* recycled — `next_locker_id` only ever moves
    /// forward. Recreating the same name allocates a fresh id, which is what
    /// stops any record the delete somehow missed from being read as part of
    /// the new locker.
    pub async fn delete_locker(&self, name: &str) -> Result<bool> {
        if self.is_locker_open(name) {
            return Err(Error::InvalidConfig(format!(
                "locker {name:?} is still open; close it before deleting it"
            )));
        }
        if !self.locker_exists(name).await? {
            return Ok(false);
        }

        let id = self.locker_id(name).await?;
        let inner = self.maintenance_inner(id, name);

        // The chunk GC ops and the record range deletion, exactly as `clear`
        // builds them, plus the two meta keys.
        let mut ops = inner.clear_value_ops().await?;
        ops.push(Op::Delete {
            table: Table::Meta,
            key: locker_key(name),
        });
        ops.push(Op::Delete {
            table: Table::Meta,
            key: schema_key(id),
        });
        self.backend.commit(ops).await?;

        if let Ok(mut guard) = self.registry.lock() {
            guard.remove(name);
        }
        if let Ok(mut guard) = self.open_lockers.lock() {
            guard.remove(name);
        }
        Ok(true)
    }

    /// Close the bank: release the backend handle.
    ///
    /// Idempotent. Every later operation — through this bank or through any
    /// locker still holding the same backend — reports [`Error::Closed`].
    ///
    /// This exists for close-then-reopen in a single process. `redb` holds an
    /// exclusive file lock for as long as its `Database` is alive, and the
    /// backend is shared through an `Arc`, so dropping the `Bank` is not
    /// enough to let the same file be opened again. Test suites reopen
    /// constantly; production code rarely closes at all.
    ///
    /// Lockers opened from this bank are *not* individually closed — they keep
    /// whatever they already hold in RAM, and their writes start failing.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.backend.close().await
    }

    /// Whether [`Bank::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn register_open(&self, name: &str, inner: &Arc<Inner>) {
        if let Ok(mut guard) = self.open_lockers.lock() {
            guard.insert(name.to_string(), Arc::downgrade(inner));
        }
    }

    /// Whether a locker is currently open under `name`.
    ///
    /// A locker counts as open while a handle to it is alive and has not had
    /// `close()` called on it. Dropped handles stop counting on the next read
    /// of this registry.
    ///
    /// Note that opening the same name twice is **allowed** and returns two
    /// independent handles over the same stored data — the eager form gives
    /// each its own resident copy, which will diverge on write. The registry
    /// records only the most recent handle per name.
    pub fn is_locker_open(&self, name: &str) -> bool {
        let Ok(mut guard) = self.open_lockers.lock() else {
            return false;
        };
        Self::prune(&mut guard);
        guard.contains_key(name)
    }

    /// Every locker name with a live, unclosed handle, in byte order.
    pub fn open_locker_names(&self) -> Vec<String> {
        let Ok(mut guard) = self.open_lockers.lock() else {
            return Vec::new();
        };
        Self::prune(&mut guard);
        let mut names: Vec<String> = guard.keys().cloned().collect();
        names.sort();
        names
    }

    /// Drop entries whose handle is gone or closed.
    fn prune(map: &mut HashMap<String, Weak<Inner>>) {
        map.retain(|_, weak| match weak.upgrade() {
            Some(inner) => !inner.is_closed(),
            None => false,
        });
    }

    fn cached(&self, name: &str) -> Option<LockerId> {
        self.registry.lock().ok()?.get(name).copied()
    }

    fn cache(&self, name: &str, id: LockerId) {
        if let Ok(mut guard) = self.registry.lock() {
            guard.insert(name.to_string(), id);
        }
    }
}

/// Delete a whole bank from the platform it lives on.
///
/// Idempotent: deleting a location that holds nothing succeeds. Every handle
/// onto that location must be closed first — natively `redb` still holds an
/// exclusive lock on an open file, and on the web an open IndexedDB connection
/// blocks the delete until it goes away.
///
/// A free function rather than a method, because a `Bank` you are about to
/// erase is not a good place to hang the erasing from.
pub async fn delete_bank(config: &BankConfig) -> Result<()> {
    match &config.location {
        // Nothing outlives the handles; there is no location to erase.
        Location::Memory => Ok(()),
        Location::Path(path) => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                match std::fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(Error::backend(format!(
                        "deleting the bank file {}: {e}",
                        path.display()
                    ))),
                }
            }
            #[cfg(target_arch = "wasm32")]
            {
                let _ = path;
                Err(Error::InvalidConfig(
                    "Location::Path is native-only; use Location::Web on wasm".into(),
                ))
            }
        }
        Location::Web(name) => {
            #[cfg(target_arch = "wasm32")]
            {
                crate::backend::IndexedDbBackend::delete_database(name).await
            }
            #[cfg(not(target_arch = "wasm32"))]
            {
                let _ = name;
                Err(Error::InvalidConfig(
                    "Location::Web is wasm-only; use Location::Path on native".into(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use futures::executor::block_on;

    fn bank() -> Bank {
        block_on(Bank::with_backend(Arc::new(MemoryBackend::new()))).unwrap()
    }

    #[test]
    fn ids_are_stable_across_lookups() {
        let b = bank();
        let first = block_on(b.locker_id("settings")).unwrap();
        let again = block_on(b.locker_id("settings")).unwrap();
        assert_eq!(first, again);
    }

    #[test]
    fn distinct_names_get_distinct_ids() {
        let b = bank();
        let a = block_on(b.locker_id("a")).unwrap();
        let c = block_on(b.locker_id("b")).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn ids_survive_reopening_over_the_same_backend() {
        // The registry cache must not be the source of truth, or a second
        // handle would allocate fresh ids over existing data.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        let first = block_on(Bank::with_backend(backend.clone())).unwrap();
        let settings = block_on(first.locker_id("settings")).unwrap();
        let candles = block_on(first.locker_id("candles")).unwrap();
        drop(first);

        let second = block_on(Bank::with_backend(backend)).unwrap();
        assert_eq!(block_on(second.locker_id("settings")).unwrap(), settings);
        assert_eq!(block_on(second.locker_id("candles")).unwrap(), candles);
    }

    #[test]
    fn a_fresh_name_after_reopen_does_not_reuse_an_id() {
        // Reusing an id would make a stale key readable as a different
        // locker's, which is the failure a persisted counter prevents.
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        let first = block_on(Bank::with_backend(backend.clone())).unwrap();
        let a = block_on(first.locker_id("a")).unwrap();
        let b_id = block_on(first.locker_id("b")).unwrap();
        drop(first);

        let second = block_on(Bank::with_backend(backend)).unwrap();
        let c = block_on(second.locker_id("c")).unwrap();
        assert_ne!(c, a);
        assert_ne!(c, b_id);
    }

    #[test]
    fn locker_names_lists_every_registration() {
        let b = bank();
        for name in ["ui_settings", "candle_cache", "reports"] {
            block_on(b.locker_id(name)).unwrap();
        }

        let mut names = block_on(b.locker_names()).unwrap();
        names.sort();
        assert_eq!(names, vec!["candle_cache", "reports", "ui_settings"]);
    }

    #[test]
    fn locker_names_pages_past_one_scan_page() {
        // The registry scan pages at 256; prove the loop actually continues.
        let b = bank();
        for i in 0..300 {
            block_on(b.locker_id(&format!("locker_{i:04}"))).unwrap();
        }
        assert_eq!(block_on(b.locker_names()).unwrap().len(), 300);
    }

    #[test]
    fn a_locker_name_may_contain_awkward_bytes() {
        let b = bank();
        let weird = "name with spaces :: and 🍎";
        block_on(b.locker_id(weird)).unwrap();
        assert_eq!(block_on(b.locker_names()).unwrap(), vec![weird]);
    }

    #[test]
    fn binding_the_same_schema_twice_is_fine() {
        let b = bank();
        let id = block_on(b.locker_id("x")).unwrap();
        block_on(b.bind_schema(id, "Settings")).unwrap();
        block_on(b.bind_schema(id, "Settings")).unwrap();
    }

    #[test]
    fn rebinding_a_different_schema_is_refused() {
        // The guard against opening Locker<A> over data written as Locker<B>.
        let b = bank();
        let id = block_on(b.locker_id("x")).unwrap();
        block_on(b.bind_schema(id, "Settings")).unwrap();

        match block_on(b.bind_schema(id, "ReportMeta")) {
            Err(Error::SchemaMismatch { stored, requested }) => {
                assert_eq!(stored, "Settings");
                assert_eq!(requested, "ReportMeta");
            }
            other => panic!("expected a schema mismatch, got {other:?}"),
        }
    }

    #[test]
    fn schemas_are_per_locker_not_global() {
        let b = bank();
        let one = block_on(b.locker_id("one")).unwrap();
        let two = block_on(b.locker_id("two")).unwrap();
        block_on(b.bind_schema(one, "Settings")).unwrap();
        block_on(b.bind_schema(two, "ReportMeta")).unwrap();
    }

    #[test]
    fn lockers_open_from_the_bank_and_persist() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        let first = block_on(Bank::with_backend(backend.clone())).unwrap();
        let settings = block_on(first.locker::<String>("ui_settings")).unwrap();
        block_on(settings.put("theme", "dark".into())).unwrap();

        let candles = block_on(first.lazy_locker::<String>("candle_cache")).unwrap();
        block_on(candles.put("BTCUSDT::1", &"ohlc".to_string())).unwrap();
        drop(first);

        let second = block_on(Bank::with_backend(backend)).unwrap();
        let settings = block_on(second.locker::<String>("ui_settings")).unwrap();
        assert_eq!(settings.get("theme").as_deref(), Some(&"dark".to_string()));

        let candles = block_on(second.lazy_locker::<String>("candle_cache")).unwrap();
        assert_eq!(
            block_on(candles.get("BTCUSDT::1")).unwrap(),
            Some("ohlc".into())
        );
    }

    #[test]
    fn two_lockers_do_not_see_each_other() {
        let b = bank();
        let one = block_on(b.locker::<String>("one")).unwrap();
        let two = block_on(b.locker::<String>("two")).unwrap();

        block_on(one.put("k", "from one".into())).unwrap();
        block_on(two.put("k", "from two".into())).unwrap();

        assert_eq!(one.get("k").as_deref(), Some(&"from one".to_string()));
        assert_eq!(two.get("k").as_deref(), Some(&"from two".to_string()));
    }

    #[test]
    fn reopening_a_locker_as_a_different_type_is_refused() {
        // The schema guard, exercised end to end through the public API.
        let b = bank();
        block_on(b.locker::<String>("x")).unwrap();

        match block_on(b.locker::<u64>("x")) {
            Err(Error::SchemaMismatch { .. }) => {}
            other => panic!("expected a schema mismatch, got {:?}", other.map(|_| "ok")),
        }
    }

    #[test]
    fn a_future_format_version_is_refused() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        block_on(backend.commit(vec![Op::Put {
            table: Table::Meta,
            key: META_FORMAT_VERSION.to_vec(),
            value: (FORMAT_VERSION + 1).to_be_bytes().to_vec(),
        }]))
        .unwrap();

        assert!(matches!(
            block_on(Bank::with_backend(backend)),
            Err(Error::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn open_memory_round_trips() {
        let bank = block_on(Bank::open(BankConfig::memory())).unwrap();
        let locker = block_on(bank.locker::<String>("ui_settings")).unwrap();
        block_on(locker.put("theme", "dark".into())).unwrap();
        assert_eq!(locker.get("theme").as_deref(), Some(&"dark".to_string()));
    }

    #[test]
    fn open_web_is_refused_on_native() {
        match block_on(Bank::open(BankConfig::web("should-not-open"))) {
            Err(Error::InvalidConfig(msg)) => {
                assert!(msg.contains("wasm-only"), "{msg}");
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn open_path_persists_across_handles() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bank.redb");

        {
            let bank = block_on(Bank::open(BankConfig::at(&path))).unwrap();
            let locker = block_on(bank.locker::<String>("ui_settings")).unwrap();
            block_on(locker.put("theme", "dark".into())).unwrap();
        }

        let bank = block_on(Bank::open(BankConfig::at(&path))).unwrap();
        let locker = block_on(bank.locker::<String>("ui_settings")).unwrap();
        assert_eq!(locker.get("theme").as_deref(), Some(&"dark".to_string()));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn delete_bank_removes_the_file_and_tolerates_a_missing_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bank.redb");
        let config = BankConfig::at(&path);

        let bank = block_on(Bank::open(config.clone())).unwrap();
        let locker = block_on(bank.locker::<String>("s")).unwrap();
        block_on(locker.put("k", "v".into())).unwrap();
        // redb still holds the file open until the bank is closed.
        block_on(bank.close()).unwrap();

        block_on(delete_bank(&config)).unwrap();
        assert!(!path.exists());

        // Idempotent: a location holding nothing is not an error.
        block_on(delete_bank(&config)).unwrap();

        let reborn = block_on(Bank::open(config)).unwrap();
        let locker = block_on(reborn.locker::<String>("s")).unwrap();
        assert_eq!(locker.get("k"), None, "the data went with the file");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn delete_bank_refuses_a_web_location_on_native() {
        assert!(matches!(
            block_on(delete_bank(&BankConfig::web("nope"))),
            Err(Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn deleting_a_memory_bank_is_a_no_op() {
        block_on(delete_bank(&BankConfig::memory())).unwrap();
    }
}
