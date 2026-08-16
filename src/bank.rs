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
use std::sync::{Arc, Mutex};

use futures::channel::mpsc;

use crate::backend::api::{Backend, Op, ScanRequest, Table};
use crate::backend::KeyRange;
use crate::codec::{default_chain, type_tag, FilterChain};
use crate::error::{Error, Result};

use crate::key::LockerId;
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
    /// A file on disk. Native only; wired up in M2 with the redb backend.
    Path(std::path::PathBuf),
    /// A named IndexedDB database. Web only; wired up in M3.
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
}

impl std::fmt::Debug for Bank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bank").field("chain", &self.chain).finish()
    }
}

impl Bank {
    /// Open a bank over an already-constructed backend.
    ///
    /// The convenience constructors that build a backend from a [`Location`]
    /// arrive with those backends, in M2 and M3.
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
        Locker::open(
            self.backend.clone(),
            self.chain.clone(),
            id,
            name.to_string(),
            config,
        )
        .await
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
        LazyLocker::open(
            self.backend.clone(),
            self.chain.clone(),
            id,
            name.to_string(),
            config,
        )
        .await
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

    fn cached(&self, name: &str) -> Option<LockerId> {
        self.registry.lock().ok()?.get(name).copied()
    }

    fn cache(&self, name: &str, id: LockerId) {
        if let Ok(mut guard) = self.registry.lock() {
            guard.insert(name.to_string(), id);
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
}
