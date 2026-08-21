//! The wasm backend, on IndexedDB via [`indexed_db`] 0.4.2.
//!
//! This is the platform that actually ships. Every method is exactly one
//! `transaction(...).run(...)` that awaits only IDB requests. A stray await
//! panics (`Transaction blocked without any request under way`) and wasm
//! release builds are `panic = "abort"`, so that panic is an unrecoverable
//! tab kill.
//!
//! Keys and values go through [`js_sys::Uint8Array::from`], never `::view`.
//! `view` aliases wasm memory, which is a `SharedArrayBuffer` on the atomics
//! lane, and IndexedDB throws `DataCloneError` only on the build that ships.

use std::cell::RefCell;
use std::convert::Infallible;
use std::ops::{Bound, RangeBounds};
use std::rc::Rc;

use indexed_db::{CursorDirection, Database, Factory};
use js_sys::Uint8Array;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;

use super::api::{BFut, Backend, KeyRange, Op, ScanPage, ScanRequest, Table, Usage};
use crate::error::{Error, Result};

const VERSION: u32 = 1;
const STORES: [&str; 3] = ["meta", "records", "chunks"];

/// A bank stored in a named IndexedDB database.
#[derive(Debug)]
pub struct IndexedDbBackend {
    /// `None` once closed.
    ///
    /// Behind an `Rc` so an in-flight operation can hold the connection across
    /// its awaits without keeping the `RefCell` borrowed — a borrow held over
    /// an await would make [`Backend::close`] a panic waiting to happen.
    db: RefCell<Option<Rc<Database<Infallible>>>>,
    name: String,
}

impl IndexedDbBackend {
    /// Open, or create, the named database at version 1.
    ///
    /// Version stays at 1 forever. Creating an object store requires a
    /// `versionchange` transaction, which force-closes every other connection,
    /// so we create the three fixed tables once and never bump again.
    pub async fn open(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let factory = Factory::<Infallible>::get().map_err(map_err)?;
        let db = factory
            .open(&name, VERSION, async move |evt| {
                let database = evt.database();
                for store in STORES {
                    database.build_object_store(store).create()?;
                }
                Ok(())
            })
            .await
            .map_err(map_err)?;
        Ok(Self {
            db: RefCell::new(Some(Rc::new(db))),
            name,
        })
    }

    /// The open connection, or [`Error::Closed`].
    fn db(&self) -> Result<Rc<Database<Infallible>>> {
        self.db.borrow().as_ref().cloned().ok_or(Error::Closed)
    }

    /// The IndexedDB database name this handle is connected to.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Delete a database. Used by the conformance harness so one case cannot
    /// leak state into the next. Deleting a name that does not exist succeeds.
    pub async fn delete_database(name: &str) -> Result<()> {
        let factory = Factory::<Infallible>::get().map_err(map_err)?;
        factory.delete_database(name).await.map_err(map_err)
    }
}

impl Drop for IndexedDbBackend {
    fn drop(&mut self) {
        // Close so a subsequent `delete_database` is not blocked waiting for
        // this connection. Close itself is asynchronous with no completion
        // signal; the harness still has to tolerate that. A backend already
        // closed explicitly has nothing left to do here.
        if let Some(db) = self.db.borrow_mut().take() {
            db.close();
        }
    }
}

fn js_bytes(bytes: &[u8]) -> JsValue {
    Uint8Array::from(bytes).into()
}

fn from_js_bytes(value: &JsValue) -> Result<Vec<u8>> {
    Ok(Uint8Array::new(value).to_vec())
}

fn map_err<E: std::fmt::Display>(err: indexed_db::Error<E>) -> Error {
    let message = err.to_string();
    if message.contains("QuotaExceeded") {
        return Error::QuotaExceeded {
            needed: 0,
            available: None,
        };
    }
    Error::Backend(message)
}

/// Owned JS bounds so `RangeBounds<JsValue>` can borrow them for one call.
struct JsBounds {
    start: Bound<JsValue>,
    end: Bound<JsValue>,
}

impl RangeBounds<JsValue> for JsBounds {
    fn start_bound(&self) -> Bound<&JsValue> {
        match &self.start {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
        }
    }

    fn end_bound(&self) -> Bound<&JsValue> {
        match &self.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(v) => Bound::Included(v),
            Bound::Excluded(v) => Bound::Excluded(v),
        }
    }
}

fn to_js_bounds(range: &KeyRange) -> JsBounds {
    fn map(bound: &Bound<Vec<u8>>) -> Bound<JsValue> {
        match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(k) => Bound::Included(js_bytes(k)),
            Bound::Excluded(k) => Bound::Excluded(js_bytes(k)),
        }
    }
    JsBounds {
        start: map(&range.start),
        end: map(&range.end),
    }
}

fn range_is_all(range: &KeyRange) -> bool {
    matches!(range.start, Bound::Unbounded) && matches!(range.end, Bound::Unbounded)
}

impl Backend for IndexedDbBackend {
    fn get<'a>(&'a self, table: Table, key: &'a [u8]) -> BFut<'a, Option<Vec<u8>>> {
        let store_name = table.name();
        let key = js_bytes(key);
        Box::pin(async move {
            let db = self.db()?;
            let got = db
                .transaction(&[store_name])
                .run(async move |t| {
                    let store = t.object_store(store_name)?;
                    store.get(&key).await
                })
                .await
                .map_err(map_err)?;
            match got {
                Some(value) => Ok(Some(from_js_bytes(&value)?)),
                None => Ok(None),
            }
        })
    }

    fn get_many<'a>(&'a self, table: Table, keys: Vec<Vec<u8>>) -> BFut<'a, Vec<Option<Vec<u8>>>> {
        let store_name = table.name();
        Box::pin(async move {
            let js_keys: Vec<JsValue> = keys.iter().map(|k| js_bytes(k)).collect();
            let db = self.db()?;
            db.transaction(&[store_name])
                .run(async move |t| {
                    let store = t.object_store(store_name)?;
                    let mut out = Vec::with_capacity(js_keys.len());
                    for key in &js_keys {
                        match store.get(key).await? {
                            Some(value) => out.push(Some(Uint8Array::new(&value).to_vec())),
                            None => out.push(None),
                        }
                    }
                    Ok(out)
                })
                .await
                .map_err(map_err)
        })
    }

    fn scan(&self, request: ScanRequest) -> BFut<'_, ScanPage> {
        let store_name = request.table.name();
        Box::pin(async move {
            let bounds = to_js_bounds(&request.range);
            let unbounded = range_is_all(&request.range);
            let reverse = request.reverse;
            let limit = request.limit;
            let want_values = request.want_values;
            let direction = if reverse {
                CursorDirection::Prev
            } else {
                CursorDirection::Next
            };

            let db = self.db()?;
            db.transaction(&[store_name])
                .run(async move |t| {
                    let store = t.object_store(store_name)?;
                    let mut builder = store.cursor().direction(direction);
                    if !unbounded {
                        builder = builder.range(bounds)?;
                    }
                    let mut cursor = if want_values {
                        builder.open().await?
                    } else {
                        builder.open_key().await?
                    };

                    let mut items = Vec::new();
                    let mut resume = None;

                    loop {
                        let Some(key_js) = cursor.key() else {
                            break;
                        };
                        let key = Uint8Array::new(&key_js).to_vec();
                        let value = if want_values {
                            cursor.value().map(|v| Uint8Array::new(&v).to_vec())
                        } else {
                            None
                        };
                        items.push((key, value));

                        if items.len() >= limit {
                            // Peek one past the page. A filled page with nothing
                            // beyond must still report `resume = None`.
                            match cursor.advance(1).await {
                                Ok(()) if cursor.key().is_some() => {
                                    resume = items.last().map(|(k, _)| k.clone());
                                }
                                Ok(()) => resume = None,
                                Err(indexed_db::Error::CursorCompleted) => resume = None,
                                Err(e) => return Err(e),
                            }
                            break;
                        }

                        match cursor.advance(1).await {
                            Ok(()) => {}
                            Err(indexed_db::Error::CursorCompleted) => break,
                            Err(e) => return Err(e),
                        }
                    }

                    Ok(ScanPage { items, resume })
                })
                .await
                .map_err(map_err)
        })
    }

    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()> {
        Box::pin(async move {
            let db = self.db()?;
            db.transaction(&STORES)
                .rw()
                .run(async move |t| {
                    for op in ops {
                        match op {
                            Op::Put { table, key, value } => {
                                let store = t.object_store(table.name())?;
                                store.put_kv(&js_bytes(&key), &js_bytes(&value)).await?;
                            }
                            Op::Delete { table, key } => {
                                let store = t.object_store(table.name())?;
                                store.delete(&js_bytes(&key)).await?;
                            }
                            Op::DeleteRange { table, range } => {
                                let store = t.object_store(table.name())?;
                                if range_is_all(&range) {
                                    store.clear().await?;
                                } else {
                                    let bounds = to_js_bounds(&range);
                                    store.delete_range(bounds).await?;
                                }
                            }
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(map_err)
        })
    }

    fn usage(&self) -> BFut<'_, Option<Usage>> {
        // `navigator.storage.estimate()` is not an IDB request and must not
        // run inside an IndexedDB transaction.
        Box::pin(async move {
            let Some(window) = web_sys::window() else {
                return Ok(None);
            };
            let storage = window.navigator().storage();

            let estimate = match storage.estimate() {
                Ok(promise) => wasm_bindgen_futures::JsFuture::from(promise).await.ok(),
                Err(_) => None,
            };
            // Read the fields off the resolved object with `Reflect` rather
            // than casting to `web_sys::StorageEstimate`. That type is a
            // WebIDL *dictionary*: it has no JS constructor, so the
            // `instanceof` check `dyn_into` performs can never succeed and
            // the cast fails on every browser — which made this method report
            // `None` everywhere until a browser test caught it.
            let Some(estimate) = estimate else {
                return Ok(None);
            };
            let number = |field: &str| -> Option<f64> {
                js_sys::Reflect::get(&estimate, &JsValue::from_str(field))
                    .ok()
                    .and_then(|v| v.as_f64())
                    .filter(|n| n.is_finite())
            };

            let used = number("usage").unwrap_or(0.0).max(0.0) as u64;
            let available = number("quota").map(|q| q.max(0.0) as u64);

            let persisted = match storage.persisted() {
                Ok(promise) => wasm_bindgen_futures::JsFuture::from(promise)
                    .await
                    .ok()
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                Err(_) => false,
            };

            Ok(Some(Usage {
                used,
                available,
                persisted,
            }))
        })
    }

    fn close(&self) -> BFut<'_, ()> {
        Box::pin(async move {
            // Closing the connection is what lets a later `delete_database`
            // or a reopen proceed without being blocked. Idempotent: a second
            // close finds `None`. IndexedDB's close has no completion signal,
            // so there is nothing to await.
            let taken = self.db.borrow_mut().take();
            if let Some(db) = taken {
                db.close();
            }
            Ok(())
        })
    }

    fn flush(&self) -> BFut<'_, ()> {
        // A completed IndexedDB transaction has already committed. There is
        // no extra durability lever to pull.
        Box::pin(async move { Ok(()) })
    }
}
