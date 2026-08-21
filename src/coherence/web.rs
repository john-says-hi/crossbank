//! Cross-tab coherence on the web: one `BroadcastChannel` per bank.
//!
//! # Rules this file follows
//!
//! * The message callback is a plain `Closure`. It touches RAM and raises
//!   events; it never awaits and never runs inside an IndexedDB transaction.
//! * `Uint8Array::from`, never `::view`. A view aliases wasm memory, which is
//!   a `SharedArrayBuffer` on the atomics lane, and structured-cloning one
//!   throws `DataCloneError` — on the build that ships, not in development.
//! * The closure is owned here and dropped by [`Coherence::close`]. A
//!   `Closure` that is merely dropped without being unregistered leaves the
//!   channel calling into freed memory.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};

use wasm_bindgen::prelude::Closure;
use wasm_bindgen::{JsCast, JsValue};

use super::api::{announcement_from_ops, Announcement, Change, Sink};
use crate::backend::api::Op;
use crate::key::LockerId;

/// A registered locker, held weakly by the channel so a dropped locker stops
/// receiving without anything having to unregister it.
pub(crate) type SinkHandle = Rc<dyn Sink>;

pub(crate) fn handle(sink: impl Sink + 'static) -> SinkHandle {
    Rc::new(sink)
}

type Sinks = Rc<RefCell<Vec<Weak<dyn Sink>>>>;

pub(crate) struct Coherence {
    /// Distinguishes this bank's own posts from another tab's.
    ///
    /// `Math.random` rather than a `getrandom` dependency: this identifies a
    /// bank handle within one origin for the length of a page's life. It is
    /// not a security boundary and does not need to be one.
    instance: u32,
    epoch: Cell<u64>,
    channel: Option<web_sys::BroadcastChannel>,
    sinks: Sinks,
    /// Kept alive for as long as the channel is listening.
    on_message: RefCell<Option<Closure<dyn FnMut(web_sys::MessageEvent)>>>,
}

impl std::fmt::Debug for Coherence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coherence")
            .field("enabled", &self.channel.is_some())
            .field("instance", &self.instance)
            .finish()
    }
}

impl Default for Coherence {
    fn default() -> Self {
        Self::disabled()
    }
}

impl Coherence {
    pub(crate) fn disabled() -> Self {
        Self {
            instance: instance_id(),
            epoch: Cell::new(0),
            channel: None,
            sinks: Rc::new(RefCell::new(Vec::new())),
            on_message: RefCell::new(None),
        }
    }

    /// Join the channel for `name`, which is the bank's database name.
    ///
    /// A channel this cannot open leaves coherence off rather than failing the
    /// open: a bank that works without cross-tab news is better than no bank.
    pub(crate) fn open(name: &str) -> Self {
        let mut me = Self::disabled();
        let Ok(channel) = web_sys::BroadcastChannel::new(&format!("crossbank::{name}")) else {
            return me;
        };

        let sinks = me.sinks.clone();
        let own = me.instance;
        let closure = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(
            move |event: web_sys::MessageEvent| {
                let Some(announcement) = decode(&event.data()) else {
                    return;
                };
                if announcement.instance == own {
                    return;
                }
                // Collect first, then apply: a sink may drop or open lockers,
                // and holding the registry borrow across that would panic.
                let live: Vec<Rc<dyn Sink>> = sinks
                    .borrow()
                    .iter()
                    .filter_map(|weak| weak.upgrade())
                    .filter(|sink| sink.locker_id() == announcement.locker_id)
                    .collect();
                for sink in live {
                    sink.apply(&announcement);
                }
            },
        );
        channel.set_onmessage(Some(closure.as_ref().unchecked_ref()));
        *me.on_message.borrow_mut() = Some(closure);
        me.channel = Some(channel);
        me
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.channel.is_some()
    }

    pub(crate) fn register(&self, sink: &SinkHandle) {
        if !self.is_enabled() {
            return;
        }
        let mut sinks = self.sinks.borrow_mut();
        sinks.retain(|weak| weak.strong_count() > 0);
        sinks.push(Rc::downgrade(sink));
    }

    /// Work out what a commit is about to say, before its op list is consumed.
    ///
    /// Split from [`Coherence::post`] so that nothing is broadcast until the
    /// commit has actually landed, without the op list having to be cloned.
    pub(crate) fn prepare(&self, locker_id: LockerId, ops: &[Op]) -> Option<Announcement> {
        if self.channel.is_none() {
            return None;
        }
        let epoch = self.epoch.get().wrapping_add(1);
        self.epoch.set(epoch);
        announcement_from_ops(self.instance, locker_id, epoch, ops)
    }

    /// Broadcast. Called only after the commit has landed.
    pub(crate) fn post(&self, announcement: Announcement) {
        let Some(channel) = self.channel.as_ref() else {
            return;
        };
        // A failed post is not a failed write. The data is committed; another
        // tab simply learns about it the next time it reads or reopens.
        let _ = channel.post_message(&encode(&announcement));
    }

    pub(crate) fn close(&self) {
        if let Some(channel) = self.channel.as_ref() {
            channel.set_onmessage(None);
            channel.close();
        }
        *self.on_message.borrow_mut() = None;
        self.sinks.borrow_mut().clear();
    }
}

fn instance_id() -> u32 {
    (js_sys::Math::random() * (u32::MAX as f64)) as u32
}

fn set(object: &js_sys::Object, key: &str, value: &JsValue) {
    let _ = js_sys::Reflect::set(object, &JsValue::from_str(key), value);
}

fn get(value: &JsValue, key: &str) -> Option<JsValue> {
    js_sys::Reflect::get(value, &JsValue::from_str(key)).ok()
}

fn encode(announcement: &Announcement) -> JsValue {
    let object = js_sys::Object::new();
    set(
        &object,
        "instance",
        &JsValue::from_f64(announcement.instance as f64),
    );
    set(
        &object,
        "locker_id",
        &JsValue::from_f64(announcement.locker_id as f64),
    );
    set(
        &object,
        "epoch",
        &JsValue::from_f64(announcement.epoch as f64),
    );
    set(
        &object,
        "cleared",
        &JsValue::from_bool(announcement.cleared),
    );

    let changes = js_sys::Array::new();
    for change in &announcement.changes {
        let item = js_sys::Object::new();
        set(
            &item,
            "key",
            &js_sys::Uint8Array::from(&change.key[..]).into(),
        );
        match &change.value {
            Some(bytes) => set(&item, "value", &js_sys::Uint8Array::from(&bytes[..]).into()),
            None => set(&item, "value", &JsValue::NULL),
        }
        set(&item, "deleted", &JsValue::from_bool(change.deleted));
        changes.push(&item);
    }
    set(&object, "changes", &changes);
    object.into()
}

fn decode(value: &JsValue) -> Option<Announcement> {
    let instance = get(value, "instance")?.as_f64()? as u32;
    let locker_id = get(value, "locker_id")?.as_f64()? as LockerId;
    let epoch = get(value, "epoch")?.as_f64()? as u64;
    let cleared = get(value, "cleared")?.as_bool().unwrap_or(false);

    let mut changes = Vec::new();
    if let Some(array) = get(value, "changes").and_then(|v| v.dyn_into::<js_sys::Array>().ok()) {
        for item in array.iter() {
            let Some(key) = get(&item, "key").and_then(bytes) else {
                continue;
            };
            changes.push(Change {
                key,
                value: get(&item, "value").and_then(bytes),
                deleted: get(&item, "deleted")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            });
        }
    }

    Some(Announcement {
        instance,
        locker_id,
        epoch,
        cleared,
        changes,
    })
}

fn bytes(value: JsValue) -> Option<Vec<u8>> {
    value
        .dyn_into::<js_sys::Uint8Array>()
        .ok()
        .map(|a| a.to_vec())
}
