//! Change notification.
//!
//! Subscribe to a locker, or to a single key, and receive a stream of events.
//! In a Flutter-style app this is what drives the UI without polling.
//!
//! # Two decisions worth stating
//!
//! **Fan-out is bounded.** An unbounded channel plus a subscriber that stops
//! reading is an out-of-memory abort waiting to happen — and on wasm that takes
//! the whole tab. Each subscriber gets a fixed-capacity queue; when it fills,
//! events are counted rather than buffered, and the subscriber is told how many
//! it missed via [`Event::Lagged`]. Losing events loudly beats growing without
//! limit.
//!
//! **`Cleared` is one event, not N deletes.** Clearing a locker with a hundred
//! thousand keys must not push a hundred thousand messages at every subscriber.
//! A subscriber that sees `Cleared` should assume it knows nothing.

use std::sync::Mutex;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::Stream;

/// Default queue depth per subscriber.
pub const DEFAULT_CAPACITY: usize = 64;

/// Something that happened to a locker.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// A key was written or overwritten.
    Put { key: String },
    /// A key was removed.
    Deleted { key: String },
    /// Every key was removed at once.
    Cleared,
    /// The subscriber fell behind and `skipped` events were dropped.
    ///
    /// Delivered before the next event that does get through, so a subscriber
    /// always learns about a gap rather than silently missing one.
    Lagged { skipped: u64 },
}

impl Event {
    /// The key this event concerns, if it concerns one.
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::Put { key } | Self::Deleted { key } => Some(key),
            Self::Cleared | Self::Lagged { .. } => None,
        }
    }

    /// Whether a subscriber filtered to `key` should receive this event.
    ///
    /// `Cleared` and `Lagged` always pass: a clear affects every key, and a
    /// gap notice must never itself be dropped.
    fn matches(&self, filter: Option<&str>) -> bool {
        match (filter, self) {
            (None, _) => true,
            (Some(_), Self::Cleared | Self::Lagged { .. }) => true,
            (Some(f), other) => other.key() == Some(f),
        }
    }
}

/// A subscription to a locker's changes.
///
/// A named type rather than `impl Stream`, so callers get a stable name in
/// their own signatures and a non-blocking [`EventStream::try_recv`] alongside
/// the `Stream` implementation.
#[derive(Debug)]
pub struct EventStream {
    receiver: mpsc::Receiver<Event>,
}

impl EventStream {
    /// Take the next event if one is already queued, without awaiting.
    pub fn try_recv(&mut self) -> Option<Event> {
        self.receiver.try_recv().ok()
    }
}

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}

struct Subscriber {
    sender: mpsc::Sender<Event>,
    filter: Option<String>,
    skipped: u64,
}

/// The set of live subscribers for one locker.
#[derive(Default)]
pub(crate) struct Watchers {
    subscribers: Mutex<Vec<Subscriber>>,
}

impl std::fmt::Debug for Watchers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watchers")
            .field(
                "subscribers",
                &self.subscribers.lock().map(|s| s.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl Watchers {
    pub(crate) fn subscribe(&self, filter: Option<String>, capacity: usize) -> EventStream {
        let (sender, receiver) = mpsc::channel(capacity);
        if let Ok(mut guard) = self.subscribers.lock() {
            guard.push(Subscriber {
                sender,
                filter,
                skipped: 0,
            });
        }
        EventStream { receiver }
    }

    /// Deliver an event to every interested subscriber.
    ///
    /// Never blocks and never fails: a full queue records a skip, and a dropped
    /// receiver prunes the subscriber.
    pub(crate) fn broadcast(&self, event: &Event) {
        let Ok(mut guard) = self.subscribers.lock() else {
            return;
        };

        guard.retain_mut(|sub| {
            if !event.matches(sub.filter.as_deref()) {
                return true;
            }

            // Tell them about any gap before the event that closes it.
            if sub.skipped > 0 {
                match sub.sender.try_send(Event::Lagged {
                    skipped: sub.skipped,
                }) {
                    Ok(()) => sub.skipped = 0,
                    Err(e) if e.is_disconnected() => return false,
                    Err(_) => {
                        sub.skipped = sub.skipped.saturating_add(1);
                        return true;
                    }
                }
            }

            match sub.sender.try_send(event.clone()) {
                Ok(()) => true,
                Err(e) if e.is_disconnected() => false,
                Err(_) => {
                    sub.skipped = sub.skipped.saturating_add(1);
                    true
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.subscribers.lock().map(|s| s.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::StreamExt;

    fn put(key: &str) -> Event {
        Event::Put {
            key: key.to_string(),
        }
    }

    #[test]
    fn a_subscriber_receives_events_in_order() {
        let w = Watchers::default();
        let mut rx = w.subscribe(None, DEFAULT_CAPACITY);

        w.broadcast(&put("a"));
        w.broadcast(&put("b"));

        assert_eq!(block_on(rx.next()), Some(put("a")));
        assert_eq!(block_on(rx.next()), Some(put("b")));
    }

    #[test]
    fn a_key_filter_excludes_other_keys() {
        let w = Watchers::default();
        let mut rx = w.subscribe(Some("wanted".into()), DEFAULT_CAPACITY);

        w.broadcast(&put("ignored"));
        w.broadcast(&put("wanted"));

        assert_eq!(block_on(rx.next()), Some(put("wanted")));
    }

    #[test]
    fn a_key_filter_still_receives_cleared() {
        // A clear affects every key, so a per-key subscriber must hear about it
        // or it will keep believing a value it no longer has.
        let w = Watchers::default();
        let mut rx = w.subscribe(Some("k".into()), DEFAULT_CAPACITY);

        w.broadcast(&Event::Cleared);
        assert_eq!(block_on(rx.next()), Some(Event::Cleared));
    }

    #[test]
    fn a_slow_subscriber_is_told_how_much_it_missed() {
        // The alternative — an unbounded queue — is an out-of-memory abort.
        let w = Watchers::default();
        let mut rx = w.subscribe(None, 2);

        for i in 0..10 {
            w.broadcast(&put(&format!("k{i}")));
        }

        // Drain what fitted.
        let mut seen = Vec::new();
        while let Some(event) = rx.try_recv() {
            seen.push(event);
        }
        assert!(!seen.is_empty(), "some events should have got through");

        // The next successful delivery must be preceded by a gap notice.
        w.broadcast(&put("later"));
        let next = rx.try_recv().unwrap();
        match next {
            Event::Lagged { skipped } => assert!(skipped > 0),
            other => panic!("expected a Lagged notice first, got {other:?}"),
        }
    }

    #[test]
    fn a_dropped_receiver_is_pruned() {
        let w = Watchers::default();
        let rx = w.subscribe(None, DEFAULT_CAPACITY);
        assert_eq!(w.count(), 1);

        drop(rx);
        w.broadcast(&put("a"));
        assert_eq!(w.count(), 0, "a dead subscriber must not be kept forever");
    }

    #[test]
    fn broadcasting_with_no_subscribers_is_harmless() {
        let w = Watchers::default();
        w.broadcast(&put("a"));
        w.broadcast(&Event::Cleared);
    }

    #[test]
    fn several_subscribers_each_get_a_copy() {
        let w = Watchers::default();
        let mut a = w.subscribe(None, DEFAULT_CAPACITY);
        let mut b = w.subscribe(None, DEFAULT_CAPACITY);

        w.broadcast(&put("k"));

        assert_eq!(block_on(a.next()), Some(put("k")));
        assert_eq!(block_on(b.next()), Some(put("k")));
    }

    #[test]
    fn event_key_reports_only_where_it_makes_sense() {
        assert_eq!(put("k").key(), Some("k"));
        assert_eq!(
            Event::Deleted {
                key: "k".to_string()
            }
            .key(),
            Some("k")
        );
        assert_eq!(Event::Cleared.key(), None);
        assert_eq!(Event::Lagged { skipped: 3 }.key(), None);
    }
}
