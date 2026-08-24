# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] — 2026-08-24

Additive only. Every 0.1.0 program still compiles and behaves the same.

`BankHandle` — the bytes-only `Send + Sync` proxy that is the *only* usable path on the web,
where a `Bank` is `!Send` — could do `get` / `put` / `delete` / `keys` / `clear` and nothing
else. That is a thin slice of the Hive surface a shim in front of it has to reproduce, and
the gaps were exactly the operations a real application leans on hardest: bulk writes, bulk
reads, and the bank-level questions. This release closes them.

### Added

- **`BankHandle::put_all`** — Hive's `putAll`. Many entries in **one** atomic commit: one
  fsync natively, one IndexedDB transaction on the web. An empty list is `Ok(())` and never
  reaches the backend. Raw values are stored unchunked, so keep a single value well under
  ~100 MiB on the web.
- **`BankHandle::delete_all`** — Hive's `deleteAll`. Many keys in one atomic commit;
  removing an absent key is not an error, and an empty list is a no-op.
- **`BankHandle::get_many`** — many keys in one backend round trip, answered positionally,
  which on IndexedDB is one transaction instead of one per key.
- **`BankHandle::entries`** — the key/value pairs behind Hive's `toMap`, filtered by prefix
  and in byte order, paging from the backend's own `scan_page_size` rather than a constant.
- **`BankHandle::contains_key`** — Hive's `containsKey`. One read, and the value is never
  decoded.
- **`BankHandle::len`** — Hive's `length`, counted over storage: the bytes-only view keeps
  no RAM index, so there is no number to remember.
- **`BankHandle::locker_exists` / `locker_names` / `delete_locker`** — Hive's `boxExists`,
  the registry listing, and `deleteBoxFromDisk`, from a handle rather than only from the
  `Bank` itself.
- **`BankHandle::flush_all` / `persist` / `is_persisted` / `usage`** — the durability and
  storage-pressure calls, reachable from the thread the handle is on.
- **`FilterChain::checksum_only()`** and **`codec::CHECKSUM_ONLY_CHAIN_ID`** — a CRC32-only
  chain, the middle option between `FilterChain::raw()` (no corruption detection at all) and
  the default LZ4-then-CRC32. For payloads LZ4 cannot shrink — densely packed floats,
  already-compressed media, encrypted blobs — where bit rot should still be caught.
- **`BankConfig::with_chain(Arc<FilterChain>)`** and the `BankConfig::chain` field — pick the
  bank's filter chain from a *location*. `Bank::with_backend_and_chain` could already do this
  over a backend you constructed yourself, but not through `Bank::open`, so choosing a chain
  for a real file or a real IndexedDB database meant giving up the location-based open (and,
  natively, the open-bank tracking that lets `delete_bank` refuse a bank that is still open).

### Notes

- `BankConfig` gained a public field. Constructing it with a struct literal rather than
  `BankConfig::at` / `web` / `memory` therefore needs the extra `chain: None`; every
  constructor and builder call is unaffected.
- The chain a bank opens under is persisted per locker and enforced on every later open, so
  `with_chain` is a decision about a *store*: reopening the same bank under a different chain
  is `Error::SchemaMismatch`, not a re-encode.

## [0.1.0] — 2026-08-21

Initial release. crossbank is local, on-device key/value storage for Rust — a direct
replacement for Flutter's [Hive](https://github.com/IO-Design-Team/hive_ce) (`hive_ce`),
with no network code, no server, no sync and no cloud in it.

The API may still change before 1.0.

### Added

**Storage.** One API over three backends: an in-memory backend everywhere, `redb` on Linux,
macOS, Windows, Android and iOS, and real IndexedDB on `wasm32-unknown-unknown`. Backends
are deliberately dumb — no chunking, no codecs, no eviction — so that one shared conformance
suite grades all of them against one spec, natively and in real browsers.

**Containers.** An eager `Locker<T>` keeping values resident for synchronous, infallible
reads, and a `LazyLocker<T>` keeping only the key index and fetching values on demand — the
Hive `Box` / `LazyBox` split, for the same reason. A `Bank` is the root handle, with a
persistent name-to-id registry so a locker is a key prefix rather than a table.

**Keys and values.** Ordered binary keys with prefix, range, reverse and limit scans; every
`&str` method has a `_by` twin taking `&[u8]`. Values are serde-typed through postcard, and
each locker records the type tag it was written with, so reopening it under a different type
fails loudly instead of decoding old bytes into the new shape. A stored empty value is
distinct from an absent key.

**Large values.** Transparent chunking past `LockerConfig::chunk_size` (256 KiB by default,
fixed by benchmark), with the pieces sealed one at a time so peak memory is bounded by the
chunk rather than the value. A streaming `Writer`/`Reader` for `LazyLocker<Vec<u8>>` never
holds the whole value at all. Orphaned chunks are collected on overwrite, delete, clear and
abort.

**Transactions and notification.** Closure-scoped `transact` on one locker: commit or roll
back as a unit, reading its own writes. Bounded `watch()` / `watch_key()` / `watch_keys()`
streams.

**Filters.** A `Filter` trait covering compression, checksumming and — if you bring your own
— encryption. LZ4 and CRC32 ship; **no cipher does**, deliberately, so key handling stays
with the application that owns the keys. A `FilterChain` carries an id that gates format
compatibility, and can be set per bank or per locker (`LockerConfig::with_chain`); the id is
recorded in storage and enforced on every later open.

**Durability and batching.** Two independent knobs, both defaulting to the safe end:
`Durability` decides how hard a commit works to reach the disk, `Commit` decides when a
commit happens at all. `Commit::Deferred { after }` coalesces writes; `flush()` and
`Bank::flush_all()` cover both knobs. Nothing flushes for you — crossbank spawns nothing.

**Storage pressure.** `Bank::persist()`, `Bank::is_persisted()` and `Bank::usage()`, plus a
byte-budget LRU that sheds least-recently-used entries from a `Policy::Evictable` locker
against a budget crossbank enforces itself. `Policy::Precious` is the default: nothing is
ever shed unless you asked for it.

**Corruption.** `OnCorrupt::Skip` opens a locker without its unreadable records and lists
them via `corrupt_keys()`; `Bank::verify` surveys without changing anything; and
`Bank::quarantine` is the only thing that deletes a record for being corrupt.

**Cross-tab coherence.** Opt-in `BroadcastChannel` invalidation on the web
(`BankConfig::with_coherence`), with `Event::Stale` for a value too large to carry in a
message. A no-op natively, where `redb`'s exclusive lock means there is no second writer.

**Threads.** `BankHandle` — a `Send + Sync + Clone` handle onto a bank that is not itself
`Send`, driven by `Bank::into_service()`. On the web this is the only usable path for a
consumer whose storage trait requires `Send` futures, because the IndexedDB backend holds
`JsValue`.

**No async runtime dependency.** `futures` only, never `tokio`. crossbank spawns nothing;
the consumer decides where work runs.

**Defaults on a lazy read.** `LazyLocker::get_or` / `get_or_by`, the twin of `Locker::get_or`,
because Hive's `get(key, defaultValue:)` is used on a `LazyBox` as readily as on a `Box`.

### Fixed

- The LRU tick clock is now seeded from the `lru::` records themselves at open, so a reopened
  bank never re-issues a tick already recorded against a key and the byte budget cannot shed
  the wrong one.

- Two large values can no longer end up sharing one set of chunks. The counter behind chunked
  values used to be saved from the number a writer took, not from the number the bank had
  reached, so a write that finished behind a newer one could push it backwards — and after
  the store was reopened the same id was handed out twice. The pieces of two different values
  then landed under one id, and deleting either one deleted both. The counter is now saved at
  the moment the write is assembled, and a reopened bank starts it above the highest id its
  stored chunks actually use, so an id in use is never handed out again. Reachable on the web
  (IndexedDB), where a write really does pause mid-flight; not on desktop or mobile.

- Two handles on one locker name no longer read stale data. `Bank::locker(name)` used to
  hand out an independent handle each time, with its own resident values (or its own key
  index), and the two never synchronised — so a `get` through one could quietly answer with
  a value the other had already overwritten or deleted, with no error anywhere. Every handle
  on a name is now a view of the one open locker, as `Hive.box(name)` is: one resident map,
  one key index, one staged batch, one set of watchers. A second open under a different
  value type or container kind is `SchemaMismatch`, and one under a different `LockerConfig`
  is `InvalidConfig` naming the field that differs; `close()` on any handle closes the
  locker for all of them. Two consequences worth noting: an eager locker's value type now
  needs `Send + Sync` (the bank holds the shared map type-erased, and `Arc::downcast` is
  defined only for `Arc<dyn Any + Send + Sync>`), and recovering a key after `Event::Stale`
  means closing the locker before opening it again, since opening it again on its own now
  returns the same resident state.

- Two opens of one locker name that overlap in time now end up as one locker. The registry
  check ran before `prepare` and the locker open were awaited, so two callers could both
  pass it, build an `Inner`, a resident state and an index each, and the second registration
  overwrote the first — leaving a live locker that `is_locker_open`, `delete_locker` and the
  next `locker(name)` could not see, and two indexes on one name each willing to prove a key
  absent that the other had chunk-written, which orphaned those chunks permanently. The name
  is now claimed under the registry lock *after* the awaits: the caller that loses the race
  discards the locker it opened (it has only read) and hands out a view of the winner.

- An eager `Locker`'s `delete` of a key whose resident copy was dropped as `Event::Stale` is
  no longer a silent no-op. The fast path treated "not in the resident map" as "not stored",
  but a stale key is precisely a key that is stored while this tab holds no value for it —
  another tab wrote something too large to carry in a coherence message, or bytes this one
  could not decode. The delete returned `Ok(())` without touching storage or announcing
  anything, and the record was back at the next reopen, while `delete_all` on the same key
  wrote the op. Such keys are now remembered and count as present, so the delete reaches
  storage and raises `Event::Deleted`.

- `delete_bank` no longer unlinks a native bank file that is still open. Doing so left the
  live `Bank` committing into a file with no name on Unix — every write after the delete was
  lost silently — and failed with an opaque backend error on Windows. A bank still open in
  this process is now refused with `Error::InvalidConfig` saying *close the bank first*, and
  nothing is removed. Closing the bank, or simply dropping it, frees the name again.

- An eager `Locker`'s `delete` and `delete_all` no longer announce `Event::Deleted` for a key
  that was not there. Hive fires only for keys that existed, so a `listenable(keys:)`-shaped
  rebuild was repainting for nothing. A key skipped as corrupt still counts as present — its
  bytes are on disk — and is still really deleted. `LazyLocker` keeps only the key index, so
  it cannot answer "was it there?" without a read; it still announces unconditionally, and
  the asymmetry is documented on both `delete` methods.

### Notes

- Hive's on-disk format is *not* read. There is no migration, by decision.
- `compact()` has no equivalent and needs none: redb and IndexedDB reclaim their own space.
- Safari's Intelligent Tracking Prevention deletes IndexedDB after seven days without user
  interaction. Nothing in code can answer that; see the README's "Web caveats".

[Unreleased]: https://github.com/john-says-hi/crossbank/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/john-says-hi/crossbank/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/john-says-hi/crossbank/releases/tag/v0.1.0
