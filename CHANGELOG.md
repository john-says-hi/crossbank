# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Fixed

- The LRU tick clock is now seeded from the `lru::` records themselves at open, so a reopened
  bank never re-issues a tick already recorded against a key and the byte budget cannot shed
  the wrong one.

### Notes

- Hive's on-disk format is *not* read. There is no migration, by decision.
- `compact()` has no equivalent and needs none: redb and IndexedDB reclaim their own space.
- Safari's Intelligent Tracking Prevention deletes IndexedDB after seven days without user
  interaction. Nothing in code can answer that; see the README's "Web caveats".

[Unreleased]: https://github.com/john-says-hi/crossbank/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/john-says-hi/crossbank/releases/tag/v0.1.0
