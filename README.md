# crossbank

Cross-platform persistent key/value storage for Rust. One API on native and in the browser.

> **Picking this up after a break?** Read **[RESUME_HERE.md](RESUME_HERE.md)** first — the
> original plan, why this project exists, the Flutter Hive parity we are matching, and the
> traps that cost a day each.

> **Status: pre-alpha.** The API, the in-memory backend, the `redb` backend and the
> IndexedDB backend all work and are covered by a shared conformance suite that runs
> natively and in real browsers (Chrome and Firefox, plain and atomics). Data persists
> on desktop, mobile, and the web. Large lazy values are chunked and can be streamed
> through `Writer`/`Reader` with bounded memory. M5 has landed too: `Bank::persist()` /
> `is_persisted()` / `usage()`, a byte-budget LRU for containers you mark evictable,
> opt-in cross-tab coherence over `BroadcastChannel`, and opt-in write coalescing.
> Do not depend on this.

## Why

Rust has no mature library that persists data well on *both* native and the web. The pieces
all exist separately — `redb`, `sled` and `fjall` are native-only; `idb`, `indexed-db` and
`indxdb` are the browser half only. The closest thing that spans both is
[`bevy_pkv`](https://github.com/johanhelsing/bevy_pkv), and its author names the gap in his
own README:

> *"Perhaps IndexedDb and something else would have been a better choice, but its API is
> complicated, and I wanted a simple implementation and a simple synchronous API."*

That choice caps it at `localStorage`'s ~5–10 MB. crossbank takes the other fork: an
async-first API so the browser backend can be real IndexedDB, with room for hundreds of
megabytes.

The design is modelled on [Hive](https://github.com/IO-Design-Team/hive_ce), the Flutter
key/value store — its architecture and ergonomics, not its file format.

## Design

- **Two container types.** An eager `Locker` keeps values in RAM for synchronous reads; a
  `LazyLocker` keeps only the key index and fetches values on demand.
- **serde-typed values** with a pluggable codec. `Vec<u8>` works as a value type.
- **Big values are handled.** Transparent auto-chunking, plus a streaming reader/writer so a
  multi-gigabyte value never has to exist in memory at once.
- **Ordered string keys** with prefix, range, reverse and limit scans.
- **Transactions** scoped to a container: commit or roll back as a unit.
- **Watch streams** at container and key level.
- **Pluggable encryption** via a `Cipher` trait. No crypto is bundled.
- **Quota-aware.** Requests persistent storage, reports usage, and sheds least-recently-used
  entries from containers you mark evictable, against a byte budget crossbank itself enforces.
- **Cross-tab coherent, when asked.** Opt-in `BroadcastChannel` invalidation on the web; a
  no-op natively, where `redb`'s exclusive lock means there is no second writer.
- **Write coalescing, when asked.** `Commit::Deferred` batches writes; you own the flush.

### Web caveats

Browser storage is not a filesystem, and three of its rules will bite an application that
assumes otherwise.

**Ask for persistence, and handle "no".** By default an origin's IndexedDB data is
*best-effort*: the browser may reclaim it under storage pressure without asking. Call
`Bank::persist()` — it maps to `navigator.storage.persist()` — and check what comes back.
Chromium decides silently from site-engagement heuristics; **Firefox shows the user a
permission prompt and the future does not resolve until they answer**, so never call it on a
startup path or behind a UI that blocks. `Bank::is_persisted()` is the read-only twin: it
never prompts, so it is safe anywhere.

**Safari deletes everything after 7 days.** Safari's Intelligent Tracking Prevention
removes all script-writable storage — IndexedDB included — for a site the user has not
*interacted with* in seven days. Installed home-screen web apps are exempt, and so is
storage the user has granted persistence to, but ordinary tabs are not. crossbank cannot
work around it, and neither can anything else. What follows from it:

- treat web storage as a **cache with a seven-day floor**, not as the only copy of anything
  a user would miss;
- anything precious belongs on a server, or in an export the user holds;
- Safari has **no CI coverage here** — `safaridriver` on a macOS runner is still a
  follow-up — so its behaviour is documented rather than proven.

**Cross-tab coherence is opt-in.** `BankConfig::with_coherence(true)` puts a bank on a
`BroadcastChannel` so other tabs' writes update this tab's resident state. It is off by
default because it changes what an eager `Locker::get()` can return: a value another tab
wrote that is too large to carry in a message (over 4 KiB sealed) cannot be decoded without
an await, and an infallible getter cannot await — so the resident copy is dropped, an
`Event::Stale` is raised, and `get()` answers `None` for that key until the locker is
reopened. Lazy lockers have no such limit. Natively the flag is accepted and does nothing:
`redb` takes an exclusive file lock, so there is no second process to stay in step with.

**Nothing flushes for you.** With `Commit::Deferred`, staged writes are lost unless the
application calls `flush()` / `Bank::flush_all()` from `pagehide` and from
`visibilitychange` when the document becomes hidden — **not** `beforeunload`, which mobile
browsers frequently never fire. crossbank spawns nothing, so there is no timer or
background task that will do it. See `examples/flush_on_pagehide.rs`.

### Backends

| Backend | Target |
|---|---|
| memory | all |
| [`redb`](https://github.com/cberner/redb) | Linux, macOS, Windows, Android, iOS |
| IndexedDB | `wasm32-unknown-unknown` |

No async runtime dependency — `futures` only. The library spawns nothing itself, and works
under threaded/shared-memory wasm builds.

## Testing

Every backend must pass one shared conformance suite. If a behaviour is not in the suite, it
is not a guaranteed behaviour.

```sh
cargo nextest run                              # native, all backends
ci/wasm-test.sh --plain --firefox              # IndexedDB in a real browser
ci/wasm-test.sh --atomics --chrome             # shared-memory lane
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
