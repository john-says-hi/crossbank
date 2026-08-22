# crossbank

**crossbank is local, on-device key/value storage for Rust — a direct replacement for
Flutter's [Hive](https://github.com/IO-Design-Team/hive_ce) (`hive_ce`).** It saves your
application's data on the machine it is running on: a `redb` file on Linux, macOS, Windows,
Android and iOS, and real IndexedDB in the browser, behind one API. There is **no network
code, no server, no sync and no cloud** anywhere in it. crossbank never talks to anything —
not to us, not to anyone. Data goes in, data comes back out, on that device only.

> **Status: 0.1.0 candidate; API may still change before 1.0.** The in-memory, `redb` and
> IndexedDB backends all pass one shared conformance suite, natively and in real browsers
> (Chrome and Firefox, plain and atomics lanes). Data persists on desktop, mobile and the
> web. Large values are chunked and streamable with bounded memory, storage pressure is
> answered by a real byte budget, and cross-tab coherence and write coalescing are there
> when you ask for them. What is not settled yet is the *shape of the API*: names and
> signatures may still move before 1.0.

> **Picking this up after a break?** Read **[RESUME_HERE.md](RESUME_HERE.md)** first — the
> original plan, why this project exists, the Hive parity we are matching, and the traps
> that cost a day each.

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

The design is modelled on Hive — its architecture and ergonomics, not its file format.
Hive's data is not migrated and cannot be read; this is a clean start with a Hive-shaped
API.

## Coming from Hive

Hive's vocabulary maps across almost word for word. `Box` is `Locker` and `LazyBox` is
`LazyLocker`, named that way because a `crossbank::Box` would shadow `std::boxed::Box` in
every file that imported it. The root handle is `Bank` rather than the `Hive` static.

| Hive (`hive_ce`) | crossbank |
|---|---|
| `Box<T>` | `Locker<T>` |
| `LazyBox<T>` | `LazyLocker<T>` |
| `box.get(key, defaultValue: v)` / `lazyBox.get(…)` | `locker.get_or(key, v)` — on both `Locker` and `LazyLocker` |
| `box.put(k, v)` | `locker.put(k, v)` |
| `box.putAll(map)` | `locker.put_all(pairs)` |
| `box.delete(k)` | `locker.delete(k)` |
| `box.deleteAll(keys)` | `locker.delete_all(keys)` |
| `box.clear()` | `locker.clear()` |
| `box.keys` / `box.containsKey` / `box.length` | `locker.keys()` / `contains_key()` / `len()` |
| `box.toMap()` | `locker.to_map()` |
| `box.watch()` | `locker.watch()` |
| `box.watch(key: k)` | `locker.watch_key(k)` |
| `ValueListenable` via `listenable(keys: […])` | `locker.watch_keys([…])` |
| `Hive.openBox` / `openLazyBox` | `Bank::locker` / `Bank::lazy_locker` |
| `Hive.isBoxOpen(name)` | `Bank::is_locker_open(name)` |
| `Hive.boxExists(name)` | `Bank::locker_exists(name)` |
| `Hive.deleteBoxFromDisk(name)` | `Bank::delete_locker(name)` |
| `Hive.deleteFromDisk()` | `crossbank::delete_bank(&config)` |
| `Hive.close()` | `Bank::close()` |
| `box.compact()` | not needed — redb and IndexedDB reclaim their own space |
| `HiveAesCipher` | the `Filter` trait — **no crypto is bundled** |

Two differences worth knowing before you port anything:

- **Keys are bytes.** A `&str` key is stored as exactly its UTF-8 bytes, and every `&str`
  method has a `_by` twin taking `&[u8]` (`get_by`, `put_by`, `range_by`, …). Hive's
  integer keys and auto-increment `add()` have no equivalent; encode the key yourself.
- **An empty value is a value.** Hive-shaped bridges often treat an empty payload as
  "missing"; crossbank distinguishes a stored empty value from an absent key, and the
  conformance suite asserts it.

## Design

- **Two container types.** An eager `Locker` keeps values in RAM for synchronous, infallible
  reads; a `LazyLocker` keeps only the key index and fetches values on demand.
- **serde-typed values** with a pluggable codec. `Vec<u8>` works as a value type. A locker
  records the type it was written with, so reopening it under a different one fails loudly
  instead of decoding old bytes into the new shape.
- **Big values are handled.** Transparent auto-chunking past `chunk_size` (256 KiB by
  default), plus a streaming `Writer`/`Reader` so a multi-gigabyte value never has to exist
  in memory at once.
- **Ordered binary keys** with prefix, range, reverse and limit scans.
- **Transactions** scoped to one locker: commit or roll back as a unit.
- **Watch streams** at locker and key level.
- **Pluggable transforms** via the `Filter` trait — compression, checksumming, and
  encryption if you bring your own. crossbank ships LZ4 and CRC32 and **no cipher at all**,
  deliberately, so key handling and its audit burden stay with the application that owns the
  keys. A chain can be set per bank or per locker (`LockerConfig::with_chain`), and the
  chain's id is recorded in storage so a locker can never be reopened under the wrong one.
- **Quota-aware, and it really evicts.** `Bank::persist()` asks the platform to keep the
  data, `Bank::is_persisted()` reads that back without prompting, and `Bank::usage()`
  reports what the origin is using. A locker marked `Policy::Evictable { max_bytes }` sheds
  its least-recently-used entries to stay under that budget — a byte budget crossbank owns
  and enforces itself, not a fraction of a browser estimate that moves when another tab
  writes. `Policy::Precious` is the default: nothing is ever shed unless you asked for it.
- **Cross-tab coherent, when asked.** Opt-in `BroadcastChannel` invalidation on the web; a
  no-op natively, where `redb`'s exclusive lock means there is no second writer.
- **Write coalescing, when asked.** `Commit::Deferred { after }` batches writes; you own the
  flush.
- **No async runtime dependency.** `futures` only, never `tokio`. crossbank spawns nothing —
  you decide where the work runs.

## Durability & performance

Two independent knobs decide when a write is safe, and they answer different questions.
`Commit` decides **when a commit happens**; `Durability` decides **how hard that commit works
to reach the disk**. Both default to the safe end and neither is ever chosen for you.

| | `Durability::Immediate` (default) | `Durability::Eventual` |
|---|---|---|
| **`Commit::Immediate`** (default) | Safest, slowest. One fsync per `put`; when `put` returns, the data survives a power cut. | One commit per `put`, no fsync. Survives the process dying; a power cut may lose recent writes until `flush`. |
| **`Commit::Deferred { after }`** | One fsync per batch. Nothing is stored — at all — until the batch fills or you `flush`. | Cheapest. Neither the batch nor the fsync happens until it fills or you `flush`. |

`flush()` covers both, on a locker or bank-wide via `Bank::flush_all()`. **Nothing flushes
for you** — no timer, no task, no destructor.

`Durability` is native-only in effect: IndexedDB has no fsync knob, so an `Eventual` locker
on the web behaves exactly like an `Immediate` one and the setting costs nothing there.

### Numbers

Dated snapshots on one machine (12th Gen Core i9-12900HK, Linux), not a CI gate. Full
tables, methodology and noise bounds are in [PLAN.md](PLAN.md) → Performance; reproduce with
`cargo bench --bench kv` and `ci/bench.sh --hive --web`.

**Natively, against Hive CE on identical byte payloads.** The default settings pay an fsync
per put and Hive does not, which is most of the gap on write-heavy shapes — so the honest
comparison is per knob:

| Workload | Hive CE | crossbank redb (default) | crossbank redb (`Eventual` + `flush`) |
|---|---|---|---|
| `settings_eager` — 90/10 get/put, 1 KiB | 4.0 µs/op | 42.5 µs/op | **1.17 µs/op** |
| `bulk_lazy_put` — 2 000 × 256 B, one put each | 27 ms | 907 ms | **25.5 ms** |
| `bulk_lazy_get` — per random get | 19 µs | **1.1 µs** | — |
| `txn_batch` — 100 puts in one `putAll` / `transact` | 0.11 ms | 1.05 ms | — |
| `big_value_put_get` — one 8 MiB value | 50 ms | **15.8 ms** | — |

Reads are structurally faster (a B-tree page read against Hive's seek-and-parse on an append
log) and big values are ~3× faster despite LZ4+CRC on every chunk. For write bursts, either
turn the durability knob or use `transact` — that is what it is for.

**On the web, neither engine is fsync-durable** (Chromium runs IndexedDB in `"relaxed"`
durability), so it really is speed against speed. Milliseconds per timed iteration, p50, in
Chrome:

| Workload | Hive CE | crossbank |
|---|---|---|
| `settings_eager` — 1 000 ops, 1 KiB | 21.4 | **12.4** |
| `bulk_lazy_put` — 2 000 × 256 B | 406 | **346** |
| `bulk_lazy_get` — 1 000 scattered gets | 119 | **52.3** |
| `txn_batch` — 100 puts in one call | **8.30** | 9.37 |
| `big_value_put_get` — one 8 MiB value | **17.3** | 22.7 |

Firefox splits differently — Hive wins the two write shapes, crossbank wins the reads. What
holds in both browsers is the **tail**: crossbank's p99s are consistently tighter where the
medians are close (24.8 ms against 218 ms on `txn_batch` in Chrome). Hive's tail is its
append log meeting an IndexedDB pause. For a UI that must not jank, that is the column that
decides.

Treat anything inside about 2× as a tie: repeated runs of identical code on this machine
moved some rows by that much on their own.

## Web caveats

Browser storage is not a filesystem, and four of its rules will bite an application that
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
- anything precious belongs in an export the user holds — or on a server *you* write, since
  crossbank will not do it for you;
- Safari has **no blocking CI coverage here** — its lane is new and non-blocking — so its
  behaviour is documented rather than proven.

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
background task that will do it. See [`examples/flush_on_pagehide.rs`](examples/flush_on_pagehide.rs).

## Browser & platform support

| Browser | Engine | Coverage |
|---|---|---|
| Chrome | Blink | CI, plain + atomics lanes |
| Firefox | Gecko | CI, plain + atomics lanes |
| Safari / WebKit | WebKit | CI lane landing — plain lane only (no headless Safari; `SharedArrayBuffer` under WebDriver is unreliable) |
| Edge | Chromium | Covered by the Chrome lane — same Blink/V8 and the same IndexedDB implementation |

| Platform | Coverage |
|---|---|
| Linux, macOS, Windows | CI, full test suite (`cargo nextest` + doctests) |
| Android (arm64, armv7, x86_64) | CI, `cargo check` only |
| iOS (device + simulator) | CI, `cargo check` only |

Safari's Intelligent Tracking Prevention deletes IndexedDB after 7 days without user
interaction. See [Web caveats](#web-caveats).

## Backends

| Backend | Target |
|---|---|
| memory | all |
| [`redb`](https://github.com/cberner/redb) | Linux, macOS, Windows, Android, iOS |
| IndexedDB | `wasm32-unknown-unknown` |

Backends are deliberately dumb — no chunking, no codecs, no eviction — so that a single
conformance suite can grade all of them against one spec. Everything above the `Backend`
trait is portable Rust with no `cfg` in it. The library works under threaded/shared-memory
wasm builds.

## Examples

```sh
cargo run --example settings          # eager, Hive-shaped: get_or, put, watch_keys
cargo run --example candles           # lazy: transact, Writer/Reader, Evictable
cargo run --example flush_on_pagehide # Commit::Deferred and who owns the flush
```

## Testing

Every backend must pass one shared conformance suite. If a behaviour is not in the suite, it
is not a guaranteed behaviour.

```sh
cargo nextest run                              # native, all backends
cargo test --doc                               # nextest does not run doctests
ci/wasm-test.sh --plain --firefox              # IndexedDB in a real browser
ci/wasm-test.sh --atomics --chrome             # shared-memory lane
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
