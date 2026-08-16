# crossbank — build plan

## Context

wise_apple's Rust hub owns real persistent state — the encrypted vault, the macro-data
bundle cache, BYOD catalogs and imported series, Lab snapshots, instrument history. It
cannot write any of it. Every read and write is marshalled over a rinf signal to Dart, which
parks the bytes in a Hive box named `hub_store` whose own service class calls itself
*"a deliberately dumb binary key/value responder. Rust owns all schemas."*

That round-trip costs a signal each way, a UUID correlation through a global mutex map, and
a 30-second timeout with three distinct failure modes. It also forces a bootstrap ordering
constraint: the hub cannot read its own cached state until Flutter is up.

The abstraction to fix this already exists — `native/hub/src/runtime/store/api.rs` defines a
four-method `KvStore` trait with `DartKvStore` as its only production implementation. What's
missing is a backend that writes bytes directly, on every platform Flutter ships to.

No mature crate does this. `bevy_pkv` is closest and its author names the gap outright:
localStorage on web because "IndexedDb… is complicated, and I wanted a simple synchronous
API." That caps it near 5–10 MB. wise_apple stores candle series and report bundles in the
hundreds of megabytes. Everything else is the web half only (`idb`, `indexed-db`, `indxdb`),
native only (`redb`, `sled`, `fjall`), far too green (`netabase_store` at 0.0.8), or an
entire database (SurrealDB).

**crossbank** fills that gap: Hive's ergonomics, pure Rust, with a real IndexedDB backend on
the web. It is a standalone public library with zero knowledge of wise_apple. The intended
outcome is that it eventually replaces Hive in wise_apple entirely — but only after it has
proven itself on its own terms.

### Correction worth recording

Hive does **not** use SQLite. It is pure Dart with a custom binary format explicitly
inspired by Bitcask: an append-only log of frames `[len u32][key][value][crc32]`, where a
delete appends an empty-value frame, the index is rebuilt by parsing the entire file at open,
and `compact()` rewrites the file to drop dead frames. Web uses IndexedDB. That design is
exactly *why* Hive has eager and lazy boxes — it is the Bitcask keydir pattern.

We copy Hive's **architecture and API**, not its bytes. Two properties of Hive's format we
specifically do not want: deleted values stay readable on disk until someone calls
`compact()`, and keys are stored in plaintext even in encrypted boxes.

---

## Decisions

Settled with the user across four rounds of questions.

| # | Decision | Choice |
|---|---|---|
| 1 | API shape | Two container types, Hive-style: eager (sync reads) + lazy (async reads) |
| 2 | Value model | serde-typed, pluggable codec; `Vec<u8>` works as a `T` |
| 3 | Big values | Transparent auto-chunking **and** a streaming Writer/Reader |
| 4 | Hive migration | None. Clean start |
| 5 | Native backend | `redb` — Hive's API, not Hive's bytes |
| 6 | Multi-tab | Broadcast invalidation (BroadcastChannel / file watcher) |
| 7 | Reactivity | `watch()` and `watch_key()` streams |
| 8 | Encryption | Pluggable `Cipher` trait, no crypto shipped |
| 9 | Storage full | Per-container eviction policy + quota API + `persist()` |
| 10 | Atomicity | Transactions scoped to one container |
| 11 | Key scans | Ordered string keys: prefix, range, reverse, limit |
| 12 | Tests | One shared conformance suite × every backend |
| 13 | Container name | `Locker`, root handle `Bank` (avoids shadowing `std::boxed::Box`) |
| 14 | Build order | Walking skeleton first |
| 15 | License | MIT OR Apache-2.0 |
| 16 | CI breadth | Desktop + web real per PR; mobile smoke nightly |
| 17 | Dart side | Rust crate only; any Dart shim belongs to wise_apple |
| 18 | Endgame | Invert the bridge — a Hive-shaped Dart shim backed by crossbank |
| 19 | Separation | crossbank has zero knowledge of wise_apple, Flutter, or rinf |

**Non-goals.** Not a SQL engine. Not a document store. Not a sync engine. Not a Hive
file-format reader. No Dart/Flutter package.

---

## API

```rust
let bank = Bank::open_at(path).await?;          // caller supplies the path
let bank = Bank::open("crossbank").await?;      // or platform default (desktop only)

// EAGER — whole locker in RAM at open, reads are sync
let settings: Locker<Settings> = bank.locker("ui_settings").await?;
let theme = settings.get("theme");
settings.put("theme", &dark).await?;

// LAZY — key index in RAM, values on demand
let candles: LazyLocker<Chunk> = bank.lazy_locker("candle_cache").await?;
let chunk = candles.get("BTCUSDT::0000001700").await?;
let ids   = candles.keys_with_prefix("BTCUSDT::");     // sync, index is in RAM

// Ordered scans
let window = candles.range("BTCUSDT::0000001700".."BTCUSDT::0000001800").await?;
let recent = candles.range_rev(..).limit(50).await?;

// Transactions — one locker, all or nothing
let tx = candles.transaction().await?;
tx.put("BTCUSDT::chunk::0", &a)?;
tx.put("BTCUSDT::manifest", &m)?;
tx.commit().await?;

// Streaming — never materialize a huge value
let mut w = candles.writer("BTCUSDT::full").await?;
for batch in batches { w.write_chunk(&batch).await?; }
w.finish().await?;

// Watch
let mut rx = settings.watch_key("theme");   // Put | Deleted | Cleared

// Quota and policy
let q = bank.quota().await?;                // { used, available, persisted }
bank.lazy_locker("candle_cache").policy(Policy::Evictable { target: 0.8 }).await?;
```

---

## Architecture

Three layers. Only the bottom is platform-specific.

```
    Bank / Locker / LazyLocker / Transaction / Writer / Reader
 ─────────────────────────────────────────────────────────────  public API
    Codec · Cipher · chunking · RAM index · watch fan-out
             · eviction policy · cross-tab coherence
 ─────────────────────────────────────────────────────────────  engine (portable)
                       trait Backend
 ─────────────────────────────────────────────────────────────
    memory              redb                IndexedDB
    (all targets)       (native)            (wasm32)
```

The engine holds all real logic and is 100% portable. Backends stay dumb — open a table,
get/put/delete a record, scan a range, run a transaction. That is what makes one conformance
suite meaningful.

### Layout

```
crossbank/
  src/
    lib.rs
    bank.rs                Bank, open, quota, remote_handle
    locker/{mod,eager,lazy,policy}.rs
    codec/{mod,api,postcard_lz4}.rs
    cipher.rs              trait only, no implementations
    chunk.rs               descriptor, split/reassemble, Writer, Reader
    watch.rs
    coherence/{mod,api,web,native}.rs
    backend/{mod,api,memory,redb,indexeddb}.rs
    quota.rs
    error.rs
  tests/conformance/       the shared spec
  examples/
```

`mod.rs` gates and re-exports only; platform code lives in `native.rs`/`web.rs`; each
subsystem's `api.rs` is the cfg-free surface. Same pattern wise_apple's hub already uses in
`runtime/byod/http/` and `runtime/worker_spawn/`.

### Storage model

Two tables per locker (redb tables / IndexedDB object stores):

- **`<locker>`** — `key -> Record`, either `Inline { bytes }` or
  `Chunked { value_id, chunks, total_len, checksum }`, plus `last_access` for eviction.
- **`<locker>$chunks`** — `(value_id, seq) -> bytes`.

Chunk payloads never share a keyspace with user keys, so no escaping rules are needed.
Deleting a key deletes its chunks in the same transaction. Default chunk size 8 MiB, tunable.

Default codec is postcard → LZ4 → CRC32 in a versioned envelope, mirroring the `WBYD`
envelope already proven in wise_apple's `runtime/store/codec.rs`. The version byte is
non-negotiable — it is what makes a format change survivable.

---

## The hard parts, named up front

Each is confronted in M0, before there is a library to rewrite.

**`Send` and `JsValue`.** IndexedDB handles are `JsValue`, which is `!Send`. wise_apple's
`KvStore` requires `Send` futures, and its wasm build runs `+atomics` with `--shared-memory`.
Plan: a `MaybeSend` bound (`Send` on native, unbounded on wasm) for the ordinary API, plus
`Bank::remote_handle()` returning a `Send + Sync` proxy that forwards operations over a
channel to the thread owning the bank. That is what lets a rayon worker read storage at all.
Deferred to M6, but the API is shaped for it from day one.

**Cross-origin isolation in CI.** Every browser requires the page to be cross-origin isolated
(`Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp`)
before `SharedArrayBuffer` is exposed — mandatory since Chrome 92. Whether
`wasm-bindgen-test-runner`'s built-in server sends those headers is **unresolved**, and I
could not settle it from documentation. This is the single highest-risk item in the plan.
Candidate fallbacks, in order: launch the browser with a flag that force-enables
`SharedArrayBuffer`; supply custom driver capabilities via `WASM_BINDGEN_TEST_DRIVER`; or
serve the test bundle from our own tiny header-setting server and drive it directly. M0 must
land on one of these before any library code is written.

**`RUSTFLAGS` clobbering.** Exporting a `RUSTFLAGS` env var *replaces* `.cargo/config.toml`
`target.*.rustflags` rather than appending. wise_apple hit exactly this — it re-exposed
atomics flags to stable's prebuilt std and broke linking. CI must never export `RUSTFLAGS`.

**Threaded wasm cannot run under Node.** wise_apple's `run_hub_tests.sh:57` uses
`wasm-pack test --node` and documents it as a *compile gate only*. Its browser suite
(`tests/wasm_smoke.rs`) has therefore **never once executed**. We do not repeat that.

**redb is synchronous.** Calls run inline inside our async fns. Acceptable for local disk;
avoids a runtime dependency. Documented, revisited only if profiling says otherwise.

**No async runtime dependency.** `futures` only, never `tokio`. The library spawns nothing.
Native tests use `futures::executor::block_on`; wasm tests use `wasm_bindgen_test`.
Note `indexed_db_futures` pulls `tokio` for one feature — which is why the pick is
`indexed-db` (Ekleog), MIT OR Apache-2.0, whose tagline is literally "can work
multi-threaded". Pin `0.5.0` (stable, released; `cargo search` misleadingly surfaces
`0.5.0-alpha.1` as latest). M0 confirms it round-trips under `+atomics`.

**Eviction write amplification.** LRU needs `last_access`, but writing it on every read
doubles read cost. Only persist it when more than N minutes stale; keep the live value in
RAM. Approximate LRU is fine for a cache.

---

## Testing

**The conformance suite is the product.** One set of async test functions, generic over
`Backend`, that every backend must pass identically. If a behavior is not in the suite, it is
not a guaranteed behavior.

Covers: get/put/delete/clear, key ordering and range/prefix/reverse/limit semantics,
transaction commit and rollback, chunk round-trips across the inline/chunked boundary,
streaming Writer/Reader, watch event ordering, codec version rejection, cipher round-trip,
empty and maximum-size values, key edge cases.

Beyond the suite: crash-and-reopen (native: child process killed mid-write; wasm: a
fault-injecting backend wrapper that aborts transactions at chosen points), torture
(multi-GB, quota exhaustion, thousands of lockers — nightly, not per PR), and property tests
against the memory backend as oracle.

### CI

Public repos get free unmetered GitHub Actions on every runner OS. This is why the matrix is
affordable here and is not in wise_apple, which is **private** with an exhausted allocation —
every workflow there is `workflow_dispatch`-only, documented in-file as a billing
consequence. There is no push or pull_request trigger anywhere in that repo.

| Lane | Backends | Where | When |
|---|---|---|---|
| native | memory + redb | Linux, macOS, Windows | per PR |
| web | IndexedDB | headless Chrome + Firefox | per PR |
| wasm atomics build | — | `+atomics`/`--shared-memory` | per PR |
| mobile compile | — | Android, iOS `cargo check` | per PR |
| mobile persistence | redb | Android emulator, iOS simulator | nightly |
| torture + crash | all | Linux, Chrome | nightly |

`cargo nextest` for native lanes, never `cargo test`.

---

## Milestones

**M0 — Spike and scaffold.** De-risking only, no library code. Repo, license, CI skeleton.
Prove `wasm-bindgen-test` runs headless in Chrome and Firefox *both* with and without shared
memory, and resolve the COOP/COEP question. Prove `indexed-db` compiles and round-trips under
`+atomics`. Prove redb builds for Android and iOS targets.
*Exit: green CI on every target platform with one trivial test. Nothing else starts until
this passes.*

**M1 — Walking skeleton.** Full public API against the memory backend: `Bank`, `Locker`,
`LazyLocker`, `Codec`, `Cipher`, ranges, transactions, watch. Conformance suite written and
green.
*Exit: API stable enough that remaining backends only have to satisfy the suite.*

**M2 — redb backend.** Passes the suite unmodified. Native crash-and-reopen tests.
*Exit: real persistence on desktop.*

**M3 — IndexedDB backend.** Passes the same suite in Chrome and Firefox, plain and atomics.
*Exit: real persistence on the web — the platform that actually ships.*

**M4 — Big data.** Auto-chunking across the inline/chunked boundary, streaming Writer and
Reader, torture tests.
*Exit: a multi-GB series written and read back on both real backends without exhausting RAM.*

**M5 — Quota, eviction, coherence.** `persist()`, quota API, per-locker policy, LRU shedding,
BroadcastChannel and native invalidation.
*Exit: filling the quota degrades gracefully instead of failing writes.*

**M6 — Consumer readiness.** `remote_handle()` for `Send` across threads on wasm, README,
docs, worked example, publish to crates.io.
*Exit: crossbank could back a `KvStore` implementation as a one-file swap.*

---

## Verification

- `cargo nextest run` — native conformance across all three backends on Linux.
- `wasm-pack test --headless --chrome` and `--firefox` — IndexedDB conformance, run both
  plain and under the atomics profile. Never export `RUSTFLAGS`.
- A red-to-green CI run on the full matrix is the M0 exit gate and the standing per-PR gate.
- Nightly: Android emulator and iOS simulator write, kill, reopen, and verify.
- The one end-to-end proof that matters: write a multi-GB series in Chrome, close the tab,
  reopen, and read it back byte-identical.

---

## Open questions

Deliberately unresolved. Each is scheduled, not forgotten.

- **COOP/COEP in the headless test runner** — the one item that could force a rethink of the
  whole wasm test lane. Resolved in M0 or the plan changes.
- Chunk size default: 8 MiB is a guess. M4's torture tests pick the real number.
- Whether eager lockers should refuse to hold chunked values outright, since a sync `get()`
  cannot await a chunk fetch. Leaning yes. Decided in M1 when the API locks.
- Whether `Bank::open` auto-requests `persist()` or leaves it explicit.
- Whether an eager locker needs a size ceiling that refuses to load, or just a loud warning.
- Whether ordered string keys stay the right call given a shim may need to encode integer
  keys. Confirmed before M1 locks the API.

---

## Appendix — the eventual wise_apple drop-in (not this project)

Recorded so crossbank's API is shaped to allow it. **No wise_apple work happens here.**

The Hive surface a Dart shim must reproduce is small and now fully known: `get` (with
`defaultValue:`), `put`, `putAll`, `delete`, `deleteAll`, `clear`, `keys`, `containsKey`,
`length`, `toMap`, `watch` (optionally `key:`), and hive_flutter's `listenable(keys:)`.
Plus statics `box`, `openBox`, `lazyBox`, `openLazyBox`, `isBoxOpen`, `boxExists`,
`deleteBoxFromDisk`, `init`/`initFlutter`.

Never used in app code: `putAt`, `deleteAt`, `getAt`, `keyAt`, `add`, `addAll`, `values`,
`valuesBetween`, `isEmpty`, `isNotEmpty`, `flush`, `compact`, `close`, `deleteFromDisk`.
No auto-increment keys. No encryption anywhere. No `compactionStrategy`, `crashRecovery`, or
custom `path:` on any open.

Three constraints worth carrying into crossbank's design:
1. `watch()` consumers read only `event.key`, never `value` or `deleted` — our event shape is
   already more than enough.
2. Production code never closes a box, but the **test suite** closes heavily (`Hive.close()`
   at 95 sites, `box.close()` at ~38). A shim must support close-and-reopen in-process.
3. One test round-trips an **integer key** (`put(42, …)`). crossbank uses ordered string keys,
   so a shim must encode non-string keys deterministically — worth confirming that ordered
   string keys remain the right call before M1 locks the API.
