# RESUME HERE — the original crossbank plan and why this project exists

**If you have been told to "find the original crossbank plan and resume work", this is that
file.** Read it end to end before touching code. `PLAN.md` next to it holds the full technical
plan; this file holds the *purpose*, the agreements, and the things that will cost you days if
you rediscover them the hard way.

- **Repo:** `github.com/john-says-hi/crossbank` (public)
- **Local checkout:** `~/Documents/crossbank`
- **Owner:** John (`john-says-hi`)
- **Started:** 2026-08-15 · **Last worked:** 2026-08-20
- **State:** M0–M4 complete. **M5 (quota, eviction, coherence) is next**; `persist()` is its first piece.

---

## 1. What crossbank is

A cross-platform persistent key/value library in pure Rust. One API that stores data on every
platform Flutter ships to — Linux, macOS, Windows, Android, iOS, and **the web**.

It is modelled on [Hive](https://github.com/IO-Design-Team/hive_ce), the Flutter key/value
package: its architecture and ergonomics, deliberately **not** its file format.

## 2. Why we are building it — the directive

John's app (**wise_apple**, a separate private repo) has a Rust "hub" that compiles to both
native and `wasm32`. That hub owns real persistent state — an encrypted vault, a macro-data
bundle cache, imported market-data series, Lab snapshots, instrument history. **It cannot
write any of it itself.** Every read and write is marshalled over a rinf signal to Dart, which
parks the bytes in a Hive box whose own service class describes itself as *"a deliberately
dumb binary key/value responder. Rust owns all schemas."*

That round-trip costs a signal each way, a UUID correlation through a global mutex map, and a
30-second timeout with three distinct failure modes. It also forces a bootstrap ordering
constraint: the hub cannot read its own cached state until Flutter has started.

**No mature crate fixes this.** The closest is
[`bevy_pkv`](https://github.com/johanhelsing/bevy_pkv), and its author names the gap in his own
README:

> *"Perhaps IndexedDb and something else would have been a better choice, but its API is
> complicated, and I wanted a simple implementation and a simple synchronous API."*

That choice caps it at `localStorage`'s ~5–10 MB. wise_apple stores candle series and report
bundles in the **hundreds of megabytes**. Everything else in the ecosystem is the web half only
(`idb`, `indexed-db`, `indxdb`), native only (`redb`, `sled`, `fjall`), far too green
(`netabase_store` at 0.0.8), or an entire database (SurrealDB).

So crossbank takes the other fork: **an async-first API, so the browser backend can be real
IndexedDB** with room for hundreds of megabytes.

### The long-term goal

crossbank should **eventually replace Hive in wise_apple entirely** — John was explicit that
this is paramount. But only after it has proven itself on its own terms. The agreed endgame is
to *invert the current bridge*: a Hive-shaped Dart shim backed by crossbank, so wise_apple's
~405 Hive call sites change by roughly one word rather than being rewritten.

**None of that work happens in this repo.** See §4.

## 3. Flutter Hive parity — what we copy and what we refuse

Hive's design is the reference. Getting this right is most of the point, so be precise about it.

### A correction that matters

**Hive does NOT use SQLite.** It is pure Dart with a custom binary format explicitly inspired
by Bitcask: an append-only log of frames `[len u32][key][value][crc32]`, where a delete appends
an empty-value frame, the index is rebuilt by parsing the entire file at open, and `compact()`
rewrites the file to drop dead frames. Web uses IndexedDB.

That design is exactly *why* Hive has eager and lazy boxes — it is the Bitcask keydir pattern.
Anyone who assumes SQLite will design the wrong thing.

### What we copy (the architecture and API)

| Hive | crossbank | Why |
|---|---|---|
| `Box<T>` | `Locker<T>` | Values resident in RAM, `get()` synchronous and infallible |
| `LazyBox<T>` | `LazyLocker<T>` | Key index resident, values fetched on demand |
| `box.watch()` | `locker.watch()` / `watch_key()` | Stream of change events |
| `HiveAesCipher` | `Filter` trait | Pluggable, but we ship **no** crypto |
| box name → file | locker name → key prefix | Same mental model |

Named `Locker` rather than `Box` on purpose: `crossbank::Box` would shadow `std::boxed::Box`
in every file that imports it. The root handle is `Bank`.

### What we deliberately refuse to copy

1. **Hive's on-disk format.** We use `redb` natively. Hive's format leaves deleted values
   readable on disk until someone calls `compact()`, and stores keys in **plaintext even in
   encrypted boxes**. We want neither property.
2. **A synchronous API on the web.** That is precisely the decision that caps `bevy_pkv` at
   localStorage. Async-first is the whole reason crossbank can exist.
3. **Hive's data.** No migration, by decision. Clean start.

### The Hive surface a future Dart shim must reproduce

Already inventoried from wise_apple, so crossbank's API is shaped to allow it:

`get` (with `defaultValue:`), `put`, `putAll`, `delete`, `deleteAll`, `clear`, `keys`,
`containsKey`, `length`, `toMap`, `watch` (optionally `key:`), `listenable(keys:)`. Plus
statics `box`, `openBox`, `lazyBox`, `openLazyBox`, `isBoxOpen`, `boxExists`,
`deleteBoxFromDisk`, `init`/`initFlutter`.

**Never used in app code** (so not required): `putAt`, `deleteAt`, `getAt`, `keyAt`, `add`,
`addAll`, `values`, `valuesBetween`, `isEmpty`, `isNotEmpty`, `flush`, `compact`, `close`,
`deleteFromDisk`. No auto-increment keys. No encryption anywhere. No `compactionStrategy`,
`crashRecovery`, or custom `path:`.

Three constraints to carry:
- `watch()` consumers read only `event.key`, never `value` or `deleted` — our event shape is
  already more than enough.
- Production code never closes a box, but wise_apple's **test suite** closes heavily
  (`Hive.close()` at 95 sites). A shim must support close-and-reopen in-process.
- One wise_apple test round-trips an **integer key**. A shim must encode non-string keys
  deterministically; binary keys make that straightforward.

One thing **not** to copy from the current bridge: wise_apple's `DartKvStore::get` treats an
empty payload as `None`. crossbank must distinguish a stored empty value from a missing key,
and the conformance suite asserts it.

## 4. Working agreements (these differ from wise_apple — read carefully)

- **Commit directly on `master`. No PRs, no worktrees.** John authorised this on 2026-08-16.
  This is the *opposite* of the wise_apple rule; do not carry that habit across.
- **crossbank must have ZERO knowledge of wise_apple, Flutter, or rinf.** No types, no
  features, no conditional code. It is a standalone public library. Any Dart shim belongs in
  wise_apple's repo, not here.
- **Rust crate only.** No Dart/Flutter package is planned.
- **Never depend on an async runtime.** `futures` only, never `tokio`. crossbank spawns
  nothing — the consumer decides where work runs.
- **Do not write files into the wise_apple tree** for this project.
- License is MIT OR Apache-2.0. `publish = false` until M6.

## 5. Where things stand

**M0 — complete.** De-risking spikes. Proved the headless browser test lanes work and are
cross-origin isolated, IndexedDB persists across reopen, the shared-memory build works, and
redb builds for Android and iOS.

**M1 — complete.** The walking skeleton: `Backend` trait, memory backend, binary keys, value
envelope and filter chain, `Bank` with a locker registry and schema guard, eager and lazy
lockers, closure-scoped transactions, bounded watch, `RemoteBank`, and an 18-case conformance
suite that runs natively **and** in real browsers.

**M2 — complete.** The `redb` backend. Passes the conformance suite unmodified — the entire
backend cost one four-line test file. Plus crash-and-reopen tests that spawn a real child
process and `abort()` it, proving a returned commit survives process death and a transaction
killed before commit leaves nothing behind. **Data persists on desktop and mobile.**

**M3 — complete.** The IndexedDB backend. Same suite, Chrome and Firefox, plain and atomics;
the persistence case was negative-controlled. **Data persists on the web.**

**M4 — complete.** Chunked lazy values (`CCHK` pointer in `records`, pieces in `chunks`, each
sealed on its own), streaming `Writer`/`Reader` for `LazyLocker<Vec<u8>>`, orphan-chunk GC on
overwrite/delete/clear/abort, and a Linux `VmHWM` test proving peak RSS is bounded by the chunk
size rather than the value. Four new conformance cases (22 total) run on every backend. Benches
fixed the chunk-size default at **256 KiB** and showed LZ4 is free on f64 candle data.

**192 tests native. 22-case suite × 3 backends in browsers on both wasm lanes.**

### Remaining milestones

- **M5 — Quota, eviction, coherence.** ← *next*. `persist()` (explicit, never automatic),
  quota API, byte-budget LRU on a logical counter, BroadcastChannel cross-tab invalidation,
  Safari ITP 7-day policy.
- **M6 — Consumer readiness.** Docs, worked example, publish to crates.io.

## 6. How to run everything

```sh
cd ~/Documents/crossbank

cargo nextest run                      # native, all backends (192 tests)
cargo +1.97.1 clippy --workspace --all-targets --all-features   # see §7
cargo test --doc --workspace           # nextest does NOT run doctests
cargo bench --bench kv                 # native Criterion; not a CI gate
ci/bench.sh                            # same, plus optional --web --firefox
cargo bench --bench kv -- "chunk_sweep|lz4_f64"   # the M4 sizing benches only

# Real browsers. Both lanes must pass.
export CROSSBANK_WBG_RUNNER=<path to a wasm-bindgen-test-runner matching Cargo.lock>
export GECKODRIVER=<path to geckodriver>
ci/wasm-test.sh --plain   --firefox
ci/wasm-test.sh --atomics --firefox
```

`ci/wasm-test.sh` is the same script CI runs, so local and CI cannot drift.

## 7. Traps that will cost you a day each

Every one of these was hit and paid for already. Do not rediscover them.

1. **Never export `RUSTFLAGS`.** It *replaces* a `.cargo/config.toml` `rustflags` array rather
   than appending, silently unlinking shared memory. `ci/guard-rustflags.sh` enforces this.
   Coverage tools (`llvm-cov`, `tarpaulin`) export it — keep them native-only.
2. **A misconfigured wasm lane exits 0 having run ZERO tests.** That is how wise_apple's
   browser suite sat green for months having never executed. `ci/assert-tests-ran.sh` is the
   backstop; every test binary needs `wasm_bindgen_test_configure!(run_in_browser)`.
3. **A lane is not trusted green until it has been observed RED.** This is the standing rule.
   crossbank's CI went red twice on genuine defects before first passing.
4. **Local clippy is OLDER than CI's stable.** Run `cargo +1.97.1 clippy` before pushing or CI
   will reject code your machine approved.
5. **The atomics lane needs the TLS export link args** (`--export=__wasm_init_tls`,
   `__tls_base`, and the two `--export-if-defined`). Without them wasm-bindgen aborts with
   *"failed to prepare module for threading"*.
6. **Use `Uint8Array::from`, never `::view`.** `view` aliases wasm memory, which is a
   `SharedArrayBuffer` on the atomics lane, and IndexedDB throws `DataCloneError` on those.
   It fails **only** on the build that ships.
7. **`indexed-db` 0.5.0 is YANKED.** Use 0.4.2. `cargo search` misleadingly lists
   `0.5.0-alpha.1` as latest — check the crates.io versions endpoint for the `yanked` field.
   That crate's newest stable is from January 2025; if it stalls, the fallback is hand-rolled
   `web-sys` bindings.
8. **Never await anything that is not an IDB request inside an IndexedDB transaction.**
   `indexed-db` panics (`"Transaction blocked without any request under way"`), and wasm
   release builds are `panic=abort`, so it is an unrecoverable app kill. This is why
   transactions are closure-scoped and stage their writes in RAM.
9. **`std::time` compiles on wasm32 and panics at runtime.** LRU uses a logical counter.
10. **`cargo nextest` does not run doctests.** They need their own step.
11. **futures' `LocalPool` refuses a nested `block_on`.**

## 8. Where the detail lives

- **`PLAN.md`** — the full technical plan: all 25 decisions with rationale, architecture, the
  `Backend` trait, the storage model, milestones, and open questions.
- **`README.md`** — the public-facing pitch and current status.
- **`src/*/api.rs`** — each subsystem's cfg-free public surface. The module docs explain *why*
  each design is the way it is, not just what it does.
- **`crossbank-conformance/src/cases.rs`** — the behavioural spec. If a behaviour is not in
  here, it is not guaranteed.
- **`tests/spike_*.rs`** — the M0 spikes. They still run in CI and still guard their findings.

## 9. First thing to do when resuming

1. Read `PLAN.md`.
2. Run `cargo nextest run` and one browser lane, and confirm green before changing anything.
3. Start M5: byte-budget LRU on a logical counter for `Policy::Evictable` lockers, the
   quota API, and BroadcastChannel cross-tab invalidation. `Bank::persist()` already exists —
   never call it implicitly on open.
4. Trap 8 still applies to any IndexedDB work: every backend method must be **one**
   `db.transaction(...).run(...)` awaiting only IDB requests.
5. Safari CI (`safaridriver` on macOS runners) is still a follow-up. Safari has **zero**
   coverage today, and its ITP deletes IndexedDB after 7 days without user interaction,
   which is a product-level risk M5 must handle deliberately.
