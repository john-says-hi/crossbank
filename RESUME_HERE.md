# RESUME HERE — the original crossbank plan and why this project exists

**If you have been told to "find the original crossbank plan and resume work", this is that
file.** Read it end to end before touching code. `PLAN.md` next to it holds the full technical
plan; this file holds the *purpose*, the agreements, and the things that will cost you days if
you rediscover them the hard way.

- **Repo:** `github.com/john-says-hi/crossbank` (public)
- **Local checkout:** `~/Documents/crossbank`
- **Owner:** John (`john-says-hi`)
- **Started:** 2026-08-15 · **Last worked:** 2026-08-21
- **State:** **M0–M6 complete and 0.1.0 is tagged** — `v0.1.0` on GitHub, and **not**
  published to crates.io: `cargo publish --dry-run` passes and `cargo publish` has
  deliberately not been run, because that is John's call. CI is fully green on GitHub,
  every lane including Safari, which is now required. Includes a Phase 3 performance pass
  (see `PLAN.md` → Performance) and the release pass in §5.

---

## 1. What crossbank is

**Local, on-device key/value storage in pure Rust — a direct replacement for Flutter's Hive
(`hive_ce`).** One API that stores data on every platform Flutter ships to — Linux, macOS,
Windows, Android, iOS, and **the web**.

It has **no network code, no server, no sync and no cloud**, and it never will. That is not
an omission to be filled in later; it is the whole shape of the thing. If a future task
sounds like "add sync", it belongs in a different crate.

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
lockers, closure-scoped transactions, bounded watch, `BankHandle` (named `RemoteBank` until M6), and an 18-case conformance
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
fixed the chunk-size default at **256 KiB** and showed LZ4 is free on f64 candle data. A Hive CE
comparison on identical byte workloads is recorded in `PLAN.md` → Performance: crossbank reads
and big values win, Hive's non-fsync puts win, `transact` is the answer for bulk writes.

**At that point: 192 tests native, and the 22-case suite × 3 backends in browsers on both
wasm lanes.** (For the counts at HEAD, see the release pass below.)

**M5 — complete.** Quota (`Bank::usage`, `Bank::is_persisted` beside the existing
`persist()`), a byte-budget LRU for `Policy::Evictable` lazy lockers on a bank-wide logical
tick, opt-in `BroadcastChannel` cross-tab coherence with `Event::Stale`, and opt-in
`Commit::Deferred` write coalescing with `flush` / `Bank::flush_all`. Safari's ITP 7-day rule
is answered in prose (README → Web caveats), because nothing in code can answer it.

**M6 — complete.** Consumer readiness, and the crate is now a **0.1.0 candidate**.

- **Two Phase 3 review must-fixes landed first.** `Resident::prior` answered from one
  handle's RAM index while two handles on one name are legal and never sync, so a handle
  overwriting a key another had chunk-written orphaned its chunks forever — `Inner` now
  carries a one-way `name_shared` flag and `prior` refuses to answer once the index is not
  authoritative (on wasm, that includes coherence being off). And `ChunkPointer::parse` sized
  two allocations from unvalidated stored numbers; it now bounds both.
- **`RemoteBank` is now `BankHandle`** (`remote.rs` → `handle.rs`, `Bank::remote()` →
  `Bank::handle()`). Deprecated aliases keep the old spellings compiling; `into_service()` is
  unchanged.
- **Per-locker filter chains.** `LockerConfig::with_chain`, with the chain id persisted as
  `chain::{locker id}` in `meta` and enforced on every later open. `LockerConfig` is no
  longer `Copy`.
- **README rewritten** to say what crossbank is in its first paragraph, with a
  Hive-to-crossbank mapping table and a Durability & performance section.
- **Publish readiness.** Version 0.1.0, `exclude` list, `CHANGELOG.md`, 13 doc examples where
  there had been **zero**, and `examples/{settings,candles,flush_on_pagehide}.rs`.

**At M6: 375 tests native, 14 doctests, 153 per wasm lane. The conformance suite × 3
backends in browsers, plus browser-only coherence tests.**

**Post-M6 — one locker name is one open locker.** `Bank::locker` handed out an independent
handle per call, so two handles on a name served each other stale reads with no error — the
single biggest accuracy risk for the Hive-replacement goal, since `Hive.box(name)` is a
process-wide singleton. Every handle on a name is now a view of one open locker. See traps
16, 27 and 35, and `PLAN.md` → Known limitations.

**Release pass (2026-08-21) — the 0.1.0 closeout.** Everything below landed on `master`
before the tag, and the whole of CI went green on GitHub on the first run (lint, msrv,
native ×3 OSes, mobile-check, all four wasm lanes, scale, web-e2e ×3 engines) with the
Safari lane going red and then green, which is what earned it its `required` status.

- **Value ids are no longer reused after a reopen.** The chunk counter was saved from the
  number a writer *took* rather than the number the bank had *reached*, so a slow write
  could push it backwards and a reopened bank handed the same id out twice — two values
  sharing one set of chunks, and deleting either deleting both.
- **One locker name is one shared locker state**, with `Hive.box(name)` semantics: one
  resident map or index, one staged batch, one watcher set; a second open must agree on
  type, kind and config; `close()` on any handle closes them all; and an eager value type
  is now `T: Send + Sync` because the shared map is held type-erased. See trap 16.
- **The race in the shared-open path is closed.** The registry check happened before the
  awaits, so two overlapping opens on one name each built their own state and the second
  registration hid the first. The name is claimed under the lock *after* the awaits now.
  See trap 39.
- **An eager `delete` after `Event::Stale` really deletes.** "Not resident" was being read
  as "not stored"; a stale key is exactly a stored key this tab holds no value for.
- **`delete_bank` on a still-open native bank is refused**, not attempted — unlinking under
  a live fd lost every later write silently on Unix.
- **The LRU tick clock is seeded from the `lru::` records at open**, so a reopened bank
  cannot re-issue a tick already recorded and shed the wrong key.
- **`LazyLocker::get_or` / `get_or_by`** — Hive's `get(key, defaultValue:)` is used on a
  `LazyBox` as readily as on a `Box`.
- **Seven new conformance cases** covering Hive-surface behaviour (delete-event fidelity
  among them), taking `CASE_COUNT` to 61.
- **MSRV is 1.90**, which is a number that has actually been run — the old `1.85` was
  fiction and failed at resolve time on `redb`. See trap 36.
- **CI:** a `msrv` job, `cargo publish --dry-run` and `cargo-semver-checks` in `lint` (the
  latter `continue-on-error` until there is a published baseline), `--lib` on the mobile
  check lanes, and the Safari lane flipped to required.

**379 tests native, 14 doctests, 154 per wasm lane. `CASE_COUNT` is 61, and
`crash_recovery` spawns 5 real child processes. The conformance suite × 3 backends in
browsers, plus browser-only coherence tests.**

### Remaining work

- **`cargo publish` 0.1.0 for real.** Not run, on purpose — John's call. When it lands,
  remove `continue-on-error: true` from the `cargo-semver-checks` step in `lint` in the
  same commit: it only errors today because there is no published baseline to diff
  against (trap 34).
- **The wise_apple migration is the next task, and it does not happen here.** The
  Hive-shaped Dart shim backed by crossbank belongs in wise_apple's own repo (§2, §4).

## 6. How to run everything

```sh
cd ~/Documents/crossbank

cargo nextest run                      # native, all backends (379 tests)
cargo +1.97.1 clippy --workspace --all-targets --all-features   # see §7
cargo test --doc                       # nextest does NOT run doctests (14 of them)
cargo run --example settings           # the Hive `Box` shape, end to end
cargo run --example candles            # lazy: transact, Writer/Reader, Evictable
cargo publish --dry-run                # must pass; do NOT publish without John
cargo bench --bench kv                 # native Criterion; not a CI gate
ci/bench.sh                            # same, plus optional --web --firefox
cargo bench --bench kv -- "chunk_sweep|lz4_f64"   # the M4 sizing benches only
ci/bench.sh --hive                     # + Hive CE comparison (bench/hive_ce, needs dart)

# Real browsers. Both lanes must pass.
export CROSSBANK_WBG_RUNNER=<path to a wasm-bindgen-test-runner matching Cargo.lock>
export GECKODRIVER=<path to geckodriver>
ci/wasm-test.sh --plain   --firefox
ci/wasm-test.sh --atomics --firefox
ci/wasm-test.sh --plain   --safari     # macOS only; sudo safaridriver --enable first
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
12. **`dyn_into::<web_sys::StorageEstimate>()` can never succeed.** WebIDL
    *dictionaries* have no JS constructor, so the `instanceof` check behind
    `dyn_into` always fails — `usage()` silently returned `None` on every
    browser until a real browser test caught it. Read dictionary fields with
    `js_sys::Reflect::get`. The same applies to any other web-sys dictionary
    type.
13. **A `Closure` must be unregistered, not merely dropped.** `Bank::close`
    clears `onmessage` *and* drops the closure. Dropping it while the channel
    still points at it leaves the browser calling into freed memory.
14. **LZ4 will defeat a lazy "incompressible" test payload.** A cheap
    multiply-shift pattern compressed under the 4 KiB coherence inline limit,
    so the "too large to carry" case passed for the wrong reason. Use a full
    32-bit LCG.
15. **Nothing flushes `Commit::Deferred` for you.** No timer, no task, no
    destructor — `Drop` cannot await, and a closing tab would not run one. The
    consumer flushes from `pagehide` / `visibilitychange:hidden` on the web and
    from the app's stop hook natively.
16. **Every handle on one locker name is a view of ONE open locker.**
    `Bank::locker` / `lazy_locker` used to hand out an independent handle per
    call — its own resident values, its own key index — and the two never
    synchronised, so a `get` through one could answer with a value the other
    had already overwritten. Silent, and exactly what a Hive-shaped shim
    (`Hive.box(name)` is a process-wide singleton, called at hundreds of sites)
    would hit. They now share one `Inner`, one resident map or index, one
    staged batch, one watcher set. Consequences to know: a second open must
    agree with the first (different value type or container kind →
    `SchemaMismatch`; different `LockerConfig` → `InvalidConfig` naming the
    field); `close()` on any handle closes the locker for **all** of them, as
    `box.close()` does; and an eager value type now needs `Send + Sync`,
    because the bank holds the shared map type-erased and `Arc::downcast` is
    defined only for `Arc<dyn Any + Send + Sync>`. This is what replaced the
    old `Commit::Deferred` single-handle guard: two deferred handles used to
    keep two staging buffers over one name and whichever flushed last silently
    overwrote the other, so the second open was refused. Sharing removes the
    hazard, so the guard is gone rather than merely unreachable.

17. **A drained batch must be restaged on every failure path.** `transact`
    absorbs the staged deferred batch so both ride in one commit; if anything
    after the drain returns `Err`, the batch has to go back exactly where it
    was or it is gone. Same rule in `flush_locked`. Every early return between
    a `take_staged()` and a landed commit needs a `restage`.
18. **Never plan an eviction without a `keep` set.** A commit that evicts a key
    it is also writing GCs the old chunks while writing new ones, and the new
    ones are then unreachable forever. `budget_ops` takes the batch's own keys;
    `tests/deferred_batches.rs` asserts the `chunks` table holds nothing that
    no record points at.
19. **A wasm use-after-free does not go red.** Removing `Drop for Coherence`
    and rerunning the browser lane passes, every time — freeing a `Closure` the
    `BroadcastChannel` still points at breaks nothing observable until much
    later. The negative control for that lives natively, in
    `src/coherence/native.rs`, where the native half carries the same
    close-on-drop contract purely so it can be tested.
20. **A lane must assert its EXPECTED test count, not merely a nonzero one.**
    `ci/expected-tests.txt` holds a per-lane number (108 for every lane at
    a25ebfe) and `ci/wasm-test.sh` passes it to `ci/assert-tests-ran.sh`.
    "At least 1" caught a lane that ran *nothing*, but not a lane that quietly
    stopped compiling one suite in and ran 40 instead of 108. **Adding test
    cases means bumping these numbers in the same commit** — run the lane and
    copy the "OK: N test(s)" figure. More than expected always passes; fewer
    always fails. The same file also documents the Safari lane's count.
21. **`ci/wasm-test.sh` now refuses a mismatched runner up front.** It compares
    `wasm-bindgen-test-runner --version` against the `wasm-bindgen` version in
    `Cargo.lock` and prints the exact `cargo install --locked` line to fix it,
    instead of letting the run die mid-way on a schema-version error.
22. **Locker `close()` is async now.** It flushes first and closes even when
    the flush fails, returning the flush error. A bare `locker.close();` is a
    dropped future that does nothing.
23. **redb 4 has no `Durability::Eventual`.** That variant was removed; the
    relaxed level is `Durability::None`, and redb's own docs are explicit that
    such a commit "will not be persisted to disk unless followed by a commit
    with `Durability::Immediate`". That is not a soft "sometime later" — make
    `RedbBackend::flush` a no-op and the eventual-durability crash test comes
    back with *nothing at all* in the file. The empty Immediate commit in
    `flush` is load-bearing, and the negative control proves it.
24. **The conformance suite has TWO case lists.** `__for_each_case!` emits the
    tests and `__count_cases!` feeds the arity guard, and they are separate
    macros in `crossbank-conformance/src/lib.rs`. Adding a case to only one of
    them fails the guard with a count that is off by exactly the number you
    forgot. Add to both, and bump `CASE_COUNT`, and bump every lane in
    `ci/expected-tests.txt` by the number of backends the suite runs in the
    browser (two, so +2 per case).
25. **A test sized against a page constant stops testing when the page grows.**
    `opening_pages_past_a_single_scan_page` wrote 600 keys against a hard-coded
    page of 256. The moment backends were allowed to advertise their own page
    size and redb/memory went to 1024, it stopped crossing a page boundary at
    all — still green, testing nothing, exactly the shape of trap 2. Size such
    a fixture **from** `backend.scan_page_size()`, never from a literal.
26. **A RAM index may over-claim presence, never absence.** The write path uses
    the key index to skip the read-before-write that finds chunks to GC
    (`locker::inner::Prior`). A key wrongly believed present costs one wasted
    read; a key wrongly believed *absent* orphans its chunks permanently. The
    trap is that a staged `Commit::Deferred` delete removes the key from the
    index while the record is still stored — so `Resident::prior` refuses to
    answer at all while anything is staged. `Writer::finish` was a second
    instance: it stored a fully chunked record and never indexed the key.

27. **A RAM index may be answering for storage someone else is writing.**
    Trap 26 is only half the story. The index can only prove absence while
    nothing this handle will never hear from can write the same records. Two
    handles on one name used to be exactly that — they were independent
    objects with independent indexes — and handle B overwriting a key handle A
    had chunk-written would skip the GC and orphan A's chunks *forever*. That
    half is gone: one name is one locker with one index (trap 16), so there is
    no second in-process index to go stale. What remains is **cross-tab** (two
    tabs with coherence off, where another tab may have chunk-written the very
    key this one is about to overwrite) and the **two-banks-over-one-backend**
    arrangement `Bank::with_backend` documents as unsupported.
    `Inner::name_shared` keeps that role — it is set on the lockers the bank
    fabricates for maintenance, which carry no index at all — and is still
    never cleared. Anything that lets a stale index prove absence must consult
    `Inner::index_is_authoritative`, not just "is anything staged".

28. **Never size an allocation from a number you read off storage.**
    `ChunkPointer::parse` took `n_chunks` and `total_len` at face value and
    `read_chunks` reserved from both. A corrupt pointer claiming `u32::MAX`
    chunks asks for gigabytes — and a wasm release build is `panic=abort`, so
    the allocation failure is an unrecoverable app kill, not an error anyone
    can catch. The chunk size is not in the pointer, so the exact check is
    impossible; the two bounds that hold regardless are `total_len <=
    MAX_DECODED_BYTES` and `n_chunks <= total_len` (a chunk holds at least one
    byte). `n_chunks == 0` stays legal: a `Writer` closed without a single
    `write` stores exactly that.

29. **`LockerConfig` is no longer `Copy`.** A locker can carry its own
    `Arc<FilterChain>`. Every call site that passed the same config to two
    `*_locker_with` calls now needs `.clone()`. It compares by chain **id**,
    not by `Arc` pointer, because the id is the thing that gates format
    compatibility.

30. **A filter chain is persistent, not a runtime option.** The id goes into
    `meta` as `chain::{locker id}` at first open, and a later open under a
    different one is `Error::SchemaMismatch`. A store written before that
    record existed has none — treat that as "write it now", never as a
    mismatch, or every existing bank stops opening.

31. **`BankConfig::at`, not `BankConfig::path`.** The native constructor is
    spelled `at`. Doc examples that guessed `path` compiled nowhere.

32. **`src/bin/` had to be excluded from the published package.**
    `crash_child` needs `futures`' `executor` feature, which only the
    *dev*-dependency turns on, and publishing strips dev-dependencies — so
    `cargo publish --dry-run` failed to verify the tarball until `src/bin/`
    joined `exclude`. Any future test-helper binary hits the same wall.

33. **`cargo test --doc` ran ZERO tests for the whole project's life.** It was
    green the entire time, which is trap 2 wearing a different hat. There were
    simply no code blocks in the crate. If you add a doc example, run
    `cargo test --doc` and check the *count* went up, not just that it passed.

34. **Two CI lanes that look green on paper and are not.** First: `cargo check
    --workspace` compiles `src/bin/` but *not* dev targets, so it builds
    `crash_child` without the `futures` `executor` feature the dev-dependency
    turns on — and resolver 2 does not unify that feature in from the dev
    side. The mobile Android/iOS lanes would have failed to compile for that
    reason alone; any lane that does not build dev targets needs `--lib`. This
    is trap 32 reached from the check side instead of the packaging side.
    Second: `cargo-semver-checks` has no baseline until the first
    `cargo publish`, and with nothing to diff against it **errors** rather
    than no-opping — so it would have failed `lint`, and `ci-green`'s `needs`
    would have failed the whole first push. It runs `continue-on-error: true`
    until 0.1.0 is on crates.io; drop that in the commit right after the first
    publish.

35. **A sink stored in the state it points at is a leak, and on the web a
    hang.** The coherence registration moved from the locker handle onto the
    shared `Resident` (one registration per name, so a second handle does not
    double-apply another tab's news and dropping the first does not stop the
    others hearing it). But `LazySink` held `Arc<Resident>`, and the `Resident`
    now held the sink — a cycle, so the locker, its `Inner` and the backend
    underneath it were never freed. Natively that is a leak nobody sees. On the
    web it is an **IndexedDB connection that never closes**, and the next
    `delete_bank` blocks forever: `web_coherence` did not fail, it timed out at
    180 s. `LazySink` holds a `Weak<Resident>` and upgrades in `apply`.
    Whenever shared state starts owning something that points back at it, check
    which way the `Arc`s run.

36. **The declared MSRV was a number nobody had ever run.** `Cargo.toml` said
    `rust-version = "1.85"` from the first commit, and no lane had ever built
    on 1.85, so the manifest was promising a toolchain that cannot compile the
    crate. The first `cargo +1.85 check` did not fail on our code at all — it
    failed at *resolve* time, before compiling a line, with `redb@4.2.0
    requires rustc 1.90`. That is the part worth remembering: cargo enforces
    every **dependency's** own `rust-version`, so the floor is the maximum of
    ours and theirs, and a dependency can raise it in a patch release without
    touching anything we wrote. 1.90.0 is the lowest toolchain that passes, so
    that is now the declared number. The `msrv` CI job pins the same version
    and runs `cargo check --workspace --lib --all-features` on every push, so
    the manifest and the truth cannot drift apart in silence again — if a
    future dependency lifts the floor, that lane goes red and tells you the
    new number instead of leaving it for a consumer to discover.

37. **Not every Hive-shaped operation can BE a conformance case.** A case is
    handed an already-open `Backend`; `delete_bank` takes a `BankConfig` — a
    *location* — and there is no backend-generic way to spell one, so it lives
    in `tests/delete_bank.rs` natively and `tests/web_delete_bank.rs` on the
    web. And the two halves are not the same test: the open-bank refusal
    (`Err(InvalidConfig)`) is native-only by construction, because the registry
    behind it is `#[cfg(not(target_arch = "wasm32"))]` and an open IndexedDB
    connection does not *fail* `deleteDatabase`, it **blocks** it. A web test
    asserting that refusal would not go red — it would hang the lane to its
    180 s timeout, which is trap 19's shape again: on the web a wrong answer
    can be indistinguishable from no answer.

38. **A watch case that expects an event too few HANGS, it does not fail.**
    `events.next().await` on a stream that will never yield again blocks
    forever, so a case written as "assert exactly these N events" proves
    over-emission and times out on under-emission. Arrange the writes so both
    directions come out as a *mismatch*: interleave the writes that must be
    filtered (a leak then names the intruder in the very next `next()`), and
    end with a terminating write on a key that IS watched, so a missing event
    shifts the terminator forward instead of leaving nothing to poll.

39. **A registry check before an `.await` is not a claim.** `Bank::locker` /
    `lazy_locker` asked the open-locker registry whether the name was already
    open, dropped the lock, and only *then* awaited `prepare` and the locker
    open. Two callers on one name could both pass that check — natively
    through a shared `Bank` on two threads, on the web through two futures
    joined together, where the second is polled while the first is suspended
    in a backend read. Each then built its own `Inner`, resident state and
    index, and `register_open`'s `HashMap::insert` meant the *second* one won:
    the first locker stayed alive, invisible to `is_locker_open`,
    `delete_locker` and the next `locker(name)`, still serving reads and
    writes to whoever held it. Worse, it was trap 27 back from the dead — two
    indexes on one name, each authoritative, each able to prove absent a key
    the other had chunk-written, so an overwrite skipped the read that finds
    the old chunks and orphaned them forever. The fix is that the last word on
    a name is spoken **under the lock, after the awaits**: `claim_open` either
    inserts or reports the winner, and the loser throws away the locker it
    just opened (it has done nothing but read) and shares the winner instead.
    `Inner::mark_name_shared` came back as the belt — both `Inner`s are marked
    when a race is detected, so even a path that got past the resolution would
    read before writing. The general rule: **a check and the action it
    authorises must happen under one hold of the lock, or the check is only a
    hint.** Two of the tests for it are in `tests/shared_handles.rs` behind a
    backend decorator that suspends in `scan`, because the memory backend's
    futures are all ready on their first poll and cannot produce the window at
    all; the browser needs no such help (`tests/web_shared_handles.rs`).
40. **macOS runners are bash 3.2, and an empty array under `set -u` is an
    unbound variable there.** Bash 4.4+ tolerates `"${arr[@]}"` on an empty
    array; 3.2 aborts with *"TOOLCHAIN[@]: unbound variable"*. The Safari
    lane's first-ever run died on exactly that, in `ci/wasm-test.sh`, before a
    single test executed — every other lane runs on Ubuntu's bash 5, so it had
    never shown. Use `${arr[@]+"${arr[@]}"}` for every empty-able array in
    `ci/*.sh` (`wasm-test.sh` and `bench.sh` are converted; `web-e2e.sh`
    already was), keep the shebang on stock `bash`, and stay off bash-4-only
    features — `declare -A`, `${var,,}`/`${var^^}`, `mapfile`/`readarray`,
    `&>>`, negative array indices, `|&`. Worth naming the other half of this:
    it is also what it took to see the Safari lane **red for the first time**,
    which by trap 3 is the only way it becomes trustworthy green.

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
2. Run `cargo nextest run`, `cargo test --doc` and one browser lane, and confirm green before
   changing anything.
3. Every milestone is done and **0.1.0 is tagged** (`v0.1.0` on GitHub). What is left is
   **the crates.io publish**, and that is John's call — run `cargo publish --dry-run`,
   never `cargo publish`, without being told to. If you are here to change the API
   instead, 0.x still allows breaking changes, but they belong in `CHANGELOG.md` under
   `[Unreleased]` and they need a version bump before the next tag.
4. Trap 8 still applies to any IndexedDB work: every backend method must be **one**
   `db.transaction(...).run(...)` awaiting only IDB requests.
5. Safari CI has a lane: `wasm-safari` on `macos-latest`, plain lane only, running
   `ci/wasm-test.sh --plain --safari`. It is **required** as of 2026-08-21 — no
   `continue-on-error`, and it is in `ci-green`'s `needs`. It earned that: it went red on
   its first run (trap 40) and green on the next, which is the only way a lane becomes
   trustworthy green (trap 3). Safari's ITP still deletes IndexedDB after 7 days without
   user interaction, which remains a product-level risk.
