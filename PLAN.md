# crossbank — build plan

**Status: M0–M6 complete; 0.1.0 candidate.** 331 tests green natively — the full conformance
suite against **both** the memory and `redb` backends, crash-and-reopen tests that kill a real
process, and a peak-RSS test that streams 8 MiB through 64 KiB chunks. The conformance suite
also passes against **IndexedDB** in Chrome and Firefox on both wasm lanes (plain and
atomics), alongside a browser-only cross-tab coherence test. **Data persists on
desktop, mobile, and the web, large lazy values no longer have to fit in RAM, and storage
pressure has an answer.** M6 closed it out: the README says plainly what crossbank is, every
public type carries a doc example, three worked examples run, `CHANGELOG.md` starts, and
`cargo publish --dry-run` passes.

> Resuming this project? Start with **[RESUME_HERE.md](RESUME_HERE.md)** — purpose, working
> agreements, current state, and the known traps. This file is the technical plan.

## Context

wise_apple's Rust hub owns real persistent state — the encrypted vault, the macro-data
bundle cache, BYOD catalogs and imported series, Lab snapshots, instrument history. It
cannot write any of it. Every read and write is marshalled over a rinf signal to Dart, which
parks the bytes in a Hive box whose own service class calls itself *"a deliberately dumb
binary key/value responder. Rust owns all schemas."*

No mature crate fills that gap. `bevy_pkv` is closest and its author names it outright:
localStorage on web because "IndexedDb… is complicated, and I wanted a simple synchronous
API." That caps it near 5–10 MB against candle series in the hundreds of megabytes.
Everything else is the web half only (`idb`, `indexed-db`, `indxdb`), native only (`redb`,
`sled`, `fjall`), far too green (`netabase_store` at 0.0.8), or a whole database (SurrealDB).

**crossbank** fills it: Hive's ergonomics, pure Rust, real IndexedDB on the web. Standalone
and public, with zero knowledge of wise_apple. It should eventually replace Hive there — but
only after proving itself on its own terms.

### Correction on record

Hive does **not** use SQLite. It is pure Dart, explicitly Bitcask-inspired: an append-only
log of frames `[len u32][key][value][crc32]`, delete appends an empty-value frame, the index
is rebuilt by parsing the whole file at open, `compact()` drops dead frames. Web uses
IndexedDB. That is precisely *why* Hive has eager and lazy boxes — it is the Bitcask keydir
pattern.

We copy Hive's **architecture and API, not its bytes**. Two of its format's properties we
explicitly reject: deleted values stay readable until someone calls `compact()`, and keys are
plaintext even in encrypted boxes.

---

## Decisions

| # | Decision | Choice |
|---|---|---|
| 1 | API shape | Eager `Locker` (sync reads) + `LazyLocker` (async reads) |
| 2 | Value model | serde-typed, pluggable codec |
| 3 | Big values | Auto-chunking + streaming Writer/Reader |
| 4 | Hive migration | None. Clean start |
| 5 | Native backend | `redb` 4.1 |
| 6 | Multi-tab | Broadcast invalidation; native is in-process only |
| 7 | Reactivity | `watch()` / `watch_key()`, bounded fan-out |
| 8 | Encryption | The pluggable `Filter` trait (a `Cipher` trait was never built), no crypto shipped |
| 9 | Storage full | `Evictable { max_bytes }` per locker + quota API |
| 10 | Atomicity | **Closure-scoped** transactions, staged write-set |
| 11 | Key scans | Ordered keys: prefix, range, reverse, limit |
| 12 | Tests | One conformance suite × every backend |
| 13 | Names | `Locker`, `LazyLocker`, root handle `Bank` |
| 14 | Build order | Walking skeleton first |
| 15 | License | MIT OR Apache-2.0 |
| 16 | CI breadth | Desktop + web per PR; mobile smoke nightly |
| 17 | Dart side | Rust crate only |
| 18 | Endgame | Invert the bridge — Hive-shaped Dart shim in wise_apple |
| 19 | Separation | Zero knowledge of wise_apple, Flutter, or rinf |
| 20 | Keys on disk | **Binary**, UTF-8 bytes; `&str` in the public API |
| 21 | Store layout | **Three fixed tables**; locker is a key prefix |
| 22 | Eager + big | **Refuse.** `ValueTooLarge` naming `LazyLocker`; budget at open |
| 23 | Send story | Boxed `CbFuture` alias + `BankHandle` (`RemoteBank` until M6), **at M1** |
| 24 | LRU clock | **Logical op counter.** `std::time` panics on wasm |
| 25 | IndexedDB crate | `indexed-db` **0.4.2** (0.5.0 is yanked) |

**Non-goals.** Not a SQL engine, document store, sync engine, or Hive format reader. No
Dart/Flutter package.

---

## M0 results

Everything below was measured, not assumed. Each claim has a test in the repo.

| Question | Answer |
|---|---|
| Is the headless runner cross-origin isolated? | **Yes.** `SharedArrayBuffer=true crossOriginIsolated=true` in Chrome and Firefox. The runner sets COOP/COEP by default; opt-out only. |
| Does IndexedDB persist across reopen? | **Yes.** 4 KiB written, closed, reopened on a fresh connection, byte-identical. |
| Does the shared-memory lane work? | **Yes.** nightly-2025-11-08 + `-Zbuild-std=std,panic_abort` + shared memory, 10 tests green. |
| Does 1 MiB survive IndexedDB under `--shared-memory`? | **Yes, via `Uint8Array::from`.** |
| Does key ordering match across backends? | **Only with binary keys.** String keys provably diverge. |
| Does redb build for mobile? | **Yes.** Android and both iOS targets `cargo check` clean. |

**Three things M0 corrected in this plan:**

1. **The atomics rustflags were incomplete.** Without `--export=__wasm_init_tls`,
   `--export=__tls_base`, and the two `--export-if-defined` TLS symbols, wasm-bindgen aborts
   with *"failed to prepare module for threading"*. Verified by omitting them.
2. **`indexed-db` 0.5.0 is yanked.** 0.4.2 (January 2025) is the newest usable release, and
   its API differs — `build_object_store` is on `Database`, via `evt.database()`. The crate's
   age is a standing risk; if it stalls, the fallback is hand-rolled `web-sys` bindings.
3. **Key ordering diverges between backends.** IndexedDB compares string keys by UTF-16 code
   units; redb and `BTreeMap` by UTF-8 bytes. They disagree above the BMP — one emoji
   reverses a range scan on web only. Fixed by storing binary keys.

Two CI guards exist because a green lane that proved nothing is worse than a red one, and
both were **negative-controlled**, not assumed:

- `ci/assert-tests-ran.sh` rejects a run that passed zero tests. A misconfigured wasm lane
  exits 0 — that is how wise_apple's browser suite sat green for months having never run.
- `lane_is_what_it_claims` compares `target_feature=atomics` against `CROSSBANK_EXPECT_ATOMICS`,
  so a silently-plain "atomics" job fails. Confirmed by forcing the flag on a plain build.

---

## API

```rust
let bank = Bank::open(BankConfig::at(path)).await?;   // caller names the location

// EAGER — decoded values resident in RAM, sync infallible reads
let settings: Locker<Settings> = bank.locker("ui_settings").await?;
let theme: Option<Arc<Settings>> = settings.get("theme");
settings.put("theme", dark).await?;

// LAZY — key index in RAM, values on demand
let candles: LazyLocker<Chunk> = bank.lazy_locker("candle_cache").await?;
let chunk = candles.get("BTCUSDT::0000001700").await?;
let ids   = candles.keys_with_prefix("BTCUSDT::");    // sync, index is in RAM
let window = candles.range("BTCUSDT::0000001700".."BTCUSDT::0000001800").await?;

// TRANSACTIONS — closure-scoped only. Writes stage in RAM, then one atomic commit.
candles.transact(|tx| async move {
    tx.put("BTCUSDT::chunk::0", &a)?;
    tx.put("BTCUSDT::manifest", &m)?;
    Ok(())
}).await?;

// STREAMING — never materialize a huge value
let mut w = candles.writer("BTCUSDT::full").await?;
for batch in batches { w.write_chunk(&batch).await?; }
w.finish().await?;

// Send + Sync handle for other threads (the ONLY usable API on wasm)
let handle = bank.handle();
wasm_bindgen_futures::spawn_local(bank.into_service());   // consumer pumps it
```

**Why closure-scoped transactions are mandatory, not stylistic.** `indexed-db` polls a
transaction body with a waker that panics if you await anything that is not an IDB request,
and aborts the transaction on a pending poll with no request in flight. Verified in 0.4.2:
`panic!("Transaction blocked without any request under way")`. Under `panic=abort` — which
wasm release builds use — that is an unrecoverable, message-less app kill. A free-floating
`tx` handle invites exactly that. The closure form makes it structurally impossible, and
staging writes in RAM means no foreign await can ever land inside a live IDB transaction.

---

## Architecture

```
   Bank / Locker / LazyLocker / Transaction / Writer / Reader / BankHandle
 ──────────────────────────────────────────────────────────────────────  public API
   Filter chain (codec · compression · CRC · cipher) · chunking
   RAM index · watch fan-out · eviction · coherence
 ──────────────────────────────────────────────────────────────────────  engine (portable)
                            trait Backend
 ──────────────────────────────────────────────────────────────────────
   memory (all)        redb (native)        IndexedDB (wasm32)
```

### The `Backend` trait

Shaped by one hard constraint: **no backend method may span a foreign await.** That rules
out any `begin_write() … commit()` handle shape, because IndexedDB cannot survive it. It also
rules out returning a `Stream` of scan results — an IDB cursor dies if user code awaits
between items. So the trait commits op-lists and pages scans:

```rust
pub type BFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

pub enum Op { Put { table, key, value }, Delete { table, key }, DeleteRange { table, range } }

pub trait Backend: 'static {
    fn get(&self, t: Table, key: &[u8]) -> BFut<'_, Option<Vec<u8>>>;
    fn get_many(&self, t: Table, keys: Vec<Vec<u8>>) -> BFut<'_, Vec<Option<Vec<u8>>>>;
    fn scan(&self, req: ScanRequest) -> BFut<'_, ScanPage>;   // returns a resume key
    fn commit(&self, ops: Vec<Op>) -> BFut<'_, ()>;           // all or none
    fn usage(&self) -> BFut<'_, Option<Usage>>;               // None on native
    fn flush(&self) -> BFut<'_, ()>;
}
```

Each backend satisfies this honestly: IndexedDB runs each method as exactly one
`transaction(...).run(...)` awaiting only IDB requests; redb runs the whole body
synchronously inside one `Offload::run_blocking` with no await between `begin_write` and
`commit`; memory is three `BTreeMap`s.

### Storage model — three fixed tables, never a version bump

`meta`, `records`, `chunks`, created once at IndexedDB version 1 and **never bumped again**.
Creating an object store requires a `versionchange` transaction, which fires on every other
open tab and force-closes their handles — so per-locker stores would make `bank.locker(name)`
a cross-tab disruption. Format migrations instead run as ordinary transactions against a
version stored in `meta`.

- key = `locker_id: u32 BE || 0x00 || user_key_utf8` — binary, ordered bytewise
- chunk key = `value_id: u64 BE || seq: u32 BE`; `value_id` is a persisted counter, **never a
  UUID** (avoids dragging `getrandom`'s wasm cfg burden onto every consumer)
- `clear()` is one `DeleteRange` over the locker prefix

Chunks are framed and compressed **per chunk, not per value**, so peak memory is
O(chunk_size) rather than O(value). This matters: wasm32 has a 4 GiB ceiling and linear
memory never shrinks, so one transient 500 MB allocation permanently raises the tab's RSS.

A `schema_tag` in `meta` prevents opening `Locker<A>` over data written as `Locker<B>` —
postcard is not self-describing and would decode garbage into a valid-looking `A`.

### The `Send` story

On wasm, `Bank` holds a `JsValue` and is `!Send`. A consumer whose trait requires `Send`
futures therefore **cannot use the ordinary API at all** — the proxy is not a convenience,
it is the only path, which is why it lands in M1.

- Public futures are a boxed alias — `Send` on native, unbounded on wasm. `MaybeSend` cannot
  bound an `async fn` return type, so boxing is not optional.
- `Error` is unconditionally `Send + Sync + 'static` and never carries a `JsValue`. JS errors
  are stringified at the boundary with the DOMException name kept as a typed enum.
- `Bank::handle()` returns a `Send + Sync + Clone` handle; `Bank::into_service()` is a future
  the *consumer* pumps on the owning thread. crossbank spawns nothing.
- Encode, compress and encrypt run on the **caller's** thread, never the pump. That keeps
  user code off the service loop, which makes re-entrancy impossible by construction.
- **Documented rule: the thread pumping `into_service()` must never block.** On wasm a
  `block_on` there traps outright.

---

## Testing

**The conformance suite is the product.** One set of async functions generic over `Harness`,
in its own crate, that every backend must pass identically. It names `Send` nowhere — the
moment it does, IndexedDB is out.

Cfg-selected emitter macros turn one list of case names into `#[test]` natively and
`#[wasm_bindgen_test]` on wasm. An arity guard asserts the number that ran equals the number
declared. Capability differences (memory does not persist) are handled *inside* a case with
`Caps`, never by a skip-list, so a skip is visible in the spec.

Under `panic=abort` there is no per-test isolation: never use `#[should_panic]`, assert on
`Err` variants instead, and never rely on `Drop` for cleanup. The plain lane is the
diagnostic lane; the atomics lane is a pass/fail gate.

Beyond the suite: a `Fault<B>` backend decorator injecting aborts, IO errors, quota
exhaustion, truncation and corruption at every op index; native crash-and-reopen via a killed
child process; and property tests using the memory backend as oracle. Quota is tested with an
injected budget per PR and a real 10 MB Firefox pref nightly.

`Fault<B>` **landed** (`crossbank-conformance/src/fault.rs`) and carries three cases of its
own. It is one-shot and aimed at an index into the op stream counted from `arm()`, which is
why a case arms it *after* opening its bank and locker — registration commits would otherwise
shift the index. `Brittle` in `tests/deferred_batches.rs` stays as it is: a backend that stays
broken across many commits is a different question.

**What `wasm_bindgen_test` structurally cannot cover.** That lane owns one page that never
navigates, so two claims are invisible to it: a real reload (a fresh wasm instance and heap
reading bytes an earlier instance wrote — reopening a `Bank` inside one page is a weaker
claim), and a `BroadcastChannel` message crossing between two *documents* rather than between
two `Bank`s in one. `examples/web_e2e_page.rs` plus `ci/web-e2e.sh` cover both with Playwright
on all three engines, nightly. `tests/web_multi_tab.rs` was deliberately **not** written:
`tests/web_coherence.rs` already covers the two-Banks-in-one-page half completely, and a
second file saying the same thing would be duplication rather than coverage.

### CI

Public repos get free unmetered runners on every OS — which is exactly why this is affordable
here and is not in wise_apple, a **private** repo whose allocation is exhausted and whose
every workflow is `workflow_dispatch`-only.

| Lane | What | When |
|---|---|---|
| lint | fmt, clippy, shellcheck | per PR |
| native | memory + redb, Linux/macOS/Windows, nextest + doctests | per PR |
| wasm | plain and atomics × Chrome and Firefox | per PR |
| wasm-safari | plain lane on `macos-latest` via `safaridriver` | per PR (non-blocking) |
| mobile-check | Android ×3, iOS ×2 `cargo check` | per PR |
| mobile persistence | write, kill, reopen on emulator/simulator | nightly |
| torture, crash, proptest | multi-GB, quota, fault matrix | nightly |
| scale | 100k keys, write / reopen / range, `CROSSBANK_SCALE=1` | nightly |
| web-e2e | real reload + two real tabs, chromium/firefox/webkit | nightly |

Edge is Chromium — same Blink, V8, and IndexedDB implementation — so the Chrome lane covers
it and no `msedgedriver` lane is warranted. Safari/WebKit is the only other engine, hence its
own macOS job. It is **plain lane only**: there is no headless Safari, and `SharedArrayBuffer`
under a WebDriver-driven Safari is unreliable, so an atomics lane there would be flaky rather
than informative.

Every wasm lane asserts a per-lane expected passing test count from `ci/expected-tests.txt`.
"At least one test ran" caught a lane that ran nothing; it does not catch a lane that quietly
stopped compiling a suite in. Adding cases means bumping those numbers in the same commit.

**Follow-up — flip the Safari lane to required.** It landed non-blocking so a first-run
infrastructure surprise on macOS cannot wedge every PR. Once John has seen **one green
`wasm-safari` run** on master, make both changes in a single commit to
`.github/workflows/ci.yml`:

1. delete `continue-on-error: true` from the `wasm-safari` job, and
2. add `wasm-safari` to `ci-green`'s `needs:` list.

Until both are done, a red Safari lane proves nothing about the merge — `ci-green` cannot
see it.

Lane rustflags live in `ci/wasm-atomics.toml`, selected with `cargo --config`. **Never** via
a `RUSTFLAGS` env var, which *replaces* a cargo-config array instead of appending and
silently unlinks shared memory. `ci/guard-rustflags.sh` enforces this.

---

## Milestones

**M0 — Spike and scaffold. ✅ COMPLETE.** See results above.

**M1 — Walking skeleton. ✅ COMPLETE.** Memory backend, the `Backend` trait above, three
fixed tables, binary keys, closure-scoped transactions, `Arc<T>` eager lockers with a budget
ceiling, `BankHandle` + `into_service()`, bounded watch, `schema_tag`, and an 18-case
conformance suite that runs natively *and* in a browser.
*Exit met: adding a backend now costs one four-line file. Both suite guards were
negative-controlled — a harness that lies about persistence fails, and a spec list out of step
with `CASE_COUNT` fails.*

**M2 — redb backend. ✅ COMPLETE.** Passes the conformance suite unmodified — the whole
backend cost one four-line test file, which is the suite's payoff. Crash-and-reopen tests
spawn a real child process and abort it. `Offload` was not needed: because `commit(Vec<Op>)`
takes a complete op list, every backend method is a single synchronous block with no await
inside, so a `WriteTransaction` can never be held across an await. Revisit only if profiling
demands it.

**M3 — IndexedDB backend. ✅ COMPLETE.** Same suite, Chrome and Firefox, plain and
atomics. `Bank::open` wires `Location::{Memory, Path, Web}`. The persistence
case was negative-controlled: a harness that lied about `persists_across_open`
went red in Firefox before the honest cap was restored.

**M4 — Big data. ✅ COMPLETE.** A lazy value whose postcard payload exceeds
`LockerConfig::chunk_size` is split into the `chunks` table behind a 26-byte `CCHK` pointer
in `records`; each chunk is sealed through the filter chain on its own. `put`, `get`, `delete`,
`clear`, `transact` and range reads all see through the pointer. `LazyLocker<Vec<u8>>::writer`
/ `reader` stream without materialising the value; `Writer` lives outside `transact()`, publishes
the pointer only at `finish()`, and `abort()` GCs its orphans, so an unfinished write leaves
the previous complete value readable. Overwrite and delete `DeleteRange` the old chunk prefix.
Exit criterion met: `tests/peak_rss.rs` streams 8 MiB in 64 KiB chunks and asserts `VmHWM`
growth stays far below the value size. Four new conformance cases run on every backend and
were negative-controlled (a broken `abort()` went red in Firefox before being restored).
Eager lockers still refuse oversized values.

**M5 — Quota, eviction, coherence. ✅ COMPLETE.** Four pieces landed, each with its own
conformance case, each negative-controlled.

*Persistence and quota.* `Bank::persist()` asks `navigator.storage.persist()` on wasm and
returns whether the origin is persistent; native returns `Ok(true)`; never called on open.
Firefox shows a permission prompt and the future waits on the user; Chromium decides
silently — so it must stay off the startup path. `Bank::is_persisted()` is the read-only
twin, safe anywhere. `Bank::usage()` wraps `Backend::usage`; the figure is not comparable
across backends and says so (origin-wide and deliberately coarsened on the web, file size on
`redb`). Fixing the IndexedDB half found a real bug: it cast the resolved estimate to
`web_sys::StorageEstimate`, a WebIDL dictionary with no JS constructor, so `dyn_into`'s
`instanceof` check could never succeed and `usage()` returned `None` on every browser.

*Byte-budget LRU.* `Policy::Evictable { max_bytes }` on a **lazy** locker (an eager one still
refuses the combination). One `meta` record per key,
`lru::{locker_id}::{key} -> [tick u64][bytes u32]`, written in the same commit as the put it
describes, so accounting can never disagree with the data. Ordering is a bank-wide logical
tick — `std::time` panics at runtime on wasm32 — persisted as a high-water mark by every
allocating commit. A `get` bumps the tick in RAM only and the bump rides along with the next
write, capped at 64 per commit. Budgets count payload bytes, not the on-disk footprint, which
compression and chunk framing make backend-dependent. `Event::Evicted`,
`LazyLocker::budget_used` and `evict_to` are the surface.

*Cross-tab coherence.* Opt-in `BankConfig::with_coherence`, one `BroadcastChannel` per bank
named `crossbank::{db name}`. Each commit's news is derived from its op list, so every write
path is covered by construction, and posted only after the commit lands; sealed values ride
along up to 4 KiB. A receiving lazy locker updates its index; an eager locker decodes an
inline value or drops its resident copy with the new `Event::Stale`. The callback is a plain
`Closure`, never inside an IDB transaction, kept alive by the bank and dropped on `close()`.
Own posts are ignored via a `Math.random` instance id. Native accepts the flag and does
nothing — redb's exclusive file lock means there is no second process to stay in step with.

*Write coalescing.* `Commit::Deferred { after }` on `LockerConfig`. Writes stage in RAM and
commit in batches; an eager locker updates its resident value at stage time, a lazy locker
its index plus a staged overlay, so a handle always sees its own writes. `flush`, `pending`,
`pending_bytes` on both locker types, `Bank::flush_all` across all of them, and `close`
(locker and bank) flushes first and still closes if the flush fails. **The consumer owns the
flush** — crossbank spawns nothing — which is why it is never the default and why
`examples/flush_on_pagehide.rs` exists.

*Safari.* Its ITP deletes script-writable storage after seven days without user interaction.
Nothing in code can answer that, so it is answered in prose: see README → Web caveats.
Safari still has zero CI coverage.

**M6 — Consumer readiness. ✅ COMPLETE.** Five pieces, plus the two Phase 3 review
must-fixes that had to land first.

*The review fixes.* `Resident::prior` answered from one handle's RAM index, but two handles
on one locker name are legal and never sync — so a handle overwriting a key another handle
had chunk-written skipped the GC and orphaned its chunks forever, and the same held across
two tabs with coherence off. `Inner` now carries a one-way `name_shared` flag, and `prior`
refuses to answer once the index is no longer authoritative. Separately,
`ChunkPointer::parse` took `n_chunks` and `total_len` straight off storage and both sized an
allocation; a pointer must now declare no more bytes than the envelope ceiling and no more
chunks than bytes, and `read_chunks` fetches in fixed groups of 64.

*README and crate docs.* The first paragraph now says what a reader most needs to know:
crossbank is local, on-device storage, a direct replacement for Hive, with no network code,
no server, no sync and no cloud. Stale claims are gone (`Cipher` was never a trait; the LRU
is real since M5), a Hive-to-crossbank mapping table covers every call wise_apple's
inventory found, and a Durability & performance section carries the headline numbers.

*`RemoteBank` → `BankHandle`.* The type was never about anything remote. `remote.rs` becomes
`handle.rs`, `Bank::remote()` becomes `Bank::handle()`, and deprecated aliases keep both old
spellings compiling.

*Per-locker filter chains.* `LockerConfig::with_chain`. The chain id is written to `meta` as
`chain::{locker id}` at first open and enforced on every later one, so a locker can never be
reopened under a chain that would decode its bytes into plausible garbage. A store written
before the record existed has none, which is not a mismatch: the id is written on that open
and enforced from the next. `LockerConfig` lost `Copy` as a result — a chain is a
`dyn Filter` list behind an `Arc`.

*Publish readiness.* `publish = false` off, version 0.1.0, an `exclude` list, `CHANGELOG.md`,
13 doc examples where the doc-test target previously ran **zero**, and
`examples/{settings,candles,flush_on_pagehide}.rs`. `cargo publish --dry-run` passes.

---

## Verification

- `cargo nextest run` — native conformance, all backends.
- `ci/wasm-test.sh --plain|--atomics --chrome|--firefox` — identical locally and in CI.
- Every lane must be observed **red on a deliberately broken assertion before it is trusted
  green**. That is the standing rule, not a one-time exercise.
- The end-to-end proof that matters: write a multi-GB series in Chrome, close the tab,
  reopen, read it back byte-identical.

---

## Performance

Dated snapshot, not a CI gate. Reproduce with `cargo bench --bench kv` and
`ci/bench.sh`. Machine: 12th Gen Intel Core i9-12900HK, Linux 7.0.11 x86_64,
2026-08-20.

| Workload | Backend | p50-ish | Notes |
|---|---|---|---|
| `txn_batch` (100 puts, one `transact`) | memory | 100 µs | ~1.0 M puts/s |
| `txn_batch` | redb | 1.02 ms | ~98 k puts/s |
| `envelope_tax` (200 × 1 KiB, **one put each**) | crossbank redb, default LZ4+CRC | 97.5 ms | ~2.0 MiB/s |
| same | crossbank redb, raw chain | 111 ms | LZ4 *helps* on this ramp payload |
| same | raw redb, one write txn | 2.82 ms | ~69 MiB/s |

**How to read this.** The 35× gap between `envelope_tax/raw_redb` and
`envelope_tax/crossbank_*` is mostly **one commit per put**, not the envelope.
`txn_batch` is the fair comparison: 100 puts in one closure-scoped transaction
on redb is about 1 ms. Callers who care about bulk ingest should `transact`.

LZ4 on a sequential-byte payload is a win here. The open question of LZ4 on
dense `f64` candle data is still open — this snapshot used a compressible
ramp, not IEEE floats. Do not drop LZ4 from the default chain on this evidence.

Web timings (`tests/bench_web.rs`, ignored) land the same named workloads
against IndexedDB; they are not in this table, but they are paired against
Hive CE on IndexedDB under **Web comparison (2026-08-21, re-run after Phase 3)** below.

### M4 numbers (2026-08-20, same machine)

**Chunk-size sweep** — one 8 MiB value, `LazyLocker::put` then `get`, redb, default chain:

| `chunk_size` | time | throughput |
|---|---|---|
| **256 KiB** | 14.96 ms | **535 MiB/s** |
| 1 MiB | 15.38 ms | 520 MiB/s |
| 4 MiB | 16.27 ms | 492 MiB/s |
| 8 MiB | 21.94 ms | 365 MiB/s |

Smaller chunks win: per-chunk LZ4 + CRC over a bounded buffer beats one large seal, and
256 KiB also keeps peak memory the smallest. **Default stays 256 KiB.** The 8 MiB guess is
retired.

**LZ4 on dense `f64`** — 1 MiB payload, one put + one get, redb:

| payload | chain | time |
|---|---|---|
| f64 OHLCV candles (random-walk mantissas) | default (LZ4+CRC) | 7.24 ms |
| f64 OHLCV candles | `FilterChain::raw()` | 7.21 ms |
| compressible ramp | default (LZ4+CRC) | 4.02 ms |
| compressible ramp | `FilterChain::raw()` | 7.41 ms |

LZ4 is ~1.0× on candle-shaped bytes — it neither helps nor measurably hurts — and 1.8× faster
end-to-end on compressible data (less to write). **LZ4 stays on by default.** A consumer with
a known-incompressible workload can open its bank with `FilterChain::raw()`; making the chain
selectable per locker rather than per bank landed at M6 (`LockerConfig::with_chain`), so a bank
can compress a candle series and store settings raw at the same time; the default is unchanged.

Reproduce: `cargo bench --bench kv -- "chunk_sweep|lz4_f64"`. A second full run put the 8 MiB
chunk at 17.5 ms; the ordering never changed.

### Hive CE comparison (2026-08-20, same machine, `bench/hive_ce`)

Hive CE (pure Dart, file backend, Bitcask-style append log) on the **same named workloads with
the same byte payloads**, via a tiny non-Flutter Dart tool. Raw JSON: `bench/results/2026-08-20.json`.

| Workload | Hive CE (file) | crossbank redb | crossbank memory |
|---|---|---|---|
| `settings_eager` — per op, 90/10 get/put, 1 KiB | 4.0 µs | 46 µs | 0.26 µs |
| `bulk_lazy_put` — 2 000 × 256 B, one put each | 27 ms (73 k/s) | **956 ms (2.1 k/s)** | 3.3 ms |
| `bulk_lazy_get` — per random get | 19 µs | 1.1 µs | 0.46 µs |
| `txn_batch` — 100 puts, one `putAll` / `transact` | 0.11 ms | 1.05 ms | 0.10 ms |
| `reopen` — write 1 KiB, close, reopen, read | 1.3 ms | 11.5 ms | — |
| `big_value_put_get` — one 8 MiB value | 50 ms | **15.8 ms** | — |

**Read the durability column before the speed column.** Every crossbank `put` on redb is a
*durable* commit — the data is on disk when the future resolves, which is what the
crash-recovery tests prove. Hive CE's `put` appends a frame to its log and returns; there is no
`fsync`, so a power cut can lose the last writes and a torn frame is dropped at next open. That
single difference is most of the 35× gap on `bulk_lazy_put` and the 10× on `settings_eager`
writes. It is the same per-commit cost that `envelope_tax` already isolates (≈0.5 ms/commit).
The honest crossbank answer to "many small puts" is `transact` — `txn_batch` is within 10× of
Hive's non-durable `putAll`, and that is the shape a candle-cache fill or manifest update should use.

Where crossbank is ahead it is structural, not tuning: lazy reads are **17×** faster (redb
B-tree page reads vs Hive's seek-and-parse on the log) and an 8 MiB value is **3×** faster
end-to-end despite LZ4+CRC on every chunk, because it never builds one contiguous 8 MiB
frame. `reopen` is slower (11 ms vs 1.3 ms) — redb opens and validates a real database
file, Hive opens an empty-ish log; this will matter for a bank opened once per app start, not
per operation.

Caveats that make this a comparison of *engines on identical byte workloads*, not of apps:
different languages and runtimes (Dart GC vs none), `Uint8List` vs `Vec<u8>` through postcard,
Hive `TypeAdapter`s are bypassed, and Hive's eager `Box.get` is a `Map` lookup exactly as
crossbank's `Locker::get` is. Web Hive (IndexedDB) vs crossbank IndexedDB was not run in this
snapshot; it is the next section, measured 2026-08-21.

**What the numbers change in the plan:** nothing in the design; one item in the roadmap.
The durable-per-put cost is real for the `settings_eager` shape (Hive `Box`-style UI settings
written on every toggle). M5 gains a small, explicit item: a *write-coalescing* option for
eager lockers (`Policy`-level, off by default) so a burst of settings writes can share one
commit without giving up durability-on-return for callers that want it.

### Web comparison (2026-08-21, re-run after Phase 3) — Hive CE IndexedDB vs crossbank IndexedDB

This is the comparison that decides "can crossbank replace Hive **on the web**". It is now
apples-to-apples in every respect that used to be a caveat: the same six named workloads over
the same byte payloads, both halves sampled the same way — one un-timed warm-up plus **20 timed
iterations**, median and p99, on `performance.now()` — in the **same two browsers**. Chrome is
Google Chrome 151.0.7922.108 (Playwright `executablePath` for the Hive half, chromedriver for
the wasm half, so it is literally one binary); Firefox is Playwright's Firefox 153.0 for Hive
and geckodriver's Firefox for the wasm half. Raw JSON: `bench/results/2026-08-21-web.json`.
The pre-Phase-3 snapshot is kept verbatim in `bench/results/2026-08-21-web-prephase3.json`.

Reproduce: `ci/bench.sh --hive --web --no-native --chrome` and the same with `--firefox`.

**Read the durability column first — and note that on the web there isn't one.** Natively,
crossbank's advantage is bought with an fsync per put and Hive's speed is bought by not having
one. On IndexedDB *neither* engine is fsync-durable: Chromium runs IndexedDB transactions in
`"relaxed"` durability by default, so a resolved put means "the transaction committed to the
browser's store", not "the bytes survive a power cut". Hive CE web puts are not fsync-durable,
and crossbank's IndexedDB backend inherits exactly the same browser-dependent guarantee. So
unlike the native table, this one really is speed vs speed.

All figures are **milliseconds per timed iteration, p50 / p99**. Lower is better; the winning
p50 of each pair is bold.

| Workload (per timed iteration) | Chrome — Hive CE | Chrome — crossbank | Firefox — Hive CE | Firefox — crossbank |
|---|---|---|---|---|
| `settings_eager` — 1 000 ops, 90/10 get/put, 1 KiB | 21.4 / 43.5 | **12.4** / 26.7 | **14.0** / 21.0 | 17.9 / 28.6 |
| `bulk_lazy_put` — 2 000 × 256 B, one put each | 406 / 874 | **346** / 413 | **260** / 1 092 | 489 / 869 |
| `bulk_lazy_get` — 1 000 scattered lazy gets | 119 / 443 | **52.3** / 79.6 | 132 / 555 | **94.0** / 564 |
| `txn_batch` — 100 puts in one `putAll` / `transact` | **8.30** / 218 | 9.37 / 24.8 | **8.00** / 14.0 | 12.4 / 41.2 |
| `reopen` — write 1 KiB, close, reopen, read | **0.60** / 0.70 | 0.85 / 3.01 | 4.00 / 7.00 | **3.36** / 15.3 |
| `big_value_put_get` — one 8 MiB value | **17.3** / 121 | 22.7 / 32.6 | 56.0 / 86.0 | **55.2** / 67.5 |

The legacy small shapes (50 settings keys, 200 bulk ops) are still emitted by both tools, so the
pre-Phase-3 rows have a like-for-like successor. They are secondary; the table above is the
comparison.

| Workload | Chrome — Hive CE | Chrome — crossbank | Firefox — Hive CE | Firefox — crossbank |
|---|---|---|---|---|
| `settings_eager_web_small` — 200 in-memory gets | 0.10 / 0.20 | **0.015** / 0.025 | 0.00 / 1.00 † | **0.020** / 0.040 |
| `bulk_lazy_put_web_small` — 200 × 256 B | 171 / 229 | **34.2** / 47.2 | **31.0** / 42.0 | 55.2 / 80.3 |
| `bulk_lazy_get_web_small` — 200 lazy gets | 23.1 / 33.5 | **11.5** / 14.9 | 18.0 / 23.0 | **14.4** / 24.0 |

† Firefox clamps `performance.now()` to ~1 ms for privacy, which is why every Hive-on-Firefox
figure is a whole number and a sub-millisecond loop reads as 0. crossbank's wasm half gets the
finer clock, so the two Firefox columns are not comparable *below about 1 ms*. Above that they
are, which covers every row that matters.

**The trigger for hand-rolled IndexedDB bindings is NOT hit.** The threshold PLAN set was
">2× slower than Hive on `bulk_lazy_put`". Measured: Chrome **0.85×** (crossbank is *faster*,
346 ms vs 406 ms), Firefox **1.88×** (489 ms vs 260 ms). Neither browser reaches 2×, and the
Chrome number is on the right side of 1.0. **We keep the `indexed-db` crate and do not write
raw bindings.** Revisit only if a later measurement puts a shipping browser past 2×.

**How to read the rest of it.** crossbank wins both read shapes in both browsers — 2.3× on
`bulk_lazy_get` in Chrome — which is the same structural win the native table shows: a keyed
store read against Hive's in-memory frame index plus a fetch. Hive still wins `txn_batch` and
`big_value_put_get` on Chrome by a small margin, and `reopen` on Chrome by 0.25 ms, which is a
per-app-start cost, not a per-op one. crossbank pays LZ4+CRC and a postcard envelope on every
value and is still level or ahead on four of six Chrome rows — a completely different picture
from the pre-Phase-3 snapshot, where the honest summary was "within ~2×".

crossbank's **p99s are consistently tighter than Hive's** where the medians are close: 24.8 ms
against 218 ms on `txn_batch`/Chrome, 413 ms against 874 ms on `bulk_lazy_put`/Chrome, 47 ms
against 229 ms on the small put row. Hive's tail is its append log meeting an IndexedDB pause;
crossbank's chunked commits spread the same work more evenly. For a UI that must not jank, the
p99 column is the one that decides.

**Noise, stated plainly.** Repeated runs of *identical* code on this machine moved
`bulk_lazy_put`/crossbank/Chrome between 200 ms and 362 ms, and Hive's
`bulk_lazy_put_web_small`/Chrome between 22 ms and 171 ms. **Treat anything inside about 2× as a
tie** and read the p99s; a single row of this table is not a result, the shape of the whole
table is.

**What is now unified** (the Phase 5 item that was outstanding): `tests/bench_web.rs` runs the
same large shapes as `benches/kv.rs`, from shared constants in `benches/common/mod.rs` (the Rust
twin of `bench/hive_ce/lib/workloads.dart`), warms up, samples 20 iterations, reports median and
p99 from `performance.now()`, and emits the same JSON row schema the Hive web tool emits. The
rows now carry a `browser` field, so Chrome and Firefox coexist in one results file. The lane
also tears its database down *before* it logs, which was the cause of the runner exiting
non-zero on a run that had already produced its numbers; `ci/bench.sh --web --chrome` and
`--firefox` both exit 0.

### Native re-run (2026-08-21, after Phase 3) — `cargo bench --bench kv`

Raw JSON: `bench/results/2026-08-21.json`. Same machine as 2026-08-20.

| Workload | crossbank redb | redb, `Eventual` + `flush` | crossbank memory | Hive CE (file, 2026-08-20) |
|---|---|---|---|---|
| `settings_eager` — per op, 90/10, 1 KiB | 41.2 µs | **1.00 µs** | 0.25 µs | 4.0 µs |
| `bulk_lazy_put` — 2 000 × 256 B | 948 ms | **27.8 ms** | 3.23 ms | 27 ms |
| `bulk_lazy_get` — per random get | 1.30 µs | — | 0.51 µs | 19 µs |
| `txn_batch` — 100 puts, one `transact` | 1.24 ms | — | 0.19 ms | 0.11 ms |
| `reopen` — open an existing bank, read a key | **2.91 ms** | — | — | 1.3 ms |
| `reopen_warm` — same file, already opened once | 1.41 ms | — | — | — |
| `index_open` — open a locker holding 2 000 keys | 1.91 ms | — | — | — |
| `envelope_tax` — 200 × 1 KiB, one put each | 95.6 ms | — | — | — |
| `chunk_sweep` — 8 MiB value, 256 KiB chunks | 16.0 ms | — | — | 50 ms |

`reopen/redb` **changed meaning** in this run and the old number should not be compared
against it. It used to create a fresh redb file, write to it, close it and reopen it, all
inside the timed closure — so it was measuring a create *and* an open against Hive's open.
The create-and-write half now lives in `iter_batched` setup, and only `Bank::open` +
`lazy_locker` + one `get` is timed. That is 2.91 ms against Hive's 1.3 ms, not the 10.0 ms
the Phase 3 table records for the old shape.

The `Eventual` column is the Phase 3 durability knob with its `flush` paid for, and it is
the row to read beside Hive CE: Hive's puts are not fsync-durable either, and on that
footing crossbank is **4× faster** on the settings shape and level on bulk put. The
default column still pays an fsync per put and is still the default.

### Phase 3 — performance (2026-08-21, same machine)

Six work items, one commit each, each A/B'd against the commit before it.
Native numbers are `cargo bench --bench kv`; web numbers are
`tests/bench_web.rs` in headless Firefox, release, median of three or more
runs. **Read the web column with the noise in mind** — repeated runs of
*identical* code on this machine spread 65–104 ms on `bulk_put_200`, so
anything under about 15% there is not a result.

| # | Change | Metric | Before | After |
|---|---|---|---|---|
| 1 | `Durability::Eventual` + explicit `flush` | `settings_eager/redb` | 42.5 µs/op | **1.17 µs/op** |
| 1 | same | `bulk_lazy_put/redb` (2 000 puts) | 907 ms | **25.5 ms** |
| 2 | skip read-before-write | `bulk_lazy_put/redb_eventual` | 25.5 ms | 24.9 ms |
| 2 + 4 | fewer IDB transactions per op | `bulk_put_200` (web) | ~102 ms | **~66 ms** |
| 3 | no write txn on open, one `get_many` | `reopen/redb_warm` | 1.62 ms | **1.39 ms** |
| 3 | same | `reopen/redb` (create + reopen) | 10.38 ms | 10.04 ms |
| 4 | chunk reads via `get_many` | `chunk_sweep/256KiB` (native) | 15.5 ms | 15.3 ms (noise) |
| 4 | same | `chunked_get_4mib` (web) | ~209 ms | ~190 ms |
| 5 | kill payload copies | `chunk_sweep/8192KiB` | 22.85 ms | **21.80 ms** |
| 5 | backend-advertised scan page (256 → 1024) | `index_open/redb` (2 000 keys) | 1.95 ms | 1.89 ms (noise) |
| 6 | `join_all` the IDB requests in one commit | — | — | **reverted** |

**Item 1 is the whole phase.** Everything else is single digits; the
durability knob is 36× on both of the shapes Hive was beating us on, and it
takes crossbank past Hive CE on Hive's own ground — `settings_eager` 1.17 µs
against Hive's 4.0 µs, `bulk_lazy_put` 25.5 ms against Hive's 27 ms — while
still committing every write atomically and making it reopen-visible. What is
traded is only the per-commit `fsync`, and only until `flush`. The default is
unchanged and still pays for the `fsync` on every put.

**Items 2 and 4 are web items and were measured as such.** Natively they are
worth almost nothing, because a redb read is a cheap B-tree hit; on IndexedDB
each one they remove is a whole transaction. `bulk_put_200` at roughly 102 →
66 ms is the pair landing on the platform that actually ships. They are
*kept on that evidence*, not on the native numbers, and the native numbers are
recorded above as the nothing they are.

**Item 5 is kept partly for a reason the clock cannot show.** `codec/api.rs`
already documents it: on a 32-bit target whose linear memory never shrinks, a
transient extra copy of a large value raises the process's memory ceiling
permanently. The 4.6% on an 8 MiB value is the visible half.

**Item 6 was reverted, and this is why.** Issuing a commit's puts and deletes
and then `join_all`-ing them does work — the `indexed-db` waker accepts it and
all 112 browser tests pass, so trap 8 is genuinely satisfied when everything
joined is an IDB request. It simply buys nothing. On a chunked 4 MiB put, one
commit carrying ~64 puts and the best case this change has, it measured 164 ms
before and 157 ms after: inside the noise. On the far commoner single-op
commit it was measurably *worse* — `bulk_put_200` went 68 → 90 ms — because
every op now costs a `Box::pin` for a batch of one. Paying a boxed allocation
per write, plus an argument about IndexedDB request ordering that has to stay
true forever, to buy nothing is a bad trade, so it went back.

The general lesson is worth more than the item: **crossbank's IndexedDB cost
is transactions, not requests within a transaction.** Items 2 and 4 removed
transactions and showed up plainly. Item 6 reshuffled requests inside one and
did not. A future web optimisation should be aimed at the former.


---

## Open questions

- ~~Chunk size default~~ — **resolved in M4: 256 KiB**, measured (see Performance).
- ~~Whether the streaming `Writer` should participate in transactions~~ — **resolved: no.**
  It spans many commits; the pointer swap at `finish()` is the only atomic step.
- Whether `indexed-db`'s age (latest stable Jan 2025, 0.5.0 yanked) forces hand-rolled
  `web-sys` bindings. Revisit if M3 hits a wall.
- ~~Whether LZ4 earns its CPU on f64 candle data~~ — **measured in M4: ~1.0×, free.** It
  stays on. `FilterChain::raw()` is the opt-out, per bank or — since M6 —
  per locker via `LockerConfig::with_chain`, whose id is recorded in `meta` and enforced on
  every later open.

## Known limitations / review notes

Behaviours a review flagged and we deliberately left as they are. They are
documented rather than fixed because each is either working as designed or
a cost we have chosen to carry. None of them loses data.

- **`Bank::verify` checks the envelope, not the type.** It validates the CBNK
  header and the filter chain (so a CRC failure or a foreign blob is caught)
  but does not attempt a `postcard` decode, because the typed decode lives on
  the locker and `verify` is deliberately type-free. **A clean `verify` is
  therefore not a promise that a strict (`OnCorrupt::Fail`) open will
  succeed** — a record whose bytes are intact but no longer match the type it
  is opened as passes verify and fails the open.
- **`OnCorrupt::Skip` also skips schema drift.** Skip cannot tell "these bytes
  are damaged" from "these bytes were written by an older shape of `T`" —
  both surface as a decode failure. Both are skipped, and both are listed in
  `corrupt_keys` / `verify`, which is where a caller sees what was dropped.
  This is the documented behaviour, not an accident: the alternative is
  refusing to open at all, which is what `OnCorrupt::Fail` is for.
- **`delete_bank` on a still-open native bank is refused, not attempted.** It
  used to unlink under a live fd: on Unix the file stays alive until the last
  handle closes, so an open `Bank` kept working against a file that no longer
  had a name and its later commits went nowhere visible, while on Windows the
  unlink failed with an opaque permission error. A process-local registry of
  open native bank paths (`OPEN_BANKS` in `src/bank.rs`, keyed by the
  canonicalised path, entered by `Bank::open` and left by `Bank::close` or
  `Drop`) now answers `Error::InvalidConfig` — *close the bank first* — and
  removes nothing. The remaining hole is a bank built with `Bank::with_backend`
  over a hand-made `RedbBackend`: nothing tracks it, so the old hazard stands
  for that arrangement, which is already documented as unsupported.
- **A deferred write is announced when it is staged, not when it commits.**
  That is when it becomes visible to its own handle, which is what a watcher
  on that handle is asking about — but it does mean an `Event::Put` can
  precede a commit that later fails. `Commit::Immediate`, the default, has no
  such window.
- **The eager size limit moves to flush time under `Commit::Deferred`.** A
  `put` still seals the value to check it, but a batch is re-sealed as one
  write-set, so a `ValueTooLarge` from a staged write surfaces from `flush`.
- **The value-id counter is now seeded from the data, not only from its own
  bookkeeping.** It used to persist `current + 1` at *allocation* time, and
  that op rode whatever commit the caller was still building. A commit that
  landed behind a newer one therefore wrote the stored `next_value_id` back
  below an id that was still live, and a reopen handed that id out again — two
  values' pieces interleaved under one `chunks` prefix, where a GC by prefix
  takes both. Only IndexedDB's awaits suspend, so only the web could reach it.
  Two changes answer it. The counter op is built at commit-build time from the
  RAM cursor's current high-water mark (`ValueIds::counter_op`, the shape
  `Ticks::counter_op` already used), which narrows the window to the commit
  itself; and a fresh cursor starts at the larger of the stored
  `next_value_id` and one past the highest id present in `chunks` — one
  reverse scan of a single record — which closes it, because the `chunks`
  table is the data and no racing commit can walk it backwards. A
  compare-and-set the `Backend` trait does not have would let the first half
  stand alone; the second half means it does not need to.
  `a_late_commit_cannot_re_issue_a_live_value_id` in the conformance suite
  fails without the fix on every backend. Ticks: the LRU clock got the
  same shape of fix — see the `fix(lru)` entry in `CHANGELOG.md`.
- **Corrupt bytes and an oversized value look the same at the eager sink.**
  A coherence message either carries a value or does not. When it does not —
  because the write was past the inline limit, or was chunked — and when it
  does but this tab cannot decode it, the eager sink reaches the same place:
  it cannot hold the value, so it drops the resident copy and raises
  `Event::Stale`. A caller therefore cannot tell "another tab wrote something
  too big to carry" from "another tab wrote something this build cannot
  decode" from the event alone. Both are honest — the resident copy really is
  gone in both cases, and the stored bytes really are intact in both — and
  distinguishing them would mean either carrying a reason code the receiver
  cannot verify, or attempting a decode the message has no bytes for. Reopen
  the locker (or read the key through a lazy handle) to find out which it was.
- **A deferred batch larger than the whole byte budget is kept whole.** When a
  single commit's own writes exceed `Policy::Evictable`'s `max_bytes`, every
  other key is shed and the batch stays, so the locker is briefly over budget.
  The alternative is refusing to store what the caller just asked us to store,
  or deleting values in the same commit that wrote them — which is the bug
  this replaced. The next commit that does not overshoot brings it back down.
- **Cross-tab epochs order one sender, not two.** See
  `BankConfig::coherence`: each bank's epoch counter is its own, so two tabs
  writing one key concurrently are not ordered against each other. Storage is
  always consistent (the backend serialises the commits); it is the *resident*
  copies that may disagree with it until a reopen. A key two tabs genuinely
  contend for belongs in a lazy locker, which reads through.
- **`Durability::Eventual` is a native-only trade.** IndexedDB exposes no
  fsync lever at all, so an `Eventual` locker on the web behaves exactly like
  an `Immediate` one and the setting costs nothing there. That keeps the
  configuration portable rather than platform-specific, but it does mean the
  speed-up is native. `IndexedDbBackend` deliberately does not override
  `commit_with`: a backend that ignores the knob is more durable than asked,
  never less.
- **An `Eventual` locker that is never flushed can lose its last writes to a
  power cut.** Not to a crash — the commit is atomic and reopen-visible the
  moment it returns — but the `fsync` is deferred until `flush`. This is the
  whole point of the knob, and it is why the default is `Immediate` and why
  `flush` on an `Eventual` locker forces the backend fsync as well as
  committing whatever is staged. The consumer's duty is exactly the one
  `Commit::Deferred` already imposes, and one `flush` discharges both.
- **The write path's fast path gives up on doubt rather than tracking harder.**
  `put` and `delete` skip the read that finds chunks to GC when the RAM index
  proves the key absent, or proves the record inline. Both proofs are refused
  outright while anything is staged, and the inline marker is dropped
  wholesale on a clear, a transaction, a close, and per key on any cross-tab
  change. A `transact` therefore always takes the slow path. That leaves reads
  on the table, deliberately: the failure mode of being wrong here is orphaned
  chunks that nothing will ever reclaim, and one wasted read is a much better
  price than a leak.
- **One `Bank` per backend instance; two is not supported.** `Bank::with_backend`
  will happily build a second bank over the same `Arc<dyn Backend>`, and the
  two share no state at all: separate open-locker registries, separate name-id
  caches, separate resident lists. So neither can see that the other holds a
  handle on a name — the sharing that makes every handle on one name a view of
  one open locker stops at the bank boundary, and the two banks' resident
  values and RAM indexes diverge silently. That is the `name_shared` hazard
  (a stale RAM index proving a key absent and orphaning its chunks forever),
  and it also defeats the "is it open?" refusals in
  `delete_locker` and `quarantine`, and `flush_all`'s claim to cover every
  open locker. This is a documented constraint rather than an enforced one:
  the backend `Arc` is the caller's, and a bank cannot tell whether another
  bank shares it without a registry that would outlive both. Share the one
  `Bank` — it is `Sync`, and `BankHandle` is cheap. Separate processes or tabs
  are a different question, and are what coherence is for.
- **Bank maintenance needs to be told about per-locker filter chains.**
  `verify`, `quarantine`, `locker_bytes` and `delete_locker` read records with
  no locker handle and so no config, and resolve each locker's chain from its
  `chain::{id}` meta record. A locker opened in this process registers its
  chain automatically; one that has not been opened here names a chain id the
  bank cannot resolve. Deletion still works (it moves bytes and never opens
  them), but `verify` refuses with `Error::SchemaMismatch` rather than
  reporting every key as corrupt — that list is documented as `quarantine`
  input, so a wrong answer there is a whole-locker delete. `Bank::register_chain`
  is the way to hand it the chain up front.
- **A locker written before per-locker chains existed can never adopt one.**
  Such a locker has no `chain::{id}` record and its bytes were sealed with the
  bank chain, so the first open under a *different* chain is refused
  (`Error::SchemaMismatch`) rather than recorded; only the bank chain open
  writes the id. There is no way to tell "legacy, sealed with the bank chain"
  from "fresh name" other than whether the name already existed, and guessing
  wrong stamps a lie into `meta` that bricks every stored value. Copy the
  values into a new locker name to move a locker onto its own chain.
- **Every handle on one locker name shares one state, and `close` closes the
  name.** `Bank::locker(name)` used to hand out an independent handle per call,
  each with its own resident values or key index, and they never synchronised —
  so a `get` through one could answer with a value another had already
  overwritten. That is a silent wrong answer, and the shape a Hive-style shim
  (which calls `box(name)` at hundreds of call sites) would hit constantly, so
  the handles now share: one `Inner`, one resident map or index, one staged
  batch, one watcher set, one coherence registration. Three consequences are
  deliberate. A second open must **agree** with the first — a different value
  type or container kind is `SchemaMismatch`, a different `LockerConfig` is
  `InvalidConfig` naming the field — because sharing means one set of rules
  governs both handles' writes. `close()` on any handle closes the locker for
  all of them, as `box.close()` does in Hive; the alternative (a handle still
  serving reads out of state its own `close` released) would make `close` mean
  nothing. And an eager locker's value type now needs `Send + Sync`: the bank
  holds the shared map type-erased so it can hand it to the next handle, and
  `Arc::downcast` — the only route back to the typed map without `unsafe`,
  which this crate forbids — exists solely for `Arc<dyn Any + Send + Sync>`.
- **Recovering an eager key after `Event::Stale` means closing the locker
  first.** The documented recovery was "reopen the locker", which used to mean
  a second `Bank::locker(name)` call reading storage afresh. Now that every
  handle on a name is a view of one open locker, that call returns the same
  resident state, stale key and all. `close()` then open, or read the key
  through a lazy handle, which never answers from a resident value at all.
- **A key written twice in one transaction is collapsed.** Only the last write
  per key is committed (and everything before a `clear` is dropped), so the
  eager size check applies to what actually lands rather than to every
  intermediate value.

---

## Appendix — the eventual wise_apple drop-in (not this project)

Recorded so crossbank's API allows it. **No wise_apple work happens here.**

The Hive surface a Dart shim must reproduce: `get` (with `defaultValue:`), `put`, `putAll`,
`delete`, `deleteAll`, `clear`, `keys`, `containsKey`, `length`, `toMap`, `watch`
(optionally `key:`), and `listenable(keys:)`. Plus statics `box`, `openBox`, `lazyBox`,
`openLazyBox`, `isBoxOpen`, `boxExists`, `deleteBoxFromDisk`, `init`/`initFlutter`.

Never used in app code: `putAt`, `deleteAt`, `getAt`, `keyAt`, `add`, `addAll`, `values`,
`valuesBetween`, `isEmpty`, `isNotEmpty`, `flush`, `compact`, `close`, `deleteFromDisk`.
No auto-increment keys, no encryption, no custom open options.

Three constraints to carry:
1. `watch()` consumers read only `event.key` — our event shape is more than enough.
2. Production never closes a box, but the test suite closes heavily (`Hive.close()` at 95
   sites). A shim must support close-and-reopen in-process.
3. One test round-trips an **integer key**. A shim must encode non-string keys
   deterministically; binary keys make that straightforward.

One thing not to copy: `DartKvStore::get` treats an empty payload as `None`. crossbank must
distinguish a stored empty value from a missing key, and the conformance suite asserts it.
