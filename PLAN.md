# crossbank — build plan

**Status: M5 complete.** 306 tests green natively — the full conformance suite against
**both** the memory and `redb` backends, crash-and-reopen tests that kill a real process, and
a peak-RSS test that streams 8 MiB through 64 KiB chunks. The same 46-case suite passes
against **IndexedDB** in Chrome and Firefox on both wasm lanes (plain and atomics), alongside
a browser-only cross-tab coherence test. **Data persists on desktop, mobile, and the web,
large lazy values no longer have to fit in RAM, and storage pressure now has an answer.**
M6 (consumer readiness) is next.

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
| 8 | Encryption | Pluggable `Cipher`, no crypto shipped |
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
| 23 | Send story | Boxed `CbFuture` alias + `RemoteBank`, **at M1** |
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
let remote = bank.remote();
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
   Bank / Locker / LazyLocker / Transaction / Writer / Reader / RemoteBank
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
- `Bank::remote()` returns a `Send + Sync + Clone` handle; `Bank::into_service()` is a future
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
ceiling, `RemoteBank` + `into_service()`, bounded watch, `schema_tag`, and an 18-case
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

**M6 — Consumer readiness.** Docs, worked example, publish. Shrinks to prose because the
proxy landed in M1.

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
against IndexedDB; they are not in this table yet.

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
selectable per locker rather than per bank is noted as an M6 ergonomics item, not a default
change.

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
crossbank's `Locker::get` is. Web Hive (IndexedDB) vs crossbank IndexedDB was not run: the
Dart-web compile is a time sink and the native comparison already answers the design question.
It stays a follow-up.

**What the numbers change in the plan:** nothing in the design; one item in the roadmap.
The durable-per-put cost is real for the `settings_eager` shape (Hive `Box`-style UI settings
written on every toggle). M5 gains a small, explicit item: a *write-coalescing* option for
eager lockers (`Policy`-level, off by default) so a burst of settings writes can share one
commit without giving up durability-on-return for callers that want it.

---

## Open questions

- ~~Chunk size default~~ — **resolved in M4: 256 KiB**, measured (see Performance).
- ~~Whether the streaming `Writer` should participate in transactions~~ — **resolved: no.**
  It spans many commits; the pointer swap at `finish()` is the only atomic step.
- Whether `indexed-db`'s age (latest stable Jan 2025, 0.5.0 yanked) forces hand-rolled
  `web-sys` bindings. Revisit if M3 hits a wall.
- ~~Whether LZ4 earns its CPU on f64 candle data~~ — **measured in M4: ~1.0×, free.** It
  stays on. `FilterChain::raw()` is the opt-out; per-locker chain selection is an M6 nicety.

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
- **`delete_bank` on a still-open native bank unlinks under a live fd.** The
  doc already says to close the bank first. On Unix the file stays alive until
  the last handle closes, so an open `Bank` keeps working against a file that
  no longer has a name, and its later commits go nowhere visible. Close first.
- **A deferred write is announced when it is staged, not when it commits.**
  That is when it becomes visible to its own handle, which is what a watcher
  on that handle is asking about — but it does mean an `Event::Put` can
  precede a commit that later fails. `Commit::Immediate`, the default, has no
  such window.
- **The eager size limit moves to flush time under `Commit::Deferred`.** A
  `put` still seals the value to check it, but a batch is re-sealed as one
  write-set, so a `ValueTooLarge` from a staged write surfaces from `flush`.
- **The value-id counter persists its high-water mark per commit.** If two
  chunk allocations commit out of order, the stored `next_value_id` can land
  one lower than the highest id actually handed out. Within a process the RAM
  cursor covers it; only a reopen immediately after such an interleaving could
  re-hand an id in use. The M5 tick counter is written the same way and
  carries the same caveat; making both durable-monotonic against out-of-order
  commits needs a compare-and-set the `Backend` trait does not have, so it is
  still open.
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
