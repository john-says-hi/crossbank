# Bench results

Dated snapshots, not CI gates. One file per recorded run: `YYYY-MM-DD.json` for
native runs, `YYYY-MM-DD-web.json` for in-browser runs.
We commit a representative file when PLAN.md's Performance section changes,
not every local run.

Schema (one document, `samples` is a list):

```json
{
  "tool": "bench/hive_ce | benches/kv.rs",
  "date_utc": "ISO-8601",
  "machine": "free text",
  "samples": [
    {
      "workload": "settings_eager",
      "backend": "hive_ce_file | hive_ce_web | crossbank_redb | crossbank_memory | crossbank_indexeddb_web | raw_redb",
      "n": 1000,
      "bytes": 1024,
      "p50_ms": 4.0,
      "p99_ms": 6.8,
      "ops_per_s": 248000
    }
  ]
}
```

`n` is the number of logical operations in one timed iteration (so `ops_per_s = n / p50`).
Criterion reports its own estimate as `p50_ms`; the Dart tool computes a true median over
`iterations` runs.

`p99_ms` is `null` when the tool cannot produce one (Criterion summaries, and the
single-shot wasm lane).

Reproduce:

```sh
cargo bench --bench kv                # crossbank + raw redb (Criterion)
ci/bench.sh --hive                    # also runs bench/hive_ce (needs a Dart SDK)
```

## Web lane

The one comparison that decides "replace Hive on the web": **Hive CE on IndexedDB vs
crossbank on IndexedDB, in the same browser, on identical byte workloads.**

```sh
ci/bench.sh --hive --web --no-native   # Hive CE half   -> backend hive_ce_web
ci/bench.sh --web      --no-native     # crossbank half -> backend crossbank_indexeddb_web
ci/bench.sh --hive --web               # both, plus the native Criterion run
```

Both default to `--chrome` (`/usr/bin/google-chrome`), which is the one browser both
halves can drive out of the box: Playwright `executablePath` for the Hive half,
chromedriver for the wasm half. `--firefox` switches both (Playwright must have a
Firefox build: `npx playwright install firefox`). Useful flags: `--iters N` (Hive
iteration count), `--date YYYY-MM-DD`, `--machine "..."`, `--debug-wasm`.

Rows are merged into `bench/results/<date>-web.json` keyed by `(workload, backend)`, so
the two halves can be run separately, hours apart, and still land in one file. Re-running
a half replaces only its own rows.

### The pieces

| Path | What it is |
|---|---|
| `bench/hive_ce/lib/workloads.dart` | shared shapes/payloads/median maths — imported by BOTH Hive tools so the rows are comparable |
| `bench/hive_ce/bin/hive_ce_bench.dart` | Hive CE, native file backend (`hive_ce_file`) |
| `bench/hive_ce/web/main.dart` + `index.html` | Hive CE on IndexedDB, `dart compile js` (`hive_ce_web`) |
| `ci/web-bench/run.mjs` | tiny same-origin, CSP-free Node static server + Playwright driver; captures `BENCH_JSON <json>` from the console |
| `ci/web-bench/merge-crossbank.mjs` | converts what `tests/bench_web.rs` logs into schema rows |
| `ci/web-bench/merge.mjs` | the merge + table printer |
| `ci/web-bench/resolve-playwright.mjs` | finds Playwright without adding a `package.json` to this repo |

### Traps

- **`ci/bench.sh --web` does NOT go through `ci/wasm-test.sh`.** That script asserts the
  full per-lane test count from `ci/expected-tests.txt` (110+); this is one `#[ignore]`d
  test, so the shrink detector would fail every time. `bench.sh` sets up the same runner
  environment itself and skips only the count assertion.
- **The wasm runner can exit non-zero after printing the numbers.** `tests/bench_web.rs`
  tears its IndexedDB database down after logging, and the runner sometimes loses the
  browser there. `bench.sh` records whatever JSON reached the log, warns, and only fails
  if there was none. A bench is not a gate.
- **The wasm lane defaults to `--release`.** A debug wasm build against an `-O2` dart2js
  build is a compiler-flag report, not a comparison. `--debug-wasm` opts out.
- **The two halves do not use the same method yet.** Hive is a median/p99 over 20 warmed
  iterations on `performance.now()`; `bench_web.rs` is one un-warmed shot on `Date.now()`
  (1 ms resolution), so its `p99_ms` is `null` and a sub-millisecond loop reads as `0`.
- **Shapes differ, so the Hive web tool emits both.** `bench_web.rs` uses 50 settings keys
  and 200 bulk ops; `benches/kv.rs` and the native Hive tool use 200 / 2000. The
  `*_web_small` rows mirror `bench_web.rs` byte-for-byte, which is why one apples-to-apples
  pair exists today. Unifying `bench_web.rs` onto the large shapes is the remaining
  Phase 5 item.
- **Every Hive iteration opens a freshly-named IndexedDB database.** A leftover database
  from a previous run in the same browser profile would make the first iteration lie.
