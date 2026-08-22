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
      "ops_per_s": 248000,
      "browser": "chrome | firefox"
    }
  ]
}
```

`n` is the number of logical operations in one timed iteration (so `ops_per_s = n / p50`).
Criterion reports its own estimate as `p50_ms`; the Dart tool computes a true median over
`iterations` runs.

`p99_ms` is `null` when the tool cannot produce one (Criterion summaries). Both web
lanes produce a real p99.

`browser` is present on web rows only, and is part of the merge key, so Chrome and
Firefox rows for the same workload coexist in one file.

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

Rows are merged into `bench/results/<date>-web.json` keyed by `(workload, backend,
browser)`, so
the two halves can be run separately, hours apart, and still land in one file. Re-running
a half replaces only its own rows.

### The pieces

| Path | What it is |
|---|---|
| `bench/hive_ce/lib/workloads.dart` | shared shapes/payloads/median maths — imported by BOTH Hive tools so the rows are comparable |
| `bench/hive_ce/bin/hive_ce_bench.dart` | Hive CE, native file backend (`hive_ce_file`) |
| `bench/hive_ce/web/main.dart` + `index.html` | Hive CE on IndexedDB, `dart compile js` (`hive_ce_web`) |
| `ci/web-bench/run.mjs` | tiny same-origin, CSP-free Node static server + Playwright driver; captures `BENCH_JSON <json>` from the console |
| `benches/common/mod.rs` | shared Rust shapes/payloads — included by BOTH `benches/kv.rs` and `tests/bench_web.rs`, and the twin of `workloads.dart` |
| `ci/web-bench/merge-crossbank.mjs` | picks the `BENCH_JSON` line out of a `tests/bench_web.rs` run and merges its rows |
| `ci/web-bench/merge.mjs` | the merge + table printer |
| `ci/web-bench/resolve-playwright.mjs` | finds Playwright without adding a `package.json` to this repo |

### Traps

- **`ci/bench.sh --web` does NOT go through `ci/wasm-test.sh`.** That script asserts the
  full per-lane test count from `ci/expected-tests.txt` (110+); this is one `#[ignore]`d
  test, so the shrink detector would fail every time. `bench.sh` sets up the same runner
  environment itself and skips only the count assertion.
- **`tests/bench_web.rs` tears its database down BEFORE it logs.** It used to log first,
  and the runner would sometimes lose the browser during the teardown and exit non-zero
  on a run that had already produced its numbers. Keep the cleanup ahead of the print.
  `bench.sh` still records whatever JSON reached the log and only fails if there was none;
  a bench is not a gate.
- **The wasm runner captures console output unless you pass `--nocapture`.** Without it
  the `BENCH_JSON` line never reaches the log and the merge fails with "no bench_web JSON
  found on stdin" on an otherwise green run.
- **The wasm lane defaults to `--release`.** A debug wasm build against an `-O2` dart2js
  build is a compiler-flag report, not a comparison. `--debug-wasm` opts out.
- **The two halves now use the same method.** Both are a median/p99 over 20 timed
  iterations after one un-timed warm-up, on `performance.now()`, over the same shapes.
  If you change the sampling on one side, change it on the other or the tables stop
  being comparable.
- **Shapes live in two files that must move together**: `benches/common/mod.rs` (Rust)
  and `bench/hive_ce/lib/workloads.dart` (Dart). Every constant in one has a same-named
  constant in the other.
- **The `*_web_small` rows are legacy shapes, kept on purpose.** They are what
  `bench_web.rs` measured before Phase 5 (50 settings keys, 200 bulk ops), retained so the
  pre-Phase-3 snapshot in `2026-08-21-web-prephase3.json` still has a like-for-like
  successor. The large rows are the ones to read.
- **Run-to-run spread on the web is large.** Repeated runs of identical code moved
  `bulk_lazy_put` between 200 ms and 490 ms and Hive's `bulk_lazy_put_web_small` between
  22 ms and 170 ms. Treat anything under about 2x as a tie, and read the p99 column.
- **Every Hive iteration opens a freshly-named IndexedDB database.** A leftover database
  from a previous run in the same browser profile would make the first iteration lie.
